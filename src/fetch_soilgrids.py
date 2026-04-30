"""
fetch_soilgrids.py

Augment a field-data CSV with SoilGrids v2 predictions.

Three access methods are tried in order:
  1. REST API  — api.isric.org  (primary)
  2. REST API  — rest.soilgrids.org  (legacy fallback)
  3. COG stream — files.isric.org GeoTIFFs via GDAL /vsicurl/
     (different domain, works when both REST endpoints are blocked)

Output columns (prefix sg_):
  sg_phh2o_0-5cm, sg_phh2o_5-15cm
  sg_soc_0-5cm,   sg_soc_5-15cm       (soil organic carbon, g/kg)
  sg_nitrogen_0-5cm, sg_nitrogen_5-15cm (g/kg)
  sg_clay_0-5cm,  sg_clay_5-15cm      (g/kg ≈ %)
  sg_sand_0-5cm,  sg_sand_5-15cm
  sg_cec_0-5cm,   sg_cec_5-15cm       (mmol(c)/kg)

Usage:
    python src/fetch_soilgrids.py data/processed/field_data_with_clay.csv
    python src/fetch_soilgrids.py data/processed/field_data_with_clay.csv ^
        --output data/processed/field_data_with_soilgrids.csv
"""

import argparse
import os
import time

import numpy as np
import pandas as pd
import requests

# REST endpoints tried in order
_REST_URLS = [
    "https://api.isric.org/soilgrids/v2.0/properties/query",
    "https://rest.soilgrids.org/soilgrids/v2.0/properties/query",
]

# SoilGrids COG base URL (files.isric.org — different domain from the REST API)
# Files are in Interrupted Goode Homolosine projection (ESRI:54052 / EPSG:152160).
_COG_BASE = "https://files.isric.org/soilgrids/latest/data"

# SoilGrids is 250 m resolution (~0.002 degrees).
_SG_GRID_DEG = 0.002

PROPERTIES = ["phh2o", "soc", "nitrogen", "clay", "sand", "cec"]
DEPTHS     = ["0-5cm", "5-15cm"]

_D_FACTOR = {
    "phh2o":    10,
    "soc":      10,
    "nitrogen": 100,
    "clay":     10,
    "sand":     10,
    "cec":      10,
}

# CRS of SoilGrids COG files
_SG_CRS = "ESRI:54052"


# ── connectivity check ────────────────────────────────────────────────────────

def _check_connectivity() -> str:
    """Return 'rest' if any REST endpoint is reachable, 'cog' if only files.isric.org
    is reachable, or 'none' if everything is blocked."""
    for url in _REST_URLS:
        try:
            r = requests.get(url, params={"lon": 120.59, "lat": 16.45,
                                           "property": ["phh2o"], "depth": ["0-5cm"],
                                           "value": "mean"}, timeout=10)
            if r.status_code < 500:
                return "rest"
        except Exception:
            pass

    # Test COG endpoint
    test_url = f"/vsicurl/{_COG_BASE}/phh2o/phh2o_0-5cm_mean.tif"
    try:
        import rasterio
        with rasterio.open(test_url) as _:
            return "cog"
    except Exception:
        pass

    return "none"


# ── REST path ─────────────────────────────────────────────────────────────────

def _fetch_rest(lat: float, lon: float, retries: int = 3) -> dict:
    last_exc = None
    for url in _REST_URLS:
        for attempt in range(retries):
            try:
                r = requests.get(url, params={
                    "lon": lon, "lat": lat,
                    "property": PROPERTIES, "depth": DEPTHS, "value": "mean",
                }, timeout=30)
                r.raise_for_status()
                out = {}
                for layer in r.json().get("properties", {}).get("layers", []):
                    prop     = layer["name"]
                    d_factor = _D_FACTOR.get(prop, 1)
                    for di in layer.get("depths", []):
                        val = di.get("values", {}).get("mean")
                        out[f"sg_{prop}_{di['label']}"] = (val / d_factor) if val is not None else np.nan
                return out
            except Exception as exc:
                last_exc = exc
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
    raise RuntimeError(str(last_exc))


# ── COG path ──────────────────────────────────────────────────────────────────

def _build_cog_cache() -> dict[str, object]:
    """Open all COG files once and return {col_name: rasterio_dataset}."""
    import rasterio
    from rasterio.warp import transform as rio_transform

    datasets = {}
    for prop in PROPERTIES:
        for depth in DEPTHS:
            col  = f"sg_{prop}_{depth}"
            path = f"/vsicurl/{_COG_BASE}/{prop}/{prop}_{depth}_mean.tif"
            try:
                ds = rasterio.open(path)
                datasets[col] = ds
            except Exception as exc:
                print(f"    WARNING: could not open COG {path}: {exc}")
    return datasets


def _fetch_cog(lat: float, lon: float, datasets: dict) -> dict:
    """Sample all open COG datasets at (lat, lon)."""
    import rasterio
    from rasterio.warp import transform as rio_transform

    out = {}
    for col, ds in datasets.items():
        try:
            xs, ys = rio_transform("EPSG:4326", ds.crs, [lon], [lat])
            row, col_idx = ds.index(xs[0], ys[0])
            from rasterio.windows import Window
            win   = Window(max(0, col_idx - 1), max(0, row - 1), 3, 3)
            patch = ds.read(1, window=win).astype(float)
            patch[patch == ds.nodata] = np.nan
            raw = float(np.nanmean(patch))

            # Parse property name from column name (sg_phh2o_0-5cm → phh2o)
            prop     = col[3:col.rindex("_", 0, col.rindex("-") - 2)]
            d_factor = _D_FACTOR.get(prop, 1)
            out[col] = raw / d_factor if np.isfinite(raw) else np.nan
        except Exception:
            out[col] = np.nan
    return out


# ── unified fetch ─────────────────────────────────────────────────────────────

def _sg_key(lat: float, lon: float) -> tuple[float, float]:
    return (
        round(round(lat / _SG_GRID_DEG) * _SG_GRID_DEG, 6),
        round(round(lon / _SG_GRID_DEG) * _SG_GRID_DEG, 6),
    )


def fetch_all(df: pd.DataFrame, delay: float = 0.5) -> pd.DataFrame:
    print("Checking ISRIC connectivity...")
    mode = _check_connectivity()
    if mode == "rest":
        print("  REST API reachable — using api.isric.org")
    elif mode == "cog":
        print("  REST API blocked — falling back to COG streaming via files.isric.org")
    else:
        print("  ERROR: all ISRIC endpoints unreachable.")
        print("  Possible fixes:")
        print("    1. Check your network / DNS (try: ping api.isric.org)")
        print("    2. Use a VPN")
        print("    3. Download tiles manually from https://files.isric.org/soilgrids/latest/data/")
        print("  Continuing without SoilGrids features.")
        return df

    df["_sg_lat"] = df["latitude"].apply(lambda x: _sg_key(x, 0.0)[0])
    df["_sg_lon"] = df["longitude"].apply(lambda x: _sg_key(0.0, x)[1])
    pts = df[["_sg_lat", "_sg_lon"]].drop_duplicates().reset_index(drop=True)
    print(f"Querying {len(pts)} unique 250 m cells (from {len(df)} rows)...")

    cog_datasets = _build_cog_cache() if mode == "cog" else {}

    cache: dict[tuple, dict] = {}
    for i, row in pts.iterrows():
        key = (row["_sg_lat"], row["_sg_lon"])
        print(f"  [{i+1}/{len(pts)}] ({key[0]:.4f}, {key[1]:.4f})", end="  ", flush=True)
        try:
            if mode == "rest":
                result = _fetch_rest(key[0], key[1])
            else:
                result = _fetch_cog(key[0], key[1], cog_datasets)
        except Exception as exc:
            print(f"failed: {exc}")
            result = {}
        cache[key] = result
        n_ok = sum(1 for v in result.values() if isinstance(v, float) and np.isfinite(v))
        print(f"{n_ok} values" if result else "no data")
        if mode == "rest" and i < len(pts) - 1:
            time.sleep(delay)

    if mode == "cog":
        for ds in cog_datasets.values():
            try:
                ds.close()
            except Exception:
                pass

    sg_rows = [cache.get((r["_sg_lat"], r["_sg_lon"]), {}) for _, r in df.iterrows()]
    df = df.drop(columns=["_sg_lat", "_sg_lon"])
    sg_df = pd.DataFrame(sg_rows, index=df.index)

    if not sg_df.empty and len(sg_df.columns):
        n_valid = sg_df.iloc[:, 0].notna().sum()
        print(f"\nSoilGrids coverage: {n_valid}/{len(df)} rows have data")

    return pd.concat([df, sg_df], axis=1)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Add SoilGrids v2 predictions to a field-data CSV"
    )
    parser.add_argument("input_csv", nargs="?",
                        default="data/processed/field_data_with_clay.csv")
    parser.add_argument("--output", default=None)
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Seconds between REST API calls (default: 0.5)")
    args = parser.parse_args()

    if not os.path.isfile(args.input_csv):
        print(f"Input file not found: {args.input_csv}")
        return

    df = pd.read_csv(args.input_csv, on_bad_lines="skip", low_memory=False)
    print(f"Loaded {len(df)} rows from {args.input_csv}")

    if "latitude" not in df.columns or "longitude" not in df.columns:
        print("ERROR: CSV must have 'latitude' and 'longitude' columns.")
        return

    already = [c for c in df.columns if c.startswith("sg_")]
    if already:
        print(f"WARNING: {len(already)} sg_* columns already present — overwriting.")
        df = df.drop(columns=already)

    df_out = fetch_all(df, delay=args.delay)

    out_path = args.output or (os.path.splitext(args.input_csv)[0] + "_soilgrids.csv")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df_out.to_csv(out_path, index=False, quoting=1)
    print(f"\nSaved: {out_path}  ({len(df_out.columns)} columns)")

    sg_cols = [c for c in df_out.columns if c.startswith("sg_")]
    if sg_cols:
        print(f"New sg_* columns ({len(sg_cols)}): {', '.join(sg_cols)}")
    print(f"\nNext step:")
    print(f"  python src/train_ordinal.py {out_path} --deduplicate --filter-barangay Paoay")


if __name__ == "__main__":
    main()

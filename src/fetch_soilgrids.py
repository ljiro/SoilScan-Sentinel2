"""
fetch_soilgrids.py

Augment a field-data CSV with SoilGrids v2 predictions from the ISRIC REST API.
Queries pH, SOC, nitrogen, clay, sand, CEC at 0-5 cm and 5-15 cm depth for every
unique GPS point, then joins the results back to the full CSV.

Output columns (prefix sg_):
  sg_phh2o_0-5cm, sg_phh2o_5-15cm
  sg_soc_0-5cm,   sg_soc_5-15cm       (soil organic carbon, g/kg)
  sg_nitrogen_0-5cm, sg_nitrogen_5-15cm (g/kg)
  sg_clay_0-5cm,  sg_clay_5-15cm      (g/kg, ~%)
  sg_sand_0-5cm,  sg_sand_5-15cm
  sg_cec_0-5cm,   sg_cec_5-15cm       (mmol(c)/kg)

Usage:
    python src/fetch_soilgrids.py data/processed/field_data_with_clay.csv
    python src/fetch_soilgrids.py data/processed/field_data_with_clay.csv \\
        --output data/processed/field_data_with_soilgrids.csv
"""

import argparse
import os
import time

import numpy as np
import pandas as pd
import requests

# Primary and fallback endpoints (ISRIC migrated from rest.soilgrids.org → api.isric.org)
_SOILGRIDS_URLS = [
    "https://api.isric.org/soilgrids/v2.0/properties/query",
    "https://rest.soilgrids.org/soilgrids/v2.0/properties/query",
]

# SoilGrids is 250 m resolution (~0.002 degrees). Round GPS to this grid before
# deduplication so we don't make hundreds of calls for points that map to the
# same 250 m pixel.
_SG_GRID_DEG = 0.002

PROPERTIES = ["phh2o", "soc", "nitrogen", "clay", "sand", "cec"]
DEPTHS     = ["0-5cm", "5-15cm"]

# SoilGrids v2 stores values as integers; divide by d_factor to get real units
_D_FACTOR = {
    "phh2o":    10,    # → pH units
    "soc":      10,    # → g/kg
    "nitrogen": 100,   # → g/kg
    "clay":     10,    # → g/kg  (≈ %)
    "sand":     10,
    "silt":     10,
    "cec":      10,    # → mmol(c)/kg
    "bdod":     100,   # → kg/dm³ (not requested, but listed for completeness)
}


def fetch_point(lat: float, lon: float, retries: int = 3) -> dict:
    """Query SoilGrids for one GPS point. Tries each endpoint in turn."""
    last_exc = None
    for url in _SOILGRIDS_URLS:
        for attempt in range(retries):
            try:
                r = requests.get(
                    url,
                    params={
                        "lon":      lon,
                        "lat":      lat,
                        "property": PROPERTIES,
                        "depth":    DEPTHS,
                        "value":    "mean",
                    },
                    timeout=30,
                )
                r.raise_for_status()
                # Parse and return on first success
                out = {}
                for layer in r.json().get("properties", {}).get("layers", []):
                    prop     = layer["name"]
                    d_factor = _D_FACTOR.get(prop, 1)
                    for depth_info in layer.get("depths", []):
                        depth = depth_info["label"]
                        val   = depth_info.get("values", {}).get("mean")
                        out[f"sg_{prop}_{depth}"] = (val / d_factor) if val is not None else np.nan
                return out
            except Exception as exc:
                last_exc = exc
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)

    print(f"    WARNING: SoilGrids failed for ({lat:.4f}, {lon:.4f}): {last_exc}")
    return {}


def _sg_key(lat: float, lon: float) -> tuple[float, float]:
    """Round to the SoilGrids 250 m grid (~0.002 deg) to avoid redundant API calls."""
    return (
        round(round(lat / _SG_GRID_DEG) * _SG_GRID_DEG, 6),
        round(round(lon / _SG_GRID_DEG) * _SG_GRID_DEG, 6),
    )


def fetch_all(df: pd.DataFrame, delay: float = 0.5) -> pd.DataFrame:
    """Fetch SoilGrids for every unique 250 m cell; join back to df."""
    df["_sg_lat"] = df["latitude"].apply(lambda x: _sg_key(x, 0.0)[0])
    df["_sg_lon"] = df["longitude"].apply(lambda x: _sg_key(0.0, x)[1])

    pts = df[["_sg_lat", "_sg_lon"]].drop_duplicates().reset_index(drop=True)
    print(f"Querying SoilGrids for {len(pts)} unique 250 m cells "
          f"(from {len(df)} rows)...")

    cache: dict[tuple, dict] = {}
    for i, row in pts.iterrows():
        key = (row["_sg_lat"], row["_sg_lon"])
        print(f"  [{i+1}/{len(pts)}] ({key[0]:.4f}, {key[1]:.4f})", end="  ", flush=True)
        result = fetch_point(key[0], key[1])
        cache[key] = result
        n_ok = sum(1 for v in result.values() if not (isinstance(v, float) and np.isnan(v)))
        print(f"{n_ok} values" if result else "no data")
        if i < len(pts) - 1:
            time.sleep(delay)

    sg_rows = []
    for _, row in df.iterrows():
        key = (row["_sg_lat"], row["_sg_lon"])
        sg_rows.append(cache.get(key, {}))

    df = df.drop(columns=["_sg_lat", "_sg_lon"])
    sg_df = pd.DataFrame(sg_rows, index=df.index)

    if not sg_df.empty and len(sg_df.columns):
        first_col = sg_df.columns[0]
        n_valid = sg_df[first_col].notna().sum()
        print(f"\nSoilGrids coverage: {n_valid}/{len(df)} rows have data")

    return pd.concat([df, sg_df], axis=1)


def main():
    parser = argparse.ArgumentParser(
        description="Add SoilGrids v2 predictions to a field-data CSV"
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        default="data/processed/field_data_with_clay.csv",
        help="Input CSV with latitude/longitude columns",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output CSV path (default: overwrites input with _soilgrids suffix)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds between API calls (default: 0.5 — be polite to ISRIC servers)",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.input_csv):
        print(f"Input file not found: {args.input_csv}")
        return

    df = pd.read_csv(args.input_csv, on_bad_lines="skip")
    print(f"Loaded {len(df)} rows from {args.input_csv}")

    if "latitude" not in df.columns or "longitude" not in df.columns:
        print("ERROR: CSV must have 'latitude' and 'longitude' columns.")
        return

    # Skip if already fetched
    already = [c for c in df.columns if c.startswith("sg_")]
    if already:
        print(f"WARNING: {len(already)} sg_* columns already present — overwriting.")
        df = df.drop(columns=already)

    df_out = fetch_all(df, delay=args.delay)

    if args.output:
        out_path = args.output
    else:
        base, ext = os.path.splitext(args.input_csv)
        out_path = f"{base}_soilgrids{ext}"

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df_out.to_csv(out_path, index=False, quoting=1)
    print(f"\nSaved: {out_path}  ({len(df_out.columns)} columns)")

    sg_cols = [c for c in df_out.columns if c.startswith("sg_")]
    print(f"New sg_* columns ({len(sg_cols)}): {', '.join(sg_cols)}")
    print(f"\nNext step:")
    print(f"  python src/train_ordinal.py {out_path} --deduplicate --filter-barangay Paoay")


if __name__ == "__main__":
    main()

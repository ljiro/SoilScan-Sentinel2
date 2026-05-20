"""
fetch_sentinel1.py

Augment a field-data CSV with Sentinel-1 GRD backscatter values (VV, VH)
fetched from the Copernicus Data Space Ecosystem.

For each GPS point, searches for the nearest S1 IW GRD product within the
specified date window, downloads it, and samples a 3×3 pixel neighbourhood.
Stores VV and VH as log10-scaled values (dB proxy) plus the VV/VH ratio.

Output columns:
  S1_VV      — mean log10 backscatter, VV polarisation (3×3 neighbourhood)
  S1_VH      — mean log10 backscatter, VH polarisation
  S1_VV_VH   — VV - VH ratio (surface roughness / volume scattering indicator)
  S1_date    — acquisition date of the matched product (YYYYMMDD)

Usage:
    python src/fetch_sentinel1.py data/processed/field_data_with_bands_growing.csv
    python src/fetch_sentinel1.py data/processed/field_data_with_bands_growing.csv \\
        --date-range 2025-10-01 2025-11-30 \\
        --output data/processed/field_data_with_s1.csv
"""

import argparse
import glob
import os
import re

import numpy as np
import pandas as pd
import rasterio
import requests
from dotenv import load_dotenv
from rasterio.warp import transform as rio_transform
from rasterio.windows import Window

from data_fetcher_copernicus import (
    CATALOG_URL,
    CDSE_S3_ENDPOINT,
    CDSE_S3_BUCKET,
    FIELD_DOWNLOAD_DIR,
    SPATIAL_GRID_DEG,
    DATE_TOLERANCE_DAYS,
    get_auth_headers,
    download_and_extract,
)

load_dotenv()

S1_DOWNLOAD_DIR = os.path.join(FIELD_DOWNLOAD_DIR, "s1")
MAX_S1_TILES    = 1   # one tile per spatial cell is enough for backscatter
_EPS            = 1.0 # floor DN before log to avoid log(0)


# ── catalog ──────────────────────────────────────────────────────────────────

def search_s1_products(bbox_wkt: str, start_str: str, end_str: str,
                       auth_headers: dict) -> list[dict]:
    """Return up to MAX_S1_TILES S1 IW GRD products covering bbox, sorted by date desc."""
    fq = (
        f"Collection/Name eq 'SENTINEL-1' "
        f"and OData.CSC.Intersects(area=geography'SRID=4326;{bbox_wkt}') "
        f"and ContentDate/Start ge {start_str} "
        f"and ContentDate/Start le {end_str} "
        f"and contains(Name,'IW_GRDH')"
    )
    params = {
        "$filter": fq,
        "$orderby": "ContentDate/Start desc",
        "$top": MAX_S1_TILES * 5,
    }
    try:
        r = requests.get(CATALOG_URL, headers=auth_headers, params=params, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        print(f"    S1 catalog search failed: {exc}")
        return []
    return r.json().get("value", [])[:MAX_S1_TILES]


# ── local SAFE lookup ─────────────────────────────────────────────────────────

def _find_local_s1(product_name: str) -> str | None:
    """Return path to local S1 .SAFE directory if already on disk."""
    for candidate in (
        os.path.join(S1_DOWNLOAD_DIR, product_name),
        os.path.join(S1_DOWNLOAD_DIR, product_name + ".SAFE"),
    ):
        if os.path.isdir(candidate):
            tiffs = glob.glob(os.path.join(candidate, "measurement", "*.tiff"))
            if tiffs:
                return candidate
    return None


# ── band file lookup ──────────────────────────────────────────────────────────

def find_s1_band_files(safe_dir: str) -> dict[str, str]:
    """Return {'VV': path, 'VH': path} from the measurement/ folder."""
    out = {}
    for tiff in glob.glob(os.path.join(safe_dir, "measurement", "*.tiff")):
        name = os.path.basename(tiff).lower()
        if "-vv-" in name:
            out["VV"] = tiff
        elif "-vh-" in name:
            out["VH"] = tiff
    return out


# ── sampling ──────────────────────────────────────────────────────────────────

def sample_s1_at_point(safe_dir: str, lon: float, lat: float) -> dict | None:
    """Sample VV and VH backscatter at (lon, lat) from a S1 GRD .SAFE directory.

    Returns dict with S1_VV, S1_VH, S1_VV_VH (all in log10 proxy dB scale),
    or None if the point falls outside the tile or band files are missing.
    """
    bands = find_s1_band_files(safe_dir)
    if "VV" not in bands or "VH" not in bands:
        return None

    vals = {}
    for pol, path in bands.items():
        try:
            with rasterio.open(path) as src:
                xs, ys = rio_transform("EPSG:4326", src.crs, [lon], [lat])
                row, col = src.index(xs[0], ys[0])
                win = Window(max(0, col - 1), max(0, row - 1), 3, 3)
                patch = src.read(1, window=win).astype(float)
                if patch.size == 0:
                    return None
                dn_mean = float(np.nanmean(patch))
                # log10-scale proxy for sigma0 dB (floor at _EPS to avoid log(0))
                vals[f"S1_{pol}"] = float(np.log10(max(dn_mean, _EPS)))
        except Exception:
            return None

    if len(vals) < 2:
        return None

    vals["S1_VV_VH"] = vals["S1_VV"] - vals["S1_VH"]
    return vals


# ── main fetch ────────────────────────────────────────────────────────────────

def fetch_sentinel1(df: pd.DataFrame, date_range: tuple[str, str] | None = None,
                    growing_season_offset: int = 0) -> pd.DataFrame:
    """Download S1 GRD tiles and sample VV/VH for every GPS point in df."""
    from datetime import datetime, timedelta

    auth = get_auth_headers()

    # Spatial cell grouping (same as data_fetcher_copernicus)
    df["_lat_cell"] = (df["latitude"]  / SPATIAL_GRID_DEG).round() * SPATIAL_GRID_DEG
    df["_lon_cell"] = (df["longitude"] / SPATIAL_GRID_DEG).round() * SPATIAL_GRID_DEG
    cells = df[["_lat_cell", "_lon_cell"]].drop_duplicates().values.tolist()

    os.makedirs(S1_DOWNLOAD_DIR, exist_ok=True)

    # s1_cache[lat_cell, lon_cell] = (safe_dir, product_date_str)
    s1_cache: dict[tuple, tuple[str, str] | None] = {}

    print(f"Searching S1 GRD for {len(cells)} spatial cells...")

    for lat_cell, lon_cell in cells:
        key = (lat_cell, lon_cell)
        half = 0.05
        bbox_wkt = (
            f"POLYGON(({lon_c-half} {lat_c-half},{lon_c+half} {lat_c-half},"
            f"{lon_c+half} {lat_c+half},{lon_c-half} {lat_c+half},{lon_c-half} {lat_c-half}))"
        )

        # Determine date window
        if date_range:
            start_str = f"{date_range[0]}T00:00:00.000Z"
            end_str   = f"{date_range[1]}T23:59:59.000Z"
        else:
            # Use the median capture date of points in this cell
            cell_rows = df[(df["_lat_cell"] == lat_c) & (df["_lon_cell"] == lon_c)]
            if "capture_datetime" in cell_rows.columns:
                median_dt = pd.to_datetime(
                    cell_rows["capture_datetime"], format="mixed", utc=True
                ).median()
                target = median_dt.date() - timedelta(days=growing_season_offset)
            else:
                target = datetime.today().date()
            start_str = (target - timedelta(days=DATE_TOLERANCE_DAYS)).strftime("%Y-%m-%dT00:00:00.000Z")
            end_str   = (target + timedelta(days=DATE_TOLERANCE_DAYS)).strftime("%Y-%m-%dT23:59:59.000Z")

        print(f"  Cell ({lat_c:.4f}, {lon_c:.4f})  window: {start_str[:10]} → {end_str[:10]}")

        products = search_s1_products(bbox_wkt, start_str, end_str, auth)
        if not products:
            print(f"    No S1 products found.")
            s1_cache[key] = None
            continue

        product      = products[0]
        product_id   = product["Id"]
        product_name = product.get("Name", product_id)
        date_match   = re.search(r"_(\d{8})T", product_name)
        product_date = date_match.group(1) if date_match else "unknown"
        print(f"    Found: {product_name[:60]}...  date={product_date}")

        safe = _find_local_s1(product_name)
        if safe:
            print(f"    Using local: {safe}")
        else:
            print(f"    Downloading...")
            try:
                safe = download_and_extract(product_id, product_name, auth, S1_DOWNLOAD_DIR)
            except Exception as exc:
                print(f"    Download failed: {exc}")
                s1_cache[key] = None
                continue

        s1_cache[key] = (safe, product_date)

    def _append_no_data():
        vv_vals.append(np.nan)
        vh_vals.append(np.nan)
        ratio_vals.append(np.nan)
        dates.append(None)

    # ── sample backscatter for every row ──────────────────────────────────────
    vv_vals, vh_vals, ratio_vals, dates = [], [], [], []
    n_ok = 0

    for _, row in df.iterrows():
        key   = (row["_lat_cell"], row["_lon_cell"])
        entry = s1_cache.get(key)
        if entry is None:
            _append_no_data()
            continue

        safe, pdate = entry
        result = sample_s1_at_point(safe, row["longitude"], row["latitude"])
        if result:
            vv_vals.append(result["S1_VV"])
            vh_vals.append(result["S1_VH"])
            ratio_vals.append(result["S1_VV_VH"])
            dates.append(pdate)
            n_ok += 1
        else:
            _append_no_data()

    df = df.drop(columns=["_lat_cell", "_lon_cell"], errors="ignore")
    df["S1_VV"]    = vv_vals
    df["S1_VH"]    = vh_vals
    df["S1_VV_VH"] = ratio_vals
    df["S1_date"]  = dates

    print(f"\nS1 sampling complete: {n_ok}/{len(df)} rows have backscatter values")
    return df


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Add Sentinel-1 GRD VV/VH backscatter to a field-data CSV"
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        default="data/processed/field_data_with_clay.csv",
        help="Input CSV with latitude/longitude columns",
    )
    parser.add_argument(
        "--output", default=None,
        help="Output CSV path (default: input basename + _s1 suffix)",
    )
    parser.add_argument(
        "--date-range", nargs=2, metavar=("START", "END"),
        help="Fixed date window YYYY-MM-DD YYYY-MM-DD (overrides growing-season-offset)",
    )
    parser.add_argument(
        "--growing-season-offset", type=int, default=0, metavar="DAYS",
        help="Shift search window N days before each sample's capture date (default: 0)",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.input_csv):
        print(f"Input file not found: {args.input_csv}")
        return

    df = pd.read_csv(args.input_csv, on_bad_lines="skip")
    print(f"Loaded {len(df)} rows from {args.input_csv}")

    already = [c for c in df.columns if c.startswith("S1_")]
    if already:
        print(f"WARNING: {len(already)} S1_* columns already present — overwriting.")
        df = df.drop(columns=already)

    date_range = tuple(args.date_range) if args.date_range else None
    df_out = fetch_sentinel1(
        df,
        date_range=date_range,
        growing_season_offset=args.growing_season_offset,
    )

    if args.output:
        out_path = args.output
    else:
        base, ext = os.path.splitext(args.input_csv)
        out_path = f"{base}_s1{ext}"

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    df_out.to_csv(out_path, index=False, quoting=1)
    print(f"\nSaved: {out_path}  ({len(df_out.columns)} columns)")
    print(f"\nNext step:")
    print(f"  python src/extract_clay_embeddings.py --input {out_path}")


if __name__ == "__main__":
    main()

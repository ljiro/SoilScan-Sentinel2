"""
analyze_vegetation_timeline.py

Scans the previous N months of Sentinel-2 imagery for each GPS cluster,
computes NDVI per tile, and produces a monthly vegetation profile to identify
the optimal date range for --date-range in data_fetcher_copernicus.py.

Strategy:
  1. Group GPS points into ~2 km spatial cells.
  2. For each cell, search CDSE catalog for all S2 L2A tiles in the lookback window.
  3. For each tile, sample NDVI using whichever is available first:
       a) Local .SAFE on disk (data/raw/field_products/)
       b) S3 streaming via GDAL /vsis3/ (requires CDSE_S3_ACCESS_KEY + SECRET_KEY)
  4. Aggregate NDVI by calendar month → mean, max, tile count.
  5. Print and save a report; recommend the peak month as --date-range window.

Usage:
    python src/analyze_vegetation_timeline.py data/processed/field_data_with_terrain.csv
    python src/analyze_vegetation_timeline.py data/processed/field_data_with_terrain.csv --months 6
    python src/analyze_vegetation_timeline.py ... --max-cloud 30 --plot
"""

import argparse
import calendar
import os
import re
import sys
from collections import defaultdict
from datetime import date, timedelta

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
import rasterio
from rasterio.warp import transform as rio_transform
from rasterio.windows import Window

from data_fetcher_copernicus import (
    CATALOG_URL,
    CDSE_S3_BUCKET,
    CDSE_S3_ENDPOINT,
    FIELD_DOWNLOAD_DIR,
    SPATIAL_GRID_DEG,
    find_band_files,
    get_auth_headers,
    sample_bands_at_point,
)

load_dotenv()

# BAND_NAMES index positions for B04 (red) and B08 (NIR) within sample_bands_at_point output
# sample_bands_at_point uses BAND_NAMES = ["B01","B02","B03","B04","B05","B06","B07","B08","B8A","B09","B11","B12"]
_B04_IDX = 3
_B08_IDX = 7

# ── catalog search ────────────────────────────────────────────────────────────
def _search_all_tiles(lon: float, lat: float, start_date: str, end_date: str,
                      auth_headers: dict, max_cloud: int = 30) -> list[dict]:
    """Return ALL S2 L2A products for a bbox + date range (no tile-count cap).

    Unlike search_products() in data_fetcher_copernicus which caps at MAX_TILES_PER_KEY,
    this returns everything in the window for timeline analysis.
    """
    half = 0.05
    bbox_wkt = (
        f"POLYGON(({lon-half} {lat-half},{lon+half} {lat-half},"
        f"{lon+half} {lat+half},{lon-half} {lat+half},{lon-half} {lat-half}))"
    )
    fq = (
        f"Collection/Name eq 'SENTINEL-2' "
        f"and OData.CSC.Intersects(area=geography'SRID=4326;{bbox_wkt}') "
        f"and ContentDate/Start ge {start_date}T00:00:00.000Z "
        f"and ContentDate/Start le {end_date}T23:59:59.000Z "
        f"and contains(Name,'MSIL2A')"
    )
    params = {"$filter": fq, "$orderby": "ContentDate/Start asc", "$top": 100}
    try:
        r = requests.get(CATALOG_URL, headers=auth_headers, params=params, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        print(f"    Catalog error: {exc}")
        return []

    results = []
    for p in r.json().get("value", []):
        pid  = p["Id"]
        name = p.get("Name", "")
        m = re.search(r"_(\d{8})T", name)
        tile_date = m.group(1) if m else None

        cc = None
        try:
            dr = requests.get(f"{CATALOG_URL}('{pid}')", headers=auth_headers, timeout=15)
            if dr.status_code == 200:
                for attr in dr.json().get("Attributes", []) or []:
                    if attr.get("Name") == "cloudCover":
                        cc = float(attr.get("Value", 100))
                        break
        except Exception:
            pass

        if cc is not None and cc > max_cloud:
            continue

        results.append({
            "id":   pid,
            "name": name,
            "date": tile_date,
            "cc":   cc if cc is not None else 100.0,
        })
    return results


# ── local SAFE lookup ─────────────────────────────────────────────────────────
def _find_local_safe(product_name: str) -> str | None:
    """Return path to existing local .SAFE directory, or None."""
    for candidate in (
        os.path.join(FIELD_DOWNLOAD_DIR, product_name),
        os.path.join(FIELD_DOWNLOAD_DIR, product_name + ".SAFE"),
    ):
        if os.path.isdir(os.path.join(candidate, "GRANULE")):
            return candidate
    return None


def _sample_ndvi_local(safe_dir: str, lon: float, lat: float) -> float | None:
    """Sample NDVI from a local .SAFE directory using the shared band sampler."""
    vals = sample_bands_at_point(safe_dir, lon, lat)
    if vals is None:
        return None
    b04, b08 = float(vals[_B04_IDX]), float(vals[_B08_IDX])
    denom = b08 + b04
    if denom == 0:
        return None
    ndvi = (b08 - b04) / denom
    return float(ndvi) if np.isfinite(ndvi) else None


# ── S3 streaming ──────────────────────────────────────────────────────────────
def _make_s3_client():
    """Return boto3 S3 client for CDSE, or None if credentials or boto3 missing."""
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except ImportError:
        return None
    ak = os.getenv("CDSE_S3_ACCESS_KEY")
    sk = os.getenv("CDSE_S3_SECRET_KEY")
    if not ak or not sk:
        return None
    return boto3.client(
        "s3",
        endpoint_url=CDSE_S3_ENDPOINT,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        config=BotoConfig(signature_version="s3v4"),
        region_name="default",
    )


def _s3_find_band_keys(s3, product_name: str, bands: list[str]) -> dict[str, str]:
    """List the S3 prefix once and return a {band: key} dict for the requested bands."""
    date_str = product_name.split("_")[2][:8]
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    prefix = f"Sentinel-2/MSI/L2A/{year}/{month}/{day}/{product_name}/"
    band_pats = {b: re.compile(rf"_{b}_\d+m\.jp2$") for b in bands}
    found: dict[str, str] = {}
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=CDSE_S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                for band, pat in list(band_pats.items()):
                    if pat.search(key):
                        found[band] = key
                        del band_pats[band]
                if not band_pats:
                    return found
    except Exception:
        pass
    return found


def _build_gdal_env() -> dict:
    """Build GDAL VSI env once; reuse across S3 calls."""
    return {
        "AWS_S3_ENDPOINT":              CDSE_S3_ENDPOINT.replace("https://", ""),
        "AWS_HTTPS":                    "YES",
        "AWS_VIRTUAL_HOSTING":          "FALSE",
        "AWS_ACCESS_KEY_ID":            os.getenv("CDSE_S3_ACCESS_KEY", ""),
        "AWS_SECRET_ACCESS_KEY":        os.getenv("CDSE_S3_SECRET_KEY", ""),
        "AWS_REGION":                   "default",
        "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
        "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".jp2",
    }


def _sample_ndvi_s3(s3, product_name: str, lon: float, lat: float,
                    gdal_env: dict) -> float | None:
    """Compute NDVI by streaming B04+B08 from CDSE S3 (single prefix list call)."""
    keys = _s3_find_band_keys(s3, product_name, ["B04", "B08"])
    if "B04" not in keys or "B08" not in keys:
        return None

    vals = {}
    for band, key in keys.items():
        vsi = f"/vsis3/{CDSE_S3_BUCKET}/{key}"
        try:
            with rasterio.Env(**gdal_env):
                with rasterio.open(vsi) as src:
                    xs, ys = rio_transform("EPSG:4326", src.crs, [lon], [lat])
                    row, col = src.index(xs[0], ys[0])
                    win = Window(max(0, col - 1), max(0, row - 1), 3, 3)
                    patch = src.read(1, window=win).astype(float)
                    v = float(np.nanmean(patch))
                    if np.isfinite(v):
                        vals[band] = v
        except Exception:
            pass

    b04 = vals.get("B04")
    b08 = vals.get("B08")
    if b04 is None or b08 is None:
        return None
    denom = b08 + b04
    return float((b08 - b04) / denom) if denom != 0 else None


# ── main analysis ─────────────────────────────────────────────────────────────
def analyze(input_csv: str, months: int, max_cloud: int, plot: bool):
    df = pd.read_csv(input_csv)
    if "latitude" not in df.columns or "longitude" not in df.columns:
        sys.exit("Input CSV must have 'latitude' and 'longitude' columns.")

    if "capture_datetime" in df.columns:
        end_date = pd.to_datetime(df["capture_datetime"], format="mixed", utc=True).max().date()
    else:
        end_date = date.today()

    start_date = end_date - timedelta(days=months * 31)
    print(f"Search window: {start_date} → {end_date}  ({months} months)")

    df["_lat_cell"] = (df["latitude"]  / SPATIAL_GRID_DEG).round() * SPATIAL_GRID_DEG
    df["_lon_cell"] = (df["longitude"] / SPATIAL_GRID_DEG).round() * SPATIAL_GRID_DEG
    clusters = df[["_lat_cell", "_lon_cell"]].drop_duplicates().values.tolist()
    print(f"Spatial clusters: {len(clusters)}")

    auth     = get_auth_headers()
    s3       = _make_s3_client()
    gdal_env = _build_gdal_env()
    if s3:
        print("S3 streaming enabled (will stream tiles not on disk).")
    else:
        print("S3 not configured — only locally cached .SAFE tiles will be sampled.")

    # month_ndvi[cluster_key][YYYY-MM] = [ndvi, ...]
    month_ndvi: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    for lat_c, lon_c in clusters:
        cluster_key = f"{lat_c:.4f},{lon_c:.4f}"
        print(f"\nCluster ({lat_c:.4f}, {lon_c:.4f})")

        tiles = _search_all_tiles(lon_c, lat_c, str(start_date), str(end_date),
                                  auth, max_cloud)
        if not tiles:
            print("  No tiles found.")
            continue
        print(f"  Found {len(tiles)} tiles in catalog.")

        for tile in tiles:
            name      = tile["name"]
            tile_date = tile["date"]
            if not tile_date or len(tile_date) < 6:
                continue
            month_key = f"{tile_date[:4]}-{tile_date[4:6]}"

            safe = _find_local_safe(name)
            if safe:
                ndvi   = _sample_ndvi_local(safe, lon_c, lat_c)
                source = "local"
            elif s3:
                ndvi   = _sample_ndvi_s3(s3, name, lon_c, lat_c, gdal_env)
                source = "s3"
            else:
                ndvi   = None
                source = "skipped"

            status = f"NDVI={ndvi:.3f}" if ndvi is not None else "no data"
            print(f"  {tile_date} cc={tile['cc']:.0f}% [{source}] {status}")

            if ndvi is not None:
                month_ndvi[cluster_key][month_key].append(ndvi)

    # ── aggregate and report ──────────────────────────────────────────────────
    rows = []
    for cluster_key, months_data in month_ndvi.items():
        lat_str, lon_str = cluster_key.split(",")
        for month_key, vals in sorted(months_data.items()):
            rows.append({
                "cluster":    cluster_key,
                "lat":        float(lat_str),
                "lon":        float(lon_str),
                "month":      month_key,
                "ndvi_mean":  float(np.mean(vals)),
                "ndvi_max":   float(np.max(vals)),
                "tile_count": len(vals),
            })

    if not rows:
        print("\nNo NDVI data collected. Check S3 credentials or run data_fetcher_copernicus.py first.")
        return

    result_df = pd.DataFrame(rows)
    os.makedirs("outputs", exist_ok=True)
    out_csv = "outputs/vegetation_timeline.csv"
    result_df.to_csv(out_csv, index=False)
    print(f"\nSaved: {out_csv}")

    # ── print summary table ───────────────────────────────────────────────────
    print("\n── Monthly NDVI profile (all clusters combined) ──")
    summary = (
        result_df
        .groupby("month")
        .agg(ndvi_mean=("ndvi_mean", "mean"),
             ndvi_max=("ndvi_max",  "max"),
             tiles=("tile_count",   "sum"))
        .reset_index()
        .sort_values("month")
    )

    print(f"{'Month':<10} {'NDVI mean':>10} {'NDVI max':>10} {'Tiles':>7}")
    print("-" * 40)
    peak_row = None
    for _, row in summary.iterrows():
        if peak_row is None or row["ndvi_mean"] > peak_row["ndvi_mean"]:
            peak_row = row
        print(f"{row['month']:<10} {row['ndvi_mean']:>10.3f} {row['ndvi_max']:>10.3f} {int(row['tiles']):>7}")

    if peak_row is not None:
        peak_month = peak_row["month"]
        yr, mo = int(peak_month[:4]), int(peak_month[5:])
        last_day = calendar.monthrange(yr, mo)[1]
        print(f"\n★ Peak vegetation month: {peak_month}  (mean NDVI={peak_row['ndvi_mean']:.3f})")
        print(f"  Suggested --date-range flag:")
        print(f"    --date-range {yr}-{mo:02d}-01 {yr}-{mo:02d}-{last_day:02d}")

    # ── optional plot ─────────────────────────────────────────────────────────
    if plot:
        try:
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(10, 5))
            for cluster_key, months_data in month_ndvi.items():
                months      = sorted(months_data.keys())
                monthly_means = [float(np.mean(months_data[m])) for m in months]
                ax.plot(months, monthly_means, marker="o", label=cluster_key, alpha=0.7)
            ax.set_xlabel("Month")
            ax.set_ylabel("Mean NDVI")
            ax.set_title("Monthly NDVI Profile per Spatial Cluster")
            ax.legend(fontsize=7, ncol=2)
            plt.tight_layout()
            out_png = "outputs/vegetation_timeline.png"
            fig.savefig(out_png, dpi=150)
            print(f"  Plot saved: {out_png}")
        except ImportError:
            print("  matplotlib not installed — skipping plot.")


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Monthly vegetation timeline from S2 NDVI")
    parser.add_argument("input_csv", help="Field data CSV with latitude/longitude columns")
    parser.add_argument("--months", type=int, default=6,
                        help="How many months back to search (default: 6)")
    parser.add_argument("--max-cloud", type=int, default=30,
                        help="Max cloud cover %% to accept (default: 30)")
    parser.add_argument("--plot", action="store_true",
                        help="Save a matplotlib timeline plot to outputs/")
    args = parser.parse_args()
    analyze(args.input_csv, args.months, args.max_cloud, args.plot)


if __name__ == "__main__":
    main()

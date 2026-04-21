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
import glob
import os
import re
import sys
from collections import defaultdict
from datetime import date, timedelta

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
from rasterio.warp import transform as rio_transform
import rasterio

load_dotenv()

# ── reuse fetcher constants ───────────────────────────────────────────────────
AUTH_URL      = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CATALOG_URL   = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
CDSE_S3_ENDPOINT = "https://eodata.dataspace.copernicus.eu"
CDSE_S3_BUCKET   = "eodata"
FIELD_DOWNLOAD_DIR = "data/raw/field_products"
SPATIAL_GRID_DEG   = 0.02   # ~2 km cell size for grouping


# ── auth ──────────────────────────────────────────────────────────────────────
def _get_token():
    user = os.getenv("COPERNICUS_USER")
    pw   = os.getenv("COPERNICUS_PASS")
    if not user or not pw:
        raise ValueError("Set COPERNICUS_USER and COPERNICUS_PASS in .env")
    r = requests.post(AUTH_URL, data={
        "client_id": "cdse-public", "username": user,
        "password": pw, "grant_type": "password",
    }, timeout=30)
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


# ── catalog search ────────────────────────────────────────────────────────────
def search_tiles(lon, lat, start_date: str, end_date: str, auth_headers, max_cloud=30):
    """Return all S2 L2A products (unsorted, no limit) for a bbox + date range."""
    half = 0.05   # ~5 km half-width around point
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
        # Parse date from product name: S2B_MSIL2A_20251023T...
        m = re.search(r"_(\d{8})T", name)
        tile_date = m.group(1) if m else None

        # Get cloud cover
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
    candidate = os.path.join(FIELD_DOWNLOAD_DIR, product_name)
    if os.path.isdir(candidate) and glob.glob(os.path.join(candidate, "GRANULE", "*")):
        return candidate
    # Also check .SAFE suffix variant
    candidate2 = candidate if candidate.endswith(".SAFE") else candidate + ".SAFE"
    if os.path.isdir(candidate2) and glob.glob(os.path.join(candidate2, "GRANULE", "*")):
        return candidate2
    return None


def _find_band_in_safe(safe_dir: str, band: str) -> str | None:
    """Find a specific band file inside a .SAFE directory."""
    patterns = [
        os.path.join(safe_dir, "GRANULE", "*", "IMG_DATA", "R10m", f"*_{band}_10m.jp2"),
        os.path.join(safe_dir, "GRANULE", "*", "IMG_DATA", "R20m", f"*_{band}_20m.jp2"),
        os.path.join(safe_dir, "GRANULE", "*", "IMG_DATA", f"*_{band}.jp2"),
    ]
    for pat in patterns:
        hits = glob.glob(pat)
        if hits:
            return hits[0]
    return None


def _sample_band_local(safe_dir: str, band: str, lon: float, lat: float) -> float | None:
    """Read a single-pixel band value from a local .SAFE directory."""
    path = _find_band_in_safe(safe_dir, band)
    if not path:
        return None
    try:
        with rasterio.open(path) as src:
            xs, ys = rio_transform("EPSG:4326", src.crs, [lon], [lat])
            row, col = src.index(xs[0], ys[0])
            from rasterio.windows import Window
            win = Window(max(0, col - 1), max(0, row - 1), 3, 3)
            patch = src.read(1, window=win).astype(float)
            val = float(np.nanmean(patch))
            return val if np.isfinite(val) else None
    except Exception:
        return None


# ── S3 streaming ──────────────────────────────────────────────────────────────
def _make_s3_client():
    """Return boto3 S3 client for CDSE, or None if credentials missing."""
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except ImportError:
        return None
    ak = os.getenv("CDSE_S3_ACCESS_KEY")
    sk = os.getenv("CDSE_S3_SECRET_KEY")
    if not ak or not sk:
        return None
    import boto3
    from botocore.config import Config as BotoConfig
    return boto3.client(
        "s3",
        endpoint_url=CDSE_S3_ENDPOINT,
        aws_access_key_id=ak,
        aws_secret_access_key=sk,
        config=BotoConfig(signature_version="s3v4"),
        region_name="default",
    )


def _s3_find_band_key(s3, product_name: str, band: str) -> str | None:
    """List the S3 prefix for a product and find the key for a given band."""
    date_str = product_name.split("_")[2][:8]
    y, mo, d = date_str[:4], date_str[4:6], date_str[6:8]
    prefix = f"Sentinel-2/MSI/L2A/{y}/{mo}/{d}/{product_name}/"
    band_pat = re.compile(rf"_{band}_\d+m\.jp2$")
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=CDSE_S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if band_pat.search(obj["Key"]):
                    return obj["Key"]
    except Exception:
        pass
    return None


def _sample_band_s3(s3, product_name: str, band: str, lon: float, lat: float) -> float | None:
    """Stream just the pixels around (lon, lat) for one band via S3 + rasterio VSI."""
    key = _s3_find_band_key(s3, product_name, band)
    if not key:
        return None

    ak = os.getenv("CDSE_S3_ACCESS_KEY")
    sk = os.getenv("CDSE_S3_SECRET_KEY")
    vsi_path = f"/vsis3/{CDSE_S3_BUCKET}/{key}"

    gdal_env = {
        "AWS_S3_ENDPOINT":        CDSE_S3_ENDPOINT.replace("https://", ""),
        "AWS_HTTPS":              "YES",
        "AWS_VIRTUAL_HOSTING":    "FALSE",
        "AWS_ACCESS_KEY_ID":      ak,
        "AWS_SECRET_ACCESS_KEY":  sk,
        "AWS_REGION":             "default",
        "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
        "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".jp2",
    }
    try:
        with rasterio.Env(**gdal_env):
            with rasterio.open(vsi_path) as src:
                xs, ys = rio_transform("EPSG:4326", src.crs, [lon], [lat])
                row, col = src.index(xs[0], ys[0])
                from rasterio.windows import Window
                win = Window(max(0, col - 1), max(0, row - 1), 3, 3)
                patch = src.read(1, window=win).astype(float)
                val = float(np.nanmean(patch))
                return val if np.isfinite(val) else None
    except Exception:
        return None


# ── NDVI computation ──────────────────────────────────────────────────────────
def compute_ndvi(b04: float | None, b08: float | None) -> float | None:
    if b04 is None or b08 is None:
        return None
    denom = b08 + b04
    if denom == 0:
        return None
    ndvi = (b08 - b04) / denom
    return float(ndvi) if np.isfinite(ndvi) else None


# ── main analysis ─────────────────────────────────────────────────────────────
def analyze(input_csv: str, months: int, max_cloud: int, plot: bool):
    df = pd.read_csv(input_csv)
    if "latitude" not in df.columns or "longitude" not in df.columns:
        sys.exit("Input CSV must have 'latitude' and 'longitude' columns.")

    # Determine lookback end date (most recent acquisition or today)
    if "capture_datetime" in df.columns:
        end_dt = pd.to_datetime(df["capture_datetime"], format="mixed", utc=True).max()
        end_date = end_dt.date()
    else:
        end_date = date.today()

    start_date = end_date - timedelta(days=months * 31)
    print(f"Search window: {start_date} → {end_date}  ({months} months)")

    # Spatial clustering
    df["_lat_cell"] = (df["latitude"]  / SPATIAL_GRID_DEG).round() * SPATIAL_GRID_DEG
    df["_lon_cell"] = (df["longitude"] / SPATIAL_GRID_DEG).round() * SPATIAL_GRID_DEG
    clusters = df[["_lat_cell", "_lon_cell"]].drop_duplicates().values.tolist()
    print(f"Spatial clusters: {len(clusters)}")

    auth = _get_token()
    s3   = _make_s3_client()
    if s3:
        print("S3 streaming enabled (will stream tiles not on disk).")
    else:
        print("S3 not configured — only locally cached .SAFE tiles will be sampled.")

    # month_ndvi[cluster_key][YYYY-MM] = [ndvi, ...]
    month_ndvi: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    for lat_c, lon_c in clusters:
        cluster_key = f"{lat_c:.4f},{lon_c:.4f}"
        print(f"\nCluster ({lat_c:.4f}, {lon_c:.4f})")

        tiles = search_tiles(lon_c, lat_c, str(start_date), str(end_date), auth, max_cloud)
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

            b04 = b08 = None

            # Try local SAFE first
            safe = _find_local_safe(name)
            if safe:
                b04 = _sample_band_local(safe, "B04", lon_c, lat_c)
                b08 = _sample_band_local(safe, "B08", lon_c, lat_c)
                source = "local"
            elif s3:
                b04 = _sample_band_s3(s3, name, "B04", lon_c, lat_c)
                b08 = _sample_band_s3(s3, name, "B08", lon_c, lat_c)
                source = "s3"
            else:
                source = "skipped"

            ndvi = compute_ndvi(b04, b08)
            status = f"NDVI={ndvi:.3f}" if ndvi is not None else "no data"
            print(f"  {tile_date} cc={tile['cc']:.0f}% [{source}] {status}")

            if ndvi is not None:
                month_ndvi[cluster_key][month_key].append(ndvi)

    # ── aggregate and report ──────────────────────────────────────────────────
    rows = []
    for cluster_key, months_data in month_ndvi.items():
        lat_s, lon_s = cluster_key.split(",")
        for month_key, vals in sorted(months_data.items()):
            rows.append({
                "cluster":    cluster_key,
                "lat":        float(lat_s),
                "lon":        float(lon_s),
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
        marker = ""
        if peak_row is None or row["ndvi_mean"] > peak_row["ndvi_mean"]:
            peak_row = row
        print(f"{row['month']:<10} {row['ndvi_mean']:>10.3f} {row['ndvi_max']:>10.3f} {int(row['tiles']):>7}")

    if peak_row is not None:
        peak_month = peak_row["month"]
        yr, mo = int(peak_month[:4]), int(peak_month[5:])
        # Suggest full-month window
        import calendar
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
                mons   = sorted(months_data.keys())
                means  = [float(np.mean(months_data[m])) for m in mons]
                ax.plot(mons, means, marker="o", label=cluster_key, alpha=0.7)
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

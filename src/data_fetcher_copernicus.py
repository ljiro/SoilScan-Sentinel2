"""
Field-level Sentinel-2 band extraction using Copernicus Data Space Ecosystem.
Downloads S2 L2A products covering your field locations/dates, then samples
B01..B12 at each (lat, lon). Output format matches data_fetcher.py for use
with train_ordinal.py.
"""
import concurrent.futures
import glob
import os
import re
import threading
import time
import zipfile
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import rasterio
import requests
from dotenv import load_dotenv
from rasterio.warp import transform
from rasterio.windows import Window
from tqdm import tqdm

load_dotenv()

# S3 endpoint for CDSE (much faster than HTTP zipper — bypasses download queue)
CDSE_S3_ENDPOINT  = "https://eodata.dataspace.copernicus.eu"
CDSE_S3_BUCKET    = "eodata"

# --- Config ---
AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
# Zipper endpoint accepts Bearer token for download (see CDSE forum / Compressed Product Download docs)
DOWNLOAD_URL_TEMPLATE = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({})/$value"
FIELD_DOWNLOAD_DIR = "data/raw/field_products"
BAND_NAMES     = ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12"]
BAND_STD_NAMES = [f"{b}_std" for b in BAND_NAMES]   # temporal std across composited tiles
# Days before/after capture date to search for imagery (wider window = more clean tiles)
DATE_TOLERANCE_DAYS = 45
# Max tiles to download per key for compositing (3 is usually sufficient)
MAX_TILES_PER_KEY = 3
# Days to shift the search window back from capture date for growing-season imagery.
# 0 = use capture date (default, post-harvest imagery).
# 75 = target ~Oct-Nov for Jan/Feb capture dates (mid-growing season in Benguet).
GROWING_SEASON_OFFSET_DAYS = 0
# Spatial grouping: same (lat_cell, lon_cell) share one product per date
SPATIAL_GRID_DEG = 0.02  # ~2 km


def get_auth_headers():
    """
    Authenticate with Copernicus Data Space.
    Download requires a token from the *password* grant (username + password with
    client_id=cdse-public). Client_credentials tokens often work for catalog but
    return 401 on download.
    """
    username = os.getenv("COPERNICUS_USER")
    password = os.getenv("COPERNICUS_PASS")

    # Password grant is required for product download (client_credentials often gives 401 on download)
    if username and password:
        r = requests.post(
            AUTH_URL,
            data={
                "client_id": "cdse-public",
                "username": username,
                "password": password,
                "grant_type": "password",
            },
        )
        if r.status_code == 200:
            token = r.json()["access_token"]
            return {"Authorization": f"Bearer {token}"}
        # If auth failed (e.g. wrong password, 2FA), do not fall back to client_credentials for download
        detail = r.text[:300].replace("\n", " ").strip()
        raise ValueError(
            f"Password login failed (status {r.status_code}). "
            "Check COPERNICUS_USER and COPERNICUS_PASS. "
            f"Auth response: {detail}. "
            "Docs: https://documentation.dataspace.copernicus.eu/APIs/Token.html"
        )

    raise ValueError(
        "Set COPERNICUS_USER and COPERNICUS_PASS in .env (your Copernicus Data Space login). "
        "Required for product download. See: https://documentation.dataspace.copernicus.eu/APIs/Token.html"
    )


def search_products(bbox_wkt, start_date, end_date, auth_headers, max_cloud=20):
    """Return up to MAX_TILES_PER_KEY S2 L2A products under max_cloud, sorted by cloud cover asc.

    Downloads cloud-cover metadata per product. Products with unknown cloud cover
    are included (they may still be usable). Returns [] if nothing found.
    """
    filter_query = (
        f"Collection/Name eq 'SENTINEL-2' "
        f"and OData.CSC.Intersects(area=geography'SRID=4326;{bbox_wkt}') "
        f"and ContentDate/Start ge {start_date} "
        f"and ContentDate/Start le {end_date} "
        f"and contains(Name,'MSIL2A')"
    )
    params = {
        "$filter": filter_query,
        "$orderby": "ContentDate/Start desc",
        "$top": 20,
    }
    try:
        r = requests.get(CATALOG_URL, headers=auth_headers, params=params, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        print(f"    Catalog search failed: {exc}")
        return []
    candidates = r.json().get("value", [])
    if not candidates:
        return []

    results = []
    for p in candidates:
        pid = p["Id"]
        cc = None
        try:
            dr = requests.get(f"{CATALOG_URL}('{pid}')", headers=auth_headers)
            if dr.status_code == 200:
                for attr in dr.json().get("Attributes", []) or []:
                    if attr.get("Name") == "cloudCover":
                        cc = float(attr.get("Value", 100))
                        break
        except Exception:
            pass
        if cc is None or cc <= max_cloud:
            # Use 100.0 for unknown cloud cover so these sort last, not first
            results.append({**p, "_cc": cc if cc is not None else 100.0})

    results.sort(key=lambda x: x["_cc"])
    return results[:MAX_TILES_PER_KEY]


def _probe_range_support(url, auth_headers):
    """Return True if the server honours Range requests (responds with 206)."""
    session = requests.Session()
    session.headers.update(auth_headers)
    try:
        r = session.get(url, headers={"Range": "bytes=0-0"}, stream=True, timeout=(30, 10))
        status = r.status_code
        r.close()
        return status == 206
    except Exception:
        return False


def _verify_zip(zip_path):
    """Full CRC integrity check. Returns True only if every entry passes testzip()."""
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            bad = z.testzip()   # returns first bad filename, or None if all OK
            return bad is None
    except Exception:
        return False


def _parallel_download(url, zip_path, total_bytes, desc, auth_headers, num_chunks):
    """Download url in parallel byte-range chunks into zip_path (always a fresh file)."""
    # Always start fresh — a pre-allocated file with null gaps cannot be safely resumed
    if os.path.exists(zip_path):
        os.remove(zip_path)

    chunk_size = total_bytes // num_chunks
    ranges = [
        (i * chunk_size,
         (i + 1) * chunk_size - 1 if i < num_chunks - 1 else total_bytes - 1)
        for i in range(num_chunks)
    ]

    # Pre-allocate: create a sparse file of exactly total_bytes
    with open(zip_path, "wb") as f:
        f.seek(total_bytes - 1)
        f.write(b"\x00")

    bar_lock = threading.Lock()
    errors = []

    def download_chunk(byte_start, byte_end):
        max_bytes = byte_end - byte_start + 1
        s = requests.Session()
        s.headers.update(auth_headers)
        r = s.get(url, headers={"Range": f"bytes={byte_start}-{byte_end}"},
                  stream=True, timeout=(60, 300))
        if r.status_code != 206:
            detail = (r.text or "")[:200].replace("\n", " ").strip()
            r.close()
            raise RuntimeError(
                f"Chunk {byte_start//1024//1024}–{byte_end//1024//1024} MB: "
                f"expected 206, got {r.status_code}. {detail}"
            )
        written = 0
        pos = byte_start
        with open(zip_path, "r+b") as f:
            f.seek(pos)
            for raw in r.iter_content(chunk_size=256 * 1024):
                if not raw:
                    continue
                remaining = max_bytes - written
                if remaining <= 0:
                    break
                data = raw[:remaining]
                f.write(data)
                written += len(data)
                with bar_lock:
                    bar.update(len(data))
        r.close()
        if written != max_bytes:
            raise RuntimeError(
                f"Chunk {byte_start//1024//1024}–{byte_end//1024//1024} MB: "
                f"wrote {written} bytes, expected {max_bytes}."
            )

    with tqdm(total=total_bytes, unit="B", unit_scale=True, unit_divisor=1024,
              desc=f"    {desc}", dynamic_ncols=True) as bar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_chunks) as pool:
            futs = {pool.submit(download_chunk, s, e): (s, e) for s, e in ranges}
            for fut in concurrent.futures.as_completed(futs):
                try:
                    fut.result()
                except Exception as exc:
                    errors.append(exc)
                    pool.shutdown(wait=False, cancel_futures=True)
                    break

    if errors:
        if os.path.exists(zip_path):
            os.remove(zip_path)
        raise RuntimeError(str(errors[0]))


def _single_stream(url, zip_path, total_bytes, resume_from, desc, auth_headers):
    """Single-connection download with resume via Range header."""
    extra_headers = {}
    if resume_from > 0:
        extra_headers["Range"] = f"bytes={resume_from}-"

    session = requests.Session()
    session.headers.update(auth_headers)
    r = session.get(url, headers=extra_headers, stream=True, timeout=(60, None))

    if r.status_code not in (200, 206):
        detail = (r.text or "")[:300].replace("\n", " ").strip()
        r.close()
        raise RuntimeError(f"HTTP {r.status_code}: {detail}")

    if r.status_code == 206:
        # Server confirmed resume
        file_mode, bar_initial = "ab", resume_from
        if resume_from > 0:
            print(f"    Server confirmed resume at {resume_from / (1024**2):.1f} MB.")
    else:
        # Server ignored Range — must overwrite from scratch
        file_mode, bar_initial = "wb", 0
        if resume_from > 0:
            print(f"    Server does not support resume (200 OK) — restarting from 0.")

    with tqdm(total=total_bytes, initial=bar_initial, unit="B", unit_scale=True,
              unit_divisor=1024, desc=f"    {desc}", dynamic_ncols=True) as bar:
        with open(zip_path, file_mode) as f:
            for chunk in r.iter_content(chunk_size=256 * 1024):
                if not chunk:
                    continue
                f.write(chunk)
                bar.update(len(chunk))
    r.close()


def _resolve_url(session, url):
    """Follow redirects (stream=True) and return the final 200 URL + total size."""
    max_redirects = 10
    hops = 0
    for _ in range(max_redirects):
        r = session.get(url, stream=True, allow_redirects=False, timeout=(60, None))
        if r.status_code in (301, 302, 303, 307, 308):
            next_url = r.headers.get("Location")
            r.close()
            if not next_url:
                raise RuntimeError("Redirect response missing Location header.")
            hops += 1
            print(f"    Redirect {hops} → {next_url[:80]}...")
            url = next_url
            continue
        if r.status_code != 200:
            detail = (r.text or "")[:300].replace("\n", " ").strip()
            r.close()
            raise RuntimeError(
                f"Download request failed ({r.status_code}) at {url}. Response: {detail}"
            )
        total = int(r.headers.get("Content-Length", 0)) or None
        r.close()
        return url, total
    raise RuntimeError("Too many redirects while resolving download URL.")


def _s3_download_direct(product_name, safe_dir, max_file_retries=10):
    """Download a Sentinel-2 product from CDSE S3 directly into a .SAFE folder.

    Downloads each file individually with per-file resume support:
    - Already-complete files (size matches S3) are skipped entirely.
    - Partial files resume from their current byte offset.
    - On connection drop, only the in-progress file retries — all others stay done.
    - No zip involved — files land directly in their final .SAFE structure.

    Returns safe_dir on success, None if credentials are missing.
    Raises RuntimeError on unrecoverable failure.
    """
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except ImportError:
        return None

    access_key = os.getenv("CDSE_S3_ACCESS_KEY")
    secret_key  = os.getenv("CDSE_S3_SECRET_KEY")
    if not access_key or not secret_key:
        return None

    date_str = product_name.split("_")[2][:8]
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    s3_prefix = f"Sentinel-2/MSI/L2A/{year}/{month}/{day}/{product_name}/"

    s3 = boto3.client(
        "s3",
        endpoint_url=CDSE_S3_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotoConfig(signature_version="s3v4"),
        region_name="default",
    )

    paginator = s3.get_paginator("list_objects_v2")
    objects   = []
    for page in paginator.paginate(Bucket=CDSE_S3_BUCKET, Prefix=s3_prefix):
        objects.extend(page.get("Contents", []))

    if not objects:
        print(f"    S3: no objects found at s3://{CDSE_S3_BUCKET}/{s3_prefix}")
        return None

    total_bytes = sum(o["Size"] for o in objects)
    size_str    = f"{total_bytes / (1024**2):.0f} MB"

    # Calculate already-downloaded bytes for resume progress bar
    already_bytes = 0
    already_files = 0
    for o in objects:
        local = os.path.join(safe_dir, o["Key"][len(s3_prefix):].replace("/", os.sep))
        if os.path.exists(local):
            sz = os.path.getsize(local)
            already_bytes += sz
            if sz == o["Size"]:
                already_files += 1

    if already_files:
        print(f"    S3: {len(objects)} files ({size_str}) — "
              f"{already_files} done, resuming remaining...")
    else:
        print(f"    S3: {len(objects)} files ({size_str}) — starting...")

    with tqdm(total=total_bytes, initial=already_bytes,
              unit="B", unit_scale=True, unit_divisor=1024,
              desc=f"    {product_name[:40]}", dynamic_ncols=True) as bar:

        for obj in objects:
            key           = obj["Key"]
            rel_path      = key[len(s3_prefix):].replace("/", os.sep)
            dest_path     = os.path.join(safe_dir, rel_path)
            expected_size = obj["Size"]

            # Skip fully downloaded files
            if os.path.exists(dest_path) and os.path.getsize(dest_path) == expected_size:
                continue

            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            resume_from = os.path.getsize(dest_path) if os.path.exists(dest_path) else 0

            for attempt in range(max_file_retries):
                try:
                    kwargs = {"Bucket": CDSE_S3_BUCKET, "Key": key}
                    if resume_from > 0:
                        kwargs["Range"] = f"bytes={resume_from}-"
                    resp = s3.get_object(**kwargs)
                    body = resp["Body"]
                    with open(dest_path, "ab" if resume_from > 0 else "wb") as f:
                        while True:
                            chunk = body.read(256 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                            resume_from += len(chunk)
                            bar.update(len(chunk))
                    break  # file complete

                except Exception as exc:
                    resume_from = os.path.getsize(dest_path) if os.path.exists(dest_path) else 0
                    if attempt < max_file_retries - 1:
                        bar.set_postfix_str(f"retry {attempt+1}: {rel_path[-25:]}")
                        continue
                    raise RuntimeError(
                        f"S3 file failed after {max_file_retries} retries: {rel_path}\n{exc}"
                    ) from exc

            actual = os.path.getsize(dest_path)
            if actual != expected_size:
                raise RuntimeError(
                    f"S3 size mismatch: {rel_path} (got {actual}, expected {expected_size})"
                )

    return safe_dir


def download_and_extract(product_id, product_name, auth_headers, out_dir,
                         max_retries=5, num_chunks=4):
    """Download a Sentinel-2 product ZIP and extract the .SAFE directory.

    Strategy:
    - Probes once whether the server supports Range requests.
    - If yes and num_chunks > 1: uses parallel chunk download (always fresh, no partial state).
    - If no: uses single-threaded streaming with resume-on-drop (Range 206 or restart on 200).
    - After every download attempt, verifies the ZIP with a full CRC check (testzip).
    - Deletes and retries if the ZIP fails verification.
    """
    os.makedirs(out_dir, exist_ok=True)
    zip_path = os.path.join(out_dir, f"{product_name}.zip")

    # Skip entirely if already extracted (via HTTP zip or previous S3 run)
    for p in glob.glob(os.path.join(out_dir, "*.SAFE")):
        if product_name in os.path.basename(p):
            return p
    # Also check direct S3 folder (no zip involved)
    s3_safe = os.path.join(out_dir, product_name)
    if os.path.isdir(s3_safe) and glob.glob(os.path.join(s3_safe, "GRANULE", "*")):
        return s3_safe

    # Skip download if a verified zip is already on disk
    if os.path.exists(zip_path):
        print(f"    Found existing zip — verifying...")
        if _verify_zip(zip_path):
            print(f"    Zip OK — skipping download.")
        else:
            print(f"    Existing zip is corrupt — deleting and re-downloading.")
            os.remove(zip_path)

    # ── Try S3 first (fastest — per-file resume, no zip) ─────────────────
    if os.getenv("CDSE_S3_ACCESS_KEY") and os.getenv("CDSE_S3_SECRET_KEY"):
        safe_dir = os.path.join(out_dir, product_name)
        print(f"    Trying S3 download (fast path, per-file resume)...")
        try:
            result = _s3_download_direct(product_name, safe_dir)
            if result:
                print(f"    S3 download complete.")
                return result
        except Exception as exc:
            print(f"    S3 error: {exc} — falling back to HTTP.")
            # Leave partial .SAFE dir intact — next run will resume from it

    # ── HTTP fallback ─────────────────────────────────────────────────────
    session = requests.Session()
    session.headers.update(auth_headers)
    base_url = DOWNLOAD_URL_TEMPLATE.format(product_id)

    print(f"    Resolving download URL...")
    final_url, total_bytes = _resolve_url(session, base_url)
    size_str = f"{total_bytes / (1024**2):.0f} MB" if total_bytes else "unknown size"

    # Decide once: parallel or single-threaded
    use_parallel = num_chunks > 1 and total_bytes is not None
    if use_parallel:
        print(f"    Probing Range support...", end=" ", flush=True)
        if _probe_range_support(final_url, get_auth_headers()):
            print(f"supported ✓  ({num_chunks} parallel connections, {size_str})")
        else:
            print(f"not supported — single connection ({size_str})")
            use_parallel = False
    else:
        print(f"    Ready ({size_str}) — single connection")

    desc = product_name[:40]

    for attempt in range(1, max_retries + 1):
        # Fresh token on every attempt (CDSE tokens expire in 10 min)
        fresh_headers = get_auth_headers()

        if use_parallel:
            # Parallel: always a clean slate — no partial state to reason about
            print(f"    Attempt {attempt}/{max_retries} — parallel download...")
            try:
                _parallel_download(final_url, zip_path, total_bytes, desc,
                                   fresh_headers, num_chunks)
            except Exception as exc:
                print(f"    Parallel download error: {exc}")
                if attempt < max_retries:
                    print(f"    Retrying...")
                    continue
                raise RuntimeError(f"Download failed after {max_retries} attempts.") from exc
        else:
            # Single-threaded with resume
            resume_from = os.path.getsize(zip_path) if os.path.exists(zip_path) else 0
            if total_bytes and resume_from >= total_bytes:
                print(f"    File fully on disk ({resume_from / (1024**2):.0f} MB) — verifying...")
            elif resume_from > 0:
                print(f"    Attempt {attempt}/{max_retries} — resuming from "
                      f"{resume_from / (1024**2):.1f} MB / {size_str}...")
            else:
                print(f"    Attempt {attempt}/{max_retries} — starting download ({size_str})...")
            try:
                _single_stream(final_url, zip_path, total_bytes, resume_from,
                               desc, fresh_headers)
            except Exception as exc:
                print(f"    Connection dropped: {exc}")
                if attempt < max_retries:
                    print(f"    Retrying...")
                    continue
                raise RuntimeError(f"Download failed after {max_retries} attempts.") from exc

        # ── Verify the ZIP with a full CRC check ────────────────────────
        print(f"    Verifying ZIP integrity...")
        if _verify_zip(zip_path):
            print(f"    ZIP OK ✓")
            break
        else:
            print(f"    ZIP is corrupt (CRC mismatch) — deleting and retrying.")
            os.remove(zip_path)
            if attempt >= max_retries:
                raise RuntimeError(
                    f"ZIP failed CRC verification after {max_retries} attempts. "
                    f"The server may be returning incomplete data."
                )

    print(f"    Extracting...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(out_dir)
    safe_dirs = glob.glob(os.path.join(out_dir, "*.SAFE"))
    if not safe_dirs:
        raise FileNotFoundError(f"No .SAFE directory found after extracting {zip_path}")
    return safe_dirs[0]


def find_band_files(safe_dir):
    """Return list of (file_path, band_name) for B01..B12, B8A."""
    patterns = [
        os.path.join(safe_dir, "GRANULE", "*", "IMG_DATA", "R10m", "*.jp2"),
        os.path.join(safe_dir, "GRANULE", "*", "IMG_DATA", "R20m", "*.jp2"),
        os.path.join(safe_dir, "GRANULE", "*", "IMG_DATA", "R60m", "*.jp2"),
    ]
    jp2s = []
    for p in patterns:
        jp2s.extend(glob.glob(p))
    band_pattern = re.compile(r"_(B(0[1-9]|1[0-2]|8A))_")
    out = []
    seen = set()
    for path in jp2s:
        name = os.path.basename(path)
        m = band_pattern.search(name)
        if not m:
            continue
        band = m.group(1)
        if band in seen:
            continue
        if band not in BAND_NAMES:
            continue
        seen.add(band)
        out.append((path, band))
    return sorted(out, key=lambda x: BAND_NAMES.index(x[1]))


def sample_bands_at_point(safe_dir, lon, lat, band_names=None):
    """Sample all 12 bands at (lon, lat) WGS84. Returns array of length 12 or None if outside extent."""
    band_names = band_names or BAND_NAMES
    band_files = find_band_files(safe_dir)
    if len(band_files) < 12:
        return None
    by_band = {b: path for path, b in band_files}
    values = np.full(12, np.nan, dtype=np.float32)
    for i, band in enumerate(band_names):
        path = by_band.get(band)
        if not path:
            continue
        try:
            with rasterio.open(path) as src:
                # Transform (lon, lat) WGS84 to raster CRS
                xs, ys = transform("EPSG:4326", src.crs, [lon], [lat])
                x, y = xs[0], ys[0]
                row, col = src.index(x, y)
                try:
                    w = Window(col - 1, row - 1, 3, 3)
                    patch = src.read(1, window=w)
                    values[i] = np.nanmean(patch)
                except (ValueError, rasterio.errors.WindowError):
                    values[i] = src.read(1, window=Window(col, row, 1, 1))[0, 0]
        except (ValueError, IndexError, rasterio.errors.WindowError):
            pass
        except Exception:
            pass
    if np.any(np.isnan(values)):
        return None
    return values


def sample_bands_all_points(safe_dir, lons, lats, band_names=None):
    """Batch-sample all 12 bands for a list of (lon, lat) points from one .SAFE dir.

    Opens each band file exactly once and samples all points in a single pass —
    much faster than calling sample_bands_9pixels per point when many points share
    the same tile.

    Returns (N, 9, 12) array where N = len(lons).
    Points that fall outside the raster extent are filled with NaN.
    """
    band_names = band_names or BAND_NAMES
    band_files = find_band_files(safe_dir)
    if len(band_files) < len(band_names):
        return None
    by_band = {b: path for path, b in band_files}
    n_pts = len(lons)
    result = np.full((n_pts, 9, len(band_names)), np.nan, dtype=np.float32)

    for bi, band in enumerate(band_names):
        path = by_band.get(band)
        if not path:
            continue
        try:
            with rasterio.open(path) as src:
                # Transform all points at once
                xs, ys = transform("EPSG:4326", src.crs, lons, lats)
                for pi, (x, y) in enumerate(zip(xs, ys)):
                    try:
                        row, col = src.index(x, y)
                        patch = src.read(1, window=Window(col - 1, row - 1, 3, 3))
                        if patch.size == 9:
                            result[pi, :, bi] = patch.flatten()
                        else:
                            result[pi, :, bi] = src.read(1, window=Window(col, row, 1, 1))[0, 0]
                    except Exception:
                        pass
        except Exception:
            pass
    return result


def sample_bands_9pixels(safe_dir, lon, lat, band_names=None):
    """Sample all 9 pixels in the 3×3 neighbourhood. Returns (9, 12) array or None.

    Row-major order: top-left → top-right → ... → bottom-right.
    Pixel 4 (index 4) is the centre pixel.
    """
    band_names = band_names or BAND_NAMES
    band_files = find_band_files(safe_dir)
    if len(band_files) < len(band_names):
        return None
    by_band = {b: path for path, b in band_files}
    pixels = np.full((9, len(band_names)), np.nan, dtype=np.float32)

    for i, band in enumerate(band_names):
        path = by_band.get(band)
        if not path:
            continue
        try:
            with rasterio.open(path) as src:
                xs, ys = transform("EPSG:4326", src.crs, [lon], [lat])
                row, col = src.index(xs[0], ys[0])
                try:
                    patch = src.read(1, window=Window(col - 1, row - 1, 3, 3))
                    if patch.size == 9:
                        pixels[:, i] = patch.flatten()
                    else:
                        # Near boundary — fill all 9 with centre value
                        pixels[:, i] = src.read(1, window=Window(col, row, 1, 1))[0, 0]
                except (ValueError, rasterio.errors.WindowError):
                    pixels[:, i] = src.read(1, window=Window(col, row, 1, 1))[0, 0]
        except Exception:
            pass

    return None if np.all(np.isnan(pixels)) else pixels


def _opposite_season_window(capture_date):
    """Return (start_str, end_str) centred on the opposite Philippine season.

    Wet season  Jun–Oct (habagat)  → pick Aug 1 of same year
    Dry season  Nov–May (amihan)   → pick Feb 1 of same year (or +1 yr if Nov/Dec)
    """
    month, year = capture_date.month, capture_date.year
    if month in (6, 7, 8, 9, 10):          # wet → target dry (Feb)
        centre = date(year, 2, 1)
    elif month in (11, 12):                 # early dry → target wet next Aug
        centre = date(year + 1, 8, 1)
    else:                                   # late dry (Jan–May) → target wet Aug
        centre = date(year, 8, 1)
    start = (centre - timedelta(days=DATE_TOLERANCE_DAYS)).strftime("%Y-%m-%dT00:00:00.000Z")
    end   = (centre + timedelta(days=DATE_TOLERANCE_DAYS)).strftime("%Y-%m-%dT23:59:59.000Z")
    return start, end


def bbox_wkt(min_lon, min_lat, max_lon, max_lat):
    return f"POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"


def _append_rows_safe(df_chunk, output_path, retries=6, delay=5):
    """Append df_chunk to output_path with retry logic.

    Handles PermissionError (file open in Excel / another process) by
    waiting and retrying up to `retries` times.  If still locked after all
    retries, the rows are saved to a numbered sidecar file so no data is lost.
    """
    for attempt in range(1, retries + 1):
        try:
            df_chunk.to_csv(output_path, mode="a", header=False, index=False, quoting=1)  # QUOTE_ALL
            # Count rows for progress display
            try:
                total = sum(1 for _ in open(output_path)) - 1
            except Exception:
                total = "?"
            print(f"  Saved {len(df_chunk)} rows to {output_path} "
                  f"(total so far: {total})")
            return
        except PermissionError:
            if attempt == 1:
                print(f"  WARNING: {output_path} is locked "
                      f"(open in Excel?). Retrying in {delay}s...")
            else:
                print(f"  Still locked — retry {attempt}/{retries} in {delay}s...")
            time.sleep(delay)

    # All retries exhausted — save to a sidecar so no data is lost
    base, ext = os.path.splitext(output_path)
    sidecar = f"{base}_overflow_{int(time.time())}{ext}"
    df_chunk.to_csv(sidecar, index=False, quoting=1)  # QUOTE_ALL
    print(f"  ERROR: Could not write to {output_path} after {retries} retries.")
    print(f"  Rows saved to sidecar file: {sidecar}")
    print(f"  Close the file in Excel, then manually merge the sidecar.")


def augment_field_data_copernicus(csv_path, output_path=None, max_products=None,
                                  num_chunks=4, all_pixels=False,
                                  growing_season_offset=0,
                                  date_range=None):
    """Load field CSV, download S2 products per (spatial cell, date), sample bands, save.

    Augmentation strategies applied:
      1. Multi-temporal compositing  — all tiles within ±DATE_TOLERANCE_DAYS are
         downloaded and averaged; per-band temporal std stored in *_std cols.
      2. Expanded date window        — DATE_TOLERANCE_DAYS = 45 (was 14).
      3. Multi-season augmentation   — also fetches tiles from the opposite Philippine
         season (wet↔dry) and adds them as extra training rows.
      4. Centre pixel only (default) — one row per GPS point using the centre pixel
         of the 3×3 patch (pixel index 4). Pass all_pixels=True / --all-pixels to
         emit all 9 neighbourhood pixels as separate rows (not recommended with few
         spatial groups — causes label redundancy across folds).

    Resume: tracks progress by _group_id so partial runs can be continued.

    Args:
        csv_path:    Path to input field CSV.
        output_path: Where to save the augmented CSV (written incrementally).
        max_products: Stop after this many spatial-cell groups (for quick tests).
        num_chunks:  Parallel connections per ZIP download.
        all_pixels:  Emit all 9 neighbourhood pixels as rows (default: False).
        date_range:  Optional (start_str, end_str) tuple in "YYYY-MM-DD" format.
                     When set, ALL GPS points are searched within this fixed window
                     regardless of their collection date or growing_season_offset.
                     Useful for targeting a specific peak-vegetation window, e.g.
                     ("2025-10-01", "2025-11-30") for Benguet highland vegetables.
    """
    df = pd.read_csv(csv_path)
    df["capture_datetime"] = pd.to_datetime(df["capture_datetime"], format="mixed", utc=True)
    df["_capture_date"] = df["capture_datetime"].dt.date

    # Drop rows with missing coordinates — they produce NaN WKT and 400 errors
    bad = df["latitude"].isna() | df["longitude"].isna()
    if bad.any():
        print(f"  WARNING: Dropping {bad.sum()} row(s) with missing lat/lon.")
        df = df[~bad].copy()

    df["_lat_cell"] = (df["latitude"] // SPATIAL_GRID_DEG) * SPATIAL_GRID_DEG
    df["_lon_cell"] = (df["longitude"] // SPATIAL_GRID_DEG) * SPATIAL_GRID_DEG
    df["_key"] = list(zip(df["_lat_cell"], df["_lon_cell"], df["_capture_date"]))

    keys = df["_key"].unique().tolist()
    if max_products is not None:
        keys = keys[:max_products]
        print(f"  (--max-products {max_products}: will process {len(keys)} of "
              f"{df['_key'].nunique()} total groups)")

    key_to_bbox = {}
    for (lc, lonc, d) in keys:
        margin = SPATIAL_GRID_DEG / 2
        key_to_bbox[(lc, lonc, d)] = (
            float(lonc - margin), float(lc - margin),
            float(lonc + margin), float(lc + margin),
        )

    print("Authenticating with Copernicus...")

    # ── Resume: track completed groups by _group_id ───────────────────────────
    already_done_groups = set()
    if output_path and os.path.exists(output_path):
        try:
            existing = pd.read_csv(output_path, usecols=["_group_id"], on_bad_lines="skip")
            already_done_groups = set(existing["_group_id"].dropna().tolist())
            print(f"  Resuming: {len(already_done_groups)} group(s) already done.")
        except Exception:
            already_done_groups = set()

    # Pre-init output CSV with all columns so training can start on partial data
    _meta_drop = ("_lat_cell", "_lon_cell", "_key", "_capture_date")
    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        if not os.path.exists(output_path):
            base_cols = [c for c in df.columns if c not in _meta_drop]
            all_cols  = base_cols + BAND_NAMES + BAND_STD_NAMES + ["_pixel_idx", "_aug_season", "_group_id"]
            pd.DataFrame(columns=all_cols).to_csv(output_path, index=False, quoting=1)

    # ── Helper: composite tiles → pixel rows ──────────────────────────────────
    def _emit_rows(group_rows, safe_dirs, group_id, aug_season):
        """Batch-sample pixels from composited tiles; return list of row dicts.

        Opens each band file once per tile for all points in group_rows —
        O(n_tiles × n_bands) file opens instead of O(n_points × n_bands).

        Default: centre pixel only (pixel index 4 of the 3×3 patch).
        With all_pixels=True: all 9 neighbourhood pixels as separate rows.
        """
        _CENTRE = 4
        lons = group_rows["longitude"].tolist()
        lats = group_rows["latitude"].tolist()

        # (n_tiles, n_pts, 9, 12)
        tile_arrays = []
        for sd in safe_dirs:
            arr = sample_bands_all_points(sd, lons, lats)
            if arr is not None:
                tile_arrays.append(arr)

        if not tile_arrays:
            return []

        stacked   = np.stack(tile_arrays)              # (n_tiles, n_pts, 9, 12)
        pixel_arr = np.mean(stacked, axis=0)            # (n_pts, 9, 12) temporal mean
        std_arr   = (np.std(stacked, axis=0)
                     if len(tile_arrays) > 1
                     else np.zeros_like(pixel_arr))

        rows_out = []
        pixel_indices = range(9) if all_pixels else [_CENTRE]
        for pi, (_, row) in enumerate(group_rows.iterrows()):
            if np.all(np.isnan(pixel_arr[pi])):
                continue
            base = row.drop(labels=list(_meta_drop), errors="ignore")
            for pix_idx in pixel_indices:
                rows_out.append({
                    **base.to_dict(),
                    **dict(zip(BAND_NAMES,     pixel_arr[pi, pix_idx].tolist())),
                    **dict(zip(BAND_STD_NAMES, std_arr[pi, pix_idx].tolist())),
                    "_pixel_idx":  pix_idx,
                    "_aug_season": aug_season,
                    "_group_id":   group_id,
                })
        return rows_out

    # ── Helper: download all products for a search window ─────────────────────
    def _download_tiles(products, auth_headers):
        safe_dirs = []
        for prod in products:
            try:
                sd = download_and_extract(prod["Id"], prod["Name"], auth_headers,
                                          FIELD_DOWNLOAD_DIR, num_chunks=num_chunks)
                safe_dirs.append(sd)
            except Exception as exc:
                print(f"    Download failed for {prod['Name']}: {exc}")
        return safe_dirs

    # ── Pre-scan: find all .SAFE dirs already on disk (avoid API calls) ─────────
    def _find_local_safe_dirs():
        """Return list of all .SAFE paths already extracted in FIELD_DOWNLOAD_DIR."""
        pattern = os.path.join(FIELD_DOWNLOAD_DIR, "*.SAFE")
        dirs = glob.glob(pattern)
        # Also check subdirs that were extracted without .SAFE suffix
        for entry in os.scandir(FIELD_DOWNLOAD_DIR) if os.path.isdir(FIELD_DOWNLOAD_DIR) else []:
            if entry.is_dir() and "MSIL2A" in entry.name and entry.path not in dirs:
                if glob.glob(os.path.join(entry.path, "GRANULE", "*")):
                    dirs.append(entry.path)
        return dirs

    def _safe_date(safe_path):
        """Extract sensing date (YYYYMMDD) from .SAFE product name."""
        name = os.path.basename(safe_path)
        m = re.search(r"_(\d{8})T\d{6}_", name)
        return m.group(1) if m else None

    local_safe_dirs = _find_local_safe_dirs()
    print(f"  Found {len(local_safe_dirs)} .SAFE product(s) already on disk.")

    def _match_local_tiles(target_date, tolerance_days):
        """Return local .SAFE dirs whose sensing date is within tolerance of target_date."""
        matched = []
        for sd in local_safe_dirs:
            ds = _safe_date(sd)
            if ds:
                tile_date = date(int(ds[:4]), int(ds[4:6]), int(ds[6:8]))
                if abs((tile_date - target_date).days) <= tolerance_days:
                    matched.append(sd)
        return matched[:MAX_TILES_PER_KEY]

    # ── Main loop ─────────────────────────────────────────────────────────────
    for i, (lc, lonc, d) in enumerate(keys):
        min_lon, min_lat, max_lon, max_lat = key_to_bbox[(lc, lonc, d)]
        wkt = bbox_wkt(min_lon, min_lat, max_lon, max_lat)

        # ── Primary season search window ──────────────────────────────────────
        # Three modes in priority order:
        #   1. --date-range START END  → fixed absolute window for all points
        #   2. --growing-season-offset N  → shift back N days from collection date
        #   3. default  → use collection date ± DATE_TOLERANCE_DAYS
        if date_range:
            start_str = f"{date_range[0]}T00:00:00.000Z"
            end_str   = f"{date_range[1]}T23:59:59.000Z"
            offset_tag = f"_dr{date_range[0]}_{date_range[1]}"
            # For local tile matching use midpoint of the range
            dr_start = datetime.strptime(date_range[0], "%Y-%m-%d").date()
            dr_end   = datetime.strptime(date_range[1], "%Y-%m-%d").date()
            target_d  = dr_start + (dr_end - dr_start) / 2
            tolerance = (dr_end - dr_start).days // 2
            print(f"  [{i+1}/{len(keys)}] Date-range window: {date_range[0]} -> {date_range[1]}")
        else:
            target_d   = d - timedelta(days=growing_season_offset)
            offset_tag = f"_gs{growing_season_offset}" if growing_season_offset else "_orig"
            start_str  = (target_d - timedelta(days=DATE_TOLERANCE_DAYS)).strftime("%Y-%m-%dT00:00:00.000Z")
            end_str    = (target_d + timedelta(days=DATE_TOLERANCE_DAYS)).strftime("%Y-%m-%dT23:59:59.000Z")
            tolerance  = DATE_TOLERANCE_DAYS
            if growing_season_offset:
                print(f"  [{i+1}/{len(keys)}] Growing-season target: {target_d} "
                      f"(offset -{growing_season_offset}d from {d})")

        orig_group_id = f"{lc:.4f}_{lonc:.4f}_{d}{offset_tag}"
        if orig_group_id in already_done_groups:
            print(f"  [{i+1}/{len(keys)}] Already done — skipping {d}{offset_tag}")
        else:
            # Check disk first — skip catalog API if tiles are already present
            safe_dirs = _match_local_tiles(target_d, tolerance)
            if safe_dirs:
                print(f"  [{i+1}/{len(keys)}] {len(safe_dirs)} local tile(s) for {target_d} — sampling...")

            group_rows = df[df["_key"] == (lc, lonc, d)]
            rows_out = _emit_rows(group_rows, safe_dirs, orig_group_id, aug_season=False) if safe_dirs else []

            # If local tiles yielded nothing (empty, wrong extent, missing bands),
            # fall through to download fresh tiles from the Copernicus catalog.
            if not rows_out:
                if safe_dirs:
                    print(f"  [{i+1}/{len(keys)}] Local tile(s) yielded 0 rows — "
                          f"fetching from Copernicus catalog...")
                auth_headers = get_auth_headers()
                products = search_products(wkt, start_str, end_str, auth_headers)
                if not products:
                    print(f"  [{i+1}/{len(keys)}] No products for cell ({lc:.4f},{lonc:.4f}) "
                          f"window {date_range[0] if date_range else target_d}")
                else:
                    cc_str = ", ".join(f"{p['_cc']:.0f}%" for p in products)
                    print(f"  [{i+1}/{len(keys)}] {len(products)} tile(s) [CC: {cc_str}] — downloading...")
                    safe_dirs = _download_tiles(products, auth_headers)
                    rows_out = _emit_rows(group_rows, safe_dirs, orig_group_id, aug_season=False)

            if rows_out and output_path:
                _append_rows_safe(pd.DataFrame(rows_out), output_path)
                already_done_groups.add(orig_group_id)

        # ── Opposite-season augmentation ──────────────────────────────────────
        aug_group_id = f"{lc:.4f}_{lonc:.4f}_{d}_aug"
        if aug_group_id in already_done_groups:
            print(f"  [{i+1}/{len(keys)}] Already done — skipping {d} aug")
        else:
            opp_start_dt, opp_end_dt = _opposite_season_window(d)
            opp_centre = date(
                int(opp_start_dt[:4]),
                int(opp_start_dt[5:7]),
                int(opp_start_dt[8:10]),
            ) + timedelta(days=DATE_TOLERANCE_DAYS)
            opp_safe_dirs = _match_local_tiles(opp_centre, DATE_TOLERANCE_DAYS)
            if opp_safe_dirs:
                print(f"    Multi-season: {len(opp_safe_dirs)} local tile(s) — sampling...")
            elif opp_centre > date.today():
                print(f"    Multi-season: target date {opp_centre} is in the future — skipping.")
                opp_safe_dirs = []
            else:
                auth_headers = get_auth_headers()
                opp_products = search_products(wkt, opp_start_dt, opp_end_dt, auth_headers)
                if not opp_products:
                    print(f"    Multi-season: no tiles found for opposite season")
                    opp_safe_dirs = []
                else:
                    print(f"    Multi-season: {len(opp_products)} tile(s) — downloading...")
                    opp_safe_dirs = _download_tiles(opp_products, auth_headers)

            if opp_safe_dirs:
                group_rows = df[df["_key"] == (lc, lonc, d)]
                opp_rows = _emit_rows(group_rows, opp_safe_dirs, aug_group_id, aug_season=True)
                if opp_rows and output_path:
                    _append_rows_safe(pd.DataFrame(opp_rows), output_path)
                already_done_groups.add(aug_group_id)

    if output_path and os.path.exists(output_path):
        final = pd.read_csv(output_path, on_bad_lines="skip")
        print(f"\nDone. {len(final)} rows saved to {output_path}")
        return final

    return None


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Augment field CSV with Sentinel-2 bands (Copernicus).")
    p.add_argument("input_csv", nargs="?", default="data/external/final_merged_data_cleaned.csv")
    p.add_argument("-o", "--output", default="data/processed/field_data_with_bands.csv")
    p.add_argument(
        "--max-products",
        type=int,
        default=None,
        metavar="N",
        help="Stop after downloading N products (e.g. 1 to test with a single tile).",
    )
    p.add_argument(
        "--chunks",
        type=int,
        default=4,
        metavar="N",
        help="Number of parallel connections per download (default: 4). "
             "Try 8 for faster downloads. Use 1 to disable parallel mode.",
    )
    p.add_argument(
        "--all-pixels",
        action="store_true",
        help="Emit all 9 neighbourhood pixels as separate rows instead of centre only. "
             "Not recommended with few spatial groups (causes label redundancy).",
    )
    p.add_argument(
        "--growing-season",
        action="store_true",
        help="Shift S2 search window back 75 days from capture date to target "
             "mid-growing-season imagery (Oct-Nov for Jan-Feb capture dates). "
             "Saves to field_data_with_bands_growing.csv by default.",
    )
    p.add_argument(
        "--growing-season-offset",
        type=int,
        default=None,
        metavar="DAYS",
        help="Custom day offset for growing-season shift (overrides --growing-season default of 75).",
    )
    p.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help=(
            "Fixed absolute date window for S2 search, e.g. --date-range 2025-10-01 2025-11-30. "
            "Overrides --growing-season-offset. All GPS points are searched within this window "
            "regardless of their collection date. Use to target a known peak-vegetation period."
        ),
    )
    args = p.parse_args()
    if not os.path.isfile(args.input_csv):
        print(f"Input not found: {args.input_csv}")
        exit(1)

    # Resolve growing-season offset
    gs_offset = 0
    if args.growing_season_offset is not None:
        gs_offset = args.growing_season_offset
    elif args.growing_season:
        gs_offset = 75

    # Date-range overrides growing-season offset
    date_range = tuple(args.date_range) if args.date_range else None
    if date_range:
        gs_offset = 0  # ignored when date_range is set

    # Auto-adjust output path for growing-season / date-range runs
    output = args.output
    if date_range and output == "data/processed/field_data_with_bands.csv":
        slug = f"{date_range[0]}_{date_range[1]}".replace("-", "")
        output = f"data/processed/field_data_with_bands_{slug}.csv"
        print(f"Date-range mode: output -> {output}")
    elif gs_offset and output == "data/processed/field_data_with_bands.csv":
        output = "data/processed/field_data_with_bands_growing.csv"
        print(f"Growing-season mode: output -> {output}")

    augment_field_data_copernicus(args.input_csv, output, args.max_products,
                                  num_chunks=args.chunks, all_pixels=args.all_pixels,
                                  growing_season_offset=gs_offset,
                                  date_range=date_range)

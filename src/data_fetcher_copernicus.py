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
import zipfile
from datetime import date, timedelta

import numpy as np
import pandas as pd
import rasterio
import requests
from dotenv import load_dotenv
from rasterio.warp import transform
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
BAND_NAMES = ["B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B09", "B11", "B12"]
# Days before/after capture date to search for imagery
DATE_TOLERANCE_DAYS = 14
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


def search_product(bbox_wkt, start_date, end_date, auth_headers, max_cloud=20):
    """Search for one S2 L2A product covering bbox and date range. Returns product dict or None."""
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
    r = requests.get(CATALOG_URL, headers=auth_headers, params=params)
    r.raise_for_status()
    products = r.json().get("value", [])
    if not products:
        return None
    # Prefer lower cloud cover
    best = None
    best_cc = float("inf")
    for p in products:
        pid = p["Id"]
        try:
            dr = requests.get(f"{CATALOG_URL}('{pid}')", headers=auth_headers)
            if dr.status_code != 200:
                continue
            for attr in dr.json().get("Attributes", []) or []:
                if attr.get("Name") == "cloudCover":
                    cc = float(attr.get("Value", 100))
                    if cc <= max_cloud and cc < best_cc:
                        best_cc = cc
                        best = p
                    break
            else:
                if best is None:
                    best = p
        except Exception:
            continue
    return best or products[0]


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
                from rasterio.windows import Window
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


def bbox_wkt(min_lon, min_lat, max_lon, max_lat):
    return f"POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"


def _append_rows_safe(df_chunk, output_path, retries=6, delay=5):
    """Append df_chunk to output_path with retry logic.

    Handles PermissionError (file open in Excel / another process) by
    waiting and retrying up to `retries` times.  If still locked after all
    retries, the rows are saved to a numbered sidecar file so no data is lost.
    """
    import time
    for attempt in range(1, retries + 1):
        try:
            df_chunk.to_csv(output_path, mode="a", header=False, index=False)
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
    sidecar = f"{base}_overflow_{int(__import__('time').time())}{ext}"
    df_chunk.to_csv(sidecar, index=False)
    print(f"  ERROR: Could not write to {output_path} after {retries} retries.")
    print(f"  Rows saved to sidecar file: {sidecar}")
    print(f"  Close the file in Excel, then manually merge the sidecar.")


def augment_field_data_copernicus(csv_path, output_path=None, max_products=None,
                                  num_chunks=4):
    """Load field CSV, download S2 products per (spatial cell, date), sample bands, save.

    Args:
        csv_path: Path to input field CSV.
        output_path: Path to save augmented CSV. Written incrementally after each product.
        max_products: If set, stop after processing this many products (useful for quick tests).
    """
    df = pd.read_csv(csv_path)
    df["capture_datetime"] = pd.to_datetime(df["capture_datetime"])
    df["_capture_date"] = df["capture_datetime"].dt.date

    # Unique (spatial cell, date) to minimize downloads
    df["_lat_cell"] = (df["latitude"] // SPATIAL_GRID_DEG) * SPATIAL_GRID_DEG
    df["_lon_cell"] = (df["longitude"] // SPATIAL_GRID_DEG) * SPATIAL_GRID_DEG
    df["_key"] = list(zip(df["_lat_cell"], df["_lon_cell"], df["_capture_date"]))

    keys = df["_key"].unique().tolist()
    if max_products is not None:
        keys = keys[:max_products]
        print(f"  (--max-products {max_products}: will process {len(keys)} of "
              f"{df['_key'].nunique()} total products)")

    # Build bbox per key (small margin around cell)
    key_to_bbox = {}
    for (lc, lonc, d) in keys:
        margin = SPATIAL_GRID_DEG / 2
        key_to_bbox[(lc, lonc, d)] = (
            float(lonc - margin), float(lc - margin),
            float(lonc + margin), float(lc + margin),
        )

    print("Authenticating with Copernicus...")

    # ── Detect already-processed keys from existing output CSV ───────────────
    already_saved_uuids = set()
    if output_path and os.path.exists(output_path):
        try:
            existing = pd.read_csv(output_path, usecols=["uuid"])
            already_saved_uuids = set(existing["uuid"].dropna().astype(str).tolist())
        except Exception:
            already_saved_uuids = set()

    def _key_already_done(key):
        """Return True if every uuid belonging to this key is already in the output."""
        group_uuids = set(df[df["_key"] == key]["uuid"].astype(str).tolist())
        return bool(group_uuids) and group_uuids.issubset(already_saved_uuids)

    # Pre-init output CSV with header so training can start on partial data
    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        if not os.path.exists(output_path):
            base_cols = [c for c in df.columns
                         if c not in ("_lat_cell", "_lon_cell", "_key", "_capture_date")]
            pd.DataFrame(columns=base_cols + BAND_NAMES).to_csv(output_path, index=False)

    # (lat_cell, lon_cell, date) -> safe_dir
    key_to_safe = {}

    for i, (lc, lonc, d) in enumerate(keys):
        # ── Skip if all rows for this key are already in the output CSV ──────
        if _key_already_done((lc, lonc, d)):
            print(f"  [{i+1}/{len(keys)}] Already processed — skipping "
                  f"cell ({lc:.4f},{lonc:.4f}) date {d}")
            continue

        # Re-authenticate before each group: CDSE tokens expire in ~600 s (10 min)
        auth_headers = get_auth_headers()

        min_lon, min_lat, max_lon, max_lat = key_to_bbox[(lc, lonc, d)]
        wkt = bbox_wkt(min_lon, min_lat, max_lon, max_lat)
        start = (d - timedelta(days=DATE_TOLERANCE_DAYS)).strftime("%Y-%m-%dT00:00:00.000Z")
        end = (d + timedelta(days=DATE_TOLERANCE_DAYS)).strftime("%Y-%m-%dT23:59:59.000Z")
        product = search_product(wkt, start, end, auth_headers)
        if not product:
            print(f"  No product for cell ({lc},{lonc}) date {d}")
            continue
        pid = product["Id"]
        name = product["Name"]
        print(f"  [{i+1}/{len(keys)}] Downloading {name} for {d}...")
        try:
            safe_path = download_and_extract(pid, name, auth_headers, FIELD_DOWNLOAD_DIR,
                                             num_chunks=num_chunks)
            key_to_safe[(lc, lonc, d)] = safe_path
        except Exception as e:
            print(f"  Download failed: {e}")
            continue

        # --- Incremental save: sample & append rows covered by this product ---
        if output_path:
            group_rows = df[df["_key"] == (lc, lonc, d)]
            rows_out = []
            for _, row in group_rows.iterrows():
                vals = sample_bands_at_point(safe_path, row["longitude"], row["latitude"])
                if vals is None:
                    continue
                base = row.drop(labels=["_lat_cell", "_lon_cell", "_key", "_capture_date"])
                rows_out.append({**base.to_dict(), **dict(zip(BAND_NAMES, vals.tolist()))})
            if rows_out:
                _append_rows_safe(pd.DataFrame(rows_out), output_path)

    # Final pass for any keys that were already extracted before this run
    # (i.e., .SAFE existed on disk — these were skipped in the loop above)
    if output_path:
        existing_output = pd.read_csv(output_path)
        already_done_keys = set()
        # Determine which keys we already saved incrementally
        for (lc, lonc, d), safe in key_to_safe.items():
            already_done_keys.add((lc, lonc, d))

        remaining = [k for k in df["_key"].unique() if k not in already_done_keys]
        if remaining:
            extra_rows = []
            for k in remaining:
                safe = key_to_safe.get(k)
                if safe is None:
                    continue
                for _, row in df[df["_key"] == k].iterrows():
                    vals = sample_bands_at_point(safe, row["longitude"], row["latitude"])
                    if vals is None:
                        continue
                    base = row.drop(labels=["_lat_cell", "_lon_cell", "_key", "_capture_date"])
                    extra_rows.append({**base.to_dict(), **dict(zip(BAND_NAMES, vals.tolist()))})
            if extra_rows:
                _append_rows_safe(pd.DataFrame(extra_rows), output_path)

        final = pd.read_csv(output_path)
        print(f"\nDone. {len(final)} rows with full band data saved to {output_path}")
        return final

    # Fallback: in-memory return (no output_path given)
    band_data = []
    for _, row in df.iterrows():
        k = row["_key"]
        safe = key_to_safe.get(k)
        if safe is None:
            band_data.append([np.nan] * 12)
            continue
        vals = sample_bands_at_point(safe, row["longitude"], row["latitude"])
        band_data.append(vals.tolist() if vals is not None else [np.nan] * 12)

    band_df = pd.DataFrame(band_data, columns=BAND_NAMES)
    out = pd.concat(
        [df.drop(columns=["_lat_cell", "_lon_cell", "_key", "_capture_date"]), band_df], axis=1
    )
    return out.dropna(subset=BAND_NAMES)


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
    args = p.parse_args()
    if not os.path.isfile(args.input_csv):
        print(f"Input not found: {args.input_csv}")
        exit(1)
    augment_field_data_copernicus(args.input_csv, args.output, args.max_products,
                                  num_chunks=args.chunks)

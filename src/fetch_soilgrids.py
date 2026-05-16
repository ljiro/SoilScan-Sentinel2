"""
fetch_soilgrids.py

Augment a field-data CSV with SoilGrids v2 predictions.

Access methods (tried in order):
  1. Local VRT  — VRT index files downloaded manually from files.isric.org,
                  plus per-property tiles downloaded on first use.
                  Use --local-data-dir to point at the VRT directory.
  2. REST API   — api.isric.org  (primary)
  3. REST API   — rest.soilgrids.org  (legacy fallback)
  4. COG stream — files.isric.org GeoTIFFs via GDAL /vsicurl/
                  (different domain, works when both REST endpoints are blocked)

For the local VRT mode, download these 12 files from:
  https://files.isric.org/soilgrids/latest/data/<PROP>/<PROP>_<DEPTH>_mean.vrt

  Properties: phh2o  soc  nitrogen  clay  sand  cec
  Depths:     0-5cm  5-15cm

  Save them to --local-data-dir (default: data/raw/soilgrids/) with this layout:
    data/raw/soilgrids/
      phh2o/phh2o_0-5cm_mean.vrt
      phh2o/phh2o_5-15cm_mean.vrt
      soc/soc_0-5cm_mean.vrt
      ...  (12 files total, ~3.5 MB each)

  On first run per property/depth, the script downloads only the single tile
  that covers your GPS points (~10-30 MB per tile) using requests (no GDAL DNS).

Output columns (prefix sg_):
  sg_phh2o_0-5cm,    sg_phh2o_5-15cm
  sg_soc_0-5cm,      sg_soc_5-15cm       (soil organic carbon, g/kg)
  sg_nitrogen_0-5cm, sg_nitrogen_5-15cm  (g/kg)
  sg_clay_0-5cm,     sg_clay_5-15cm      (g/kg)
  sg_sand_0-5cm,     sg_sand_5-15cm      (g/kg)
  sg_cec_0-5cm,      sg_cec_5-15cm       (mmol(c)/kg)

Usage:
    python src/fetch_soilgrids.py data/processed/field_data_with_clay.csv
    python src/fetch_soilgrids.py data/processed/field_data_with_clay.csv ^
        --local-data-dir data/raw/soilgrids ^
        --output data/processed/field_data_with_soilgrids.csv
"""

import argparse
import os
import socket
import subprocess
import time
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import requests

# REST endpoints tried in order
_REST_URLS = [
    "https://api.isric.org/soilgrids/v2.0/properties/query",
    "https://rest.soilgrids.org/soilgrids/v2.0/properties/query",
]

# SoilGrids COG base URL (files.isric.org — different domain from the REST API)
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

# CRS of SoilGrids COG files (Interrupted Goode Homolosine)
_SG_CRS = "ESRI:54052"

# Populated by _patch_dns_to_google; used by COG path to bypass GDAL DNS
_ISRIC_IP_CACHE: dict[str, str] = {}


# ── DNS patch ─────────────────────────────────────────────────────────────────

def _resolve_via_nslookup(hostname: str) -> str | None:
    """Use nslookup (Windows built-in) to resolve hostname via Google 8.8.8.8."""
    for qtype in ("-type=A", "-type=AAAA"):
        try:
            out = subprocess.run(
                ["nslookup", qtype, hostname, "8.8.8.8"],
                capture_output=True, text=True, timeout=10,
            ).stdout
            past_server = False
            for line in out.splitlines():
                if "8.8.8.8" in line:
                    past_server = True
                    continue
                if past_server and line.strip().lower().startswith("address:"):
                    ip = line.split(":", 1)[1].strip().split("#")[0].strip()
                    if ip:
                        return ip
        except Exception:
            pass
    return None


def _patch_dns_to_google():
    """Override socket.getaddrinfo for ISRIC hostnames to use Google DNS 8.8.8.8.

    Cloudflare 1.1.1.1 (Families) blocks ISRIC domains. This patches only
    those hostnames — the rest of the system DNS is unaffected.
    Uses nslookup (Windows built-in, no extra packages needed).
    """
    _isric_hosts = {"api.isric.org", "rest.soilgrids.org", "files.isric.org"}
    _ip_cache: dict[str, str] = {}

    try:
        import dns.resolver
        _res = dns.resolver.Resolver()
        _res.nameservers = ["8.8.8.8"]
        for host in _isric_hosts:
            for qtype in ("A", "AAAA"):
                try:
                    ip = str(_res.resolve(host, qtype)[0])
                    _ip_cache[host] = ip
                    break
                except Exception:
                    pass
    except ImportError:
        pass

    if not _ip_cache:
        for host in _isric_hosts:
            ip = _resolve_via_nslookup(host)
            if ip:
                _ip_cache[host] = ip

    if not _ip_cache:
        print("  DNS override: could not resolve ISRIC hosts via 8.8.8.8")
        return

    global _ISRIC_IP_CACHE
    _ISRIC_IP_CACHE = _ip_cache
    print(f"  DNS override active: {_ip_cache}")
    _orig = socket.getaddrinfo

    def _patched(host, port, family=0, type=0, proto=0, flags=0):
        if host in _ip_cache:
            return _orig(_ip_cache[host], port, family, type, proto, flags)
        return _orig(host, port, family, type, proto, flags)

    socket.getaddrinfo = _patched


# ── local VRT mode ────────────────────────────────────────────────────────────

def _vrt_path(local_dir: str, prop: str, depth: str) -> str:
    return os.path.join(local_dir, prop, f"{prop}_{depth}_mean.vrt")


def _vrt_tile_for_point(vrt_path: str, lon: float, lat: float) -> str | None:
    """Parse a SoilGrids VRT and return the SourceFilename entry covering (lon, lat).

    The VRT is in IGH projection (ESRI:54052). We transform the GPS point to IGH,
    compute its pixel coordinate within the VRT, then find the SimpleSource whose
    DstRect spans that pixel.
    """
    try:
        from pyproj import Transformer
    except ImportError:
        print("  ERROR: pyproj not installed. Run: pip install pyproj")
        return None

    try:
        tree = ET.parse(vrt_path)
    except ET.ParseError as exc:
        print(f"  ERROR: could not parse VRT {vrt_path}: {exc}")
        return None

    root = tree.getroot()

    # GeoTransform: x_min, pixel_w, 0, y_max, 0, pixel_h  (pixel_h is negative)
    gt_text = root.findtext("GeoTransform", "").strip()
    if not gt_text:
        print(f"  ERROR: no GeoTransform in {vrt_path}")
        return None
    gt = [float(x.strip()) for x in gt_text.split(",")]

    # VRT CRS (may be WKT or PROJ string in the SRS element)
    srs_elem = root.find("SRS") or root.find("SRSWKT")
    vrt_crs  = srs_elem.text.strip() if srs_elem is not None else _SG_CRS

    try:
        transformer = Transformer.from_crs("EPSG:4326", vrt_crs, always_xy=True)
        x_igh, y_igh = transformer.transform(lon, lat)
    except Exception as exc:
        print(f"  ERROR: CRS transform failed: {exc}")
        return None

    # Convert IGH coords → VRT pixel coordinates
    # gt[0] = x origin (left edge), gt[1] = pixel width
    # gt[3] = y origin (top edge),  gt[5] = pixel height (negative)
    if gt[1] == 0 or gt[5] == 0:
        print(f"  ERROR: degenerate GeoTransform in {vrt_path}")
        return None

    px_col = (x_igh - gt[0]) / gt[1]
    px_row = (y_igh - gt[3]) / gt[5]

    # Search all band SimpleSource / ComplexSource entries
    for band in root.findall("VRTRasterBand"):
        for tag in ("SimpleSource", "ComplexSource"):
            for src in band.findall(tag):
                fname = src.findtext("SourceFilename", "").strip()
                dst   = src.find("DstRect")
                if dst is None or not fname:
                    continue
                xoff  = float(dst.get("xOff",  0))
                yoff  = float(dst.get("yOff",  0))
                xsize = float(dst.get("xSize", 0))
                ysize = float(dst.get("ySize", 0))
                if (xoff <= px_col < xoff + xsize) and (yoff <= px_row < yoff + ysize):
                    return fname
    return None


def _download_tile(tile_rel: str, prop: str, local_dir: str) -> str | None:
    """Download a single SoilGrids tile via requests (DNS-patched) and cache it.

    tile_rel is a path like 'phh2o_0-5cm_mean/tileSG-003-022.tif'
    Returns the local file path, or None on failure.
    """
    local_path = os.path.join(local_dir, prop, tile_rel)
    if os.path.isfile(local_path) and os.path.getsize(local_path) > 0:
        return local_path

    url = f"{_COG_BASE}/{prop}/{tile_rel}"
    print(f"    Downloading tile: {tile_rel}")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
        size_mb = os.path.getsize(local_path) / 1e6
        print(f"    Downloaded: {local_path}  ({size_mb:.1f} MB)")
        return local_path
    except Exception as exc:
        print(f"    Tile download failed: {exc}")
        if os.path.isfile(local_path):
            os.remove(local_path)
        return None


def _sample_3x3_mean(ds, lon: float, lat: float) -> float:
    """Sample a 3×3 pixel neighbourhood from an open rasterio dataset. Returns nanmean."""
    from rasterio.warp import transform as rio_transform
    from rasterio.windows import Window
    xs, ys     = rio_transform("EPSG:4326", ds.crs, [lon], [lat])
    row, col_i = ds.index(xs[0], ys[0])
    win        = Window(max(0, col_i - 1), max(0, row - 1), 3, 3)
    patch      = ds.read(1, window=win).astype(float)
    nodata     = ds.nodata
    if nodata is not None:
        patch[patch == nodata] = np.nan
    return float(np.nanmean(patch))


def _sample_tif(tif_path: str, lon: float, lat: float, d_factor: float) -> float:
    """Sample a 3×3 pixel neighbourhood from a local GeoTIFF and apply d_factor."""
    import rasterio
    with rasterio.open(tif_path) as src:
        raw = _sample_3x3_mean(src, lon, lat)
    return (raw / d_factor) if np.isfinite(raw) else np.nan


# tile cache: (prop, depth) -> local tif path  (populated lazily)
_TILE_CACHE: dict[tuple[str, str], str | None] = {}


def _fetch_local(lat: float, lon: float, local_dir: str) -> dict:
    """Sample all 12 property/depth combinations from local VRT-indexed tiles."""
    out: dict[str, float] = {}
    for prop in PROPERTIES:
        for depth in DEPTHS:
            col      = f"sg_{prop}_{depth}"
            key      = (prop, depth)
            d_factor = _D_FACTOR.get(prop, 1)

            if key not in _TILE_CACHE:
                vrt = _vrt_path(local_dir, prop, depth)
                if not os.path.isfile(vrt):
                    _TILE_CACHE[key] = None
                else:
                    tile_rel = _vrt_tile_for_point(vrt, lon, lat)
                    if tile_rel is None:
                        print(f"    WARNING: no tile found for ({lat:.4f}, {lon:.4f}) in {vrt}")
                        _TILE_CACHE[key] = None
                    else:
                        _TILE_CACHE[key] = _download_tile(tile_rel, prop, local_dir)

            tif = _TILE_CACHE[key]
            if tif is None:
                out[col] = np.nan
            else:
                try:
                    out[col] = _sample_tif(tif, lon, lat, d_factor)
                except Exception as exc:
                    print(f"    WARNING: sampling failed for {col}: {exc}")
                    out[col] = np.nan
    return out


def _check_local_vrt(local_dir: str) -> bool:
    """Return True if at least one VRT file exists in local_dir."""
    for prop in PROPERTIES:
        for depth in DEPTHS:
            if os.path.isfile(_vrt_path(local_dir, prop, depth)):
                return True
    return False


def _print_vrt_download_instructions(local_dir: str):
    print("\nTo use local VRT mode, download these 12 files from ISRIC:")
    print(f"  Base URL: {_COG_BASE}/")
    print(f"  Save to:  {local_dir}/\n")
    for prop in PROPERTIES:
        for depth in DEPTHS:
            rel = f"{prop}/{prop}_{depth}_mean.vrt"
            print(f"  {_COG_BASE}/{rel}")
    print(f"\nExpected layout after download:")
    print(f"  {local_dir}/phh2o/phh2o_0-5cm_mean.vrt")
    print(f"  {local_dir}/phh2o/phh2o_5-15cm_mean.vrt")
    print(f"  {local_dir}/soc/soc_0-5cm_mean.vrt  ... (12 files total)")
    print(f"\nThen re-run:  python src/fetch_soilgrids.py <csv> --local-data-dir {local_dir}")


# ── connectivity check ────────────────────────────────────────────────────────

def _cog_url(prop: str, depth: str) -> str:
    ip   = _ISRIC_IP_CACHE.get("files.isric.org")
    host = ip if ip else "files.isric.org"
    return f"/vsicurl/https://{host}/soilgrids/latest/data/{prop}/{prop}_{depth}_mean.tif"


def _gdal_cog_env() -> dict:
    env = {
        "GDAL_DISABLE_READDIR_ON_OPEN":     "EMPTY_DIR",
        "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif,.tiff",
        "GDAL_HTTP_MAX_RETRY":              "2",
        "GDAL_HTTP_RETRY_DELAY":            "1",
    }
    if _ISRIC_IP_CACHE.get("files.isric.org"):
        env["GDAL_HTTP_UNSAFESSL"]      = "YES"
        env["GDAL_HTTP_SSL_VERIFYPEER"] = "NO"
        env["GDAL_HTTP_SSL_VERIFYHOST"] = "NO"
        env["GDAL_HTTP_HEADER_FILE"]    = _write_host_header("files.isric.org")
    return env


def _write_host_header(hostname: str) -> str:
    import tempfile
    path = os.path.join(tempfile.gettempdir(), "soilgrids_host_header.txt")
    with open(path, "w") as f:
        f.write(f"Host: {hostname}\r\n")
    return path


def _check_connectivity() -> str:
    for url in _REST_URLS:
        try:
            r = requests.get(url, params={"lon": 120.59, "lat": 16.45,
                                           "property": ["phh2o"], "depth": ["0-5cm"],
                                           "value": "mean"}, timeout=10)
            if r.status_code < 500:
                return "rest"
        except Exception:
            pass

    gdal_env = _gdal_cog_env()
    for k, v in gdal_env.items():
        os.environ[k] = v

    try:
        import rasterio
        test_url = _cog_url("phh2o", "0-5cm")
        print(f"  COG test URL : {test_url}")
        with rasterio.Env(**gdal_env):
            with rasterio.open(test_url) as src:
                print(f"  COG opened OK — CRS: {src.crs}, size: {src.width}×{src.height}")
                return "cog"
    except Exception as exc:
        print(f"  COG test failed: {exc}")

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
    import rasterio
    gdal_env = _gdal_cog_env()
    datasets = {}
    with rasterio.Env(**gdal_env):
        for prop in PROPERTIES:
            for depth in DEPTHS:
                col  = f"sg_{prop}_{depth}"
                path = _cog_url(prop, depth)
                try:
                    ds = rasterio.open(path)
                    datasets[col] = ds
                except Exception as exc:
                    print(f"    WARNING: could not open COG for {col}: {exc}")
    return datasets


def _fetch_cog(lat: float, lon: float, datasets: dict) -> dict:
    import rasterio
    gdal_env = _gdal_cog_env()
    out = {}
    with rasterio.Env(**gdal_env):
        for col, ds in datasets.items():
            try:
                raw      = _sample_3x3_mean(ds, lon, lat)
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


def fetch_all(df: pd.DataFrame, delay: float = 0.5,
              local_dir: str | None = None) -> pd.DataFrame:
    _patch_dns_to_google()

    # ── mode selection ────────────────────────────────────────────────────────
    if local_dir is not None:
        if _check_local_vrt(local_dir):
            mode = "local"
            print(f"Local VRT mode — reading from {local_dir}")
        else:
            print(f"ERROR: no VRT files found in {local_dir}")
            _print_vrt_download_instructions(local_dir)
            return df
    else:
        print("Checking ISRIC connectivity...")
        mode = _check_connectivity()
        if mode == "rest":
            print("  REST API reachable — using api.isric.org")
        elif mode == "cog":
            print("  REST API blocked — falling back to COG streaming via files.isric.org")
        else:
            print("  ERROR: all ISRIC endpoints unreachable.")
            print("  Possible fixes:")
            print("    1. Use --local-data-dir after downloading VRT files (run with --help for details)")
            print("    2. Check your network / DNS (try: ping api.isric.org)")
            print("    3. Use a VPN")
            return df

    # ── cluster GPS points to 250 m cells ────────────────────────────────────
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
            if mode == "local":
                result = _fetch_local(key[0], key[1], local_dir)
            elif mode == "rest":
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
        description="Add SoilGrids v2 predictions to a field-data CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Local VRT mode (recommended when REST API is blocked):
  Download 12 VRT index files (~3.5 MB each) from:
    https://files.isric.org/soilgrids/latest/data/<PROP>/<PROP>_<DEPTH>_mean.vrt
  Properties: phh2o soc nitrogen clay sand cec  |  Depths: 0-5cm 5-15cm
  Save to data/raw/soilgrids/<PROP>/<PROP>_<DEPTH>_mean.vrt
  Then run with:  --local-data-dir data/raw/soilgrids
  The script will auto-download only the tile covering your GPS points (~10-30 MB each).
""",
    )
    parser.add_argument("input_csv", nargs="?",
                        default="data/processed/field_data_with_clay.csv")
    parser.add_argument("--output", default=None)
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Seconds between REST API calls (default: 0.5)")
    parser.add_argument(
        "--local-data-dir", default=None, metavar="PATH",
        help=(
            "Directory containing manually downloaded VRT files. "
            "If VRT files exist, skips REST/COG and reads local tiles. "
            "Run without this flag to see download instructions."
        ),
    )
    args = parser.parse_args()

    if args.local_data_dir is None and not os.environ.get("SOILGRIDS_LOCAL_DIR"):
        # No local dir given — check if default location has VRTs
        default_local = "data/raw/soilgrids"
        if _check_local_vrt(default_local):
            args.local_data_dir = default_local
            print(f"Auto-detected local VRT files at {default_local}")

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

    df_out = fetch_all(df, delay=args.delay, local_dir=args.local_data_dir)

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

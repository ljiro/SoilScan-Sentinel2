"""Extract Clay foundation model embeddings for all GPS field points.

Clay v1.5 is a geospatial Vision Transformer (DOFA architecture) trained on
Sentinel-2, Sentinel-1, Landsat-8/9, and DEM imagery via masked autoencoding.
It generates a 768-dimensional embedding per image patch that encodes spatial
context, texture, and spectral patterns invisible to raw band values.

For pH prediction this is valuable because Clay embeddings capture:
  - Soil colour gradients (iron oxide, organic matter)
  - Textural patterns (bare vs vegetated, erosion, tillage)
  - Spatial neighbourhood context (hillslope position, drainage)
  — all at the 256×256 pixel (~2.5km²) patch level around each field point.

Data sources supported:
  1. Sentinel-2 L2A  — already downloaded in data/raw/field_products/*.SAFE
  2. Landsat 8/9 C2L2 — downloaded via NASA earthaccess (same credentials)

Output: data/processed/field_data_with_clay.csv
        (input CSV + 768 clay_* embedding columns)

Clay model setup (one-time):
    pip install torch torchvision einops timm huggingface_hub
    # Clay code is loaded directly from HuggingFace — no git clone needed.

Run:
    python src/extract_clay_embeddings.py
    python src/extract_clay_embeddings.py --source sentinel2     # S2 only
    python src/extract_clay_embeddings.py --source landsat       # Landsat only
    python src/extract_clay_embeddings.py --source both          # merge both (default)
"""

import argparse
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import rasterio
from rasterio.transform import rowcol

warnings.filterwarnings("ignore", category=rasterio.errors.NotGeoreferencedWarning)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INPUT_CSV   = "data/processed/field_data_with_terrain.csv"
FIELD_CSV   = "data/external/final_merged_data_cleaned.csv"
OUTPUT_CSV  = "data/processed/field_data_with_clay.csv"
SAFE_DIR    = "data/raw/field_products"
LANDSAT_DIR = "data/raw/landsat"

CLAY_REPO   = "made-with-clay/Clay"
CLAY_CKPT   = "v1.5/clay-v1.5.ckpt"
PATCH_SIZE  = 128   # pixels — 128×128 @ 10m = 1.28km patch around each point

# Sentinel-2 L2A band config for Clay
# Clay expects (band_data, wavelengths_nm, gsd_m)
S2_BANDS = {
    "B02": (490,  10),
    "B03": (560,  10),
    "B04": (665,  10),
    "B08": (842,  10),
    "B05": (705,  20),
    "B06": (740,  20),
    "B07": (783,  20),
    "B8A": (865,  20),
    "B11": (1610, 20),
    "B12": (2190, 20),
}

# Landsat 8/9 OLI band config
L8_BANDS = {
    "B2":  (482,  30),
    "B3":  (562,  30),
    "B4":  (655,  30),
    "B5":  (865,  30),
    "B6":  (1610, 30),
    "B7":  (2200, 30),
}

EMBED_DIM = 768


# ---------------------------------------------------------------------------
# Clay model loader
# ---------------------------------------------------------------------------

def _load_clay_model(device="cpu"):
    """Download and initialise Clay v1.5 encoder.

    Clay's architecture is not registered in HuggingFace transformers, so we
    load its code dynamically from the GitHub source tree bundled in the
    HuggingFace repo.  The checkpoint is ~1.1 GB and is cached after first
    download.
    """
    import torch
    from huggingface_hub import hf_hub_download, snapshot_download

    print("  Loading Clay v1.5 model...")

    # Download checkpoint
    ckpt_path = hf_hub_download(CLAY_REPO, CLAY_CKPT)
    print(f"  Checkpoint: {ckpt_path}")

    # Clay source is in the HF repo — snapshot gives us the full tree
    repo_dir = snapshot_download(CLAY_REPO)

    # Insert Clay src into path so we can import it
    clay_src = os.path.join(repo_dir, "src")
    if clay_src not in sys.path and os.path.isdir(clay_src):
        sys.path.insert(0, clay_src)

    try:
        from model import Clay  # Clay's own model.py
        ckpt  = torch.load(ckpt_path, map_location=device)
        state = ckpt.get("state_dict", ckpt)
        # Strip "model." prefix if present (Lightning convention)
        state = {k.replace("model.", "", 1): v for k, v in state.items()}
        model = Clay()
        model.load_state_dict(state, strict=False)
        model.eval().to(device)
        print(f"  Clay loaded OK on {device}")
        return model
    except ImportError:
        # Fallback: use Clay's transformers-compatible encoder if available
        try:
            from transformers import AutoModel
            model = AutoModel.from_pretrained(CLAY_REPO, trust_remote_code=True)
            model.eval().to(device)
            print(f"  Clay (AutoModel) loaded on {device}")
            return model
        except Exception as e:
            raise RuntimeError(
                f"Could not load Clay model: {e}\n"
                "Try: pip install torch torchvision einops timm"
            ) from e


# ---------------------------------------------------------------------------
# Patch extraction helpers
# ---------------------------------------------------------------------------

def _safe_cloud_cover(safe_path):
    """Return tile-level cloud cover % from MTD_MSIL2A.xml (0–100). Returns 100 on failure."""
    try:
        from xml.etree import ElementTree as ET
        mtd = os.path.join(safe_path, "MTD_MSIL2A.xml")
        if not os.path.exists(mtd):
            return 100.0
        tree = ET.parse(mtd)
        for elem in tree.getroot().iter():
            if "Cloud_Coverage_Assessment" in elem.tag:
                return float(elem.text)
    except Exception:
        pass
    return 100.0


def _find_safe_for_point(lat, lon, safe_root):
    """Return the lowest-cloud SAFE dir whose footprint covers (lat, lon)."""
    import glob
    from rasterio.crs import CRS
    from rasterio.warp import transform_bounds

    wgs84 = CRS.from_epsg(4326)
    candidates = []   # (cloud_pct, safe_path)
    for safe_path in glob.glob(os.path.join(safe_root, "**/*.SAFE"), recursive=True):
        # Quick check: try to open B04 and see if point is inside
        b04s = glob.glob(os.path.join(safe_path, "**/*B04_10m.jp2"), recursive=True)
        if not b04s:
            continue
        try:
            with rasterio.open(b04s[0]) as src:
                # Convert native (UTM) bounds → WGS84 lon/lat for point-in-tile check
                if src.crs and src.crs != wgs84:
                    left, bottom, right, top = transform_bounds(
                        src.crs, wgs84, *src.bounds
                    )
                else:
                    left, bottom, right, top = src.bounds
                if left <= lon <= right and bottom <= lat <= top:
                    cloud = _safe_cloud_cover(safe_path)
                    candidates.append((cloud, safe_path))
        except Exception:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _lonlat_to_rowcol(src, lon, lat):
    """Convert WGS84 (lon, lat) to (row, col) in a rasterio dataset."""
    from pyproj import Transformer
    from rasterio.crs import CRS

    wgs84 = CRS.from_epsg(4326)
    if src.crs and src.crs.to_epsg() != 4326:
        t = Transformer.from_crs(wgs84, src.crs, always_xy=True)
        x, y = t.transform(lon, lat)
    else:
        x, y = lon, lat
    return rowcol(src.transform, x, y)


def _extract_s2_patch(safe_path, lat, lon, patch_px=PATCH_SIZE):
    """Extract a (C, H, W) Sentinel-2 patch centred on (lat, lon).

    Returns (patch_np, wavelengths_nm, gsd_m) or None if point is outside tile.
    """
    import glob
    bands_out, waves, gsds = [], [], []

    for band_name, (wl, gsd) in S2_BANDS.items():
        # Find the band file at the right resolution
        res = f"{gsd}m"
        pattern = os.path.join(safe_path, f"**/*{band_name}_{res}.jp2")
        files = glob.glob(pattern, recursive=True)
        if not files:
            # Try without resolution suffix
            pattern = os.path.join(safe_path, f"**/*{band_name}.jp2")
            files = glob.glob(pattern, recursive=True)
        if not files:
            continue

        try:
            with rasterio.open(files[0]) as src:
                row, col = _lonlat_to_rowcol(src, lon, lat)
                half = patch_px // 2
                window = rasterio.windows.Window(
                    col - half, row - half, patch_px, patch_px
                )
                data = src.read(1, window=window).astype(np.float32)
                if data.shape != (patch_px, patch_px):
                    # Pad if near edge
                    pad_h = patch_px - data.shape[0]
                    pad_w = patch_px - data.shape[1]
                    data = np.pad(data, ((0, pad_h), (0, pad_w)), mode="reflect")
                # Normalise to [0, 1] using S2 L2A reflectance scale.
                # Processing baseline N0400+ uses offset -1000 DN (= -0.1 reflectance).
                data = np.clip(data / 10000.0 - 0.1, 0, 1)
                bands_out.append(data)
                waves.append(wl)
                gsds.append(gsd)
        except Exception:
            continue

    if not bands_out:
        return None
    return np.stack(bands_out), np.array(waves, dtype=np.float32), np.array(gsds, dtype=np.float32)


def _find_landsat_for_point(lat, lon, landsat_root):
    """Return the Landsat scene directory covering (lat, lon)."""
    from rasterio.crs import CRS
    from rasterio.warp import transform_bounds

    wgs84 = CRS.from_epsg(4326)
    for scene_dir in Path(landsat_root).glob("**"):
        b4s = list(scene_dir.glob("*_B4.TIF")) + list(scene_dir.glob("*_B4.tif"))
        if not b4s:
            continue
        try:
            with rasterio.open(b4s[0]) as src:
                if src.crs and src.crs != wgs84:
                    left, bottom, right, top = transform_bounds(
                        src.crs, wgs84, *src.bounds
                    )
                else:
                    left, bottom, right, top = src.bounds
                if left <= lon <= right and bottom <= lat <= top:
                    return str(scene_dir)
        except Exception:
            continue
    return None


def _extract_landsat_patch(scene_dir, lat, lon, patch_px=PATCH_SIZE):
    """Extract a Landsat 8/9 patch centred on (lat, lon)."""
    bands_out, waves, gsds = [], [], []

    for band_name, (wl, gsd) in L8_BANDS.items():
        files = (list(Path(scene_dir).glob(f"*_{band_name}.TIF")) +
                 list(Path(scene_dir).glob(f"*_{band_name}.tif")))
        if not files:
            continue
        try:
            with rasterio.open(files[0]) as src:
                row, col = _lonlat_to_rowcol(src, lon, lat)
                half = patch_px // 2
                window = rasterio.windows.Window(
                    col - half, row - half, patch_px, patch_px
                )
                data = src.read(1, window=window).astype(np.float32)
                if data.shape != (patch_px, patch_px):
                    pad_h = patch_px - data.shape[0]
                    pad_w = patch_px - data.shape[1]
                    data = np.pad(data, ((0, pad_h), (0, pad_w)), mode="reflect")
                # Landsat OLI SR scale factor
                data = np.clip(data * 2.75e-5 - 0.2, 0, 1)
                bands_out.append(data)
                waves.append(wl)
                gsds.append(gsd)
        except Exception:
            continue

    if not bands_out:
        return None
    return np.stack(bands_out), np.array(waves, dtype=np.float32), np.array(gsds, dtype=np.float32)


# ---------------------------------------------------------------------------
# Patch-statistics features (no model download required)
# ---------------------------------------------------------------------------

# Band names used for patch-stats output columns
S2_BAND_NAMES = list(S2_BANDS.keys())   # B02,B03,B04,B08,B05,B06,B07,B8A,B11,B12

def _patch_stats(patch: np.ndarray, band_names: list) -> np.ndarray:
    """Compute per-band and spatial statistics for a (C, H, W) patch.

    Returns a 1-D float32 feature vector with:
      - Per-band: mean, std, p25, p75, p95  → C×5 features
      - Spatial indices computed from specific bands (NDVI, NDWI, BSI, NDRE)
      - Per-band local variance (texture proxy): mean of 3×3 var within patch
    Total for S2 (10 bands): 10×5 + 4 + 10 = 64 features.
    """
    C, H, W = patch.shape
    feats = []

    # Per-band stats
    for c in range(C):
        band = patch[c].ravel()
        band = band[~np.isnan(band)]
        if len(band) == 0:
            feats.extend([np.nan] * 5)
            continue
        feats.append(float(np.mean(band)))
        feats.append(float(np.std(band)))
        feats.append(float(np.percentile(band, 25)))
        feats.append(float(np.percentile(band, 75)))
        feats.append(float(np.percentile(band, 95)))

    # Spectral indices (using mean over the patch)
    def _band_mean(name):
        if name in band_names:
            idx = band_names.index(name)
            return float(np.nanmean(patch[idx]))
        return np.nan

    b02 = _band_mean("B02")   # blue
    b03 = _band_mean("B03")   # green
    b04 = _band_mean("B04")   # red
    b08 = _band_mean("B08")   # NIR
    b05 = _band_mean("B05")   # red-edge
    b11 = _band_mean("B11")   # SWIR1

    with np.errstate(invalid="ignore", divide="ignore"):
        ndvi  = (b08 - b04) / (b08 + b04 + 1e-9)
        ndwi  = (b03 - b08) / (b03 + b08 + 1e-9)
        bsi   = ((b11 + b04) - (b08 + b02)) / ((b11 + b04) + (b08 + b02) + 1e-9)
        ndre  = (b08 - b05) / (b08 + b05 + 1e-9)
    feats.extend([ndvi, ndwi, bsi, ndre])

    # Per-band local variance (texture proxy) — use 5×5 sliding window variance
    from scipy.ndimage import uniform_filter
    for c in range(C):
        band = patch[c]
        mu   = uniform_filter(band, size=5)
        mu2  = uniform_filter(band**2, size=5)
        var  = mu2 - mu**2
        feats.append(float(np.nanmean(var)))

    return np.array(feats, dtype=np.float32)


def extract_patch_stats(df, source="sentinel2"):
    """Extract patch-level statistics for every row — no model, no large download.

    Returns a (N, n_features) float32 array.
    Rows without a matching scene are NaN.
    """
    from scipy.ndimage import uniform_filter  # ensure import at call time

    lats = df["latitude"].to_numpy(dtype=float)
    lons = df["longitude"].to_numpy(dtype=float)
    n    = len(df)

    # Determine feature count from a dummy patch
    dummy = np.zeros((len(S2_BAND_NAMES), PATCH_SIZE, PATCH_SIZE), dtype=np.float32)
    n_feats = len(_patch_stats(dummy, S2_BAND_NAMES))
    features = np.full((n, n_feats), np.nan, dtype=np.float32)

    # Map unique GPS points to scene files
    unique_pts = pd.DataFrame({"lat": lats, "lon": lons}).drop_duplicates()
    pt_to_s2   = {}
    pt_to_l8   = {}

    print(f"  Mapping {len(unique_pts)} unique GPS points to scene files...")
    for _, row in unique_pts.iterrows():
        key = (row["lat"], row["lon"])
        if source in ("sentinel2", "both"):
            pt_to_s2[key] = _find_safe_for_point(row["lat"], row["lon"], SAFE_DIR)
        if source in ("landsat", "both") and os.path.isdir(LANDSAT_DIR):
            pt_to_l8[key] = _find_landsat_for_point(row["lat"], row["lon"], LANDSAT_DIR)

    covered_s2 = sum(1 for v in pt_to_s2.values() if v)
    covered_l8 = sum(1 for v in pt_to_l8.values() if v)
    print(f"  S2 coverage: {covered_s2}/{len(unique_pts)} points")
    if source in ("landsat", "both"):
        print(f"  L8 coverage: {covered_l8}/{len(unique_pts)} points")

    for i in range(n):
        key = (lats[i], lons[i])
        patch_data = None
        band_names = S2_BAND_NAMES

        if source in ("sentinel2", "both"):
            safe = pt_to_s2.get(key)
            if safe:
                patch_data = _extract_s2_patch(safe, lats[i], lons[i])
                band_names = S2_BAND_NAMES

        if patch_data is None and source in ("landsat", "both"):
            l8 = pt_to_l8.get(key)
            if l8:
                patch_data = _extract_landsat_patch(l8, lats[i], lons[i])
                band_names = list(L8_BANDS.keys())

        if patch_data is None:
            continue

        try:
            patch, _, _ = patch_data
            stats = _patch_stats(patch, band_names)
            # Pad/trim to match expected feature count
            if len(stats) > n_feats:
                stats = stats[:n_feats]
            elif len(stats) < n_feats:
                stats = np.pad(stats, (0, n_feats - len(stats)), constant_values=np.nan)
            features[i] = stats
        except Exception as exc:
            print(f"  WARNING row {i}: stats failed — {exc}")

        if (i + 1) % 50 == 0:
            ok = int(np.sum(~np.isnan(features[:i+1, 0])))
            print(f"  Progress: {i+1}/{n}  processed={ok}", end="\r")

    print()
    return features, n_feats


# ---------------------------------------------------------------------------
# Embedding extraction (Clay model — requires ~5 GB disk + torch)
# ---------------------------------------------------------------------------

def _embed_patch(model, patch, wavelengths, gsds, lat, lon, device="cpu"):
    """Run a single patch through the Clay encoder, return (768,) embedding."""
    import torch

    # Clay DOFA expects:
    #   pixels     : (1, C, H, W) float32 tensor, normalised [0,1]
    #   wavelengths: (1, C) float32 tensor in nm
    #   gsd        : (1,) float32 — ground sample distance in metres
    #   latlon     : (1, 2) float32
    pixels = torch.from_numpy(patch[None]).float().to(device)        # (1,C,H,W)
    waves  = torch.from_numpy(wavelengths[None]).float().to(device)  # (1,C)
    gsd    = torch.tensor([[gsds.mean()]], dtype=torch.float32).to(device)
    ll     = torch.tensor([[lat, lon]], dtype=torch.float32).to(device)

    with torch.no_grad():
        try:
            # Clay's own API
            out = model.encode(pixels, waves, gsd, ll)
        except AttributeError:
            try:
                out = model(pixels, wavelengths=waves, gsd=gsd, latlon=ll)
            except Exception:
                # Last resort: raw forward pass
                out = model(pixels)

    # out may be (1, tokens, embed_dim) — take mean over tokens (CLS + patches)
    if isinstance(out, torch.Tensor):
        emb = out.squeeze(0)
        if emb.ndim == 2:
            emb = emb.mean(dim=0)
        return emb.cpu().numpy()
    # Some versions return a dict
    if hasattr(out, "last_hidden_state"):
        return out.last_hidden_state[0].mean(dim=0).cpu().numpy()
    raise ValueError(f"Unexpected Clay output type: {type(out)}")


def extract_embeddings(df, source="both", device="cpu"):
    """For every row in df, extract Clay embeddings from S2 and/or Landsat.

    Returns a (N, EMBED_DIM) array.  Rows with no matching scene are NaN.
    """
    model = _load_clay_model(device)

    lats = df["latitude"].to_numpy(dtype=float)
    lons = df["longitude"].to_numpy(dtype=float)
    n    = len(df)

    embeddings = np.full((n, EMBED_DIM), np.nan, dtype=np.float32)

    # Pre-map unique GPS points to SAFE/Landsat dirs (saves repeated glob)
    unique_pts = pd.DataFrame({"lat": lats, "lon": lons}).drop_duplicates()
    pt_to_s2   = {}
    pt_to_l8   = {}

    print(f"  Mapping {len(unique_pts)} unique GPS points to scene files...")
    for _, row in unique_pts.iterrows():
        key = (row["lat"], row["lon"])
        if source in ("sentinel2", "both"):
            pt_to_s2[key] = _find_safe_for_point(row["lat"], row["lon"], SAFE_DIR)
        if source in ("landsat", "both") and os.path.isdir(LANDSAT_DIR):
            pt_to_l8[key] = _find_landsat_for_point(row["lat"], row["lon"], LANDSAT_DIR)

    covered_s2 = sum(1 for v in pt_to_s2.values() if v)
    covered_l8 = sum(1 for v in pt_to_l8.values() if v)
    print(f"  S2 coverage: {covered_s2}/{len(unique_pts)} points")
    print(f"  L8 coverage: {covered_l8}/{len(unique_pts)} points")

    for i in range(n):
        key = (lats[i], lons[i])
        patch_data = None

        # Try Sentinel-2 first
        if source in ("sentinel2", "both"):
            safe = pt_to_s2.get(key)
            if safe:
                patch_data = _extract_s2_patch(safe, lats[i], lons[i])

        # Fall back to / also try Landsat
        if patch_data is None and source in ("landsat", "both"):
            l8 = pt_to_l8.get(key)
            if l8:
                patch_data = _extract_landsat_patch(l8, lats[i], lons[i])

        if patch_data is None:
            continue

        try:
            patch, wavelengths, gsds = patch_data
            embeddings[i] = _embed_patch(
                model, patch, wavelengths, gsds, lats[i], lons[i], device
            )
        except Exception as exc:
            print(f"  WARNING row {i}: embedding failed — {exc}")

        if (i + 1) % 50 == 0:
            ok = int(np.sum(~np.isnan(embeddings[:i+1, 0])))
            print(f"  Progress: {i+1}/{n}  embedded={ok}", end="\r")

    print()
    return embeddings


# ---------------------------------------------------------------------------
# Landsat download helper
# ---------------------------------------------------------------------------

def download_landsat(aoi, output_dir, temporal=("2022-01-01", "2026-01-01"),
                     max_scenes=3):
    """Download Landsat 8/9 C2L2 SR band files via Microsoft Planetary Computer.

    Planetary Computer hosts the full Landsat Collection 2 Level-2 archive,
    free to access with no account required — only a URL signing step.

    Downloads individual band GeoTIFFs (B2-B7) for the lowest-cloud scenes
    covering the AOI, sorted by cloud cover ascending.
    """
    try:
        import planetary_computer as pc
        from pystac_client import Client
    except ImportError:
        print("  pip install planetary-computer pystac-client")
        return

    PC_STAC = "https://planetarycomputer.microsoft.com/api/stac/v1"

    print(f"  Searching Planetary Computer Landsat C2L2 over {aoi}...")
    catalog = Client.open(PC_STAC, modifier=pc.sign_inplace)

    lon_min, lat_min, lon_max, lat_max = aoi
    results = catalog.search(
        collections=["landsat-c2-l2"],
        bbox=[lon_min, lat_min, lon_max, lat_max],
        datetime=f"{temporal[0]}/{temporal[1]}",
        query={"eo:cloud_cover": {"lt": 50}},
        max_items=max_scenes * 3,
    )
    items = list(results.items())

    if not items:
        print("  No Landsat scenes found on Planetary Computer for this AOI.")
        print("  Continuing with Sentinel-2 only.")
        return

    # Sort by cloud cover, keep best N
    items.sort(key=lambda i: i.properties.get("eo:cloud_cover", 100))
    items = items[:max_scenes]

    print(f"  Found {len(items)} scene(s):")
    for it in items:
        print(f"    {it.id}  cloud={it.properties.get('eo:cloud_cover','?'):.1f}%"
              f"  date={it.properties.get('datetime','?')[:10]}")

    # Band mapping: Planetary Computer asset key → our L8_BANDS dict key
    PC_BAND_MAP = {
        "blue":   "B2",
        "green":  "B3",
        "red":    "B4",
        "nir08":  "B5",
        "swir16": "B6",
        "swir22": "B7",
    }

    os.makedirs(output_dir, exist_ok=True)

    for item in items:
        scene_dir = os.path.join(output_dir, item.id)
        os.makedirs(scene_dir, exist_ok=True)

        for pc_key, our_key in PC_BAND_MAP.items():
            if pc_key not in item.assets:
                continue
            href   = item.assets[pc_key].href
            fname  = os.path.join(scene_dir, f"{item.id}_{our_key}.TIF")
            if os.path.exists(fname):
                print(f"    Already exists: {os.path.basename(fname)}")
                continue
            print(f"    Downloading {item.id} {pc_key} ({our_key})...", end=" ")
            try:
                resp = requests.get(href, stream=True, timeout=60)
                resp.raise_for_status()
                with open(fname, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1 << 20):
                        fh.write(chunk)
                size_mb = os.path.getsize(fname) / 1e6
                print(f"{size_mb:.1f} MB")
            except Exception as e:
                print(f"FAILED: {e}")

    print("  Landsat download complete.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract spatial features for field GPS points from satellite imagery."
    )
    parser.add_argument(
        "--source",
        choices=["sentinel2", "landsat", "both", "patch-stats"],
        default="patch-stats",
        help=(
            "Feature extraction mode:\n"
            "  patch-stats  — per-band statistics + indices from image patches (no model download, default)\n"
            "  sentinel2    — Clay embeddings from S2 (requires ~5 GB disk + torch)\n"
            "  landsat      — Clay embeddings from Landsat\n"
            "  both         — Clay embeddings, S2 primary + Landsat fallback"
        ),
    )
    parser.add_argument("--download-landsat", action="store_true",
                        help="Download Landsat 8/9 scenes via Planetary Computer before extracting.")
    parser.add_argument("--device", default="cpu",
                        help="Torch device for Clay model: 'cpu' or 'cuda' (default: cpu).")
    parser.add_argument("--input", default=None,
                        help="Input CSV (default: field_data_with_terrain.csv or field_data_with_bands.csv).")
    parser.add_argument("--output", default=None,
                        help="Output CSV path (default: data/processed/field_data_with_clay.csv).")
    args = parser.parse_args()

    out_csv = args.output or OUTPUT_CSV

    # 1. Load field data
    src_csv = args.input or (
        INPUT_CSV if os.path.exists(INPUT_CSV) else
        "data/processed/field_data_with_bands.csv" if os.path.exists("data/processed/field_data_with_bands.csv")
        else FIELD_CSV
    )
    print(f"1. Loading {src_csv}...")
    df = pd.read_csv(src_csv, on_bad_lines="skip")
    print(f"   {len(df)} rows")

    # 2. Optionally download Landsat
    if args.download_landsat:
        print("2. Downloading Landsat 8/9 scenes via Planetary Computer...")
        aoi = (
            df["longitude"].min() - 0.05,
            df["latitude"].min()  - 0.05,
            df["longitude"].max() + 0.05,
            df["latitude"].max()  + 0.05,
        )
        download_landsat(aoi, LANDSAT_DIR)
    else:
        print("2. Skipping Landsat download (use --download-landsat to fetch).")

    # 3. Extract features
    if args.source == "patch-stats":
        print("3. Extracting patch-level statistics from S2 imagery (no model required)...")
        features, n_feats = extract_patch_stats(df, source="sentinel2")

        n_ok = int(np.sum(~np.isnan(features[:, 0])))
        print(f"   Processed: {n_ok}/{len(df)} rows ({n_ok/len(df)*100:.1f}%)  features={n_feats}")

        if n_ok == 0:
            print("\n  ERROR: No patch statistics extracted.")
            print("  Check that Sentinel-2 .SAFE files exist in data/raw/field_products/")
            sys.exit(1)

        # Column names: <band>_mean/std/p25/p75/p95, then indices, then <band>_var
        stat_names = []
        for b in S2_BAND_NAMES:
            for s in ("mean", "std", "p25", "p75", "p95"):
                stat_names.append(f"patch_{b}_{s}")
        for idx in ("ndvi", "ndwi", "bsi", "ndre"):
            stat_names.append(f"patch_{idx}")
        for b in S2_BAND_NAMES:
            stat_names.append(f"patch_{b}_var")

        # Trim to actual feature count
        stat_names = stat_names[:n_feats]
        feat_cols  = stat_names[:n_feats]

        print("4. Merging patch statistics into dataset...")
        feat_df = pd.DataFrame(features[:, :n_feats], columns=feat_cols)
        df_out  = pd.concat([df.reset_index(drop=True), feat_df], axis=1)

        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        df_out.to_csv(out_csv, index=False, quoting=1)
        print(f"\n5. Saved: {out_csv}  shape={df_out.shape}")
        print(f"   Patch feature columns: {feat_cols[0]} … {feat_cols[-1]}")
        print(f"\nNext step:")
        print(f"  python src/train_ordinal.py {out_csv} --deduplicate --filter-barangay Paoay")

    else:
        # Clay embedding path (requires torch + ~5 GB disk)
        print(f"3. Extracting Clay embeddings (source={args.source}, device={args.device})...")
        embeddings = extract_embeddings(df, source=args.source, device=args.device)

        n_ok = int(np.sum(~np.isnan(embeddings[:, 0])))
        print(f"   Embedded: {n_ok}/{len(df)} rows ({n_ok/len(df)*100:.1f}%)")

        if n_ok == 0:
            print("\n  ERROR: No embeddings extracted.")
            print("  Check that Sentinel-2 .SAFE files exist in data/raw/field_products/")
            sys.exit(1)

        print("4. Merging embeddings into dataset...")
        embed_cols = [f"clay_{i:03d}" for i in range(EMBED_DIM)]
        embed_df   = pd.DataFrame(embeddings, columns=embed_cols)
        df_out     = pd.concat([df.reset_index(drop=True), embed_df], axis=1)

        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        df_out.to_csv(out_csv, index=False, quoting=1)
        print(f"\n5. Saved: {out_csv}  shape={df_out.shape}")
        print(f"   Embedding columns: clay_000 … clay_{EMBED_DIM-1:03d}")
        print(f"\nNext step:")
        print(f"  python src/train_ordinal.py {out_csv} --deduplicate --filter-barangay Paoay")


if __name__ == "__main__":
    main()

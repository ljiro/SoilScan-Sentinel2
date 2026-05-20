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
import datetime
import math
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

# Clay v1.5 (large) produces 1024-dim CLS-token embeddings
EMBED_DIM = 1024

# ResNet output dims: 512 for ResNet-18, 2048 for ResNet-50
RESNET_EMBED_DIM_18 = 512
RESNET_EMBED_DIM_50 = 2048

# Clay metadata for Sentinel-2 L2A — matches configs/metadata.yaml in the repo
# Band order must follow Clay's expected sequence for the sentinel-2-l2a platform
CLAY_S2_BANDS = {
    # band_file: (clay_band_name, wavelength_um, gsd_m, dn_mean, dn_std)
    "B02": ("blue",      0.493, 10, 1105.0, 1809.0),
    "B03": ("green",     0.560, 10, 1355.0, 1757.0),
    "B04": ("red",       0.665, 10, 1552.0, 1888.0),
    "B05": ("rededge1",  0.704, 20, 1887.0, 1870.0),
    "B06": ("rededge2",  0.740, 20, 2422.0, 1732.0),
    "B07": ("rededge3",  0.783, 20, 2630.0, 1697.0),
    "B08": ("nir",       0.842, 10, 2743.0, 1742.0),
    "B8A": ("nir08",     0.865, 20, 2785.0, 1648.0),
    "B11": ("swir16",    1.610, 20, 2388.0, 1470.0),
    "B12": ("swir22",    2.190, 20, 1835.0, 1379.0),
}


# ---------------------------------------------------------------------------
# Clay model loader
# ---------------------------------------------------------------------------

def _ensure_clay_source():
    """Download Clay model source + configs from GitHub to a local cache.

    The HuggingFace repo (made-with-clay/Clay) only contains the checkpoint.
    The model code lives in the GitHub repo (Clay-foundation/model) under
    the claymodel/ package directory.
    """
    cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".clay_src")
    claymodel_dir = os.path.join(cache_dir, "claymodel")
    configs_dir   = os.path.join(cache_dir, "configs")
    os.makedirs(claymodel_dir, exist_ok=True)
    os.makedirs(configs_dir,   exist_ok=True)

    # Already cached if model.py is present
    if os.path.exists(os.path.join(claymodel_dir, "model.py")):
        return cache_dir

    print("  Fetching Clay source code from GitHub...")
    gh_base = "https://api.github.com/repos/Clay-foundation/model/contents/"
    raw_base = "https://raw.githubusercontent.com/Clay-foundation/model/main/"

    # Download all .py files under claymodel/
    resp = requests.get(gh_base + "claymodel", timeout=30)
    resp.raise_for_status()
    for entry in resp.json():
        if entry.get("type") == "file" and entry["name"].endswith(".py"):
            dest = os.path.join(claymodel_dir, entry["name"])
            if not os.path.exists(dest):
                print(f"    -> claymodel/{entry['name']}")
                r = requests.get(entry["download_url"], timeout=30)
                r.raise_for_status()
                with open(dest, "w", encoding="utf-8") as f:
                    f.write(r.text)

    # Create __init__.py if missing so it's importable as a package
    init_py = os.path.join(claymodel_dir, "__init__.py")
    if not os.path.exists(init_py):
        open(init_py, "w").close()

    # Download configs/metadata.yaml (needed by ClayMAEModule)
    meta_dest = os.path.join(configs_dir, "metadata.yaml")
    if not os.path.exists(meta_dest):
        print("    -> configs/metadata.yaml")
        r = requests.get(raw_base + "configs/metadata.yaml", timeout=30)
        r.raise_for_status()
        with open(meta_dest, "w", encoding="utf-8") as f:
            f.write(r.text)

    print(f"  Clay source cached at: {cache_dir}")
    return cache_dir


def _normalize_timestamp(dt):
    """Return (week_norm, hour_norm) each as np.array([sin, cos]) for Clay datacube."""
    doy  = dt.timetuple().tm_yday
    week = doy / 7.0
    hour = dt.hour
    return (
        np.array([math.sin(2*math.pi*week/52), math.cos(2*math.pi*week/52)], dtype=np.float32),
        np.array([math.sin(2*math.pi*hour/24), math.cos(2*math.pi*hour/24)], dtype=np.float32),
    )


def _normalize_latlon(lat, lon):
    """Return (lat_norm, lon_norm) each as np.array([sin, cos]) for Clay datacube."""
    return (
        np.array([math.sin(lat*math.pi/180), math.cos(lat*math.pi/180)], dtype=np.float32),
        np.array([math.sin(lon*math.pi/180), math.cos(lon*math.pi/180)], dtype=np.float32),
    )


def _load_clay_model(device="cpu"):
    """Download and initialise the Clay v1.5 encoder (Encoder class only).

    Bypasses Lightning / ClayMAEModule entirely to avoid the Lightning CLI
    instantiation bug and the 1.1 GB teacher-model download.  We load only
    the encoder branch of the checkpoint — that is all we need for inference.

    Confirmed from checkpoint inspection:
      - model_size: large  (dim=1024, depth=24, heads=16)
      - encoder keys:  model.encoder.*
      - cls_token shape: [1, 1, 1024]
    """
    import torch
    from huggingface_hub import hf_hub_download

    print("  Loading Clay v1.5 encoder...")
    ckpt_path = hf_hub_download(CLAY_REPO, CLAY_CKPT)
    print(f"  Checkpoint: {ckpt_path}")

    clay_src = _ensure_clay_source()
    if clay_src not in sys.path:
        sys.path.insert(0, clay_src)

    try:
        from claymodel.model import Encoder

        # Instantiate the large-model encoder with mask_ratio=0 for inference
        # (all patches visible → CLS token encodes the full patch context)
        encoder = Encoder(
            mask_ratio=0.0,
            patch_size=8,
            shuffle=False,
            dim=1024,
            depth=24,
            heads=16,
            dim_head=64,
            mlp_ratio=4,
        )

        ckpt      = torch.load(ckpt_path, map_location=device, weights_only=False)
        state     = ckpt["state_dict"]
        enc_state = {
            k[len("model.encoder."):]: v
            for k, v in state.items()
            if k.startswith("model.encoder.")
        }
        missing, _ = encoder.load_state_dict(enc_state, strict=True)
        if missing:
            print(f"  WARNING: missing keys: {missing[:3]}")
        encoder.eval().to(device)
        print(f"  Clay encoder loaded OK on {device}  (dim=1024, depth=24)")
        return encoder

    except Exception as e:
        raise RuntimeError(
            f"Could not load Clay encoder: {e}\n"
            "Try: pip install torch torchvision einops timm"
        ) from e


# ---------------------------------------------------------------------------
# ResNet feature extractor (pretrained ImageNet, multi-spectral first conv)
# ---------------------------------------------------------------------------

def _load_resnet_model(n_channels: int = 10, model_size: str = "resnet50",
                       device: str = "cpu"):
    """Load pretrained ResNet as a multi-spectral feature extractor.

    The first conv layer is replaced to accept `n_channels` input bands.
    New channel weights are initialised by averaging the 3 pretrained RGB
    weights — standard practice for multi-spectral transfer learning.

    Returns (model, embed_dim).
    """
    try:
        import torch
        import torch.nn as nn
        import torchvision.models as tvm
    except ImportError:
        raise RuntimeError(
            "pip install torch torchvision  (required for --source resnet)"
        )

    if model_size == "resnet18":
        model     = tvm.resnet18(weights="DEFAULT")
        embed_dim = RESNET_EMBED_DIM_18
    else:
        model     = tvm.resnet50(weights="DEFAULT")
        embed_dim = RESNET_EMBED_DIM_50

    if n_channels != 3:
        old      = model.conv1
        new_conv = nn.Conv2d(
            n_channels, old.out_channels,
            kernel_size=old.kernel_size, stride=old.stride,
            padding=old.padding, bias=False,
        )
        with torch.no_grad():
            avg = old.weight.mean(dim=1, keepdim=True)            # (64, 1, 7, 7)
            new_conv.weight.copy_(avg.expand(-1, n_channels, -1, -1))
        model.conv1 = new_conv

    model.fc = nn.Identity()   # strip classifier, keep pool features
    model.eval().to(device)
    print(f"  ResNet-{model_size[-2:]} loaded: {n_channels}-ch input, {embed_dim}-dim output")
    return model, embed_dim


def _embed_patch_resnet(model, patch_dn: np.ndarray, device: str = "cpu") -> np.ndarray:
    """Run a (C, H, W) S2 DN patch through ResNet, return pooled feature vector.

    DN values are scaled to [0, 1] and resized to 224×224 for ResNet's
    expected input size.
    """
    import torch
    import torch.nn.functional as F

    patch_norm = np.clip(patch_dn.astype(np.float32) / 10000.0, 0.0, 1.0)
    tensor     = torch.from_numpy(patch_norm[None]).float().to(device)   # (1,C,H,W)
    tensor     = F.interpolate(tensor, size=(224, 224), mode="bilinear", align_corners=False)
    with torch.no_grad():
        feat = model(tensor)   # (1, embed_dim)
    return feat.squeeze(0).cpu().numpy()


def extract_resnet_embeddings(df: pd.DataFrame, model_size: str = "resnet50",
                               device: str = "cpu") -> tuple[np.ndarray, int]:
    """Extract pretrained ResNet features for every GPS point.

    Uses the same S2 SAFE patch extraction as the Clay path but feeds raw DN
    patches through a 10-channel ResNet instead of the Clay encoder.

    Returns (embeddings array of shape (N, embed_dim), embed_dim).
    """
    n_bands         = len(S2_BAND_NAMES)
    model, embed_dim = _load_resnet_model(n_bands, model_size, device)

    lats = df["latitude"].to_numpy(dtype=float)
    lons = df["longitude"].to_numpy(dtype=float)
    n    = len(df)
    embeddings = np.full((n, embed_dim), np.nan, dtype=np.float32)

    unique_pts = pd.DataFrame({"lat": lats, "lon": lons}).drop_duplicates()
    pt_to_s2   = {}
    for _, row in unique_pts.iterrows():
        key = (row["lat"], row["lon"])
        pt_to_s2[key] = _find_safe_for_point(row["lat"], row["lon"], SAFE_DIR)

    covered = sum(1 for v in pt_to_s2.values() if v)
    print(f"  S2 coverage: {covered}/{len(unique_pts)} points")

    for i in range(n):
        key  = (lats[i], lons[i])
        safe = pt_to_s2.get(key)
        if not safe:
            continue
        patch_data = _extract_s2_patch(safe, lats[i], lons[i], raw=True)
        if patch_data is None:
            continue
        try:
            patch, _, _ = patch_data
            embeddings[i] = _embed_patch_resnet(model, patch, device)
        except Exception as exc:
            print(f"  WARNING row {i}: ResNet embed failed — {exc}")

        if (i + 1) % 50 == 0:
            ok = int(np.sum(~np.isnan(embeddings[:i+1, 0])))
            print(f"  Progress: {i+1}/{n}  embedded={ok}", end="\r")

    print()
    return embeddings, embed_dim


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


def _find_all_safes_for_point(lat, lon, safe_root, max_cloud=80):
    """Return all SAFE dirs whose footprint covers (lat, lon), sorted by cloud cover asc."""
    import glob as _glob
    from rasterio.crs import CRS
    from rasterio.warp import transform_bounds

    wgs84 = CRS.from_epsg(4326)
    candidates = []
    for safe_path in _glob.glob(os.path.join(safe_root, "**/*.SAFE"), recursive=True):
        b04s = _glob.glob(os.path.join(safe_path, "**/*B04_10m.jp2"), recursive=True)
        if not b04s:
            continue
        try:
            with rasterio.open(b04s[0]) as src:
                if src.crs and src.crs != wgs84:
                    left, bottom, right, top = transform_bounds(src.crs, wgs84, *src.bounds)
                else:
                    left, bottom, right, top = src.bounds
                if left <= lon <= right and bottom <= lat <= top:
                    cloud = _safe_cloud_cover(safe_path)
                    if cloud <= max_cloud:
                        candidates.append((cloud, safe_path))
        except Exception:
            continue
    candidates.sort(key=lambda x: x[0])
    return [p for _, p in candidates]


def _composite_s2_patch(safe_paths, lat, lon, patch_px=PATCH_SIZE):
    """Extract patches from multiple SAFEs and return median-composited (C, H, W) reflectance.

    Compositing across tiles reduces atmospheric noise, cloud shadows, and
    sensor artefacts. Uses pixel-wise median, which suppresses outliers better
    than mean and avoids cloud contamination from individual tiles.
    """
    patches = []
    meta = None
    for safe in safe_paths:
        result = _extract_s2_patch(safe, lat, lon, patch_px=patch_px, raw=False)
        if result is not None:
            patch, wavelengths, gsds = result
            # Only stack patches with the expected number of bands
            if meta is None:
                meta = (wavelengths, gsds)
                patches.append(patch)
            elif patch.shape[0] == patches[0].shape[0]:
                patches.append(patch)
    if not patches:
        return None
    if len(patches) == 1:
        return patches[0], meta[0], meta[1]
    stacked = np.stack(patches, axis=0)   # (N, C, H, W)
    composited = np.nanmedian(stacked, axis=0).astype(np.float32)
    return composited, meta[0], meta[1]


def _lonlat_to_rowcol(src, lon, lat):
    """Convert WGS84 (lon, lat) to (row, col) in a rasterio dataset."""
    from pyproj import Transformer
    from rasterio.crs import CRS

    wgs84 = CRS.from_epsg(4326)
    if src.crs and src.crs.to_epsg() != 4326:
        transformer = Transformer.from_crs(wgs84, src.crs, always_xy=True)
        x, y = transformer.transform(lon, lat)
    else:
        x, y = lon, lat
    return rowcol(src.transform, x, y)


def _extract_s2_patch(safe_path, lat, lon, patch_px=PATCH_SIZE, raw=False):
    """Extract a (C, H, W) Sentinel-2 patch centred on (lat, lon).

    Args:
        raw: if True return raw DN values (0-20000); if False return reflectance
             (dn/10000 - 0.1, clipped to [0,1]).  Use raw=True for Clay embeddings
             since Clay normalises with its own per-band mean/std in DN units.

    Returns (patch_np, wavelengths_nm, gsd_m) or None if point is outside tile.
    """
    import glob
    bands_out, waves, gsds = [], [], []

    # Use Clay's expected band order when raw=True, otherwise legacy S2_BANDS order
    band_iter = (
        {b: (info[1]*1000, info[2]) for b, info in CLAY_S2_BANDS.items()}.items()
        if raw else S2_BANDS.items()
    )

    for band_name, (wl, gsd) in band_iter:
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
                # Normalise unless caller wants raw DN for Clay's own normalization.
                # N0400+ baseline uses -1000 DN offset (= -0.1 reflectance).
                if not raw:
                    data = np.clip(data / 10000.0 - 0.1, 0, 1)
                bands_out.append(data)
                waves.append(wl)
                gsds.append(gsd)
        except Exception:
            continue

    if not bands_out:
        return None
    return np.stack(bands_out), np.array(waves, dtype=np.float32), np.array(gsds, dtype=np.float32)


# SCL class values (S2 L2A Scene Classification Layer)
# https://sentinels.copernicus.eu/web/sentinel/technical-guides/sentinel-2-msi/level-2a/algorithm
_SCL_VEGETATION    = 4
_SCL_BARE_SOIL     = 5
_SCL_WATER         = 6
_SCL_CLOUD_SHADOW  = 3
_SCL_CLOUD_MED     = 8
_SCL_CLOUD_HIGH    = 9
_SCL_CIRRUS        = 10
_SCL_SNOW          = 11
_SCL_NO_DATA       = 0


def _extract_scl_patch(safe_path, lat, lon, patch_px=PATCH_SIZE):
    """Extract the SCL (Scene Classification Layer) patch centred on (lat, lon).

    SCL is available at 20 m in S2 L2A products.  Returns a (H, W) uint8 array
    or None if the band is not found.
    """
    import glob as _glob
    patterns = [
        os.path.join(safe_path, "**/*SCL_20m.jp2"),
        os.path.join(safe_path, "**/*SCL.jp2"),
        os.path.join(safe_path, "**/*SCL*.jp2"),
    ]
    scl_files = []
    for pat in patterns:
        scl_files = _glob.glob(pat, recursive=True)
        if scl_files:
            break
    if not scl_files:
        return None
    try:
        with rasterio.open(scl_files[0]) as src:
            row, col = _lonlat_to_rowcol(src, lon, lat)
            half = patch_px // 2
            window = rasterio.windows.Window(col - half, row - half, patch_px, patch_px)
            data = src.read(1, window=window)
            if data.shape != (patch_px, patch_px):
                pad_h = patch_px - data.shape[0]
                pad_w = patch_px - data.shape[1]
                data = np.pad(data, ((0, pad_h), (0, pad_w)), mode="constant", constant_values=0)
        return data.astype(np.uint8)
    except Exception:
        return None


def _patch_quality(patch_reflectance, scl=None, band_names=None):
    """Compute vegetation quality metrics for a (C, H, W) reflectance patch.

    Returns a dict with:
        ndvi_mean     — mean NDVI over the patch (-1 to 1)
        ndvi_p75      — 75th-percentile NDVI (better indicator than mean)
        veg_fraction  — fraction of pixels with NDVI > 0.2
        cloud_frac    — fraction of pixels classified as cloud/shadow by SCL
        valid_frac    — fraction of pixels classified as usable (veg/soil/water)
        scl_available — whether SCL data was provided
    """
    if band_names is None:
        band_names = S2_BAND_NAMES
    try:
        idx_nir = band_names.index("B08")
        idx_red = band_names.index("B04")
        nir = patch_reflectance[idx_nir].astype(np.float32)
        red = patch_reflectance[idx_red].astype(np.float32)
        with np.errstate(invalid="ignore", divide="ignore"):
            ndvi = (nir - red) / (nir + red + 1e-9)
        ndvi_mean = float(np.nanmean(ndvi))
        ndvi_p75  = float(np.nanpercentile(ndvi, 75))
        veg_frac  = float(np.nanmean(ndvi > 0.2))
    except (ValueError, IndexError):
        ndvi_mean = float("nan")
        ndvi_p75  = float("nan")
        veg_frac  = float("nan")

    if scl is not None:
        cloud_mask = np.isin(scl, [_SCL_CLOUD_SHADOW, _SCL_CLOUD_MED,
                                    _SCL_CLOUD_HIGH,   _SCL_CIRRUS])
        valid_mask = np.isin(scl, [_SCL_VEGETATION, _SCL_BARE_SOIL, _SCL_WATER])
        cloud_frac = float(np.mean(cloud_mask))
        valid_frac = float(np.mean(valid_mask))
        scl_ok     = True
    else:
        cloud_frac = float("nan")
        valid_frac = float("nan")
        scl_ok     = False

    return {
        "quality_ndvi_mean":  ndvi_mean,
        "quality_ndvi_p75":   ndvi_p75,
        "quality_veg_frac":   veg_frac,
        "quality_cloud_frac": cloud_frac,
        "quality_valid_frac": valid_frac,
        "quality_scl_ok":     scl_ok,
    }


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
        band_mean    = uniform_filter(band, size=5)
        band_mean_sq = uniform_filter(band**2, size=5)
        band_var     = band_mean_sq - band_mean**2
        feats.append(float(np.nanmean(band_var)))

    return np.array(feats, dtype=np.float32)


def extract_patch_stats(df, source="sentinel2", composite=False):
    """Extract patch-level statistics for every row — no model, no large download.

    Args:
        composite: if True, median-composite patches across all available tiles
                   covering each GPS point before computing statistics. Reduces
                   atmospheric noise and cloud shadow artefacts.

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

    # Quality metric accumulators (filled alongside patch stats)
    quality_cols: list = []
    quality_data: dict = {}

    # Map unique GPS points to scene files
    unique_pts = pd.DataFrame({"lat": lats, "lon": lons}).drop_duplicates()
    pt_to_s2   = {}   # key → single best SAFE (non-composite mode)
    pt_to_s2s  = {}   # key → list of all SAFEs (composite mode)
    pt_to_l8   = {}

    print(f"  Mapping {len(unique_pts)} unique GPS points to scene files...")
    if composite:
        print("  Composite mode: median across all available tiles per point")
    for _, row in unique_pts.iterrows():
        key = (row["lat"], row["lon"])
        if source in ("sentinel2", "both"):
            if composite:
                pt_to_s2s[key] = _find_all_safes_for_point(row["lat"], row["lon"], SAFE_DIR)
            else:
                pt_to_s2[key] = _find_safe_for_point(row["lat"], row["lon"], SAFE_DIR)
        if source in ("landsat", "both") and os.path.isdir(LANDSAT_DIR):
            pt_to_l8[key] = _find_landsat_for_point(row["lat"], row["lon"], LANDSAT_DIR)

    if composite:
        covered_s2 = sum(1 for v in pt_to_s2s.values() if v)
        tile_counts = [len(v) for v in pt_to_s2s.values() if v]
        avg_tiles = sum(tile_counts) / len(tile_counts) if tile_counts else 0
        print(f"  S2 coverage: {covered_s2}/{len(unique_pts)} points  (avg {avg_tiles:.1f} tiles/point)")
    else:
        covered_s2 = sum(1 for v in pt_to_s2.values() if v)
        print(f"  S2 coverage: {covered_s2}/{len(unique_pts)} points")
    if source in ("landsat", "both"):
        covered_l8 = sum(1 for v in pt_to_l8.values() if v)
        print(f"  L8 coverage: {covered_l8}/{len(unique_pts)} points")

    for i in range(n):
        key = (lats[i], lons[i])
        patch_data = None
        band_names = S2_BAND_NAMES

        if source in ("sentinel2", "both"):
            if composite:
                safes = pt_to_s2s.get(key) or []
                if safes:
                    patch_data = _composite_s2_patch(safes, lats[i], lons[i])
                    band_names = S2_BAND_NAMES
            else:
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

            # Vegetation / cloud quality check using SCL band
            safe = pt_to_s2.get(key) if source in ("sentinel2", "both") else None
            scl  = _extract_scl_patch(safe, lats[i], lons[i]) if safe else None
            quality = _patch_quality(patch, scl, band_names=band_names)
            for qk, qv in quality.items():
                if qk not in quality_cols:
                    quality_cols.append(qk)
                    quality_data[qk] = np.full(n, np.nan, dtype=np.float32)
                quality_data[qk][i] = float(qv)

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

    # Print quality summary
    if "quality_ndvi_mean" in quality_data:
        ndvi_vals = quality_data["quality_ndvi_mean"]
        valid = ndvi_vals[~np.isnan(ndvi_vals)]
        if len(valid):
            print(f"  Quality summary ({len(valid)} patches with SCL/NDVI data):")
            print(f"    NDVI mean  : {valid.mean():.3f}  (min {valid.min():.3f}, max {valid.max():.3f})")
        if "quality_veg_frac" in quality_data:
            veg_frac = quality_data["quality_veg_frac"]
            vf_valid = veg_frac[~np.isnan(veg_frac)]
            print(f"    Veg frac   : {vf_valid.mean():.2%} of pixels have NDVI > 0.2")
        if "quality_cloud_frac" in quality_data:
            cloud_frac = quality_data["quality_cloud_frac"]
            cf_valid = cloud_frac[~np.isnan(cloud_frac)]
            if len(cf_valid):
                print(f"    Cloud frac : {cf_valid.mean():.2%} mean (SCL-based)")

    return features, n_feats, quality_cols, quality_data


# ---------------------------------------------------------------------------
# Embedding extraction (Clay model — requires ~5 GB disk + torch)
# ---------------------------------------------------------------------------

def _embed_patch(model, patch_dn, wavelengths_nm, lat, lon, dt, device="cpu"):
    """Run a patch through the Clay v1.5 encoder, return (1024,) CLS embedding.

    Args:
        patch_dn:       (C, H, W) float32 — raw S2 DN values (pre-normalisation)
        wavelengths_nm: (C,) float32 — wavelengths in nanometres
        dt:             datetime of the tile acquisition
    """
    import torch

    C = patch_dn.shape[0]
    n_meta = len(CLAY_S2_BANDS)
    if C > n_meta:
        patch_dn = patch_dn[:n_meta]
        wavelengths_nm = wavelengths_nm[:n_meta]
        C = n_meta

    # Clay normalisation: (dn - band_mean) / band_std  (per-band, in DN units)
    means = np.array([info[3] for info in list(CLAY_S2_BANDS.values())[:C]], dtype=np.float32)
    stds  = np.array([info[4] for info in list(CLAY_S2_BANDS.values())[:C]], dtype=np.float32)
    patch_norm = (patch_dn - means[:, None, None]) / stds[:, None, None]

    # Wavelengths in micrometers (Clay expects μm, not nm)
    waves_um = wavelengths_nm / 1000.0

    # Time and location encodings
    week_norm, hour_norm = _normalize_timestamp(dt)
    lat_norm,  lon_norm  = _normalize_latlon(lat, lon)

    pixels  = torch.from_numpy(patch_norm[None]).float().to(device)           # (1,C,H,W)
    time_t  = torch.tensor(np.hstack((week_norm, hour_norm)),
                            dtype=torch.float32, device=device).unsqueeze(0)  # (1,4)
    latlon_t = torch.tensor(np.hstack((lat_norm, lon_norm)),
                             dtype=torch.float32, device=device).unsqueeze(0) # (1,4)
    gsd_t   = torch.tensor(10.0, device=device)                               # S2 primary GSD
    waves_t = torch.tensor(waves_um, dtype=torch.float32, device=device)      # (C,)

    datacube = {
        "pixels": pixels,
        "time":   time_t,
        "latlon": latlon_t,
        "gsd":    gsd_t,
        "waves":  waves_t,
    }

    with torch.no_grad():
        # model IS the Encoder — call it directly
        unmsk_patch, _, _, _ = model(datacube)

    # CLS token is at index 0 → (1024,) embedding
    return unmsk_patch[:, 0, :].squeeze(0).cpu().numpy()


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
    if pt_to_s2:
        print(f"  S2 coverage: {covered_s2}/{len(unique_pts)} points")
    if pt_to_l8:
        print(f"  L8 coverage: {covered_l8}/{len(unique_pts)} points")

    for i in range(n):
        key = (lats[i], lons[i])
        patch_data = None

        # Try Sentinel-2 first — raw=True returns DN values for Clay's normalisation
        if source in ("sentinel2", "both"):
            safe = pt_to_s2.get(key)
            if safe:
                patch_data = _extract_s2_patch(safe, lats[i], lons[i], raw=True)

        # Fall back to / also try Landsat
        if patch_data is None and source in ("landsat", "both"):
            l8 = pt_to_l8.get(key)
            if l8:
                patch_data = _extract_landsat_patch(l8, lats[i], lons[i])

        if patch_data is None:
            continue

        try:
            patch, wavelengths, gsds = patch_data
            # Resolve acquisition datetime for time encoding
            dt_col = None
            for col in ("capture_datetime", "date", "datetime"):
                if col in df.columns:
                    dt_col = col
                    break
            if dt_col:
                raw_dt = df.iloc[i][dt_col]
                try:
                    tile_dt = pd.to_datetime(raw_dt).to_pydatetime()
                except Exception:
                    tile_dt = datetime.datetime(2025, 10, 15)
            else:
                tile_dt = datetime.datetime(2025, 10, 15)

            embeddings[i] = _embed_patch(
                model, patch, wavelengths, lats[i], lons[i], tile_dt, device
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
        choices=["sentinel2", "landsat", "both", "patch-stats", "resnet"],
        default="patch-stats",
        help=(
            "Feature extraction mode:\n"
            "  patch-stats  — per-band statistics + indices from image patches (no model, default)\n"
            "  sentinel2    — Clay v1.5 embeddings from S2 (requires ~5 GB disk + torch)\n"
            "  landsat      — Clay embeddings from Landsat\n"
            "  both         — Clay embeddings, S2 primary + Landsat fallback\n"
            "  resnet       — pretrained ResNet-50 embeddings from S2 (requires torch + torchvision)"
        ),
    )
    parser.add_argument(
        "--resnet-size",
        choices=["resnet18", "resnet50"],
        default="resnet50",
        help="ResNet variant for --source resnet (default: resnet50 = 2048-dim).",
    )
    parser.add_argument("--download-landsat", action="store_true",
                        help="Download Landsat 8/9 scenes via Planetary Computer before extracting.")
    parser.add_argument("--device", default="cpu",
                        help="Torch device for Clay model: 'cpu' or 'cuda' (default: cpu).")
    parser.add_argument("--input", default=None,
                        help="Input CSV (default: field_data_with_terrain.csv or field_data_with_bands.csv).")
    parser.add_argument("--output", default=None,
                        help="Output CSV path (default: data/processed/field_data_with_clay.csv).")
    parser.add_argument("--composite", action="store_true",
                        help="Median-composite patches across all available tiles per point "
                             "(reduces cloud/atmospheric noise, recommended when multiple tiles exist).")
    parser.add_argument("--min-ndvi", type=float, default=None, metavar="FLOAT",
                        help="Drop patches where mean NDVI < threshold (e.g. 0.2). "
                             "Useful to exclude bare soil / cloud-covered patches.")
    parser.add_argument("--min-veg-frac", type=float, default=None, metavar="FLOAT",
                        help="Drop patches where the fraction of pixels with NDVI > 0.2 "
                             "is below this threshold (e.g. 0.3 = at least 30%% vegetated).")
    parser.add_argument("--max-cloud-frac", type=float, default=None, metavar="FLOAT",
                        help="Drop patches where SCL cloud fraction exceeds this threshold "
                             "(e.g. 0.1 = reject if more than 10%% of pixels are cloud/shadow).")
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
        features, n_feats, quality_cols, quality_data = extract_patch_stats(
            df, source="sentinel2", composite=args.composite)

        n_ok = int(np.sum(~np.isnan(features[:, 0])))
        print(f"   Processed: {n_ok}/{len(df)} rows ({n_ok/len(df)*100:.1f}%)  features={n_feats}")

        if n_ok == 0:
            print("\n  ERROR: No patch statistics extracted.")
            print("  Check that Sentinel-2 .SAFE files exist in data/raw/field_products/")
            sys.exit(1)

        # Apply optional vegetation / cloud quality filters
        min_ndvi    = args.min_ndvi
        min_veg_frac = args.min_veg_frac
        max_cloud   = args.max_cloud_frac
        if any(v is not None for v in [min_ndvi, min_veg_frac, max_cloud]):
            mask = np.ones(len(df), dtype=bool)
            if min_ndvi is not None and "quality_ndvi_mean" in quality_data:
                ndvi_arr = quality_data["quality_ndvi_mean"]
                mask &= (ndvi_arr >= min_ndvi) | np.isnan(ndvi_arr)
                n_dropped = int(np.sum(~np.isnan(ndvi_arr) & (ndvi_arr < min_ndvi)))
                print(f"   Filter --min-ndvi {min_ndvi}: dropped {n_dropped} low-vegetation rows")
            if min_veg_frac is not None and "quality_veg_frac" in quality_data:
                vf_arr = quality_data["quality_veg_frac"]
                mask &= (vf_arr >= min_veg_frac) | np.isnan(vf_arr)
                n_dropped = int(np.sum(~np.isnan(vf_arr) & (vf_arr < min_veg_frac)))
                print(f"   Filter --min-veg-frac {min_veg_frac}: dropped {n_dropped} rows")
            if max_cloud is not None and "quality_cloud_frac" in quality_data:
                cf_arr = quality_data["quality_cloud_frac"]
                mask &= (cf_arr <= max_cloud) | np.isnan(cf_arr)
                n_dropped = int(np.sum(~np.isnan(cf_arr) & (cf_arr > max_cloud)))
                print(f"   Filter --max-cloud-frac {max_cloud}: dropped {n_dropped} cloudy rows")
            df       = df[mask].reset_index(drop=True)
            features = features[mask]
            for qk in quality_cols:
                quality_data[qk] = quality_data[qk][mask]
            print(f"   After filters: {len(df)} rows remaining")

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
        feat_df  = pd.DataFrame(features[:, :n_feats], columns=feat_cols)
        qual_df  = pd.DataFrame({qk: quality_data[qk] for qk in quality_cols}) if quality_cols else pd.DataFrame()
        df_out   = pd.concat([df.reset_index(drop=True), feat_df, qual_df], axis=1)

        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        df_out.to_csv(out_csv, index=False, quoting=1)
        print(f"\n5. Saved: {out_csv}  shape={df_out.shape}")
        print(f"   Patch feature columns: {feat_cols[0]} … {feat_cols[-1]}")
        print(f"\nNext step:")
        print(f"  python src/train_ordinal.py {out_csv} --deduplicate --filter-barangay Paoay")

    elif args.source == "resnet":
        # ResNet pretrained feature extraction — fast, no large checkpoint download
        model_size = getattr(args, "resnet_size", "resnet50")
        print(f"3. Extracting ResNet ({model_size}) embeddings from S2 patches...")
        embeddings, embed_dim = extract_resnet_embeddings(df, model_size=model_size,
                                                           device=args.device)
        n_ok = int(np.sum(~np.isnan(embeddings[:, 0])))
        print(f"   Embedded: {n_ok}/{len(df)} rows ({n_ok/len(df)*100:.1f}%)")
        if n_ok == 0:
            print("\n  ERROR: No ResNet embeddings extracted.")
            print("  Check that Sentinel-2 .SAFE files exist in data/raw/field_products/")
            sys.exit(1)

        print("4. Merging embeddings into dataset...")
        embed_cols = [f"resnet_{i:04d}" for i in range(embed_dim)]
        embed_df   = pd.DataFrame(embeddings, columns=embed_cols)
        df_out     = pd.concat([df.reset_index(drop=True), embed_df], axis=1)
        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        df_out.to_csv(out_csv, index=False, quoting=1)
        print(f"\n5. Saved: {out_csv}  shape={df_out.shape}")
        print(f"   Embedding columns: resnet_0000 … resnet_{embed_dim-1:04d}")
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

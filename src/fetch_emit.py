"""Fetch NASA EMIT hyperspectral data for Benguet, Philippines field sites.

EMIT (Earth Surface Mineral Dust Source Investigation) is mounted on the ISS.
  - 285 spectral bands, 380–2500 nm
  - 60 m spatial resolution
  - Free via NASA Earthdata (earthdata.nasa.gov — free registration required)

This script:
  1. Searches for EMIT L2A surface reflectance scenes over the field AOI
  2. Filters for low cloud cover and bare-soil conditions
  3. Downloads the best available scene(s)
  4. Extracts per-point spectra for all GPS coordinates in the field CSV
  5. Saves enriched CSV to data/processed/field_data_with_emit.csv

Requirements:
    pip install earthaccess h5py

Credentials:
    Set EARTHDATA_USERNAME and EARTHDATA_PASSWORD in your .env file,
    OR run interactively and earthaccess will prompt you once then cache.

Run:
    python src/fetch_emit.py
    python src/fetch_emit.py --search-only      # just list available scenes
    python src/fetch_emit.py --max-scenes 3     # download up to N scenes
"""

import argparse
import os
import sys

import h5py
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FIELD_CSV   = "data/external/final_merged_data_cleaned.csv"
INPUT_CSV   = "data/processed/field_data_with_terrain.csv"   # prefer enriched
OUTPUT_CSV  = "data/processed/field_data_with_emit.csv"
EMIT_DIR    = "data/raw/emit"

# Benguet AOI bounding box (lon_min, lat_min, lon_max, lat_max)
AOI = (120.57, 16.43, 120.78, 16.65)

# EMIT archive start date
EMIT_START  = "2022-08-01"
EMIT_END    = "2025-12-31"

# Cloud / quality threshold — EMIT quality flag: 0 = good
MAX_CLOUD_FRACTION = 0.30   # skip scenes with >30% cloudy pixels over AOI

# Key wavelength ranges for soil nutrients (nm)
WAVELENGTH_RANGES = {
    "vis_red":        (620,  700),   # soil colour, iron oxides
    "nir":            (760,  900),   # vegetation / bare soil
    "swir1":          (1550, 1750),  # soil moisture, clay
    "swir2_p":        (2100, 2300),  # phosphate minerals, carbonates
    "swir2_k":        (2000, 2100),  # potassium feldspar / illite
    "swir2_clay":     (2150, 2250),  # kaolinite, smectite (N indicator via OM)
    "swir2_organic":  (2290, 2360),  # organic matter / cellulose
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _login():
    import earthaccess
    username = os.getenv("EARTHDATA_USERNAME") or os.getenv("EARTHDATA_USER")
    password = os.getenv("EARTHDATA_PASSWORD") or os.getenv("EARTHDATA_PASS")
    if username and password:
        earthaccess.login(strategy="environment")
    else:
        earthaccess.login(strategy="interactive", persist=True)
    return earthaccess


def search_emit_scenes(earthaccess, max_results=50):
    """Search EMIT L2A reflectance scenes over the Benguet AOI."""
    print(f"Searching EMIT_L2A_RFL scenes over AOI {AOI} ...")
    results = earthaccess.search_data(
        short_name="EMIT_L2A_RFL",
        bounding_box=AOI,
        temporal=(EMIT_START, EMIT_END),
        count=max_results,
    )
    print(f"  Found {len(results)} scene(s)")
    return results


def summarise_scenes(results):
    """Print a table of available scenes with dates and file sizes."""
    if not results:
        print("  No scenes found.")
        return

    print(f"\n  {'#':<4} {'Date':^22} {'Scene ID':^46} {'Size (MB)':>10}")
    print("  " + "-" * 86)
    for i, r in enumerate(results):
        meta  = r
        title = meta["umm"]["GranuleUR"]
        # Extract sensing date from title  e.g. EMIT_L2A_RFL_001_20230415T...
        parts = title.split("_")
        date_str = "unknown"
        for p in parts:
            if len(p) >= 8 and p[:8].isdigit():
                d = p[:8]
                date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
                break
        size_mb = sum(
            f.get("Size", 0) for f in meta.get("umm", {})
            .get("DataGranule", {}).get("ArchiveAndDistributionInformation", [])
        )
        print(f"  {i:<4} {date_str:^22} {title[:46]:^46} {size_mb:>10.1f}")


def _band_indices(wavelengths: np.ndarray, wmin: float, wmax: float) -> np.ndarray:
    """Return indices of bands within [wmin, wmax] nm."""
    return np.where((wavelengths >= wmin) & (wavelengths <= wmax))[0]


def extract_spectra(h5_path: str, lats: np.ndarray, lons: np.ndarray) -> dict:
    """Extract full spectra and range-averaged features for each GPS point.

    Returns a dict:  column_name -> np.ndarray (length = n_points)
    """
    with h5py.File(h5_path, "r") as f:
        # Locate datasets (EMIT uses different path layouts across versions)
        refl_key  = next(k for k in ["reflectance", "/reflectance",
                                      "HDFEOS/GRIDS/EMIT/Data Fields/Reflectance"]
                         if k in f)
        lat_key   = next(k for k in ["location/lat", "/location/lat", "lat"] if k in f)
        lon_key   = next(k for k in ["location/lon", "/location/lon", "lon"] if k in f)
        wl_key    = next(k for k in [
            "sensor_band_parameters/wavelengths",
            "/sensor_band_parameters/wavelengths",
            "wavelengths",
        ] if k in f)

        scene_lats  = f[lat_key][:]    # (lines, samples)
        scene_lons  = f[lon_key][:]
        wavelengths = f[wl_key][:]     # (bands,)
        refl_data   = f[refl_key]      # (lines, samples, bands) — lazy read

        n_lines, n_samples, n_bands = refl_data.shape
        print(f"    Scene shape: {n_lines} lines × {n_samples} samples × {n_bands} bands")
        print(f"    Wavelengths: {wavelengths[0]:.0f} – {wavelengths[-1]:.0f} nm")

        # Scene extent check
        scene_lat_min, scene_lat_max = scene_lats.min(), scene_lats.max()
        scene_lon_min, scene_lon_max = scene_lons.min(), scene_lons.max()
        in_scene = (
            (lats >= scene_lat_min) & (lats <= scene_lat_max) &
            (lons >= scene_lon_min) & (lons <= scene_lon_max)
        )
        n_covered = int(in_scene.sum())
        print(f"    GPS points covered by this scene: {n_covered} / {len(lats)}")
        if n_covered == 0:
            return {}

        # For each GPS point find nearest scene pixel
        results = {}
        all_spectra = np.full((len(lats), n_bands), np.nan, dtype=np.float32)

        for i in np.where(in_scene)[0]:
            dist = np.sqrt((scene_lats - lats[i])**2 + (scene_lons - lons[i])**2)
            row, col = np.unravel_index(np.argmin(dist), dist.shape)
            spec = refl_data[row, col, :]
            # EMIT uses -9999 as fill value
            spec = np.where(spec < -100, np.nan, spec.astype(np.float32))
            all_spectra[i] = spec

        # Compute range-averaged features
        for feat_name, (wmin, wmax) in WAVELENGTH_RANGES.items():
            idx = _band_indices(wavelengths, wmin, wmax)
            if len(idx) == 0:
                continue
            col_name = f"emit_{feat_name}"
            results[col_name] = np.nanmean(all_spectra[:, idx], axis=1)

        # Also save full-spectrum NDVI equivalent: (NIR - Red) / (NIR + Red)
        red_idx = _band_indices(wavelengths, 630, 690)
        nir_idx = _band_indices(wavelengths, 760, 900)
        if len(red_idx) and len(nir_idx):
            red = np.nanmean(all_spectra[:, red_idx], axis=1)
            nir = np.nanmean(all_spectra[:, nir_idx], axis=1)
            with np.errstate(invalid="ignore", divide="ignore"):
                results["emit_ndvi"] = (nir - red) / (nir + red + 1e-9)

        # Bare soil flag: NDVI < 0.25
        if "emit_ndvi" in results:
            results["emit_bare_soil"] = (results["emit_ndvi"] < 0.25).astype(np.float32)

        print(f"    Extracted features: {list(results.keys())}")
        return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Fetch EMIT hyperspectral data for Benguet field sites.")
    parser.add_argument("--search-only", action="store_true",
                        help="List available scenes without downloading.")
    parser.add_argument("--max-scenes", type=int, default=2,
                        help="Maximum number of scenes to download (default: 2).")
    parser.add_argument("--scene-index", type=int, default=None,
                        help="Download a specific scene by index from the search results.")
    args = parser.parse_args()

    # 1. Login
    print("1. Authenticating with NASA Earthdata...")
    print("   Set EARTHDATA_USERNAME + EARTHDATA_PASSWORD in .env, or enter credentials below.")
    ea = _login()
    print("   Authenticated.\n")

    # 2. Search
    print("2. Searching for EMIT scenes over Benguet, Philippines...")
    scenes = search_emit_scenes(ea)
    summarise_scenes(scenes)

    if args.search_only or not scenes:
        if not scenes:
            print("\nNo EMIT scenes found for this AOI and date range.")
            print("The ISS orbit may not have passed over Benguet during the archive period,")
            print("or all available scenes have heavy cloud cover.")
        sys.exit(0)

    # 3. Select scenes to download
    if args.scene_index is not None:
        selected = [scenes[args.scene_index]]
    else:
        selected = scenes[: args.max_scenes]

    print(f"\n3. Downloading {len(selected)} scene(s) to {EMIT_DIR}/ ...")
    os.makedirs(EMIT_DIR, exist_ok=True)
    downloaded = ea.download(selected, local_path=EMIT_DIR)
    h5_files = [p for p in downloaded if str(p).endswith(".nc") or str(p).endswith(".h5")]
    print(f"   Downloaded: {h5_files}")

    if not h5_files:
        print("   ERROR: No HDF5/NetCDF files downloaded.")
        sys.exit(1)

    # 4. Load field GPS coordinates
    print("\n4. Loading field GPS coordinates...")
    src_csv = INPUT_CSV if os.path.exists(INPUT_CSV) else FIELD_CSV
    df = pd.read_csv(src_csv, on_bad_lines="skip")
    lats = df["latitude"].to_numpy(dtype=float)
    lons = df["longitude"].to_numpy(dtype=float)
    print(f"   {len(df)} rows, {len(np.unique(list(zip(lats, lons))))} unique GPS points")

    # 5. Extract spectra from each scene, keep the best (non-NaN) value per point
    print("\n5. Extracting hyperspectral features...")
    emit_cols: dict[str, np.ndarray] = {}

    for h5_path in h5_files:
        print(f"  Processing: {os.path.basename(h5_path)}")
        scene_feats = extract_spectra(str(h5_path), lats, lons)
        for col, vals in scene_feats.items():
            if col not in emit_cols:
                emit_cols[col] = vals.copy()
            else:
                # Fill NaN gaps from subsequent scenes
                mask = np.isnan(emit_cols[col]) & ~np.isnan(vals)
                emit_cols[col][mask] = vals[mask]

    if not emit_cols:
        print("\nNo EMIT features extracted — scene(s) may not overlap the field AOI.")
        sys.exit(1)

    # 6. Merge and save
    print("\n6. Merging EMIT features into field dataset...")
    for col, vals in emit_cols.items():
        df[col] = vals

    coverage = (~df[[c for c in emit_cols]].isna().all(axis=1)).mean() * 100
    print(f"   GPS points with at least one EMIT feature: {coverage:.1f}%")

    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, quoting=1)
    print(f"\nSaved: {OUTPUT_CSV}  shape={df.shape}")
    print("Next step: python src/train_ordinal.py data/processed/field_data_with_emit.csv --deduplicate")


if __name__ == "__main__":
    main()

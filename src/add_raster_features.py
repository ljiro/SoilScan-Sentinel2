"""
LEGACY SCRIPT — European LUCAS / AgroLens workflow. DO NOT USE for the Philippine pipeline.

This script loads the European LUCAS SOIL dataset and extracts Sentinel-2 raster
features for those coordinates. It is kept for reference only.

For the Philippine pipeline (SoilScan-Sentinel2), band extraction and spectral
index computation are handled directly by:
    src/data_fetcher_copernicus.py

That script fetches Sentinel-2 L2A tiles for Philippine GPS coordinates (Benguet)
and computes the 12 spectral bands + 10 vegetation indices needed for ordinal
classification training.

--- Improvements applied to this legacy script ---
- Warns when multiple .SAFE directories are found; uses most recently modified
- Vegetation indices use shared compute_vegetation_indices() from utils.py
- Replaced bare exit() with sys.exit()
"""

import glob
import os
import re
import sys

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import Window

from utils import compute_vegetation_indices

# --- 1. Configuration ---
LUCAS_FILE_PATH = 'data/external/final_merged_data_cleaned.csv'
YIELD_DIR = 'data/raw/fao_gaez/'
OUTPUT_CSV_PATH = 'data/processed/LUCAS_with_Raster_Features.csv'

# --- 2. Helper Functions ---
def find_safe_directory():
    """Find the downloaded .SAFE directory in data/raw/.

    If multiple directories exist, the most recently modified one is used
    and a warning is printed so the user can verify the selection.
    """
    safe_dirs = (
        glob.glob('data/raw/S2A_MSIL2A_*.SAFE')
        + glob.glob('data/raw/S2B_MSIL2A_*.SAFE')
    )
    if not safe_dirs:
        return None
    if len(safe_dirs) > 1:
        safe_dirs.sort(key=os.path.getmtime, reverse=True)
        print(f"   WARNING: {len(safe_dirs)} .SAFE directories found. "
              f"Using the most recently modified one:")
        for d in safe_dirs:
            print(f"     {'*' if d == safe_dirs[0] else ' '} {d}")
    return safe_dirs[0]

def find_jp2_files(safe_dir):
    """Find all .jp2 files in the SAFE directory structure"""
    jp2_files = []
    
    # Search in all possible locations
    patterns = [
        os.path.join(safe_dir, 'GRANULE', '*', 'IMG_DATA', 'R10m', '*.jp2'),
        os.path.join(safe_dir, 'GRANULE', '*', 'IMG_DATA', 'R20m', '*.jp2'),
        os.path.join(safe_dir, 'GRANULE', '*', 'IMG_DATA', 'R60m', '*.jp2'),
    ]
    
    for pattern in patterns:
        jp2_files.extend(glob.glob(pattern))
    
    return jp2_files

def extract_band_name(filename):
    """Extract band name from filename, return None for non-band files"""
    # Skip non-band files (AOT, TCI, WVP, SCL)
    skip_patterns = ['AOT', 'TCI', 'WVP', 'SCL']
    if any(pattern in filename for pattern in skip_patterns):
        return None
    
    # Extract band name using multiple patterns
    patterns = [
        r'_(B(0[1-9]|1[0-2]|8A))_',
        r'_([A-Z0-9]{3})\.jp2$'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            return match.group(1)
    
    return None

# --- 3. Load Ground-Truth (LUCAS) Data ---
print(f"1. Loading LUCAS data from {LUCAS_FILE_PATH}...")
try:
    df_lucas = pd.read_csv(LUCAS_FILE_PATH)
except FileNotFoundError:
    print(f"ERROR: LUCAS file not found at {LUCAS_FILE_PATH}")
    sys.exit(1)

print(f"   Loaded {len(df_lucas)} soil sample points.")

# Convert to GeoDataFrame
gdf_lucas_wgs84 = gpd.GeoDataFrame(
    df_lucas,
    geometry=gpd.points_from_xy(df_lucas['longitude'], df_lucas['latitude']),
    crs='EPSG:4326'
)
coords_wgs84 = [(pt.x, pt.y) for pt in gdf_lucas_wgs84.geometry]

# --- 4. Sample Crop Yield (CRY) Features  ---
print(f"2. Sampling Crop Yield (CRY) data from {YIELD_DIR}...")
try:
    yield_files = [f for f in os.listdir(YIELD_DIR) if f.endswith('.tif')]
except FileNotFoundError:
    print(f"   Warning: Yield directory not found at {YIELD_DIR}. Skipping.")
    yield_files = []

for filename in yield_files:
    feature_name = os.path.splitext(filename)[0]
    file_path = os.path.join(YIELD_DIR, filename)
    
    with rasterio.open(file_path) as src:
        samples = [val[0] for val in src.sample(coords_wgs84)]
        df_lucas[feature_name] = samples
        print(f"   ...added feature: {feature_name}")

# --- 5. Find and Process Downloaded Sentinel-2 Data ---
print(f"3. Finding downloaded Sentinel-2 data...")
safe_dir = find_safe_directory()
if not safe_dir:
    print("ERROR: No .SAFE directory found in data/raw/")
    print("   Make sure data_acquisition.py ran successfully first.")
    sys.exit(1)

print(f"   Found SAFE directory: {safe_dir}")

jp2_files = find_jp2_files(safe_dir)
if not jp2_files:
    print(f"ERROR: No .jp2 files found in {safe_dir}")
    sys.exit(1)

print(f"   Found {len(jp2_files)} JP2 files")

# Filter only band files and remove duplicates
band_files = []
processed_bands = set()

for file_path in jp2_files:
    filename = os.path.basename(file_path)
    band_name = extract_band_name(filename)
    
    if band_name and band_name not in processed_bands:
        band_files.append((file_path, band_name))
        processed_bands.add(band_name)
    elif band_name is None:
        print(f"   ⚠️  Skipping non-band file: {filename}")
    else:
        print(f"   ⚠️  Skipping duplicate band: {band_name}")

print(f"   Processing {len(band_files)} unique bands: {sorted(processed_bands)}")

# Get the target CRS from the first Sentinel file
target_crs = None
with rasterio.open(band_files[0][0]) as src:
    target_crs = src.crs

print(f"   Target CRS (from satellite data): {target_crs}")

# Reproject LUCAS points to match the Sentinel CRS
gdf_projected = gdf_lucas_wgs84.to_crs(target_crs)
coords_projected = [(pt.x, pt.y) for pt in gdf_projected.geometry]

print(f"4. Extracting 3x3 neighbor pixels (SURR) for {len(coords_projected)} points...")

# Collect all band data first, then add to DataFrame at once (for performance)
all_band_data = {}

for file_path, band_name in band_files:
    print(f"   Processing {band_name}...")
    
    # Create 9 new column names for this band
    neighbor_cols = [f"{band_name}_{i+1}" for i in range(9)]
    band_data_3x3 = np.full((len(coords_projected), 9), np.nan, dtype=np.float32)

    with rasterio.open(file_path) as src:
        for i, (x, y) in enumerate(coords_projected):
            try:
                # Get the row, col index for the center pixel
                row, col = src.index(x, y)
                # Define a 3x3 window centered on the pixel 
                window = Window(col - 1, row - 1, 3, 3)
                # Read the 3x3 patch
                patch = src.read(1, window=window)
                # Flatten the 3x3 patch into a 1x9 array
                band_data_3x3[i] = patch.flatten()
            except (ValueError, IndexError, rasterio.errors.WindowError):
                # Point is outside the raster bounds
                band_data_3x3[i] = np.nan
            except Exception as e:
                print(f"   ⚠️  Error sampling point {i} for {band_name}: {e}")
                band_data_3x3[i] = np.nan
    
    # Store band data for later concatenation
    for j, col_name in enumerate(neighbor_cols):
        all_band_data[col_name] = band_data_3x3[:, j]

# Add all band data to DataFrame at once (performance improvement)
print("   Adding all band data to DataFrame...")
band_df = pd.DataFrame(all_band_data)
df_lucas = pd.concat([df_lucas, band_df], axis=1)

# --- 6. Calculate Vegetation Indices from Center Pixel ---
print("5. Calculating Vegetation Indices from center pixels...")

# Pixel 5 (index 4) is the center of the 3x3 grid
vi_data = compute_vegetation_indices(df_lucas, center_suffix='_5')
if vi_data:
    vi_df = pd.DataFrame(vi_data, index=df_lucas.index)
    df_lucas = pd.concat([df_lucas, vi_df], axis=1)
    print(f"   Calculated: {list(vi_data.keys())}")
else:
    print("   WARNING: Center-pixel band columns not found — skipping VI calculation.")

# Replace inf/-inf values with NaN
df_lucas.replace([np.inf, -np.inf], np.nan, inplace=True)

# --- 7. Save Final Merged CSV ---
print("6. Saving merged raster features...")
os.makedirs('data/processed', exist_ok=True)
df_lucas.to_csv(OUTPUT_CSV_PATH, index=False)

# Print summary statistics
sentinel_features = [col for col in df_lucas.columns if re.match(r'B(0[1-9]|1[0-2]|8A)_\d', col)]
vi_features = [col for col in df_lucas.columns if col in ['NDVI', 'NDRE', 'GNDVI']]
yield_features = [col for col in df_lucas.columns if col.startswith('yld_')]

print(f"\n✅ Success! Merged data saved to {OUTPUT_CSV_PATH}")
print(f"   Total samples: {len(df_lucas)}")
print(f"   Sentinel features: {len(sentinel_features)}")
print(f"   Vegetation indices: {len(vi_features)}")
print(f"   Yield features: {len(yield_features)}")
print(f"   Total features: {len(df_lucas.columns)}")
print("   Next step: Run `src/add_weather_features.py`")
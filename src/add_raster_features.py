# Save this file as: src/add_raster_features.py

import pandas as pd
import geopandas as gpd
import rasterio
import rasterio.sample
from rasterio.windows import Window
import os
import re
import numpy as np

# --- 1. Configuration ---
LUCAS_FILE_PATH = 'data/external/LUCAS SOIL Modified.csv'

# ⚠️ Point this to your downloaded FAO GAEZ GeoTIFFs
YIELD_DIR = 'data/raw/fao_gaez/'

# ⚠️ UPDATE THIS PATH to the specific image data folder from your download
JP2_DIR = 'data/raw/YOUR_DOWNLOADED_SAFE_FILE.SAFE/GRANULE/L2A_.../IMG_DATA/R10m'

# This is the intermediate output file
OUTPUT_CSV_PATH = 'data/processed/LUCAS_with_Raster_Features.csv'

# --- 2. Load Ground-Truth (LUCAS) Data ---
print(f"1. Loading LUCAS data from {LUCAS_FILE_PATH}...")
try:
    df_lucas = pd.read_csv(LUCAS_FILE_PATH)
except FileNotFoundError:
    print(f"❌ Error: LUCAS file not found at {LUCAS_FILE_PATH}")
    exit()

print(f"   Loaded {len(df_lucas)} soil sample points.")

# Convert to GeoDataFrame, keeping it in WGS84 (EPSG:4326) [cite: 991]
gdf_lucas_wgs84 = gpd.GeoDataFrame(
    df_lucas,
    geometry=gpd.points_from_xy(df_lucas['TH_LONG'], df_lucas['TH_LAT']),
    crs='EPSG:4326' # WGS84 Lat/Lon
)
# Get WGS84 coordinates for sampling
coords_wgs84 = [(pt.x, pt.y) for pt in gdf_lucas_wgs84.geometry]

# --- 3. Sample Crop Yield (CRY) Features  ---
print(f"2. Sampling Crop Yield (CRY) data from {YIELD_DIR}...")
try:
    yield_files = [f for f in os.listdir(YIELD_DIR) if f.endswith('.tif')]
except FileNotFoundError:
    print(f"   Warning: Yield directory not found at {YIELD_DIR}. Skipping.")
    yield_files = []

for filename in yield_files:
    # Use filename as feature name
    feature_name = os.path.splitext(filename)[0]
    file_path = os.path.join(YIELD_DIR, filename)
    
    with rasterio.open(file_path) as src:
        # Sample yield rasters using WGS84 coordinates
        samples = [val[0] for val in src.sample(coords_wgs84)]
        df_lucas[feature_name] = samples
        print(f"   ...added feature: {feature_name}")

# --- 4. Sample 3x3 Neighbor Pixel (SURR) Features  ---
print(f"3. Scanning for Sentinel-2 .jp2 files in {JP2_DIR}...")
try:
    jp2_files_all = [f for f in os.listdir(JP2_DIR) if f.endswith('.jp2')]
    jp2_files_all.sort()
except FileNotFoundError:
    print(f"❌ Error: JP2 directory not found at {JP2_DIR}")
    exit()

if not jp2_files_all:
    print(f"❌ Error: No .jp2 files found in {JP2_DIR}.")
    exit()

# Get the target CRS from the first Sentinel file
target_crs = None
with rasterio.open(os.path.join(JP2_DIR, jp2_files_all[0])) as src:
    target_crs = src.crs

print(f"   Target CRS (from satellite data): {target_crs}")

# **Reproject** LUCAS points to match the Sentinel CRS
gdf_projected = gdf_lucas_wgs84.to_crs(target_crs)
coords_projected = [(pt.x, pt.y) for pt in gdf_projected.geometry]

print(f"4. Extracting 3x3 neighbor pixels (SURR) for {len(coords_projected)} points...")

for filename in jp2_files_all:
    # Extract band name (e.g., "B04")
    match = re.search(r'_(B(0[1-9]|1[0-2]|8A))_', filename)
    if not match:
        continue
    
    band_name = match.group(1) # e.g., "B04"
    file_path = os.path.join(JP2_DIR, filename)
    
    # Create 9 new column names for this band, e.g., B04_1, B04_2...
    neighbor_cols = [f"{band_name}_{i+1}" for i in range(9)]
    # Create a temporary array to hold all 9 values for all points
    band_data_3x3 = np.zeros((len(coords_projected), 9), dtype=np.int16)

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
            except Exception as e:
                # print(f"   Warning: Could not sample point {i} for {band_name}. Error: {e}")
                band_data_3x3[i] = np.nan # Mark as NaN if out of bounds
    
    # Add the 9 new features to the main DataFrame
    df_lucas[neighbor_cols] = band_data_3x3
    print(f"   ...added 9 neighbor features for {band_name}")

# --- 5. Save Final Merged CSV ---
print("5. Saving merged raster features...")
df_lucas.to_csv(OUTPUT_CSV_PATH, index=False)
print(f"\n✅ Success! Merged data saved to {OUTPUT_CSV_PATH}")
print("   Next step: Run `src/add_weather_features.py`")
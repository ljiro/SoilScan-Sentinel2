import os
import pandas as pd
from sentinelsat import SentinelAPI
from datetime import date
from dotenv import load_dotenv  # 👈 --- 1. Import library ---

# --- 1. Configuration ---
load_dotenv()  # 👈 --- 2. Load variables from .env file ---

# ⚠️ --- 3. Get credentials from environment ---
# os.getenv() safely reads the variables.
API_USER = os.getenv('COPERNICUS_USER')
API_PASS = os.getenv('COPERNICUS_PASS')
# ⬇️ *** FIXED LINE *** ⬇️
# Changed from '/lta' to the correct '/catalogue' endpoint
API_URL = 'https://catalogue.dataspace.copernicus.eu/'

# Check if credentials are loaded
if not API_USER or not API_PASS:
    print("❌ Error: API_USER or API_PASS not found in .env file.")
    print("Please create a .env file in the project root with your credentials.")
    exit()

# Point this to your ground-truth file
LUCAS_FILE_PATH = 'data/external/LUCAS SOIL Modified.csv' 
DOWNLOAD_DIR = 'data/raw/'
DATE_RANGE = (date(2018, 5, 1), date(2018, 8, 31)) 
CLOUD_COVER = (0, 20) 

# --- 2. Generate AOI from LUCAS File ---
print(f"1. Reading LUCAS file to generate AOI: {LUCAS_FILE_PATH}")
try:
    df_lucas = pd.read_csv(LUCAS_FILE_PATH)
except FileNotFoundError:
    print(f"❌ Error: LUCAS file not found at {LUCAS_FILE_PATH}")
    print("Please place your file in the 'data/external/' directory.")
    exit()

# Find the bounding box, adding a small buffer
min_lon = df_lucas['TH_LONG'].min() - 0.01
max_lon = df_lucas['TH_LONG'].max() + 0.01
min_lat = df_lucas['TH_LAT'].min() - 0.01
max_lat = df_lucas['TH_LAT'].max() + 0.01

# Create WKT (Well-Known Text) string for the bounding box
AOI_WKT = f'POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))'
print(f"   Generated Bounding Box: {AOI_WKT}")

# --- 3. Connect and Search API ---
try:
    # Pass the loaded variables to the API
    api = SentinelAPI(API_USER, API_PASS, API_URL)
    print("2. Connected to Copernicus API")
except Exception as e:
    print(f"❌ Failed to connect to API. Check credentials. Error: {e}")
    exit()

# Search for Sentinel-2, Level-2A (atmospherically corrected) products
products = api.query(
    AOI_WKT,
    date=DATE_RANGE,
    platformname='Sentinel-2',
    producttype='S2MSI2A',
    cloudcoverpercentage=CLOUD_COVER
)

if not products:
    print("🤷 No products found for the given criteria.")
    exit()

products_df = api.to_dataframe(products)
product_to_download = products_df.sort_values('cloudcoverpercentage', ascending=True).iloc[0]
print(f"3. Found {len(products_df)} products. Selected least cloudy: {product_to_download['filename']}")

# --- 4. Download Products ---
print(f"4. ⬇️ Downloading product...")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
# Download the full .SAFE zip file and unzip it
api.download(
    product_to_download.name, 
    directory_path=DOWNLOAD_DIR,
    unzip=True
)

print(f"✅ Download complete and unzipped to: {DOWNLOAD_DIR}")
print("\nNext step: Run `src/add_raster_features.py`")
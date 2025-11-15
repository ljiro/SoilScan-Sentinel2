import os
import pandas as pd
import requests
from datetime import date
from dotenv import load_dotenv
import zipfile
import io

# --- 0. Load Environment Variables ---
load_dotenv()

# --- 1. Configuration ---
CLIENT_ID = os.getenv('COPERNICUS_CLIENT_ID')
CLIENT_SECRET = os.getenv('COPERNICUS_CLIENT_SECRET')

# Endpoints for the new Copernicus Data Space Ecosystem
AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
DOWNLOAD_URL_TEMPLATE = "https://zipper.dataspace.copernicus.eu/odata/v1/Products('{}')/$value"

# Project file paths
LUCAS_FILE_PATH = 'data/external/LUCAS SOIL Modified.csv'
DOWNLOAD_DIR = 'data/raw/'

# Query parameters
DATE_RANGE = (date(2018, 5, 1), date(2018, 8, 31))
CLOUD_COVER = 20 # Max cloud cover %

# --- 2. Authenticate and Get Access Token ---
print("1. Authenticating with Copernicus Data Space...")
try:
    auth_data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    response = requests.post(AUTH_URL, data=auth_data)
    response.raise_for_status() # Raise an error for bad responses (4xx, 5xx)
    access_token = response.json()['access_token']
    print("   ✅ Authentication successful.")
except Exception as e:
    print(f"❌ FAILED to authenticate. Check COPERNICUS_CLIENT_ID and COPERNICUS_CLIENT_SECRET in .env file.")
    print(f"   Error: {e}")
    exit(1)

auth_headers = {'Authorization': f'Bearer {access_token}'}

# --- 3. Generate AOI from LUCAS File ---
print(f"2. Reading LUCAS file to generate AOI: {LUCAS_FILE_PATH}")
try:
    df_lucas = pd.read_csv(LUCAS_FILE_PATH)
except FileNotFoundError:
    print(f"❌ Error: LUCAS file not found at {LUCAS_FILE_PATH}")
    exit(1)

# Find the bounding box
min_lon = df_lucas['TH_LONG'].min() - 0.01
max_lon = df_lucas['TH_LONG'].max() + 0.01
min_lat = df_lucas['TH_LAT'].min() - 0.01
max_lat = df_lucas['TH_LAT'].max() + 0.01

# Create WKT (Well-Known Text) string for the bounding box
# Note: OData API requires WGS84 (EPSG:4326) coordinates
AOI_WKT = f'POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))'
print(f"   Generated Bounding Box (WGS84): {AOI_WKT}")

# --- 4. Search for Products ---
print(f"3. Querying for products...")
start_date_str = DATE_RANGE[0].strftime('%Y-%m-%dT00:00:00.000Z')
end_date_str = DATE_RANGE[1].strftime('%Y-%m-%dT23:59:59.000Z')

# Construct the OData $filter query
# This is a complex string, but it's what the API requires
filter_query = (
    f"Collection/Name eq 'SENTINEL-2' "
    f"and OData.CSC.Intersects(area=geography'SRID=4326;{AOI_WKT}') "
    f"and ContentDate/Start ge {start_date_str} "
    f"and ContentDate/Start le {end_date_str} "
    f"and contains(Name, 'MSIL2A') " # This specifies Level-2A products
    f"and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/Value le {CLOUD_COVER})"
)

# Construct the full API request
search_params = {
    '$filter': filter_query,
    '$orderby': 'Attributes/OData.CSC.DoubleAttribute/Cast(\'cloudCover\', \'Edm.Double\') asc', # Order by cloud cover
    '$top': 5 # Get the top 5 least cloudy
}

try:
    response = requests.get(CATALOG_URL, headers=auth_headers, params=search_params)
    response.raise_for_status()
    products = response.json().get('value', [])
except Exception as e:
    print(f"❌ FAILED to query products. Error: {e}")
    exit(1)

if not products:
    print("🤷 No products found for the given criteria.")
    exit()

# Select the best product (first one, since we sorted by cloud cover)
product_to_download = products[0]
product_name = product_to_download['Name']
product_id = product_to_download['Id']
print(f"4. Found {len(products)} products. Selected least cloudy: {product_name}")

# --- 5. Download and Unzip Product ---
print(f"5. ⬇️ Downloading product (this may take a while)...")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
download_url = DOWNLOAD_URL_TEMPLATE.format(product_id)

try:
    with requests.get(download_url, headers=auth_headers, stream=True) as r:
        r.raise_for_status()
        
        # Check if the file already exists
        zip_path = os.path.join(DOWNLOAD_DIR, f"{product_name}.zip")
        if os.path.exists(zip_path):
            print(f"   File {product_name}.zip already exists. Skipping download.")
        else:
            with open(zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"   ...Download complete: {zip_path}")
    
    # Unzip the file
    print(f"6. Unzipping file: {zip_path}")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(DOWNLOAD_DIR)
    
    print(f"✅ Download and extraction complete in: {DOWNLOAD_DIR}")
    print("\nNext step: Run `src/add_raster_features.py`")

except Exception as e:
    print(f"❌ FAILED during download or unzip: {e}")
    exit(1)
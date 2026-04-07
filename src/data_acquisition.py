"""
LEGACY SCRIPT — European LUCAS workflow. DO NOT USE for the Philippine pipeline.

This script was adapted from cvims/AgroLens and targets the original European LUCAS
soil survey dataset. It uses 'client_credentials' authentication and expects a LUCAS
CSV with 'TH_LONG' / 'TH_LAT' columns.

For the Philippine field data pipeline (SoilScan-Sentinel2), use:
    src/data_fetcher_copernicus.py

That script handles Sentinel-2 L2A product search, download (HTTP + S3), band
extraction, and spectral index computation for Philippine GPS coordinates collected
via the AgriCapture mobile app.

--- Improvements applied to this legacy script ---
- Extracted repeated download blocks into _try_download() (DRY)
- Fixed binary ZIP error reading (reads as bytes, decodes safely)
- Fixed credential error message to match actual env var names
- Replaced bare exit() with sys.exit()
"""

import io
import os
import sys
import zipfile

import pandas as pd
import requests
from datetime import date
from dotenv import load_dotenv

# --- 0. Load Environment Variables ---
load_dotenv()

# --- 1. Configuration ---
CLIENT_ID     = os.getenv('COPERNICUS_CLIENT_ID') or os.getenv('COPERNICUS_USER')
CLIENT_SECRET = os.getenv('COPERNICUS_CLIENT_SECRET') or os.getenv('COPERNICUS_PASS')

AUTH_URL          = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CATALOG_URL       = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
DOWNLOAD_URL_TMPL = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({})/$value"

LUCAS_FILE_PATH = 'data/external/LUCAS SOIL Modified.csv'
DOWNLOAD_DIR    = 'data/raw/'

DATE_RANGE  = (date(2018, 5, 1), date(2018, 8, 31))
CLOUD_COVER = 20  # Max cloud cover %

# Fallback download endpoints tried in order when the primary URL returns non-200
_ALT_URLS = [
    "https://catalogue.dataspace.copernicus.eu/odata/v1/Products({})/$value",
    "https://download.dataspace.copernicus.eu/odata/v1/Products({})/$value",
]


# --- 2. Authenticate ---
print("1. Authenticating with Copernicus Data Space...")
if not CLIENT_ID or not CLIENT_SECRET:
    print("ERROR: Missing credentials.")
    print("Set COPERNICUS_CLIENT_ID + COPERNICUS_CLIENT_SECRET "
          "(or COPERNICUS_USER + COPERNICUS_PASS) in your .env file.")
    sys.exit(1)

try:
    auth_resp = requests.post(
        AUTH_URL,
        data={'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET,
              'grant_type': 'client_credentials'},
    )
    auth_resp.raise_for_status()
    access_token = auth_resp.json()['access_token']
    print("   Authentication successful.")
except Exception as exc:
    print(f"ERROR: Authentication failed — {exc}")
    print("Check COPERNICUS_CLIENT_ID / COPERNICUS_CLIENT_SECRET in your .env file.")
    sys.exit(1)

auth_headers = {'Authorization': f'Bearer {access_token}'}


# --- 3. Generate AOI from LUCAS file ---
print(f"2. Reading LUCAS file to generate AOI: {LUCAS_FILE_PATH}")
try:
    df_lucas = pd.read_csv(LUCAS_FILE_PATH)
except FileNotFoundError:
    print(f"ERROR: LUCAS file not found at {LUCAS_FILE_PATH}")
    sys.exit(1)

# 0.01° buffer around the point cloud so the tile covers all samples
min_lon = df_lucas['TH_LONG'].min() - 0.01
max_lon = df_lucas['TH_LONG'].max() + 0.01
min_lat = df_lucas['TH_LAT'].min()  - 0.01
max_lat = df_lucas['TH_LAT'].max()  + 0.01

AOI_WKT = (
    f'POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, '
    f'{max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))'
)
print(f"   Bounding box (WGS84): {AOI_WKT}")


# --- 4. Search for Products ---
print("3. Querying catalogue for products...")
start_str = DATE_RANGE[0].strftime('%Y-%m-%dT00:00:00.000Z')
end_str   = DATE_RANGE[1].strftime('%Y-%m-%dT23:59:59.000Z')

filter_query = (
    f"Collection/Name eq 'SENTINEL-2' "
    f"and OData.CSC.Intersects(area=geography'SRID=4326;{AOI_WKT}') "
    f"and ContentDate/Start ge {start_str} "
    f"and ContentDate/Start le {end_str} "
    f"and contains(Name,'MSIL2A')"
)

try:
    resp = requests.get(CATALOG_URL, headers=auth_headers,
                        params={'$filter': filter_query,
                                '$orderby': 'ContentDate/Start desc',
                                '$top': 10})
    resp.raise_for_status()
    products = resp.json().get('value', [])
    print(f"   Found {len(products)} products")
except Exception as exc:
    print(f"ERROR: Catalogue query failed — {exc}")
    sys.exit(1)

if not products:
    print("No products found for the given criteria.")
    sys.exit(0)

# Filter by cloud cover (fetching per-product attributes)
filtered_products = []
for product in products:
    cloud_cover = None
    try:
        dr = requests.get(f"{CATALOG_URL}('{product['Id']}')", headers=auth_headers)
        if dr.status_code == 200:
            for attr in dr.json().get('Attributes', []):
                if attr.get('Name') == 'cloudCover':
                    cloud_cover = float(attr['Value'])
                    break
    except Exception:
        pass

    if cloud_cover is None or cloud_cover <= CLOUD_COVER:
        filtered_products.append({
            'Id':         product['Id'],
            'Name':       product['Name'],
            'CloudCover': cloud_cover,
        })

if not filtered_products:
    print(f"WARNING: No products found with cloud cover <= {CLOUD_COVER}%. "
          "Falling back to the first available product.")
    filtered_products = [{'Id': p['Id'], 'Name': p['Name'], 'CloudCover': 'unknown'}
                         for p in products[:1]]

product_to_download = filtered_products[0]
product_name = product_to_download['Name']
product_id   = product_to_download['Id']
print(f"4. Selected product: {product_name}")
print(f"   Cloud Cover: {product_to_download['CloudCover']}%")
print(f"   Product ID:  {product_id}")


# --- 5. Download Helper ---

def _stream_to_file(response, zip_path):
    """Write a streaming response to *zip_path*, printing progress."""
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    print(f"   Progress: {downloaded / total_size * 100:.1f}%", end='\r')
    print()  # newline after \r progress
    print(f"   Download complete — {downloaded / 1_048_576:.2f} MB")


def _unzip(zip_path, extract_dir):
    """Unzip *zip_path* into *extract_dir*. Provides a clear error on bad ZIPs."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_dir)
        print(f"   Extracted to: {extract_dir}")
    except zipfile.BadZipFile:
        print("ERROR: The downloaded file is not a valid ZIP.")
        # Try to decode an error message from the server (binary-safe)
        try:
            raw = open(zip_path, 'rb').read(2048)
            msg = raw.decode('utf-8', errors='replace').replace('\n', ' ').strip()
            print(f"   Server response (first 2 KB): {msg}")
        except Exception:
            pass
        sys.exit(1)


def _try_download(url, product_name, download_dir, auth_headers):
    """Try to download *url* → ZIP, then extract. Returns True on success."""
    zip_path = os.path.join(download_dir, f"{product_name}.zip")
    if os.path.exists(zip_path):
        print(f"   {product_name}.zip already exists — skipping download.")
        _unzip(zip_path, download_dir)
        return True
    try:
        with requests.get(url, headers=auth_headers, stream=True) as r:
            r.raise_for_status()
            _stream_to_file(r, zip_path)
        _unzip(zip_path, download_dir)
        return True
    except requests.HTTPError as exc:
        print(f"   HTTP {exc.response.status_code} from {url}")
    except Exception as exc:
        print(f"   Failed: {exc}")
    return False


# --- 6. Download with fallback URLs ---
print("5. Downloading product (this may take a while)...")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

all_urls = [DOWNLOAD_URL_TMPL.format(product_id)] + [u.format(product_id) for u in _ALT_URLS]

for attempt, url in enumerate(all_urls, 1):
    print(f"   Attempt {attempt}/{len(all_urls)}: {url}")
    if _try_download(url, product_name, DOWNLOAD_DIR, auth_headers):
        print("\nDownload and extraction complete.")
        print("Next step: Run `src/add_raster_features.py`")
        sys.exit(0)

print("\nERROR: All download attempts failed.")
print("Check your internet connection, account permissions, and product availability.")
sys.exit(1)

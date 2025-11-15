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
DOWNLOAD_URL_TEMPLATE = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({})/$value"

# Project file paths
LUCAS_FILE_PATH = 'data/external/LUCAS SOIL Modified.csv'
DOWNLOAD_DIR = 'data/raw/'

# Query parameters
DATE_RANGE = (date(2018, 5, 1), date(2018, 8, 31))
CLOUD_COVER = 20  # Max cloud cover %

# --- 2. Authenticate and Get Access Token ---
print("1. Authenticating with Copernicus Data Space...")
try:
    auth_data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    response = requests.post(AUTH_URL, data=auth_data)
    response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)
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
AOI_WKT = f'POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))'
print(f"   Generated Bounding Box (WGS84): {AOI_WKT}")

# --- 4. Search for Products ---
print(f"3. Querying for products...")
start_date_str = DATE_RANGE[0].strftime('%Y-%m-%dT00:00:00.000Z')
end_date_str = DATE_RANGE[1].strftime('%Y-%m-%dT23:59:59.000Z')

# CORRECTED filter query - simplified approach
filter_query = (
    f"Collection/Name eq 'SENTINEL-2' "
    f"and OData.CSC.Intersects(area=geography'SRID=4326;{AOI_WKT}') "
    f"and ContentDate/Start ge {start_date_str} "
    f"and ContentDate/Start le {end_date_str} "
    f"and contains(Name,'MSIL2A')"
)

print(f"   Filter query: {filter_query}")

# Construct the full API request
search_params = {
    '$filter': filter_query,
    '$orderby': 'ContentDate/Start desc',
    '$top': 10  # Get more results to choose from
}

try:
    response = requests.get(CATALOG_URL, headers=auth_headers, params=search_params)
    response.raise_for_status()
    products = response.json().get('value', [])
    print(f"   ✅ Found {len(products)} products")
except Exception as e:
    print(f"❌ FAILED to query products. Error: {e}")
    if hasattr(e, 'response') and e.response is not None:
        print(f"   Server Response: {e.response.text}")
    exit(1)

if not products:
    print("🤷 No products found for the given criteria.")
    exit()

# Filter products by cloud cover manually (since the attribute filter might be causing issues)
filtered_products = []
for product in products:
    # Try to get cloud cover information if available
    cloud_cover = None
    try:
        # Get full product details to check cloud cover
        product_details_url = f"{CATALOG_URL}('{product['Id']}')"
        details_response = requests.get(product_details_url, headers=auth_headers)
        if details_response.status_code == 200:
            details = details_response.json()
            # Look for cloud cover in attributes
            for attr in details.get('Attributes', []):
                if attr.get('Name') == 'cloudCover':
                    cloud_cover = attr.get('Value')
                    break
    except:
        pass
    
    # If we have cloud cover info, use it to filter
    if cloud_cover is None or float(cloud_cover) <= CLOUD_COVER:
        filtered_products.append({
            'Id': product['Id'],
            'Name': product['Name'],
            'CloudCover': cloud_cover
        })

if not filtered_products:
    print("🤷 No products found with cloud cover <= 20%.")
    # Fall back to original products without cloud cover filtering
    filtered_products = [{'Id': p['Id'], 'Name': p['Name'], 'CloudCover': 'Unknown'} for p in products[:1]]

# Select the first product with lowest cloud cover
product_to_download = filtered_products[0]
product_name = product_to_download['Name']
product_id = product_to_download['Id']
cloud_cover_info = product_to_download['CloudCover']

print(f"4. Selected product: {product_name}")
print(f"   Cloud Cover: {cloud_cover_info}%")
print(f"   Product ID: {product_id}")

# --- 5. Download and Unzip Product ---
print(f"5. ⬇️ Downloading product (this may take a while)...")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# FIXED: Use the correct download endpoint format without quotes around product ID
download_url = DOWNLOAD_URL_TEMPLATE.format(product_id)

print(f"   Download URL: {download_url}")

try:
    # First, check if we can access the download
    head_response = requests.head(download_url, headers=auth_headers)
    print(f"   Pre-flight check: {head_response.status_code}")
    
    if head_response.status_code == 200:
        print(f"   Download is accessible, starting download...")
        
        with requests.get(download_url, headers=auth_headers, stream=True) as r:
            r.raise_for_status()
            
            # Check if the file already exists
            zip_path = os.path.join(DOWNLOAD_DIR, f"{product_name}.zip")
            if os.path.exists(zip_path):
                print(f"   File {product_name}.zip already exists. Skipping download.")
            else:
                print(f"   Downloading to {zip_path}...")
                total_size = int(r.headers.get('content-length', 0))
                downloaded_size = 0
                
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            if total_size > 0:
                                percent = (downloaded_size / total_size) * 100
                                print(f"   Progress: {percent:.1f}%", end='\r')
                
                print(f"   ...Download complete. File size: {downloaded_size / (1024*1024):.2f} MB")
        
        # Unzip the file
        print(f"6. Unzipping file: {zip_path}")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(DOWNLOAD_DIR)
            
            print(f"✅ Download and extraction complete in: {DOWNLOAD_DIR}")
            print("\nNext step: Run `src/add_raster_features.py`")
            
        except zipfile.BadZipFile:
            print(f"❌ Error: The downloaded file is not a valid ZIP file")
            # Try to read the error message
            try:
                with open(zip_path, 'r', encoding='utf-8') as f:
                    error_content = f.read()
                    print(f"   Server response: {error_content}")
            except:
                print(f"   Could not read error response")
            
    else:
        print(f"❌ Cannot access download. Status: {head_response.status_code}")
        print(f"   Response headers: {head_response.headers}")
        
        # Try alternative download endpoints
        print("   Trying alternative download endpoints...")
        
        # Alternative 1: Direct download from catalog
        alt_download_url1 = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
        print(f"   Trying alternative 1: {alt_download_url1}")
        
        try:
            with requests.get(alt_download_url1, headers=auth_headers, stream=True) as r:
                r.raise_for_status()
                zip_path = os.path.join(DOWNLOAD_DIR, f"{product_name}.zip")
                
                print(f"   Alternative download successful! Downloading...")
                total_size = int(r.headers.get('content-length', 0))
                downloaded_size = 0
                
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            if total_size > 0:
                                percent = (downloaded_size / total_size) * 100
                                print(f"   Progress: {percent:.1f}%", end='\r')
                
                print(f"   ...Alternative download complete. File size: {downloaded_size / (1024*1024):.2f} MB")
                
                # Unzip
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(DOWNLOAD_DIR)
                
                print(f"✅ Download and extraction complete in: {DOWNLOAD_DIR}")
                print("\nNext step: Run `src/add_raster_features.py`")
                
        except Exception as alt_e:
            print(f"❌ Alternative 1 failed: {alt_e}")
            
            # Alternative 2: Newer download endpoint
            alt_download_url2 = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
            print(f"   Trying alternative 2: {alt_download_url2}")
            
            try:
                with requests.get(alt_download_url2, headers=auth_headers, stream=True) as r:
                    r.raise_for_status()
                    zip_path = os.path.join(DOWNLOAD_DIR, f"{product_name}.zip")
                    
                    print(f"   Alternative download 2 successful! Downloading...")
                    total_size = int(r.headers.get('content-length', 0))
                    downloaded_size = 0
                    
                    with open(zip_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded_size += len(chunk)
                                if total_size > 0:
                                    percent = (downloaded_size / total_size) * 100
                                    print(f"   Progress: {percent:.1f}%", end='\r')
                    
                    print(f"   ...Alternative download complete. File size: {downloaded_size / (1024*1024):.2f} MB")
                    
                    # Unzip
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(DOWNLOAD_DIR)
                    
                    print(f"✅ Download and extraction complete in: {DOWNLOAD_DIR}")
                    print("\nNext step: Run `src/add_raster_features.py`")
                    
            except Exception as alt_e2:
                print(f"❌ All download attempts failed:")
                print(f"   Alternative 2 error: {alt_e2}")
                print("   Please check:")
                print("   - Your internet connection")
                print("   - That the product is available for download")
                print("   - Your account permissions")
                exit(1)

except requests.exceptions.HTTPError as e:
    print(f"❌ HTTP Error during download: {e}")
    if e.response.status_code == 422:
        print("   This usually means the product format is not supported for direct download.")
        print("   Trying alternative download approach...")
        
        # Alternative: Use the product download endpoint directly
        alt_download_url = f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
        print(f"   Trying alternative URL: {alt_download_url}")
        
        try:
            with requests.get(alt_download_url, headers=auth_headers, stream=True) as r:
                r.raise_for_status()
                zip_path = os.path.join(DOWNLOAD_DIR, f"{product_name}.zip")
                
                print(f"   Alternative download successful! Downloading...")
                total_size = int(r.headers.get('content-length', 0))
                downloaded_size = 0
                
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            if total_size > 0:
                                percent = (downloaded_size / total_size) * 100
                                print(f"   Progress: {percent:.1f}%", end='\r')
                
                print(f"   ...Alternative download complete. File size: {downloaded_size / (1024*1024):.2f} MB")
                
                # Unzip
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(DOWNLOAD_DIR)
                
                print(f"✅ Download and extraction complete in: {DOWNLOAD_DIR}")
                print("\nNext step: Run `src/add_raster_features.py`")
                
        except Exception as alt_e:
            print(f"❌ Alternative download also failed: {alt_e}")

except Exception as e:
    print(f"❌ FAILED during download or unzip: {e}")
    exit(1)
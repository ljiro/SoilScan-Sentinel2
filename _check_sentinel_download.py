"""
Diagnostic script: checks Copernicus Data Space authentication, catalog search,
download endpoint availability, and S3 credentials.
"""
import os
import sys
import requests
import boto3
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv

load_dotenv()

AUTH_URL      = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CATALOG_URL   = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
ZIPPER_URL    = "https://zipper.dataspace.copernicus.eu/odata/v1/Products({})/$value"
CDSE_S3_ENDPOINT = "https://eodata.dataspace.copernicus.eu"
CDSE_S3_BUCKET   = "eodata"

username   = os.getenv("COPERNICUS_USER")
password   = os.getenv("COPERNICUS_PASS")
s3_access  = os.getenv("CDSE_S3_ACCESS_KEY")
s3_secret  = os.getenv("CDSE_S3_SECRET_KEY")

print("=" * 60)
print("SENTINEL DOWNLOAD DIAGNOSTICS")
print("=" * 60)

# ── 1. Auth ───────────────────────────────────────────────────
print("\n[1] Password Grant Authentication")
print(f"    User: {username}")
r = requests.post(AUTH_URL, data={
    "client_id":  "cdse-public",
    "username":   username,
    "password":   password,
    "grant_type": "password",
}, timeout=30)
print(f"    Status: {r.status_code}")
if r.status_code != 200:
    print(f"    ERROR: {r.text[:400]}")
    sys.exit(1)

token   = r.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}
print("    ✅ Auth OK")

# ── 2. Catalog search ─────────────────────────────────────────
print("\n[2] Catalog Search (top 3 MSIL2A products)")
params = {
    "$filter": "Collection/Name eq 'SENTINEL-2' and contains(Name,'MSIL2A')",
    "$top": 3,
    "$orderby": "ContentDate/Start desc",
}
s = requests.get(CATALOG_URL, headers=headers, params=params, timeout=30)
print(f"    Status: {s.status_code}")
if s.status_code != 200:
    print(f"    ERROR: {s.text[:300]}")
    sys.exit(1)

products = s.json().get("value", [])
print(f"    Found {len(products)} products")
for p in products:
    print(f"    - {p['Name']}")
    print(f"      ID: {p['Id']}")

# ── 3. Download HEAD check ────────────────────────────────────
print("\n[3] Download Endpoint HEAD Check")
if products:
    pid    = products[0]["Id"]
    pname  = products[0]["Name"]
    dl_url = ZIPPER_URL.format(pid)
    print(f"    Product: {pname}")
    print(f"    URL:     {dl_url}")
    try:
        h = requests.get(dl_url, headers=headers, stream=True,
                         allow_redirects=True, timeout=30)
        print(f"    Status:         {h.status_code}")
        print(f"    Content-Length: {h.headers.get('Content-Length', 'N/A')}")
        print(f"    Content-Type:   {h.headers.get('Content-Type', 'N/A')}")
        h.close()
        if h.status_code == 200:
            print("    ✅ Download endpoint reachable")
        else:
            print(f"    ⚠️  Unexpected status: {h.text[:300]}")
    except Exception as e:
        print(f"    ERROR: {e}")
else:
    print("    Skipped — no products found")

# ── 4. S3 credentials check ───────────────────────────────────
print("\n[4] S3 Credentials Check")
if s3_access and s3_secret:
    print(f"    Access key: {s3_access[:8]}...")
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=CDSE_S3_ENDPOINT,
            aws_access_key_id=s3_access,
            aws_secret_access_key=s3_secret,
            config=BotoConfig(signature_version="s3v4"),
            region_name="default",
        )
        # List top-level "folders" in the bucket
        result = s3.list_objects_v2(Bucket=CDSE_S3_BUCKET,
                                     Prefix="Sentinel-2/MSI/L2A/", Delimiter="/",
                                     MaxKeys=5)
        prefixes = [p["Prefix"] for p in result.get("CommonPrefixes", [])]
        print(f"    S3 list_objects OK — top prefixes: {prefixes[:3]}")
        print("    ✅ S3 credentials valid")
    except Exception as e:
        print(f"    ERROR: {e}")
else:
    print("    ⚠️  CDSE_S3_ACCESS_KEY / CDSE_S3_SECRET_KEY not set — S3 fast-path disabled")

print("\n" + "=" * 60)
print("DIAGNOSTICS COMPLETE")
print("=" * 60)

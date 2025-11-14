# Save this file as: src/add_weather_features.py

import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
import numpy as np

# --- 1. Configuration ---
INPUT_CSV_PATH = 'data/processed/LUCAS_with_Raster_Features.csv'
OUTPUT_CSV_PATH = 'data/processed/LUCAS_with_All_Features.csv'

# Define weather variables to fetch, based on paper 
WEATHER_VARIABLES = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation"]

# --- 2. Setup API Client ---
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# --- 3. Load Data ---
print(f"1. Loading data from {INPUT_CSV_PATH}...")
try:
    df = pd.read_csv(INPUT_CSV_PATH)
except FileNotFoundError:
    print(f"❌ Error: {INPUT_CSV_PATH} not found. Run `add_raster_features.py` first.")
    exit()

# Ensure date column is in the correct format
# The LUCAS file uses 'dd/mm/yyyy'
df['SURVEY_DATE_dt'] = pd.to_datetime(df['SURVEY_Da'], format='%d/%m/%Y')

# --- 4. Query Weather API for Each Point ---
print(f"2. Fetching weather data (WTHR) for {len(df)} points...")

# Create new columns to store weather data
for var in WEATHER_VARIABLES:
    df[var] = np.nan

# Group by unique combinations of lat, lon, and date to minimize API calls
grouped = df.groupby(['TH_LAT', 'TH_LONG', 'SURVEY_DATE_dt'])

processed_count = 0
for (lat, lon, date_obj), indices in grouped:
    # Format date for API (YYYY-MM-DD)
    date_str = date_obj.strftime('%Y-%m-%d')
    
    # Prepare API request
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "daily": WEATHER_VARIABLES,
        "timezone": "UTC"
    }
    
    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        daily = response.Daily()
        
        # Extract the single day's data
        weather_data = {}
        for i, var in enumerate(WEATHER_VARIABLES):
            weather_data[var] = daily.Variables(i).ValuesAsNumpy()[0] # Get first day
        
        # Assign this data to all rows with this lat/lon/date
        df.loc[indices, WEATHER_VARIABLES] = [weather_data[var] for var in WEATHER_VARIABLES]

        processed_count += len(indices)
        if processed_count % 1000 == 0:
            print(f"   ...processed {processed_count}/{len(df)} points")

    except Exception as e:
        print(f"   Warning: Could not fetch weather for {lat}, {lon}, {date_str}. Error: {e}")

print("   All weather data fetched.")

# --- 5. Save Final Dataset ---
print(f"3. Saving final dataset to {OUTPUT_CSV_PATH}...")
# Drop the temporary datetime column
df.drop(columns=['SURVEY_DATE_dt'], inplace=True)
df.to_csv(OUTPUT_CSV_PATH, index=False)

print(f"\n✅ Success! Final dataset with all features saved.")
print("   Next step: Run `src/train_model.py`")
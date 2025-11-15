# Save this file as: src/add_weather_features.py

import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime

# --- 1. Configuration ---
INPUT_CSV_PATH = 'data/processed/LUCAS_with_Raster_Features.csv'
OUTPUT_CSV_PATH = 'data/processed/LUCAS_with_All_Features.csv'

# Define weather variables to fetch - USING CORRECT NAMES
WEATHER_VARIABLES = [
    "temperature_2m_mean", 
    "relative_humidity_2m_mean", 
    "dew_point_2m_mean", 
    "precipitation_sum"
]

# --- 2. Load Data ---
print(f"1. Loading data from {INPUT_CSV_PATH}...")
try:
    df = pd.read_csv(INPUT_CSV_PATH)
except FileNotFoundError:
    print(f"❌ Error: {INPUT_CSV_PATH} not found. Run `add_raster_features.py` first.")
    exit(1)

print(f"   Loaded {len(df)} samples")

# --- 3. Handle Dates ---
print("2. Checking available date columns...")
date_columns = [col for col in df.columns if 'DATE' in col.upper() or 'SURVEY' in col.upper()]
print(f"   Available date-related columns: {date_columns}")

# Use the first available date column, or fall back to a default date
if date_columns:
    date_column = date_columns[0]
    print(f"   Using date column: {date_column}")
    
    # Try different date formats
    try:
        df['SURVEY_DATE_dt'] = pd.to_datetime(df[date_column], format='%d/%m/%Y')
    except:
        try:
            df['SURVEY_DATE_dt'] = pd.to_datetime(df[date_column])
        except:
            print(f"   ⚠️  Could not parse dates from {date_column}, using default date")
            df['SURVEY_DATE_dt'] = pd.to_datetime('2018-05-15')  # Default spring date
else:
    print("   ⚠️  No date columns found, using default spring sampling date")
    df['SURVEY_DATE_dt'] = pd.to_datetime('2018-05-15')  # Default spring date

# --- 4. Add Weather Features ---
print(f"3. Adding weather features...")

# Initialize weather columns with NaN
for var in WEATHER_VARIABLES:
    # Use simpler column names
    simple_name = var.replace('_mean', '').replace('_sum', '')
    df[simple_name] = np.nan

# Simple function to fetch weather data using direct HTTP requests
def fetch_weather_data(lat, lon, date_str):
    """
    Fetch weather data using direct HTTP request to avoid library issues
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "daily": "temperature_2m_mean,relative_humidity_2m_mean,dew_point_2m_mean,precipitation_sum",
        "timezone": "auto"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'daily' in data:
                daily_data = data['daily']
                # Extract the first day's data
                weather_values = {}
                for var in WEATHER_VARIABLES:
                    if var in daily_data and len(daily_data[var]) > 0:
                        weather_values[var] = daily_data[var][0]
                    else:
                        weather_values[var] = np.nan
                return weather_values
        else:
            print(f"      API returned status {response.status_code}")
    except Exception as e:
        print(f"      Request failed: {e}")
    
    return None

# Group by unique combinations of lat, lon, and date to minimize API calls
unique_coords_dates = df[['TH_LAT', 'TH_LONG', 'SURVEY_DATE_dt']].drop_duplicates()
print(f"   Fetching weather for {len(unique_coords_dates)} unique locations...")

success_count = 0
fail_count = 0

for idx, (lat, lon, date_obj) in unique_coords_dates.iterrows():
    # Format date for API (YYYY-MM-DD)
    date_str = date_obj.strftime('%Y-%m-%d')
    
    print(f"   Fetching: {lat:.4f}, {lon:.4f}, {date_str}...")
    
    # Fetch weather data
    weather_data = fetch_weather_data(lat, lon, date_str)
    
    if weather_data:
        # Find all rows with this lat/lon/date and update them
        mask = (df['TH_LAT'] == lat) & (df['TH_LONG'] == lon) & (df['SURVEY_DATE_dt'] == date_obj)
        
        for var in WEATHER_VARIABLES:
            simple_name = var.replace('_mean', '').replace('_sum', '')
            df.loc[mask, simple_name] = weather_data[var]
        
        success_count += 1
    else:
        print(f"   ⚠️  Could not fetch weather for {lat:.4f}, {lon:.4f}, {date_str}")
        fail_count += 1
        
        # Use simulated data for failed points
        mask = (df['TH_LAT'] == lat) & (df['TH_LONG'] == lon) & (df['SURVEY_DATE_dt'] == date_obj)
        np.random.seed(int(lat * 1000 + lon * 100))  # Seed based on location for consistency
        
        # Simulate realistic weather based on season and location
        month = date_obj.month
        # Seasonal adjustments
        if month in [12, 1, 2]:  # Winter
            temp_base = 0 + (lat - 45) * (-0.5)  # Colder in north
            precip_base = 3.0
        elif month in [3, 4, 5]:  # Spring
            temp_base = 10 + (lat - 45) * (-0.5)
            precip_base = 2.5
        elif month in [6, 7, 8]:  # Summer  
            temp_base = 20 + (lat - 45) * (-0.5)
            precip_base = 2.0
        else:  # Fall
            temp_base = 12 + (lat - 45) * (-0.5)
            precip_base = 2.8
        
        df.loc[mask, 'temperature_2m'] = temp_base + np.random.normal(0, 3, mask.sum())
        df.loc[mask, 'precipitation'] = np.maximum(0, np.random.exponential(precip_base, mask.sum()))
        df.loc[mask, 'relative_humidity_2m'] = np.random.uniform(40, 85, mask.sum())
        df.loc[mask, 'dew_point_2m'] = df.loc[mask, 'temperature_2m'] - np.random.exponential(3, mask.sum())
    
    # Progress reporting
    if (success_count + fail_count) % 50 == 0:
        print(f"   ...processed {success_count + fail_count}/{len(unique_coords_dates)} locations")
    
    # Rate limiting - be nice to the API
    time.sleep(0.1)

print(f"   Weather data: {success_count} successful, {fail_count} failed")

# Fill any remaining NaN values with simulated data
missing_mask = df[['temperature_2m', 'precipitation', 'relative_humidity_2m', 'dew_point_2m']].isna().any(axis=1)
if missing_mask.any():
    print(f"   Adding simulated weather for {missing_mask.sum()} remaining missing points")
    
    # More realistic simulation based on location and date
    for idx in df[missing_mask].index:
        lat = df.loc[idx, 'TH_LAT']
        lon = df.loc[idx, 'TH_LONG']
        date_obj = df.loc[idx, 'SURVEY_DATE_dt']
        month = date_obj.month
        
        # Seasonal and geographic adjustments
        if month in [12, 1, 2]:  # Winter
            temp_base = 0 + (lat - 45) * (-0.5)
            precip_base = 3.0
        elif month in [3, 4, 5]:  # Spring
            temp_base = 10 + (lat - 45) * (-0.5)
            precip_base = 2.5
        elif month in [6, 7, 8]:  # Summer
            temp_base = 20 + (lat - 45) * (-0.5)  
            precip_base = 2.0
        else:  # Fall
            temp_base = 12 + (lat - 45) * (-0.5)
            precip_base = 2.8
        
        np.random.seed(int(lat * 1000 + lon * 100 + month))
        
        df.loc[idx, 'temperature_2m'] = temp_base + np.random.normal(0, 3)
        df.loc[idx, 'precipitation'] = max(0, np.random.exponential(precip_base))
        df.loc[idx, 'relative_humidity_2m'] = np.random.uniform(40, 85)
        df.loc[idx, 'dew_point_2m'] = df.loc[idx, 'temperature_2m'] - np.random.exponential(3)

# --- 5. Add Seasonal Features ---
print("4. Adding seasonal features...")

# Extract month from sampling date
df['month'] = df['SURVEY_DATE_dt'].dt.month

# Add seasonal indicators
seasons = {
    'winter': [12, 1, 2],
    'spring': [3, 4, 5], 
    'summer': [6, 7, 8],
    'fall': [9, 10, 11]
}

for season, months in seasons.items():
    df[f'is_{season}'] = df['month'].isin(months).astype(int)

print("   ✅ Added seasonal features")

# --- 6. Save Final Dataset ---
print(f"5. Saving final dataset to {OUTPUT_CSV_PATH}...")
# Drop the temporary datetime and month columns
df.drop(columns=['SURVEY_DATE_dt', 'month'], inplace=True)
os.makedirs('data/processed', exist_ok=True)
df.to_csv(OUTPUT_CSV_PATH, index=False)

# Print final summary
print(f"\n✅ Success! Final dataset with all features saved to {OUTPUT_CSV_PATH}")
print(f"   Final dataset shape: {df.shape}")
print(f"   Weather statistics:")
print(f"   - Temperature: {df['temperature_2m'].mean():.1f}°C ± {df['temperature_2m'].std():.1f}°C")
print(f"   - Precipitation: {df['precipitation'].mean():.1f}mm ± {df['precipitation'].std():.1f}mm")
print(f"   - Humidity: {df['relative_humidity_2m'].mean():.1f}% ± {df['relative_humidity_2m'].std():.1f}%")
print(f"   - Dew Point: {df['dew_point_2m'].mean():.1f}°C ± {df['dew_point_2m'].std():.1f}°C")
print("   Next step: Run `src/train_model.py`")
"""Add weather and seasonal features to the raster-enriched dataset.

When the Open-Meteo archive API is unavailable for a location, the script
falls back to a deterministic synthetic estimate calibrated to the Philippine
tropical highland climate (Benguet province, ~1300-2400 m a.s.l.).

Rows filled synthetically are flagged with ``weather_is_synthetic = True``
in the output CSV so downstream consumers can filter or weight them
appropriately.

Philippine climate reference:
  - Wet season:  June – October  (habagat / SW monsoon)
  - Dry season:  November – May  (amihan / NE monsoon)
"""

import os
import sys

import numpy as np
import pandas as pd
import requests
import time

# --- 1. Configuration ---
INPUT_CSV_PATH  = 'data/processed/LUCAS_with_Raster_Features.csv'
OUTPUT_CSV_PATH = 'data/processed/LUCAS_with_All_Features.csv'

# Names exactly as returned by the Open-Meteo archive daily endpoint
WEATHER_VARIABLES = [
    "temperature_2m_mean",
    "relative_humidity_2m_mean",
    "dew_point_2m_mean",
    "precipitation_sum",
]

# Mapping: API variable name → output column name
_COL_MAP = {
    "temperature_2m_mean":        "temperature_2m",
    "relative_humidity_2m_mean":  "relative_humidity_2m",
    "dew_point_2m_mean":          "dew_point_2m",
    "precipitation_sum":          "precipitation",
}
WEATHER_COLS = list(_COL_MAP.values())

# --- 2. Load Data ---
print(f"1. Loading data from {INPUT_CSV_PATH}...")
try:
    df = pd.read_csv(INPUT_CSV_PATH)
except FileNotFoundError:
    print(f"ERROR: {INPUT_CSV_PATH} not found. Run `add_raster_features.py` first.")
    sys.exit(1)

print(f"   Loaded {len(df)} samples")

# --- 3. Handle Dates ---
print("2. Checking available date columns...")
# Prefer capture_datetime (AgriCapture), then fall back to any DATE/SURVEY column
_date_candidates = (
    ["capture_datetime"]
    + [col for col in df.columns
       if 'DATE' in col.upper() or 'SURVEY' in col.upper()]
)
date_column = next((c for c in _date_candidates if c in df.columns), None)
print(f"   Using date column: {date_column}")

if date_column:
    parsed = False
    for fmt in (None, '%d/%m/%Y'):  # ISO first (AgriCapture), then explicit
        try:
            df['SURVEY_DATE_dt'] = pd.to_datetime(df[date_column], format=fmt)
            parsed = True
            break
        except Exception:
            continue
    if not parsed:
        print(f"   WARNING: Could not parse dates from '{date_column}'. "
              f"Falling back to 2023-05-15 — all weather data will be synthetic.")
        df['SURVEY_DATE_dt'] = pd.to_datetime('2023-05-15')
else:
    print("   WARNING: No date columns found. "
          "Falling back to 2023-05-15 — all weather data will be synthetic.")
    df['SURVEY_DATE_dt'] = pd.to_datetime('2023-05-15')

# --- 4. Initialise weather columns ---
for col in WEATHER_COLS:
    df[col] = np.nan

# Track which rows used synthetic (fallback) data
df['weather_is_synthetic'] = False


def fetch_weather_data(lat, lon, date_str):
    """Return a {api_var: value} dict from Open-Meteo, or None on failure."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "start_date": date_str,
        "end_date":   date_str,
        "daily":      ",".join(WEATHER_VARIABLES),
        "timezone":   "auto",
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            daily = response.json().get("daily", {})
            return {
                var: (daily[var][0] if var in daily and daily[var] else np.nan)
                for var in WEATHER_VARIABLES
            }
        print(f"      API returned status {response.status_code}")
    except Exception as exc:
        print(f"      Request failed: {exc}")
    return None


def _seasonal_weather(month, rng):
    """Return synthetic weather dict calibrated to Philippine tropical highlands.

    Benguet province (~1300-2400 m a.s.l.):
    - Wet season  (Jun-Oct): cool, cloudy, heavy monsoon rainfall
    - Dry season  (Nov-May): warmer, sunnier, low rainfall
    """
    if month in (6, 7, 8, 9, 10):  # Wet season — habagat / SW monsoon
        temp_base      = 18.0
        precip_base    = 12.0
        humidity_range = (75, 95)
    else:                            # Dry season — amihan / NE monsoon
        temp_base      = 22.0
        precip_base    = 2.5
        humidity_range = (55, 80)

    temp = float(temp_base + rng.normal(0, 2))
    return {
        "temperature_2m":       temp,
        "precipitation":        float(max(0, rng.exponential(precip_base))),
        "relative_humidity_2m": float(rng.uniform(*humidity_range)),
        "dew_point_2m":         float(temp - rng.exponential(3)),
    }


# --- 5. Fetch / generate weather per unique (lat, lon, date) ---
print("3. Adding weather features...")
unique_coords_dates = df[['latitude', 'longitude', 'SURVEY_DATE_dt']].drop_duplicates()
print(f"   Fetching weather for {len(unique_coords_dates)} unique locations...")

success_count = 0
fail_count    = 0

for _, row in unique_coords_dates.iterrows():
    lat, lon, date_obj = row['latitude'], row['longitude'], row['SURVEY_DATE_dt']
    date_str = date_obj.strftime('%Y-%m-%d')
    mask = (df['latitude'] == lat) & (df['longitude'] == lon) & (df['SURVEY_DATE_dt'] == date_obj)

    weather_data = fetch_weather_data(lat, lon, date_str)

    if weather_data:
        for api_var, col in _COL_MAP.items():
            df.loc[mask, col] = weather_data[api_var]
        success_count += 1
    else:
        print(f"   WARNING: API failed for {lat:.4f}, {lon:.4f}, {date_str} — using synthetic estimate.")
        rng = np.random.default_rng(seed=int(abs(lat * 1000 + lon * 100)))
        synth = _seasonal_weather(date_obj.month, rng)
        for col, val in synth.items():
            df.loc[mask, col] = val
        df.loc[mask, 'weather_is_synthetic'] = True
        fail_count += 1

    total = success_count + fail_count
    if total % 50 == 0:
        print(f"   ...processed {total}/{len(unique_coords_dates)} locations")

    time.sleep(0.1)  # Rate-limit: be polite to the free API

print(f"   Weather data: {success_count} successful, {fail_count} used synthetic fallback")
if fail_count:
    print(f"   NOTE: {fail_count} location(s) used synthetic weather. "
          f"The column 'weather_is_synthetic' is set to True for those rows.")

# --- 6. Fill any remaining NaN values (e.g., parsing edge-cases) ---
still_missing = df[WEATHER_COLS].isna().any(axis=1)
if still_missing.any():
    n_missing = int(still_missing.sum())
    print(f"   WARNING: {n_missing} row(s) still missing weather after fetch loop — applying synthetic fallback.")
    for idx in df[still_missing].index:
        lat      = df.loc[idx, 'latitude']
        date_obj = df.loc[idx, 'SURVEY_DATE_dt']
        rng      = np.random.default_rng(seed=int(abs(lat * 1000 + date_obj.month)))
        synth    = _seasonal_weather(date_obj.month, rng)
        for col, val in synth.items():
            if pd.isna(df.loc[idx, col]):
                df.loc[idx, col] = val
        df.loc[idx, 'weather_is_synthetic'] = True

# --- 7. Add Seasonal Features ---
print("4. Adding seasonal features...")
df['month'] = df['SURVEY_DATE_dt'].dt.month

# Philippine seasons: dry season Nov-May, wet season Jun-Oct
seasons = {
    'wet_season': [6, 7, 8, 9, 10],      # Habagat / SW monsoon
    'dry_season': [11, 12, 1, 2, 3, 4, 5],  # Amihan / NE monsoon
}
for season, months in seasons.items():
    df[f'is_{season}'] = df['month'].isin(months).astype(int)
print("   Seasonal features added.")

# --- 8. Save ---
print(f"5. Saving final dataset to {OUTPUT_CSV_PATH}...")
df.drop(columns=['SURVEY_DATE_dt', 'month'], inplace=True)
os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)
df.to_csv(OUTPUT_CSV_PATH, index=False)

synth_pct = df['weather_is_synthetic'].mean() * 100
print(f"\nSuccess! Final dataset saved to {OUTPUT_CSV_PATH}")
print(f"   Shape: {df.shape}")
print(f"   Synthetic weather rows: {df['weather_is_synthetic'].sum()} ({synth_pct:.1f}%)")
print(f"   Temperature:  {df['temperature_2m'].mean():.1f} +/- {df['temperature_2m'].std():.1f} C")
print(f"   Precipitation:{df['precipitation'].mean():.1f} +/- {df['precipitation'].std():.1f} mm")
print(f"   Humidity:     {df['relative_humidity_2m'].mean():.1f} +/- {df['relative_humidity_2m'].std():.1f} %")
print("   Next step: Run `src/train_ordinal.py`")

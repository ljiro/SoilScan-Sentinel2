# Save this file as: src/train_model.py

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import numpy as np
import re

# --- 1. Configuration ---
FINAL_DATASET_PATH = 'data/processed/LUCAS_with_All_Features.csv'
MODEL_TARGET = 'N' # ⚠️ Choose what to predict: 'N', 'P', or 'K' [cite: 95]

# --- 2. Load the Final Merged Data ---
print(f"1. Loading final dataset from {FINAL_DATASET_PATH}...")
try:
    df_model = pd.read_csv(FINAL_DATASET_PATH)
except FileNotFoundError:
    print(f"❌ Error: {FINAL_DATASET_PATH} not found.")
    print("Please run `src/add_weather_features.py` first.")
    exit()

# --- 3. Feature Engineering (VIs) ---
print("2. Calculating Vegetation Indices (VIs) from center pixel...")
# We use the 5th pixel (index 4) as it's the center of the 3x3 grid [cite: 209]
center_pixel_suffix = '_5'

try:
    # (NIR - Red) / (NIR + Red) 
    df_model['NDVI'] = (df_model[f'B08{center_pixel_suffix}'] - df_model[f'B04{center_pixel_suffix}']) / \
                       (df_model[f'B08{center_pixel_suffix}'] + df_model[f'B04{center_pixel_suffix}'])
    # (NIR - Red Edge 1) / (NIR + Red Edge 1)
    df_model['NDRE'] = (df_model[f'B08{center_pixel_suffix}'] - df_model[f'B05{center_pixel_suffix}']) / \
                       (df_model[f'B08{center_pixel_suffix}'] + df_model[f'B05{center_pixel_suffix}'])
except KeyError as e:
    print(f"   Warning: Could not calculate VI, missing center pixel band: {e}")

# Replace inf/-inf values (from division by zero) with NaN
df_model.replace([np.inf, -np.inf], np.nan, inplace=True)
# Drop any rows that have NaN values in our target or VIs
df_model.dropna(subset=[MODEL_TARGET, 'NDVI', 'NDRE'], inplace=True)

# --- 4. Define All Features ---
print(f"3. Preparing feature set for {MODEL_TARGET} prediction...")

# 1. All Sentinel 3x3 neighbor pixels (SURR) 
sentinel_cols = [col for col in df_model.columns if re.match(r'B(0[1-9]|1[0-2]|8A)_\d', col)]
# 2. All Crop Yield columns (CRY) 
yield_cols = [col for col in df_model.columns if col.startswith('yld_')]
# 3. All Weather columns (WTHR) 
weather_cols = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation"]
# 4. Our new VIs 
vi_cols = ['NDVI', 'NDRE']

features_present = sentinel_cols + yield_cols + weather_cols + vi_cols
# Ensure all selected features actually exist and have no NaNs
features_present = [f for f in features_present if f in df_model.columns]
features_present = df_model[features_present].dropna(axis=1).columns.tolist()

if not features_present:
    print("❌ Error: No features available for modeling.")
    exit()
    
print(f"   Using {len(features_present)} total features (SURR+WTHR+CRY+VIs).")

X = df_model[features_present]
y = df_model[MODEL_TARGET]

# --- 5. Train Model ---
# Use a Single Split (80:20) as the baseline [cite: 330]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"4. Training Random Forest model...")
model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)

# --- 6. Evaluate Model ---
print("5. Evaluating model...")
preds = model.predict(X_test)
rmse = np.sqrt(mean_squared_error(y_test, preds)) # [cite: 49]

print(f"\n✅ Model training complete.")
print(f"   Target: {MODEL_TARGET}")
print(f"   Test RMSE: {rmse:.4f} (units of {MODEL_TARGET})")

# [cite: 488]
importance = pd.Series(model.feature_importances_, index=features_present).sort_values(ascending=False)
print("\nTop 20 Most Important Features:")
print(importance.head(20))
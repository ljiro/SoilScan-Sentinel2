"""
LEGACY REGRESSION SCRIPT — European AgroLens workflow. DO NOT USE for the Philippine pipeline.

This script trains a Random Forest *regressor* on continuous N/P/K values from the
LUCAS dataset, mirroring the original cvims/AgroLens approach.

For the Philippine pipeline (SoilScan-Sentinel2) use:
    src/train_ordinal.py

That script performs ordinal *classification* (Low / Medium / High for NPK; 11-class
CPR scale for pH) with spatial cross-validation (GroupKFold by barangay) and
class-imbalance weighting — the correct framing for Rapid Soil Test Kit (STK) data
collected in Benguet, Philippines.

--- Improvements applied to this legacy script ---
- Trained model is now saved to outputs/models/ with joblib
- Vegetation indices use shared compute_vegetation_indices() from utils.py
- Replaced bare exit() with sys.exit()
- Warns when synthetic weather rows are present in the dataset
"""

import os
import sys
import re

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

from utils import compute_vegetation_indices

# --- 1. Configuration ---
FINAL_DATASET_PATH = 'data/external/final_merged_data_cleaned.csv'
MODEL_TARGET       = 'n'   # Choose target: 'n', 'p', or 'k'
MODEL_OUTPUT_DIR   = 'outputs/models'

# --- 2. Load the Final Merged Data ---
print(f"1. Loading final dataset from {FINAL_DATASET_PATH}...")
try:
    df_model = pd.read_csv(FINAL_DATASET_PATH)
except FileNotFoundError:
    print(f"ERROR: {FINAL_DATASET_PATH} not found.")
    print("Please run `src/add_weather_features.py` first.")
    sys.exit(1)

# Warn if the dataset contains a significant fraction of synthetic weather
if 'weather_is_synthetic' in df_model.columns:
    synth_frac = df_model['weather_is_synthetic'].mean()
    if synth_frac > 0:
        print(f"   WARNING: {synth_frac:.1%} of rows have synthetic weather data "
              f"(flagged by 'weather_is_synthetic'). Interpret results accordingly.")

# --- 3. Feature Engineering (VIs) ---
print("2. Calculating Vegetation Indices (VIs) from center pixel...")
# Pixel 5 (index 4) is the center of the 3x3 grid
vi_dict = compute_vegetation_indices(df_model, center_suffix='_5')
if vi_dict:
    vi_df = pd.DataFrame(vi_dict, index=df_model.index)
    df_model = pd.concat([df_model, vi_df], axis=1)
    print(f"   Calculated: {list(vi_dict.keys())}")
else:
    print("   WARNING: No center-pixel band columns found — skipping VIs.")

# Replace inf/-inf (from division by zero) with NaN
df_model.replace([np.inf, -np.inf], np.nan, inplace=True)

# Drop rows with NaN in the target; keep rows with NaN VIs by excluding only if all are NaN
vi_cols = list(vi_dict.keys())
df_model.dropna(subset=[MODEL_TARGET], inplace=True)

# --- 4. Define All Features ---
print(f"3. Preparing feature set for {MODEL_TARGET} prediction...")

sentinel_cols = [col for col in df_model.columns if re.match(r'B(0[1-9]|1[0-2]|8A)_\d', col)]
yield_cols    = [col for col in df_model.columns if col.startswith('yld_')]
weather_cols  = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation"]

features_present = sentinel_cols + yield_cols + weather_cols + vi_cols
features_present = [f for f in features_present if f in df_model.columns]
# Drop feature columns that are entirely NaN
features_present = df_model[features_present].dropna(axis=1, how='all').columns.tolist()

if not features_present:
    print("ERROR: No features available for modeling.")
    sys.exit(1)

print(f"   Using {len(features_present)} total features (SURR+WTHR+CRY+VIs).")

X = df_model[features_present].fillna(df_model[features_present].median())
y = df_model[MODEL_TARGET]

# --- 5. Train Model ---
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print(f"4. Training Random Forest model (target={MODEL_TARGET})...")
model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)

# --- 6. Evaluate Model ---
print("5. Evaluating model...")
preds = model.predict(X_test)
rmse  = np.sqrt(mean_squared_error(y_test, preds))

print(f"\nModel training complete.")
print(f"   Target:    {MODEL_TARGET}")
print(f"   Test RMSE: {rmse:.4f} (units of {MODEL_TARGET})")

importance = (
    pd.Series(model.feature_importances_, index=features_present)
    .sort_values(ascending=False)
)
print("\nTop 20 Most Important Features:")
print(importance.head(20))

# --- 7. Save Model ---
os.makedirs(MODEL_OUTPUT_DIR, exist_ok=True)
model_path = os.path.join(MODEL_OUTPUT_DIR, f"rf_{MODEL_TARGET.lower()}.joblib")
joblib.dump(model, model_path)
print(f"\nModel saved to {model_path}")
print(f"   Load with: model = joblib.load('{model_path}')")

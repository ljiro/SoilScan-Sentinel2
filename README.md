# SoilScan-Sentinel2

Soil nutrient prediction for Philippine highland farms using Sentinel-2 satellite imagery and machine learning.

---

## Overview

**SoilScan-Sentinel2** is adapted from [cvims/AgroLens](https://github.com/cvims/AgroLens) and repurposed for the **Philippines**. Instead of predicting continuous nutrient values from European LUCAS lab data, it predicts **ordinal nutrient classes** (Low / Medium / High for Nitrogen, Phosphorus, and Potassium; 11-class CPR scale for pH) from **Rapid Soil Test Kit (STK)** results collected in the field.

The study area is the **Benguet highlands** (1,300–2,400 m above sea level), where smallholder vegetable farmers practice intensive agriculture. Ground truth data is collected via the **AgriCapture** mobile app (GPS coordinates, microclimate readings, and STK colour-chart results). Sentinel-2 L2A spectral bands are fetched from the **Copernicus Data Space Ecosystem (CDSE)** and paired with each field observation.

---

## Pipeline Steps

```
1. Field data collection
   AgriCapture app  →  GPS, temperature, humidity, altitude, crop type, STK results
   Saved to: data/external/combined_field_data.csv

2. Sentinel-2 band extraction
   src/data_fetcher_copernicus.py
   Searches CDSE for cloud-free L2A tiles, downloads via HTTP (with resume) or S3,
   extracts 12 spectral bands + 10 vegetation indices per sample point.
   Output: data/processed/final_dataset_with_stk.csv

3. Feature engineering (26-dimensional feature vector)
   ┌─ 12 spectral bands  (B01–B12, B8A)
   ├─ 10 vegetation indices  (NDVI, EVI, SAVI, NDRE, GNDVI, NDWI, NDMI, CRI, SIPI, PSRI)
   ├─  3 microclimate features  (temperature, relative humidity, altitude)
   └─  1 crop type feature

4. Model training with spatial cross-validation
   src/train_ordinal.py
   XGBoost, Random Forest, SVM trained with GroupKFold (grouped by barangay).
   Class-imbalance handled via compute_sample_weight.
   Outputs: confusion matrices, feature importance plots, metrics CSV.
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your credentials:

| Variable | Description |
|---|---|
| `COPERNICUS_USER` | Your Copernicus Data Space username (email) |
| `COPERNICUS_PASS` | Your Copernicus Data Space password |
| `CDSE_S3_ACCESS_KEY` | CDSE S3 access key (optional, 3–10× faster downloads) |
| `CDSE_S3_SECRET_KEY` | CDSE S3 secret key (optional) |

Generate S3 credentials at: **dataspace.copernicus.eu → User Settings → S3 Access → Generate**

---

## How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up environment variables
cp .env.example .env
# Edit .env with your credentials

# 3. Fetch Sentinel-2 data and build feature dataset
python src/data_fetcher_copernicus.py

# 4. Train ordinal classification models
python src/train_ordinal.py data/processed/final_dataset_with_stk.csv

# --- OR test the full pipeline with synthetic data ---
python src/generate_synthetic_data.py
python src/train_ordinal.py data/processed/synthetic_test.csv
```

---

## Project Structure

```
SoilScan-Sentinel2/
├── data/
│   ├── external/
│   │   └── combined_field_data.csv   # Field observations from AgriCapture
│   ├── raw/                          # Downloaded Sentinel-2 .SAFE tiles
│   └── processed/                    # Feature datasets ready for training
├── outputs/
│   ├── figures/                      # Confusion matrices, feature importance plots
│   ├── pubmat/                       # Publication-ready summary graphics
│   ├── metrics_summary.csv           # OA, Macro F1, Kappa, Ordinal MAE per model/target
│   └── feature_importances.csv       # Aggregated feature importance scores
├── src/
│   ├── data_fetcher_copernicus.py    # ✅ Main pipeline: search → download → extract features
│   ├── train_ordinal.py              # ✅ Ordinal classification training (XGBoost/RF/SVM)
│   ├── generate_synthetic_data.py    # ✅ Synthetic data generator for pipeline testing
│   ├── add_weather_features.py       # Weather feature enrichment via Open-Meteo API
│   ├── orchestrator.py               # Pipeline orchestration helper
│   ├── plot_pubmat.py                # Publication material figure generator
│   ├── export_pipeline_doc.py        # Pipeline documentation exporter
│   ├── get_cdse_token.py             # CDSE authentication helper
│   ├── data_fetcher.py               # Generic data fetch utilities
│   ├── data_acquisition.py           # ⚠️ LEGACY — European LUCAS workflow
│   ├── add_raster_features.py        # ⚠️ LEGACY — European LUCAS workflow
│   └── train_model.py                # ⚠️ LEGACY — Regression script (AgroLens)
├── .env.example                      # Template for environment variables
├── requirements.txt
└── README.md
```

---

## Models & Evaluation

Three classifier families are trained per nutrient target (N, P, K, pH):

| Model | Notes |
|---|---|
| **XGBoost** | Gradient-boosted trees; class weights via `scale_pos_weight` |
| **Random Forest** | Ensemble of decision trees; `compute_sample_weight` for balance |
| **SVM** | Radial-basis kernel with `StandardScaler` preprocessing |

### Evaluation Metrics

- **Overall Accuracy (OA)**
- **Macro F1-score** (treats all classes equally)
- **Cohen's Kappa** (corrects for chance agreement)
- **Ordinal MAE** (penalises predictions that are further from true class in ordinal order)

Cross-validation uses **GroupKFold** with barangay as the group variable to prevent spatial data leakage between folds.

---

## Acknowledgments

Based on [cvims/AgroLens](https://github.com/cvims/AgroLens). The original AgroLens framework provided the Copernicus API integration, Sentinel-2 band extraction logic, and model training scaffold. Key adaptations for the Philippine context include:

- Ordinal classification replacing regression
- STK ordinal labels replacing LUCAS continuous measurements
- Spatial cross-validation (GroupKFold by barangay) replacing random splits
- Philippine GPS coordinates and Copernicus password-grant authentication
- Tropical wet/dry season features replacing European seasonal indicators
- Microclimate features (temperature, humidity, altitude) from AgriCapture

# SoilScan-Sentinel2

Soil nutrient prediction for Philippine highland farms using Sentinel-2 satellite imagery, terrain features, and geospatial foundation model embeddings.

---

## Overview

**SoilScan-Sentinel2** predicts ordinal soil nutrient classes (Low / Medium / High for N, P, K; 11-class CPR scale for pH) from **Sentinel-2 L2A satellite imagery** paired with GPS field samples collected in the **Benguet highlands** (1,300–2,400 m ASL), Philippines.

Ground truth is collected via the **AgriCapture** mobile app (GPS, microclimate, Rapid Soil Test Kit colour-chart results). Sentinel-2 tiles are fetched from the **Copernicus Data Space Ecosystem (CDSE)** and spatially matched to each sample point.

A key finding from this project: for nutrient prediction via plant-stress spectral signatures (N, P, K), imagery must be **temporally matched to the growing season** — not collected post-harvest. Using growing-season (Oct–Nov 2025) tiles instead of post-harvest (Feb–Mar 2026) tiles improved P Kappa from 0.047 → **0.116**.

---

## Pipeline

```
1. Field data collection
   AgriCapture app → GPS, temperature, humidity, altitude, crop type, STK results
   Output: data/external/final_merged_data_cleaned.csv

2. Terrain feature extraction
   src/data_fetcher_copernicus.py  (run once, adds DEM-derived slope/aspect/elevation)
   Output: data/processed/field_data_with_terrain.csv

3. Sentinel-2 band extraction  [TEMPORAL MATCHING CRITICAL]
   src/data_fetcher_copernicus.py --growing-season-offset 105
   Searches CDSE for cloud-free L2A tiles at the correct seasonal window.
   Downloads .SAFE products, samples 9-pixel neighbourhood per GPS point.
   Output: data/processed/field_data_with_bands_growing.csv

4. Patch-level feature extraction  (choose one or both)

   a) Patch statistics (fast, no GPU needed)
      python src/extract_clay_embeddings.py
      Extracts 64 patch-level features per point:
        - Per-band mean/std/p25/p75/p95 (10 bands × 5 = 50)
        - Spectral indices: NDVI, NDWI, BSI, NDRE (4)
        - Per-band local variance (10)
      Output: data/processed/field_data_with_clay.csv

   b) Clay v1.5 foundation model embeddings (requires ~1.1 GB disk + PyTorch)
      set HF_HOME=D:\HuggingFace   # redirect to a drive with space
      python src/extract_clay_embeddings.py --source sentinel2
      Generates 1024-dim CLS-token embeddings from the Clay geospatial ViT.
      Output: data/processed/field_data_with_clay.csv

5. Model training with spatial cross-validation
   python src/train_ordinal.py data/processed/field_data_with_clay.csv \
     --deduplicate --filter-barangay Paoay
   XGBoost, Random Forest, SVM, FCNN trained with GroupKFold (grouped by barangay).
   Output: outputs/metrics_summary.csv, confusion matrices, feature importance plots
```

---

## Temporal Matching (Growing Season)

Field samples were collected **January–February 2026**, after crop harvest. Post-harvest S2 imagery shows bare soil — good for pH (iron oxide / organic matter signals) but poor for N/P/K (which rely on plant-stress spectral signatures).

For N/P/K detection, imagery must be from the **growing season** (Oct–Nov 2025 for Benguet highland vegetables). Use `--growing-season-offset 105` to shift the S2 search window back 105 days from each sample's capture date, targeting peak canopy biomass.

```bash
python src/data_fetcher_copernicus.py data/processed/field_data_with_terrain.csv \
  --growing-season-offset 105
```

| Offset | Target window | Effect |
|--------|---------------|--------|
| 0 (default) | Feb–Mar 2026 | Post-harvest bare soil |
| 75 days | Nov–Dec 2025 | Late growing / early harvest |
| 105 days | Oct–Nov 2025 | Peak canopy biomass ✓ |

---

## Feature Sets

Features are auto-detected from column names in the input CSV:

| Prefix | Source | Dims |
|--------|--------|------|
| `patch_*` | Patch statistics (no model) | 64 |
| `clay_*` | Clay v1.5 encoder embeddings | 1024 |
| `dem_*`, `slope`, `aspect`, `altitude` | Terrain | ~5 |
| Raw S2 bands (`B02`–`B12`, `B8A`) | Direct pixel values | 10–12 |

---

## Models & Evaluation

Four classifier families trained per target (N, P, K, pH):

| Model | Notes |
|-------|-------|
| **XGBoost** | Gradient-boosted trees; class weights via `scale_pos_weight` |
| **Random Forest** | Ensemble; `compute_sample_weight` for class balance |
| **SVM** | RBF kernel with `StandardScaler` |
| **FCNN** | 3-layer MLP (256→128→64) with BatchNorm and Dropout |

Also supports `--regression` mode (treats labels as continuous, clips predictions to ordinal range).

**Evaluation metrics:**
- Overall Accuracy (OA)
- Macro F1-score
- Cohen's Kappa (primary metric — corrects for chance)
- Ordinal MAE

Cross-validation uses **GroupKFold** (grouped by barangay) to prevent spatial data leakage.

---

## Current Results (Growing-Season S2, Paoay, deduplicated)

| Target | Best Model | OA | Kappa | Notes |
|--------|-----------|-----|-------|-------|
| **P** | Random Forest | — | **0.116** | Best signal; plant-stress pathway confirmed |
| **K** | Random Forest | 0.467 | 0.011 | Near-chance; geographic confound |
| **pH** | FCNN | 0.270 | -0.075 | Worse with vegetation; needs bare-soil imagery |
| **N** | All | — | ~0.0 | Collapses to Low; no High-N samples in Paoay |

---

## Environment Variables

Copy `.env.example` to `.env` and fill in Copernicus credentials:

| Variable | Description |
|----------|-------------|
| `COPERNICUS_USER` | Copernicus Data Space username (email) |
| `COPERNICUS_PASS` | Copernicus Data Space password |
| `CDSE_S3_ACCESS_KEY` | CDSE S3 key (optional, faster downloads) |
| `CDSE_S3_SECRET_KEY` | CDSE S3 secret (optional) |

Generate S3 credentials at: **dataspace.copernicus.eu → User Settings → S3 Access → Generate**

---

## How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt
pip install lightning python-box einops timm  # for Clay embeddings

# 2. Set credentials
cp .env.example .env

# 3. Fetch growing-season S2 tiles (105-day offset = Oct-Nov target)
python src/data_fetcher_copernicus.py data/processed/field_data_with_terrain.csv \
  --growing-season-offset 105

# 4a. Extract patch statistics (no GPU, fast)
python src/extract_clay_embeddings.py

# 4b. Extract Clay embeddings (requires ~1.1 GB free on HF_HOME drive)
set HF_HOME=D:\HuggingFace
python src/extract_clay_embeddings.py --source sentinel2

# 5. Train and evaluate
python src/train_ordinal.py data/processed/field_data_with_clay.csv \
  --deduplicate --filter-barangay Paoay

# With hyperparameter tuning (Optuna)
python src/train_ordinal.py data/processed/field_data_with_clay.csv \
  --deduplicate --filter-barangay Paoay --tune
```

---

## Project Structure

```
SoilScan-Sentinel2/
├── data/
│   ├── external/
│   │   └── final_merged_data_cleaned.csv     # Raw field observations (AgriCapture)
│   ├── raw/
│   │   └── field_products/                   # Downloaded .SAFE tiles
│   └── processed/
│       ├── field_data_with_terrain.csv        # Field data + terrain features
│       ├── field_data_with_bands_growing.csv  # + S2 band values (growing-season)
│       └── field_data_with_clay.csv           # + patch stats or Clay embeddings
├── outputs/
│   ├── figures/                               # Confusion matrices, feature importance
│   ├── metrics_summary.csv                    # OA / F1 / Kappa / MAE per model/target
│   └── feature_importances.csv               # Aggregated feature importance scores
├── src/
│   ├── data_fetcher_copernicus.py            # S2 tile search, download, band extraction
│   ├── extract_clay_embeddings.py            # Patch stats + Clay v1.5 embeddings
│   ├── train_ordinal.py                      # Classification + regression training
│   └── .clay_src/                            # Cached Clay model source (auto-downloaded)
├── .env.example
├── requirements.txt
└── README.md
```

---

## Key Design Decisions

**Why ordinal classification?** STK colour-chart results are inherently ordinal (Low < Medium < High). Treating them as nominal loses ordering information; treating them as continuous regression overstates precision.

**Why GroupKFold by barangay?** Farm plots within the same barangay share soil parent material, microclimate, and farming practices. Random splits would leak spatial autocorrelation into held-out folds and inflate evaluation metrics.

**Why growing-season imagery?** The plant-stress spectral pathway for N/P/K detection requires chlorophyll and canopy responses visible only during active vegetative growth — not on bare/senescent post-harvest fields.

**Why Clay over plain patch statistics?** Clay v1.5 is a geospatial Vision Transformer pretrained on multi-sensor EO data (S2, S1, Landsat, DEM) via masked autoencoding. Its 1024-dim embeddings encode spatial texture, spectral context, and multi-scale patterns that per-band statistics cannot capture.

---

## Acknowledgments

Based on [cvims/AgroLens](https://github.com/cvims/AgroLens). Clay foundation model by [Made With Clay](https://github.com/Clay-foundation/model).

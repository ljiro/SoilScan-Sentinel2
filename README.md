# SoilScan-Sentinel2

Ordinal soil nutrient classification for Philippine highland smallholder farms using Sentinel-2 satellite imagery, terrain features, and SoilGrids priors.

---

## Overview

**SoilScan-Sentinel2** predicts ordinal soil nutrient classes (Low / Medium / High for N, P, K; 11-class CPR scale for pH) from **Sentinel-2 L2A satellite imagery** paired with GPS field samples collected in the **Benguet highlands** (1,300–2,400 m ASL), Philippines.

Ground truth is collected via the **AgriCapture** mobile app (GPS, microclimate, Rapid Soil Test Kit colour-chart results). Sentinel-2 tiles are fetched from the **Copernicus Data Space Ecosystem (CDSE)** and spatially matched to each sample point. Global soil property priors from **SoilGrids v2 (ISRIC)** are appended as additional features.

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
   Searches CDSE for cloud-free L2A tiles at Oct-Nov 2025 (peak canopy biomass).
   Downloads .SAFE products, samples 9-pixel neighbourhood per GPS point.
   Output: data/processed/field_data_with_bands_growing.csv

4. Patch-level feature extraction
   python src/extract_clay_embeddings.py --source patch-stats
   Extracts 64 patch-level features per point:
     - Per-band mean/std/p25/p75/p95 (10 bands × 5 = 50)
     - Spectral indices: NDVI, NDWI, BSI, NDRE (4)
     - Per-band local variance (10)
   Output: data/processed/field_data_with_clay.csv

5. Append SoilGrids priors
   python src/fetch_soilgrids.py data/processed/field_data_with_clay.csv \
     --output data/processed/field_data_growing_soilgrids.csv
   Adds 12 sg_* columns (phh2o, soc, nitrogen, clay, sand, cec at 0-5 cm and 5-15 cm).

6. Model training
   python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate
   XGBoost, RF, SVM, FCNN trained with 5-fold StratifiedKFold (default).
   Output: outputs/metrics_summary.csv, confusion matrices, feature importance plots
```

---

## Temporal Matching (Growing Season)

Field samples were collected **January–February 2026**, after crop harvest. Post-harvest S2 imagery shows bare soil — suitable for pH (iron oxide / organic matter signals) but poor for N/P/K (which rely on plant-stress spectral signatures during active vegetative growth).

For N/P/K detection, imagery must be from the **growing season** (Oct–Nov 2025 for Benguet highland vegetables). Use `--growing-season-offset 105` to shift the S2 search window back 105 days from each sample's capture date.

```bash
python src/data_fetcher_copernicus.py data/processed/field_data_with_terrain.csv \
  --growing-season-offset 105
```

| Offset | Target window | Effect |
|--------|---------------|--------|
| 0 (default) | Feb–Mar 2026 | Post-harvest bare soil |
| 75 days | Nov–Dec 2025 | Late growing / early harvest |
| 105 days | Oct–Nov 2025 | Peak canopy biomass ✓ |

Alternatively, use `--date-range` to target a hard absolute window:

```bash
python src/data_fetcher_copernicus.py data/processed/field_data_with_terrain.csv \
  --date-range 2025-10-01 2025-11-30
```

---

## Vegetation Timeline Analysis

Before committing to a fixed offset, use `analyze_vegetation_timeline.py` to scan the past N months of S2 data and compute a data-driven monthly NDVI profile per GPS cluster.

```bash
python src/analyze_vegetation_timeline.py data/processed/field_data_with_terrain.csv \
  --months 8 --max-cloud 20 --plot
```

Output saved to `outputs/vegetation_timeline.csv` (and `.png` with `--plot`).

---

## Patch Quality Columns

Every run of `extract_clay_embeddings.py` writes per-patch quality metrics alongside features:

| Column | Description |
|--------|-------------|
| `quality_ndvi_mean` | Mean NDVI over the 128×128 patch |
| `quality_ndvi_p75` | 75th-percentile NDVI |
| `quality_veg_frac` | Fraction of pixels with NDVI > 0.2 |
| `quality_cloud_frac` | Fraction flagged as cloud or shadow (SCL-based) |
| `quality_valid_frac` | Fraction classified as vegetation / bare soil / water |

Optional filters drop rows that fail thresholds before saving:

```bash
python src/extract_clay_embeddings.py \
  --min-ndvi 0.2 --min-veg-frac 0.3 --max-cloud-frac 0.1
```

---

## SoilGrids Integration

Global 250 m soil property predictions from ISRIC SoilGrids v2. Adds 12 `sg_*` columns auto-detected by `train_ordinal.py`.

**REST API** (simplest):
```cmd
python src/fetch_soilgrids.py data/processed/field_data_with_clay.csv \
  --output data/processed/field_data_growing_soilgrids.csv
```

**Local VRT mode** (for networks blocking `api.isric.org`):
```cmd
python src/fetch_soilgrids.py data/processed/field_data_with_clay.csv \
    --local-data-dir data/raw/soilgrids \
    --output data/processed/field_data_growing_soilgrids.csv
```

---

## Feature Sets

Features are auto-detected from column names in the input CSV:

| Prefix | Source | Dims |
|--------|--------|------|
| `patch_*` | Patch statistics (current baseline) | 64 |
| `sg_*` | SoilGrids v2 soil property priors | 12 |
| `dem_*`, `slope`, `aspect`, `altitude` | Terrain | ~5 |
| Raw S2 bands (`B02`–`B12`, `B8A`) | Direct pixel values | 10–12 |
| `clay_*` | Clay v1.5 encoder embeddings *(available, not yet evaluated)* | 1024 |
| `resnet_*` | ResNet-50 pretrained embeddings *(available, not yet evaluated)* | 2048 |

---

## Models & Evaluation

Four classifier families trained per target (N, P, K, pH):

| Model | Notes |
|-------|-------|
| **XGBoost** | Gradient-boosted trees; `sample_weight` for class balance |
| **Random Forest** | Ensemble; `compute_sample_weight` for class balance |
| **SVM** | RBF kernel with `StandardScaler`; `class_weight="balanced"` |
| **FCNN** | 3-layer MLP (256→128→64) with BatchNorm and Dropout; minority oversampling |

**Primary metric: Cohen's Kappa** — corrects for chance agreement. Interpretation: 0.01–0.20 Slight, 0.21–0.40 Fair, 0.41–0.60 Moderate, 0.61–0.80 Substantial.

**Cross-validation:** 5-fold StratifiedKFold (default). Use `--spatial-kfold` for GroupKFold by barangay (honest deployment metric but severely limited with only 2 spatial groups).

---

## Current Results

Input: `field_data_growing_soilgrids.csv --deduplicate` (~475 rows, Oct+SoilGrids features)

### 5-fold StratifiedKFold (primary — matches standard benchmark comparisons)

| Target | Best Model | Kappa | Interpretation |
|--------|-----------|-------|----------------|
| **N** | Random Forest | **0.338** | Fair — only Low/Medium detectable; no High-N samples exist |
| **P** | Random Forest | **0.430** | Moderate — growing-season spectral signal confirmed |
| **K** | SVM | **0.670** | Substantial — inflated by geographic confound (see note) |
| **pH** | Random Forest | **0.392** | Fair — 55% exact class, 25.5% off by one CPR step |

### Spatial GroupKFold (honest deployment metric — `--spatial-kfold`)

| Target | Kappa | vs Random k-fold |
|--------|-------|-----------------|
| P | 0.133 | 3.2× lower |
| K | ~0.000 | Geographic confound confirmed — High-K only in one barangay |
| pH | 0.210 | 1.9× lower |
| N | ~0.000 | No High-N samples anywhere |

**The gap between random and spatial Kappa quantifies spatial autocorrelation leakage.** With only 2 barangays, random k-fold mixes adjacent GPS points across train/test folds — the model partially memorizes location. K=0.670 random vs ~0.000 spatial confirms K is a geographic confound, not a spectral signal. Spatial holdout is the correct metric for deployment generalization; random k-fold is useful for comparison with published benchmarks that use random splits.

---

## Data Limitations

- **No High-N samples**: All samples across both barangays are Low or Medium N. No model can learn the Low→High boundary. Collect from plots with heavy urea fertilisation 2–4 weeks before a Sentinel-2 overpass.
- **K geographic confound**: High-K samples exist only in Paoay. Balili has zero High-K. Any K model trained on this data memorizes barangay rather than spectral signal.
- **Two barangays only**: GroupKFold produces only 2 folds. Results are sensitive to the training/test split and underrepresent minority classes per fold. Adding a third barangay (e.g., Atok Betag) would meaningfully improve spatial CV reliability.

---

## How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set Copernicus credentials
cp .env.example .env  # fill in COPERNICUS_USER and COPERNICUS_PASS

# 3. Fetch growing-season S2 tiles (105-day offset → Oct-Nov target)
python src/data_fetcher_copernicus.py data/processed/field_data_with_terrain.csv \
  --growing-season-offset 105

# 4. Extract patch statistics (no GPU required)
python src/extract_clay_embeddings.py

# 5. Append SoilGrids priors
python src/fetch_soilgrids.py data/processed/field_data_with_clay.csv \
  --output data/processed/field_data_growing_soilgrids.csv

# 6. Train and evaluate (5-fold StratifiedKFold by default)
python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate

# With hyperparameter tuning (Optuna)
python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate --tune

# Spatial holdout instead of random k-fold
python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate --spatial-kfold

# Export best models to outputs/models/
python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate --save-models
```

---

## Project Structure

```
SoilScan-Sentinel2/
├── data/
│   ├── external/
│   │   └── final_merged_data_cleaned.csv         # Raw field observations (AgriCapture)
│   ├── raw/
│   │   └── field_products/                       # Downloaded .SAFE tiles
│   └── processed/
│       ├── field_data_with_terrain.csv            # Field data + terrain features
│       ├── field_data_with_bands_growing.csv      # + S2 band values (Oct-Nov growing season)
│       ├── field_data_with_clay.csv               # + patch statistics
│       └── field_data_growing_soilgrids.csv       # + SoilGrids priors (canonical input)
├── outputs/
│   ├── figures/                                   # Confusion matrices, feature importance
│   ├── models/                                    # Exported best models (--save-models)
│   ├── metrics_summary.csv                        # Random k-fold results
│   ├── metrics_summary_spatial.csv                # Spatial holdout results
│   └── feature_importances.csv                    # Aggregated feature importance scores
├── src/
│   ├── data_fetcher_copernicus.py                 # S2 tile search, download, band extraction
│   ├── extract_clay_embeddings.py                 # Patch stats + Clay v1.5 embeddings
│   ├── train_ordinal.py                           # Classification + regression training
│   ├── analyze_vegetation_timeline.py             # Monthly NDVI profile → peak date-range
│   ├── merge_temporal.py                          # Merge two temporal feature CSVs
│   └── fetch_soilgrids.py                         # Add SoilGrids v2 priors (sg_* columns)
├── .env.example
├── requirements.txt
└── README.md
```

---

## Key Design Decisions

**Why ordinal classification?** STK colour-chart results are inherently ordinal (Low < Medium < High). Treating them as nominal loses ordering information; treating them as continuous regression overstates precision given that rapid test kits only produce three discrete categories.

**Why growing-season imagery?** The plant-stress spectral pathway for N/P/K detection requires chlorophyll and canopy responses visible only during active vegetative growth — not on bare post-harvest fields.

**Why patch statistics over plain band values?** Per-band statistics (mean, std, percentiles, local variance) capture spatial texture and variability within the 128×128 patch, which single-pixel band values cannot. Clay v1.5 embeddings are available and may improve results but have not been benchmarked on this dataset yet.

**Why 5-fold StratifiedKFold as default?** With only 2 barangays, GroupKFold produces 2-fold CV — each fold trains on one barangay and tests on the other, severely limiting the training distribution and underrepresenting minority classes. StratifiedKFold preserves class proportions across folds and gives more stable estimates. Use `--spatial-kfold` when the goal is to measure true geographic generalization.

---

## Model Export

Pass `--save-models` to save the best model per target after training. Two files are written per target:

| File | Contents |
|------|----------|
| `outputs/models/{target}_{model}.joblib` | Full sklearn `Pipeline` — preprocessor + fitted classifier |
| `outputs/models/{target}_{model}_meta.json` | Feature names, class labels, model type, training sample count |

```python
import joblib, json

pipeline = joblib.load("outputs/models/p_RandomForest.joblib")
meta     = json.load(open("outputs/models/p_RandomForest_meta.json"))

y_pred = pipeline.predict(X_new[meta["feature_names"]])
# map integer indices with meta["class_names"]
```

---

## Acknowledgments

Inspired by [cvims/AgroLens](https://github.com/cvims/AgroLens). Clay foundation model by [Made With Clay](https://github.com/Clay-foundation/model).

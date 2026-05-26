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

4. Append SoilGrids priors
   python src/fetch_soilgrids.py data/processed/field_data_with_bands_growing.csv \
     --output data/processed/field_data_growing_soilgrids.csv
   Adds 12 sg_* columns (phh2o, soc, nitrogen, clay, sand, cec at 0-5 cm and 5-15 cm).

5. Model training
   python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate
   XGBoost, RF, SVM, FCNN trained with 5-fold StratifiedKFold (default).
   Spectral indices (NDVI, EVI, NDRE, BSI, etc.) are computed on-the-fly from raw bands.
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
| `quality_scl_ok` | Boolean — `False` when SCL band was unavailable; cloud/shadow fracs will be NaN |

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

## Input Features

All features are auto-detected from column names in the input CSV. The current canonical input (`field_data_growing_soilgrids.csv`) provides:

### Sentinel-2 Spectral Bands (Oct–Nov 2025, growing season)

| Feature | Bands | Description |
|---------|-------|-------------|
| Raw band values | B01–B12, B8A | 12 bands, mean pixel value over 3×3 neighbourhood |
| Band std | B01_std–B12_std | Spatial variability within the 3×3 patch |

### Computed Spectral Indices (derived from S2 bands)

| Index | Formula bands | Sensitivity |
|-------|--------------|-------------|
| NDVI | B08, B04 | Canopy density / biomass |
| EVI | B08, B04, B02 | Canopy density (soil-adjusted) |
| SAVI | B08, B04 | Vegetation with soil brightness correction |
| MSAVI | B08, B04 | Modified SAVI |
| NDRE | B8A, B05 | Chlorophyll / nitrogen stress (red-edge) |
| CHL_re | B8A, B05 | Chlorophyll red-edge index |
| BSI | B11, B04, B08, B02 | Bare soil fraction |
| BI | B04, B08 | Brightness index |
| NDWI | B03, B08 | Water / moisture content |
| NDMI | B08, B11 | Moisture / dry matter |

### Terrain Features (DEM-derived)

| Feature | Description |
|---------|-------------|
| `elevation_m` | Elevation above sea level |
| `slope_deg` | Terrain slope in degrees |
| `aspect_deg` | Slope aspect (compass direction) |
| `twi` | Topographic wetness index |
| `curvature` | Surface curvature |
| `northness` | cos(aspect) — north-facing component |
| `eastness` | sin(aspect) — east-facing component |

### Microclimate (recorded at time of field sample collection)

| Feature | Description |
|---------|-------------|
| `temperature_c` | Air temperature (°C) |
| `humidity_percent` | Relative humidity (%) |
| `altitude_m` | GPS-recorded altitude (m) |

### SoilGrids v2 Priors (ISRIC, 250 m resolution)

| Feature | Description |
|---------|-------------|
| `sg_phh2o_0-5cm`, `sg_phh2o_5-15cm` | Soil pH (H₂O) |
| `sg_soc_0-5cm`, `sg_soc_5-15cm` | Soil organic carbon (dg/kg) |
| `sg_nitrogen_0-5cm`, `sg_nitrogen_5-15cm` | Total nitrogen (cg/kg) |
| `sg_clay_0-5cm`, `sg_clay_5-15cm` | Clay content (g/kg) |
| `sg_sand_0-5cm`, `sg_sand_5-15cm` | Sand content (g/kg) |
| `sg_cec_0-5cm`, `sg_cec_5-15cm` | Cation exchange capacity (mmol/kg) |

### Categorical

| Feature | Description |
|---------|-------------|
| `crops` | Crop type at the sample point (one-hot encoded) |

### Additional Feature Sources (available, not yet benchmarked)

| Prefix | Source | Dims |
|--------|--------|------|
| `patch_*` | Patch statistics from `extract_clay_embeddings.py --source patch-stats` | 64 |
| `clay_*` | Clay v1.5 geospatial ViT embeddings | 1024 |
| `resnet_*` | ResNet-50 pretrained embeddings | 2048 |

---

## Models & Evaluation

Four classifier families trained per target (N, P, K, pH):

| Model | Notes |
|-------|-------|
| **XGBoost** | Gradient-boosted trees; `sample_weight` for class balance |
| **Random Forest** | Ensemble; `compute_sample_weight` for class balance |
| **SVM** | RBF kernel with `StandardScaler`; `class_weight="balanced"` |
| **FCNN** | 3-layer MLP (128→64→32), ReLU, L2 regularisation, early stopping; minority oversampling for class balance |

**Primary metric: Cohen's Kappa** — corrects for chance agreement. Interpretation: 0.01–0.20 Slight, 0.21–0.40 Fair, 0.41–0.60 Moderate, 0.61–0.80 Substantial.

**Cross-validation:** 5-fold StratifiedKFold (default). Use `--spatial-kfold` for GroupKFold by barangay (honest deployment metric but severely limited with only 2 spatial groups).

---

## Current Results

Input: `field_data_growing_soilgrids.csv --deduplicate` (~475 rows, Oct+SoilGrids features)

### 5-fold StratifiedKFold — primary results

| Target | Best Model | Kappa | Interpretation |
|--------|-----------|-------|----------------|
| **N** | Random Forest | **0.338** | Fair — only Low/Medium detectable; no High-N samples exist |
| **P** | Random Forest | **0.430** | Moderate — growing-season spectral signal confirmed |
| **K** | SVM | **0.670** | Substantial — spectral signal confirmed (see ablations below) |
| **pH** | Random Forest | **0.392** | Fair — 55% exact class, 25.5% off by one CPR step |

### Spatial GroupKFold (`--spatial-kfold`)

| Target | Kappa | vs Random k-fold |
|--------|-------|-----------------|
| P | 0.133 | 3.2× lower |
| K | ~0.000 | Validation gap — Balili has no High-K samples to test against |
| pH | 0.210 | 1.9× lower |
| N | ~0.000 | No High-N samples anywhere |

The gap quantifies spatial autocorrelation leakage from mixing GPS points across folds. Use `--spatial-kfold` for deployment generalization estimates; random k-fold for comparison with published benchmarks.

### Signal ablations

Two experiments probe whether results reflect genuine spectral signals or geographic memorization:

**Spectral-only (`--spectral-only`)** — removes terrain, microclimate, and SoilGrids; keeps only S2 bands, spectral indices:

| Target | Kappa | vs Full features |
|--------|-------|-----------------|
| K | 0.629 | −0.041 — signal is spectral, not terrain proxy |
| P | 0.302 | −0.128 — terrain adds meaningful lift |
| pH | 0.179 | −0.213 — elevation is the dominant pH signal |

**Location demeaning (`--demean-barangay`)** — subtracts per-barangay means from all features, forcing the model to learn within-location variation only:

| Target | Kappa | vs Baseline |
|--------|-------|-------------|
| K | 0.673 | ≈ unchanged — signal is within-location, not barangay offset |
| P | 0.430 | ≈ unchanged |
| pH | 0.389 | ≈ unchanged |

**Conclusion:** the random k-fold Kappa scores reflect genuine within-location spectral and terrain patterns, not gross Paoay vs Balili offset memorization. The K spatial holdout ~0.000 is a *validation gap* (Balili has no High-K to test against), not absence of signal — K Kappa=0.658 within Paoay alone confirms this.

---

## Data Limitations

- **No High-N samples**: All samples across both barangays are Low or Medium N. No model can learn the Low→High boundary. Collect from plots with heavy urea fertilisation 2–4 weeks before a Sentinel-2 overpass.
- **K validation gap**: High-K samples exist only in Paoay; Balili has none. Spatial holdout cannot validate K predictions because the test fold has no High-K examples. However, K Kappa=0.658 within Paoay alone confirms the spectral signal is real — collecting High-K samples in Balili would resolve this.
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

# 6. Train and evaluate (5-fold StratifiedKFold, saves models by default)
python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate

# Spatial holdout (honest deployment metric, limited with only 2 barangays)
python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate --spatial-kfold

# Spectral-only: S2 bands + indices + SoilGrids, no terrain/microclimate
python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate --spectral-only

# Demean features by barangay (removes between-location offsets)
python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate --demean-barangay

# With hyperparameter tuning (Optuna, ~5-10 min extra per target)
python src/train_ordinal.py data/processed/field_data_growing_soilgrids.csv --deduplicate --tune

# Optional: append Sentinel-1 GRD backscatter (VV, VH) — not yet benchmarked
python src/fetch_sentinel1.py data/processed/field_data_growing_soilgrids.csv \
  --growing-season-offset 105 \
  --output data/processed/field_data_growing_soilgrids_s1.csv
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
│   ├── fetch_soilgrids.py                         # Add SoilGrids v2 priors (sg_* columns)
│   └── fetch_sentinel1.py                         # Optional: add S1 GRD VV/VH backscatter
├── .env.example
├── requirements.txt
└── README.md
```

---

## Key Design Decisions

**Why ordinal classification?** STK colour-chart results are inherently ordinal (Low < Medium < High). Treating them as nominal loses ordering information; treating them as continuous regression overstates precision given that rapid test kits only produce three discrete categories.

**Why growing-season imagery?** The plant-stress spectral pathway for N/P/K detection requires chlorophyll and canopy responses visible only during active vegetative growth — not on bare post-harvest fields.

**Why raw S2 bands as the current baseline?** The canonical pipeline uses raw band values and band std from the 3×3 neighbourhood, with spectral indices computed on-the-fly. Patch statistics (`extract_clay_embeddings.py`) and Clay v1.5 embeddings are available and may improve results but have not been benchmarked against the raw-band baseline on this dataset.

**Why 5-fold StratifiedKFold as default?** With only 2 barangays, GroupKFold produces 2-fold CV — each fold trains on one barangay and tests on the other, severely limiting the training distribution and underrepresenting minority classes. StratifiedKFold preserves class proportions across folds and gives more stable estimates. Use `--spatial-kfold` when the goal is to measure true geographic generalization.

---

## Model Export

Models are saved by default (`--save-models` is on). Use `--no-save-models` to skip. Two files are written per target:

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

## Recommendations / Future Work

- **Collect High-K samples in Balili**: K has a confirmed spectral signal (Kappa=0.658 within Paoay). Adding High-K samples in a second location would enable cross-barangay validation and likely yield a meaningful spatial holdout Kappa.
- **Collect High-N samples**: N is Low everywhere across both barangays. Target plots with heavy urea/ammonium fertilisation 2–4 weeks before an S2 overpass.
- **Add a third barangay**: With only 2 groups, spatial CV is 2-fold. A third sampling location would enable leave-one-out spatial validation and produce more robust generalization estimates.
- **Benchmark patch statistics and Clay embeddings**: `extract_clay_embeddings.py` produces 64 patch-level statistics or 1024-dim Clay v1.5 embeddings — neither has been compared against the raw-band baseline on this dataset.
- **Evaluate Sentinel-1 backscatter**: `fetch_sentinel1.py` is implemented and fetches C-band VV/VH from CDSE. Benchmark whether S1 features (soil moisture, surface roughness) improve Kappa over the S2+SoilGrids baseline, particularly for K and pH.
- **Adaptive cloud threshold**: `data_fetcher_copernicus.py` uses a fixed `max_cloud=20` cutoff. A tiered fallback (≤15% → ≤25% → ≤40%) would reduce download failures during cloudy seasons while still preferring clean tiles.
- **SoilGrids batch parallelism**: `fetch_soilgrids.py` queries one cell at a time with a 0.5 s delay. Batching requests with a `requests.Session` could reduce wall-clock time from ~30 min to ~5 min on large datasets without exceeding ISRIC rate limits.
- **NDVI result caching in vegetation timeline**: `analyze_vegetation_timeline.py` re-samples every tile on every run. A simple cache CSV keyed by (product name, cluster) would avoid redundant I/O when re-running with different `--months` windows.

## Acknowledgments

Inspired by [cvims/AgroLens](https://github.com/cvims/AgroLens). Clay foundation model by [Made With Clay](https://github.com/Clay-foundation/model).

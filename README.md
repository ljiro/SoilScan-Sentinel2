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

   c) ResNet-50 pretrained embeddings (fast, no large checkpoint)
      python src/extract_clay_embeddings.py --source resnet
      python src/extract_clay_embeddings.py --source resnet --resnet-size resnet18
      Adapts pretrained ImageNet ResNet to 10 S2 input channels by averaging
      the RGB channel weights. Generates 2048-dim (ResNet-50) or 512-dim
      (ResNet-18) pooled feature vectors stored as resnet_* columns.
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

Alternatively, use `--date-range` to target a hard absolute window rather than a per-sample offset:

```bash
python src/data_fetcher_copernicus.py data/processed/field_data_with_terrain.csv \
  --date-range 2025-10-01 2025-11-30
```

Output is automatically named `field_data_with_bands_20251001_20251130.csv`. This guarantees every GPS point gets imagery from the same phenological window regardless of when the soil sample was collected.

---

## Vegetation Timeline Analysis

Before committing to a fixed offset or date range, use `analyze_vegetation_timeline.py` to scan the past N months of S2 data and compute a data-driven monthly NDVI profile for each GPS cluster.

```bash
python src/analyze_vegetation_timeline.py data/processed/field_data_with_terrain.csv
python src/analyze_vegetation_timeline.py data/processed/field_data_with_terrain.csv \
  --months 8 --max-cloud 20 --plot
```

The script:
1. Groups GPS points into ~2 km spatial cells.
2. Searches CDSE catalog for all S2 L2A tiles in the lookback window.
3. Samples B04 + B08 per tile using whichever source is available:
   - **Local `.SAFE`** directories already on disk (no download needed)
   - **S3 streaming** via CDSE S3 credentials (reads only the pixels near each GPS point — no full download)
4. Aggregates NDVI by calendar month (mean, max, tile count).
5. Prints a monthly table and recommends the peak month as a `--date-range` window.

```
Month      NDVI mean   NDVI max   Tiles
----------------------------------------
2025-08        0.182      0.241       4
2025-09        0.271      0.318       6
2025-10        0.481      0.562       8   ← peak
2025-11        0.412      0.503       5
2025-12        0.203      0.280       3

★ Peak vegetation month: 2025-10
  Suggested --date-range flag:
    --date-range 2025-10-01 2025-10-31
```

Output saved to `outputs/vegetation_timeline.csv` (and `outputs/vegetation_timeline.png` with `--plot`).

**S3 streaming** (optional but recommended for tiles not yet on disk) requires `boto3` and CDSE S3 credentials in `.env`:
```
CDSE_S3_ACCESS_KEY=...
CDSE_S3_SECRET_KEY=...
```

---

## Patch Quality Analysis

Every run of `extract_clay_embeddings.py` reads the **Scene Classification Layer (SCL)** band from the S2 L2A product and computes per-patch quality metrics, stored as `quality_*` columns in the output CSV:

| Column | Description |
|--------|-------------|
| `quality_ndvi_mean` | Mean NDVI over the 128×128 patch |
| `quality_ndvi_p75` | 75th-percentile NDVI (robust to soil/shadow outliers) |
| `quality_veg_frac` | Fraction of pixels with NDVI > 0.2 |
| `quality_cloud_frac` | Fraction flagged as cloud or cloud shadow (SCL-based) |
| `quality_valid_frac` | Fraction classified as vegetation / bare soil / water |

A quality summary is printed at the end of extraction. For Oct–Nov growing-season tiles, expect `quality_ndvi_mean` around 0.3–0.6 for active vegetable crops.

Optional filters drop rows that fail thresholds before saving:

```bash
python src/extract_clay_embeddings.py \
  --min-ndvi 0.2        # exclude patches with low vegetation signal
  --min-veg-frac 0.3    # require at least 30% vegetated pixels
  --max-cloud-frac 0.1  # reject if more than 10% cloud/shadow
```

---

## Feature Sets

Features are auto-detected from column names in the input CSV:

| Prefix | Source | Dims |
|--------|--------|------|
| `patch_*` | Patch statistics (no model) | 64 |
| `clay_*` | Clay v1.5 encoder embeddings | 1024 |
| `resnet_*` | ResNet-50 pretrained embeddings | 2048 (or 512 for ResNet-18) |
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

## Current Results

### October 2025 (peak growing season — `--growing-season-offset 105`)
| Target | Best Model | OA | Kappa | Notes |
|--------|-----------|-----|-------|-------|
| **P** | Random Forest | — | **0.116** | Best signal; plant-stress pathway confirmed |
| **K** | Random Forest | 0.467 | 0.011 | Near-chance; geographic confound |
| **pH** | FCNN | 0.270 | -0.075 | Worse with vegetation; needs bare-soil imagery |
| **N** | All | — | ~0.0 | Collapses to Low; no High-N samples in Paoay |

### December 2025 (late season — `--date-range 2025-12-01 2025-12-31`)
| Target | Best Model | OA | Kappa | Notes |
|--------|-----------|-----|-------|-------|
| **P** | XGBoost | 0.416 | 0.062 | Weaker than Oct; vegetation thinning reduces stress signal |
| **K** | SVM | 0.589 | **0.289** | Major improvement over Oct — late-season canopy/senescence signal |
| **pH** | SVM | 0.307 | 0.079 | Slight improvement; bare soil more visible |
| **N** | All | — | ~0.0 | Collapses to Low; no High-N samples |

**Key finding:** different nutrients have different optimal imagery windows. P peaks in October (active biomass), K peaks in December (late-season senescence). A multi-temporal approach combining both windows is the logical next step.

### Multi-temporal pipeline

Merge Oct and Dec feature sets into one dataset, so the model can leverage both windows simultaneously:

```bash
# 1. Extract patch stats for each window (already done if you ran extract_clay_embeddings.py on both)
python src/extract_clay_embeddings.py --input data/processed/field_data_with_bands_growing.csv \
    --output data/processed/field_data_with_clay.csv

python src/extract_clay_embeddings.py --input data/processed/field_data_with_bands_20251201_20251231.csv \
    --output data/processed/field_data_dec2025_clay.csv

# 2. Merge into one multi-temporal CSV (adds _oct / _dec suffixes to all feature columns)
python src/merge_temporal.py \
    data/processed/field_data_with_clay.csv \
    data/processed/field_data_dec2025_clay.csv \
    --suffix1 oct --suffix2 dec \
    --output data/processed/field_data_multitemporal.csv

# 3. Train
python src/train_ordinal.py data/processed/field_data_multitemporal.csv \
    --deduplicate --filter-barangay Paoay
```

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
│   ├── analyze_vegetation_timeline.py        # Monthly NDVI profile → peak date-range finder
│   ├── merge_temporal.py                     # Merge two temporal feature CSVs (adds _oct/_dec suffixes)
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

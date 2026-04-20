# CLAUDE.md — SoilScan-Sentinel2

Project context and conventions for AI-assisted development.

## What This Project Does

Ordinal classification of soil nutrients (N, P, K, pH) from Sentinel-2 satellite imagery for smallholder farms in the Benguet highlands, Philippines. Field samples were collected Jan–Feb 2026 via the AgriCapture app using Rapid Soil Test Kits. Labels are Low/Medium/High (N, P, K) or 11-class CPR scale (pH).

## Critical Domain Knowledge

**Temporal matching is essential for N/P/K.** Field samples were taken post-harvest (Jan–Feb 2026). Using imagery from the same date (bare soil) gives near-zero Kappa for N/P/K because the plant-stress spectral pathway requires active vegetation. Always use `--growing-season-offset 105` to target Oct–Nov tiles (peak canopy biomass for Benguet highland vegetables).

**pH works on bare soil.** pH detection via iron oxide / organic matter signatures actually performs better on post-harvest imagery. Growing-season imagery (masked by vegetation) degrades pH results.

**N collapses to all-Low.** All samples in the Paoay subset are Low N — no model can learn the boundary. This is a data collection problem, not a model problem.

**K has a geographic confound.** High-K samples are concentrated in a single barangay (Paoay), making it hard to distinguish spectral signal from location.

## Pipeline

```
field_data_with_terrain.csv          (base: GPS + terrain features)
        ↓ data_fetcher_copernicus.py --growing-season-offset 105
field_data_with_bands_growing.csv    (+ S2 band pixel values, Oct-Nov 2025)
        ↓ extract_clay_embeddings.py [--source sentinel2]
field_data_with_clay.csv             (+ 64 patch stats OR 1024 Clay embeddings)
        ↓ train_ordinal.py --deduplicate --filter-barangay Paoay
outputs/metrics_summary.csv
```

## Key Files

| File | Purpose |
|------|---------|
| `src/data_fetcher_copernicus.py` | S2 tile search → download → band sampling. Has resume logic, cloud-sorted tile selection, growing-season offset, local tile fallthrough to API. |
| `src/extract_clay_embeddings.py` | Two modes: (1) `--source patch-stats` (default) = 64 per-band stats, no model needed; (2) `--source sentinel2` = Clay v1.5 1024-dim embeddings. Clay source auto-downloaded from GitHub to `src/.clay_src/`. Checkpoint cached at `HF_HOME`. |
| `src/train_ordinal.py` | XGBoost / RF / SVM / FCNN classification. Also `--regression` mode. GroupKFold by barangay. Auto-detects `patch_*`, `clay_*`, terrain feature columns. |

## Key Flags Added Since Initial Build

| Script | Flag | Purpose |
|--------|------|---------|
| `data_fetcher_copernicus.py` | `--growing-season-offset N` | Shift S2 search N days before sample date |
| `data_fetcher_copernicus.py` | `--date-range START END` | Fixed absolute window for all points (overrides offset) |
| `extract_clay_embeddings.py` | `--source sentinel2` | Clay v1.5 embeddings instead of patch stats |
| `extract_clay_embeddings.py` | `--min-ndvi FLOAT` | Drop patches below NDVI threshold |
| `extract_clay_embeddings.py` | `--min-veg-frac FLOAT` | Drop patches with too little vegetated area |
| `extract_clay_embeddings.py` | `--max-cloud-frac FLOAT` | Drop patches with too much cloud/shadow (SCL-based) |
| `train_ordinal.py` | `--regression` | Treat labels as continuous, clip to ordinal range |
| `train_ordinal.py` | `--tune` | Optuna hyperparameter search |

## Patch Quality Columns

`extract_clay_embeddings.py` always writes these alongside patch features:
- `quality_ndvi_mean`, `quality_ndvi_p75` — vegetation signal strength
- `quality_veg_frac` — fraction of pixels with NDVI > 0.2
- `quality_cloud_frac` — SCL-based cloud/shadow fraction
- `quality_valid_frac` — fraction of usable pixels (veg/soil/water per SCL)

## S2 Processing Notes

- **N0400+ offset**: S2 L2A DN must be scaled as `reflectance = DN/10000 - 0.1` (not just `/10000`). Products from 2022+ use this baseline.
- **Clay normalization**: Clay expects raw DN values (0–10000 range), normalizes with its own per-band mean/std from `configs/metadata.yaml`. Do NOT apply the reflectance conversion before passing to Clay.
- **UTM vs WGS84**: Always use `transform_bounds(src.crs, wgs84, *src.bounds)` before comparing GPS lat/lon to tile bounds. Never compare degree coordinates to UTM meter coordinates.
- **Band order for Clay**: B02, B03, B04, B05, B06, B07, B08, B8A, B11, B12 (Clay's `sentinel-2-l2a` platform order). Wavelengths in micrometers.

## Clay Model Loading

The Clay HuggingFace repo (`made-with-clay/Clay`) only contains the checkpoint. Model code is in the GitHub repo (`Clay-foundation/model`) under `claymodel/`. The loader in `extract_clay_embeddings.py`:
1. Downloads checkpoint via `hf_hub_download` (cached at `HF_HOME`)
2. Downloads `claymodel/` package to `src/.clay_src/` via GitHub API
3. Loads only the `Encoder` class directly (strips `model.encoder.*` keys)
4. No Lightning CLI, no teacher model, no extra downloads

**Do NOT use `ClayMAEModule.load_from_checkpoint`** — causes a Lightning CLI `ArgumentParser` crash.

## Current Best Results (growing-season, Paoay, deduplicated)

| Target | Best Kappa | Model |
|--------|-----------|-------|
| P | 0.116 | Random Forest |
| K | 0.011 | Random Forest |
| pH | -0.075 | FCNN (worse with veg cover) |
| N | ~0.0 | All (no High-N samples) |

## Commit Style

Short imperative subject line, no `Co-Authored-By` lines.

## Environment

- Python 3.10, Windows 10
- HF_HOME should point to D drive (C drive has limited space): `set HF_HOME=D:\HuggingFace`
- Clay checkpoint already downloaded at `D:\HuggingFace\hub\models--made-with-clay--Clay\snapshots\70200ebcccdf67bf2a0cb9984c77ddee26c10ed2\v1.5\clay-v1.5.ckpt`

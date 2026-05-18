"""
merge_temporal.py

Merge two temporally-distinct S2 feature CSVs (band values or patch stats)
into one multi-temporal dataset for training.

Feature columns (bands, patch_*, clay_*, resnet_*, quality_*, spectral indices)
are renamed with per-file suffixes so both windows are visible to the model.
Terrain, GPS, and label columns are taken from the primary (first) file.

Usage:
    python src/merge_temporal.py \\
        data/processed/field_data_with_clay.csv \\
        data/processed/field_data_dec2025_clay.csv \\
        --suffix1 oct --suffix2 dec \\
        --output data/processed/field_data_multitemporal.csv
"""

import argparse
import os

import pandas as pd

# Columns that should NOT be renamed — kept once from the primary file
_KEEP_COLS = {
    "latitude", "longitude",
    "n", "p", "k", "ph",
    "barangay", "municipality", "crop_type", "crops",
    "capture_datetime", "date", "datetime",
    "temperature_c", "humidity_percent", "altitude_m",
    "elevation_m", "slope_deg", "aspect_deg",
    "twi", "curvature", "northness", "eastness",
    "_group_id",
}

# Raw S2 band column prefixes/names to rename
_BAND_NAMES = [
    "B01", "B02", "B03", "B04", "B05", "B06",
    "B07", "B08", "B8A", "B09", "B11", "B12",
]

# Prefixes that identify feature columns (will be renamed with suffix)
_FEATURE_PREFIXES = ("patch_", "clay_", "resnet_", "emit_", "quality_",
                     "NDVI", "EVI", "SAVI", "MSAVI", "NDRE", "CHL",
                     "BSI", "BI", "NDWI", "NDMI", "CI_")


def _is_feature_col(col: str) -> bool:
    if col in _KEEP_COLS:
        return False
    if col in _BAND_NAMES:
        return True
    if col.endswith("_std") and col.replace("_std", "") in _BAND_NAMES:
        return True
    return any(col.startswith(p) for p in _FEATURE_PREFIXES)


def merge(csv1: str, csv2: str, suffix1: str, suffix2: str, output: str):
    df1 = pd.read_csv(csv1, on_bad_lines="skip")
    df2 = pd.read_csv(csv2, on_bad_lines="skip")

    print(f"Primary   ({suffix1}): {len(df1)} rows × {len(df1.columns)} cols  [{csv1}]")
    print(f"Secondary ({suffix2}): {len(df2)} rows × {len(df2.columns)} cols  [{csv2}]")

    # Rename feature columns in each file
    rename1 = {c: f"{c}_{suffix1}" for c in df1.columns if _is_feature_col(c)}
    rename2 = {c: f"{c}_{suffix2}" for c in df2.columns if _is_feature_col(c)}
    df1 = df1.rename(columns=rename1)
    df2 = df2.rename(columns=rename2)

    print(f"  Renamed {len(rename1)} columns with _{suffix1} suffix")
    print(f"  Renamed {len(rename2)} columns with _{suffix2} suffix")

    # Keep only keep-cols + suffixed feature cols from df2 to avoid duplicate terrain/labels
    keep2 = ["latitude", "longitude"] + [c for c in df2.columns if c not in _KEEP_COLS]
    df2_slim = df2[keep2]

    # Round GPS coordinates to 6 decimal places (~0.1 m precision) before merging
    # to avoid silent row-drops caused by floating-point CSV round-trip differences
    for df_item in (df1, df2_slim):
        df_item["latitude"]  = df_item["latitude"].round(6)
        df_item["longitude"] = df_item["longitude"].round(6)

    merged = pd.merge(
        df1, df2_slim,
        on=["latitude", "longitude"],
        how="inner",
        suffixes=("", f"_dup_{suffix2}"),
    )
    dropped = len(df1) - len(merged)
    if dropped > 0:
        print(f"  WARNING: {dropped} rows from primary CSV had no match in secondary "
              f"(inner join on lat/lon). Check that both files cover the same GPS points.")

    # Drop any accidental duplicate columns (e.g. if df2 had keep-cols that slipped through)
    dup_cols = [c for c in merged.columns if c.endswith(f"_dup_{suffix2}")]
    if dup_cols:
        merged = merged.drop(columns=dup_cols)

    print(f"\nMerged: {len(merged)} rows × {len(merged.columns)} cols")
    feat1 = [c for c in merged.columns if c.endswith(f"_{suffix1}")]
    feat2 = [c for c in merged.columns if c.endswith(f"_{suffix2}")]
    print(f"  {suffix1} feature cols: {len(feat1)}")
    print(f"  {suffix2} feature cols: {len(feat2)}")

    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
    merged.to_csv(output, index=False, quoting=1)
    print(f"\nSaved: {output}")
    print(f"\nNext step:")
    print(f"  python src/train_ordinal.py {output} --deduplicate --filter-barangay Paoay")


def main():
    parser = argparse.ArgumentParser(description="Merge two temporal S2 feature CSVs")
    parser.add_argument("csv1",   help="Primary CSV (e.g. Oct growing-season)")
    parser.add_argument("csv2",   help="Secondary CSV (e.g. Dec late-season)")
    parser.add_argument("--suffix1", default="oct", help="Suffix for csv1 features (default: oct)")
    parser.add_argument("--suffix2", default="dec", help="Suffix for csv2 features (default: dec)")
    parser.add_argument("--output", default="data/processed/field_data_multitemporal.csv",
                        help="Output CSV path")
    args = parser.parse_args()
    merge(args.csv1, args.csv2, args.suffix1, args.suffix2, args.output)


if __name__ == "__main__":
    main()

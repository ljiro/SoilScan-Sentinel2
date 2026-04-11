"""Add SRTM-derived terrain features to the satellite bands dataset.

Fetches elevation for each GPS coordinate from the Open-Elevation API
(no API key required), then computes:
  - elevation_m      : metres above sea level
  - slope_deg        : steepness in degrees
  - aspect_deg       : downhill facing direction (0=N, 90=E, 180=S, 270=W)
  - twi              : Topographic Wetness Index  ln(a / tan(slope))
  - curvature        : plan curvature (concave/convex)
  - northness        : cos(aspect) — proxy for solar radiation
  - eastness         : sin(aspect)

Terrain strongly influences:
  - Nutrient leaching (TWI, slope)
  - Soil moisture retention (TWI, curvature)
  - Solar insolation / microclimate (aspect, northness)
  - Erosion potential (slope, curvature)

Input : data/processed/field_data_with_bands.csv
Output: data/processed/field_data_with_terrain.csv

Run AFTER data_fetcher_copernicus.py, BEFORE train_ordinal.py.
"""

import math
import os
import sys
import time

import numpy as np
import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
INPUT_CSV  = "data/processed/field_data_with_bands.csv"
OUTPUT_CSV = "data/processed/field_data_with_terrain.csv"

# Open-Elevation public API — no key needed, max 100 locations per POST
OPEN_ELEV_URL    = "https://api.open-elevation.com/api/v1/lookup"
BATCH_SIZE       = 100   # points per request
REQUEST_TIMEOUT  = 30    # seconds
RETRY_LIMIT      = 3
RETRY_WAIT       = 5     # seconds between retries

# For TWI we need a contributing area estimate.  Without a full DEM we use
# a 30 m pixel area (SRTM resolution) as the unit catchment area.
SRTM_RES_M = 30.0

TERRAIN_COLS = [
    "elevation_m",
    "slope_deg",
    "aspect_deg",
    "twi",
    "curvature",
    "northness",
    "eastness",
]


# ---------------------------------------------------------------------------
# Elevation fetch helpers
# ---------------------------------------------------------------------------

def _fetch_elevation_batch(coords: list[tuple[float, float]]) -> list[float | None]:
    """POST up to BATCH_SIZE (lat, lon) pairs; returns list of elevations (m)."""
    locations = [{"latitude": lat, "longitude": lon} for lat, lon in coords]
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = requests.post(
                OPEN_ELEV_URL,
                json={"locations": locations},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                return [r.get("elevation") for r in results]
            print(f"      HTTP {resp.status_code} on attempt {attempt}")
        except Exception as exc:
            print(f"      Request error attempt {attempt}: {exc}")
        if attempt < RETRY_LIMIT:
            time.sleep(RETRY_WAIT)
    return [None] * len(coords)


def fetch_elevations(lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """Fetch elevation for all points in batches. Returns float array (NaN on failure)."""
    n = len(lats)
    elevations = np.full(n, np.nan)
    pairs = list(zip(lats, lons))

    for start in range(0, n, BATCH_SIZE):
        batch = pairs[start : start + BATCH_SIZE]
        end   = start + len(batch)
        results = _fetch_elevation_batch(batch)
        for i, val in enumerate(results):
            elevations[start + i] = val if val is not None else np.nan

        done = end
        print(f"   Elevation: {done}/{n} fetched", end="\r")
        if end < n:
            time.sleep(0.5)  # be polite to the free API

    print()
    return elevations


# ---------------------------------------------------------------------------
# Terrain derivative computation
# ---------------------------------------------------------------------------

def _deg_to_rad(degrees: float) -> float:
    return degrees * math.pi / 180.0


def _compute_slope_aspect(
    elevations: np.ndarray,
    lats: np.ndarray,
    lons: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Estimate per-point slope and aspect using nearest-neighbour finite difference.

    For each point i, we find its closest neighbour j and approximate
    dz/dh where h is the horizontal distance in metres.  This is a rough
    approximation for scattered points (not a gridded DEM), but it captures
    the broad landscape position — sufficient for a machine-learning feature.
    """
    n = len(elevations)
    slopes  = np.full(n, np.nan)
    aspects = np.full(n, np.nan)

    # Approximate metres per degree at Philippine latitude (~16–17°N)
    # 1° lat ≈ 111 km, 1° lon ≈ 111 km × cos(lat)
    lat_m = 111_000.0

    for i in range(n):
        if np.isnan(elevations[i]):
            continue

        dlat = (lats - lats[i]) * lat_m
        dlon = (lons - lons[i]) * lat_m * math.cos(_deg_to_rad(lats[i]))
        dist = np.sqrt(dlat**2 + dlon**2)

        # Exclude self and points further than 2 km
        mask = (dist > 1) & (dist < 2000) & ~np.isnan(elevations)
        if not mask.any():
            continue

        j     = np.argmin(np.where(mask, dist, np.inf))
        dz    = elevations[j] - elevations[i]
        horiz = dist[j]
        if horiz < 1:
            continue

        slope_rad = math.atan(abs(dz) / horiz)
        slopes[i] = math.degrees(slope_rad)

        # Aspect: bearing from i to j (degrees from N, clockwise)
        aspect_rad = math.atan2(dlon[j], dlat[j])
        aspect_deg = (math.degrees(aspect_rad) + 360) % 360
        # If terrain descends towards j, aspect faces that direction
        if dz < 0:
            aspects[i] = aspect_deg
        else:
            aspects[i] = (aspect_deg + 180) % 360

    return slopes, aspects


def compute_terrain_features(
    elevations: np.ndarray,
    lats: np.ndarray,
    lons: np.ndarray,
) -> pd.DataFrame:
    """Return a DataFrame with all terrain columns."""
    slopes, aspects = _compute_slope_aspect(elevations, lats, lons)

    # TWI = ln(a / tan(slope)), where a = unit catchment area (SRTM pixel)
    slope_rad   = np.radians(np.where(slopes < 0.1, 0.1, slopes))  # avoid tan(0)
    unit_area   = SRTM_RES_M ** 2
    twi         = np.log(unit_area / np.tan(slope_rad))

    # Plan curvature proxy: use elevation difference to nearest neighbour
    # (positive = convex ridge, negative = concave hollow)
    n    = len(elevations)
    curv = np.full(n, np.nan)
    lat_m = 111_000.0
    for i in range(n):
        if np.isnan(elevations[i]):
            continue
        dlat = (lats - lats[i]) * lat_m
        dlon = (lons - lons[i]) * lat_m * math.cos(_deg_to_rad(lats[i]))
        dist = np.sqrt(dlat**2 + dlon**2)
        mask = (dist > 1) & (dist < 2000) & ~np.isnan(elevations)
        if not mask.any():
            continue
        j = np.argmin(np.where(mask, dist, np.inf))
        # Simple 2nd-order finite difference: (z_j - z_i) / dist^2
        curv[i] = (elevations[j] - elevations[i]) / (dist[j] ** 2 + 1e-9)

    northness = np.cos(np.radians(aspects))
    eastness  = np.sin(np.radians(aspects))

    return pd.DataFrame({
        "elevation_m": elevations,
        "slope_deg":   slopes,
        "aspect_deg":  aspects,
        "twi":         twi,
        "curvature":   curv,
        "northness":   northness,
        "eastness":    eastness,
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # 1. Load input
    print(f"1. Loading {INPUT_CSV}...")
    if not os.path.exists(INPUT_CSV):
        print(f"   ERROR: {INPUT_CSV} not found.")
        print("   Run src/data_fetcher_copernicus.py first.")
        sys.exit(1)
    df = pd.read_csv(INPUT_CSV, on_bad_lines="skip")
    print(f"   Loaded {len(df)} rows")

    if "latitude" not in df.columns or "longitude" not in df.columns:
        print("   ERROR: 'latitude' / 'longitude' columns missing.")
        sys.exit(1)

    lats = df["latitude"].to_numpy(dtype=float)
    lons = df["longitude"].to_numpy(dtype=float)

    # 2. Fetch elevations for unique coordinates (saves API calls)
    print("2. Fetching SRTM elevations from Open-Elevation API...")
    unique_coords = pd.DataFrame({"lat": lats, "lon": lons}).drop_duplicates()
    print(f"   {len(unique_coords)} unique locations (of {len(df)} rows)")

    u_lats = unique_coords["lat"].to_numpy()
    u_lons = unique_coords["lon"].to_numpy()
    u_elev = fetch_elevations(u_lats, u_lons)

    n_ok  = int(np.sum(~np.isnan(u_elev)))
    n_bad = int(np.sum( np.isnan(u_elev)))
    print(f"   Elevation: {n_ok} OK, {n_bad} failed (NaN)")

    # 3. Compute terrain derivatives for unique coords
    print("3. Computing terrain features (slope, aspect, TWI, curvature)...")
    terrain_df = compute_terrain_features(u_elev, u_lats, u_lons)
    unique_coords = unique_coords.reset_index(drop=True)
    unique_coords = pd.concat([unique_coords, terrain_df], axis=1)

    # 4. Merge back to full dataset by (lat, lon)
    print("4. Merging terrain features into full dataset...")
    df_out = df.merge(
        unique_coords.rename(columns={"lat": "latitude", "lon": "longitude"}),
        on=["latitude", "longitude"],
        how="left",
    )

    present = [c for c in TERRAIN_COLS if c in df_out.columns]
    missing = [c for c in TERRAIN_COLS if c not in df_out.columns]
    if missing:
        print(f"   WARNING: These terrain cols are missing: {missing}")

    # 5. Save
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    df_out.to_csv(OUTPUT_CSV, index=False, quoting=1)
    print(f"\n5. Saved to {OUTPUT_CSV}")
    print(f"   Shape: {df_out.shape}")
    for col in present:
        vals = df_out[col].dropna()
        if len(vals):
            print(f"   {col:15s}: mean={vals.mean():.2f}, "
                  f"min={vals.min():.2f}, max={vals.max():.2f}")

    print("\nNext step: Run src/train_ordinal.py --data data/processed/field_data_with_terrain.csv")


if __name__ == "__main__":
    main()

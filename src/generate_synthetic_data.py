"""
Generate a synthetic dataset that mirrors the real field data structure.

Use --strength to control how learnable the data is:
  0.0  = pure random noise (no pattern, ~33% accuracy)
  0.5  = moderate signal  (~60% accuracy)
  0.8  = strong signal    (~80% accuracy)  [default]
  1.0  = near-perfect separation (~95%+ accuracy)
"""
import argparse
import os

import numpy as np
import pandas as pd

# ── Real-world anchors ───────────────────────────────────────────────────────
BARANGAYS    = ["Balili", "Paoay", "Alapang", "Betag"]
MUNICIPALITY = {"Balili": "La Trinidad", "Paoay": "La Trinidad",
                "Alapang": "La Trinidad", "Betag": "Atok"}
CROPS        = ["cabbage", "potato", "lettuce", "tomato", "corn",
                "broccoli", "pechay", "carrot", "sayote", "beans"]

TEMP_RANGE   = (8.4,  23.8)
HUMID_RANGE  = (27.0, 84.0)
ALT_RANGE    = (1358, 2442)

NUTRIENT_PROBS = {
    "nitrogen":   [0.30, 0.45, 0.25],   # Low / Medium / High
    "phosphorus": [0.40, 0.35, 0.25],
    "potassium":  [0.25, 0.40, 0.35],
}

# Rapid soil test kit Color Plate pH values (11-point CPR scale).
# Distribution weighted toward slightly acidic highland soils in Benguet.
PH_VALUES = [4.0, 4.4, 4.8, 5.2, 5.4, 5.8, 6.0, 6.4, 6.8, 7.2, 7.6]
PH_PROBS  = [0.04, 0.06, 0.10, 0.13, 0.15, 0.16, 0.14, 0.10, 0.08, 0.03, 0.01]

# For each nutrient, define which bands are diagnostic and how they shift
# per class (Low → index 0, Medium → index 1, High → index 2).
# Values are the TARGET reflectance means for each class.
# The model learns these separations — wider gaps = easier to classify.
BAND_CLASS_MEANS = {
    # band: {nutrient: [Low_mean, Medium_mean, High_mean]}
    "B08": {          # NIR — vegetation density → nitrogen proxy
        "nitrogen":   [ 900, 1600, 2300],
    },
    "B8A": {          # NIR narrow — chlorophyll
        "nitrogen":   [ 900, 1550, 2200],
    },
    "B07": {          # Red edge 3
        "nitrogen":   [1000, 1500, 2100],
    },
    "B05": {          # Red edge 1 — phosphorus / chlorophyll
        "phosphorus": [ 500,  850, 1150],
    },
    "B06": {          # Red edge 2
        "phosphorus": [ 800, 1300, 1900],
    },
    "B11": {          # SWIR 1 — soil moisture / potassium
        "potassium":  [ 600, 1100, 1700],
    },
    "B12": {          # SWIR 2
        "potassium":  [ 250,  580,  950],
    },
    # pH signals across 11 CPR values (4.0 -> 7.6).
    # Red reflectance decreases as pH rises (more alkaline -> denser canopy).
    # 11 values: [4.0, 4.4, 4.8, 5.2, 5.4, 5.8, 6.0, 6.4, 6.8, 7.2, 7.6]
    "B04": {"ph": [700, 660, 620, 580, 550, 510, 480, 440, 400, 360, 320]},
    "B03": {"ph": [380, 420, 460, 500, 530, 570, 610, 660, 720, 790, 860]},
    "B02": {"ph": [340, 375, 410, 450, 480, 515, 550, 600, 660, 720, 780]},
}

# Bands not in BAND_CLASS_MEANS use these base ranges (background noise only)
BAND_BASE_RANGES = {
    "B01": (300,  700),
    "B02": (350,  800),
    "B03": (400,  900),
    "B04": (300,  700),
    "B05": (500, 1200),
    "B06": (800, 2000),
    "B07": (900, 2200),
    "B08": (900, 2500),
    "B8A": (900, 2400),
    "B09": (200,  600),
    "B11": (500, 1800),
    "B12": (200, 1000),
}
BAND_NAMES = list(BAND_BASE_RANGES.keys())


def generate(n_samples: int = 995, seed: int = 42, strength: float = 0.8):
    """
    Generate synthetic data.

    Args:
        n_samples: Number of rows.
        seed:      Random seed for reproducibility.
        strength:  Signal strength 0.0–1.0.
                   Controls how much the class mean dominates over random noise.
                   0.0 = all noise, 1.0 = almost no noise.
    """
    rng = np.random.default_rng(seed)
    strength = float(np.clip(strength, 0.0, 1.0))

    # ── Spatial groups & microclimate ────────────────────────────────────
    barangays      = rng.choice(BARANGAYS, size=n_samples)
    municipalities = [MUNICIPALITY[b] for b in barangays]
    crops          = rng.choice(CROPS, size=n_samples)
    temperature    = rng.uniform(*TEMP_RANGE,  size=n_samples).round(1)
    humidity       = rng.uniform(*HUMID_RANGE, size=n_samples).round(1)
    altitude       = rng.uniform(*ALT_RANGE,   size=n_samples).round(0)

    # ── N / P / K labels (Low / Medium / High) ───────────────────────────
    labels    = {}
    label_idx = {}
    classes   = ["Low", "Medium", "High"]

    for nutrient, probs in NUTRIENT_PROBS.items():
        lab = rng.choice(classes, size=n_samples, p=probs)
        labels[nutrient]    = lab
        label_idx[nutrient] = np.array([{"Low": 0, "Medium": 1, "High": 2}[l]
                                         for l in lab])

    # ── pH label (11-point CPR scale) ────────────────────────────────────
    ph_idx          = rng.choice(len(PH_VALUES), size=n_samples, p=PH_PROBS)
    labels["ph"]    = [PH_VALUES[i] for i in ph_idx]
    label_idx["ph"] = ph_idx

    # ── Sentinel-2 bands ─────────────────────────────────────────────────
    bands = {}
    for band, (lo, hi) in BAND_BASE_RANGES.items():
        band_range = hi - lo

        # Start with pure random baseline
        base = rng.uniform(lo, hi, size=n_samples).astype(float)

        # For each nutrient that has a signal in this band, blend in the
        # class-conditional mean according to `strength`.
        if band in BAND_CLASS_MEANS:
            for nutrient, class_means in BAND_CLASS_MEANS[band].items():
                class_means = np.array(class_means, dtype=float)
                target = class_means[label_idx[nutrient]]  # per-sample target mean

                # Add small noise around the class mean
                noise_std = band_range * (1.0 - strength) * 0.3
                signal    = target + rng.normal(0, noise_std, size=n_samples)

                # Blend: strength=1 → all signal, strength=0 → all base noise
                base = (1.0 - strength) * base + strength * signal

        bands[band] = np.clip(base, lo * 0.7, hi * 1.3).astype(int)

    # ── Assemble ─────────────────────────────────────────────────────────
    df = pd.DataFrame({
        "barangay":         barangays,
        "municipality":     municipalities,
        "crops":            crops,
        "temperature_c":    temperature,
        "humidity_percent": humidity,
        "altitude_m":       altitude,
        **bands,
        **labels,
    })
    return df


def print_summary(df, strength):
    print(f"\nGenerated {len(df)} synthetic samples  (strength={strength})")
    print(f"  Barangays : {df['barangay'].value_counts().to_dict()}")
    for col in ["nitrogen", "phosphorus", "potassium"]:
        print(f"  {col:<12}: {df[col].value_counts().to_dict()}")
    counts = dict(sorted(df["ph"].value_counts().to_dict().items()))
    print(f"  {'ph':<12}: {counts}")
    print(f"\n  Expected accuracy guide:")
    guide = {0.0: "~33%", 0.3: "~45%", 0.5: "~60%",
             0.7: "~72%", 0.8: "~80%", 1.0: "~95%+"}
    closest = min(guide.keys(), key=lambda k: abs(k - strength))
    print(f"    strength={strength:.1f}  =>  {guide[closest]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate synthetic Sentinel-2 + STK dataset."
    )
    parser.add_argument("-n", "--samples", type=int, default=995)
    parser.add_argument("-o", "--output",  default="data/processed/synthetic_test.csv")
    parser.add_argument("--seed",          type=int,   default=42)
    parser.add_argument(
        "--strength", type=float, default=0.8,
        help="Signal strength 0.0–1.0 (default 0.8 → ~80%% expected accuracy).",
    )
    args = parser.parse_args()

    df = generate(n_samples=args.samples, seed=args.seed, strength=args.strength)
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    df.to_csv(args.output, index=False)
    print_summary(df, args.strength)
    print(f"\nSaved to: {args.output}")
    print(f"Run: python src/train_ordinal.py {args.output}")

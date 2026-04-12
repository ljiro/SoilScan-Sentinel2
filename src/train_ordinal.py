"""
Ordinal classification (Low/Medium/High for N/P/K; 11-class CPR scale for pH)
using XGBoost, Random Forest, and SVM with spatial GroupKFold.
Expects a merged dataset: Field Data + Satellite Bands + STK ground truth.
"""
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.stats as st
import xgboost as xgb
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    cohen_kappa_score,
    confusion_matrix,
    f1_score,
    mean_absolute_error,
)
from sklearn.inspection import permutation_importance
from sklearn.model_selection import GroupKFold, StratifiedKFold
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.svm import SVC
from sklearn.utils.class_weight import compute_sample_weight

CLASS_NAMES = ["Low", "Medium", "High"]

# Philippine Bureau of Soils and Water Management STK thresholds
# used to label each ordinal class with its actual concentration range.
NUTRIENT_RANGES = {
    "n":  {
        0: "Low\n(<11 mg/kg)",
        1: "Medium\n(11-20 mg/kg)",
        2: "High\n(>20 mg/kg)",
    },
    "p":  {
        0: "Low\n(<11 mg/kg)",
        1: "Medium\n(11-25 mg/kg)",
        2: "High\n(>25 mg/kg)",
    },
    "k":  {
        0: "Low\n(<78 mg/kg)",
        1: "Medium\n(78-156 mg/kg)",
        2: "High\n(>156 mg/kg)",
    },
    "ph": None,   # pH uses its own CPR scale labels
}

# Short versions for CSV / report columns (no newline)
NUTRIENT_RANGES_SHORT = {
    "n":  {0: "Low (<11 mg/kg)",    1: "Medium (11-20 mg/kg)", 2: "High (>20 mg/kg)"},
    "p":  {0: "Low (<11 mg/kg)",    1: "Medium (11-25 mg/kg)", 2: "High (>25 mg/kg)"},
    "k":  {0: "Low (<78 mg/kg)",    1: "Medium (78-156 mg/kg)",2: "High (>156 mg/kg)"},
    "ph": None,
}

# Average class width in real units — used to translate ordinal MAE into
# interpretable units (mg/kg or pH units) for agronomists.
# Each entry is (avg_step_size, unit_string).
# Derived from Philippine BSWM STK thresholds:
#   N : Low 0-11 (mid 5.5), Medium 11-20 (mid 15.5), High 20-31 (mid 25.5) → step ~10 mg/kg
#   P : Low 0-11 (mid 5.5), Medium 11-25 (mid 18),   High 25-50 (mid 37.5) → step ~16 mg/kg
#   K : Low 0-78 (mid 39),  Medium 78-156 (mid 117),  High 156-234 (mid 195) → step ~78 mg/kg
#   pH: CPR scale step = 0.4 pH units
MAE_REAL_UNITS = {
    "n":  (10.0,  "mg/kg"),
    "p":  (16.0,  "mg/kg"),
    "k":  (78.0,  "mg/kg"),
    "ph": (0.4,   "pH units"),
}

# Small epsilon to avoid division by zero in index calculations
_EPS = 1e-6


class _OrdinalXGBWrapper:
    """Thin wrapper that makes XGBRegressor behave like a classifier for
    ordinal integer labels.

    Using ``reg:squarederror`` preserves the ordinal structure of pH
    (11-class CPR scale) by treating adjacent classes as numerically close,
    unlike ``multi:softprob`` which treats all pairs as equally distant.

    predict() rounds continuous outputs and clips them to [0, n_classes-1].
    """

    def __init__(self, n_classes: int, **kwargs):
        self._n = n_classes
        self._model = xgb.XGBRegressor(objective="reg:squarederror", **kwargs)

    def fit(self, X, y, sample_weight=None):
        self._model.fit(X, y, sample_weight=sample_weight)
        return self

    def predict(self, X):
        raw = self._model.predict(X)
        return np.clip(np.round(raw).astype(int), 0, self._n - 1)

    @property
    def feature_importances_(self):
        return self._model.feature_importances_


def _add_spectral_indices(df):
    """Compute physically meaningful spectral indices from Sentinel-2 bands.

    These capture soil and vegetation signals that raw bands miss:
    - NDVI / EVI / SAVI / MSAVI  : canopy density & biomass
    - NDRE / CHL                 : chlorophyll / nitrogen stress
    - BSI / BI                   : bare soil fraction
    - NDWI / NDMI                : moisture content
    - CI_re                      : crop health via red-edge
    """
    b = {col: df[col].astype(float) for col in
         ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"]}

    indices = {}

    # Vegetation indices
    indices["NDVI"]  = (b["B08"] - b["B04"]) / (b["B08"] + b["B04"] + _EPS)
    indices["EVI"]   = 2.5 * (b["B08"] - b["B04"]) / (b["B08"] + 6*b["B04"] - 7.5*b["B02"] + 1 + _EPS)
    indices["SAVI"]  = 1.5 * (b["B08"] - b["B04"]) / (b["B08"] + b["B04"] + 0.5 + _EPS)
    indices["MSAVI"] = (2*b["B08"] + 1 - np.sqrt(np.maximum((2*b["B08"] + 1)**2 - 8*(b["B08"] - b["B04"]), 0))) / 2

    # Red-edge indices (sensitive to chlorophyll / nitrogen)
    indices["NDRE"]   = (b["B8A"] - b["B05"]) / (b["B8A"] + b["B05"] + _EPS)
    indices["CHL_re"] = (b["B8A"] / b["B05"]) - 1   # Chlorophyll Red-Edge index

    # Soil indices
    indices["BSI"] = ((b["B11"] + b["B04"]) - (b["B08"] + b["B02"])) / \
                     ((b["B11"] + b["B04"]) + (b["B08"] + b["B02"]) + _EPS)
    indices["BI"]  = np.sqrt((b["B04"]**2 + b["B08"]**2) / 2)   # Brightness Index

    # Moisture / water
    indices["NDWI"] = (b["B03"] - b["B08"]) / (b["B03"] + b["B08"] + _EPS)
    indices["NDMI"] = (b["B08"] - b["B11"]) / (b["B08"] + b["B11"] + _EPS)

    return pd.DataFrame(indices, index=df.index)


def deduplicate_gps(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse multiple AgriCapture shots at the same GPS point into one row.

    Multiple shots at the same spot produce identical spectral features
    (same Sentinel-2 pixel) and nearly identical labels — they inflate n
    without adding information.

    Strategy:
    - Label columns (n/p/k/ph) → mode (most frequent raw value per GPS point).
      Using mode avoids the median-of-floats problem where e.g. median(6.4, 6.8)=6.6
      which is not a valid pH step and maps to NaN in ph_mapping.
    - Other numeric columns → median
    - Categorical columns   → first value
    """
    if "latitude" not in df.columns or "longitude" not in df.columns:
        return df

    before = len(df)
    key = ["latitude", "longitude"]
    label_cols = [c for c in ["n", "p", "k", "ph"] if c in df.columns]

    num_cols = [c for c in df.select_dtypes(include="number").columns
                if c not in key and c not in label_cols]
    cat_cols  = [c for c in df.columns
                 if c not in num_cols and c not in key and c not in label_cols]

    agg = {c: "median" for c in num_cols}
    agg.update({c: "first" for c in cat_cols})

    df_dedup = df.groupby(key, as_index=False).agg(agg)

    # Label columns: use mode so we always land on a valid original value
    for col in label_cols:
        modes = (
            df.groupby(key)[col]
            .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0])
            .reset_index()
            .rename(columns={col: f"__{col}_mode"})
        )
        df_dedup = df_dedup.merge(modes, on=key, how="left")
        df_dedup[col] = df_dedup.pop(f"__{col}_mode")

    after = len(df_dedup)
    print(f"   Deduplicated: {before} rows -> {after} unique GPS locations "
          f"(removed {before - after} duplicate shots)")
    return df_dedup


def balance_locations(df: pd.DataFrame, targets: list[str]) -> pd.DataFrame:
    """Undersample so every class has equal representation across barangays.

    For each target and each class, finds the minimum count across locations
    that have at least one sample of that class, then randomly keeps that many
    rows from every location.  Classes that exist in only one location are kept
    as-is with a warning — they remain confounded but aren't removed entirely.

    A small class (< 10 samples per location after capping) is also warned about.
    """
    location_col = next((c for c in ["barangay", "municipality"] if c in df.columns), None)
    if not location_col:
        print("   WARNING: No barangay/municipality column — skipping location balancing.")
        return df

    before = len(df)
    keep_idx = set(df.index)   # start with all rows

    CLASS_LABELS = {0: "Low", 1: "Medium", 2: "High"}

    for target in targets:
        if target not in df.columns:
            continue
        col = pd.to_numeric(df[target], errors="coerce")
        classes = sorted(col.dropna().unique().astype(int))

        for cls in classes:
            cls_mask  = col == cls
            locs      = df.loc[cls_mask, location_col].dropna().unique()
            counts    = {loc: int((cls_mask & (df[location_col] == loc)).sum())
                         for loc in locs}
            have_both = {loc: c for loc, c in counts.items() if c > 0}

            if len(have_both) < 2:
                only_loc = list(have_both.keys())
                print(f"   WARNING [{target.upper()} {CLASS_LABELS.get(cls, cls)}]: "
                      f"only in {only_loc} — keeping all, still confounded.")
                continue

            cap = min(have_both.values())
            if cap < 10:
                print(f"   WARNING [{target.upper()} {CLASS_LABELS.get(cls, cls)}]: "
                      f"cap={cap} is very small — model may be unreliable for this class.")

            for loc, count in have_both.items():
                loc_cls_idx = df.index[cls_mask & (df[location_col] == loc)]
                if count > cap:
                    drop = set(
                        np.random.default_rng(seed=42).choice(
                            loc_cls_idx, size=count - cap, replace=False
                        ).tolist()
                    )
                    keep_idx -= drop

    df_bal = df.loc[sorted(keep_idx)].reset_index(drop=True)
    after  = len(df_bal)
    print(f"   Location-balanced: {before} rows -> {after} rows "
          f"(removed {before - after} surplus samples)")

    # Print resulting distribution
    for target in targets:
        if target not in df_bal.columns:
            continue
        col = pd.to_numeric(df_bal[target], errors="coerce").astype("Int64")
        ct  = pd.crosstab(df_bal[location_col], col)
        ct.columns = [CLASS_LABELS.get(int(c), str(c)) for c in ct.columns]
        print(f"   {target.upper()} after balancing:")
        for loc in ct.index:
            row_str = "  ".join(f"{c}={ct.loc[loc, c]}" for c in ct.columns)
            print(f"     {loc}: {row_str}")

    return df_bal


def load_and_prepare_data(csv_path, deduplicate=False, balance_locs=False):
    """Load unified dataset, engineer spectral indices, prepare features and targets."""
    df = pd.read_csv(csv_path, on_bad_lines="skip")

    if deduplicate:
        print("2a. Deduplicating GPS points...")
        df = deduplicate_gps(df)

    if balance_locs:
        print("2b. Balancing classes across locations...")
        balance_locations(df, ["n", "p", "k"])

    # N / P / K: ordinal 3-class already encoded as 0=Low, 1=Medium, 2=High
    # ph: ordinal 11-class from rapid soil test kit CPR scale
    #   (4.0, 4.4, 4.8, 5.2, 5.4, 5.8, 6.0, 6.4, 6.8, 7.2, 7.6)
    PH_VALUES  = [4.0, 4.4, 4.8, 5.2, 5.4, 5.8, 6.0, 6.4, 6.8, 7.2, 7.6]
    ph_mapping = {v: i for i, v in enumerate(PH_VALUES)}

    npk_targets = ["n", "p", "k"]
    ph_targets  = ["ph"]
    targets     = npk_targets + ph_targets

    for t in npk_targets:
        if t in df.columns:
            df[t] = pd.to_numeric(df[t], errors="coerce")
    for t in ph_targets:
        if t in df.columns:
            df[t] = pd.to_numeric(df[t], errors="coerce").map(ph_mapping)

    spectral_features = [
        "B01", "B02", "B03", "B04", "B05", "B06",
        "B07", "B08", "B8A", "B09", "B11", "B12",
    ]
    # Temporal std features — present when multi-tile compositing was used
    spectral_std_features = [f"{b}_std" for b in spectral_features
                             if f"{b}_std" in df.columns]
    microclimate_features = ["temperature_c", "humidity_percent", "altitude_m"]
    terrain_features = [c for c in [
        "elevation_m", "slope_deg", "aspect_deg",
        "twi", "curvature", "northness", "eastness",
    ] if c in df.columns]
    emit_features = [c for c in df.columns if c.startswith("emit_")]
    clay_features = [c for c in df.columns if c.startswith("clay_")]
    if clay_features:
        print(f"   Clay embeddings detected: {len(clay_features)} dimensions")
    categorical_features  = ["crops"]

    # Compute and attach spectral indices
    idx_df = _add_spectral_indices(df)
    index_features = idx_df.columns.tolist()
    df = pd.concat([df, idx_df], axis=1)

    # Set attrs AFTER concat — pd.concat drops attrs from the input frames
    df.attrs["ph_targets"] = ph_targets
    df.attrs["ph_values"]  = PH_VALUES

    all_numeric = spectral_features + spectral_std_features + microclimate_features + terrain_features + emit_features + clay_features + index_features
    X = df[all_numeric + categorical_features]

    # Group by location to prevent spatial leakage.
    # If _group_id is present (augmented dataset), use it so that all 9 pixels
    # from the same GPS point + season land in the same fold.
    if "_group_id" in df.columns:
        groups = df["_group_id"]
    else:
        groups = df["barangay"].fillna(df["municipality"])

    return (
        df,
        X,
        groups,
        targets,
        all_numeric,
        categorical_features,
    )


def build_pipeline(num_features, cat_features):
    """Preprocessing: impute → scale for numerics, one-hot for categorical.

    SimpleImputer(median) handles NaN in temporal-std features (B*_std = 0
    when only one tile was downloaded for a location).
    """
    num_pipe = Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale",  StandardScaler()),
    ])
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", num_pipe, num_features),
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_features),
        ]
    )
    return preprocessor


def _ci95(values):
    """95% confidence interval using t-distribution (appropriate for small n)."""
    n = len(values)
    if n < 2:
        return 0.0
    mean = np.mean(values)
    se = st.sem(values)
    h = se * st.t.ppf(0.975, df=n - 1)
    return h


def get_feature_names(preprocessor, num_features, cat_features):
    """Return the full ordered list of feature names after preprocessing."""
    ohe = preprocessor.named_transformers_["cat"]
    cat_names = list(ohe.get_feature_names_out(cat_features))
    return num_features + cat_names


def compute_importances(model, model_name, X_test_tr, y_test,
                        preprocessor, num_features, cat_features):
    """
    Return (feature_names, importances_array) for any model type.
    - XGBoost / RandomForest: use built-in feature_importances_ (fast).
    - SVM: use permutation importance on the last-fold test set (slower but valid).
    """
    feature_names = get_feature_names(preprocessor, num_features, cat_features)

    if hasattr(model, "feature_importances_"):
        importances = model.feature_importances_
        if len(importances) != len(feature_names):
            feature_names = [f"f{i}" for i in range(len(importances))]
    else:
        # Permutation importance for SVM
        result = permutation_importance(
            model, X_test_tr, y_test,
            n_repeats=10, random_state=42, n_jobs=-1,
            scoring="accuracy",
        )
        importances = result.importances_mean
        if len(importances) != len(feature_names):
            feature_names = [f"f{i}" for i in range(len(importances))]

    return feature_names, importances


def plot_feature_importance(model, model_name, X_test_tr, y_test,
                            preprocessor, num_features, cat_features,
                            target_col, out_dir, top_n=15):
    """
    Save a horizontal bar chart of top-N feature importances.
    Works for XGBoost, RandomForest (built-in) and SVM (permutation).
    Color-coded by model.
    """
    MODEL_COLORS = {"XGBoost": "#2E74B5", "RandomForest": "#2EA86E", "SVM": "#E05C3A"}
    color = MODEL_COLORS.get(model_name, "steelblue")

    feature_names, importances = compute_importances(
        model, model_name, X_test_tr, y_test, preprocessor, num_features, cat_features)

    idx  = np.argsort(importances)[-top_n:]
    vals = importances[idx]
    names = [feature_names[i] for i in idx]

    fig, ax = plt.subplots(figsize=(8, max(4, top_n * 0.38)))
    bars = ax.barh(names, vals, color=color, alpha=0.85, edgecolor="white")

    # Value labels on each bar
    for bar, v in zip(bars, vals):
        ax.text(bar.get_width() + max(vals) * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{v:.4f}", va="center", fontsize=7.5, color="#333333")

    ax.set_xlabel("Importance  (gain for XGB/RF,  accuracy drop for SVM)", fontsize=9)
    ax.set_title(f"Top {top_n} Features — {target_col.upper()} [{model_name}]",
                 fontsize=11, fontweight="bold")
    ax.set_xlim(0, max(vals) * 1.18)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()

    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"feature_importance_{target_col}_{model_name}.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Feature importance saved: {path}")

    return feature_names, importances


def plot_confusion_matrix(cm, target_col, out_dir, class_names=None):
    """Save a color-coded confusion matrix figure.

    For 3-class (N/P/K) a compact 5x4 grid is used.
    For 11-class pH a larger figure with rotated x-labels and a
    diagonal-emphasis colormap makes the ordinal pattern readable.
    """
    if class_names is None:
        class_names = CLASS_NAMES
    n = len(class_names)
    is_large = n > 5

    # Scale figure size with number of classes
    cell = 0.65 if is_large else 1.2
    fig_w = max(5, n * cell + 1.5)
    fig_h = max(4, n * cell + 1.0)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    # Normalize row-wise so colour shows recall per class, not raw counts
    row_sums = cm.sum(axis=1, keepdims=True).astype(float)
    cm_norm  = np.divide(cm, row_sums, where=row_sums != 0)

    im = ax.imshow(cm_norm, interpolation="nearest",
                   cmap="Blues", vmin=0, vmax=1)
    cbar = plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Recall (row %)", fontsize=8)

    ticks = np.arange(n)
    ax.set_xticks(ticks)
    ax.set_xticklabels(class_names,
                       rotation=45 if is_large else 0,
                       ha="right" if is_large else "center",
                       fontsize=8 if is_large else 10)
    ax.set_yticks(ticks)
    ax.set_yticklabels(class_names, fontsize=8 if is_large else 10)
    ax.set_ylabel("Actual")
    ax.set_xlabel("Predicted")
    ax.set_title(f"Confusion Matrix - {target_col.replace('_', ' ').title()}")

    # Annotate with raw counts; white text on dark cells
    thresh = 0.5
    font_size = 7 if is_large else 9
    for i in range(n):
        for j in range(n):
            ax.text(j, i, str(cm[i, j]),
                    ha="center", va="center", fontsize=font_size,
                    color="white" if cm_norm[i, j] > thresh else "black")

    plt.tight_layout()
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"confusion_matrix_{target_col}.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Confusion matrix plot saved: {path}")


def tune_with_optuna(X_tr, y_tr, X_val, y_val, n_classes: int,
                     is_ph: bool = False, n_trials: int = 50) -> dict:
    """Use Optuna to find best hyperparameters for XGBoost, RF, and FCNN.

    Optimises macro-F1 on a held-out validation split (20% of training data).
    Returns a dict of {model_name: best_params} to override _build_models defaults.

    Args:
        X_tr, y_tr : training features/labels (already preprocessed numpy arrays)
        X_val, y_val : validation features/labels
        n_classes   : number of target classes
        is_ph       : whether target is pH (ordinal regression objective)
        n_trials    : Optuna trials per model (default 50)
    """
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    best_params = {}

    # --- XGBoost ---
    def xgb_objective(trial):
        params = dict(
            max_depth        = trial.suggest_int("max_depth", 3, 12),
            learning_rate    = trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
            n_estimators     = trial.suggest_int("n_estimators", 100, 800),
            subsample        = trial.suggest_float("subsample", 0.5, 1.0),
            colsample_bytree = trial.suggest_float("colsample_bytree", 0.5, 1.0),
            reg_alpha        = trial.suggest_float("reg_alpha", 1e-4, 10.0, log=True),
            reg_lambda       = trial.suggest_float("reg_lambda", 1e-4, 10.0, log=True),
            min_child_weight = trial.suggest_int("min_child_weight", 1, 10),
            gamma            = trial.suggest_float("gamma", 0.0, 1.0),
            random_state     = 42, n_jobs=-1,
        )
        if is_ph:
            m = _OrdinalXGBWrapper(n_classes, eval_metric="rmse", **params)
        else:
            m = xgb.XGBClassifier(
                objective="multi:softprob", num_class=n_classes,
                eval_metric="mlogloss", **params,
            )
        sw = compute_sample_weight("balanced", y_tr)
        m.fit(X_tr, y_tr, sample_weight=sw)
        yp = np.array(m.predict(X_val))
        if yp.ndim == 2:
            yp = np.argmax(yp, axis=1)
        return f1_score(y_val, yp.flatten().astype(int), average="macro", zero_division=0)

    print("  Optuna tuning XGBoost...")
    study = optuna.create_study(direction="maximize")
    study.optimize(xgb_objective, n_trials=n_trials, show_progress_bar=False)
    best_params["XGBoost"] = study.best_params
    print(f"    Best XGBoost macro-F1: {study.best_value:.4f}  params: {study.best_params}")

    # --- Random Forest ---
    def rf_objective(trial):
        m = RandomForestClassifier(
            n_estimators    = trial.suggest_int("n_estimators", 50, 500),
            max_depth       = trial.suggest_int("max_depth", 3, 30),
            min_samples_leaf= trial.suggest_int("min_samples_leaf", 1, 10),
            max_features    = trial.suggest_categorical("max_features", ["sqrt", "log2", 0.5]),
            class_weight    = "balanced",
            random_state    = 42, n_jobs=-1,
        )
        sw = compute_sample_weight("balanced", y_tr)
        m.fit(X_tr, y_tr, sample_weight=sw)
        yp = m.predict(X_val)
        return f1_score(y_val, yp.astype(int), average="macro", zero_division=0)

    print("  Optuna tuning RandomForest...")
    study = optuna.create_study(direction="maximize")
    study.optimize(rf_objective, n_trials=n_trials, show_progress_bar=False)
    best_params["RandomForest"] = study.best_params
    print(f"    Best RF macro-F1: {study.best_value:.4f}  params: {study.best_params}")

    # --- FCNN ---
    def fcnn_objective(trial):
        n_layers = trial.suggest_int("n_layers", 1, 5)
        layers   = tuple(
            trial.suggest_int(f"n_units_l{i}", 8, 128) for i in range(n_layers)
        )
        m = MLPClassifier(
            hidden_layer_sizes = layers,
            activation         = trial.suggest_categorical("activation", ["relu", "tanh"]),
            alpha              = trial.suggest_float("alpha", 1e-5, 1e-1, log=True),
            learning_rate_init = trial.suggest_float("lr", 1e-4, 1e-2, log=True),
            max_iter=500, early_stopping=True,
            validation_fraction=0.1, n_iter_no_change=20,
            random_state=42,
        )
        m.fit(X_tr, y_tr)
        yp = m.predict(X_val)
        return f1_score(y_val, yp.astype(int), average="macro", zero_division=0)

    print("  Optuna tuning FCNN...")
    study = optuna.create_study(direction="maximize")
    study.optimize(fcnn_objective, n_trials=n_trials, show_progress_bar=False)
    best_params["FCNN"] = study.best_params
    print(f"    Best FCNN macro-F1: {study.best_value:.4f}  params: {study.best_params}")

    return best_params


def _apply_optuna_params(models: dict, best_params: dict,
                         n_classes: int, is_ph: bool) -> dict:
    """Rebuild models with Optuna-found hyperparameters."""
    updated = dict(models)

    if "XGBoost" in best_params:
        p = best_params["XGBoost"]
        if is_ph:
            updated["XGBoost"] = _OrdinalXGBWrapper(
                n_classes, eval_metric="rmse", **p, random_state=42, n_jobs=-1)
        else:
            updated["XGBoost"] = xgb.XGBClassifier(
                objective="multi:softprob", num_class=n_classes,
                eval_metric="mlogloss", **p, random_state=42, n_jobs=-1)

    if "RandomForest" in best_params:
        p = best_params["RandomForest"]
        updated["RandomForest"] = RandomForestClassifier(
            **p, class_weight="balanced", random_state=42, n_jobs=-1)

    if "FCNN" in best_params:
        p = best_params["FCNN"]
        n_layers = p["n_layers"]
        layers   = tuple(p[f"n_units_l{i}"] for i in range(n_layers))
        updated["FCNN"] = MLPClassifier(
            hidden_layer_sizes=layers,
            activation=p["activation"],
            alpha=p["alpha"],
            learning_rate_init=p["lr"],
            max_iter=500, early_stopping=True,
            validation_fraction=0.1, n_iter_no_change=20,
            random_state=42,
        )

    return updated


def _build_models(n_classes: int, is_ph: bool = False) -> dict:
    """Return {name: model} for XGBoost, Random Forest, and SVM.

    For pH (``is_ph=True``) XGBoost uses an ordinal regression objective
    (``reg:squarederror`` with rounded predictions) rather than unordered
    multiclass softmax, which better respects the 11-step CPR scale.
    For N/P/K (3 ordered classes) multiclass softmax is used as before.
    """
    _xgb_shared = dict(
        max_depth=6, min_child_weight=3, n_estimators=500, learning_rate=0.03,
        subsample=0.8, colsample_bytree=0.7, colsample_bylevel=0.7,
        reg_alpha=0.1, reg_lambda=1.5, gamma=0.1, random_state=42, n_jobs=-1,
    )
    if is_ph:
        xgb_model = _OrdinalXGBWrapper(n_classes, eval_metric="rmse", **_xgb_shared)
    else:
        xgb_model = xgb.XGBClassifier(
            objective="multi:softprob", num_class=n_classes,
            eval_metric="mlogloss", **_xgb_shared,
        )
    return {
        "XGBoost": xgb_model,
        "RandomForest": RandomForestClassifier(
            n_estimators=500, max_depth=None, min_samples_leaf=2,
            max_features="sqrt", class_weight="balanced",
            random_state=42, n_jobs=-1,
        ),
        "SVM": SVC(
            kernel="rbf", C=10, gamma="scale",
            class_weight="balanced", decision_function_shape="ovr",
            random_state=42,
        ),
        "FCNN": MLPClassifier(
            hidden_layer_sizes=(128, 64, 32),
            activation="relu",
            solver="adam",
            alpha=1e-3,           # L2 regularization (replaces dropout)
            learning_rate_init=1e-3,
            max_iter=500,
            early_stopping=True,
            validation_fraction=0.1,
            n_iter_no_change=20,
            random_state=42,
        ),
    }


def _run_one_model(model, model_name, X_valid, y_valid, groups_valid,
                   preprocessor, gkf, use_smote=False):
    """Run GroupKFold for one model.

    Returns (y_true, y_pred, fold_metrics_dict, last_Xte, last_yte).
    last_Xte / last_yte are the preprocessed test arrays from the final fold —
    used later for permutation importance (SVM) and feature importance plots.
    """
    fold_oa, fold_f1, fold_wf1, fold_kappa, fold_mae = [], [], [], [], []
    all_true, all_pred = [], []
    last_Xte, last_yte = None, None

    for train_idx, test_idx in gkf(X_valid, y_valid, groups_valid):
        Xtr = preprocessor.fit_transform(X_valid.iloc[train_idx])
        Xte = preprocessor.transform(X_valid.iloc[test_idx])
        ytr, yte = y_valid.iloc[train_idx], y_valid.iloc[test_idx]

        if use_smote:
            try:
                from imblearn.over_sampling import SMOTE
                Xtr, ytr = SMOTE(random_state=42).fit_resample(Xtr, ytr)
            except ImportError:
                print("  WARNING: imbalanced-learn not installed — skipping SMOTE. "
                      "Run: pip install imbalanced-learn")

        if len(np.unique(ytr)) < 2:
            # SVM (and some other estimators) crash on single-class folds.
            # Skip this fold — it provides no discriminative signal anyway.
            continue

        sw = compute_sample_weight("balanced", ytr)

        if model_name in ("SVM", "FCNN"):
            # SVM and sklearn MLP do not support sample_weight
            model.fit(Xtr, ytr)
        else:
            model.fit(Xtr, ytr, sample_weight=sw)

        yp = np.array(model.predict(Xte))
        if yp.ndim == 2:   # multi:softprob returns (n, n_classes) in some XGB versions
            yp = np.argmax(yp, axis=1)
        yp = yp.flatten().astype(int)
        yte_arr = np.array(yte.values).flatten().astype(int)
        all_true.extend(yte_arr)
        all_pred.extend(yp)
        fold_oa.append(accuracy_score(yte_arr, yp))
        fold_f1.append(f1_score(yte_arr, yp, average="macro", zero_division=0))
        fold_wf1.append(f1_score(yte_arr, yp, average="weighted", zero_division=0))
        fold_kappa.append(cohen_kappa_score(yte_arr, yp))
        fold_mae.append(mean_absolute_error(yte_arr, yp))
        # Keep the last fold's test split for importance computation
        last_Xte, last_yte = Xte, yte_arr

    return (
        np.array(all_true), np.array(all_pred),
        dict(oa=fold_oa, macro_f1=fold_f1, weighted_f1=fold_wf1,
             kappa=fold_kappa, mae=fold_mae),
        last_Xte, last_yte,
    )


def _print_one_model(model_name, y_true, y_pred, folds,
                     n_classes, class_names, is_ph,
                     class_names_short=None, target_col=None):
    if class_names_short is None:
        class_names_short = class_names

    oa      = accuracy_score(y_true, y_pred)
    mf1     = f1_score(y_true, y_pred, average="macro",    zero_division=0)
    wf1     = f1_score(y_true, y_pred, average="weighted", zero_division=0)
    kappa   = cohen_kappa_score(y_true, y_pred)
    mae_raw = mean_absolute_error(y_true, y_pred)
    mae_d   = mae_raw * 0.4 if is_ph else mae_raw
    unit    = "pH units" if is_ph else "class steps"
    labels  = list(range(n_classes))
    cm      = confusion_matrix(y_true, y_pred, labels=labels)

    kappa_lbl = ("Slight" if kappa < 0.20 else "Fair" if kappa < 0.40
                 else "Moderate" if kappa < 0.60 else "Substantial"
                 if kappa < 0.80 else "Almost Perfect")

    print(f"\n  -- {model_name} --")
    print(f"  {'Metric':<16} {'Mean':>7}  {'Std':>7}  {'95% CI':>12}")
    print(f"  {'-'*46}")
    for lbl, vals in [("OA", folds["oa"]), ("Macro F1", folds["macro_f1"]),
                      ("Weighted F1", folds["weighted_f1"]),
                      ("Kappa", folds["kappa"]), ("MAE", folds["mae"])]:
        m, s, ci = np.mean(vals), np.std(vals), _ci95(vals)
        print(f"  {lbl:<16} {m:>7.4f}  {s:>7.4f}  +/-{ci:>10.4f}")

    # Translate ordinal MAE to real units for interpretability
    real_step, real_unit = MAE_REAL_UNITS.get(target_col, (1.0, unit))
    mae_real = mae_raw * real_step
    mae_real_str = f"  (~{mae_real:.1f} {real_unit})" if not is_ph else ""

    print(f"\n  Pooled: OA={oa:.4f}  MacroF1={mf1:.4f}  "
          f"WF1={wf1:.4f}  Kappa={kappa:.4f}({kappa_lbl})  "
          f"MAE={mae_d:.4f} {unit}{mae_real_str}")

    if not is_ph:
        # Use short names for the text confusion matrix
        col_w = max(16, max(len(n) for n in class_names_short) + 2)
        print(f"\n  Confusion Matrix (rows=Actual, cols=Predicted):")
        print("  " + " " * col_w + "".join(f"  {c:>{col_w}}" for c in class_names_short))
        for i, rn in enumerate(class_names_short):
            print(f"  {rn:>{col_w}}" + "".join(f"  {cm[i,j]:>{col_w}}" for j in range(n_classes)))
    else:
        cor = np.diag(cm).sum()
        ob1 = sum(cm[i,j] for i in range(n_classes)
                  for j in range(n_classes) if abs(i-j) == 1)
        tot = cm.sum()
        print(f"  pH: Exact={cor/tot:.1%}  "
              f"Off+/-1step={ob1/tot:.1%}  "
              f"Off2+={(tot-cor-ob1)/tot:.1%}")

    print(f"\n  Classification Report:")
    print(classification_report(y_true, y_pred, labels=labels,
                                target_names=class_names_short, zero_division=0))
    return {
        "model": model_name, "oa": oa, "macro_f1": mf1, "weighted_f1": wf1,
        "kappa": kappa, "mae": mae_d,
        "mae_real": round(mae_real, 2), "mae_real_unit": real_unit,
        "fold_oa_mean": np.mean(folds["oa"]),       "fold_oa_std": np.std(folds["oa"]),
        "fold_oa_ci95": _ci95(folds["oa"]),
        "fold_macro_f1_mean": np.mean(folds["macro_f1"]),
        "fold_macro_f1_std":  np.std(folds["macro_f1"]),
        "fold_macro_f1_ci95": _ci95(folds["macro_f1"]),
        "fold_kappa_mean": np.mean(folds["kappa"]),  "fold_kappa_std": np.std(folds["kappa"]),
        "fold_kappa_ci95": _ci95(folds["kappa"]),
        "fold_mae_mean": np.mean(folds["mae"]),      "fold_mae_std": np.std(folds["mae"]),
        "fold_mae_ci95": _ci95(folds["mae"]),
    }, cm


def plot_model_comparison(results_by_model, target_col, figures_dir):
    """Grouped bar chart: OA / Macro F1 / Kappa for XGBoost vs RF vs SVM."""
    model_names = list(results_by_model.keys())
    metrics     = ["oa", "macro_f1", "kappa"]
    labels      = ["Overall Accuracy", "Macro F1", "Cohen's Kappa"]
    x = np.arange(len(metrics))
    w = 0.25
    colors = ["steelblue", "seagreen", "tomato"]

    fig, ax = plt.subplots(figsize=(9, 5))
    for i, (name, color) in enumerate(zip(model_names, colors)):
        vals = [results_by_model[name][m] for m in metrics]
        bars = ax.bar(x + i * w, vals, w, label=name, color=color, alpha=0.85)
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.005,
                    f"{v:.3f}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x + w)
    ax.set_xticklabels(labels)
    ax.set_ylim(0, 1.1)
    ax.set_ylabel("Score")
    ax.set_title(f"Model Comparison — {target_col.capitalize()}")
    ax.legend()
    plt.tight_layout()
    os.makedirs(figures_dir, exist_ok=True)
    path = os.path.join(figures_dir, f"model_comparison_{target_col}.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  Model comparison plot saved: {path}")


def train_and_evaluate(df, X, groups, target_col, preprocessor,
                       num_features, cat_features, figures_dir="outputs/figures",
                       use_smote=False, min_groups_for_spatial_cv=4,
                       use_optuna=False, optuna_trials=50):
    """
    Train XGBoost, Random Forest, and SVM with GroupKFold (or StratifiedKFold fallback).
    Prints per-fold + pooled metrics for each model and saves comparison plots.
    Returns (best_model, list_of_metric_dicts).

    When the number of unique spatial groups is below min_groups_for_spatial_cv,
    falls back to 5-fold StratifiedKFold so leave-one-barangay-out doesn't dominate
    results with only 2 groups.
    """
    ph_targets  = df.attrs.get("ph_targets", [])
    ph_values   = df.attrs.get("ph_values",  [])
    is_ph       = target_col in ph_targets
    n_classes   = len(ph_values) if is_ph else 3
    if is_ph:
        class_names = [str(v) for v in ph_values]
    elif target_col in NUTRIENT_RANGES and NUTRIENT_RANGES[target_col]:
        ranges = NUTRIENT_RANGES[target_col]
        class_names = [ranges[i] for i in sorted(ranges)]
    else:
        class_names = CLASS_NAMES

    # Short class names for reports/CSV (no newlines)
    if not is_ph and target_col in NUTRIENT_RANGES_SHORT and NUTRIENT_RANGES_SHORT[target_col]:
        short_ranges = NUTRIENT_RANGES_SHORT[target_col]
        class_names_short = [short_ranges[i] for i in sorted(short_ranges)]
    else:
        class_names_short = class_names

    print(f"\n{'='*60}")
    print(f"  Target: {target_col.upper()}"
          + (f"  [11-class CPR: {ph_values[0]}-{ph_values[-1]}]"
             if is_ph else "  [3-class: Low / Medium / High]"))
    print(f"{'='*60}")

    valid_idx    = df[target_col].notna()
    X_valid      = X[valid_idx]
    y_valid      = df.loc[valid_idx, target_col].astype(int)
    groups_valid = groups[valid_idx]

    n_groups = groups_valid.nunique()
    if n_groups < min_groups_for_spatial_cv:
        print(f"  Note: only {n_groups} spatial group(s) — falling back to "
              f"5-fold StratifiedKFold (too few groups for meaningful GroupKFold).")
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        # StratifiedKFold.split does not use groups
        splitter = lambda X, y, g: cv.split(X, y)
    else:
        n_splits = min(5, n_groups)
        if n_splits < 5:
            print(f"  Note: {n_groups} unique groups — using {n_splits}-fold GroupKFold.")
        cv = GroupKFold(n_splits=n_splits)
        splitter = lambda X, y, g: cv.split(X, y, groups=g)
    gkf = splitter  # passed into _run_one_model as the split callable

    models           = _build_models(n_classes, is_ph=is_ph)

    if use_optuna:
        print(f"\n  Running Optuna hyperparameter search ({optuna_trials} trials per model)...")
        # Use a 80/20 split of the training data for Optuna validation
        from sklearn.model_selection import train_test_split
        Xnp  = preprocessor.fit_transform(X_valid)
        X_ot, X_ov, y_ot, y_ov = train_test_split(
            Xnp, y_valid.to_numpy(), test_size=0.2,
            stratify=y_valid.to_numpy(), random_state=42,
        )
        best_params = tune_with_optuna(
            X_ot, y_ot, X_ov, y_ov,
            n_classes=n_classes, is_ph=is_ph, n_trials=optuna_trials,
        )
        models = _apply_optuna_params(models, best_params, n_classes, is_ph)

    results_by_model = {}
    cms              = {}
    importances_by_model = {}   # model_name -> (feature_names, importances_array)
    best_model, best_oa = None, -1

    for model_name, model in models.items():
        print(f"\n  Training {model_name}...")
        y_true, y_pred, folds, last_Xte, last_yte = _run_one_model(
            model, model_name, X_valid, y_valid, groups_valid, preprocessor, gkf,
            use_smote=use_smote)
        metrics_dict, cm = _print_one_model(
            model_name, y_true, y_pred, folds,
            n_classes, class_names, is_ph,
            class_names_short=class_names_short,
            target_col=target_col)
        metrics_dict["target"] = target_col
        results_by_model[model_name] = metrics_dict
        cms[model_name] = cm
        if metrics_dict["oa"] > best_oa:
            best_oa, best_model = metrics_dict["oa"], model

        # Feature importance for every model
        print(f"  Computing feature importance for {model_name}...")
        feat_names, imps = plot_feature_importance(
            model, model_name, last_Xte, last_yte,
            preprocessor, num_features, cat_features,
            target_col, figures_dir,
        )
        importances_by_model[model_name] = (feat_names, imps)

    best_name = max(results_by_model, key=lambda n: results_by_model[n]["oa"])
    print(f"\n  Best for {target_col}: {best_name} "
          f"(OA={results_by_model[best_name]['oa']:.4f})")

    plot_model_comparison(results_by_model, target_col, figures_dir)

    # Confusion matrix for every model — use short range labels on figures
    for model_name in results_by_model:
        plot_confusion_matrix(cms[model_name], f"{target_col}_{model_name}",
                              figures_dir, class_names=class_names_short)

    return best_model, list(results_by_model.values()), importances_by_model


def save_summary_table(results, out_dir="outputs"):
    """Save CSV with all models × all targets — paste directly into paper."""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "metrics_summary.csv")
    pd.DataFrame(results).to_csv(path, index=False)
    print(f"\nSummary table saved: {path}")


def print_data_collection_guidance(df: pd.DataFrame, output_dir: str = "outputs") -> None:
    """Print and save a report flagging missing class/location combinations.

    For each soil target (N, P, K) identifies which Low/Medium/High classes
    are absent or severely under-represented at each barangay, and prints
    concrete collection targets (how many new samples are needed to balance).
    """
    CLASS_LABELS = {0: "Low", 1: "Medium", 2: "High"}
    npk_targets  = [t for t in ["n", "p", "k"] if t in df.columns]
    location_col = next((c for c in ["barangay", "municipality"] if c in df.columns), None)

    if not location_col:
        print("  WARNING: No barangay/municipality column found — skipping guidance.")
        return

    locations  = sorted(df[location_col].dropna().unique())
    all_classes = [0, 1, 2]

    lines = []
    sep   = "=" * 66

    lines.append(sep)
    lines.append("  DATA COLLECTION GUIDANCE")
    lines.append("  Missing / under-represented class × location combinations")
    lines.append(sep)

    for target in npk_targets:
        label_name = {
            "n": "Nitrogen (N)   [Low <11 | Medium 11-20 | High >20 mg/kg]",
            "p": "Phosphorus (P) [Low <11 | Medium 11-25 | High >25 mg/kg]",
            "k": "Potassium (K)  [Low <78 | Medium 78-156 | High >156 mg/kg]",
        }[target]
        lines.append(f"\n  Target: {label_name}")
        lines.append("  " + "-" * 62)

        # Cross-tab: rows = location, cols = class
        ct = (
            df.groupby([location_col, target])
            .size()
            .unstack(fill_value=0)
            .reindex(columns=all_classes, fill_value=0)
        )
        ct.columns = [CLASS_LABELS[c] for c in ct.columns]

        # Print current counts
        lines.append(f"  {'Location':<20} {'Low':>8} {'Medium':>8} {'High':>8}  {'Total':>8}")
        lines.append("  " + "-" * 56)
        for loc in locations:
            row = ct.loc[loc] if loc in ct.index else pd.Series({"Low": 0, "Medium": 0, "High": 0})
            total = row.sum()
            lines.append(
                f"  {str(loc):<20} {int(row.get('Low',0)):>8} "
                f"{int(row.get('Medium',0)):>8} {int(row.get('High',0)):>8}  {int(total):>8}"
            )
        total_row = ct.sum()
        lines.append("  " + "-" * 56)
        lines.append(
            f"  {'TOTAL':<20} {int(total_row.get('Low',0)):>8} "
            f"{int(total_row.get('Medium',0)):>8} {int(total_row.get('High',0)):>8}  "
            f"{int(total_row.sum()):>8}"
        )

        # Flag problems
        issues = []
        target_per_class = max(50, int(ct.values.max()))  # aim for at least 50 per cell
        for loc in locations:
            row = ct.loc[loc] if loc in ct.index else pd.Series({"Low": 0, "Medium": 0, "High": 0})
            for cls in ["Low", "Medium", "High"]:
                count = int(row.get(cls, 0))
                if count == 0:
                    issues.append(f"  !! MISSING  — {loc}: {cls} {label_name} (need ~{target_per_class} samples)")
                elif count < 20:
                    issues.append(f"  !! CRITICAL — {loc}: {cls} {label_name} "
                                  f"(only {count} samples, need ~{target_per_class - count} more)")
                elif count < target_per_class // 2:
                    issues.append(f"     LOW      — {loc}: {cls} {label_name} "
                                  f"({count} samples, suggest {target_per_class // 2 - count} more)")

        if issues:
            lines.append("\n  Issues:")
            lines.extend(issues)
        else:
            lines.append("\n  All class × location cells are adequately populated.")

    # Global N note if no High samples exist at all
    if "n" in df.columns and df["n"].max() < 2:
        lines.append(f"\n  {sep}")
        lines.append("  NOTE: Zero 'High N' samples exist anywhere in the dataset.")
        lines.append("  Collect from plots that received heavy urea/ammonium fertilisation")
        lines.append("  within the last 4 weeks (before Sentinel-2 overpass).")

    lines.append(f"\n{sep}\n")
    report = "\n".join(lines)
    print(report)

    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "data_collection_guidance.txt")
    with open(report_path, "w") as f:
        f.write(report)
    print(f"  Guidance saved: {report_path}\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Train XGBoost / Random Forest / SVM for STK + pH ordinal classification."
    )
    parser.add_argument("data_path", nargs="?",
                        default="data/processed/field_data_with_bands.csv")
    parser.add_argument("--figures-dir", default="outputs/figures")
    parser.add_argument("--output-dir",  default="outputs")
    parser.add_argument("--smote", action="store_true",
                        help="Apply SMOTE oversampling on minority classes per fold "
                             "(requires: pip install imbalanced-learn).")
    parser.add_argument("--min-groups-spatial-cv", type=int, default=4, metavar="N",
                        help="Minimum unique spatial groups required to use GroupKFold. "
                             "Falls back to StratifiedKFold when below this threshold (default: 4).")
    parser.add_argument("--deduplicate", action="store_true",
                        help="Collapse multiple AgriCapture shots at the same GPS point "
                             "into one row (median numeric, first categorical) before training.")
    parser.add_argument("--balance-locations", action="store_true",
                        help="Undersample surplus class samples so every class has equal "
                             "representation across barangays, breaking geographic confounds.")
    parser.add_argument("--tune", action="store_true",
                        help="Run Optuna hyperparameter search before training "
                             "(adds ~5-10 min per target but finds better params).")
    parser.add_argument("--optuna-trials", type=int, default=50, metavar="N",
                        help="Number of Optuna trials per model (default: 50).")
    parser.add_argument("--filter-barangay", metavar="NAME",
                        help="Train on a single barangay only (e.g. Paoay).")
    args = parser.parse_args()

    if not os.path.isfile(args.data_path):
        print(f"Data file not found: {args.data_path}")
        exit(1)

    df, X, groups, targets, num_feat, cat_feat = load_and_prepare_data(
        args.data_path, deduplicate=args.deduplicate,
        balance_locs=args.balance_locations,
    )

    if args.filter_barangay:
        loc_col = next((c for c in ["barangay", "municipality"] if c in df.columns), None)
        if loc_col:
            before = len(df)
            df = df[df[loc_col].str.lower() == args.filter_barangay.lower()].reset_index(drop=True)
            X = X.loc[df.index] if hasattr(X, 'loc') else X[df.index]
            groups = groups.loc[df.index] if hasattr(groups, 'loc') else groups[df.index]
            X = X.reset_index(drop=True)
            groups = groups.reset_index(drop=True)
            print(f"Filtered to {args.filter_barangay}: {before} -> {len(df)} rows")
    print_data_collection_guidance(df, output_dir=args.output_dir)
    preprocessor = build_pipeline(num_feat, cat_feat)

    all_results       = []
    all_importances   = {}   # (target, model) -> (feat_names, imps)

    for t in targets:
        if t not in df.columns:
            continue
        _, metrics_list, imp_by_model = train_and_evaluate(
            df, X, groups, t, preprocessor,
            num_feat, cat_feat, figures_dir=args.figures_dir,
            use_smote=args.smote,
            min_groups_for_spatial_cv=args.min_groups_spatial_cv,
            use_optuna=args.tune,
            optuna_trials=args.optuna_trials,
        )
        all_results.extend(metrics_list)
        for model_name, (feat_names, imps) in imp_by_model.items():
            all_importances[(t, model_name)] = (feat_names, imps)

    if all_results:
        save_summary_table(all_results, out_dir=args.output_dir)

    # Save importances as CSV so plot_pubmat can load them without re-training
    if all_importances:
        rows = []
        for (tgt, mdl), (feat_names, imps) in all_importances.items():
            for fn, iv in zip(feat_names, imps):
                rows.append({"target": tgt, "model": mdl,
                             "feature": fn, "importance": iv})
        imp_df = pd.DataFrame(rows)
        imp_path = os.path.join(args.output_dir, "feature_importances.csv")
        os.makedirs(args.output_dir, exist_ok=True)
        imp_df.to_csv(imp_path, index=False)
        print(f"\nFeature importances saved: {imp_path}")

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
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    cohen_kappa_score,
    confusion_matrix,
    f1_score,
    mean_absolute_error,
)
from sklearn.inspection import permutation_importance
from sklearn.model_selection import GroupKFold
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.svm import SVC
from sklearn.utils.class_weight import compute_sample_weight

CLASS_NAMES = ["Low", "Medium", "High"]

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


def load_and_prepare_data(csv_path):
    """Load unified dataset, engineer spectral indices, prepare features and targets."""
    df = pd.read_csv(csv_path)

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
    microclimate_features = ["temperature_c", "humidity_percent", "altitude_m"]
    categorical_features  = ["crops"]

    # Compute and attach spectral indices
    idx_df = _add_spectral_indices(df)
    index_features = idx_df.columns.tolist()
    df = pd.concat([df, idx_df], axis=1)

    # Set attrs AFTER concat — pd.concat drops attrs from the input frames
    df.attrs["ph_targets"] = ph_targets
    df.attrs["ph_values"]  = PH_VALUES

    all_numeric = spectral_features + microclimate_features + index_features
    X = df[all_numeric + categorical_features]

    # Group by location to prevent spatial leakage
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
    """Preprocessing: standard scaling for numerics, one-hot for categorical."""
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), num_features),
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
    }


def _run_one_model(model, model_name, X_valid, y_valid, groups_valid,
                   preprocessor, gkf):
    """Run GroupKFold for one model.

    Returns (y_true, y_pred, fold_metrics_dict, last_Xte, last_yte).
    last_Xte / last_yte are the preprocessed test arrays from the final fold —
    used later for permutation importance (SVM) and feature importance plots.
    """
    fold_oa, fold_f1, fold_wf1, fold_kappa, fold_mae = [], [], [], [], []
    all_true, all_pred = [], []
    last_Xte, last_yte = None, None

    for train_idx, test_idx in gkf.split(X_valid, y_valid, groups=groups_valid):
        Xtr = preprocessor.fit_transform(X_valid.iloc[train_idx])
        Xte = preprocessor.transform(X_valid.iloc[test_idx])
        ytr, yte = y_valid.iloc[train_idx], y_valid.iloc[test_idx]
        sw = compute_sample_weight("balanced", ytr)

        if model_name == "SVM":
            model.fit(Xtr, ytr)
        else:
            model.fit(Xtr, ytr, sample_weight=sw)

        yp = model.predict(Xte)
        all_true.extend(yte.values)
        all_pred.extend(yp)
        fold_oa.append(accuracy_score(yte, yp))
        fold_f1.append(f1_score(yte, yp, average="macro", zero_division=0))
        fold_wf1.append(f1_score(yte, yp, average="weighted", zero_division=0))
        fold_kappa.append(cohen_kappa_score(yte, yp))
        fold_mae.append(mean_absolute_error(yte, yp))
        # Keep the last fold's test split for importance computation
        last_Xte, last_yte = Xte, yte.values

    return (
        np.array(all_true), np.array(all_pred),
        dict(oa=fold_oa, macro_f1=fold_f1, weighted_f1=fold_wf1,
             kappa=fold_kappa, mae=fold_mae),
        last_Xte, last_yte,
    )


def _print_one_model(model_name, y_true, y_pred, folds,
                     n_classes, class_names, is_ph):
    oa      = accuracy_score(y_true, y_pred)
    mf1     = f1_score(y_true, y_pred, average="macro",    zero_division=0)
    wf1     = f1_score(y_true, y_pred, average="weighted", zero_division=0)
    kappa   = cohen_kappa_score(y_true, y_pred)
    mae_raw = mean_absolute_error(y_true, y_pred)
    mae_d   = mae_raw * 0.4 if is_ph else mae_raw
    unit    = "pH units" if is_ph else "classes"
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

    print(f"\n  Pooled: OA={oa:.4f}  MacroF1={mf1:.4f}  "
          f"WF1={wf1:.4f}  Kappa={kappa:.4f}({kappa_lbl})  "
          f"MAE={mae_d:.4f} {unit}")

    if not is_ph:
        print(f"\n  Confusion Matrix:")
        hdr = f"  {'':>10}" + "".join(f"  {c:>8}" for c in class_names)
        print(hdr)
        for i, rn in enumerate(class_names):
            print(f"  {rn:>10}" + "".join(f"  {cm[i,j]:>8}" for j in range(n_classes)))
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
                                target_names=class_names, zero_division=0))
    return {
        "model": model_name, "oa": oa, "macro_f1": mf1, "weighted_f1": wf1,
        "kappa": kappa, "mae": mae_d,
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
                       num_features, cat_features, figures_dir="outputs/figures"):
    """
    Train XGBoost, Random Forest, and SVM with GroupKFold.
    Prints per-fold + pooled metrics for each model and saves comparison plots.
    Returns (best_model, list_of_metric_dicts).
    """
    ph_targets  = df.attrs.get("ph_targets", [])
    ph_values   = df.attrs.get("ph_values",  [])
    is_ph       = target_col in ph_targets
    n_classes   = len(ph_values) if is_ph else 3
    class_names = [str(v) for v in ph_values] if is_ph else CLASS_NAMES

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
    n_splits = min(5, n_groups)
    if n_splits < 5:
        print(f"  Note: {n_groups} unique groups — using {n_splits}-fold.")
    gkf = GroupKFold(n_splits=n_splits)

    models           = _build_models(n_classes, is_ph=is_ph)
    results_by_model = {}
    cms              = {}
    importances_by_model = {}   # model_name -> (feature_names, importances_array)
    best_model, best_oa = None, -1

    for model_name, model in models.items():
        print(f"\n  Training {model_name}...")
        y_true, y_pred, folds, last_Xte, last_yte = _run_one_model(
            model, model_name, X_valid, y_valid, groups_valid, preprocessor, gkf)
        metrics_dict, cm = _print_one_model(
            model_name, y_true, y_pred, folds,
            n_classes, class_names, is_ph)
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

    # Confusion matrix for every model
    for model_name in results_by_model:
        plot_confusion_matrix(cms[model_name], f"{target_col}_{model_name}",
                              figures_dir, class_names=class_names)

    return best_model, list(results_by_model.values()), importances_by_model


def save_summary_table(results, out_dir="outputs"):
    """Save CSV with all models × all targets — paste directly into paper."""
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "metrics_summary.csv")
    pd.DataFrame(results).to_csv(path, index=False)
    print(f"\nSummary table saved: {path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Train XGBoost / Random Forest / SVM for STK + pH ordinal classification."
    )
    parser.add_argument("data_path", nargs="?",
                        default="data/external/final_merged_data_cleaned.csv")
    parser.add_argument("--figures-dir", default="outputs/figures")
    parser.add_argument("--output-dir",  default="outputs")
    args = parser.parse_args()

    if not os.path.isfile(args.data_path):
        print(f"Data file not found: {args.data_path}")
        exit(1)

    df, X, groups, targets, num_feat, cat_feat = load_and_prepare_data(args.data_path)
    preprocessor = build_pipeline(num_feat, cat_feat)

    all_results       = []
    all_importances   = {}   # (target, model) -> (feat_names, imps)

    for t in targets:
        if t not in df.columns:
            continue
        _, metrics_list, imp_by_model = train_and_evaluate(
            df, X, groups, t, preprocessor,
            num_feat, cat_feat, figures_dir=args.figures_dir,
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

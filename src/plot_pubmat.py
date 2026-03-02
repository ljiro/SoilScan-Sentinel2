"""
Publication-ready summary figures for SoilScan Sentinel-2.

Generates four figures from metrics_summary.csv:
  1. Summary heatmap   — models x targets, coloured by Overall Accuracy
  2. Radar chart       — per-model polygon across all targets and metrics
  3. Multi-panel bar   — per-target grouped bars (OA / Macro F1 / Kappa)
  4. Confusion matrix grid — best-model CM for each target in one figure

Usage:
    python src/plot_pubmat.py
    python src/plot_pubmat.py --metrics outputs/real/metrics_summary.csv
                              --cm-dir   outputs/real/figures
                              --out-dir  outputs/real/pubmat
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
from matplotlib.patches import FancyBboxPatch
from matplotlib.colors import LinearSegmentedColormap

# ── Palette ───────────────────────────────────────────────────────────────────
MODEL_COLORS  = {"XGBoost": "#2E74B5", "RandomForest": "#2EA86E", "SVM": "#E05C3A"}
TARGET_LABELS = {
    "nitrogen":   "Nitrogen",
    "phosphorus": "Phosphorus",
    "potassium":  "Potassium",
    "ph":         "pH (CPR)",
}
METRIC_LABELS = {
    "oa":       "Overall\nAccuracy",
    "macro_f1": "Macro\nF1",
    "kappa":    "Cohen's\nKappa",
}
ACCENT = "#1F497D"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _save(fig, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path}")


def _best_per_target(df):
    """Return one row per target — the model with the highest OA."""
    return df.loc[df.groupby("target")["oa"].idxmax()].reset_index(drop=True)


# ── Figure 1 — Summary Heatmap ────────────────────────────────────────────────

def fig_heatmap(df, out_dir):
    """
    Heatmap:  rows = targets,  columns = models,  cell colour = OA.
    Each cell also shows Macro F1 and Kappa as small text.
    """
    targets = [t for t in TARGET_LABELS if t in df["target"].values]
    models  = [m for m in MODEL_COLORS  if m in df["model"].values]

    oa_grid = np.full((len(targets), len(models)), np.nan)
    f1_grid = np.full_like(oa_grid, np.nan)
    ka_grid = np.full_like(oa_grid, np.nan)

    for r, tgt in enumerate(targets):
        for c, mdl in enumerate(models):
            row = df[(df["target"] == tgt) & (df["model"] == mdl)]
            if not row.empty:
                oa_grid[r, c] = row["oa"].values[0]
                f1_grid[r, c] = row["macro_f1"].values[0]
                ka_grid[r, c] = row["kappa"].values[0]

    cmap = LinearSegmentedColormap.from_list(
        "oa_cmap", ["#f7fbff", "#6baed6", "#08306b"])

    fig, ax = plt.subplots(figsize=(9, 4.5))
    im = ax.imshow(oa_grid, cmap=cmap, vmin=0.0, vmax=1.0, aspect="auto")

    cbar = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02)
    cbar.set_label("Overall Accuracy", fontsize=9)

    ax.set_xticks(range(len(models)))
    ax.set_xticklabels(models, fontsize=11, fontweight="bold")
    ax.set_yticks(range(len(targets)))
    ax.set_yticklabels([TARGET_LABELS[t] for t in targets], fontsize=11)
    ax.set_title("Model Performance Summary — Soil Nutrient Prediction",
                 fontsize=13, fontweight="bold", color=ACCENT, pad=12)

    for r in range(len(targets)):
        for c in range(len(models)):
            oa = oa_grid[r, c]
            if np.isnan(oa):
                continue
            text_color = "white" if oa > 0.55 else "black"
            ax.text(c, r - 0.12, f"OA  {oa:.3f}",
                    ha="center", va="center", fontsize=10,
                    fontweight="bold", color=text_color)
            ax.text(c, r + 0.18, f"F1  {f1_grid[r,c]:.3f}   κ  {ka_grid[r,c]:.3f}",
                    ha="center", va="center", fontsize=8, color=text_color)

    ax.set_xticks(np.arange(len(models)) - 0.5, minor=True)
    ax.set_yticks(np.arange(len(targets)) - 0.5, minor=True)
    ax.grid(which="minor", color="white", linewidth=2)
    ax.tick_params(which="minor", bottom=False, left=False)

    _save(fig, os.path.join(out_dir, "pubmat_1_heatmap.png"))


# ── Figure 2 — Radar Chart ────────────────────────────────────────────────────

def fig_radar(df, out_dir):
    """
    Radar chart: one polygon per model.
    Axes = one per (target, metric) combination → gives a rich multi-axis view.
    """
    targets = [t for t in TARGET_LABELS if t in df["target"].values]
    metrics = ["oa", "macro_f1", "kappa"]
    models  = [m for m in MODEL_COLORS if m in df["model"].values]

    # Build axis labels and values
    axis_labels = []
    for tgt in targets:
        for met in metrics:
            axis_labels.append(f"{TARGET_LABELS[tgt]}\n{METRIC_LABELS[met]}")

    N = len(axis_labels)
    angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
    angles += angles[:1]  # close polygon

    fig, ax = plt.subplots(figsize=(9, 9),
                           subplot_kw=dict(polar=True))

    for mdl in models:
        values = []
        for tgt in targets:
            row = df[(df["target"] == tgt) & (df["model"] == mdl)]
            for met in metrics:
                values.append(row[met].values[0] if not row.empty else 0.0)
        values += values[:1]
        ax.plot(angles, values, "o-", linewidth=2,
                label=mdl, color=MODEL_COLORS[mdl])
        ax.fill(angles, values, alpha=0.10, color=MODEL_COLORS[mdl])

    ax.set_thetagrids(np.degrees(angles[:-1]), axis_labels, fontsize=8)
    ax.set_ylim(0, 1)
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(["0.2", "0.4", "0.6", "0.8", "1.0"], fontsize=7)
    ax.set_title("Multi-Metric Radar — XGBoost vs Random Forest vs SVM",
                 fontsize=12, fontweight="bold", color=ACCENT, pad=20)
    ax.legend(loc="upper right", bbox_to_anchor=(1.35, 1.15), fontsize=10)

    _save(fig, os.path.join(out_dir, "pubmat_2_radar.png"))


# ── Figure 3 — Multi-panel Grouped Bar ───────────────────────────────────────

def fig_grouped_bars(df, out_dir):
    """
    4 rows (one per target) x 3 columns (OA / Macro F1 / Kappa).
    Each subplot has 3 bars (one per model).
    """
    targets = [t for t in TARGET_LABELS if t in df["target"].values]
    metrics = list(METRIC_LABELS.keys())
    models  = [m for m in MODEL_COLORS if m in df["model"].values]

    fig, axes = plt.subplots(len(targets), len(metrics),
                             figsize=(11, 3 * len(targets)),
                             sharey=False)
    if len(targets) == 1:
        axes = [axes]

    fig.suptitle("Soil Nutrient Classification — Model Comparison",
                 fontsize=14, fontweight="bold", color=ACCENT, y=1.01)

    x = np.arange(len(models))
    w = 0.55

    for r, tgt in enumerate(targets):
        for c, met in enumerate(metrics):
            ax = axes[r][c]
            vals, errs = [], []
            for mdl in models:
                row = df[(df["target"] == tgt) & (df["model"] == mdl)]
                if row.empty:
                    vals.append(0); errs.append(0)
                else:
                    vals.append(float(row[met].values[0]))
                    # use fold std as error bar if available
                    std_col = met.replace("oa", "fold_oa") \
                                 .replace("macro_f1", "fold_macro_f1") \
                                 .replace("kappa", "fold_kappa") + "_std"
                    errs.append(float(row[std_col].values[0])
                                if std_col in row.columns else 0)

            bars = ax.bar(x, vals, w,
                          color=[MODEL_COLORS[m] for m in models],
                          yerr=errs, capsize=4,
                          error_kw={"elinewidth": 1.2, "ecolor": "gray"})

            for bar, v in zip(bars, vals):
                ax.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 0.01,
                        f"{v:.3f}", ha="center", va="bottom", fontsize=7.5)

            ax.set_ylim(0, 1.12)
            ax.set_xticks(x)
            ax.set_xticklabels(models, fontsize=8, rotation=15, ha="right")
            ax.set_ylabel("Score" if c == 0 else "", fontsize=8)
            ax.yaxis.set_tick_params(labelsize=8)
            ax.axhline(0.7, color="gray", linestyle="--", linewidth=0.8, alpha=0.6)

            if r == 0:
                ax.set_title(METRIC_LABELS[met].replace("\n", " "),
                             fontsize=10, fontweight="bold")
            if c == 0:
                ax.set_ylabel(TARGET_LABELS[tgt], fontsize=10,
                              fontweight="bold", color=ACCENT)

    plt.tight_layout()
    _save(fig, os.path.join(out_dir, "pubmat_3_grouped_bars.png"))


# ── Figure 4 — Confusion Matrix Grid ─────────────────────────────────────────

def fig_cm_grid(df, cm_dir, out_dir):
    """
    One row per target, one column per model.
    Loads the pre-saved CM PNGs and arranges them in a grid with labels.
    """
    import matplotlib.image as mpimg

    targets = [t for t in TARGET_LABELS if t in df["target"].values]
    models  = [m for m in MODEL_COLORS  if m in df["model"].values]

    fig, axes = plt.subplots(len(targets), len(models),
                             figsize=(5.5 * len(models), 5 * len(targets)))
    if len(targets) == 1:
        axes = [axes]

    fig.suptitle("Confusion Matrices — All Targets x All Models",
                 fontsize=14, fontweight="bold", color=ACCENT, y=1.01)

    for r, tgt in enumerate(targets):
        for c, mdl in enumerate(models):
            ax = axes[r][c]
            path = os.path.join(cm_dir, f"confusion_matrix_{tgt}_{mdl}.png")
            if os.path.exists(path):
                img = mpimg.imread(path)
                ax.imshow(img)
            else:
                ax.text(0.5, 0.5, "Not found", ha="center", va="center",
                        transform=ax.transAxes, fontsize=9, color="gray")
            ax.axis("off")
            if r == 0:
                ax.set_title(mdl, fontsize=11, fontweight="bold",
                             color=MODEL_COLORS[mdl], pad=6)
            if c == 0:
                ax.set_ylabel(TARGET_LABELS[tgt], fontsize=11,
                              fontweight="bold", color=ACCENT)
                # ylabel doesn't show on imshow axes — use text instead
                ax.text(-0.08, 0.5, TARGET_LABELS[tgt],
                        transform=ax.transAxes, fontsize=11,
                        fontweight="bold", color=ACCENT,
                        va="center", ha="right", rotation=90)

    plt.tight_layout()
    _save(fig, os.path.join(out_dir, "pubmat_4_cm_grid.png"))


# ── Figure 5 — Best-Model Summary Card ───────────────────────────────────────

def fig_summary_card(df, out_dir):
    """
    Clean single-panel 'results card' showing the best model per target
    with OA / Macro F1 / Kappa as coloured badges — ideal for poster abstract.
    """
    targets = [t for t in TARGET_LABELS if t in df["target"].values]
    best    = _best_per_target(df)

    fig, ax = plt.subplots(figsize=(10, 1.5 * len(targets) + 1.2))
    ax.axis("off")

    ax.set_title("Best Model Per Soil Property",
                 fontsize=14, fontweight="bold", color=ACCENT, pad=10,
                 loc="left", x=0.02)

    col_x    = [0.02, 0.22, 0.40, 0.56, 0.70, 0.84]
    col_hdrs = ["Target", "Best Model", "OA", "Macro F1", "Kappa", "pH MAE / MAE"]
    for cx, hdr in zip(col_x, col_hdrs):
        ax.text(cx, 1.0, hdr, transform=ax.transAxes,
                fontsize=9, fontweight="bold", color="white",
                va="center",
                bbox=dict(boxstyle="round,pad=0.3", facecolor=ACCENT, alpha=0.9))

    for i, tgt in enumerate(targets):
        row = best[best["target"] == tgt]
        if row.empty:
            continue
        row = row.iloc[0]
        y   = 1.0 - (i + 1) * (1.0 / (len(targets) + 1))
        bg  = "#F0F4FA" if i % 2 == 0 else "#FFFFFF"

        ax.add_patch(FancyBboxPatch((0.0, y - 0.06), 1.0, 0.11,
                                   transform=ax.transAxes,
                                   boxstyle="round,pad=0.01",
                                   facecolor=bg, edgecolor="#CCCCCC",
                                   linewidth=0.5, zorder=0))

        mdl_color = MODEL_COLORS.get(row["model"], ACCENT)
        values = [
            (col_x[0], TARGET_LABELS[tgt],        "black"),
            (col_x[1], row["model"],               mdl_color),
            (col_x[2], f"{row['oa']:.3f}",         "#1a6b1a"),
            (col_x[3], f"{row['macro_f1']:.3f}",   "#7B3F00"),
            (col_x[4], f"{row['kappa']:.3f}",      ACCENT),
            (col_x[5], f"{row['mae']:.3f}",        "#555555"),
        ]
        for cx, txt, col in values:
            ax.text(cx, y, txt, transform=ax.transAxes,
                    fontsize=10, fontweight="bold" if cx == col_x[1] else "normal",
                    color=col, va="center")

    plt.tight_layout()
    _save(fig, os.path.join(out_dir, "pubmat_5_summary_card.png"))


# ── Figure 6 — Overall Cross-Nutrient Performance Overview ───────────────────

def fig_overall_performance(df, out_dir):
    """
    Single panel that answers: "How well does each model perform overall?"

    Layout — two side-by-side sub-panels:
      LEFT:  Dot-plot (Cleveland plot) — OA per nutrient per model,
             one row per nutrient, dots coloured by model, connected by
             thin horizontal lines to highlight spread.
      RIGHT: Aggregated bar — mean OA / Macro F1 / Kappa across all
             nutrients per model, with individual-nutrient dots overlaid.

    This is the single figure that best captures both the
    per-nutrient detail and the overall picture simultaneously.
    """
    targets = [t for t in TARGET_LABELS if t in df["target"].values]
    models  = [m for m in MODEL_COLORS  if m in df["model"].values]
    n_tgt   = len(targets)
    metrics = ["oa", "macro_f1", "kappa"]
    met_labels = ["Overall Accuracy", "Macro F1", "Cohen's Kappa"]

    fig = plt.figure(figsize=(14, max(5, 1.1 * n_tgt + 2)))
    gs  = fig.add_gridspec(1, 2, width_ratios=[1.4, 1], wspace=0.38)
    ax_dot = fig.add_subplot(gs[0])
    ax_bar = fig.add_subplot(gs[1])

    fig.suptitle(
        "Overall Model Performance Across Soil Nutrients",
        fontsize=14, fontweight="bold", color=ACCENT, y=1.02
    )

    # ── LEFT: Cleveland dot-plot ──────────────────────────────────────────────
    y_pos = {tgt: n_tgt - 1 - i for i, tgt in enumerate(targets)}

    # Collect per-target best (model, oa) for annotation
    best_per_target = {}   # tgt -> (best_oa, best_model)
    for tgt in targets:
        y = y_pos[tgt]
        vals = []
        for mdl in models:
            row = df[(df["target"] == tgt) & (df["model"] == mdl)]
            if not row.empty:
                vals.append((float(row["oa"].values[0]), mdl))
        if vals:
            lo = min(v for v, _ in vals)
            hi = max(v for v, _ in vals)
            ax_dot.hlines(y, lo, hi, color="#CCCCCC", linewidth=2, zorder=1)
            best_per_target[tgt] = max(vals, key=lambda x: x[0])

    for mdl in models:
        xs, ys = [], []
        for tgt in targets:
            row = df[(df["target"] == tgt) & (df["model"] == mdl)]
            if not row.empty:
                xs.append(float(row["oa"].values[0]))
                ys.append(y_pos[tgt])
        ax_dot.scatter(xs, ys, s=120, color=MODEL_COLORS[mdl],
                       zorder=3, label=mdl, edgecolors="white", linewidths=0.8)
        for x, y, tgt in zip(xs, ys, targets):
            row = df[(df["target"] == tgt) & (df["model"] == mdl)]
            std_val = float(row["fold_oa_std"].values[0]) if not row.empty else 0
            ax_dot.errorbar(x, y, xerr=std_val, fmt="none",
                            ecolor=MODEL_COLORS[mdl], elinewidth=1.2,
                            capsize=3, alpha=0.6, zorder=2)

    # Annotate the best OA dot per nutrient — label to the RIGHT of the dot
    for tgt, (best_oa, best_mdl) in best_per_target.items():
        y = y_pos[tgt]
        ax_dot.annotate(
            f"{best_oa:.3f}  ({best_mdl})",
            xy=(best_oa, y),
            xytext=(best_oa + 0.05, y),
            fontsize=9, fontweight="bold",
            color=MODEL_COLORS[best_mdl],
            va="center", ha="left",
            arrowprops=dict(arrowstyle="->",
                            color=MODEL_COLORS[best_mdl],
                            lw=1.5),
            bbox=dict(boxstyle="round,pad=0.3", fc="white",
                      ec=MODEL_COLORS[best_mdl], alpha=0.95, lw=1.5),
            zorder=6,
        )

    ax_dot.set_yticks(list(y_pos.values()))
    ax_dot.set_yticklabels([TARGET_LABELS[t] for t in targets], fontsize=11)
    ax_dot.set_xlabel("Overall Accuracy  (error bars = fold std)", fontsize=9)
    ax_dot.set_xlim(0, 1.35)
    ax_dot.axvline(0.7, color="#AAAAAA", linestyle="--", linewidth=1, alpha=0.7)
    ax_dot.text(0.705, n_tgt - 0.5, "0.70", color="#AAAAAA",
                fontsize=7.5, va="top")
    ax_dot.set_title("Per-Nutrient Accuracy by Model", fontsize=11,
                     fontweight="bold", color=ACCENT)
    ax_dot.legend(loc="lower right", fontsize=9,
                  framealpha=0.9, edgecolor="#CCCCCC")
    ax_dot.spines["top"].set_visible(False)
    ax_dot.spines["right"].set_visible(False)
    ax_dot.grid(axis="x", linestyle=":", color="#DDDDDD", linewidth=0.8)

    # ── RIGHT: Aggregated bar with nutrient dots overlaid ─────────────────────
    x_pos = np.arange(len(metrics))
    bar_w = 0.22
    offsets = np.linspace(-(len(models)-1)/2, (len(models)-1)/2, len(models)) * bar_w

    for mi, mdl in enumerate(models):
        agg_vals, agg_err = [], []
        nutrient_dots = {met: [] for met in metrics}

        for tgt in targets:
            row = df[(df["target"] == tgt) & (df["model"] == mdl)]
            if row.empty:
                continue
            for met in metrics:
                nutrient_dots[met].append(float(row[met].values[0]))

        for met in metrics:
            v = nutrient_dots[met]
            agg_vals.append(np.mean(v) if v else 0)
            agg_err.append(np.std(v)  if v else 0)

        bars = ax_bar.bar(x_pos + offsets[mi], agg_vals, bar_w,
                          color=MODEL_COLORS[mdl], label=mdl,
                          alpha=0.85, zorder=2,
                          yerr=agg_err, capsize=3,
                          error_kw={"elinewidth": 1.2, "ecolor": "dimgray"})

        # Overlay individual nutrient dots
        for xi, met in enumerate(metrics):
            for v in nutrient_dots[met]:
                ax_bar.scatter(xi + offsets[mi], v, s=28,
                               color=MODEL_COLORS[mdl], zorder=4,
                               edgecolors="white", linewidths=0.5, alpha=0.9)

        # Value labels on top of bars
        for bar, val in zip(bars, agg_vals):
            ax_bar.text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + agg_err[agg_vals.index(val)] + 0.012,
                        f"{val:.3f}", ha="center", va="bottom",
                        fontsize=7.5, color=MODEL_COLORS[mdl], fontweight="bold")

    # Star the best model for each aggregated metric
    for xi, met in enumerate(metrics):
        best_val, best_mdl_name, best_x = -1, None, None
        for mi, mdl in enumerate(models):
            row_subset = df[df["model"] == mdl]
            vals = [float(row_subset[row_subset["target"] == t][met].values[0])
                    for t in targets
                    if not row_subset[row_subset["target"] == t].empty]
            if vals:
                mean_val = np.mean(vals)
                if mean_val > best_val:
                    best_val     = mean_val
                    best_mdl_name = mdl
                    best_x       = xi + offsets[models.index(mdl)]
        if best_mdl_name:
            ax_bar.annotate(
                f"★ {best_val:.3f}",
                xy=(best_x, best_val + 0.015),
                ha="center", va="bottom",
                fontsize=8.5, fontweight="bold",
                color=MODEL_COLORS[best_mdl_name],
                zorder=6,
            )

    ax_bar.set_xticks(x_pos)
    ax_bar.set_xticklabels(met_labels, fontsize=9)
    ax_bar.set_ylim(0, 1.12)
    ax_bar.axhline(0.7, color="#AAAAAA", linestyle="--", linewidth=1, alpha=0.7)
    ax_bar.set_ylabel("Score  (mean across all nutrients)", fontsize=9)
    ax_bar.set_title("Aggregated Metrics  (dots = per-nutrient,  ★ = best)",
                     fontsize=11, fontweight="bold", color=ACCENT)
    ax_bar.legend(fontsize=9, framealpha=0.9, edgecolor="#CCCCCC",
                  loc="upper right")
    ax_bar.spines["top"].set_visible(False)
    ax_bar.spines["right"].set_visible(False)
    ax_bar.grid(axis="y", linestyle=":", color="#DDDDDD", linewidth=0.8, zorder=0)

    plt.tight_layout()
    _save(fig, os.path.join(out_dir, "pubmat_6_overall_performance.png"))


# ── Figure 7 — Feature Importance Comparison Grid ────────────────────────────

def fig_feature_importance_grid(imp_csv, out_dir, top_n=10):
    """
    Grid layout: rows = targets, columns = models.
    Each cell is a horizontal bar chart of the top-N features.
    Loads from feature_importances.csv saved by train_ordinal.py.
    """
    if not os.path.isfile(imp_csv):
        print(f"  feature_importances.csv not found: {imp_csv} — skipping fig 7.")
        return

    df  = pd.read_csv(imp_csv)
    targets = list(TARGET_LABELS.keys())
    targets = [t for t in targets if t in df["target"].values]
    models  = list(MODEL_COLORS.keys())
    models  = [m for m in models  if m in df["model"].values]

    n_rows, n_cols = len(targets), len(models)
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(5.5 * n_cols, 3.8 * n_rows))
    if n_rows == 1:
        axes = [axes]
    if n_cols == 1:
        axes = [[ax] for ax in axes]

    fig.suptitle("Feature Importance — All Nutrients × All Models",
                 fontsize=14, fontweight="bold", color=ACCENT, y=1.01)

    for r, tgt in enumerate(targets):
        for c, mdl in enumerate(models):
            ax = axes[r][c]
            sub = df[(df["target"] == tgt) & (df["model"] == mdl)]
            if sub.empty:
                ax.axis("off")
                continue

            sub = sub.nlargest(top_n, "importance").iloc[::-1]
            color = MODEL_COLORS[mdl]

            bars = ax.barh(sub["feature"], sub["importance"],
                           color=color, alpha=0.85, edgecolor="white")

            # Value labels
            max_v = sub["importance"].max()
            for bar, v in zip(bars, sub["importance"]):
                ax.text(v + max_v * 0.01,
                        bar.get_y() + bar.get_height() / 2,
                        f"{v:.4f}", va="center", fontsize=7, color="#333")

            ax.set_xlim(0, max_v * 1.25)
            ax.tick_params(axis="y", labelsize=8)
            ax.tick_params(axis="x", labelsize=7)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            ax.grid(axis="x", linestyle=":", color="#DDDDDD", linewidth=0.6)

            # Column header (model name) on first row
            if r == 0:
                ax.set_title(mdl, fontsize=11, fontweight="bold",
                             color=color, pad=8)
            # Row label (target name) on first column
            if c == 0:
                ax.set_ylabel(TARGET_LABELS[tgt], fontsize=11,
                              fontweight="bold", color=ACCENT, labelpad=6)
            # X-axis label only on last row
            if r == n_rows - 1:
                imp_type = "Permutation Accuracy Drop" if mdl == "SVM" else "Importance (Gain)"
                ax.set_xlabel(imp_type, fontsize=8)

    plt.tight_layout()
    _save(fig, os.path.join(out_dir, "pubmat_7_feature_importance_grid.png"))


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate publication-ready summary figures from metrics_summary.csv"
    )
    parser.add_argument("--metrics",
                        default="outputs/metrics_summary.csv",
                        help="Path to metrics_summary.csv")
    parser.add_argument("--cm-dir",
                        default="outputs/figures",
                        help="Directory containing confusion_matrix_*.png files")
    parser.add_argument("--imp-csv",
                        default="outputs/feature_importances.csv",
                        help="Path to feature_importances.csv from train_ordinal.py")
    parser.add_argument("--out-dir",
                        default="outputs/pubmat",
                        help="Output directory for pubmat figures")
    args = parser.parse_args()

    if not os.path.isfile(args.metrics):
        print(f"Metrics file not found: {args.metrics}")
        print("Run train_ordinal.py first to generate metrics_summary.csv")
        exit(1)

    df = pd.read_csv(args.metrics)
    # Normalise model name column (may be absent in older runs)
    if "model" not in df.columns:
        df["model"] = "XGBoost"

    print(f"\nLoaded {len(df)} rows from {args.metrics}")
    print(f"Targets : {df['target'].unique().tolist()}")
    print(f"Models  : {df['model'].unique().tolist()}")
    print(f"Output  : {args.out_dir}\n")

    fig_heatmap(df, args.out_dir)
    fig_radar(df, args.out_dir)
    fig_grouped_bars(df, args.out_dir)
    fig_cm_grid(df, args.cm_dir, args.out_dir)
    fig_summary_card(df, args.out_dir)
    fig_overall_performance(df, args.out_dir)
    fig_feature_importance_grid(args.imp_csv, args.out_dir)

    print("\nAll pubmat figures saved.")
    print("Files:")
    for f in sorted(os.listdir(args.out_dir)):
        if f.endswith(".png"):
            print(f"  {os.path.join(args.out_dir, f)}")

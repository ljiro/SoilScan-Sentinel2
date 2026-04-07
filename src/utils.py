"""
Shared utilities for the SoilScan-Sentinel2 pipeline.

Provides:
- get_logger()               — consistent logging across all pipeline scripts
- compute_vegetation_indices() — single source of truth for NDVI / NDRE / GNDVI
"""
import logging
import sys

import numpy as np

# Small epsilon to prevent division by zero in index calculations
_EPS = 1e-6


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger that writes to stdout with a consistent format.

    Calling this multiple times with the same name is safe — handlers are
    added only once, so no duplicate log lines are produced.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
        )
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


def compute_vegetation_indices(df, center_suffix: str = "_5") -> dict:
    """Compute NDVI, NDRE, and GNDVI from Sentinel-2 center-pixel columns.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with band columns named like ``B08_5``, ``B04_5``, etc.
    center_suffix : str
        Column suffix that identifies the center pixel (default ``'_5'``).

    Returns
    -------
    dict of {str: pd.Series}
        Only indices whose required bands are present in *df* are included.
    """
    indices: dict = {}

    def _ndx(band_a: str, band_b: str, name: str) -> None:
        col_a = f"{band_a}{center_suffix}"
        col_b = f"{band_b}{center_suffix}"
        if col_a in df.columns and col_b in df.columns:
            a = df[col_a].astype(float)
            b = df[col_b].astype(float)
            indices[name] = (a - b) / (a + b + _EPS)

    _ndx("B08", "B04", "NDVI")   # Normalized Difference Vegetation Index
    _ndx("B08", "B05", "NDRE")   # Normalized Difference Red-Edge
    _ndx("B08", "B03", "GNDVI")  # Green Normalized Difference Vegetation Index

    return indices

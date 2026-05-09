"""Derived metrics for triplet rows (gain %, duration, stock success rate)."""

from __future__ import annotations

import math
from typing import Tuple

import numpy as np
import pandas as pd


def format_gain_duration(total_seconds: float) -> str:
    """Human-readable span from first candle start to max candle start."""
    if total_seconds is None or (isinstance(total_seconds, float) and math.isnan(total_seconds)):
        return ""
    s = float(total_seconds)
    if s < 0:
        s = abs(s)
    sign = "-" if float(total_seconds) < 0 else ""
    if s < 60:
        return f"{sign}{s:.1f} s"
    if s < 3600:
        return f"{sign}{s / 60:.1f} min"
    if s < 86400:
        return f"{sign}{s / 3600:.1f} h"
    if s < 30 * 86400:
        return f"{sign}{s / 86400:.1f} d"
    mo = 30 * 86400
    return f"{sign}{s / mo:.2f} mo (~30 d units)"


def enrich_triplet_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Adds gain_percentage, gain_duration_seconds, gain_duration (human)."""
    if df.empty:
        return df
    out = df.copy()
    fm = out["first_max_price"].astype(float)
    safe = fm.replace(0.0, np.nan)
    out["gain_percentage"] = (out["max_peak_price"].astype(float) - fm) / safe * 100.0

    t0 = pd.to_datetime(out["first_candle_start"], utc=False)
    t1 = pd.to_datetime(out["max_candle_start"], utc=False)
    delta = t1 - t0
    out["gain_duration_seconds"] = delta.dt.total_seconds()
    out["gain_duration"] = out["gain_duration_seconds"].map(format_gain_duration)
    return out


def stock_success_rate_percent(
    gain_percentages: pd.Series,
    max_gain_threshold: float,
) -> Tuple[float, int, int]:
    """
    Per stock: N = total excursions; M = excursions with gain% > max_gain_threshold (strict).
    Success % = (M / N) × 100 — share of excursions that strictly beat the gain threshold.
    Returns (success_pct, N, M). If N==0, returns (0.0, 0, 0).
    """
    n = int(gain_percentages.shape[0])
    if n == 0:
        return 0.0, 0, 0
    thr = float(max_gain_threshold)
    m = int(((gain_percentages > thr) & gain_percentages.notna()).sum())
    return (m / n) * 100.0, n, m


def ui_triplet_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop last-candle columns for on-screen table only."""
    if df.empty:
        return df
    drop = [
        c
        for c in df.columns
        if c.startswith("last_")
    ]
    return df.drop(columns=drop, errors="ignore")


def gain_row_highlight_styler(ui_df: pd.DataFrame, gain_threshold: float) -> pd.io.formats.style.Styler:
    """
    Emphasize rows where gain_percentage >= gain_threshold: dark green text only
    (same background as other rows — avoids low-contrast filled cells in Streamlit).
    """
    thr = float(gain_threshold)

    def _row_styles(row: pd.Series) -> list[str]:
        if "gain_percentage" not in row.index:
            return [""] * len(row)
        v = row["gain_percentage"]
        try:
            ok = pd.notna(v) and float(v) >= thr
        except (TypeError, ValueError):
            ok = False
        # Dark green readable on light Streamlit theme; no background override.
        cell = "color: #065f46; font-weight: 600" if ok else ""
        return [cell] * len(row)

    return ui_df.style.apply(_row_styles, axis=1)

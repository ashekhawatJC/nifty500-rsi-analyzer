"""OHLCV resampling and interval helpers."""

from __future__ import annotations

import pandas as pd


def resample_ohlcv_minutes(df: pd.DataFrame, target_mins: int) -> pd.DataFrame:
    """Resample OHLCV to target bar size (left label, right-closed bars)."""
    if df.empty:
        return df
    rule = f"{target_mins}min"
    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    cols = [c for c in agg if c in df.columns]
    agg = {c: agg[c] for c in cols}
    out = df.resample(rule, label="left", closed="right").agg(agg).dropna(
        how="any", subset=[c for c in ("open", "high", "low", "close") if c in agg]
    )
    return out

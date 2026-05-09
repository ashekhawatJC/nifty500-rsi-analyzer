"""RSI (Wilder-style) and RSI-threshold pair / triplet analysis."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional

import numpy as np
import pandas as pd


def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """
    RSI using exponential smoothing equivalent to Wilder's smoothing
    (alpha=1/period), common in TA libraries.
    """
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi


def add_candle_bounds(
    df: pd.DataFrame, interval_mins: int
) -> pd.DataFrame:
    """Add candle_start, candle_end (IST wall time if tz-aware index)."""
    out = df.copy()
    out["candle_start"] = out.index
    delta = timedelta(minutes=interval_mins)
    out["candle_end"] = out.index + delta
    out["max_stock_price"] = out["high"]
    return out


@dataclass
class CandleSnapshot:
    candle_start: pd.Timestamp
    candle_end: pd.Timestamp
    rsi: float
    max_stock_price: float


@dataclass
class Triplet:
    pair_index: int
    first: CandleSnapshot
    last: CandleSnapshot
    max_: CandleSnapshot


def find_rsi_threshold_triplets(
    df: pd.DataFrame,
    rsi: pd.Series,
    threshold: float,
    interval_mins: int,
) -> List[Triplet]:
    """
    Pairs: RSI crosses from below threshold to at/above (first candle),
    then from above to at/below (last candle). Between first and last inclusive,
    find the candle with maximum high (max candle).
    """
    if df.empty or rsi.empty:
        return []

    work = add_candle_bounds(df, interval_mins)
    work["rsi"] = rsi

    r = work["rsi"].to_numpy()

    triplets: List[Triplet] = []
    state = "seek_up"
    first_i: Optional[int] = None
    pair_no = 0

    for i in range(1, len(work)):
        prev_r, cur_r = r[i - 1], r[i]
        if np.isnan(prev_r) or np.isnan(cur_r):
            continue

        if state == "seek_up":
            if prev_r < threshold and cur_r >= threshold:
                first_i = i
                state = "seek_down"
        elif state == "seek_down" and first_i is not None:
            if prev_r > threshold and cur_r <= threshold:
                last_i = i
                seg = work.iloc[first_i : last_i + 1]
                if seg.empty:
                    state = "seek_up"
                    first_i = None
                    continue
                j_rel = int(seg["high"].values.argmax())
                max_i = first_i + j_rel

                def snap(k: int) -> CandleSnapshot:
                    row = work.iloc[k]
                    return CandleSnapshot(
                        candle_start=pd.Timestamp(row["candle_start"]),
                        candle_end=pd.Timestamp(row["candle_end"]),
                        rsi=float(row["rsi"]),
                        max_stock_price=float(row["high"]),
                    )

                pair_no += 1
                triplets.append(
                    Triplet(
                        pair_index=pair_no,
                        first=snap(first_i),
                        last=snap(last_i),
                        max_=snap(max_i),
                    )
                )
                first_i = None
                state = "seek_up"

    return triplets


def triplets_to_dataframe(triplets: List[Triplet]) -> pd.DataFrame:
    rows = []
    for t in triplets:
        rows.append(
            {
                "pair": t.pair_index,
                "first_candle_start": t.first.candle_start,
                "first_candle_end": t.first.candle_end,
                "first_rsi": t.first.rsi,
                "first_max_price": t.first.max_stock_price,
                "last_candle_start": t.last.candle_start,
                "last_candle_end": t.last.candle_end,
                "last_rsi": t.last.rsi,
                "last_max_price": t.last.max_stock_price,
                "max_candle_start": t.max_.candle_start,
                "max_candle_end": t.max_.candle_end,
                "max_rsi": t.max_.rsi,
                "max_peak_price": t.max_.max_stock_price,
            }
        )
    return pd.DataFrame(rows)

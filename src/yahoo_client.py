"""Yahoo Finance OHLCV fetch with interval mapping and range limits."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Tuple

import pandas as pd
import yfinance as yf

from src.resample import resample_ohlcv_minutes

# Minutes user can pick; 240 is built by resampling 60m bars.
SUPPORTED_MINUTES = (1, 2, 5, 15, 30, 60, 90, 240)


def minutes_to_yf_interval(mins: int) -> str:
    if mins not in SUPPORTED_MINUTES:
        raise ValueError(
            f"Candle interval must be one of {SUPPORTED_MINUTES} minutes "
            "(Yahoo Finance / resampler limitation)."
        )
    if mins == 240:
        return "60m"
    return f"{mins}m"


@dataclass
class FetchMeta:
    interval: str
    warning: Optional[str]


def validate_intraday_range(
    start: date, end: date, interval_mins: int
) -> Optional[str]:
    days = (end - start).days + 1
    if interval_mins == 1 and days > 7:
        return (
            "1-minute data from Yahoo is typically limited to ~7 days. "
            "Shorten the range or use a larger interval."
        )
    if interval_mins in (2, 5, 15, 30) and days > 60:
        return (
            f"{interval_mins}-minute data is often limited to ~60 days on Yahoo. "
            "Consider shortening the range."
        )
    if interval_mins in (60, 90, 240) and days > 730:
        return "Very long intraday ranges may be truncated by Yahoo Finance."
    return None


def _fetch_raw(
    symbol: str,
    start: date,
    end: date,
    yf_interval: str,
    *,
    auto_adjust: bool = False,
) -> pd.DataFrame:
    end_excl = end + timedelta(days=1)
    t = yf.Ticker(symbol)
    df = t.history(
        start=start.isoformat(),
        end=end_excl.isoformat(),
        interval=yf_interval,
        auto_adjust=auto_adjust,
        prepost=False,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    keep = [c for c in ("open", "high", "low", "close", "volume") if c in df.columns]
    df = df[keep].copy()
    df.index = pd.to_datetime(df.index)
    if df.index.tz is not None:
        df.index = df.index.tz_convert("Asia/Kolkata")
    return df


def fetch_ohlcv(
    symbol: str,
    start: date,
    end: date,
    interval_mins: int,
    *,
    auto_adjust: bool = False,
) -> Tuple[pd.DataFrame, FetchMeta]:
    """
    OHLCV indexed by candle start. For 240m, downloads 60m and resamples to 240m.
    """
    warn = validate_intraday_range(start, end, interval_mins)
    yf_iv = minutes_to_yf_interval(interval_mins)
    df = _fetch_raw(symbol, start, end, yf_iv, auto_adjust=auto_adjust)

    if interval_mins == 240 and not df.empty:
        df = resample_ohlcv_minutes(df, 240)
        meta_iv = "240m (resampled from 60m Yahoo bars)"
    else:
        meta_iv = yf_iv

    return df, FetchMeta(interval=meta_iv, warning=warn)

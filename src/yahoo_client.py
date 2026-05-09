"""Yahoo Finance OHLCV fetch with interval mapping, range limits, chunking, and rate-limit retries."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Optional, Tuple

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
    chunks_fetched: int


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


def _calendar_chunks(start: date, end: date, max_days_inclusive: int) -> List[Tuple[date, date]]:
    """Non-overlapping calendar windows covering [start, end]."""
    if start > end:
        return []
    out: List[Tuple[date, date]] = []
    cur = start
    span = max(1, max_days_inclusive)
    while cur <= end:
        chunk_end = min(end, cur + timedelta(days=span - 1))
        out.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return out


def _chunk_span_days(interval_mins: int) -> int:
    """
    Yahoo/yfinance often truncates long intraday pulls.
    Use conservative calendar chunk sizes (inclusive days per request).
    """
    if interval_mins == 1:
        return 5
    if interval_mins in (2, 5, 15, 30):
        return 55
    if interval_mins in (60, 90, 240):
        return 200
    return 55


def _effective_chunk_span_days(interval_mins: int, multiplier: float) -> int:
    """Scale base chunk length (fewer HTTP calls when multiplier > 1, capped for safety)."""
    m = max(1.0, min(float(multiplier), 2.5))
    scaled = max(1, int(_chunk_span_days(interval_mins) * m))
    if interval_mins == 1:
        return min(scaled, 7)
    if interval_mins in (2, 5, 15, 30):
        return min(scaled, 59)
    return min(scaled, 270)


def _is_rate_limited(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return (
        "too many requests" in msg
        or "rate limit" in msg
        or "429" in msg
    )


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


def _fetch_raw_with_retry(
    symbol: str,
    start: date,
    end: date,
    yf_interval: str,
    *,
    auto_adjust: bool = False,
    max_retries: int = 6,
    base_delay_sec: float = 2.0,
) -> pd.DataFrame:
    """Call Yahoo with exponential backoff on HTTP 429 / rate-limit style errors."""
    delay = float(base_delay_sec)
    for attempt in range(max(1, int(max_retries))):
        try:
            return _fetch_raw(symbol, start, end, yf_interval, auto_adjust=auto_adjust)
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_retries - 1 or not _is_rate_limited(exc):
                raise
            time.sleep(delay + random.uniform(0.0, 0.4))
            delay = min(delay * 1.85, 60.0)


def _merge_chunk_frames(parts: List[pd.DataFrame]) -> pd.DataFrame:
    if not parts:
        return pd.DataFrame()
    df = pd.concat(parts, axis=0)
    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()
    return df


def fetch_ohlcv(
    symbol: str,
    start: date,
    end: date,
    interval_mins: int,
    *,
    auto_adjust: bool = False,
    inter_chunk_delay_sec: float = 0.35,
    rate_limit_retries: int = 6,
    rate_limit_base_delay_sec: float = 2.0,
    chunk_day_multiplier: float = 1.0,
) -> Tuple[pd.DataFrame, FetchMeta]:
    """
    OHLCV indexed by candle start. For 240m, downloads 60m (chunked) then resamples.

    Intraday pulls are **chunked by calendar window** and concatenated so we are less
    likely to miss bars that a single long `history()` call would truncate.

    ``inter_chunk_delay_sec`` adds a short pause between chunk HTTP calls (reduces 429s).
    ``rate_limit_*`` controls retries with backoff when Yahoo returns rate limits.
    ``chunk_day_multiplier`` > 1 uses wider calendar windows (fewer HTTP calls, slightly
    higher truncation risk on long intraday ranges).
    """
    warn = validate_intraday_range(start, end, interval_mins)
    if float(chunk_day_multiplier) > 1.01:
        note = (
            f"Larger Yahoo chunk windows (×{float(chunk_day_multiplier):.2f}); fewer API calls, "
            "slightly higher truncation risk on long intraday spans."
        )
        warn = f"{warn} {note}" if warn else note
    yf_iv = minutes_to_yf_interval(interval_mins)
    span = _effective_chunk_span_days(interval_mins, chunk_day_multiplier)
    windows = _calendar_chunks(start, end, span)

    parts: List[pd.DataFrame] = []
    n_win = len(windows)
    for j, (ws, we) in enumerate(windows):
        part = _fetch_raw_with_retry(
            symbol,
            ws,
            we,
            yf_iv,
            auto_adjust=auto_adjust,
            max_retries=rate_limit_retries,
            base_delay_sec=rate_limit_base_delay_sec,
        )
        if not part.empty:
            parts.append(part)
        if inter_chunk_delay_sec > 0 and j < n_win - 1:
            time.sleep(float(inter_chunk_delay_sec))

    df = _merge_chunk_frames(parts)

    if interval_mins == 240 and not df.empty:
        df = resample_ohlcv_minutes(df, 240)
        meta_iv = "240m (resampled from 60m Yahoo bars, chunked fetch)"
    else:
        meta_iv = yf_iv + (" (chunked fetch)" if n_win > 1 else "")

    return df, FetchMeta(interval=meta_iv, warning=warn, chunks_fetched=n_win)

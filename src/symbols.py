"""Load Nifty 500 Yahoo symbols (.NS for NSE)."""

from __future__ import annotations

import io
import logging
import re
from typing import List

import pandas as pd
import requests

logger = logging.getLogger(__name__)

WIKI_URL = "https://en.wikipedia.org/wiki/NIFTY_500"


def _symbol_to_yahoo(symbol: str) -> str:
    s = str(symbol).strip().upper()
    if s.endswith(".NS") or s.endswith(".BO"):
        return s
    return f"{s}.NS"


def _pick_constituents_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    """Wikipedia often returns numeric column names; pick the large constituents table."""
    best = None
    best_n = 0
    for t in tables:
        if len(t) < 200:
            continue
        if len(t) > best_n:
            best = t
            best_n = len(t)
    if best is None:
        raise ValueError("No large constituents table found on Wikipedia page")
    return best


def _normalize_constituents_df(t: pd.DataFrame) -> pd.DataFrame:
    """Turn Wikipedia quirks into columns including Symbol."""
    t = t.copy()
    # Header row sometimes appears as first data row (columns 0..5)
    first = t.iloc[0].astype(str).str.strip()
    if first.str.contains("Symbol", case=False, na=False).any():
        t = t.iloc[1:].reset_index(drop=True)
    # Map numeric columns by position (Sl.No, Company, Industry, Symbol, ...)
    if list(t.columns) == list(range(len(t.columns))) or all(
        isinstance(c, int) for c in t.columns
    ):
        if t.shape[1] >= 4:
            t = t.rename(columns={t.columns[3]: "Symbol"})
    # If Symbol column missing, try find column named Symbol
    sym_col = None
    for c in t.columns:
        if str(c).strip().lower() == "symbol":
            sym_col = c
            break
    if sym_col is None:
        raise ValueError("Could not resolve Symbol column in constituents table")
    out = t[[sym_col]].copy()
    out.columns = ["Symbol"]
    return out


def load_nifty500_from_wikipedia(timeout: int = 30) -> List[str]:
    """Parse Wikipedia NIFTY 500 table; returns Yahoo tickers with .NS."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; NiftyRSIAnalyzer/1.0)",
    }
    r = requests.get(WIKI_URL, headers=headers, timeout=timeout)
    r.raise_for_status()
    tables = pd.read_html(io.StringIO(r.text))
    t = _pick_constituents_table(tables)
    t = _normalize_constituents_df(t)

    raw = t["Symbol"].dropna().astype(str)
    out: List[str] = []
    for x in raw:
        x = re.sub(r"\s+", "", x)
        if not x or x.lower() == "symbol":
            continue
        out.append(_symbol_to_yahoo(x))
    seen: set[str] = set()
    uniq: List[str] = []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def load_symbols_fallback() -> List[str]:
    """Small fallback list if Wikipedia is unreachable (for smoke tests)."""
    return [
        "RELIANCE.NS",
        "TCS.NS",
        "HDFCBANK.NS",
        "INFY.NS",
        "ICICIBANK.NS",
    ]

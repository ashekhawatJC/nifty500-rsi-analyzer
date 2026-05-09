"""
Streamlit UI: Nifty 500 stock RSI threshold excursion analysis (Yahoo Finance).
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.rsi_engine import (  # noqa: E402
    add_candle_bounds,
    compute_rsi,
    find_rsi_threshold_triplets,
    triplets_to_dataframe,
)
from src.symbols import load_nifty500_from_wikipedia, load_symbols_fallback  # noqa: E402
from src.yahoo_client import SUPPORTED_MINUTES, fetch_ohlcv  # noqa: E402

st.set_page_config(
    page_title="Nifty 500 RSI Analyzer",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data(ttl=86400, show_spinner=False)
def cached_nifty500_symbols() -> list[str]:
    try:
        return load_nifty500_from_wikipedia()
    except Exception:
        return load_symbols_fallback()


def _configured_access_password() -> str | None:
    """Optional shared password: env `ACCESS_PASSWORD` (Render) or Streamlit secrets."""
    env = os.environ.get("ACCESS_PASSWORD", "").strip()
    if env:
        return env
    try:
        if "ACCESS_PASSWORD" in st.secrets:
            p = str(st.secrets["ACCESS_PASSWORD"]).strip()
            return p or None
    except (FileNotFoundError, KeyError, RuntimeError, TypeError):
        pass
    return None


def _ensure_access() -> None:
    """If ACCESS_PASSWORD is set, block the app until the user enters it (session-scoped)."""
    pwd = _configured_access_password()
    if not pwd:
        return
    if st.session_state.get("_access_ok"):
        return
    st.title("Nifty 500 RSI Analyzer")
    st.caption("This deployment is password-protected.")
    entered = st.text_input("Access password", type="password", autocomplete="current-password")
    if st.button("Continue", type="primary"):
        if entered == pwd:
            st.session_state["_access_ok"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()


def main() -> None:
    _ensure_access()

    st.title("Nifty 500 — RSI threshold excursion analyzer")
    st.markdown(
        "Pulls historical candles from **Yahoo Finance**, computes **RSI**, "
        "then finds **pairs** where RSI crosses **up** through your threshold "
        "and later crosses **down**. For each pair it reports the **first**, **last**, "
        "and **highest-high** candle in between."
    )

    with st.sidebar:
        st.header("Inputs")
        sym_list = cached_nifty500_symbols()
        default_sym = "RELIANCE.NS" if "RELIANCE.NS" in sym_list else sym_list[0]
        symbol = st.selectbox(
            "Stock (Nifty 500 universe)",
            options=sym_list,
            index=sym_list.index(default_sym) if default_sym in sym_list else 0,
            help="Symbols use Yahoo suffix `.NS` (NSE). List is loaded from Wikipedia NIFTY 500.",
        )
        override = st.text_input(
            "Override Yahoo symbol (optional)",
            placeholder="e.g. BHARTIARTL.NS",
            help="If set, this ticker is used instead of the dropdown selection.",
        )
        if override.strip():
            symbol = override.strip().upper()
        today = date.today()
        start_d = st.date_input("Start date", value=today - timedelta(days=30))
        end_d = st.date_input("End date", value=today)
        if end_d < start_d:
            st.error("End date must be on or after start date.")
            st.stop()

        candle_mins = st.selectbox(
            "Candle interval (minutes)",
            options=list(SUPPORTED_MINUTES),
            index=list(SUPPORTED_MINUTES).index(15)
            if 15 in SUPPORTED_MINUTES
            else 0,
            help="Yahoo Finance limits very fine intraday history; see warnings after fetch.",
        )
        threshold = st.number_input(
            "RSI threshold",
            min_value=0.0,
            max_value=100.0,
            value=70.0,
            step=0.5,
        )
        rsi_period = st.number_input("RSI period (bars)", min_value=2, max_value=50, value=14)
        run = st.button("Run analysis", type="primary")

    if not run:
        st.info("Set parameters in the sidebar and click **Run analysis**.")
        return

    with st.spinner(f"Downloading {symbol} ({candle_mins}m)…"):
        df, meta = fetch_ohlcv(symbol, start_d, end_d, int(candle_mins))

    if meta.warning:
        st.warning(meta.warning)
    st.caption(f"Yahoo interval / pipeline: **{meta.interval}** — rows: **{len(df)}**")

    if df.empty:
        st.error("No data returned. Try a shorter range, a different interval, or another symbol.")
        return

    rsi = compute_rsi(df["close"], period=int(rsi_period))
    triplets = find_rsi_threshold_triplets(
        df, rsi, float(threshold), int(candle_mins)
    )
    enriched = add_candle_bounds(df, int(candle_mins))
    enriched["rsi"] = rsi

    tab1, tab2, tab3 = st.tabs(["Triplet table", "All candles (RSI + OHLC)", "About"])

    with tab1:
        if not triplets:
            st.warning(
                "No complete threshold excursions in this window "
                "(need RSI to cross **above** then **below** the threshold)."
            )
        else:
            trip_df = triplets_to_dataframe(triplets)
            st.metric("Completed threshold excursions (pairs)", len(trip_df))
            st.dataframe(
                trip_df,
                use_container_width=True,
                hide_index=True,
            )
            csv = trip_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download triplets CSV",
                data=csv,
                file_name=f"{symbol.replace('.', '_')}_rsi_triplets_{start_d}_{end_d}.csv",
                mime="text/csv",
            )

    with tab2:
        show = enriched.reset_index(names="index_time")
        st.dataframe(show, use_container_width=True, height=420)
        st.download_button(
            "Download candles + RSI CSV",
            data=show.to_csv(index=False).encode("utf-8"),
            file_name=f"{symbol.replace('.', '_')}_candles_rsi_{start_d}_{end_d}.csv",
            mime="text/csv",
            key="dl_candles",
        )
        c1, c2 = st.columns(2)
        with c1:
            st.line_chart(
                show.set_index("candle_start")[["close"]].rename(columns={"close": "Close"}),
                height=220,
            )
        with c2:
            rsi_chart = show.set_index("candle_start")[["rsi"]].rename(columns={"rsi": "RSI"})
            st.line_chart(rsi_chart, height=220)

    with tab3:
        st.markdown(
            """
**Flow**

1. OHLCV candles are downloaded for the symbol and date range (Yahoo Finance).
2. RSI is computed on the close series (Wilder-style smoothing, configurable period).
3. A **pair** starts when RSI crosses **from below** to **at/above** the threshold (first candle).
4. The pair ends when RSI later crosses **from above** to **at/below** the threshold (last candle).
5. The **max** candle is the bar between first and last (inclusive) with the **highest high**.

**Notes**

- Yahoo Finance intraday depth is limited (especially 1m). Widen the interval or shorten the window if data is missing.
- **240m** bars are **resampled** from downloaded **60m** Yahoo bars.
- This tool is for education and research, not investment advice.
            """
        )


if __name__ == "__main__":
    main()

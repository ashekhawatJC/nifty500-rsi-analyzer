"""
Streamlit UI: Nifty 500 stock RSI threshold excursion analysis (Yahoo Finance).
"""

from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.metrics import (  # noqa: E402
    enrich_triplet_dataframe,
    gain_row_highlight_styler,
    stock_success_rate_percent,
    ui_triplet_columns,
)
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


def _parse_override_symbols(raw: str) -> list[str]:
    parts = [p.strip().upper() for p in raw.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _analyze_single_stock(
    symbol: str,
    start_d: date,
    end_d: date,
    candle_mins: int,
    threshold: float,
    rsi_period: int,
    max_gain_pct: float,
    success_min_pct: float,
) -> dict:
    """Fetch + RSI + triplets + filters. Thread-safe for I/O-bound Yahoo calls."""
    out: dict = {
        "symbol": symbol,
        "error": None,
        "warning": None,
        "interval": None,
        "chunks": 0,
        "full_df": pd.DataFrame(),
        "display_df": pd.DataFrame(),
        "n_gain_ge_threshold": 0,
        "enriched_candles": pd.DataFrame(),
        "success_pct": 0.0,
        "n_pairs": 0,
        "m_strict_gt": 0,
        "show": False,
    }
    try:
        df, meta = fetch_ohlcv(symbol, start_d, end_d, int(candle_mins))
        out["warning"] = meta.warning
        out["interval"] = meta.interval
        out["chunks"] = meta.chunks_fetched
    except Exception as exc:  # noqa: BLE001
        out["error"] = str(exc)
        return out

    if df.empty:
        out["error"] = "no data"
        return out

    rsi = compute_rsi(df["close"], period=int(rsi_period))
    triplets = find_rsi_threshold_triplets(df, rsi, float(threshold), int(candle_mins))
    enriched = add_candle_bounds(df, int(candle_mins))
    enriched["rsi"] = rsi
    out["enriched_candles"] = enriched

    if not triplets:
        return out

    trip_df = triplets_to_dataframe(triplets)
    trip_df = enrich_triplet_dataframe(trip_df)
    success, n, m = stock_success_rate_percent(trip_df["gain_percentage"], max_gain_pct)
    out["success_pct"] = success
    out["n_pairs"] = n
    out["m_strict_gt"] = m

    gp = trip_df["gain_percentage"]
    out["n_gain_ge_threshold"] = int((gp.notna() & (gp >= float(max_gain_pct))).sum())

    out["full_df"] = trip_df
    out["display_df"] = trip_df.copy()
    out["show"] = bool(n > 0 and success >= success_min_pct)
    return out


def main() -> None:
    _ensure_access()

    st.title("Nifty 500 — RSI threshold excursion analyzer")
    st.markdown(
        "Batch mode: analyzes **every Nifty 500 symbol** (or your **override** list), "
        "pulls **Yahoo Finance** candles using **chunked** intraday requests, computes **RSI**, "
        "and shows **every excursion row** for stocks whose **success rate** clears your filter. "
        "Rows with **gain % ≥ max gain %** are **highlighted** in the table."
    )

    with st.sidebar:
        st.header("Inputs")
        sym_list = cached_nifty500_symbols()
        override_raw = st.text_input(
            "Override Yahoo symbol(s) (optional)",
            placeholder="One ticker or comma-separated, e.g. RELIANCE.NS, TCS.NS",
            help="If empty, all Nifty 500 names are analyzed. If set, only these tickers.",
        )
        override_syms = _parse_override_symbols(override_raw)
        symbols = override_syms if override_syms else sym_list

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
        max_gain_pct = st.number_input(
            "Max gain % (highlight threshold)",
            min_value=-1000.0,
            max_value=1000.0,
            value=0.0,
            step=0.1,
            help="Rows with gain_percentage ≥ this value are highlighted. "
            "Same value is used inside success-rate M = count(gain% > this).",
        )
        success_min_pct = st.number_input(
            "Minimum stock success rate %",
            min_value=0.0,
            max_value=100.0,
            value=0.0,
            step=0.1,
            help="Show the full excursion table for a stock only if its success rate ≥ this. "
            "Success % = ((N − M) / N)×100 with N=all excursions, M=count with gain% > max gain %.",
        )
        workers = st.slider(
            "Parallel download workers",
            min_value=1,
            max_value=12,
            value=5,
            help="Higher = faster but more risk of Yahoo throttling.",
        )
        run = st.button("Run analysis", type="primary")

    if not run:
        st.info("Set parameters in the sidebar and click **Run analysis**.")
        return

    n_sym = len(symbols)
    st.caption(
        f"Analyzing **{n_sym}** symbol(s). "
        f"Intraday history is fetched in **calendar chunks** per symbol to reduce truncation."
    )
    prog = st.progress(0.0, text="Starting…")
    results: list[dict] = []
    done = 0

    max_w = max(1, min(int(workers), n_sym))

    def _one(sym: str) -> dict:
        return _analyze_single_stock(
            sym,
            start_d,
            end_d,
            int(candle_mins),
            float(threshold),
            int(rsi_period),
            float(max_gain_pct),
            float(success_min_pct),
        )

    with ThreadPoolExecutor(max_workers=max_w) as ex:
        futs = {ex.submit(_one, s): s for s in symbols}
        for fut in as_completed(futs):
            results.append(fut.result())
            done += 1
            prog.progress(done / max(n_sym, 1), text=f"Completed {done}/{n_sym}…")

    prog.empty()

    shown = [r for r in results if r.get("show")]
    errors = [r for r in results if r.get("error") and r["error"] != "no data"]
    nodata = [r for r in results if r.get("error") == "no data"]
    no_trips = [r for r in results if not r.get("error") and r["full_df"].empty]

    st.markdown(
        f"**Stocks shown (after filters):** {len(shown)} · "
        f"**No data:** {len(nodata)} · **No complete excursions:** {len(no_trips)} · "
        f"**Fetch errors:** {len(errors)}"
    )

    if errors:
        with st.expander("Fetch errors (sample)"):
            for r in errors[:25]:
                st.text(f"{r['symbol']}: {r['error']}")

    tab1, tab2, tab3 = st.tabs(["Triplet tables", "Candles (single symbol only)", "About"])

    with tab1:
        if not shown:
            st.warning(
                "No stocks met the minimum success rate (with at least one completed excursion). "
                "Try lowering the success rate threshold or adjusting dates / RSI settings."
            )
        else:
            shown.sort(key=lambda x: x["symbol"])
            all_for_csv: list[pd.DataFrame] = []
            for r in shown:
                sym = r["symbol"]
                disp = r["display_df"].copy()
                disp.insert(0, "symbol", sym)
                all_for_csv.append(ui_triplet_columns(disp))

                st.subheader(sym)
                st.caption(
                    f"Yahoo: **{r.get('interval')}** · chunks **{r.get('chunks', 0)}** · "
                    f"success **{r['success_pct']:.2f}%** (N={r['n_pairs']}, M={r['m_strict_gt']}) · "
                    f"highlighted rows (gain % ≥ max gain %): **{r.get('n_gain_ge_threshold', 0)}** / "
                    f"**{len(disp)}** total"
                )
                if r.get("warning"):
                    st.warning(r["warning"])

                ui_df = ui_triplet_columns(disp)
                if ui_df.empty:
                    st.caption("No excursion rows.")
                else:
                    st.caption("Highlighted rows meet **gain % ≥ max gain %**.")
                    st.dataframe(
                        gain_row_highlight_styler(ui_df, float(max_gain_pct)),
                        use_container_width=True,
                        hide_index=True,
                    )
                st.download_button(
                    f"Download CSV — {sym}",
                    data=ui_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{sym.replace('.', '_')}_rsi_rows_{start_d}_{end_d}.csv",
                    mime="text/csv",
                    key=f"dl_{sym}",
                )
                st.divider()

            merged = pd.concat(all_for_csv, ignore_index=True) if all_for_csv else pd.DataFrame()
            if not merged.empty:
                st.download_button(
                    "Download combined CSV (all shown stocks)",
                    data=merged.to_csv(index=False).encode("utf-8"),
                    file_name=f"nifty500_rsi_excursions_{start_d}_{end_d}.csv",
                    mime="text/csv",
                    key="dl_merged",
                )

    with tab2:
        if len(symbols) != 1:
            st.info(
                "Candle + RSI charts are available when exactly **one** symbol is analyzed "
                "(use override with a single ticker)."
            )
        else:
            sym = symbols[0]
            row = next((r for r in results if r["symbol"] == sym), None)
            if not row or row.get("error"):
                st.warning("No candle series for that symbol.")
            else:
                enc = row["enriched_candles"]
                if enc.empty:
                    st.warning("No candle rows.")
                else:
                    show = enc.reset_index(names="index_time")
                    st.dataframe(show, use_container_width=True, height=420)
                    st.download_button(
                        "Download candles + RSI CSV",
                        data=show.to_csv(index=False).encode("utf-8"),
                        file_name=f"{sym.replace('.', '_')}_candles_rsi_{start_d}_{end_d}.csv",
                        mime="text/csv",
                        key="dl_candles",
                    )
                    c1, c2 = st.columns(2)
                    with c1:
                        st.line_chart(
                            show.set_index("candle_start")[["close"]].rename(
                                columns={"close": "Close"}
                            ),
                            height=220,
                        )
                    with c2:
                        rsi_chart = show.set_index("candle_start")[["rsi"]].rename(
                            columns={"rsi": "RSI"}
                        )
                        st.line_chart(rsi_chart, height=220)

    with tab3:
        st.markdown(
            """
**Data loading (Yahoo)**

- Each symbol’s intraday range is downloaded in **multiple calendar windows** (chunks), then merged.
- This reduces **silent truncation** from asking Yahoo for one very long intraday span in a single call.
- Yahoo can still omit data outside its published intraday depth; widen the interval or shorten dates if bars are missing.

**RSI excursions**

1. OHLCV candles for the symbol and date range.
2. RSI on closes (Wilder-style smoothing).
3. A **pair** starts when RSI crosses **from below** to **at/above** the RSI threshold (first candle).
4. The pair ends when RSI crosses **from above** to **at/below** the threshold (last candle).
5. **Max** candle = highest **high** between first and last (inclusive).

**Table columns**

- **gain_percentage** = `(max_peak_price − first_max_price) / first_max_price × 100`.
- **gain_duration** = time from **first_candle_start** to **max_candle_start** (human units).
- **gain_duration_seconds** = same span in seconds (for sorting / export).

**Filters**

- For each stock, **every** completed excursion row is listed once the stock’s **success rate** clears your minimum.
- Rows with **gain_percentage ≥ max gain %** are **highlighted** (amber) in the UI; CSV downloads include all rows without cell colors.
- **Success rate %** for showing a stock:  
  **N** = all completed excursions for that symbol,  
  **M** = excursions with **gain_percentage > max gain %** (strict),  
  **success %** = `((N − M) / N) × 100`.  
  The stock table appears only if **success % ≥ minimum success rate %** and **N ≥ 1**.

**UI**

- Last-candle columns are omitted from on-screen tables (logic still uses them internally).

**Disclaimer:** Educational / research use only; not investment advice.
            """
        )


if __name__ == "__main__":
    main()

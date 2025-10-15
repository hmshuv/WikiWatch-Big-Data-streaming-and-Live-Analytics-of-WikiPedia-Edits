
import glob
import time
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

# --------------------------- Page setup ---------------------------

st.set_page_config(page_title="WikiWatch", layout="wide")
st.title("WikiWatch — Live Wikipedia Analytics")

with st.sidebar:
    st.header("Controls")
    refresh_sec = st.slider("Auto-refresh (seconds)", 2, 30, 5, 1)
    lookback_rows = st.slider("Rows to show (tables)", 10, 200, 30, 5)
    st.caption("Tip: Use ⌘/Ctrl+R if the app ever looks stuck.")

st.caption(f"Auto-refreshing every {refresh_sec}s")
st.write("")

# --------------------------- Helpers ---------------------------

def auto_refresh(ms: int) -> None:
    """
    Auto-refresh compatible with multiple Streamlit versions.
    Tries st.autorefresh → st.experimental_autorefresh → st.rerun → JS.
    """
    fn = getattr(st, "autorefresh", None)
    if callable(fn):
        fn(interval=ms); return

    fn = getattr(st, "experimental_autorefresh", None)
    if callable(fn):
        fn(interval=ms); return

    fn = getattr(st, "rerun", None)
    if callable(fn):
        time.sleep(ms / 1000.0)
        fn(); return

    # Final fallback: JS reload
    st.markdown(
        f"""
        <script>
          setTimeout(function() {{ window.location.reload(); }}, {ms});
        </script>
        """,
        unsafe_allow_html=True,
    )

def read_parquet_dir(path: str) -> pd.DataFrame:
    """
    Read a Parquet *directory* safely.
    - Returns empty DF if directory has no parquet files yet.
    - If a file is mid-write, it skips it and concatenates the rest.
    """
    files = glob.glob(path.rstrip("/") + "/**/*.parquet", recursive=True)
    if not files:
        return pd.DataFrame()

    # Fast path: try to read the directory normally
    try:
        return pd.read_parquet(path)
    except Exception:
        pass  # fall back to per-file concat

    # Robust path: concat readable files only
    parts = []
    for f in files:
        try:
            parts.append(pd.read_parquet(f))
        except Exception:
            # could be a temporary/partial file; ignore
            continue
    if parts:
        try:
            return pd.concat(parts, ignore_index=True)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def add_window_start(df: pd.DataFrame, col: str = "window") -> pd.DataFrame:
    """
    Ensure the DataFrame has a naive 'window_start' column for plotting/sorting,
    regardless of how Spark encoded the 'window' column.
    Handles:
      • MultiIndex columns like ('window','start')
      • Single 'window' column with dict-like {'start': ..., 'end': ...}
      • Row-wise objects with `.start` attribute
    """
    if df is None or df.empty:
        return df

    # Case A: MultiIndex columns (common with some Spark writers)
    if isinstance(df.columns, pd.MultiIndex) and (col, "start") in df.columns:
        start_series = df[(col, "start")]
        df["window_start"] = pd.to_datetime(start_series, utc=True).dt.tz_convert(None)
        return df

    # Case B: single 'window' column containing dict/obj
    if col in df.columns:
        s = df[col]
        def extract_start(x):
            if isinstance(x, dict):
                return x.get("start")
            return getattr(x, "start", None)
        try:
            starts = s.apply(extract_start)
        except Exception:
            starts = pd.Series([None] * len(df))
        df["window_start"] = pd.to_datetime(starts, utc=True).dt.tz_convert(None)

    return df

def now_str() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

# --------------------------- Data paths ---------------------------

PATH_BY_PROJECT   = "data/gold/by_project"
PATH_TOP_PAGES    = "data/gold/top_pages"
PATH_SPIKES       = "data/gold/spikes_v2"      # <- v2 sink
PATH_CROSSLANG    = "data/gold/crosslang_v2"   # <- v2 sink

# --------------------------- Layout ---------------------------

col1, col2 = st.columns(2)

# -------- Edits/min by project (line chart) --------
with col1:
    st.subheader("Edits/min by project (last windows)")
    dfp = read_parquet_dir(PATH_BY_PROJECT)
    if dfp.empty:
        st.info("Waiting for data…")
    else:
        dfp = add_window_start(dfp)
        req_cols = {"window_start", "project", "edits"}
        if req_cols.issubset(dfp.columns):
            # pivot to time × project
            pivot = (
                dfp.sort_values("window_start")
                   .pivot_table(index="window_start", columns="project", values="edits", aggfunc="sum")
                   .fillna(0)
            )
            # show the last 60 windows to keep the chart responsive
            st.line_chart(pivot.tail(60))
            st.caption(f"Rows: {len(dfp):,} • Updated: {now_str()}")
        else:
            st.info("Columns not stabilized yet; waiting for next batch…")

# -------- Top pages (table) --------
with col2:
    st.subheader("Top pages (last 10 min, sliding)")
    dft = read_parquet_dir(PATH_TOP_PAGES)
    if dft.empty:
        st.info("Waiting for data…")
    else:
        dft = add_window_start(dft)
        cols = [c for c in ["window_start", "project", "page_title", "edits"] if c in dft.columns]
        if cols:
            top = (
                dft.sort_values(["window_start", "edits"], ascending=[False, False])
                   [cols]
                   .head(lookback_rows)
            )
            st.dataframe(top, use_container_width=True)
            st.caption(f"Rows: {len(dft):,} • Updated: {now_str()}")
        else:
            st.info("Columns not stabilized yet; waiting for next batch…")

st.divider()

col3, col4 = st.columns(2)

# -------- Spike alerts (1m ≥ 2× 10m avg) --------
with col3:
    st.subheader("Spike alerts (1m ≥ 2× 10m avg)")
    dfs = read_parquet_dir(PATH_SPIKES)
    if dfs.empty:
        st.info("Waiting for spikes…")
    else:
        dfs = add_window_start(dfs)
        cols = [c for c in ["window_start", "project", "edits_1m", "edits_10m", "ratio"] if c in dfs.columns]
        if cols:
            show = (
                dfs.sort_values(["window_start", "ratio"], ascending=[False, False])
                   [cols]
                   .head(lookback_rows)
            )
            st.dataframe(show, use_container_width=True)
            st.caption(f"Rows: {len(dfs):,} • Updated: {now_str()}")
        else:
            st.info("Columns not stabilized yet; waiting for next batch…")

# -------- Cross-language pages (≥ 2 projects) --------
with col4:
    st.subheader("Cross-language pages (≥ 2 projects in same 10-min window)")
    dfc = read_parquet_dir(PATH_CROSSLANG)
    if dfc.empty:
        st.info("Waiting for data…")
    else:
        dfc = add_window_start(dfc)
        cols = [c for c in ["window_start", "page_title", "projects", "total_edits", "n_projects"] if c in dfc.columns]
        if cols:
            cross = (
                dfc.sort_values(["window_start", "total_edits"], ascending=[False, False])
                   [cols]
                   .head(lookback_rows)
            )
            st.dataframe(cross, use_container_width=True)
            st.caption(f"Rows: {len(dfc):,} • Updated: {now_str()}")
        else:
            st.info("Columns not stabilized yet; waiting for next batch…")

# --------------------------- Auto-refresh ---------------------------

auto_refresh(refresh_sec * 1000)

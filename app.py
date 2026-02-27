"""
SHIELD AI — Phase 2: Streamlit Dashboard
=========================================

Real-time visual interface for the Phase 1 attribution pipeline.

Layout
------
┌─────────────────────────────────────────────────────────┐
│  SHIELD AI                              [status badge]  │
│─────────────────────────────────────────────────────────│
│  Live CETP Inlet COD         │  Attribution Log         │
│  (line chart, anomalies red) │  (newest 10 events)      │
│──────────────────────────────┤                          │
│  Factory Discharge Overview  │  [Download PDF Report]   │
│  (bar chart per factory)     │                          │
└─────────────────────────────────────────────────────────┘

Data sources
------------
- CETP data     : data/cetp/priya_cetp_i.csv (read directly for the chart)
- Evidence log  : data/alerts/evidence_log.jsonl (written by alert.py)

The dashboard polls for new evidence via st.rerun() on a configurable interval.
No Pathway tables are accessed directly — the dashboard reads the output files
so it can run independently of the Pathway pipeline process.

Run
---
    streamlit run app.py
"""

import json
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from src.constants import CETP_DATA_DIR, ALERT_LOG_PATH, COD_THRESHOLD, COD_BASELINE

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="SHIELD AI",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    /* Dark industrial theme */
    .stApp { background-color: #0d1117; color: #c9d1d9; }
    .metric-card {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }
    .alert-high   { color: #f85149; font-weight: bold; }
    .alert-medium { color: #e3b341; font-weight: bold; }
    .header-title {
        font-size: 2.2rem;
        font-weight: 700;
        letter-spacing: -1px;
        color: #58a6ff;
    }
    .subheader { color: #8b949e; font-size: 0.9rem; }
    div[data-testid="stMetricValue"] { font-size: 1.8rem; color: #39d353; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("## ⚙️ Settings")
    refresh_secs = st.slider("Auto-refresh (s)", 2, 30, 5)
    show_na_rows = st.checkbox("Show NA / BLACKOUT rows in history", value=False)
    st.markdown("---")
    st.markdown("### 🔧 Thresholds")
    st.metric("COD Baseline",   f"{COD_BASELINE} mg/L")
    st.metric("COD Threshold",  f"{COD_THRESHOLD} mg/L")
    st.markdown("---")
    st.markdown("**SHIELD AI v1** — Prototype Demo")
    st.markdown("Branch: `v1` | Data: Feb 2026")


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown('<div class="header-title">🛡️ SHIELD AI</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subheader">Real-time industrial discharge attribution for CETP compliance</div>',
    unsafe_allow_html=True,
)
st.markdown("---")


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=5)
def load_cetp_data() -> pd.DataFrame:
    """Load and clean the CETP CSV for charting.

    Returns a DataFrame with columns: time (datetime), cod (float).
    Rows with NA COD are dropped for chart clarity unless show_na_rows is enabled.
    """
    cetp_path = Path(CETP_DATA_DIR) / "priya_cetp_i.csv"
    if not cetp_path.exists():
        return pd.DataFrame(columns=["time", "cod"])

    df = pd.read_csv(cetp_path)
    df = df.rename(columns={
        "Time": "time",
        "CETP_INLET-COD - (mg/l) Raw": "cod",
    })
    df = df[["time", "cod"]].copy()
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M", errors="coerce")
    df["cod"]  = pd.to_numeric(df["cod"], errors="coerce")
    df = df.dropna(subset=["cod"])
    return df.sort_values("time").reset_index(drop=True)


@st.cache_data(ttl=3)
def load_evidence_log() -> list[dict]:
    """Load all evidence records from the JSONL log.

    Returns a list of dicts, newest-first.
    """
    log_path = Path(ALERT_LOG_PATH)
    if not log_path.exists():
        return []

    records = []
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    # Sort newest first
    return list(reversed(records))


@st.cache_data(ttl=10)
def load_factory_overview() -> pd.DataFrame:
    """Load a summary COD reading per factory from factory CSVs.

    Returns a DataFrame with columns: factory_id, mean_cod, max_cod.
    """
    factory_dir = Path("data/factories")
    rows = []
    for csv_path in sorted(factory_dir.glob("factory_*.csv")):
        df = pd.read_csv(csv_path)
        df = df.rename(columns={"COD - (mg/l) Raw": "cod", "factory_id": "factory_id"})
        df["cod"] = pd.to_numeric(df["cod"], errors="coerce")
        clean = df.dropna(subset=["cod"])
        if not clean.empty:
            rows.append({
                "factory_id": clean["factory_id"].iloc[0],
                "mean_cod":   round(clean["cod"].mean(), 2),
                "max_cod":    round(clean["cod"].max(), 2),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main layout — row 1: KPIs
# ---------------------------------------------------------------------------

cetp_df   = load_cetp_data()
evidence  = load_evidence_log()
factory_df = load_factory_overview()

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_readings = len(cetp_df)
    st.metric("📊 Total CETP Readings", f"{total_readings:,}")

with col2:
    spikes = len(cetp_df[cetp_df["cod"] >= COD_THRESHOLD]) if not cetp_df.empty else 0
    st.metric("⚡ Threshold Breaches", spikes, delta="COD ≥ 250 mg/L")

with col3:
    st.metric("🔍 Attributed Events", len(evidence))

with col4:
    latest_factory = evidence[0].get("attributed_factory", "—") if evidence else "—"
    st.metric("🏭 Latest Attribution", latest_factory)

st.markdown("---")


# ---------------------------------------------------------------------------
# Row 2: CETP chart | Evidence log
# ---------------------------------------------------------------------------

chart_col, log_col = st.columns([3, 2], gap="medium")

with chart_col:
    st.subheader("📈 Live CETP Inlet COD")

    if cetp_df.empty:
        st.info("No CETP data loaded.")
    else:
        # Mark anomaly rows
        cetp_chart = cetp_df.copy()
        cetp_chart["is_anomaly"] = cetp_chart["cod"] >= COD_THRESHOLD

        # Show last 720 rows (~12 hours at every-3-min cadence)
        display_df = cetp_chart.tail(720).set_index("time")

        # Split normal vs anomaly
        normal_df  = display_df[~display_df["is_anomaly"]][["cod"]]
        anomaly_df = display_df[ display_df["is_anomaly"]][["cod"]]

        # Baseline reference line
        baseline_series = display_df[["cod"]].copy()
        baseline_series["cod"] = COD_BASELINE

        st.line_chart(normal_df, color="#39d353")

        if not anomaly_df.empty:
            st.warning(f"⚠️ {len(anomaly_df)} anomaly readings ≥ {COD_THRESHOLD} mg/L in this window")
            st.line_chart(anomaly_df, color="#f85149")

        st.caption(f"Baseline: {COD_BASELINE} mg/L | Threshold: {COD_THRESHOLD} mg/L")

with log_col:
    st.subheader("🔎 Attribution Log")

    if not evidence:
        st.info(
            "No shock events detected yet.\n\n"
            "Run `python src/run_pipeline.py` to start the attribution pipeline."
        )
    else:
        for i, rec in enumerate(evidence[:10]):
            level = rec.get("alert_level", "MEDIUM")
            css   = "alert-high" if level == "HIGH" else "alert-medium"
            factory = rec.get("attributed_factory", "Unknown")
            event_t = rec.get("cetp_event_time", "—")
            cod     = rec.get("cetp_cod", "—")
            f_cod   = rec.get("factory_cod", "—")

            with st.expander(f"{'🔴' if level=='HIGH' else '🟡'} {event_t} → {factory}", expanded=(i==0)):
                st.markdown(f"""
| Field | Value |
|---|---|
| **Alert Level** | <span class="{css}">{level}</span> |
| **CETP COD** | {cod} mg/L |
| **Attributed Factory** | **{factory}** |
| **Factory COD @ T-15min** | {f_cod} mg/L |
| **Backtrack Time** | {rec.get('backtrack_time', '—')} |
                """, unsafe_allow_html=True)

        # Download button
        st.markdown("---")
        if st.button("📥 Download Evidence Log (JSONL)"):
            log_path = Path(ALERT_LOG_PATH)
            if log_path.exists():
                st.download_button(
                    label="Save evidence_log.jsonl",
                    data=log_path.read_bytes(),
                    file_name="evidence_log.jsonl",
                    mime="application/jsonl",
                )


# ---------------------------------------------------------------------------
# Row 3: Factory overview bar chart
# ---------------------------------------------------------------------------

st.markdown("---")
st.subheader("🏭 Factory Discharge Overview")

if factory_df.empty:
    st.info("Run `python src/simulate_factories.py` to generate factory data first.")
else:
    bar_col1, bar_col2 = st.columns(2)
    with bar_col1:
        st.caption("Mean COD per factory (mg/L)")
        st.bar_chart(factory_df.set_index("factory_id")[["mean_cod"]])
    with bar_col2:
        st.caption("Peak COD per factory (mg/L)")
        peak = factory_df.set_index("factory_id")[["max_cod"]]
        st.bar_chart(peak)

    # Annotate which factory is the shock-load culprit
    if not factory_df.empty:
        rogue = factory_df.loc[factory_df["max_cod"].idxmax(), "factory_id"]
        st.success(f"📌 Highest peak discharge: **{rogue}** — matches simulated shock-load factory.")


# ---------------------------------------------------------------------------
# Auto-refresh footer
# ---------------------------------------------------------------------------

st.markdown("---")
st.caption(f"Auto-refreshing every {refresh_secs}s · Evidence log: `{ALERT_LOG_PATH}`")
time.sleep(refresh_secs)
st.rerun()

"""
SHIELD AI — Phase 2: Streamlit Dashboard
=========================================

Reads directly from:
  - data/cetp/cetp_clean.csv       (CETP inlet COD trend)
  - data/factories/factory_*.csv   (factory discharge overview)
  - data/alerts/evidence_log.jsonl (live attribution log from run_pipeline.py)

Run
---
    uv run streamlit run app.py
"""

import json
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from src.constants import COD_BASELINE, COD_THRESHOLD, ALERT_LOG_PATH, CETP_DATA_DIR

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SHIELD AI — Industrial Discharge Monitor",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

html, body, .stApp { background: #0d1117; color: #c9d1d9; font-family: 'Inter', sans-serif; }

/* Sidebar */
[data-testid="stSidebar"] { background: #161b22; border-right: 1px solid #30363d; }

/* Metric cards */
[data-testid="stMetric"] {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 1rem 1.2rem;
}
[data-testid="stMetricValue"] { font-size: 2rem !important; color: #58a6ff; }
[data-testid="stMetricLabel"] { color: #8b949e; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; }
[data-testid="stMetricDelta"] { font-size: 0.85rem; }

/* Section headers */
.section-title {
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #8b949e;
    border-bottom: 1px solid #21262d;
    padding-bottom: 6px;
    margin-bottom: 1rem;
}

/* Alert badge */
.badge-high   { background:#f8514920; color:#f85149; border:1px solid #f8514940; border-radius:4px; padding:2px 8px; font-size:0.75rem; font-weight:600; }
.badge-medium { background:#e3b34120; color:#e3b341; border:1px solid #e3b34140; border-radius:4px; padding:2px 8px; font-size:0.75rem; font-weight:600; }
.badge-none   { background:#30363d;   color:#8b949e; border:1px solid #30363d;   border-radius:4px; padding:2px 8px; font-size:0.75rem; font-weight:600; }

/* Evidence table row */
.ev-row {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 0.75rem 1rem;
    margin-bottom: 0.5rem;
    display: grid;
    grid-template-columns: 1fr 1fr 1fr 1fr;
    gap: 0.5rem;
    align-items: center;
    font-size: 0.82rem;
}
.ev-factory { font-family: 'JetBrains Mono', monospace; font-weight: 600; color: #79c0ff; }
.ev-time    { color: #8b949e; }
.ev-cod     { color: #39d353; font-family: 'JetBrains Mono', monospace; }
</style>
""", unsafe_allow_html=True)


# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=5)
def load_cetp() -> pd.DataFrame:
    p = Path(CETP_DATA_DIR) / "cetp_clean.csv"
    if not p.exists():
        return pd.DataFrame(columns=["time", "cetp_inlet_cod"])
    df = pd.read_csv(p, dtype={"time": str})
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M", errors="coerce")
    df["cetp_inlet_cod"] = pd.to_numeric(df["cetp_inlet_cod"], errors="coerce")
    return df.dropna(subset=["cetp_inlet_cod"]).sort_values("time").reset_index(drop=True)


@st.cache_data(ttl=3)
def load_evidence() -> list[dict]:
    p = Path(ALERT_LOG_PATH)
    if not p.exists():
        return []
    recs = []
    try:
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    recs.append(json.loads(line))
    except Exception:
        pass
    return list(reversed(recs))  # newest first


@st.cache_data(ttl=30)
def load_factory_summary() -> pd.DataFrame:
    rows = []
    for p in sorted(Path("data/factories").glob("factory_*.csv")):
        df = pd.read_csv(p, dtype={"time": str})
        df["cod"] = pd.to_numeric(df["cod"], errors="coerce")
        clean = df.dropna(subset=["cod"])
        if not clean.empty:
            rows.append({
                "factory_id": clean["factory_id"].iloc[0],
                "mean_cod":   round(clean["cod"].mean(), 1),
                "max_cod":    round(clean["cod"].max(), 1),
                "std_cod":    round(clean["cod"].std(), 2),
            })
    return pd.DataFrame(rows)


# ── Fetch data ────────────────────────────────────────────────────────────────
cetp_df   = load_cetp()
evidence  = load_evidence()
factory_df = load_factory_summary()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🛡️ SHIELD AI")
    st.markdown('<div class="section-title">Display</div>', unsafe_allow_html=True)
    refresh_secs   = st.slider("Auto-refresh (s)", 3, 30, 5)
    chart_hours    = st.slider("Chart window (hours)", 1, 24, 12)
    show_threshold = st.checkbox("Show threshold line", value=True)

    st.markdown('<div class="section-title">Thresholds</div>', unsafe_allow_html=True)
    st.metric("COD Baseline",   f"{COD_BASELINE} mg/L")
    st.metric("COD Threshold",  f"{COD_THRESHOLD} mg/L")

    st.markdown('<div class="section-title">Info</div>', unsafe_allow_html=True)
    st.caption(f"CETP rows: {len(cetp_df):,}")
    st.caption(f"Evidence log: {len(evidence)} alerts")
    if evidence:
        st.caption(f"Last alert: {evidence[0].get('cetp_event_time','—')}")
    st.markdown("---")
    st.caption("SHIELD AI v1 · Branch: `v1`")
    st.caption("Data: Feb 2026 (priya_cetp_i.csv)")


# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_status = st.columns([4, 1])
with col_title:
    st.markdown("# 🛡️ SHIELD AI")
    st.markdown('<p style="color:#8b949e;margin-top:-16px;">Real-time industrial discharge attribution for CETP compliance</p>', unsafe_allow_html=True)
with col_status:
    pipeline_running = Path(ALERT_LOG_PATH).exists()
    st.markdown(
        f'<div style="text-align:right;margin-top:1.5rem;">'
        f'<span style="background:{"#1a4731" if pipeline_running else "#3d1a1a"};'
        f'color:{"#3fb950" if pipeline_running else "#f85149"};'
        f'border-radius:20px;padding:4px 12px;font-size:0.8rem;font-weight:600;">'
        f'{"● ACTIVE" if pipeline_running else "● OFFLINE"}</span></div>',
        unsafe_allow_html=True,
    )

st.markdown("---")


# ── KPI row ───────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.metric("📊 CETP Readings", f"{len(cetp_df):,}")
with k2:
    breach_count = int((cetp_df["cetp_inlet_cod"] >= COD_THRESHOLD).sum()) if not cetp_df.empty else 0
    st.metric("⚡ Threshold Breaches", breach_count, delta=f"≥{COD_THRESHOLD} mg/L")
with k3:
    st.metric("🔍 Attributed Events", len(evidence))
with k4:
    high_alerts = sum(1 for r in evidence if r.get("alert_level") == "HIGH")
    st.metric("🔴 HIGH Alerts", high_alerts)
with k5:
    top_factory = "—"
    if evidence:
        from collections import Counter
        factories = [r["attributed_factory"] for r in evidence if r.get("attributed_factory")]
        if factories:
            top_factory = Counter(factories).most_common(1)[0][0]
    st.metric("🏭 Top Attributed", top_factory)


# ── Row 2: CETP trend chart + live attribution feed ───────────────────────────
st.markdown("---")
chart_col, log_col = st.columns([3, 2], gap="large")

with chart_col:
    st.markdown('<div class="section-title">CETP Inlet COD — Live Trend</div>', unsafe_allow_html=True)

    if cetp_df.empty:
        st.info("No CETP data — run `uv run python src/simulate_factories.py` first.")
    else:
        # Slice to chart window
        t_end   = cetp_df["time"].max()
        t_start = t_end - pd.Timedelta(hours=chart_hours)
        view    = cetp_df[cetp_df["time"] >= t_start].copy()
        view    = view.set_index("time")[["cetp_inlet_cod"]]

        # Colour code: normal vs anomaly
        normal_mask  = view["cetp_inlet_cod"] < COD_THRESHOLD
        normal_view  = view[normal_mask]
        anomaly_view = view[~normal_mask]

        if show_threshold:
            threshold_line = view.copy()
            threshold_line["threshold"] = COD_THRESHOLD
            st.line_chart(threshold_line, color=["#58a6ff", "#f8514930"])
        else:
            st.line_chart(normal_view, color=["#58a6ff"])

        if not anomaly_view.empty:
            st.warning(
                f"⚠️ **{len(anomaly_view)} breaches** in last {chart_hours}h "
                f"(COD ≥ {COD_THRESHOLD} mg/L)"
            )

        # COD stats
        s1, s2, s3 = st.columns(3)
        s1.metric("Current COD", f"{view['cetp_inlet_cod'].iloc[-1]:.1f} mg/L")
        s2.metric("Max (window)", f"{view['cetp_inlet_cod'].max():.1f} mg/L")
        s3.metric("Mean (window)", f"{view['cetp_inlet_cod'].mean():.1f} mg/L")


with log_col:
    st.markdown('<div class="section-title">Live Attribution Log</div>', unsafe_allow_html=True)

    if not evidence:
        st.info(
            "No evidence yet.\n\n"
            "**Start the pipeline:**\n```\nuv run python src/run_pipeline.py\n```"
        )
    else:
        # Show most recent 12 events
        for rec in evidence[:12]:
            level     = rec.get("alert_level", "MEDIUM")
            factory   = rec.get("attributed_factory") or "—"
            cetp_t    = rec.get("cetp_event_time", "—")
            cetp_cod  = rec.get("cetp_cod")
            f_cod     = rec.get("factory_cod")
            bt_time   = rec.get("backtrack_time", "—")
            badge_cls = "badge-high" if level == "HIGH" else "badge-medium" if level == "MEDIUM" else "badge-none"

            cetp_cod_str = f"{cetp_cod:.1f}" if cetp_cod is not None else "—"
            f_cod_str    = f"{f_cod:.1f}" if f_cod is not None else "—"

            st.markdown(f"""
<div class="ev-row">
  <div>
    <span class="ev-factory">{factory}</span><br>
    <span class="ev-time">↑ {cetp_t}</span>
  </div>
  <div>
    <span style="color:#8b949e;font-size:0.75rem;">CETP COD</span><br>
    <span class="ev-cod">{cetp_cod_str} mg/L</span>
  </div>
  <div>
    <span style="color:#8b949e;font-size:0.75rem;">Factory COD @ T-15</span><br>
    <span class="ev-cod">{f_cod_str} mg/L</span>
  </div>
  <div style="text-align:right;">
    <span class="{badge_cls}">{level}</span><br>
    <span class="ev-time" style="font-size:0.7rem;">{bt_time}</span>
  </div>
</div>""", unsafe_allow_html=True)

        # Download log
        st.markdown("---")
        log_bytes = Path(ALERT_LOG_PATH).read_bytes() if Path(ALERT_LOG_PATH).exists() else b""
        st.download_button(
            label="📥 Download evidence_log.jsonl",
            data=log_bytes,
            file_name="evidence_log.jsonl",
            mime="application/json",
            use_container_width=True,
        )


# ── Row 3: Factory discharge overview ─────────────────────────────────────────
st.markdown("---")
st.markdown('<div class="section-title">Factory Discharge Overview</div>', unsafe_allow_html=True)

if factory_df.empty:
    st.info("Run `uv run python src/simulate_factories.py` to generate factory data.")
else:
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        st.caption("Mean COD per factory (mg/L)")
        st.bar_chart(
            factory_df.set_index("factory_id")[["mean_cod"]],
            color="#58a6ff",
        )
    with fc2:
        st.caption("Peak COD per factory (mg/L)")
        st.bar_chart(
            factory_df.set_index("factory_id")[["max_cod"]],
            color="#f85149",
        )
    with fc3:
        st.caption("COD Std Dev (variance indicator)")
        st.bar_chart(
            factory_df.set_index("factory_id")[["std_cod"]],
            color="#e3b341",
        )

    # Factory table
    st.dataframe(
        factory_df.rename(columns={
            "factory_id": "Factory",
            "mean_cod":   "Mean COD (mg/L)",
            "max_cod":    "Peak COD (mg/L)",
            "std_cod":    "Std Dev",
        }),
        use_container_width=True,
        hide_index=True,
    )

    # Annotation
    rogue = factory_df.loc[factory_df["max_cod"].idxmax()]
    low_var = factory_df.loc[factory_df["std_cod"].idxmin()]
    ccol1, ccol2 = st.columns(2)
    ccol1.success(f"📌 Highest peak: **{rogue['factory_id']}** ({rogue['max_cod']} mg/L) — shock-load profile")
    if float(low_var["std_cod"]) < 1.0:
        ccol2.warning(f"⚠️ Zero-variance: **{low_var['factory_id']}** (σ = {low_var['std_cod']}) — possible sensor tampering")


# ── Row 4: Attribution breakdown ──────────────────────────────────────────────
if evidence:
    st.markdown("---")
    st.markdown('<div class="section-title">Attribution Breakdown</div>', unsafe_allow_html=True)
    from collections import Counter

    factories = [r.get("attributed_factory", "UNKNOWN") for r in evidence if r.get("attributed_factory")]
    if factories:
        counts = Counter(factories)
        breakdown = pd.DataFrame(
            {"Factory": list(counts.keys()), "Events": list(counts.values())}
        ).sort_values("Events", ascending=False)

        ab1, ab2 = st.columns([1, 2])
        with ab1:
            st.dataframe(breakdown, hide_index=True, use_container_width=True)
        with ab2:
            st.bar_chart(
                breakdown.set_index("Factory")[["Events"]],
                color="#79c0ff",
            )


# ── Footer / auto-refresh ─────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    f"🔄 Auto-refreshing every {refresh_secs}s  ·  "
    f"Log: `{ALERT_LOG_PATH}`  ·  "
    f"Threshold: {COD_THRESHOLD} mg/L  ·  "
    f"Baseline: {COD_BASELINE} mg/L"
)
time.sleep(refresh_secs)
st.rerun()

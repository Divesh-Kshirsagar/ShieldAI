"""
SHIELD AI — Phase 1: Stateful Anomaly Detection (The Tripwire)
==============================================================

Continuously monitors the clean CETP inlet stream against the empirical
COD baseline. The millisecond a threshold breach is detected, a row is
emitted to the `shock_events` table.

Algorithm
---------
For each valid (non-NA) CETP inlet row:
    if COD >= COD_THRESHOLD → emit a ShockLoadEvent

Thresholds (see constants.py)
    COD_BASELINE  = 193.0 mg/L  (empirical mean from priya_cetp_i.csv)
    COD_THRESHOLD = 250.0 mg/L  (demo value; 450+ recommended for production)

The breach_magnitude field quantifies how far above baseline the spike is,
which is used downstream to prioritise alert severity.

Usage
-----
    from src.tripwire import detect_anomalies
    shock_events = detect_anomalies(cetp_stream)
"""

import pathway as pw

from src.constants import COD_BASELINE, COD_THRESHOLD


def detect_anomalies(cetp_stream: pw.Table) -> pw.Table:
    """Detect COD shock-load events in the CETP inlet stream.

    Emits a row to shock_events whenever CETP inlet COD breaches COD_THRESHOLD.
    The emitted table is used by backtrack.py to trigger the temporal join.

    Args:
        cetp_stream: Clean CETP stream from ingest.load_cetp_stream().
                     Must contain columns: time, cetp_inlet_cod.

    Returns:
        Pathway Table with columns:
            time          (str)   — timestamp of the breach
            cod_value     (float) — observed COD at breach
            breach_mag    (float) — COD - COD_BASELINE (positive = above baseline)
            alert_level   (str)   — "HIGH" if >2× baseline, else "MEDIUM"
    """
    # NOTE: COD_THRESHOLD and COD_BASELINE are imported from constants.py so
    # that adjusting the values there propagates everywhere without touching logic.
    shock_events: pw.Table = (
        cetp_stream
        # Step 1 — apply the threshold filter (the Tripwire)
        .filter(pw.this.cetp_inlet_cod >= COD_THRESHOLD)
        # Step 2 — enrich with breach metadata
        .with_columns(
            cod_value   = pw.this.cetp_inlet_cod,
            breach_mag  = pw.this.cetp_inlet_cod - COD_BASELINE,
            alert_level = pw.if_else(
                pw.this.cetp_inlet_cod >= COD_BASELINE * 2,
                pw.cast(str, "HIGH"),
                pw.cast(str, "MEDIUM"),
            ),
        )
        # Step 3 — project only the columns needed downstream
        .select(
            pw.this.time,
            pw.this.cod_value,
            pw.this.breach_mag,
            pw.this.alert_level,
        )
    )

    return shock_events


def get_rolling_stats(cetp_stream: pw.Table, window_minutes: int = 15) -> pw.Table:
    """Compute rolling mean and std-dev of CETP inlet COD over a sliding window.

    Used by the Streamlit dashboard for the live trend chart and by
    anti_cheat.py (v2) for the zero-variance alarm on factory streams.

    Args:
        cetp_stream:     Clean CETP stream (from ingest.load_cetp_stream()).
        window_minutes:  Width of the rolling window in minutes.

    Returns:
        Pathway Table with columns: time, rolling_mean_cod, rolling_std_cod.
    """
    # NOTE: pw.temporal.sliding_window uses event-time semantics — rows are
    # assigned to windows based on their 'time' field, not wall-clock time.
    # This ensures correct behaviour when replaying historical CSV data.
    window_duration = pw.Duration(minutes=window_minutes)

    windowed = cetp_stream.windowby(
        pw.this.time,
        window=pw.temporal.sliding(
            duration=window_duration,
            hop=pw.Duration(minutes=1),
        ),
        instance=pw.this.time,  # one instance per timestamp slice
    ).reduce(
        time           = pw.reducers.max(pw.this.time),
        rolling_mean_cod = pw.reducers.avg(pw.this.cetp_inlet_cod),
        rolling_std_cod  = pw.reducers.stddev(pw.this.cetp_inlet_cod),
    )

    return windowed

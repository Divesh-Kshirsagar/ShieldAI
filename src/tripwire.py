"""
SHIELD AI — Phase 1: Stateful Anomaly Detection (The Tripwire)
==============================================================

Monitors the clean CETP inlet stream and emits a row to shock_events
the moment COD breaches COD_THRESHOLD.

Thresholds (see constants.py)
    COD_BASELINE  = 193.0 mg/L  (empirical mean, priya_cetp_i.csv)
    COD_THRESHOLD = 250.0 mg/L  (demo value; 450+ for production)

Usage
-----
    from src.tripwire import detect_anomalies
    shock_events = detect_anomalies(cetp_stream)
"""

import pathway as pw

from src.config import CONFIG as _cfg

_COD_BASELINE:  float = _cfg.cod_baseline
_COD_THRESHOLD: float = _cfg.cod_threshold


def detect_anomalies(cetp_stream: pw.Table) -> pw.Table:
    """Detect COD shock-load events in the CETP inlet stream.

    Emits a row to shock_events whenever CETP inlet COD >= COD_THRESHOLD.
    The emitted table is consumed by backtrack.py to trigger the temporal join.

    Args:
        cetp_stream: Clean CETP stream from ingest.load_cetp_stream().
                     Must contain: time (str), cetp_inlet_cod (float).

    Returns:
        Pathway Table with columns:
            time        (str)   — timestamp of the breach
            cod_value   (float) — observed COD value
            breach_mag  (float) — COD minus COD_BASELINE (positive = above baseline)
            alert_level (str)   — "HIGH" (>=2× baseline) or "MEDIUM"
    """
    # NOTE: _COD_THRESHOLD and _COD_BASELINE come from config.CONFIG so adjusting
    # them there propagates everywhere without touching this logic.
    shock_events: pw.Table = (
        cetp_stream
        # Step 1 — The Tripwire filter
        .filter(pw.this.cetp_inlet_cod >= _COD_THRESHOLD)
        # Step 2 — Enrich with breach metadata
        .with_columns(
            cod_value   = pw.this.cetp_inlet_cod,
            breach_mag  = pw.this.cetp_inlet_cod - _COD_BASELINE,
            alert_level = pw.if_else(
                pw.this.cetp_inlet_cod >= _COD_BASELINE * 2,
                "HIGH",
                "MEDIUM",
            ),
        )
        # Step 3 — Project needed columns
        .select(
            pw.this.time,
            pw.this.cod_value,
            pw.this.breach_mag,
            pw.this.alert_level,
        )
    )

    return shock_events


def get_rolling_stats(cetp_stream: pw.Table, window_minutes: int = 15) -> pw.Table:
    """Compute rolling mean of CETP inlet COD over a sliding window.

    NOTE: pw.reducers.stddev is not available in Pathway 0.29.x.
    Rolling variance for anti-cheat (v2) will be computed via a UDF reducer.
    For Phase 1 this function provides the rolling mean used by the dashboard.

    Args:
        cetp_stream:    Clean CETP stream (time str, cetp_inlet_cod float).
        window_minutes: Width of the rolling window in minutes.

    Returns:
        Pathway Table with columns: time, rolling_mean_cod.
    """
    # NOTE: pw.temporal.windowby uses event-time semantics — rows are placed
    # in windows based on their 'time' column value, not wall-clock time.
    # This is correct for replaying historical CSV data as a live stream.

    # Parse time string to DateTimeNaive for Pathway's temporal operators
    cetp_with_ts = cetp_stream.with_columns(
        ts=pw.this.time.dt.strptime(fmt="%Y-%m-%d %H:%M")
    )

    windowed = cetp_with_ts.windowby(
        cetp_with_ts.ts,
        window=pw.temporal.sliding(
            duration=pw.Duration(minutes=window_minutes),
            hop=pw.Duration(minutes=1),
        ),
    ).reduce(
        time             = pw.reducers.max(pw.this.time),
        rolling_mean_cod = pw.reducers.avg(pw.this.cetp_inlet_cod),
    )

    return windowed

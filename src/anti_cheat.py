"""
SHIELD AI — Phase 4 (v2): Anti-Cheating Detection
===================================================

Three detection mechanisms that identify factories manipulating their
sensor data to evade Phase 1 attribution:

1. Zero-Variance Alarm       — Detects software-capped sensors (flat COD readings).
2. Chemical Fingerprint      — Detects dilution tampering via COD/TSS ratio anomaly.
3. Guilt by Disconnection    — Detects strategic sensor blackouts before dumping.

Each mechanism emits a tamper_events table with a `tamper_type` field.
These feed into the alert pipeline and Streamlit dashboard in Phase 4.

NOTE: This module is a v2 stub — the detection logic is fully documented
and architected here but wired to Pathway's streaming API in Phase 4.
The constants driving these checks all live in constants.py.

Usage (v2 — not called in Phase 1)
-----
    from src.anti_cheat import detect_zero_variance, detect_chemical_fingerprint
                                detect_blackout_dump
"""

import pathway as pw

from src.constants import (
    ZERO_VARIANCE_MINUTES,
    COD_DROP_FRACTION,
    TSS_STABLE_FRACTION,
    PIPE_TRAVEL_MINUTES,
)


# ---------------------------------------------------------------------------
# 1. Zero-Variance Alarm
# ---------------------------------------------------------------------------

def detect_zero_variance(factory_stream: pw.Table) -> pw.Table:
    """Detect sensors reporting a suspiciously flat (zero-variance) COD reading.

    A legitimate sensor shows natural Gaussian variation. A software-capped
    sensor (digital tampering) emits the exact same value for minutes or hours.

    Algorithm:
        - Compute rolling std-dev of COD over a ZERO_VARIANCE_MINUTES window
          per factory_id.
        - Flag as 'DIGITALLY_TAMPERED' if std-dev == 0 for the full window.

    NOTE: This is a v2 feature stub. The rolling window uses pw.temporal.sliding
    grouped by factory_id so each factory is assessed independently.

    Args:
        factory_stream: Full factory stream (including BLACKOUT rows) from
                        ingest.load_factory_streams(). Must include: time,
                        factory_id, cod (float | None).

    Returns:
        Pathway Table of tamper events with columns:
            factory_id, window_end_time, rolling_std_cod, tamper_type.
    """
    # NOTE: ZERO_VARIANCE_MINUTES = 5 (see constants.py).
    # If rolling_std_cod == 0 for the entire window, the sensor is flagged.
    window_duration = pw.Duration(minutes=ZERO_VARIANCE_MINUTES)

    # Group by factory_id, slide over time, compute std-dev
    windowed = factory_stream.filter(pw.this.cod.is_not_none()).windowby(
        pw.this.time,
        window=pw.temporal.sliding(
            duration=window_duration,
            hop=pw.Duration(minutes=1),
        ),
        instance=pw.this.factory_id,
    ).reduce(
        factory_id       = pw.this.factory_id,
        window_end_time  = pw.reducers.max(pw.this.time),
        rolling_std_cod  = pw.reducers.stddev(pw.this.cod),
    )

    # Flag windows where std-dev is zero (or practically zero — float precision)
    tamper_events = windowed.filter(
        pw.this.rolling_std_cod == 0.0
    ).with_columns(
        tamper_type = pw.cast(str, "DIGITALLY_TAMPERED")
    )

    return tamper_events


# ---------------------------------------------------------------------------
# 2. Chemical Fingerprint
# ---------------------------------------------------------------------------

def detect_chemical_fingerprint(factory_stream: pw.Table) -> pw.Table:
    """Detect the bucket-trick: diluting COD while TSS stays high.

    When a factory pours clean water into their sample point before the
    sensor fires, COD drops dramatically but TSS (which measures particles)
    remains high — the ratio breaks.

    Algorithm:
        - Compute rolling mean COD and TSS over a 15-min window.
        - Flag 'PHYSICAL_TAMPERING' if:
            COD_current < COD_prior × (1 - COD_DROP_FRACTION) [e.g. drops ≥80%]
            AND TSS_current > TSS_prior × (1 - TSS_STABLE_FRACTION) [stays within 20%]

    NOTE: This is a v2 feature stub. Requires two consecutive windows to compare.

    Args:
        factory_stream: Clean factory stream (BLACKOUT rows excluded). Must
                        include: time, factory_id, cod (float), tss (float).

    Returns:
        Pathway Table of tamper events with columns:
            factory_id, event_time, cod_ratio, tss_ratio, tamper_type.
    """
    # NOTE: COD_DROP_FRACTION = 0.80, TSS_STABLE_FRACTION = 0.20 (constants.py).
    # A realistic clean-water bucket trick will drop COD by 60-90% in one window.

    window_duration = pw.Duration(minutes=15)

    stats = factory_stream.filter(pw.this.cod.is_not_none()).windowby(
        pw.this.time,
        window=pw.temporal.sliding(
            duration=window_duration,
            hop=pw.Duration(minutes=1),
        ),
        instance=pw.this.factory_id,
    ).reduce(
        factory_id    = pw.this.factory_id,
        event_time    = pw.reducers.max(pw.this.time),
        mean_cod      = pw.reducers.avg(pw.this.cod),
        mean_tss      = pw.reducers.avg(pw.this.tss),
    )

    # TODO (v2): Join consecutive windows to compute ratio change.
    # For now, flag directly on absolute threshold as a placeholder.
    # A mean COD below 30 mg/L with TSS above 60 mg/L is a fingerprint signal.
    tamper_events = stats.filter(
        (pw.this.mean_cod < 30.0) & (pw.this.mean_tss > 60.0)
    ).with_columns(
        cod_ratio   = pw.this.mean_cod,
        tss_ratio   = pw.this.mean_tss,
        tamper_type = pw.cast(str, "PHYSICAL_TAMPERING"),
    ).select(
        pw.this.factory_id,
        pw.this.event_time,
        pw.this.cod_ratio,
        pw.this.tss_ratio,
        pw.this.tamper_type,
    )

    return tamper_events


# ---------------------------------------------------------------------------
# 3. Guilt by Disconnection
# ---------------------------------------------------------------------------

def detect_blackout_dump(factory_full_stream: pw.Table,
                         shock_events: pw.Table) -> pw.Table:
    """Detect factories that went offline exactly PIPE_TRAVEL_MINUTES before a CETP spike.

    A legitimate outage is random. A strategic blackout is timed so sensors are
    offline when the illegal dump travels to the CETP inlet — leaving no record
    of the discharge.

    Algorithm:
        - Take BLACKOUT-tagged rows from factory_full_stream.
        - For each CETP shock event at time T, compute T_backtrack = T - PIPE_TRAVEL_MINUTES.
        - If a factory's blackout window overlaps T_backtrack → flag 'STRATEGIC_BLACKOUT_DUMP'.

    NOTE: PIPE_TRAVEL_MINUTES = 15 (fixed constant for v1; see constants.py NOTE).
    This function uses the same travel time as backtrack.attribute_factory() —
    they are intentionally kept in sync via the shared constant.

    Args:
        factory_full_stream: Full factory stream INCLUDING BLACKOUT rows.
                             From ingest.load_factory_streams().
        shock_events:        CETP shock events from tripwire.detect_anomalies().

    Returns:
        Pathway Table of tamper events with columns:
            factory_id, blackout_time, cetp_event_time, tamper_type.
    """
    # NOTE: In v2 this will use pw.temporal.asof_join (same as backtrack.py)
    # to correlate blackout windows with shock event timestamps.
    # For Phase 1 compatibility the logic is stubbed as a filter pass-through.

    # Extract only BLACKOUT-tagged rows
    blackout_rows = factory_full_stream.filter(
        pw.this.status == "BLACKOUT"
    ).select(
        pw.this.factory_id,
        blackout_time = pw.this.time,
    )

    # TODO (v2): Join blackout_rows with shock_events on the T_backtrack window.
    # For now return blackout_rows enriched with tamper_type as the stub output.
    tamper_events = blackout_rows.with_columns(
        cetp_event_time = pw.cast(str, "PENDING_JOIN"),  # filled in v2
        tamper_type     = pw.cast(str, "STRATEGIC_BLACKOUT_DUMP"),
    )

    return tamper_events

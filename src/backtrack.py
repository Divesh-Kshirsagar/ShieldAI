"""
SHIELD AI — Phase 1: Temporal Backtracking (The "Time Machine" Join)
=====================================================================

The core attribution engine. Triggered by each shock event from tripwire.py,
it subtracts the estimated pipe travel time to compute a "backtrack timestamp"
and then searches the Industrial Discharge Stream for the factory that was
dumping the highest COD load at that exact window.

Algorithm
---------
For every shock event at time T:
    1. T_backtrack = T − PIPE_TRAVEL_MINUTES
    2. pw.temporal.asof_join: find the factory row in industrial_stream
       whose timestamp is closest to T_backtrack (within ASOF_TOLERANCE_SECONDS).
    3. The joined row = evidence record: factory attributed as the source.

NOTE: PIPE_TRAVEL_MINUTES = 15 is a FIXED CONSTANT for v1.
In v2 this will be replaced by a dynamic, pipe-length-aware calculation
derived from GIS network data and real-time flow-rate sensors.
Do NOT remove this constant — it is the single source of truth for the
temporal offset used in the asof_join attribution logic.

Usage
-----
    from src.backtrack import attribute_factory
    evidence = attribute_factory(shock_events, industrial_stream)
"""

from datetime import timedelta

import pathway as pw

from src.constants import PIPE_TRAVEL_MINUTES, ASOF_TOLERANCE_SECONDS


def attribute_factory(
    shock_events: pw.Table,
    industrial_stream: pw.Table,
) -> pw.Table:
    """Run the temporal backtrack join to attribute a factory to each CETP spike.

    For each shock event the join looks PIPE_TRAVEL_MINUTES back in the
    industrial stream to find the factory record that most closely aligns
    with the arrival time of the pollution plume at the CETP inlet.

    Args:
        shock_events:      Output of tripwire.detect_anomalies().
                           Columns: time, cod_value, breach_mag, alert_level.
        industrial_stream: Clean factory stream from aggregate.build_industrial_stream().
                           Columns: time, factory_id, cod, bod, ph, tss, status.

    Returns:
        evidence_table — Pathway Table with columns:
            cetp_event_time   (str)   — timestamp of the CETP breach
            cetp_cod          (float) — CETP COD reading at breach
            breach_mag        (float) — breach magnitude above baseline
            alert_level       (str)   — "HIGH" / "MEDIUM"
            backtrack_time    (str)   — T_backtrack used for the join
            attributed_factory(str)   — factory_id of the matched discharge
            factory_cod       (float) — factory COD at T_backtrack
            factory_bod       (float) — factory BOD at T_backtrack
            factory_tss       (float) — factory TSS at T_backtrack
    """
    # NOTE: Pathway asof_join requires integer/numeric time values.
    # We express all times as Unix milliseconds for the join key.
    # The pw.cast helpers below convert the 'time' string column to that form.

    tolerance_ms = ASOF_TOLERANCE_SECONDS * 1000  # ±2 min in ms
    travel_ms    = PIPE_TRAVEL_MINUTES * 60 * 1000  # 15 min in ms

    # Step 1 — Parse shock event timestamps to milliseconds
    shock_with_ts = shock_events.with_columns(
        event_ts_ms = pw.apply(
            lambda t: int(
                pw.DateTimeNaive(t, fmt="%Y-%m-%d %H:%M").timestamp() * 1000
            ),
            pw.this.time,
        ),
        # Compute the backtrack key that the right side will be matched against
        backtrack_ts_ms = pw.apply(
            lambda t: int(
                pw.DateTimeNaive(t, fmt="%Y-%m-%d %H:%M").timestamp() * 1000
            ) - travel_ms,
            pw.this.time,
        ),
        backtrack_time = pw.apply(
            lambda t: str(
                pw.DateTimeNaive(t, fmt="%Y-%m-%d %H:%M") - pw.Duration(minutes=PIPE_TRAVEL_MINUTES)
            ),
            pw.this.time,
        ),
    )

    # Step 2 — Parse factory timestamps to milliseconds
    factory_with_ts = industrial_stream.with_columns(
        factory_ts_ms = pw.apply(
            lambda t: int(
                pw.DateTimeNaive(t, fmt="%Y-%m-%d %H:%M").timestamp() * 1000
            ),
            pw.this.time,
        ),
    )

    # Step 3 — asof_join: left=shock events, right=factory stream
    # Joins each shock event to the factory row whose timestamp is closest
    # to T_backtrack, searching within ±ASOF_TOLERANCE_SECONDS.
    #
    # NOTE: pw.temporal.asof_join matches the LEFT row's join key (backtrack_ts_ms)
    # against the RIGHT row's join key (factory_ts_ms), finding the closest row
    # *at or before* the key (backward-looking). The tolerance bounds the window.
    evidence = pw.temporal.asof_join(
        shock_with_ts,
        factory_with_ts,
        shock_with_ts.backtrack_ts_ms,
        factory_with_ts.factory_ts_ms,
        how=pw.JoinMode.LEFT,
        tolerance=pw.Duration(milliseconds=tolerance_ms),
    ).select(
        cetp_event_time    = pw.left.time,
        cetp_cod           = pw.left.cod_value,
        breach_mag         = pw.left.breach_mag,
        alert_level        = pw.left.alert_level,
        backtrack_time     = pw.left.backtrack_time,
        attributed_factory = pw.right.factory_id,
        factory_cod        = pw.right.cod,
        factory_bod        = pw.right.bod,
        factory_tss        = pw.right.tss,
    )

    return evidence

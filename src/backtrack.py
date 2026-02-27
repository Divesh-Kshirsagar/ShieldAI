"""
SHIELD AI — Phase 1: Temporal Backtracking (The "Time Machine" Join)
=====================================================================

The core attribution engine. For each CETP shock event at time T:
    1. Compute T_backtrack = T − PIPE_TRAVEL_MINUTES.
    2. Run interval_join_left against the Industrial Discharge Stream,
       finding factory rows within ±ASOF_TOLERANCE window of T_backtrack.
    3. The matched factory row becomes the evidence record.

NOTE: PIPE_TRAVEL_MINUTES = 15 is a FIXED CONSTANT for v1.
In v2 this will be replaced by a dynamic, pipe-length-aware calculation
derived from GIS network data and real-time flow-rate sensors.
Do NOT remove this constant — it is the single source of truth for the
temporal offset used in the attribution logic.

Why interval_join_left (not asof_join)
---------------------------------------
In Pathway 0.29.x, asof_join with JoinMode.LEFT across heterogeneous streaming
tables causes a Rust-level panic ("key missing in output table"). interval_join_left
is the stable, recommended alternative for bounded-window temporal joins.
It matches each LEFT row to ALL RIGHT rows whose time falls within:
    [LEFT.backtrack_ts + lower_bound,  LEFT.backtrack_ts + upper_bound]

Usage
-----
    from src.backtrack import attribute_factory
    evidence = attribute_factory(shock_events, industrial_stream)
"""

import datetime

import pathway as pw

from src.constants import PIPE_TRAVEL_MINUTES, ASOF_TOLERANCE_SECONDS


def attribute_factory(
    shock_events: pw.Table,
    industrial_stream: pw.Table,
) -> pw.Table:
    """Run the temporal backtrack join to attribute a factory to each CETP spike.

    Args:
        shock_events:      Output of tripwire.detect_anomalies().
                           Columns: time (str), cod_value, breach_mag, alert_level.
        industrial_stream: Clean factory stream (NORMAL rows only).
                           Columns: time (str), factory_id, cod, bod, ph, tss.

    Returns:
        Pathway Table (evidence_table) with columns:
            cetp_event_time     — CETP breach timestamp string
            cetp_cod            — CETP COD at breach
            breach_mag          — magnitude above baseline
            alert_level         — HIGH / MEDIUM
            attributed_factory  — factory_id of matched discharge
            factory_cod         — factory COD at T_backtrack window
            factory_bod         — factory BOD at T_backtrack window
            factory_tss         — factory TSS at T_backtrack window
    """
    travel_duration  = datetime.timedelta(minutes=PIPE_TRAVEL_MINUTES)
    tolerance_td     = datetime.timedelta(seconds=ASOF_TOLERANCE_SECONDS)

    # Step 1 — Parse timestamps to DatetimeNaive and compute backtrack key
    shock_ts = shock_events.with_columns(
        backtrack_ts = pw.this.time.dt.strptime(fmt="%Y-%m-%d %H:%M")
                       - pw.Duration(minutes=PIPE_TRAVEL_MINUTES),
    )

    factory_ts = industrial_stream.with_columns(
        ts = pw.this.time.dt.strptime(fmt="%Y-%m-%d %H:%M")
    )

    # Step 2 — interval_join_left:
    # LEFT join key  = shock_ts.backtrack_ts
    # RIGHT join key = factory_ts.ts
    # Window: factory row must satisfy
    #   backtrack_ts - tolerance <= factory.ts <= backtrack_ts + tolerance
    #
    # JoinMode is LEFT (interval_join_left) — unmatched shock events are kept
    # with None values for factory columns (no false negatives).
    evidence = (
        shock_ts.interval_join_left(
            factory_ts,
            shock_ts.backtrack_ts,
            factory_ts.ts,
            pw.temporal.Interval(
                lower_bound=-tolerance_td,
                upper_bound= tolerance_td,
            ),
        )
        .select(
            cetp_event_time    = pw.left.time,
            cetp_cod           = pw.left.cod_value,
            breach_mag         = pw.left.breach_mag,
            alert_level        = pw.left.alert_level,
            attributed_factory = pw.right.factory_id,
            factory_cod        = pw.right.cod,
            factory_bod        = pw.right.bod,
            factory_tss        = pw.right.tss,
        )
    )

    return evidence

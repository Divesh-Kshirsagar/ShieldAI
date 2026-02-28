"""
SHIELD AI — Windowed Statistics
================================

Computes per-sensor sliding-window statistics using Pathway's native
``windowby`` + ``pw.temporal.sliding`` engine.  The output table
``windowed_stats`` is the authoritative source of rolling statistics consumed
by ``zscore.py`` for z-score computation.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HOW PATHWAY'S INCREMENTAL RECOMPUTATION WORKS FOR THIS WINDOW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Pathway treats every table as a *changelog* of (key, value, +1/-1) deltas
rather than a materialised snapshot.  When a new sensor row arrives:

  1. Pathway computes which windows the new timestamp falls into.
     A row at time T belongs to every window [T - duration + hop, T] that
     covers it — typically (duration / hop) windows overlap at any instant.

  2. For each affected window Pathway emits a *positive delta* for the new
     row and, if an old row is sliding out of a window, a *negative delta*
     for the expiring row.

  3. The reducers (avg, min, max, count) maintain *partial aggregates*:
     - avg  is tracked as (running_sum, running_count); a new row adds to
       both; an expiring row subtracts from both.  No re-scan of the full
       window is ever performed.
     - min/max use a monotonic deque (sliding minimum/maximum) of size O(n)
       updated in O(1) amortised time per event.
     - count increments +1 / decrements -1 per delta.

  4. Only the changed windows are updated — windows that received no new or
     expiring rows are untouched.

WHY THIS IS MORE EFFICIENT THAN RE-SCANNING:

  A naïve rolling-window implementation re-reads every row inside the window
  for every new arriving row → O(W · N) work where W = window size in rows
  and N = total rows.  Pathway's incremental approach processes each row
  exactly twice (once on entry, once on expiry), giving O(N) total work
  regardless of window size.  For a 30-second window on 1-minute data with
  WINDOW_HOP_MS=5000 this is 6× fewer reducer evaluations than re-scan.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Inputs
------
    sensor_stream: Pathway Table with columns:
        sensor_id (str), time (str as TIME_FORMAT), value (float).

Outputs
-------
    windowed_stats: Pathway Table with columns:
        sensor_id    (str)   — channel identifier
        window_start (str)   — bucket start (ISO-like, formatted from _pw_window_start)
        window_end   (str)   — bucket end   (ISO-like, formatted from _pw_window_end)
        mean         (float) — avg(value) over the window
        std          (float) — population std derived from avg(value²) − avg(value)²
        min          (float) — min(value) over the window
        max          (float) — max(value) over the window
        sample_count (int)   — number of readings in the window

Assumptions
-----------
- sensor_stream rows have already been renamed to sensor_id / value / time
  (i.e. ingest / aggregate output has been passed through zscore._attach_sensor_value).
- time strings match CONFIG['TIME_FORMAT'].
- No I/O, no sinks, no pw.run() — pure Pathway graph construction.
- All parameters come from config.CONFIG.
"""

from __future__ import annotations

import logging
import math

import pathway as pw

from src.config import CONFIG as _cfg

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level CONFIG — all window parameters from config.CONFIG.
# ---------------------------------------------------------------------------

CONFIG: dict = {
    "WINDOW_DURATION_MS": _cfg.window_duration_ms,  # sliding window length (ms)
    "WINDOW_HOP_MS":      _cfg.window_hop_ms,        # hop between window starts (ms)
    "TIME_FORMAT":        _cfg.input_time_format,    # strptime format for sensor time
    "EPSILON":            _cfg.epsilon,              # stddev floor; prevents zero-div
}


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

class WindowedStatsSchema(pw.Schema):
    """Output schema for the windowed_stats table — one row per (sensor, window)."""

    sensor_id:    str
    window_start: str
    window_end:   str
    mean:         float
    std:          float
    min:          float
    max:          float
    sample_count: int


# ---------------------------------------------------------------------------
# Pure-Python helper
# ---------------------------------------------------------------------------

def _population_std(mean: float, mean_sq: float) -> float:
    """Compute population std from E[X] and E[X²] using the algebraic identity.

    Var(X) = E[X²] − E[X]²;  std = sqrt(max(0, Var(X))) + EPSILON.
    The max(0, …) clamp absorbs floating-point rounding that can yield tiny
    negative variances when X values are nearly constant.
    """
    variance = mean_sq - mean * mean
    return math.sqrt(max(0.0, variance)) + CONFIG["EPSILON"]


# ---------------------------------------------------------------------------
# Pathway UDFs
# ---------------------------------------------------------------------------

@pw.udf
def _udf_std(mean: float, mean_sq: float) -> float:
    """Derive population standard deviation from the two avg() reducers."""
    return _population_std(mean, mean_sq)


@pw.udf
def _udf_format_window_bound(dt_naive: pw.DateTimeNaive) -> str:
    """Format a Pathway DateTimeNaive window bound as a human-readable string."""
    # pw.DateTimeNaive.strftime is available in Pathway 0.29.x.
    # The format mirrors CONFIG['TIME_FORMAT'] for downstream compatibility.
    return dt_naive.strftime(CONFIG["TIME_FORMAT"])


# ---------------------------------------------------------------------------
# Pathway graph builders
# ---------------------------------------------------------------------------

def _parse_timestamps(sensor_stream: pw.Table) -> pw.Table:
    """Attach a DateTimeNaive column ``ts`` parsed from the ``time`` string column.

    Pathway's native windowby requires a typed temporal column; string timestamps
    must be parsed once here and are never used again after windowing.
    """
    return sensor_stream.with_columns(
        ts=pw.this.time.dt.strptime(fmt=CONFIG["TIME_FORMAT"])
    )


def _build_windowed_aggregates(stream_with_ts: pw.Table) -> pw.Table:
    """Apply sliding window and reduce per (sensor_id, window) to raw aggregates.

    Uses pw.temporal.sliding(duration, hop) so that:
    - Each window spans WINDOW_DURATION_MS milliseconds of sensor history.
    - A new window is emitted every WINDOW_HOP_MS milliseconds.
    - instance=sensor_id produces independent windows per sensor channel.
    Returns a table with: sensor_id, _pw_window_start, _pw_window_end,
    mean, mean_sq, min, max, sample_count.
    """
    duration = pw.Duration(milliseconds=CONFIG["WINDOW_DURATION_MS"])
    hop      = pw.Duration(milliseconds=CONFIG["WINDOW_HOP_MS"])

    log.debug(
        "window configured",
        extra={
            "window_duration_ms": CONFIG["WINDOW_DURATION_MS"],
            "window_hop_ms":      CONFIG["WINDOW_HOP_MS"],
        },
    )

    return stream_with_ts.windowby(
        pw.this.ts,
        window=pw.temporal.sliding(duration=duration, hop=hop),
        instance=pw.this.sensor_id,
    ).reduce(
        sensor_id    = pw.this.sensor_id,
        window_start = pw.this._pw_window_start,
        window_end   = pw.this._pw_window_end,
        mean         = pw.reducers.avg(pw.this.value),
        mean_sq      = pw.reducers.avg(pw.this.value_sq),
        min          = pw.reducers.min(pw.this.value),
        max          = pw.reducers.max(pw.this.value),
        sample_count = pw.reducers.count(),
    )


def _derive_std_column(aggregates: pw.Table) -> pw.Table:
    """Add the ``std`` column computed from mean and mean_sq via the variance identity."""
    return aggregates.with_columns(
        std=_udf_std(pw.this.mean, pw.this.mean_sq),
    )


def _format_window_bounds(aggregates: pw.Table) -> pw.Table:
    """Convert DateTimeNaive window bounds to CONFIG['TIME_FORMAT'] strings."""
    return aggregates.with_columns(
        window_start=_udf_format_window_bound(pw.this.window_start),
        window_end=_udf_format_window_bound(pw.this.window_end),
    )


def _project_windowed_stats_output(enriched: pw.Table) -> pw.Table:
    """Project to the WindowedStatsSchema output columns."""
    return enriched.select(
        pw.this.sensor_id,
        pw.this.window_start,
        pw.this.window_end,
        pw.this.mean,
        pw.this.std,
        pw.this.min,
        pw.this.max,
        pw.this.sample_count,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_windowed_stats(sensor_stream: pw.Table) -> pw.Table:
    """Build the windowed_stats table over a generic (sensor_id, time, value) stream.

    Applies Pathway's native sliding window (duration=WINDOW_DURATION_MS,
    hop=WINDOW_HOP_MS) grouped per sensor_id.  Each output row represents one
    (sensor, window) pair and contains the complete statistical summary needed
    by the downstream z-score scorer.

    Pathway computes these statistics *incrementally*: each arriving row is
    applied as a delta to the affected windows without re-scanning the full
    window history.  See the module docstring for the full complexity analysis.

    Args:
        sensor_stream: Pathway Table with columns:
                       sensor_id (str), time (str), value (float), value_sq (float).
                       Rows must have already been renamed from raw factory schema.

    Returns:
        windowed_stats — Pathway Table matching WindowedStatsSchema.
    """
    log.info(
        "window computed",
        extra={
            "window_duration_ms": CONFIG["WINDOW_DURATION_MS"],
            "window_hop_ms":      CONFIG["WINDOW_HOP_MS"],
        },
    )
    stream_with_ts = _parse_timestamps(sensor_stream)
    aggregates     = _build_windowed_aggregates(stream_with_ts)
    with_std       = _derive_std_column(aggregates)
    formatted      = _format_window_bounds(with_std)
    windowed_stats: pw.Table = _project_windowed_stats_output(formatted)
    return windowed_stats

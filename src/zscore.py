"""
SHIELD AI — Per-Channel Rolling Z-Score Anomaly Scorer
=======================================================

Computes a per-sensor z-score by combining:
  1. windowed_stats.build_windowed_stats()  — Pathway-native sliding window stats
  2. A join of each raw reading against its window's (mean, std)
  3. UDF-based z-score and anomaly flag computation

The rolling statistics (mean, std) are no longer computed inline here.
They come from the ``windowed_stats`` table, which is built using Pathway's
``windowby`` engine with configurable ``WINDOW_DURATION_MS`` and
``WINDOW_HOP_MS``.  See ``windowed_stats.py`` for the full explanation of
Pathway's incremental recomputation model.

Z-score formula
---------------
    rolling_var = E[X²] − E[X]²        (population variance identity)
    rolling_std = sqrt(max(0, var))     (clamp fp rounding)
    z_score     = (value − mean) / (std + EPSILON)

Note on std availability
------------------------
windowed_stats.std already includes EPSILON (added inside _population_std).
The z-score denominator therefore adds no further EPSILON here.

Output table
------------
    scored_stream  (the sole public table exported by this module)

Usage
-----
    from src.zscore import build_scored_stream
    from src.aggregate import build_industrial_stream

    factory_stream = build_industrial_stream()
    scored_stream  = build_scored_stream(factory_stream)
"""

from __future__ import annotations

import logging

import pathway as pw

from src.config import CONFIG as _cfg
import src.windowed_stats as _ws

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module CONFIG — all tuneable constants from config.CONFIG.
# ---------------------------------------------------------------------------

CONFIG: dict = {
    "ZSCORE_THRESHOLD": _cfg.zscore_threshold,   # |z| above this → is_anomaly=True
    "EPSILON":          _cfg.epsilon,            # stddev floor (also applied in windowed_stats)
    "TIME_FORMAT":      _cfg.input_time_format,  # strptime format matching ingest CSVs
}


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

class ScoredSchema(pw.Schema):
    """Output schema for the scored_stream table."""

    sensor_id:    str
    timestamp:    str
    value:        float
    rolling_mean: float
    rolling_std:  float
    z_score:      float
    is_anomaly:   bool


# ---------------------------------------------------------------------------
# Private helpers (pure Python — directly unit-testable)
# ---------------------------------------------------------------------------

def _zscore(value: float, mean: float, std: float) -> float:
    """Compute z = (value − mean) / std; std already includes EPSILON floor.

    A purely functional helper — no Pathway dependencies — so it can be tested
    with plain floats without a Pathway runtime.
    """
    return (value - mean) / std


def _is_anomaly(z_score: float) -> bool:
    """Return True when |z_score| exceeds ZSCORE_THRESHOLD."""
    return abs(z_score) > CONFIG["ZSCORE_THRESHOLD"]


# ---------------------------------------------------------------------------
# Pathway UDFs
# ---------------------------------------------------------------------------

@pw.udf
def _udf_zscore(value: float, mean: float, std: float) -> float:
    """Score one reading relative to its window's rolling distribution."""
    z = _zscore(value, mean, std)
    if log.isEnabledFor(logging.DEBUG):
        log.debug(
            "z_score computed",
            extra={"value": value, "mean": round(mean, 4), "z_score": round(z, 4)},
        )
    return z


@pw.udf
def _udf_is_anomaly(z_score: float) -> bool:
    """Return True when the absolute z-score exceeds ZSCORE_THRESHOLD."""
    return _is_anomaly(z_score)


# ---------------------------------------------------------------------------
# Private graph-building helpers
# ---------------------------------------------------------------------------

def _attach_sensor_value(factory_stream: pw.Table) -> pw.Table:
    """Rename factory columns to the generic sensor_id / value names and add value_sq.

    value_sq is required by windowed_stats._build_windowed_aggregates() for
    computing the mean-of-squares used in the variance identity.
    """
    return factory_stream.with_columns(
        sensor_id = pw.this.factory_id,
        value     = pw.this.cod,
        value_sq  = pw.this.cod * pw.this.cod,
    )


def _join_readings_to_windows(
    sensor_stream: pw.Table,
    windowed_stats: pw.Table,
) -> pw.Table:
    """Join each individual reading to its matching window aggregate.

    Joins on sensor_id and on the reading's time string falling at the
    window_end boundary (the reduce already carries window_endtime as the
    max(time) within the window).  Each row in the result carries both the
    raw value and the window's (mean, std) needed for z-scoring.

    Streaming semantics: Pathway performs this as a streaming join; rows only
    appear in the output once BOTH the left reading AND the window aggregate
    for that (sensor_id, window_end) are available.
    """
    return sensor_stream.join(
        windowed_stats,
        pw.left.sensor_id == pw.right.sensor_id,
        pw.left.time      == pw.right.window_end,
    ).select(
        sensor_id    = pw.left.sensor_id,
        timestamp    = pw.left.time,
        value        = pw.left.value,
        rolling_mean = pw.right.mean,
        rolling_std  = pw.right.std,
    )


def _score_readings(joined: pw.Table) -> pw.Table:
    """Attach z_score and is_anomaly columns to the joined reading-window table.

    Streaming semantics: each row in ``joined`` is processed independently;
    z_score and is_anomaly are pure per-row transformations with no state.
    """
    with_z = joined.with_columns(
        z_score=_udf_zscore(
            pw.this.value,
            pw.this.rolling_mean,
            pw.this.rolling_std,
        )
    )
    return with_z.with_columns(
        is_anomaly=_udf_is_anomaly(pw.this.z_score),
    )


def _project_scored_output(scored: pw.Table) -> pw.Table:
    """Project to the declared ScoredSchema output schema."""
    return scored.select(
        pw.this.sensor_id,
        pw.this.timestamp,
        pw.this.value,
        pw.this.rolling_mean,
        pw.this.rolling_std,
        pw.this.z_score,
        pw.this.is_anomaly,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_scored_stream(factory_stream: pw.Table) -> pw.Table:
    """Build the per-channel rolling z-score anomaly stream from a factory table.

    Orchestrates the full scoring pipeline:
      1. Rename factory columns to the generic sensor vocabulary.
      2. Delegate to windowed_stats.build_windowed_stats() for window-level stats.
      3. Join each raw reading back to its window's (mean, std).
      4. Compute z_score and is_anomaly per reading.
      5. Project to ScoredSchema.

    Streaming semantics: Pathway processes each factory row as a delta.  The
    windowed_stats table is updated incrementally (see windowed_stats module
    docstring for complexity analysis).  The join and scoring steps add no
    additional state — they are pure row-level transforms on the changelog.

    Args:
        factory_stream: Pathway Table with at minimum:
                        factory_id (str), time (str), cod (float).

    Returns:
        scored_stream — Pathway Table matching ScoredSchema with columns:
            sensor_id, timestamp, value, rolling_mean, rolling_std,
            z_score, is_anomaly.
    """
    log.info(
        "scoring pipeline building",
        extra={
            "zscore_threshold": CONFIG["ZSCORE_THRESHOLD"],
            "epsilon":          CONFIG["EPSILON"],
        },
    )
    # Step 1 — generic sensor vocabulary + value_sq for variance computation
    sensor_stream = _attach_sensor_value(factory_stream)

    # Step 2 — sliding-window statistics via Pathway's native windowby engine
    windowed_stats = _ws.build_windowed_stats(sensor_stream)

    # Step 3 — join each point reading to its matching (sensor_id, window_end)
    joined = _join_readings_to_windows(sensor_stream, windowed_stats)

    # Step 4 — z-score and anomaly flag
    scored = _score_readings(joined)

    # Step 5 — project to public schema
    scored_stream: pw.Table = _project_scored_output(scored)
    return scored_stream

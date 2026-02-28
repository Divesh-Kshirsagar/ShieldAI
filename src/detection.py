"""
SHIELD AI — Detection Pipeline Facade
======================================

Single public entry point for the anomaly detection pipeline stage.
Orchestrates two sub-modules:

    1. zscore.py   — per-channel rolling z-score scoring
    2. persistence.py — consecutive-anomaly gate (emits confirmed alerts only)

Inputs
------
    input_table: Pathway Table from the ingest / aggregate layer with columns:
        factory_id (str), time (str), cod (float)

Outputs
-------
    pw.Table (named ``scored_stream`` by convention) with the confirmed-anomaly
    schema from persistence.ScoredPersistenceSchema:
        sensor_id         (str)   — channel identifier
        timestamp         (str)   — reading time string
        consecutive_count (int)   — streak length at confirmation
        z_score           (float) — z-score of the confirming reading
        value             (float) — raw sensor value

Assumptions
-----------
- Input rows contain no null ``cod`` values (caller must pre-filter BLACKOUT rows).
- ``time`` strings match config.CONFIG.input_time_format ("%Y-%m-%d %H:%M" default).
- Pathway engine is already configured by the caller; this module never calls pw.run().
- All tuning parameters are read from config.CONFIG — no magic numbers here.

Usage
-----
    from src.detection import build_scored_stream
    from src.aggregate import build_industrial_stream

    scored_stream = build_scored_stream(build_industrial_stream())
"""

from __future__ import annotations

import logging

import pathway as pw

from src import persistence as _persistence
from src import zscore as _zscore
from src.config import CONFIG as _cfg

# ---------------------------------------------------------------------------
# Module logger — handlers configured by the caller, not this module
# ---------------------------------------------------------------------------

logger: logging.Logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _validate_input_columns(input_table: pw.Table) -> None:
    """Raise ValueError if required columns are absent from input_table."""
    required = {
        _cfg.input_schema_sensor_column,  # e.g. "factory_id"
        _cfg.input_schema_value_column,   # e.g. "cod"
        "time",
    }
    available = set(input_table.schema.column_names())
    missing = required - available
    if missing:
        raise ValueError(
            f"detection.build_scored_stream: input_table is missing "
            f"required columns: {sorted(missing)}. "
            f"Available columns: {sorted(available)}"
        )


def _log_pipeline_config() -> None:
    """Emit pipeline configuration at DEBUG level for operator visibility."""
    logger.debug(
        "Detection pipeline config — window_seconds=%d  zscore_threshold=%.2f  "
        "persistence_count=%d  sensor_col=%r  value_col=%r",
        _cfg.window_seconds,
        _cfg.zscore_threshold,
        _cfg.persistence_count,
        _cfg.input_schema_sensor_column,
        _cfg.input_schema_value_column,
    )


def _apply_zscore(input_table: pw.Table) -> pw.Table:
    """Pass input_table through the z-score scorer and return the scored intermediate."""
    logger.debug("Detection: building z-score scored stream")
    return _zscore.build_scored_stream(input_table)


def _apply_persistence_gate(intermediate: pw.Table) -> pw.Table:
    """Apply the persistence filter to the z-scored stream and return confirmed alerts."""
    logger.debug(
        "Detection: applying persistence gate (PERSISTENCE_COUNT=%d)",
        _cfg.persistence_count,
    )
    return _persistence.build_confirmed_anomalies(intermediate)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_scored_stream(input_table: pw.Table) -> pw.Table:
    """Build the full detection pipeline and return the confirmed-anomaly stream.

    Orchestrates z-score scoring followed by persistence filtering.
    No I/O, no sinks, no pw.run() — pure Pathway graph construction.

    Args:
        input_table: Raw factory/sensor Pathway Table from ingest or aggregate.
                     Must contain: factory_id (or configured sensor column),
                     cod (or configured value column), time (str).

    Returns:
        scored_stream — Pathway Table with confirmed anomalies only, containing:
            sensor_id, timestamp, consecutive_count, z_score, value.
    """
    _log_pipeline_config()

    # Stage 1: z-score scoring (per-channel rolling statistics)
    intermediate: pw.Table = _apply_zscore(input_table)

    # Stage 2: persistence gate (emit only after N consecutive anomalies)
    scored_stream: pw.Table = _apply_persistence_gate(intermediate)

    logger.debug("Detection: graph construction complete")
    return scored_stream

"""
SHIELD AI — Pipeline Instrumentation
======================================

Threads ingestion timestamps through the Pathway pipeline and emits a
``metrics_stream`` table with per-event latency measurements at each stage.

Architecture
------------
Two concerns are separated:

  Timestamp injection  — a module-level _TimelineStore (same stateful UDF
                         pattern as _ZScoreTracker and _CooldownStore) records
                         wall-clock timestamps at four pipeline stages:
                         ingestion, scoring, eri, alert.

  Metrics projection   — build_metrics_stream() produces one row per (event_id,
                         pipeline_stage) pair by querying the timeline store,
                         and feeds the LatencyCollector for P50/P99 reporting.

Event identity
--------------
An event is identified by the string key "{sensor_id}|{time}" — the same
(sensor, timestamp) pair the pipeline uses throughout.  This avoids threading
an extra column through every intermediate table.

Pathway integration
-------------------
Call instrument_ingestion(stream)  on the raw sensor stream.
Call instrument_scoring(stream)    after zscore scoring.
Call instrument_eri(stream)        after ERI enrichment.
Call instrument_alert(stream)      after alerts.build_alert_stream().
Call build_metrics_stream(stream)  on the alert stream to get metrics_stream.

The instrumented streams are pass-through: they return the same table unchanged,
recording the timestamp as a side effect inside the UDF.

Assumptions
-----------
- No I/O, no sinks beyond side-effect UDFs and LatencyCollector.record().
- All parameters from config.CONFIG.
- metrics.py (LatencyCollector, MetricsReporter) has no Pathway dependency.
"""

from __future__ import annotations

import logging
import time

import pathway as pw

from src.config import CONFIG as _cfg
from src.metrics import LatencyCollector, MetricsReporter, format_latency_summary

logger: logging.Logger = logging.getLogger(__name__)

CONFIG: dict = {
    "METRICS_LOG_INTERVAL_SECONDS": _cfg.metrics_log_interval_seconds,
}

# Shared collector and reporter — module singletons, reset between test runs.
collector: LatencyCollector  = LatencyCollector()
reporter:  MetricsReporter   = MetricsReporter(
    collector,
    interval_seconds=CONFIG["METRICS_LOG_INTERVAL_SECONDS"],
    logger=logger,
)


# ---------------------------------------------------------------------------
# Pipeline stage timeline store
# ---------------------------------------------------------------------------

# Stage name constants used as keys and as pipeline_stage column values.
STAGE_INGESTION = "ingestion"
STAGE_SCORING   = "scoring"
STAGE_ERI       = "eri"
STAGE_ALERT     = "alert"


class _TimelineStore:
    """Record wall-clock timestamps for each pipeline stage per event_id.

    Key: event_id string "{sensor_id}|{event_time}".
    Value: dict mapping stage name → time.time() float.

    The store grows monotonically during a pipeline run.  For production use,
    consider periodically evicting old entries (e.g., after alert emission).
    """

    def __init__(self) -> None:
        """Initialise with empty timeline."""
        self._store: dict[str, dict[str, float]] = {}

    def record(self, event_id: str, stage: str) -> None:
        """Record the current wall-clock time for event_id at stage."""
        if event_id not in self._store:
            self._store[event_id] = {}
        self._store[event_id][stage] = time.time()

    def get_stage_time(self, event_id: str, stage: str) -> float:
        """Return the recorded wall-clock time for (event_id, stage), or 0.0."""
        return self._store.get(event_id, {}).get(stage, 0.0)

    def latency_ms(self, event_id: str, from_stage: str, to_stage: str) -> float:
        """Return (to_stage_time − from_stage_time) × 1000, or -1.0 if unknown."""
        t0 = self.get_stage_time(event_id, from_stage)
        t1 = self.get_stage_time(event_id, to_stage)
        if t0 == 0.0 or t1 == 0.0:
            return -1.0
        return (t1 - t0) * 1000.0

    def reset(self) -> None:
        """Clear all recorded timelines (for testing)."""
        self._store.clear()


_timeline: _TimelineStore = _TimelineStore()


# ---------------------------------------------------------------------------
# Pure helpers (stat-less, testable without Pathway)
# ---------------------------------------------------------------------------

def make_event_id(sensor_id: str, event_time: str) -> str:
    """Combine sensor_id and event_time into a unique event identifier string."""
    return f"{sensor_id}|{event_time}"


# ---------------------------------------------------------------------------
# Pathway UDFs — one per pipeline stage
# ---------------------------------------------------------------------------

@pw.udf
def _udf_record_ingestion(sensor_id: str, event_time: str) -> str:
    """Record ingestion timestamp; return event_id for downstream columns."""
    eid = make_event_id(sensor_id, event_time)
    _timeline.record(eid, STAGE_INGESTION)
    return eid


@pw.udf
def _udf_record_scoring(event_id: str) -> str:
    """Record scoring-stage timestamp; pass event_id through unchanged."""
    _timeline.record(event_id, STAGE_SCORING)
    return event_id


@pw.udf
def _udf_record_eri(event_id: str) -> str:
    """Record ERI-stage timestamp; pass event_id through unchanged."""
    _timeline.record(event_id, STAGE_ERI)
    return event_id


@pw.udf
def _udf_record_alert(event_id: str) -> str:
    """Record alert-stage timestamp; pass event_id through unchanged."""
    _timeline.record(event_id, STAGE_ALERT)
    return event_id


@pw.udf
def _udf_alert_timestamp(event_id: str) -> float:
    """Return the wall-clock time of alert emission for this event."""
    return _timeline.get_stage_time(event_id, STAGE_ALERT)


@pw.udf
def _udf_ingestion_timestamp(event_id: str) -> float:
    """Return the wall-clock time of ingestion for this event."""
    return _timeline.get_stage_time(event_id, STAGE_INGESTION)


@pw.udf
def _udf_latency_ms(event_id: str) -> float:
    """Return end-to-end latency in ms (ingestion → alert); -1.0 if incomplete."""
    latency = _timeline.latency_ms(event_id, STAGE_INGESTION, STAGE_ALERT)
    if latency >= 0.0:
        collector.record(latency)
        reporter.maybe_report()
    return latency


@pw.udf
def _udf_stage_latency_ms(event_id: str, stage: str) -> float:
    """Return ingestion→stage latency in ms; -1.0 if stage timestamp missing."""
    return _timeline.latency_ms(event_id, STAGE_INGESTION, stage)


# ---------------------------------------------------------------------------
# Instrumentation pass-through builders
# ---------------------------------------------------------------------------

def instrument_ingestion(sensor_stream: pw.Table) -> pw.Table:
    """Attach event_id and record ingestion timestamp on the raw sensor stream.

    Returns the same stream enriched with an ``event_id`` column.
    The event_id column is propagated through all downstream tables so the
    timeline store can correlate measurements across stages.

    Streaming semantics: timestamp is recorded when Pathway processes the row —
    i.e., at the moment the row enters the computation graph, not at CSV read.
    """
    return sensor_stream.with_columns(
        event_id=_udf_record_ingestion(pw.this.sensor_id, pw.this.time),
        ingestion_timestamp=_udf_ingestion_timestamp(
            _udf_record_ingestion(pw.this.sensor_id, pw.this.time)
        ),
    )


def instrument_scoring(scored_stream: pw.Table) -> pw.Table:
    """Record scoring-stage timestamp; passes the stream through unchanged.

    Requires scored_stream to carry an ``event_id`` column from instrument_ingestion.
    """
    return scored_stream.with_columns(
        event_id=_udf_record_scoring(pw.this.event_id),
    )


def instrument_eri(eri_stream: pw.Table) -> pw.Table:
    """Record ERI-stage timestamp; passes the stream through unchanged."""
    return eri_stream.with_columns(
        event_id=_udf_record_eri(pw.this.event_id),
    )


def instrument_alert(alert_stream: pw.Table) -> pw.Table:
    """Record alert-stage timestamp and attach timestamps to the alert stream."""
    with_eid = alert_stream.with_columns(
        event_id=_udf_record_alert(pw.this.event_id),
    )
    return with_eid.with_columns(
        alert_timestamp    = _udf_alert_timestamp(pw.this.event_id),
        ingestion_timestamp= _udf_ingestion_timestamp(pw.this.event_id),
    )


def build_metrics_stream(instrumented_alert_stream: pw.Table) -> pw.Table:
    """Build the metrics_stream table from an instrumented alert stream.

    Emits one row for each alert for each of the four pipeline stages.
    Each row contains: event_id, latency_ms (ingestion→stage), pipeline_stage,
    stage_timestamp (wall-clock seconds), and end-to-end latency_ms.

    This function also drives LatencyCollector.record() and MetricsReporter
    via the _udf_latency_ms side effect.

    Args:
        instrumented_alert_stream: Output of instrument_alert(), carrying
                                   event_id, ingestion_timestamp, alert_timestamp.

    Returns:
        metrics_stream — Pathway Table with columns:
            event_id, latency_ms, pipeline_stage, stage_timestamp.
    """
    # End-to-end latency (also drives collector + reporter as a side effect)
    with_e2e = instrumented_alert_stream.with_columns(
        latency_ms=_udf_latency_ms(pw.this.event_id),
    )

    # Emit one row per stage for the metrics breakdown
    stage_rows = []
    for stage in (STAGE_INGESTION, STAGE_SCORING, STAGE_ERI, STAGE_ALERT):
        @pw.udf
        def _stage_lat(eid: str, _s: str = stage) -> float:
            return _udf_stage_latency_ms(eid, _s)

        @pw.udf
        def _stage_ts(eid: str, _s: str = stage) -> float:
            return _timeline.get_stage_time(eid, _s)

        @pw.udf
        def _stage_name(_: str, _s: str = stage) -> str:
            return _s

        stage_row = with_e2e.select(
            event_id       = pw.this.event_id,
            latency_ms     = _stage_lat(pw.this.event_id),
            pipeline_stage = _stage_name(pw.this.event_id),
            stage_timestamp= _stage_ts(pw.this.event_id),
        )
        stage_rows.append(stage_row)

    metrics_stream: pw.Table = pw.Table.concat_reindex(*stage_rows)
    return metrics_stream

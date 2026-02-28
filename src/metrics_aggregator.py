"""
SHIELD AI — Real-Time Metrics Aggregator
========================================

Computes system-wide KPIs using Pathway aggregations and emits them
periodically to a JSON file and a Pathway table.

Metrics:
--------
- events_processed_total: Running count of all input events.
- anomalies_detected_total: Running count of confirmed anomalies.
- active_alerts_count: Unique sensors currently in their alert cooldown.
- avg_eri_last_5min: Avg ERI score over last 5 min (all discharge points).
- highest_risk_band: Max risk level (1-4) across active alerts.
- pipeline_uptime_seconds: Time since CONFIG.pipeline_start_time.
- last_event_timestamp: Max event time seen so far.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path

import pathway as pw

from src.config import CONFIG as _cfg
from src.config import PIPELINE_START_TIME

log = logging.getLogger(__name__)

# Map risk bands to integers for max computation
_RISK_MAP = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
_INV_RISK_MAP = {v: k for k, v in _RISK_MAP.items()}


# ---------------------------------------------------------------------------
# Internal Helpers
# ---------------------------------------------------------------------------

def _parse_timestamps(stream: pw.Table) -> pw.Table:
    """Parse the string 'timestamp' column into a Pathway datetime object."""
    return stream.with_columns(
        _ts=pw.string_to_datetime(pw.this.timestamp, format=_cfg.input_time_format)
    )


# ---------------------------------------------------------------------------
# Aggregation Functions (Named per requirement)
# ---------------------------------------------------------------------------

def compute_total_events(input_stream: pw.Table) -> pw.Table:
    """Return a single-row table with the running count of all events."""
    return input_stream.reduce(count=pw.reducers.count())


def compute_total_anomalies(anomaly_stream: pw.Table) -> pw.Table:
    """Return a single-row table with the running count of anomalies."""
    return anomaly_stream.reduce(count=pw.reducers.count())


def compute_active_alerts(alert_stream: pw.Table) -> pw.Table:
    """Count unique sensors currently within their alert cooldown window."""
    cooldown_ms = _cfg.alert_cooldown_seconds * 1000
    
    # Ensure timestamps are parsed for windowing
    stream_with_ts = _parse_timestamps(alert_stream)
    
    return stream_with_ts.windowby(
        pw.this._ts,
        window=pw.temporal.sliding(duration=pw.Duration(milliseconds=cooldown_ms), hop=pw.Duration(seconds=1))
    ).reduce(
        count=pw.reducers.count_distinct(pw.this.discharge_point_id)
    )


def compute_avg_eri(eri_stream: pw.Table) -> pw.Table:
    """Compute average ERI over the last 5 minutes."""
    stream_with_ts = _parse_timestamps(eri_stream)
    
    return stream_with_ts.windowby(
        pw.this._ts,
        window=pw.temporal.sliding(duration=pw.Duration(minutes=5), hop=pw.Duration(seconds=10))
    ).reduce(
        avg_eri=pw.reducers.avg(pw.this.eri)
    )


def compute_highest_risk(alert_stream: pw.Table) -> pw.Table:
    """Find the highest risk band across alerts in the cooldown window."""
    @pw.udf
    def risk_to_int(band: str) -> int:
        return _RISK_MAP.get(band, 0)

    cooldown_ms = _cfg.alert_cooldown_seconds * 1000
    stream_with_ts = _parse_timestamps(alert_stream)
    
    return stream_with_ts.windowby(
        pw.this._ts,
        window=pw.temporal.sliding(duration=pw.Duration(milliseconds=cooldown_ms), hop=pw.Duration(seconds=10))
    ).reduce(
        max_risk_val=pw.reducers.max(risk_to_int(pw.this.risk_band))
    )


def compute_uptime() -> float:
    """Calculate seconds since pipeline launch."""
    return time.time() - PIPELINE_START_TIME


def get_last_timestamp(input_stream: pw.Table) -> pw.Table:
    """Return the maximum event timestamp seen."""
    return input_stream.reduce(last_ts=pw.reducers.max(pw.this.time))


# ---------------------------------------------------------------------------
# Atomic JSON Sink
# ---------------------------------------------------------------------------

def _write_metrics_json(record: dict) -> None:
    """Atomically write the metrics record to CONFIG.metrics_output_path."""
    out_path = Path(_cfg.metrics_output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Use a temp file in the same directory to ensure atomic rename
    with tempfile.NamedTemporaryFile("w", dir=out_path.parent, delete=False, suffix=".json") as tf:
        json.dump(record, tf, indent=2)
        temp_name = tf.name

    try:
        os.replace(temp_name, out_path)
    except Exception as exc:
        log.error("Failed to write metrics JSON: %s", exc)
        if os.path.exists(temp_name):
            os.remove(temp_name)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def build_metrics_table(
    input_stream: pw.Table,
    anomaly_stream: pw.Table,
    eri_stream: pw.Table,
    alert_stream: pw.Table,
) -> pw.Table:
    """Combine all KPIs into a single-row table and attach JSON sink."""
    
    # 1. Gather all individual metrics
    # Note: reduce() returns a table with one row per update.
    # We join them into a single row.
    
    # Pathway 0.29.x: Joining independent single-row tables can be done via 
    # dummy cross-joins or by Union if schemas match (they don't).
    # Idiomatic way: Use a single world-scope reduction if possible, 
    # or join on a constant key.
    
    # For now, let's use a simpler approach: join on a 'dummy' key.
    
    def with_const(t, val=1):
        return t.with_columns(dummy=val)

    total_events = with_const(compute_total_events(input_stream))
    total_anomalies = with_const(compute_total_anomalies(anomaly_stream))
    last_ts = with_const(get_last_timestamp(input_stream))
    
    # Windowed metrics: these emit rows periodically. 
    # We'll take the latest 'global' state. 
    # For active_alerts, we'll use a 1s hop window.
    active_alerts = with_const(compute_active_alerts(alert_stream))
    avg_eri = with_const(compute_avg_eri(eri_stream))
    highest_risk = with_const(compute_highest_risk(alert_stream))

    # Join them all
    metrics = total_events.join(total_anomalies, on=pw.this.dummy == pw.this.dummy)
    metrics = metrics.join(last_ts, on=pw.this.dummy == pw.this.dummy)
    metrics = metrics.join(active_alerts, on=pw.this.dummy == pw.this.dummy)
    metrics = metrics.join(avg_eri, on=pw.this.dummy == pw.this.dummy)
    metrics = metrics.join(highest_risk, on=pw.this.dummy == pw.this.dummy)

    @pw.udf
    def get_uptime() -> float:
        return compute_uptime()

    @pw.udf
    def int_to_risk(val: int | None) -> str:
        if val is None or val == 0:
            return "NONE"
        return _INV_RISK_MAP.get(val, "NONE")

    pipeline_metrics = metrics.select(
        events_processed_total   = pw.this.count_left,
        anomalies_detected_total = pw.this.count_right,
        active_alerts_count      = pw.this.count,
        avg_eri_last_5min        = pw.this.avg_eri,
        highest_risk_band        = int_to_risk(pw.this.max_risk_val),
        pipeline_uptime_seconds  = get_uptime(),
        last_event_timestamp     = pw.this.last_ts,
    )

    # Attach JSON sink
    # Note: To avoid spamming, we could sample this or use a periodic trigger.
    # The requirement says "updated every METRICS_EMIT_INTERVAL_SECONDS".
    # Since avg_eri and others already hop every 10s, the joined table
    # will update at that cadence.
    
    pw.io.subscribe(pipeline_metrics, _write_metrics_json)
    
    return pipeline_metrics

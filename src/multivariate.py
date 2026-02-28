"""
SHIELD AI — Multivariate Anomaly Scorer
========================================

Combines per-sensor z-scores (from scored_stream) across configurable sensor
groups to produce a group-level composite anomaly score with causal attribution.

Algorithm
---------
For each sensor group defined in CONFIG.sensor_groups:
  1. Filter scored_stream to sensors belonging to the group.
  2. Align readings into SYNC_TOLERANCE_MS time buckets (floor division).
  3. Within each bucket, collect the z-score from every sensor that fired.
  4. Compute composite_score = sqrt(sum(z_i^2) / n)   — RMS of z-scores.
  5. Compute attribution: fraction_i = z_i^2 / sum(z_j^2) per contributor.
  6. Emit a row to group_anomalies for every time bucket.

Sensor membership is encoded as an integer bitmask (bit i = member i fired)
so that a single pw.reducers.sum() reducer tracks which sensors contributed.
Individual z-scores for attribution are tracked in _ZScoreTracker, a
module-level stateful store (same design as persistence._SensorStateStore).

Inputs
------
    scored_stream: Pathway Table from zscore.build_scored_stream() or
                   detection.build_scored_stream(), with columns:
                       sensor_id (str), timestamp (str), z_score (float)

Outputs
-------
    group_anomalies: Pathway Table with columns:
        group_name           (str)   — CONFIG.sensor_groups key
        timestamp            (str)   — latest timestamp in the bucket
        composite_score      (float) — RMS z-score across contributing sensors
        contributing_sensors (str)   — comma-separated sensor_ids that fired
        missing_sensors      (str)   — comma-separated sensor_ids absent in bucket
        is_group_anomaly     (bool)  — composite_score > GROUP_THRESHOLD
        top_contributor      (str)   — sensor_id with largest z^2 fraction
        attribution_detail   (str)   — JSON {sensor_id: fraction_3dp} descending
        alert_message        (str)   — human-readable anomaly summary

Assumptions
-----------
- sensor_id values in scored_stream match the strings in CONFIG.sensor_groups.
- timestamp strings conform to CONFIG.input_time_format.
- No I/O, no sinks, no pw.run() — pure Pathway graph construction.
- All tuning parameters come from config.CONFIG.
"""

from __future__ import annotations

import logging
import math

import pathway as pw

import src.attribution as _attribution
import src.config as _config_mod

logger: logging.Logger = logging.getLogger(__name__)

CONFIG: dict = {
    "SENSOR_GROUPS":     _config_mod.CONFIG.sensor_groups,    # group → [sensor_ids]
    "GROUP_THRESHOLD":   _config_mod.CONFIG.group_threshold,  # RMS threshold for alarm
    "SYNC_TOLERANCE_MS": _config_mod.CONFIG.sync_tolerance_ms, # bucket width (ms)
}


# ---------------------------------------------------------------------------
# Stateful store for per-sensor z-scores (attribution data)
# ---------------------------------------------------------------------------

class _ZScoreTracker:
    """Accumulate individual sensor z-scores per (group_name, time_bucket)."""

    def __init__(self) -> None:
        """Initialise with empty store."""
        self._store: dict[tuple[str, str], dict[str, float]] = {}

    def record(self, group_name: str, bucket: str, sensor_id: str, z_score: float) -> None:
        """Record or update z_score for sensor_id in the given (group, bucket)."""
        key = (group_name, bucket)
        if key not in self._store:
            self._store[key] = {}
        self._store[key][sensor_id] = z_score

    def get(self, group_name: str, bucket: str) -> dict[str, float]:
        """Return {sensor_id: z_score} for the given (group, bucket), or {}."""
        return self._store.get((group_name, bucket), {})

    def reset_all(self) -> None:
        """Clear all recorded z-scores (for testing)."""
        self._store.clear()


_z_score_tracker: _ZScoreTracker = _ZScoreTracker()


# ---------------------------------------------------------------------------
# Pure-Python math helpers (no external libraries)
# ---------------------------------------------------------------------------

def _rms(z_scores: list[float]) -> float:
    """Return the root-mean-square of a list of z-scores (empty list → 0.0)."""
    if not z_scores:
        return 0.0
    return math.sqrt(sum(z * z for z in z_scores) / len(z_scores))


def _sensors_from_bitmask(group_name: str, bitmask: int) -> list[str]:
    """Return sensor_ids whose bits are set in bitmask for the given group."""
    members = CONFIG["SENSOR_GROUPS"].get(group_name, [])
    return [s for i, s in enumerate(members) if bitmask & (1 << i)]


def _missing_from_bitmask(group_name: str, bitmask: int) -> list[str]:
    """Return sensor_ids whose bits are NOT set in bitmask for the given group."""
    members = CONFIG["SENSOR_GROUPS"].get(group_name, [])
    return [s for i, s in enumerate(members) if not (bitmask & (1 << i))]


def _sensor_bit(group_name: str, sensor_id: str) -> int:
    """Return the bitmask bit for sensor_id within group_name (0 if not found)."""
    members = CONFIG["SENSOR_GROUPS"].get(group_name, [])
    try:
        return 1 << members.index(sensor_id)
    except ValueError:
        return 0


def _timestamp_bucket(timestamp: str, tolerance_ms: int) -> str:
    """Truncate a timestamp string to SYNC_TOLERANCE_MS granularity for alignment.

    Converts the timestamp to seconds-since-epoch (approximated from string),
    bins it, and returns the bin start as a string. For Pathway-based use this
    is replaced by the UDF _udf_time_bucket which operates on the pw column.
    """
    import time as _time
    import datetime
    try:
        dt = datetime.datetime.strptime(timestamp, _config_mod.CONFIG.input_time_format)
        epoch_ms = int(dt.timestamp() * 1000)
        bucket_ms = (epoch_ms // tolerance_ms) * tolerance_ms
        bucket_dt = datetime.datetime.fromtimestamp(bucket_ms / 1000)
        return bucket_dt.strftime(_config_mod.CONFIG.input_time_format)
    except (ValueError, OSError):
        return timestamp


# ---------------------------------------------------------------------------
# Pathway UDFs
# ---------------------------------------------------------------------------

@pw.udf
def _udf_time_bucket(timestamp: str) -> str:
    """Bin a timestamp string into SYNC_TOLERANCE_MS-wide alignment buckets."""
    return _timestamp_bucket(timestamp, CONFIG["SYNC_TOLERANCE_MS"])


@pw.udf
def _udf_composite_score(sum_sq: float, sensor_count: int) -> float:
    """Compute RMS z-score from the aggregated sum-of-squares and count."""
    if sensor_count == 0:
        return 0.0
    return math.sqrt(sum_sq / sensor_count)


@pw.udf
def _udf_contributing_sensors(group_name: str, bitmask: int) -> str:
    """Decode contributing sensor_ids from bitmask into a comma-separated string."""
    return ",".join(_sensors_from_bitmask(group_name, bitmask))


@pw.udf
def _udf_missing_sensors(group_name: str, bitmask: int) -> str:
    """Decode missing sensor_ids from bitmask into a comma-separated string."""
    return ",".join(_missing_from_bitmask(group_name, bitmask))


@pw.udf
def _udf_is_group_anomaly(composite_score: float) -> bool:
    """Return True when composite_score exceeds GROUP_THRESHOLD."""
    return composite_score > CONFIG["GROUP_THRESHOLD"]


@pw.udf
def _udf_top_contributor(group_name: str, time_bucket: str) -> str:
    """Return the sensor_id with the largest z² fraction for this group-bucket."""
    z_scores = _z_score_tracker.get(group_name, time_bucket)
    fractions = _attribution._compute_fractions(z_scores)
    sorted_pairs = _attribution._sort_descending(fractions)
    sid, _ = _attribution._top_contributor(sorted_pairs)
    return sid


@pw.udf
def _udf_attribution_detail(group_name: str, time_bucket: str) -> str:
    """Return JSON attribution_detail string for this group-bucket."""
    z_scores = _z_score_tracker.get(group_name, time_bucket)
    fractions = _attribution._compute_fractions(z_scores)
    sorted_pairs = _attribution._sort_descending(fractions)
    return _attribution._format_attribution_detail(sorted_pairs)


@pw.udf
def _udf_alert_message(group_name: str, time_bucket: str) -> str:
    """Return a human-readable alert message for this group-bucket."""
    z_scores = _z_score_tracker.get(group_name, time_bucket)
    fractions = _attribution._compute_fractions(z_scores)
    sorted_pairs = _attribution._sort_descending(fractions)
    top_sid, top_frac = _attribution._top_contributor(sorted_pairs)
    return _attribution._format_alert_message(group_name, top_sid, top_frac)


# ---------------------------------------------------------------------------
# Pathway graph builders (one logical step per function)
# ---------------------------------------------------------------------------

def _build_membership_stream(scored_stream: pw.Table) -> pw.Table | None:
    """Annotate scored_stream with group membership columns; return None if no groups.

    For each (group_name, sensor_id) pair in CONFIG.sensor_groups, filters
    scored_stream to that sensor, attaches group_name/sensor_bit/group_size/
    z_score_sq, and records the raw z_score in _z_score_tracker for attribution.
    The per-sensor filtered streams are concatenated to form one 'exploded' view.
    """
    group_streams: list[pw.Table] = []

    for group_name, members in CONFIG["SENSOR_GROUPS"].items():
        group_size = len(members)
        for bit_pos, sensor_id in enumerate(members):
            bit_value = 1 << bit_pos

            @pw.udf
            def _tag_group(_sid: str, _gn: str = group_name) -> str:
                """Return the group name constant for this annotated row."""
                return _gn

            @pw.udf
            def _tag_bit(_sid: str, _bv: int = bit_value) -> int:
                """Return the bitmask constant for this sensor in its group."""
                return _bv

            @pw.udf
            def _tag_size(_sid: str, _gs: int = group_size) -> int:
                """Return the group size constant for this annotated row."""
                return _gs

            @pw.udf
            def _track_z(
                timestamp: str,
                z_score: float,
                _gn: str = group_name,
                _sid: str = sensor_id,
            ) -> str:
                """Record z_score in tracker; return the time_bucket string."""
                bucket = _timestamp_bucket(timestamp, CONFIG["SYNC_TOLERANCE_MS"])
                _z_score_tracker.record(_gn, bucket, _sid, z_score)
                return bucket

            sensor_stream = (
                scored_stream
                .filter(pw.this.sensor_id == sensor_id)
                .with_columns(
                    group_name  = _tag_group(pw.this.sensor_id),
                    sensor_bit  = _tag_bit(pw.this.sensor_id),
                    group_size  = _tag_size(pw.this.sensor_id),
                    z_score_sq  = pw.this.z_score * pw.this.z_score,
                    time_bucket = _track_z(pw.this.timestamp, pw.this.z_score),
                )
            )
            group_streams.append(sensor_stream)

    if not group_streams:
        logger.warning("No sensor groups configured — group_anomalies will be empty.")
        return None

    return pw.Table.concat_reindex(*group_streams)


def _aggregate_by_bucket(membership_stream: pw.Table) -> pw.Table:
    """Group by (group_name, time_bucket); reduce to RMS inputs and bitmask."""
    return membership_stream.groupby(
        pw.this.group_name,
        pw.this.time_bucket,
    ).reduce(
        group_name     = pw.this.group_name,
        time_bucket    = pw.this.time_bucket,
        timestamp      = pw.reducers.max(pw.this.timestamp),
        sum_sq_zscores = pw.reducers.sum(pw.this.z_score_sq),
        sensor_count   = pw.reducers.count(),
        sensor_bitmask = pw.reducers.sum(pw.this.sensor_bit),
        group_size     = pw.reducers.max(pw.this.group_size),
    )


def _derive_composite_columns(aggregated: pw.Table) -> pw.Table:
    """Derive composite_score, attribution fields, and is_group_anomaly."""
    with_score = aggregated.with_columns(
        composite_score = _udf_composite_score(
            pw.this.sum_sq_zscores,
            pw.this.sensor_count,
        )
    )
    with_flags = with_score.with_columns(
        contributing_sensors = _udf_contributing_sensors(
            pw.this.group_name,
            pw.this.sensor_bitmask,
        ),
        missing_sensors = _udf_missing_sensors(
            pw.this.group_name,
            pw.this.sensor_bitmask,
        ),
        is_group_anomaly = _udf_is_group_anomaly(pw.this.composite_score),
    )
    return with_flags.with_columns(
        top_contributor    = _udf_top_contributor(
            pw.this.group_name, pw.this.time_bucket
        ),
        attribution_detail = _udf_attribution_detail(
            pw.this.group_name, pw.this.time_bucket
        ),
        alert_message      = _udf_alert_message(
            pw.this.group_name, pw.this.time_bucket
        ),
    )


def _project_output(enriched: pw.Table) -> pw.Table:
    """Project to the declared group_anomalies output schema."""
    return enriched.select(
        pw.this.group_name,
        pw.this.timestamp,
        pw.this.composite_score,
        pw.this.contributing_sensors,
        pw.this.missing_sensors,
        pw.this.is_group_anomaly,
        pw.this.top_contributor,
        pw.this.attribution_detail,
        pw.this.alert_message,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_group_anomalies(scored_stream: pw.Table) -> pw.Table:
    """Build the multivariate group anomaly stream from a per-sensor scored stream.

    Orchestrates group annotation, time-bucket alignment, RMS computation, and
    threshold gating. Returns the group_anomalies table; performs no I/O.

    Args:
        scored_stream: Pathway Table with at minimum:
                       sensor_id (str), timestamp (str), z_score (float).

    Returns:
        group_anomalies — Pathway Table with columns:
            group_name, timestamp, composite_score,
            contributing_sensors, missing_sensors, is_group_anomaly,
            top_contributor, attribution_detail, alert_message.
    """
    logger.debug(
        "Multivariate: building group anomaly stream "
        "(groups=%d, threshold=%.2f, sync_ms=%d)",
        len(CONFIG["SENSOR_GROUPS"]),
        CONFIG["GROUP_THRESHOLD"],
        CONFIG["SYNC_TOLERANCE_MS"],
    )

    membership_stream = _build_membership_stream(scored_stream)
    if membership_stream is None:
        return scored_stream.select(
            pw.this.sensor_id.rename("group_name"),
            pw.this.timestamp,
            composite_score      = pw.this.z_score,
            contributing_sensors = pw.this.sensor_id,
            missing_sensors      = pw.this.sensor_id,
            is_group_anomaly     = pw.this.is_anomaly,
        )

    aggregated            = _aggregate_by_bucket(membership_stream)
    enriched              = _derive_composite_columns(aggregated)
    group_anomalies: pw.Table = _project_output(enriched)

    logger.debug("Multivariate: graph construction complete")
    return group_anomalies

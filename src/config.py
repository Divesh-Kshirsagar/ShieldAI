"""
SHIELD AI — Centralized Configuration
======================================

Single source of truth for every tunable constant in the pipeline.
All modules import exclusively from here; no magic numbers appear elsewhere.

Environment overrides
---------------------
Every field reads from an env variable of the same name (loaded via os.getenv).
Set them in .env or export them in the shell before starting the pipeline.

Inspecting the current config
------------------------------
    python -m src.config
"""

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _Config:
    """Immutable runtime configuration for the ShieldAI pipeline."""

    # ------------------------------------------------------------------
    # Z-score anomaly scoring (zscore.py)
    # ------------------------------------------------------------------

    window_seconds: int = field(
        default_factory=lambda: int(os.getenv("WINDOW_SECONDS", "300"))
    )
    # Trailing window width for per-sensor rolling mean/std.
    # Valid range: >= 10 (shorter windows give unstable stats).
    # NOTE: superseded by window_duration_ms for windowed_stats.py;
    # kept for backward compatibility with alert_cooldown and test fixtures.

    window_duration_ms: int = field(
        default_factory=lambda: int(os.getenv("WINDOW_DURATION_MS", "30000"))
    )
    # Sliding-window duration in milliseconds for windowed_stats.py.
    # Controls the length of each sliding window (how much history is visible).
    # Valid range: > window_hop_ms.
    # Default: 30 000 ms (30 seconds) — tuned for 1-minute factory CSV cadence.

    window_hop_ms: int = field(
        default_factory=lambda: int(os.getenv("WINDOW_HOP_MS", "5000"))
    )
    # Sliding-window hop in milliseconds for windowed_stats.py.
    # Controls how frequently a new window is emitted.
    # Valid range: >= 1 and < window_duration_ms.
    # Default: 5 000 ms (5 seconds).

    zscore_threshold: float = field(
        default_factory=lambda: float(os.getenv("ZSCORE_THRESHOLD", "3.0"))
    )
    # |z-score| above which a reading is flagged as anomalous.
    # Valid range: > 0.0 (industry convention: 2.5–4.0).

    epsilon: float = field(
        default_factory=lambda: float(os.getenv("EPSILON", "1e-9"))
    )
    # Denominator floor added to rolling_std to prevent zero-division.
    # Valid range: > 0.0 and < 1e-6 (must be negligibly small).

    # ------------------------------------------------------------------
    # Persistence filter (persistence.py)
    # ------------------------------------------------------------------

    persistence_count: int = field(
        default_factory=lambda: int(os.getenv("PERSISTENCE_COUNT", "3"))
    )
    # Consecutive anomalous readings required before emitting a confirmed alert.
    # Valid range: >= 1.

    # ------------------------------------------------------------------
    # Alert gating
    # ------------------------------------------------------------------

    alert_cooldown_seconds: int = field(
        default_factory=lambda: int(os.getenv("ALERT_COOLDOWN_SECONDS", "60"))
    )
    # Minimum gap between successive alerts for the same sensor.
    # Prevents alert floods on sustained anomalies.
    # Valid range: >= 0 (0 = no cooldown).

    alert_min_risk_band: str = field(
        default_factory=lambda: os.getenv("ALERT_MIN_RISK_BAND", "MEDIUM").upper()
    )
    # Minimum ERI risk band required to emit an alert.
    # Alerts below this band are silently suppressed.
    # Valid values: LOW, MEDIUM, HIGH, CRITICAL.

    metrics_log_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("METRICS_LOG_INTERVAL_SECONDS", "30"))
    )
    # How often (seconds) a latency summary line is printed by MetricsReporter.
    # Valid range: >= 1.

    metrics_emit_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("METRICS_EMIT_INTERVAL_SECONDS", "10"))
    )
    # How often (seconds) the pipeline_metrics table is re-computed and emitted.
    # Default: 10 seconds.
    # Valid range: >= 1.

    metrics_output_path: str = field(
        default_factory=lambda: os.getenv("METRICS_OUTPUT_PATH", "data/alerts/pipeline_metrics.json")
    )
    # Path to the JSON file where real-time KPIs are written.
    # Must be a valid file path; directory will be created if missing.



    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper()
    )
    # Python logging level for the entire pipeline.
    # Valid values: DEBUG, INFO, WARNING, ERROR, CRITICAL.

    # ------------------------------------------------------------------
    # Input schema / time format
    # ------------------------------------------------------------------

    input_time_format: str = field(
        default_factory=lambda: os.getenv("INPUT_TIME_FORMAT", "%Y-%m-%d %H:%M")
    )
    # strptime/strftime format for the `time` column in all CSV sources.
    # Must match the format written by simulate_factories.py.

    input_schema_sensor_column: str = field(
        default_factory=lambda: os.getenv("INPUT_SCHEMA_SENSOR_COLUMN", "factory_id")
    )
    # Column name in factory CSVs that identifies the sensor / factory channel.
    # Valid values: any non-empty string matching CSV headers.

    input_schema_value_column: str = field(
        default_factory=lambda: os.getenv("INPUT_SCHEMA_VALUE_COLUMN", "cod")
    )
    # Column name carrying the primary measurement value used for z-scoring.
    # Valid values: cod, bod, ph, tss (must exist in factory CSV headers).

    max_sensor_id_length: int = field(
        default_factory=lambda: int(os.getenv("MAX_SENSOR_ID_LENGTH", "64"))
    )
    # Maximum allowed length for a sensor_id string.
    # Valid range: >= 1.

    sensor_value_range: dict = field(
        default_factory=lambda: json.loads(
            os.getenv(
                "SENSOR_VALUE_RANGE",
                '{"*ph*": [0.0, 14.0], "*turbidity*": [0.0, 1000.0], "*flow*": [0.0, 10000.0], "*": [-1.0e9, 1.0e9]}'
            )
        )
    )
    # Maps sensor_id glob patterns to [min, max] allowed values.
    # Patterns are matched using fnmatch.
    # Valid values: dict; each value a [min, max] list where min < max.


    # ------------------------------------------------------------------
    # Pathway I/O paths
    # ------------------------------------------------------------------

    cetp_data_directory: str = field(
        default_factory=lambda: os.getenv("CETP_DATA_DIR", "data/cetp")
    )
    # Directory containing cetp_clean.csv (output of simulate_factories.py).

    factory_data_directory: str = field(
        default_factory=lambda: os.getenv("FACTORY_DATA_DIR", "data/factories")
    )
    # Directory containing factory_A/B/C/D.csv files.

    alert_log_path: str = field(
        default_factory=lambda: os.getenv("ALERT_LOG_PATH", "data/alerts/evidence_log.jsonl")
    )
    # Append-only JSONL file for Phase 1 alert evidence records.

    tamper_log_path: str = field(
        default_factory=lambda: os.getenv("TAMPER_LOG_PATH", "data/alerts/tamper_log.jsonl")
    )
    # Append-only JSONL file written by the anti-cheat engine.

    # ------------------------------------------------------------------
    # Static threshold tripwire (tripwire.py — legacy Phase 1)
    # ------------------------------------------------------------------

    cod_baseline: float = field(
        default_factory=lambda: float(os.getenv("COD_BASELINE", "193.0"))
    )
    # Empirical CETP inlet COD mean (mg/L) from priya_cetp_i.csv (Feb 2026).
    # Used to compute breach_mag and classify HIGH vs MEDIUM alerts.
    # Valid range: > 0.0.

    cod_threshold: float = field(
        default_factory=lambda: float(os.getenv("COD_THRESHOLD", "200.0"))
    )
    # COD (mg/L) above which a CETP reading triggers a shock event.
    # Demo value: 200 mg/L. Production: raise to 450+ per regulatory limits.
    # Valid range: > cod_baseline.

    # ------------------------------------------------------------------
    # Temporal backtracking (backtrack.py)
    # ------------------------------------------------------------------

    pipe_travel_minutes: int = field(
        default_factory=lambda: int(os.getenv("PIPE_TRAVEL_MINUTES", "15"))
    )
    # Fixed pipe travel time used to backtrack CETP events to factory discharge.
    # v1 constant; v2 will derive this dynamically from GIS + flow-rate sensors.
    # Valid range: >= 1.

    asof_tolerance_seconds: int = field(
        default_factory=lambda: int(os.getenv("ASOF_TOLERANCE_SECONDS", "120"))
    )
    # Half-width of the temporal search window for asof_join attribution (±seconds).
    # Valid range: >= 1.

    # ------------------------------------------------------------------
    # Anti-cheat engine (anti_cheat.py)
    # ------------------------------------------------------------------

    zero_variance_minutes: int = field(
        default_factory=lambda: int(os.getenv("ZERO_VARIANCE_MINUTES", "5"))
    )
    # Tumbling window width for zero-variance (frozen sensor) detection.
    # Valid range: >= 1.

    cod_drop_fraction: float = field(
        default_factory=lambda: float(os.getenv("COD_DROP_FRACTION", "0.80"))
    )
    # COD must drop by at least this fraction vs the prior window to flag dilution.
    # Valid range: 0.0 < value < 1.0.

    tss_stable_fraction: float = field(
        default_factory=lambda: float(os.getenv("TSS_STABLE_FRACTION", "0.20"))
    )
    # TSS must stay within (1 - tss_stable_fraction) of the prior window mean.
    # Valid range: 0.0 < value < 1.0.

    blackout_min_minutes: int = field(
        default_factory=lambda: int(os.getenv("BLACKOUT_MIN_MINUTES", "10"))
    )
    # Minimum window length for guilt-by-disconnection blackout detection.
    # Valid range: >= 1.

    # ------------------------------------------------------------------
    # Integrations
    # ------------------------------------------------------------------

    shield_webhook_url: str = field(
        default_factory=lambda: os.getenv("SHIELD_WEBHOOK_URL", "")
    )
    # HTTP(S) URL for alert webhook POST. Leave empty to disable.
    # Valid values: empty string (disabled) or a valid http/https URL.

    # ------------------------------------------------------------------
    # Environmental Risk Index — ERI (eri.py)
    # ------------------------------------------------------------------

    river_sensitivity: dict = field(
        default_factory=lambda: json.loads(
            os.getenv(
                "RIVER_SENSITIVITY",
                '{"discharge_point_A": 3.5, "discharge_point_B": 1.2}',
            )
        )
    )
    # Maps discharge_point_id → sensitivity_factor (float).
    # Higher values indicate ecologically sensitive stretches of river.
    # Valid values: each factor in [1.0, 5.0].
    # Override via env: RIVER_SENSITIVITY='{"point_A": 3.5}' (JSON string).

    default_sensitivity: float = field(
        default_factory=lambda: float(os.getenv("DEFAULT_SENSITIVITY", "2.0"))
    )
    # Sensitivity factor applied when a discharge_point_id is absent from
    # river_sensitivity. Sets unknown_sensitivity=True on those rows.
    # Valid range: 1.0 – 5.0.

    severity_multiplier: float = field(
        default_factory=lambda: float(os.getenv("SEVERITY_MULTIPLIER", "1.0"))
    )
    # Global scaling factor applied to every ERI computation.
    # ERI = composite_score * sensitivity_factor * severity_multiplier.
    # Valid range: > 0.0.

    eri_threshold_low: float = field(
        default_factory=lambda: float(os.getenv("ERI_THRESHOLD_LOW", "2.0"))
    )
    # ERI below this value → risk_band = LOW.
    # Valid range: > 0.0 and < eri_threshold_medium.

    eri_threshold_medium: float = field(
        default_factory=lambda: float(os.getenv("ERI_THRESHOLD_MEDIUM", "5.0"))
    )
    # ERI in [eri_threshold_low, eri_threshold_medium) → MEDIUM.
    # Valid range: > eri_threshold_low and < eri_threshold_high.

    eri_threshold_high: float = field(
        default_factory=lambda: float(os.getenv("ERI_THRESHOLD_HIGH", "10.0"))
    )
    # ERI in [eri_threshold_medium, eri_threshold_high) → HIGH.
    # ERI ≥ eri_threshold_high → CRITICAL.
    # Valid range: > eri_threshold_medium.

    # ------------------------------------------------------------------
    # Multivariate anomaly scoring (multivariate.py)
    # ------------------------------------------------------------------

    sensor_groups: dict = field(
        default_factory=lambda: json.loads(
            os.getenv(
                "SENSOR_GROUPS",
                '{"discharge_point_A": ["FACTORY_A", "FACTORY_B", "FACTORY_C", "FACTORY_D"]}',
            )
        )
    )
    # Maps group names to ordered lists of sensor_ids that form the group.
    # Each sensor_id must match values in the input stream’s sensor_id column.
    # Override via env: SENSOR_GROUPS='{"group": ["s1","s2"]}' (JSON string).
    # Valid values: non-empty dict; each value a non-empty list of strings.

    group_threshold: float = field(
        default_factory=lambda: float(os.getenv("GROUP_THRESHOLD", "2.5"))
    )
    # RMS z-score above which a sensor group reading is flagged as a group anomaly.
    # Valid range: > 0.0.

    sync_tolerance_ms: int = field(
        default_factory=lambda: int(os.getenv("SYNC_TOLERANCE_MS", "5000"))
    )
    # Width of the timestamp-alignment bucket used to synchronise z-scores
    # from different sensors within the same group before computing RMS.
    # Valid range: >= 1 (milliseconds).


# ---------------------------------------------------------------------------
# Module-level singleton — the one true CONFIG object
# ---------------------------------------------------------------------------

CONFIG: _Config = _Config()


# ---------------------------------------------------------------------------
# Pipeline Start Time
# ---------------------------------------------------------------------------

# Captured once at module initialization. Used for uptime calculation.
PIPELINE_START_TIME: float = time.time()



# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_VALID_LOG_LEVELS: frozenset[str] = frozenset(
    {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
)


def validate_config(cfg: _Config = CONFIG) -> None:
    """Raise ValueError if any CONFIG field violates its documented constraint.

    Args:
        cfg: Config instance to validate (defaults to the module singleton CONFIG).

    Raises:
        ValueError: Describing the first constraint that is violated.
    """
    # --- z-score scorer ---
    if cfg.window_seconds < 10:
        raise ValueError(
            f"window_seconds must be >= 10 (got {cfg.window_seconds}). "
            "Shorter windows produce statistically unreliable z-scores."
        )
    if cfg.window_hop_ms < 1:
        raise ValueError(
            f"window_hop_ms must be >= 1 ms (got {cfg.window_hop_ms})."
        )
    if cfg.window_duration_ms <= cfg.window_hop_ms:
        raise ValueError(
            f"window_duration_ms ({cfg.window_duration_ms}) must be strictly "
            f"greater than window_hop_ms ({cfg.window_hop_ms})."
        )
    if cfg.zscore_threshold <= 0.0:
        raise ValueError(
            f"zscore_threshold must be > 0.0 (got {cfg.zscore_threshold})."
        )
    if not (0.0 < cfg.epsilon < 1e-6):
        raise ValueError(
            f"epsilon must be in (0.0, 1e-6) (got {cfg.epsilon}). "
            "It is a numerical floor for stddev — it must be negligibly small."
        )

    # --- persistence filter ---
    if cfg.persistence_count < 1:
        raise ValueError(
            f"persistence_count must be >= 1 (got {cfg.persistence_count})."
        )

    # --- alert gating ---
    if cfg.alert_cooldown_seconds < 0:
        raise ValueError(
            f"alert_cooldown_seconds must be >= 0 (got {cfg.alert_cooldown_seconds})."
        )
    _VALID_RISK_BANDS: frozenset[str] = frozenset({"LOW", "MEDIUM", "HIGH", "CRITICAL"})
    if cfg.alert_min_risk_band not in _VALID_RISK_BANDS:
        raise ValueError(
            f"alert_min_risk_band must be one of {sorted(_VALID_RISK_BANDS)} "
            f"(got {cfg.alert_min_risk_band!r})."
        )
    if cfg.metrics_log_interval_seconds < 1:
        raise ValueError(
            f"metrics_log_interval_seconds must be >= 1 (got {cfg.metrics_log_interval_seconds})."
        )
    if cfg.metrics_emit_interval_seconds < 1:
        raise ValueError(
            f"metrics_emit_interval_seconds must be >= 1 (got {cfg.metrics_emit_interval_seconds})."
        )


    # --- logging ---
    if cfg.log_level not in _VALID_LOG_LEVELS:
        raise ValueError(
            f"log_level must be one of {sorted(_VALID_LOG_LEVELS)} "
            f"(got {cfg.log_level!r})."
        )

    # --- input schema ---
    if not cfg.input_time_format.strip():
        raise ValueError("input_time_format must not be empty.")
    if not cfg.input_schema_sensor_column.strip():
        raise ValueError("input_schema_sensor_column must not be empty.")
    if not cfg.input_schema_value_column.strip():
        raise ValueError("input_schema_value_column must not be empty.")

    if cfg.max_sensor_id_length < 1:
        raise ValueError(
            f"max_sensor_id_length must be >= 1 (got {cfg.max_sensor_id_length})."
        )

    if not isinstance(cfg.sensor_value_range, dict):
        raise ValueError("sensor_value_range must be a dictionary.")
    for pattern, bounds in cfg.sensor_value_range.items():
        if not (isinstance(bounds, list) and len(bounds) == 2):
            raise ValueError(
                f"sensor_value_range[{pattern!r}] must be a list of [min, max]."
            )
        if not (isinstance(bounds[0], (int, float)) and isinstance(bounds[1], (int, float))):
            raise ValueError(
                f"sensor_value_range[{pattern!r}] bounds must be numeric."
            )
        if bounds[0] >= bounds[1]:
            raise ValueError(
                f"sensor_value_range[{pattern!r}] min ({bounds[0]}) must be "
                f"strictly less than max ({bounds[1]})."
            )


    # --- tripwire thresholds ---
    if cfg.cod_baseline <= 0.0:
        raise ValueError(
            f"cod_baseline must be > 0.0 mg/L (got {cfg.cod_baseline})."
        )
    if cfg.cod_threshold <= cfg.cod_baseline:
        raise ValueError(
            f"cod_threshold ({cfg.cod_threshold}) must be > cod_baseline "
            f"({cfg.cod_baseline}). Threshold must exceed the baseline mean."
        )

    # --- backtracking ---
    if cfg.pipe_travel_minutes < 1:
        raise ValueError(
            f"pipe_travel_minutes must be >= 1 (got {cfg.pipe_travel_minutes})."
        )
    if cfg.asof_tolerance_seconds < 1:
        raise ValueError(
            f"asof_tolerance_seconds must be >= 1 (got {cfg.asof_tolerance_seconds})."
        )

    # --- anti-cheat ---
    if cfg.zero_variance_minutes < 1:
        raise ValueError(
            f"zero_variance_minutes must be >= 1 (got {cfg.zero_variance_minutes})."
        )
    if not (0.0 < cfg.cod_drop_fraction < 1.0):
        raise ValueError(
            f"cod_drop_fraction must be in (0.0, 1.0) (got {cfg.cod_drop_fraction})."
        )
    if not (0.0 < cfg.tss_stable_fraction < 1.0):
        raise ValueError(
            f"tss_stable_fraction must be in (0.0, 1.0) (got {cfg.tss_stable_fraction})."
        )
    if cfg.blackout_min_minutes < 1:
        raise ValueError(
            f"blackout_min_minutes must be >= 1 (got {cfg.blackout_min_minutes})."
        )

    # --- multivariate scoring ---
    if not cfg.sensor_groups:
        raise ValueError("sensor_groups must not be empty.")
    for gname, members in cfg.sensor_groups.items():
        if not members:
            raise ValueError(
                f"sensor_groups[{gname!r}] must contain at least one sensor_id."
            )
    if cfg.group_threshold <= 0.0:
        raise ValueError(
            f"group_threshold must be > 0.0 (got {cfg.group_threshold})."
        )
    if cfg.sync_tolerance_ms < 1:
        raise ValueError(
            f"sync_tolerance_ms must be >= 1 ms (got {cfg.sync_tolerance_ms})."
        )

    # --- ERI ---
    for pt, factor in cfg.river_sensitivity.items():
        if not (1.0 <= factor <= 5.0):
            raise ValueError(
                f"river_sensitivity[{pt!r}] must be in [1.0, 5.0] (got {factor})."
            )
    if not (1.0 <= cfg.default_sensitivity <= 5.0):
        raise ValueError(
            f"default_sensitivity must be in [1.0, 5.0] (got {cfg.default_sensitivity})."
        )
    if cfg.severity_multiplier <= 0.0:
        raise ValueError(
            f"severity_multiplier must be > 0.0 (got {cfg.severity_multiplier})."
        )
    if not (0.0 < cfg.eri_threshold_low < cfg.eri_threshold_medium < cfg.eri_threshold_high):
        raise ValueError(
            f"ERI thresholds must satisfy 0 < low ({cfg.eri_threshold_low}) "
            f"< medium ({cfg.eri_threshold_medium}) "
            f"< high ({cfg.eri_threshold_high})."
        )


# ---------------------------------------------------------------------------
# CLI inspection
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    validate_config(CONFIG)
    print(json.dumps(asdict(CONFIG), indent=2))

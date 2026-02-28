"""
SHIELD AI — Environmental Risk Index (ERI)
==========================================

Pure transformation module: computes a per-discharge-point Environmental Risk
Index by scaling the composite anomaly score from multivariate.py against a
static river sensitivity lookup defined in CONFIG.

ERI Formula
-----------
    eri = composite_score * sensitivity_factor * SEVERITY_MULTIPLIER

    sensitivity_factor comes from CONFIG.river_sensitivity[discharge_point_id].
    If discharge_point_id is absent, DEFAULT_SENSITIVITY is used and
    unknown_sensitivity is set to True on that row.

Risk bands (all thresholds configurable)
-----------------------------------------
    ERI < LOW_THRESHOLD              → "LOW"
    LOW_THRESHOLD  ≤ ERI < MED_THRESHOLD  → "MEDIUM"
    MED_THRESHOLD  ≤ ERI < HIGH_THRESHOLD → "HIGH"
    ERI ≥ HIGH_THRESHOLD             → "CRITICAL"

Inputs
------
    group_anomalies: Pathway Table from multivariate.build_group_anomalies(),
                     with columns: group_name (str), timestamp (str),
                     composite_score (float).

    The group_name column is treated as the discharge_point_id for the
    river sensitivity lookup — the two share the same identifier namespace.

Outputs
-------
    eri_stream: Pathway Table with columns:
        discharge_point_id (str)   — group_name / discharge point identifier
        timestamp          (str)   — reading timestamp
        composite_score    (float) — raw RMS z-score from multivariate stage
        sensitivity_factor (float) — factor from RIVER_SENSITIVITY or default
        eri                (float) — computed Environmental Risk Index
        risk_band          (str)   — LOW / MEDIUM / HIGH / CRITICAL
        unknown_sensitivity (bool) — True when default sensitivity was applied

Assumptions
-----------
- group_anomalies table has been produced by multivariate.build_group_anomalies().
- No I/O, no sinks, no pw.run() — pure Pathway graph construction.
- All parameters come from config.CONFIG.
"""

from __future__ import annotations

import logging

import pathway as pw

import src.config as _config_mod

log: logging.Logger = logging.getLogger(__name__)

CONFIG: dict = {
    "RIVER_SENSITIVITY":   _config_mod.CONFIG.river_sensitivity,   # point_id → factor
    "DEFAULT_SENSITIVITY": _config_mod.CONFIG.default_sensitivity, # fallback factor
    "SEVERITY_MULTIPLIER": _config_mod.CONFIG.severity_multiplier, # global scaler
    # Sorted threshold list: (upper_bound_exclusive, band_name)
    "ERI_THRESHOLDS": [
        (_config_mod.CONFIG.eri_threshold_low,    "LOW"),
        (_config_mod.CONFIG.eri_threshold_medium, "MEDIUM"),
        (_config_mod.CONFIG.eri_threshold_high,   "HIGH"),
    ],
}


# ---------------------------------------------------------------------------
# Pure-Python helpers (fully testable without Pathway runtime)
# ---------------------------------------------------------------------------

def classify_eri(eri_value: float) -> str:
    """Assign a risk band to an ERI value using a sorted threshold list.

    Bands: LOW < MEDIUM < HIGH; anything above the highest threshold is CRITICAL.
    All thresholds are drawn from CONFIG['ERI_THRESHOLDS'].
    """
    for upper_bound, band in CONFIG["ERI_THRESHOLDS"]:
        if eri_value < upper_bound:
            return band
    return "CRITICAL"


def _lookup_sensitivity(discharge_point_id: str) -> tuple[float, bool]:
    """Return (sensitivity_factor, unknown_sensitivity) for a discharge point.

    Looks up discharge_point_id in CONFIG['RIVER_SENSITIVITY']. Falls back to
    CONFIG['DEFAULT_SENSITIVITY'] and sets unknown_sensitivity=True when absent.
    """
    table = CONFIG["RIVER_SENSITIVITY"]
    if discharge_point_id in table:
        return table[discharge_point_id], False
    log.warning(
        "unknown_sensitivity",
        extra={
            "discharge_point_id": discharge_point_id,
            "default_sensitivity": CONFIG["DEFAULT_SENSITIVITY"],
        },
    )
    return CONFIG["DEFAULT_SENSITIVITY"], True


def _compute_eri(composite_score: float, sensitivity_factor: float) -> float:
    """Apply ERI formula: composite_score * sensitivity_factor * SEVERITY_MULTIPLIER."""
    return composite_score * sensitivity_factor * CONFIG["SEVERITY_MULTIPLIER"]


# ---------------------------------------------------------------------------
# Pathway UDFs
# ---------------------------------------------------------------------------

@pw.udf
def _udf_sensitivity_factor(discharge_point_id: str) -> float:
    """Return the sensitivity factor for the given discharge point."""
    factor, _ = _lookup_sensitivity(discharge_point_id)
    return factor


@pw.udf
def _udf_unknown_sensitivity(discharge_point_id: str) -> bool:
    """Return True when the default sensitivity was applied (point not in table)."""
    _, unknown = _lookup_sensitivity(discharge_point_id)
    return unknown


@pw.udf
def _udf_eri(composite_score: float, sensitivity_factor: float) -> float:
    """Compute the Environmental Risk Index from score and sensitivity."""
    return _compute_eri(composite_score, sensitivity_factor)


@pw.udf
def _udf_risk_band(eri_value: float) -> str:
    """Classify an ERI value into a risk band string."""
    return classify_eri(eri_value)


# ---------------------------------------------------------------------------
# Pathway graph builders
# ---------------------------------------------------------------------------

def _rename_group_to_point(group_anomalies: pw.Table) -> pw.Table:
    """Rename group_name to discharge_point_id for the ERI output schema."""
    return group_anomalies.with_columns(
        discharge_point_id=pw.this.group_name,
    )


def _attach_sensitivity(stream: pw.Table) -> pw.Table:
    """Add sensitivity_factor and unknown_sensitivity columns."""
    return stream.with_columns(
        sensitivity_factor  = _udf_sensitivity_factor(pw.this.discharge_point_id),
        unknown_sensitivity = _udf_unknown_sensitivity(pw.this.discharge_point_id),
    )


def _attach_eri(stream: pw.Table) -> pw.Table:
    """Add eri and risk_band columns."""
    with_eri = stream.with_columns(
        eri=_udf_eri(pw.this.composite_score, pw.this.sensitivity_factor),
    )
    return with_eri.with_columns(
        risk_band=_udf_risk_band(pw.this.eri),
    )


def _project_eri_output(stream: pw.Table) -> pw.Table:
    """Project to the declared eri_stream output schema."""
    return stream.select(
        pw.this.discharge_point_id,
        pw.this.timestamp,
        pw.this.composite_score,
        pw.this.sensitivity_factor,
        pw.this.eri,
        pw.this.risk_band,
        pw.this.unknown_sensitivity,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_eri_stream(group_anomalies: pw.Table) -> pw.Table:
    """Compute the Environmental Risk Index stream from the group anomaly table.

    Enriches each group-anomaly row with river sensitivity, ERI value, and risk
    band. Performs no I/O and does not call pw.run().

    Args:
        group_anomalies: Pathway Table with at minimum:
                         group_name (str), timestamp (str), composite_score (float).

    Returns:
        eri_stream — Pathway Table with columns:
            discharge_point_id, timestamp, composite_score, sensitivity_factor,
            eri, risk_band, unknown_sensitivity.
    """
    log.info(
        "config loaded",
        extra={
            "sensitivity_points": len(CONFIG["RIVER_SENSITIVITY"]),
            "severity_multiplier": CONFIG["SEVERITY_MULTIPLIER"],
            "thresholds": [(t, b) for t, b in CONFIG["ERI_THRESHOLDS"]],
        },
    )

    stream = _rename_group_to_point(group_anomalies)
    stream = _attach_sensitivity(stream)
    stream = _attach_eri(stream)
    eri_stream: pw.Table = _project_eri_output(stream)

    logger.debug("ERI: graph construction complete")
    return eri_stream

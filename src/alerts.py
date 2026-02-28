"""
SHIELD AI — Risk-Gated Alert Router
=====================================

Filters and formats alerts from the ERI stream based on configurable risk-band
thresholds and per-discharge-point cooldown windows.

Routing logic
-------------
1. Band filter     — drop rows whose risk_band rank < ALERT_MIN_RISK_BAND rank.
2. Cooldown gate   — drop rows for a discharge_point_id that emitted an alert
                     within the last ALERT_COOLDOWN_SECONDS.
3. Field masking   — HIGH/CRITICAL rows carry the full field set; MEDIUM rows
                     carry the minimal set (eri, risk_band, discharge_point_id,
                     timestamp); extra fields are empty for MEDIUM.
4. alert_level     — MEDIUM → "INFO", HIGH → "WARNING", CRITICAL → "CRITICAL".

Risk band ordering
------------------
    RISK_BAND_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

    All comparisons use this dict — no string comparison against band names.

Inputs
------
    eri_stream: Pathway Table from eri.build_eri_stream() with columns:
        discharge_point_id (str), timestamp (str), composite_score (float),
        sensitivity_factor (float), eri (float), risk_band (str),
        unknown_sensitivity (bool), top_contributor (str),
        attribution_detail (str), alert_message (str).

    This module must NOT import from eri.py — eri_stream is passed as a
    parameter.

Outputs
-------
    active_alerts: Pathway Table with columns:
        discharge_point_id (str)   — discharge point identifier
        timestamp          (str)   — reading timestamp
        eri                (float) — Environmental Risk Index value
        risk_band          (str)   — LOW / MEDIUM / HIGH / CRITICAL
        alert_level        (str)   — INFO / WARNING / CRITICAL
        sensitivity_factor (float) — always present (0.0 for MEDIUM rows)
        top_contributor    (str)   — sensor driving anomaly (empty for MEDIUM)
        alert_message      (str)   — human-readable summary (empty for MEDIUM)

Assumptions
-----------
- eri_stream has been produced by eri.build_eri_stream() or a table of the
  same schema.
- No I/O, no sinks, no pw.run() — pure Pathway graph construction.
- All parameters come from config.CONFIG.
"""

from __future__ import annotations

import datetime
import logging

import pathway as pw

import src.config as _config_mod

logger: logging.Logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Band ordering — all comparisons go through this dict
# ---------------------------------------------------------------------------

RISK_BAND_ORDER: dict[str, int] = {
    "LOW":      0,
    "MEDIUM":   1,
    "HIGH":     2,
    "CRITICAL": 3,
}

# alert_level mapping for bands that pass the filter
_ALERT_LEVEL: dict[str, str] = {
    "MEDIUM":   "INFO",
    "HIGH":     "WARNING",
    "CRITICAL": "CRITICAL",
}

CONFIG: dict = {
    "RISK_BAND_ORDER":        RISK_BAND_ORDER,
    "ALERT_MIN_RISK_BAND":    _config_mod.CONFIG.alert_min_risk_band,
    "ALERT_COOLDOWN_SECONDS": _config_mod.CONFIG.alert_cooldown_seconds,
    "TIME_FORMAT":            _config_mod.CONFIG.input_time_format,
}


# ---------------------------------------------------------------------------
# Stateful cooldown store (one entry per discharge_point_id)
# ---------------------------------------------------------------------------

class _CooldownStore:
    """Track last-alert timestamps to suppress re-alerts within the cooldown window."""

    def __init__(self) -> None:
        """Initialise with empty store."""
        self._last_alert: dict[str, datetime.datetime] = {}

    def can_alert(self, discharge_point_id: str, timestamp: str) -> bool:
        """Return True when enough time has passed since the last alert for this point."""
        cooldown = CONFIG["ALERT_COOLDOWN_SECONDS"]
        if cooldown == 0:
            return True
        fmt = CONFIG["TIME_FORMAT"]
        try:
            now = datetime.datetime.strptime(timestamp, fmt)
        except ValueError:
            return True   # unparseable timestamp never suppressed
        last = self._last_alert.get(discharge_point_id)
        if last is None:
            return True
        return (now - last).total_seconds() >= cooldown

    def record(self, discharge_point_id: str, timestamp: str) -> None:
        """Record the alert time for the given discharge point."""
        fmt = CONFIG["TIME_FORMAT"]
        try:
            self._last_alert[discharge_point_id] = datetime.datetime.strptime(
                timestamp, fmt
            )
        except ValueError:
            pass   # leave previous entry intact on bad timestamp

    def reset_all(self) -> None:
        """Clear all cooldown state (for testing)."""
        self._last_alert.clear()


_cooldown_store: _CooldownStore = _CooldownStore()


# ---------------------------------------------------------------------------
# Pure-Python helpers
# ---------------------------------------------------------------------------

def _band_passes_threshold(risk_band: str) -> bool:
    """Return True when risk_band rank >= ALERT_MIN_RISK_BAND rank."""
    min_rank = RISK_BAND_ORDER.get(CONFIG["ALERT_MIN_RISK_BAND"], 1)
    row_rank = RISK_BAND_ORDER.get(risk_band, -1)
    return row_rank >= min_rank


def _get_alert_level(risk_band: str) -> str:
    """Return alert_level string for the given risk_band (empty if band unknown)."""
    return _ALERT_LEVEL.get(risk_band, "")


def _is_high_or_critical(risk_band: str) -> bool:
    """Return True when risk_band is HIGH or CRITICAL."""
    return RISK_BAND_ORDER.get(risk_band, 0) >= RISK_BAND_ORDER["HIGH"]


def _mask_str_field(risk_band: str, value: str) -> str:
    """Return value for HIGH/CRITICAL bands; empty string for MEDIUM."""
    return value if _is_high_or_critical(risk_band) else ""


def _mask_float_field(risk_band: str, value: float) -> float:
    """Return value for HIGH/CRITICAL bands; 0.0 for MEDIUM."""
    return value if _is_high_or_critical(risk_band) else 0.0


# ---------------------------------------------------------------------------
# Pathway UDFs
# ---------------------------------------------------------------------------

@pw.udf
def _udf_passes_threshold(risk_band: str) -> bool:
    """Return True when risk_band meets the ALERT_MIN_RISK_BAND requirement."""
    return _band_passes_threshold(risk_band)


@pw.udf
def _udf_not_in_cooldown(discharge_point_id: str, timestamp: str) -> bool:
    """Return True when this discharge point is outside its cooldown window."""
    if not _cooldown_store.can_alert(discharge_point_id, timestamp):
        return False
    _cooldown_store.record(discharge_point_id, timestamp)
    return True


@pw.udf
def _udf_alert_level(risk_band: str) -> str:
    """Map risk_band to alert_level string."""
    return _get_alert_level(risk_band)


@pw.udf
def _udf_mask_str(risk_band: str, value: str) -> str:
    """Return value for HIGH/CRITICAL; empty string for MEDIUM."""
    return _mask_str_field(risk_band, value)


@pw.udf
def _udf_mask_float(risk_band: str, value: float) -> float:
    """Return value for HIGH/CRITICAL; 0.0 for MEDIUM."""
    return _mask_float_field(risk_band, value)


# ---------------------------------------------------------------------------
# Pathway graph builders
# ---------------------------------------------------------------------------

def _filter_by_band(eri_stream: pw.Table) -> pw.Table:
    """Drop rows whose risk_band falls below ALERT_MIN_RISK_BAND."""
    return eri_stream.filter(
        _udf_passes_threshold(pw.this.risk_band)
    )


def _filter_by_cooldown(stream: pw.Table) -> pw.Table:
    """Drop rows for discharge points still within the cooldown window."""
    return stream.filter(
        _udf_not_in_cooldown(pw.this.discharge_point_id, pw.this.timestamp)
    )


def _attach_alert_level(stream: pw.Table) -> pw.Table:
    """Add alert_level column derived from risk_band."""
    return stream.with_columns(
        alert_level=_udf_alert_level(pw.this.risk_band),
    )


def _apply_field_masking(stream: pw.Table) -> pw.Table:
    """Blank out HIGH/CRITICAL-only fields on MEDIUM rows."""
    return stream.with_columns(
        top_contributor    = _udf_mask_str(pw.this.risk_band, pw.this.top_contributor),
        alert_message      = _udf_mask_str(pw.this.risk_band, pw.this.alert_message),
        sensitivity_factor = _udf_mask_float(pw.this.risk_band, pw.this.sensitivity_factor),
    )


def _project_alert_output(stream: pw.Table) -> pw.Table:
    """Project to the active_alerts output schema."""
    return stream.select(
        pw.this.discharge_point_id,
        pw.this.timestamp,
        pw.this.eri,
        pw.this.risk_band,
        pw.this.alert_level,
        pw.this.sensitivity_factor,
        pw.this.top_contributor,
        pw.this.alert_message,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_alert_stream(eri_stream: pw.Table) -> pw.Table:
    """Build the active_alerts table by applying band filtering and cooldown gating.

    Accepts an eri_stream (from eri.build_eri_stream or equivalent schema),
    applies risk-band threshold suppression, per-point cooldown suppression,
    alert level assignment, and field masking.  Performs no I/O.

    Args:
        eri_stream: Pathway Table with discharge_point_id, timestamp, eri,
                    risk_band, sensitivity_factor, top_contributor,
                    alert_message columns (at minimum).

    Returns:
        active_alerts — Pathway Table with columns:
            discharge_point_id, timestamp, eri, risk_band, alert_level,
            sensitivity_factor, top_contributor, alert_message.
    """
    logger.debug(
        "Alerts: building stream (min_band=%r, cooldown=%ds)",
        CONFIG["ALERT_MIN_RISK_BAND"],
        CONFIG["ALERT_COOLDOWN_SECONDS"],
    )

    stream = _filter_by_band(eri_stream)
    stream = _filter_by_cooldown(stream)
    stream = _attach_alert_level(stream)
    stream = _apply_field_masking(stream)
    active_alerts: pw.Table = _project_alert_output(stream)

    logger.debug("Alerts: graph construction complete")
    return active_alerts

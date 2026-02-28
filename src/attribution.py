"""
SHIELD AI — Causal Attribution Formatter
==========================================

Pure-Python (no Pathway, no third-party libraries) module that takes an
enriched group-anomaly row and computes sensor-level causal attribution.

Attribution formula
-------------------
For each sensor i that contributed to a group anomaly:
    fraction_i = z_i² / Σ z_j²

Sensors are ranked descending by fraction. The top contributor is the sensor
with the largest fraction. The composite score is fully explained by the
fractions (they sum to 1.0 by construction, excluding missing sensors).

Output
------
format_alert(row) accepts a dict and returns a new dict with three added fields:

    top_contributor   (str)  — sensor_id with the largest fraction
    attribution_detail (str) — JSON: {"sensor_id": fraction_3dp, ...} descending
    alert_message      (str) — human-readable summary

Row contract
------------
Input dict must contain:
    group_name       (str)              — group label from CONFIG.sensor_groups
    sensor_z_scores  (dict[str, float]) — {sensor_id: z_score} for contributing sensors
    composite_score  (float)            — RMS z-score for the group window

All other keys are forwarded unchanged to the output dict.

Usage
-----
    from src.attribution import format_alert

    enriched = format_alert({
        "group_name":      "discharge_point_A",
        "composite_score": 3.14,
        "sensor_z_scores": {"pH": 4.0, "turbidity": -2.5, "flow": 1.0},
        "is_group_anomaly": True,
    })
    # enriched["top_contributor"]    → "pH"
    # enriched["attribution_detail"] → '{"pH": 0.762, "turbidity": 0.298, "flow": 0.048}'
    # enriched["alert_message"]       → "Anomaly in discharge_point_A: ..."
"""

from __future__ import annotations

import json


# ---------------------------------------------------------------------------
# Pure-Python computation helpers
# ---------------------------------------------------------------------------

def _compute_fractions(sensor_z_scores: dict[str, float]) -> dict[str, float]:
    """Return the fraction of total z² each sensor contributes.

    fraction_i = z_i² / Σ z_j².   Returns equal fractions when total is zero.
    """
    z_sq = {sid: z * z for sid, z in sensor_z_scores.items()}
    total = sum(z_sq.values())
    if total == 0.0:
        n = len(z_sq)
        return {sid: (1.0 / n if n > 0 else 0.0) for sid in z_sq}
    return {sid: sq / total for sid, sq in z_sq.items()}


def _sort_descending(fractions: dict[str, float]) -> list[tuple[str, float]]:
    """Return (sensor_id, fraction) pairs sorted by fraction descending."""
    return sorted(fractions.items(), key=lambda pair: pair[1], reverse=True)


def _format_attribution_detail(sorted_pairs: list[tuple[str, float]]) -> str:
    """Serialize attribution fractions as a JSON string, values rounded to 3 dp."""
    return json.dumps({sid: round(frac, 3) for sid, frac in sorted_pairs})


def _format_alert_message(
    group_name: str,
    top_contributor: str,
    top_fraction: float,
) -> str:
    """Return human-readable alert message naming the primary anomaly driver."""
    return (
        f"Anomaly in {group_name}: primary driver {top_contributor} "
        f"({top_fraction:.0%} of score)"
    )


def _top_contributor(sorted_pairs: list[tuple[str, float]]) -> tuple[str, float]:
    """Return (sensor_id, fraction) for the highest-contributing sensor.

    Returns ("", 0.0) when no sensors contributed.
    """
    return sorted_pairs[0] if sorted_pairs else ("", 0.0)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def format_alert(row: dict) -> dict:
    """Enrich a group-anomaly row dict with causal attribution fields.

    Computes each contributing sensor's fractional z² share, ranks them
    descending, and adds top_contributor, attribution_detail, and alert_message
    to the returned dict.  The input dict is not mutated.

    Args:
        row: Dict with at minimum:
                 group_name      (str)
                 sensor_z_scores (dict[str, float]) — {sensor_id: z_score}
                 composite_score (float)
             All other keys are forwarded unchanged.

    Returns:
        New dict with all original keys plus:
            top_contributor   (str)  — sensor_id with highest fraction
            attribution_detail (str) — JSON of {sensor_id: fraction} to 3 dp
            alert_message      (str) — human-readable summary
    """
    sensor_z_scores: dict[str, float] = row.get("sensor_z_scores", {})
    group_name: str = row.get("group_name", "")

    fractions   = _compute_fractions(sensor_z_scores)
    sorted_pairs = _sort_descending(fractions)

    top_sid, top_frac = _top_contributor(sorted_pairs)
    attribution_detail = _format_attribution_detail(sorted_pairs)
    alert_message      = _format_alert_message(group_name, top_sid, top_frac)

    return {
        **row,
        "top_contributor":    top_sid,
        "attribution_detail": attribution_detail,
        "alert_message":      alert_message,
    }

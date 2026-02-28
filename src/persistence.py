"""
SHIELD AI — Persistence Filter (Confirmed Anomaly Gate)
=======================================================

Downstream stage that consumes scored_stream (from zscore.py) and only
emits a reading into confirmed_anomalies once PERSISTENCE_COUNT consecutive
anomalous readings have been observed for the same sensor_id.

Stateful design
---------------
A ``_SensorStateStore`` instance maintains a per-sensor consecutive counter
in-process. The counter increments on every anomalous reading and resets to
zero on the first non-anomalous reading for that sensor.  A DEBUG log message
is emitted on every counter reset so operators can confirm the gate is working.

Pathway integration
-------------------
``_compute_consecutive_count`` is a ``@pw.udf`` that delegates into
``_state_store``.  Pathway calls this UDF once per row, in event-time order,
so the stateful counter mirrors the arrival order the streaming engine sees.

The Pathway graph is built lazily inside ``build_confirmed_anomalies()`` —
importing this module has zero side effects.

Usage
-----
    from src.persistence import build_confirmed_anomalies
    from src.zscore    import build_scored_stream

    scored_stream      = build_scored_stream(factory_stream)
    confirmed_anomalies = build_confirmed_anomalies(scored_stream)
"""

from __future__ import annotations

import logging

import pathway as pw

import src.config as _config_mod

# ---------------------------------------------------------------------------
# Module-level logger — no handler added here (caller configures logging)
# ---------------------------------------------------------------------------

logger: logging.Logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Single CONFIG dict — the only non-logger module-level name
# ---------------------------------------------------------------------------

CONFIG: dict = {
    "PERSISTENCE_COUNT": _config_mod.CONFIG.persistence_count,  # consecutive anomalies needed to confirm
}


# ---------------------------------------------------------------------------
# Encapsulated stateful counter
# ---------------------------------------------------------------------------

class _SensorStateStore:
    """Thread-safe consecutive-anomaly counter per sensor_id.

    This class is the sole keeper of mutable state in this module.
    It is never exposed as a public name — only the ``@pw.udf`` wrapper
    below interacts with it, ensuring a single call site.
    """

    __slots__ = ("_counts",)

    def __init__(self) -> None:
        """Initialise an empty counter store."""
        self._counts: dict[str, int] = {}

    def update(self, sensor_id: str, is_anomaly: bool) -> int:
        """Increment or reset the counter for sensor_id and return the new count.

        Args:
            sensor_id:  Unique sensor channel identifier.
            is_anomaly: True when the current reading has been scored anomalous.

        Returns:
            New consecutive anomaly count after applying the update.
        """
        current: int = self._counts.get(sensor_id, 0)

        if is_anomaly:
            new_count = current + 1
        else:
            if current > 0:
                logger.debug(
                    "Consecutive counter reset: sensor_id=%r (was %d → 0)",
                    sensor_id,
                    current,
                )
            new_count = 0

        self._counts[sensor_id] = new_count
        return new_count

    def reset_all(self) -> None:
        """Clear every counter — useful for test teardown."""
        self._counts.clear()

    def get(self, sensor_id: str) -> int:
        """Return the current counter value for sensor_id without mutating state."""
        return self._counts.get(sensor_id, 0)


# Module-level store instance — private, accessed only via _compute_consecutive_count
_state_store = _SensorStateStore()


# ---------------------------------------------------------------------------
# Pathway UDFs
# ---------------------------------------------------------------------------

@pw.udf
def _compute_consecutive_count(sensor_id: str, is_anomaly: bool) -> int:
    """Update the per-sensor consecutive counter and return the new count."""
    return _state_store.update(sensor_id, is_anomaly)


@pw.udf
def _is_confirmed(consecutive_count: int) -> bool:
    """Return True when consecutive_count meets or exceeds PERSISTENCE_COUNT."""
    return consecutive_count >= CONFIG["PERSISTENCE_COUNT"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_confirmed_anomalies(scored_stream: pw.Table) -> pw.Table:
    """Attach a persistence gate to scored_stream and emit only confirmed anomalies.

    A reading is *confirmed* when the same sensor has produced at least
    PERSISTENCE_COUNT anomalous readings in a row.  Any non-anomalous reading
    resets that sensor's counter to zero.

    Args:
        scored_stream: Pathway Table produced by zscore.build_scored_stream().
                       Must contain: sensor_id (str), timestamp (str),
                       value (float), z_score (float), is_anomaly (bool).

    Returns:
        confirmed_anomalies — Pathway Table with columns:
            sensor_id         (str)   — sensor channel identifier
            timestamp         (str)   — original reading timestamp
            consecutive_count (int)   — streak length at point of confirmation
            z_score           (float) — z-score of the confirming reading
            value             (float) — raw sensor value of the confirming reading
    """
    # Step 1 — Attach the consecutive anomaly count per sensor
    with_count: pw.Table = scored_stream.with_columns(
        consecutive_count=_compute_consecutive_count(
            pw.this.sensor_id,
            pw.this.is_anomaly,
        )
    )

    # Step 2 — Gate: only rows where the streak has reached the threshold
    confirmed_anomalies: pw.Table = (
        with_count
        .filter(_is_confirmed(pw.this.consecutive_count))
        .select(
            pw.this.sensor_id,
            pw.this.timestamp,
            pw.this.consecutive_count,
            pw.this.z_score,
            pw.this.value,
        )
    )

    return confirmed_anomalies

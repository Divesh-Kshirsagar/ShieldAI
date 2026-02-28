"""
SHIELD AI — Pipeline Latency Metrics
======================================

Pure-Python module (no Pathway imports) for rolling latency measurement,
percentile computation, and periodic summary logging.

Three concerns are strictly separated:

  Measurement   — LatencyCollector records latency_ms values in a fixed-size
                  rolling deque and timestamps for rate computation.

  Aggregation   — compute_percentile() is a pure function that derives P50/P99
                  (or any percentile) from a sequence without external libraries.

  Reporting     — MetricsReporter drives the periodic "Latency P50 | P99 | Rate"
                  log line, decoupling the scheduling logic from the math.

Usage (from Pathway pipeline)
------------------------------
    from src.metrics import LatencyCollector, MetricsReporter

    collector = LatencyCollector()
    reporter  = MetricsReporter(collector, interval_seconds=30)

    # in a Pathway UDF or callback:
    collector.record(latency_ms)
    reporter.maybe_report(logger)

Usage (standalone)
------------------
    from src.metrics import compute_percentile, format_latency_summary
    import collections

    data = collections.deque([12.3, 45.1, 8.7, ...], maxlen=1000)
    p50  = compute_percentile(data, 50.0)
    p99  = compute_percentile(data, 99.0)
    line = format_latency_summary(p50, p99, alerts_per_min=4.2)
"""

from __future__ import annotations

import collections
import logging
import time


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROLLING_WINDOW_SIZE: int = 1000  # maximum samples retained in the deque


# ---------------------------------------------------------------------------
# Percentile computation — pure function, no state
# ---------------------------------------------------------------------------

def compute_percentile(data: collections.deque, percentile: float) -> float:
    """Compute the given percentile (0–100) of a sequence using linear interpolation.

    Uses the same interpolation method as numpy.percentile(method='linear'):
        index = percentile / 100 * (n - 1)
        result = data[floor(index)] * (1 - frac) + data[ceil(index)] * frac

    Returns 0.0 for empty sequences.

    Args:
        data:       Any iterable of floats (deque, list, tuple).
        percentile: Target percentile in the range [0, 100].
    """
    if not data:
        return 0.0
    sorted_data = sorted(data)
    n = len(sorted_data)
    if n == 1:
        return sorted_data[0]
    k  = (percentile / 100.0) * (n - 1)
    lo = int(k)
    hi = min(lo + 1, n - 1)
    return sorted_data[lo] * (1.0 - (k - lo)) + sorted_data[hi] * (k - lo)


# ---------------------------------------------------------------------------
# Measurement — LatencyCollector
# ---------------------------------------------------------------------------

class LatencyCollector:
    """Rolling window of latency measurements with percentile and rate accessors.

    Internally maintains two fixed-size deques (maxlen=ROLLING_WINDOW_SIZE):
    - _latencies: latency_ms float per recorded alert
    - _mono_times: monotonic timestamp at the moment of recording (for rate)

    All reads are non-destructive; the deque evicts oldest entries automatically.
    """

    def __init__(self, maxlen: int = ROLLING_WINDOW_SIZE) -> None:
        """Initialise with empty rolling window of the given capacity."""
        self._latencies: collections.deque[float] = collections.deque(maxlen=maxlen)
        self._mono_times: collections.deque[float] = collections.deque(maxlen=maxlen)

    def record(self, latency_ms: float) -> None:
        """Record one latency measurement and its wall-clock arrival time."""
        self._latencies.append(latency_ms)
        self._mono_times.append(time.monotonic())

    def p50(self) -> float:
        """Return the 50th percentile (median) of the rolling window."""
        return compute_percentile(self._latencies, 50.0)

    def p99(self) -> float:
        """Return the 99th percentile of the rolling window."""
        return compute_percentile(self._latencies, 99.0)

    def alerts_per_min(self, rate_window_seconds: float = 60.0) -> float:
        """Return the alert rate per minute over the last rate_window_seconds.

        Counts entries in _mono_times that fall within [now - window, now],
        then normalises to per-minute.  Returns 0.0 when window is zero.
        """
        if rate_window_seconds <= 0:
            return 0.0
        cutoff = time.monotonic() - rate_window_seconds
        recent = sum(1 for t in self._mono_times if t >= cutoff)
        return recent * (60.0 / rate_window_seconds)

    def __len__(self) -> int:
        """Return the number of samples currently in the window."""
        return len(self._latencies)

    def reset(self) -> None:
        """Clear all measurements (for testing)."""
        self._latencies.clear()
        self._mono_times.clear()


# ---------------------------------------------------------------------------
# Reporting — format + periodic emission
# ---------------------------------------------------------------------------

def format_latency_summary(p50: float, p99: float, alerts_per_min: float) -> str:
    """Return the standard latency summary log line (values to 1 decimal place).

    Format: "Latency P50: Xms | P99: Yms | Alerts/min: Z"
    """
    return (
        f"Latency P50: {p50:.1f}ms"
        f" | P99: {p99:.1f}ms"
        f" | Alerts/min: {alerts_per_min:.1f}"
    )


class MetricsReporter:
    """Drive periodic latency summary logging without blocking the pipeline.

    Callers invoke maybe_report() on every alert; the reporter checks whether
    METRICS_LOG_INTERVAL_SECONDS have elapsed since the last emission and, if
    so, logs the summary and resets the timer.  No threads or timers needed.

    Args:
        collector:        LatencyCollector shared with the measurement layer.
        interval_seconds: Minimum seconds between successive log lines.
        logger:           Logger to write to (optional; defaults to module logger).
    """

    def __init__(
        self,
        collector: LatencyCollector,
        interval_seconds: float,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialise reporter; first log line fires after interval_seconds."""
        self._collector        = collector
        self._interval         = interval_seconds
        self._logger           = logger or logging.getLogger(__name__)
        self._last_report_mono = time.monotonic()

    def maybe_report(self) -> bool:
        """Log a latency summary if the interval has elapsed.

        Returns True when a summary was emitted, False otherwise.
        Silently skips when the collector is empty (no data yet).
        """
        if not self._collector:
            return False
        now = time.monotonic()
        if now - self._last_report_mono < self._interval:
            return False
        self._last_report_mono = now
        summary = format_latency_summary(
            self._collector.p50(),
            self._collector.p99(),
            self._collector.alerts_per_min(),
        )
        self._logger.info(summary)
        return True

"""
Tests for src/metrics — pure-Python unit tests.

Covers:
  - compute_percentile: edge cases, known values, interpolation
  - LatencyCollector: record/p50/p99/alerts_per_min/len/reset
  - format_latency_summary: formatting and precision
  - MetricsReporter: interval gating, first-run behaviour
  - _TimelineStore (from instrumentation): record/get/latency_ms
  - make_event_id (from instrumentation): key format

Run with:
    python3 -m pytest tests/test_metrics.py -v
"""

from __future__ import annotations

import collections
import importlib
import sys
import time
import types


# ---------------------------------------------------------------------------
# Stubs (for instrumentation module only)
# ---------------------------------------------------------------------------

def _stub_dotenv() -> None:
    if "dotenv" in sys.modules:
        return
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *a, **kw: None
    sys.modules["dotenv"] = dotenv


def _stub_pathway() -> None:
    if "pathway" in sys.modules:
        return
    pw = types.ModuleType("pathway")
    pw.Schema = object; pw.Table = object; pw.udf = lambda fn: fn
    pw.Duration = lambda **kw: kw; pw.DateTimeNaive = object
    for sub in ("pathway.reducers", "pathway.temporal"):
        sys.modules[sub] = types.ModuleType(sub)
    sys.modules["pathway"] = pw


def _clear_src() -> None:
    for k in list(sys.modules):
        if k.startswith("src."):
            del sys.modules[k]


def _import_metrics():
    """Fresh import of src.metrics (no stubs needed — pure Python)."""
    _stub_dotenv()
    _clear_src()
    return importlib.import_module("src.metrics")


def _import_instrumentation():
    """Fresh import of src.instrumentation with Pathway stubbed."""
    _stub_dotenv()
    _stub_pathway()
    _clear_src()
    return importlib.import_module("src.instrumentation")


# ---------------------------------------------------------------------------
# compute_percentile
# ---------------------------------------------------------------------------

class TestComputePercentile:
    """Percentile computation: edge cases, interpolation, and known values."""

    def _p(self, data, pct):
        m = _import_metrics()
        return m.compute_percentile(collections.deque(data), pct)

    def test_empty_returns_zero(self):
        """Empty deque → 0.0."""
        assert self._p([], 50) == 0.0

    def test_single_element_any_percentile(self):
        """Single element → that element for any percentile."""
        assert self._p([42.0], 0)   == 42.0
        assert self._p([42.0], 50)  == 42.0
        assert self._p([42.0], 100) == 42.0

    def test_two_elements_p50(self):
        """P50 of [1, 2] with linear interpolation = 1.5."""
        assert abs(self._p([1.0, 2.0], 50) - 1.5) < 1e-9

    def test_p0_is_minimum(self):
        """P0 always returns the minimum value."""
        assert self._p([5.0, 3.0, 9.0, 1.0], 0) == 1.0

    def test_p100_is_maximum(self):
        """P100 always returns the maximum value."""
        assert self._p([5.0, 3.0, 9.0, 1.0], 100) == 9.0

    def test_uniform_values_any_percentile(self):
        """All-equal values → the same value for any percentile."""
        data = [7.0] * 20
        for pct in (25, 50, 75, 99):
            assert abs(self._p(data, pct) - 7.0) < 1e-9

    def test_p50_odd_count(self):
        """P50 of [1,2,3,4,5] = 3.0 (exact middle)."""
        assert abs(self._p([5.0, 1.0, 3.0, 2.0, 4.0], 50) - 3.0) < 1e-9

    def test_p99_large_dataset(self):
        """P99 of 100 evenly-spaced values [1..100] ≈ 99.0 (linear interp)."""
        data = [float(i) for i in range(1, 101)]
        result = self._p(data, 99)
        assert abs(result - 99.0) < 0.1

    def test_interpolation_between_two_sorted(self):
        """P25 of [10, 20] = 12.5 via linear interpolation."""
        result = self._p([10.0, 20.0], 25)
        assert abs(result - 12.5) < 1e-9

    def test_accepts_deque_type(self):
        """compute_percentile accepts a collections.deque directly."""
        m = _import_metrics()
        d = collections.deque([1.0, 2.0, 3.0], maxlen=100)
        assert abs(m.compute_percentile(d, 50) - 2.0) < 1e-9


# ---------------------------------------------------------------------------
# LatencyCollector
# ---------------------------------------------------------------------------

class TestLatencyCollector:
    """Rolling window behaviour and percentile accessors."""

    def _col(self, maxlen: int = 1000):
        m = _import_metrics()
        return m.LatencyCollector(maxlen=maxlen)

    def test_empty_len(self):
        """Empty collector has length 0."""
        assert len(self._col()) == 0

    def test_record_increments_length(self):
        """Each record call increases len by 1 up to maxlen."""
        c = self._col()
        c.record(10.0)
        c.record(20.0)
        assert len(c) == 2

    def test_maxlen_evicts_oldest(self):
        """Deque evicts oldest entries once maxlen is reached."""
        c = self._col(maxlen=3)
        for v in [1.0, 2.0, 3.0, 4.0]:
            c.record(v)
        assert len(c) == 3

    def test_p50_single_value(self):
        """P50 of a single-element collector = that element."""
        c = self._col()
        c.record(42.0)
        assert abs(c.p50() - 42.0) < 1e-9

    def test_p99_single_value(self):
        """P99 of a single-element collector = that element."""
        c = self._col()
        c.record(42.0)
        assert abs(c.p99() - 42.0) < 1e-9

    def test_p50_empty_returns_zero(self):
        """P50 on empty collector = 0.0."""
        assert self._col().p50() == 0.0

    def test_p99_empty_returns_zero(self):
        """P99 on empty collector = 0.0."""
        assert self._col().p99() == 0.0

    def test_p99_higher_than_p50(self):
        """P99 >= P50 for any realistic distribution."""
        c = self._col()
        for v in range(1, 101):
            c.record(float(v))
        assert c.p99() >= c.p50()

    def test_alerts_per_min_zero_when_empty(self):
        """alerts_per_min = 0.0 for an empty collector."""
        assert self._col().alerts_per_min() == 0.0

    def test_alerts_per_min_zero_window(self):
        """alerts_per_min with rate_window_seconds=0 returns 0.0 (safe guard)."""
        c = self._col()
        c.record(1.0)
        assert c.alerts_per_min(rate_window_seconds=0.0) == 0.0

    def test_alerts_per_min_immediate_counts(self):
        """alerts_per_min > 0 when records made within the window."""
        c = self._col()
        for _ in range(5):
            c.record(1.0)
        rate = c.alerts_per_min(rate_window_seconds=60.0)
        assert rate > 0.0

    def test_reset_clears_all(self):
        """reset() empties both latencies and timestamps."""
        c = self._col()
        c.record(10.0)
        c.reset()
        assert len(c) == 0
        assert c.p50() == 0.0

    def test_record_keeps_float_precision(self):
        """Recorded value survives through p50() without precision loss."""
        c = self._col()
        c.record(123.456789)
        assert abs(c.p50() - 123.456789) < 1e-6


# ---------------------------------------------------------------------------
# format_latency_summary
# ---------------------------------------------------------------------------

class TestFormatLatencySummary:
    """Output format and precision rules."""

    def _fmt(self, p50, p99, rate):
        m = _import_metrics()
        return m.format_latency_summary(p50, p99, rate)

    def test_contains_p50(self):
        """Output must contain 'P50'."""
        assert "P50" in self._fmt(10.0, 50.0, 5.0)

    def test_contains_p99(self):
        """Output must contain 'P99'."""
        assert "P99" in self._fmt(10.0, 50.0, 5.0)

    def test_contains_alerts_per_min(self):
        """Output must contain 'Alerts/min'."""
        assert "Alerts/min" in self._fmt(10.0, 50.0, 5.0)

    def test_known_format(self):
        """Exact format: 'Latency P50: 10.0ms | P99: 50.0ms | Alerts/min: 5.0'"""
        result = self._fmt(10.0, 50.0, 5.0)
        assert result == "Latency P50: 10.0ms | P99: 50.0ms | Alerts/min: 5.0"

    def test_one_decimal_place_p50(self):
        """P50 must be formatted to exactly 1 decimal place."""
        result = self._fmt(12.3456, 99.0, 1.0)
        assert "12.3ms" in result

    def test_one_decimal_place_p99(self):
        """P99 must be formatted to exactly 1 decimal place."""
        result = self._fmt(10.0, 99.9876, 1.0)
        assert "99.99" not in result   # must not have > 1 dp
        assert "100.0ms" in result     # rounds to 1dp

    def test_one_decimal_place_rate(self):
        """Alerts/min must be formatted to exactly 1 decimal place."""
        result = self._fmt(10.0, 50.0, 4.9999)
        assert "5.0" in result

    def test_zero_values_allowed(self):
        """Zero values produce valid output without exceptions."""
        result = self._fmt(0.0, 0.0, 0.0)
        assert "0.0ms" in result


# ---------------------------------------------------------------------------
# MetricsReporter
# ---------------------------------------------------------------------------

class TestMetricsReporter:
    """Interval gating and first-report behaviour."""

    def _reporter_and_collector(self, interval_s=30):
        m = _import_metrics()
        col = m.LatencyCollector()
        rep = m.MetricsReporter(col, interval_seconds=interval_s)
        return rep, col

    def test_no_report_before_interval(self):
        """maybe_report() returns False immediately (interval not elapsed)."""
        rep, col = self._reporter_and_collector(interval_s=30)
        col.record(10.0)
        # Interval is 30 s; should not fire immediately.
        assert rep.maybe_report() is False

    def test_no_report_on_empty_collector(self):
        """maybe_report() returns False when collector is empty."""
        rep, col = self._reporter_and_collector(interval_s=0)
        assert rep.maybe_report() is False

    def test_report_fires_after_interval(self):
        """maybe_report() returns True once the interval has elapsed."""
        import logging
        m = _import_metrics()
        col = m.LatencyCollector()
        col.record(10.0)
        rep = m.MetricsReporter(col, interval_seconds=0.0)
        # With interval=0, next call fires immediately
        rep._last_report_mono -= 1.0   # force interval elapsed
        assert rep.maybe_report() is True

    def test_report_resets_timer(self):
        """After a report fires, subsequent call within interval returns False."""
        m = _import_metrics()
        col = m.LatencyCollector()
        col.record(10.0)
        rep = m.MetricsReporter(col, interval_seconds=100.0)
        rep._last_report_mono -= 200.0   # force first fire (200 s overdue)
        rep.maybe_report()               # fires, resets timer to now
        # Interval is 100 s; next call should be well within cooldown
        assert rep.maybe_report() is False



# ---------------------------------------------------------------------------
# _TimelineStore (instrumentation)
# ---------------------------------------------------------------------------

class TestTimelineStore:
    """Timeline store: record, retrieve, and latency computation."""

    def _store(self):
        m = _import_instrumentation()
        store = m._TimelineStore()
        return store

    def test_unknown_event_returns_zero(self):
        """get_stage_time on unknown event_id returns 0.0."""
        store = self._store()
        assert store.get_stage_time("x|y", "ingestion") == 0.0

    def test_record_then_get(self):
        """Recorded timestamp is retrievable within a small delta."""
        store = self._store()
        before = time.time()
        store.record("a|b", "ingestion")
        after = time.time()
        t = store.get_stage_time("a|b", "ingestion")
        assert before <= t <= after

    def test_latency_ms_both_known(self):
        """latency_ms = (t1 - t0) * 1000 when both stages recorded."""
        store = self._store()
        store._store["ev"] = {"ingestion": 1000.0, "alert": 1000.5}
        result = store.latency_ms("ev", "ingestion", "alert")
        assert abs(result - 500.0) < 1e-6

    def test_latency_ms_missing_stage_returns_negative(self):
        """latency_ms returns -1.0 when either stage is missing."""
        store = self._store()
        store._store["ev"] = {"ingestion": 1000.0}
        assert store.latency_ms("ev", "ingestion", "alert") == -1.0

    def test_latency_ms_unknown_event_returns_negative(self):
        """latency_ms returns -1.0 for a completely unknown event_id."""
        store = self._store()
        assert store.latency_ms("unknown", "ingestion", "alert") == -1.0

    def test_reset_clears_store(self):
        """reset() removes all recorded timelines."""
        store = self._store()
        store.record("a|b", "ingestion")
        store.reset()
        assert store.get_stage_time("a|b", "ingestion") == 0.0

    def test_multiple_stages_independent(self):
        """Each stage is stored independently for the same event."""
        store = self._store()
        store._store["ev"] = {
            "ingestion": 1000.0,
            "scoring":   1000.1,
            "eri":       1000.2,
            "alert":     1000.3,
        }
        assert store.get_stage_time("ev", "scoring")  == 1000.1
        assert store.get_stage_time("ev", "eri")      == 1000.2


# ---------------------------------------------------------------------------
# make_event_id (instrumentation)
# ---------------------------------------------------------------------------

class TestMakeEventId:
    """Event ID key format."""

    def _fn(self):
        m = _import_instrumentation()
        return m.make_event_id

    def test_format(self):
        """event_id = '{sensor_id}|{event_time}'."""
        fn = self._fn()
        assert fn("FACTORY_A", "2026-02-01 12:00") == "FACTORY_A|2026-02-01 12:00"

    def test_pipe_delimiter(self):
        """Result must contain a pipe character as delimiter."""
        fn = self._fn()
        assert "|" in fn("S1", "T1")

    def test_round_trip(self):
        """Splitting on '|' recovers original parts."""
        fn = self._fn()
        sid, ts = "FACTORY_B", "2026-02-01 09:30"
        eid = fn(sid, ts)
        parts = eid.split("|", 1)
        assert parts[0] == sid and parts[1] == ts


# ---------------------------------------------------------------------------
# Public API / importability
# ---------------------------------------------------------------------------

class TestPublicAPI:
    """Module surface and import hygiene."""

    def test_metrics_no_pathway_import(self):
        """src.metrics must not import pathway."""
        import inspect
        m = _import_metrics()
        src = inspect.getsource(m)
        assert "import pathway" not in src

    def test_metrics_exports_collector(self):
        """LatencyCollector must be importable from src.metrics."""
        m = _import_metrics()
        assert callable(m.LatencyCollector)

    def test_metrics_exports_reporter(self):
        """MetricsReporter must be importable from src.metrics."""
        m = _import_metrics()
        assert callable(m.MetricsReporter)

    def test_metrics_exports_compute_percentile(self):
        """compute_percentile must be callable from src.metrics."""
        m = _import_metrics()
        assert callable(m.compute_percentile)

    def test_metrics_exports_format_latency_summary(self):
        """format_latency_summary must be callable from src.metrics."""
        m = _import_metrics()
        assert callable(m.format_latency_summary)

    def test_instrumentation_exports_build_metrics_stream(self):
        """build_metrics_stream must be callable from src.instrumentation."""
        m = _import_instrumentation()
        assert callable(m.build_metrics_stream)

    def test_rolling_window_size_constant(self):
        """ROLLING_WINDOW_SIZE must equal 1000 (as required)."""
        m = _import_metrics()
        assert m.ROLLING_WINDOW_SIZE == 1000

    def test_default_collector_maxlen(self):
        """Default LatencyCollector uses ROLLING_WINDOW_SIZE capacity."""
        m = _import_metrics()
        c = m.LatencyCollector()
        for _ in range(m.ROLLING_WINDOW_SIZE + 50):
            c.record(1.0)
        assert len(c) == m.ROLLING_WINDOW_SIZE

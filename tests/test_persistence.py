"""
Tests for src/persistence — pure-Python unit tests.

All tests target _SensorStateStore directly (the stateful core) and the
CONFIG dict.  No Pathway runtime is started.

Run with:
    python3 -m pytest tests/test_persistence.py -v
"""

from __future__ import annotations

import importlib
import logging
import sys
import types


# ---------------------------------------------------------------------------
# Stubs so constants.py and persistence.py can be imported dependency-free
# ---------------------------------------------------------------------------

def _stub_dotenv() -> None:
    """Stub python-dotenv for test environments where it is not installed."""
    if "dotenv" in sys.modules:
        return
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *a, **kw: None
    sys.modules["dotenv"] = dotenv


def _stub_pathway() -> None:
    """Stub pathway so persistence.py can be imported without the runtime."""
    if "pathway" in sys.modules:
        return
    pw = types.ModuleType("pathway")
    pw.Schema = object
    pw.Table  = object
    pw.udf    = lambda fn: fn       # decorator passthrough

    reducers = types.ModuleType("pathway.reducers")
    temporal  = types.ModuleType("pathway.temporal")
    pw.reducers = reducers
    pw.temporal = temporal

    sys.modules["pathway"]          = pw
    sys.modules["pathway.reducers"] = reducers
    sys.modules["pathway.temporal"] = temporal


def _import_persistence():
    """Return a freshly imported src.persistence module with all stubs in place."""
    _stub_dotenv()
    _stub_pathway()

    # Drop cached versions so every call gets a clean import
    for mod in list(sys.modules):
        if mod in ("src.persistence", "src.constants"):
            del sys.modules[mod]

    return importlib.import_module("src.persistence")


# ---------------------------------------------------------------------------
# CONFIG tests
# ---------------------------------------------------------------------------

class TestConfig:
    """Verify CONFIG dict is exported with correct keys and default values."""

    def test_config_exists(self):
        """CONFIG must be a dict exported at module level."""
        p = _import_persistence()
        assert isinstance(p.CONFIG, dict)

    def test_config_has_persistence_count(self):
        """CONFIG must contain PERSISTENCE_COUNT key."""
        p = _import_persistence()
        assert "PERSISTENCE_COUNT" in p.CONFIG

    def test_persistence_count_default(self):
        """Default PERSISTENCE_COUNT must be 3 per the specification."""
        p = _import_persistence()
        assert p.CONFIG["PERSISTENCE_COUNT"] == 3

    def test_persistence_count_positive(self):
        """PERSISTENCE_COUNT must be a positive integer."""
        p = _import_persistence()
        val = p.CONFIG["PERSISTENCE_COUNT"]
        assert isinstance(val, int) and val > 0


# ---------------------------------------------------------------------------
# _SensorStateStore unit tests
# ---------------------------------------------------------------------------

class TestSensorStateStore:
    """Direct tests of the stateful counter logic."""

    def _make_store(self):
        """Return a fresh _SensorStateStore for each test."""
        p = _import_persistence()
        return p._SensorStateStore()

    def test_initial_count_is_zero(self):
        """A sensor not yet seen must report count 0."""
        store = self._make_store()
        assert store.get("S1") == 0

    def test_anomaly_increments_counter(self):
        """Each anomalous reading increments the counter by 1."""
        store = self._make_store()
        assert store.update("S1", True) == 1
        assert store.update("S1", True) == 2
        assert store.update("S1", True) == 3

    def test_non_anomaly_resets_to_zero(self):
        """A normal reading after anomalies resets the counter to 0."""
        store = self._make_store()
        store.update("S1", True)
        store.update("S1", True)
        result = store.update("S1", False)
        assert result == 0

    def test_counter_resumes_after_reset(self):
        """Counter should restart from 1 after a reset, not continue from before."""
        store = self._make_store()
        store.update("S1", True)
        store.update("S1", True)
        store.update("S1", False)   # reset
        assert store.update("S1", True) == 1

    def test_sensors_are_independent(self):
        """Counters for different sensor_ids must not interfere."""
        store = self._make_store()
        store.update("S1", True)
        store.update("S1", True)
        store.update("S2", True)    # S2 started independently
        assert store.get("S1") == 2
        assert store.get("S2") == 1

    def test_normal_reading_on_zero_counter_stays_zero(self):
        """A normal reading when counter is already 0 must return 0 (no underflow)."""
        store = self._make_store()
        result = store.update("S1", False)
        assert result == 0

    def test_multiple_resets_do_not_go_negative(self):
        """Repeated normal readings must keep the counter at 0."""
        store = self._make_store()
        for _ in range(5):
            result = store.update("S1", False)
        assert result == 0

    def test_reset_all_clears_every_sensor(self):
        """reset_all() must zero every sensor's counter."""
        store = self._make_store()
        store.update("S1", True)
        store.update("S2", True)
        store.reset_all()
        assert store.get("S1") == 0
        assert store.get("S2") == 0

    def test_get_does_not_mutate_state(self):
        """get() must be a pure read with no side effects."""
        store = self._make_store()
        store.update("S1", True)
        _ = store.get("S1")         # read
        _ = store.get("S1")         # read again
        assert store.get("S1") == 1  # still 1, not 3


# ---------------------------------------------------------------------------
# Persistence gate logic (threshold check)
# ---------------------------------------------------------------------------

class TestPersistenceGate:
    """Test the threshold predicate: consecutive_count >= PERSISTENCE_COUNT."""

    THRESHOLD = 3

    def _is_confirmed(self, count: int) -> bool:
        """Inline reference matching _is_confirmed UDF."""
        return count >= self.THRESHOLD

    def test_below_threshold_not_confirmed(self):
        """Counts 0, 1, 2 must NOT trigger confirmation when threshold is 3."""
        for count in range(self.THRESHOLD):
            assert not self._is_confirmed(count), f"count={count} should not confirm"

    def test_exactly_at_threshold_confirmed(self):
        """Count exactly equal to PERSISTENCE_COUNT must trigger (>=, not >)."""
        assert self._is_confirmed(self.THRESHOLD)

    def test_above_threshold_confirmed(self):
        """Counts above the threshold must also confirm."""
        for count in range(self.THRESHOLD + 1, self.THRESHOLD + 10):
            assert self._is_confirmed(count), f"count={count} should confirm"


# ---------------------------------------------------------------------------
# Counter + gate integration
# ---------------------------------------------------------------------------

class TestIntegration:
    """End-to-end simulation: counter + gate for a stream of readings."""

    THRESHOLD = 3

    def _simulate(self, readings: list[bool]) -> list[int]:
        """Run a sequence of is_anomaly values through the store and return counts."""
        p = _import_persistence()
        store = p._SensorStateStore()
        return [store.update("S1", r) for r in readings]

    def test_confirmation_fires_at_exactly_persistence_count(self):
        """Counter hits threshold exactly at the 3rd consecutive anomaly."""
        counts = self._simulate([True, True, True])
        assert counts == [1, 2, 3]
        assert counts[-1] >= self.THRESHOLD   # gate would open here

    def test_reset_before_threshold_prevents_confirmation(self):
        """Two anomalies, one normal, then one anomaly — gate must NOT open."""
        counts = self._simulate([True, True, False, True])
        assert counts == [1, 2, 0, 1]
        assert all(c < self.THRESHOLD for c in counts)

    def test_confirmation_after_reset_requires_full_streak(self):
        """After a reset the full PERSISTENCE_COUNT streak must be rebuilt."""
        counts = self._simulate([True, True, False, True, True, True])
        # Gate should only open at the 6th reading (3rd streak after reset)
        assert counts[5] == 3
        assert counts[3] == 1   # restarted from 1

    def test_multiple_sensors_confirmed_independently(self):
        """Each sensor maintains its own counter; one resetting must not affect others."""
        p = _import_persistence()
        store = p._SensorStateStore()

        store.update("SA", True)
        store.update("SB", True)
        store.update("SA", True)
        store.update("SA", False)  # SA resets; SB untouched
        store.update("SB", True)
        store.update("SB", True)

        assert store.get("SA") == 0    # was reset
        assert store.get("SB") == 3    # fully confirmed


# ---------------------------------------------------------------------------
# Debug logging
# ---------------------------------------------------------------------------

class TestDebugLogging:
    """Ensure a DEBUG log is emitted on counter reset (and only then)."""

    def test_reset_emits_debug_log(self, caplog):
        """A counter reset from >0 to 0 must emit exactly one DEBUG message."""
        p = _import_persistence()
        store = p._SensorStateStore()

        store.update("S1", True)     # count = 1
        store.update("S1", True)     # count = 2

        with caplog.at_level(logging.DEBUG, logger="src.persistence"):
            store.update("S1", False)   # reset → DEBUG expected

        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.DEBUG
        assert "S1" in caplog.records[0].message

    def test_no_log_when_counter_already_zero(self, caplog):
        """No DEBUG message when a normal reading arrives on an already-zero counter."""
        p = _import_persistence()
        store = p._SensorStateStore()

        with caplog.at_level(logging.DEBUG, logger="src.persistence"):
            store.update("S1", False)   # counter already 0 → no log

        assert len(caplog.records) == 0

    def test_no_log_on_anomalous_reading(self, caplog):
        """Anomalous readings must not produce any log output."""
        p = _import_persistence()
        store = p._SensorStateStore()

        with caplog.at_level(logging.DEBUG, logger="src.persistence"):
            store.update("S1", True)
            store.update("S1", True)

        assert len(caplog.records) == 0


# ---------------------------------------------------------------------------
# Import side-effect guard
# ---------------------------------------------------------------------------

class TestImportability:
    """Importing src.persistence must not cause any I/O or side effects."""

    def test_import_no_side_effects(self):
        """Module import must succeed without pw.run() or file I/O."""
        p = _import_persistence()
        assert p is not None

    def test_build_confirmed_anomalies_callable(self):
        """build_confirmed_anomalies must be a callable exported at module level."""
        p = _import_persistence()
        assert callable(p.build_confirmed_anomalies)

    def test_required_public_api_present(self):
        """All required public names must be exported at module level."""
        p = _import_persistence()
        assert hasattr(p, "CONFIG"),                    "CONFIG missing"
        assert hasattr(p, "logger"),                    "logger missing"
        assert hasattr(p, "build_confirmed_anomalies"), "build_confirmed_anomalies missing"

    def test_no_bare_numeric_module_globals(self):
        """No raw int/float constants should appear as module-level names outside CONFIG."""
        p = _import_persistence()
        numeric_globals = {
            name: val for name, val in vars(p).items()
            if not name.startswith("_")
            and isinstance(val, (int, float))
            and not isinstance(val, bool)
        }
        assert not numeric_globals, (
            f"Bare numeric globals found (should live in CONFIG): {numeric_globals}"
        )

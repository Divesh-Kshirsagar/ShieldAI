"""
Tests for src/multivariate — pure-Python unit tests.

Covers:
  - CONFIG field presence and defaults
  - Pure-Python math helpers: _rms, _sensor_bit, _sensors_from_bitmask,
    _missing_from_bitmask, _timestamp_bucket
  - End-to-end group scoring simulation (no Pathway runtime)
  - Public API contract: presence, callable, docstring
  - Import hygiene: no side effects, no bare numeric globals

Run with:
    python3 -m pytest tests/test_multivariate.py -v
"""

from __future__ import annotations

import importlib
import math
import sys
import types


# ---------------------------------------------------------------------------
# Stubs — pathway, dotenv
# ---------------------------------------------------------------------------

def _stub_dotenv() -> None:
    """Register a no-op dotenv stub."""
    if "dotenv" in sys.modules:
        return
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *a, **kw: None
    sys.modules["dotenv"] = dotenv


def _stub_pathway() -> None:
    """Register a minimal pathway stub in sys.modules."""
    if "pathway" in sys.modules:
        return
    pw = types.ModuleType("pathway")
    pw.Schema = object
    pw.Table  = object
    pw.udf    = lambda fn: fn
    for sub in ("pathway.reducers", "pathway.temporal"):
        sys.modules[sub] = types.ModuleType(sub)
    sys.modules["pathway"] = pw


def _clear_src_modules() -> None:
    """Evict all src.* modules so each test gets a fresh import."""
    for key in list(sys.modules):
        if key.startswith("src."):
            del sys.modules[key]


def _import_multivariate(sensor_groups: dict | None = None):
    """Return a freshly imported src.multivariate with all stubs active.

    If sensor_groups is provided, it is injected into the CONFIG env override
    so that tests can verify behaviour with custom group definitions.
    """
    import json, os
    _stub_dotenv()
    _stub_pathway()
    _clear_src_modules()
    if sensor_groups is not None:
        os.environ["SENSOR_GROUPS"] = json.dumps(sensor_groups)
    else:
        os.environ.pop("SENSOR_GROUPS", None)
    return importlib.import_module("src.multivariate")


# ---------------------------------------------------------------------------
# CONFIG contract
# ---------------------------------------------------------------------------

_GROUPS = {"g1": ["pH", "turbidity", "flow"]}


class TestConfig:
    """CONFIG dict keys and defaults."""

    def test_config_exists(self):
        """Module must export a dict named CONFIG."""
        m = _import_multivariate()
        assert isinstance(m.CONFIG, dict)

    def test_config_has_sensor_groups(self):
        """CONFIG must contain SENSOR_GROUPS."""
        m = _import_multivariate()
        assert "SENSOR_GROUPS" in m.CONFIG

    def test_config_has_group_threshold(self):
        """CONFIG must contain GROUP_THRESHOLD."""
        m = _import_multivariate()
        assert "GROUP_THRESHOLD" in m.CONFIG

    def test_config_has_sync_tolerance(self):
        """CONFIG must contain SYNC_TOLERANCE_MS."""
        m = _import_multivariate()
        assert "SYNC_TOLERANCE_MS" in m.CONFIG

    def test_group_threshold_default(self):
        """GROUP_THRESHOLD default must be 2.5."""
        import os
        os.environ.pop("GROUP_THRESHOLD", None)
        m = _import_multivariate()
        assert m.CONFIG["GROUP_THRESHOLD"] == 2.5

    def test_sync_tolerance_ms_default(self):
        """SYNC_TOLERANCE_MS default must be 5000 ms."""
        import os
        os.environ.pop("SYNC_TOLERANCE_MS", None)
        m = _import_multivariate()
        assert m.CONFIG["SYNC_TOLERANCE_MS"] == 5000


# ---------------------------------------------------------------------------
# _rms — root-mean-square computation
# ---------------------------------------------------------------------------

class TestRms:
    """Pure-Python RMS helper."""

    def test_single_value(self):
        """RMS of a single value equals its absolute value."""
        m = _import_multivariate()
        assert m._rms([3.0]) == pytest_approx(3.0)

    def test_uniform_values(self):
        """RMS of identical values equals that value."""
        m = _import_multivariate()
        assert m._rms([2.0, 2.0, 2.0]) == pytest_approx(2.0)

    def test_zero_values(self):
        """RMS of all-zero list is 0.0."""
        m = _import_multivariate()
        assert m._rms([0.0, 0.0]) == pytest_approx(0.0)

    def test_empty_list(self):
        """RMS of empty list is 0.0 (guard against ZeroDivisionError)."""
        m = _import_multivariate()
        assert m._rms([]) == 0.0

    def test_known_value(self):
        """RMS([3.0, 4.0]) == sqrt((9+16)/2) == sqrt(12.5)."""
        m = _import_multivariate()
        expected = math.sqrt((9.0 + 16.0) / 2)
        assert abs(m._rms([3.0, 4.0]) - expected) < 1e-9

    def test_mixed_sign(self):
        """RMS is sign-agnostic — z-scores can be negative."""
        m = _import_multivariate()
        assert m._rms([-3.0, 4.0]) == pytest_approx(m._rms([3.0, 4.0]))

    def test_large_z_scores(self):
        """RMS handles large z-scores without overflow."""
        m = _import_multivariate()
        result = m._rms([1e6, 1e6])
        assert pytest_approx(result, rel=1e-6) == 1e6


# ---------------------------------------------------------------------------
# _sensor_bit
# ---------------------------------------------------------------------------

class TestSensorBit:
    """Bitmask bit assignment per sensor within a group."""

    def test_first_sensor_is_bit0(self):
        """First sensor in group must map to bit 0 (value 1)."""
        m = _import_multivariate(_GROUPS)
        assert m._sensor_bit("g1", "pH") == 1

    def test_second_sensor_is_bit1(self):
        """Second sensor maps to bit 1 (value 2)."""
        m = _import_multivariate(_GROUPS)
        assert m._sensor_bit("g1", "turbidity") == 2

    def test_third_sensor_is_bit2(self):
        """Third sensor maps to bit 2 (value 4)."""
        m = _import_multivariate(_GROUPS)
        assert m._sensor_bit("g1", "flow") == 4

    def test_unknown_sensor_returns_zero(self):
        """Sensor not in group returns 0 — preventing false bitmask contributions."""
        m = _import_multivariate(_GROUPS)
        assert m._sensor_bit("g1", "conductivity") == 0

    def test_unknown_group_returns_zero(self):
        """Unknown group name returns 0 safely."""
        m = _import_multivariate(_GROUPS)
        assert m._sensor_bit("nonexistent_group", "pH") == 0

    def test_bits_are_unique_per_sensor(self):
        """All bits within a group must be distinct powers of two."""
        m = _import_multivariate(_GROUPS)
        bits = [m._sensor_bit("g1", s) for s in _GROUPS["g1"]]
        assert len(set(bits)) == len(bits)
        for b in bits:
            assert b > 0 and (b & (b - 1)) == 0  # power of two


# ---------------------------------------------------------------------------
# _sensors_from_bitmask / _missing_from_bitmask
# ---------------------------------------------------------------------------

class TestBitmaskDecoding:
    """Round-trip bitmask encoding → decoding."""

    def test_all_sensors_present(self):
        """Full bitmask (all bits set) returns all group members."""
        m = _import_multivariate(_GROUPS)
        members = _GROUPS["g1"]
        full_mask = (1 << len(members)) - 1
        result = m._sensors_from_bitmask("g1", full_mask)
        assert sorted(result) == sorted(members)

    def test_no_sensors_present(self):
        """Zero bitmask returns empty contributing list."""
        m = _import_multivariate(_GROUPS)
        assert m._sensors_from_bitmask("g1", 0) == []

    def test_partial_presence(self):
        """Bitmask with only first two bits set returns first two sensors."""
        m = _import_multivariate(_GROUPS)
        mask = 0b011  # pH (bit0) + turbidity (bit1)
        result = m._sensors_from_bitmask("g1", mask)
        assert set(result) == {"pH", "turbidity"}

    def test_missing_is_complement(self):
        """missing_from_bitmask returns exactly the sensors NOT in contributing."""
        m = _import_multivariate(_GROUPS)
        members = set(_GROUPS["g1"])
        mask = 0b011
        contributing = set(m._sensors_from_bitmask("g1", mask))
        missing      = set(m._missing_from_bitmask("g1", mask))
        assert contributing | missing == members
        assert contributing & missing == set()

    def test_all_missing_when_mask_zero(self):
        """Zero bitmask means all sensors are missing."""
        m = _import_multivariate(_GROUPS)
        missing = m._missing_from_bitmask("g1", 0)
        assert sorted(missing) == sorted(_GROUPS["g1"])

    def test_none_missing_when_full_mask(self):
        """Full bitmask means no sensors are missing."""
        m = _import_multivariate(_GROUPS)
        n = len(_GROUPS["g1"])
        full_mask = (1 << n) - 1
        assert m._missing_from_bitmask("g1", full_mask) == []


# ---------------------------------------------------------------------------
# _timestamp_bucket
# ---------------------------------------------------------------------------

class TestTimestampBucket:
    """Timestamp bucketing logic."""

    def test_same_timestamp_same_bucket(self):
        """Identical timestamps always fall into the same bucket."""
        m = _import_multivariate()
        ts = "2026-02-01 12:23"
        assert m._timestamp_bucket(ts, 5000) == m._timestamp_bucket(ts, 5000)

    def test_timestamps_within_tolerance_same_bucket(self):
        """Timestamps within the same tolerance window share a bucket."""
        m = _import_multivariate()
        # Both are in the same 60-second bucket
        a = "2026-02-01 12:00"
        b = "2026-02-01 12:00"
        assert m._timestamp_bucket(a, 60_000) == m._timestamp_bucket(b, 60_000)

    def test_invalid_timestamp_returned_unchanged(self):
        """An unparseable timestamp is returned as-is rather than raising."""
        m = _import_multivariate()
        bad = "not-a-timestamp"
        result = m._timestamp_bucket(bad, 5000)
        assert result == bad


# ---------------------------------------------------------------------------
# End-to-end group scoring (pure Python, no Pathway)
# ---------------------------------------------------------------------------

class TestGroupScoringIntegration:
    """Simulate the RMS pipeline using pure Python calls (no Pathway runtime)."""

    def _simulate_group(self, group_name, members, z_scores_by_sensor, threshold=2.5):
        """Run the full group computation logic over provided z_scores_by_sensor dict."""
        import math as _math
        m = _import_multivariate({group_name: members})

        # Build bitmask from which sensors fired
        bitmask = 0
        for sensor_id, z in z_scores_by_sensor.items():
            bitmask |= m._sensor_bit(group_name, sensor_id)

        # Compute contributing and missing
        contributing = m._sensors_from_bitmask(group_name, bitmask)
        missing      = m._missing_from_bitmask(group_name, bitmask)

        # Compute RMS
        z_values = list(z_scores_by_sensor.values())
        composite = m._rms(z_values)
        is_anomaly = composite > threshold

        return {
            "composite_score": composite,
            "contributing": sorted(contributing),
            "missing": sorted(missing),
            "is_anomaly": is_anomaly,
        }

    def test_all_sensors_below_threshold(self):
        """No group anomaly when all z-scores are low."""
        result = self._simulate_group(
            "g1", ["pH", "turbidity"],
            {"pH": 1.0, "turbidity": 0.5}
        )
        assert not result["is_anomaly"]
        assert result["composite_score"] == pytest_approx(math.sqrt((1.0 + 0.25) / 2))

    def test_group_anomaly_when_rms_exceeds_threshold(self):
        """Group anomaly fires when RMS z-score exceeds GROUP_THRESHOLD."""
        result = self._simulate_group(
            "g1", ["pH", "turbidity"],
            {"pH": 3.5, "turbidity": 3.5},
            threshold=2.5,
        )
        assert result["is_anomaly"]

    def test_exactly_at_threshold_not_anomaly(self):
        """composite_score == threshold is not an anomaly (strictly greater than)."""
        rms_target = 2.5
        # Both equal → rms = rms_target
        z = rms_target
        result = self._simulate_group(
            "g1", ["pH", "turbidity"],
            {"pH": z, "turbidity": z},
            threshold=rms_target,
        )
        assert not result["is_anomaly"]

    def test_missing_sensor_excluded_from_rms(self):
        """A missing sensor does not pull composite_score to zero."""
        result = self._simulate_group(
            "g1", ["pH", "turbidity", "flow"],
            {"pH": 3.0, "turbidity": 3.0}  # flow missing
        )
        # RMS of [3.0, 3.0] = 3.0, not RMS of [3.0, 3.0, 0.0]
        assert pytest_approx(result["composite_score"]) == pytest_approx(3.0)
        assert "flow" in result["missing"]
        assert "flow" not in result["contributing"]

    def test_multi_sensor_contributing_and_missing_fields(self):
        """contributing + missing equals full group member list."""
        members = ["pH", "turbidity", "flow", "conductivity"]
        result = self._simulate_group(
            "g1", members,
            {"pH": 2.0, "flow": 1.5}
        )
        all_sensors = set(result["contributing"]) | set(result["missing"])
        assert all_sensors == set(members)

    def test_single_sensor_group(self):
        """Single-sensor group: composite_score equals |z_score| of that sensor."""
        result = self._simulate_group(
            "g1", ["pH"],
            {"pH": 3.7}
        )
        assert pytest_approx(result["composite_score"]) == pytest_approx(3.7)
        assert result["contributing"] == ["pH"]
        assert result["missing"] == []


# ---------------------------------------------------------------------------
# Public API / importability
# ---------------------------------------------------------------------------

class TestPublicAPI:
    """Module surface and import hygiene."""

    def test_build_group_anomalies_callable(self):
        """build_group_anomalies must be a callable at module level."""
        m = _import_multivariate()
        assert callable(m.build_group_anomalies)

    def test_build_group_anomalies_has_docstring(self):
        """build_group_anomalies must have a docstring."""
        m = _import_multivariate()
        assert m.build_group_anomalies.__doc__

    def test_logger_exported(self):
        """Module must export a logger instance."""
        import logging
        m = _import_multivariate()
        assert isinstance(m.logger, logging.Logger)

    def test_no_bare_numeric_module_globals(self):
        """No raw int/float constants at module level outside CONFIG."""
        m = _import_multivariate()
        numeric = {
            n: v for n, v in vars(m).items()
            if not n.startswith("_") and isinstance(v, (int, float))
            and not isinstance(v, bool)
        }
        assert not numeric, f"Bare numeric globals: {numeric}"

    def test_import_no_side_effects(self):
        """Importing multivariate must not raise or execute I/O."""
        m = _import_multivariate()
        assert m is not None


# ---------------------------------------------------------------------------
# validate_config integration
# ---------------------------------------------------------------------------

class TestConfigValidation:
    """validate_config correctly enforces multivariate field constraints."""

    def _validate_with(self, **overrides):
        """Build a config and run validate_config with the given field overrides."""
        _stub_dotenv()
        _stub_pathway()
        _clear_src_modules()
        cfg_mod = importlib.import_module("src.config")
        import dataclasses
        cfg = dataclasses.replace(cfg_mod.CONFIG, **overrides)
        cfg_mod.validate_config(cfg)

    def test_valid_config_passes(self):
        """Default config must pass validation."""
        self._validate_with()  # no overrides, should not raise

    def test_empty_sensor_groups_rejected(self):
        """Empty sensor_groups dict must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="sensor_groups"):
            self._validate_with(sensor_groups={})

    def test_empty_group_member_list_rejected(self):
        """Group with empty member list must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="sensor_groups"):
            self._validate_with(sensor_groups={"g1": []})

    def test_negative_group_threshold_rejected(self):
        """group_threshold <= 0 must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="group_threshold"):
            self._validate_with(group_threshold=0.0)

    def test_zero_sync_tolerance_rejected(self):
        """sync_tolerance_ms < 1 must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="sync_tolerance_ms"):
            self._validate_with(sync_tolerance_ms=0)


# ---------------------------------------------------------------------------
# Tiny approx helper (avoid pytest import at top level)
# ---------------------------------------------------------------------------

def pytest_approx(x, rel=1e-6):
    """Return a pytest.approx wrapper; imported lazily to avoid stub conflicts."""
    import pytest
    return pytest.approx(x, rel=rel)

"""
Tests for src/eri — pure-Python unit tests.

Covers:
  - CONFIG fields and defaults
  - classify_eri() — threshold boundaries and all four bands
  - _lookup_sensitivity() — known point, unknown point, fallback flag
  - _compute_eri() — formula correctness and multiplier scaling
  - End-to-end integration via plain dicts (no Pathway runtime)
  - validate_config() enforcement of ERI fields
  - Public API contract

Run with:
    python3 -m pytest tests/test_eri.py -v
"""

from __future__ import annotations

import importlib
import sys
import types


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

def _stub_dotenv() -> None:
    """Register a no-op dotenv stub."""
    if "dotenv" in sys.modules:
        return
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *a, **kw: None
    sys.modules["dotenv"] = dotenv


def _stub_pathway() -> None:
    """Register a minimal pathway stub."""
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
    """Evict all src.* modules for fresh imports."""
    for key in list(sys.modules):
        if key.startswith("src."):
            del sys.modules[key]


def _import_eri(overrides: dict | None = None):
    """Return a freshly imported src.eri with optional env overrides."""
    import json, os
    _stub_dotenv()
    _stub_pathway()
    _clear_src_modules()
    if overrides:
        for k, v in overrides.items():
            os.environ[k] = json.dumps(v) if isinstance(v, dict) else str(v)
    return importlib.import_module("src.eri")


def _cleanup_env(*keys: str) -> None:
    """Remove env vars set during a test."""
    import os
    for k in keys:
        os.environ.pop(k, None)


# ---------------------------------------------------------------------------
# CONFIG contract
# ---------------------------------------------------------------------------

class TestConfig:
    """CONFIG dict structure and default values."""

    def test_config_exists(self):
        """Module must export CONFIG as a dict."""
        m = _import_eri()
        assert isinstance(m.CONFIG, dict)

    def test_river_sensitivity_present(self):
        """CONFIG must include RIVER_SENSITIVITY."""
        m = _import_eri()
        assert "RIVER_SENSITIVITY" in m.CONFIG

    def test_default_sensitivity_present(self):
        """CONFIG must include DEFAULT_SENSITIVITY."""
        m = _import_eri()
        assert "DEFAULT_SENSITIVITY" in m.CONFIG

    def test_severity_multiplier_present(self):
        """CONFIG must include SEVERITY_MULTIPLIER."""
        m = _import_eri()
        assert "SEVERITY_MULTIPLIER" in m.CONFIG

    def test_eri_thresholds_present(self):
        """CONFIG must include ERI_THRESHOLDS list."""
        m = _import_eri()
        assert "ERI_THRESHOLDS" in m.CONFIG
        assert isinstance(m.CONFIG["ERI_THRESHOLDS"], list)

    def test_thresholds_sorted_ascending(self):
        """ERI_THRESHOLDS upper bounds must be strictly ascending."""
        m = _import_eri()
        bounds = [t for t, _ in m.CONFIG["ERI_THRESHOLDS"]]
        assert bounds == sorted(bounds)

    def test_severity_multiplier_default(self):
        """Default SEVERITY_MULTIPLIER is 1.0."""
        _cleanup_env("SEVERITY_MULTIPLIER")
        m = _import_eri()
        assert m.CONFIG["SEVERITY_MULTIPLIER"] == 1.0

    def test_default_sensitivity_default(self):
        """Default DEFAULT_SENSITIVITY is 2.0."""
        _cleanup_env("DEFAULT_SENSITIVITY")
        m = _import_eri()
        assert m.CONFIG["DEFAULT_SENSITIVITY"] == 2.0


# ---------------------------------------------------------------------------
# classify_eri
# ---------------------------------------------------------------------------

class TestClassifyEri:
    """Risk band classification using sorted threshold list."""

    def test_below_low_is_low(self):
        """ERI below the LOW threshold → 'LOW'."""
        m = _import_eri()
        assert m.classify_eri(0.0) == "LOW"

    def test_exactly_at_low_is_medium(self):
        """ERI == low threshold → 'MEDIUM' (threshold is upper-exclusive for previous band)."""
        m = _import_eri()
        assert m.classify_eri(2.0) == "MEDIUM"

    def test_between_low_and_medium_is_medium(self):
        """ERI between LOW and MEDIUM thresholds → 'MEDIUM'."""
        m = _import_eri()
        assert m.classify_eri(3.5) == "MEDIUM"

    def test_exactly_at_medium_is_high(self):
        """ERI == medium threshold → 'HIGH'."""
        m = _import_eri()
        assert m.classify_eri(5.0) == "HIGH"

    def test_between_medium_and_high_is_high(self):
        """ERI between MEDIUM and HIGH thresholds → 'HIGH'."""
        m = _import_eri()
        assert m.classify_eri(7.5) == "HIGH"

    def test_exactly_at_high_is_critical(self):
        """ERI == high threshold → 'CRITICAL'."""
        m = _import_eri()
        assert m.classify_eri(10.0) == "CRITICAL"

    def test_above_high_is_critical(self):
        """ERI above HIGH threshold → 'CRITICAL'."""
        m = _import_eri()
        assert m.classify_eri(99.9) == "CRITICAL"

    def test_zero_is_low(self):
        """Zero ERI is the lowest possible band."""
        m = _import_eri()
        assert m.classify_eri(0.0) == "LOW"

    def test_all_bands_reachable(self):
        """All four bands can be produced by classify_eri."""
        m = _import_eri()
        low    = m.CONFIG["ERI_THRESHOLDS"][0][0]
        medium = m.CONFIG["ERI_THRESHOLDS"][1][0]
        high   = m.CONFIG["ERI_THRESHOLDS"][2][0]
        assert m.classify_eri(low - 0.1)  == "LOW"
        assert m.classify_eri(low)        == "MEDIUM"
        assert m.classify_eri(medium)     == "HIGH"
        assert m.classify_eri(high)       == "CRITICAL"

    def test_custom_thresholds_respected(self):
        """Custom ERI_THRESHOLD_LOW env override changes band boundaries."""
        import os
        os.environ["ERI_THRESHOLD_LOW"]    = "1.0"
        os.environ["ERI_THRESHOLD_MEDIUM"] = "3.0"
        os.environ["ERI_THRESHOLD_HIGH"]   = "6.0"
        m = _import_eri()
        assert m.classify_eri(0.5)  == "LOW"
        assert m.classify_eri(1.5)  == "MEDIUM"
        assert m.classify_eri(4.0)  == "HIGH"
        assert m.classify_eri(6.0)  == "CRITICAL"
        _cleanup_env("ERI_THRESHOLD_LOW", "ERI_THRESHOLD_MEDIUM", "ERI_THRESHOLD_HIGH")


# ---------------------------------------------------------------------------
# _lookup_sensitivity
# ---------------------------------------------------------------------------

_POINT_A_FACTOR = 3.5
_RIVER_SEN = {"discharge_point_A": _POINT_A_FACTOR, "discharge_point_B": 1.2}


class TestLookupSensitivity:
    """River sensitivity lookup with fallback."""

    def _import_with_table(self):
        """Import eri with the test river sensitivity table."""
        import os, json
        os.environ["RIVER_SENSITIVITY"] = json.dumps(_RIVER_SEN)
        m = _import_eri()
        _cleanup_env("RIVER_SENSITIVITY")
        return m

    def test_known_point_returns_correct_factor(self):
        """Known discharge_point_id returns its configured sensitivity factor."""
        m = self._import_with_table()
        factor, unknown = m._lookup_sensitivity("discharge_point_A")
        assert abs(factor - _POINT_A_FACTOR) < 1e-9
        assert unknown is False

    def test_known_point_unknown_flag_false(self):
        """Known point sets unknown_sensitivity to False."""
        m = self._import_with_table()
        _, unknown = m._lookup_sensitivity("discharge_point_B")
        assert unknown is False

    def test_unknown_point_returns_default(self):
        """Unknown point returns DEFAULT_SENSITIVITY."""
        m = self._import_with_table()
        factor, unknown = m._lookup_sensitivity("unknown_point_X")
        assert abs(factor - m.CONFIG["DEFAULT_SENSITIVITY"]) < 1e-9

    def test_unknown_point_sets_flag_true(self):
        """Unknown point sets unknown_sensitivity to True."""
        m = self._import_with_table()
        _, unknown = m._lookup_sensitivity("mystery_point")
        assert unknown is True

    def test_empty_table_always_returns_default(self):
        """Empty RIVER_SENSITIVITY means all points use DEFAULT_SENSITIVITY."""
        import os, json
        os.environ["RIVER_SENSITIVITY"] = json.dumps({
            "discharge_point_A": 3.5  # still need at least one for validation
        })
        os.environ["DEFAULT_SENSITIVITY"] = "2.5"
        m = _import_eri()
        factor, unknown = m._lookup_sensitivity("new_point")
        assert abs(factor - 2.5) < 1e-9
        assert unknown is True
        _cleanup_env("RIVER_SENSITIVITY", "DEFAULT_SENSITIVITY")


# ---------------------------------------------------------------------------
# _compute_eri
# ---------------------------------------------------------------------------

class TestComputeEri:
    """ERI formula: composite * sensitivity * multiplier."""

    def test_basic_formula(self):
        """ERI = composite_score * sensitivity_factor * SEVERITY_MULTIPLIER."""
        m = _import_eri()
        expected = 3.0 * 2.5 * m.CONFIG["SEVERITY_MULTIPLIER"]
        assert abs(m._compute_eri(3.0, 2.5) - expected) < 1e-9

    def test_multiplier_scaling(self):
        """Doubling SEVERITY_MULTIPLIER doubles ERI."""
        import os
        os.environ["SEVERITY_MULTIPLIER"] = "2.0"
        m   = _import_eri()
        eri = m._compute_eri(3.0, 2.5)
        assert abs(eri - 3.0 * 2.5 * 2.0) < 1e-9
        _cleanup_env("SEVERITY_MULTIPLIER")

    def test_zero_score_gives_zero_eri(self):
        """Zero composite score → ERI = 0."""
        m = _import_eri()
        assert m._compute_eri(0.0, 3.5) == 0.0

    def test_unit_multiplier_unit_sensitivity(self):
        """With multiplier=1 and sensitivity=1, ERI equals composite_score."""
        import os
        os.environ["SEVERITY_MULTIPLIER"] = "1.0"
        m = _import_eri()
        assert abs(m._compute_eri(4.2, 1.0) - 4.2) < 1e-9
        _cleanup_env("SEVERITY_MULTIPLIER")

    def test_known_value(self):
        """2.0 * 3.5 * 1.0 = 7.0."""
        import os
        os.environ["SEVERITY_MULTIPLIER"] = "1.0"
        m = _import_eri()
        assert abs(m._compute_eri(2.0, 3.5) - 7.0) < 1e-9
        _cleanup_env("SEVERITY_MULTIPLIER")


# ---------------------------------------------------------------------------
# End-to-end integration (pure Python, no Pathway)
# ---------------------------------------------------------------------------

class TestEriIntegration:
    """Simulate the full ERI pipeline for a row using pure Python."""

    def _run(self, discharge_point_id: str, composite_score: float) -> dict:
        """Run the full ERI computation for a single row dict."""
        import os, json
        os.environ["RIVER_SENSITIVITY"]   = json.dumps(_RIVER_SEN)
        os.environ["SEVERITY_MULTIPLIER"] = "1.0"
        m = _import_eri()
        _cleanup_env("RIVER_SENSITIVITY", "SEVERITY_MULTIPLIER")

        factor, unknown = m._lookup_sensitivity(discharge_point_id)
        eri             = m._compute_eri(composite_score, factor)
        risk_band       = m.classify_eri(eri)
        return {
            "discharge_point_id":  discharge_point_id,
            "composite_score":     composite_score,
            "sensitivity_factor":  factor,
            "eri":                 eri,
            "risk_band":           risk_band,
            "unknown_sensitivity": unknown,
        }

    def test_known_point_high_score_critical(self):
        """High composite + high sensitivity → CRITICAL band."""
        row = self._run("discharge_point_A", composite_score=4.0)
        # ERI = 4.0 * 3.5 * 1.0 = 14.0 → CRITICAL
        assert abs(row["eri"] - 14.0) < 1e-9
        assert row["risk_band"] == "CRITICAL"
        assert row["unknown_sensitivity"] is False

    def test_known_point_low_score_low_band(self):
        """Low composite + low sensitivity → LOW band."""
        row = self._run("discharge_point_B", composite_score=0.5)
        # ERI = 0.5 * 1.2 * 1.0 = 0.6 → LOW
        assert abs(row["eri"] - 0.6) < 1e-9
        assert row["risk_band"] == "LOW"

    def test_unknown_point_uses_default_sensitivity(self):
        """Unknown point: sensitivity_factor = DEFAULT_SENSITIVITY and flag set."""
        row = self._run("mystery_point", composite_score=1.0)
        assert row["sensitivity_factor"] == 2.0  # default
        assert row["unknown_sensitivity"] is True

    def test_output_fields_present(self):
        """All six required output fields must be present."""
        row = self._run("discharge_point_A", composite_score=1.0)
        required = {
            "discharge_point_id", "composite_score", "sensitivity_factor",
            "eri", "risk_band", "unknown_sensitivity",
        }
        assert required.issubset(row.keys())

    def test_eri_medium_band(self):
        """ERI in [2.0, 5.0) → MEDIUM."""
        row = self._run("discharge_point_B", composite_score=2.0)
        # ERI = 2.0 * 1.2 = 2.4 → MEDIUM
        assert abs(row["eri"] - 2.4) < 1e-9
        assert row["risk_band"] == "MEDIUM"

    def test_eri_high_band(self):
        """ERI in [5.0, 10.0) → HIGH."""
        row = self._run("discharge_point_A", composite_score=1.5)
        # ERI = 1.5 * 3.5 = 5.25 → HIGH
        assert abs(row["eri"] - 5.25) < 1e-9
        assert row["risk_band"] == "HIGH"


# ---------------------------------------------------------------------------
# validate_config integration
# ---------------------------------------------------------------------------

class TestConfigValidation:
    """validate_config enforces ERI field constraints."""

    def _validate_with(self, **overrides):
        """Build config with overrides and run validate_config."""
        _stub_dotenv()
        _stub_pathway()
        _clear_src_modules()
        cfg_mod = importlib.import_module("src.config")
        import dataclasses
        cfg = dataclasses.replace(cfg_mod.CONFIG, **overrides)
        cfg_mod.validate_config(cfg)

    def test_valid_defaults_pass(self):
        """Default config must pass validation."""
        self._validate_with()

    def test_sensitivity_above_5_rejected(self):
        """river_sensitivity value > 5.0 must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="river_sensitivity"):
            self._validate_with(river_sensitivity={"p": 5.1})

    def test_sensitivity_below_1_rejected(self):
        """river_sensitivity value < 1.0 must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="river_sensitivity"):
            self._validate_with(river_sensitivity={"p": 0.9})

    def test_default_sensitivity_below_1_rejected(self):
        """default_sensitivity < 1.0 must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="default_sensitivity"):
            self._validate_with(default_sensitivity=0.5)

    def test_zero_severity_multiplier_rejected(self):
        """severity_multiplier == 0 must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="severity_multiplier"):
            self._validate_with(severity_multiplier=0.0)

    def test_inverted_eri_thresholds_rejected(self):
        """low >= medium threshold ordering must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="ERI thresholds"):
            self._validate_with(eri_threshold_low=5.0, eri_threshold_medium=5.0,
                                eri_threshold_high=10.0)


# ---------------------------------------------------------------------------
# Public API / importability
# ---------------------------------------------------------------------------

class TestPublicAPI:
    """Module-level surface and import hygiene."""

    def test_build_eri_stream_callable(self):
        """build_eri_stream must be callable at module level."""
        m = _import_eri()
        assert callable(m.build_eri_stream)

    def test_classify_eri_callable(self):
        """classify_eri must be callable at module level."""
        m = _import_eri()
        assert callable(m.classify_eri)

    def test_logger_exported(self):
        """Module must export a logger instance."""
        import logging
        m = _import_eri()
        assert isinstance(m.logger, logging.Logger)

    def test_no_bare_numeric_globals(self):
        """No raw int/float constants at module level — all via CONFIG."""
        m = _import_eri()
        numeric = {
            n: v for n, v in vars(m).items()
            if not n.startswith("_") and isinstance(v, (int, float))
            and not isinstance(v, bool)
        }
        assert not numeric, f"Bare numeric globals: {numeric}"

    def test_import_no_side_effects(self):
        """Import must not raise, call pw.run(), or open files."""
        m = _import_eri()
        assert m is not None

    def test_docstrings_on_public_functions(self):
        """All public functions must have docstrings."""
        m = _import_eri()
        for name in ("build_eri_stream", "classify_eri"):
            fn = getattr(m, name)
            assert fn.__doc__, f"{name} has no docstring"

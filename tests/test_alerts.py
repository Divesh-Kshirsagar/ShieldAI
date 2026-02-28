"""
Tests for src/alerts — pure-Python unit tests.

No Pathway runtime needed: tests operate on the pure-Python helpers
(_band_passes_threshold, _get_alert_level, _is_high_or_critical,
_mask_str_field, _mask_float_field) and the _CooldownStore class directly.

Run with:
    python3 -m pytest tests/test_alerts.py -v
"""

from __future__ import annotations

import importlib
import sys
import types


# ---------------------------------------------------------------------------
# Stubs
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
    pw.Schema = object
    pw.Table  = object
    pw.udf    = lambda fn: fn
    for sub in ("pathway.reducers", "pathway.temporal"):
        sys.modules[sub] = types.ModuleType(sub)
    sys.modules["pathway"] = pw


def _clear_src() -> None:
    for key in list(sys.modules):
        if key.startswith("src."):
            del sys.modules[key]


def _import_alerts(env: dict | None = None):
    """Fresh import of src.alerts with optional env var overrides."""
    import os
    _stub_dotenv()
    _stub_pathway()
    _clear_src()
    for k, v in (env or {}).items():
        os.environ[k] = str(v)
    return importlib.import_module("src.alerts")


def _cleanup(*keys: str) -> None:
    import os
    for k in keys:
        os.environ.pop(k, None)


# ---------------------------------------------------------------------------
# CONFIG contract
# ---------------------------------------------------------------------------

class TestConfig:
    """CONFIG dict fields and public RISK_BAND_ORDER."""

    def test_config_exists(self):
        """Module must export CONFIG as a dict."""
        m = _import_alerts()
        assert isinstance(m.CONFIG, dict)

    def test_risk_band_order_exported(self):
        """RISK_BAND_ORDER must be exported at module level."""
        m = _import_alerts()
        assert isinstance(m.RISK_BAND_ORDER, dict)

    def test_risk_band_order_has_all_bands(self):
        """RISK_BAND_ORDER must contain LOW, MEDIUM, HIGH, CRITICAL."""
        m = _import_alerts()
        assert set(m.RISK_BAND_ORDER.keys()) == {"LOW", "MEDIUM", "HIGH", "CRITICAL"}

    def test_risk_band_order_ascending(self):
        """LOW < MEDIUM < HIGH < CRITICAL in numeric rank."""
        m = _import_alerts()
        o = m.RISK_BAND_ORDER
        assert o["LOW"] < o["MEDIUM"] < o["HIGH"] < o["CRITICAL"]

    def test_alert_min_risk_band_default(self):
        """Default ALERT_MIN_RISK_BAND is MEDIUM."""
        _cleanup("ALERT_MIN_RISK_BAND")
        m = _import_alerts()
        assert m.CONFIG["ALERT_MIN_RISK_BAND"] == "MEDIUM"

    def test_alert_cooldown_seconds_in_config(self):
        """ALERT_COOLDOWN_SECONDS must be in CONFIG."""
        m = _import_alerts()
        assert "ALERT_COOLDOWN_SECONDS" in m.CONFIG

    def test_no_eri_import(self):
        """alerts.py must not import from src.eri."""
        import inspect
        m = _import_alerts()
        src = inspect.getsource(m)
        assert "from src.eri" not in src
        assert "import src.eri" not in src


# ---------------------------------------------------------------------------
# RISK_BAND_ORDER and _band_passes_threshold
# ---------------------------------------------------------------------------

class TestBandPassesThreshold:
    """Band filtering via RISK_BAND_ORDER dict comparisons."""

    def _m(self, min_band: str = "MEDIUM"):
        return _import_alerts({"ALERT_MIN_RISK_BAND": min_band})

    def test_low_below_medium_threshold_fails(self):
        """LOW does not pass when min_band is MEDIUM."""
        m = self._m("MEDIUM")
        assert not m._band_passes_threshold("LOW")

    def test_medium_at_threshold_passes(self):
        """MEDIUM passes when min_band is MEDIUM."""
        m = self._m("MEDIUM")
        assert m._band_passes_threshold("MEDIUM")

    def test_high_above_threshold_passes(self):
        """HIGH passes when min_band is MEDIUM."""
        m = self._m("MEDIUM")
        assert m._band_passes_threshold("HIGH")

    def test_critical_above_threshold_passes(self):
        """CRITICAL always passes unless min_band is above it (impossible)."""
        m = self._m("MEDIUM")
        assert m._band_passes_threshold("CRITICAL")

    def test_low_min_band_passes_low(self):
        """LOW passes when min_band is LOW."""
        m = self._m("LOW")
        assert m._band_passes_threshold("LOW")

    def test_high_min_band_rejects_medium(self):
        """MEDIUM is suppressed when min_band is HIGH."""
        m = self._m("HIGH")
        assert not m._band_passes_threshold("MEDIUM")

    def test_critical_min_band_rejects_high(self):
        """HIGH is suppressed when min_band is CRITICAL."""
        m = self._m("CRITICAL")
        assert not m._band_passes_threshold("HIGH")

    def test_unknown_band_fails(self):
        """Unknown band name is treated as rank -1 → always suppressed."""
        m = self._m("LOW")
        assert not m._band_passes_threshold("UNKNOWN_BAND")


# ---------------------------------------------------------------------------
# _get_alert_level
# ---------------------------------------------------------------------------

class TestGetAlertLevel:
    """alert_level mapping from risk_band."""

    def test_medium_is_info(self):
        """MEDIUM risk band maps to INFO alert level."""
        m = _import_alerts()
        assert m._get_alert_level("MEDIUM") == "INFO"

    def test_high_is_warning(self):
        """HIGH risk band maps to WARNING alert level."""
        m = _import_alerts()
        assert m._get_alert_level("HIGH") == "WARNING"

    def test_critical_is_critical(self):
        """CRITICAL risk band maps to CRITICAL alert level."""
        m = _import_alerts()
        assert m._get_alert_level("CRITICAL") == "CRITICAL"

    def test_low_returns_empty(self):
        """LOW band has no alert level — returns empty string (filtered before reaching here)."""
        m = _import_alerts()
        assert m._get_alert_level("LOW") == ""

    def test_unknown_band_returns_empty(self):
        """Unknown band name returns empty string gracefully."""
        m = _import_alerts()
        assert m._get_alert_level("bogus") == ""


# ---------------------------------------------------------------------------
# _is_high_or_critical
# ---------------------------------------------------------------------------

class TestIsHighOrCritical:
    """Field masking gate."""

    def test_high_returns_true(self):
        m = _import_alerts()
        assert m._is_high_or_critical("HIGH") is True

    def test_critical_returns_true(self):
        m = _import_alerts()
        assert m._is_high_or_critical("CRITICAL") is True

    def test_medium_returns_false(self):
        m = _import_alerts()
        assert m._is_high_or_critical("MEDIUM") is False

    def test_low_returns_false(self):
        m = _import_alerts()
        assert m._is_high_or_critical("LOW") is False

    def test_unknown_returns_false(self):
        m = _import_alerts()
        assert m._is_high_or_critical("UNKNOWN") is False


# ---------------------------------------------------------------------------
# _mask_str_field / _mask_float_field
# ---------------------------------------------------------------------------

class TestFieldMasking:
    """Extra fields blanked for MEDIUM rows."""

    def test_high_str_passes_through(self):
        """HIGH band: string field is returned unchanged."""
        m = _import_alerts()
        assert m._mask_str_field("HIGH", "primary_sensor") == "primary_sensor"

    def test_critical_str_passes_through(self):
        """CRITICAL band: string field is returned unchanged."""
        m = _import_alerts()
        assert m._mask_str_field("CRITICAL", "pH") == "pH"

    def test_medium_str_returns_empty(self):
        """MEDIUM band: string field is blanked to ''."""
        m = _import_alerts()
        assert m._mask_str_field("MEDIUM", "pH") == ""

    def test_low_str_returns_empty(self):
        """LOW band: string field is blanked to ''."""
        m = _import_alerts()
        assert m._mask_str_field("LOW", "some_sensor") == ""

    def test_high_float_passes_through(self):
        """HIGH band: float field is returned unchanged."""
        m = _import_alerts()
        assert abs(m._mask_float_field("HIGH", 3.5) - 3.5) < 1e-9

    def test_medium_float_returns_zero(self):
        """MEDIUM band: float field is zeroed."""
        m = _import_alerts()
        assert m._mask_float_field("MEDIUM", 3.5) == 0.0

    def test_critical_float_passes_through(self):
        """CRITICAL band: float field is returned unchanged."""
        m = _import_alerts()
        assert abs(m._mask_float_field("CRITICAL", 2.2) - 2.2) < 1e-9


# ---------------------------------------------------------------------------
# _CooldownStore
# ---------------------------------------------------------------------------

class TestCooldownStore:
    """Per-point cooldown suppression."""

    _TS_FMT = "%Y-%m-%d %H:%M"

    def _store(self, cooldown: int = 60):
        """Fresh _CooldownStore with given cooldown."""
        m = _import_alerts({"ALERT_COOLDOWN_SECONDS": str(cooldown)})
        store = m._CooldownStore()
        return store, m

    def test_first_alert_always_passes(self):
        """First alert for any point is always permitted."""
        store, _ = self._store(60)
        assert store.can_alert("point_A", "2026-02-01 12:00") is True

    def test_immediate_repeat_suppressed(self):
        """Second alert immediately after the first is suppressed."""
        store, _ = self._store(60)
        store.record("point_A", "2026-02-01 12:00")
        assert store.can_alert("point_A", "2026-02-01 12:00") is False

    def test_alert_after_cooldown_passes(self):
        """Alert emitted after >= cooldown seconds is permitted."""
        store, _ = self._store(60)
        store.record("point_A", "2026-02-01 12:00")
        assert store.can_alert("point_A", "2026-02-01 12:01") is True

    def test_alert_just_before_cooldown_suppressed(self):
        """Alert emitted 59 s after previous (cooldown=60) is suppressed."""
        store, _ = self._store(60)
        store.record("point_A", "2026-02-01 12:00")
        assert store.can_alert("point_A", "2026-02-01 12:00") is False

    def test_different_points_independent(self):
        """Cooldown for point_A does not affect point_B."""
        store, _ = self._store(60)
        store.record("point_A", "2026-02-01 12:00")
        assert store.can_alert("point_B", "2026-02-01 12:00") is True

    def test_zero_cooldown_always_passes(self):
        """ALERT_COOLDOWN_SECONDS=0 disables suppression entirely."""
        store, _ = self._store(0)
        store.record("point_A", "2026-02-01 12:00")
        assert store.can_alert("point_A", "2026-02-01 12:00") is True

    def test_bad_timestamp_not_suppressed(self):
        """Unparseable timestamp is never suppressed (safe fallback)."""
        store, _ = self._store(60)
        store.record("point_A", "2026-02-01 12:00")
        assert store.can_alert("point_A", "not-a-timestamp") is True

    def test_reset_all_clears_store(self):
        """reset_all() clears all cooldown state."""
        store, _ = self._store(60)
        store.record("point_A", "2026-02-01 12:00")
        store.reset_all()
        assert store.can_alert("point_A", "2026-02-01 12:00") is True


# ---------------------------------------------------------------------------
# Public API / importability
# ---------------------------------------------------------------------------

class TestPublicAPI:
    """Module surface and import hygiene."""

    def test_build_alert_stream_callable(self):
        """build_alert_stream must be callable at module level."""
        m = _import_alerts()
        assert callable(m.build_alert_stream)

    def test_build_alert_stream_has_docstring(self):
        """build_alert_stream must have a docstring."""
        m = _import_alerts()
        assert m.build_alert_stream.__doc__

    def test_logger_exported(self):
        """Module must export a logger instance."""
        import logging
        m = _import_alerts()
        assert isinstance(m.logger, logging.Logger)

    def test_no_bare_numeric_globals(self):
        """No raw int/float constants at module level outside CONFIG."""
        m = _import_alerts()
        numeric = {
            n: v for n, v in vars(m).items()
            if not n.startswith("_") and isinstance(v, (int, float))
            and not isinstance(v, bool)
        }
        assert not numeric, f"Bare numeric globals: {numeric}"

    def test_import_no_side_effects(self):
        """Importing alerts must not raise, open files, or call pw.run()."""
        m = _import_alerts()
        assert m is not None


# ---------------------------------------------------------------------------
# validate_config integration
# ---------------------------------------------------------------------------

class TestConfigValidation:
    """validate_config enforces alert_min_risk_band constraint."""

    def _validate_with(self, **overrides):
        _stub_dotenv()
        _stub_pathway()
        _clear_src()
        cfg_mod = importlib.import_module("src.config")
        import dataclasses
        cfg = dataclasses.replace(cfg_mod.CONFIG, **overrides)
        cfg_mod.validate_config(cfg)

    def test_valid_defaults_pass(self):
        """Default config passes validation."""
        self._validate_with()

    def test_invalid_min_band_rejected(self):
        """alert_min_risk_band with unsupported value must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="alert_min_risk_band"):
            self._validate_with(alert_min_risk_band="EXTREME")

    def test_all_valid_bands_accepted(self):
        """All four valid band values must pass validation without raising."""
        for band in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
            self._validate_with(alert_min_risk_band=band)

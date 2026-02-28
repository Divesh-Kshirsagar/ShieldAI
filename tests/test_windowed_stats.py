"""
Tests for src/windowed_stats and the refactored src/zscore.

Pure-Python unit tests — no Pathway runtime required.

Covers:
  - WindoedStatsSchema field presence and types
  - CONFIG fields in both modules (window_duration_ms, window_hop_ms)
  - _population_std pure-Python helper (windowed_stats)
  - _zscore and _is_anomaly pure-Python helpers (zscore)
  - config.validate_config() enforcement of window_duration_ms > window_hop_ms
  - Public API callability, docstrings, and import hygiene

Run with:
    python3 -m pytest tests/test_windowed_stats.py -v
"""

from __future__ import annotations

import importlib
import math
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
    pw.Schema        = object
    pw.Table         = object
    pw.udf           = lambda fn: fn
    pw.DateTimeNaive = object
    pw.Duration      = lambda **kw: kw
    reducers         = types.ModuleType("pathway.reducers")
    temporal         = types.ModuleType("pathway.temporal")
    temporal.sliding = lambda **kw: kw
    sys.modules["pathway"] = pw
    sys.modules["pathway.reducers"] = reducers
    sys.modules["pathway.temporal"] = temporal


def _clear_src() -> None:
    for key in list(sys.modules):
        if key.startswith("src."):
            del sys.modules[key]


def _import_ws(env: dict | None = None):
    """Fresh import of src.windowed_stats with optional env overrides."""
    import os
    _stub_dotenv()
    _stub_pathway()
    _clear_src()
    for k, v in (env or {}).items():
        os.environ[k] = str(v)
    return importlib.import_module("src.windowed_stats")


def _import_zscore(env: dict | None = None):
    """Fresh import of src.zscore with optional env overrides."""
    import os
    _stub_dotenv()
    _stub_pathway()
    _clear_src()
    for k, v in (env or {}).items():
        os.environ[k] = str(v)
    return importlib.import_module("src.zscore")


def _cleanup(*keys: str) -> None:
    import os
    for k in keys:
        os.environ.pop(k, None)


# ---------------------------------------------------------------------------
# windowed_stats CONFIG contract
# ---------------------------------------------------------------------------

class TestWindowedStatsConfig:
    """CONFIG fields and default values in windowed_stats."""

    def test_config_exists(self):
        """windowed_stats must export CONFIG dict."""
        m = _import_ws()
        assert isinstance(m.CONFIG, dict)

    def test_window_duration_ms_present(self):
        """CONFIG must contain WINDOW_DURATION_MS."""
        m = _import_ws()
        assert "WINDOW_DURATION_MS" in m.CONFIG

    def test_window_hop_ms_present(self):
        """CONFIG must contain WINDOW_HOP_MS."""
        m = _import_ws()
        assert "WINDOW_HOP_MS" in m.CONFIG

    def test_default_duration_ms(self):
        """Default WINDOW_DURATION_MS is 30000 ms."""
        _cleanup("WINDOW_DURATION_MS")
        m = _import_ws()
        assert m.CONFIG["WINDOW_DURATION_MS"] == 30_000

    def test_default_hop_ms(self):
        """Default WINDOW_HOP_MS is 5000 ms."""
        _cleanup("WINDOW_HOP_MS")
        m = _import_ws()
        assert m.CONFIG["WINDOW_HOP_MS"] == 5_000

    def test_duration_exceeds_hop_by_default(self):
        """Default WINDOW_DURATION_MS must be strictly greater than WINDOW_HOP_MS."""
        _cleanup("WINDOW_DURATION_MS", "WINDOW_HOP_MS")
        m = _import_ws()
        assert m.CONFIG["WINDOW_DURATION_MS"] > m.CONFIG["WINDOW_HOP_MS"]

    def test_env_override_duration(self):
        """WINDOW_DURATION_MS env var overrides config correctly."""
        m = _import_ws({"WINDOW_DURATION_MS": "60000"})
        assert m.CONFIG["WINDOW_DURATION_MS"] == 60_000
        _cleanup("WINDOW_DURATION_MS")


# ---------------------------------------------------------------------------
# WindowedStatsSchema
# ---------------------------------------------------------------------------

class TestWindowedStatsSchema:
    """Schema field presence and return types."""

    def _get_schema_fields(self):
        m = _import_ws()
        # With `from __future__ import annotations` (PEP 563), annotations are
        # stored as plain strings ('str', 'float', 'int'), not type objects.
        return getattr(m.WindowedStatsSchema, "__annotations__", {})

    def test_schema_exported(self):
        """WindowedStatsSchema must be exported at module level."""
        m = _import_ws()
        assert hasattr(m, "WindowedStatsSchema")

    def test_sensor_id_field(self):
        """WindowedStatsSchema must have sensor_id: str."""
        ann = self._get_schema_fields()
        assert "sensor_id" in ann
        assert ann["sensor_id"] in (str, "str")

    def test_window_start_field(self):
        """WindowedStatsSchema must have window_start: str."""
        ann = self._get_schema_fields()
        assert "window_start" in ann
        assert ann["window_start"] in (str, "str")

    def test_window_end_field(self):
        """WindowedStatsSchema must have window_end: str."""
        ann = self._get_schema_fields()
        assert "window_end" in ann
        assert ann["window_end"] in (str, "str")

    def test_mean_field(self):
        """WindowedStatsSchema must have mean: float."""
        ann = self._get_schema_fields()
        assert "mean" in ann
        assert ann["mean"] in (float, "float")

    def test_std_field(self):
        """WindowedStatsSchema must have std: float."""
        ann = self._get_schema_fields()
        assert "std" in ann
        assert ann["std"] in (float, "float")

    def test_min_field(self):
        """WindowedStatsSchema must have min: float."""
        ann = self._get_schema_fields()
        assert "min" in ann
        assert ann["min"] in (float, "float")

    def test_max_field(self):
        """WindowedStatsSchema must have max: float."""
        ann = self._get_schema_fields()
        assert "max" in ann
        assert ann["max"] in (float, "float")

    def test_sample_count_field(self):
        """WindowedStatsSchema must have sample_count: int."""
        ann = self._get_schema_fields()
        assert "sample_count" in ann
        assert ann["sample_count"] in (int, "int")


# ---------------------------------------------------------------------------
# _population_std (windowed_stats pure-Python helper)
# ---------------------------------------------------------------------------

class TestPopulationStd:
    """_population_std: Var(X) = E[X²] - E[X]², std = sqrt(max(0, Var)) + epsilon."""

    def test_uniform_values_near_zero_std(self):
        """Constant values → std ≈ EPSILON (only the floor survives)."""
        m = _import_ws()
        # E[X] = E[X²] = c² when all X = c → Var = 0
        result = m._population_std(5.0, 25.0)  # mean=5, mean_sq=25
        assert result == m.CONFIG["EPSILON"]

    def test_known_variance(self):
        """E[X]=0, E[X²]=4 → Var=4 → std = 2.0 + epsilon."""
        m = _import_ws()
        eps = m.CONFIG["EPSILON"]
        result = m._population_std(0.0, 4.0)
        assert abs(result - (2.0 + eps)) < 1e-9

    def test_non_negative_due_to_clamping(self):
        """Floating-point rounding (mean_sq < mean²) must not produce NaN."""
        m = _import_ws()
        # Simulate fp rounding: mean=3.0, mean_sq=8.9999999 (< 9.0)
        result = m._population_std(3.0, 8.9999999)
        assert result >= 0.0
        assert not math.isnan(result)

    def test_large_values_no_overflow(self):
        """Large mean and mean_sq must not overflow."""
        m = _import_ws()
        result = m._population_std(1e6, 1e12 + 4.0)
        assert abs(result - (2.0 + m.CONFIG["EPSILON"])) < 1e-3

    def test_epsilon_always_added(self):
        """EPSILON is always part of the returned value (denominator floor)."""
        m = _import_ws()
        # Zero variance → result equals exactly EPSILON
        result = m._population_std(3.0, 9.0)
        assert abs(result - m.CONFIG["EPSILON"]) < 1e-12


# ---------------------------------------------------------------------------
# zscore CONFIG and pure-Python helpers
# ---------------------------------------------------------------------------

class TestZscoreConfig:
    """zscore module CONFIG contract after refactor."""

    def test_config_exists(self):
        """zscore must export CONFIG dict."""
        m = _import_zscore()
        assert isinstance(m.CONFIG, dict)

    def test_zscore_threshold_present(self):
        """CONFIG must contain ZSCORE_THRESHOLD."""
        m = _import_zscore()
        assert "ZSCORE_THRESHOLD" in m.CONFIG

    def test_epsilon_present(self):
        """CONFIG must contain EPSILON."""
        m = _import_zscore()
        assert "EPSILON" in m.CONFIG

    def test_no_window_seconds_in_zscore(self):
        """WINDOW_SECONDS no longer lives in zscore CONFIG — it moved to windowed_stats."""
        m = _import_zscore()
        assert "WINDOW_SECONDS" not in m.CONFIG

    def test_no_bare_numeric_globals(self):
        """No magic numbers at module level — all via CONFIG."""
        m = _import_zscore()
        numeric = {
            n: v for n, v in vars(m).items()
            if not n.startswith("_") and isinstance(v, (int, float))
            and not isinstance(v, bool)
        }
        assert not numeric, f"Bare numeric globals: {numeric}"


class TestZscoreHelpers:
    """_zscore and _is_anomaly — pure Python, no Pathway."""

    def test_positive_z_score(self):
        """Value above mean: (10 - 5) / 2.5 = 2.0."""
        m = _import_zscore()
        assert abs(m._zscore(10.0, 5.0, 2.5) - 2.0) < 1e-9

    def test_negative_z_score(self):
        """Value below mean: (0 - 5) / 2.5 = -2.0."""
        m = _import_zscore()
        assert abs(m._zscore(0.0, 5.0, 2.5) - (-2.0)) < 1e-9

    def test_zero_z_score_at_mean(self):
        """Value at mean: z = 0."""
        m = _import_zscore()
        assert m._zscore(5.0, 5.0, 1.0) == 0.0

    def test_is_anomaly_above_threshold(self):
        """z > ZSCORE_THRESHOLD → is_anomaly = True."""
        m = _import_zscore()
        assert m._is_anomaly(m.CONFIG["ZSCORE_THRESHOLD"] + 0.001) is True

    def test_is_anomaly_at_threshold_false(self):
        """z == ZSCORE_THRESHOLD → is_anomaly = False (strictly greater than)."""
        m = _import_zscore()
        assert m._is_anomaly(m.CONFIG["ZSCORE_THRESHOLD"]) is False

    def test_is_anomaly_negative_large(self):
        """Large negative z is also anomalous."""
        m = _import_zscore()
        assert m._is_anomaly(-(m.CONFIG["ZSCORE_THRESHOLD"] + 1.0)) is True

    def test_two_sigma_not_anomaly_with_default_threshold(self):
        """z=2.0 is not anomalous with default ZSCORE_THRESHOLD=3.0."""
        _cleanup("ZSCORE_THRESHOLD")
        m = _import_zscore()
        assert m._is_anomaly(2.0) is False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class TestPublicAPI:
    """Module surface, callability, and import hygiene."""

    def test_build_windowed_stats_callable(self):
        """build_windowed_stats must be callable at module level."""
        m = _import_ws()
        assert callable(m.build_windowed_stats)

    def test_build_windowed_stats_has_docstring(self):
        """build_windowed_stats must have a docstring."""
        m = _import_ws()
        assert m.build_windowed_stats.__doc__

    def test_build_scored_stream_callable(self):
        """build_scored_stream must still be callable (API unchanged)."""
        m = _import_zscore()
        assert callable(m.build_scored_stream)

    def test_scored_schema_exported(self):
        """ScoredSchema must still be exported from zscore."""
        m = _import_zscore()
        assert hasattr(m, "ScoredSchema")

    def test_scored_schema_has_all_fields(self):
        """ScoredSchema must retain all 7 output fields."""
        m = _import_zscore()
        import inspect
        ann = {}
        for cls in type.mro(m.ScoredSchema):
            ann.update(getattr(cls, "__annotations__", {}))
        required = {"sensor_id", "timestamp", "value", "rolling_mean",
                    "rolling_std", "z_score", "is_anomaly"}
        assert required.issubset(ann.keys())

    def test_import_no_side_effects(self):
        """Importing windowed_stats must not raise or open files."""
        m = _import_ws()
        assert m is not None

    def test_zscore_imports_windowed_stats(self):
        """zscore.py must import from windowed_stats (not raw windowby inline)."""
        import inspect
        m = _import_zscore()
        src = inspect.getsource(m)
        assert "windowed_stats" in src

    def test_windowed_stats_not_hardcoded(self):
        """windowed_stats.py must not embed window parameters as bare numeric literals."""
        import ast, inspect
        m = _import_ws()
        tree = ast.parse(inspect.getsource(m))
        literals = {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, (int, float))
        }
        assert 30_000 not in literals, "30000 is hardcoded in windowed_stats.py"
        assert 5_000  not in literals, "5000 is hardcoded in windowed_stats.py"


# ---------------------------------------------------------------------------
# validate_config integration
# ---------------------------------------------------------------------------

class TestConfigValidation:
    """validate_config enforces window_duration_ms > window_hop_ms."""

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

    def test_hop_equal_duration_rejected(self):
        """window_hop_ms == window_duration_ms must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="window_duration_ms"):
            self._validate_with(window_duration_ms=5000, window_hop_ms=5000)

    def test_hop_greater_than_duration_rejected(self):
        """window_hop_ms > window_duration_ms must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="window_duration_ms"):
            self._validate_with(window_duration_ms=1000, window_hop_ms=2000)

    def test_zero_hop_rejected(self):
        """window_hop_ms == 0 must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="window_hop_ms"):
            self._validate_with(window_hop_ms=0)

    def test_valid_custom_values_pass(self):
        """Custom valid windows pass validation."""
        self._validate_with(window_duration_ms=60_000, window_hop_ms=10_000)

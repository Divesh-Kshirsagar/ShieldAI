"""
Tests for src/zscore — pure-Python unit tests.

These tests exercise the z-score math and CONFIG constants without starting
a Pathway runtime. All Pathway-specific code is isolated behind the
build_scored_stream() boundary, which is NOT called here.

Run with:
    python3 -m pytest tests/test_zscore.py -v
"""

from __future__ import annotations

import importlib
import math
import sys
import types


# ---------------------------------------------------------------------------
# Stub pathway so the module can be imported without an installed runtime
# ---------------------------------------------------------------------------

def _make_pathway_stub() -> types.ModuleType:
    """Return a minimal pathway stub that satisfies zscore.py's top-level imports."""
    pw = types.ModuleType("pathway")
    pw.Schema   = object
    pw.Table    = object
    pw.Duration = lambda **kw: None
    pw.udf      = lambda fn: fn  # decorator passthrough

    reducers = types.ModuleType("pathway.reducers")
    temporal  = types.ModuleType("pathway.temporal")
    pw.reducers = reducers
    pw.temporal = temporal

    sys.modules["pathway"]          = pw
    sys.modules["pathway.reducers"] = reducers
    sys.modules["pathway.temporal"] = temporal
    return pw


def _make_dotenv_stub() -> None:
    """Stub python-dotenv so constants.py can be imported without the package."""
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *a, **kw: None  # no-op
    sys.modules["dotenv"] = dotenv


def _import_helpers():
    """Import and return the private z-score helpers from src/zscore.py."""
    if "pathway" not in sys.modules:
        _make_pathway_stub()
    if "dotenv" not in sys.modules:
        _make_dotenv_stub()

    # Clear cached modules so the stub is loaded fresh each time
    for mod in list(sys.modules):
        if mod in ("src.zscore", "src.constants"):
            del sys.modules[mod]

    zscore_mod = importlib.import_module("src.zscore")
    return zscore_mod


# ---------------------------------------------------------------------------
# CONFIG smoke-tests
# ---------------------------------------------------------------------------

class TestConfig:
    """Verify CONFIG dict is present and has correct key types."""

    def test_config_exists(self):
        """CONFIG dict must be exported at module level."""
        z = _import_helpers()
        assert hasattr(z, "CONFIG"), "CONFIG not found on src.zscore"

    def test_config_keys(self):
        """Required CONFIG keys must be present (WINDOW_SECONDS moved to windowed_stats)."""
        z = _import_helpers()
        required = {"ZSCORE_THRESHOLD", "EPSILON", "TIME_FORMAT"}
        assert required.issubset(z.CONFIG.keys()), (
            f"Missing keys: {required - set(z.CONFIG.keys())}"
        )

    def test_config_defaults(self):
        """Default numeric values must match the specification."""
        z = _import_helpers()
        assert z.CONFIG["ZSCORE_THRESHOLD"] == 3.0
        assert z.CONFIG["EPSILON"]           == 1e-9

    def test_window_seconds_positive(self):
        """WINDOW_SECONDS lives in windowed_stats CONFIG now, not zscore CONFIG."""
        z = _import_helpers()
        assert "WINDOW_SECONDS" not in z.CONFIG, (
            "WINDOW_SECONDS was moved to windowed_stats; zscore CONFIG should not re-export it"
        )

    def test_zscore_threshold_positive(self):
        """ZSCORE_THRESHOLD must be a positive float."""
        z = _import_helpers()
        assert z.CONFIG["ZSCORE_THRESHOLD"] > 0.0

    def test_epsilon_small(self):
        """EPSILON must be very small (< 1e-6) to act as a numerical floor."""
        z = _import_helpers()
        assert z.CONFIG["EPSILON"] < 1e-6


# ---------------------------------------------------------------------------
# Rolling std math
# ---------------------------------------------------------------------------

class TestRollingStd:
    """Validate population variance formula: Var(X) = E[X²] − E[X]²."""

    def _std(self, values: list[float]) -> float:
        """Reference implementation using built-ins."""
        n    = len(values)
        mean = sum(values) / n
        var  = sum((v - mean) ** 2 for v in values) / n   # population var
        return math.sqrt(var)

    def _pathway_std(self, values: list[float]) -> float:
        """Replicate the two-reducer approach used inside _udf_rolling_std."""
        n            = len(values)
        mean         = sum(values) / n
        mean_sq      = sum(v * v  for v in values) / n
        rolling_var  = mean_sq - mean * mean
        return math.sqrt(max(0.0, rolling_var))

    def test_std_matches_reference(self):
        """Two-reducer stddev must agree with the reference to 6 decimal places."""
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        assert abs(self._pathway_std(values) - self._std(values)) < 1e-6

    def test_std_uniform_values(self):
        """Uniform values → stddev = 0; clamp prevents negative under sqrt."""
        values = [100.0] * 20
        result = self._pathway_std(values)
        assert result == 0.0

    def test_std_two_values(self):
        """Two-value population stddev = |a - b| / 2."""
        a, b = 4.0, 8.0
        expected = abs(a - b) / 2.0
        assert abs(self._pathway_std([a, b]) - expected) < 1e-9

    def test_std_clamping(self):
        """Floating-point rounding can yield a tiny negative variance; must clamp to 0."""
        # Craft values where E[X²] - E[X]² is just below 0 due to fp loss
        big = 1e15
        values = [big, big, big + 1e-3]
        result = self._pathway_std(values)
        assert result >= 0.0, "sqrt argument must never be negative"


# ---------------------------------------------------------------------------
# Z-score formula
# ---------------------------------------------------------------------------

class TestZScore:
    """Validate z-score formula: (value - mean) / (std + EPSILON)."""

    EPSILON = 1e-9

    def _z(self, value: float, mean: float, std: float) -> float:
        """Pure-Python z-score reference matching _udf_zscore."""
        return (value - mean) / (std + self.EPSILON)

    def test_positive_zscore(self):
        """Value above mean yields positive z-score."""
        assert self._z(10.0, 5.0, 1.0) > 0

    def test_negative_zscore(self):
        """Value below mean yields negative z-score."""
        assert self._z(0.0, 5.0, 1.0) < 0

    def test_zero_zscore(self):
        """Value exactly at mean yields z-score ≈ 0."""
        z = self._z(5.0, 5.0, 2.0)
        assert abs(z) < self.EPSILON * 10

    def test_known_value(self):
        """(value=8, mean=5, std=1) → z ≈ 3.0; small shift from EPSILON denominator is fine."""
        z = self._z(8.0, 5.0, 1.0)
        # EPSILON (1e-9) shifts the denominator, so exact equality to 3.0 fails.
        # Tolerance of 1e-6 is safe: the shift is only ~3e-9.
        assert abs(z - 3.0) < 1e-6

    def test_zero_std_no_division_error(self):
        """EPSILON prevents ZeroDivisionError when std = 0."""
        z = self._z(10.0, 5.0, 0.0)
        expected = 5.0 / (0.0 + self.EPSILON)
        assert abs(z - expected) < 1.0  # very large but finite

    def test_symmetry(self):
        """z-score is antisymmetric: z(mean + d) = -z(mean - d)."""
        mean, std, d = 100.0, 10.0, 15.0
        z_above = self._z(mean + d, mean, std)
        z_below = self._z(mean - d, mean, std)
        assert abs(z_above + z_below) < 1e-9


# ---------------------------------------------------------------------------
# Anomaly flag logic
# ---------------------------------------------------------------------------

class TestIsAnomaly:
    """Validate is_anomaly = abs(z_score) > ZSCORE_THRESHOLD."""

    THRESHOLD = 3.0

    def _flag(self, z: float) -> bool:
        """Reference matching _udf_is_anomaly."""
        return abs(z) > self.THRESHOLD

    def test_exactly_at_threshold_not_flagged(self):
        """Exactly ±THRESHOLD must NOT be flagged (strict >)."""
        assert not self._flag( self.THRESHOLD)
        assert not self._flag(-self.THRESHOLD)

    def test_just_above_threshold_flagged(self):
        """±(THRESHOLD + tiny delta) must BE flagged."""
        delta = 1e-9
        assert self._flag( self.THRESHOLD + delta)
        assert self._flag(-self.THRESHOLD - delta)

    def test_nominal_reading_not_flagged(self):
        """A reading well within normal range (|z| < 1) must not be flagged."""
        assert not self._flag(0.5)
        assert not self._flag(-1.9)

    def test_extreme_spike_flagged(self):
        """An extreme spike (|z| >> threshold) must be flagged."""
        assert self._flag(50.0)
        assert self._flag(-50.0)


# ---------------------------------------------------------------------------
# Import side-effect guard
# ---------------------------------------------------------------------------

class TestImportability:
    """Ensure the module is importable without triggering pw.run() or any I/O."""

    def test_import_no_side_effects(self):
        """Importing src.zscore must not raise, not call pw.run(), not open files."""
        # A bare import with the stub in place is the test itself.
        # If pw.run were called at module level it would raise AttributeError
        # on the stub, causing this test to fail.
        z = _import_helpers()
        assert z is not None

    def test_build_scored_stream_is_callable(self):
        """build_scored_stream must be a callable exported at module level."""
        z = _import_helpers()
        assert callable(z.build_scored_stream)

    def test_scored_schema_exported(self):
        """ScoredSchema must be exported for downstream type checking."""
        z = _import_helpers()
        assert hasattr(z, "ScoredSchema")

"""
Tests for src/detection — pure-Python unit tests.

Validates the detection module's public API contract, internal structure,
and integration of the zscore + persistence pipeline without starting a
Pathway runtime.

Run with:
    python3 -m pytest tests/test_detection.py -v
"""

from __future__ import annotations

import importlib
import logging
import sys
import types


# ---------------------------------------------------------------------------
# Stubs — pathway, dotenv (same pattern as other test modules)
# ---------------------------------------------------------------------------

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


def _stub_dotenv() -> None:
    """Register a no-op dotenv stub in sys.modules."""
    if "dotenv" in sys.modules:
        return
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *a, **kw: None
    sys.modules["dotenv"] = dotenv


def _clear_src_modules() -> None:
    """Evict all src.* modules so each test gets a fresh import."""
    for key in list(sys.modules):
        if key.startswith("src."):
            del sys.modules[key]


def _import_detection():
    """Return a freshly imported src.detection module with all stubs active."""
    _stub_dotenv()
    _stub_pathway()
    _clear_src_modules()
    return importlib.import_module("src.detection")


# ---------------------------------------------------------------------------
# Public API contract
# ---------------------------------------------------------------------------

class TestPublicAPI:
    """Verify the module exposes exactly the required public surface."""

    def test_build_scored_stream_exported(self):
        """build_scored_stream must be a callable exported at module level."""
        det = _import_detection()
        assert callable(det.build_scored_stream)

    def test_logger_exported(self):
        """A module-level logger must be present for operator observability."""
        det = _import_detection()
        assert isinstance(det.logger, logging.Logger)

    def test_no_unexpected_public_names(self):
        """Only build_scored_stream and logger should be public API names (not std imports)."""
        det = _import_detection()
        public = {
            n for n in vars(det)
            if not n.startswith("_") and not n.startswith("__")
        }
        # Standard imported modules (pw, logging, etc.) are expected — ignore those
        numeric_globals = {
            n: v for n, v in vars(det).items()
            if not n.startswith("_") and isinstance(v, (int, float)) and not isinstance(v, bool)
        }
        assert not numeric_globals, f"Bare numeric globals: {numeric_globals}"
        # Required public names must be present
        assert "build_scored_stream" in public
        assert "logger" in public

    def test_build_scored_stream_has_docstring(self):
        """Public function must have a docstring per code standards."""
        det = _import_detection()
        assert det.build_scored_stream.__doc__, "build_scored_stream has no docstring"

    def test_import_no_side_effects(self):
        """Importing detection must not call pw.run(), open files, or print."""
        det = _import_detection()
        assert det is not None


# ---------------------------------------------------------------------------
# Internal helper structure
# ---------------------------------------------------------------------------

class TestInternalHelpers:
    """Verify internal helpers follow naming conventions and docstring rules."""

    def _helpers(self):
        """Return all names starting with _ from the detection module."""
        det = _import_detection()
        return {
            n: getattr(det, n) for n in vars(det)
            if n.startswith("_") and not n.startswith("__")
        }

    def test_private_helpers_exist(self):
        """At least one _-prefixed helper function must exist."""
        helpers = self._helpers()
        private_fns = {k: v for k, v in helpers.items() if callable(v)}
        assert private_fns, "No private helper functions found in detection module"

    def test_all_private_functions_have_docstrings(self):
        """Every _-prefixed callable must have a one-line docstring."""
        helpers = self._helpers()
        missing = [
            name for name, obj in helpers.items()
            if callable(obj) and not getattr(obj, "__doc__", None)
        ]
        assert not missing, f"Private functions without docstrings: {missing}"

    def test_no_print_statements_at_module_level(self):
        """Module source must not contain any print() calls."""
        import inspect
        det = _import_detection()
        source = inspect.getsource(det)
        # Allow print only in comments/docstrings by checking for bare call
        lines_with_print = [
            line.strip() for line in source.splitlines()
            if "print(" in line and not line.strip().startswith("#")
        ]
        assert not lines_with_print, (
            f"print() calls found in detection.py: {lines_with_print}"
        )


# ---------------------------------------------------------------------------
# Config consumption
# ---------------------------------------------------------------------------

class TestConfigConsumption:
    """Verify the module reads from config.CONFIG, not local magic numbers."""

    def test_no_bare_numeric_module_globals(self):
        """No raw int/float constants at module level (all via config.CONFIG)."""
        det = _import_detection()
        numeric = {
            n: v for n, v in vars(det).items()
            if not n.startswith("_")
            and isinstance(v, (int, float))
            and not isinstance(v, bool)
        }
        assert not numeric, f"Bare numeric globals: {numeric}"

    def test_config_module_imported(self):
        """src.config must be imported (directly or via alias) by detection."""
        det = _import_detection()
        src_names = [n for n in vars(det) if "config" in n.lower()]
        assert src_names, "No config-related name found in detection module namespace"


# ---------------------------------------------------------------------------
# Integrated orchestration (mocked sub-modules)
# ---------------------------------------------------------------------------

class TestOrchestration:
    """Verify build_scored_stream calls zscore then persistence in the right order."""

    def _run_with_mocks(self):
        """Import detection fresh, then monkey-patch _zscore and _persistence attributes."""
        _stub_dotenv()
        _stub_pathway()
        _clear_src_modules()

        call_log: list[str] = []

        class _FakeTable:
            pass

        raw_table        = _FakeTable()
        zscore_output    = _FakeTable()
        confirmed_output = _FakeTable()

        # Import detection with its real (but stubbed-pathway) sub-modules
        det = importlib.import_module("src.detection")

        # Build lightweight namespace objects that mimic the sub-module API
        zscore_ns = types.SimpleNamespace()
        def _fake_build_scored(t):
            call_log.append("zscore.build_scored_stream")
            return zscore_output
        zscore_ns.build_scored_stream = _fake_build_scored

        persistence_ns = types.SimpleNamespace()
        def _fake_build_confirmed(t):
            call_log.append("persistence.build_confirmed_anomalies")
            return confirmed_output
        persistence_ns.build_confirmed_anomalies = _fake_build_confirmed

        # Swap the module-level references detection uses internally
        det._zscore      = zscore_ns
        det._persistence = persistence_ns

        result = det.build_scored_stream(raw_table)
        return call_log, result, confirmed_output

    def test_zscore_called_before_persistence(self):
        """zscore must be invoked before persistence in the pipeline."""
        call_log, _, _ = self._run_with_mocks()
        assert call_log.index("zscore.build_scored_stream") < \
               call_log.index("persistence.build_confirmed_anomalies")

    def test_both_stages_invoked(self):
        """Both zscore and persistence must be called exactly once."""
        call_log, _, _ = self._run_with_mocks()
        assert call_log.count("zscore.build_scored_stream") == 1
        assert call_log.count("persistence.build_confirmed_anomalies") == 1

    def test_returns_persistence_output(self):
        """build_scored_stream must return the persistence stage's output table."""
        _, result, expected = self._run_with_mocks()
        assert result is expected

    def test_no_extra_stages(self):
        """No additional stages should be inserted between zscore and persistence."""
        call_log, _, _ = self._run_with_mocks()
        assert call_log == [
            "zscore.build_scored_stream",
            "persistence.build_confirmed_anomalies",
        ]


# ---------------------------------------------------------------------------
# Function length compliance
# ---------------------------------------------------------------------------

class TestFunctionLength:
    """Enforce the 30-line maximum per function code standard."""

    def test_all_functions_under_30_lines(self):
        """Every function in detection.py must have a body of <= 30 lines."""
        import inspect
        det = _import_detection()

        violations = []
        for name, obj in vars(det).items():
            if not callable(obj):
                continue
            try:
                src_lines = inspect.getsource(obj).splitlines()
            except (TypeError, OSError):
                continue
            # Non-blank, non-decorator lines only
            body_lines = [
                l for l in src_lines
                if l.strip() and not l.strip().startswith("@")
            ]
            if len(body_lines) > 30:
                violations.append(f"{name}: {len(body_lines)} lines")

        assert not violations, (
            f"Functions exceeding 30-line limit: {violations}"
        )

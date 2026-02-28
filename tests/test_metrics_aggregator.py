"""
Tests for src/metrics_aggregator.py — real-time KPIs and atomic JSON sink.
"""

import json
import os
import time
from pathlib import Path
import pytest
from src.metrics_aggregator import compute_uptime, _write_metrics_json
from src.config import CONFIG as _cfg


def test_compute_uptime():
    # Uptime should be > some small value since we just loaded the module
    uptime = compute_uptime()
    assert uptime >= 0
    time.sleep(0.1)
    assert compute_uptime() > uptime


def test_atomic_json_sink(tmp_path):
    # Override config output path for testing
    test_json = tmp_path / "metrics.json"
    _cfg_dict = _cfg.__dict__.copy()
    _cfg_dict["metrics_output_path"] = str(test_json)
    
    # We need to manually monkeypatch CONFIG because it's a frozen dataclass
    # or just use the fact that _write_metrics_json uses _cfg which is the singleton.
    # Since _cfg is a singleton, we can't easily swap it if it's frozen.
    # But we can monkeypatch the module-level reference.
    
    import src.metrics_aggregator
    original_cfg = src.metrics_aggregator._cfg
    
    from dataclasses import replace
    mock_cfg = replace(_cfg, metrics_output_path=str(test_json))
    src.metrics_aggregator._cfg = mock_cfg
    
    try:
        sample_data = {"events": 100, "uptime": 10.5}
        _write_metrics_json(sample_data)
        
        assert test_json.exists()
        with open(test_json) as f:
            data = json.load(f)
            assert data == sample_data
            
        # Verify atomicity (no partial writes)
        # In a test, we just check it writes successfully.
    finally:
        src.metrics_aggregator._cfg = original_cfg


def test_risk_map_lookup():
    from src.metrics_aggregator import _RISK_MAP, _INV_RISK_MAP
    assert _RISK_MAP["CRITICAL"] == 4
    assert _INV_RISK_MAP[3] == "HIGH"

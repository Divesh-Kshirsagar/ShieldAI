"""
Tests for src/validation — input validation and schema enforcement.

Covers:
  - Required field presence
  - sensor_id length and type
  - numeric finiteness (NaN, Inf)
  - timestamp format (ISO 8601, epoch)
  - SENSOR_VALUE_RANGE pattern matching
"""

import math
import pytest
from src.validation import validate_record
from src.config import CONFIG as _cfg


def test_missing_fields():
    assert validate_record({}) == (False, "missing 'sensor_id'")
    assert validate_record({"sensor_id": "s1"}) == (False, "missing 'timestamp'")
    assert validate_record({"sensor_id": "s1", "timestamp": "2026-02-28 12:00"}) == (False, "missing 'value'")


def test_invalid_sensor_id():
    assert validate_record({"sensor_id": "", "timestamp": "2026-02-28", "value": 10}) == (False, "invalid 'sensor_id type/content: str")
    assert validate_record({"sensor_id": 123, "timestamp": "2026-02-28", "value": 10}) == (False, "invalid 'sensor_id type/content: int")
    
    long_id = "a" * (_cfg.max_sensor_id_length + 1)
    assert validate_record({"sensor_id": long_id, "timestamp": "2026-02-28", "value": 10}) == (False, f"sensor_id exceeds max length ({len(long_id)} > {_cfg.max_sensor_id_length})")


def test_invalid_value():
    assert validate_record({"sensor_id": "s1", "timestamp": "2026-02-28", "value": "high"}) == (False, "value must be numeric (got str)")
    assert validate_record({"sensor_id": "s1", "timestamp": "2026-02-28", "value": float('nan')}) == (False, "value must be finite (got nan)")
    assert validate_record({"sensor_id": "s1", "timestamp": "2026-02-28", "value": float('inf')}) == (False, "value must be finite (got inf)")


def test_valid_timestamp_formats():
    # ISO 8601
    assert validate_record({"sensor_id": "s1", "timestamp": "2026-02-28T12:00:00Z", "value": 10})[0] is True
    assert validate_record({"sensor_id": "s1", "timestamp": "2026-02-28 12:00", "value": 10})[0] is True
    # Unix epoch
    assert validate_record({"sensor_id": "s1", "timestamp": 1740743600.0, "value": 10})[0] is True
    assert validate_record({"sensor_id": "s1", "timestamp": "1740743600", "value": 10})[0] is True


def test_invalid_timestamp_format():
    assert validate_record({"sensor_id": "s1", "timestamp": "invalid date", "value": 10}) == (False, "invalid 'timestamp' format: 'invalid date'")


def test_sensor_value_range_ph():
    # pH range is [0.0, 14.0] (matched via *ph*)
    assert validate_record({"sensor_id": "ph_sensor_1", "timestamp": "2026-02-28", "value": 7.0})[0] is True
    assert validate_record({"sensor_id": "ph_sensor_1", "timestamp": "2026-02-28", "value": -1.0}) == (False, "value -1.0 out of range [0.0, 14.0] for pattern '*ph*'")
    assert validate_record({"sensor_id": "ph_sensor_1", "timestamp": "2026-02-28", "value": 15.0}) == (False, "value 15.0 out of range [0.0, 14.0] for pattern '*ph*'")


def test_sensor_value_range_turbidity():
    # turbidity range is [0.0, 1000.0] (matched via *turbidity*)
    assert validate_record({"sensor_id": "turbidity_1", "timestamp": "2026-02-28", "value": 500.0})[0] is True
    assert validate_record({"sensor_id": "turbidity_1", "timestamp": "2026-02-28", "value": 1001.0}) == (False, "value 1001.0 out of range [0.0, 1000.0] for pattern '*turbidity*'")


def test_sensor_value_range_flow():
    # flow range is [0.0, 10000.0] (matched via *flow*)
    assert validate_record({"sensor_id": "flow_meter_A", "timestamp": "2026-02-28", "value": 9000.0})[0] is True
    assert validate_record({"sensor_id": "flow_meter_A", "timestamp": "2026-02-28", "value": 10001.0}) == (False, "value 10001.0 out of range [0.0, 10000.0] for pattern '*flow*'")


def test_default_range_star():
    # * range is [-1e9, 1e9]
    assert validate_record({"sensor_id": "random_sensor", "timestamp": "2026-02-28", "value": 1e6})[0] is True
    assert validate_record({"sensor_id": "random_sensor", "timestamp": "2026-02-28", "value": 2e9}) == (False, "value 2000000000.0 out of range [-1000000000.0, 1000000000.0] for pattern '*'")


def test_valid_record_minimal():
    is_valid, reason = validate_record({"sensor_id": "S1", "timestamp": "2026-02-28", "value": 100})
    assert is_valid is True
    assert reason == ""

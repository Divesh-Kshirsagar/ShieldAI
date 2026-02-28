"""
SHIELD AI — Input Validation & Schema Enforcement
=================================================

Pure-Python module for validating raw sensor records before they enter the
Pathway pipeline.

Validation Rules:
-----------------
1. Required fields: 'sensor_id', 'timestamp', 'value'.
2. 'sensor_id': Must be a non-empty string, max length CONFIG.max_sensor_id_length.
3. 'timestamp': Must be ISO 8601 string or a Unix epoch float.
4. 'value': Must be numeric (int/float), not NaN or Inf.
5. Value Range: Must be within SENSOR_VALUE_RANGE (via fnmatch pattern matching).

Usage:
------
    from src.validation import validate_record
    is_valid, reason = validate_record(record_dict)
"""

from __future__ import annotations

import datetime
import fnmatch
import math
from typing import Any

from src.config import CONFIG as _cfg


def validate_record(record: dict[str, Any]) -> tuple[bool, str]:
    """Validate a single sensor record against industrial safety rules.

    Returns:
        (True, "") if the record is valid.
        (False, "reason") if the record is rejected.
    """
    # 1. Field presence checks
    if record.get("sensor_id") is None:
        return False, "missing 'sensor_id'"
    if record.get("timestamp") is None:
        return False, "missing 'timestamp'"
    if record.get("value") is None:
        return False, "missing 'value'"

    sensor_id = record["sensor_id"]
    timestamp = record["timestamp"]
    value     = record["value"]

    # 2. sensor_id validation
    if not isinstance(sensor_id, str) or not sensor_id.strip():
        return False, f"invalid 'sensor_id type/content: {type(sensor_id).__name__}"
    if len(sensor_id) > _cfg.max_sensor_id_length:
        return False, f"sensor_id exceeds max length ({len(sensor_id)} > {_cfg.max_sensor_id_length})"

    # 3. value validation (numeric, finite)
    if not isinstance(value, (int, float)):
        return False, f"value must be numeric (got {type(value).__name__})"
    if not math.isfinite(value):
        return False, f"value must be finite (got {value})"

    # 4. timestamp validation (ISO 8601 or Unix epoch)
    valid_ts = False
    if isinstance(timestamp, (int, float)):
        valid_ts = True
    elif isinstance(timestamp, str):
        # Try ISO 8601
        try:
            datetime.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            valid_ts = True
        except ValueError:
            # Try parsing as float (Unix epoch string)
            try:
                float(timestamp)
                valid_ts = True
            except ValueError:
                pass

    if not valid_ts:
        return False, f"invalid 'timestamp' format: {timestamp!r}"

    # 5. SENSOR_VALUE_RANGE enforcement
    matched_range = None
    # We iterate through patterns and pick the first match (e.g. "ph" before "*")
    # Sort keys to ensure deterministic matching (alphabetical normally, but "*" is usually last in config)
    for pattern, bounds in _cfg.sensor_value_range.items():
        if fnmatch.fnmatch(sensor_id, pattern):
            matched_range = bounds
            break

    if matched_range:
        v_min, v_max = matched_range
        if not (v_min <= float(value) <= v_max):
            return False, f"value {value} out of range [{v_min}, {v_max}] for pattern {pattern!r}"

    return True, ""

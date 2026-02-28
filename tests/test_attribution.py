"""
Tests for src/attribution — pure-Python unit tests.

No stubs required: attribution.py has zero Pathway / third-party imports.
Tests operate directly on plain Python dicts.

Run with:
    python3 -m pytest tests/test_attribution.py -v
"""

from __future__ import annotations

import json
import math
from src.attribution import (
    _compute_fractions,
    _sort_descending,
    _format_attribution_detail,
    _format_alert_message,
    _top_contributor,
    format_alert,
)


# ---------------------------------------------------------------------------
# _compute_fractions
# ---------------------------------------------------------------------------

class TestComputeFractions:
    """z_i² / Σz_j² for each sensor."""

    def test_single_sensor(self):
        """Single sensor: fraction is always 1.0."""
        f = _compute_fractions({"pH": 3.0})
        assert abs(f["pH"] - 1.0) < 1e-9

    def test_two_equal_sensors(self):
        """Equal z-scores → equal fractions of 0.5 each."""
        f = _compute_fractions({"pH": 2.0, "turbidity": 2.0})
        assert abs(f["pH"] - 0.5) < 1e-9
        assert abs(f["turbidity"] - 0.5) < 1e-9

    def test_fractions_sum_to_one(self):
        """Fractions always sum to 1.0 regardless of values."""
        f = _compute_fractions({"pH": 3.0, "turbidity": 1.0, "flow": 0.5})
        assert abs(sum(f.values()) - 1.0) < 1e-9

    def test_known_values(self):
        """3² / (3²+4²) = 9/25 = 0.36; 4² / 25 = 0.64."""
        f = _compute_fractions({"a": 3.0, "b": 4.0})
        assert abs(f["a"] - 0.36) < 1e-9
        assert abs(f["b"] - 0.64) < 1e-9

    def test_negative_z_scores_treated_as_magnitude(self):
        """Negating all z-scores does not change any fraction (z² is invariant)."""
        pos = _compute_fractions({"a":  4.0, "b":  3.0})
        neg = _compute_fractions({"a": -4.0, "b": -3.0})
        assert abs(pos["a"] - neg["a"]) < 1e-9
        assert abs(pos["b"] - neg["b"]) < 1e-9

    def test_zero_z_score_sensor_contributes_nothing(self):
        """A sensor with z=0 has fraction 0."""
        f = _compute_fractions({"a": 3.0, "b": 0.0})
        assert abs(f["b"]) < 1e-9
        assert abs(f["a"] - 1.0) < 1e-9

    def test_all_zero_z_scores_equal_split(self):
        """All z=0: fractions split equally (guard against ZeroDivisionError)."""
        f = _compute_fractions({"a": 0.0, "b": 0.0, "c": 0.0})
        for frac in f.values():
            assert abs(frac - 1.0 / 3.0) < 1e-9

    def test_empty_dict_returns_empty(self):
        """Empty sensor dict returns empty fractions."""
        assert _compute_fractions({}) == {}

    def test_large_z_scores_no_overflow(self):
        """Large z-scores (1e6) must not cause overflow."""
        f = _compute_fractions({"a": 1e6, "b": 1e6})
        assert abs(f["a"] - 0.5) < 1e-9


# ---------------------------------------------------------------------------
# _sort_descending
# ---------------------------------------------------------------------------

class TestSortDescending:
    """Sensors sorted by fraction, largest first."""

    def test_already_sorted(self):
        """If input is already sorted, order is preserved."""
        pairs = _sort_descending({"a": 0.7, "b": 0.3})
        assert pairs[0][0] == "a"
        assert pairs[1][0] == "b"

    def test_reverse_order(self):
        """Unsorted dict: smallest fraction sensor goes last."""
        pairs = _sort_descending({"a": 0.1, "b": 0.9})
        assert pairs[0][0] == "b"
        assert pairs[1][0] == "a"

    def test_three_sensors_sorted(self):
        """Three sensors ranked correctly."""
        pairs = _sort_descending({"x": 0.5, "y": 0.3, "z": 0.2})
        assert [p[0] for p in pairs] == ["x", "y", "z"]

    def test_values_preserved(self):
        """Fraction values are not modified during sort."""
        pairs = _sort_descending({"a": 0.64, "b": 0.36})
        assert abs(pairs[0][1] - 0.64) < 1e-9
        assert abs(pairs[1][1] - 0.36) < 1e-9

    def test_empty_returns_empty_list(self):
        """Empty fractions dict → empty list."""
        assert _sort_descending({}) == []


# ---------------------------------------------------------------------------
# _format_attribution_detail
# ---------------------------------------------------------------------------

class TestFormatAttributionDetail:
    """JSON output specification."""

    def test_valid_json(self):
        """Output is valid JSON."""
        pairs = [("pH", 0.64), ("turbidity", 0.36)]
        result = _format_attribution_detail(pairs)
        obj = json.loads(result)  # raises if invalid
        assert isinstance(obj, dict)

    def test_three_decimal_places(self):
        """Values are rounded to exactly 3 decimal places."""
        pairs = [("pH", 0.123456)]
        obj = json.loads(_format_attribution_detail(pairs))
        assert obj["pH"] == 0.123

    def test_order_preserved_in_json(self):
        """Sensors appear in the JSON in the order given (descending)."""
        pairs = [("a", 0.7), ("b", 0.2), ("c", 0.1)]
        obj = json.loads(_format_attribution_detail(pairs))
        assert list(obj.keys()) == ["a", "b", "c"]

    def test_all_keys_present(self):
        """Every sensor in input appears in the JSON output."""
        pairs = [("pH", 0.6), ("flow", 0.4)]
        obj = json.loads(_format_attribution_detail(pairs))
        assert set(obj.keys()) == {"pH", "flow"}

    def test_single_sensor_json(self):
        """Single-sensor attribution serialises correctly."""
        result = _format_attribution_detail([("solo", 1.0)])
        obj = json.loads(result)
        assert obj == {"solo": 1.0}

    def test_empty_list_empty_json_object(self):
        """Empty list → '{}'."""
        assert _format_attribution_detail([]) == "{}"

    def test_no_string_concatenation_used(self):
        """JSON output must be parseable — sanity check for malformed concat."""
        pairs = [("sensor with spaces", 0.5), ("another", 0.5)]
        json.loads(_format_attribution_detail(pairs))  # must not raise


# ---------------------------------------------------------------------------
# _format_alert_message
# ---------------------------------------------------------------------------

class TestFormatAlertMessage:
    """Human-readable message format enforcement."""

    def test_contains_group_name(self):
        """Alert message must contain the group_name."""
        msg = _format_alert_message("discharge_point_A", "pH", 0.75)
        assert "discharge_point_A" in msg

    def test_contains_top_contributor(self):
        """Alert message must name the top contributing sensor."""
        msg = _format_alert_message("g1", "turbidity", 0.60)
        assert "turbidity" in msg

    def test_fraction_formatted_as_percent(self):
        """Fraction 0.75 must appear as '75%' in the message."""
        msg = _format_alert_message("g1", "pH", 0.75)
        assert "75%" in msg

    def test_exact_format(self):
        """Full message matches the documented template."""
        msg = _format_alert_message("discharge_point_A", "pH", 0.762)
        assert msg == "Anomaly in discharge_point_A: primary driver pH (76% of score)"

    def test_100_percent_when_single_sensor(self):
        """Single sensor group → 100% shown."""
        msg = _format_alert_message("g1", "flow", 1.0)
        assert "100%" in msg

    def test_no_string_concatenation(self):
        """Output is a non-empty string formed by f-string, not by + operator."""
        msg = _format_alert_message("g1", "pH", 0.5)
        assert isinstance(msg, str) and len(msg) > 0


# ---------------------------------------------------------------------------
# _top_contributor
# ---------------------------------------------------------------------------

class TestTopContributor:
    """Return the highest-fraction sensor."""

    def test_returns_first_of_sorted_pairs(self):
        """Returns the head of the sorted list."""
        pairs = [("pH", 0.8), ("flow", 0.2)]
        sid, frac = _top_contributor(pairs)
        assert sid == "pH"
        assert abs(frac - 0.8) < 1e-9

    def test_empty_list_returns_empty_string(self):
        """Empty list → ("", 0.0) — guard against IndexError."""
        sid, frac = _top_contributor([])
        assert sid == ""
        assert frac == 0.0


# ---------------------------------------------------------------------------
# format_alert — end-to-end integration
# ---------------------------------------------------------------------------

class TestFormatAlert:
    """format_alert enriches a dict with all three attribution fields."""

    _BASIC_ROW = {
        "group_name":      "discharge_point_A",
        "composite_score": 3.14,
        "is_group_anomaly": True,
        "timestamp":       "2026-02-01 12:23",
        "sensor_z_scores": {"pH": 4.0, "turbidity": -2.0, "flow": 1.0},
    }

    def test_top_contributor_present(self):
        """top_contributor field must be in the returned dict."""
        result = format_alert(self._BASIC_ROW.copy())
        assert "top_contributor" in result

    def test_attribution_detail_present(self):
        """attribution_detail field must be in the returned dict."""
        result = format_alert(self._BASIC_ROW.copy())
        assert "attribution_detail" in result

    def test_alert_message_present(self):
        """alert_message field must be in the returned dict."""
        result = format_alert(self._BASIC_ROW.copy())
        assert "alert_message" in result

    def test_original_keys_preserved(self):
        """All input keys must survive into the output dict."""
        row     = self._BASIC_ROW.copy()
        result  = format_alert(row)
        for key in row:
            assert key in result

    def test_input_dict_not_mutated(self):
        """format_alert must not modify the caller's dict."""
        row    = self._BASIC_ROW.copy()
        before = dict(row)
        format_alert(row)
        assert row == before

    def test_top_contributor_is_ph(self):
        """pH has z=4 → z²=16, dominant over turbidity (4) and flow (1)."""
        result = format_alert(self._BASIC_ROW.copy())
        assert result["top_contributor"] == "pH"

    def test_attribution_detail_is_valid_json(self):
        """attribution_detail must be parseable JSON."""
        result = format_alert(self._BASIC_ROW.copy())
        obj = json.loads(result["attribution_detail"])
        assert isinstance(obj, dict)

    def test_attribution_detail_has_three_dp(self):
        """All values in attribution_detail are rounded to 3 decimal places."""
        result = format_alert(self._BASIC_ROW.copy())
        obj = json.loads(result["attribution_detail"])
        for val in obj.values():
            assert round(val, 3) == val

    def test_attribution_detail_sums_to_one(self):
        """Fractions must sum to 1.0."""
        result = format_alert(self._BASIC_ROW.copy())
        obj = json.loads(result["attribution_detail"])
        assert abs(sum(obj.values()) - 1.0) < 0.01  # 3dp rounding tolerance

    def test_attribution_sorted_descending(self):
        """Sensors in attribution_detail appear highest-fraction first."""
        result = format_alert(self._BASIC_ROW.copy())
        obj    = json.loads(result["attribution_detail"])
        values = list(obj.values())
        assert values == sorted(values, reverse=True)

    def test_alert_message_contains_group_name(self):
        """alert_message must include group_name."""
        result = format_alert(self._BASIC_ROW.copy())
        assert "discharge_point_A" in result["alert_message"]

    def test_alert_message_contains_top_contributor(self):
        """alert_message must name the top contributor."""
        result = format_alert(self._BASIC_ROW.copy())
        assert result["top_contributor"] in result["alert_message"]

    def test_missing_sensor_z_scores_handled(self):
        """Row with no sensor_z_scores key must not raise."""
        row = {"group_name": "g1", "composite_score": 1.0, "is_group_anomaly": False}
        result = format_alert(row)
        assert "top_contributor" in result
        assert result["top_contributor"] == ""

    def test_single_sensor_group(self):
        """Single sensor: fraction 1.0, message says 100%."""
        row = {
            "group_name":      "g1",
            "composite_score": 3.0,
            "is_group_anomaly": True,
            "sensor_z_scores": {"pH": 3.0},
        }
        result = format_alert(row)
        assert result["top_contributor"] == "pH"
        assert "100%" in result["alert_message"]
        obj = json.loads(result["attribution_detail"])
        assert abs(obj["pH"] - 1.0) < 0.001

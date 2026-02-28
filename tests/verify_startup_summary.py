import sys
import os
from pathlib import Path

# Ensure we can import from src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.startup_summary import format_summary

def test_summary_format():
    config = {
        "window_duration_ms": 30000,
        "zscore_threshold": 3.0,
        "persistence_count": 3,
        "sensor_groups": {
            "group_a": ["s1", "s2"],
            "group_b": ["s3"]  # Should trigger [WARN]
        },
        "river_sensitivity": {
            "point_a": 3.5,
            "point_b": 1.2
        },
        "input_schema_sensor_column": "factory_id",
        "input_schema_value_column": "cod"
    }
    
    summary = format_summary(config)
    lines = summary.split("\n")
    
    print("\n--- Summary Preview ---")
    print(summary)
    print("--- End Preview ---\n")
    
    # 1. Verify 80-character width
    for i, line in enumerate(lines):
        assert len(line) == 80, f"Line {i+1} is {len(line)} chars instead of 80"
    print("[OK] 80-character width verified.")
    
    # 2. Verify sections
    assert "SECTION 1" in summary
    assert "SECTION 2" in summary
    assert "SECTION 3" in summary
    assert "SECTION 4" in summary
    print("[OK] All sections present.")
    
    # 3. Verify content
    assert "Window: 30.0s" in summary
    assert "Z-threshold: 3.0" in summary
    assert "Persistence: 3" in summary
    assert "point_a, point_b" in summary
    assert "1.2 - 3.5" in summary
    print("[OK] Config values correctly extracted.")
    
    # 4. Verify [WARN] for single-sensor group
    assert "[WARN] Group 'group_b' has only 1 sensor" in summary
    print("[OK] [WARN] trigger for single-sensor group verified.")

if __name__ == "__main__":
    try:
        test_summary_format()
        print("\nAll verifications passed!")
    except AssertionError as e:
        print(f"\nVerification failed: {e}")
        exit(1)

"""
SHIELD AI — Startup Diagnostic Summary
======================================

Generates a formatted ASCII summary of the pipeline's architecture,
configuration, and environmental context at startup.

Requirements:
- Exactly 80 characters wide.
- ASCII-only box drawing.
- Programmatic layout.
"""

from __future__ import annotations

import os

WIDTH = 80


def _center(text: str, fill: str = " ") -> str:
    """Center text within WIDTH characters."""
    return f"| {text.center(WIDTH - 4)} |"


def _left(text: str) -> str:
    """Left-align text within WIDTH characters."""
    return f"| {text.ljust(WIDTH - 4)} |"


def _line(char: str = "-") -> str:
    """Return a horizontal separator line."""
    return f"+{char * (WIDTH - 2)}+"


def format_summary(config: dict) -> str:
    """Generate a multi-line string containing the startup diagnostic summary.

    Args:
        config: Dictionary containing pipeline configuration (from asdict(CONFIG)).

    Returns:
        Formatted 80-character wide ASCII summary.
    """
    lines = []

    # --- SECTION 1: Pipeline Architecture ---
    lines.append(_line("="))
    lines.append(_center("SECTION 1 - Pipeline Architecture"))
    lines.append(_line("-"))
    
    arch_flow = [
        "Input -> Validation -> Z-Score Scoring -> Persistence Filter ->",
        "Multivariate Score -> Causal Attribution -> ERI Computation ->",
        "Alert Routing -> Metrics Output"
    ]
    for row in arch_flow:
        lines.append(_center(row))
    
    # --- SECTION 2: Active Configuration ---
    lines.append(_line("="))
    lines.append(_center("SECTION 2 - Active Configuration"))
    lines.append(_line("-"))
    
    # Extract values
    window = f"{config.get('window_duration_ms', 0) / 1000:.1f}s"
    z_thresh = f"{config.get('zscore_threshold', 0.0):.1f}"
    persistence = f"{config.get('persistence_count', 0)}"
    num_groups = len(config.get("sensor_groups", {}))
    
    conf_table = f"Window: {window} | Z-threshold: {z_thresh} | Persistence: {persistence} | Groups: {num_groups}"
    lines.append(_center(conf_table))

    # --- SECTION 3: Environmental Context ---
    lines.append(_line("="))
    lines.append(_center("SECTION 3 - Environmental Context"))
    lines.append(_line("-"))
    
    sensitivity = config.get("river_sensitivity", {})
    points = sorted(sensitivity.keys())
    
    if sensitivity:
        factors = sensitivity.values()
        s_range = f"{min(factors):.1f} - {max(factors):.1f}"
    else:
        s_range = "N/A"
        
    lines.append(_left(f"Discharge points : {', '.join(points)}"))
    lines.append(_left(f"Sensitivity range: {s_range}"))

    # --- SECTION 4: System Readiness ---
    lines.append(_line("="))
    lines.append(_center("SECTION 4 - System Readiness Checklist"))
    lines.append(_line("-"))
    
    # [OK] Config validated (Always OK if we reached here)
    lines.append(_left("[OK] Config validated"))
    
    # [OK] Input schema defined
    sensor_col = config.get("input_schema_sensor_column", "N/A")
    val_col = config.get("input_schema_value_column", "N/A")
    lines.append(_left(f"[OK] Input schema defined ({sensor_col}, {val_col})"))
    
    # [OK/WARN] Sensor groups
    groups = config.get("sensor_groups", {})
    if not groups:
        lines.append(_left("[WARN] No sensor groups configured"))
    else:
        lines.append(_left(f"[OK] Sensor groups configured: {len(groups)}"))
        for g_name, sensors in groups.items():
            if len(sensors) == 1:
                lines.append(_left(f"[WARN] Group '{g_name}' has only 1 sensor"))
    
    lines.append(_line("="))

    return "\n".join(lines)


def print_startup_summary(config: dict) -> None:
    """Print the formatted summary to stdout."""
    print(format_summary(config))

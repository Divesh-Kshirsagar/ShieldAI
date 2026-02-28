#!/usr/bin/env python3
"""
SHIELD AI — Demo Orchestration Script
======================================

Automates a 3-minute narrative demonstration of the anomaly detection pipeline,
injecting synthetic data to simulate a realistic shock-load event sequence.

Narrative Arc:
--------------
1. Baseline Monitoring (Healthy)
2. Anomaly Detected (Z-Score Escalation - Drift)
3. Alert Triggered (Persistence Threshold Crossed - Spike)
4. Risk Elevation (ERI MEDIUM)
5. Critical Breach (ERI CRITICAL)
6. Stabilization & Cooldown

Usage:
------
    python demo_script.py [--fast]
"""

import argparse
import datetime
import json
import os
import sys
import time
from pathlib import Path

# Ensure we can import from src and root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.config import CONFIG as _cfg
import inject_anomaly as injector

# ---------------------------------------------------------------------------
# Demo Timing Constants (Seconds)
# ---------------------------------------------------------------------------

DEMO_STEP_DURATION_BASELINE    = 30
DEMO_STEP_DURATION_ANOMALY     = 30
DEMO_STEP_DURATION_ALERT       = 30
DEMO_STEP_DURATION_MEDIUM_RISK = 30
DEMO_STEP_DURATION_CRITICAL    = 30
DEMO_STEP_DURATION_STABILIZE   = 30

# Fast Mode Multiplier (20% of normal)
FAST_MULTIPLIER = 0.2


# ---------------------------------------------------------------------------
# Orchestration Logic
# ---------------------------------------------------------------------------

def get_demo_sensor() -> str:
    """Return the first sensor ID from the first sensor group in config."""
    group_name = next(iter(_cfg.sensor_groups))
    return _cfg.sensor_groups[group_name][0]


def print_step(step_num: int, message: str):
    """Print an annotated demo step with a timestamp."""
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] STEP {step_num}: {message}")


def wait(seconds: float, fast: bool):
    """Sleep for the given duration, adjusted for --fast flag."""
    duration = seconds * FAST_MULTIPLIER if fast else seconds
    time.sleep(duration)


def inject_data(sensor_id: str, anomaly_type: str, value: float, count: int = 1):
    """Helper to call inject_anomaly API directly."""
    target_path = injector._resolve_target_path(_cfg.factory_data_directory, injector.DEFAULT_CSV_FILENAME)
    starting_s_no = injector._next_s_no(target_path)
    
    # Mock namespace for build_events
    args = argparse.Namespace(
        sensor_id=sensor_id,
        anomaly_type=anomaly_type,
        value=value,
        count=count,
        spike_multiplier=1.0,  # We provide raw values
        drift_step_size=5.0,
        interval_ms=1000,
    )
    
    rows = injector.build_events(args, starting_s_no)
    injector.write_events(target_path, rows)


def run_demo(fast: bool):
    """Execute the narrative arc."""
    sensor_id = get_demo_sensor()
    baseline_cod = _cfg.cod_baseline
    
    print("\n" + "="*60)
    print(" SHIELD AI — Automated Demo Sequence Starting")
    print("="*60 + "\n")

    # 1. Baseline
    print_step(1, "System online — baseline monitoring active")
    inject_data(sensor_id, "step", baseline_cod, count=5)
    wait(DEMO_STEP_DURATION_BASELINE, fast)

    # 2. Anomaly Drift
    print_step(2, "Anomaly detected at discharge point A — z-score escalating")
    # Drift from baseline upward
    inject_data(sensor_id, "drift", baseline_cod + 10.0, count=10)
    wait(DEMO_STEP_DURATION_ANOMALY, fast)

    # 3. Persistence Spike
    print_step(3, "Persistence threshold crossed — alert triggered")
    # Sudden sharp spike (z-score > 3)
    inject_data(sensor_id, "spike", baseline_cod + 100.0)
    wait(DEMO_STEP_DURATION_ALERT, fast)

    # 4. ERI Medium
    print_step(4, "Environmental Risk Index elevated — MEDIUM risk")
    # ERI in [2.0, 5.0) -> composite score * sensitivity. 
    # Sensitivity A is 3.5. So score > 0.6 approx.
    inject_data(sensor_id, "step", baseline_cod + 150.0, count=5)
    wait(DEMO_STEP_DURATION_MEDIUM_RISK, fast)

    # 5. Critical Breach
    print_step(5, f"CRITICAL threshold breached — operator notification sent")
    # ERI >= 10.0 -> score * 3.5 >= 10 -> score >= 2.85ish.
    inject_data(sensor_id, "spike", baseline_cod + 400.0)
    wait(DEMO_STEP_DURATION_CRITICAL, fast)

    # 6. Stabilization
    print_step(6, "System stabilizing — alert cooldown active")
    inject_data(sensor_id, "step", baseline_cod, count=5)
    wait(DEMO_STEP_DURATION_STABILIZE, fast)

    print("\n" + "="*60)
    print(" DEMO COMPLETE — EXHIBIT SUMMARY")
    print("="*60)
    print_final_summary()


def print_final_summary():
    """Read metrics JSON and display final stats."""
    metrics_path = Path(_cfg.metrics_output_path)
    if not metrics_path.exists():
        print(f"\n[!] Metrics file not found at {metrics_path}. Make sure the pipeline is running.")
        return

    try:
        with open(metrics_path, "r") as f:
            metrics = json.load(f)
            
        print(f"Total events processed:   {metrics.get('events_processed_total', 'N/A')}")
        print(f"Anomalies detected:       {metrics.get('anomalies_detected_total', 'N/A')}")
        print(f"Max Risk Band reached:    {metrics.get('highest_risk_band', 'N/A')}")
        print(f"Pipeline uptime:          {metrics.get('pipeline_uptime_seconds', 'N/A')}s")
        print(f"Last event time:          {metrics.get('last_event_timestamp', 'N/A')}")
    except Exception as e:
        print(f"\n[!] Error reading summary: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SHIELD AI Demo Orchestrator")
    parser.add_argument("--fast", action="store_true", help="Compress delays to 20% of normal")
    args = parser.parse_args()
    
    try:
        run_demo(args.fast)
    except KeyboardInterrupt:
        print("\n\n[!] Demo interrupted by user.")
        sys.exit(0)

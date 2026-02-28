"""
SHIELD AI — Phase 4: Anti-Cheat Alert Sink
===========================================

pw.io.subscribe callbacks for each anti-cheat detector.
All tamper events are appended to data/alerts/tamper_log.jsonl.

Usage
-----
    from src.alert_anticheat import attach_tamper_sinks
    attach_tamper_sinks(zero_var_events, fingerprint_events, blackout_events)
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pathway as pw

from src.config import CONFIG as _cfg

_TAMPER_LOG_PATH: str = _cfg.tamper_log_path


def _write_tamper(record: dict) -> None:
    """Append one tamper record to the tamper JSONL log."""
    Path(_TAMPER_LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(_TAMPER_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
    print(
        f"[TAMPER] {record['tamper_type']} | "
        f"Factory: {record['factory_id']} | "
        f"Window: {record.get('window_end','?')}"
    )


def _zero_var_callback(key: pw.Pointer, row: dict, time: int, is_addition: bool) -> None:
    if not is_addition:
        return
    record = {
        "logged_at":   datetime.now(tz=timezone.utc).isoformat(),
        "tamper_type": row.get("tamper_type", "ZERO_VARIANCE"),
        "factory_id":  row.get("factory_id"),
        "window_end":  row.get("window_end"),
        "cod_max":     row.get("cod_max"),
        "cod_min":     row.get("cod_min"),
        "cod_range":   row.get("cod_range"),
        "row_count":   row.get("row_count"),
    }
    _write_tamper(record)


def _fingerprint_callback(key: pw.Pointer, row: dict, time: int, is_addition: bool) -> None:
    if not is_addition:
        return
    record = {
        "logged_at":    datetime.now(tz=timezone.utc).isoformat(),
        "tamper_type":  row.get("tamper_type", "DILUTION_TAMPER"),
        "factory_id":   row.get("factory_id"),
        "window_end":   row.get("window_end"),
        "mean_cod":     row.get("mean_cod"),
        "mean_tss":     row.get("mean_tss"),
        "baseline_cod": row.get("baseline_cod"),
        "baseline_tss": row.get("baseline_tss"),
    }
    _write_tamper(record)


def _blackout_callback(key: pw.Pointer, row: dict, time: int, is_addition: bool) -> None:
    if not is_addition:
        return
    record = {
        "logged_at":      datetime.now(tz=timezone.utc).isoformat(),
        "tamper_type":    row.get("tamper_type", "BLACKOUT_TAMPER"),
        "factory_id":     row.get("factory_id"),
        "window_end":     row.get("window_end"),
        "total_rows":     row.get("total_rows"),
        "blackout_rows":  row.get("blackout_rows"),
        "blackout_ratio": row.get("blackout_ratio"),
    }
    _write_tamper(record)


def attach_tamper_sinks(
    zero_var_events,
    fingerprint_events,
    blackout_events,
) -> None:
    """Register pw.io.subscribe callbacks for all three tamper detectors."""
    pw.io.subscribe(zero_var_events,     _zero_var_callback)
    pw.io.subscribe(fingerprint_events,  _fingerprint_callback)
    pw.io.subscribe(blackout_events,     _blackout_callback)

#!/usr/bin/env python3
"""
inject_anomaly.py — Synthetic Anomaly Injector for SHIELD AI
=============================================================

Writes synthetic anomalous sensor readings into the pipeline's CSV input
directory, letting you exercise the z-score scorer and ERI alerting pipeline
without waiting for real plant data.

Usage
-----
    python inject_anomaly.py --sensor-id FACTORY_A --value 95.0 --anomaly-type spike
    python inject_anomaly.py --sensor-id FACTORY_B --anomaly-type drift --count 10
    python inject_anomaly.py --sensor-id FACTORY_C --value 80.0 --anomaly-type step --count 5 --dry-run

Anomaly types
-------------
    spike   Emit a single extreme reading at (VALUE × SPIKE_MULTIPLIER).
            Designed to trip the z-score threshold in one shot.
    drift   Emit COUNT readings each DRIFT_STEP_SIZE above the previous,
            starting at VALUE.  Simulates a slow sensor runaway.
    step    Emit COUNT readings all fixed at VALUE.  Simulates a sudden,
            sustained process change.

Output CSV columns (matches data/factories/factory_*.csv schema)
----------------------------------------------------------------
    s_no, time, factory_id, cod, bod, ph, tss
    factory_id  = --sensor-id  (must match a value in CONFIG.sensor_groups)
    cod         = synthetic anomalous reading   (other columns left empty)

All default values are defined as constants at the top of this file.
The script is self-contained — it does NOT import from any pipeline module.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import os
import sys
import time

# ---------------------------------------------------------------------------
# Defaults — modify these constants to change behaviour without CLI flags
# ---------------------------------------------------------------------------

DEFAULT_COUNT            = 1           # number of events to emit
DEFAULT_INTERVAL_MS      = 500         # delay between events (milliseconds)
DEFAULT_ANOMALY_TYPE     = "spike"     # spike | drift | step
DEFAULT_VALUE            = 50.0        # base reading value
DEFAULT_SPIKE_MULTIPLIER = 5.0         # spike = value × this multiplier
DEFAULT_DRIFT_STEP_SIZE  = 0.1         # added per reading in drift mode
DEFAULT_TIME_FORMAT      = "%Y-%m-%d %H:%M"          # must match pipeline TIME_FORMAT
DEFAULT_CSV_DIR          = "data/factories"           # directory watched by pw.io.csv
DEFAULT_CSV_FILENAME     = "injected_anomalies.csv"  # appended on every run

# CSV schema column names (must match factory_*.csv header)
CSV_COLUMNS = ["s_no", "time", "factory_id", "cod", "bod", "ph", "tss"]


# ---------------------------------------------------------------------------
# Event generation — pure functions, no I/O
# ---------------------------------------------------------------------------

def _next_s_no(filepath: str) -> int:
    """Return the next sequential row number for the target CSV file.

    Counts existing non-header rows so injected rows continue the sequence.
    Returns 1 if the file does not exist.
    """
    if not os.path.isfile(filepath):
        return 1
    with open(filepath, newline="") as fh:
        reader = csv.DictReader(fh)
        rows = sum(1 for _ in reader)
    return rows + 1


def _make_row(
    s_no: int,
    sensor_id: str,
    value: float,
    timestamp: datetime.datetime,
) -> dict:
    """Return a single CSV row dict with the given value in the ``cod`` column."""
    return {
        "s_no":       s_no,
        "time":       timestamp.strftime(DEFAULT_TIME_FORMAT),
        "factory_id": sensor_id,
        "cod":        round(value, 6),
        "bod":        "",
        "ph":         "",
        "tss":        "",
    }


def generate_spike_events(
    sensor_id: str,
    value: float,
    multiplier: float,
    starting_s_no: int,
    start_time: datetime.datetime,
) -> list[dict]:
    """Generate a single spike event at value × multiplier."""
    spike_value = value * multiplier
    return [_make_row(starting_s_no, sensor_id, spike_value, start_time)]


def generate_drift_events(
    sensor_id: str,
    value: float,
    count: int,
    step_size: float,
    starting_s_no: int,
    start_time: datetime.datetime,
    interval_ms: int,
) -> list[dict]:
    """Generate COUNT readings incrementally drifting upward from VALUE."""
    rows = []
    for i in range(count):
        drifted = value + i * step_size
        ts = start_time + datetime.timedelta(milliseconds=i * interval_ms)
        rows.append(_make_row(starting_s_no + i, sensor_id, drifted, ts))
    return rows


def generate_step_events(
    sensor_id: str,
    value: float,
    count: int,
    starting_s_no: int,
    start_time: datetime.datetime,
    interval_ms: int,
) -> list[dict]:
    """Generate COUNT readings all fixed at VALUE."""
    rows = []
    for i in range(count):
        ts = start_time + datetime.timedelta(milliseconds=i * interval_ms)
        rows.append(_make_row(starting_s_no + i, sensor_id, value, ts))
    return rows


def build_events(args: argparse.Namespace, starting_s_no: int) -> list[dict]:
    """Dispatch to the correct generator based on args.anomaly_type."""
    now = datetime.datetime.now()
    atype = args.anomaly_type

    if atype == "spike":
        return generate_spike_events(
            args.sensor_id, args.value,
            args.spike_multiplier, starting_s_no, now,
        )
    if atype == "drift":
        return generate_drift_events(
            args.sensor_id, args.value, args.count,
            args.drift_step_size, starting_s_no, now, args.interval_ms,
        )
    if atype == "step":
        return generate_step_events(
            args.sensor_id, args.value, args.count,
            starting_s_no, now, args.interval_ms,
        )
    raise ValueError(f"Unknown anomaly_type: {atype!r}")


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _resolve_target_path(csv_dir: str, filename: str) -> str:
    """Resolve and return the absolute path of the target CSV file."""
    return os.path.abspath(os.path.join(csv_dir, filename))


def _ensure_header(filepath: str) -> None:
    """Write the CSV header if the file is new or empty."""
    if os.path.isfile(filepath) and os.path.getsize(filepath) > 0:
        return
    with open(filepath, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()


def write_events(filepath: str, rows: list[dict]) -> None:
    """Append rows to the CSV file, writing the header if needed."""
    _ensure_header(filepath)
    with open(filepath, "a", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writerows(rows)


def print_preview(rows: list[dict]) -> None:
    """Print a human-readable preview of events that would be written."""
    col_w = 22
    header = "  ".join(f"{c:<{col_w}}" for c in CSV_COLUMNS)
    print(f"\n{'─' * len(header)}")
    print(header)
    print(f"{'─' * len(header)}")
    for row in rows:
        line = "  ".join(f"{str(row[c]):<{col_w}}" for c in CSV_COLUMNS)
        print(line)
    print(f"{'─' * len(header)}\n")


def emit_events(
    rows: list[dict],
    filepath: str,
    interval_ms: int,
    dry_run: bool,
) -> None:
    """Write (or preview) events, sleeping interval_ms between each row."""
    if dry_run:
        print("\n[DRY RUN] The following rows would be written:")
        print_preview(rows)
        return

    for i, row in enumerate(rows):
        write_events(filepath, [row])
        if i < len(rows) - 1:
            time.sleep(interval_ms / 1000.0)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    """Construct and return the argument parser."""
    p = argparse.ArgumentParser(
        prog="inject_anomaly",
        description="Write synthetic anomalous sensor readings into the SHIELD AI pipeline input.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--sensor-id",
        required=True,
        metavar="SENSOR_ID",
        help="Sensor / factory identifier (e.g. FACTORY_A).  Must match a value in the pipeline's SENSOR_GROUPS config.",
    )
    p.add_argument(
        "--value",
        type=float,
        default=DEFAULT_VALUE,
        metavar="VALUE",
        help="Base reading value.  For spike mode this is multiplied by --spike-multiplier.",
    )
    p.add_argument(
        "--count",
        type=int,
        default=DEFAULT_COUNT,
        metavar="COUNT",
        help="Number of events to emit.  Ignored for spike (always 1).",
    )
    p.add_argument(
        "--interval-ms",
        type=int,
        default=DEFAULT_INTERVAL_MS,
        dest="interval_ms",
        metavar="MS",
        help="Delay between successive events in milliseconds.",
    )
    p.add_argument(
        "--anomaly-type",
        choices=["spike", "drift", "step"],
        default=DEFAULT_ANOMALY_TYPE,
        dest="anomaly_type",
        help="Type of anomaly pattern to inject.",
    )
    p.add_argument(
        "--spike-multiplier",
        type=float,
        default=DEFAULT_SPIKE_MULTIPLIER,
        dest="spike_multiplier",
        metavar="MULT",
        help="For spike mode: emitted value = VALUE × MULT.",
    )
    p.add_argument(
        "--drift-step-size",
        type=float,
        default=DEFAULT_DRIFT_STEP_SIZE,
        dest="drift_step_size",
        metavar="STEP",
        help="For drift mode: each successive reading increases by this amount.",
    )
    p.add_argument(
        "--csv-dir",
        default=DEFAULT_CSV_DIR,
        dest="csv_dir",
        metavar="DIR",
        help="Directory containing pipeline input CSV files.",
    )
    p.add_argument(
        "--csv-filename",
        default=DEFAULT_CSV_FILENAME,
        dest="csv_filename",
        metavar="FILE",
        help="CSV filename within csv-dir to append events to.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="Print what would be written without touching any files.",
    )
    return p


def _validate_args(args: argparse.Namespace) -> None:
    """Raise SystemExit with a message if any argument combination is invalid."""
    if args.count < 1:
        print(f"error: --count must be >= 1 (got {args.count})", file=sys.stderr)
        sys.exit(1)
    if args.interval_ms < 0:
        print(f"error: --interval-ms must be >= 0 (got {args.interval_ms})", file=sys.stderr)
        sys.exit(1)
    if args.spike_multiplier <= 0:
        print(f"error: --spike-multiplier must be > 0 (got {args.spike_multiplier})", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    """Parse CLI arguments, generate events, and write (or preview) them."""
    parser = _build_parser()
    args   = parser.parse_args()
    _validate_args(args)

    target_path = _resolve_target_path(args.csv_dir, args.csv_filename)
    starting_s_no = 1 if args.dry_run else _next_s_no(target_path)

    rows = build_events(args, starting_s_no)

    emit_events(rows, target_path, args.interval_ms, args.dry_run)

    actual_count = len(rows)
    if args.dry_run:
        print(
            f"[DRY RUN] Would inject {actual_count} {args.anomaly_type} "
            f"event(s) for {args.sensor_id} → {target_path}"
        )
    else:
        print(
            f"Injected {actual_count} {args.anomaly_type} "
            f"event(s) for {args.sensor_id} → {target_path}"
        )


if __name__ == "__main__":
    main()

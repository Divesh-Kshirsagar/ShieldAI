"""
SHIELD AI — Phase 1 Pipeline Entry Point
=========================================

Wires all Phase 1 modules together into a single Pathway execution graph:

    ingest  →  aggregate  →  tripwire  →  backtrack  →  alert

Run modes
---------
replay  (default): Stream the pre-existing CSV files as if they are arriving live.
                   Pathway's streaming CSV reader tails the files at file-system speed.

Usage
-----
    python src/run_pipeline.py
    python src/run_pipeline.py --mode replay --data-dir data/

The pipeline runs until interrupted (Ctrl-C). Evidence records accumulate in
data/alerts/evidence_log.jsonl in real time.
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path when run as a script
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pathway as pw

from src.ingest    import load_cetp_stream
from src.aggregate import build_industrial_stream
from src.tripwire  import detect_anomalies
from src.backtrack import attribute_factory
from src.alert     import attach_alert_sink
from src.constants import CETP_DATA_DIR, FACTORY_DATA_DIR


def build_pipeline(cetp_dir: str = CETP_DATA_DIR,
                   factory_dir: str = FACTORY_DATA_DIR) -> pw.Table:
    """Construct the full Phase 1 Pathway computation graph.

    This function only defines the graph — it does NOT run it.
    Call pw.run() afterwards (or use run_pipeline() below).

    Args:
        cetp_dir:    Directory containing CETP CSV file(s).
        factory_dir: Directory containing factory CSV files.

    Returns:
        evidence_table — the final attributed evidence Pathway Table.
    """
    # Step 1 — Ingest
    cetp_stream        = load_cetp_stream(cetp_dir)
    industrial_stream  = build_industrial_stream(factory_dir, include_blackout=False)

    # Step 2 — Detect anomalies (the Tripwire)
    shock_events = detect_anomalies(cetp_stream)

    # Step 3 — Temporal backtrack join (the Time Machine)
    evidence = attribute_factory(shock_events, industrial_stream)

    # Step 4 — Attach the alert sink (JSONL + optional webhook)
    attach_alert_sink(evidence)

    return evidence


def run_pipeline(cetp_dir: str = CETP_DATA_DIR,
                 factory_dir: str = FACTORY_DATA_DIR) -> None:
    """Build and run the Phase 1 pipeline.

    Blocks until interrupted. All evidence records are appended to
    data/alerts/evidence_log.jsonl as they are detected.

    Args:
        cetp_dir:    Directory containing CETP CSV file(s).
        factory_dir: Directory containing factory CSV files.
    """
    print("SHIELD AI — Starting Phase 1 pipeline")
    print(f"  CETP source    : {cetp_dir}")
    print(f"  Factory source : {factory_dir}")
    print("  Press Ctrl-C to stop.\n")

    build_pipeline(cetp_dir, factory_dir)

    # NOTE: pw.run() starts the Pathway event loop. It processes all existing
    # CSV rows first (replay mode), then continues tailing for new rows.
    pw.run()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SHIELD AI — Phase 1 Pathway pipeline runner"
    )
    parser.add_argument(
        "--mode",
        choices=["replay"],
        default="replay",
        help="Execution mode. 'replay' tails existing CSVs as live streams.",
    )
    parser.add_argument(
        "--cetp-dir",
        default=CETP_DATA_DIR,
        help=f"Directory containing CETP CSV(s). Default: {CETP_DATA_DIR}",
    )
    parser.add_argument(
        "--factory-dir",
        default=FACTORY_DATA_DIR,
        help=f"Directory containing factory CSVs. Default: {FACTORY_DATA_DIR}",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_pipeline(cetp_dir=args.cetp_dir, factory_dir=args.factory_dir)

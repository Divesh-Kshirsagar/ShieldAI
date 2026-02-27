"""
SHIELD AI — Phase 1 Pipeline Entry Point
=========================================

Wires all Phase 1 modules together:

    ingest (CETP) → tripwire → alert (with backtrack attribution in callback)

The factory index is loaded eagerly at startup and passed to the alert sink,
which runs pandas-based temporal backtrack attribution for each shock event.

Architecture note
-----------------
In Phase 1, factory CSVs are static historical data. The backtrack join is
performed in the pw.io.subscribe() callback using a pre-loaded pandas index.
This avoids cross-stream clock issues with interval_join across two independent
Pathway streaming sources (documented in backtrack.py). The v2 upgrade path
(live per-factory MPCB feeds → pw.temporal.asof_join_left on merged stream)
is described in backtrack.py.

Run
---
    uv run python src/run_pipeline.py
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pathway as pw

from src.alert     import attach_alert_sink
from src.backtrack import build_factory_index
from src.constants import CETP_DATA_DIR, FACTORY_DATA_DIR
from src.ingest    import load_cetp_stream
from src.tripwire  import detect_anomalies


def build_pipeline(
    cetp_dir: str = CETP_DATA_DIR,
    factory_dir: str = FACTORY_DATA_DIR,
) -> None:
    """Construct and run the Phase 1 Pathway computation graph.

    Steps:
        1. Load factory index into memory (pandas) for backtrack attribution.
        2. Start Pathway streaming read of CETP CSV.
        3. Apply tripwire filter: COD >= COD_THRESHOLD → shock_events table.
        4. Attach evidence log sink with backtrack attribution callback.
        5. pw.run() — processes all existing rows then tails for new data.

    Args:
        cetp_dir:    Directory containing cetp_clean.csv.
        factory_dir: Directory containing factory_A/B/C/D.csv.
    """
    # Step 1 — Eagerly load the factory index (pandas, not Pathway)
    # This is called before pw.run() so the index is available in the callback.
    factory_index = build_factory_index(factory_dir)

    # Step 2 — CETP streaming read + NA filter
    cetp_stream = load_cetp_stream(cetp_dir)

    # Step 3 — Tripwire: filter to shock events
    shock_events = detect_anomalies(cetp_stream)

    # Step 4 — Register the evidence log sink (attributions run inside callback)
    attach_alert_sink(shock_events, factory_index)


def run_pipeline(
    cetp_dir: str = CETP_DATA_DIR,
    factory_dir: str = FACTORY_DATA_DIR,
) -> None:
    """Build and run the Phase 1 pipeline.

    Blocks until interrupted (Ctrl-C). Every shock event is attributed and
    appended to data/alerts/evidence_log.jsonl in real time.
    """
    print("SHIELD AI — Starting Phase 1 pipeline")
    print(f"  CETP source    : {cetp_dir}")
    print(f"  Factory source : {factory_dir}")
    print("  Press Ctrl-C to stop.\n")

    build_pipeline(cetp_dir, factory_dir)
    pw.run()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SHIELD AI — Phase 1 Pathway pipeline runner"
    )
    parser.add_argument("--cetp-dir",    default=CETP_DATA_DIR)
    parser.add_argument("--factory-dir", default=FACTORY_DATA_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_pipeline(cetp_dir=args.cetp_dir, factory_dir=args.factory_dir)

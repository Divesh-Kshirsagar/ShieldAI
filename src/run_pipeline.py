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
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pathway as pw

from src.logger import configure_logging
from src.config import CONFIG as _cfg
from src.ingest import load_cetp_stream, load_factory_streams
from src.aggregate import build_industrial_stream
from src.multivariate import build_group_anomalies
from src.eri import build_eri_stream
from src.alerts import build_alert_stream
from src.alert import attach_alert_sink
from src.metrics_aggregator import build_metrics_table

log = logging.getLogger(__name__)

_CETP_DATA_DIR:    str = _cfg.cetp_data_directory
_FACTORY_DATA_DIR: str = _cfg.factory_data_directory


def build_pipeline(
    cetp_dir: str = _CETP_DATA_DIR,
    factory_dir: str = _FACTORY_DATA_DIR,
) -> pw.Table:
    """Construct the full SHIELD AI pipeline (Phase 2 + Metrics).

    Returns:
        pipeline_metrics — Pathway Table with real-time KPIs.
    """
    # 1. Ingestion
    cetp_stream = load_cetp_stream(cetp_dir)
    factory_stream = build_industrial_stream(factory_dir) # already filtered by ingest.py updates

    # 2. Multivariate Anomaly Detection (Group-level)
    group_anomalies = build_group_anomalies(factory_stream)

    # 3. Environmental Risk Index (ERI)
    eri_stream = build_eri_stream(group_anomalies)

    # 4. Risk-Gated Alerts
    active_alerts = build_alert_stream(eri_stream)

    # 5. Sinks
    # Phase 2 Sink (Alert Registry / Evidence Log)
    # We use build_factory_index internally if None is passed
    attach_alert_sink(active_alerts)

    # 6. Metrics Aggregation (The new requirement)
    # We provide:
    #   input_stream = factory_stream (union of all factories)
    #   anomaly_stream = group_anomalies
    #   eri_stream = eri_stream
    #   alert_stream = active_alerts
    metrics_table = build_metrics_table(
        input_stream=factory_stream,
        anomaly_stream=group_anomalies,
        eri_stream=eri_stream,
        alert_stream=active_alerts
    )
    
    return metrics_table


def run_pipeline(
    cetp_dir: str = _CETP_DATA_DIR,
    factory_dir: str = _FACTORY_DATA_DIR,
) -> None:
    """Build and run the full SHIELD AI pipeline."""
    configure_logging(level=_cfg.log_level)
    
    # Print startup diagnostic summary (80-char ASCII box)
    from dataclasses import asdict
    from src.startup_summary import print_startup_summary
    print_startup_summary(asdict(_cfg))

    log.info(
        "pipeline started (Phase 2 + Metrics)",
        extra={
            "cetp_dir":    cetp_dir,
            "factory_dir": factory_dir,
            "log_level":   _cfg.log_level,
            "metrics_path": _cfg.metrics_output_path,
        },
    )

    build_pipeline(cetp_dir, factory_dir)
    pw.run()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SHIELD AI — Full Pathway pipeline runner (Phase 2 + Metrics)"
    )
    parser.add_argument("--cetp-dir",    default=_CETP_DATA_DIR)
    parser.add_argument("--factory-dir", default=_FACTORY_DATA_DIR)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_pipeline(cetp_dir=args.cetp_dir, factory_dir=args.factory_dir)

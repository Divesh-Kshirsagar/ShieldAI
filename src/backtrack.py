"""
SHIELD AI — Phase 1: Temporal Backtracking (The "Time Machine" Join)
=====================================================================

The core attribution engine. For each CETP shock event at time T:
    1. Compute T_backtrack = T − PIPE_TRAVEL_MINUTES.
    2. Find the factory CSV row whose timestamp is closest to T_backtrack,
       within ±ASOF_TOLERANCE_SECONDS.
    3. Return the matched factory_id + discharge readings as the evidence record.

Implementation note
-------------------
Pathway's interval_join_left / asof_join across two independently-clocked
streaming CSV sources can produce None matches in 0.29.x because each source
has its own internal logical clock and the join closes windows before rows from
the other stream arrive. For this prototype we therefore perform the temporal
attribution inside the pw.io.subscribe() callback using pandas, which is:
  - Guaranteed to see all factory rows (they're pre-loaded into memory once)
  - Deterministic (no streaming clock race conditions)
  - Equally correct — the factory CSVs are static historical data

In a production deployment with genuinely live per-factory MPCB feeds,
the correct approach would be pw.temporal.asof_join_left on a single
merged stream with a shared clock. This is the documented upgrade path for v2.

NOTE: PIPE_TRAVEL_MINUTES = 15 is a FIXED CONSTANT for v1.
In v2 this will be replaced by a dynamic, pipe-length-aware calculation
derived from GIS network data and real-time flow-rate sensors.
Do NOT remove this constant — it is the single source of truth for the
temporal offset used in the attribution logic.

Usage
-----
    from src.backtrack import build_factory_index, attribute_event
    factory_index = build_factory_index()
    evidence = attribute_event(cetp_time="2026-02-01 12:23", factory_index=factory_index)
"""

import datetime
import logging
from pathlib import Path

import pandas as pd

from src.config import CONFIG as _cfg

_FACTORY_DATA_DIR:      str = _cfg.factory_data_directory
_PIPE_TRAVEL_MINUTES:   int = _cfg.pipe_travel_minutes
_ASOF_TOLERANCE_SECONDS: int = _cfg.asof_tolerance_seconds

log = logging.getLogger(__name__)


def build_factory_index(factory_dir: str = _FACTORY_DATA_DIR) -> pd.DataFrame:
    """Load all factory CSVs into a single sorted DataFrame for fast backtrack lookup.

    Called once at pipeline startup — factory data is historical so loading
    it eagerly is correct and avoids cross-stream clock issues.

    Args:
        factory_dir: Directory containing factory_A/B/C/D.csv.

    Returns:
        DataFrame with columns: factory_id, time_dt (datetime), cod, bod, ph, tss.
        Sorted by time ascending; only rows with non-null COD included.
    """
    dfs = []
    factory_path = Path(factory_dir)
    for csv_path in sorted(factory_path.glob("factory_*.csv")):
        df = pd.read_csv(csv_path, dtype={"time": str})
        df["time_dt"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M", errors="coerce")
        df["cod"]     = pd.to_numeric(df["cod"], errors="coerce")
        df["bod"]     = pd.to_numeric(df["bod"], errors="coerce")
        df["ph"]      = pd.to_numeric(df["ph"], errors="coerce")
        df["tss"]     = pd.to_numeric(df["tss"], errors="coerce")
        # Only keep rows with a valid COD reading (NORMAL rows, no BLACKOUT)
        df = df.dropna(subset=["cod"])[["factory_id", "time_dt", "cod", "bod", "ph", "tss"]]
        dfs.append(df)

    index = pd.concat(dfs, ignore_index=True).sort_values("time_dt").reset_index(drop=True)
    log.info(
        "config loaded",
        extra={"index_rows": len(index), "factories": index['factory_id'].nunique()},
    )
    return index


def attribute_event(
    cetp_time: str,
    factory_index: pd.DataFrame,
    travel_minutes: int = _PIPE_TRAVEL_MINUTES,
    tolerance_seconds: int = _ASOF_TOLERANCE_SECONDS,
) -> dict:
    """Find the factory most likely responsible for a CETP shock event.

    Searches factory_index for the row closest to T_backtrack within the
    tolerance window. If multiple factories have rows in the window, the one
    with the highest COD reading is attributed (highest discharge = culprit).

    Args:
        cetp_time:         Timestamp string of the CETP shock event ('YYYY-MM-DD HH:MM').
        factory_index:     Pre-loaded factory DataFrame from build_factory_index().
        travel_minutes:    Pipe travel time in minutes (default: PIPE_TRAVEL_MINUTES).
        tolerance_seconds: Search window radius in seconds (default: ASOF_TOLERANCE_SECONDS).

    Returns:
        Dict with keys: attributed_factory, factory_cod, factory_bod, factory_tss,
        backtrack_time. All values are None if no factory row found in the window.
    """
    t               = pd.to_datetime(cetp_time, format="%Y-%m-%d %H:%M", errors="coerce")
    t_backtrack     = t - datetime.timedelta(minutes=travel_minutes)
    tolerance_td    = datetime.timedelta(seconds=tolerance_seconds)
    t_lower         = t_backtrack - tolerance_td
    t_upper         = t_backtrack + tolerance_td

    window_rows = factory_index[
        (factory_index["time_dt"] >= t_lower) &
        (factory_index["time_dt"] <= t_upper)
    ]

    if window_rows.empty:
        return {
            "attributed_factory": None,
            "factory_cod":        None,
            "factory_bod":        None,
            "factory_tss":        None,
            "backtrack_time":     t_backtrack.strftime("%Y-%m-%d %H:%M"),
        }

    # Attribution rule: highest COD reading in the window = most likely culprit.
    # NOTE: In v2 this will be augmented with chemical fingerprint matching
    # and statistical weighting by factory discharge permit volume.
    best = window_rows.loc[window_rows["cod"].idxmax()]

    return {
        "attributed_factory": best["factory_id"],
        "factory_cod":        round(float(best["cod"]), 2),
        "factory_bod":        round(float(best["bod"]), 2) if pd.notna(best["bod"]) else None,
        "factory_tss":        round(float(best["tss"]), 2) if pd.notna(best["tss"]) else None,
        "backtrack_time":     t_backtrack.strftime("%Y-%m-%d %H:%M"),
    }

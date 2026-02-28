"""
SHIELD AI — Phase 4: Anti-Cheating Engine (v2)
===============================================

Three detection mechanisms, all running on factory discharge data loaded as pandas
DataFrames (consistent with the backtrack.py approach). Pathway's windowby reducers
reject Optional float columns in 0.29.x, so anti-cheat logic runs in pandas and
results are written to tamper_log.jsonl directly.

1. Zero-Variance Alarm
   detect_zero_variance():
   For each factory in each tumbling ZERO_VARIANCE_MINUTES window, check if
   all COD readings are identical (max - min < TINY_EPS). If so → flag.
   Signal: factory_C — digital copy-paste / frozen sensor value.

2. Chemical Fingerprint
   detect_chemical_fingerprint():
   Compute 60-min rolling mean COD and TSS per factory. Flag windows where:
     - mean_COD ≤ baseline_COD × (1 - COD_DROP_FRACTION)
     - mean_TSS ≥ baseline_TSS × (1 - TSS_STABLE_FRACTION)
   Signal: bucket dilution — COD drops 80%+ while TSS stays high.

3. Guilt-by-Disconnection
   detect_guilt_by_disconnection():
   For each factory in tumbling BLACKOUT_MIN_MINUTES windows, compute the
   fraction of NA rows (BLACKOUT rows). If fraction ≥ 0.80 → flag.
   Signal: factory_D — strategic sensor blackout to hide a dump.

Usage
-----
    from src.anti_cheat import run_all_detectors
    tamper_records = run_all_detectors()     # returns list of dicts
"""

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.config import CONFIG as _cfg

log = logging.getLogger(__name__)

_BLACKOUT_MIN_MINUTES: int   = _cfg.blackout_min_minutes
_COD_DROP_FRACTION:    float = _cfg.cod_drop_fraction
_FACTORY_DATA_DIR:     str   = _cfg.factory_data_directory
_TSS_STABLE_FRACTION:  float = _cfg.tss_stable_fraction
_ZERO_VARIANCE_MINUTES: int  = _cfg.zero_variance_minutes

TINY_EPS = 1e-4  # |max - min| below this value = declared zero-variance


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def _load_factories(factory_dir: str = _FACTORY_DATA_DIR) -> pd.DataFrame:
    """Load all factory CSVs into a single DataFrame with parsed timestamps."""
    dfs = []
    for p in sorted(Path(factory_dir).glob("factory_*.csv")):
        df = pd.read_csv(p, dtype={"time": str, "factory_id": str})
        df["time_dt"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M", errors="coerce")
        df["cod"]     = pd.to_numeric(df["cod"], errors="coerce")
        df["bod"]     = pd.to_numeric(df["bod"], errors="coerce")
        df["ph"]      = pd.to_numeric(df["ph"],  errors="coerce")
        df["tss"]     = pd.to_numeric(df["tss"], errors="coerce")
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True).sort_values("time_dt").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 1. Zero-Variance Alarm
# ---------------------------------------------------------------------------

def detect_zero_variance(
    df: pd.DataFrame,
    window_minutes: int = _ZERO_VARIANCE_MINUTES,
) -> list[dict]:
    """Flag factories reporting a perfectly flat/frozen COD reading.

    Args:
        df:             Full factory DataFrame from _load_factories().
        window_minutes: Tumbling window size in minutes.

    Returns:
        List of tamper evidence dicts (one per flagged window per factory).
    """
    records = []
    window_td = pd.Timedelta(minutes=window_minutes)

    for factory_id, group in df.groupby("factory_id"):
        clean = group.dropna(subset=["cod"]).sort_values("time_dt")
        if clean.empty:
            continue

        t_min = clean["time_dt"].min()
        t_max = clean["time_dt"].max()
        t     = t_min

        while t <= t_max:
            t_end = t + window_td
            window = clean[(clean["time_dt"] >= t) & (clean["time_dt"] < t_end)]
            t = t_end

            if len(window) < 2:
                continue

            cod_vals = window["cod"].values
            cod_range = float(np.nanmax(cod_vals) - np.nanmin(cod_vals))

            if cod_range < TINY_EPS:
                records.append({
                    "tamper_type": "ZERO_VARIANCE",
                    "factory_id":  factory_id,
                    "window_end":  window["time"].iloc[-1],
                    "cod_max":     round(float(np.nanmax(cod_vals)), 4),
                    "cod_min":     round(float(np.nanmin(cod_vals)), 4),
                    "cod_range":   round(cod_range, 6),
                    "row_count":   len(window),
                })

    return records


# ---------------------------------------------------------------------------
# 2. Chemical Fingerprint (dilution detection)
# ---------------------------------------------------------------------------

def detect_chemical_fingerprint(
    df: pd.DataFrame,
    window_minutes: int = 60,
    cod_drop: float = _COD_DROP_FRACTION,
    tss_stable: float = _TSS_STABLE_FRACTION,
) -> list[dict]:
    """Flag dilution tampering: COD drops sharply while TSS stays stable.

    Physical basis: Adding clean water drops COD (dissolved organics) but
    suspended solids (TSS) don't dissolve, so TSS stays high.

    Args:
        df:             Full factory DataFrame.
        window_minutes: Tumbling window for rolling COD/TSS averaging.
        cod_drop:       COD must drop by at least this fraction vs preceding window.
        tss_stable:     TSS must stay within (1 - tss_stable) of preceding window.

    Returns:
        List of tamper evidence dicts.
    """
    records = []
    window_td = pd.Timedelta(minutes=window_minutes)

    for factory_id, group in df.groupby("factory_id"):
        clean = group.dropna(subset=["cod", "tss"]).sort_values("time_dt")
        if len(clean) < 6:
            continue

        t_min = clean["time_dt"].min()
        t_max = clean["time_dt"].max()
        t     = t_min
        prev_cod = None
        prev_tss = None

        while t <= t_max:
            t_end  = t + window_td
            window = clean[(clean["time_dt"] >= t) & (clean["time_dt"] < t_end)]
            t      = t_end

            if len(window) < 3:
                continue

            mean_cod = float(window["cod"].mean())
            mean_tss = float(window["tss"].mean())

            if prev_cod is not None and prev_tss is not None:
                cod_threshold = prev_cod * (1.0 - cod_drop)     # e.g. 20% of prev
                tss_threshold = prev_tss * (1.0 - tss_stable)   # e.g. 80% of prev

                if (mean_cod <= cod_threshold) and (mean_tss >= tss_threshold):
                    records.append({
                        "tamper_type":  "DILUTION_TAMPER",
                        "factory_id":   factory_id,
                        "window_end":   window["time"].iloc[-1],
                        "mean_cod":     round(mean_cod, 2),
                        "mean_tss":     round(mean_tss, 2),
                        "baseline_cod": round(prev_cod, 2),
                        "baseline_tss": round(prev_tss, 2),
                    })

            prev_cod = mean_cod
            prev_tss = mean_tss

    return records


# ---------------------------------------------------------------------------
# 3. Guilt-by-Disconnection
# ---------------------------------------------------------------------------

def detect_guilt_by_disconnection(
    df: pd.DataFrame,
    window_minutes: int = _BLACKOUT_MIN_MINUTES,
    blackout_threshold: float = 0.80,
) -> list[dict]:
    """Flag strategic sensor blackouts (high ratio of NA/null COD rows).

    Factory D silences its sensor before dumping. This shows up as a window
    where ≥80% of rows have null COD, even though the factory is connected.

    Args:
        df:                  Full factory DataFrame (including null-COD rows).
        window_minutes:      Tumbling window size in minutes.
        blackout_threshold:  Fraction of null rows to trigger alarm.

    Returns:
        List of tamper evidence dicts.
    """
    records = []
    window_td = pd.Timedelta(minutes=window_minutes)

    for factory_id, group in df.groupby("factory_id"):
        # Use the raw group (including nulls) for blackout detection
        valid = group.dropna(subset=["time_dt"]).sort_values("time_dt")
        if valid.empty:
            continue

        t_min = valid["time_dt"].min()
        t_max = valid["time_dt"].max()
        t     = t_min

        while t <= t_max:
            t_end  = t + window_td
            window = valid[(valid["time_dt"] >= t) & (valid["time_dt"] < t_end)]
            t      = t_end

            if len(window) < window_minutes:
                continue

            total_rows   = len(window)
            blackout_rows = int(window["cod"].isna().sum())
            ratio         = blackout_rows / total_rows

            if ratio >= blackout_threshold:
                records.append({
                    "tamper_type":    "BLACKOUT_TAMPER",
                    "factory_id":     factory_id,
                    "window_end":     window["time"].iloc[-1],
                    "total_rows":     total_rows,
                    "blackout_rows":  blackout_rows,
                    "blackout_ratio": round(ratio, 3),
                })

    return records


# ---------------------------------------------------------------------------
# Unified runner
# ---------------------------------------------------------------------------

def run_all_detectors(factory_dir: str = _FACTORY_DATA_DIR) -> list[dict]:
    """Run all three anti-cheat detectors and return combined list of tamper records.

    Args:
        factory_dir: Directory containing factory CSV files.

    Returns:
        List of tamper dicts (combined from all 3 detectors), sorted by window_end.
    """
    df = _load_factories(factory_dir)
    log.info(
        "config loaded",
        extra={"factory_rows": len(df), "factories": df['factory_id'].nunique()},
    )

    zv  = detect_zero_variance(df)
    fp  = detect_chemical_fingerprint(df)
    gd  = detect_guilt_by_disconnection(df)

    all_records = zv + fp + gd
    all_records.sort(key=lambda r: r.get("window_end", ""))

    print(f"  [ANTI-CHEAT] Detections: "
          f"ZERO_VARIANCE={len(zv)}  "
          f"DILUTION_TAMPER={len(fp)}  "
          f"BLACKOUT_TAMPER={len(gd)}")

    return all_records

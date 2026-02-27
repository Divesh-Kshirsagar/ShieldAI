"""
SHIELD AI — Phase 0: Factory Data Simulation
=============================================

Reads the real CETP CSV (priya_cetp_i.csv) to obtain the master timeline,
then generates 4 synthetic factory CSVs that replicate the same 1-minute
timestamp sequence and NA-pattern (sensor fires every ~3 rows).

Factory profiles
----------------
factory_A : Normal operation.  COD baseline ~120 mg/L with Gaussian jitter.
factory_B : Shock-load event.  COD spikes to ~450 mg/L at T_shock - PIPE_TRAVEL_MINUTES
            (i.e. exactly 15 min before the known CETP spike) to test Phase 1 attribution.
factory_C : Zero-variance.  COD locked at a constant value for >5-min windows to test
            the v2 digital-tampering alarm.
factory_D : Blackout dump.  COD goes NA for a 20-min window at T_shock - PIPE_TRAVEL_MINUTES
            to test the v2 "Guilt by Disconnection" logic.

Output columns (for every factory CSV)
---------------------------------------
S. No | Time | factory_id | COD - (mg/l) Raw | BOD - (mg/l) Raw | pH - (pH) Raw | TSS - (mg/l) Raw

Run
---
    python src/simulate_factories.py
"""

import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# Allow import of constants even when run as __main__
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.constants import CETP_DATA_DIR, FACTORY_DATA_DIR, PIPE_TRAVEL_MINUTES

# ---------------------------------------------------------------------------
# Constants used only by the simulator
# ---------------------------------------------------------------------------

# Seed for reproducibility — makes demo deterministic
RNG_SEED: int = 42

# Known timestamps in priya_cetp_i.csv where CETP inlet COD spikes visibly
# (identified from manual inspection; COD ≈ 207 at 2026-02-01 12:23).
# We subtract PIPE_TRAVEL_MINUTES so factory_B's spike arrives at the CETP 15 min later.
KNOWN_CETP_SPIKE_TIMES: list[str] = [
    "2026-02-01 12:23",  # COD = 207.02 — moderate spike in real data
    "2026-02-01 11:35",  # COD = 199.69 — secondary spike
]

# Factory-level baselines (COD in mg/L)
FACTORY_COD_BASELINE: float = 120.0   # Normal operating range for industrial effluent
FACTORY_BOD_BASELINE: float = 45.0
FACTORY_PH_BASELINE: float = 7.2
FACTORY_TSS_BASELINE: float = 80.0

# Shock magnitudes for factory_B
SHOCK_COD_VALUE: float = 450.0
SHOCK_BOD_VALUE: float = 180.0
SHOCK_TSS_VALUE: float = 300.0
SHOCK_WINDOW_MINUTES: int = 6   # Duration of the spike plateau

# Zero-variance value for factory_C
ZERO_VAR_COD: float = 115.00   # Suspiciously round/flat value

# Blackout window duration for factory_D (minutes of NA)
BLACKOUT_WINDOW_MINUTES: int = 20


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_cetp_timeline(cetp_dir: str) -> pd.DataFrame:
    """Load the CETP master CSV and return just the timing skeleton.

    Returns a DataFrame with columns: ['S. No', 'Time', 'is_valid']
    where is_valid=True indicates rows where the real sensor fired (non-NA COD).
    """
    cetp_path = Path(cetp_dir) / "priya_cetp_i.csv"
    if not cetp_path.exists():
        raise FileNotFoundError(
            f"Real CETP data not found at '{cetp_path}'. "
            "Ensure priya_cetp_i.csv is present in data/cetp/."
        )

    df = pd.read_csv(cetp_path)

    # Rename the messy header columns to short names for internal use
    df = df.rename(columns={
        "S. No": "S. No",
        "Time": "Time",
        "CETP_INLET-COD - (mg/l) Raw": "cetp_cod",
    })

    # Mark rows where the real sensor fired (used to replicate the NA gap pattern)
    df["is_valid"] = df["cetp_cod"].apply(lambda v: str(v).strip().upper() != "NA")
    df["Time"] = df["Time"].astype(str).str.strip()

    return df[["S. No", "Time", "is_valid"]].copy()


def _base_factory_df(timeline: pd.DataFrame, factory_id: str, rng: np.random.Generator) -> pd.DataFrame:
    """Generate a factory DataFrame with Gaussian noise around the baselines.

    Rows that correspond to NA slots in the CETP data are also set to NA
    to faithfully replicate the real MPCB sensor firing cadence.
    """
    n = len(timeline)

    # NOTE: We mirror the CETP NA pattern — sensors fire every ~3 rows in the real data.
    # This keeps factory streams realistic and consistent with MPCB transmission specs.
    is_valid = timeline["is_valid"].values

    cod = np.where(is_valid,
                   rng.normal(FACTORY_COD_BASELINE, 3.0, n),
                   np.nan)
    bod = np.where(is_valid,
                   rng.normal(FACTORY_BOD_BASELINE, 1.5, n),
                   np.nan)
    ph  = np.where(is_valid,
                   rng.normal(FACTORY_PH_BASELINE, 0.05, n).clip(6.5, 8.5),
                   np.nan)
    tss = np.where(is_valid,
                   rng.normal(FACTORY_TSS_BASELINE, 5.0, n),
                   np.nan)

    return pd.DataFrame({
        "S. No":              timeline["S. No"].values,
        "Time":               timeline["Time"].values,
        "factory_id":         factory_id,
        "COD - (mg/l) Raw":   _fmt(cod),
        "BOD - (mg/l) Raw":   _fmt(bod),
        "pH - (pH) Raw":      _fmt(ph),
        "TSS - (mg/l) Raw":   _fmt(tss),
    })


def _fmt(arr: np.ndarray) -> list[str]:
    """Format float array as strings, replacing NaN with 'NA'."""
    return [f"{v:.2f}" if not np.isnan(v) else "NA" for v in arr]


def _inject_shock(df: pd.DataFrame, timeline: pd.DataFrame,
                  spike_times: list[str], travel_min: int,
                  rng: np.random.Generator) -> pd.DataFrame:
    """Inject COD/BOD/TSS spike into factory_B at T_factory = T_cetp_spike - travel_min.

    The spike plateau lasts SHOCK_WINDOW_MINUTES minutes.
    """
    df = df.copy()
    time_col = pd.to_datetime(df["Time"], format="%Y-%m-%d %H:%M", errors="coerce")
    delta = pd.Timedelta(minutes=travel_min)

    for cetp_spike in spike_times:
        t_factory = pd.Timestamp(cetp_spike) - delta
        t_end = t_factory + pd.Timedelta(minutes=SHOCK_WINDOW_MINUTES)

        mask = (time_col >= t_factory) & (time_col < t_end) & (df["COD - (mg/l) Raw"] != "NA")
        n_rows = mask.sum()
        if n_rows == 0:
            print(f"  [WARN] No valid rows found near {t_factory} for shock injection.")
            continue

        noise_cod = rng.normal(0, 5.0, n_rows)
        noise_bod = rng.normal(0, 3.0, n_rows)
        noise_tss = rng.normal(0, 10.0, n_rows)

        df.loc[mask, "COD - (mg/l) Raw"] = [f"{SHOCK_COD_VALUE + v:.2f}" for v in noise_cod]
        df.loc[mask, "BOD - (mg/l) Raw"] = [f"{SHOCK_BOD_VALUE + v:.2f}" for v in noise_bod]
        df.loc[mask, "TSS - (mg/l) Raw"] = [f"{SHOCK_TSS_VALUE + v:.2f}" for v in noise_tss]
        print(f"  [OK] factory_B shock injected: {t_factory} → {t_end} ({n_rows} rows)")

    return df


def _inject_zero_variance(df: pd.DataFrame, timeline: pd.DataFrame) -> pd.DataFrame:
    """Lock factory_C's COD to a flat constant for every valid reading.

    This simulates a software-capped sensor (digital tampering).
    """
    df = df.copy()
    mask = df["COD - (mg/l) Raw"] != "NA"
    df.loc[mask, "COD - (mg/l) Raw"] = f"{ZERO_VAR_COD:.2f}"
    return df


def _inject_blackout(df: pd.DataFrame, spike_times: list[str], travel_min: int) -> pd.DataFrame:
    """Replace factory_D's values with NA during a blackout window.

    NOTE: Blackout rows are retained (not dropped) — ingest.py tags these as
    BLACKOUT status for the v2 "Guilt by Disconnection" anti-cheat logic.
    """
    df = df.copy()
    time_col = pd.to_datetime(df["Time"], format="%Y-%m-%d %H:%M", errors="coerce")
    delta = pd.Timedelta(minutes=travel_min)

    for cetp_spike in spike_times:
        t_start = pd.Timestamp(cetp_spike) - delta - pd.Timedelta(minutes=5)
        t_end   = t_start + pd.Timedelta(minutes=BLACKOUT_WINDOW_MINUTES)

        mask = (time_col >= t_start) & (time_col < t_end)
        df.loc[mask, "COD - (mg/l) Raw"] = "NA"
        df.loc[mask, "BOD - (mg/l) Raw"] = "NA"
        df.loc[mask, "pH - (pH) Raw"]    = "NA"
        df.loc[mask, "TSS - (mg/l) Raw"] = "NA"
        print(f"  [OK] factory_D blackout: {t_start} → {t_end} ({mask.sum()} rows silenced)")

    return df


def _save(df: pd.DataFrame, out_dir: str, filename: str) -> None:
    """Write a factory CSV to the output directory."""
    out_path = Path(out_dir) / filename
    df.to_csv(out_path, index=False, quoting=1)  # quoting=1 = QUOTE_ALL, matches MPCB format
    print(f"  [SAVED] {out_path}  ({len(df)} rows)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def simulate(cetp_dir: str = CETP_DATA_DIR, out_dir: str = FACTORY_DATA_DIR) -> None:
    """Generate all 4 simulated factory CSVs.

    Args:
        cetp_dir: Directory containing priya_cetp_i.csv.
        out_dir:  Directory where factory_A/B/C/D.csv will be written.
    """
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(RNG_SEED)

    print("SHIELD AI — Simulating factory data")
    print(f"  Source CETP dir : {cetp_dir}")
    print(f"  Output dir      : {out_dir}")
    print(f"  Pipe travel time: {PIPE_TRAVEL_MINUTES} min\n")

    # Step 1: Load master timeline from real CETP CSV
    print("[1/5] Loading CETP timeline...")
    timeline = _load_cetp_timeline(cetp_dir)
    print(f"       {len(timeline)} rows loaded, "
          f"{timeline['is_valid'].sum()} valid sensor readings.\n")

    # Step 2: factory_A — normal operation
    print("[2/5] Generating factory_A (normal)...")
    fa = _base_factory_df(timeline, "FACTORY_A", rng)
    _save(fa, out_dir, "factory_A.csv")
    print()

    # Step 3: factory_B — shock load injected
    print("[3/5] Generating factory_B (shock-load)...")
    fb = _base_factory_df(timeline, "FACTORY_B", rng)
    fb = _inject_shock(fb, timeline, KNOWN_CETP_SPIKE_TIMES, PIPE_TRAVEL_MINUTES, rng)
    _save(fb, out_dir, "factory_B.csv")
    print()

    # Step 4: factory_C — zero-variance (digital tampering)
    print("[4/5] Generating factory_C (zero-variance)...")
    fc = _base_factory_df(timeline, "FACTORY_C", rng)
    fc = _inject_zero_variance(fc, timeline)
    _save(fc, out_dir, "factory_C.csv")
    print()

    # Step 5: factory_D — blackout window (strategic dump)
    print("[5/5] Generating factory_D (blackout)...")
    fd = _base_factory_df(timeline, "FACTORY_D", rng)
    fd = _inject_blackout(fd, KNOWN_CETP_SPIKE_TIMES, PIPE_TRAVEL_MINUTES)
    _save(fd, out_dir, "factory_D.csv")

    print("\n✅  All factory CSVs ready. Run the Pathway pipeline next.")


if __name__ == "__main__":
    simulate()

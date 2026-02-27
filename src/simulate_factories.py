"""
SHIELD AI — Phase 0: Factory Data Simulation
=============================================

Reads the real CETP CSV (priya_cetp_i.csv) to obtain the master timeline,
then generates 4 synthetic factory CSVs that replicate the same 1-minute
timestamp sequence and NA-pattern (sensor fires every ~3 rows).

Also produces a preprocessed CETP CSV with Pathway-compatible column names
(priya_cetp_clean.csv), since the raw MPCB export headers contain spaces,
hyphens, and unit strings that can't be used as Python identifiers.

Factory profiles
----------------
factory_A : Normal operation.  COD baseline ~120 mg/L with Gaussian jitter.
factory_B : Shock-load event.  COD spikes to ~450 mg/L at T_shock - PIPE_TRAVEL_MINUTES.
factory_C : Zero-variance.  COD locked at a constant value for >5-min windows.
factory_D : Blackout dump.  COD goes NA for a 20-min window at T_shock - PIPE_TRAVEL_MINUTES.

Output — CETP preprocessed CSV
--------------------------------
data/cetp/cetp_clean.csv  (Pathway reads this instead of the raw MPCB file)
Columns: s_no, time, cetp_inlet_cod, cetp_inlet_bod, cetp_inlet_ph, cetp_inlet_tss,
         cetp_outlet_cod, cetp_outlet_bod, cetp_outlet_ph, cetp_outlet_tss

Output — Factory CSVs
----------------------
data/factories/factory_A/B/C/D.csv
Columns: s_no, time, factory_id, cod, bod, ph, tss

Run
---
    uv run python src/simulate_factories.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Allow import of constants even when run as __main__
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.constants import CETP_DATA_DIR, FACTORY_DATA_DIR, PIPE_TRAVEL_MINUTES

# ---------------------------------------------------------------------------
# Constants used only by the simulator
# ---------------------------------------------------------------------------

RNG_SEED: int = 42

# Known CETP spike timestamps (manually identified from priya_cetp_i.csv inspection)
# factory_B will inject its COD spike at CETP_SPIKE_TIME - PIPE_TRAVEL_MINUTES
KNOWN_CETP_SPIKE_TIMES: list[str] = [
    "2026-02-01 12:23",  # CETP COD = 207.02 — moderate spike
    "2026-02-01 11:35",  # CETP COD = 199.69 — secondary spike
]

# Factory-level baselines
FACTORY_COD_BASELINE: float = 120.0
FACTORY_BOD_BASELINE: float = 45.0
FACTORY_PH_BASELINE: float  = 7.2
FACTORY_TSS_BASELINE: float = 80.0

# Shock magnitudes for factory_B
SHOCK_COD_VALUE: float      = 450.0
SHOCK_BOD_VALUE: float      = 180.0
SHOCK_TSS_VALUE: float      = 300.0
SHOCK_WINDOW_MINUTES: int   = 6

# Zero-variance value for factory_C
ZERO_VAR_COD: float         = 115.00

# Blackout window duration for factory_D
BLACKOUT_WINDOW_MINUTES: int = 20


# ---------------------------------------------------------------------------
# Step 0: Preprocess CETP CSV — rename MPCB long headers to clean schema names
# ---------------------------------------------------------------------------

# NOTE: Pathway 0.29.x requires Schema field names to exactly match CSV headers.
# Since MPCB exports have headers like "CETP_INLET-COD - (mg/l) Raw" (with spaces
# and hyphens), we write a preprocessed CSV with Pythonic column names that the
# Pathway Schema can reference directly. The raw file is never modified.
_CETP_RAW_COL_MAP: dict[str, str] = {
    "S. No":                           "s_no",
    "Time":                            "time",
    "CETP_INLET-COD - (mg/l) Raw":    "cetp_inlet_cod",
    "CETP_INLET-BOD - (mg/l) Raw":    "cetp_inlet_bod",
    "CETP_INLET-pH - (pH) Raw":       "cetp_inlet_ph",
    "CETP_INLET-TSS - (mg/l) Raw":    "cetp_inlet_tss",
    "CETP_OUTLET-COD - (mg/l) Raw":   "cetp_outlet_cod",
    "CETP_OUTLET-BOD - (mg/l) Raw":   "cetp_outlet_bod",
    "CETP_OUTLET-pH - (pH) Raw":      "cetp_outlet_ph",
    "CETP_OUTLET-TSS - (mg/l) Raw":   "cetp_outlet_tss",
}


def preprocess_cetp(cetp_dir: str) -> pd.DataFrame:
    """Load raw CETP CSV, rename columns, save a Pathway-compatible clean copy.

    Returns the cleaned DataFrame (used as the master timeline).

    Args:
        cetp_dir: Directory containing priya_cetp_i.csv.

    Returns:
        DataFrame with columns: s_no, time, cetp_inlet_cod, ... (all clean names)
    """
    raw_path   = Path(cetp_dir) / "priya_cetp_i.csv"
    clean_path = Path(cetp_dir) / "cetp_clean.csv"

    if not raw_path.exists():
        raise FileNotFoundError(
            f"Real CETP data not found at '{raw_path}'. "
            "Ensure priya_cetp_i.csv is in data/cetp/."
        )

    df = pd.read_csv(raw_path)
    df = df.rename(columns=_CETP_RAW_COL_MAP)

    # Keep only the mapped columns (drop any extra MPCB metadata columns)
    df = df[[c for c in _CETP_RAW_COL_MAP.values() if c in df.columns]].copy()

    # Replace "NA" strings with empty string so Pathway reads as null
    df.replace("NA", "", inplace=True)

    df.to_csv(clean_path, index=False)
    n_valid = (df["cetp_inlet_cod"] != "").sum()
    print(f"  [PREPROCESSED] {clean_path}  ({len(df)} rows, {n_valid} valid COD readings)")

    return df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_timeline(cetp_dir: str) -> pd.DataFrame:
    """Return master timeline with is_valid flag (True = sensor fired, COD non-NA)."""
    df = preprocess_cetp(cetp_dir)
    df["is_valid"] = df["cetp_inlet_cod"].apply(
        lambda v: str(v).strip() not in ("", "NA", "nan")
    )
    return df[["s_no", "time", "is_valid"]].copy()


def _fmt(arr: np.ndarray) -> list[str]:
    """Format float array as strings; NaN → empty string (Pathway reads as null)."""
    return [f"{v:.2f}" if not np.isnan(v) else "" for v in arr]


def _base_factory_df(timeline: pd.DataFrame, factory_id: str,
                     rng: np.random.Generator) -> pd.DataFrame:
    """Generate factory DataFrame with Gaussian noise, matching CETP NA-gap pattern."""
    n        = len(timeline)
    is_valid = timeline["is_valid"].values

    cod = np.where(is_valid, rng.normal(FACTORY_COD_BASELINE, 3.0, n),   np.nan)
    bod = np.where(is_valid, rng.normal(FACTORY_BOD_BASELINE, 1.5, n),   np.nan)
    ph  = np.where(is_valid, rng.normal(FACTORY_PH_BASELINE,  0.05, n).clip(6.5, 8.5), np.nan)
    tss = np.where(is_valid, rng.normal(FACTORY_TSS_BASELINE, 5.0, n),   np.nan)

    # NOTE: Factory CSVs use clean Pythonic column names (cod, bod, ph, tss)
    # so that the Pathway FactorySchema can match them without deprecated kwargs.
    return pd.DataFrame({
        "s_no":       timeline["s_no"].values,
        "time":       timeline["time"].values,
        "factory_id": factory_id,
        "cod":        _fmt(cod),
        "bod":        _fmt(bod),
        "ph":         _fmt(ph),
        "tss":        _fmt(tss),
    })


def _inject_shock(df: pd.DataFrame, spike_times: list[str],
                  travel_min: int, rng: np.random.Generator) -> pd.DataFrame:
    """Inject COD/BOD/TSS spike into factory_B at T = T_cetp_spike - travel_min."""
    df = df.copy()
    time_col = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M", errors="coerce")
    delta    = pd.Timedelta(minutes=travel_min)

    for cetp_spike in spike_times:
        t_factory = pd.Timestamp(cetp_spike) - delta
        t_end     = t_factory + pd.Timedelta(minutes=SHOCK_WINDOW_MINUTES)
        mask      = (time_col >= t_factory) & (time_col < t_end) & (df["cod"] != "")
        n_rows    = mask.sum()
        if n_rows == 0:
            print(f"  [WARN] No valid rows near {t_factory} for shock injection.")
            continue
        df.loc[mask, "cod"] = [f"{SHOCK_COD_VALUE + v:.2f}" for v in rng.normal(0, 5, n_rows)]
        df.loc[mask, "bod"] = [f"{SHOCK_BOD_VALUE + v:.2f}" for v in rng.normal(0, 3, n_rows)]
        df.loc[mask, "tss"] = [f"{SHOCK_TSS_VALUE + v:.2f}" for v in rng.normal(0, 10, n_rows)]
        print(f"  [OK] factory_B shock injected: {t_factory} → {t_end} ({n_rows} rows)")

    return df


def _inject_zero_variance(df: pd.DataFrame) -> pd.DataFrame:
    """Lock factory_C COD to a flat constant (digital tampering simulation)."""
    df = df.copy()
    df.loc[df["cod"] != "", "cod"] = f"{ZERO_VAR_COD:.2f}"
    return df


def _inject_blackout(df: pd.DataFrame, spike_times: list[str], travel_min: int) -> pd.DataFrame:
    """Empty factory_D values for a blackout window.

    NOTE: Blackout rows use empty strings (not 'NA') — Pathway reads these as null.
    ingest.py tags these rows as status='BLACKOUT' for v2 anti-cheat logic.
    """
    df = df.copy()
    time_col = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M", errors="coerce")
    delta    = pd.Timedelta(minutes=travel_min)

    for cetp_spike in spike_times:
        t_start = pd.Timestamp(cetp_spike) - delta - pd.Timedelta(minutes=5)
        t_end   = t_start + pd.Timedelta(minutes=BLACKOUT_WINDOW_MINUTES)
        mask    = (time_col >= t_start) & (time_col < t_end)
        for col in ("cod", "bod", "ph", "tss"):
            df.loc[mask, col] = ""
        print(f"  [OK] factory_D blackout: {t_start} → {t_end} ({mask.sum()} rows silenced)")

    return df


def _save(df: pd.DataFrame, out_dir: str, filename: str) -> None:
    out_path = Path(out_dir) / filename
    df.to_csv(out_path, index=False)
    print(f"  [SAVED] {out_path}  ({len(df)} rows)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def simulate(cetp_dir: str = CETP_DATA_DIR, out_dir: str = FACTORY_DATA_DIR) -> None:
    """Generate all 4 simulated factory CSVs and the preprocessed CETP CSV.

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

    # Step 0: Preprocess CETP CSV (rename raw MPCB headers → clean schema names)
    print("[0/5] Preprocessing CETP CSV (Pathway-compatible headers)...")
    timeline = _load_timeline(cetp_dir)
    print(f"       {len(timeline)} rows, {timeline['is_valid'].sum()} valid readings.\n")

    # factory_A — normal
    print("[2/5] Generating factory_A (normal)...")
    _save(_base_factory_df(timeline, "FACTORY_A", rng), out_dir, "factory_A.csv")
    print()

    # factory_B — shock-load
    print("[3/5] Generating factory_B (shock-load)...")
    fb = _base_factory_df(timeline, "FACTORY_B", rng)
    fb = _inject_shock(fb, KNOWN_CETP_SPIKE_TIMES, PIPE_TRAVEL_MINUTES, rng)
    _save(fb, out_dir, "factory_B.csv")
    print()

    # factory_C — zero-variance
    print("[4/5] Generating factory_C (zero-variance)...")
    fc = _base_factory_df(timeline, "FACTORY_C", rng)
    fc = _inject_zero_variance(fc)
    _save(fc, out_dir, "factory_C.csv")
    print()

    # factory_D — blackout
    print("[5/5] Generating factory_D (blackout)...")
    fd = _base_factory_df(timeline, "FACTORY_D", rng)
    fd = _inject_blackout(fd, KNOWN_CETP_SPIKE_TIMES, PIPE_TRAVEL_MINUTES)
    _save(fd, out_dir, "factory_D.csv")

    print("\n✅  All CSVs ready. Run the Pathway pipeline next.")


if __name__ == "__main__":
    simulate()

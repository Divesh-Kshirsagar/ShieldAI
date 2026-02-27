"""
SHIELD AI — Phase 1: Data Ingestion & Real-Time Cleaning
=========================================================

Provides two Pathway tables consumed by downstream modules:

    cetp_stream      : Clean CETP inlet readings (COD, BOD, pH, TSS as floats).
    factory_raw_stream: All factory rows including NA (BLACKOUT) context for v2.

Cleaning rules
--------------
1. Drop rows where COD == "NA" for the CETP stream (unusable for tripwire).
2. For factory streams: keep NA rows tagged with status="BLACKOUT" (v2 anti-cheat).
3. Rename long MPCB column headers to short uniform names on ingest.
4. Cast COD, BOD, pH, TSS columns to float (drop row on cast failure).

Pathway streaming mode
----------------------
mode="streaming" tails the CSV files as new rows are appended, replicating
a live MPCB feed when historical CSVs are played back sequentially.
"""

import pathway as pw
from pathlib import Path

from src.constants import CETP_DATA_DIR, FACTORY_DATA_DIR


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

class CETPSchema(pw.Schema):
    """Schema for priya_cetp_i.csv — raw MPCB column names with unit suffixes."""

    # NOTE: Pathway schema field names must match CSV headers exactly.
    # The long names are an artifact of the MPCB export format and will be
    # aliased to short names immediately after read.
    s_no: int = pw.column_definition(primary_key=True)
    time: str
    cetp_inlet_cod: float | None  # mapped from "CETP_INLET-COD - (mg/l) Raw"
    cetp_inlet_bod: float | None
    cetp_inlet_ph:  float | None
    cetp_inlet_tss: float | None
    cetp_outlet_cod: float | None
    cetp_outlet_bod: float | None
    cetp_outlet_ph:  float | None
    cetp_outlet_tss: float | None


class FactorySchema(pw.Schema):
    """Schema for simulated factory CSVs."""

    s_no:       int = pw.column_definition(primary_key=True)
    time:       str
    factory_id: str
    cod:        float | None  # COD - (mg/l) Raw
    bod:        float | None  # BOD - (mg/l) Raw
    ph:         float | None  # pH  - (pH) Raw
    tss:        float | None  # TSS - (mg/l) Raw


# ---------------------------------------------------------------------------
# Column name mapping (long MPCB names → short internal names)
# ---------------------------------------------------------------------------

# Maps raw CSV header to the expected pw.Schema field name.
_CETP_COL_MAP: dict[str, str] = {
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

_FACTORY_COL_MAP: dict[str, str] = {
    "S. No":              "s_no",
    "Time":               "time",
    "factory_id":         "factory_id",
    "COD - (mg/l) Raw":   "cod",
    "BOD - (mg/l) Raw":   "bod",
    "pH - (pH) Raw":      "ph",
    "TSS - (mg/l) Raw":   "tss",
}


# ---------------------------------------------------------------------------
# CETP stream
# ---------------------------------------------------------------------------

def load_cetp_stream(cetp_dir: str = CETP_DATA_DIR) -> pw.Table:
    """Read CETP CSV files in streaming mode and return a clean inlet stream.

    Performs:
        - Column rename from MPCB long names to short schema fields.
        - Filter: drop rows where cetp_inlet_cod is None (sensor gap / NA).
        - Result contains only CETP inlet readings (outlet columns excluded
          downstream; they remain in the table for optional use).

    Args:
        cetp_dir: Directory path containing the CETP CSV file(s).

    Returns:
        Pathway Table with schema matching CETPSchema, NA-COD rows removed.
    """
    if not Path(cetp_dir).exists():
        raise FileNotFoundError(
            f"CETP data directory not found: '{cetp_dir}'. "
            "Run from the project root or set CETP_DATA_DIR env var."
        )

    # NOTE: value_columns maps raw CSV headers → schema field names.
    # "NA" string values are automatically treated as None by Pathway's CSV reader
    # when the schema declares the field as float | None.
    raw: pw.Table = pw.io.csv.read(
        cetp_dir,
        schema=CETPSchema,
        mode="streaming",
        csv_settings=pw.io.csv.CsvParserSettings(
            delimiter=",",
        ),
        # Map long MPCB column names to our compact schema field names
        value_columns=list(_CETP_COL_MAP.values()),
        column_names=_CETP_COL_MAP,
        autocommit_duration_ms=1_000,
    )

    # Filter: keep only rows where the CETP inlet COD sensor fired
    # (non-null = valid sensor reading every ~3 minutes in the MPCB data)
    cetp_clean: pw.Table = raw.filter(pw.this.cetp_inlet_cod.is_not_none())

    return cetp_clean


# ---------------------------------------------------------------------------
# Factory streams
# ---------------------------------------------------------------------------

def load_factory_streams(factory_dir: str = FACTORY_DATA_DIR) -> pw.Table:
    """Read all factory CSVs in streaming mode into a unified tagged table.

    Each factory CSV in factory_dir is read individually; rows are tagged with
    their factory_id (embedded in the CSV itself by simulate_factories.py).

    Two variants are returned inside the same table:
        status = "NORMAL"   — valid COD reading (cod is not None)
        status = "BLACKOUT" — sensor gap / NA row (retained for v2 anti-cheat)

    Args:
        factory_dir: Directory path containing factory_A/B/C/D.csv files.

    Returns:
        Pathway Table with FactorySchema fields plus a `status` string column.
    """
    if not Path(factory_dir).exists():
        raise FileNotFoundError(
            f"Factory data directory not found: '{factory_dir}'. "
            "Run src/simulate_factories.py first."
        )

    raw: pw.Table = pw.io.csv.read(
        factory_dir,
        schema=FactorySchema,
        mode="streaming",
        csv_settings=pw.io.csv.CsvParserSettings(
            delimiter=",",
        ),
        value_columns=list(_FACTORY_COL_MAP.values()),
        column_names=_FACTORY_COL_MAP,
        autocommit_duration_ms=1_000,
    )

    # Tag each row with NORMAL or BLACKOUT status.
    # NOTE: BLACKOUT rows are *not* dropped here — they flow to anti_cheat.py (v2).
    # For Phase 1, only NORMAL rows participate in backtrack joins.
    factory_with_status: pw.Table = raw.with_columns(
        status=pw.if_else(
            pw.this.cod.is_not_none(),
            pw.cast(str, "NORMAL"),
            pw.cast(str, "BLACKOUT"),
        )
    )

    return factory_with_status


def load_clean_factory_stream(factory_dir: str = FACTORY_DATA_DIR) -> pw.Table:
    """Convenience wrapper: factory stream with BLACKOUT rows removed.

    Used by Phase 1 (backtrack.py) which only needs valid COD readings.
    Anti-cheat logic (v2) should use load_factory_streams() to retain context.

    Args:
        factory_dir: Directory path containing factory CSV files.

    Returns:
        Pathway Table containing only NORMAL (non-NA) factory rows.
    """
    full = load_factory_streams(factory_dir)
    return full.filter(pw.this.cod.is_not_none())

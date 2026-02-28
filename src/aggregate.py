"""
SHIELD AI — Phase 1: Stream Unification (The Aggregator)
=========================================================

Reads factory streams from ingest.py and:
    1. Auto-tags each row with its factory_id (already embedded in factory CSVs).
    2. Concatenates all factory tables into one unified Industrial Discharge Stream.

The Industrial Discharge Stream is the single Pathway table consumed by
backtrack.py for the temporal join step.

Usage
-----
    from src.aggregate import build_industrial_stream
    industrial_stream = build_industrial_stream()
"""

import pathway as pw

from src.ingest import load_factory_streams, load_clean_factory_stream
from src.config import CONFIG as _cfg

_FACTORY_DATA_DIR: str = _cfg.factory_data_directory


def build_industrial_stream(
    factory_dir: str = _FACTORY_DATA_DIR,
    include_blackout: bool = False,
) -> pw.Table:
    """Build the unified Industrial Discharge Stream from all factory CSVs.

    In the real MPCB deployment each factory would be a separate data source;
    here we read them from the factory_dir directory and union them via
    Pathway's concat_reindex so the stream engine sees one logical table.

    Args:
        factory_dir:      Directory containing factory_A/B/C/D.csv files.
        include_blackout: If True, retain BLACKOUT (NA) rows in the stream.
                          Set True for v2 anti-cheat logic; False (default)
                          for Phase 1 clean attribution joins.

    Returns:
        Unified Pathway Table with columns:
            s_no, time, factory_id, cod, bod, ph, tss, status
    """
    if include_blackout:
        # NOTE: Full stream including NA rows — used by anti_cheat.py (v2).
        # Do NOT use this for the asof_join in backtrack.py (floats only).
        raw_stream = load_factory_streams(factory_dir)
    else:
        # Phase 1 default: clean stream, BLACKOUT rows excluded.
        raw_stream = load_clean_factory_stream(factory_dir)

    # NOTE: factory_id is already embedded in each CSV row by simulate_factories.py.
    # In a live MPCB deployment, factory_id would be derived from the authenticated
    # site_id field in the MPCB API auth header (see api.py).

    # Pathway concat_reindex re-assigns internal row IDs across all source tables,
    # producing a single monotonic stream safe for temporal joins.
    # (For a single directory read, pw.io.csv.read already unions the files,
    #  so here we simply surface the table with correct column documentation.)
    industrial_stream: pw.Table = raw_stream

    return industrial_stream


def get_factory_ids(industrial_stream: pw.Table) -> pw.Table:
    """Return a deduplicated table of all factory_id values seen in the stream.

    Useful for the Streamlit dashboard to populate factory filter dropdowns.

    Args:
        industrial_stream: Output of build_industrial_stream().

    Returns:
        Pathway Table with a single column: factory_id.
    """
    # NOTE: Pathway's groupby on a streaming table produces a live-updating
    # table — new factory_ids appearing in the stream are automatically added.
    return industrial_stream.groupby(pw.this.factory_id).reduce(
        factory_id=pw.this.factory_id
    )

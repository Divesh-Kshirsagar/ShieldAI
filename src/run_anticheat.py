"""
SHIELD AI — Phase 4: Anti-Cheat Pipeline Entry Point
=====================================================

Runs all three anti-cheat detectors on factory data (pandas-based,
consistent with the backtrack.py approach) and writes results to
data/alerts/tamper_log.jsonl.

    uv run python src/run_anticheat.py
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.anti_cheat import run_all_detectors
from src.constants import FACTORY_DATA_DIR, TAMPER_LOG_PATH


def run_anticheat(factory_dir: str = FACTORY_DATA_DIR) -> None:
    """Run all detectors and write tamper_log.jsonl."""
    print("SHIELD AI — Phase 4 Anti-Cheat Engine")
    print(f"  Factory source : {factory_dir}")
    print(f"  Output log     : {TAMPER_LOG_PATH}\n")

    records = run_all_detectors(factory_dir)

    Path(TAMPER_LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(TAMPER_LOG_PATH, "w", encoding="utf-8") as f:
        for rec in records:
            rec["logged_at"] = datetime.now(tz=timezone.utc).isoformat()
            f.write(json.dumps(rec) + "\n")
            print(
                f"[TAMPER] {rec['tamper_type']} | "
                f"Factory: {rec['factory_id']} | "
                f"Window: {rec.get('window_end', '?')}"
            )

    print(f"\n✅  {len(records)} tamper events written to {TAMPER_LOG_PATH}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="SHIELD AI Phase 4 Anti-Cheat runner")
    parser.add_argument("--factory-dir", default=FACTORY_DATA_DIR)
    args = parser.parse_args()
    run_anticheat(factory_dir=args.factory_dir)

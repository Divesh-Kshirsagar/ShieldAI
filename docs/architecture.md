# Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                   │
│  priya_cetp_i.csv (real)   factory_A/B/C/D.csv (simulated)     │
└────────────────────┬─────────────────────────┬─────────────────┘
                     │                         │
             ┌───────▼───────┐         ┌───────▼───────┐
             │   ingest.py   │         │   ingest.py   │
             │ MPCB col rename│        │ NA → BLACKOUT │
             │ NA → null float│        └───────┬───────┘
             └───────┬───────┘                 │
                     │                         │
             ┌───────▼───────┐         ┌───────▼───────┐
             │  tripwire.py  │         │  aggregate.py │
             │ COD ≥ threshold│        │ unified stream │
             │ → shock_events │        └───────┬───────┘
             └───────┬───────┘                 │
                     │                         │
             ┌───────▼─────────────────────────▼───────┐
             │                alert.py                  │
             │  pw.io.subscribe callback:                │
             │    backtrack.attribute_event()           │
             │    → evidence_log.jsonl (append-only)    │
             └───────────────────┬──────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │        app.py           │
                    │    Streamlit dashboard  │
                    │  (reads JSONL + CSVs)   │
                    └─────────────────────────┘
```

## Module Responsibilities

| Module | Role | Key Output |
|---|---|---|
| `simulate_factories.py` | Generate 4 test factory CSVs from CETP timeline | `data/factories/`, `data/cetp/cetp_clean.csv` |
| `ingest.py` | Pathway streaming read, MPCB column rename, NA/BLACKOUT tagging | `pw.Table` (CETP + factory) |
| `aggregate.py` | Convenience wrapper to unify factory stream | `pw.Table` |
| `tripwire.py` | COD threshold filter → shock events | `pw.Table` (shock_events) |
| `backtrack.py` | Pandas factory index + nearest-timestamp attribution | `dict` (evidence record) |
| `alert.py` | `pw.io.subscribe` callback: runs backtrack, writes JSONL, fires webhook | `evidence_log.jsonl` |
| `anti_cheat.py` | v2 stubs: zero-variance, fingerprint, blackout detection | `pw.Table` (tamper_events) |
| `api.py` | FastAPI stub of MPCB Open API v2.3 (8 endpoints) | REST API |
| `app.py` | Streamlit dashboard | UI |
| `constants.py` | Single source of truth for all tuneable parameters | — |

## Data Flow: Attribution

```
CETP spike detected at T
        │
        ▼
T_backtrack = T − PIPE_TRAVEL_MINUTES (15 min)
        │
        ▼
Scan factory_index for rows where
  T_backtrack − 2min ≤ factory.time ≤ T_backtrack + 2min
        │
        ▼
Select row with MAX(factory.cod)
        │
        ▼
Emit evidence record → evidence_log.jsonl
```

## Streaming Design

SHIELD AI uses **Pathway** as the streaming engine for the CETP side:

- `pw.io.csv.read(..., mode="streaming")` tails the CETP CSV like a live sensor feed
- `pw.io.subscribe()` fires a Python callback for every new shock event
- The callback performs attribution via a pre-loaded pandas DataFrame (fast, deterministic)

Factory data is loaded eagerly as a static pandas index because factory CSVs are historical files, not live feeds. In a live deployment with MPCB-connected sensors for each factory, the join would move into Pathway using `pw.temporal.asof_join_left` on a merged stream (see `backtrack.py` for the upgrade path).

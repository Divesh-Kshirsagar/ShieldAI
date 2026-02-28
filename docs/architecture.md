# Architecture

## System Overview

```mermaid
flowchart TD
    A["priya_cetp_i.csv (real)"] --> B["ingest.py\nMPCB col rename\nNA → null float"]
    C["factory_A/B/C/D.csv (simulated)"] --> D["ingest.py\nNA → BLACKOUT"]
    B --> E["tripwire.py\nCOD ≥ threshold\n→ shock_events"]
    D --> F["aggregate.py\nunified stream"]
    F --> G["backtrack.py\nbuild_factory_index()"]
    E --> H["alert.py\npw.io.subscribe callback\nattribute_event()"]
    G --> H
    H --> I["evidence_log.jsonl\nappend-only"]
    I --> J["app.py\nStreamlit dashboard"]
```

## Data Flow: Attribution

```mermaid
flowchart LR
    A["CETP spike detected at T"] --> B["T_backtrack = T − 15 min"]
    B --> C["Scan factory_index\nfor rows within ±2 min"]
    C --> D{"Any matches?"}
    D -- Yes --> E["Select MAX cod row\n→ attributed_factory"]
    D -- No --> F["attributed_factory = None"]
    E --> G["Append to\nevidence_log.jsonl"]
    F --> G
```

## Streaming Design

```mermaid
flowchart LR
    subgraph Pathway["Pathway Streaming Graph"]
        P1["pw.io.csv.read\nmode=streaming\nCETP CSV"] --> P2["tripwire filter\ncod >= threshold"]
        P2 --> P3["pw.io.subscribe\ncallback"]
    end

    subgraph Pandas["Pandas (Eager, Static)"]
        Q1["build_factory_index()\nloads all factory CSVs once"] --> Q2["attribute_event()\nfor each shock event"]
    end

    P3 --> Q2
    Q2 --> R["evidence_log.jsonl"]
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
| `anti_cheat.py` | Pandas tumbling-window: zero-variance, fingerprint, blackout detection | `tamper_log.jsonl` |
| `api.py` | FastAPI stub of MPCB Open API v2.3 (8 endpoints) | REST API |
| `app.py` | Streamlit dashboard | UI |
| `constants.py` | Single source of truth for all tuneable parameters | — |

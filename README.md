# SHIELD AI

**Real-time industrial discharge anomaly detection for CETP (Common Effluent Treatment Plant) compliance monitoring.**

SHIELD AI ingests 1-minute frequency sensor streams, detects COD shock-load events at the CETP inlet, reverse-temporally joins the factory discharge streams to pinpoint the rogue factory, and surfaces evidence via a Streamlit dashboard and an un-falsifiable JSONL audit log.

---

## Quick Start

```bash
# 1. Install dependencies
uv sync

# 2. Generate simulated factory data (run once)
python src/simulate_factories.py

# 3. Start the Phase 1 attribution pipeline
python src/run_pipeline.py

# 4. In a second terminal, start the dashboard
streamlit run app.py

# 5. (Optional) Start the MPCB API stub
uvicorn src.api:app --reload
# Swagger docs → http://localhost:8000/docs
```

---

## Project Structure

```
shield_ai/
├── data/
│   ├── cetp/             # Real government CETP CSV data
│   └── factories/        # Simulated factory streams (generated)
├── src/
│   ├── constants.py      # All tuneable parameters
│   ├── simulate_factories.py
│   ├── ingest.py         # Pathway streaming reader + NA filter
│   ├── aggregate.py      # Unified industrial discharge stream
│   ├── tripwire.py       # COD anomaly detection
│   ├── backtrack.py      # Temporal asof_join attribution
│   ├── anti_cheat.py     # v2 tamper detection stubs
│   ├── alert.py          # JSONL evidence log + webhook/email/PDF
│   ├── api.py            # MPCB API v2.3 FastAPI stub
│   └── run_pipeline.py   # Pathway pipeline entry point
├── app.py                # Streamlit dashboard
├── docs/                 # MkDocs documentation
└── pyproject.toml
```

---

## Configuration

All tuneable parameters live in `src/constants.py` and can be overridden via environment variables (`.env` file supported):

| Variable | Default | Description |
|---|---|---|
| `COD_BASELINE` | 193.0 | Empirical CETP COD mean (mg/L) |
| `COD_THRESHOLD` | 250.0 | Shock-load trigger threshold (mg/L) |
| `PIPE_TRAVEL_MINUTES` | 15 | Factory→CETP pipe travel time |
| `SHIELD_WEBHOOK_URL` | *(blank)* | Alert webhook endpoint |

---

## Data

| File | Status | Description |
|---|---|---|
| `data/cetp/priya_cetp_i.csv` | **Real** | CETP inlet/outlet sensor, Feb 2026, 18 K rows |
| `data/factories/factory_A.csv` | Simulated | Normal baseline factory |
| `data/factories/factory_B.csv` | Simulated | Shock-load event (tests Phase 1 attribution) |
| `data/factories/factory_C.csv` | Simulated | Zero-variance COD (tests v2 digital tampering alarm) |
| `data/factories/factory_D.csv` | Simulated | Blackout window (tests v2 guilt-by-disconnection) |

---

## Phases

| Phase | Status | Description |
|---|---|---|
| Phase 0 | ✅ | Data simulation |
| Phase 1 | ✅ | Core engine (ingest → tripwire → backtrack → alert) |
| Phase 2 | ✅ | Streamlit dashboard + MPCB API stub |
| Phase 3 | 🔲 | Polish, email/PDF reports, MkDocs |
| Phase 4 (v2) | 🔲 | Anti-cheating mechanisms |

---

## Tech Stack

- [Pathway](https://pathway.com) — streaming data engine
- [Pandas / NumPy](https://pandas.pydata.org) — data wrangling
- [Streamlit](https://streamlit.io) — dashboard
- [FastAPI](https://fastapi.tiangolo.com) — MPCB API stub
- [MkDocs](https://www.mkdocs.org) — documentation

---

## Disclaimer

This is a **prototype/demo**. Factory data is entirely simulated from the real CETP baseline. The MPCB API endpoints match the v2.3 specification structure but make no live connections.

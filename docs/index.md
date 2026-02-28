# SHIELD AI

**Real-time industrial discharge anomaly detection for CETP compliance.**

SHIELD AI monitors 1-minute frequency sensor streams from a Common Effluent Treatment Plant (CETP), detects COD shock-load events, reverse-temporally joins factory discharge streams to pinpoint the rogue factory, and surfaces evidence via a Streamlit dashboard and an un-falsifiable JSONL audit log.

---

## Quick Start

```bash
uv sync                                    # install dependencies
uv run python src/simulate_factories.py    # generate factory data (once)
uv run python src/run_pipeline.py          # start attribution pipeline
uv run streamlit run app.py               # dashboard → localhost:8501
uv run uvicorn src.api:app --reload        # MPCB API stub → localhost:8000/docs
```

---

## Key Claims

| Claim | Status |
|---|---|
| Detects CETP COD breach in real time | ✅ verified (206 breach events in real data) |
| Attributes discharge to upstream factory | ✅ verified (factory_B at 447 mg/L, T-15min) |
| Detects digital tampering (zero-variance) | 🔲 v2 stub in `anti_cheat.py` |
| MPCB API v2.3 compliance shape | ✅ mirrored in `api.py` |

---

## Documentation

- [Architecture](architecture.md) — system design and data flow
- [Methodology](methodology.md) — approach and design decisions
- [Assumptions](assumptions.md) — explicit engineering assumptions
- [Math](math.md) — equations and derivations
- [ML Roadmap](ml_roadmap.md) — future ML extensions
- [MPCB API Reference](MpcbApi.md) — original API spec

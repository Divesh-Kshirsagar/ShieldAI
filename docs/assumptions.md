# Assumptions

This page documents every explicit engineering assumption made in the Phase 1 prototype. Each assumption should be revisited before a production deployment.

---

## A1 — Fixed Pipe Travel Time

> **Assumed value:** 15 minutes (`PIPE_TRAVEL_MINUTES` in `constants.py`)

**Basis:** Expert estimate for a typical CETP catchment area where factories are within 1–3 km of the CETP inlet via gravity-fed sewers at ~0.6 m/s flow velocity.

**Impact:** Every temporal backtrack is shifted by exactly this value. An incorrect estimate shifts all attributions earlier or later, potentially pointing at the wrong factory or missing the event window.

**Production fix (v2):** Replace with a dynamic calculation:
```
T_travel = Σ(pipe_segment_length / flow_velocity_from_SCADA)
```
derived from GIS pipe network data and real-time flow-rate sensors at junctions.

---

## A2 — COD is the Primary Detection Metric

> **Assumed:** BOD, pH, TSS are available but only COD drives the Tripwire alarm.

**Basis:** COD has the fastest response time at 1-minute sensor cadence. BOD requires a 5-day incubation (lab measurement), making it unsuitable for real-time use. pH and TSS are retained for corroborating evidence but not for primary detection.

**Production fix:** Multi-parameter weighted alarm (COD × 0.6 + TSS × 0.3 + pH deviation × 0.1).

---

## A3 — Factory with Highest COD at T_backtrack is the Culprit

> **Assumed:** Among all factories with readings in the ±2 min window, the one with the maximum COD value is attributed.

**Basis:** A factory dumping illegally will register a dramatically elevated COD relative to the normal baseline (~120 mg/L). The greedy max rule identifies the clearest statistical outlier.

**Limitation:** If two factories dump simultaneously, only the higher one is attributed.

**Production fix (v2):** Proportional discharge weighting using:
- Pipe diameter and connectivity matrix from GIS
- Permitted volumetric discharge quotas from MPCB licence database

---

## A4 — Sensor Fires Every ~3 Rows

> **Assumed:** The MPCB SCADA transmits a valid reading every ~3 minutes (not every minute), with NA gaps in between.

**Basis:** Observed in `priya_cetp_i.csv` — out of 18,781 rows at 1-min frequency, only ~6,185 have valid COD readings (~1 per 3 rows). This mirrors the MPCB spec where real-time transmission can be interrupted by communication gaps.

**Impact:** Simulated factory CSVs replicate this pattern so test data is representative.

---

## A5 — The CETP Processes All Upstream Factory Waste

> **Assumed:** Every factory in the catchment area is connected to the same CETP sewer network and their effluent contributes to the CETP inlet readings.

**Basis:** Standard CETP design — factories within the industrial estate are mandated to connect to the CETP rather than discharge directly.

**Limitation:** Satellite/illegal discharge directly to water bodies bypasses the CETP entirely and is not detectable by this system.

---

## A6 — Simulated Factories Cover the Same Date Range as Real CETP Data

> **Assumed:** Factory CSVs span Feb 1–24 2026 (same as `priya_cetp_i.csv`), generated from the same timestamp index.

**Basis:** `simulate_factories.py` reads the CETP timeline first and generates factory rows for every CETP timestamp.

**Production fix:** In live deployment, factory streams would arrive asynchronously with their own timestamps and would be merged into a unified stream via Pathway's `pw.io.csv.read()` on the factory sewer monitoring network.

---

## A7 — `evidence_log.jsonl` is Tamper-Evident by Append-Only Design

> **Assumed:** The JSONL file, once written, is not modified. Each record is self-contained.

**Basis:** Python's `open(..., "a")` appends a new JSON line per event. Existing lines are never overwritten.

**Production hardening:** Cryptographic signing of each record using a Hardware Security Module (HSM), Merkle tree chaining, or write to an immutable ledger (e.g., blockchain anchor).

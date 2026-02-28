# Methodology

## Approach: Historical Data as a Live Stream

SHIELD AI demonstrates a key principle of stream processing: **historical CSV data replayed as if it were live**. This is valid for prototyping because:

1. The Pathway `mode="streaming"` CSV reader tails files line-by-line, faithfully mimicking a live MPCB sensor feed
2. All temporal operators (window joins, thresholds) use *event time* (the `time` column in the data), not wall-clock time — so replaying 13 days of data at filesystem speed produces correct results
3. The evidence log is append-only — exactly as it would be in production

This approach means the prototype can be demonstrated without any live sensor hardware, while the Pathway graph is **production-ready** for a live feed: simply point `CETP_DATA_DIR` at a directory where a live MPCB dump script is writing new rows.

---

## Phase 1: Tripwire Detection

**Why COD only?**

COD (Chemical Oxygen Demand) is the primary statutory compliance parameter under India's MPCB effluent discharge norms. BOD is slower and less sensitive at 1-minute resolution. TSS is used as a secondary confirmation signal but is not the primary alarm trigger.

**Threshold selection:**

The demo threshold of `200 mg/L` was chosen because:
- The real CETP baseline is ~193 mg/L (mean from priya_cetp_i.csv)
- The real data maximum is ~230 mg/L
- A value of 200 catches the 206 genuine spikes without being below baseline

In production, regulatory thresholds are typically 450 mg/L for treated effluent discharge. Adjust `COD_THRESHOLD` in `constants.py` or via environment variable.

---

## Phase 1: Temporal Backtracking

The attribution model relies on a **fixed pipe travel time** (15 minutes by default). When a CETP spike is detected at time T:

1. Compute `T_backtrack = T − 15 min`
2. Search all factory discharge records within a `±2 min` tolerance window
3. Select the factory with the **maximum COD** in that window as the attributed source

This is a greedy, non-probabilistic attribution — intentionally simple for v1. The v2 upgrade path uses statistical weighting and chemical fingerprint matching (see `anti_cheat.py`).

---

## Factory Simulation Design

Since only one real CETP CSV is available, four synthetic factory profiles were generated using the same 1-minute timestamp skeleton:

| Factory | Profile | Test Target |
|---|---|---|
| A | Normal baseline (Gaussian, σ=3 mg/L COD) | False negative rate |
| B | Shock-load (450 mg/L spike at T-15min) | Phase 1 attribution accuracy |
| C | Zero-variance (constant 115.00 mg/L) | v2 digital tampering alarm |
| D | Blackout (20-min NA window at T-15min) | v2 guilt-by-disconnection |

The NA gap pattern (sensor fires every ~3 rows in the real data) is replicated in all factory CSVs to maintain MPCB transmission spec consistency.

---

## Streaming vs Batch

| Concern | SHIELD AI approach |
|---|---|
| Throughput | Pathway batch-and-stream hybrid — handles 18K+ rows in seconds |
| Latency | Configurable `autocommit_duration_ms` (default 1000ms) |
| Fault tolerance | JSONL log is append-only; Pathway persists state between restarts |
| Backpressure | Pathway's internal scheduler handles CSV tail lag |

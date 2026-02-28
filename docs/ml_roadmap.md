# ML Roadmap

Phase 1 uses deterministic rule-based detection. This page outlines the planned ML extensions for v2 and beyond.

---

## ML-1: Anomaly Scoring with Isolation Forest

**Problem:** The threshold-based tripwire fires on all COD readings ≥ θ, regardless of whether the spike is part of a seasonal pattern or a genuine illegal dump.

**Approach:**
- Train an `IsolationForest` (scikit-learn) on the pre-existing 13 days of CETP data
- Score each incoming CETP reading; outlier score > 0.5 → fire the alarm
- Advantage: adapts to natural COD variability (monsoon vs dry season)

**Data requirement:** 90+ days of baseline CETP data (currently have 13 days)

```python
from sklearn.ensemble import IsolationForest

model = IsolationForest(contamination=0.02, random_state=42)
model.fit(cetp_baseline[["cetp_inlet_cod", "cetp_inlet_tss"]])
scores = model.decision_function(live_window)
```

---

## ML-2: Multi-Factory Attribution Probability

**Problem:** The greedy `max(factory.COD)` rule fails when multiple factories dump simultaneously.

**Approach:**
- Train a multi-label classifier on historical (factory, CETP spike) pairs
- Features: `[factory_cod, factory_tss, factory_bod, pipe_distance, time_of_day, day_of_week]`
- Output: probability distribution over factories for each CETP event

**Model:** Gradient Boosted Trees (XGBoost) or a multinomial logistic regression baseline

---

## ML-3: Dynamic Pipe Travel Time Prediction

**Problem:** Pipe travel time is fixed at 15 min (v1 assumption). Flow rate varies with upstream load, season, and rainfall.

**Approach:**
- Collect flow-rate sensor readings from junction boxes
- Train a regression model: `τ_travel = f(flow_rate, pipe_fill_level, rainfall_mm)`
- Replace the constant in `constants.py` with the model's live prediction

**Data requirement:** Junction flow-rate sensors (currently not available in the prototype)

---

## ML-4: Zero-Variance Detection via CUSUM

**Problem:** The v2 zero-variance rule (σ = 0) only catches perfectly flat sensors. A more subtle tamper gradually reduces COD to avoid detection.

**Approach:**
- CUSUM (Cumulative Sum) change-point detection on factory COD time series
- Flags any sustained downward trend that is statistically inconsistent with the baseline

```python
from ruptures import Pelt
model = Pelt(model="rbf").fit(factory_cod_series)
changepoints = model.predict(pen=10)
```

---

## ML-5: Seasonal Baseline Adaptation

**Problem:** `COD_BASELINE = 193.0` is computed from one month of data. The true baseline shifts with industrial cycles, monsoon dilution, and upstream CETP load.

**Approach:**
- Maintain a rolling 30-day exponential moving average of CETP baseline COD
- Update `COD_BASELINE` dynamically in the Pathway graph via `pw.apply_with_context`

---

## Implementation Priority

| Phase | Feature | Complexity | Impact |
|---|---|---|---|
| v2 | Zero-variance CUSUM (ML-4) | Low | High |
| v2 | Dynamic baseline (ML-5) | Low | Medium |
| v3 | Isolation Forest scoring (ML-1) | Medium | High |
| v3 | Attribution probabilities (ML-2) | High | Very High |
| v3 | Dynamic pipe travel time (ML-3) | High | High |

# Mathematical Foundations

## 1. COD Threshold Exceedance (Tripwire)

The Tripwire emits a *ShockLoadEvent* when:

$$
\text{COD}_{\text{CETP,inlet}}(t) \geq \theta
$$

where:

- $\text{COD}_{\text{CETP,inlet}}(t)$ is the CETP inlet COD reading at time $t$ (mg/L)
- $\theta = 200 \text{ mg/L}$ (demo threshold; see `constants.py`)

The **breach magnitude** is:

$$
\Delta_{\text{breach}}(t) = \text{COD}_{\text{CETP,inlet}}(t) - \mu_{\text{baseline}}
$$

where $\mu_{\text{baseline}} = 193.0 \text{ mg/L}$ (empirical mean from Feb 2026 real data).

Alert level is classified as:

$$
\text{AlertLevel} = \begin{cases} \text{HIGH} & \text{if } \text{COD} \geq 2\mu_{\text{baseline}} \\ \text{MEDIUM} & \text{otherwise} \end{cases}
$$

---

## 2. Temporal Backtracking

Given a ShockLoadEvent at CETP inlet time $T$, the **backtrack timestamp** is:

$$
T_{\text{backtrack}} = T - \tau_{\text{travel}}
$$

where $\tau_{\text{travel}} = 15 \text{ min}$ (pipe travel time constant, v1).

The **attribution search window** is:

$$
W = \left[ T_{\text{backtrack}} - \epsilon,\; T_{\text{backtrack}} + \epsilon \right]
$$

where $\epsilon = 120 \text{ s}$ (tolerance, `ASOF_TOLERANCE_SECONDS`).

**Attribution rule** — the attributed factory $\hat{f}$ is:

$$
\hat{f} = \arg\max_{f \in \mathcal{F}} \left\{ \text{COD}_{f}(t) \;\middle|\; t \in W \right\}
$$

where $\mathcal{F}$ is the set of all factories with a valid COD reading in window $W$.

---

## 3. Pipe Travel Time (v1 simplification)

In v1, $\tau_{\text{travel}}$ is a fixed constant. The physically correct formula is:

$$
\tau_{\text{travel}} = \sum_{s \in \text{path}} \frac{L_s}{v_s}
$$

where:
- $L_s$ = length of pipe segment $s$ (m)
- $v_s$ = average effluent flow velocity in segment $s$ (m/s)

For a typical industrial estate CETP catchment (1–3 km radius, gravity sewers):

$$
v_s \approx 0.6 \text{ m/s} \implies \tau \approx \frac{2000 \text{ m}}{0.6 \text{ m/s}} \approx 55 \text{ min (max)}
$$

The 15-minute default assumes factories are within ~0.5 km of the CETP inlet. Adjust via `PIPE_TRAVEL_MINUTES`.

---

## 4. Zero-Variance Alarm (v2 — stub)

A sensor is flagged as *digitally tampered* when its rolling standard deviation over window $W_{\text{var}}$ equals zero:

$$
\sigma_{\text{COD},f}(t, W_{\text{var}}) = \sqrt{\frac{1}{|W_{\text{var}}|} \sum_{t' \in W_{\text{var}}} \left(\text{COD}_{f}(t') - \bar{\text{COD}}_{f}\right)^2} = 0
$$

for $|W_{\text{var}}| \geq 5$ consecutive minutes.

Since floating-point equality to exactly zero is unreliable, in practice the condition is:

$$
\sigma_{\text{COD},f}(t, W_{\text{var}}) < \varepsilon_{\text{float}} \approx 10^{-6}
$$

---

## 5. Chemical Fingerprint (v2 — stub)

The **dilution tampering** signal is:

$$
\text{PHYSICAL\_TAMPERING} \iff \frac{\text{COD}_{f}(t)}{\text{COD}_{f}(t - \Delta)} < (1 - \alpha) \;\text{ AND }\; \frac{\text{TSS}_{f}(t)}{\text{TSS}_{f}(t - \Delta)} > (1 - \beta)
$$

where:
- $\alpha = 0.80$ — COD must drop by ≥ 80% (`COD_DROP_FRACTION`)
- $\beta = 0.20$ — TSS must remain within 20% of prior value (`TSS_STABLE_FRACTION`)
- $\Delta = 15 \text{ min}$ rolling window

This captures the *bucket trick*: adding clean water drops COD (dilution) but particles (TSS) remain.

# Step 19 — Advanced Market & System Anomaly Detection

## Mode

V3.27 starts and remains in **ANOMALY_SHADOW_MODE**.

The Anomaly Detection Engine has no authority to:

- create LONG/SHORT signals;
- send or cancel broker orders directly;
- increase leverage or hard risk limits;
- deploy strategies, models, allocation policies or execution policies;
- reset Emergency Stop or Governance locks.

It only observes, classifies, scores, stores and recommends conservative responses.

## Reused protections

Step 19 does not replace:

- Market Regime Detector abnormal/uncertain regime detection;
- Trade Memory degradation and concept-drift analysis;
- Monitoring stale-data/resource/latency alarms;
- Recovery/Reconciliation state and position checks;
- System Evaluation degradation classification;
- Governance freeze/lock policies;
- Smart Execution spread/liquidity/slippage/latency safeguards;
- Ensemble disagreement/correlation/calibration;
- Capital Allocation heat/concentration/risk-off logic.

Instead, the new engine correlates evidence across those domains.

## Architecture

```text
MARKET / DATA / MODELS / EXECUTION / PORTFOLIO / SYSTEM
                         ↓
               ANOMALY DETECTION ENGINE
                    [SHADOW ONLY]
                         ↓
     ┌───────────────────┼────────────────────┐
     ↓                   ↓                    ↓
ENSEMBLE confidence  CAPITAL ALLOCATION   SYSTEM EVALUATION
recommendation       risk-off bias        anomaly history
     ↓                   ↓                    ↓
AI DIRECTOR           RISK ENGINE          GOVERNANCE
                         ↓                    ↓
                 SMART EXECUTION       adaptation freeze
```

All real actions remain with their responsible modules.

## Anomaly categories

- MARKET_ANOMALY
- DATA_ANOMALY
- LIQUIDITY_ANOMALY
- VOLATILITY_ANOMALY
- CORRELATION_ANOMALY
- STRATEGY_ANOMALY
- ENSEMBLE_ANOMALY
- EXECUTION_ANOMALY
- PORTFOLIO_ANOMALY
- BROKER_ANOMALY
- SYSTEM_ANOMALY

Severity and confidence are separate fields.

Severity:

`NORMAL → WATCH → ELEVATED → HIGH → CRITICAL`

Lifecycle:

`DETECTED → ACTIVE → STABILIZING → RECOVERED`

## Baselines and horizons

The first implementation deliberately uses robust, explainable statistics:

- median;
- MAD;
- 5th/95th quantiles;
- robust z-scores;
- rolling percentiles;
- contextual ratios;
- persistent median-level change detection.

Multiple horizons are maintained:

- VERY_SHORT_TERM: 5 minutes
- SHORT_TERM: 30 minutes
- MEDIUM_TERM: 180 minutes
- LONG_TERM: 1440 minutes

Current observations are stored with timestamps, and baseline queries use only `timestamp < current observation`; replay does not use future data.

## OOD and regime uncertainty

Current feature vectors can be compared with training/validation distributions.

A sufficiently unusual feature vector generates:

`OUT_OF_DISTRIBUTION_DATA`

The market regime may validly be:

- `UNKNOWN_REGIME`
- `OUT_OF_DISTRIBUTION_REGIME`
- `REGIME_TRANSITION`

Unknown conditions are not forced into BULL/BEAR/RANGE with artificial confidence.

## Correlation and diversification

The engine detects:

- `CORRELATION_REGIME_SHIFT`
- `CORRELATION_CONVERGENCE`
- `DIVERSIFICATION_BREAKDOWN`

This information is passed to Capital Allocation as a conservative risk-off/reduction recommendation. Correlated exposure is never treated as extra diversification.

## Data integrity

Critical deterministic rules remain explicit and do not depend on a black-box score:

- non-positive impossible prices;
- crossed/invalid quotes;
- negative volume;
- future timestamp;
- duplicate bars;
- timestamp reversal;
- missing bars;
- frozen prices;
- feed divergence.

These complement, rather than replace, the existing stale-data and reconciliation protections.

## Strategy and ensemble anomalies

Strategy behavior includes:

- `SIGNAL_FLOOD`
- `UNEXPECTED_STRATEGY_SILENCE`
- drawdown acceleration.

Ensemble behavior includes:

- `ENSEMBLE_DISAGREEMENT_SHOCK`
- `SUSPICIOUS_MODEL_CONVERGENCE`

Agreement is therefore not automatically treated as certainty.

## Execution and broker anomalies

Consumes Step 16 / Monitoring evidence:

- slippage spikes;
- fill-rate collapse;
- latency spikes;
- rejection spikes;
- broker state/format anomalies.

The recommendation to Smart Execution can only become more conservative: cautious, reduce or block affected execution.

## Composite score

Multiple independent domains amplify uncertainty.

A composite event such as:

```text
VOLATILITY ↑
+ SPREAD ↑
+ LIQUIDITY ↓
+ CORRELATION ↑
+ UNKNOWN REGIME
+ OOD INPUTS
+ ENSEMBLE DISAGREEMENT
+ EXECUTION LATENCY/SLIPPAGE
```

can generate `COMPOSITE_ANOMALY_CRITICAL`.

The anomaly engine itself still performs no direct trade action.

## False-positive control / hysteresis

A configurable persistence count is required to confirm an anomaly. Recovery also requires multiple normal confirmations.

Default behavior:

- anomaly confirmation: 2 observations;
- recovery confirmation: 3 observations;
- entry threshold: 0.50;
- exit threshold: 0.25.

A single normal tick after a critical event does not immediately restore normal operation.

## Structural changes

A robust recent-window versus previous-window test looks for both:

- a large median shift;
- persistence of observations away from the prior baseline.

This produces `STRUCTURAL_CHANGE_DETECTED`, which is distinct from a single OUTLIER and from strategy/model `CONCEPT_DRIFT`.

## Rare Event Memory

Composite HIGH/CRITICAL episodes create a persistent `RARE_EVENT_ID`.

Stored context includes:

- before/during/after context;
- event fingerprint;
- affected trades;
- recommended risk actions;
- execution behavior;
- portfolio impact;
- recovery duration;
- final outcome.

Similar-event search returns similarity and historical context only. It is not a prediction that the new event will end the same way.

## Adaptive Learning protection

By default, server-side Trade Memory / Adaptive Learning queries exclude trades overlapping HIGH/CRITICAL anomaly intervals.

The historical trades are not rewritten or deleted. They remain available for explicit anomalous-period research; they are merely excluded from normal adaptive-learning evidence to avoid confusing extraordinary infrastructure/market conditions with normal strategy behavior.

## Shadow evaluation

The engine stores hypothetical responses and supports:

- true positives;
- false positives;
- false negatives;
- true negatives;
- detection delay;
- precision;
- recall;
- false-positive rate.

The one-event synthetic scenario in the certification evidence is not sufficient to claim real precision/recall. Real validation requires replay and future shadow evidence.

## Future ML

Potential later research:

- Isolation Forest;
- one-class models;
- density methods;
- dedicated change-point algorithms.

No ML anomaly model is required for deterministic safety protections, and no black-box score may replace reconciliation, stale-data, hard-risk, impossible-price or duplicate-order rules.

## Activation path

```text
SHADOW
→ VALIDATION
→ PAPER / EVENT REPLAY
→ CANARY OF CONTROLS
→ LIMITED INTEGRATION
```

V3.27 does not advance beyond SHADOW.

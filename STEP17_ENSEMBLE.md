# Step 17 — Intelligent Ensemble of Models and Strategies

## Status

**Version:** V3.25  
**Mode:** `ENSEMBLE_SHADOW_MODE`  
**Signal authority:** none  
**Risk-increase authority:** none  
**Direct execution authority:** none

The Ensemble observes the current system and records what it would recommend. It does not replace the current production decision path.

## Audit of the current model/strategy map

V3.25 does not pretend that every indicator is an independent strategy.

| Source | Role | Family | Main inputs | Output | Independence note |
|---|---|---|---|---|---|
| `TECHNICAL_CORE` | Directional | `TREND_STRUCTURE` | H1/M15/M5/M1 price, EMA, ATR, structure, pullback, momentum | LONG/SHORT/ABSTAIN + confidence + edge | M15 slope, M5 momentum, M1 confirmation and second-pullback logic remain one correlated technical source |
| `ML_SUCCESS_CALIBRATOR` | Calibrator | `TECHNICAL_CALIBRATION` | Current technical feature vector + resolved labels | probability current setup succeeds | Not treated as an independent LONG/SHORT vote |
| `NEWS_CONTEXT` | Directional/contextual | `NEWS_MACRO` | GDELT 180m currency headlines | LONG/SHORT/ABSTAIN | Information source is meaningfully different from price-derived technical core |
| `MARKET_REGIME_CONTEXT` | Context | `MARKET_REGIME` | Multi-timeframe price, ATR, efficiency | regime/confidence/volatility/trend strength | It reuses price inputs, so it does not count as another independent directional vote |
| `WEEKEND_CONTEXT` | Directional/contextual when available | `WEEKEND_CONTEXT` | Weekend news + reopen reaction | LONG/SHORT/ABSTAIN | Conditional source; only participates when valid/fresh |
| `CROSS_ASSET_*` | Context/research | `CROSS_ASSET` | Other FX pairs/shared factors | contextual direction/strength | Remains context until a directional relationship is validated; not a production vote by default |

### Dependency overlap

The static dependency audit finds material input overlap between the technical core and regime detector because both consume the same price/ATR streams. That overlap is intentional and is why the regime detector is a **context modifier**, not a second price-direction vote.

A model can additionally be downweighted using empirical signal/return correlation when enough resolved Ensemble history exists.

## Standard Signal Interface

```text
strategy_id
strategy_version
symbol
timestamp
direction = LONG | SHORT | NEUTRAL | ABSTAIN
confidence
expected_edge
market_regime
time_horizon
signal_strength
risk_characteristics
data_quality
family
input_dependencies
role = DIRECTIONAL | CALIBRATOR | CONTEXT
ttl_seconds
status
metadata
```

`ABSTAIN` is a first-class output. No model is forced to predict.

## Baselines implemented

- `MAJORITY`
- `WEIGHTED`
- `CONFIDENCE_WEIGHTED`
- `PERFORMANCE_WEIGHTED`
- `REGIME_WEIGHTED`

The initial live-shadow integration uses `REGIME_WEIGHTED`; the other methods are baselines for comparison.

## Reliability

Reliability is not win-rate-only. The engine combines, when available:

- resolved expectancy;
- profit factor;
- stability;
- calibration / Brier-style error;
- sample size;
- recent/regime evidence;
- data quality;
- degradation context.

With fewer than `MIN_SAMPLE_SIZE`, evidence remains `LOW_EVIDENCE` and weight cannot be promoted aggressively.

## Correlation-aware weighting

The engine applies three controls:

1. **Model cap:** `MAX_MODEL_WEIGHT`.
2. **Family cap:** `MAX_FAMILY_WEIGHT`.
3. **Correlation discount:** highly correlated peers are discounted approximately by `1/sqrt(1 + correlated_peers)`.

Feature/dependency similarity is used as a conservative prior when historical correlation is not yet available. Empirical correlation can replace/refine that prior as resolved Ensemble history accumulates.

This prevents five copies of essentially the same trend signal from being interpreted as five independent confirmations.

## Agreement, disagreement and diversity

The output records:

- `agreement_score` — directional consistency after correlation-aware weights;
- `disagreement_score` — opposing directional evidence;
- `diversity_score` — family diversity, empirical/dependency correlation, input overlap and horizon diversity.

High directional agreement with low diversity can still have low Ensemble confidence.

## Confidence calibration

The success-probability ML component is used as a calibrator rather than as another directional vote. Resolved model outcomes are stored so predicted confidence can be compared with realized outcomes. Miscalibrated models lose reliability instead of gaining influence from repeatedly reporting high confidence.

## Time horizon and freshness

Signals have a TTL. Stale signals stop voting. Signals with incompatible horizons are not treated as direct contradictions in the same decision bucket.

## Data dependency awareness

Each model declares `input_dependencies`. If models share dependencies, similarity contributes to correlation penalties. If a data source becomes degraded/offline, those models are excluded or confidence is reduced. Multiple broken descendants of one feed cannot create artificial collective confidence.

## Expected Net Edge

The ensemble aggregates directional expected edge and subtracts the **known-before-decision** execution-cost estimate from Step 16:

```text
ENSEMBLE_EXPECTED_EDGE
- EXPECTED_EXECUTION_COST
= EXPECTED_NET_EDGE
```

Smart Execution TCA is queried only with `ts < current decision timestamp`, preventing look-ahead.

If net edge is non-positive, `NO_CLEAR_EDGE_AFTER_EXECUTION_COSTS` can force `ENSEMBLE_ABSTAIN`.

## Weight stability

Weight changes are bounded by:

- `WEIGHT_CHANGE_LIMIT`;
- `WEIGHT_COOLDOWN`;
- minimum observation requirements;
- immutable `ensemble_weight_version` records.

The stability limiter is applied **before the weights influence the Ensemble output**, not merely to the stored audit record.

## Candidate weights

Adaptive Learning can create a research-only weight candidate:

```text
CURRENT_WEIGHTS
→ CANDIDATE_WEIGHTS
→ VALIDATION
→ PAPER
→ CANARY
→ APPROVAL
```

`auto_deploy = false`.

No meta-model has been deployed in Step 17. A future meta-model would have to pass the existing temporal Validation Pipeline and leakage protections.

## Shadow comparison

Each Ensemble decision can be compared with the current system. Actual results and hypothetical results are stored separately. A hypothetical trade/fill is not invented simply because the Ensemble would have chosen a different direction.

Until enough resolvable shadow outcomes accumulate, the correct result is:

`NO_ENSEMBLE_ADVANTAGE_DETECTED / INSUFFICIENT_DATA`.

## Downstream authority

```text
ENSEMBLE SHADOW OPINION
→ AI STRATEGY DIRECTOR reviews it
→ RISK ENGINE may allow/reduce/block
→ SMART EXECUTION may only reduce execution quantity
→ RECOVERY / BROKER
```

The Ensemble cannot:

- send an order;
- bypass the Director;
- bypass Risk;
- multiply leverage because more models agree;
- increase hard risk limits;
- promote its own weights or models.

## Governance

Governance tracks:

- Ensemble weight/model churn;
- low diversity;
- execution/System Evaluation degradation;
- validation state for Ensemble promotions.

It can block or freeze new Ensemble policy changes but cannot turn an Ensemble opinion directly into a trade.

## Critical correlated-vote scenario

The Step-17 integration suite includes:

```text
3 correlated TREND models = LONG
1 BREAKOUT model          = LONG
1 independent MEANREV     = SHORT
1 volatility model        = ABSTAIN
```

The three trend models are capped as one family. The independent SHORT retains nonzero weight. The resulting confidence is substantially below a naïve `4 vs 1` interpretation.

A separate unit test also covers **5 correlated LONG vs 1 independent SHORT** and verifies that correlation/family caps prevent enormous confidence.

## Activation path

```text
SHADOW
→ VALIDATION
→ PAPER
→ CANARY
→ LIMITED_ENSEMBLE
→ PRODUCTION_ENSEMBLE
```

V3.25 remains at **SHADOW**.

## Tests

```bash
python test_ensemble_engine.py
python test_ensemble_integration.py
python test_system_integration_framework.py
python test_smart_execution.py
python test_smart_execution_integration.py
python test_production_readiness.py
python test_governance_engine.py
python test_system_evaluation.py
python test_security_manager.py
python test_security_integration.py
python test_recovery_manager.py
python test_observability.py
python test_deployment_manager.py
python test_validation_pipeline.py
```

## Current limitations

- There are currently only a few genuinely different directional information sources. The system should therefore often abstain rather than manufacture diversity.
- Cross-asset research is still context, not a validated independent directional model.
- Weekend context is intermittent.
- No complex meta-model is enabled.
- No live Ensemble value-added claim is made before sufficient shadow evidence.
- Adaptive Risk remains shadow-only, so the broader Production Readiness NO-GO remains in force.

# Step 16 — Smart Execution Engine

## Architecture

```text
STRATEGY SIGNAL
→ AI STRATEGY DIRECTOR
→ RISK ENGINE
→ SMART EXECUTION ENGINE [SHADOW]
   ├─ Execution Intent
   ├─ Market Snapshot
   ├─ Expected Slippage
   ├─ Spread / Liquidity Intelligence
   ├─ Fill Probability
   ├─ Execution Confidence
   ├─ Smart Size Reduction
   ├─ Slice Plan
   └─ TCA / Execution Memory
→ EXISTING EXECUTION PATH
→ RECOVERY MANAGER / ORDER STATE MACHINE
→ BROKER
```

Smart Execution has no signal authority and no risk-increase authority.

## Execution Intent

Fields include:

`execution_intent_id`, `trade_id`, `decision_id`, `risk_decision_id`,
`strategy_id`, `symbol`, `side`, `target_quantity`, `maximum_quantity`,
`risk_approved_quantity`, `urgency`, `expected_price`,
`maximum_slippage_bps`, `time_limit_seconds`, `signal_time`,
`created_at`, `expires_at`, `risk_approval_valid_until`, `mode`.

## Market Snapshot

Persists:

bid, ask, spread, spread_bps, mid, last price, available liquidity,
recent volume when available, volatility, market regime, broker health,
broker latency, data age, market status and order-book metadata.

The current OANDA pricing feed provides bid/ask liquidity levels but not a
full exchange-style central limit order book; the engine records that limitation.

## Slippage model

The estimator is deliberately explainable:

1. Uses only TCA records timestamped **before** the current execution intent.
2. With sufficient history, uses a robust median / upper-quartile estimate.
3. With insufficient history, falls back to spread, participation,
   volatility and latency components.
4. LIMIT estimates receive a lower expected-slippage component but are paired
   with a fill-probability estimate.

No future fill or post-trade information enters a pre-execution decision.

## Order selection

Current shadow recommendations use broker-supported `MARKET` or `LIMIT` behavior.

- High urgency + normal spread + acceptable expected slippage can recommend MARKET.
- Lower urgency, wider spread or lower liquidity tends toward LIMIT.
- Extremely poor conditions may DELAY, REDUCE_SIZE or REJECT_EXECUTION.

The engine never increases quantity.

## Liquidity and slicing

`recommended_quantity = min(target, maximum, risk_approved, available_liquidity)` when explicit liquidity is available.

Large orders can receive a deterministic slice plan. The sum of all slices
is always bounded by the original Risk Engine authorization.

## Partial fills

A partial fill does not imply completion.

Before any remaining execution can continue, the engine revalidates:

- risk approval;
- strategy intent;
- current position state;
- price / spread;
- liquidity;
- data freshness;
- slippage;
- intent expiry;
- Emergency Stop.

A stale or uneconomic remaining quantity is cancelled instead of chased.

## Execution Quality Score

The score is 0–100 and penalizes:

- unfavorable slippage;
- poor fill rate / partial fill;
- broker latency;
- rejection;
- spread cost;
- fees;
- estimated market impact.

It is not a speed-only score.

## Transaction Cost Analysis

TCA stores:

- expected vs actual fill;
- absolute / bps / monetary slippage;
- spread cost;
- fees;
- estimated impact;
- delay-cost field;
- total execution cost;
- expected gross edge;
- expected net edge;
- fill rate;
- quality score;
- entry / exit quality;
- stop slippage;
- adverse selection;
- strategy-vs-execution attribution.

If expected gross edge is positive but costs consume it, the observation can
be marked `EXECUTION_LOSS`.

## Execution memory / learning boundary

Execution history can be grouped by symbol, strategy, order type, session,
regime, volatility and liquidity.

The learning authority is:

`OBSERVE → ANALYZE → RECOMMEND`

A proposed execution policy becomes a `CANDIDATE_EXECUTION_POLICY` and is
research-only. It must pass simulation, paper, validation and Canary before it
could replace production execution.

## Integrations

- **Risk Engine:** superior authority; Smart Execution can only reduce.
- **Recovery Manager:** preserves idempotency, UNKNOWN-order handling,
  partial fills, duplicate prevention and reconciliation.
- **Trade Memory:** execution shadow decision and actual TCA are linked to the trade context.
- **System Evaluation:** consumes Smart Execution quality, slippage, fill rate and costs.
- **Governance:** can freeze/review execution-policy deployments during execution degradation.
- **Monitoring:** exposes Smart Execution mode, quality, slippage, fill rate,
  rejection rate, latency, active intents, partial fills, costs and degradation.
- **Production Readiness:** V3.24 changes the release fingerprint; prior release evidence cannot silently authorize this code.

## Test suites

```bash
python test_smart_execution.py
python test_smart_execution_integration.py
python test_system_integration_framework.py
python test_production_readiness.py
python test_recovery_manager.py
python test_system_evaluation.py
python test_governance_engine.py
python test_security_manager.py
python test_security_integration.py
python test_observability.py
python test_deployment_manager.py
python test_validation_pipeline.py
```

## Known pending evidence

- No genuine historical Smart-vs-Base microstructure backtest is possible from
  pre-Step-16 data because bid/ask/liquidity snapshots were not persisted at
  sufficient granularity.
- Smart Execution remains SHADOW.
- The Adaptive Risk Engine production-authority issue from Step 15 remains a
  production-readiness blocker.
- Paper, Canary and real execution evidence for Smart Execution have not yet
  been accumulated.

# Market Alert V3.27 — Advanced Anomaly Detection Shadow

V3.27 implements **Step 19: Advanced Anomaly Detection Shadow** on top of the previously validated execution, ensemble, allocation, governance, recovery, and evaluation layers. The Anomaly Engine starts in **SHADOW**, has no BUY/SELL authority, cannot increase risk, and does not directly control production.

See [`STEP17_ENSEMBLE.md`](STEP17_ENSEMBLE.md) for the architecture, model audit, family/correlation rules, validation boundaries and tests.

---

## Step 16 — Smart Execution Shadow & TCA

V3.24 implements **Step 16: Execution Optimization, Slippage and Fill Quality** as a second-generation layer.

The safety priority is:

**EXECUTION QUALITY → COST CONTROL → LIQUIDITY → SPEED → SAFETY**

## Critical boundary

Smart Execution starts in **SHADOW**:

```text
RISK ENGINE
    ↓
SMART EXECUTION ENGINE (SHADOW)
    ↓
EXISTING SAFE EXECUTION
    ↓
RECOVERY / ORDER STATE
    ↓
BROKER
```

It does **not** generate BUY/SELL signals, does **not** increase Risk Engine authorization, does **not** replace the current MARKET/FOK execution yet, and does **not** send hypothetical LIMIT/sliced orders to the broker.

Hard invariants:

- `EXECUTED_QUANTITY <= RISK_APPROVED_QUANTITY`
- no execution without valid risk approval
- no execution with stale critical data
- no duplicate execution
- expired intent means no new order
- Emergency Stop means no new entry
- Smart Execution failure never falls through to an uncontrolled market order

## What V3.24 adds

- Structured Smart Execution intents.
- Pre-execution bid/ask/spread/liquidity snapshots.
- Explainable expected-slippage estimator.
- Market-vs-Limit recommendation in shadow.
- Fill-probability estimate for passive execution.
- Liquidity-aware size reduction.
- Order-slicing plans with an absolute Risk Engine ceiling.
- Partial-fill revalidation and intent expiry.
- Transaction Cost Analysis (TCA).
- Execution Quality Score.
- Execution Confidence.
- Spread and liquidity states.
- Broker latency baselines.
- Entry/exit and stop execution analysis.
- Adverse-selection measurement.
- Execution memory grouped by symbol, strategy, order type, session, regime, volatility and liquidity.
- Daily execution-cost monitoring.
- Execution-degradation detection.
- Shadow actual-vs-hypothetical comparison without assuming a hypothetical LIMIT would have filled.
- Research-only Candidate Execution Policies that cannot auto-deploy.

## Activation path

```text
SHADOW
→ PAPER
→ CANARY
→ LIMITED_EXECUTION
→ PRODUCTION_EXECUTION
```

V3.24 is intentionally fixed at **SHADOW**. Advancing this path requires new validation evidence and the existing Change Management / Validation / Deployment / Governance controls.

## Historical comparison limitation

Before V3.24 the project did not persist enough bid/ask/liquidity microstructure to reconstruct a genuine historical Smart-vs-Base execution counterfactual. Therefore the included Step 16 comparison is a **seeded synthetic microstructure simulation**, not evidence of live profitability.

The simulation is used to validate mechanics, risk ceilings, fill assumptions and cost accounting. It must not be interpreted as proof that Smart Execution improves market returns.

---

## Step 15 — Production Readiness & Minimal Live Certification

V3.23 implements **Step 15: Production Certification and Gradual Real Activation**.

The safety principle is:

**SECURITY → EVIDENCE → MINIMUM CAPITAL → OBSERVATION → CONTROLLED SCALING**

Passing Step 14 does **not** automatically authorize real trading. The new `ProductionReadinessGate` must certify a frozen release and all mandatory checks must pass.

## Current certification result

The current V3.23 release candidate is intentionally **NO_GO / BLOCKED** for real capital because the production prerequisites are not yet satisfied. Most importantly, the Adaptive Risk Engine is still configured as `RISK_ENGINE_SHADOW_MODE=True`, so it is not yet the final authority over real order risk. In addition, no real broker account has been verified, no exact-release Final Paper certification run has been recorded, no production dry run has been completed against live market conditions, and explicit production authorization remains disabled.

This is expected behavior. V3.23 implements the process without pretending that the system is already ready for money.

## Production states

Readiness states:

- `NOT_READY`
- `BLOCKED`
- `CONDITIONALLY_READY`
- `READY_FOR_MINIMAL_LIVE`
- `MINIMAL_LIVE`
- `LIMITED_LIVE`
- `CONTROLLED_LIVE`
- `PRODUCTION_APPROVED`
- `SUSPENDED`

A release can never jump directly from certification to full production.

## Production Readiness checklist

Mandatory checks include:

- Step 14: zero critical failures and zero safety violations.
- No known Risk Engine bypass.
- No known duplicate-order vulnerability.
- Reconciliation ready.
- Emergency Stop tested and persistent across restart.
- Risk Engine healthy **and not shadow-only**.
- Production broker/account verified.
- Market data fresh.
- Audit chain healthy.
- Governance healthy.
- Deployment state consistent.
- No state corruption/P0 incident.
- Exact-release Final Paper pass.
- Production Dry Run pass with zero real order requests.
- Canary controls tested.
- Recovery tests pass.
- Security tests pass.
- Change Management ready.
- Release Candidate frozen.
- Explicit `PRODUCTION_AUTHORIZED=true`.
- Stable System Evaluation.
- Monitoring ready.

Any mandatory failure produces `NOT_READY` or `BLOCKED`.

## Release Candidate discipline

A release candidate records immutable fingerprints of:

- source files;
- managed configuration;
- dependency/version metadata;
- Step 14 report.

A material code/config/dependency change requires `NEW_RELEASE_CANDIDATE_REQUIRED`. Production certification is version-bound and expires after 30 days by default in the gate implementation. Major code, strategy, risk-framework, broker-integration changes, long inactivity or critical incidents can invalidate it earlier.

## Final Paper Run

The exact frozen release must run in paper using the same code/configuration/strategies/Risk/Governance/execution pipeline. Minimum certification defaults are 10 paper trades, 3 days and at least one observed regime, with exact code/config/risk/governance parity and zero critical incidents.

This requirement is separate from prior research/validation paper testing.

## Production Dry Run

In `PRODUCTION_DRY_RUN_MODE=true`, the pipeline can reach:

`MARKET DATA → SIGNAL → DIRECTOR → RISK → GOVERNANCE → EXECUTION PREPARED`

but the final broker order send is blocked. A passing dry run requires `real_broker_request_count=0`.

## Real account verification

Before certification can pass, the gate verifies:

- broker;
- account id;
- account type;
- currency;
- trading permission;
- market access;
- balance range;
- margin/leverage settings.

`TRADING_ENVIRONMENT=PRODUCTION` alone is insufficient. Live primary broker selection also requires `PRIMARY_OANDA_ENV=live` and `PRODUCTION_AUTHORIZED=true`, and test/simulation processes force the practice endpoint.

## Minimal Live limits

V3.23 defaults are intentionally much lower than existing Risk Engine hard limits:

- Risk-cap multiplier: 5%.
- Max trade-risk fraction: `min(0.0005, Risk Engine hard max)`.
- Max portfolio exposure: `min(0.005, Risk Engine hard max)`.
- Max stage drawdown: `min(0.005, Risk Engine drawdown stop)`.
- Minimum evidence before promotion: 10 live trades and 5 live days.

The order-unit cap is also multiplied by the current production-stage risk cap. This is an extra ceiling, never a replacement for Risk Engine authority.

Subsequent defaults:

- LIMITED_LIVE: 10% risk-cap multiplier, 25 trades / 10 days.
- CONTROLLED_LIVE: 25% risk-cap multiplier, 50 trades / 20 days.
- PRODUCTION_APPROVED: still subject to all Risk Engine hard limits.

## Promotion gates

Promotion is sequential only:

`MINIMAL_LIVE → LIMITED_LIVE → CONTROLLED_LIVE → PRODUCTION_APPROVED`

Each promotion checks:

- minimum live trades;
- minimum time in stage;
- no P0/P1 incidents;
- clean reconciliation;
- Risk Engine ready;
- Governance `NORMAL_ADAPTATION`;
- System Evaluation HEALTHY/EXCELLENT;
- data quality;
- drawdown;
- live stability score;
- Paper-vs-Real execution divergence.

Profit alone cannot promote a stage.

## Paper vs real comparison

The gate compares Final Paper against live evidence for available metrics including slippage, fill rate, expectancy and trade frequency. Material divergence produces `LIVE_EXECUTION_DIVERGENCE` and blocks scaling.

## Automatic safety de-escalation

Deterministic safety failures can only reduce freedom/capital:

- P0 incident;
- Risk Engine unavailable;
- broker instability;
- reconciliation critical;
- data-quality failure;
- Emergency Stop;
- account mismatch.

Critical conditions suspend production. Less severe deterministic conditions can downgrade one stage. No automatic mechanism increases exposure.

## Suspension and resume

`SUSPENDED` means:

- no new entries;
- position monitoring continues;
- reconciliation continues;
- Risk Engine continues;
- monitoring continues;
- promotions blocked.

Resume is never full production. Required flow:

`SUSPENDED → incident resolved → reconciliation → health check → MINIMAL_LIVE → observation → possible promotion`

## Real execution verification

For each real trade the production layer stores:

- expected order;
- actual order;
- broker fill;
- slippage;
- broker latency;
- fees;
- partial/rejection state;
- reconciliation result;
- protection verification;
- audit/trade-memory linkage.

Primary live fills trigger immediate post-fill reconciliation before the evidence record is considered clean.

Trade Memory is extended rather than duplicated with:

- `release_id`;
- `production_certification_id`;
- `production_stage`.

## Live Stability Score

Live stability is based on:

- operational reliability;
- execution quality;
- risk consistency;
- reconciliation accuracy;
- incident frequency.

It is deliberately not PnL-only.

## Continuous certification

While real capital is enabled, certification is continuously reevaluated. It can be invalidated by:

- release fingerprint change;
- major code change;
- major strategy change;
- risk framework change;
- broker integration change;
- critical incident;
- long inactivity;
- certification expiration.

Material managed config changes also invalidate an active certification through Change Management side effects.

## Production alerts

V3.23 integrates or defines alerts for:

- `PRODUCTION_READINESS_LOST`
- `LIVE_EXECUTION_DIVERGENCE`
- `PRODUCTION_STAGE_DOWNGRADED`
- `PRODUCTION_SUSPENDED`
- `ACCOUNT_MISMATCH`
- `RECONCILIATION_CRITICAL`
- `RISK_ENGINE_UNAVAILABLE`
- `CERTIFICATION_INVALIDATED`

## APIs

- `GET /api/production-readiness/dashboard`
- `POST /api/production-readiness/release-candidate/freeze`
- `POST /api/production-readiness/account/verify`
- `POST /api/production-readiness/final-paper/record`
- `POST /api/production-readiness/dry-run/record`
- `POST /api/production-readiness/certify`
- `POST /api/production-readiness/activate-minimal-live`
- `POST /api/production-readiness/promote/{target_stage}`
- `POST /api/production-readiness/suspend`
- `POST /api/production-readiness/resume`
- `POST /api/production-readiness/incidents`
- `POST /api/production-readiness/incidents/{incident_id}/resolve`

Capital activation/promotion/resume require Risk Manager/Admin-level permissions already enforced by Step 11 RBAC.

## Operator Runbook

See `PRODUCTION_RUNBOOK.md` for broker disconnect, market-data failure, UNKNOWN orders, position mismatch, Emergency Stop, drawdown breach, System Critical, software rollback and suspension procedures.

## Tests

Run:

```bash
python test_production_readiness.py
python test_system_integration_framework.py
python test_governance_engine.py
python test_system_evaluation.py
python test_security_manager.py
python test_security_integration.py
python test_recovery_manager.py
python test_observability.py
python test_deployment_manager.py
python test_validation_pipeline.py
```

Step 15 tests prove, among other things:

- Step 14 PASS alone still returns NO_GO.
- Shadow-only Risk Engine blocks production.
- Exact release evidence can reach READY_FOR_MINIMAL_LIVE in simulation.
- Material release changes invalidate certification.
- Explicit production authorization is mandatory.
- Fast profits cannot promote a stage.
- Evidence-based promotion works in simulation.
- Risk failure suspends production.
- Position mismatch P0 suspends production.
- Resume restarts at MINIMAL_LIVE.
- Dry run fails if even one real broker request is observed.

## Current real-world status

**NO_GO / BLOCKED.**

This is not a defect in the gate. It is the correct certification result until the remaining live prerequisites are completed. In particular, real capital must not be activated while the Risk Engine remains shadow-only.

## V3.26 — Step 18 Dynamic Capital Allocation Shadow
Adds a shadow-only Capital Allocation Engine between AI Strategy Director and the final Risk Engine veto. It distributes an already-authorized risk budget using net edge, reliability, volatility, drawdown, execution quality, regime evidence and correlation, while permitting unused risk. It cannot generate signals/orders, cannot increase hard risk limits, and cannot auto-deploy. See `STEP18_CAPITAL_ALLOCATION.md`.


## Step 19 safety status

- Anomaly Engine mode: **SHADOW**
- Direct trading authority: **none**
- Risk-increase authority: **none**
- Critical anomaly integration: Governance receives critical composite anomalies as a conservative **freeze recommendation**; SHADOW mode does not enforce the freeze.
- Production use: keep disabled until replay/shadow evidence and production-readiness gates are satisfied.
## Certification Evidence

Validation and certification evidence is organized separately from the application source code.

- `certification-evidence/` — Historical certification and validation artifacts for Steps 14–18.
- `step19-certification-evidence/` — Step 19 anomaly-detection shadow validation, simulation, audit, replay, limitation, and certification artifacts.

These artifacts provide reproducible evidence for validation and auditing. Their presence does not authorize live trading or override Production Readiness, Risk Engine, Governance, or deployment controls.

# BotsTrader V3.36.1 — Multi-Asset PAPER Isolation Audit

## Scope and canonical baseline

This candidate starts from the current GitHub `main` baseline observed as `06862d12350e64eead55b7ffb2e2bc7bbe0d01fc`, whose only change over the reconciled code commit `6e1aabe8eb4903d1f973a2f038c69d8821341860` is the canonical-baseline note in `PROJECT_STATUS.md`.

The purpose of V3.36.1 is architectural isolation, not optimization. No RR, spread, barrier, break-even, anomaly, ensemble, leverage or hard-risk thresholds were increased or tuned from GBP/USD or USD/JPY outcomes.

## Rule classification before implementation

### GLOBAL_SAFETY

These controls are instrument-independent and remain common to EUR_USD, GBP_USD and USD_JPY:

- Security Manager and Runtime Integrity.
- PAPER/OANDA Practice endpoint restrictions and existing production-readiness gates.
- Broker metadata verification before a secondary instrument can construct an order.
- Pip size, display precision, unit precision, minimum trade-size normalization and broker formatting.
- Instrument-aware sizing; it may reduce requested/legacy units but cannot increase them.
- Existing hard trade, strategy, portfolio, margin, drawdown and correlated-position ceilings.
- Market-closed, stale/abnormal risk-context and excessive-risk vetoes.
- Duplicate-order/idempotency controls and per-instrument existing-position checks.
- Recovery/reconciliation, persistence, management and order/trade identity with instrument attribution.
- Broker-confirmed fill price as managed entry reference.
- Base break-even, stop/target and protective-order management.
- Explicit mutating OANDA request body safety (`body=...`).
- Market-data integrity and observability.
- Learning/research namespaces by instrument.
- Global new-entry blackouts: 07:00–10:00 ET and 15:00–19:00 ET.
- Existing weekday daily cutoff: 16:50–19:00 ET; open trades continue management under the existing policy.

### GLOBAL_STRATEGY_BASE

These are frozen strategy geometry/quality rules, not newly inferred EUR-loss exceptions, and therefore remain shared:

- `minimum_rr` and `MIN_ENTRY_RR = 0.40`.
- `barrier_room_ok`.
- Canonical `m1_confirmation` as the base trigger requirement.
- Frozen directional score / directional-edge requirements.
- Existing entry-extension/chasing ceiling.
- Volatility-sanity and structural geometry checks.
- Existing stop/target and barrier geometry.

No threshold in this group was changed in V3.36.1.

### EUR_USD_SPECIFIC

The following execution authority is explicitly confined to the EUR_USD profile because it came from the recent EUR/USD forward/research evidence path rather than independent evidence for new instruments:

1. `LOW_ROOM_LOW_RR` PAPER forward veto.
2. `LOW_ROOM_EXTENDED` PAPER forward veto.
3. `M1_ALTERNATIVE_ADMISSION` — the previously validated EUR path that can admit an entry without the stricter canonical M1 confirmation when its existing alternate conditions are met.
4. Learned/active research-veto authority. Existing promoted research rules are not inherited by GBP_USD or USD_JPY.

The underlying LOW_ROOM pattern flags are still recorded for all instruments as observations. For GBP_USD/USD_JPY they are not effective execution vetoes unless a future, separately approved profile change grants that authority.

### RESEARCH_ONLY

These remain non-production research/diagnostic mechanisms:

- Advanced reconciled `directional_null_test.py`, including E1/BH-FDR methodology.
- Directional Test B and component diagnostics.
- Filter stacking / Jaccard / exclusive-veto / remove-one analysis.
- Resolver-semantics audit.
- External research and candidate research rules until separately validated/promoted under existing governance.
- Adaptive risk recommendations, AI Director, Ensemble and other components already designated SHADOW.

The stacking warning remains: `high_jaccard_does_not_mean: overfiltering_or_bad_veto`.

Resolver-semantic differences remain diagnostic and `does not imply an old-resolver bug`.

Research cannot automatically alter BUY/SELL, increase risk, bypass safety, change real sizing or grant itself production veto authority.

## Instrument profile architecture

`instrument_profiles.py` centralizes execution permissions and instrument-specific exceptions. Large strategy blocks are not duplicated and no new per-symbol decision branches are scattered through `server.py`.

- EUR_USD: PAPER allowed; existing EUR production path is preserved behind all existing production authorization/security gates; EUR-specific forward vetoes/exceptions retained.
- GBP_USD: PAPER/Practice allowed; LIVE denied by profile; no EUR-specific vetoes/exceptions; learned research-veto authority denied.
- USD_JPY: PAPER/Practice allowed; LIVE denied by profile; no EUR-specific vetoes/exceptions; learned research-veto authority denied.
- Unknown instruments: inert until explicitly profiled.

`TEST`, `INTEGRATION_TEST` and `SIMULATION` can use the PAPER logical path for deterministic regression/replay; this does not create LIVE broker authority, and existing test-mode endpoint hardening remains in force.

## Forward observability separation

`forward_observation_snapshot()` now records both:

- `vetoes`: raw observed pattern/gate state.
- `effective_vetoes`: the subset with actual authority for that instrument.

`forward_audit.py` consumes `effective_vetoes` when present, preserving backwards compatibility with legacy snapshots. This prevents an observed GBP/JPY LOW_ROOM pattern from being reported as a veto that actually blocked execution.

## Multi-asset risk isolation

The pre-existing Risk Engine already defined hard ceilings for maximum trade risk, portfolio risk, margin usage and correlated positions, but adaptive recommendations remained SHADOW. V3.36.1 adds `portfolio_execution_guard()` to enforce only those existing hard ceilings in the actual entry path.

It does not increase any ceiling and does not create a new correlation strategy. The existing conservative shared-currency proxy is reused. With `RISK_MAX_CORRELATED_POSITIONS = 2`, if two existing positions already share USD exposure, a third USD-sharing candidate is blocked. The guard also rejects a candidate whose prospective risk would cross the existing portfolio-risk cap.

This is intentionally a minimum safety control: it prevents enabling additional instruments from silently multiplying aggregate risk while leaving portfolio optimization/correlation modeling out of scope.

## USD_JPY metadata

The registry includes a conservative USD_JPY fallback:

- display precision: 3
- pip location: -2
- pip size: 0.01
- trade-unit precision: 0
- minimum trade size: 1 unit

As with every newly PAPER-enabled secondary instrument, order construction still requires broker-verified OANDA metadata. Runtime OANDA metadata supersedes the fallback when available.

## Isolation invariants

Tests verify that:

- EUR_USD remains the explicit primary instrument.
- GBP_USD and USD_JPY can reach the PAPER execution gate only after OANDA metadata verification.
- GBP_USD/USD_JPY cannot receive EUR-specific LOW_ROOM veto authority or the EUR M1 alternate-admission exception.
- GBP_USD/USD_JPY cannot inherit active learned EUR research veto authority.
- Global blackouts, cutoff and aggregate hard-risk limits apply across instruments.
- Instrument-aware sizing cannot increase allowed units.
- Deterministic order-intent identity includes symbol and does not collide across instruments.
- Runtime Integrity and production-release fingerprinting include `instrument_profiles.py`.
- Existing explicit OANDA `body=` request safety remains intact.

## Boundaries / limitations

- This is offline/unit/integration validation. It does not claim that GBP_USD or USD_JPY has been PAPER-forward validated against a live OANDA Practice session yet.
- No historical profitability claim is made for GBP_USD or USD_JPY.
- The shared-currency correlation rule is deliberately conservative and simple; V3.36.1 does not introduce a rolling-correlation portfolio strategy.
- The current portfolio-open-risk context uses the existing broker/open-unit risk proxy. V3.36.1 does not replace it with a new risk model.
- Railway is not modified or deployed by this candidate.

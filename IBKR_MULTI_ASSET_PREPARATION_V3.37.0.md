# BotsTrader V3.37.0 — IBKR Multi-Asset Preparation

## Status

Development / local hardening only. **V3.37.0 IS NOT YET DEPLOYMENT-CERTIFIED.**
Canonical source is V3.36.1 commit `1a35f004a467cb4f38a5004f43ed0ed5d7c6d35f`.
No Railway deployment, IBKR connection, IBKR order, or LIVE authority is part of this version.

## Five-instrument analysis universe

- EUR_USD
- GBP_USD
- USD_JPY
- AUD_USD
- USD_CAD

All five can be analyzed in one batch and can appear in the opportunity ranking. OANDA execution authority remains separate: AUD_USD and USD_CAD are intentionally execution-inert in their V3.37.0 instrument profiles. GBP_USD and USD_JPY retain their V3.36.1 PAPER-only protections.

## Decision architecture

The worker now uses an explicit cycle:

`COLLECT -> NORMALIZE/RANK -> SLOT ALLOCATION -> BROKER RISK -> PORTFOLIO/CORRELATION -> EXECUTE`

No new order is sent while candidate collection is still in progress. This removes accidental slot assignment caused by loop order.

### Opportunity Ranking

`opportunity_ranker.py` is deterministic and side-effect free. It receives already-generated strategy candidates and does not generate BUY/SELL signals. The conservative policy uses bounded pre-entry fields already present in the candidate:

- strategy quality score: 45%
- calibrated signal confidence: 35%
- bounded RR quality: 10%
- bounded structural room quality: 5%
- bounded cost/spread quality: 5%

These weights are a transparent development policy, not backtest optimization. Outcome/future fields are ignored. Instrument symbol is the deterministic tie-break, so scan order is not authoritative.

### Slot Allocation

`slot_allocator.py` owns policy tiers:

- NLV < $5,000: max 1 concurrent slot.
- NLV >= $5,000: max 2 concurrent slots.
- Higher NLV remains capped at 2 until a future explicit policy change.

The tier is only a strategic ceiling. Existing open positions reduce available slots. Broker risk and portfolio risk can reduce 2 -> 1/0 or 1 -> 0; they cannot increase the tier.

The allocator evaluates ranked candidates one by one and can skip an incompatible candidate to choose the next compatible candidate. It never bypasses broker/global hard guards to preserve rank order.

## Global risk vs broker-specific risk

Existing hard limits remain unchanged. `portfolio_execution_guard()` remains final authority for approved global portfolio/margin/correlation ceilings.

`broker_risk.py` introduces broker-specific adapters without giving IBKR execution authority:

- `OandaBrokerRiskAdapter`: reuses PAPER/instrument/metadata/margin eligibility.
- `IbkrBrokerRiskAdapter`: INACTIVE, `execution_authority=False`, always fail-closed.

A future active IBKR adapter must consume real broker values such as NetLiquidation, AvailableFunds, ExcessLiquidity, InitMarginReq, MaintMarginReq, BuyingPower when applicable, instrument minimums, and an order-specific prospective/what-if margin result. No fixed `IBKR_MARGIN_RATE` has authority in V3.37.0. Synthetic values cannot unlock execution.

## Metadata

`instrument_registry.py` now has conservative offline fallbacks for AUD_USD and USD_CAD in addition to EUR_USD, GBP_USD and USD_JPY. Fallback metadata supports analysis/formatting only. It must not grant future IBKR execution authority; verified broker metadata remains mandatory for any future IBKR order path.

## Observability

`multi_asset_decision_cycles` records each batch decision with:

- cycle timestamp/id
- broker/trading mode
- NLV and slot tier
- open positions and slots available
- candidate summaries and metadata status
- final ranking and score components
- selected/rejected candidates and rejection reason
- execution results
- explicit `research_authority=False`
- explicit `ibkr_execution_authority=False`
- `look_ahead=False`

Existing signal/trade/forward/recovery tables continue to hold the detailed instrument-scoped evidence.

## Preserved V3.36.1 invariants

- OANDA Practice execution path and `body=` safety.
- Broker-confirmed fill as managed entry.
- Break-even/stop/target management.
- Recovery/reconciliation and idempotency.
- Security Manager and Runtime Integrity.
- Secondary OANDA metadata verification.
- Global blackouts 07:00-10:00 ET and 15:00-19:00 ET.
- Weekday cutoff 16:50-19:00 ET.
- EUR-only LOW_ROOM_LOW_RR, LOW_ROOM_EXTENDED, M1_ALTERNATIVE_ADMISSION and learned/active research-veto authority.
- Research and adaptive risk remain SHADOW/no-authority.
- No leverage, hard-risk limit, MIN_ENTRY_RR or break-even threshold increase.

## Known limitations before IBKR Paper

- No IBKR account is connected.
- No real IBKR account/margin/contract metadata has been observed.
- No IBKR what-if margin request exists yet.
- IBKR execution remains impossible by design.
- Correlation still uses the existing conservative shared-currency hard guard; no rolling correlation optimizer was introduced.
- Slot policy has only the two explicitly approved tiers.
- Ranking weights are conservative engineering defaults and require independent audit before any future adaptive authority.
- This phase provides local implementation evidence, not profitability evidence and not deployment certification.

## Controlled hardening after IA #2 independent audit

The first V3.37.0 review was **NOT ACCEPTED**. IA #2 reproduced a HIGH-severity ranking defect: `float("nan")` does not raise, so the previous clamp path could convert non-finite confidence into an artificially favorable component and let a weaker USD_JPY candidate displace a valid EUR_USD candidate.

### Numeric sanitation policy

All numeric ranking inputs now pass finite validation (`math.isfinite`) before ranking math. NaN, +inf, -inf, `None`, non-numeric strings, containers and malformed objects cannot propagate into accepted rank components or final scores.

Critical fields are strategy signal quality (`score`) and explicitly supplied confidence (`dynamic_confidence`, or `confidence` when used). A malformed/non-finite critical field invalidates that candidate only; other valid candidates continue competing. Missing confidence retains the existing backwards-compatible finite signal-quality fallback, but an explicitly supplied corrupt confidence is never treated as missing.

Optional fields use conservative fallbacks rather than invalidating the whole cycle: corrupt RR => zero RR quality; corrupt structural room => zero room quality; corrupt spread/cost => zero cost quality. These defaults cannot improve the candidate.

The IA #2 reproduction is now a regression test: USD_JPY score 65 + `dynamic_confidence=NaN` is excluded, while EUR_USD score 68 + confidence 0.5 remains rankable and receives the slot.

### Safe instrument configuration default

`INSTRUMENTS` unset or empty now resolves to `PRIMARY_INSTRUMENT` (`EUR_USD`) only. GBP_USD/USD_JPY remain PAPER-capable by their unchanged profiles but require explicit configuration. AUD_USD/USD_CAD were analysis-only in the accepted hardening baseline. In the five-pair execution candidate they are OANDA Practice-capable only when explicitly configured, while LIVE remains denied.

### Single-worker execution invariant

V3.37.0 requires a single execution worker / single active replica per broker account. Horizontal scaling of execution workers is **NOT supported** until distributed coordination/locking exists. This hardening detects common explicit local/process worker-count settings and blocks new batch executions when the configured count is not exactly one. This is a local fail-closed guard, not a distributed lock; deployment configuration must keep execution worker/replica count at 1.


## Five-pair OANDA Practice execution hardening

All five target FX pairs now share the common PAPER execution framework: strategy candidate generation, deterministic ranking, slot policy, OANDA broker feasibility, global portfolio/correlation guards, verified metadata, broker-confirmed fills, trade management, recovery, persistence and observability. AUD_USD and USD_CAD have empty instrument-specific veto/exception sets and no learned research-veto authority.

Secondary instruments (GBP_USD, USD_JPY, AUD_USD, USD_CAD) require `InstrumentMetadata.source == "OANDA"` immediately before order construction. The execution path attempts a fresh metadata refresh and fails closed with `INSTRUMENT_METADATA_UNVERIFIED` if broker verification is unavailable. Local fallbacks remain analysis/test aids only.

### Ranked fallback and execution intents

The accepted RecoveryManager intent table remains the authoritative idempotency layer. A deterministic order key based on account, instrument, side, strategy, market time and order geometry is stable across scan cycles and restarts. Batch metadata adds cycle ID, signal ID, rank, slot index, broker and environment. Existing intent states and reconciliation prevent retries from creating a second order.

A clear pre-execution rejection or explicit OANDA rejection can fall through to the next ranked candidate. A timeout, transport ambiguity, incomplete response, acknowledged-but-unconfirmed order or duplicate non-terminal intent is treated as uncertain: fallback stops and RecoveryManager reconciliation is required before any replacement candidate can consume the slot.

Before every submit the system rebuilds broker risk context and rechecks slot availability, portfolio/correlation, OANDA metadata, global entry time, recovery state and broker feasibility. A first confirmed fill therefore changes the context used before a second-slot submission.

### Worker-count hardening

`WEB_CONCURRENCY`, `UVICORN_WORKERS` and `GUNICORN_WORKERS` are evaluated together. Empty values are treated as unset. Any malformed, non-positive or value greater than one fails closed; one or all explicit values equal to 1 are safe. This remains a single-replica invariant, not distributed locking.

### IBKR contracts remain inactive

`IbkrAccountSnapshot`, `IbkrWhatIfMarginResult` and broker-specific minimum-order verification contracts are defined for future integration. No productive fixed IBKR margin rate exists, and `IbkrBrokerRiskAdapter.execution_authority` remains `False`.


## V3.37.0 counterfactual selector observability — pre-deployment final
- Added `counterfactual_tracker.py` as SHADOW-only selector observability (`execution_authority=False`, `research_authority=False`, `look_ahead=False`).
- Productive COLLECT/RANK/SLOT/BROKER RISK/PORTFOLIO/EXECUTE decisions remain unchanged; `opportunity_ranker.py` and `slot_allocator.py` are byte-for-byte unchanged from the accepted five-pair baseline.
- Valid no-slot/lower-rank alternatives are persisted idempotently in `counterfactual_opportunities`; safety/execution rejections are logged separately and excluded from selector-regret evidence.
- Frozen entry/stop/target resolve incrementally to WIN/LOSS/TIMEOUT/AMBIGUOUS using only bars strictly after market time. Same-bar stop+target is AMBIGUOUS.
- Selector regret, per-instrument reliability, and head-to-head analytics are on-demand only and have no productive authority.
- Executed broker/PAPER results remain sourced from `trade_memory`; shadow rows are never represented as real broker trades or positions.
- Evidence grades are informational only: <15 UNDERPOWERED, 15–29 WEAK_LIMITED_EVIDENCE, >=30 USABLE.
- Tracker failures are isolated/logged and cannot modify selection or create duplicate orders.
- `counterfactual_tracker.py` is included in Runtime Integrity/release fingerprint for evidence integrity, despite having no execution authority.
- Five-pair OANDA Practice/PAPER authority, secondary LIVE denials, worker guard, risk ceilings, ranking weights, UNKNOWN-submit handling, recovery/idempotency, and IBKR execution-authority=false remain frozen.

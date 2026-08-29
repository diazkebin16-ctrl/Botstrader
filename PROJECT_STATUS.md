# BotsTrader Project Status

## Current development state — V3.37.0 IBKR Multi-Asset Preparation
- Canonical source commit: `1a35f004a467cb4f38a5004f43ed0ed5d7c6d35f` (certified V3.36.1 Multi-Asset PAPER Isolation).
- Development version: `3.37.0`. This is **NOT deployment-certified** and has not been pushed/deployed by this work.
- Five-pair analysis universe: EUR_USD, GBP_USD, USD_JPY, AUD_USD, USD_CAD.
- OANDA execution authority remains limited by instrument profiles: EUR_USD/GBP_USD/USD_JPY PAPER paths preserved; AUD_USD/USD_CAD analysis-ready but OANDA execution-inert.
- New batch flow: COLLECT -> RANK -> SLOT ALLOCATION -> BROKER RISK -> PORTFOLIO/CORRELATION -> EXECUTE.
- Capital policy: NLV < $5,000 => max 1 slot; NLV >= $5,000 => max 2 slots. Slots are ceilings only; broker/global risk may reduce to zero.
- IBKR adapter exists only as an INACTIVE/fail-closed preparation layer with `execution_authority=False`; no IBKR connection, credentials, account IDs or order path exist.
- Research/adaptive risk remain SHADOW/NO-AUTHORITY. No strategy threshold, leverage or hard-risk ceiling was increased.
- Railway/OANDA runtime deployment is out of scope for V3.37.0 development.

## Authoritative local baseline
- Version: V3.36.0 MULTI-ASSET FOUNDATION
- Base commit: GitHub `6d7f2bc` / V3.35.3 runtime-integrity baseline.
- Environment target: PAPER / OANDA Practice, but V3.36.0 has **not** been deployed to Railway.
- EUR_USD: execution-enabled baseline; regression behavior must remain preserved.
- GBP_USD: implemented in the Instrument Registry and multi-asset architecture; disabled by default and eligible for explicit OFFLINE/SHADOW activation only.
- GBP_USD: **NOT YET PAPER VALIDATED** and not authorized for LIVE.
- Production/Railway code must not be patched ad hoc. Changes are made to the local master, tested, certified, then promoted separately.

## V3.36.0 Multi-Asset Foundation
- Instrument Registry owns symbol normalization, broker metadata, pip location, display precision, trade-unit precision, minimum trade size and price/unit formatting.
- OANDA instrument metadata can replace conservative local fallbacks without granting execution authority.
- EUR_USD keeps its historical execution/model paths and primary strategy behavior.
- GBP_USD uses isolated signal/trade/management/recovery/learning/model/strategy-health/observation namespaces.
- Secondary instruments cannot inherit legacy active research-rule vetoes learned from EUR_USD.
- Instrument-aware sizing can reduce legacy requested units but cannot increase them or raise hard/global risk ceilings.
- Multi-instrument scanning remains sequential inside one worker with per-symbol exception isolation; heavy synchronous research/evaluation work remains offloaded from the asyncio event loop.
- Recovery/reconciliation persists symbol identity and can reconstruct simultaneous EUR_USD and GBP_USD managed positions deterministically.
- Runtime Integrity includes `instrument_registry.py` in critical code hashing and release fingerprinting.
- Architecture is precision/pip aware for future JPY instruments, but USD_JPY strategy calibration/generalization is explicitly out of scope.

## Preserved operational hardening
- OANDA dependent-stop replacement uses an explicit JSON `body=body` request.
- Trade-management R thresholds use broker-confirmed fill price when available.
- Protective-order failures are persisted/observable and retried on later scans.
- New-entry blackouts in `America/New_York`: 07:00-10:00 ET and 15:00-19:00 ET.
- Managed positions continue normal management during entry-blackout windows.
- Weekday rollover flattening window is 16:50-19:00 ET.
- Synchronous outcome resolvers, research refreshes, model refresh work, system evaluation, governance, smart-execution observability and ensemble observability remain off the asyncio event-loop thread where already hardened.
- Regression guard prevents direct POST/PUT/PATCH `req()` calls from passing request bodies positionally.

## Strategy-change boundary
V3.36.0 is an architectural expansion, not GBP_USD optimization. No RR, spread, barrier, entry, break-even, anomaly, ensemble, governance, leverage or hard-risk thresholds were relaxed to generate GBP_USD trades. Do not modify EUR_USD from GBP_USD results. Do not create new strategy exceptions from one or two outcomes.

## Validation classification
- EUR_USD regression: must be PASS before GitHub promotion.
- GBP_USD architecture/scenario validation: OFFLINE/SHADOW only.
- Broker metadata path: unit/scenario validated; live OANDA metadata must be observed again when GBP_USD is explicitly enabled in Practice/SHADOW.
- GBP_USD historical replay: not represented as completed unless a separate bid/ask historical dataset is actually run.
- GBP_USD PAPER: **NOT VALIDATED**.
- Railway runtime: **NOT DEPLOYED / NOT VALIDATED for V3.36.0**.

## Known limitations and next validation
- EUR_USD and GBP_USD can be correlated through USD exposure. Existing global/correlated hard limits remain active; no new correlation veto was invented in this release. Portfolio-correlation policy needs later evidence.
- Some strategy thresholds are intentionally unchanged from the frozen EUR_USD baseline. Instrument Registry generalizes execution precision/pips; future JPY strategy validation may still require evidence-based normalization review.
- Real event-loop lag and broker behavior must be measured after a separately approved runtime deployment. Offline tests verify the architecture does not add parallel scanner workers or remove existing offloads.
- The historical EUR_USD Indicator Discrimination Audit remains separate from GBP_USD expansion and must not use GBP shadow observations as EUR evidence.

## Public GitHub safety
Do not commit `.env`, credentials, runtime DBs, virtual environments, caches, logs, generated archives or large audit artifacts. Review the repository for secrets before every public synchronization.

## Canonical Git baseline
- Canonical reconciled code commit: `6e1aabe` — BotsTrader V3.36.0 research reconciliation.
- All future IA/chat work must start from the current GitHub `main` branch and preserve this reconciled baseline unless a newer explicitly approved canonical commit replaces it.
- Do not reconstruct work from older branches or local ZIPs when GitHub `main` contains newer certified changes.

## V3.36.1 candidate — Multi-Asset PAPER Isolation
- Candidate version: `V3.36.1`; canonical source remains current GitHub `main` (`06862d1`) over reconciled code reference `6e1aabe`.
- Added central instrument profiles so EUR-specific forward/research authority cannot leak into newly enabled symbols.
- EUR_USD remains primary and retains approved EUR-only `LOW_ROOM_LOW_RR`, `LOW_ROOM_EXTENDED`, `M1_ALTERNATIVE_ADMISSION`, and learned research-veto authority.
- GBP_USD and USD_JPY are prepared for PAPER/OANDA Practice execution, require broker-verified metadata before secondary order construction, and explicitly deny LIVE authority.
- GBP_USD and USD_JPY have no EUR-specific vetoes/exceptions by default.
- Global strategy geometry remains shared, including `MIN_ENTRY_RR=0.40`, `minimum_rr`, `barrier_room_ok`, canonical M1 confirmation, approved global blackouts and weekday cutoff.
- Existing hard portfolio, margin and correlated-position ceilings are now enforced by a minimum global execution guard so additional instruments cannot silently multiply aggregate risk. No hard-risk value or leverage ceiling was raised.
- Forward observations separate raw observed patterns from effective instrument-authorized vetoes.
- Directional research reconciliation, stacking/Jaccard and resolver-semantics work remain research/offline and were not reverted.
- Final local regression for this candidate: `275 passed, 0 failed, 4 warnings`; warnings are existing FastAPI `on_event` deprecations.
- V3.36.1 has **not** been deployed to Railway. GBP_USD/USD_JPY PAPER broker behavior still requires separately approved forward runtime observation.

## V3.37.0 controlled hardening after independent IA #2 review
- Independent review result for the first V3.37.0 candidate: **NOT ACCEPTED** because `opportunity_ranker._clamp01()` did not reject non-finite floats. In Python, `float("nan")` succeeds, and the old clamp expression could promote NaN to maximum component quality.
- Ranking inputs are now sanitized through finite-number validation using `math.isfinite()`.
- Critical ranking fields (`score` / signal quality and explicitly supplied confidence) invalidate only the corrupt candidate when malformed, NaN, +inf, -inf or otherwise non-numeric.
- Optional RR/room/cost fields use worst-case conservative quality on malformed/non-finite input: RR=0 quality, room=0 quality, cost=0 quality. Corrupt data cannot improve ranking.
- Accepted rank components and final `rank_score` are required to remain finite.
- `INSTRUMENTS` now defaults/falls back to `PRIMARY_INSTRUMENT` only. Secondary analysis instruments require explicit configuration. AUD_USD/USD_CAD were execution-inert in the accepted hardening baseline; the subsequent five-pair execution candidate enables them for OANDA Practice only, with LIVE still denied.
- Operational invariant: V3.37.0 requires **one execution worker / one active replica per broker account**. Horizontal scaling of execution workers is NOT supported until distributed coordination/locking is implemented.
- Common process worker-count settings (`WEB_CONCURRENCY`, `UVICORN_WORKERS`, `GUNICORN_WORKERS`) are detected; an explicit count other than one (or malformed explicit value) fails closed for new batch executions. This is not distributed locking and does not make multi-replica execution safe.
- Railway, IBKR connectivity and LIVE authority remain untouched/disabled. Version remains `3.37.0` pending independent re-audit.


## V3.37.0 five-pair OANDA Practice execution candidate
- Development base: accepted hardened ZIP SHA-256 `3454cd514a9ef997135876f7d248c1b9a6ad6f961176cf050cb05a1e74c3b218`.
- EUR_USD, GBP_USD, USD_JPY, AUD_USD and USD_CAD are PAPER/OANDA Practice-capable when explicitly configured. Secondary LIVE authority remains denied.
- AUD_USD/USD_CAD start with no EUR-specific vetoes/exceptions and no learned research-veto authority.
- Every secondary order requires verified OANDA metadata; FALLBACK metadata cannot authorize an order.
- Worker-count validation now inspects WEB_CONCURRENCY, UVICORN_WORKERS and GUNICORN_WORKERS together; any malformed, non-positive or >1 explicit setting blocks new batch execution.
- The existing RecoveryManager execution-intent/idempotency system is reused rather than duplicated. Its deterministic key remains stable across cycles/restarts; batch cycle, signal, rank, slot, broker and environment are attached as intent metadata.
- Ranked execution now falls through after clear pre-submit or explicit broker rejection, but UNKNOWN/submitted-unconfirmed outcomes stop fallback pending reconciliation.
- Broker/account/portfolio/metadata/recovery/slot state is freshly revalidated before each submit, including a possible second slot.
- Horizontal execution scaling remains unsupported; single active replica/worker per broker account is still mandatory.
- IBKR contracts were strengthened, but `IbkrBrokerRiskAdapter.execution_authority=False` remains invariant.
- This remains development/local validation only and is NOT deployment-ready.


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

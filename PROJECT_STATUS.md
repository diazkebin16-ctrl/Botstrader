# BotsTrader Project Status

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

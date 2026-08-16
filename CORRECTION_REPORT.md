# V3.27 Step 19 Correction Report

## Changes applied

1. Fixed Governance ↔ Anomaly integration so the latest confirmed/actionable critical composite anomaly is consumed as a conservative governance freeze recommendation.
2. Preserved SHADOW semantics: the recommendation can be `ADAPTATION_FROZEN`, while `enforced` remains false in SHADOW mode.
3. Added a repository `.gitignore` covering secrets, Python caches, local databases/logs, IDE metadata, and generated packages.
4. Updated the README header/status from V3.25 Step 17 to V3.27 Step 19 and documented the Step 19 safety posture.

## Validation

- Full pytest suite: **138 passed, 0 failed, 4 FastAPI deprecation warnings**.
- Step 14 integration framework command: exit code **0**.
- Basic secret-like pattern scan: **no matches found** in source/config files scanned (excluding docs/reports/example env).

## Remaining non-blocking warnings

FastAPI `@app.on_event("startup"/"shutdown")` is deprecated in favor of lifespan handlers. This does not fail the current test suite but should be migrated in a later maintenance pass.

## Safety posture

- Anomaly Engine direct trade authority: none.
- Anomaly Engine risk-increase authority: none.
- Governance mode remains SHADOW unless explicitly configured otherwise.
- This package is suitable for repository/testing work; real-money production readiness still requires the production gates and external/paper validation described by the project.

## V3.27 runtime hardening — 2026-08-16

Runtime review identified four presentation/integration issues and they were corrected without increasing production authority or risk:

1. **Closed-market data state** — weekend/closed-market candles are now reported as `MARKET_CLOSED`, with `fresh=false` and `stale=false`. A scheduled market close is healthy for research/monitoring, but is no longer presented as fresh tradable data.
2. **Ensemble duplicate-model protection** — repeated observations with the same `strategy_id` are collapsed to the newest observation per cycle. Participating, abstaining, offline and reasoning lists are also de-duplicated before persistence/output.
3. **Adaptive-learning authority clarity** — the learning status now explicitly reports `changes_execution=false` and `adaptive_learning_changes_production_execution=false`; the separate calibrated-confidence gate is exposed as `adaptive_confidence_gate_enabled` so it cannot be confused with Adaptive Learning deployment authority.
4. **Worker restart semantics** — the initial supervised worker launch no longer increments `worker_restarts`. The counter now represents actual replacement launches after the initial worker.

Additional fix: recovery startup no longer references the undefined `fx_market_open()` helper; it uses the existing weekend-market-close function.

### Validation

- `pytest -q`: **142 passed, 0 failed**.
- Remaining warnings: 4 FastAPI `on_event` deprecation warnings (non-blocking; migrate to lifespan handlers in a later maintenance pass).
- `system_integration_test_framework.py`: exit code **0** after the runtime hardening.

No hard risk limits were increased. Smart Execution, Capital Allocation, Ensemble, Adaptive Risk and Anomaly/Governance controls retain their existing SHADOW/advisory authority boundaries.

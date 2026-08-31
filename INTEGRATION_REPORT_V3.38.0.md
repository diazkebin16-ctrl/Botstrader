# EUR/USD + GBP/USD Forward Experiment Integration Report — V3.38.0 Candidate

## Input identity

- Canonical base SHA-256: `f648f95b246573de67726d7874a0d51d7ead58e61d208fe11473a3f740af5c25` — PASS.
- EUR Phase 2 SHA-256: `8763b9302c40fbcdfb4a92a278052dfc55c756352723c9a05316d499c35424d8` — PASS.
- GBP Phase 2 SHA-256: `d0d078134e6f0a614e55463c09774d3f3bebe4fb9f3e75fbac5a8b701ad7ae7f` — PASS.
- EUR historical JSON SHA-256: `0d848028e64b8f9590100de73c048795857efb4ea7480cbfdf1a91ab49bf4474` — PASS.
- GBP historical JSON SHA-256: `f7360988947b76743f5abdf594a22ccba22e2b4440684608b9c51809d6a35fef` — PASS.

## EUR/USD rule and strategic map

Frozen forward candidate: `legacy_v331_directional_score >= 31.0`. Runtime computes the exact V331 BUY and SELL scores from pre-entry hypothesis state; historical tie semantics are preserved (`BUY` when BUY >= SELL). The legacy directional score is the score of that legacy-selected direction. The current session-aware production `direction_score` is not reused as a substitute.

Historical equivalence: PASS. Discovery reproduces 27 WIN / 69 LOSS kept and 0 WIN / 10 LOSS blocked. Holdout reproduces 14 WIN / 30 LOSS kept and 2 WIN / 7 LOSS blocked. The Phase 2 population is 159 unique opportunity IDs = 43 WIN + 116 LOSS.

Strategic rule map:

- M1 canonical confirmation — current V3.37 status: ACTIVE global strategic quality gate. Phase 1: opened for the target population. Phase 2: candidate evaluated on the Phase 1-opened population. Forward V1: BYPASS only for EUR PAPER/practice. Reason: adding directional score on top of canonical M1 would contradict the tested Phase 2 population.
- M1_ALTERNATIVE_ADMISSION — current V3.37 status: ACTIVE EUR-specific exception. Phase 1: 38 WIN / 108 LOSS admitted before incremental opening. Phase 2: FRAGILE as a candidate. Forward V1: profile/authority is preserved, but is not reached while the EUR PAPER experiment M1 bypass is active; outside the experiment its V3.37 behavior remains unchanged.
- LOW_ROOM_LOW_RR — current V3.37 status: ACTIVE EUR PAPER veto. Phase 1: OPEN/BYPASS. Phase 2: FAILED. Forward V1: BYPASS only for EUR PAPER/practice.
- LOW_ROOM_EXTENDED — current V3.37 status: ACTIVE EUR PAPER veto. Phase 1: OPEN/BYPASS. Phase 2: HOLDOUT_SURVIVOR but not the selected individual forward candidate. Forward V1: BYPASS only for EUR PAPER/practice so the forward test remains the frozen individual `directional_score` candidate rather than an unselected combination.
- QUALITY:EXTENSION — current V3.37 status: ACTIVE global strategic timing gate. Phase 1: OPEN/BYPASS. Phase 2: HOLDOUT_SURVIVOR but not the selected individual forward candidate. Forward V1: BYPASS only for EUR PAPER/practice.
- learned research veto — current V3.37 status: EUR execution authority present. Historical classification remains `NOT_HISTORICALLY_RECONSTRUCTABLE`. Forward V1: authority PRESERVED exactly; no PASS/FAIL is invented and no historical approximation is used.

The Phase 1 exception-aware population excluded one LOSS. No trade-ID exception, timestamp exception, or new Phase 1 threshold was encoded to preserve that single case. The forward configuration generalizes the explicitly opened strategic M1 state rather than overfitting a one-row historical exclusion.

## GBP/USD rule and BUY/SELL semantics

Frozen forward combo: `extension_atr <= 1.4985678822167452 AND legacy_v331_buy_score >= 16.400000000000002`.

DISC003 semantics are literal: the historical feature is the V331 **BUY-side score for every episode**, including SELL episodes. Runtime therefore does not substitute SELL score, max-side score, chosen-direction score, or the current session-aware production score.

Historical equivalence: PASS. Discovery reproduces 20 WIN / 43 LOSS kept and 5 WIN / 50 LOSS blocked. Holdout reproduces 22 WIN / 38 LOSS kept and 4 WIN / 22 LOSS blocked.

GBP Phase 1 froze the canonical M1 components FULLY_OPEN. Forward V1 therefore bypasses canonical M1 only for GBP PAPER/practice. The existing global 1.50 ATR quality ceiling is preserved because the frozen DISC002 threshold is stricter and therefore determines admission first/equivalently for passing candidates.

## Observability

Each forward evaluation stores an experiment identifier and deterministic audit fields in the existing forward decision audit: instrument, runtime direction, legacy V331 BUY/SELL scores, legacy chosen direction/directional score for EUR, extension and thresholds for GBP, per-component pass/fail, combined pass/fail, and the stable rejection reason. No outcome or future field is an input to the forward gate.

## Safety/infrastructure invariants

Standalone risk/recovery/execution/profile modules are byte-identical to V3.37.0. Critical server function bodies for sizing, risk validation, break-even/trade management, time gates, broker execution, recoverable execution, ranked execution and portfolio guards are SHA-identical to the canonical base. See `TEST_EVIDENCE_V3.38.0.json`.

## Validation

- Dedicated V3.38 forward tests: 14 passed / 0 failed.
- Full regression: 408 collected; 408 passed / 0 failed / 0 errors across three isolated pytest chunks. The monolithic invocation exceeded the execution-tool time limit, so the entire collected suite was executed in deterministic chunks instead; every collected test passed.
- Historical equivalence: PASS for EUR and GBP.
- `compileall`: PASS.
- Look-ahead: PASS.
- Outcome dependency: NONE.
- Determinism: PASS.

## Known limitations

1. These selected historical configurations remain negative expectancy/PF on their cited holdouts; this package is for forward evidence collection, not profitability certification.
2. EUR learned research veto state is not historically reconstructable and remains authoritative forward; future forward attribution must distinguish its rejections from the new frozen Phase 2 gate.
3. Upstream session-aware runtime direction selection is preserved. Legacy V331 direction/score is computed for the frozen EUR admission feature and recorded so divergence from runtime direction is observable rather than silently conflated. GBP DISC003 intentionally remains BUY-side regardless of runtime direction.

## Authority

No deployment. No Railway. No OANDA call/runtime change. No GitHub remote change. No increase in LIVE authority.

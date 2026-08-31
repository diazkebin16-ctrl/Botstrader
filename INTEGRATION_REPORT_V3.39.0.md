# BotsTrader V3.39.0 — USD/JPY Forward Experiment Implementation

Base: GitHub main commit fc1cdd09742f29be404f69de742e4213a417480e, BotsTrader V3.38.1.

Implemented USDJPY_PHASE2_FORWARD_V1 by extending the existing forward_experiment policy/evaluator. No second strategy engine or execution path was created. The experiment is active only when instrument=USD_JPY and the existing server-side forward experiment authority verifies TRADING_ENVIRONMENT=PAPER, PRIMARY_OANDA_ENV=practice, and the fxpractice endpoint.

Frozen strategy: M1 confirmation bypass OPEN, QUALITY:EXTENSION bypass OPEN, then chosen_legacy_score >= 33.0. Runtime chosen score is BUY-side legacy V331 score for BUY and SELL-side legacy V331 score for SELL. GBP's intentionally different BUY-side-even-for-SELL semantics remain untouched.

Historical equivalence PASS: Discovery 44 WIN kept / 4 blocked / 69 LOSS kept / 10 blocked / 14 TIMEOUT kept. Holdout 24 WIN kept / 4 blocked / 46 LOSS kept / 12 blocked / 7 TIMEOUT kept. Mutable historical state remains NOT_HISTORICALLY_RECONSTRUCTABLE.

Protected server functions for time gates, sizing, portfolio guard, execution, recovery execution, trade management, post-fill geometry/replacement/verification/reanchor and quality/execution decision are SHA-identical to V3.38.1. record_trade_memory_entry changed only to add pre-entry experiment attribution to entry_context_json.

Validation: focused USDJPY 15/15; existing forward 14/14; post-fill 32/32; execution/risk/recovery 81/81; full regression 457/457 with 4 existing FastAPI deprecation warnings; compileall PASS.

No Railway changes, no OANDA writes, no GitHub remote changes, no deployment.

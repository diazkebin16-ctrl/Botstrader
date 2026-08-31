# V3.38.1 Pre-Deployment Closure

Base candidate SHA-256: `17a9f1a9981a4ae78bf220f51a6ebed195d4013935b3748da7e688d84960f351`

## Runtime version correction

`server.py` now declares `VERSION_TAG = "3.38.1"`. No production function body changed. Searches of non-document runtime/test source contain no remaining `3.38.0` identification. Historical documentation/evidence references were intentionally preserved.

## GLOBAL_ENTRY_TIME_GATE reproducibility

The original real-clock full regression was reproduced during the 15:00-19:00 ET blackout: 437 passed, 3 failed, 4 warnings. The three failures were the exact previously identified tests and stopped at `GLOBAL_ENTRY_TIME_GATE` before reaching their intended assertion (`AUTO_TRADE=false`).

The two affected test functions (three pytest cases because one is parameterized for AUD_USD/USD_CAD) now explicitly monkeypatch `new_entry_time_gate` to an allowed deterministic result because those tests validate fresh risk-context/slot behavior and metadata verification, not wall-clock entry gating. Production gate logic was not changed.

A dedicated deterministic boundary suite (`test_global_entry_time_gate_repro_v3381.py`) verifies the production gate directly at fixed New York boundaries: 06:59 allowed, 07:00 blocked, 10:00 allowed, 14:59 allowed, 15:00 blocked, 19:00 allowed.

Focused version/time validation: 45 passed, 0 failed, 4 warnings.
Post-fill suites: 32 passed, 0 failed, 4 warnings.
Full regression: 442 passed, 0 failed, 4 warnings.
Compileall: PASS.
EUR and GBP historical equivalence: PASS exact.

No Railway, OANDA, GitHub remote, or deployment actions were performed.

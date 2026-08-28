# Refactor Report — v3.35 Historical Execution + OOS Validation

## Scope
This release completes the pre-Step-20 replay fidelity work without changing live strategy parameters, risk limits, signal thresholds, or order authority.

## Architectural changes
- Added `historical_execution.py` as an isolated research-only execution model.
- Historical downloads now request OANDA midpoint + bid + ask (`MBA`) candles.
- Historical replay no longer uses the midpoint shifted-barrier cost approximation. BUY entries cross ask; SELL entries cross bid; exits are evaluated on the executable side.
- Entry and exit slippage are explicit deterministic adverse assumptions and are applied once in fill prices.
- A stale signal is not force-filled when the first executable price is already outside valid stop/target geometry (`ENTRY_INVALIDATED`).
- Midpoint-only historical caches fail explicitly with `DATA_INSUFFICIENT`; there is no silent fallback to optimistic execution.
- Added `replay_validation.py` for chronological discovery/validation/test holdout with event-overlap purging and temporal embargo.
- Added fixed-policy walk-forward reporting. No parameter is selected or tuned inside these folds.
- Default session-scale sweep was removed. The certification-eligible path is frozen at `SESSION_1X`; explicit multi-scale sweeps are marked research-only.

## Preserved behavior
- Live/paper order execution is unchanged.
- Strategy thresholds, minimum RR, directional weights, safety gates, leverage, and hard risk limits are unchanged.
- TIMEOUT and AMBIGUOUS remain non-binary terminal outcomes and do not receive fabricated R values.
- Correlated snapshots are still collapsed into market episodes before performance evaluation.

## Validation
- Full pytest regression: 189 passed.
- Remaining warnings are FastAPI `on_event` deprecations in the existing runtime lifecycle code; they are not replay failures and were not mixed into this refactor because replacing lifecycle architecture is a separate production-runtime change.

## Known limitations
- M1 OHLC cannot reveal intrabar tick ordering; if executable-side TP and SL are both touched in one candle the result remains `AMBIGUOUS`.
- Historical candle data has no order-book depth, so partial-fill probability cannot be reconstructed faithfully. The replay does not invent depth or partial fills.

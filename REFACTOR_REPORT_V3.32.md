# Botstrader V3.32 — Evidence + Session-Regime Refactor

## Goals

This release removes two root causes found during the V3.31 audit instead of adding another execution bypass:

1. research statistics were inflated by minute-by-minute copies of the same market opportunity;
2. directional scoring had no explicit current-session trend, so slow H1/M15 context could dominate an intraday reversal/continuation.

## Changes

### Research outcome engine

`research_evidence.py` now owns outcome resolution and market-episode collapsing.

- M1 midpoint outcome resolution is cost-aware via `RESEARCH_ROUND_TRIP_COST_PIPS` (default 1.0 pip).
- Effective TP/SL levels are stored for auditability.
- `TIMEOUT` and `AMBIGUOUS` remain explicit terminal states and are no longer conceptually confused with pending samples.
- New DB fields persist outcome cost and effective levels for canonical and shadow outcomes.
- `refresh_filter_hypotheses()` evaluates independent market episodes rather than every minute snapshot.
- `should_retrain_model()` uses independent episode counts, reducing false sample maturity.

### Session regime engine

`session_regime.py` explicitly models ASIA, LONDON and NEW_YORK using timezone-aware clocks.

The current session direction is inferred from:

- displacement from session open normalized by M5 ATR;
- recent session momentum;
- session structure (higher/lower range progression).

Overlaps prefer the newer liquidity center (New York > London > Asia).

Directional hierarchy was changed so H1 is slow context rather than a direction lock:

- H1 weight reduced;
- M15 remains relevant;
- current-session regime receives a large positive/negative contribution;
- M5/M1 remain execution evidence.

`countertrend` now means opposing the current session and M15 together, rather than simply opposing H1+M15.

### RR correction

When the M5 structural swing provides no positive reward, `rr_raw` is now `0.0` rather than being fabricated as `MIN_RR` (1.5 by default).

### Version/test hygiene

- Version advanced to V3.32.
- Stale V3.27 dashboard/version assertions were updated.
- Added dedicated tests for research outcomes, episode collapsing and session-regime detection.

## Validation

Python compilation passed for the modified modules.

Test suites were executed in two groups:

- 90 passed
- 77 passed

Total: **167 tests passed**, with only FastAPI deprecation warnings for legacy `@app.on_event` startup/shutdown hooks.

## Not changed

This release does **not** relax `minimum_rr`, M1 confirmation, Production Readiness, Recovery, Security, Governance, broker protections, or live-order authorization. The existing `TREND_CONTINUATION_SHADOW` remains research-only.

## Next phase

The remaining evidence consumers (`empirical_confidence`, autonomous discovery, strategy-health summaries, and external hypothesis evaluation) should be migrated to a single episode-aware dataset API so no subsystem can accidentally return to minute-level pseudoreplication.

# Counterfactual Selector Observability — V3.37.0

## Purpose and authority

This module measures selector outcomes without changing them. `counterfactual_tracker.py` is SHADOW observability only: `execution_authority=False`, `research_authority=False`, and `look_ahead=False`. It is not imported by `opportunity_ranker.py` or `slot_allocator.py`, and its reports are never consumed by entry, sizing, risk, ranking, veto, promotion, demotion, or broker-order logic.

## Valid selector counterfactual

A primary counterfactual is a strategy-valid ranked opportunity that could have passed the current safety framework as an alternative, but did not receive execution capacity because another safe candidate consumed the available slot(s). These rows use `rejection_category=SELECTION_REJECTED` and reasons such as `NO_SLOT`, `LOWER_RANK`, or `BEST_SAFE_SET_NOT_SELECTED`.

Safety or execution failures are retained as tracker events, not as valid selector counterfactual trades. Examples include portfolio/correlation risk, metadata, broker risk, global gates, recovery/security state, explicit broker rejection, and uncertain post-submit state. This prevents unsafe alternatives from being interpreted as evidence that the selector chose the wrong instrument.

## Persistence and idempotency

`counterfactual_opportunities` stores deterministic persistent identity, cycle/signal/decision linkage, instrument/side/strategy, immutable entry-stop-target geometry, initial risk, target R, rank/score/components, slot/cycle context, winner linkage, rejection classification, state, result R, duration, and a pre-entry JSON snapshot. The primary key is a deterministic SHA-256-derived counterfactual ID. A second UNIQUE constraint covers cycle/instrument/signal/market-time/geometry. Reprocessing or restart therefore does not duplicate logical evidence.

Indexes cover `(status,instrument,market_time)`, `cycle_id`, winner/status, and rejection category/reason. Resolution only queries OPEN rows for the instrument currently being scanned. Historical analytics are on-demand and are not executed in the ranking path.

`counterfactual_tracker_events` stores non-counterfactual rejections and tracker lifecycle/errors without credentials or authorization headers.

## Outcome state machine

States are `OPEN`, `WIN`, `LOSS`, `TIMEOUT`, `AMBIGUOUS`, `INVALIDATED`, with `CANCELLED` reserved as an optional terminal state.

- WIN: frozen target touched before frozen stop. `result_r = abs(target-entry)/abs(entry-stop)`.
- LOSS: frozen stop touched before target. `result_r = -1.0` because the stored initial stop defines one initial risk unit.
- AMBIGUOUS: the same available bar touches both stop and target and no finer ordering evidence exists. No numeric R is assigned.
- TIMEOUT: neither boundary resolves within `COUNTERFACTUAL_HORIZON_BARS`; no numeric R is assigned.
- INVALIDATED: malformed or insufficient geometry/time evidence; excluded from closed expectancy.

The default shadow horizon reuses `OUTCOME_HORIZON_MIN` (180 M1 bars) unless the shadow-only `COUNTERFACTUAL_HORIZON_BARS` is explicitly configured. This setting has no execution authority.

## No-look-ahead rule

Resolution uses only bars whose timestamp is strictly greater than the frozen `market_time`. Bars before T and the bar stamped exactly T are ignored. Entry, stop and target are never recalculated from later data. If intrabar ordering cannot be established, the result is AMBIGUOUS rather than guessed.

## Winner linkage and selector regret

The highest-ranked executed candidate in the cycle is linked by instrument/rank/score and, when available, signal ID, execution intent ID and broker trade ID. The executed result remains sourced from the existing real/PAPER `trade_memory` ledger; it is not duplicated as a shadow trade.

For comparable terminal outcomes:

`selector_regret_R = rejected_counterfactual_result_R - executed_winner_result_R`

Positive regret means the rejected valid opportunity finished better; negative regret means the selected winner finished better; zero is a tie. TIMEOUT, AMBIGUOUS, INVALIDATED, or missing executed R produces `regret_unknown` and no forced numeric value.

## Analytics

`instrument_reliability_report()` keeps executed broker/PAPER evidence and counterfactual shadow evidence separate. It exposes counts, wins, losses, win rate, expectancy R, average win/loss R, recent sample count/expectancy, selection rate, shadow rejection rate, and evidence grade. Combined counts are explicitly labeled mixed evidence and are not broker-execution evidence.

`head_to_head_report(A,B)` derives reproducible pairwise competition evidence from raw rows: times competed, A/B selected, selected wins/losses, counterfactual wins/losses, selector correct/wrong/ties, ambiguous/timeouts, and average regret R.

Evidence grades are informational only: n < 15 `UNDERPOWERED`; 15–29 `WEAK_LIMITED_EVIDENCE`; n >= 30 `USABLE`. They grant no authority.

## Failure isolation and Runtime Integrity

Tracker persistence/resolution failures are logged and surfaced as WARNING observability events when observability is enabled. They do not alter the already-computed productive ranking, do not create orders, and do not grant fallback or veto authority.

Although the module has no execution authority, it is included in the release fingerprint and Runtime Integrity list because pre-deployment evidence integrity is important and silent code drift in the tracker is undesirable. This inclusion does not make it an execution-critical decision module.

## Explicit non-features

There is no pair reliability weight, automatic rank weight, dynamic instrument preference, online selector learning, auto-promotion, auto-demotion, reliability gate, counterfactual veto, risk change, or score change in V3.37.0.

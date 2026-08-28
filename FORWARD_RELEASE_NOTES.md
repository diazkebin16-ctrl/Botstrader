# BotsTrader V3.35.2 — Clean PAPER Forward Release

This release was rebuilt from `botstrader_directional_component_diag_ready.zip` and consolidates the validated post-ZIP trading changes directly into the persistent source tree rather than relying on runtime container patches.

## Consolidated changes

- Final target remains `MIN_RR=1.50R`.
- Entry admission is split from target sizing with `MIN_ENTRY_RR=0.40R`.
- Maximum entry extension remains `1.50 ATR`; the extra low-confidence extension veto inside that envelope is removed.
- Strong-barrier RR admission uses the `0.40R` entry threshold while the managed target remains `1.50R`.
- M1 admission telemetry is explicit (`m1_ema9_side_ok`, `m1_candle_color_ok`, `m1_exception_shadow`) and the validated admission path is integrated without rewriting canonical `m1_confirmation`.
- Adaptive confidence uses resolved `executed=1` observations only. Before `CONFIDENCE_MIN_SAMPLES` executed labels exist, the adaptive confidence gate is `OBSERVE_ONLY` and has no veto authority.
- `low_room_low_rr_shadow`: `room_to_barrier_r < 0.40` and `rr_raw < 1.00`.
- `low_room_extended_shadow`: `room_to_barrier_r < 0.60` and `extension_atr > 0.80`.
- The two forward filters have execution authority only when `TRADING_ENVIRONMENT=PAPER`, `PRIMARY_OANDA_ENV=practice`, and the broker endpoint is OANDA practice. They cannot silently become production filters.
- Break-even activates at `+1.00R` and moves the stop to `0.00R` by default (`BREAK_EVEN_LOCK_R=0.00`). Profit-lock and trailing logic remain unchanged.
- The shadow flags are intentionally excluded from `FEATURE_COLUMNS`, so this release does not alter ML input dimensionality.

## Validation

- `python -m py_compile server.py`: PASS
- New forward-release tests: 6/6 PASS
- Existing test suite was split into two deterministic batches to avoid a harness-level long-running shutdown issue:
  - Batch A: 115 PASS
  - Batch B: 90 PASS
  - Total existing + new tests: 205 PASS
- Security integration and runtime-hardening tests: PASS.

## Deployment boundary

This package is intended for OANDA Practice / PAPER forward validation. The two newly activated entry filters are explicitly denied authority in production mode. No hard risk limit, leverage ceiling, or position-size ceiling is increased by this release.

## Forward observability / attribution hardening

Added observational-only telemetry for the current PAPER experiment without changing execution authority or thresholds:

- Every `decision_log` row now stores `forward_audit_json` with pre-filter scores, direction edge, RR/room/extension, current-vs-prior RR reference, and independent veto flags for `minimum_rr`, `barrier_room_ok`, `LOW_ROOM_LOW_RR`, and `LOW_ROOM_EXTENDED`.
- The veto vector is evaluated independently of execution short-circuit order so later stacking analysis is order-independent.
- `trade_forward_observations` and `trade_forward_events` record R milestones (0.50/0.75/1.00/1.25/1.50R), BE activation, max R seen, and max R observed after BE. Telemetry failures are best-effort and cannot veto, modify, or submit orders.
- `run_filter_stacking_audit.py` reports total vetoes, true unique vetoes, remove-one delta, pairwise intersection/Jaccard, and preregistered evidence classes: n<15 `INCONCLUSIVE_UNDERPOWERED`, 15-29 `WEAK_EVIDENCE`, >=30 `USABLE_EVIDENCE`.
- `run_resolver_semantics_audit.py` provides an offline old-label vs current cost-aware resolver comparison when a historical M1 candle bundle is supplied. It does not write to the database.

Interpretation guardrail: high Jaccard means mechanical redundancy only; it is not evidence by itself that a filter is over-restrictive or should be relaxed.

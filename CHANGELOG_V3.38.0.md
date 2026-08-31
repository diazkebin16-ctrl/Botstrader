# BotsTrader V3.38.0 Candidate Changelog

## Added
- Shared pure legacy V331 scoring formula (`legacy_v331_scoring.py`).
- Instrument-scoped PAPER forward experiment policy (`forward_experiment.py`).
- EUR Phase 2 gate: `legacy_v331_directional_score >= 31.0`.
- GBP Phase 2 combo: `extension_atr <= 1.4985678822167452 AND legacy_v331_buy_score >= 16.400000000000002`.
- Stable experiment identities: `EUR_PHASE2_FORWARD_V1`, `GBP_PHASE2_FORWARD_V1`.
- Deterministic forward observability for legacy scores, thresholds, component pass/fail, combined pass/fail and direction.
- Historical-equivalence runner/evidence and dedicated tests.

## Strategic admission updates
- EUR PAPER/practice: canonical M1, LOW_ROOM_LOW_RR, LOW_ROOM_EXTENDED and QUALITY:EXTENSION are opened/bypassed for this forward experiment before applying the frozen EUR Phase 2 gate. Safety remains untouched.
- GBP PAPER/practice: canonical M1 is opened as frozen by GBP Phase 1; the existing 1.50 ATR quality ceiling remains, while the stricter frozen DISC002 threshold is enforced by the experimental combo.
- EUR learned research veto remains authoritative and is not approximated historically.

## Not changed
Safety, risk, leverage, sizing, broker protections, trade management, break-even, recovery, time gates, duplicate protection, portfolio controls, order idempotency, and non-target instrument strategy behavior.

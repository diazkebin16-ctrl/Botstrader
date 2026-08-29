# BotsTrader V3.35.3 — Research Direction Audit

Research-only extension. No production trading rule, safety gate, risk limit, minimum_rr threshold, live direction selector, or order path was changed.

## Added
- Population / selection audit for frozen geometry and Test-B shadow populations.
- E1 `DIFFERENTIAL_EVIDENCE_ASSOCIATION_AUDIT` using BUY-minus-SELL **score contributions**, not duplicated raw market metrics. Primary E1 excludes M1 confirmation because it is a hard replay-inclusion gate; RR is secondary only.
- E3 score-margin calibration diagnostic.
- E2 frozen `session_regime` shadow diagnostic for reconstructed counterfactual rows.
- Session direction/strength captured in component snapshots.

## Methodological constraints
- Existing benchmark results remain diagnostic only.
- E1 reports association, not causality, because opportunity inclusion is selection-conditioned.
- `m1_score_contribution` is **NON_IDENTIFIABLE_SELECTION_CONDITIONED** on this benchmark and is excluded from primary E1/BH-FDR.
- `rr_score_contribution` is secondary because RR/minimum-RR are endogenous to constructed directional geometry.
- The M1 exclusion applies the same pre-run inclusion-gate criterion already used for extension.
- Session-regime findings on existing data are not confirmatory; prospective data is required.
- `p > threshold` is not treated as proof of no effect; low effective N is reported as `INCONCLUSIVE_UNDERPOWERED`.
- Benjamini-Hochberg FDR is applied to E1 concordance tests.

## Verified provenance
- Raw H1/M15 gaps/slopes and raw M5/M1 momentum are common market measurements; E1 therefore does not subtract duplicated raw values.
- `second_pullback` is not an explicit `_replay_gate()` requirement.
- extension is a replay quality gate but not a directional differential.
- M1 confirmation is also a replay inclusion gate; contributing to the live score does not make it identifiable in a sample conditioned on M1 confirmation.
- Test B continues to ignore only `minimum_rr` and `barrier_room_ok` for research-only counterfactual geometry.

## Validation
- `python -m py_compile server.py historical_replay.py directional_null_test.py session_regime.py`: PASS
- `pytest -q test_directional_null_test.py test_replay_validation.py test_session_regime.py test_historical_execution.py`: PASS

Production behavior changed: **FALSE**.

## Pre-run methodology correction
- Frozen before upload/run: 2026-08-25T23:49:46.337633+00:00
- Primary E1: H1, M15, M5, pullback, broken barriers, total direction score.
- Secondary E1: RR contribution (selection-conditioned/endogenous geometry).
- Excluded from primary E1: M1 confirmation and extension.

## Reconciliation note for V3.36.0
This research layer was selectively reconciled onto V3.36.0 Multi-Asset Foundation. Production code, strategy thresholds, execution gates, sizing, forward PAPER filters, and Multi-Asset behavior were not changed.

### Predefined E1 evidence-size thresholds
These thresholds are fixed before examining reconciled results and must not be tuned post-hoc:
- `n < 15`: `UNDERPOWERED`
- `15 <= n < 30`: `WEAK_LIMITED_EVIDENCE`
- `n >= 30`: `USABLE`

### Explicit E1 exclusion
`m1_score_contribution` is excluded from the E1/BH-FDR hypothesis family whenever the replay population is mechanically conditioned on M1 confirmation for the selected/actionable side. It is not independent evidence in that population because its contribution is mechanically conditioned by construction of the chosen side. This exclusion is disclosed rather than replaced with a post-hoc variable.

# Botstrader v3.32.1 — Evidence Integrity

## Scope
This release completes the episode-aware evidence refactor before any further session-regime calibration.

## Changes
- Added `annotate_market_episodes()` so canonical and shadow observations from the same instrument/direction/time move share one `episode_id` regardless of source or variant.
- `_auto_dataset()` now builds the canonical+shadow union first, assigns episodes globally, then retains one canonical row plus one row per shadow variant per episode.
- Autonomous discovery now uses `split_episode_holdout()`: a chronological holdout split by whole episodes. An episode can never occur in both discovery and validation.
- Research metrics now distinguish raw shadow trial activity from shadow episode counts and expose canonical episode counts.
- Version advanced to 3.32.1; security/version assertions updated accordingly.

## Statistical semantics
Shadow variants remain available as counterfactual comparisons and retain their configured shadow weight, but repeated minute snapshots from the same market episode no longer create repeated independent votes. Holdout membership is assigned at episode granularity to prevent canonical/shadow leakage across the temporal boundary.

## Validation
- Python compilation succeeds for modified modules.
- `test_research_evidence.py`: 6/6 pass, including explicit tests that source/variant do not change episode identity and that holdout never splits an episode.
- Full-suite execution progressed beyond the modified/relevant tests without a functional failure; the monolithic suite exceeds the execution window in this environment. The only initial failure was the expected version assertion (3.32 -> 3.32.1), which was updated.

## Intentionally deferred
- No further tuning of `session_regime` weights.
- No dynamic spread/slippage calibration yet; fixed research cost remains conservative and deterministic.
- No live execution/security/governance authority changes.

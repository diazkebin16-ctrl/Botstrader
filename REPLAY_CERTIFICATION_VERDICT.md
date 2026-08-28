# Replay Certification Verdict

Status: NO CERTIFICATION

Benchmark file:
- /data/historical_replay_m1shadow.json

Methodology:
- Historical bid/ask execution
- Explicit adverse slippage
- Independent market episodes
- TIMEOUT / AMBIGUOUS explicit
- Chronological holdout
- Walk-forward validation
- Purging
- 30-minute embargo

Frozen configuration:
- EUR_USD
- 2026-07-01T00:00:00Z to 2026-08-21T20:00:00Z
- Horizon: 180 M1 bars
- Entry slippage: 0.10 pip
- Exit slippage: 0.10 pip
- Session scale: 1.0

V331_BASELINE:
- Overall expectancy R: -0.468
- Overall PF: 0.434
- OOS expectancy R: -0.691
- OOS PF: 0.253
- Positive walk-forward folds: 1 / 4

SESSION_1X:
- Overall expectancy R: -0.445
- Overall PF: 0.455
- OOS expectancy R: -0.812
- OOS PF: 0.170
- Positive walk-forward folds: 1 / 4

Conclusion:
The current deterministic strategy core does not demonstrate robust positive
out-of-sample edge and is NOT certified for promotion based on this replay.

Research rule:
Do not optimize parameters against the validation/test results above.
Any new strategy hypothesis must be developed from discovery evidence only,
then evaluated on untouched future/out-of-sample data.

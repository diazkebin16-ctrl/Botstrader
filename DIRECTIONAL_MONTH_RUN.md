# Ten-lane two-month optimization

Frozen window: 2026-07-15 through 2026-09-14.
Month 1: 2026-07-15 through 2026-08-14 (discovery only).
Month 2: 2026-08-15 through 2026-09-14 (frozen holdout).
Minimum 10 resolved WIN/LOSS outcomes in each month for every final candidate.
TIMEOUT and AMBIGUOUS episodes do not count toward the monthly minimum.
Target: strictly above 50% wins with positive expectancy in both months.
Research-only worker: AUTO_TRADE=false, SIMULATION, no order authority.

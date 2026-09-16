# Adaptive 60-day PAPER strategies

The ten lanes (BUY and SELL on five FX pairs) use one frozen rolling 60-day window. Both months contribute to rule selection together. This research does not create holdouts, split months, compare periods, or claim out-of-sample validation. Forty days of earlier candles initialize indicators only; their outcomes do not enter selection.

## Causal trend and reversals

`major_trend.py` uses closed native H1/H4 candles with EMA20/EMA50, ATR14 and the last three-bar EMA20 slope. A timeframe is directional when normalized gap and slope agree and exceed 0.15 and 0.03; it is strong above 0.50 and 0.10. H4 receives twice the weight of H1. Agreement at full strength gives a strong regime, intermediate agreement/disagreement can give a weak bias, and near-zero combined bias is lateral. These trend thresholds are fixed before fitting entry filters.

Trend is recalculated at each decision. A BUY can be WITH, AGAINST or LATERAL at different times; SELL reverses those roles. A completed Friday H4 remains the last known context at reopening until another H4 closes. Runtime price-freshness checks remain authoritative.

## Adaptive sample floors

A reference budget of 54 resolved trades per pair is apportioned by eligible market-minute exposure across the entire window:

| Regime covering the full window | BUY reference | SELL reference |
| --- | ---: | ---: |
| Strong up | 36 | 18 |
| Weak up | 31.5 | 22.5 |
| Lateral | 27 | 27 |
| Weak down | 22.5 | 31.5 |
| Strong down | 18 | 36 |

Mixed regimes receive exposure-weighted allocations. Largest-remainder rounding preserves 54 outcomes total. Hard floors apply to each direction's WITH, AGAINST and LATERAL totals; strong and weak states of the same role share evidence. Per-regime numbers in the JSON are allocation components, not additional independent minimum-sample requirements.

For example, equal strong-up and strong-down exposure gives 27 BUY and 27 SELL outcomes, including 36 WITH and 18 AGAINST across the pair. Direction reversals follow the indicators, not calendar-month boundaries.

Only WIN and LOSS outcomes closed within the window count toward floors or win rates. BUY and SELL must each finish strictly above 50% win rate with positive expectancy. The search prefers the requested balance but cannot manufacture entries or guarantee future monthly trade frequency.

## Executable evidence and activation

Conditional technical filters are fitted by direction and relative-trend role. Candidate combinations are simulated chronologically across the whole pair: filters run before admitting a position, and a second position cannot start until the first closes. TP/SL exit-minute occupancy is conservative. Bid/ask spreads and adverse slippage of 0.10 pip on entry and exit are included. Existing runtime safety, timing, portfolio, confidence and broker gates remain in effect; mutable runtime gates are not reconstructed by the candle backtest.

`ACTIVE_TREND_60D_PAPER.json` contains the frozen evidence, data checksums and source commit. The loader recomputes evidence floors and verifies candidate checksums before accepting complete BUY/SELL pairs. All definitions are PAPER-only and have no LIVE authority.

The existing research workflow now runs every Monday at 02:17 UTC. Five workers use one shared window, one complete pair per worker; the selector rejects mixed-window artifacts. Automatic refresh occurs only when all ten qualify and main has not changed during research. A separate manual workflow can reselect saved pair artifacts without fetching another time period.

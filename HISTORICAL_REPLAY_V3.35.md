# Historical Replay v3.35

Historical replay is research-only and broker read-only. Decisions use only fully closed candles available at decision time. Future bars are accessible only to the outcome/execution resolver.

Execution uses historical OANDA bid/ask M1 candles. A market BUY fills from the ask open plus configured adverse entry slippage; a market SELL fills from the bid open minus configured adverse entry slippage. BUY exits are tested on bid and SELL exits on ask. Exit slippage is applied to the resulting fill once. Midpoint-only caches are rejected instead of approximated.

The report includes full-sample descriptive metrics, a chronological discovery/validation/test holdout, purging of events whose outcome interval crosses the next partition boundary, an embargo after each boundary, and fixed-policy walk-forward folds. These partitions are evaluation surfaces, not optimization loops.

The default runner freezes the current session scale at 1.0. Multiple session scales require an explicit research-sweep flag and make the run non-certification evidence.

Limitations: candle OHLC does not contain tick ordering or order-book depth. Same-bar TP/SL remains AMBIGUOUS, and partial fills are not fabricated.

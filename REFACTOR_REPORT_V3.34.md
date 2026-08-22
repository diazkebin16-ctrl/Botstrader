# Refactor Report — v3.34 Historical Candle Replay

## Objective
Build the missing counterfactual research layer: replay the strategy from historical candles without using future bars during signal generation, compare the v3.31 directional baseline against session-aware scoring, and resolve outcomes with the cost-aware evidence engine.

## Added
- `historical_candles.py`: GET-only OANDA historical downloader with pagination and reusable JSON cache.
- `historical_replay.py`: completed-bar candle views, v3.31 directional reconstruction, session-weight variants, deterministic replay gating, episode collapse, cost-aware outcome resolution, and performance summaries.
- `run_historical_replay.py`: CLI runner.
- `test_historical_replay.py`: explicit no-lookahead boundary tests.
- `HISTORICAL_REPLAY_V3.34.md`: methodology and scope documentation.

## Methodological boundaries
- Candle timestamp means bar start; no timeframe candle is exposed until its full duration has elapsed.
- Future M1 data is only passed to outcome resolution after the signal candle.
- The v3.31 comparison reconstructs its directional score/countertrend policy while sharing the current stop/target/structure implementation. It is therefore a directional-policy comparison, not a byte-identical emulator of the old binary.
- Learned confidence, research-rule state, strategy health, governance state, and re-entry state are intentionally excluded because candle history alone cannot reconstruct them causally.
- Repeated actionable minute snapshots are collapsed into independent directional episodes before performance statistics.

## Validation
- Python compilation passed for the new replay modules and `server.py`.
- Full test suite was run in two non-overlapping batches: 89 + 89 = 178 tests passed.
- Existing FastAPI `on_event` deprecation warnings remain; no new functional failure was observed.

## Production effect
No live execution path was changed. The only `server.py` runtime changes in this release are version/title strings. Historical replay is an offline research facility.

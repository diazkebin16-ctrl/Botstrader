# Botstrader v3.34.1 — Historical Candle Replay

This release adds an independent, read-only candle replay layer. It does not place orders and does not change the production execution path.

## Purpose

Replay H1/M15/M5/M1 chronologically, expose only fully closed candles to strategy logic, and reserve future M1 candles exclusively for outcome resolution. This makes it possible to compare the v3.31 directional baseline with the session-regime scoring introduced later.

## Research variants

- `V331_BASELINE`: reconstructs the v3.31 directional weights and its H1+M15 countertrend definition.
- `SESSION_*X`: reconstructs the current session-aware scoring while varying only the session contribution scale.

All variants share the same stop/target/structure calculations from the current strategy core. Therefore this is a directional-policy comparison, not a byte-for-byte historical execution emulator of every old release.

## No-lookahead rules

OANDA timestamps are candle start times. A candle is visible to the strategy only when `start + timeframe_duration <= decision_time`. The current M1 candle is evaluated after its close. Outcome resolution starts with the next M1 candle.

## Scope limitation

`REPLAY_ACTIONABLE` includes deterministic direction, safety, M1-confirmation and entry-extension gates. It intentionally excludes learned confidence, mutable research rules, strategy-health state, governance state and re-entry state because those cannot be reconstructed causally from candles alone.

## Example

```bash
python run_historical_replay.py \
  --instrument EUR_USD \
  --start 2026-07-01T00:00:00Z \
  --end 2026-08-21T20:00:00Z \
  --cache /data/eurusd_replay_candles.json \
  --output /data/historical_replay_v3.34.1.json
```

The OANDA downloader is GET-only and uses `OANDA_TOKEN`. Candle bundles can be cached and replayed repeatedly without another broker request.


## Cost-accounting invariant (v3.34.1)

The replay uses a shifted-barrier midpoint model. Round-trip cost moves the effective profit barrier farther from entry and the effective loss barrier closer to entry. Because that shift already embeds the cost in the required midpoint move, realized R must subtract `cost_r` exactly once from the gross move to the effective barrier. Under the current model this yields the nominal strategy payoff (`rr` for WIN, `-1R` for LOSS). Subtracting `cost_r` again from those nominal values would double-count transaction cost.

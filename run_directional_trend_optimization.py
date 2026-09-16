#!/usr/bin/env python3
"""Replay exactly 60 days, retaining all evidence dates together during fitting."""
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path


from directional_trend_optimizer import optimize_all
from directional_strategies import SUPPORTED_INSTRUMENTS
from historical_candles import fetch_oanda_candles, save_bundle
from historical_execution import HistoricalExecutionConfig
from historical_replay import ReplayConfig, ReplayVariant, replay_history


def frozen_window(now=None):
    end = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1)
    return end - timedelta(days=60), end


async def main():
    os.environ["AUTO_TRADE"] = "false"
    os.environ["TRADING_ENVIRONMENT"] = "SIMULATION"
    os.environ["PRIMARY_OANDA_ENV"] = "practice"
    os.environ["PRODUCTION_AUTHORIZED"] = "false"
    os.environ.setdefault("DB_PATH", "/tmp/directional_trend_research.db")
    import server
    root = Path(os.getenv("DIRECTIONAL_TREND_ROOT", "/tmp/directional_trend_60d"))
    root.mkdir(parents=True, exist_ok=True)
    start, end = frozen_window()
    rows, exposure, datasets = {}, {}, {}
    print(f"TREND60 window {start.isoformat()} {end.isoformat()}", flush=True)
    for index, inst in enumerate(SUPPORTED_INSTRUMENTS, 1):
        print(f"TREND60 pair_start {index}/5 {inst}", flush=True)
        tfs = ("H4", "H1", "M15", "M5", "M1")
        # Warmup is used only for indicators, never for strategy selection/outcomes.
        values = await asyncio.gather(*(fetch_oanda_candles(inst, tf, start - timedelta(days=40), end)
                                       for tf in tfs))
        bundle = dict(zip(tfs, values))
        cache = root / f"{inst}_candles.json"
        save_bundle(str(cache), bundle)
        datasets[inst] = {"sha256": hashlib.sha256(cache.read_bytes()).hexdigest(),
                          "candles": {tf: len(bundle[tf]) for tf in tfs}}
        replay = replay_history(server, bundle, inst, start, end, [ReplayVariant("TREND60")],
            ReplayConfig(horizon_bars=240, adaptive_major_trend=True, temporal_validation=False,
                         execution=HistoricalExecutionConfig(entry_slippage_pips=.10, exit_slippage_pips=.10,
                                                             latency_bars=0, require_bid_ask=True)))
        result = replay["variants"]["TREND60"]
        rows[inst] = result["episodes"]
        exposure[inst] = result["major_trend_exposure_minutes"]
        (root / f"{inst}_episodes.json").write_text(json.dumps(result, default=str))
        print(f"TREND60 pair_complete {inst} episodes={len(rows[inst])} exposure={exposure[inst]}", flush=True)
    report = optimize_all(rows, exposure, start, end)
    report["datasets"] = datasets
    report["execution_model"] = {"bid_ask_required": True, "entry_slippage_pips": .10,
        "exit_slippage_pips": .10, "closed_within_window_only": True, "overlapping_positions": False,
        "mutable_runtime_gates_reconstructed": False,
        "warmup_days": 40, "warmup_used_for_selection": False}
    path = root / "DIRECTIONAL_TREND_60D_EVIDENCE.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for result in report["results"]:
        print("TREND60 lane_result " + json.dumps(result, separators=(",", ":")), flush=True)
    print(f"TREND60 approved {report['approved_count']}/10", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

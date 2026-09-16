#!/usr/bin/env python3
"""Replay one shared 60-day window; pairs may run in independent workers."""
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
from major_trend import utc


def frozen_window(now=None):
    end = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1)
    return end - timedelta(days=60), end


def configured_window():
    if os.getenv("TREND60_WINDOW_END"):
        start, end = utc(os.environ["TREND60_WINDOW_START"]), utc(os.environ["TREND60_WINDOW_END"])
        if end - start != timedelta(days=60):
            raise ValueError("All workers must share exactly 60 days")
        return start, end
    return frozen_window()


async def replay_pair(inst, start, end, root):
    os.environ.update(AUTO_TRADE="false", TRADING_ENVIRONMENT="SIMULATION",
                      PRIMARY_OANDA_ENV="practice", PRODUCTION_AUTHORIZED="false")
    os.environ.setdefault("DB_PATH", "/tmp/directional_trend_research.db")
    import server
    print(f"TREND60 pair_start {inst}", flush=True)
    tfs = ("H4", "H1", "M15", "M5", "M1")
    # Warmup is for indicators only, never strategy selection or outcomes.
    values = await asyncio.gather(*(fetch_oanda_candles(inst, tf, start - timedelta(days=40), end) for tf in tfs))
    bundle = dict(zip(tfs, values))
    cache = root / f"{inst}_candles.json"
    save_bundle(str(cache), bundle)
    dataset = {"sha256": hashlib.sha256(cache.read_bytes()).hexdigest(),
               "candles": {tf: len(bundle[tf]) for tf in tfs}}
    result = replay_history(server, bundle, inst, start, end, [ReplayVariant("TREND60")],
        ReplayConfig(horizon_bars=240, adaptive_major_trend=True, temporal_validation=False,
                     execution=HistoricalExecutionConfig(entry_slippage_pips=.10, exit_slippage_pips=.10,
                                                         latency_bars=0, require_bid_ask=True)))
    payload = {"instrument": inst, "window": {"start": start.isoformat(), "end": end.isoformat()},
               "dataset": dataset, "replay": result}
    (root / f"{inst}_replay.json").write_text(json.dumps(payload, default=str))
    print(f"TREND60 pair_complete {inst} episodes={len(result['variants']['TREND60']['episodes'])}", flush=True)


def aggregate(root, start, end):
    rows, exposure, datasets, replay_hashes = {}, {}, {}, {}
    for inst in SUPPORTED_INSTRUMENTS:
        path = root / f"{inst}_replay.json"
        payload = json.loads(path.read_text())
        if payload["instrument"] != inst or payload["window"] != {"start": start.isoformat(), "end": end.isoformat()}:
            raise ValueError("Pair workers used different windows")
        replay = payload["replay"]["variants"]["TREND60"]
        if replay["holdout"] is not None or replay["walk_forward"]:
            raise ValueError("Unexpected temporal split in pair replay")
        rows[inst] = replay["episodes"]
        exposure[inst] = replay["major_trend_exposure_minutes"]
        datasets[inst] = payload["dataset"]
        replay_hashes[inst] = hashlib.sha256(path.read_bytes()).hexdigest()
    report = optimize_all(rows, exposure, start, end)
    report["datasets"] = datasets
    report["replay_sha256"] = replay_hashes
    report["source_commit"] = os.getenv("GITHUB_SHA")
    report["execution_model"] = {"bid_ask_required": True, "entry_slippage_pips": .10,
        "exit_slippage_pips": .10, "closed_within_window_only": True, "overlapping_positions": False,
        "mutable_runtime_gates_reconstructed": False, "warmup_days": 40, "warmup_used_for_selection": False}
    path = root / "DIRECTIONAL_TREND_60D_EVIDENCE.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for result in report["results"]:
        print("TREND60 lane_result " + json.dumps(result, separators=(",", ":")), flush=True)
    print(f"TREND60 approved {report['approved_count']}/10", flush=True)
    return report


async def main():
    root = Path(os.getenv("DIRECTIONAL_TREND_ROOT", "/tmp/directional_trend_60d"))
    root.mkdir(parents=True, exist_ok=True)
    start, end = configured_window()
    print(f"TREND60 window {start.isoformat()} {end.isoformat()}", flush=True)
    instrument = os.getenv("TREND60_INSTRUMENT")
    if os.getenv("TREND60_AGGREGATE_ONLY") != "true":
        if instrument and instrument not in SUPPORTED_INSTRUMENTS:
            raise ValueError("Unsupported pair")
        for inst in (instrument,) if instrument else SUPPORTED_INSTRUMENTS:
            await replay_pair(inst, start, end, root)
    if not instrument:
        aggregate(root, start, end)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

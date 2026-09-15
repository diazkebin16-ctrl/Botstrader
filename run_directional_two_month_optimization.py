#!/usr/bin/env python3
"""Download two frozen months and optimize all ten directional PAPER lanes."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

# This worker is research-only even when it runs beside the PAPER service.
os.environ["AUTO_TRADE"] = "false"
os.environ["TRADING_ENVIRONMENT"] = "SIMULATION"
os.environ["PRIMARY_OANDA_ENV"] = "practice"
os.environ.setdefault("DB_PATH", "/tmp/directional_optimization.db")

from directional_strategies import SUPPORTED_INSTRUMENTS, SUPPORTED_DIRECTIONS, strategy_definition
from directional_two_month_optimizer import OptimizationPolicy, optimize_lane
from historical_candles import fetch_bundle, save_bundle
from historical_execution import HistoricalExecutionConfig
from historical_replay import ReplayConfig, ReplayVariant, replay_history
from replay_validation import ReplayValidationConfig


START = datetime(2026, 7, 15, tzinfo=timezone.utc)
END = datetime(2026, 9, 14, 23, 59, 59, tzinfo=timezone.utc)
OUTPUT_ROOT = Path(os.getenv("DIRECTIONAL_OPTIMIZATION_ROOT", "/tmp/two_month_directional_optimization"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _coverage(bundle: dict[str, list[dict]]) -> dict[str, dict[str, object]]:
    required = {"o", "h", "l", "c", "bid_o", "bid_h", "bid_l", "bid_c", "ask_o", "ask_h", "ask_l", "ask_c"}
    report: dict[str, dict[str, object]] = {}
    for timeframe in ("H1", "M15", "M5", "M1"):
        candles = bundle.get(timeframe) or []
        timestamps = [str(candle.get("t") or "") for candle in candles]
        complete = bool(candles) and all(required.issubset(candle) for candle in candles)
        report[timeframe] = {
            "count": len(candles),
            "first": timestamps[0] if timestamps else None,
            "last": timestamps[-1] if timestamps else None,
            "bid_ask_complete": complete,
            "ordered": timestamps == sorted(timestamps),
            "duplicates": len(timestamps) - len(set(timestamps)),
        }
        if not complete or report[timeframe]["ordered"] is not True or report[timeframe]["duplicates"] != 0:
            raise RuntimeError(f"historical integrity failed for {timeframe}")
    return report


async def main() -> int:
    import server

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    final = {
        "schema_version": 1,
        "status": "RUNNING",
        "window": {"start": START.isoformat(), "end": END.isoformat()},
        "split": {"discovery": 0.50, "validation": 0.25, "holdout": 0.25},
        "source": "OANDA_PRACTICE_MID_BID_ASK",
        "methodology": {
            "future_bars_only_for_outcomes": True,
            "spread_and_slippage": True,
            "global_entry_windows_unchanged": True,
            "maximum_candidate_rules": 2,
            "candidate_filters_cannot_increase_frequency": True,
            "production_authority": False,
        },
        "instruments": {},
        "lanes": [],
    }
    policy = OptimizationPolicy()
    for instrument in SUPPORTED_INSTRUMENTS:
        cache = OUTPUT_ROOT / f"{instrument}_20260715_20260914_mba.json"
        print(f"RESEARCH_PROGRESS download_start {instrument}", flush=True)
        bundle = await fetch_bundle(instrument, START, END, warmup_days=10, horizon_minutes=240)
        save_bundle(str(cache), bundle)
        coverage = _coverage(bundle)
        final["instruments"][instrument] = {
            "cache_sha256": _sha256(cache),
            "cache_bytes": cache.stat().st_size,
            "coverage": coverage,
        }
        print(f"RESEARCH_DATA {instrument} {json.dumps(final['instruments'][instrument], separators=(',', ':'))}", flush=True)
        replay = replay_history(
            server,
            bundle,
            instrument,
            START,
            END,
            [ReplayVariant("SESSION_1X", "SESSION", 1.0)],
            ReplayConfig(
                horizon_bars=240,
                save_m1_rejection_shadow=False,
                save_target_population=False,
                execution=HistoricalExecutionConfig(
                    entry_slippage_pips=0.10,
                    exit_slippage_pips=0.10,
                    latency_bars=0,
                    require_bid_ask=True,
                ),
                validation=ReplayValidationConfig(embargo_minutes=30),
            ),
        )
        episodes = replay["variants"]["SESSION_1X"]["episodes"]
        print(f"RESEARCH_PROGRESS replay_complete {instrument} episodes={len(episodes)}", flush=True)
        for direction in SUPPORTED_DIRECTIONS:
            definition = strategy_definition(instrument, direction)
            lane = optimize_lane(
                episodes,
                instrument=instrument,
                direction=direction,
                start=START,
                end=END,
                current_rules=definition["filters"],
                policy=policy,
            )
            lane["strategy_id"] = definition["strategy_id"]
            final["lanes"].append(lane)
            print(f"RESEARCH_LANE {definition['strategy_id']} {json.dumps(lane, separators=(',', ':'))}", flush=True)
        del replay, bundle
    final["status"] = "PASS"
    report_path = OUTPUT_ROOT / "DIRECTIONAL_TWO_MONTH_OPTIMIZATION.json"
    report_path.write_text(json.dumps(final, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    final["report_sha256"] = _sha256(report_path)
    print(f"RESEARCH_FINAL {json.dumps(final, separators=(',', ':'))}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

#!/usr/bin/env python3
"""Download and optimize all ten directional PAPER lanes over two months."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

os.environ["AUTO_TRADE"] = "false"
os.environ["TRADING_ENVIRONMENT"] = "SIMULATION"
os.environ["PRIMARY_OANDA_ENV"] = "practice"
os.environ.setdefault("DB_PATH", "/tmp/directional_month_optimization.db")

from directional_month_optimizer import optimize_all_lanes
from historical_candles import fetch_bundle, save_bundle
from historical_execution import HistoricalExecutionConfig
from historical_replay import ReplayConfig, ReplayVariant, replay_history
from replay_validation import ReplayValidationConfig


START = datetime(2026, 7, 15, tzinfo=timezone.utc)
SECOND_MONTH_START = datetime(2026, 8, 15, tzinfo=timezone.utc)
END = datetime(2026, 9, 14, 23, 59, 59, tzinfo=timezone.utc)
INSTRUMENTS = ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD")
OUTPUT_ROOT = Path(os.getenv("DIRECTIONAL_MONTH_ROOT", "/tmp/directional_month_optimization"))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def main() -> int:
    import server

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    rows_by_instrument = {}
    datasets = {}
    for index, instrument in enumerate(INSTRUMENTS, 1):
        print(f"DIRECTIONAL_MONTH pair_start {index}/5 {instrument}", flush=True)
        bundle = await fetch_bundle(instrument, START, END, warmup_days=10, horizon_minutes=240)
        cache = OUTPUT_ROOT / f"{instrument}_20260715_20260914_mba.json"
        save_bundle(str(cache), bundle)
        identity = {
            "sha256": sha256(cache),
            "bytes": cache.stat().st_size,
            "m1_candles": len(bundle.get("M1") or []),
            "bid_ask_required": True,
        }
        datasets[instrument] = identity
        print(f"DIRECTIONAL_MONTH data_identity {instrument} {json.dumps(identity, separators=(',', ':'))}", flush=True)
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
        rows_by_instrument[instrument] = episodes
        direction_counts = {
            direction: sum(1 for row in episodes if str(row.get("signal") or "").upper() == direction)
            for direction in ("BUY", "SELL")
        }
        print(
            f"DIRECTIONAL_MONTH pair_complete {instrument} episodes={len(episodes)} directions={json.dumps(direction_counts, separators=(',', ':'))}",
            flush=True,
        )

    report = optimize_all_lanes(
        rows_by_instrument, start=START, second_month_start=SECOND_MONTH_START, end=END,
    )
    report["schema_version"] = 1
    report["datasets"] = datasets
    report["execution_model"] = {
        "source": "OANDA Practice",
        "bid_ask_required": True,
        "entry_slippage_pips": 0.10,
        "exit_slippage_pips": 0.10,
        "horizon_minutes": 240,
        "look_ahead": False,
    }
    output = OUTPUT_ROOT / "DIRECTIONAL_MONTH_OPTIMIZATION.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for item in report["results"]:
        compact = {
            "strategy_id": item["strategy_id"],
            "verdict": item["verdict"],
            "baseline": item["baseline"],
            "frozen_candidate": item["frozen_candidate"],
            "second_month": item.get("second_month"),
            "total": item.get("total"),
            "final_gates": item.get("final_gates"),
        }
        print(f"DIRECTIONAL_MONTH lane_result {json.dumps(compact, separators=(',', ':'))}", flush=True)
    print(f"DIRECTIONAL_MONTH final {json.dumps(report, separators=(',', ':'))}", flush=True)
    print(f"DIRECTIONAL_MONTH report_sha256 {sha256(output)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

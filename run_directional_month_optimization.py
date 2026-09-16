#!/usr/bin/env python3
"""Backtest all ten directional PAPER lanes over one rolling 30-day window."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path

os.environ["AUTO_TRADE"] = "false"
os.environ["TRADING_ENVIRONMENT"] = "SIMULATION"
os.environ["PRIMARY_OANDA_ENV"] = "practice"
os.environ["PRODUCTION_AUTHORIZED"] = "false"
os.environ.setdefault("DB_PATH", "/tmp/directional_month_optimization.db")

from directional_month_optimizer import optimize_all_lanes
from historical_candles import fetch_bundle, save_bundle
from historical_execution import HistoricalExecutionConfig
from historical_replay import ReplayConfig, ReplayVariant, replay_history
from replay_validation import ReplayValidationConfig


def frozen_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    end = current.replace(second=0, microsecond=0) - timedelta(minutes=1)
    return end - timedelta(days=30), end


START, END = frozen_window()
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
    suffix = f"{START:%Y%m%d}_{END:%Y%m%d}"
    for index, instrument in enumerate(INSTRUMENTS, 1):
        print(f"DIRECTIONAL_MONTH pair_start {index}/5 {instrument}", flush=True)
        bundle = await fetch_bundle(instrument, START, END, warmup_days=10, horizon_minutes=240)
        cache = OUTPUT_ROOT / f"{instrument}_{suffix}_30d.json"
        save_bundle(str(cache), bundle)
        datasets[instrument] = {
            "sha256": sha256(cache), "bytes": cache.stat().st_size,
            "m1_candles": len(bundle.get("M1") or []), "bid_ask_required": True,
        }
        replay = replay_history(
            server, bundle, instrument, START, END,
            [ReplayVariant("SESSION_1X", "SESSION", 1.0)],
            ReplayConfig(
                horizon_bars=240, save_m1_rejection_shadow=False, save_target_population=False,
                execution=HistoricalExecutionConfig(entry_slippage_pips=0.10, exit_slippage_pips=0.10,
                                                    latency_bars=0, require_bid_ask=True),
                validation=ReplayValidationConfig(embargo_minutes=0),
            ),
        )
        episodes = replay["variants"]["SESSION_1X"]["episodes"]
        rows_by_instrument[instrument] = episodes
        print(f"DIRECTIONAL_MONTH pair_complete {instrument} episodes={len(episodes)}", flush=True)

    report = optimize_all_lanes(rows_by_instrument, start=START, end=END)
    report["schema_version"] = 2
    report["datasets"] = datasets
    report["execution_model"] = {
        "source": "OANDA Practice", "bid_ask_required": True,
        "entry_slippage_pips": 0.10, "exit_slippage_pips": 0.10,
        "horizon_minutes": 240, "look_ahead": False,
    }
    output = OUTPUT_ROOT / "DIRECTIONAL_ONE_MONTH_MIN10_EVIDENCE.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for item in report["results"]:
        compact = {"strategy_id": item["strategy_id"], "verdict": item["verdict"],
                   "baseline": item["baseline"], "frozen_candidate": item["frozen_candidate"],
                   "final_gates": item.get("final_gates")}
        print(f"DIRECTIONAL_MONTH lane_result {json.dumps(compact, separators=(',', ':'))}", flush=True)
    print(f"DIRECTIONAL_MONTH final {json.dumps(report, separators=(',', ':'))}", flush=True)
    print(f"DIRECTIONAL_MONTH report_sha256 {sha256(output)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

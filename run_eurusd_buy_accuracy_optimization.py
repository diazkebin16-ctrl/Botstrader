#!/usr/bin/env python3
"""Run the one-shot >55% EUR/USD BUY research attempt."""
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
os.environ.setdefault("DB_PATH", "/tmp/eurusd_buy_accuracy_optimization.db")

from eurusd_buy_accuracy_optimizer import optimize_accuracy
from historical_candles import fetch_bundle, save_bundle
from historical_execution import HistoricalExecutionConfig
from historical_replay import ReplayConfig, ReplayVariant, replay_history
from replay_validation import ReplayValidationConfig


START = datetime(2026, 7, 15, tzinfo=timezone.utc)
END = datetime(2026, 9, 14, 23, 59, 59, tzinfo=timezone.utc)
EXPECTED_CACHE_SHA256 = "28079d1bfd3b27385f022bd693967c58768ad4096534277e23b5b63ca0205eff"
OUTPUT_ROOT = Path(os.getenv("EURUSD_BUY_ACCURACY_ROOT", "/tmp/eurusd_buy_accuracy_optimization"))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def main() -> int:
    import server

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    cache = OUTPUT_ROOT / "EUR_USD_20260715_20260914_mba.json"
    print("EURUSD_BUY_ACCURACY download_start", flush=True)
    bundle = await fetch_bundle("EUR_USD", START, END, warmup_days=10, horizon_minutes=240)
    save_bundle(str(cache), bundle)
    cache_sha = sha256(cache)
    identity = {
        "sha256": cache_sha,
        "expected_sha256": EXPECTED_CACHE_SHA256,
        "bytes": cache.stat().st_size,
        "m1_candles": len(bundle.get("M1") or []),
        "sha_match": cache_sha == EXPECTED_CACHE_SHA256,
    }
    print(f"EURUSD_BUY_ACCURACY data_identity {json.dumps(identity, separators=(',', ':'))}", flush=True)
    if cache_sha != EXPECTED_CACHE_SHA256:
        raise RuntimeError("FROZEN_DATASET_SHA256_MISMATCH")

    replay = replay_history(
        server,
        bundle,
        "EUR_USD",
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
    report = optimize_accuracy(episodes, start=START, end=END)
    report["schema_version"] = 1
    report["dataset"] = identity
    report["execution_model"] = {
        "bid_ask_required": True,
        "entry_slippage_pips": 0.10,
        "exit_slippage_pips": 0.10,
        "horizon_minutes": 240,
        "embargo_minutes": 30,
        "look_ahead": False,
    }
    output = OUTPUT_ROOT / "EURUSD_BUY_ACCURACY_OPTIMIZATION.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"EURUSD_BUY_ACCURACY final {json.dumps(report, separators=(',', ':'))}", flush=True)
    print(f"EURUSD_BUY_ACCURACY report_sha256 {sha256(output)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

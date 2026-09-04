"""Deterministic bid/ask execution model for historical M1 replay.

Research-only. This module never places orders and never changes live execution.
It consumes historical bid/ask candles and models a market entry at the first
executable M1 open after the signal candle. Spread is observed from the candle;
slippage is an explicit adverse deterministic assumption.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Sequence

from research_evidence import pip_size
from operational_time import fixed_entry_gate, operational_close_after


def _dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        out = value
    else:
        out = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if out.tzinfo is None:
        out = out.replace(tzinfo=timezone.utc)
    return out.astimezone(timezone.utc)


@dataclass(frozen=True)
class HistoricalExecutionConfig:
    """Frozen execution assumptions used by historical replay."""

    entry_slippage_pips: float = 0.10
    exit_slippage_pips: float = 0.10
    latency_bars: int = 0
    require_bid_ask: bool = True


def has_bid_ask(candle: Mapping[str, Any]) -> bool:
    return all(k in candle for k in ("bid_o", "bid_h", "bid_l", "bid_c", "ask_o", "ask_h", "ask_l", "ask_c"))


def _invalid(note: str, status: str = "INVALID") -> Dict[str, Any]:
    return {
        "status": status,
        "label": None,
        "bars": 0,
        "mfe_r": 0.0,
        "mae_r": 0.0,
        "realized_r": None,
        "note": note,
    }


def resolve_executed_outcome(
    sample: Mapping[str, Any],
    future_m1: Sequence[Mapping[str, Any]],
    *,
    horizon_bars: int,
    config: HistoricalExecutionConfig = HistoricalExecutionConfig(),
) -> Optional[Dict[str, Any]]:
    """Resolve a trade using executable bid/ask prices rather than midpoint fills.

    Entry is a market fill at the first eligible M1 open after the signal. BUYs
    cross the ask and SELLs cross the bid. Exit touches use the executable side:
    BUY exits on bid, SELL exits on ask. Explicit adverse slippage is applied to
    entry and exit fill prices, but never double-counted as a second R deduction.
    """
    direction = str(sample.get("direction") or "").upper()
    if direction not in ("BUY", "SELL"):
        return _invalid("invalid direction")

    try:
        planned_entry = float(sample["entry"])
        stop = float(sample["stop"])
        target = float(sample["target"])
    except (KeyError, TypeError, ValueError):
        return _invalid("invalid entry/stop/target")

    planned_risk = abs(planned_entry - stop)
    if planned_risk <= 0:
        return _invalid("zero planned risk")
    if direction == "BUY" and not (stop < planned_entry < target):
        return _invalid("invalid BUY price geometry")
    if direction == "SELL" and not (target < planned_entry < stop):
        return _invalid("invalid SELL price geometry")

    latency = max(0, int(config.latency_bars))
    if len(future_m1) <= latency:
        return None
    bars = list(future_m1[latency:])
    if config.require_bid_ask and not has_bid_ask(bars[0]):
        return _invalid("historical bid/ask candles required", "DATA_INSUFFICIENT")

    pip = pip_size(str(sample.get("instrument") or ""))
    entry_slip = max(0.0, float(config.entry_slippage_pips)) * pip
    exit_slip = max(0.0, float(config.exit_slippage_pips)) * pip
    first = bars[0]
    if not has_bid_ask(first):
        return _invalid("historical bid/ask candles required", "DATA_INSUFFICIENT")

    bid_o = float(first["bid_o"])
    ask_o = float(first["ask_o"])
    if ask_o < bid_o:
        return _invalid("negative spread in historical candle", "DATA_INTEGRITY_ERROR")

    entry_ts = _dt(first["t"])
    operational_gate = fixed_entry_gate(entry_ts)
    if operational_gate["allowed"] is not True:
        out = _invalid(f"new entry blocked by fixed operational schedule: {operational_gate['reason']}", "ENTRY_BLOCKED_OPERATIONAL_TIME")
        out.update({"entry_ts": entry_ts.isoformat(), "operational_entry_gate": operational_gate})
        return out
    operational_close = operational_close_after(entry_ts)

    if direction == "BUY":
        entry_fill = ask_o + entry_slip
        if not (stop < entry_fill < target):
            out = _invalid("market moved beyond valid BUY entry geometry before fill", "ENTRY_INVALIDATED")
            out.update({"entry_ts": _dt(first["t"]).isoformat(), "entry_fill": entry_fill, "planned_entry": planned_entry})
            return out
    else:
        entry_fill = bid_o - entry_slip
        if not (target < entry_fill < stop):
            out = _invalid("market moved beyond valid SELL entry geometry before fill", "ENTRY_INVALIDATED")
            out.update({"entry_ts": _dt(first["t"]).isoformat(), "entry_fill": entry_fill, "planned_entry": planned_entry})
            return out

    entry_spread_pips = (ask_o - bid_o) / pip if pip > 0 else None
    mfe = 0.0
    mae = 0.0
    data_coverage_horizon_bars = max(1, int(horizon_bars))

    for idx, bar in enumerate(bars, start=1):
        bar_ts = _dt(bar["t"])
        if bar_ts >= operational_close:
            return {
                "status": "TIMEOUT", "label": None, "bars": idx - 1,
                "entry_ts": entry_ts.isoformat(), "exit_ts": operational_close.isoformat(),
                "entry_fill": entry_fill, "planned_entry": planned_entry,
                "entry_spread_pips": entry_spread_pips,
                "entry_slippage_pips": float(config.entry_slippage_pips),
                "exit_slippage_pips": float(config.exit_slippage_pips),
                "mfe_r": mfe, "mae_r": mae, "realized_r": None,
                "data_coverage_horizon_bars": data_coverage_horizon_bars,
                "operational_close": operational_close.isoformat(),
                "note": "Neither TP nor SL touched before 16:50 America/New_York operational close",
            }
        if config.require_bid_ask and not has_bid_ask(bar):
            return _invalid("bid/ask series became incomplete inside outcome horizon", "DATA_INSUFFICIENT")
        if not has_bid_ask(bar):
            return _invalid("historical bid/ask candles required", "DATA_INSUFFICIENT")

        if direction == "BUY":
            high = float(bar["bid_h"])
            low = float(bar["bid_l"])
            mfe = max(mfe, (high - entry_fill) / planned_risk)
            mae = min(mae, (low - entry_fill) / planned_risk)
            hit_tp = high >= target
            hit_sl = low <= stop
        else:
            low = float(bar["ask_l"])
            high = float(bar["ask_h"])
            mfe = max(mfe, (entry_fill - low) / planned_risk)
            mae = min(mae, (entry_fill - high) / planned_risk)
            hit_tp = low <= target
            hit_sl = high >= stop

        common = {
            "bars": idx,
            "entry_ts": entry_ts.isoformat(),
            "exit_ts": bar_ts.isoformat(),
            "entry_fill": entry_fill,
            "planned_entry": planned_entry,
            "entry_spread_pips": entry_spread_pips,
            "entry_slippage_pips": float(config.entry_slippage_pips),
            "exit_slippage_pips": float(config.exit_slippage_pips),
            "mfe_r": mfe,
            "mae_r": mae,
            "data_coverage_horizon_bars": data_coverage_horizon_bars,
            "operational_close": operational_close.isoformat(),
        }
        if hit_tp and hit_sl:
            return {"status": "AMBIGUOUS", "label": None, "realized_r": None,
                    "note": "TP and SL touched inside the same executable-side M1 candle", **common}
        if hit_tp:
            exit_fill = target - exit_slip if direction == "BUY" else target + exit_slip
            realized = ((exit_fill - entry_fill) if direction == "BUY" else (entry_fill - exit_fill)) / planned_risk
            return {"status": "WIN", "label": 1, "exit_fill": exit_fill, "realized_r": realized,
                    "note": None, **common}
        if hit_sl:
            exit_fill = stop - exit_slip if direction == "BUY" else stop + exit_slip
            realized = ((exit_fill - entry_fill) if direction == "BUY" else (entry_fill - exit_fill)) / planned_risk
            return {"status": "LOSS", "label": 0, "exit_fill": exit_fill, "realized_r": realized,
                    "note": None, **common}

    # Exhausting the caller's available candles before operational close is
    # not a trade TIMEOUT.  It is unresolved evidence and must remain PENDING so
    # data coverage can be repaired independently of trade lifetime semantics.
    return None

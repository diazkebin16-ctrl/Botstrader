"""Independent, episode-aware research validation utilities (v3.33).

This module is deliberately broker-free and has no authority to place orders or
mutate production strategy.  It validates *pre-trade policies* against realized
outcomes using strict chronological walk-forward splits.

Important scope rule:
- trade_memory validation can test whether a pre-trade filter/score improves the
  already-executed trade population;
- it cannot prove the performance of missed/counterfactual entries.  A candle
  replay is required for that stronger claim.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
import json
import math
import random


Row = Dict[str, Any]
Policy = Callable[[Row, Any], bool]


def _dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _finite(v: Any) -> Optional[float]:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def _json(v: Any, fallback: Any) -> Any:
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v or "")
    except Exception:
        return fallback


def normalize_trade_memory_row(row: Row) -> Optional[Row]:
    """Return a research-safe closed-trade row using only frozen entry context.

    The policy-visible namespace is stored in ``pretrade``.  Post-trade fields
    such as realized_r/MFE/MAE remain outside it so policy callbacks cannot
    accidentally use future information.
    """
    entry_ts = _dt(row.get("entry_ts"))
    realized_r = _finite(row.get("realized_r"))
    if entry_ts is None or realized_r is None:
        return None
    ctx = _json(row.get("entry_context_json"), {})
    if not isinstance(ctx, dict):
        ctx = {}
    direction = str(row.get("direction") or "").upper()
    if direction == "LONG":
        direction = "BUY"
    elif direction == "SHORT":
        direction = "SELL"
    return {
        "trade_id": str(row.get("trade_id") or row.get("id") or ""),
        "instrument": str(row.get("symbol") or row.get("instrument") or "UNKNOWN"),
        "direction": direction,
        "entry_ts": entry_ts.isoformat(),
        "exit_ts": row.get("exit_ts"),
        "realized_r": realized_r,
        "net_result": _finite(row.get("net_result")),
        "mfe_r": _finite(row.get("mfe_r")),
        "mae_r": _finite(row.get("mae_r")),
        "entry_session": row.get("entry_session"),
        "market_regime_entry": row.get("market_regime_entry"),
        "pretrade": {
            "strategy": row.get("strategy"),
            "strategy_confidence_entry": _finite(row.get("strategy_confidence_entry")),
            "director_confidence_entry": _finite(row.get("director_confidence_entry")),
            "risk_multiplier_entry": _finite(row.get("risk_multiplier_entry")),
            "entry_session": row.get("entry_session"),
            "market_regime_entry": row.get("market_regime_entry"),
            "volatility_state_entry": row.get("volatility_state_entry"),
            "trend_strength_entry": _finite(row.get("trend_strength_entry")),
            "context": ctx,
            "features": ctx.get("features") if isinstance(ctx.get("features"), dict) else {},
            "filters": ctx.get("filters") if isinstance(ctx.get("filters"), dict) else {},
        },
    }


def normalize_trade_memory(rows: Iterable[Row]) -> List[Row]:
    out = []
    for row in rows:
        x = normalize_trade_memory_row(dict(row))
        if x is not None:
            out.append(x)
    return sorted(out, key=lambda r: (_dt(r["entry_ts"]), r.get("trade_id", "")))


def annotate_trade_episodes(rows: Sequence[Row], gap_minutes: int = 15) -> List[Row]:
    """Assign episode IDs by instrument+direction and consecutive time gap."""
    ordered = sorted((dict(r) for r in rows), key=lambda r: (_dt(r.get("entry_ts")) or datetime.min.replace(tzinfo=timezone.utc), str(r.get("trade_id", ""))))
    last: Dict[Tuple[str, str], datetime] = {}
    serial: Dict[Tuple[str, str], int] = {}
    out: List[Row] = []
    for row in ordered:
        ts = _dt(row.get("entry_ts"))
        if ts is None:
            continue
        key = (str(row.get("instrument") or "UNKNOWN"), str(row.get("direction") or "UNKNOWN"))
        prev = last.get(key)
        if prev is None or (ts - prev).total_seconds() > max(0, int(gap_minutes)) * 60:
            serial[key] = serial.get(key, 0) + 1
        last[key] = ts
        row["episode_id"] = f"{key[0]}|{key[1]}|{serial[key]:06d}"
        out.append(row)
    return out


def collapse_trade_episodes(rows: Sequence[Row], gap_minutes: int = 15) -> List[Row]:
    """One realized trade per correlated episode, preserving earliest entry.

    This is intentionally conservative. Multiple live fills inside one 15-minute
    directional episode are not treated as independent statistical evidence.
    """
    tagged = annotate_trade_episodes(rows, gap_minutes)
    seen = set()
    out = []
    for row in tagged:
        eid = row["episode_id"]
        if eid in seen:
            continue
        seen.add(eid)
        out.append(row)
    return out


def performance_metrics(rows: Sequence[Row]) -> Dict[str, Any]:
    vals = [float(r["realized_r"]) for r in rows if _finite(r.get("realized_r")) is not None]
    n = len(vals)
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else (999.0 if gp > 0 else None)
    curve = peak = dd = 0.0
    for x in vals:
        curve += x
        peak = max(peak, curve)
        dd = max(dd, peak - curve)
    mean = sum(vals) / n if n else None
    if n > 1 and mean is not None:
        var = sum((x - mean) ** 2 for x in vals) / (n - 1)
        sd = math.sqrt(var)
        se = sd / math.sqrt(n)
        ci = (mean - 1.96 * se, mean + 1.96 * se)
    else:
        ci = (None, None)
    return {
        "episodes": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / n if n else None,
        "expectancy_r": mean,
        "profit_factor": pf,
        "max_drawdown_r": dd,
        "net_r": sum(vals),
        "expectancy_95ci_low": ci[0],
        "expectancy_95ci_high": ci[1],
    }


def bootstrap_expectancy_ci(rows: Sequence[Row], simulations: int = 1000, seed: int = 17) -> Dict[str, Any]:
    vals = [float(r["realized_r"]) for r in rows if _finite(r.get("realized_r")) is not None]
    if len(vals) < 2:
        return {"samples": len(vals), "low": None, "median": vals[0] if vals else None, "high": None}
    rng = random.Random(seed)
    means = []
    for _ in range(max(100, int(simulations))):
        sample = [vals[rng.randrange(len(vals))] for _ in vals]
        means.append(sum(sample) / len(sample))
    means.sort()
    def q(p: float) -> float:
        return means[min(len(means)-1, max(0, int(round((len(means)-1)*p))))]
    return {"samples": len(vals), "low": q(0.025), "median": q(0.5), "high": q(0.975)}


@dataclass(frozen=True)
class WalkForwardConfig:
    train_episodes: int = 60
    test_episodes: int = 20
    step_episodes: int = 20
    min_train_selected: int = 20
    min_test_selected: int = 5


def _policy_rows(rows: Sequence[Row], policy: Policy, parameter: Any) -> List[Row]:
    selected = []
    for row in rows:
        # Callback receives only the pre-trade view plus identifiers/time.
        safe = {
            "trade_id": row.get("trade_id"),
            "instrument": row.get("instrument"),
            "direction": row.get("direction"),
            "entry_ts": row.get("entry_ts"),
            "episode_id": row.get("episode_id"),
            "pretrade": row.get("pretrade") or {},
        }
        if bool(policy(safe, parameter)):
            selected.append(row)
    return selected


def walk_forward_optimize(
    rows: Sequence[Row],
    parameters: Sequence[Any],
    policy: Policy,
    config: WalkForwardConfig = WalkForwardConfig(),
) -> Dict[str, Any]:
    """Optimize on each train window, then freeze parameter for the next test.

    There is no random split. A unique episode belongs to one side of a fold.
    Test outcomes never participate in parameter selection.
    """
    episodes = collapse_trade_episodes(rows)
    episodes = sorted(episodes, key=lambda r: (_dt(r.get("entry_ts")), r.get("episode_id", "")))
    windows = []
    start = max(1, int(config.train_episodes))
    step = max(1, int(config.step_episodes))
    test_n = max(1, int(config.test_episodes))
    while start + test_n <= len(episodes):
        train = episodes[max(0, start-config.train_episodes):start]
        test = episodes[start:start+test_n]
        candidates = []
        for parameter in parameters:
            selected = _policy_rows(train, policy, parameter)
            m = performance_metrics(selected)
            if m["episodes"] < config.min_train_selected:
                continue
            candidates.append((parameter, m))
        if not candidates:
            windows.append({"window": len(windows)+1, "status":"INSUFFICIENT_TRAIN_SELECTION",
                            "train_episodes":len(train), "test_episodes":len(test)})
            start += step
            continue
        # Primary objective expectancy; PF and sample count are deterministic tie breakers.
        def rank(item):
            _, m = item
            return (m.get("expectancy_r") if m.get("expectancy_r") is not None else -999.0,
                    m.get("profit_factor") if m.get("profit_factor") is not None else -999.0,
                    m.get("episodes") or 0)
        parameter, train_metrics = max(candidates, key=rank)
        selected_test = _policy_rows(test, policy, parameter)
        test_metrics = performance_metrics(selected_test)
        windows.append({
            "window": len(windows)+1,
            "status": "OK" if test_metrics["episodes"] >= config.min_test_selected else "INSUFFICIENT_TEST_SELECTION",
            "chosen_parameter": parameter,
            "train_period": {"start":train[0]["entry_ts"] if train else None,"end":train[-1]["entry_ts"] if train else None},
            "test_period": {"start":test[0]["entry_ts"] if test else None,"end":test[-1]["entry_ts"] if test else None},
            "train_episode_ids":[r["episode_id"] for r in train],
            "test_episode_ids":[r["episode_id"] for r in test],
            "train_metrics": train_metrics,
            "test_metrics": test_metrics,
        })
        start += step
    valid = [w for w in windows if w.get("status") == "OK"]
    pooled_test_ids = []
    seen = set()
    for w in valid:
        for eid in w["test_episode_ids"]:
            if eid not in seen:
                seen.add(eid); pooled_test_ids.append(eid)
    lookup = {r["episode_id"]:r for r in episodes}
    pooled = [lookup[eid] for eid in pooled_test_ids if eid in lookup]
    return {
        "status":"OK" if valid else "INSUFFICIENT_DATA",
        "scope":"EXECUTED_TRADE_MEMORY_ONLY",
        "causal_claim":"FILTER_ASSOCIATION_ONLY_NOT_COUNTERFACTUAL_ENTRY_EDGE",
        "raw_rows":len(rows),
        "independent_episodes":len(episodes),
        "windows":windows,
        "valid_windows":len(valid),
        "pooled_oos":performance_metrics(pooled),
        "pooled_oos_bootstrap":bootstrap_expectancy_ci(pooled),
    }


def session_opposition_policy(row: Row, min_strength: float) -> bool:
    """Research-only filter: reject strong session opposition to trade direction."""
    pre = row.get("pretrade") or {}
    f = pre.get("features") or {}
    direction = str(row.get("direction") or "")
    session_direction = str(f.get("session_direction") or "NEUTRAL")
    strength = _finite(f.get("session_strength")) or 0.0
    opposed = session_direction in ("BUY", "SELL") and session_direction != direction
    return not (opposed and strength >= float(min_strength))


def session_alignment_policy(row: Row, min_strength: float) -> bool:
    """Research-only stricter variant: neutral allowed; directional session must align."""
    pre = row.get("pretrade") or {}
    f = pre.get("features") or {}
    direction = str(row.get("direction") or "")
    session_direction = str(f.get("session_direction") or "NEUTRAL")
    strength = _finite(f.get("session_strength")) or 0.0
    if session_direction == "NEUTRAL" or strength < float(min_strength):
        return True
    return session_direction == direction


def validate_trade_memory_session_policy(rows: Iterable[Row], gap_minutes: int = 15) -> Dict[str, Any]:
    normalized = normalize_trade_memory(rows)
    episodes = collapse_trade_episodes(normalized, gap_minutes)
    thresholds = (0.25, 0.40, 0.55, 0.70, 0.85)
    cfg = WalkForwardConfig()
    baseline = performance_metrics(episodes)
    opposition = walk_forward_optimize(episodes, thresholds, session_opposition_policy, cfg)
    alignment = walk_forward_optimize(episodes, thresholds, session_alignment_policy, cfg)
    return {
        "status":"OK" if episodes else "INSUFFICIENT_DATA",
        "method":"EPISODE_AWARE_CHRONOLOGICAL_WALK_FORWARD",
        "scope":"EXECUTED_TRADE_MEMORY_ONLY",
        "limitations":[
            "Does not include trades the live system rejected or never generated.",
            "Cannot establish v3.31-vs-v3.32 directional counterfactuals without historical candle replay.",
            "Parameters are selected only on each training window and frozen for its subsequent test window.",
        ],
        "raw_closed_trades":len(normalized),
        "independent_episodes":len(episodes),
        "baseline":baseline,
        "baseline_bootstrap":bootstrap_expectancy_ci(episodes),
        "session_opposition_filter":opposition,
        "session_alignment_filter":alignment,
    }

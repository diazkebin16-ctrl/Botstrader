"""Single-window selector for the ten directional PAPER FX strategies.

The selector evaluates one frozen 30-day window without temporal splitting,
holdout comparison, or evidence imported from another period. Each lane must
retain at least ten resolved WIN/LOSS outcomes and finish strictly above 50%
win rate. Results may authorize PAPER research only and never LIVE trading.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
import hashlib
import json
import math
from typing import Any, Iterable, Mapping, Sequence

from directional_strategies import strategy_definition

RESOLVED = {"WIN", "LOSS"}
FEATURES = (
    "rr_raw", "room_to_barrier_r", "extension_atr", "volatility_ratio", "m1_momentum",
    "buy_score", "direction_edge", "h1_gap_atr", "h1_slope_atr", "m15_gap_atr",
    "m15_slope_atr", "session_strength", "session_displacement_atr", "session_momentum_atr",
)
QUANTILES = tuple(index / 10.0 for index in range(1, 10))


@dataclass(frozen=True)
class MonthPolicy:
    target_win_rate: float = 0.50
    minimum_resolved: int = 10
    maximum_rules: int = 2


def _dt(value: Any) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _quantile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def rule_passes(row: Mapping[str, Any], rule: Mapping[str, Any]) -> bool:
    features = row.get("features") if isinstance(row.get("features"), Mapping) else {}
    value = _finite(features.get(str(rule["feature"])))
    if value is None:
        return False
    threshold = float(rule["threshold"])
    if rule["operator"] == ">=":
        return value >= threshold
    if rule["operator"] == "<=":
        return value <= threshold
    raise ValueError(f"unsupported operator {rule['operator']}")


def apply_rules(rows: Iterable[Mapping[str, Any]], rules: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if all(rule_passes(row, rule) for rule in rules)]


def metrics(rows: Sequence[Mapping[str, Any]], days: float) -> dict[str, Any]:
    wins = losses = timeouts = ambiguous = 0
    resolved_r: list[float] = []
    maximum_loss_streak = loss_streak = 0
    for row in rows:
        status = str(row.get("outcome_status") or row.get("outcome") or "PENDING").upper()
        if status == "WIN":
            wins += 1
            loss_streak = 0
        elif status == "LOSS":
            losses += 1
            loss_streak += 1
            maximum_loss_streak = max(maximum_loss_streak, loss_streak)
        elif status == "TIMEOUT":
            timeouts += 1
        elif status == "AMBIGUOUS":
            ambiguous += 1
        value = _finite(row.get("realized_r"))
        if status in RESOLVED and value is not None:
            resolved_r.append(value)
    resolved = wins + losses
    gains = sum(value for value in resolved_r if value > 0)
    loss_sum = abs(sum(value for value in resolved_r if value < 0))
    return {
        "episodes": len(rows), "resolved": resolved, "wins": wins, "losses": losses,
        "timeouts": timeouts, "ambiguous": ambiguous,
        "win_rate": wins / resolved if resolved else None, "net_r": sum(resolved_r),
        "expectancy_r": sum(resolved_r) / len(resolved_r) if resolved_r else None,
        "profit_factor": gains / loss_sum if loss_sum else (999.0 if gains else None),
        "maximum_loss_streak": maximum_loss_streak,
        "resolved_per_30_days": resolved * 30.0 / max(days, 1.0),
        "episodes_per_30_days": len(rows) * 30.0 / max(days, 1.0),
    }


def _wilson_lower(wins: int, total: int, z: float = 1.2815515655446004) -> float:
    if total <= 0:
        return 0.0
    proportion = wins / total
    denominator = 1.0 + z * z / total
    centre = proportion + z * z / (2.0 * total)
    margin = z * math.sqrt((proportion * (1.0 - proportion) + z * z / (4.0 * total)) / total)
    return (centre - margin) / denominator


def _definition_sha(rules: Sequence[Mapping[str, Any]]) -> str:
    payload = json.dumps(list(rules), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _lane(rows: Sequence[Mapping[str, Any]], instrument: str, direction: str) -> list[dict[str, Any]]:
    return [dict(row) for row in rows
            if str(row.get("instrument") or "").upper() == instrument
            and str(row.get("signal") or row.get("research_direction") or "").upper() == direction]


def _window(rows: Sequence[Mapping[str, Any]], start: Any, end: Any) -> list[dict[str, Any]]:
    start_dt, end_dt = _dt(start), _dt(end)
    if not start_dt < end_dt:
        raise ValueError("start must be before end")
    return [dict(row) for row in sorted(rows, key=lambda item: str(item.get("candle_ts") or ""))
            if start_dt <= _dt(row["candle_ts"]) <= end_dt]


def _candidate_rules(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, float]] = set()
    for feature in FEATURES:
        values = []
        for row in rows:
            source = row.get("features") if isinstance(row.get("features"), Mapping) else {}
            value = _finite(source.get(feature))
            if value is not None:
                values.append(value)
        if len(values) < 10 or min(values) == max(values):
            continue
        for fraction in QUANTILES:
            threshold = round(_quantile(values, fraction), 8)
            for operator in (">=", "<="):
                identity = (feature, operator, threshold)
                if identity not in seen:
                    seen.add(identity)
                    result.append({"feature": feature, "operator": operator, "threshold": threshold})
    return result


def _wr(value: Mapping[str, Any]) -> float:
    return float(value["win_rate"]) if value.get("win_rate") is not None else -1.0


def _positive(value: Mapping[str, Any]) -> bool:
    return (float(value.get("expectancy_r") or 0.0) > 0.0
            and float(value.get("net_r") or 0.0) > 0.0
            and float(value.get("profit_factor") or 0.0) > 1.0)


def optimize_lane(rows: Sequence[Mapping[str, Any]], *, instrument: str, direction: str,
                  start: Any, end: Any, policy: MonthPolicy = MonthPolicy()) -> dict[str, Any]:
    definition = strategy_definition(instrument, direction)
    current_rules = list(definition["filters"])
    raw_window = _window(_lane(rows, instrument, direction), start, end)
    days = (_dt(end) - _dt(start)).total_seconds() / 86400.0
    raw_baseline = metrics(raw_window, days)
    incumbent = metrics(apply_rules(raw_window, current_rules), days)
    report: dict[str, Any] = {
        "strategy_id": definition["strategy_id"], "instrument": instrument, "direction": direction,
        "window": {"start": _dt(start).isoformat(), "end": _dt(end).isoformat(), "days": days},
        "split": None, "comparison_periods": [],
        "resolved_definition": "WIN + LOSS only; TIMEOUT and AMBIGUOUS excluded",
        "policy": policy.__dict__, "current_rules": current_rules,
        "baseline": incumbent, "unfiltered_baseline": raw_baseline,
        "search": None, "frozen_candidate": None, "holdout_opened": False,
        "verdict": "INSUFFICIENT_RAW_EVIDENCE",
        "selection_protocol": "ONE_30_DAY_WINDOW_MIN10_PAPER_ONLY", "production_authority": False,
    }
    if raw_baseline["resolved"] < policy.minimum_resolved:
        return report

    def screen(rules: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        result_metrics = metrics(apply_rules(raw_window, rules), days)
        gates = {
            "minimum_10_resolved": result_metrics["resolved"] >= policy.minimum_resolved,
            "win_rate_strictly_above_50": _wr(result_metrics) > policy.target_win_rate,
            "positive_expectancy_safety": _positive(result_metrics),
        }
        return {
            "all_rules": [dict(rule) for rule in rules], "metrics": result_metrics,
            "final_gates": gates,
            "wilson_lower_80": _wilson_lower(result_metrics["wins"], result_metrics["resolved"]),
            "qualified": all(gates.values()),
            "selection_score": (10.0 * _wilson_lower(result_metrics["wins"], result_metrics["resolved"])
                                + 2.0 * float(result_metrics.get("expectancy_r") or -999.0)
                                + 0.50 * _wr(result_metrics)
                                + 0.01 * math.log1p(result_metrics["resolved"]) - 0.05 * len(rules)),
        }

    rules = _candidate_rules(raw_window)
    evaluated = [screen([]), screen(current_rules)]
    singles = [screen([rule]) for rule in rules]
    evaluated.extend(singles)
    if policy.maximum_rules >= 2:
        for left, right in combinations(rules, 2):
            if left["feature"] != right["feature"]:
                evaluated.append(screen([left, right]))
    eligible = [item for item in evaluated if item["qualified"]]
    report["search"] = {
        "single_candidates": len(singles), "evaluated_candidates": len(evaluated),
        "eligible_one_window_candidates": len(eligible),
        "threshold_source": "SAME_FROZEN_30_DAY_WINDOW",
        "candidate_base": "UNFILTERED_DIRECTIONAL_LANE", "selection_is_out_of_sample": False,
        "temporal_comparison": False, "directional_score_excluded": True,
    }
    if not eligible:
        report["verdict"] = "NO_ONE_MONTH_CANDIDATE"
        return report
    frozen = sorted(eligible, key=lambda item: (-float(item["selection_score"]), str(item["all_rules"])))[0]
    report["frozen_candidate"] = {**frozen, "definition_sha256": _definition_sha(frozen["all_rules"]),
                                  "paper_only": True, "out_of_sample_validated": False}
    report["final_gates"] = frozen["final_gates"]
    report["verdict"] = "PAPER_CANDIDATE"
    return report


def optimize_all_lanes(rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]], *, start: Any,
                       end: Any, policy: MonthPolicy = MonthPolicy()) -> dict[str, Any]:
    results = []
    for instrument in ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD"):
        for direction in ("BUY", "SELL"):
            results.append(optimize_lane(rows_by_instrument.get(instrument, ()), instrument=instrument,
                                         direction=direction, start=start, end=end, policy=policy))
    approved = [item["strategy_id"] for item in results if item["verdict"] == "PAPER_CANDIDATE"]
    return {
        "window": {"start": _dt(start).isoformat(), "end": _dt(end).isoformat()},
        "split": None, "comparison_periods": [], "strategy_count": 10, "policy": policy.__dict__,
        "results": results, "approved_strategy_ids": approved, "approved_count": len(approved),
        "all_ten_qualified": len(approved) == 10, "production_authority": False,
    }

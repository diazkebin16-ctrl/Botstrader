"""Deterministic, research-only optimizer for directional strategy lanes.

The optimizer consumes already-resolved historical episodes.  Candidate rules
read only decision-time features; outcomes are used exclusively for scoring.
Discovery selects thresholds, while validation and holdout are immutable gates.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
import math
from typing import Any, Iterable, Mapping, Sequence


RESOLVED_STATUSES = {"WIN", "LOSS"}
NON_BINARY_STATUSES = (
    "TIMEOUT",
    "AMBIGUOUS",
    "PENDING",
    "NOT_HISTORICALLY_RECONSTRUCTABLE",
    "DATA_INSUFFICIENT",
    "DATA_INTEGRITY_ERROR",
    "ENTRY_INVALIDATED",
)
SEARCH_FEATURES = (
    "rr_raw",
    "room_to_barrier_r",
    "extension_atr",
    "volatility_ratio",
    "m1_momentum",
    "buy_score",
    "sell_score",
    "direction_edge",
    "h1_gap_atr",
    "h1_slope_atr",
    "m15_gap_atr",
    "m15_slope_atr",
    "session_strength",
    "session_displacement_atr",
    "session_momentum_atr",
)
QUANTILES = (0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80)


@dataclass(frozen=True)
class OptimizationPolicy:
    minimum_total_baseline_resolved: int = 15
    minimum_discovery_baseline_resolved: int = 7
    minimum_validation_baseline_resolved: int = 3
    minimum_holdout_baseline_resolved: int = 3
    minimum_candidate_total_resolved: int = 10
    minimum_candidate_oos_resolved: int = 5
    minimum_candidate_holdout_resolved: int = 2
    minimum_resolved_retention: float = 0.40
    maximum_resolved_retention: float = 0.90
    maximum_trades_per_30_days: float = 18.0
    minimum_discovery_win_rate_gain: float = 0.05
    minimum_oos_win_rate_gain: float = 0.03
    maximum_split_win_rate_regression: float = 0.05
    maximum_rules: int = 2
    maximum_pair_seed_rules: int = 30


def _dt(value: Any) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _quantile(values: Sequence[float], fraction: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _normalized_rule(feature: str, operator: str, threshold: float) -> dict[str, Any]:
    return {"feature": feature, "operator": operator, "threshold": round(float(threshold), 8)}


def rule_passes(row: Mapping[str, Any], rule: Mapping[str, Any]) -> bool:
    features = row.get("features") if isinstance(row.get("features"), Mapping) else {}
    value = _finite(features.get(str(rule["feature"])))
    if value is None:
        return False
    threshold = float(rule["threshold"])
    operator = str(rule["operator"])
    if operator == ">=":
        return value >= threshold
    if operator == "<=":
        return value <= threshold
    raise ValueError(f"unsupported operator: {operator}")


def apply_rules(rows: Iterable[Mapping[str, Any]], rules: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if all(rule_passes(row, rule) for rule in rules)]


def metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    statuses: dict[str, int] = {name: 0 for name in ("WIN", "LOSS", *NON_BINARY_STATUSES)}
    resolved_r: list[float] = []
    loss_streak = maximum_loss_streak = 0
    for row in rows:
        status = str(row.get("outcome_status") or row.get("outcome") or "PENDING").upper()
        statuses[status] = statuses.get(status, 0) + 1
        if status == "LOSS":
            loss_streak += 1
            maximum_loss_streak = max(maximum_loss_streak, loss_streak)
        elif status == "WIN":
            loss_streak = 0
        value = _finite(row.get("realized_r"))
        if status in RESOLVED_STATUSES and value is not None:
            resolved_r.append(value)
    wins = statuses.get("WIN", 0)
    losses = statuses.get("LOSS", 0)
    resolved = wins + losses
    gains = sum(value for value in resolved_r if value > 0)
    loss_sum = abs(sum(value for value in resolved_r if value < 0))
    return {
        "episodes": len(rows),
        "resolved": resolved,
        "wins": wins,
        "losses": losses,
        "win_rate": wins / resolved if resolved else None,
        "net_r": sum(resolved_r),
        "expectancy_r": sum(resolved_r) / len(resolved_r) if resolved_r else None,
        "profit_factor": gains / loss_sum if loss_sum else (999.0 if gains else None),
        "maximum_loss_streak": maximum_loss_streak,
        "statuses": statuses,
    }


def chronological_partitions(
    rows: Sequence[Mapping[str, Any]], start: Any, end: Any
) -> dict[str, list[dict[str, Any]]]:
    start_dt, end_dt = _dt(start), _dt(end)
    span = end_dt - start_dt
    discovery_end = start_dt + span * 0.50
    validation_end = start_dt + span * 0.75
    result = {"discovery": [], "validation": [], "holdout": []}
    for row in sorted(rows, key=lambda item: str(item.get("candle_ts") or "")):
        timestamp = _dt(row["candle_ts"])
        if timestamp < start_dt or timestamp > end_dt:
            continue
        bucket = "discovery" if timestamp < discovery_end else "validation" if timestamp < validation_end else "holdout"
        result[bucket].append(dict(row))
    return result

def _candidate_rules(discovery: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    seen: set[tuple[str, str, float]] = set()
    for feature in SEARCH_FEATURES:
        values = []
        for row in discovery:
            features = row.get("features") if isinstance(row.get("features"), Mapping) else {}
            value = _finite(features.get(feature))
            if value is not None:
                values.append(value)
        if len(values) < 5 or min(values) == max(values):
            continue
        for fraction in QUANTILES:
            threshold = round(_quantile(values, fraction), 8)
            for operator in (">=", "<="):
                identity = (feature, operator, threshold)
                if identity not in seen:
                    seen.add(identity)
                    rules.append(_normalized_rule(feature, operator, threshold))
    return rules


def _wr(metric: Mapping[str, Any]) -> float:
    value = metric.get("win_rate")
    return float(value) if value is not None else -1.0


def _exp(metric: Mapping[str, Any]) -> float:
    value = metric.get("expectancy_r")
    return float(value) if value is not None else -999.0


def _evaluate_candidate(
    partitions: Mapping[str, Sequence[Mapping[str, Any]]],
    baseline: Mapping[str, Mapping[str, Any]],
    rules: Sequence[Mapping[str, Any]],
    policy: OptimizationPolicy,
    window_days: float,
) -> dict[str, Any]:
    selected = {name: apply_rules(rows, rules) for name, rows in partitions.items()}
    result = {name: metrics(rows) for name, rows in selected.items()}
    combined_oos_rows = [*selected["validation"], *selected["holdout"]]
    baseline_oos_rows = [*partitions["validation"], *partitions["holdout"]]
    result["oos"] = metrics(combined_oos_rows)
    baseline_oos = metrics(baseline_oos_rows)
    result["total"] = metrics([*selected["discovery"], *combined_oos_rows])
    baseline_total = metrics([*partitions["discovery"], *baseline_oos_rows])
    resolved_retention = result["total"]["resolved"] / max(1, baseline_total["resolved"])
    trades_per_30_days = result["total"]["episodes"] * 30.0 / max(window_days, 1.0)
    qualifications = {
        "minimum_total_resolved": result["total"]["resolved"] >= policy.minimum_candidate_total_resolved,
        "minimum_oos_resolved": result["oos"]["resolved"] >= policy.minimum_candidate_oos_resolved,
        "minimum_holdout_resolved": result["holdout"]["resolved"] >= policy.minimum_candidate_holdout_resolved,
        "retention_floor": resolved_retention >= policy.minimum_resolved_retention,
        "retention_ceiling": resolved_retention <= policy.maximum_resolved_retention,
        "frequency_cap": trades_per_30_days <= policy.maximum_trades_per_30_days,
        "discovery_win_rate_gain": _wr(result["discovery"]) >= _wr(baseline["discovery"]) + policy.minimum_discovery_win_rate_gain,
        "oos_win_rate_gain": _wr(result["oos"]) >= _wr(baseline_oos) + policy.minimum_oos_win_rate_gain,
        "validation_not_regressed": _wr(result["validation"]) >= _wr(baseline["validation"]) - policy.maximum_split_win_rate_regression,
        "holdout_not_regressed": _wr(result["holdout"]) >= _wr(baseline["holdout"]) - policy.maximum_split_win_rate_regression,
        "positive_validation_expectancy": _exp(result["validation"]) > 0.0,
        "positive_holdout_expectancy": _exp(result["holdout"]) > 0.0,
        "oos_expectancy_improved": _exp(result["oos"]) > _exp(baseline_oos),
        "oos_net_positive": float(result["oos"]["net_r"]) > 0.0,
        "loss_streak_not_worse": result["oos"]["maximum_loss_streak"] <= baseline_oos["maximum_loss_streak"],
    }
    win_rate_gain = _wr(result["oos"]) - _wr(baseline_oos)
    expectancy_gain = _exp(result["oos"]) - _exp(baseline_oos)
    score = (
        4.0 * win_rate_gain
        + 1.5 * expectancy_gain
        + 0.5 * (_wr(result["discovery"]) - _wr(baseline["discovery"]))
        + 0.15 * resolved_retention
        - 0.02 * trades_per_30_days
    )
    return {
        "rules": [dict(rule) for rule in rules],
        "splits": result,
        "resolved_retention": resolved_retention,
        "trades_per_30_days": trades_per_30_days,
        "qualifications": qualifications,
        "qualified": all(qualifications.values()),
        "score": score,
    }


def optimize_lane(
    rows: Sequence[Mapping[str, Any]],
    *,
    instrument: str,
    direction: str,
    start: Any,
    end: Any,
    current_rules: Sequence[Mapping[str, Any]] = (),
    policy: OptimizationPolicy = OptimizationPolicy(),
) -> dict[str, Any]:
    """Discover at most two rules and gate them on untouched OOS periods."""
    lane_rows = [
        dict(row)
        for row in rows
        if str(row.get("instrument") or "").upper() == instrument.upper()
        and str(row.get("signal") or row.get("research_direction") or "").upper() == direction.upper()
    ]
    current_rows = apply_rules(lane_rows, current_rules)
    partitions = chronological_partitions(current_rows, start, end)
    baseline = {name: metrics(items) for name, items in partitions.items()}
    baseline_total = metrics([item for items in partitions.values() for item in items])
    baseline_oos = metrics([*partitions["validation"], *partitions["holdout"]])
    baseline["oos"] = baseline_oos
    baseline["total"] = baseline_total
    sufficiency = {
        "total": baseline_total["resolved"] >= policy.minimum_total_baseline_resolved,
        "discovery": baseline["discovery"]["resolved"] >= policy.minimum_discovery_baseline_resolved,
        "validation": baseline["validation"]["resolved"] >= policy.minimum_validation_baseline_resolved,
        "holdout": baseline["holdout"]["resolved"] >= policy.minimum_holdout_baseline_resolved,
    }
    result: dict[str, Any] = {
        "instrument": instrument.upper(),
        "direction": direction.upper(),
        "current_rules": [dict(rule) for rule in current_rules],
        "baseline": baseline,
        "sufficiency": sufficiency,
        "candidate": None,
        "verdict": "INSUFFICIENT_EVIDENCE" if not all(sufficiency.values()) else "INCUMBENT_RETAINED",
        "production_authority": False,
    }
    if not all(sufficiency.values()):
        return result

    discovery = partitions["discovery"]
    rules = _candidate_rules(discovery)
    window_days = max(1.0, (_dt(end) - _dt(start)).total_seconds() / 86400.0)
    singles = [
        _evaluate_candidate(partitions, baseline, [rule], policy, window_days)
        for rule in rules
    ]
    seeds = sorted(
        singles,
        key=lambda item: (-float(item["score"]), str(item["rules"])),
    )[: policy.maximum_pair_seed_rules]
    evaluated = list(singles)
    if policy.maximum_rules >= 2:
        for left, right in combinations((item["rules"][0] for item in seeds), 2):
            if left["feature"] == right["feature"]:
                continue
            evaluated.append(_evaluate_candidate(partitions, baseline, [left, right], policy, window_days))
    qualified = [item for item in evaluated if item["qualified"]]
    if not qualified:
        return result
    best = sorted(qualified, key=lambda item: (-float(item["score"]), str(item["rules"])))[0]
    result["candidate"] = best
    result["verdict"] = "RESEARCH_CANDIDATE"
    return result

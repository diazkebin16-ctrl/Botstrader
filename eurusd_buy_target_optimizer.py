"""Frozen-window optimizer for the EUR/USD BUY directional lane.

Thresholds are derived from discovery only.  Validation selects exactly one
candidate, which is frozen before the holdout is opened.  Outcomes never feed
decision-time features and ``directional_score`` is intentionally excluded.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
import hashlib
import json
import math
from typing import Any, Iterable, Mapping, Sequence


RESOLVED = {"WIN", "LOSS"}
FEATURES = (
    "rr_raw",
    "room_to_barrier_r",
    "extension_atr",
    "volatility_ratio",
    "m1_momentum",
    "buy_score",
    "direction_edge",
    "h1_gap_atr",
    "h1_slope_atr",
    "m15_gap_atr",
    "m15_slope_atr",
    "session_strength",
    "session_displacement_atr",
    "session_momentum_atr",
)
QUANTILES = tuple(index / 20.0 for index in range(2, 19))


@dataclass(frozen=True)
class TargetPolicy:
    minimum_trades_per_30_days: float = 10.0
    maximum_trades_per_30_days: float = 20.0
    split_frequency_tolerance: float = 2.0
    minimum_discovery_win_rate_gain: float = 0.08
    maximum_validation_win_rate_regression: float = 0.03
    minimum_holdout_win_rate_gain: float = 0.05
    minimum_oos_win_rate_gain: float = 0.05
    minimum_validation_resolved: int = 5
    minimum_holdout_resolved: int = 5
    minimum_oos_resolved: int = 12
    maximum_rules: int = 2
    pair_seed_count: int = 60


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


def split_rows(rows: Sequence[Mapping[str, Any]], start: Any, end: Any) -> dict[str, list[dict[str, Any]]]:
    start_dt, end_dt = _dt(start), _dt(end)
    span = end_dt - start_dt
    discovery_end = start_dt + span * 0.50
    validation_end = start_dt + span * 0.75
    result = {"discovery": [], "validation": [], "holdout": []}
    for row in sorted(rows, key=lambda item: str(item.get("candle_ts") or "")):
        timestamp = _dt(row["candle_ts"])
        if not start_dt <= timestamp <= end_dt:
            continue
        name = "discovery" if timestamp < discovery_end else "validation" if timestamp < validation_end else "holdout"
        result[name].append(dict(row))
    return result


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
        "episodes": len(rows),
        "resolved": resolved,
        "wins": wins,
        "losses": losses,
        "timeouts": timeouts,
        "ambiguous": ambiguous,
        "win_rate": wins / resolved if resolved else None,
        "net_r": sum(resolved_r),
        "expectancy_r": sum(resolved_r) / len(resolved_r) if resolved_r else None,
        "profit_factor": gains / loss_sum if loss_sum else (999.0 if gains else None),
        "maximum_loss_streak": maximum_loss_streak,
        "resolved_per_30_days": resolved * 30.0 / max(days, 1.0),
        "episodes_per_30_days": len(rows) * 30.0 / max(days, 1.0),
    }


def _rules_from_discovery(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
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


def _expectancy(value: Mapping[str, Any]) -> float:
    return float(value["expectancy_r"]) if value.get("expectancy_r") is not None else -999.0


def _definition_sha(rules: Sequence[Mapping[str, Any]]) -> str:
    payload = json.dumps(list(rules), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def optimize_eurusd_buy(
    rows: Sequence[Mapping[str, Any]], *, start: Any, end: Any, policy: TargetPolicy = TargetPolicy()
) -> dict[str, Any]:
    lane = [
        dict(row)
        for row in rows
        if str(row.get("instrument") or "").upper() == "EUR_USD"
        and str(row.get("signal") or row.get("research_direction") or "").upper() == "BUY"
    ]
    partitions = split_rows(lane, start, end)
    start_dt, end_dt = _dt(start), _dt(end)
    total_days = (end_dt - start_dt).total_seconds() / 86400.0
    days = {"discovery": total_days * 0.50, "validation": total_days * 0.25, "holdout": total_days * 0.25}
    baseline = {name: metrics(values, days[name]) for name, values in partitions.items()}
    baseline["oos"] = metrics([*partitions["validation"], *partitions["holdout"]], days["validation"] + days["holdout"])
    baseline["total"] = metrics(lane, total_days)

    discovery_rules = _rules_from_discovery(partitions["discovery"])

    def discovery_screen(rules: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        selected = apply_rules(partitions["discovery"], rules)
        value = metrics(selected, days["discovery"])
        frequency_distance = abs(value["resolved_per_30_days"] - 15.0)
        qualified = (
            policy.minimum_trades_per_30_days <= value["resolved_per_30_days"] <= policy.maximum_trades_per_30_days
            and _wr(value) >= _wr(baseline["discovery"]) + policy.minimum_discovery_win_rate_gain
            and _expectancy(value) > 0.0
        )
        score = 5.0 * _wr(value) + 1.5 * _expectancy(value) - 0.04 * frequency_distance - 0.03 * len(rules)
        return {"rules": [dict(rule) for rule in rules], "discovery": value, "qualified": qualified, "score": score}

    singles = [discovery_screen([rule]) for rule in discovery_rules]
    seeds = sorted(singles, key=lambda item: (-float(item["score"]), str(item["rules"])))[: policy.pair_seed_count]
    screened = list(singles)
    if policy.maximum_rules >= 2:
        for left, right in combinations((item["rules"][0] for item in seeds), 2):
            if left["feature"] == right["feature"]:
                continue
            screened.append(discovery_screen([left, right]))

    validation_candidates = []
    for item in screened:
        if not item["qualified"]:
            continue
        validation = metrics(apply_rules(partitions["validation"], item["rules"]), days["validation"])
        low = policy.minimum_trades_per_30_days - policy.split_frequency_tolerance
        high = policy.maximum_trades_per_30_days + policy.split_frequency_tolerance
        qualified = (
            validation["resolved"] >= policy.minimum_validation_resolved
            and low <= validation["resolved_per_30_days"] <= high
            and _wr(validation) >= _wr(baseline["validation"]) - policy.maximum_validation_win_rate_regression
            and _expectancy(validation) > 0.0
            and validation["net_r"] > 0.0
        )
        frequency_distance = abs(validation["resolved_per_30_days"] - 15.0)
        selection_score = (
            6.0 * _wr(validation)
            + 2.0 * _expectancy(validation)
            + 1.5 * _wr(item["discovery"])
            - 0.05 * frequency_distance
            - 0.04 * len(item["rules"])
        )
        validation_candidates.append({**item, "validation": validation, "validation_qualified": qualified, "selection_score": selection_score})

    eligible = [item for item in validation_candidates if item["validation_qualified"]]
    report: dict[str, Any] = {
        "strategy_id": "EURUSD_BUY_ONLY_V1",
        "instrument": "EUR_USD",
        "direction": "BUY",
        "window": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
        "split": {"discovery": 0.50, "validation": 0.25, "holdout": 0.25},
        "policy": policy.__dict__,
        "baseline": baseline,
        "search": {
            "features": list(FEATURES),
            "directional_score_excluded": True,
            "single_candidates": len(singles),
            "screened_candidates": len(screened),
            "validation_candidates": len(validation_candidates),
            "eligible_before_holdout": len(eligible),
        },
        "frozen_candidate": None,
        "holdout_opened": False,
        "verdict": "NO_CANDIDATE_BEFORE_HOLDOUT",
        "production_authority": False,
    }
    if not eligible:
        return report

    frozen = sorted(eligible, key=lambda item: (-float(item["selection_score"]), str(item["rules"])))[0]
    rules = frozen["rules"]
    report["frozen_candidate"] = {
        "rules": rules,
        "definition_sha256": _definition_sha(rules),
        "discovery": frozen["discovery"],
        "validation": frozen["validation"],
        "selection_score": frozen["selection_score"],
    }

    # Holdout is opened only after the winning definition above is frozen.
    report["holdout_opened"] = True
    selected = {name: apply_rules(values, rules) for name, values in partitions.items()}
    holdout = metrics(selected["holdout"], days["holdout"])
    oos = metrics([*selected["validation"], *selected["holdout"]], days["validation"] + days["holdout"])
    total = metrics([*selected["discovery"], *selected["validation"], *selected["holdout"]], total_days)
    low = policy.minimum_trades_per_30_days - policy.split_frequency_tolerance
    high = policy.maximum_trades_per_30_days + policy.split_frequency_tolerance
    gates = {
        "holdout_minimum_sample": holdout["resolved"] >= policy.minimum_holdout_resolved,
        "oos_minimum_sample": oos["resolved"] >= policy.minimum_oos_resolved,
        "holdout_frequency_stable": low <= holdout["resolved_per_30_days"] <= high,
        "full_frequency_target": policy.minimum_trades_per_30_days <= total["resolved_per_30_days"] <= policy.maximum_trades_per_30_days,
        "holdout_win_rate_improved": _wr(holdout) >= _wr(baseline["holdout"]) + policy.minimum_holdout_win_rate_gain,
        "oos_win_rate_improved": _wr(oos) >= _wr(baseline["oos"]) + policy.minimum_oos_win_rate_gain,
        "holdout_expectancy_positive": _expectancy(holdout) > 0.0,
        "oos_expectancy_positive": _expectancy(oos) > 0.0,
        "holdout_net_positive": holdout["net_r"] > 0.0,
        "oos_net_positive": oos["net_r"] > 0.0,
        "oos_loss_streak_reduced": oos["maximum_loss_streak"] < baseline["oos"]["maximum_loss_streak"],
    }
    report["holdout"] = holdout
    report["oos"] = oos
    report["total"] = total
    report["final_gates"] = gates
    report["verdict"] = "RESEARCH_CANDIDATE" if all(gates.values()) else "HOLDOUT_REJECTED"
    return report

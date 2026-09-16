"""One-shot EUR/USD BUY accuracy search with a frozen second-half holdout."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
import hashlib
import json
import math
from typing import Any, Mapping, Sequence

from eurusd_buy_target_optimizer import FEATURES, apply_rules, metrics


QUANTILES = tuple(index / 20.0 for index in range(1, 20))


@dataclass(frozen=True)
class AccuracyPolicy:
    target_win_rate: float = 0.55
    minimum_discovery_win_rate: float = 0.60
    minimum_discovery_resolved: int = 10
    minimum_holdout_resolved: int = 8
    maximum_rules: int = 2
    pair_seed_count: int = 40


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


def _split(rows: Sequence[Mapping[str, Any]], start: Any, end: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    start_dt, end_dt = _dt(start), _dt(end)
    boundary = start_dt + (end_dt - start_dt) * 0.50
    discovery: list[dict[str, Any]] = []
    holdout: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: str(item.get("candle_ts") or "")):
        timestamp = _dt(row["candle_ts"])
        if not start_dt <= timestamp <= end_dt:
            continue
        (discovery if timestamp < boundary else holdout).append(dict(row))
    return discovery, holdout


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
                # The prior attempt proved this entire mechanism unstable OOS.
                if feature == "session_momentum_atr" and operator == "<=":
                    continue
                identity = (feature, operator, threshold)
                if identity not in seen:
                    seen.add(identity)
                    result.append({"feature": feature, "operator": operator, "threshold": threshold})
    return result


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


def optimize_accuracy(
    rows: Sequence[Mapping[str, Any]], *, start: Any, end: Any, policy: AccuracyPolicy = AccuracyPolicy()
) -> dict[str, Any]:
    lane = [
        dict(row)
        for row in rows
        if str(row.get("instrument") or "").upper() == "EUR_USD"
        and str(row.get("signal") or row.get("research_direction") or "").upper() == "BUY"
    ]
    discovery, holdout = _split(lane, start, end)
    total_days = (_dt(end) - _dt(start)).total_seconds() / 86400.0
    half_days = total_days * 0.50
    baseline = {
        "discovery": metrics(discovery, half_days),
        "holdout": metrics(holdout, half_days),
        "total": metrics(lane, total_days),
    }

    def screen(rules: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        selected = metrics(apply_rules(discovery, rules), half_days)
        win_rate = float(selected["win_rate"]) if selected["win_rate"] is not None else -1.0
        expectancy = float(selected["expectancy_r"]) if selected["expectancy_r"] is not None else -999.0
        lower = _wilson_lower(selected["wins"], selected["resolved"])
        qualified = (
            selected["resolved"] >= policy.minimum_discovery_resolved
            and win_rate >= policy.minimum_discovery_win_rate
            and expectancy > 0.0
            and selected["net_r"] > 0.0
        )
        score = 8.0 * lower + 1.5 * expectancy + 0.01 * math.log1p(selected["resolved"]) - 0.04 * len(rules)
        return {
            "rules": [dict(rule) for rule in rules],
            "discovery": selected,
            "wilson_lower_80": lower,
            "qualified": qualified,
            "selection_score": score,
        }

    rules = _candidate_rules(discovery)
    singles = [screen([rule]) for rule in rules]
    seeds = sorted(singles, key=lambda item: (-float(item["selection_score"]), str(item["rules"])))[: policy.pair_seed_count]
    evaluated = list(singles)
    if policy.maximum_rules >= 2:
        for left, right in combinations((item["rules"][0] for item in seeds), 2):
            if left["feature"] == right["feature"]:
                continue
            evaluated.append(screen([left, right]))
    eligible = [item for item in evaluated if item["qualified"]]

    report: dict[str, Any] = {
        "strategy_id": "EURUSD_BUY_ONLY_V1",
        "instrument": "EUR_USD",
        "direction": "BUY",
        "window": {"start": _dt(start).isoformat(), "end": _dt(end).isoformat()},
        "split": {"discovery": 0.50, "holdout": 0.50},
        "policy": policy.__dict__,
        "baseline": baseline,
        "search": {
            "single_candidates": len(singles),
            "evaluated_candidates": len(evaluated),
            "eligible_before_holdout": len(eligible),
            "directional_score_excluded": True,
            "prior_failed_mechanism_excluded": "session_momentum_atr <= threshold",
        },
        "frozen_candidate": None,
        "holdout_opened": False,
        "verdict": "NO_CANDIDATE_BEFORE_HOLDOUT",
        "confidence_class": "REJECTED",
        "production_authority": False,
    }
    if not eligible:
        return report

    frozen = sorted(eligible, key=lambda item: (-float(item["selection_score"]), str(item["rules"])))[0]
    candidate_rules = frozen["rules"]
    report["frozen_candidate"] = {
        **frozen,
        "definition_sha256": _definition_sha(candidate_rules),
    }
    report["holdout_opened"] = True
    holdout_metrics = metrics(apply_rules(holdout, candidate_rules), half_days)
    total_metrics = metrics(apply_rules(lane, candidate_rules), total_days)
    holdout_win_rate = float(holdout_metrics["win_rate"]) if holdout_metrics["win_rate"] is not None else -1.0
    gates = {
        "holdout_minimum_sample": holdout_metrics["resolved"] >= policy.minimum_holdout_resolved,
        "holdout_win_rate_above_55": holdout_win_rate > policy.target_win_rate,
        "holdout_expectancy_positive": (holdout_metrics["expectancy_r"] or 0.0) > 0.0,
        "holdout_net_positive": holdout_metrics["net_r"] > 0.0,
        "holdout_profit_factor_above_one": (holdout_metrics["profit_factor"] or 0.0) > 1.0,
        "holdout_loss_streak_reduced": holdout_metrics["maximum_loss_streak"] < baseline["holdout"]["maximum_loss_streak"],
    }
    report["holdout"] = holdout_metrics
    report["total"] = total_metrics
    report["final_gates"] = gates
    if all(gates.values()):
        report["verdict"] = "RESEARCH_CANDIDATE"
        report["confidence_class"] = "EXPERIMENTAL"
    else:
        report["verdict"] = "HOLDOUT_REJECTED"
    return report

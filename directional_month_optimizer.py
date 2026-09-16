"""One-month, lane-isolated optimizer for the ten PAPER FX strategies.

Thresholds are learned only from the chronological discovery segment.  Exactly
one candidate per lane may be frozen before the purged holdout is opened.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import combinations
import hashlib
import json
import math
from typing import Any, Mapping, Sequence

from directional_strategies import strategy_definition
from eurusd_buy_target_optimizer import FEATURES, apply_rules, metrics


QUANTILES = tuple(index / 10.0 for index in range(1, 10))


@dataclass(frozen=True)
class MonthPolicy:
    target_win_rate: float = 0.50
    minimum_total_resolved: int = 20
    minimum_discovery_resolved: int = 12
    minimum_holdout_resolved: int = 8
    minimum_discovery_win_rate: float = 0.55
    minimum_discovery_bucket_resolved: int = 4
    minimum_win_rate_gain: float = 0.03
    discovery_fraction: float = 0.60
    purge_minutes: int = 240
    embargo_minutes: int = 30
    maximum_rules: int = 2
    pair_seed_count: int = 36


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
    return [
        dict(row)
        for row in rows
        if str(row.get("instrument") or "").upper() == instrument
        and str(row.get("signal") or row.get("research_direction") or "").upper() == direction
    ]


def _segments(
    rows: Sequence[Mapping[str, Any]], start: Any, end: Any, policy: MonthPolicy
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    start_dt, end_dt = _dt(start), _dt(end)
    boundary = start_dt + (end_dt - start_dt) * policy.discovery_fraction
    discovery_end = boundary - timedelta(minutes=policy.purge_minutes)
    holdout_start = boundary + timedelta(minutes=policy.embargo_minutes)
    discovery: list[dict[str, Any]] = []
    holdout: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: str(item.get("candle_ts") or "")):
        timestamp = _dt(row["candle_ts"])
        if start_dt <= timestamp <= discovery_end:
            discovery.append(dict(row))
        elif holdout_start <= timestamp <= end_dt:
            holdout.append(dict(row))
    return discovery, holdout


def _candidate_rules(rows: Sequence[Mapping[str, Any]], current_rules: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str, float]] = set()
    current = {(str(rule["feature"]), str(rule["operator"]), float(rule["threshold"])) for rule in current_rules}
    for feature in FEATURES:
        values = []
        for row in rows:
            source = row.get("features") if isinstance(row.get("features"), Mapping) else {}
            value = _finite(source.get(feature))
            if value is not None:
                values.append(value)
        if len(values) < 12 or min(values) == max(values):
            continue
        for fraction in QUANTILES:
            threshold = round(_quantile(values, fraction), 8)
            for operator in (">=", "<="):
                identity = (feature, operator, threshold)
                if identity in seen or identity in current:
                    continue
                seen.add(identity)
                result.append({"feature": feature, "operator": operator, "threshold": threshold})
    return result


def _split_discovery_buckets(rows: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered = sorted((dict(row) for row in rows), key=lambda item: str(item.get("candle_ts") or ""))
    midpoint = len(ordered) // 2
    return ordered[:midpoint], ordered[midpoint:]


def optimize_lane(
    rows: Sequence[Mapping[str, Any]], *, instrument: str, direction: str, start: Any, end: Any,
    policy: MonthPolicy = MonthPolicy(),
) -> dict[str, Any]:
    definition = strategy_definition(instrument, direction)
    current_rules = list(definition["filters"])
    raw_lane = _lane(rows, instrument, direction)
    current_lane = apply_rules(raw_lane, current_rules)
    discovery, holdout = _segments(current_lane, start, end, policy)
    total_days = (_dt(end) - _dt(start)).total_seconds() / 86400.0
    discovery_days = total_days * policy.discovery_fraction
    holdout_days = total_days * (1.0 - policy.discovery_fraction)
    baseline = {
        "total": metrics(current_lane, total_days),
        "discovery": metrics(discovery, discovery_days),
        "holdout": metrics(holdout, holdout_days),
    }
    report: dict[str, Any] = {
        "strategy_id": definition["strategy_id"],
        "instrument": instrument,
        "direction": direction,
        "window": {"start": _dt(start).isoformat(), "end": _dt(end).isoformat()},
        "split": {
            "discovery_fraction": policy.discovery_fraction,
            "purge_minutes": policy.purge_minutes,
            "embargo_minutes": policy.embargo_minutes,
        },
        "policy": policy.__dict__,
        "current_rules": current_rules,
        "baseline": baseline,
        "search": None,
        "frozen_candidate": None,
        "holdout_opened": False,
        "verdict": "INSUFFICIENT_BASELINE_EVIDENCE",
        "production_authority": False,
    }
    if baseline["total"]["resolved"] < policy.minimum_total_resolved:
        return report

    first_bucket, second_bucket = _split_discovery_buckets(discovery)

    def screen(additional_rules: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        selected = apply_rules(discovery, additional_rules)
        selected_metrics = metrics(selected, discovery_days)
        first_metrics = metrics(apply_rules(first_bucket, additional_rules), discovery_days / 2.0)
        second_metrics = metrics(apply_rules(second_bucket, additional_rules), discovery_days / 2.0)
        win_rate = float(selected_metrics["win_rate"]) if selected_metrics["win_rate"] is not None else -1.0
        baseline_rate = float(baseline["discovery"]["win_rate"] or 0.0)
        bucket_stable = all(
            item["resolved"] >= policy.minimum_discovery_bucket_resolved
            and float(item["win_rate"] or 0.0) > policy.target_win_rate
            and float(item["net_r"]) > 0.0
            for item in (first_metrics, second_metrics)
        )
        qualified = (
            selected_metrics["resolved"] >= policy.minimum_discovery_resolved
            and win_rate >= policy.minimum_discovery_win_rate
            and win_rate >= baseline_rate + policy.minimum_win_rate_gain
            and float(selected_metrics["expectancy_r"] or 0.0) > 0.0
            and float(selected_metrics["net_r"]) > 0.0
            and bucket_stable
        )
        score = (
            8.0 * _wilson_lower(selected_metrics["wins"], selected_metrics["resolved"])
            + 1.5 * float(selected_metrics["expectancy_r"] or -999.0)
            + 0.01 * math.log1p(selected_metrics["resolved"])
            - 0.04 * len(additional_rules)
        )
        return {
            "additional_rules": [dict(rule) for rule in additional_rules],
            "discovery": selected_metrics,
            "discovery_first_bucket": first_metrics,
            "discovery_second_bucket": second_metrics,
            "wilson_lower_80": _wilson_lower(selected_metrics["wins"], selected_metrics["resolved"]),
            "qualified": qualified,
            "selection_score": score,
        }

    rules = _candidate_rules(discovery, current_rules)
    singles = [screen([rule]) for rule in rules]
    seeds = sorted(singles, key=lambda item: (-float(item["selection_score"]), str(item["additional_rules"])))[:policy.pair_seed_count]
    evaluated = list(singles)
    if policy.maximum_rules >= 2:
        for left, right in combinations((item["additional_rules"][0] for item in seeds), 2):
            if left["feature"] == right["feature"]:
                continue
            evaluated.append(screen([left, right]))
    eligible = [item for item in evaluated if item["qualified"]]
    report["search"] = {
        "single_candidates": len(singles),
        "evaluated_candidates": len(evaluated),
        "eligible_before_holdout": len(eligible),
        "directional_score_excluded": True,
    }
    if not eligible:
        report["verdict"] = "NO_CANDIDATE_BEFORE_HOLDOUT"
        return report

    frozen = sorted(eligible, key=lambda item: (-float(item["selection_score"]), str(item["additional_rules"])))[0]
    additional_rules = frozen["additional_rules"]
    all_rules = current_rules + additional_rules
    report["frozen_candidate"] = {
        **frozen,
        "all_rules": all_rules,
        "definition_sha256": _definition_sha(all_rules),
    }
    report["holdout_opened"] = True
    holdout_metrics = metrics(apply_rules(holdout, additional_rules), holdout_days)
    total_metrics = metrics(apply_rules(current_lane, additional_rules), total_days)
    holdout_rate = float(holdout_metrics["win_rate"] or 0.0)
    total_rate = float(total_metrics["win_rate"] or 0.0)
    baseline_total_rate = float(baseline["total"]["win_rate"] or 0.0)
    baseline_holdout_rate = float(baseline["holdout"]["win_rate"] or 0.0)
    gates = {
        "total_minimum_20_resolved": total_metrics["resolved"] >= policy.minimum_total_resolved,
        "total_win_rate_above_50": total_rate > policy.target_win_rate,
        "total_win_rate_improved": total_rate >= baseline_total_rate + policy.minimum_win_rate_gain,
        "total_expectancy_positive": float(total_metrics["expectancy_r"] or 0.0) > 0.0,
        "total_net_positive": float(total_metrics["net_r"]) > 0.0,
        "total_profit_factor_above_one": float(total_metrics["profit_factor"] or 0.0) > 1.0,
        "holdout_minimum_sample": holdout_metrics["resolved"] >= policy.minimum_holdout_resolved,
        "holdout_win_rate_above_50": holdout_rate > policy.target_win_rate,
        "holdout_not_worse_than_baseline": holdout_rate >= baseline_holdout_rate,
        "holdout_expectancy_positive": float(holdout_metrics["expectancy_r"] or 0.0) > 0.0,
        "holdout_net_positive": float(holdout_metrics["net_r"]) > 0.0,
        "holdout_profit_factor_above_one": float(holdout_metrics["profit_factor"] or 0.0) > 1.0,
        "holdout_loss_streak_not_worse": holdout_metrics["maximum_loss_streak"] <= baseline["holdout"]["maximum_loss_streak"],
    }
    report["holdout"] = holdout_metrics
    report["total"] = total_metrics
    report["final_gates"] = gates
    report["verdict"] = "RESEARCH_CANDIDATE" if all(gates.values()) else "HOLDOUT_REJECTED"
    return report


def optimize_all_lanes(
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]], *, start: Any, end: Any,
    policy: MonthPolicy = MonthPolicy(),
) -> dict[str, Any]:
    results = []
    for instrument in ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD"):
        rows = rows_by_instrument.get(instrument, ())
        for direction in ("BUY", "SELL"):
            results.append(optimize_lane(rows, instrument=instrument, direction=direction, start=start, end=end, policy=policy))
    approved = [item["strategy_id"] for item in results if item["verdict"] == "RESEARCH_CANDIDATE"]
    return {
        "window": {"start": _dt(start).isoformat(), "end": _dt(end).isoformat()},
        "policy": policy.__dict__,
        "results": results,
        "approved_strategy_ids": approved,
        "approved_count": len(approved),
        "production_authority": False,
    }

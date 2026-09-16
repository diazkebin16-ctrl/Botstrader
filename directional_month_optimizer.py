"""Two-month, lane-isolated optimizer for the ten PAPER FX strategies.

The first frozen month is used for discovery. Exactly one candidate per lane
may be frozen before the second frozen month is opened as an out-of-sample
holdout. A candidate must retain at least ten resolved WIN/LOSS outcomes in
each month; TIMEOUT and AMBIGUOUS episodes never count toward that minimum.
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
    minimum_resolved_per_month: int = 10
    minimum_total_resolved: int = 20
    minimum_first_month_win_rate_gain: float = 0.01
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
    rows: Sequence[Mapping[str, Any]], start: Any, second_month_start: Any, end: Any,
    policy: MonthPolicy,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    start_dt, second_start_dt, end_dt = _dt(start), _dt(second_month_start), _dt(end)
    if not start_dt < second_start_dt <= end_dt:
        raise ValueError("second_month_start must fall inside the frozen window")
    first_month_end = second_start_dt - timedelta(minutes=policy.purge_minutes)
    second_month_open = second_start_dt + timedelta(minutes=policy.embargo_minutes)
    first_month: list[dict[str, Any]] = []
    second_month: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: str(item.get("candle_ts") or "")):
        timestamp = _dt(row["candle_ts"])
        if start_dt <= timestamp < first_month_end:
            first_month.append(dict(row))
        elif second_month_open <= timestamp <= end_dt:
            second_month.append(dict(row))
    return first_month, second_month


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
        if len(values) < 10 or min(values) == max(values):
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


def _wr(value: Mapping[str, Any]) -> float:
    return float(value["win_rate"]) if value.get("win_rate") is not None else -1.0


def _positive(value: Mapping[str, Any]) -> bool:
    return (
        float(value.get("expectancy_r") or 0.0) > 0.0
        and float(value.get("net_r") or 0.0) > 0.0
        and float(value.get("profit_factor") or 0.0) > 1.0
    )


def optimize_lane(
    rows: Sequence[Mapping[str, Any]], *, instrument: str, direction: str, start: Any,
    second_month_start: Any, end: Any, policy: MonthPolicy = MonthPolicy(),
) -> dict[str, Any]:
    definition = strategy_definition(instrument, direction)
    current_rules = list(definition["filters"])
    raw_lane = _lane(rows, instrument, direction)
    current_lane = apply_rules(raw_lane, current_rules)
    first_month, second_month = _segments(current_lane, start, second_month_start, end, policy)
    first_days = (_dt(second_month_start) - _dt(start)).total_seconds() / 86400.0
    second_days = (_dt(end) - _dt(second_month_start)).total_seconds() / 86400.0
    total_days = first_days + second_days
    baseline = {
        "first_month": metrics(first_month, first_days),
        "second_month": metrics(second_month, second_days),
        "total": metrics([*first_month, *second_month], total_days),
    }
    report: dict[str, Any] = {
        "strategy_id": definition["strategy_id"],
        "instrument": instrument,
        "direction": direction,
        "window": {
            "start": _dt(start).isoformat(),
            "second_month_start": _dt(second_month_start).isoformat(),
            "end": _dt(end).isoformat(),
        },
        "split": {
            "first_month_role": "DISCOVERY",
            "second_month_role": "FROZEN_HOLDOUT",
            "purge_minutes": policy.purge_minutes,
            "embargo_minutes": policy.embargo_minutes,
        },
        "resolved_definition": "WIN + LOSS only; TIMEOUT and AMBIGUOUS excluded",
        "policy": policy.__dict__,
        "current_rules": current_rules,
        "baseline": baseline,
        "search": None,
        "frozen_candidate": None,
        "holdout_opened": False,
        "verdict": "INSUFFICIENT_BASELINE_EVIDENCE",
        "production_authority": False,
    }
    if (
        baseline["first_month"]["resolved"] < policy.minimum_resolved_per_month
        or baseline["second_month"]["resolved"] < policy.minimum_resolved_per_month
    ):
        return report

    baseline_first_rate = _wr(baseline["first_month"])

    def screen(additional_rules: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        selected_metrics = metrics(apply_rules(first_month, additional_rules), first_days)
        win_rate = _wr(selected_metrics)
        qualified = (
            selected_metrics["resolved"] >= policy.minimum_resolved_per_month
            and win_rate > policy.target_win_rate
            and win_rate >= baseline_first_rate + policy.minimum_first_month_win_rate_gain
            and _positive(selected_metrics)
        )
        score = (
            8.0 * _wilson_lower(selected_metrics["wins"], selected_metrics["resolved"])
            + 1.5 * float(selected_metrics["expectancy_r"] or -999.0)
            + 0.01 * math.log1p(selected_metrics["resolved"])
            - 0.04 * len(additional_rules)
        )
        return {
            "additional_rules": [dict(rule) for rule in additional_rules],
            "first_month": selected_metrics,
            "wilson_lower_80": _wilson_lower(selected_metrics["wins"], selected_metrics["resolved"]),
            "qualified": qualified,
            "selection_score": score,
        }

    rules = _candidate_rules(first_month, current_rules)
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
        "threshold_source": "FIRST_MONTH_ONLY",
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
    second_metrics = metrics(apply_rules(second_month, additional_rules), second_days)
    total_metrics = metrics(apply_rules([*first_month, *second_month], additional_rules), total_days)
    second_rate = _wr(second_metrics)
    total_rate = _wr(total_metrics)
    gates = {
        "first_month_minimum_10_resolved": frozen["first_month"]["resolved"] >= policy.minimum_resolved_per_month,
        "first_month_win_rate_above_50": _wr(frozen["first_month"]) > policy.target_win_rate,
        "first_month_positive": _positive(frozen["first_month"]),
        "second_month_minimum_10_resolved": second_metrics["resolved"] >= policy.minimum_resolved_per_month,
        "second_month_win_rate_above_50": second_rate > policy.target_win_rate,
        "second_month_not_worse_than_baseline": second_rate >= _wr(baseline["second_month"]),
        "second_month_positive": _positive(second_metrics),
        "total_minimum_20_resolved": total_metrics["resolved"] >= policy.minimum_total_resolved,
        "total_win_rate_above_50": total_rate > policy.target_win_rate,
        "total_positive": _positive(total_metrics),
    }
    report["second_month"] = second_metrics
    report["total"] = total_metrics
    report["final_gates"] = gates
    report["verdict"] = "RESEARCH_CANDIDATE" if all(gates.values()) else "SECOND_MONTH_REJECTED"
    return report


def optimize_all_lanes(
    rows_by_instrument: Mapping[str, Sequence[Mapping[str, Any]]], *, start: Any,
    second_month_start: Any, end: Any, policy: MonthPolicy = MonthPolicy(),
) -> dict[str, Any]:
    results = []
    for instrument in ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD"):
        rows = rows_by_instrument.get(instrument, ())
        for direction in ("BUY", "SELL"):
            results.append(
                optimize_lane(
                    rows, instrument=instrument, direction=direction, start=start,
                    second_month_start=second_month_start, end=end, policy=policy,
                )
            )
    approved = [item["strategy_id"] for item in results if item["verdict"] == "RESEARCH_CANDIDATE"]
    return {
        "window": {
            "start": _dt(start).isoformat(),
            "second_month_start": _dt(second_month_start).isoformat(),
            "end": _dt(end).isoformat(),
        },
        "policy": policy.__dict__,
        "results": results,
        "approved_strategy_ids": approved,
        "approved_count": len(approved),
        "production_authority": False,
    }

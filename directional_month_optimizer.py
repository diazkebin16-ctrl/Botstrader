"""Two-month, lane-isolated selector for the ten PAPER FX strategies.

This selector is intentionally retrospective: both frozen months participate
in candidate selection.  It is used only to start a PAPER forward experiment,
never as proof of out-of-sample profitability or as LIVE authority.  A final
candidate must retain at least ten resolved WIN/LOSS outcomes in each month
and finish strictly above 50% with positive expectancy in each month.
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
    purge_minutes: int = 240
    embargo_minutes: int = 30
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
                if identity in seen:
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
    raw_first_month, raw_second_month = _segments(raw_lane, start, second_month_start, end, policy)
    current_first_month = apply_rules(raw_first_month, current_rules)
    current_second_month = apply_rules(raw_second_month, current_rules)
    first_days = (_dt(second_month_start) - _dt(start)).total_seconds() / 86400.0
    second_days = (_dt(end) - _dt(second_month_start)).total_seconds() / 86400.0
    total_days = first_days + second_days
    raw_baseline = {
        "first_month": metrics(raw_first_month, first_days),
        "second_month": metrics(raw_second_month, second_days),
        "total": metrics([*raw_first_month, *raw_second_month], total_days),
    }
    incumbent = {
        "first_month": metrics(current_first_month, first_days),
        "second_month": metrics(current_second_month, second_days),
        "total": metrics([*current_first_month, *current_second_month], total_days),
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
            "first_month_role": "RETROSPECTIVE_SELECTION",
            "second_month_role": "RETROSPECTIVE_SELECTION",
            "purge_minutes": policy.purge_minutes,
            "embargo_minutes": policy.embargo_minutes,
        },
        "resolved_definition": "WIN + LOSS only; TIMEOUT and AMBIGUOUS excluded",
        "policy": policy.__dict__,
        "current_rules": current_rules,
        "baseline": incumbent,
        "unfiltered_baseline": raw_baseline,
        "search": None,
        "frozen_candidate": None,
        "holdout_opened": False,
        "verdict": "INSUFFICIENT_RAW_EVIDENCE",
        "selection_protocol": "TWO_MONTH_CONSTRAINED_PAPER_ONLY",
        "production_authority": False,
    }
    if (
        raw_baseline["first_month"]["resolved"] < policy.minimum_resolved_per_month
        or raw_baseline["second_month"]["resolved"] < policy.minimum_resolved_per_month
    ):
        return report

    def screen(rules: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        first_metrics = metrics(apply_rules(raw_first_month, rules), first_days)
        second_metrics = metrics(apply_rules(raw_second_month, rules), second_days)
        total_metrics = metrics(
            apply_rules([*raw_first_month, *raw_second_month], rules), total_days,
        )
        gates = {
            "first_month_minimum_10_resolved": first_metrics["resolved"] >= policy.minimum_resolved_per_month,
            "first_month_win_rate_above_50": _wr(first_metrics) > policy.target_win_rate,
            "first_month_positive": _positive(first_metrics),
            "second_month_minimum_10_resolved": second_metrics["resolved"] >= policy.minimum_resolved_per_month,
            "second_month_win_rate_above_50": _wr(second_metrics) > policy.target_win_rate,
            "second_month_positive": _positive(second_metrics),
            "total_minimum_20_resolved": total_metrics["resolved"] >= policy.minimum_total_resolved,
            "total_win_rate_above_50": _wr(total_metrics) > policy.target_win_rate,
            "total_positive": _positive(total_metrics),
        }
        qualified = all(gates.values())
        first_wilson = _wilson_lower(first_metrics["wins"], first_metrics["resolved"])
        second_wilson = _wilson_lower(second_metrics["wins"], second_metrics["resolved"])
        minimum_expectancy = min(
            float(first_metrics.get("expectancy_r") or -999.0),
            float(second_metrics.get("expectancy_r") or -999.0),
        )
        score = (
            10.0 * min(first_wilson, second_wilson)
            + 2.0 * minimum_expectancy
            + 0.50 * min(_wr(first_metrics), _wr(second_metrics))
            + 0.01 * math.log1p(total_metrics["resolved"])
            - 0.05 * len(rules)
        )
        return {
            "all_rules": [dict(rule) for rule in rules],
            "first_month": first_metrics,
            "second_month": second_metrics,
            "total": total_metrics,
            "final_gates": gates,
            "minimum_month_wilson_lower_80": min(first_wilson, second_wilson),
            "qualified": qualified,
            "selection_score": score,
        }

    rules = _candidate_rules([*raw_first_month, *raw_second_month])
    evaluated = [screen([]), screen(current_rules)]
    singles = [screen([rule]) for rule in rules]
    evaluated.extend(singles)
    if policy.maximum_rules >= 2:
        for left, right in combinations(rules, 2):
            if left["feature"] == right["feature"]:
                continue
            evaluated.append(screen([left, right]))
    eligible = [item for item in evaluated if item["qualified"]]
    report["search"] = {
        "single_candidates": len(singles),
        "evaluated_candidates": len(evaluated),
        "eligible_two_month_candidates": len(eligible),
        "threshold_source": "BOTH_FROZEN_MONTHS",
        "candidate_base": "UNFILTERED_DIRECTIONAL_LANE",
        "selection_is_out_of_sample": False,
        "directional_score_excluded": True,
    }
    if not eligible:
        report["verdict"] = "NO_TWO_MONTH_CANDIDATE"
        return report

    frozen = sorted(
        eligible,
        key=lambda item: (-float(item["selection_score"]), str(item["all_rules"])),
    )[0]
    all_rules = frozen["all_rules"]
    report["frozen_candidate"] = {
        **frozen,
        "definition_sha256": _definition_sha(all_rules),
        "paper_only": True,
        "out_of_sample_validated": False,
    }
    report["second_month"] = frozen["second_month"]
    report["total"] = frozen["total"]
    report["final_gates"] = frozen["final_gates"]
    report["verdict"] = "PAPER_CANDIDATE"
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
    approved = [item["strategy_id"] for item in results if item["verdict"] == "PAPER_CANDIDATE"]
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

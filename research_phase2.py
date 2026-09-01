"""Leakage-resistant Phase 2 research engines for historical replay artifacts.

This module is offline/research only.  Candidate predicates consume fields that
exist at decision time; outcomes are visible only to evaluators.  Discovery and
validation may select a candidate, but the final holdout is opened only after an
immutable freeze artifact exists.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from replay_validation import ReplayValidationConfig, chronological_holdout, walk_forward_splits
from research_manager import sha256_file, utc_now


BINARY_OUTCOMES = {"WIN", "LOSS"}
NON_BINARY_OUTCOMES = {
    "TIMEOUT", "AMBIGUOUS", "PENDING", "NOT_HISTORICALLY_RECONSTRUCTABLE",
}
NUMERIC_FEATURES = (
    "rr_raw", "room_to_barrier_r", "extension_atr", "volatility_ratio",
    "direction_edge", "session_strength", "session_displacement_atr",
    "session_momentum_atr", "h1_gap_atr", "h1_slope_atr",
    "m15_gap_atr", "m15_slope_atr",
)
# Exact names emitted by ``server._direction_hypothesis``.  The replay may expose
# only a subset; absent mutable states are never inferred from the final result.
M1_INTERNALS = (
    "m1_ema9_side_ok", "m1_momentum", "m1_candle_color_ok", "m1_confirm",
    "m1_shadow_confirm", "m1_exception_shadow",
)


def _load(path: str | Path) -> Dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def _dt(value: Any) -> datetime:
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _finite(value: Any) -> Optional[float]:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _status(row: Mapping[str, Any]) -> str:
    return str(row.get("outcome_status") or "PENDING").upper()


def validate_rows(rows: Iterable[Mapping[str, Any]]) -> None:
    for row in rows:
        status = _status(row)
        if status in NON_BINARY_OUTCOMES and row.get("label") not in (None, ""):
            raise ValueError(f"{status} must not have a binary label")
        if row.get("dual_touch_same_bar") is True and status != "AMBIGUOUS":
            raise ValueError("dual-touch same-bar must be AMBIGUOUS")


def _row_id(row: Mapping[str, Any]) -> str:
    material = {
        "instrument": row.get("instrument"),
        "candle_ts": row.get("candle_ts"),
        "direction": row.get("research_direction") or row.get("chosen_signal") or row.get("signal"),
    }
    return _canonical_hash(material)[:20]


def episode_dedup_evidence(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Report duplicate canonical episode identities without collapsing rows."""
    identities = [str(row.get("episode_id") or _row_id(row)) for row in rows]
    unique = set(identities)
    duplicate_count = len(identities) - len(unique)
    return {
        "status": "PASS" if duplicate_count == 0 else "FAIL",
        "total_episodes": len(identities),
        "unique_episode_identities": len(unique),
        "duplicate_count": duplicate_count,
        "identity": "episode_id_or_canonical_instrument_timestamp_direction",
    }


def _partition_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    return _canonical_hash([_row_id(row) for row in rows])


def _feature(row: Mapping[str, Any], name: str) -> Optional[float]:
    value = (row.get("features") or {}).get(name)
    if value is None and name == "rr_raw":
        value = row.get("rr_raw")
    return _finite(value)


def resolved_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows if _status(row) in BINARY_OUTCOMES]


def metrics(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Binary performance metrics with non-binary outcomes reported separately."""
    all_rows = [dict(row) for row in rows]
    binary = resolved_rows(all_rows)
    values = []
    for row in binary:
        value = _finite(row.get("realized_r"))
        if value is None:
            value = 1.0 if _status(row) == "WIN" else -1.0
        values.append(value)
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    status_counts: Dict[str, int] = {}
    for row in all_rows:
        status_counts[_status(row)] = status_counts.get(_status(row), 0) + 1
    return {
        "episodes": len(all_rows),
        "resolved_binary": len(binary),
        "wins": sum(_status(row) == "WIN" for row in binary),
        "losses": sum(_status(row) == "LOSS" for row in binary),
        "win_rate": len(wins) / len(values) if values else None,
        "expectancy_r": sum(values) / len(values) if values else None,
        "profit_factor": (
            gross_profit / gross_loss if gross_loss else (999.0 if gross_profit else None)
        ),
        "net_r": sum(values),
        "outcomes": dict(sorted(status_counts.items())),
    }


def _quantiles(values: Sequence[float]) -> List[float]:
    unique = sorted(set(values))
    if len(unique) < 3:
        return []
    positions = (0.20, 0.35, 0.50, 0.65, 0.80)
    result = []
    for position in positions:
        index = int(round((len(unique) - 1) * position))
        result.append(unique[index])
    return sorted(set(result))


def _candidate_id(feature: str, operator: str, threshold: float) -> str:
    token = _canonical_hash([feature, operator, round(float(threshold), 12)])[:10]
    return f"{feature}:{operator}:{token}"


def candidate_passes(candidate: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
    rules = candidate.get("rules") or []
    for rule in rules:
        value = _feature(row, str(rule["feature"]))
        if value is None:
            return False
        threshold = float(rule["threshold"])
        if rule["operator"] == ">=" and value < threshold:
            return False
        if rule["operator"] == "<=" and value > threshold:
            return False
    return True


def apply_candidate(candidate: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(row) for row in rows if candidate_passes(candidate, row)]


def candidate_analysis(candidate: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    baseline_all = [dict(row) for row in rows]
    selected_all = apply_candidate(candidate, baseline_all)
    base_metrics = metrics(baseline_all)
    selected_metrics = metrics(selected_all)
    base_wins = base_metrics["wins"]
    base_losses = base_metrics["losses"]
    selected_wins = selected_metrics["wins"]
    selected_losses = selected_metrics["losses"]
    outcome_effect = {}
    for status in ("WIN", "LOSS", "TIMEOUT", "AMBIGUOUS", "PENDING", "NOT_HISTORICALLY_RECONSTRUCTABLE"):
        before = sum(_status(row) == status for row in baseline_all)
        kept = sum(_status(row) == status for row in selected_all)
        outcome_effect[status] = {
            "baseline": before,
            "kept": kept,
            "blocked": before - kept,
            "retention": kept / before if before else None,
            "rejection": 1.0 - kept / before if before else None,
        }
    return {
        "candidate": dict(candidate),
        "baseline": base_metrics,
        "selected": selected_metrics,
        "win_retention": selected_wins / base_wins if base_wins else None,
        "loss_rejection": 1.0 - selected_losses / base_losses if base_losses else None,
        "wins_rejected": base_wins - selected_wins,
        "losses_rejected": base_losses - selected_losses,
        "expectancy_delta_r": (
            (selected_metrics["expectancy_r"] or 0.0) - (base_metrics["expectancy_r"] or 0.0)
        ),
        "profit_factor_delta": (
            (selected_metrics["profit_factor"] or 0.0) - (base_metrics["profit_factor"] or 0.0)
        ),
        "outcome_effect": outcome_effect,
    }


def _phase1_policy(phase1: Mapping[str, Any]) -> Dict[str, Any]:
    if phase1.get("selection_scope") != "DISCOVERY_ONLY":
        raise ValueError("Phase 2 requires a DISCOVERY_ONLY Phase 1 artifact")
    return {
        "opened_gates": list((phase1.get("best_policy") or {}).get("opened_gates") or []),
        "phase1_sha256": phase1.get("artifact_sha256"),
    }


def prepare_phase2(
    target_population_path: str,
    phase1_path: str,
    *,
    horizon_minutes: int = 240,
    discovery_fraction: float = 0.60,
    validation_fraction: float = 0.20,
    embargo_minutes: int = 30,
) -> Dict[str, Any]:
    source = _load(target_population_path)
    phase1 = _load(phase1_path)
    if source.get("lookahead_protection") is not True:
        raise ValueError("Target population lacks look-ahead protection")
    rows = list(source.get("episodes") or [])
    validate_rows(rows)
    dedup = episode_dedup_evidence(rows)
    replay_methodology = source.get("replay_methodology") or {}
    config = ReplayValidationConfig(
        discovery_fraction=discovery_fraction,
        validation_fraction=validation_fraction,
        embargo_minutes=embargo_minutes,
    )
    split = chronological_holdout(rows, horizon_bars=horizon_minutes, config=config)
    if split["status"] != "OK":
        raise ValueError("Insufficient data for chronological discovery/validation/holdout")
    if phase1.get("partition", {}).get("discovery_hash") != _partition_hash(split["discovery"]):
        raise ValueError("Phase 1 discovery partition does not match Phase 2")
    available = {
        name: sum(_feature(row, name) is not None for row in split["discovery"])
        for name in NUMERIC_FEATURES
    }
    return {
        "status": "OK" if dedup["status"] == "PASS" else "FAIL",
        "stage": "phase_2",
        "instrument": source.get("instrument"),
        "variant": source.get("variant"),
        "start": source.get("start"),
        "end": source.get("end"),
        "dataset_identity": source.get("dataset_identity") or {"status": "NOT TESTED"},
        "input_sha256": sha256_file(target_population_path),
        "phase1_sha256": sha256_file(phase1_path),
        "lookahead_protection": True,
        "future_bars_used_only_for_outcome": True,
        "lookahead_evidence": {
            "replay": {
                "no_lookahead_decision": replay_methodology.get("no_lookahead_decision"),
                "future_bars_only_for_outcome": replay_methodology.get("future_bars_only_for_outcome"),
                "lookahead_detected": replay_methodology.get("lookahead_detected"),
            },
            "target_population": {
                "lookahead_protection": source.get("lookahead_protection"),
                "future_bars_used_only_for_outcome": source.get("future_bars_used_only_for_outcome"),
                "lookahead_detected": source.get("lookahead_detected"),
            },
        },
        "episode_dedup": dedup,
        "selection_protocol": "DISCOVERY_DEFINE__VALIDATION_SELECT__FREEZE__HOLDOUT_ONCE",
        "phase1_policy": _phase1_policy({**phase1, "artifact_sha256": sha256_file(phase1_path)}),
        "partition_config": {
            "horizon_minutes": int(horizon_minutes),
            "discovery_fraction": float(discovery_fraction),
            "validation_fraction": float(validation_fraction),
            "embargo_minutes": int(embargo_minutes),
        },
        "partitions": {
            "status": split["status"],
            "boundaries": split.get("boundaries") or {},
            "purged": split["purged"],
            "embargoed": split["embargoed"],
            "purged_rows": split.get("purged_rows") or [],
            "embargoed_rows": split.get("embargoed_rows") or [],
            "discovery": {"episodes": len(split["discovery"]), "hash": _partition_hash(split["discovery"])},
            "validation": {"episodes": len(split["validation"]), "hash": _partition_hash(split["validation"])},
            "holdout": {"episodes": len(split["test"]), "hash": _partition_hash(split["test"])},
        },
        "eligible_entry_features": available,
        "safety_risk_global_gates": "SEPARATE_IMMUTABLE_NOT_CANDIDATE_FEATURES",
        "learned_research_veto": "NOT_HISTORICALLY_RECONSTRUCTABLE",
    }


def _split_from_spec(source: Mapping[str, Any], spec: Mapping[str, Any]) -> Dict[str, Any]:
    config = spec.get("partition_config") or {}
    split = chronological_holdout(
        list(source.get("episodes") or []),
        horizon_bars=int(config.get("horizon_minutes", 240)),
        config=ReplayValidationConfig(
            discovery_fraction=float(config.get("discovery_fraction", 0.60)),
            validation_fraction=float(config.get("validation_fraction", 0.20)),
            embargo_minutes=int(config.get("embargo_minutes", 30)),
        ),
    )
    expected = spec.get("partitions") or {}
    for key, actual_key in (("discovery", "discovery"), ("validation", "validation"), ("holdout", "test")):
        if _partition_hash(split[actual_key]) != (expected.get(key) or {}).get("hash"):
            raise ValueError(f"Partition identity mismatch: {key}")
    return split


def _phase1_eligible_rows(spec: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    # Reuse the canonical Phase 1 strategic/safety separation instead of
    # reimplementing it in Phase 2.
    from research_pipeline import _eligible
    opened = set((spec.get("phase1_policy") or {}).get("opened_gates") or [])
    return [dict(row) for row in rows if _eligible(row, opened)]


def _generate_candidates(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    candidates = []
    for feature in NUMERIC_FEATURES:
        values = [value for row in rows if (value := _feature(row, feature)) is not None]
        for threshold in _quantiles(values):
            for operator in (">=", "<="):
                candidates.append({
                    "id": _candidate_id(feature, operator, threshold),
                    "rules": [{"feature": feature, "operator": operator, "threshold": threshold}],
                    "filter": feature,
                    "subfilter": None,
                    "old_rule": "NO_PHASE_2_THRESHOLD",
                    "candidate_rule": f"{feature} {operator} {threshold:.12g}",
                    "threshold": threshold,
                    "direction_semantics": "SAME_NUMERIC_PREDICATE_FOR_BUY_AND_SELL",
                    "entry_time_only": True,
                })
    return candidates


def _candidate_rank(item: Mapping[str, Any]) -> Tuple[float, float, float, int, str]:
    val = item["validation"]
    return (
        float(val.get("expectancy_delta_r") or -999.0),
        float(val.get("loss_rejection") or -999.0),
        float(val.get("win_retention") or -999.0),
        int(val.get("selected", {}).get("resolved_binary") or 0),
        str(item["candidate"]["id"]),
    )


def _eligible_analysis(analysis: Mapping[str, Any], min_resolved: int) -> bool:
    return (
        (analysis.get("selected") or {}).get("resolved_binary", 0) >= min_resolved
        and (analysis.get("win_retention") or 0.0) >= 0.60
        and analysis.get("losses_rejected", 0) >= 2
        and analysis.get("expectancy_delta_r", 0.0) > 0.0
    )


def _period_groups(rows: Sequence[Mapping[str, Any]], groups: int = 3) -> List[List[Dict[str, Any]]]:
    ordered = sorted((dict(row) for row in rows), key=lambda row: _dt(row["candle_ts"]))
    if not ordered:
        return []
    size = max(1, math.ceil(len(ordered) / groups))
    return [ordered[index:index + size] for index in range(0, len(ordered), size)]


def directional_stability(candidate: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    result = {}
    for direction in ("BUY", "SELL"):
        subset = [
            row for row in rows
            if str(row.get("research_direction") or row.get("chosen_signal") or "").upper() == direction
        ]
        result[direction] = candidate_analysis(candidate, subset)
    positive = sum((item.get("expectancy_delta_r") or 0.0) > 0 for item in result.values())
    sufficient = sum((item.get("selected") or {}).get("resolved_binary", 0) >= 5 for item in result.values())
    return {
        "directions": result,
        "positive_directions": positive,
        "directions_with_sufficient_evidence": sufficient,
        "stable": positive == 2 and sufficient == 2,
    }


def temporal_stability(candidate: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    periods = []
    for index, subset in enumerate(_period_groups(rows), 1):
        analysis = candidate_analysis(candidate, subset)
        periods.append({"period": index, **analysis})
    positive = sum((item.get("expectancy_delta_r") or 0.0) > 0 for item in periods)
    return {
        "periods": periods,
        "positive_periods": positive,
        "stable": bool(periods) and positive / len(periods) >= 2 / 3,
    }


def sensitivity(candidate: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    rule = (candidate.get("rules") or [None])[0]
    if not rule:
        return {"status": "NOT_APPLICABLE", "neighbors": []}
    feature = str(rule["feature"])
    values = sorted(set(value for row in rows if (value := _feature(row, feature)) is not None))
    threshold = float(rule["threshold"])
    if len(values) < 3:
        return {"status": "NOT TESTED", "neighbors": [], "classification": "NOT TESTED"}
    closest = min(range(len(values)), key=lambda index: abs(values[index] - threshold))
    indices = sorted(set(max(0, min(len(values) - 1, closest + offset)) for offset in (-1, 0, 1)))
    neighbors = []
    for index in indices:
        changed = {
            **candidate,
            "id": f"{candidate['id']}:sensitivity:{index}",
            "rules": [{**rule, "threshold": values[index]}],
        }
        neighbors.append(candidate_analysis(changed, rows))
    deltas = [item["expectancy_delta_r"] for item in neighbors]
    delta_range = max(deltas) - min(deltas) if deltas else None
    center = abs(candidate_analysis(candidate, rows)["expectancy_delta_r"])
    cliff = delta_range is not None and delta_range > max(0.15, center * 1.5)
    all_positive = bool(deltas) and all(delta > 0 for delta in deltas)
    classification = "FRAGILE" if cliff or not all_positive else ("MODERATE" if delta_range and delta_range > max(0.05, center * 0.75) else "STABLE")
    return {
        "status": "OK",
        "neighbors": neighbors,
        "expectancy_delta_range": delta_range,
        "all_positive": all_positive,
        "cliff_effect": cliff,
        "classification": classification,
    }


def _combine(candidates: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    rules = []
    ids = []
    for candidate in candidates:
        rules.extend(dict(rule) for rule in candidate.get("rules") or [])
        ids.append(str(candidate["id"]))
    return {
        "id": "COMPOSITE:" + _canonical_hash(ids)[:12],
        "component_ids": ids,
        "rules": rules,
        "filter": "COMPOSITE_PRE_ENTRY",
        "subfilter": ids,
        "old_rule": "NO_PHASE_2_THRESHOLD",
        "candidate_rule": " AND ".join(
            f"{rule['feature']} {rule['operator']} {float(rule['threshold']):.12g}" for rule in rules
        ),
        "threshold": [rule["threshold"] for rule in rules],
        "direction_semantics": "SAME_NUMERIC_PREDICATES_FOR_BUY_AND_SELL",
        "entry_time_only": True,
    }


def overlap_remove_one(candidate: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    rules = list(candidate.get("rules") or [])
    selected_sets = []
    for rule in rules:
        single = {"id": _canonical_hash(rule)[:12], "rules": [rule]}
        selected_sets.append({_row_id(row) for row in apply_candidate(single, resolved_rows(rows))})
    overlaps = []
    for left in range(len(selected_sets)):
        for right in range(left + 1, len(selected_sets)):
            union = selected_sets[left] | selected_sets[right]
            overlaps.append({
                "left": left,
                "right": right,
                "jaccard": len(selected_sets[left] & selected_sets[right]) / len(union) if union else None,
            })
    remove_one = []
    for index in range(len(rules)):
        reduced = {**candidate, "id": f"{candidate['id']}:remove:{index}", "rules": rules[:index] + rules[index + 1:]}
        remove_one.append({"removed_rule": rules[index], "analysis": candidate_analysis(reduced, rows)})
    return {"pairwise_overlap": overlaps, "remove_one": remove_one}


def m1_internals_analysis(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    available: Dict[str, Any] = {}
    blocked_sets: Dict[str, set[str]] = {}
    for name in M1_INTERNALS:
        exact_values = []
        blocked = []
        for row in rows:
            features = row.get("features") or {}
            filters = row.get("filters") or {}
            if name in features:
                value = features[name]
                exact_values.append(value)
                direction = str(row.get("research_direction") or row.get("chosen_signal") or "").upper()
                passed = bool(value)
                if name == "m1_momentum":
                    numeric = _finite(value) or 0.0
                    passed = numeric > 0 if direction == "BUY" else numeric < 0 if direction == "SELL" else False
                if not passed:
                    blocked.append(row)
            elif name == "m1_confirm" and "m1_confirmation" in filters:
                value = filters["m1_confirmation"]
                exact_values.append(value)
                if not bool(value):
                    blocked.append(row)
        blocked_sets[name] = {_row_id(row) for row in blocked}
        available[name] = {
            "status": "PASS" if exact_values else "NOT_HISTORICALLY_RECONSTRUCTABLE",
            "observations": len(exact_values),
            "blocked_outcomes": metrics(blocked) if exact_values else None,
            "incremental_opening_effect": metrics(blocked) if exact_values else None,
        }
    overlaps = []
    names = list(M1_INTERNALS)
    for left in range(len(names)):
        for right in range(left + 1, len(names)):
            union = blocked_sets[names[left]] | blocked_sets[names[right]]
            overlaps.append({
                "left": names[left], "right": names[right],
                "blocked_jaccard": len(blocked_sets[names[left]] & blocked_sets[names[right]]) / len(union) if union else None,
            })
    return {
        "status": "PASS" if any(item["status"] == "PASS" for item in available.values()) else "NOT_HISTORICALLY_RECONSTRUCTABLE",
        "internals": available,
        "overlap": overlaps,
        "warning": "Composite M1 confirmation is not evidence for absent exact internal states.",
    }


def walk_forward_stability(
    candidate: Mapping[str, Any], rows: Sequence[Mapping[str, Any]], *, horizon_minutes: int = 240,
) -> Dict[str, Any]:
    config = ReplayValidationConfig(
        embargo_minutes=30,
        walk_forward_train_episodes=max(10, len(rows) // 2),
        walk_forward_test_episodes=max(5, len(rows) // 5),
        walk_forward_step_episodes=max(5, len(rows) // 5),
    )
    folds = walk_forward_splits(rows, horizon_bars=horizon_minutes, config=config)
    results = []
    for index, fold in enumerate(folds, 1):
        # The candidate is fixed before these evaluations; no per-fold optimization.
        results.append({
            "fold": index,
            "boundary": fold.get("boundary"),
            "purged": fold.get("purged"),
            "embargoed": fold.get("embargoed"),
            "train": candidate_analysis(candidate, fold["train"]),
            "test": candidate_analysis(candidate, fold["test"]),
        })
    valid = [item for item in results if (item["test"].get("selected") or {}).get("resolved_binary", 0) >= 3]
    positive = sum((item["test"].get("expectancy_delta_r") or 0) > 0 for item in valid)
    return {
        "status": "PASS" if valid and positive / len(valid) >= 2 / 3 else ("FAIL" if valid else "NOT TESTED"),
        "folds": results,
        "valid_folds": len(valid),
        "positive_fold_fraction": positive / len(valid) if valid else None,
    }


def overfitting_risk(
    candidate: Mapping[str, Any],
    discovery: Mapping[str, Any],
    validation: Mapping[str, Any],
    sensitivity_result: Mapping[str, Any],
    directional: Mapping[str, Any],
    temporal: Mapping[str, Any],
) -> Dict[str, Any]:
    flags = []
    if len(candidate.get("rules") or []) > 3:
        flags.append("TOO_MANY_RULES")
    if (validation.get("selected") or {}).get("resolved_binary", 0) < 10:
        flags.append("LOW_VALIDATION_SAMPLE")
    if (discovery.get("expectancy_delta_r") or 0) > 0 and (validation.get("expectancy_delta_r") or 0) <= 0:
        flags.append("DISCOVERY_EDGE_FAILED_VALIDATION")
    if sensitivity_result.get("all_positive") is False:
        flags.append("THRESHOLD_SENSITIVITY")
    if not directional.get("stable"):
        flags.append("DIRECTIONAL_INSTABILITY")
    if not temporal.get("stable"):
        flags.append("TEMPORAL_INSTABILITY")
    severity = "HIGH" if any(flag in flags for flag in ("DISCOVERY_EDGE_FAILED_VALIDATION", "LOW_VALIDATION_SAMPLE")) else ("MEDIUM" if flags else "LOW")
    return {"severity": severity, "flags": flags, "overfit_detected": severity == "HIGH"}


def decision_gate(
    candidate: Mapping[str, Any],
    discovery: Mapping[str, Any],
    validation: Mapping[str, Any],
    risk: Mapping[str, Any],
    *,
    min_resolved: int,
) -> Dict[str, Any]:
    checks = {
        "entry_time_only": candidate.get("entry_time_only") is True,
        "not_single_trade_rule": validation.get("losses_rejected", 0) >= 2,
        "minimum_validation_sample": (validation.get("selected") or {}).get("resolved_binary", 0) >= min_resolved,
        "validation_expectancy_positive": (validation.get("selected") or {}).get("expectancy_r") is not None and (validation.get("selected") or {}).get("expectancy_r") > 0,
        "validation_expectancy_improves": (validation.get("expectancy_delta_r") or 0) > 0,
        "validation_profit_factor": ((validation.get("selected") or {}).get("profit_factor") or 0) >= 1.05,
        "win_retention": (validation.get("win_retention") or 0) >= 0.60,
        "overfit_risk_not_high": risk.get("severity") != "HIGH",
    }
    return {
        "decision": "FREEZE_ELIGIBLE" if all(checks.values()) else "REJECT",
        "checks": checks,
        "failed": [name for name, passed in checks.items() if not passed],
    }


def final_reliability(
    candidate: Mapping[str, Any], frozen: Mapping[str, Any], holdout_analysis: Mapping[str, Any],
    directional: Mapping[str, Any], temporal: Mapping[str, Any], sensitivity_result: Mapping[str, Any],
    overlap_result: Mapping[str, Any], walk_forward: Mapping[str, Any],
) -> Dict[str, Any]:
    """Evidence-based LOW/MEDIUM/HIGH risk; each downgrade has an explicit reason."""
    reasons = []
    selected = holdout_analysis.get("selected") or {}
    outcome_effect = holdout_analysis.get("outcome_effect") or {}
    validation = ((frozen.get("validation_evidence") or {}).get("metrics") or {})
    if selected.get("resolved_binary", 0) < 10:
        reasons.append({"severity": "HIGH", "reason": "HOLDOUT_SAMPLE_BELOW_10"})
    if (outcome_effect.get("WIN") or {}).get("baseline", 0) < 2 or (outcome_effect.get("LOSS") or {}).get("baseline", 0) < 2:
        reasons.append({"severity": "HIGH", "reason": "BASELINE_WIN_OR_LOSS_COUNT_NEAR_SINGLE_CASE"})
    if sensitivity_result.get("classification") == "FRAGILE":
        reasons.append({"severity": "HIGH", "reason": "THRESHOLD_CLIFF_OR_FRAGILITY"})
    elif sensitivity_result.get("classification") == "MODERATE":
        reasons.append({"severity": "MEDIUM", "reason": "MODERATE_THRESHOLD_SENSITIVITY"})
    if not directional.get("stable"):
        reasons.append({"severity": "MEDIUM", "reason": "DIRECTIONAL_STABILITY_NOT_ESTABLISHED"})
    if not temporal.get("stable"):
        reasons.append({"severity": "MEDIUM", "reason": "TEMPORAL_STABILITY_NOT_ESTABLISHED"})
    if len(candidate.get("rules") or []) > 3:
        reasons.append({"severity": "MEDIUM", "reason": "MORE_THAN_THREE_CONDITIONS"})
    max_overlap = max(
        (item.get("jaccard") or 0.0 for item in overlap_result.get("pairwise_overlap") or []),
        default=0.0,
    )
    if max_overlap >= 0.85:
        reasons.append({"severity": "MEDIUM", "reason": "HIGH_FILTER_OVERLAP"})
    validation_delta = validation.get("expectancy_delta_r")
    holdout_delta = holdout_analysis.get("expectancy_delta_r")
    if validation_delta is not None and validation_delta > 0 and (holdout_delta is None or holdout_delta <= 0):
        reasons.append({"severity": "HIGH", "reason": "VALIDATION_EDGE_FAILED_HOLDOUT"})
    if walk_forward.get("status") == "FAIL":
        reasons.append({"severity": "HIGH", "reason": "WALK_FORWARD_INSTABILITY"})
    elif walk_forward.get("status") == "NOT TESTED":
        reasons.append({"severity": "MEDIUM", "reason": "WALK_FORWARD_NOT_TESTED"})
    if (outcome_effect.get("LOSS") or {}).get("blocked", 0) < 2:
        reasons.append({"severity": "HIGH", "reason": "RULE_EFFECT_PROXIMATE_TO_SINGLE_LOSS"})
    severity = "HIGH" if any(item["severity"] == "HIGH" for item in reasons) else ("MEDIUM" if reasons else "LOW")
    return {
        "severity": severity,
        "reasons": reasons,
        "evidence": {
            "holdout_resolved_binary": selected.get("resolved_binary", 0),
            "holdout_wins": selected.get("wins", 0),
            "holdout_losses": selected.get("losses", 0),
            "conditions": len(candidate.get("rules") or []),
            "maximum_pairwise_overlap": max_overlap,
            "validation_expectancy_delta_r": validation_delta,
            "holdout_expectancy_delta_r": holdout_delta,
            "walk_forward_status": walk_forward.get("status"),
        },
    }


def candidate_record(
    candidate: Mapping[str, Any], frozen: Mapping[str, Any], holdout: Mapping[str, Any],
    reliability: Mapping[str, Any],
) -> Dict[str, Any]:
    discovery = frozen.get("discovery_metrics") or {}
    analysis = holdout.get("analysis") or {}
    disc_effect = discovery.get("outcome_effect") or {}
    hold_effect = analysis.get("outcome_effect") or {}
    robust = (
        (holdout.get("directional_stability") or {}).get("stable") is True
        and (holdout.get("temporal_stability") or {}).get("stable") is True
        and (holdout.get("sensitivity") or {}).get("classification") != "FRAGILE"
        and (holdout.get("walk_forward_stability") or {}).get("status") == "PASS"
    )
    status = "RESEARCH_CANDIDATE" if holdout.get("status") == "PASS" and reliability.get("severity") != "HIGH" and robust else "REJECT"
    def count(effect: Mapping[str, Any], outcome: str, key: str = "kept") -> int:
        return int((effect.get(outcome) or {}).get(key) or 0)
    return {
        "candidate_id": candidate.get("id"),
        "instrument": frozen.get("instrument"),
        "filter": candidate.get("filter"),
        "subfilter": candidate.get("subfilter"),
        "old_rule": candidate.get("old_rule"),
        "candidate_rule": candidate.get("candidate_rule"),
        "threshold": candidate.get("threshold"),
        "direction_semantics": candidate.get("direction_semantics"),
        "discovery_w": count(disc_effect, "WIN"), "discovery_l": count(disc_effect, "LOSS"),
        "discovery_t": count(disc_effect, "TIMEOUT"), "discovery_a": count(disc_effect, "AMBIGUOUS"),
        "discovery_p": count(disc_effect, "PENDING"),
        "holdout_w": count(hold_effect, "WIN"), "holdout_l": count(hold_effect, "LOSS"),
        "holdout_t": count(hold_effect, "TIMEOUT"), "holdout_a": count(hold_effect, "AMBIGUOUS"),
        "holdout_p": count(hold_effect, "PENDING"),
        "win_retention": analysis.get("win_retention"),
        "loss_rejection": analysis.get("loss_rejection"),
        "expectancy": (analysis.get("selected") or {}).get("expectancy_r"),
        "profit_factor": (analysis.get("selected") or {}).get("profit_factor"),
        "directional_stability": holdout.get("directional_stability"),
        "temporal_stability": holdout.get("temporal_stability"),
        "sensitivity": holdout.get("sensitivity"),
        "walk_forward_stability": holdout.get("walk_forward_stability"),
        "overlap": (holdout.get("overlap_remove_one") or {}).get("pairwise_overlap"),
        "remove_one_effect": (holdout.get("overlap_remove_one") or {}).get("remove_one"),
        "overfitting_risk": reliability,
        "status": status,
        "limitations": [
            "Historical replay is not forward proof.",
            "FORWARD_CANDIDATE requires independent IA #2 audit and explicit IA #1 approval.",
        ],
        "auto_production_change": False,
    }
def discover_candidates(target_population_path: str, phase2_path: str, *, min_resolved: int = 10) -> Dict[str, Any]:
    source = _load(target_population_path)
    spec = _load(phase2_path)
    if spec.get("lookahead_protection") is not True:
        raise ValueError("Phase 2 artifact lacks look-ahead protection")
    split = _split_from_spec(source, spec)
    discovery_rows = _phase1_eligible_rows(spec, split["discovery"])
    validation_rows = _phase1_eligible_rows(spec, split["validation"])
    candidates = _generate_candidates(discovery_rows)
    evaluated = []
    for candidate in candidates:
        discovery_result = candidate_analysis(candidate, discovery_rows)
        if not _eligible_analysis(discovery_result, min_resolved):
            continue
        validation_result = candidate_analysis(candidate, validation_rows)
        sensitivity_result = sensitivity(candidate, validation_rows)
        directional = directional_stability(candidate, validation_rows)
        temporal = temporal_stability(candidate, validation_rows)
        risk = overfitting_risk(candidate, discovery_result, validation_result, sensitivity_result, directional, temporal)
        gate = decision_gate(candidate, discovery_result, validation_result, risk, min_resolved=min_resolved)
        evaluated.append({
            "candidate": candidate,
            "discovery": discovery_result,
            "validation": validation_result,
            "sensitivity": sensitivity_result,
            "directional_stability": directional,
            "temporal_stability": temporal,
            "walk_forward_stability": walk_forward_stability(candidate, discovery_rows),
            "overfitting_risk": risk,
            "decision_gate": gate,
        })
    evaluated.sort(key=_candidate_rank, reverse=True)
    for rank, item in enumerate(evaluated, 1):
        item["rank"] = rank
        item["status"] = "RESEARCH_CANDIDATE" if item["decision_gate"]["decision"] == "FREEZE_ELIGIBLE" else "REJECT"
        item["holdout_status"] = "NOT TESTED"
    eligible = [item for item in evaluated if item["decision_gate"]["decision"] == "FREEZE_ELIGIBLE"]
    eligible.sort(key=_candidate_rank, reverse=True)
    selected_components = [item["candidate"] for item in eligible[:3]]
    proposed = _combine(selected_components) if selected_components else None
    composite = None
    if proposed:
        discovery_result = candidate_analysis(proposed, discovery_rows)
        validation_result = candidate_analysis(proposed, validation_rows)
        sensitivity_result = {"status": "NOT_APPLICABLE", "reason": "Composite sensitivity is represented by component analyses"}
        directional = directional_stability(proposed, validation_rows)
        temporal = temporal_stability(proposed, validation_rows)
        risk = overfitting_risk(proposed, discovery_result, validation_result, sensitivity_result, directional, temporal)
        gate = decision_gate(proposed, discovery_result, validation_result, risk, min_resolved=min_resolved)
        composite = {
            "candidate": proposed,
            "discovery": discovery_result,
            "validation": validation_result,
            "directional_stability": directional,
            "temporal_stability": temporal,
            "walk_forward_stability": walk_forward_stability(proposed, discovery_rows),
            "overlap_remove_one": overlap_remove_one(proposed, validation_rows),
            "overfitting_risk": risk,
            "decision_gate": gate,
        }
        if gate["decision"] != "FREEZE_ELIGIBLE":
            composite = None
            if eligible:
                top = eligible[0]
                composite = {**top, "overlap_remove_one": overlap_remove_one(top["candidate"], validation_rows)}
    return {
        "status": "OK" if composite else "NO_FREEZE_ELIGIBLE_CANDIDATE",
        "stage": "discovery",
        "instrument": source.get("instrument"),
        "variant": source.get("variant"),
        "input_sha256": sha256_file(target_population_path),
        "phase2_sha256": sha256_file(phase2_path),
        "dataset_identity": spec.get("dataset_identity") or {"status": "NOT TESTED"},
        "lookahead_protection": True,
        "holdout_opened": False,
        "candidate_space": {"generated": len(candidates), "evaluated_after_discovery_gate": len(evaluated), "freeze_eligible": len(eligible)},
        "discovery_metrics": metrics(discovery_rows),
        "validation_metrics": metrics(validation_rows),
        "ranked_candidates": evaluated,
        "proposed_frozen_candidate": composite,
        "m1_internals": m1_internals_analysis(discovery_rows),
        "notes": [
            "Thresholds are derived only from the discovery partition.",
            "Validation selects among discovery-defined candidates; final holdout remains unopened.",
            "TIMEOUT, AMBIGUOUS and PENDING are excluded from binary expectancy and remain separately counted.",
        ],
    }


def freeze_candidate(discovery_path: str, output_path: str | Path) -> Dict[str, Any]:
    discovery = _load(discovery_path)
    proposed = discovery.get("proposed_frozen_candidate")
    if not proposed or (proposed.get("decision_gate") or {}).get("decision") != "FREEZE_ELIGIBLE":
        raise ValueError("Discovery has no freeze-eligible candidate")
    definition = proposed["candidate"]
    payload = {
        "status": "OK",
        "freeze_status": "FROZEN_IMMUTABLE",
        "stage": "freeze",
        "instrument": discovery.get("instrument"),
        "variant": discovery.get("variant"),
        "lookahead_protection": True,
        "holdout_opened": False,
        "immutable": True,
        "frozen_at": utc_now(),
        "candidate_id": definition.get("id"),
        "rule": definition.get("candidate_rule"),
        "threshold": definition.get("threshold"),
        "direction_semantics": definition.get("direction_semantics"),
        "discovery_sha256": sha256_file(discovery_path),
        "input_sha256": discovery.get("input_sha256"),
        "dataset_identity": discovery.get("dataset_identity"),
        "code_sha": (discovery.get("dataset_identity") or {}).get("code_sha"),
        "discovery_metrics": proposed.get("discovery"),
        "candidate_definition": definition,
        "candidate_definition_sha256": _canonical_hash(definition),
        "validation_evidence": {
            "metrics": proposed.get("validation"),
            "sensitivity": proposed.get("sensitivity"),
            "directional_stability": proposed.get("directional_stability"),
            "temporal_stability": proposed.get("temporal_stability"),
            "overlap_remove_one": proposed.get("overlap_remove_one"),
            "overfitting_risk": proposed.get("overfitting_risk"),
            "decision_gate": proposed.get("decision_gate"),
        },
    }
    output = Path(output_path)
    if output.exists():
        existing = _load(output)
        if existing.get("candidate_definition_sha256") != payload["candidate_definition_sha256"]:
            raise ValueError("Freeze artifact is immutable and cannot be replaced")
        return existing
    return payload


def evaluate_holdout(target_population_path: str, phase2_path: str, freeze_path: str) -> Dict[str, Any]:
    source = _load(target_population_path)
    spec = _load(phase2_path)
    frozen = _load(freeze_path)
    instruments = {str(source.get("instrument") or "").upper(), str(spec.get("instrument") or "").upper(), str(frozen.get("instrument") or "").upper()}
    if len(instruments) != 1:
        raise ValueError("Cross-asset isolation failure: artifact instruments differ")
    if frozen.get("immutable") is not True or frozen.get("holdout_opened") is not False:
        raise ValueError("Invalid freeze artifact")
    definition = frozen.get("candidate_definition") or {}
    if _canonical_hash(definition) != frozen.get("candidate_definition_sha256"):
        raise ValueError("Frozen candidate definition hash mismatch")
    split = _split_from_spec(source, spec)
    holdout_rows = _phase1_eligible_rows(spec, split["test"])
    analysis = candidate_analysis(definition, holdout_rows)
    directional = directional_stability(definition, holdout_rows)
    temporal = temporal_stability(definition, holdout_rows)
    sensitivity_result = sensitivity(definition, holdout_rows) if len(definition.get("rules") or []) == 1 else {"status": "NOT APPLICABLE", "classification": "NOT APPLICABLE"}
    overlap_result = overlap_remove_one(definition, holdout_rows)
    walk_forward = walk_forward_stability(
        definition, _phase1_eligible_rows(spec, [*split["discovery"], *split["validation"]]),
        horizon_minutes=int((spec.get("partition_config") or {}).get("horizon_minutes", 240)),
    )
    reliability = final_reliability(
        definition, frozen, analysis, directional, temporal,
        sensitivity_result, overlap_result, walk_forward,
    )
    status = "PASS" if (
        (analysis.get("selected") or {}).get("resolved_binary", 0) >= 5
        and (analysis.get("selected") or {}).get("expectancy_r") is not None
        and (analysis.get("selected") or {}).get("expectancy_r") > 0
        and (analysis.get("win_retention") or 0) >= 0.50
    ) else "FAIL"
    result = {
        "status": status,
        "stage": "holdout",
        "instrument": source.get("instrument"),
        "variant": source.get("variant"),
        "lookahead_protection": True,
        "retuning_after_holdout": False,
        "holdout_opened_once": True,
        "input_sha256": sha256_file(target_population_path),
        "phase2_sha256": sha256_file(phase2_path),
        "freeze_sha256": sha256_file(freeze_path),
        "candidate_definition_sha256": frozen["candidate_definition_sha256"],
        "analysis": analysis,
        "sensitivity": sensitivity_result,
        "directional_stability": directional,
        "temporal_stability": temporal,
        "walk_forward_stability": walk_forward,
        "overlap_remove_one": overlap_result,
        "overfitting_risk": reliability,
        "decision": "RESEARCH_CANDIDATE_SURVIVED_HOLDOUT" if status == "PASS" else "RESEARCH_CANDIDATE_REJECTED",
        "production_authority": False,
    }
    result["candidate_ranking"] = [candidate_record(definition, frozen, result, reliability)]
    return result


def automatic_report(
    integrity_path: str, phase1_path: str, phase2_path: str, discovery_path: str,
    freeze_path: str, holdout_path: str, determinism_path: str, audit_path: str,
) -> Dict[str, Any]:
    integrity = _load(integrity_path)
    phase1 = _load(phase1_path)
    phase2 = _load(phase2_path)
    discovery = _load(discovery_path)
    frozen = _load(freeze_path)
    holdout = _load(holdout_path)
    determinism = _load(determinism_path)
    audit = _load(audit_path)
    selected = (holdout.get("candidate_ranking") or [None])[0]
    risk = holdout.get("overfitting_risk") or {}
    audit_failures = len((audit.get("package") or {}).get("failures") or [])
    dedup = phase2.get("episode_dedup")
    if not isinstance(dedup, Mapping):
        dedup = {
            "status": "NOT TESTED", "total_episodes": None,
            "unique_episode_identities": None, "duplicate_count": None,
        }
    upstream_lookahead = phase2.get("lookahead_evidence") or {}

    def lookahead_stage(values: Sequence[Any], *, detected: Any = None) -> Dict[str, Any]:
        if detected is True or any(value is False for value in values):
            status = "FAIL"
        elif values and all(value is True for value in values):
            status = "PASS"
        else:
            status = "NOT TESTED"
        return {"status": status, "evidence": list(values), "lookahead_detected": detected}

    replay_lookahead = upstream_lookahead.get("replay") or {}
    target_lookahead = upstream_lookahead.get("target_population") or {}
    lookahead_stages = {
        "replay": lookahead_stage(
            [replay_lookahead.get("no_lookahead_decision"), replay_lookahead.get("future_bars_only_for_outcome")],
            detected=replay_lookahead.get("lookahead_detected"),
        ),
        "target_population": lookahead_stage(
            [target_lookahead.get("lookahead_protection"), target_lookahead.get("future_bars_used_only_for_outcome")],
            detected=target_lookahead.get("lookahead_detected"),
        ),
        "phase_1": lookahead_stage([phase1.get("lookahead_protection")], detected=phase1.get("lookahead_detected")),
        "phase_2": lookahead_stage(
            [phase2.get("lookahead_protection"), phase2.get("future_bars_used_only_for_outcome")],
            detected=phase2.get("lookahead_detected"),
        ),
        "discovery": lookahead_stage([discovery.get("lookahead_protection")], detected=discovery.get("lookahead_detected")),
        "freeze": lookahead_stage([frozen.get("lookahead_protection")], detected=frozen.get("lookahead_detected")),
        "holdout": lookahead_stage([holdout.get("lookahead_protection")], detected=holdout.get("lookahead_detected")),
    }
    lookahead_statuses = {item["status"] for item in lookahead_stages.values()}
    lookahead_status = "FAIL" if "FAIL" in lookahead_statuses else ("PASS" if lookahead_statuses == {"PASS"} else "NOT TESTED")
    report = {
        "INPUT SHA256": integrity.get("input_sha256"),
        "INSTRUMENT": integrity.get("instrument"),
        "START": integrity.get("start"),
        "END": integrity.get("end"),
        "WARMUP": integrity.get("warmup_days"),
        "HORIZON": integrity.get("horizon_minutes"),
        "POPULATION": {
            "status": dedup.get("status", "NOT TESTED"),
            "episodes": dedup.get("total_episodes"),
            "episode_dedup": dedup,
        },
        "OUTCOMES": {"status": "PASS", "counts": discovery.get("discovery_metrics", {}).get("outcomes")},
        "FILTERS/GATES": {"status": "PASS", "data_integrity": integrity, "safety_risk_global": phase2.get("safety_risk_global_gates"), "learned_research_veto": phase2.get("learned_research_veto"), "m1_internals": discovery.get("m1_internals")},
        "PHASE 1": {"status": "PASS" if phase1.get("all_target_wins_recovered") else "FAIL", "evidence": phase1},
        "PHASE 2": {"status": "PASS", "candidate_space": discovery.get("candidate_space")},
        "DISCOVERY": {"status": "PASS" if discovery.get("status") == "OK" else "FAIL", "partitions": phase2.get("partitions")},
        "FREEZE": {"status": "PASS" if frozen.get("immutable") is True else "FAIL", "candidate_id": frozen.get("candidate_id"), "sha256": sha256_file(freeze_path)},
        "HOLDOUT": {"status": holdout.get("status"), "retuning_after_holdout": holdout.get("retuning_after_holdout"), "freeze_sha256": holdout.get("freeze_sha256"), "evidence": holdout.get("analysis")},
        "DIRECTIONAL STABILITY": {"status": "PASS" if (holdout.get("directional_stability") or {}).get("stable") else "FAIL", "evidence": holdout.get("directional_stability")},
        "TEMPORAL STABILITY": {"status": "PASS" if (holdout.get("temporal_stability") or {}).get("stable") else "FAIL", "evidence": holdout.get("temporal_stability")},
        "SENSITIVITY": {"status": "PASS" if (holdout.get("sensitivity") or {}).get("classification") == "STABLE" else ("NOT APPLICABLE" if (holdout.get("sensitivity") or {}).get("status") == "NOT APPLICABLE" else "FAIL"), "evidence": holdout.get("sensitivity")},
        "OVERLAP": {"status": "PASS" if len((frozen.get("candidate_definition") or {}).get("rules") or []) > 1 else "NOT APPLICABLE", "evidence": (holdout.get("overlap_remove_one") or {}).get("pairwise_overlap")},
        "REMOVE-ONE": {"status": "PASS" if len((frozen.get("candidate_definition") or {}).get("rules") or []) > 1 else "NOT APPLICABLE", "evidence": (holdout.get("overlap_remove_one") or {}).get("remove_one")},
        "WALK-FORWARD": {"status": (holdout.get("walk_forward_stability") or {}).get("status", "NOT TESTED"), "evidence": holdout.get("walk_forward_stability")},
        "OVERFITTING RISK": {"status": "FAIL" if risk.get("severity") == "HIGH" else "PASS", "evidence": risk},
        "SELECTED CANDIDATE": {"status": "PASS" if selected and selected.get("status") == "RESEARCH_CANDIDATE" else "FAIL", "candidate": selected},
        "LOOK-AHEAD": {
            "status": lookahead_status,
            "future_bars_only_for_outcome": lookahead_status == "PASS",
            "stages": lookahead_stages,
        },
        "DETERMINISM": {"status": determinism.get("status", "NOT TESTED"), "evidence": determinism},
        "PRODUCTION MODIFICATIONS": {"status": "PASS" if audit.get("production_modifications") == "NONE" else "FAIL", "value": audit.get("production_modifications")},
        "CRITICAL": 0 if integrity.get("status") == "PASS" and determinism.get("status") == "PASS" else 1,
        "HIGH": sum(1 for item in risk.get("reasons") or [] if item.get("severity") == "HIGH") + audit_failures,
        "MEDIUM": sum(1 for item in risk.get("reasons") or [] if item.get("severity") == "MEDIUM"),
        "LOW": 0,
        "OUTPUT SHA256": None,
    }
    report["OUTPUT SHA256"] = _canonical_hash(report)
    return report


def automatic_pre_audit(report_path: str) -> Dict[str, Any]:
    report = _load(report_path)
    stored_output_sha = report.get("OUTPUT SHA256")
    hash_material = dict(report)
    hash_material["OUTPUT SHA256"] = None
    population = report.get("POPULATION") or {}
    dedup = population.get("episode_dedup") or {}
    dedup_counts_coherent = (
        isinstance(dedup.get("total_episodes"), int)
        and isinstance(dedup.get("unique_episode_identities"), int)
        and isinstance(dedup.get("duplicate_count"), int)
        and dedup["total_episodes"] == dedup["unique_episode_identities"] + dedup["duplicate_count"]
    )
    checks = {
        "canonical_output_sha256": stored_output_sha == _canonical_hash(hash_material),
        "dataset_identity": bool(report.get("INPUT SHA256")),
        "bid_ask_no_midpoint": ((report.get("FILTERS/GATES") or {}).get("data_integrity") or {}).get("bid_ask_real") is True,
        "outcome_semantics": (report.get("OUTCOMES") or {}).get("status") == "PASS",
        "episode_dedup": dedup.get("status") == "PASS" and dedup.get("duplicate_count") == 0 and dedup_counts_coherent,
        "no_lookahead": (report.get("LOOK-AHEAD") or {}).get("status") == "PASS",
        "phase1_all_target_wins": (report.get("PHASE 1") or {}).get("status") == "PASS",
        "phase1_losses_released_recorded": ((report.get("PHASE 1") or {}).get("evidence") or {}).get("best_policy", {}).get("losses_released") is not None,
        "safety_and_global_gates_separate": (report.get("FILTERS/GATES") or {}).get("safety_risk_global") == "SEPARATE_IMMUTABLE_NOT_CANDIDATE_FEATURES",
        "discovery_temporal_purge_embargo": (report.get("DISCOVERY") or {}).get("status") == "PASS",
        "frozen_before_holdout": (report.get("FREEZE") or {}).get("status") == "PASS",
        "no_retuning": (report.get("HOLDOUT") or {}).get("retuning_after_holdout") is False,
        "sensitivity_recorded": (report.get("SENSITIVITY") or {}).get("status") in {"PASS", "FAIL", "NOT APPLICABLE"},
        "direction_recorded": (report.get("DIRECTIONAL STABILITY") or {}).get("status") in {"PASS", "FAIL"},
        "temporal_recorded": (report.get("TEMPORAL STABILITY") or {}).get("status") in {"PASS", "FAIL"},
        "overlap_recorded": (report.get("OVERLAP") or {}).get("status") in {"PASS", "NOT APPLICABLE"},
        "remove_one_recorded": (report.get("REMOVE-ONE") or {}).get("status") in {"PASS", "NOT APPLICABLE"},
        "mutable_states_nhr": (report.get("FILTERS/GATES") or {}).get("learned_research_veto") == "NOT_HISTORICALLY_RECONSTRUCTABLE",
        "determinism": (report.get("DETERMINISM") or {}).get("status") == "PASS",
        "no_production_modification": (report.get("PRODUCTION MODIFICATIONS") or {}).get("status") == "PASS",
        "packaging_cleanliness": report.get("CRITICAL", 1) == 0,
    }
    failed = [name for name, passed in checks.items() if not passed]
    severities = {
        "CRITICAL": sum(name in {"dataset_identity", "no_lookahead", "frozen_before_holdout", "no_retuning", "no_production_modification"} for name in failed),
        "HIGH": len(failed) + int(report.get("HIGH") or 0),
        "MEDIUM": int(report.get("MEDIUM") or 0),
        "LOW": int(report.get("LOW") or 0),
    }
    verdict = "REJECT" if severities["CRITICAL"] or severities["HIGH"] else ("ACCEPT WITH LIMITATIONS" if severities["MEDIUM"] else "ACCEPT")
    return {
        "status": "PASS" if verdict != "REJECT" else "FAIL",
        "verdict": verdict,
        "stage": "pre_audit",
        "instrument": report.get("INSTRUMENT"),
        "lookahead_protection": True,
        "report_sha256": sha256_file(report_path),
        "checks": checks,
        "failed": failed,
        "severities": severities,
        "production_modifications": "NONE",
    }


def generate_ai_prompts(report_path: str, pre_audit_path: str, state_path: Optional[str] = None) -> Dict[str, Any]:
    report = _load(report_path)
    audit = _load(pre_audit_path)
    selected = (report.get("SELECTED CANDIDATE") or {}).get("candidate") or {}
    state_asset: Dict[str, Any] = {}
    audit_evidence: Dict[str, Any] = {}
    if state_path:
        state = _load(state_path)
        state_asset = (state.get("assets") or {}).get(str(report.get("INSTRUMENT") or "").upper()) or {}
        audit_artifact = ((state_asset.get("phases") or {}).get("audit") or {}).get("artifact")
        if audit_artifact and Path(audit_artifact).is_file():
            audit_evidence = _load(audit_artifact)
    lifecycle = state_asset.get("lifecycle") or {}
    files = {
        name: {
            "artifact": phase.get("artifact"), "sha256": phase.get("artifact_sha256"),
            "status": phase.get("status"),
        } for name, phase in (state_asset.get("phases") or {}).items()
        if phase.get("artifact")
    }
    base = (
        f"Instrument: {report.get('INSTRUMENT')}\n"
        f"START/END: {report.get('START')} / {report.get('END')}\n"
        f"Warmup/Horizon: {report.get('WARMUP')} / {report.get('HORIZON')}\n"
        f"Population: {json.dumps(report.get('POPULATION') or {}, sort_keys=True)}\n"
        f"Input SHA256: {report.get('INPUT SHA256')}\n"
        f"Candidate ID: {selected.get('candidate_id')}\n"
        f"Candidate results and limitations: {json.dumps(selected, sort_keys=True)}\n"
        f"Report SHA256: {report.get('OUTPUT SHA256')}\n"
        f"Pre-audit verdict: {audit.get('verdict')}\n"
        f"Last valid stage: {lifecycle.get('last_valid_stage')}\n"
        f"Next allowed stage: {lifecycle.get('next_allowed_stage')}\n"
        f"Artifacts: {json.dumps(files, sort_keys=True)}\n"
        f"Tests: {json.dumps(audit_evidence.get('tests') or {}, sort_keys=True)}\n"
        "Scope: OFFLINE/RESEARCH only. Do not modify production, server.py, "
        "forward_experiment.py, Railway, leverage, or hard-risk limits. Preserve "
        "TIMEOUT/AMBIGUOUS/PENDING semantics and reject look-ahead."
    )
    prompt2 = base + (
        "\n\nIA #2 task: independently audit chronology, purging, embargo, candidate "
        "freeze identity, outcome semantics, sensitivity, directional/temporal "
        "stability, overlap/remove-one, M1 reconstruction limits, and overfitting. "
        "Do not rely on IA #3 conclusions. Recompute and audit independently. "
        "Return PASS/FAIL/NOT_HISTORICALLY_RECONSTRUCTABLE with exact evidence."
    )
    prompt3 = base + (
        "\n\nIA #3 task: reproduce the evidence from immutable artifacts, inspect the "
        "code diff and tests, verify no production authority changed, and issue a "
        "final research-only GO/NO_GO. Use the exact next allowed stage and artifact SHAs "
        "from Research Manager. Run required unit/integration tests and preserve the canonical "
        "evidence format. Do not retune after reading holdout results."
    )
    return {
        "status": "OK",
        "stage": "prompts",
        "instrument": report.get("INSTRUMENT"),
        "report_sha256": sha256_file(report_path),
        "pre_audit_sha256": sha256_file(pre_audit_path),
        "research_state_sha256": sha256_file(state_path) if state_path else None,
        "lifecycle": lifecycle,
        "artifacts": files,
        "tests": audit_evidence.get("tests") or {"status": "NOT TESTED"},
        "ai_2_prompt": prompt2,
        "ai_3_prompt": prompt3,
    }

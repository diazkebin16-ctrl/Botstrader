"""Structured discovery pre-gate diagnostics for Automation V3.

This module does not change candidate generation or research thresholds. It
replays the existing discovery pre-gate analysis for every generated candidate
so outcomes rejected before ``evaluated`` remain auditable.
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

MIN_RESOLVED_DEFAULT = 10
MIN_WIN_RETENTION = 0.60
MIN_LOSSES_REJECTED = 2
MIN_EXPECTANCY_DELTA = 0.0


def _result(candidate: Mapping[str, Any], analysis: Mapping[str, Any], *, min_resolved: int) -> dict[str, Any]:
    selected = analysis.get("selected") or {}
    resolved = int(selected.get("resolved_binary") or 0)
    win_retention = float(analysis.get("win_retention") or 0.0)
    losses_rejected = int(analysis.get("losses_rejected") or 0)
    expectancy_delta = float(analysis.get("expectancy_delta_r") or 0.0)
    checks = {
        "resolved_gte_min": resolved >= int(min_resolved),
        "win_retention_gte_060": win_retention >= MIN_WIN_RETENTION,
        "losses_rejected_gte_2": losses_rejected >= MIN_LOSSES_REJECTED,
        "expectancy_delta_gt_0": expectancy_delta > MIN_EXPECTANCY_DELTA,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "candidate_id": candidate.get("id"),
        "resolved_binary": resolved,
        "win_retention": win_retention,
        "losses_rejected": losses_rejected,
        "expectancy_delta_r": expectancy_delta,
        "checks": checks,
        "failed_checks": failed,
        "pass_all_pre_gate": all(checks.values()),
    }


def summarize_pre_gate_results(
    candidate_results: Sequence[Mapping[str, Any]],
    *,
    min_resolved: int = MIN_RESOLVED_DEFAULT,
    discovery_rows: int | None = None,
) -> dict[str, Any]:
    results = [dict(item) for item in candidate_results]
    total = len(results)
    fail_resolved = sum(not bool((item.get("checks") or {}).get("resolved_gte_min")) for item in results)
    fail_win = sum(not bool((item.get("checks") or {}).get("win_retention_gte_060")) for item in results)
    fail_loss = sum(not bool((item.get("checks") or {}).get("losses_rejected_gte_2")) for item in results)
    fail_expectancy = sum(not bool((item.get("checks") or {}).get("expectancy_delta_gt_0")) for item in results)
    pass_resolved = total - fail_resolved
    pass_win = total - fail_win
    pass_loss = total - fail_loss
    pass_expectancy = total - fail_expectancy
    pass_all = sum(bool(item.get("pass_all_pre_gate")) for item in results)
    max_resolved = max((int(item.get("resolved_binary") or 0) for item in results), default=0)

    combinations: Counter[str] = Counter()
    for item in results:
        failed = tuple(str(name) for name in item.get("failed_checks") or [])
        combinations["+".join(failed) if failed else "PASS_ALL"] += 1

    counts = {
        "INSUFFICIENT_SUPPORT": fail_resolved,
        "LOW_WIN_RETENTION": fail_win,
        "INSUFFICIENT_LOSS_REJECTION": fail_loss,
        "NO_POSITIVE_EXPECTANCY": fail_expectancy,
    }
    if pass_all:
        dominant = "PRE_GATE_PASS_EXISTS"
        action = "CONTINUE_EXISTING_EVALUATED_PATH"
    elif not total:
        dominant = "OTHER_METHODOLOGY_FAILURE"
        action = "STOP"
    else:
        highest = max(counts.values())
        leaders = sorted(name for name, count in counts.items() if count == highest and count > 0)
        if len(leaders) == 1:
            dominant = leaders[0]
        elif leaders:
            dominant = "MULTIPLE_PRE_GATE_FAILURES"
        else:
            dominant = "OTHER_METHODOLOGY_FAILURE"
        action = "EXPAND_LOOKBACK" if dominant == "INSUFFICIENT_SUPPORT" else (
            "NO_VALID_CANDIDATE" if dominant in {
                "LOW_WIN_RETENTION", "INSUFFICIENT_LOSS_REJECTION",
                "NO_POSITIVE_EXPECTANCY", "MULTIPLE_PRE_GATE_FAILURES",
            } else "STOP"
        )

    return {
        "available": True,
        "generated_candidates": total,
        "discovery_rows": int(discovery_rows or 0),
        "max_resolved": max_resolved,
        "min_resolved_required": int(min_resolved),
        "fail_resolved_lt_min": fail_resolved,
        "fail_win_retention_lt_060": fail_win,
        "fail_losses_rejected_lt_2": fail_loss,
        "fail_expectancy_delta_le_0": fail_expectancy,
        "pass_resolved": pass_resolved,
        "pass_win_retention": pass_win,
        "pass_loss_rejection": pass_loss,
        "pass_expectancy": pass_expectancy,
        "pass_all_pre_gate": pass_all,
        "failure_combinations": dict(sorted(combinations.items())),
        "dominant_failure": dominant,
        "recommended_action": action,
        "candidate_results": results,
        "thresholds": {
            "min_resolved": int(min_resolved),
            "min_win_retention": MIN_WIN_RETENTION,
            "min_losses_rejected": MIN_LOSSES_REJECTED,
            "min_expectancy_delta_exclusive": MIN_EXPECTANCY_DELTA,
        },
        "production_authority": False,
    }


def build_pre_gate_diagnostic(
    target_population_path: str | Path,
    phase2_path: str | Path,
    *,
    min_resolved: int = MIN_RESOLVED_DEFAULT,
) -> dict[str, Any]:
    """Recompute pre-gate evidence using the canonical Phase 2 functions."""
    from research_phase2 import (
        _generate_candidates,
        _load,
        _phase1_eligible_rows,
        _split_from_spec,
        candidate_analysis,
    )

    try:
        source = _load(target_population_path)
        spec = _load(phase2_path)
    except FileNotFoundError:
        return {
            "available": False,
            "reason": "PRE_GATE_SOURCE_ARTIFACT_UNAVAILABLE",
            "min_resolved_required": int(min_resolved),
            "production_authority": False,
        }
    split = _split_from_spec(source, spec)
    discovery_rows = _phase1_eligible_rows(spec, split["discovery"])
    candidates = _generate_candidates(discovery_rows)
    results = [
        _result(candidate, candidate_analysis(candidate, discovery_rows), min_resolved=min_resolved)
        for candidate in candidates
    ]
    return summarize_pre_gate_results(results, min_resolved=min_resolved, discovery_rows=len(discovery_rows))


def classify_discovery_outcome(
    discovery: Mapping[str, Any],
    pre_gate: Mapping[str, Any],
    fallback: Mapping[str, Any],
) -> dict[str, Any]:
    """Prefer complete pre-gate evidence only when no candidate passed it."""
    diagnostic = dict(fallback)
    diagnostic["pre_gate_diagnostic"] = dict(pre_gate)
    if pre_gate.get("available") is not True:
        diagnostic["production_authority"] = False
        return diagnostic
    diagnostic["generated_candidates"] = int(pre_gate.get("generated_candidates") or diagnostic.get("generated_candidates") or 0)
    diagnostic["discovery_rows"] = int(pre_gate.get("discovery_rows") or diagnostic.get("discovery_rows") or 0)
    diagnostic["max_resolved"] = int(pre_gate.get("max_resolved") or diagnostic.get("max_resolved") or 0)
    diagnostic["min_resolved_required"] = int(pre_gate.get("min_resolved_required") or diagnostic.get("min_resolved_required") or MIN_RESOLVED_DEFAULT)
    diagnostic["fail_support_count"] = int(pre_gate.get("fail_resolved_lt_min") or 0)
    diagnostic["fail_win_retention_count"] = int(pre_gate.get("fail_win_retention_lt_060") or 0)
    diagnostic["fail_loss_rejection_count"] = int(pre_gate.get("fail_losses_rejected_lt_2") or 0)
    diagnostic["fail_expectancy_count"] = int(pre_gate.get("fail_expectancy_delta_le_0") or 0)
    if int(pre_gate.get("pass_all_pre_gate") or 0) == 0:
        diagnostic["dominant_failure"] = str(pre_gate.get("dominant_failure") or "OTHER_METHODOLOGY_FAILURE")
        diagnostic["recommended_action"] = str(pre_gate.get("recommended_action") or "STOP")
    diagnostic["production_authority"] = False
    return diagnostic

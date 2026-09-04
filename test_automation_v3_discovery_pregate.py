from __future__ import annotations

import json
from pathlib import Path

from automation_v3_discovery_pregate import (
    MIN_EXPECTANCY_DELTA,
    MIN_LOSSES_REJECTED,
    MIN_RESOLVED_DEFAULT,
    MIN_WIN_RETENTION,
    _result,
    classify_discovery_outcome,
    summarize_pre_gate_results,
)
from automation_v3_remote_worker import _snapshot
from autonomous_asset_optimizer import V3Ledger
from research_phase2 import NUMERIC_FEATURES, _generate_candidates, candidate_analysis


def _record(*failed: str, candidate_id: str = "c") -> dict:
    failed_set = set(failed)
    checks = {
        "resolved_gte_min": "resolved" not in failed_set,
        "win_retention_gte_060": "win" not in failed_set,
        "losses_rejected_gte_2": "loss" not in failed_set,
        "expectancy_delta_gt_0": "expectancy" not in failed_set,
    }
    names = [name for name, passed in checks.items() if not passed]
    return {
        "candidate_id": candidate_id,
        "resolved_binary": 9 if "resolved" in failed_set else 10,
        "win_retention": 0.5 if "win" in failed_set else 0.75,
        "losses_rejected": 1 if "loss" in failed_set else 2,
        "expectancy_delta_r": 0.0 if "expectancy" in failed_set else 0.1,
        "checks": checks,
        "failed_checks": names,
        "pass_all_pre_gate": not names,
    }


def _summary(records):
    return summarize_pre_gate_results(records, min_resolved=10, discovery_rows=16)


def test_all_candidates_fail_resolved_is_insufficient_support():
    out = _summary([_record("resolved", candidate_id=str(i)) for i in range(12)])
    assert out["dominant_failure"] == "INSUFFICIENT_SUPPORT"
    assert out["recommended_action"] == "EXPAND_LOOKBACK"


def test_enough_resolved_but_win_retention_fails():
    out = _summary([_record("win", candidate_id=str(i)) for i in range(12)])
    assert out["dominant_failure"] == "LOW_WIN_RETENTION"
    assert out["recommended_action"] == "NO_VALID_CANDIDATE"


def test_enough_resolved_but_loss_rejection_fails():
    out = _summary([_record("loss", candidate_id=str(i)) for i in range(12)])
    assert out["dominant_failure"] == "INSUFFICIENT_LOSS_REJECTION"
    assert out["recommended_action"] == "NO_VALID_CANDIDATE"


def test_enough_resolved_but_expectancy_fails():
    out = _summary([_record("expectancy", candidate_id=str(i)) for i in range(12)])
    assert out["dominant_failure"] == "NO_POSITIVE_EXPECTANCY"
    assert out["recommended_action"] == "NO_VALID_CANDIDATE"


def test_mixed_failures_have_deterministic_classification():
    records = [_record("win", candidate_id=f"w{i}") for i in range(5)]
    records += [_record("loss", candidate_id=f"l{i}") for i in range(5)]
    out = _summary(records)
    assert out["dominant_failure"] == "MULTIPLE_PRE_GATE_FAILURES"
    assert out["recommended_action"] == "NO_VALID_CANDIDATE"


def test_pass_all_pre_gate_uses_existing_evaluated_path():
    pre = _summary([_record(candidate_id="pass"), _record("expectancy", candidate_id="fail")])
    fallback = {"dominant_failure": "INSTABILITY", "recommended_action": "NO_VALID_CANDIDATE", "production_authority": False}
    out = classify_discovery_outcome({}, pre, fallback)
    assert pre["pass_all_pre_gate"] == 1
    assert out["dominant_failure"] == "INSTABILITY"
    assert out["recommended_action"] == "NO_VALID_CANDIDATE"


def test_diagnostic_does_not_change_candidate_generation():
    rows = []
    for index in range(8):
        features = {name: float(index + feature_index / 100) for feature_index, name in enumerate(NUMERIC_FEATURES)}
        rows.append({"features": features, "rr_raw": float(index), "outcome_status": "WIN" if index % 2 == 0 else "LOSS", "realized_r": 1.0 if index % 2 == 0 else -1.0})
    before = json.dumps(_generate_candidates(rows), sort_keys=True)
    candidates = _generate_candidates(rows)
    results = [_result(candidate, candidate_analysis(candidate, rows), min_resolved=10) for candidate in candidates]
    summarize_pre_gate_results(results, min_resolved=10, discovery_rows=len(rows))
    after = json.dumps(_generate_candidates(rows), sort_keys=True)
    assert before == after


def test_thresholds_are_unchanged():
    assert MIN_RESOLVED_DEFAULT == 10
    assert MIN_WIN_RETENTION == 0.60
    assert MIN_LOSSES_REJECTED == 2
    assert MIN_EXPECTANCY_DELTA == 0.0


def test_real_run_33875270055_pregate_distribution():
    combinations = [
        (35, ("resolved", "win", "expectancy")),
        (29, ("resolved", "win")),
        (27, ("resolved",)),
        (11, ("resolved", "loss", "expectancy")),
        (9, ("resolved", "win", "loss", "expectancy")),
        (4, ("resolved", "expectancy")),
        (3, ("loss", "expectancy")),
        (1, ("resolved", "loss")),
        (1, ("loss",)),
    ]
    records = []
    for count, failures in combinations:
        for index in range(count):
            records.append(_record(*failures, candidate_id=f"{failures}-{index}"))
    out = _summary(records)
    assert out["generated_candidates"] == 120
    assert out["fail_resolved_lt_min"] == 116
    assert out["fail_win_retention_lt_060"] == 73
    assert out["fail_losses_rejected_lt_2"] == 25
    assert out["fail_expectancy_delta_le_0"] == 62
    assert out["pass_resolved"] == 4
    assert out["pass_all_pre_gate"] == 0
    assert out["dominant_failure"] == "INSUFFICIENT_SUPPORT"
    assert out["recommended_action"] == "EXPAND_LOOKBACK"


def test_production_authority_false_preserved():
    out = _summary([_record("resolved")])
    assert out["production_authority"] is False


def test_remote_status_exposes_pregate_diagnostic(tmp_path: Path, monkeypatch):
    root = tmp_path / "root"
    work = root / "AUD_USD" / "autonomous_v3"
    work.mkdir(parents=True)
    ledger = V3Ledger(work / "automation_v3_state.json")
    pre = _summary([_record("resolved", candidate_id="one")])
    ledger.mutate("AUD_USD", code_sha="abc", status="RUNNING", pre_gate_diagnostic=pre)
    monkeypatch.delenv("GITHUB_SHA", raising=False)
    monkeypatch.delenv("GITHUB_WORKFLOW", raising=False)
    snap = _snapshot(root, "AUD_USD", "33875270055")
    assert snap["pre_gate_diagnostic"]["dominant_failure"] == "INSUFFICIENT_SUPPORT"
    assert snap["production_authority"] is False

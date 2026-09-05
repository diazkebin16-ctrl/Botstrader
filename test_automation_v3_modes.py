import json
from pathlib import Path

import pytest

from automation_v3_candidate_mapping import CandidateNotDeployable
from automation_v3_modes import (
    FULL_AUTO_TO_PAPER,
    KEEP_INCUMBENT,
    load_json,
    ReviewBeforeHoldoutOptimizer,
    V3Ledger,
    REVIEW_BEFORE_HOLDOUT_DEPLOY,
    SELECT_REVIEW_CANDIDATE,
    _bind_selection,
    build_review_shortlist,
    canonical_sha256,
    parse_natural_language_intent,
    resolve_review_candidate,
    validate_structured_request,
    verify_review_shortlist,
    write_json,
)


INCUMBENT = {
    "resolved_binary": 46,
    "wins": 16,
    "losses": 30,
    "win_rate": 0.3478,
    "expectancy_r": -0.25541,
    "profit_factor": 0.6514,
}


def _candidate(
    cid,
    exp_delta,
    pf_delta,
    *,
    expectancy=None,
    eligible=True,
    directional=True,
    temporal=True,
    risk="LOW",
    beats=None,
    material=None,
):
    if expectancy is None:
        expectancy = INCUMBENT["expectancy_r"] + exp_delta
    definition = {
        "id": cid,
        "rules": [{"feature": "room_to_barrier_r", "operator": "<=", "threshold": 0.55}],
        "candidate_rule": "room_to_barrier_r <= 0.55",
        "entry_time_only": True,
    }
    challenger = {
        "expectancy_r": expectancy,
        "profit_factor": INCUMBENT["profit_factor"] + pf_delta,
        "win_rate": 0.39,
        "resolved_binary": 100,
        "wins": 39,
        "losses": 61,
    }
    if beats is None:
        beats = exp_delta > 0 and pf_delta > 0
    if material is None:
        material = beats
    failed = []
    if not directional:
        failed.append("directional_stability")
    if not temporal:
        failed.append("temporal_stability")
    if risk == "HIGH":
        failed.append("overfit_risk_not_high")
    gate = {
        "decision": "FREEZE_ELIGIBLE" if eligible else "REJECT",
        "diagnostic_state": "CHALLENGER_DEPLOYABLE" if eligible else (
            "CHALLENGER_BETTER_BUT_NOT_ROBUST" if beats else "NO_MEANINGFUL_IMPROVEMENT"
        ),
        "paper_candidate_classification": "RELATIVE_IMPROVEMENT_PAPER_CANDIDATE" if eligible else None,
        "failed": failed,
    }
    return {
        "candidate": definition,
        "validation": {
            "selected": challenger,
            "win_retention": 0.80,
            "loss_rejection": 0.20,
            "losses_rejected": 20,
        },
        "incumbent_comparison": {
            "validation": {
                "incumbent": dict(INCUMBENT),
                "challenger": challenger,
                "expectancy_delta_vs_incumbent": exp_delta,
                "profit_factor_delta_vs_incumbent": pf_delta,
                "win_rate_delta_vs_incumbent": challenger["win_rate"] - INCUMBENT["win_rate"],
                "challenger_beats_incumbent": beats,
                "material_improvement": material,
            }
        },
        "directional_stability": {"stable": directional},
        "temporal_stability": {"stable": temporal},
        "sensitivity": {"classification": "STABLE", "all_positive": True},
        "walk_forward_stability": {"status": "PASS"},
        "overfitting_risk": {
            "severity": risk,
            "flags": ["DISCOVERY_EDGE_FAILED_VALIDATION"] if risk == "HIGH" else [],
        },
        "decision_gate": gate,
    }


def _workspace(tmp_path: Path, candidates, *, instrument="GBP_USD", proposed=None):
    sha = "a" * 40
    dataset = {"code_sha": sha, "data_sha256": "d" * 64}
    write_json(tmp_path / "03_target_population.json", {"instrument": instrument, "dataset_identity": dataset})
    write_json(tmp_path / "04_phase_1.json", {"stage": "phase_1"})
    write_json(
        tmp_path / "05_phase_2.json",
        {
            "instrument": instrument,
            "dataset_identity": dataset,
            "selection_protocol": "DISCOVERY_DEFINE__VALIDATION_SELECT__FREEZE__HOLDOUT_ONCE",
            "partition_config": {"horizon_minutes": 240, "embargo_minutes": 30},
            "lookahead_protection": True,
        },
    )
    write_json(
        tmp_path / "06_discovery.json",
        {
            "instrument": instrument,
            "dataset_identity": dataset,
            "holdout_opened": False,
            "incumbent": {
                "definition": {"methodology_identity": "m" * 64},
                "incumbent_definition_sha256": "i" * 64,
                "validation": {"metrics": dict(INCUMBENT)},
            },
            "ranked_candidates": list(candidates),
            "proposed_frozen_candidate": proposed,
            "production_authority": False,
        },
    )
    write_json(tmp_path / "08_determinism.json", {"status": "PASS"})
    return sha


def test_natural_language_mode_mapping_includes_show_me_first():
    assert parse_natural_language_intent("Automatiza GBP/USD, muéstrame primero") == {
        "instrument": "GBP_USD",
        "mode": REVIEW_BEFORE_HOLDOUT_DEPLOY,
    }
    assert parse_natural_language_intent("Automatiza EUR/USD") == {
        "instrument": "EUR_USD",
        "mode": FULL_AUTO_TO_PAPER,
    }
    assert parse_natural_language_intent("Despliega la 2") == {
        "instrument": None,
        "mode": SELECT_REVIEW_CANDIDATE,
        "rank": 2,
    }
    assert parse_natural_language_intent("Quédate con la actual") == {
        "instrument": None,
        "mode": KEEP_INCUMBENT,
    }


def test_structured_selection_is_strictly_bound():
    payload = validate_structured_request(
        {"instrument": "GBP/USD", "mode": SELECT_REVIEW_CANDIDATE, "shortlist_sha256": "a" * 64, "rank": 2}
    )
    assert payload == {
        "instrument": "GBP_USD",
        "mode": SELECT_REVIEW_CANDIDATE,
        "shortlist_sha256": "a" * 64,
        "rank": 2,
    }
    with pytest.raises(ValueError):
        validate_structured_request({"instrument": "GBP_USD", "mode": SELECT_REVIEW_CANDIDATE, "rank": 1})
    with pytest.raises(ValueError):
        validate_structured_request(
            {"instrument": "GBP_USD", "mode": SELECT_REVIEW_CANDIDATE, "shortlist_sha256": "a" * 64, "rank": 5}
        )


def test_120_evaluated_zero_deployable_returns_top3_diagnostic_candidates(tmp_path):
    candidates = [_candidate(f"c{i:03d}", 0.20 - i * 0.001, 0.15 - i * 0.001, eligible=False) for i in range(120)]
    _workspace(tmp_path, candidates)
    _, shortlist = build_review_shortlist(tmp_path, run_id="33937616832")
    assert len(shortlist["diagnostic_top_candidates"]) == 3
    assert shortlist["deployable_candidates"] == []
    assert all(item["deployment_eligible"] is False for item in shortlist["diagnostic_top_candidates"])


def test_better_but_directionally_unstable_candidate_is_shown(tmp_path):
    candidates = [
        _candidate("unstable", 0.20, 0.10, eligible=False, directional=False),
        _candidate("second", 0.10, 0.05, eligible=False),
        _candidate("third", 0.05, 0.02, eligible=False),
    ]
    _workspace(tmp_path, candidates)
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    first = shortlist["diagnostic_top_candidates"][0]
    assert first["candidate_id"] == "unstable"
    assert first["status"] == "BETTER_THAN_INCUMBENT_NOT_ROBUST"
    assert first["deployment_eligible"] is False
    assert "directional_stability" in first["reason"]


def test_high_overfitting_candidate_is_shown_but_not_selectable(tmp_path):
    sha = _workspace(
        tmp_path,
        [
            _candidate("overfit", 0.30, 0.20, eligible=False, risk="HIGH"),
            _candidate("b", 0.10, 0.05, eligible=False),
            _candidate("c", 0.05, 0.02, eligible=False),
        ],
    )
    path, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["diagnostic_top_candidates"][0]["status"] == "HIGH_OVERFITTING_RISK"
    with pytest.raises(CandidateNotDeployable, match="CANDIDATE_NOT_DEPLOYABLE"):
        resolve_review_candidate(path, current_code_sha=sha, rank=1)


def test_worse_than_incumbent_candidate_can_be_shown_in_top3(tmp_path):
    candidates = [
        _candidate("a", 0.20, 0.10, eligible=False),
        _candidate("b", 0.10, 0.05, eligible=False),
        _candidate("worse", -0.10, -0.05, eligible=False, beats=False),
    ]
    _workspace(tmp_path, candidates)
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["diagnostic_top_candidates"][2]["status"] == "WORSE_THAN_INCUMBENT"


def test_no_meaningful_improvement_status_is_visible(tmp_path):
    candidates = [
        _candidate("small", 0.01, 0.00, eligible=False, beats=False, material=False),
        _candidate("worse1", -0.01, -0.01, eligible=False, beats=False),
        _candidate("worse2", -0.02, -0.02, eligible=False, beats=False),
    ]
    _workspace(tmp_path, candidates)
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["diagnostic_top_candidates"][0]["status"] == "NO_MEANINGFUL_IMPROVEMENT"


def test_current_incumbent_metrics_are_always_included(tmp_path):
    _workspace(tmp_path, [_candidate("a", 0.10, 0.05, eligible=False)])
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["incumbent_metrics"] == INCUMBENT
    first = shortlist["diagnostic_top_candidates"][0]
    assert first["incumbent_expectancy_R"] == INCUMBENT["expectancy_r"]
    assert first["incumbent_profit_factor"] == INCUMBENT["profit_factor"]


def test_diagnostic_candidate_contains_required_user_facing_metrics(tmp_path):
    _workspace(tmp_path, [_candidate("a", 0.10, 0.05, eligible=False)])
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    item = shortlist["diagnostic_top_candidates"][0]
    required = {
        "rank", "candidate_id", "rule", "resolved", "sample_size", "WIN", "LOSS", "win_rate",
        "expectancy_R", "profit_factor", "incumbent_expectancy_R", "expectancy_delta_vs_incumbent",
        "incumbent_profit_factor", "profit_factor_delta_vs_incumbent", "win_rate_delta_vs_incumbent",
        "win_retention", "losses_rejected", "temporal_stability", "directional_stability", "sensitivity",
        "overfitting_status", "deployment_eligible", "status", "reason",
    }
    assert required.issubset(item)


def test_selection_of_diagnostic_non_deployable_rank1_is_rejected(tmp_path):
    sha = _workspace(tmp_path, [_candidate("a", 0.20, 0.10, eligible=False), _candidate("b", 0.10, 0.05, eligible=False)])
    path, _ = build_review_shortlist(tmp_path, run_id="1")
    with pytest.raises(CandidateNotDeployable, match="CANDIDATE_NOT_DEPLOYABLE"):
        resolve_review_candidate(path, current_code_sha=sha, rank=1)
    assert not (tmp_path / "09_freeze.json").exists()
    assert not (tmp_path / "10_holdout.json").exists()


def test_deployable_diagnostic_rank1_can_be_selected(tmp_path):
    good = _candidate("good", 0.30, 0.20, eligible=True)
    sha = _workspace(tmp_path, [good, _candidate("b", 0.10, 0.05, eligible=False)], proposed=good)
    path, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["diagnostic_top_candidates"][0]["deployment_eligible"] is True
    _, source = resolve_review_candidate(path, current_code_sha=sha, rank=1)
    assert source["candidate"]["id"] == "good"


def test_fewer_than_three_candidates_shows_available_only(tmp_path):
    _workspace(tmp_path, [_candidate("one", 0.10, 0.05, eligible=False), _candidate("two", 0.05, 0.02, eligible=False)])
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert len(shortlist["diagnostic_top_candidates"]) == 2


def test_zero_candidates_returns_empty_diagnostic_and_deployable_lists(tmp_path):
    _workspace(tmp_path, [])
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["diagnostic_top_candidates"] == []
    assert shortlist["deployable_candidates"] == []


def test_diagnostic_ranking_is_deterministic(tmp_path):
    candidates = [
        _candidate("a", 0.20, 0.10, eligible=False),
        _candidate("b", 0.20, 0.10, eligible=False),
        _candidate("c", 0.10, 0.05, eligible=False),
    ]
    _workspace(tmp_path, candidates)
    _, first = build_review_shortlist(tmp_path, run_id="1")
    _, second = build_review_shortlist(tmp_path, run_id="1")
    assert first["diagnostic_top_candidates"] == second["diagnostic_top_candidates"]
    assert first["shortlist_sha256"] == second["shortlist_sha256"]


def test_shortlist_hash_binds_diagnostic_ranking(tmp_path):
    sha = _workspace(
        tmp_path,
        [_candidate("a", 0.20, 0.10, eligible=False), _candidate("b", 0.10, 0.05, eligible=False), _candidate("c", 0.05, 0.02, eligible=False)],
    )
    path, shortlist = build_review_shortlist(tmp_path, run_id="1")
    tampered = dict(shortlist)
    tampered["diagnostic_top_candidates"] = list(reversed(shortlist["diagnostic_top_candidates"]))
    write_json(path, tampered)
    with pytest.raises(ValueError, match="(?:diagnostic review|shortlist) hash mismatch"):
        verify_review_shortlist(path, current_code_sha=sha)


def test_review_mode_leaves_holdout_unopened_and_authority_false(tmp_path):
    _workspace(tmp_path, [_candidate("a", 0.10, 0.05, eligible=False)])
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["holdout_opened"] is False
    assert shortlist["production_authority"] is False
    assert not (tmp_path / "10_holdout.json").exists()


def test_shortlist_is_immutable_and_identity_bound(tmp_path):
    sha = _workspace(tmp_path, [_candidate("a", 0.10, 0.05, eligible=False)])
    path, shortlist = build_review_shortlist(tmp_path, run_id="1")
    material = dict(shortlist)
    material.pop("shortlist_sha256")
    assert shortlist["shortlist_sha256"] == canonical_sha256(material)
    assert verify_review_shortlist(path, current_code_sha=sha)["shortlist_sha256"] == shortlist["shortlist_sha256"]
    target = json.loads((tmp_path / "03_target_population.json").read_text())
    target["changed"] = True
    write_json(tmp_path / "03_target_population.json", target)
    with pytest.raises(ValueError, match="STALE_REVIEW_SHORTLIST"):
        verify_review_shortlist(path, current_code_sha=sha)


def test_holdout_binding_cannot_switch_candidate(tmp_path):
    state_path = tmp_path / "selection.json"
    first = {
        "instrument": "GBP_USD",
        "shortlist_sha256": "a" * 64,
        "rank": 1,
        "candidate_id": "c1",
        "candidate_definition_sha256": "1" * 64,
    }
    second = {**first, "rank": 2, "candidate_id": "c2", "candidate_definition_sha256": "2" * 64}
    state = _bind_selection(state_path, first)
    assert state["holdout_opened"] is False
    with pytest.raises(ValueError, match="HOLDOUT_ALREADY_BOUND"):
        _bind_selection(state_path, second)


def test_full_auto_constant_and_deployment_policy_not_redefined_here():
    assert FULL_AUTO_TO_PAPER == "FULL_AUTO_TO_PAPER"



def _terminal_workspace(tmp_path: Path, *, status_kind="HIGH_OVERFITTING_RISK", count=30, include_determinism=True):
    sha = "b" * 40
    root = tmp_path / "GBP_USD" / "autonomous_v3"
    workspace = root / f"lookback_01m_{sha[:12]}"
    workspace.mkdir(parents=True)
    dataset = {"code_sha": sha, "data_sha256": "d" * 64}
    write_json(workspace / "03_target_population.json", {"instrument": "GBP_USD", "dataset_identity": dataset})
    write_json(workspace / "04_phase_1.json", {"stage": "phase_1"})
    write_json(workspace / "05_phase_2.json", {
        "instrument": "GBP_USD", "dataset_identity": dataset,
        "selection_protocol": "DISCOVERY_DEFINE__VALIDATION_SELECT__FREEZE__HOLDOUT_ONCE",
        "partition_config": {"horizon_minutes": 240, "embargo_minutes": 30},
        "lookahead_protection": True,
    })
    records = []
    for i in range(count):
        rec = _candidate(f"diag{i}", 0.20 - i * 0.001, 0.12 - i * 0.001, eligible=False)
        rec["validation"]["selected"].update({"wins": 40 + i, "losses": 60, "resolved_binary": 100 + i})
        rec["incumbent_comparison"]["validation"]["incumbent"].update({"wins": 16, "losses": 30, "resolved_binary": 46})
        if status_kind == "HIGH_OVERFITTING_RISK":
            rec["overfitting_risk"] = {"severity": "HIGH", "flags": ["HIGH_OVERFITTING_RISK"]}
            rec["decision_gate"] = {"decision": "REJECT", "diagnostic_state": "NO_VALID_CANDIDATE", "failed": ["HIGH_OVERFITTING_RISK"]}
        elif status_kind == "NO_MEANINGFUL_IMPROVEMENT":
            rec["incumbent_comparison"]["validation"].update({"challenger_beats_incumbent": False, "material_improvement": False})
            rec["decision_gate"] = {"decision": "REJECT", "diagnostic_state": "NO_MEANINGFUL_IMPROVEMENT", "failed": ["NO_MEANINGFUL_IMPROVEMENT"]}
        elif status_kind == "CHALLENGER_BETTER_BUT_NOT_ROBUST":
            rec["incumbent_comparison"]["validation"].update({"challenger_beats_incumbent": True, "material_improvement": True})
            rec["directional_stability"] = {"stable": False}
            rec["decision_gate"] = {"decision": "REJECT", "diagnostic_state": "CHALLENGER_BETTER_BUT_NOT_ROBUST", "failed": ["DIRECTIONAL_INSTABILITY"]}
        records.append(rec)
    discovery = {
        "instrument": "GBP_USD", "dataset_identity": dataset, "holdout_opened": False,
        "incumbent": {"definition": {"methodology_identity": "m" * 64}, "incumbent_definition_sha256": "i" * 64},
        "candidate_space": {"generated": 120 if count else 0, "evaluated_after_discovery_gate": count, "freeze_eligible": 0},
        "ranked_candidates": records, "proposed_frozen_candidate": None, "production_authority": False,
    }
    write_json(workspace / "06_discovery.json", discovery)
    if include_determinism:
        write_json(workspace / "08_determinism.json", {"status": "PASS"})
    ledger = V3Ledger(root / "automation_v3_state.json")
    ledger.mutate("GBP_USD", status="RUNNING", code_sha=sha, workspace=str(root), lookback_attempts=[{"months": 1, "code_sha": sha}])
    optimizer = ReviewBeforeHoldoutOptimizer(Path.cwd(), code_sha_provider=lambda: sha)
    return optimizer, ledger, root, workspace, sha


def test_review_no_valid_candidate_terminal_persists_real_diagnostics(tmp_path):
    optimizer, ledger, root, workspace, sha = _terminal_workspace(tmp_path, status_kind="HIGH_OVERFITTING_RISK", count=30)
    result = optimizer._terminal(
        ledger, "GBP_USD", "NO_VALID_CANDIDATE", "HIGH_OVERFITTING_RISK",
        diagnostic={"generated_candidates": 120, "evaluated_after_discovery_gate": 30, "freeze_eligible": 0, "dominant_failure": "HIGH_OVERFITTING_RISK"},
    )
    assert result["status"] == "NO_VALID_CANDIDATE"
    assert result["final_outcome"] == "NO_VALID_CANDIDATE"
    assert result["incumbent_metrics"]
    assert len(result["diagnostic_top_candidates"]) == 3
    assert result["deployable_candidates"] == []
    assert len(result["review_shortlist_sha256"]) == 64
    assert all(c["deployment_eligible"] is False for c in result["diagnostic_top_candidates"])
    assert all(c["status"] == "HIGH_OVERFITTING_RISK" for c in result["diagnostic_top_candidates"])
    shortlist = load_json(Path(result["review_shortlist"]))
    assert shortlist["shortlist_sha256"] == result["review_shortlist_sha256"]
    assert shortlist["holdout_opened"] is False
    assert result.get("paper_deployment") is None
    assert result["production_authority"] is False


@pytest.mark.parametrize("kind", ["NO_MEANINGFUL_IMPROVEMENT", "CHALLENGER_BETTER_BUT_NOT_ROBUST"])
def test_review_scientific_rejection_still_reports_top3(tmp_path, kind):
    optimizer, ledger, *_ = _terminal_workspace(tmp_path, status_kind=kind, count=6)
    result = optimizer._terminal(ledger, "GBP_USD", "NO_VALID_CANDIDATE", kind)
    assert result["status"] == "NO_VALID_CANDIDATE"
    assert len(result["diagnostic_top_candidates"]) == 3
    assert result["deployable_candidates"] == []
    assert result["review_shortlist_sha256"]
    assert result["production_authority"] is False


def test_review_zero_evaluated_candidates_may_return_empty_diagnostic(tmp_path):
    optimizer, ledger, *_ = _terminal_workspace(tmp_path, count=0)
    result = optimizer._terminal(ledger, "GBP_USD", "NO_VALID_CANDIDATE", "NO_EVALUATED_CANDIDATES")
    assert result["status"] == "NO_VALID_CANDIDATE"
    assert result["diagnostic_top_candidates"] == []
    assert result["deployable_candidates"] == []
    assert result["review_shortlist_sha256"]
    assert result["production_authority"] is False



def test_real_33940485772_path_builds_diagnostic_top3_without_determinism(tmp_path):
    optimizer, ledger, root, workspace, sha = _terminal_workspace(
        tmp_path, status_kind="HIGH_OVERFITTING_RISK", count=30, include_determinism=False
    )
    result = optimizer._terminal(
        ledger, "GBP_USD", "NO_VALID_CANDIDATE", "HIGH_OVERFITTING_RISK",
        diagnostic={"generated_candidates": 120, "evaluated_after_discovery_gate": 30, "freeze_eligible": 0},
    )
    assert result["status"] == "NO_VALID_CANDIDATE"
    assert result["incumbent_metrics"]
    assert len(result["diagnostic_top_candidates"]) == 3
    assert result["deployable_candidates"] == []
    assert result["review_shortlist_sha256"] is None
    assert len(result["diagnostic_review_sha256"]) == 64
    review = load_json(Path(result["review_shortlist"]))
    assert review["diagnostic_only"] is True
    assert review["deployment_evidence_complete"] is False
    assert review["determinism_artifact_sha256"] is None
    assert review["shortlist_sha256"] is None
    assert review["diagnostic_review_sha256"] == result["diagnostic_review_sha256"]
    assert all(c["deployment_eligible"] is False for c in review["diagnostic_top_candidates"])
    assert all(c["diagnostic_only"] is True for c in review["diagnostic_top_candidates"])
    assert not (workspace / "09_freeze.json").exists()
    assert not (workspace / "10_holdout.json").exists()
    assert result.get("paper_deployment") is None
    assert result["production_authority"] is False


def test_missing_determinism_blocks_selection_even_if_scientifically_freeze_eligible(tmp_path):
    good = _candidate("good", 0.30, 0.20, eligible=True)
    sha = _workspace(tmp_path, [good], proposed=good)
    (tmp_path / "08_determinism.json").unlink()
    path, review = build_review_shortlist(tmp_path, run_id="33940485772")
    assert review["diagnostic_top_candidates"][0]["scientific_pre_holdout_eligible"] is True
    assert review["diagnostic_top_candidates"][0]["deployment_eligible"] is False
    assert review["deployable_candidates"] == []
    assert review["shortlist_sha256"] is None
    with pytest.raises(CandidateNotDeployable, match="determinism is required before selection"):
        resolve_review_candidate(path, current_code_sha=sha, rank=1)
    assert not (tmp_path / "09_freeze.json").exists()
    assert not (tmp_path / "10_holdout.json").exists()


def test_diagnostic_review_hash_binds_pre_determinism_top3(tmp_path):
    _workspace(tmp_path, [_candidate("a", 0.2, 0.1, eligible=False), _candidate("b", 0.1, 0.05, eligible=False)])
    (tmp_path / "08_determinism.json").unlink()
    _, review = build_review_shortlist(tmp_path, run_id="33940485772")
    material = dict(review)
    material.pop("diagnostic_review_sha256")
    material.pop("shortlist_sha256")
    assert review["diagnostic_review_sha256"] == canonical_sha256(material)
    assert review["shortlist_sha256"] is None


def test_unexpected_review_report_build_failure_is_explicit(tmp_path, monkeypatch):
    optimizer, ledger, *_ = _terminal_workspace(tmp_path, include_determinism=False)
    import automation_v3_modes as modes
    monkeypatch.setattr(modes, "build_review_shortlist", lambda *a, **k: (_ for _ in ()).throw(ValueError("synthetic review failure")))
    result = optimizer._terminal(ledger, "GBP_USD", "NO_VALID_CANDIDATE", "HIGH_OVERFITTING_RISK")
    assert result["status"] == "REVIEW_REPORT_BUILD_FAILED"
    assert result["final_outcome"] == "NO_VALID_CANDIDATE"
    assert result["review_report_error"]["code"] == "REVIEW_REPORT_BUILD_FAILED"
    assert "synthetic review failure" in result["review_report_error"]["reason"]
    assert result["production_authority"] is False

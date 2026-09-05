import json
from pathlib import Path

import pytest

from automation_v3_candidate_mapping import CandidateNotDeployable
from automation_v3_modes import (
    FULL_AUTO_TO_PAPER,
    KEEP_INCUMBENT,
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
    with pytest.raises(ValueError, match="shortlist hash mismatch"):
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

import json
from pathlib import Path

import pytest

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


def _candidate(cid, exp_delta, pf_delta, expectancy=-0.1, eligible=True):
    definition = {"id": cid, "rules": [{"feature": "room_to_barrier_r", "operator": "<=", "threshold": 0.55}], "candidate_rule": "room_to_barrier_r <= 0.55", "entry_time_only": True}
    gate = {"decision": "FREEZE_ELIGIBLE" if eligible else "REJECT", "diagnostic_state": "CHALLENGER_DEPLOYABLE" if eligible else "CHALLENGER_BETTER_BUT_NOT_ROBUST", "paper_candidate_classification": "RELATIVE_IMPROVEMENT_PAPER_CANDIDATE" if eligible else None}
    return {
        "candidate": definition,
        "validation": {"selected": {"expectancy_r": expectancy, "profit_factor": 0.8, "win_rate": 0.39, "resolved_binary": 100}, "win_retention": 0.8, "loss_rejection": 0.2, "losses_rejected": 20},
        "incumbent_comparison": {"validation": {"incumbent": {"expectancy_r": -0.25, "profit_factor": 0.65, "win_rate": 0.35, "resolved_binary": 46}, "challenger": {"expectancy_r": expectancy, "profit_factor": 0.8}, "expectancy_delta_vs_incumbent": exp_delta, "profit_factor_delta_vs_incumbent": pf_delta, "challenger_beats_incumbent": eligible, "material_improvement": eligible}},
        "directional_stability": {"stable": eligible}, "temporal_stability": {"stable": eligible},
        "sensitivity": {"classification": "STABLE", "all_positive": True}, "walk_forward_stability": {"status": "PASS"},
        "overfitting_risk": {"severity": "LOW" if eligible else "MEDIUM"}, "decision_gate": gate,
    }


def _workspace(tmp_path: Path, count=5):
    sha = "a" * 40
    dataset = {"code_sha": sha, "data_sha256": "d" * 64}
    write_json(tmp_path / "03_target_population.json", {"instrument": "EUR_USD", "dataset_identity": dataset})
    write_json(tmp_path / "04_phase_1.json", {"stage": "phase_1"})
    phase2 = {"instrument": "EUR_USD", "dataset_identity": dataset, "selection_protocol": "DISCOVERY_DEFINE__VALIDATION_SELECT__FREEZE__HOLDOUT_ONCE", "partition_config": {"horizon_minutes": 240, "embargo_minutes": 30}, "lookahead_protection": True}
    write_json(tmp_path / "05_phase_2.json", phase2)
    ranked = [_candidate(f"c{i}", 0.20 - i * 0.01, 0.15 - i * 0.01) for i in range(count)]
    ranked.append(_candidate("bad", 1.0, 1.0, eligible=False))
    incumbent_definition = {"methodology_identity": "m" * 64}
    discovery = {"instrument": "EUR_USD", "dataset_identity": dataset, "holdout_opened": False, "incumbent": {"definition": incumbent_definition, "incumbent_definition_sha256": "i" * 64}, "ranked_candidates": ranked, "proposed_frozen_candidate": ranked[0], "production_authority": False}
    write_json(tmp_path / "06_discovery.json", discovery)
    write_json(tmp_path / "08_determinism.json", {"status": "PASS"})
    return sha


def test_natural_language_mode_mapping():
    assert parse_natural_language_intent("Automatiza EUR/USD") == {"instrument": "EUR_USD", "mode": FULL_AUTO_TO_PAPER}
    assert parse_natural_language_intent("Optimiza EUR/USD pero no despliegues") == {"instrument": "EUR_USD", "mode": REVIEW_BEFORE_HOLDOUT_DEPLOY}
    assert parse_natural_language_intent("Automatiza EUR/USD y antes de desplegar dame los resultados")["mode"] == REVIEW_BEFORE_HOLDOUT_DEPLOY
    assert parse_natural_language_intent("Despliega la 2") == {"instrument": None, "mode": SELECT_REVIEW_CANDIDATE, "rank": 2}
    assert parse_natural_language_intent("Quédate con la actual") == {"instrument": None, "mode": KEEP_INCUMBENT}


def test_structured_selection_is_strictly_bound():
    payload = validate_structured_request({"instrument": "EUR/USD", "mode": SELECT_REVIEW_CANDIDATE, "shortlist_sha256": "a" * 64, "rank": 2})
    assert payload == {"instrument": "EUR_USD", "mode": SELECT_REVIEW_CANDIDATE, "shortlist_sha256": "a" * 64, "rank": 2}
    with pytest.raises(ValueError):
        validate_structured_request({"instrument": "EUR_USD", "mode": SELECT_REVIEW_CANDIDATE, "rank": 1})
    with pytest.raises(ValueError):
        validate_structured_request({"instrument": "EUR_USD", "mode": SELECT_REVIEW_CANDIDATE, "shortlist_sha256": "a" * 64, "rank": 5})


def test_shortlist_is_immutable_top4_and_excludes_noneligible(tmp_path):
    sha = _workspace(tmp_path, count=5)
    path, shortlist = build_review_shortlist(tmp_path, run_id="123")
    assert path.name == f"review_shortlist_{shortlist['shortlist_sha256']}.json"
    assert shortlist["production_authority"] is False
    assert shortlist["holdout_opened"] is False
    assert len(shortlist["candidates"]) == 4
    assert [item["candidate_id"] for item in shortlist["candidates"]] == ["c0", "c1", "c2", "c3"]
    assert all(item["pre_holdout_eligible"] for item in shortlist["candidates"])
    material = dict(shortlist); material.pop("shortlist_sha256")
    assert shortlist["shortlist_sha256"] == canonical_sha256(material)
    assert verify_review_shortlist(path, current_code_sha=sha)["shortlist_sha256"] == shortlist["shortlist_sha256"]


def test_selection_resolves_against_backend_discovery_identity(tmp_path):
    sha = _workspace(tmp_path, count=3)
    path, shortlist = build_review_shortlist(tmp_path, run_id="123")
    loaded, source = resolve_review_candidate(path, current_code_sha=sha, rank=2)
    assert loaded["shortlist_sha256"] == shortlist["shortlist_sha256"]
    assert source["candidate"]["id"] == "c1"
    assert canonical_sha256(source["candidate"]) == shortlist["candidates"][1]["candidate_definition_sha256"]


def test_stale_code_or_artifact_rejected(tmp_path):
    sha = _workspace(tmp_path, count=2)
    path, _ = build_review_shortlist(tmp_path, run_id="123")
    with pytest.raises(ValueError, match="STALE_REVIEW_SHORTLIST"):
        verify_review_shortlist(path, current_code_sha="b" * 40)
    target = json.loads((tmp_path / "03_target_population.json").read_text())
    target["changed"] = True
    write_json(tmp_path / "03_target_population.json", target)
    with pytest.raises(ValueError, match="STALE_REVIEW_SHORTLIST"):
        verify_review_shortlist(path, current_code_sha=sha)


def test_holdout_binding_cannot_switch_candidate(tmp_path):
    state_path = tmp_path / "selection.json"
    first = {"instrument": "EUR_USD", "shortlist_sha256": "a" * 64, "rank": 1, "candidate_id": "c1", "candidate_definition_sha256": "1" * 64}
    second = {**first, "rank": 2, "candidate_id": "c2", "candidate_definition_sha256": "2" * 64}
    state = _bind_selection(state_path, first)
    assert state["holdout_opened"] is False
    with pytest.raises(ValueError, match="HOLDOUT_ALREADY_BOUND"):
        _bind_selection(state_path, second)


def test_review_shortlist_does_not_manufacture_four(tmp_path):
    _workspace(tmp_path, count=2)
    _, shortlist = build_review_shortlist(tmp_path, run_id="123")
    assert len(shortlist["candidates"]) == 2

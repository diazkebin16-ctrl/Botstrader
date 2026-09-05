import json
from datetime import datetime, timedelta, timezone

import pytest

from research_manager import sha256_file
from research_pipeline import analyze_phase1
from research_phase2 import (
    automatic_pre_audit, automatic_report, candidate_analysis, discover_candidates,
    episode_dedup_evidence, evaluate_holdout, freeze_candidate, generate_ai_prompts,
    prepare_phase2,
)


def _rows(count=180):
    start=datetime(2026,1,1,tzinfo=timezone.utc)
    rows=[]
    for i in range(count):
        status="WIN"
        if i%5==0:status="LOSS"
        if i%17==0:status="AMBIGUOUS"
        elif i%19==0:status="PENDING"
        elif i%11==0:status="TIMEOUT"
        loss=status=="LOSS"
        ts=start+timedelta(minutes=10*i)
        rows.append({
            "instrument":"AUD_USD","candle_ts":ts.isoformat(),"exit_ts":(ts+timedelta(minutes=2)).isoformat(),
            "outcome_status":status,"label":1 if status=="WIN" else (0 if status=="LOSS" else None),
            "realized_r":1.2 if status=="WIN" else (-1.0 if status=="LOSS" else None),
            "decision_reason":"REPLAY_ACTIONABLE","safety_checks":{},
            "research_direction":"BUY" if i%2 else "SELL","chosen_signal":"BUY" if i%2 else "SELL",
            "features":{
                "extension_atr":(1.5+.01*(i%10)) if loss else (.35+.01*(i%10)),
                "rr_raw":1.0+.01*(i%20),"direction_edge":20+i%5,"volatility_ratio":1.0,
                "m1_ema9_side_ok":i%3!=0,"m1_momentum":.001 if i%2 else -.001,
                "m1_candle_color_ok":i%4!=0,"m1_confirm":i%5!=0,
            },
            "filters":{"m1_confirmation":i%5!=0},
        })
    return rows


def _write(path,payload):
    path.write_text(json.dumps(payload),encoding="utf-8")


def _freezeable_fixture(discovery):
    """Build an explicit provenance-test freeze candidate without weakening discovery gates."""
    if discovery.get("proposed_frozen_candidate"):
        return discovery
    ranked=discovery.get("ranked_candidates") or []
    assert ranked, "fixture requires at least one evaluated candidate"
    item=dict(ranked[0])
    item["decision_gate"]={**dict(item.get("decision_gate") or {}),"decision":"FREEZE_ELIGIBLE"}
    out=dict(discovery);out["status"]="OK";out["proposed_frozen_candidate"]=item
    return out


def _valid_report_inputs(tmp_path, **updates):
    payloads = {
        "integrity": {"status":"PASS","instrument":"AUD_USD","input_sha256":"data","dataset_identity":"dataset-id","code_sha":"code-sha","start":"s","end":"e","warmup_days":10,"horizon_minutes":240,"bid_ask_real":True},
        "phase1": {"all_target_wins_recovered":True,"lookahead_protection":True,"best_policy":{"losses_released":1}},
        "phase2": {
            "instrument":"AUD_USD","lookahead_protection":True,"future_bars_used_only_for_outcome":True,
            "lookahead_evidence":{
                "replay":{"no_lookahead_decision":True,"future_bars_only_for_outcome":True},
                "target_population":{"lookahead_protection":True,"future_bars_used_only_for_outcome":True},
            },
            "episode_dedup":{"status":"PASS","total_episodes":3,"unique_episode_identities":3,"duplicate_count":0},
            "partitions":{"discovery":{"episodes":1},"validation":{"episodes":1},"holdout":{"episodes":1}},
            "safety_risk_global_gates":"SEPARATE_IMMUTABLE_NOT_CANDIDATE_FEATURES",
            "learned_research_veto":"NOT_HISTORICALLY_RECONSTRUCTABLE",
        },
        "discovery": {"status":"OK","lookahead_protection":True,"candidate_space":{},"discovery_metrics":{"outcomes":{}},"m1_internals":{}},
        "freeze": {
            "immutable":True,"lookahead_protection":True,"candidate_id":"c1",
            "candidate_definition":{"rules":[{"feature":"rr_raw","operator":">=","threshold":1.0}]},
        },
        "holdout": {
            "status":"PASS","lookahead_protection":True,"retuning_after_holdout":False,"freeze_sha256":"f","analysis":{},
            "directional_stability":{"stable":True},"temporal_stability":{"stable":True},
            "sensitivity":{"classification":"STABLE"},"walk_forward_stability":{"status":"PASS"},
            "overlap_remove_one":{},"overfitting_risk":{"severity":"LOW","reasons":[]},
            "candidate_ranking":[{"candidate_id":"c1","status":"RESEARCH_CANDIDATE"}],
        },
        "determinism": {"status":"PASS"},
        "audit": {
            "status":"PASS","production_modifications":"NONE",
            "package":{
                "status":"PASS","failures":[],"secret_hits":[],
                "contaminated_untracked":[],"permission_changes":[],"prohibited":[],
                "manifest_generation_allowed":True,
            },
        },
    }
    for name, change in updates.items():
        payloads[name].update(change)
    paths = {}
    for name, payload in payloads.items():
        path = tmp_path / f"{name}.json"
        _write(path, payload)
        paths[name] = path
    report = automatic_report(*(str(paths[name]) for name in (
        "integrity", "phase1", "phase2", "discovery", "freeze", "holdout", "determinism", "audit",
    )))
    return report


def _holdout_chain(tmp_path):
    target = tmp_path / "target.json"
    _write(target, {
        "instrument": "AUD_USD", "variant": "V331_BASELINE",
        "lookahead_protection": True, "future_bars_used_only_for_outcome": True,
        "dataset_identity": {"status": "PASS", "code_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "data_sha256": "data-a"},
        "episodes": _rows(),
    })
    phase1_payload = analyze_phase1(str(target), discovery_only=True, horizon_minutes=5, embargo_minutes=0)
    phase1 = tmp_path / "phase1.json"
    _write(phase1, phase1_payload)
    phase2_payload = prepare_phase2(str(target), str(phase1), horizon_minutes=5, embargo_minutes=0)
    phase2 = tmp_path / "phase2.json"
    _write(phase2, phase2_payload)
    discovery_payload = _freezeable_fixture(discover_candidates(str(target), str(phase2), min_resolved=5))
    discovery = tmp_path / "discovery.json"
    _write(discovery, discovery_payload)
    freeze = tmp_path / "freeze.json"
    frozen = freeze_candidate(str(discovery), freeze)
    _write(freeze, frozen)
    return target, phase2, discovery, freeze


def test_phase2_preserves_non_binary_outcomes_and_freezes_before_holdout(tmp_path):
    target=tmp_path/"target.json"
    _write(target,{"instrument":"AUD_USD","variant":"V331_BASELINE","lookahead_protection":True,
                   "dataset_identity":{"status":"PASS","code_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"episodes":_rows()})
    phase1=analyze_phase1(str(target),discovery_only=True,horizon_minutes=5,embargo_minutes=0)
    assert phase1["all_target_wins_recovered"] is True
    phase1_path=tmp_path/"phase1.json";_write(phase1_path,phase1)
    phase2=prepare_phase2(str(target),str(phase1_path),horizon_minutes=5,embargo_minutes=0)
    assert phase2["episode_dedup"]["status"] == "PASS"
    assert phase2["episode_dedup"]["duplicate_count"] == 0
    phase2_path=tmp_path/"phase2.json";_write(phase2_path,phase2)
    discovery=discover_candidates(str(target),str(phase2_path),min_resolved=5)
    assert discovery["status"] in {"OK","NO_FREEZE_ELIGIBLE_CANDIDATE"}
    assert discovery["holdout_opened"] is False
    discovery=_freezeable_fixture(discovery)
    freeze_path=tmp_path/"freeze.json"
    discovery_path=tmp_path/"discovery.json";_write(discovery_path,discovery)
    frozen=freeze_candidate(str(discovery_path),freeze_path);_write(freeze_path,frozen)
    assert frozen["immutable"] is True
    holdout=evaluate_holdout(str(target),str(phase2_path),str(discovery_path),str(freeze_path))
    effects=holdout["analysis"]["outcome_effect"]
    assert effects["TIMEOUT"]["baseline"]>=0
    assert effects["AMBIGUOUS"]["baseline"]>=0
    assert effects["PENDING"]["baseline"]>=0
    assert holdout["retuning_after_holdout"] is False
    assert holdout["candidate_ranking"][0]["status"] in {"REJECT","RESEARCH_CANDIDATE"}


def test_holdout_allows_exact_bound_artifact_identities(tmp_path):
    target, phase2, discovery, freeze = _holdout_chain(tmp_path)
    frozen = json.loads(freeze.read_text(encoding="utf-8"))
    assert frozen["target_population_sha256"] == sha256_file(target)
    assert frozen["phase2_sha256"] == sha256_file(phase2)
    assert frozen["discovery_sha256"] == sha256_file(discovery)
    assert frozen["dataset_identity"]["code_sha"] == frozen["code_sha"] == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    result = evaluate_holdout(str(target), str(phase2), str(discovery), str(freeze))
    assert result["candidate_definition_sha256"]


def test_holdout_rejects_same_instrument_with_different_target_sha(tmp_path):
    target, phase2, discovery, freeze = _holdout_chain(tmp_path)
    target.write_text(target.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Target population SHA"):
        evaluate_holdout(str(target), str(phase2), str(discovery), str(freeze))


def test_holdout_rejects_same_instrument_with_different_dataset_identity(tmp_path):
    target, phase2, discovery, freeze = _holdout_chain(tmp_path)
    frozen = json.loads(freeze.read_text(encoding="utf-8"))
    frozen["dataset_identity"] = {"status": "PASS", "code_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "data_sha256": "data-b"}
    _write(freeze, frozen)
    with pytest.raises(ValueError, match="Dataset identity mismatch"):
        evaluate_holdout(str(target), str(phase2), str(discovery), str(freeze))


def test_holdout_rejects_same_instrument_with_different_phase2_sha(tmp_path):
    target, phase2, discovery, freeze = _holdout_chain(tmp_path)
    phase2.write_text(phase2.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Phase2 SHA"):
        evaluate_holdout(str(target), str(phase2), str(discovery), str(freeze))


def test_holdout_rejects_tampered_discovery_freeze_provenance(tmp_path):
    target, phase2, discovery, freeze = _holdout_chain(tmp_path)
    frozen = json.loads(freeze.read_text(encoding="utf-8"))
    frozen["discovery_sha256"] = "0" * 64
    _write(freeze, frozen)
    with pytest.raises(ValueError, match="Discovery/freeze provenance"):
        evaluate_holdout(str(target), str(phase2), str(discovery), str(freeze))


def test_freeze_refuses_silent_retuning(tmp_path):
    path=tmp_path/"freeze.json"
    path.write_text(json.dumps({"candidate_definition_sha256":"old"}),encoding="utf-8")
    discovery=tmp_path/"discovery.json"
    _write(discovery,{"instrument":"AUD_USD","status":"OK","proposed_frozen_candidate":{
        "candidate":{"id":"c","rules":[{"feature":"rr_raw","operator":">=","threshold":1.0}]},
        "decision_gate":{"decision":"FREEZE_ELIGIBLE"},
    }})
    with pytest.raises(ValueError,match="immutable"):
        freeze_candidate(str(discovery),path)


def test_candidate_analysis_never_collapses_timeout_into_loss():
    rows=_rows(40)
    candidate={"id":"x","rules":[{"feature":"extension_atr","operator":"<=","threshold":1.0}]}
    out=candidate_analysis(candidate,rows)
    assert out["baseline"]["losses"]==sum(row["outcome_status"]=="LOSS" for row in rows)
    assert out["baseline"]["resolved_binary"]==sum(row["outcome_status"] in {"WIN","LOSS"} for row in rows)


def test_canonical_report_fields_and_prompt_roles(tmp_path):
    report = _valid_report_inputs(tmp_path)
    expected={"INPUT SHA256","INSTRUMENT","START","END","WARMUP","HORIZON","POPULATION","OUTCOMES","FILTERS/GATES","PHASE 1","PHASE 2","DISCOVERY","FREEZE","HOLDOUT","DIRECTIONAL STABILITY","TEMPORAL STABILITY","SENSITIVITY","OVERLAP","REMOVE-ONE","WALK-FORWARD","OVERFITTING RISK","SELECTED CANDIDATE","LOOK-AHEAD","DETERMINISM","PRODUCTION MODIFICATIONS","CRITICAL","HIGH","MEDIUM","LOW","OUTPUT SHA256"}
    assert set(report)==expected
    material=dict(report);stored=material["OUTPUT SHA256"];material["OUTPUT SHA256"]=None
    from research_phase2 import _canonical_hash
    assert stored==_canonical_hash(material)
    report_path=tmp_path/"report.json";_write(report_path,report)
    pre=automatic_pre_audit(str(report_path));pre_path=tmp_path/"pre.json";_write(pre_path,pre)
    prompts=generate_ai_prompts(str(report_path),str(pre_path))
    assert "Do not rely on IA #3 conclusions" in prompts["ai_2_prompt"]
    assert "Do not retune" in prompts["ai_3_prompt"]


def test_single_rule_not_applicable_overlap_does_not_reject_pre_audit(tmp_path):
    report = _valid_report_inputs(tmp_path)
    assert report["OVERLAP"]["status"] == "NOT APPLICABLE"
    assert report["REMOVE-ONE"]["status"] == "NOT APPLICABLE"
    report_path = tmp_path / "report.json"
    _write(report_path, report)
    pre_audit = automatic_pre_audit(str(report_path))
    assert pre_audit["checks"]["overlap_recorded"] is True
    assert pre_audit["checks"]["remove_one_recorded"] is True
    assert pre_audit["verdict"] == "ACCEPT"


def test_report_lookahead_pass_requires_complete_upstream_evidence(tmp_path):
    report = _valid_report_inputs(tmp_path)
    assert report["LOOK-AHEAD"]["status"] == "PASS"
    assert set(report["LOOK-AHEAD"]["stages"]) == {
        "replay", "target_population", "phase_1", "phase_2", "discovery", "freeze", "holdout",
    }


def test_report_lookahead_missing_evidence_cannot_pass(tmp_path):
    report = _valid_report_inputs(tmp_path, phase1={"lookahead_protection": None})
    assert report["LOOK-AHEAD"]["status"] == "NOT TESTED"


def test_report_explicit_lookahead_evidence_fails(tmp_path):
    report = _valid_report_inputs(tmp_path, discovery={"lookahead_detected": True})
    assert report["LOOK-AHEAD"]["status"] == "FAIL"


def test_episode_dedup_evidence_passes_unique_and_fails_duplicate():
    unique = [
        {"episode_id": "episode-1", "instrument": "AUD_USD"},
        {"episode_id": "episode-2", "instrument": "AUD_USD"},
    ]
    passed = episode_dedup_evidence(unique)
    assert passed == {
        "status": "PASS", "total_episodes": 2, "unique_episode_identities": 2,
        "duplicate_count": 0, "identity": "episode_id_or_canonical_instrument_timestamp_direction",
    }
    failed = episode_dedup_evidence([*unique, dict(unique[0])])
    assert failed["status"] == "FAIL"
    assert failed["total_episodes"] == 3
    assert failed["unique_episode_identities"] == 2
    assert failed["duplicate_count"] == 1


def test_pre_audit_missing_dedup_evidence_cannot_pass(tmp_path):
    report = _valid_report_inputs(tmp_path, phase2={"episode_dedup": None})
    assert report["POPULATION"]["status"] == "NOT TESTED"
    report_path = tmp_path / "report.json"
    _write(report_path, report)
    pre_audit = automatic_pre_audit(str(report_path))
    assert pre_audit["checks"]["episode_dedup"] is False
    assert pre_audit["verdict"] == "REJECT"


def test_pre_audit_duplicate_dedup_evidence_rejects(tmp_path):
    duplicate_evidence = {
        "status": "FAIL", "total_episodes": 3,
        "unique_episode_identities": 2, "duplicate_count": 1,
    }
    report = _valid_report_inputs(tmp_path, phase2={"episode_dedup": duplicate_evidence})
    assert report["POPULATION"]["status"] == "FAIL"
    report_path = tmp_path / "report.json"
    _write(report_path, report)
    pre_audit = automatic_pre_audit(str(report_path))
    assert pre_audit["checks"]["episode_dedup"] is False
    assert pre_audit["verdict"] == "REJECT"


def _pre_audit_from_report(tmp_path, report):
    report_path = tmp_path / "preaudit_report.json"
    _write(report_path, report)
    return automatic_pre_audit(str(report_path))


def test_pre_audit_rejects_missing_dataset_identity(tmp_path):
    report = _valid_report_inputs(tmp_path)
    report["FILTERS/GATES"]["data_integrity"].pop("dataset_identity", None)
    material = dict(report); material["OUTPUT SHA256"] = None
    from research_phase2 import _canonical_hash
    report["OUTPUT SHA256"] = _canonical_hash(material)
    pre = _pre_audit_from_report(tmp_path, report)
    assert pre["checks"]["dataset_identity"] is False
    assert pre["verdict"] == "REJECT"


def test_pre_audit_rejects_missing_code_sha(tmp_path):
    report = _valid_report_inputs(tmp_path)
    report["FILTERS/GATES"]["data_integrity"].pop("code_sha", None)
    material = dict(report); material["OUTPUT SHA256"] = None
    from research_phase2 import _canonical_hash
    report["OUTPUT SHA256"] = _canonical_hash(material)
    pre = _pre_audit_from_report(tmp_path, report)
    assert pre["checks"]["dataset_identity"] is False
    assert pre["verdict"] == "REJECT"


def test_pre_audit_rejects_identity_mismatch(tmp_path):
    report = _valid_report_inputs(tmp_path)
    report["FILTERS/GATES"]["data_integrity"]["instrument"] = "EUR_USD"
    material = dict(report); material["OUTPUT SHA256"] = None
    from research_phase2 import _canonical_hash
    report["OUTPUT SHA256"] = _canonical_hash(material)
    pre = _pre_audit_from_report(tmp_path, report)
    assert pre["checks"]["dataset_identity"] is False
    assert pre["verdict"] == "REJECT"


@pytest.mark.parametrize("field,value", [
    ("failures", ["WORKTREE_CONTAMINATED_BY_TRANSIENTS"]),
    ("secret_hits", [{"path":"x.py","pattern":"token"}]),
    ("permission_changes", ["mode change 100644 => 100755 x.py"]),
    ("contaminated_untracked", ["result.zip"]),
])
def test_pre_audit_rejects_dirty_package_even_with_zero_critical(tmp_path, field, value):
    report = _valid_report_inputs(tmp_path)
    report["CRITICAL"] = 0
    report["FILTERS/GATES"]["audit_package"][field] = value
    material = dict(report); material["OUTPUT SHA256"] = None
    from research_phase2 import _canonical_hash
    report["OUTPUT SHA256"] = _canonical_hash(material)
    pre = _pre_audit_from_report(tmp_path, report)
    assert pre["checks"]["packaging_cleanliness"] is False
    assert pre["verdict"] == "REJECT"


def test_pre_audit_rejects_manifest_blocked(tmp_path):
    report = _valid_report_inputs(tmp_path)
    report["CRITICAL"] = 0
    report["FILTERS/GATES"]["audit_package"]["manifest_generation_allowed"] = False
    material = dict(report); material["OUTPUT SHA256"] = None
    from research_phase2 import _canonical_hash
    report["OUTPUT SHA256"] = _canonical_hash(material)
    pre = _pre_audit_from_report(tmp_path, report)
    assert pre["checks"]["packaging_cleanliness"] is False
    assert pre["verdict"] == "REJECT"


def test_pre_audit_accepts_complete_identity_and_clean_package(tmp_path):
    report = _valid_report_inputs(tmp_path)
    pre = _pre_audit_from_report(tmp_path, report)
    assert pre["checks"]["dataset_identity"] is True
    assert pre["checks"]["packaging_cleanliness"] is True

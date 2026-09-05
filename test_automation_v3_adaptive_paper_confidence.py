import pytest

from automation_v3_incumbent_challenger import compare_metrics
from research_phase2 import (
    STANDARD_PAPER_CANDIDATE, EXPERIMENTAL_PAPER_CANDIDATE, REJECTED_CHALLENGER,
    classify_paper_confidence, decision_gate, final_reliability,
)


def comparison(exp=.10, pf=.12, *, material=True, beats=True):
    return {"challenger_beats_incumbent": beats, "material_improvement": material,
            "expectancy_delta_vs_incumbent": exp, "profit_factor_delta_vs_incumbent": pf}


def candidate():
    return {"id":"c1","entry_time_only":True,"rules":[{"feature":"room_to_barrier_r","operator":"<=","threshold":.555555555556}]}


def validation(exp=-.1491):
    return {"selected":{"resolved_binary":456,"wins":178,"losses":278,"win_rate":.3904,"expectancy_r":exp,"profit_factor":.7827},
            "losses_rejected":20,"win_retention":.80}


def risk(severity="LOW", flags=None): return {"severity":severity,"flags":flags or []}
def direction(stable=True): return {"stable":stable,"directions_with_sufficient_evidence":2,"positive_directions":2 if stable else 1}
def temporal(stable=True): return {"stable":stable,"positive_periods":3 if stable else 2,"periods":[1,2,3]}
def sensitivity(kind="STABLE"): return {"classification":kind,"all_positive":kind!="FRAGILE","cliff_effect":kind=="FRAGILE"}
def walk(status="PASS", frac=1.0, folds=3): return {"status":status,"positive_fold_fraction":frac,"valid_folds":folds}


def classify(**kw):
    values=dict(candidate=candidate(),validation=validation(),risk=risk(),discovery_comparison=comparison(),
                validation_comparison=comparison(),directional=direction(),temporal=temporal(),
                sensitivity_result=sensitivity(),walk_forward=walk(),min_resolved=10)
    values.update(kw)
    return classify_paper_confidence(**values)


def test_robust_materially_better_is_standard():
    assert classify()["classification"] == STANDARD_PAPER_CANDIDATE

def test_mild_directional_warning_is_experimental():
    out=classify(directional=direction(False),risk=risk("MEDIUM",["DIRECTIONAL_INSTABILITY"]))
    assert out["classification"] == EXPERIMENTAL_PAPER_CANDIDATE and out["eligible"] is True

def test_moderate_walk_forward_warning_is_experimental():
    out=classify(walk_forward=walk("FAIL",.5,4),risk=risk("MEDIUM",["WALK_FORWARD_INSTABILITY"]))
    assert out["classification"] == EXPERIMENTAL_PAPER_CANDIDATE

def test_severe_walk_forward_collapse_is_rejected():
    out=classify(walk_forward=walk("FAIL",.25,4),risk=risk("MEDIUM"))
    assert out["classification"] == REJECTED_CHALLENGER

def test_high_overfit_is_rejected():
    assert classify(risk=risk("HIGH",["DISCOVERY_EDGE_FAILED_VALIDATION"]))["classification"] == REJECTED_CHALLENGER

def test_discovery_validation_collapse_is_rejected():
    assert classify(validation_comparison=comparison(beats=False,material=False),risk=risk("HIGH",["DISCOVERY_EDGE_FAILED_VALIDATION"]))["classification"] == REJECTED_CHALLENGER

def test_fragile_sensitivity_is_rejected():
    assert classify(sensitivity_result=sensitivity("FRAGILE"),risk=risk("MEDIUM",["THRESHOLD_SENSITIVITY"]))["classification"] == REJECTED_CHALLENGER

def test_moderate_sensitivity_can_be_experimental():
    out=classify(sensitivity_result=sensitivity("MODERATE"),risk=risk("MEDIUM",["THRESHOLD_SENSITIVITY"]))
    assert out["classification"] == EXPERIMENTAL_PAPER_CANDIDATE

def test_negative_expectancy_but_materially_better_can_be_experimental():
    out=classify(directional=direction(False),risk=risk("MEDIUM",["DIRECTIONAL_INSTABILITY"]))
    assert validation()["selected"]["expectancy_r"] < 0 and out["eligible"] is True

def test_microscopic_improvement_is_not_material():
    inc={"resolved_binary":46,"expectancy_r":-.2554,"profit_factor":.6514,"win_rate":.3478}
    ch={"resolved_binary":456,"expectancy_r":-.2553,"profit_factor":.6515,"win_rate":.3480}
    out=compare_metrics(incumbent=inc,challenger=ch,evaluation_population_sha256="x"*64)
    assert out["challenger_beats_incumbent"] is True
    assert out["material_improvement"] is False
    assert out["materiality_floor_r"] == pytest.approx(1/46)

def test_gbp_known_delta_is_material():
    inc={"resolved_binary":46,"expectancy_r":-.25541,"profit_factor":.6514,"win_rate":.3478}
    ch={"resolved_binary":456,"expectancy_r":-.14915,"profit_factor":.78266,"win_rate":.3904}
    out=compare_metrics(incumbent=inc,challenger=ch,evaluation_population_sha256="x"*64)
    assert out["material_improvement"] is True
    assert out["expectancy_delta_vs_incumbent"] == pytest.approx(.10626)

def test_worse_candidate_rejected():
    assert classify(validation_comparison=comparison(beats=False,material=False))["classification"] == REJECTED_CHALLENGER

def test_no_material_improvement_rejected():
    assert classify(validation_comparison=comparison(material=False))["classification"] == REJECTED_CHALLENGER

def test_experimental_gate_is_freeze_eligible_but_labeled():
    gate=decision_gate(candidate(),{},validation(),risk("MEDIUM",["DIRECTIONAL_INSTABILITY"]),min_resolved=10,
        discovery_comparison=comparison(),validation_comparison=comparison(),directional=direction(False),temporal=temporal(),
        sensitivity_result=sensitivity(),walk_forward=walk())
    assert gate["decision"]=="FREEZE_ELIGIBLE"
    assert gate["confidence_class"]=="EXPERIMENTAL"
    assert gate["experimental"] is True and gate["production_authority"] is False

def test_standard_gate_is_preferred_class():
    gate=decision_gate(candidate(),{},validation(.10),risk(),min_resolved=10,discovery_comparison=comparison(),validation_comparison=comparison(),
        directional=direction(),temporal=temporal(),sensitivity_result=sensitivity(),walk_forward=walk())
    assert gate["paper_candidate_classification"]==STANDARD_PAPER_CANDIDATE

def test_experimental_metadata_is_paper_only_not_profit_certified():
    gate=decision_gate(candidate(),{},validation(),risk("MEDIUM",["DIRECTIONAL_INSTABILITY"]),min_resolved=10,
        discovery_comparison=comparison(),validation_comparison=comparison(),directional=direction(False),temporal=temporal(),sensitivity_result=sensitivity(),walk_forward=walk())
    assert gate["paper_only"] is True and gate["not_profit_certified"] is True

def test_insufficient_support_is_rejected():
    v=validation(); v["selected"]["resolved_binary"]=5
    assert classify(validation=v)["classification"]==REJECTED_CHALLENGER

def test_low_win_retention_is_rejected():
    v=validation(); v["win_retention"]=.59
    assert classify(validation=v)["classification"]==REJECTED_CHALLENGER

def test_single_loss_effect_is_rejected():
    v=validation(); v["losses_rejected"]=1
    assert classify(validation=v)["classification"]==REJECTED_CHALLENGER

def test_walk_forward_not_tested_is_rejected():
    assert classify(walk_forward=walk("NOT TESTED",0,0))["classification"]==REJECTED_CHALLENGER

def test_confidence_policy_never_grants_live_authority():
    for out in (classify(), classify(directional=direction(False),risk=risk("MEDIUM")), classify(risk=risk("HIGH"))):
        assert out["production_authority"] is False


def test_final_reliability_moderate_walk_forward_is_not_high():
    frozen={"validation_evidence":{"metrics":{"expectancy_delta_r":.10}}}
    analysis={"selected":{"resolved_binary":20},"outcome_effect":{"WIN":{"baseline":5},"LOSS":{"baseline":10,"blocked":3}},"expectancy_delta_r":.08}
    out=final_reliability(candidate(),frozen,analysis,direction(),temporal(),sensitivity(),{"pairwise_overlap":[]},walk("FAIL",.5,4))
    assert out["severity"]=="MEDIUM"

def test_final_reliability_severe_walk_forward_stays_high():
    frozen={"validation_evidence":{"metrics":{"expectancy_delta_r":.10}}}
    analysis={"selected":{"resolved_binary":20},"outcome_effect":{"WIN":{"baseline":5},"LOSS":{"baseline":10,"blocked":3}},"expectancy_delta_r":.08}
    out=final_reliability(candidate(),frozen,analysis,direction(),temporal(),sensitivity(),{"pairwise_overlap":[]},walk("FAIL",.25,4))
    assert out["severity"]=="HIGH"

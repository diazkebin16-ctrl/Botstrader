from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text(encoding='utf-8')
    if text.count(old) != 1:
        raise SystemExit(f'{path}: expected one match, found {text.count(old)}')
    p.write_text(text.replace(old, new, 1), encoding='utf-8')

# 1) Evidence-based materiality in incumbent comparison.
replace_once('automation_v3_incumbent_challenger.py', '''    exp=delta("expectancy_r"); pf=delta("profit_factor"); wr=delta("win_rate")
    resolved=int(challenger.get("resolved_binary") or 0)-int(incumbent.get("resolved_binary") or 0)
    beats=exp is not None and pf is not None and exp>0 and pf>0
    return {
''', '''    exp=delta("expectancy_r"); pf=delta("profit_factor"); wr=delta("win_rate")
    resolved=int(challenger.get("resolved_binary") or 0)-int(incumbent.get("resolved_binary") or 0)
    beats=exp is not None and pf is not None and exp>0 and pf>0
    incumbent_n=int(incumbent.get("resolved_binary") or 0); challenger_n=int(challenger.get("resolved_binary") or 0)
    common_support=min(x for x in (incumbent_n,challenger_n) if x>0) if incumbent_n>0 and challenger_n>0 else 0
    # A replacement must improve expectancy by at least one R-unit distributed
    # across the smaller same-partition support. This is sample-adaptive and
    # rejects sub-trade-granularity numerical noise without a fixed percentage.
    materiality_floor_r=(1.0/common_support) if common_support else None
    material=bool(beats and materiality_floor_r is not None and exp>=materiality_floor_r)
    return {
''')
replace_once('automation_v3_incumbent_challenger.py', '''        "challenger_beats_incumbent":beats,
        "material_improvement":beats,
        "materiality_basis":"EXPECTANCY_AND_PROFIT_FACTOR_IMPROVE_WITH_EXISTING_ROBUSTNESS_GATES",
''', '''        "challenger_beats_incumbent":beats,
        "material_improvement":material,
        "materiality_floor_r":materiality_floor_r,
        "materiality_support":common_support,
        "materiality_basis":"EXPECTANCY_DELTA_AT_LEAST_ONE_R_UNIT_OVER_SMALLER_SAME_PARTITION_SUPPORT_AND_PF_IMPROVES",
''')

# 2) Three-level confidence policy in Phase 2.
marker = '''def _robustness_pass(*, risk: Mapping[str, Any], directional: Mapping[str, Any], temporal: Mapping[str, Any], sensitivity_result: Mapping[str, Any], walk_forward: Mapping[str, Any]) -> bool:
'''
policy = '''STANDARD_PAPER_CANDIDATE = "STANDARD_PAPER_CANDIDATE"
EXPERIMENTAL_PAPER_CANDIDATE = "EXPERIMENTAL_PAPER_CANDIDATE"
REJECTED_CHALLENGER = "REJECTED_CHALLENGER"
# A walk-forward FAIL is considered moderate only when at least half of valid
# chronological folds retain positive edge. The existing STANDARD threshold is
# two-thirds; 50% is the explicit lower experimental envelope, not a PASS.
EXPERIMENTAL_MIN_WALK_FORWARD_POSITIVE_FRACTION = 0.50
EXPERIMENTAL_MIN_WALK_FORWARD_VALID_FOLDS = 2


def _walk_forward_experimental_ok(walk_forward: Mapping[str, Any]) -> bool:
    if walk_forward.get("status") == "PASS":
        return True
    if walk_forward.get("status") != "FAIL":
        return False
    return (
        int(walk_forward.get("valid_folds") or 0) >= EXPERIMENTAL_MIN_WALK_FORWARD_VALID_FOLDS
        and float(walk_forward.get("positive_fold_fraction") or 0.0) >= EXPERIMENTAL_MIN_WALK_FORWARD_POSITIVE_FRACTION
    )


def classify_paper_confidence(*, candidate: Mapping[str, Any], validation: Mapping[str, Any], risk: Mapping[str, Any],
                              discovery_comparison: Mapping[str, Any], validation_comparison: Mapping[str, Any],
                              directional: Mapping[str, Any], temporal: Mapping[str, Any],
                              sensitivity_result: Mapping[str, Any], walk_forward: Mapping[str, Any],
                              min_resolved: int) -> Dict[str, Any]:
    selected=validation.get("selected") or {}
    sensitivity_class=str(sensitivity_result.get("classification") or sensitivity_result.get("status") or "").upper().replace(" ","_")
    core_checks={
        "entry_time_only":candidate.get("entry_time_only") is True,
        "not_single_trade_rule":validation.get("losses_rejected",0)>=2,
        "minimum_validation_sample":selected.get("resolved_binary",0)>=min_resolved,
        "win_retention":validation.get("win_retention",0)>=0.60,
        "challenger_beats_incumbent_discovery":discovery_comparison.get("challenger_beats_incumbent") is True,
        "challenger_beats_incumbent_validation":validation_comparison.get("challenger_beats_incumbent") is True,
        "material_relative_improvement":validation_comparison.get("material_improvement") is True,
        "overfit_risk_not_high":str(risk.get("severity") or "").upper()!="HIGH",
    }
    hard_failures=[k for k,v in core_checks.items() if not v]
    if hard_failures:
        return {"classification":REJECTED_CHALLENGER,"confidence_class":"REJECTED","experimental":False,
                "eligible":False,"hard_failures":hard_failures,"warnings":[],"production_authority":False}
    if sensitivity_class in {"FRAGILE","NOT_TESTED"} or sensitivity_result.get("all_positive") is False:
        return {"classification":REJECTED_CHALLENGER,"confidence_class":"REJECTED","experimental":False,
                "eligible":False,"hard_failures":["sensitivity_not_credible"],"warnings":[],"production_authority":False}
    standard_checks={
        "directional_stability":directional.get("stable") is True,
        "temporal_stability":temporal.get("stable") is True,
        "sensitivity":sensitivity_class in {"STABLE","NOT_APPLICABLE"},
        "walk_forward_stability":walk_forward.get("status")=="PASS",
        "overfit_risk_low":str(risk.get("severity") or "").upper()=="LOW",
    }
    if all(standard_checks.values()):
        return {"classification":STANDARD_PAPER_CANDIDATE,"confidence_class":"STANDARD","experimental":False,
                "eligible":True,"hard_failures":[],"warnings":[],"production_authority":False}
    warnings=[k for k,v in standard_checks.items() if not v]
    if not _walk_forward_experimental_ok(walk_forward):
        return {"classification":REJECTED_CHALLENGER,"confidence_class":"REJECTED","experimental":False,
                "eligible":False,"hard_failures":["walk_forward_outside_experimental_envelope"],"warnings":warnings,
                "production_authority":False}
    if sensitivity_class not in {"STABLE","MODERATE","NOT_APPLICABLE"}:
        return {"classification":REJECTED_CHALLENGER,"confidence_class":"REJECTED","experimental":False,
                "eligible":False,"hard_failures":["sensitivity_outside_experimental_envelope"],"warnings":warnings,
                "production_authority":False}
    return {"classification":EXPERIMENTAL_PAPER_CANDIDATE,"confidence_class":"EXPERIMENTAL","experimental":True,
            "eligible":True,"hard_failures":[],"warnings":warnings,
            "reason_for_experimental":warnings,"paper_only":True,"not_profit_certified":True,
            "production_authority":False}


'''
replace_once('research_phase2.py', marker, policy + marker)

old_gate = '''    robust=_robustness_pass(risk=risk,directional=direction,temporal=time_stability,sensitivity_result=sens,walk_forward=wf)
    checks={
        "entry_time_only":candidate.get("entry_time_only") is True,
        "not_single_trade_rule":validation.get("losses_rejected",0)>=2,
        "minimum_validation_sample":selected.get("resolved_binary",0)>=min_resolved,
        "win_retention":validation.get("win_retention",0)>=0.60,
        "challenger_beats_incumbent_discovery":disc_comp.get("challenger_beats_incumbent") is True,
        "challenger_beats_incumbent_validation":val_comp.get("challenger_beats_incumbent") is True,
        "material_relative_improvement":val_comp.get("material_improvement") is True,
        "directional_stability":direction.get("stable") is True,
        "temporal_stability":time_stability.get("stable") is True,
        "sensitivity":str(sens.get("classification") or "").upper() in {"STABLE","NOT APPLICABLE","NOT_APPLICABLE"} and sens.get("all_positive") is not False,
        "walk_forward_stability":wf.get("status")=="PASS",
        "overfit_risk_not_high":risk.get("severity")!="HIGH",
    }
    deployable=all(checks.values())
    negative=(selected.get("expectancy_r") is not None and selected.get("expectancy_r")<0)
    return {
        "decision":"FREEZE_ELIGIBLE" if deployable else "REJECT",
        "checks":checks,"failed":[k for k,v in checks.items() if not v],
        "diagnostic_state":diagnostic_state(discovery_comparison=disc_comp,validation_comparison=val_comp,robust=robust,deployable=deployable),
        "comparison_contract":"ACTUAL_INCUMBENT_SAME_PARTITION",
        "paper_candidate_classification":"RELATIVE_IMPROVEMENT_PAPER_CANDIDATE" if deployable and negative else ("CHALLENGER_DEPLOYABLE" if deployable else None),
        "absolute_profitability":{"validation_expectancy_positive":selected.get("expectancy_r") is not None and selected.get("expectancy_r")>0,"validation_profit_factor_ge_1_05":(selected.get("profit_factor") or 0)>=1.05},
        "production_authority":False,
    }
'''
new_gate = '''    confidence=classify_paper_confidence(candidate=candidate,validation=validation,risk=risk,
        discovery_comparison=disc_comp,validation_comparison=val_comp,directional=direction,temporal=time_stability,
        sensitivity_result=sens,walk_forward=wf,min_resolved=min_resolved)
    deployable=confidence["eligible"] is True
    robust=confidence.get("confidence_class")=="STANDARD"
    negative=(selected.get("expectancy_r") is not None and selected.get("expectancy_r")<0)
    checks={
        "entry_time_only":candidate.get("entry_time_only") is True,
        "not_single_trade_rule":validation.get("losses_rejected",0)>=2,
        "minimum_validation_sample":selected.get("resolved_binary",0)>=min_resolved,
        "win_retention":validation.get("win_retention",0)>=0.60,
        "challenger_beats_incumbent_discovery":disc_comp.get("challenger_beats_incumbent") is True,
        "challenger_beats_incumbent_validation":val_comp.get("challenger_beats_incumbent") is True,
        "material_relative_improvement":val_comp.get("material_improvement") is True,
        "directional_stability":direction.get("stable") is True,
        "temporal_stability":time_stability.get("stable") is True,
        "sensitivity":str(sens.get("classification") or "").upper() in {"STABLE","NOT APPLICABLE","NOT_APPLICABLE"} and sens.get("all_positive") is not False,
        "walk_forward_stability":wf.get("status")=="PASS",
        "overfit_risk_not_high":risk.get("severity")!="HIGH",
    }
    paper_class=confidence["classification"]
    if deployable and negative:
        relative_label=("EXPERIMENTAL_RELATIVE_IMPROVEMENT_PAPER_CANDIDATE" if confidence.get("experimental") else "RELATIVE_IMPROVEMENT_PAPER_CANDIDATE")
    else:
        relative_label=paper_class if deployable else None
    return {
        "decision":"FREEZE_ELIGIBLE" if deployable else "REJECT",
        "checks":checks,"failed":confidence.get("hard_failures") or [k for k,v in checks.items() if not v],
        "diagnostic_state":diagnostic_state(discovery_comparison=disc_comp,validation_comparison=val_comp,robust=robust,deployable=deployable),
        "comparison_contract":"ACTUAL_INCUMBENT_SAME_PARTITION",
        "paper_candidate_classification":paper_class,
        "relative_improvement_classification":relative_label,
        "confidence_class":confidence.get("confidence_class"),"experimental":confidence.get("experimental") is True,
        "reason_for_experimental":list(confidence.get("reason_for_experimental") or []),
        "robustness_warnings":list(confidence.get("warnings") or []),
        "paper_only":bool(confidence.get("experimental")),"not_profit_certified":bool(confidence.get("experimental") or negative),
        "absolute_profitability":{"validation_expectancy_positive":selected.get("expectancy_r") is not None and selected.get("expectancy_r")>0,"validation_profit_factor_ge_1_05":(selected.get("profit_factor") or 0)>=1.05},
        "production_authority":False,
    }
'''
replace_once('research_phase2.py', old_gate, new_gate)

# Prefer STANDARD over EXPERIMENTAL in automatic selection; do not force a change.
old_sel = '''    eligible=[item for item in evaluated if item["decision_gate"]["decision"]=="FREEZE_ELIGIBLE"]
    eligible.sort(key=_candidate_rank,reverse=True)
    selected_components=[item["candidate"] for item in eligible[:3]]; proposed=_combine(selected_components) if selected_components else None; composite=None
    if proposed:
'''
new_sel = '''    eligible=[item for item in evaluated if item["decision_gate"]["decision"]=="FREEZE_ELIGIBLE"]
    standard=[item for item in eligible if (item.get("decision_gate") or {}).get("confidence_class")=="STANDARD"]
    experimental=[item for item in eligible if (item.get("decision_gate") or {}).get("confidence_class")=="EXPERIMENTAL"]
    standard.sort(key=_candidate_rank,reverse=True); experimental.sort(key=_candidate_rank,reverse=True)
    eligible=[*standard,*experimental]
    selected_components=[item["candidate"] for item in standard[:3]]; proposed=_combine(selected_components) if selected_components else None; composite=None
    if proposed:
'''
replace_once('research_phase2.py', old_sel, new_sel)
old_fallback = '''            if eligible:
                top=eligible[0]; composite={**top,"overlap_remove_one":overlap_remove_one(top["candidate"],validation_rows)}
    states=[str((item.get("decision_gate") or {}).get("diagnostic_state") or "") for item in evaluated]
'''
new_fallback = '''            if standard:
                top=standard[0]; composite={**top,"overlap_remove_one":overlap_remove_one(top["candidate"],validation_rows)}
    if composite is None and not standard and experimental:
        top=experimental[0]; composite={**top,"overlap_remove_one":overlap_remove_one(top["candidate"],validation_rows)}
    states=[str((item.get("decision_gate") or {}).get("diagnostic_state") or "") for item in evaluated]
'''
replace_once('research_phase2.py', old_fallback, new_fallback)

# Freeze confidence metadata.
replace_once('research_phase2.py', '''        "paper_candidate_classification": (proposed.get("decision_gate") or {}).get("paper_candidate_classification"),
        "candidate_definition": definition,
''', '''        "paper_candidate_classification": (proposed.get("decision_gate") or {}).get("paper_candidate_classification"),
        "confidence_class": (proposed.get("decision_gate") or {}).get("confidence_class"),
        "experimental": (proposed.get("decision_gate") or {}).get("experimental") is True,
        "reason_for_experimental": list((proposed.get("decision_gate") or {}).get("reason_for_experimental") or []),
        "paper_only": True,
        "not_profit_certified": (proposed.get("decision_gate") or {}).get("not_profit_certified") is True,
        "candidate_definition": definition,
''')

# Walk-forward FAIL may be medium only inside the governed experimental envelope.
replace_once('research_phase2.py', '''    if walk_forward.get("status") == "FAIL":
        reasons.append({"severity": "HIGH", "reason": "WALK_FORWARD_INSTABILITY"})
    elif walk_forward.get("status") == "NOT TESTED":
''', '''    if walk_forward.get("status") == "FAIL":
        if _walk_forward_experimental_ok(walk_forward):
            reasons.append({"severity": "MEDIUM", "reason": "WALK_FORWARD_INSTABILITY_EXPERIMENTAL_ENVELOPE"})
        else:
            reasons.append({"severity": "HIGH", "reason": "WALK_FORWARD_INSTABILITY"})
    elif walk_forward.get("status") == "NOT TESTED":
''')

# Holdout keeps untouched relative improvement as the decisive test; medium confidence is allowed only to PAPER.
old_hold = '''    reliability=final_reliability(definition,frozen,analysis,directional,temporal,sensitivity_result,overlap_result,walk_forward)
    robust=(directional.get("stable") is True and temporal.get("stable") is True and str(sensitivity_result.get("classification") or "").upper()!="FRAGILE" and walk_forward.get("status")=="PASS" and reliability.get("severity")!="HIGH")
    selected=analysis.get("selected") or {}; relative_ok=comparison.get("challenger_beats_incumbent") is True and comparison.get("material_improvement") is True
    status="PASS" if selected.get("resolved_binary",0)>=5 and (analysis.get("win_retention") or 0)>=0.50 and relative_ok and robust else "FAIL"
    negative=selected.get("expectancy_r") is not None and selected.get("expectancy_r")<0
    paper_policy="PAPER_EXPERIMENT_ONLY" if status=="PASS" and negative else ("STANDARD_PAPER_CANDIDATE" if status=="PASS" else None)
'''
new_hold = '''    reliability=final_reliability(definition,frozen,analysis,directional,temporal,sensitivity_result,overlap_result,walk_forward)
    robust=(directional.get("stable") is True and temporal.get("stable") is True and str(sensitivity_result.get("classification") or "").upper()!="FRAGILE" and walk_forward.get("status")=="PASS" and reliability.get("severity")!="HIGH")
    selected=analysis.get("selected") or {}; relative_ok=comparison.get("challenger_beats_incumbent") is True and comparison.get("material_improvement") is True
    evidence_ok=selected.get("resolved_binary",0)>=5 and (analysis.get("win_retention") or 0)>=0.50 and reliability.get("severity")!="HIGH"
    status="PASS" if evidence_ok and relative_ok else "FAIL"
    negative=selected.get("expectancy_r") is not None and selected.get("expectancy_r")<0
    pre_confidence=str(frozen.get("confidence_class") or "STANDARD").upper()
    holdout_confidence="STANDARD" if status=="PASS" and pre_confidence=="STANDARD" and robust else ("EXPERIMENTAL" if status=="PASS" else "REJECTED")
    paper_class=STANDARD_PAPER_CANDIDATE if holdout_confidence=="STANDARD" else (EXPERIMENTAL_PAPER_CANDIDATE if holdout_confidence=="EXPERIMENTAL" else REJECTED_CHALLENGER)
    paper_policy="PAPER_EXPERIMENT_ONLY" if status=="PASS" and (holdout_confidence=="EXPERIMENTAL" or negative) else ("STANDARD_PAPER_CANDIDATE" if status=="PASS" else None)
'''
replace_once('research_phase2.py', old_hold, new_hold)
replace_once('research_phase2.py', '''        "paper_release_policy":paper_policy,"relative_improvement":bool(status=="PASS" and relative_ok),"profit_certified":bool(status=="PASS" and not negative),"production_authority":False,
''', '''        "paper_release_policy":paper_policy,"paper_candidate_classification":paper_class,"confidence_class":holdout_confidence,
        "experimental":holdout_confidence=="EXPERIMENTAL","reason_for_experimental":list(frozen.get("reason_for_experimental") or []) if holdout_confidence=="EXPERIMENTAL" else [],
        "paper_only":True,"not_profit_certified":bool(holdout_confidence=="EXPERIMENTAL" or negative),
        "relative_improvement":bool(status=="PASS" and relative_ok),"profit_certified":bool(status=="PASS" and not negative and holdout_confidence=="STANDARD"),"production_authority":False,
''')

# candidate_record must not re-veto medium experimental after a passing holdout.
replace_once('research_phase2.py', '''    status = "RESEARCH_CANDIDATE" if holdout.get("status") == "PASS" and reliability.get("severity") != "HIGH" and robust else "REJECT"
''', '''    status = "RESEARCH_CANDIDATE" if holdout.get("status") == "PASS" and reliability.get("severity") != "HIGH" else "REJECT"
''')
replace_once('research_phase2.py', '''        "paper_release_policy": holdout.get("paper_release_policy"),
        "relative_improvement": holdout.get("relative_improvement") is True,
''', '''        "paper_release_policy": holdout.get("paper_release_policy"),
        "paper_candidate_classification": holdout.get("paper_candidate_classification"),
        "confidence_class": holdout.get("confidence_class"),
        "experimental": holdout.get("experimental") is True,
        "reason_for_experimental": list(holdout.get("reason_for_experimental") or []),
        "paper_only": True,
        "not_profit_certified": holdout.get("not_profit_certified") is True,
        "relative_improvement": holdout.get("relative_improvement") is True,
''')

# 3) Review output exposes confidence class and warnings.
replace_once('automation_v3_modes.py', '''        "paper_candidate_classification": gate.get("paper_candidate_classification"),
        "deployment_eligible": deployment_eligible, "pre_holdout_eligible": deployment_eligible,
''', '''        "paper_candidate_classification": gate.get("paper_candidate_classification"),
        "confidence_class": gate.get("confidence_class") or "REJECTED",
        "experimental": gate.get("experimental") is True,
        "reason_for_experimental": list(gate.get("reason_for_experimental") or []),
        "robustness_warnings": list(gate.get("robustness_warnings") or []),
        "deployment_eligible": deployment_eligible, "pre_holdout_eligible": deployment_eligible,
''')

# 4) Persist confidence metadata into active managed rules and release evidence.
replace_once('automation_v3_candidate_mapping.py', '''def _managed_payload(candidate_id: str, definition_sha: str, rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"candidate_definition_sha256": definition_sha, "candidate_id": candidate_id, **rule}
        for rule in rules
    ]
''', '''def _managed_payload(candidate_id: str, definition_sha: str, rules: list[dict[str, Any]], *, confidence_class: str, experimental: bool) -> list[dict[str, Any]]:
    return [
        {"candidate_definition_sha256": definition_sha, "candidate_id": candidate_id,
         "confidence_class": confidence_class, "experimental": bool(experimental), "paper_only": True, **rule}
        for rule in rules
    ]
''')
replace_once('automation_v3_candidate_mapping.py', '''    payload = _managed_payload(candidate_id, definition_sha, rules)
''', '''    confidence_class=str(holdout.get("confidence_class") or freeze.get("confidence_class") or "STANDARD").upper()
    experimental=confidence_class=="EXPERIMENTAL" or holdout.get("experimental") is True
    payload = _managed_payload(candidate_id, definition_sha, rules, confidence_class=confidence_class, experimental=experimental)
''')
replace_once('automation_v3_candidate_mapping.py', '''        "paper_release_policy": holdout.get("paper_release_policy"),
    }
''', '''        "paper_release_policy": holdout.get("paper_release_policy"),
        "confidence_class": confidence_class,
        "experimental": experimental,
        "reason_for_experimental": list(holdout.get("reason_for_experimental") or freeze.get("reason_for_experimental") or []),
    }
''')
replace_once('automation_v3_candidate_mapping.py', '''        "paper_release_policy": holdout.get("paper_release_policy"),
        "release_labels": (["PAPER_EXPERIMENT_ONLY","RELATIVE_IMPROVEMENT","NOT_PROFIT_CERTIFIED","PRODUCTION_AUTHORITY_FALSE"] if holdout.get("paper_release_policy")=="PAPER_EXPERIMENT_ONLY" else ["PRODUCTION_AUTHORITY_FALSE"]),
''', '''        "paper_release_policy": holdout.get("paper_release_policy"),
        "confidence_class": confidence_class,"experimental": experimental,"paper_only": True,
        "not_profit_certified": bool(holdout.get("not_profit_certified") or experimental),
        "reason_for_experimental": list(holdout.get("reason_for_experimental") or freeze.get("reason_for_experimental") or []),
        "release_labels": (["PAPER_EXPERIMENT_ONLY","RELATIVE_IMPROVEMENT","NOT_PROFIT_CERTIFIED","PRODUCTION_AUTHORITY_FALSE"] if holdout.get("paper_release_policy")=="PAPER_EXPERIMENT_ONLY" or experimental else ["PRODUCTION_AUTHORITY_FALSE"]),
''')

# 5) Operational no-change state remains explicit while preserving scientific terminal for review reporting.
replace_once('autonomous_asset_optimizer.py', '''      if diag["recommended_action"]=="NO_VALID_CANDIDATE":return self._terminal(ledger,i,"NO_VALID_CANDIDATE",diag["dominant_failure"],diagnostic=diag,pre_gate_diagnostic=pre_gate)
''', '''      if diag["recommended_action"]=="NO_VALID_CANDIDATE":return self._terminal(ledger,i,"NO_VALID_CANDIDATE",diag["dominant_failure"],diagnostic=diag,pre_gate_diagnostic=pre_gate,operational_decision="INCUMBENT_RETAINS_CONTROL")
''')
replace_once('autonomous_asset_optimizer.py', '''   if h.get("status")!="PASS" or (h.get("overfitting_risk") or {}).get("severity")=="HIGH" or pre.get("verdict") not in {"ACCEPT","ACCEPT WITH LIMITATIONS"} or not any(isinstance(x,Mapping) and x.get("status")=="RESEARCH_CANDIDATE" for x in ranking):return self._terminal(ledger,i,"NO_VALID_CANDIDATE","holdout/pre-audit did not establish PAPER candidate",lookback_months=months)
''', '''   if h.get("status")!="PASS" or (h.get("overfitting_risk") or {}).get("severity")=="HIGH" or pre.get("verdict") not in {"ACCEPT","ACCEPT WITH LIMITATIONS"} or not any(isinstance(x,Mapping) and x.get("status")=="RESEARCH_CANDIDATE" for x in ranking):return self._terminal(ledger,i,"NO_VALID_CANDIDATE","holdout/pre-audit did not establish PAPER candidate",lookback_months=months,operational_decision="INCUMBENT_RETAINS_CONTROL")
''')

# 6) Focused policy tests.
Path('test_automation_v3_adaptive_paper_confidence.py').write_text(r'''import copy
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
''', encoding='utf-8')

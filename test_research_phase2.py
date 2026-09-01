import json
from datetime import datetime, timedelta, timezone

import pytest

from research_pipeline import analyze_phase1
from research_phase2 import (
    automatic_pre_audit, automatic_report, candidate_analysis, discover_candidates,
    evaluate_holdout, freeze_candidate, generate_ai_prompts, prepare_phase2,
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


def test_phase2_preserves_non_binary_outcomes_and_freezes_before_holdout(tmp_path):
    target=tmp_path/"target.json"
    _write(target,{"instrument":"AUD_USD","variant":"V331_BASELINE","lookahead_protection":True,
                   "dataset_identity":{"status":"PASS","code_sha":"abc"},"episodes":_rows()})
    phase1=analyze_phase1(str(target),discovery_only=True,horizon_minutes=5,embargo_minutes=0)
    assert phase1["all_target_wins_recovered"] is True
    phase1_path=tmp_path/"phase1.json";_write(phase1_path,phase1)
    phase2=prepare_phase2(str(target),str(phase1_path),horizon_minutes=5,embargo_minutes=0)
    phase2_path=tmp_path/"phase2.json";_write(phase2_path,phase2)
    discovery=discover_candidates(str(target),str(phase2_path),min_resolved=5)
    assert discovery["status"]=="OK"
    assert discovery["holdout_opened"] is False
    freeze_path=tmp_path/"freeze.json"
    discovery_path=tmp_path/"discovery.json";_write(discovery_path,discovery)
    frozen=freeze_candidate(str(discovery_path),freeze_path);_write(freeze_path,frozen)
    assert frozen["immutable"] is True
    holdout=evaluate_holdout(str(target),str(phase2_path),str(freeze_path))
    effects=holdout["analysis"]["outcome_effect"]
    assert effects["TIMEOUT"]["baseline"]>=0
    assert effects["AMBIGUOUS"]["baseline"]>=0
    assert effects["PENDING"]["baseline"]>=0
    assert holdout["retuning_after_holdout"] is False
    assert holdout["candidate_ranking"][0]["status"] in {"REJECT","RESEARCH_CANDIDATE"}


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
    integrity=tmp_path/"integrity.json";_write(integrity,{"status":"PASS","instrument":"AUD_USD","input_sha256":"data","start":"s","end":"e","warmup_days":10,"horizon_minutes":240,"bid_ask_real":True})
    phase1=tmp_path/"phase1.json";_write(phase1,{"all_target_wins_recovered":True})
    phase2=tmp_path/"phase2.json";_write(phase2,{"instrument":"AUD_USD","partitions":{"discovery":{"episodes":1},"validation":{"episodes":1},"holdout":{"episodes":1}},"safety_risk_global_gates":"SEPARATE_IMMUTABLE_NOT_CANDIDATE_FEATURES"})
    discovery=tmp_path/"discovery.json";_write(discovery,{"status":"OK","candidate_space":{},"discovery_metrics":{"outcomes":{}},"m1_internals":{}})
    freeze=tmp_path/"freeze.json";_write(freeze,{"immutable":True,"candidate_id":"c1","candidate_definition":{"rules":[]}})
    holdout=tmp_path/"holdout.json";_write(holdout,{"status":"PASS","retuning_after_holdout":False,"freeze_sha256":"f","analysis":{},"directional_stability":{"stable":True},"temporal_stability":{"stable":True},"sensitivity":{"classification":"STABLE"},"walk_forward_stability":{"status":"PASS"},"overlap_remove_one":{},"overfitting_risk":{"severity":"LOW","reasons":[]},"candidate_ranking":[{"candidate_id":"c1","status":"RESEARCH_CANDIDATE"}]})
    determinism=tmp_path/"determinism.json";_write(determinism,{"status":"PASS"})
    audit=tmp_path/"audit.json";_write(audit,{"status":"PASS","production_modifications":"NONE","package":{"failures":[]}})
    report=automatic_report(str(integrity),str(phase1),str(phase2),str(discovery),str(freeze),str(holdout),str(determinism),str(audit))
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

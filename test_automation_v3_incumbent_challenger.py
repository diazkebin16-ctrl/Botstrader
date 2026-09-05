
import pytest
from automation_v3_incumbent_challenger import build_incumbent_definition, compare_metrics, diagnostic_state
from research_phase2 import decision_gate

def m(exp,pf,wr=.4,res=100): return {"expectancy_r":exp,"profit_factor":pf,"win_rate":wr,"resolved_binary":res,"wins":40,"losses":60,"episodes":110}
def analysis(exp,pf,delta=.1): return {"selected":m(exp,pf),"losses_rejected":10,"win_retention":.8,"expectancy_delta_r":delta}
def robust_args():
    return dict(risk={"severity":"LOW"},directional={"stable":True},temporal={"stable":True},sensitivity_result={"classification":"STABLE","all_positive":True},walk_forward={"status":"PASS"})
def comp(inc_exp,inc_pf,ch_exp,ch_pf): return compare_metrics(incumbent=m(inc_exp,inc_pf),challenger=m(ch_exp,ch_pf),evaluation_population_sha256='a'*64)
def gate(dexp,ve,ip=-.3,ipf=.6,vpf=.8,**over):
    dc=comp(ip,ipf,dexp,max(ipf+.01,.61)); vc=comp(ip,ipf,ve,vpf); kw=robust_args(); kw.update(over)
    return decision_gate({"entry_time_only":True},analysis(dexp,max(ipf+.01,.61)),analysis(ve,vpf),kw.pop('risk'),min_resolved=10,discovery_comparison=dc,validation_comparison=vc,**kw)

def test_worse_than_incumbent_rejected_even_if_absolute_positive():
    g=gate(.2,.1,ip=.3,ipf=1.5,vpf=1.2); assert g['decision']=='REJECT'; assert 'challenger_beats_incumbent_validation' in g['failed']
def test_positive_material_better_robust_advances(): assert gate(.2,.15,ip=-.2,ipf=.6,vpf=1.2)['decision']=='FREEZE_ELIGIBLE'
def test_negative_but_better_robust_is_relative_paper_candidate():
    g=gate(-.1,-.1,ip=-.3,ipf=.5,vpf=.8); assert g['decision']=='FREEZE_ELIGIBLE'; assert g['paper_candidate_classification']=='RELATIVE_IMPROVEMENT_PAPER_CANDIDATE'
def test_better_discovery_worse_validation_rejected(): assert gate(-.1,-.4,ip=-.3,ipf=.7,vpf=.5)['decision']=='REJECT'
def test_high_overfit_better_is_not_robust():
    g=gate(-.1,-.1,risk={"severity":"HIGH"},directional={"stable":True},temporal={"stable":True},sensitivity_result={"classification":"STABLE","all_positive":True},walk_forward={"status":"PASS"}); assert g['decision']=='REJECT'; assert g['diagnostic_state']=='CHALLENGER_BETTER_BUT_NOT_ROBUST'
def test_stability_fail_blocks(): assert gate(-.1,-.1,directional={"stable":False})['decision']=='REJECT'
def test_sensitivity_fail_blocks(): assert gate(-.1,-.1,sensitivity_result={"classification":"FRAGILE","all_positive":False})['decision']=='REJECT'
def test_walk_forward_fail_blocks(): assert gate(-.1,-.1,walk_forward={"status":"FAIL"})['decision']=='REJECT'
def test_population_identity_required():
    with pytest.raises(ValueError): compare_metrics(incumbent=m(-.2,.7),challenger=m(-.1,.8),evaluation_population_sha256='')
def test_unknown_incumbent_identity_fails_closed():
    with pytest.raises(ValueError): build_incumbent_definition(instrument='GBP_USD',code_sha='',dataset_identity={},managed_rules=[],methodology={})
def test_dataset_code_mismatch_fails_closed():
    with pytest.raises(ValueError): build_incumbent_definition(instrument='GBP_USD',code_sha='a'*40,dataset_identity={'code_sha':'b'*40},managed_rules=[],methodology={})
def test_lower_win_rate_higher_expectancy_can_beat_incumbent():
    c=compare_metrics(incumbent=m(-.2,.6,.5),challenger=m(-.1,.8,.4),evaluation_population_sha256='a'*64); assert c['challenger_beats_incumbent'] is True and c['win_rate_delta_vs_incumbent']<0
def test_higher_win_rate_worse_expectancy_does_not_win():
    c=compare_metrics(incumbent=m(.1,1.2,.4),challenger=m(.05,1.3,.5),evaluation_population_sha256='a'*64); assert c['challenger_beats_incumbent'] is False
def test_absolute_positive_but_worse_rejected(): assert gate(.2,.1,ip=.3,ipf=1.4,vpf=1.2)['decision']=='REJECT'
def test_diagnostic_better_not_robust(): assert diagnostic_state(discovery_comparison={'challenger_beats_incumbent':True},validation_comparison={'challenger_beats_incumbent':True},robust=False,deployable=False)=='CHALLENGER_BETTER_BUT_NOT_ROBUST'
def test_production_authority_false_in_identity_and_comparison():
    i=build_incumbent_definition(instrument='GBP_USD',code_sha='a'*40,dataset_identity={'code_sha':'a'*40},managed_rules=[],methodology={}); c=comp(-.2,.6,-.1,.8); assert i['production_authority'] is False and c['production_authority'] is False
def test_incumbent_hash_deterministic():
    kw=dict(instrument='GBP_USD',code_sha='a'*40,dataset_identity={'code_sha':'a'*40},managed_rules=[],methodology={'x':1}); assert build_incumbent_definition(**kw)['incumbent_definition_sha256']==build_incumbent_definition(**kw)['incumbent_definition_sha256']
def test_negative_relative_policy_does_not_forge_profitability():
    g=gate(-.1,-.1,ip=-.3,ipf=.5,vpf=.8); assert g['absolute_profitability']['validation_expectancy_positive'] is False and g['paper_candidate_classification']=='RELATIVE_IMPROVEMENT_PAPER_CANDIDATE'

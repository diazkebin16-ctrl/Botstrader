import tempfile,os
from capital_allocation import CapitalAllocationEngine

def eng(**kw):
 p=tempfile.mktemp(suffix='.db');e=CapitalAllocationEngine(p,**kw);e.ensure_schema();return e,p
def item(s,f='F1',req=25,edge=2,rel=.8,n=100,vol=.6,dd=.02,eq=.9,conf=.7,reg=.8,cluster=None,d='LONG'):
 return dict(strategy_id=s,family=f,symbol=s+'USD',asset_class='FX',direction=d,requested_risk=req,expected_net_edge=edge,reliability=rel,calibration=.85,sample_size=n,stability=.8,volatility=vol,drawdown=dd,tail_risk=.3,execution_quality=eq,ensemble_confidence=conf,regime_compatibility=reg,recent_performance=.2,data_quality=1,degraded=False,cluster=cluster)
def test_budget_never_exceeded():
 e,p=eng();r=e.allocate([item('A'),item('B'),item('C')],100);assert r['used_risk_budget']<=100;os.unlink(p)
def test_unused_budget_allowed():
 e,p=eng();r=e.allocate([item('A',req=5)],100);assert r['unused_risk_budget']>=95;os.unlink(p)
def test_correlated_cluster_capped():
 e,p=eng(max_cluster_risk=.30);xs=[item('A',cluster='X'),item('B',cluster='X'),item('C',cluster='X')];r=e.allocate(xs,100);assert sum(r['allocations'].values())<=30.0001;assert 'HIDDEN_CONCENTRATION_DETECTED' in r['alerts'];os.unlink(p)
def test_winner_cannot_take_all():
 e,p=eng(max_strategy_allocation=.25);xs=[item('A',edge=100,rel=1),item('B',edge=.2),item('C',edge=.2)];r=e.allocate(xs,100);assert r['allocations']['A']<=25.0001;os.unlink(p)
def test_low_evidence_penalty():
 e,p=eng(heat_limit=1,max_directional_risk=.9,max_asset_risk=.8,max_strategy_allocation=.5,max_family_risk=.8);r=e.allocate([item('A',f='F1',n=2,req=100),item('B',f='F2',n=100,req=100)],100);assert r['strategies']['A']['reliability_adjusted_edge']<r['strategies']['B']['reliability_adjusted_edge'];os.unlink(p)
def test_degradation_reduces():
 e,p=eng(heat_limit=1,max_directional_risk=.9,max_asset_risk=.8,max_strategy_allocation=.5,max_family_risk=.8);a=item('A',f='F1',req=100);b=item('B',f='F2',req=100);a['degraded']=True;r=e.allocate([a,b],100);assert r['strategies']['A']['allocation_confidence']<r['strategies']['B']['allocation_confidence'];os.unlink(p)
def test_volatility_normalization():
 e,p=eng();r=e.allocate([item('A',vol=2),item('B',vol=.4)],100);assert r['strategies']['A']['reliability_adjusted_edge']<=r['strategies']['B']['reliability_adjusted_edge'];os.unlink(p)
def test_drawdown_risk_off():
 e,p=eng();r=e.allocate([item('A'),item('B')],100,portfolio_drawdown=.09);assert r['risk_off'] and r['used_risk_budget']==0;os.unlink(p)
def test_execution_degraded_risk_off():
 e,p=eng();r=e.allocate([item('A')],100,execution_degraded=True);assert r['used_risk_budget']==0;os.unlink(p)
def test_governance_freeze_risk_off():
 e,p=eng();r=e.allocate([item('A')],100,governance_frozen=True);assert r['risk_off'];os.unlink(p)
def test_no_martingale_after_loss():
 e,p=eng(heat_limit=1,max_directional_risk=.9,max_asset_risk=.8,max_strategy_allocation=.5,max_family_risk=.8);a=item('A',f='F1',dd=.30,req=100);b=item('B',f='F2',dd=.01,req=100);r=e.allocate([a,b],100);assert r['used_risk_budget']<=100 and r['allocations']['A']<=a['requested_risk'];os.unlink(p)
def test_heat_limit_reduces():
 e,p=eng(heat_limit=.30);r=e.allocate([item('A'),item('B')],100);assert r['portfolio_heat']<=.3001;os.unlink(p)
def test_risk_engine_veto_property():
 e,p=eng();r=e.allocate([item('A',req=25)],8);assert r['used_risk_budget']<=8;os.unlink(p)
def test_engine_has_no_order_authority():
 e,p=eng();r=e.allocate([item('A')],100);assert not r['order_authority'] and not r['signal_authority'] and not r['risk_limit_authority'];os.unlink(p)
def test_candidate_never_autodeploys():
 e,p=eng();c=e.candidate_policy('current',{},{});assert c['auto_deploy'] is False and c['path'][0]=='SHADOW';os.unlink(p)
def test_correlation_spike_stress_detected():
 e,p=eng();xs=[item('A',cluster='X'),item('B',cluster='X'),item('C',cluster='X')];r=e.allocate(xs,100);assert r['stress_heat']>=r['portfolio_heat'];os.unlink(p)
def test_critical_winning_strategy():
 e,p=eng(max_strategy_allocation=.20,max_change_per_cycle=.05);r=e.allocate([item('HOT',edge=999,rel=1),item('B'),item('C')],100);assert r['allocations']['HOT']<=20.0001;os.unlink(p)
def test_critical_drawdown_never_increases_to_recover():
 e,p=eng();normal=e.allocate([item('A'),item('B')],100);loss=e.allocate([item('A',dd=.5),item('B',dd=.5)],100,portfolio_drawdown=.09);assert loss['used_risk_budget']<=normal['used_risk_budget'] and loss['used_risk_budget']==0;os.unlink(p)

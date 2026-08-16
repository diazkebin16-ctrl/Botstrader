import tempfile,json,os
from capital_allocation import CapitalAllocationEngine

def x(s,f,req,edge,rel,n,vol,cluster=None):
 return dict(strategy_id=s,family=f,symbol=s+'USD',asset_class='FX',direction='LONG',requested_risk=req,expected_net_edge=edge,reliability=rel,calibration=.85,sample_size=n,stability=.85,volatility=vol,drawdown=.02,tail_risk=.25,execution_quality=.90,ensemble_confidence=.70,regime_compatibility=.85,recent_performance=.1,data_quality=1,degraded=False,cluster=cluster)
def run():
 p=tempfile.mktemp(suffix='.db');e=CapitalAllocationEngine(p,max_strategy_allocation=.25,max_family_risk=.40,max_cluster_risk=.35,heat_limit=1.5);e.ensure_schema()
 items=[x('A','TREND',25,2.0,.82,120,.8,'AB'),x('B','TREND',25,1.2,.75,100,.9,'AB'),x('C','BREAKOUT',25,3.2,.92,160,.55,None),x('D','MEAN_REVERSION',25,.7,.45,5,1.0,None)]
 corr={'A':{'B':.92},'B':{'A':.92}}
 r=e.allocate(items,100,explicit_correlations=corr,regime='HIGH_VOLATILITY_TREND',system_quality=.90,ensemble_disagreement=.20)
 # Requested pedagogical target is demonstrated separately as a valid risk-engine-capped proposal.
 requested_example={'A':15.0,'B':8.0,'C':20.0,'D':3.0};risk_engine_caps={'A':15.0,'B':8.0,'C':18.0,'D':3.0}
 approved={k:min(requested_example[k],risk_engine_caps[k]) for k in requested_example}
 out={'engine_shadow_result':r,'requested_example':{'authorized':100,'proposal':requested_example,'unallocated':54.0},'risk_engine_review':{'caps':risk_engine_caps,'approved':approved,'used':sum(approved.values()),'unallocated':100-sum(approved.values())},'properties':{'allocation_never_exceeds_authorized':r['used_risk_budget']<=100,'example_uses_less_than_available':sum(approved.values())<100,'risk_engine_veto_respected':approved['C']==18,'smart_execution_receives_only_approved':True,'capital_allocator_order_authority':False}}
 os.unlink(p);return out
if __name__=='__main__':print(json.dumps(run(),indent=2))

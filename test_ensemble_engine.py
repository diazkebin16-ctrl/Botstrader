import os,tempfile,json
from datetime import datetime,timezone,timedelta
from ensemble_engine import EnsembleEngine

def engine(**kw):
    p=tempfile.mktemp(suffix='.db');e=EnsembleEngine(p,mode='SHADOW',**kw);e.ensure_schema();return e,p

def sig(i,d='LONG',c=.7,f='FAM',deps=None,role='DIRECTIONAL',ts=None,status='ONLINE',edge=5.0,h='INTRADAY'):
    return {'strategy_id':i,'strategy_version':i+'@1','symbol':'EUR_USD','timestamp':ts or datetime.now(timezone.utc).isoformat(),
            'direction':d,'confidence':c,'expected_edge':edge,'market_regime':'BULLISH_TREND','time_horizon':h,
            'signal_strength':c,'risk_characteristics':{},'data_quality':1.0,'family':f,'input_dependencies':deps or [i],
            'role':role,'ttl_seconds':300,'status':status}

def test_all_agree():
    e,p=engine(min_active_directional=2)
    r=e.evaluate([sig('A'),sig('B',c=.8,f='F2'),sig('C',c=.6,f='F3')],regime='BULLISH_TREND',execution_cost=1)
    assert r['ensemble_direction']=='LONG'
    assert r['agreement_score']>.95
    assert r['expected_net_edge']>0
    os.remove(p)

def test_all_disagree_abstains():
    e,p=engine(min_active_directional=2)
    r=e.evaluate([sig('A','LONG',.8,'F1'),sig('B','SHORT',.8,'F2')],regime='RANGE')
    assert r['ensemble_direction']=='ABSTAIN'
    assert 'ENSEMBLE_CONFLICT' in r['reasoning_summary']
    os.remove(p)

def test_one_high_confidence_cannot_dominate():
    e,p=engine(max_model_weight=.35,min_active_directional=2)
    r=e.evaluate([sig('A','LONG',.99,'F1'),sig('B','SHORT',.6,'F2'),sig('C','SHORT',.6,'F3')],regime='RANGE')
    assert r['weights']['A']<=.35+1e-12
    assert r['ensemble_confidence']<.99
    os.remove(p)

def test_critical_five_correlated_long_vs_one_independent_short():
    e,p=engine(max_model_weight=.35,max_family_weight=.50,correlation_threshold=.70,min_active_directional=2)
    deps=['PRICE','EMA','ATR','TREND']
    xs=[sig(f'T{i}','LONG',.85,'TREND_FAMILY',deps) for i in range(5)]
    xs.append(sig('MR','SHORT',.80,'MEAN_REVERSION',['PRICE','RSI','RANGE']))
    r=e.evaluate(xs,regime='HIGH_VOLATILITY',execution_cost=.5)
    trend_weight=sum(v for k,v in r['weights'].items() if k.startswith('T'))
    assert trend_weight<=.50+1e-9
    assert r['weights']['MR']>0
    # 5 correlated votes must not become enormous confidence.
    assert r['ensemble_confidence']<.75
    assert r['diversity_score']<.80
    assert r['correlation']['high_pairs']
    os.remove(p)

def test_independent_contradiction_counts_more_than_duplicate_vote():
    e,p=engine(max_family_weight=.5,correlation_threshold=.7,min_active_directional=2)
    xs=[sig('A','LONG',.8,'TREND',['P','EMA']),sig('B','LONG',.8,'TREND',['P','EMA']),
        sig('C','SHORT',.8,'MEANREV',['P','RSI'])]
    r=e.evaluate(xs,regime='RANGE')
    assert sum(r['weights'][x] for x in ('A','B'))<=.5+1e-9
    assert r['disagreement_score']>0
    os.remove(p)

def test_offline_model_reduces_information():
    e,p=engine(min_active_directional=2)
    r=e.evaluate([sig('A','LONG'),sig('B','LONG',status='OFFLINE',f='F2')],regime='RANGE')
    assert 'B' in r['offline_models']
    assert r['ensemble_direction']=='ABSTAIN'
    assert 'INSUFFICIENT_ENSEMBLE_INFORMATION' in r['reasoning_summary']
    os.remove(p)

def test_stale_signals_do_not_vote():
    e,p=engine(min_active_directional=2)
    old=(datetime.now(timezone.utc)-timedelta(hours=2)).isoformat()
    a=sig('A','LONG',ts=old);a['ttl_seconds']=60
    r=e.evaluate([a,sig('B','SHORT',f='F2')],regime='RANGE')
    assert 'A' in r['abstaining_models']
    assert r['ensemble_direction']=='ABSTAIN'
    os.remove(p)

def test_data_quality_never_increases_confidence():
    e,p=engine(min_active_directional=2)
    base=[sig('A','LONG',.8,'F1'),sig('B','LONG',.8,'F2')]
    good=e.evaluate(base,regime='RANGE')
    bad=[dict(x,data_quality=.3) for x in base]
    low=e.evaluate(bad,regime='RANGE')
    assert low['ensemble_confidence']<=good['ensemble_confidence']
    os.remove(p)

def test_regime_specific_reliability_with_minimum_evidence():
    e,p=engine(min_sample_size=10,min_active_directional=2,max_model_weight=.60,max_family_weight=.80)
    c=e.conn();now=datetime.now(timezone.utc)
    # A good in trend, B poor in trend; historical rows all precede current signal.
    for i in range(20):
        ts=(now-timedelta(days=30-i)).isoformat()
        for mid,label in [('A',1 if i<16 else 0),('B',1 if i<6 else 0)]:
            c.execute("""INSERT INTO ensemble_signals(signal_id,strategy_id,strategy_version,symbol,ts,direction,confidence,
             market_regime,time_horizon,signal_strength,risk_characteristics_json,data_quality,family,input_dependencies_json,
             role,ttl_seconds,status,metadata_json,resolved_label,resolved_return) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
             (f'{mid}{i}',mid,mid+'@1','EUR_USD',ts,'LONG',.7,'BULLISH_TREND','INTRADAY',.7,'{}',1,mid+'F','[]','DIRECTIONAL',300,'ONLINE','{}',label,1 if label else -1))
    c.commit();c.close()
    r=e.evaluate([sig('A','LONG',.7,'AF'),sig('B','LONG',.7,'BF')],regime='BULLISH_TREND')
    assert r['weights']['A']>r['weights']['B']
    os.remove(p)

def test_calibrator_cannot_create_direction():
    e,p=engine(min_active_directional=2)
    cal=sig('ML','LONG',.99,'CAL',['TECH'],role='CALIBRATOR')
    r=e.evaluate([sig('A','ABSTAIN'),cal],regime='RANGE')
    assert r['ensemble_direction']=='ABSTAIN'
    os.remove(p)

def test_time_horizon_mismatch_not_false_conflict():
    e,p=engine(min_active_directional=1)
    r=e.evaluate([sig('FAST','LONG',h='5M'),sig('SLOW','SHORT',f='F2',h='3D')],target_horizon='5M',regime='RANGE')
    assert 'SLOW' in r['abstaining_models']
    assert r['ensemble_direction']=='LONG'
    os.remove(p)

def test_execution_cost_can_remove_edge():
    e,p=engine(min_active_directional=2)
    r=e.evaluate([sig('A','LONG',edge=2),sig('B','LONG',f='F2',edge=2)],regime='RANGE',execution_cost=3)
    assert r['expected_net_edge']<0
    assert r['ensemble_direction']=='ABSTAIN'
    assert 'NO_CLEAR_EDGE_AFTER_EXECUTION_COSTS' in r['reasoning_summary']
    os.remove(p)

def test_weight_candidate_never_auto_deploys():
    e,p=engine();x=e.candidate_weights('w1',{'A':.4,'B':.6},{'samples':100})
    assert x['auto_deploy'] is False
    assert x['required_path']==['VALIDATION','PAPER','CANARY','APPROVAL']
    os.remove(p)

def test_value_added_requires_evidence():
    e,p=engine(min_sample_size=10)
    assert e.value_added()['status']=='NO_ENSEMBLE_ADVANTAGE_DETECTED'
    assert e.value_added()['evidence']=='INSUFFICIENT_DATA'
    os.remove(p)

def test_weight_change_limit_and_cooldown():
    e,p=engine(weight_change_limit=.05,weight_cooldown_hours=24,min_active_directional=2)
    a=e.evaluate([sig('A','LONG',.9,'F1'),sig('B','LONG',.5,'F2')],regime='RANGE')
    b=e.evaluate([sig('A','LONG',.1,'F1'),sig('B','LONG',.99,'F2')],regime='RANGE')
    c=e.conn();rows=c.execute('SELECT weights_json FROM ensemble_weight_versions ORDER BY created_at').fetchall();c.close()
    import json
    w1=json.loads(rows[-2]['weights_json']);w2=json.loads(rows[-1]['weights_json'])
    for k in set(w1)&set(w2):assert abs(w2[k]-w1[k])<=.0500001
    os.remove(p)


def test_performance_metrics_and_degradation_are_shadow_evidence_only():
    e,p=engine(min_sample_size=5)
    import sqlite3,json
    now=datetime.now(timezone.utc)
    c=e.conn()
    # Historical healthy, recent degraded. Insert only explicitly resolvable shadow comparisons.
    for i in range(6):
        ts=(now-timedelta(days=15-i)).isoformat();did=f'H{i}'
        c.execute("""INSERT INTO ensemble_outputs(ensemble_decision_id,ensemble_cycle_id,symbol,ts,mode,method,
          ensemble_direction,ensemble_confidence,agreement_score,disagreement_score,diversity_score,market_regime,data_quality,
          participating_models_json,abstaining_models_json,offline_models_json,model_contributions_json,correlation_json,
          families_json,reasoning_summary_json,hypothetical_only) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
          (did,did,'EUR_USD',ts,'SHADOW','REGIME_WEIGHTED','LONG',.7,.8,.2,.7,'RANGE',1,'[]','[]','[]','[]','{}','{}','[]'))
        c.execute("""INSERT INTO ensemble_shadow_comparisons(comparison_id,ensemble_decision_id,ts,current_direction,
          current_executed,ensemble_direction,ensemble_confidence,actual_result,hypothetical_result,details_json)
          VALUES(?,?,?,?,?,?,?,?,?,?)""",(f'C{did}',did,ts,'LONG',1,'LONG',.7,.4,.8,'{}'))
    for i in range(6):
        ts=(now-timedelta(days=2,hours=i)).isoformat();did=f'R{i}'
        c.execute("""INSERT INTO ensemble_outputs(ensemble_decision_id,ensemble_cycle_id,symbol,ts,mode,method,
          ensemble_direction,ensemble_confidence,agreement_score,disagreement_score,diversity_score,market_regime,data_quality,
          participating_models_json,abstaining_models_json,offline_models_json,model_contributions_json,correlation_json,
          families_json,reasoning_summary_json,hypothetical_only) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
          (did,did,'EUR_USD',ts,'SHADOW','REGIME_WEIGHTED','LONG',.8,.8,.2,.7,'RANGE',1,'[]','[]','[]','[]','{}','{}','[]'))
        c.execute("""INSERT INTO ensemble_shadow_comparisons(comparison_id,ensemble_decision_id,ts,current_direction,
          current_executed,ensemble_direction,ensemble_confidence,actual_result,hypothetical_result,details_json)
          VALUES(?,?,?,?,?,?,?,?,?,?)""",(f'C{did}',did,ts,'LONG',1,'LONG',.8,.2,-.8,'{}'))
    c.commit();c.close()
    m=e.performance_metrics(days=30)
    d=e.degradation(recent_days=7,historical_days=30)
    assert m['ensemble']['samples']==12
    assert m['ensemble_brier'] is not None
    assert d['status']=='ENSEMBLE_DEGRADATION_DETECTED'
    assert d['causal_claim'] is False
    os.remove(p)


def test_return_correlation_can_identify_duplicate_evidence_without_shared_features():
    e,p=engine(correlation_threshold=.70,min_active_directional=2)
    # Seed matched resolved returns with distinct declared feature names.
    c=e.conn();base=datetime.now(timezone.utc)-timedelta(minutes=40)
    for i in range(8):
        ts=(base+timedelta(minutes=i)).isoformat();ret=1.0 if i%2==0 else -1.0
        for m,deps in [('A',['FEATURE_A']),('B',['FEATURE_B'])]:
            c.execute("""INSERT INTO ensemble_signals(signal_id,ensemble_cycle_id,strategy_id,strategy_version,symbol,ts,direction,
              confidence,expected_edge,market_regime,time_horizon,signal_strength,risk_characteristics_json,data_quality,family,
              input_dependencies_json,role,ttl_seconds,status,metadata_json,resolved_label,resolved_return,resolved_ts)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (f'{m}{i}',f'c{i}',m,'v1','EUR_USD',ts,'LONG' if ret>0 else 'SHORT',.7,1,'RANGE','INTRADAY',.7,'{}',1,
               f'F{m}',json.dumps(deps),'DIRECTIONAL',300,'ONLINE','{}',1 if ret>0 else 0,ret,ts))
    c.commit();c.close()
    now=datetime.now(timezone.utc).isoformat()
    xs=[sig('A','LONG',.7,'FA',['FEATURE_A'],ts=now),sig('B','LONG',.7,'FB',['FEATURE_B'],ts=now)]
    std=[e.standardize(x) for x in xs]
    corr=e.correlation_matrix(std,now)
    detail=corr['pair_details']['A|B']
    assert detail['feature_similarity']==0
    assert detail['return_correlation'] is not None and abs(detail['return_correlation'])>.99
    assert corr['high_pairs']
    os.remove(p)

if __name__=='__main__':
    tests=[v for k,v in sorted(globals().items()) if k.startswith('test_') and callable(v)]
    for t in tests:t()
    print(f'ensemble engine tests: OK ({len(tests)})')


def test_missing_execution_cost_does_not_fake_net_edge_or_cost_reason():
    e,p=engine(min_active_directional=1)
    r=e.evaluate([sig('A','LONG',edge=-2)],regime='RANGE',execution_cost=None)
    assert r['weighted_expected_edge'] < 0
    assert r['expected_execution_cost'] is None
    assert r['execution_cost_available'] is False
    assert r['expected_net_edge'] is None
    assert r['net_edge_evaluable'] is False
    assert 'NO_CLEAR_GROSS_EDGE' in r['reasoning_summary']
    assert 'NO_CLEAR_EDGE_AFTER_EXECUTION_COSTS' not in r['reasoning_summary']
    os.remove(p)


def test_positive_gross_edge_without_cost_reports_cost_unavailable():
    e,p=engine(min_active_directional=1)
    r=e.evaluate([sig('A','LONG',edge=2)],regime='RANGE',execution_cost=None)
    assert r['weighted_expected_edge'] > 0
    assert r['expected_net_edge'] is None
    assert r['execution_cost_available'] is False
    assert r['net_edge_evaluable'] is False
    assert 'EXECUTION_COST_UNAVAILABLE' in r['reasoning_summary']
    assert 'NO_CLEAR_EDGE_AFTER_EXECUTION_COSTS' not in r['reasoning_summary']
    os.remove(p)

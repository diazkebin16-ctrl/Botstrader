import os,tempfile
from datetime import datetime,timezone,timedelta
from anomaly_engine import AnomalyDetectionEngine

def engine(**kw):
    p=tempfile.mktemp(suffix='.db');e=AnomalyDetectionEngine(p,**kw);e.ensure_schema();return e,p

def base_snapshot(ts=None):
    return {'timestamp':(ts or datetime.now(timezone.utc)).isoformat(),'symbol':'EUR_USD','session':'NY','market_regime':'RANGE',
      'metrics':{'price':1.10,'bid':1.0999,'ask':1.1001,'return_abs':.001,'volatility':.01,'volume':1000,'spread_bps':1.8,'available_liquidity':1000,'correlation_mean':.2},
      'features':{'atr':.01,'momentum':.1},'training_baseline':{'atr':{'median':.01,'mad':.002,'q05':.005,'q95':.02},'momentum':{'median':.1,'mad':.05,'q05':-.1,'q95':.3}},
      'correlations':{'A:B':.2},'correlation_baseline':{'A:B':.2},
      'ensemble':{'agreement':.6,'disagreement':.4,'baseline_agreement':.6,'baseline_disagreement':.4},
      'execution':{'slippage_bps':1,'baseline_slippage_bps':1,'latency_ms':100,'baseline_latency_ms':100,'fill_rate':.95,'baseline_fill_rate':.95,'rejection_rate':.01,'baseline_rejection_rate':.01},
      'portfolio':{'correlation_concentration':.2,'exposure_utilization':.3,'portfolio_heat':.3,'position_mismatch':False},
      'broker':{'latency_ms':100,'baseline_latency_ms':100},'system':{'cpu':20,'baseline_cpu':20,'memory':100,'baseline_memory':100}}

def seed_baseline(e,start,n=40):
    for i in range(n):
        s=base_snapshot(start+timedelta(minutes=i));e.observe(s)

def test_normal_stays_normal():
    e,p=engine();start=datetime.now(timezone.utc)-timedelta(minutes=60);seed_baseline(e,start)
    r=e.evaluate(base_snapshot(datetime.now(timezone.utc)))
    assert r['severity'] in ('NORMAL','WATCH')
    assert r['signal_authority'] is False and r['risk_increase_authority'] is False
    os.remove(p)

def test_data_integrity_deterministic_critical():
    e,p=engine(persistence_confirmations=1);s=base_snapshot();s['metrics']['price']=-1
    r=e.evaluate(s);assert r['severity']=='CRITICAL';assert any(x['subtype']=='DATA_INTEGRITY_ANOMALY' for x in r['anomalies'])
    os.remove(p)

def test_ood_unknown_regime_reduce_confidence():
    e,p=engine(persistence_confirmations=1);s=base_snapshot();s['market_regime']='UNKNOWN';s['regime_distance']=.95;s['features']={'atr':.2,'momentum':2}
    r=e.evaluate(s);subs={x['subtype'] for x in r['anomalies']};assert 'UNKNOWN_REGIME' in subs;assert 'OUT_OF_DISTRIBUTION_DATA' in subs;assert 'REDUCE_MODEL_CONFIDENCE' in r['recommendations']
    os.remove(p)

def test_correlation_convergence_detected():
    e,p=engine();s=base_snapshot();s['correlations']={'A:B':.95};s['correlation_baseline']={'A:B':.1}
    r=e.evaluate(s);assert any(x['subtype']=='CORRELATION_CONVERGENCE' for x in r['anomalies']);assert 'REDUCE_CORRELATED_ALLOCATIONS' in r['recommendations']
    os.remove(p)

def test_strategy_signal_flood():
    e,p=engine();s=base_snapshot();s['strategy_metrics']={'S1':{'baseline_signal_rate':5,'signal_rate':300,'opportunity_present':True}}
    r=e.evaluate(s);assert any(x['subtype']=='STRATEGY_BEHAVIOR_ANOMALY' for x in r['anomalies'])
    os.remove(p)

def test_strategy_silence_distinguishes_opportunity():
    e,p=engine();s=base_snapshot();s['strategy_metrics']={'S1':{'baseline_signal_rate':5,'signal_rate':0,'opportunity_present':True}}
    r=e.evaluate(s);assert any('UNEXPECTED_STRATEGY_SILENCE' in str(x['metrics']) for x in r['anomalies'])
    e2,p2=engine();s2=base_snapshot();s2['strategy_metrics']={'S1':{'baseline_signal_rate':5,'signal_rate':0,'opportunity_present':False}}
    r2=e2.evaluate(s2);assert not any(x['subtype']=='STRATEGY_BEHAVIOR_ANOMALY' for x in r2['anomalies'])
    os.remove(p);os.remove(p2)

def test_execution_broker_system_anomalies():
    e,p=engine();s=base_snapshot();s['execution'].update({'slippage_bps':8,'latency_ms':2000,'fill_rate':.4});s['broker'].update({'latency_ms':3000});s['system'].update({'cpu':95,'baseline_cpu':20})
    r=e.evaluate(s);subs={x['subtype'] for x in r['anomalies']};assert 'EXECUTION_ANOMALY' in subs;assert 'BROKER_LATENCY_ANOMALY' in subs;assert 'SYSTEM_RESOURCE_ANOMALY' in subs
    os.remove(p)

def test_portfolio_position_anomaly():
    e,p=engine();s=base_snapshot();s['portfolio'].update({'position_mismatch':True,'correlation_concentration':.9})
    r=e.evaluate(s);assert any(x['anomaly_type']=='PORTFOLIO_ANOMALY' for x in r['anomalies']);assert r['severity']=='CRITICAL'
    os.remove(p)

def test_composite_critical_combined_scenario():
    e,p=engine(persistence_confirmations=1);s=base_snapshot();s['market_regime']='UNKNOWN';s['regime_distance']=.95;s['features']={'atr':.20,'momentum':2.0};s['correlations']={'A:B':.95,'A:C':.92};s['correlation_baseline']={'A:B':.1,'A:C':.2};s['ensemble'].update({'disagreement':.95,'baseline_disagreement':.3});s['execution'].update({'slippage_bps':12,'latency_ms':2600,'fill_rate':.3});s['portfolio'].update({'correlation_concentration':.95,'portfolio_heat':1.1});s['metrics'].update({'spread_bps':10,'available_liquidity':300,'volatility':.08})
    r=e.evaluate(s);assert r['severity']=='CRITICAL';assert r['composite_anomaly_score']>=.85;assert 'CAPITAL_ALLOCATION_RISK_OFF' in r['recommendations'];assert 'GOVERNANCE_ADAPTATION_FROZEN' in r['recommendations'];assert r['rare_event_id']
    os.remove(p)

def test_false_alarm_hysteresis_not_immediate_recovery():
    e,p=engine(persistence_confirmations=2,recovery_confirmations=3);s=base_snapshot();s['market_regime']='UNKNOWN';s['regime_distance']=.9
    r=e.evaluate(s);aid=r['anomaly_ids'][0]
    # one normal reading only -> stabilizing, not recovered
    one=e.evaluate(base_snapshot(datetime.now(timezone.utc)+timedelta(seconds=1)))
    d=e.dashboard();entry=[x for x in d['current_anomalies'] if x['id']==aid][0];assert entry['state']=='STABILIZING'
    assert one['composite_anomaly_score']>0.0
    # enough confirmation -> recovered
    e.evaluate(base_snapshot(datetime.now(timezone.utc)+timedelta(seconds=2)));e.evaluate(base_snapshot(datetime.now(timezone.utc)+timedelta(seconds=3)))
    assert not any(x['id']==aid for x in e.dashboard()['current_anomalies'])
    os.remove(p)

def test_structural_shift_persistent_not_single_outlier():
    e,p=engine(structural_min_points=8,baseline_min_samples=8);start=datetime.now(timezone.utc)-timedelta(minutes=50)
    for i in range(20):
        s=base_snapshot(start+timedelta(minutes=i));s['metrics']['volatility']=.01;e.observe(s)
    for i in range(20,40):
        s=base_snapshot(start+timedelta(minutes=i));s['metrics']['volatility']=.08;e.observe(s)
    s=base_snapshot(start+timedelta(minutes=41));s['metrics']['volatility']=.08;r=e.evaluate(s)
    assert any(x['subtype']=='STRUCTURAL_CHANGE_DETECTED' for x in r['anomalies'])
    os.remove(p)

def test_event_replay_uses_no_future_data():
    e,p=engine(baseline_min_samples=8);start=datetime.now(timezone.utc)-timedelta(minutes=30)
    detected=[]
    for i in range(25):
        s=base_snapshot(start+timedelta(minutes=i));s['metrics']['spread_bps']=1.0 if i<20 else 8.0;r=e.evaluate(s);detected.append(any(x['subtype']=='SPREAD_ANOMALY' for x in r['anomalies']))
    assert not any(detected[:20]);assert any(detected[20:])
    os.remove(p)

def test_rare_event_memory_and_similarity():
    e,p=engine(persistence_confirmations=1);s=base_snapshot();s['market_regime']='UNKNOWN';s['regime_distance']=1;s['portfolio']['position_mismatch']=True;r=e.evaluate(s);rid=r['rare_event_id'];assert rid
    sims=e.similar_events({'composite':r['composite_anomaly_score']});assert sims and sims[0]['rare_event_id']==rid
    assert e.recover_event(rid,{'stable':True},{'result':'RECOVERED'})
    os.remove(p)

def test_anomaly_never_becomes_signal_or_leverage():
    e,p=engine(persistence_confirmations=1);s=base_snapshot();s['market_regime']='UNKNOWN';s['regime_distance']=1;r=e.evaluate(s)
    assert r['signal_authority'] is False;assert r['risk_increase_authority'] is False;assert r['direct_control_authority'] is False
    assert not any('BUY' in x or 'SELL' in x or 'INCREASE_LEVERAGE' in x for x in r['recommendations'])
    os.remove(p)


def test_prediction_and_confidence_distribution_drift():
    e,p=engine();s=base_snapshot()
    s["model_distributions"]={"M1":{"baseline":{"LONG":.4,"SHORT":.4,"ABSTAIN":.2},
                                     "current":{"LONG":.99,"SHORT":.005,"ABSTAIN":.005}}}
    s["confidence_distributions"]={"M1":{"baseline_mean":.62,"baseline_mad":.05,"current_mean":.98}}
    out=e.evaluate(s);subs={x["subtype"] for x in out["anomalies"]}
    assert "PREDICTION_DISTRIBUTION_DRIFT" in subs
    assert "CONFIDENCE_DISTRIBUTION_ANOMALY" in subs
    os.remove(p)

def test_pnl_reconciliation_anomaly():
    e,p=engine();s=base_snapshot()
    s["portfolio"]={"actual_pnl":500,"expected_pnl":100,"pnl_tolerance":10,
                    "correlation_concentration":.2,"exposure_utilization":.2,"portfolio_heat":.2}
    out=e.evaluate(s)
    assert any(x["subtype"]=="PNL_RECONCILIATION_ANOMALY" for x in out["anomalies"])
    os.remove(p)

def test_single_domain_statistical_spike_is_not_immediate_full_defensive_action():
    e,p=engine(persistence_confirmations=2);base=datetime.now(timezone.utc)-timedelta(minutes=40)
    for i in range(30):
        x=base_snapshot(base+timedelta(minutes=i));x["metrics"]["return_abs"]=.0002+(i%3)*.00001;e.evaluate(x)
    x=base_snapshot(base+timedelta(minutes=31));x["metrics"]["return_abs"]=.02
    out=e.evaluate(x)
    assert out["composite_anomaly_score"]>=.5
    assert out["actionable_anomaly_score"]<.5
    assert out["confirmed"] is False
    assert e.integration_context(out)["allocation_risk_off"] is False
    os.remove(p)

if __name__=='__main__':
    tests=[v for k,v in sorted(globals().items()) if k.startswith('test_') and callable(v)]
    for t in tests:t()
    print(f'anomaly engine tests: OK ({len(tests)})')

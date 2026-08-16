import os,tempfile
from datetime import datetime,timezone,timedelta
from anomaly_engine import AnomalyDetectionEngine
from capital_allocation import CapitalAllocationEngine
from smart_execution import SmartExecutionEngine
from governance_engine import GovernanceEngine


def normal(ts):
    return {'timestamp':ts.isoformat(),'symbol':'EUR_USD','market_regime':'RANGE','metrics':{'price':1.1,'bid':1.0999,'ask':1.1001,'spread_bps':1.8,'available_liquidity':1000,'volatility':.01,'volume':1000,'return_abs':.001,'correlation_mean':.2},'features':{'atr':.01},'training_baseline':{'atr':{'median':.01,'mad':.002,'q05':.005,'q95':.02}},'correlations':{'A:B':.2},'correlation_baseline':{'A:B':.2},'ensemble':{'agreement':.6,'disagreement':.4,'baseline_agreement':.6,'baseline_disagreement':.4},'execution':{'slippage_bps':1,'baseline_slippage_bps':1,'latency_ms':100,'baseline_latency_ms':100,'fill_rate':.95,'baseline_fill_rate':.95,'rejection_rate':.01,'baseline_rejection_rate':.01},'portfolio':{'correlation_concentration':.2,'portfolio_heat':.3,'exposure_utilization':.2,'position_mismatch':False},'broker':{'latency_ms':100,'baseline_latency_ms':100},'system':{'cpu':20,'baseline_cpu':20}}

def critical(ts):
    s=normal(ts);s.update({'market_regime':'UNKNOWN','regime_distance':.98,'regime_transition_score':.9});s['features']={'atr':.20};s['correlations']={'A:B':.96,'A:C':.94};s['correlation_baseline']={'A:B':.1,'A:C':.2};s['ensemble'].update({'disagreement':.96,'baseline_disagreement':.3});s['execution'].update({'slippage_bps':15,'latency_ms':3000,'fill_rate':.25});s['portfolio'].update({'correlation_concentration':.95,'portfolio_heat':1.1});s['metrics'].update({'spread_bps':10,'available_liquidity':300,'volatility':.09});return s

def allocation_items():
    return [
      dict(strategy_id='A',family='TREND',symbol='EUR_USD',asset_class='FX',direction='LONG',requested_risk=25,expected_net_edge=1,reliability=.8,calibration=.8,sample_size=100,stability=.8,volatility=1,drawdown=.02,tail_risk=.2,execution_quality=.8,ensemble_confidence=.7,regime_compatibility=.8),
      dict(strategy_id='B',family='TREND',symbol='GBP_USD',asset_class='FX',direction='LONG',requested_risk=25,expected_net_edge=.8,reliability=.8,calibration=.8,sample_size=100,stability=.8,volatility=1,drawdown=.02,tail_risk=.2,execution_quality=.8,ensemble_confidence=.7,regime_compatibility=.8),
    ]

def test_critical_anomaly_drives_only_conservative_recommendations():
    db=tempfile.mktemp(suffix='.db');a=AnomalyDetectionEngine(db,persistence_confirmations=1);a.ensure_schema();start=datetime.now(timezone.utc)-timedelta(minutes=30)
    for i in range(25):a.observe(normal(start+timedelta(minutes=i)))
    result=a.evaluate(critical(datetime.now(timezone.utc)));ctx=a.integration_context(result)
    assert result['severity']=='CRITICAL';assert ctx['allocation_risk_off'];assert ctx['ensemble_confidence_multiplier']<1;assert ctx['risk_engine_recommendation'] in ('EMERGENCY_REVIEW','BLOCK_OR_REDUCE');assert ctx['direct_actions'] is False

    ca=CapitalAllocationEngine(db,'3.27',mode='SHADOW');ca.ensure_schema();alloc=ca.allocate(allocation_items(),100,governance_frozen=(ctx['governance_recommendation']=='ADAPTATION_FROZEN'),execution_degraded=True,ensemble_disagreement=.95)
    assert alloc['risk_off'] is True and alloc['used_risk_budget']==0

    se=SmartExecutionEngine(db,'3.27',mode='SHADOW');se.ensure_schema();intent=se.create_intent(strategy_id='A',symbol='EUR_USD',side='BUY',target_quantity=100,maximum_quantity=100,risk_approved_quantity=100,expected_price=1.1,maximum_slippage_bps=8,risk_approval_valid=True)
    ss=se.capture_snapshot(intent['execution_intent_id'],bid=1.095,ask=1.105,last_price=1.1,available_liquidity=20,recent_volume=100,volatility='EXTREME',market_regime='UNKNOWN',broker_health='OK',broker_latency_ms=3000,market_status='tradeable')
    dec=se.recommend(intent['execution_intent_id'],ss)
    assert dec['recommended_quantity']<=100;assert dec['action'] in ('REJECT_EXECUTION','DELAY','REDUCE_SIZE')

    g=GovernanceEngine(db,'3.27',mode='SHADOW');g.ensure_schema();gd=g.evaluate('anomaly-test')
    assert gd['recommended_state']=='ADAPTATION_FROZEN';assert gd['enforced'] is False
    os.remove(db)

def test_hysteresis_requires_multiple_normal_readings_before_recovery():
    db=tempfile.mktemp(suffix='.db');a=AnomalyDetectionEngine(db,persistence_confirmations=1,recovery_confirmations=3);a.ensure_schema();t=datetime.now(timezone.utc)
    r=a.evaluate(critical(t));assert r['severity']=='CRITICAL'
    r1=a.evaluate(normal(datetime.now(timezone.utc)));assert r1['composite_anomaly_score']>0
    a.evaluate(normal(datetime.now(timezone.utc)));a.evaluate(normal(datetime.now(timezone.utc)))
    assert all(x['state']!='ACTIVE' for x in a.dashboard()['current_anomalies'])
    os.remove(db)

if __name__=='__main__':
    test_critical_anomaly_drives_only_conservative_recommendations();test_hysteresis_requires_multiple_normal_readings_before_recovery();print('anomaly integration tests: OK (2)')

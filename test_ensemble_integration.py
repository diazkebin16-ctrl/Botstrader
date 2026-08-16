import os,tempfile
os.environ['DB_PATH']=tempfile.mktemp(suffix='.db')
os.environ['TRADING_ENVIRONMENT']='TEST'
os.environ['UNIT_TEST']='1'
os.environ['AUTO_TRADE']='false'
os.environ['PRODUCTION_AUTHORIZED']='false'
os.environ['DEPLOYMENT_LIVE_EXECUTION_ENABLED']='false'
os.environ['OANDA_ACCOUNT_ID']='test'
os.environ['OANDA_TOKEN']='test'

from datetime import datetime,timezone
import server


def base_r(signal='BUY',news_bias='BULLISH'):
    return {
      'instrument':'EUR_USD','signal':signal,'technical':88,'score':90,
      'buy_score':88.0,'sell_score':55.0,'direction_edge':33.0,'direction_state':'TREND',
      'entry':1.1000,'stop':1.0980,'target':1.1040,'managed_target':1.1040,'rr':2.0,'rr_raw':2.0,
      'blocked':False,'alignment':'CONFIRMA' if news_bias=='BULLISH' else 'CONTRADICE',
      'news_bias':news_bias,'news_positive_hits':3 if news_bias=='BULLISH' else 0,
      'news_negative_hits':3 if news_bias=='BEARISH' else 0,
      'news_articles':[{'title':'x'}],
      'features':{'m15_gap_atr':.7,'m15_slope_atr':.4,'m5_momentum':.3,'m1_momentum':.2,'rr_raw':2,
                  'technical_score':88,'final_score':90,'volatility_ratio':1.0,'hour_ny':10},
      'filters':{'m15_context':True,'m5_structure':True,'second_pullback':True,'m1_confirmation':True,
                 'not_extended':True,'volatility_ok':True,'ny_session':True},
      'market_regime':{'market_regime':'BULLISH_TREND','confidence':.8,'volatility_state':'HIGH','trend_strength':.7},
      'market_data_stale':False,'candle_ts':datetime.now(timezone.utc).isoformat(),
      'weekend_research':{'active_signal_context':None}
    }


def conf():
    return {'probability':.72,'source':'TEST','samples':100,'local_samples':50,'required_confidence':.7,
            'variant':'SECOND_PULLBACK_NEWS_CONFIRM_RR2_Q90'}


def test_current_model_map_is_honest():
    models={x['strategy_id']:x for x in server.ensemble_engine.registry()}
    assert models['TECHNICAL_CORE']['role']=='DIRECTIONAL'
    assert models['ML_SUCCESS_CALIBRATOR']['role']=='CALIBRATOR'
    assert models['MARKET_REGIME_CONTEXT']['role']=='CONTEXT'
    assert models['NEWS_CONTEXT']['family']=='NEWS_MACRO'
    # The ML success model is not falsely counted as an independent BUY/SELL model.
    assert models['ML_SUCCESS_CALIBRATOR']['role']!='DIRECTIONAL'


def test_shadow_ensemble_sits_before_director_without_changing_execution():
    r=base_r();c=conf();before=server.execution_decision(dict(r),dict(c))
    ens=server.evaluate_ensemble_shadow(r,c,.75)
    r['ensemble_shadow']=ens
    director=server.ai_strategy_director_recommendation('EUR_USD',server.setup_variant(r),r['market_regime'],c['probability'],ens)
    after=server.execution_decision(dict(r),dict(c))
    assert ens['hypothetical_only'] is True
    assert ens['policy_authority'] is False
    assert before==after
    assert director['ensemble_shadow']['ensemble_decision_id']==ens['ensemble_decision_id']


def test_news_contradiction_can_create_conflict_but_not_trade_authority():
    r=base_r(news_bias='BEARISH');c=conf();ens=server.evaluate_ensemble_shadow(r,c,.70)
    assert ens['ensemble_direction'] in ('ABSTAIN','LONG','SHORT')
    assert ens['policy_authority'] is False
    assert ens['risk_multiplier_authority'] is False
    # Existing execution gate remains the only legacy decision here.
    d=server.execution_decision(r,c)
    assert isinstance(d,dict) and 'execute' in d


def test_risk_shadow_records_family_concentration_without_leverage_loop():
    r=base_r();c=conf();ens=server.evaluate_ensemble_shadow(r,c,.70)
    director=server.ai_strategy_director_recommendation('EUR_USD',server.setup_variant(r),r['market_regime'],c['probability'],ens)
    risk=server.adaptive_risk_recommendation('EUR_USD',server.setup_variant(r),r['market_regime'],director,c['probability'],
       {'nav':10000,'current_drawdown':.01,'margin_usage':.1,'portfolio_open_risk':.01,'open_instruments':[],
        'consecutive_losses':0,'data_stale':False,'system_abnormal':False},100)
    # Agreement has no pathway that can make multiplier exceed the Risk Engine's own ceiling.
    assert float(risk['risk_multiplier'])<=1.0
    assert server.RISK_ENGINE_SHADOW_MODE is True


def test_trade_memory_schema_has_ensemble_trace_fields():
    c=server.conn();cols={x[1] for x in c.execute('PRAGMA table_info(trade_memory)').fetchall()};c.close()
    for name in ('ensemble_decision_id','ensemble_direction','ensemble_confidence','ensemble_agreement',
                 'ensemble_diversity','ensemble_weight_version','ensemble_context_json'):
        assert name in cols


def test_release_fingerprint_includes_ensemble_engine():
    files=server.production_release_files()
    assert any(x.endswith('ensemble_engine.py') for x in files)
    assert server.production_release_versions()['ensemble_version'].endswith(':SHADOW')

if __name__=='__main__':
    tests=[v for k,v in sorted(globals().items()) if k.startswith('test_') and callable(v)]
    for t in tests:t()
    print(f'ensemble integration tests: OK ({len(tests)})')

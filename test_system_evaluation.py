
import os, sqlite3, tempfile, json, math
from datetime import datetime, timezone, timedelta
from system_evaluation import SystemEvaluationEngine

ASOF=datetime(2026,8,15,12,0,tzinfo=timezone.utc)

def iso(dt): return dt.isoformat()

def db():
    path=tempfile.mktemp(suffix=".db")
    c=sqlite3.connect(path)
    c.executescript("""
    CREATE TABLE trade_memory(
      id INTEGER PRIMARY KEY AUTOINCREMENT,trade_id TEXT UNIQUE,strategy TEXT,symbol TEXT,direction TEXT,status TEXT,
      entry_ts TEXT,exit_ts TEXT,net_result REAL,gross_result REAL,realized_pl REAL,realized_r REAL,fees_total REAL,
      financing REAL,entry_slippage_pips REAL,exit_slippage_pips REAL,approved_risk REAL,entry_session TEXT,
      market_regime_entry TEXT,deployment_version TEXT,execution_quality_compromised INTEGER DEFAULT 0,
      risk_config_version TEXT,runtime_code_hash TEXT);
    CREATE TABLE observability_alert_history(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,alert_key TEXT,severity TEXT,status TEXT,module TEXT,event_type TEXT,
      message TEXT,details_json TEXT,correlation_id TEXT);
    CREATE TABLE observability_metrics(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,cpu_percent REAL,memory_rss_mb REAL,memory_percent REAL,
      disk_used_percent REAL,event_loop_lag_ms REAL,queue_depth INTEGER,processing_time_ms REAL,db_latency_ms REAL,
      broker_latency_ms REAL,market_data_latency_ms REAL,details_json TEXT);
    CREATE TABLE observability_heartbeats(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,module_name TEXT,status TEXT,latency_ms REAL,details_json TEXT);
    CREATE TABLE observability_capital_history(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,source TEXT,equity REAL,cash REAL,unrealized_pnl REAL,realized_pnl REAL,
      daily_pnl REAL,weekly_pnl REAL,drawdown REAL,peak_equity REAL,exposure REAL,margin_usage REAL,open_risk REAL,
      remaining_risk_budget REAL,details_json TEXT);
    CREATE TABLE recovery_incidents(
      incident_id TEXT PRIMARY KEY,account_scope TEXT,started_ts TEXT,recovered_ts TEXT,status TEXT,severity TEXT,
      incident_type TEXT,correlation_id TEXT,execution_intent_id TEXT,reason TEXT,details_json TEXT);
    CREATE TABLE recovery_reconciliation_runs(
      reconciliation_id TEXT PRIMARY KEY,account_scope TEXT,started_ts TEXT,completed_ts TEXT,status TEXT,
      broker_transaction_id TEXT,broker_balance REAL,broker_nav REAL,broker_margin_used REAL,summary_json TEXT);
    CREATE TABLE recovery_state(
      account_scope TEXT PRIMARY KEY,state TEXT,safe_mode INTEGER,emergency_stop INTEGER,new_trades_allowed INTEGER,
      last_transaction_id TEXT,last_broker_success_ts TEXT,last_market_data_ts TEXT,last_reconciliation_ts TEXT,
      last_reconciliation_status TEXT,last_risk_verified_ts TEXT,incident_started_ts TEXT,broker_disconnect_started_ts TEXT,
      ready_ts TEXT,details_json TEXT,updated_ts TEXT);
    CREATE TABLE adaptive_risk_decisions(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,instrument TEXT,setup_variant TEXT,market_regime TEXT,volatility_state TEXT,
      strategy_confidence REAL,recent_win_rate REAL,current_drawdown REAL,nav REAL,margin_usage REAL,portfolio_open_risk REAL,
      strategy_open_risk REAL,requested_risk REAL,approved_risk REAL,requested_units REAL,shadow_max_position_size REAL,
      risk_multiplier REAL,max_exposure REAL,allow_new_trades INTEGER,reduce_existing_positions INTEGER,emergency_stop INTEGER,
      hard_limit_triggered INTEGER,reason TEXT,metrics_json TEXT,shadow_mode INTEGER);
    CREATE TABLE signals(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,candle_ts TEXT,instrument TEXT,signal TEXT,technical INTEGER,score INTEGER,
      alignment TEXT,blocked INTEGER,entry REAL,stop REAL,target REAL,rr REAL,executed INTEGER,order_id TEXT,ml_probability REAL,
      dynamic_confidence REAL,confidence_source TEXT,confidence_samples INTEGER,required_confidence REAL,decision_reason TEXT,
      setup_variant TEXT,features_json TEXT,filters_json TEXT);
    CREATE TABLE learning_samples(
      id INTEGER PRIMARY KEY AUTOINCREMENT,signal_id INTEGER,created_ts TEXT,candle_ts TEXT,instrument TEXT,direction TEXT,
      entry REAL,stop REAL,target REAL,technical INTEGER,score INTEGER,blocked INTEGER,executed INTEGER,features_json TEXT,
      status TEXT,label INTEGER,resolved_ts TEXT,bars_to_resolution INTEGER,mfe_r REAL,mae_r REAL,note TEXT);
    CREATE TABLE ai_strategy_director_decisions(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,instrument TEXT,setup_variant TEXT,recommended_state TEXT,confidence REAL,
      market_regime TEXT,regime_confidence REAL,volatility_state TEXT,strategy_health_status TEXT,historical_win_rate REAL,
      recent_win_rate REAL,historical_samples INTEGER,recent_samples INTEGER,signal_confidence REAL,
      score_components_json TEXT,reasons_json TEXT,observation_only INTEGER);
    CREATE TABLE ai_strategy_director_outcomes(
      id INTEGER PRIMARY KEY AUTOINCREMENT,director_decision_id INTEGER,signal_id INTEGER,resolved_label INTEGER,
      executed INTEGER,blocked INTEGER,resolved_ts TEXT);
    CREATE TABLE candidate_strategies(
      candidate_id TEXT PRIMARY KEY,generated_at TEXT);
    CREATE TABLE candidate_validation_runs(
      validation_id TEXT PRIMARY KEY,candidate_id TEXT,completed_ts TEXT,final_status TEXT,
      oos_results_json TEXT,paper_results_json TEXT);
    CREATE TABLE candidate_paper_trades(
      id INTEGER PRIMARY KEY AUTOINCREMENT,candidate_id TEXT,created_ts TEXT);
    CREATE TABLE deployment_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,candidate_id TEXT,new_stage TEXT,event_type TEXT);
    CREATE TABLE deployment_registry(candidate_id TEXT PRIMARY KEY,current_stage TEXT);
    CREATE TABLE deployment_live_trades(
      id INTEGER PRIMARY KEY AUTOINCREMENT,candidate_id TEXT,realized_r REAL,opened_ts TEXT,closed_ts TEXT);
    CREATE TABLE market_regime_history(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,candle_ts TEXT,instrument TEXT,market_regime TEXT,confidence REAL,
      volatility_state TEXT,trend_strength REAL,abnormality_score REAL,supporting_metrics_json TEXT);
    CREATE TABLE security_audit_log(
      seq INTEGER PRIMARY KEY AUTOINCREMENT,audit_id TEXT,timestamp TEXT,actor TEXT,actor_role TEXT,action TEXT,resource TEXT,
      old_value_json TEXT,new_value_json TEXT,reason TEXT,result TEXT,correlation_id TEXT,prev_hash TEXT,record_hash TEXT);
    """)
    c.commit();c.close()
    return path

def trade(c,i,days_ago,strategy,pnl,r,regime="BULLISH_TREND",symbol="EUR_USD",direction="LONG",
          slip=.2,comp=0,deployment="prod_v1",risk=.005):
    exit_ts=ASOF-timedelta(days=days_ago,hours=(i%12))
    entry_ts=exit_ts-timedelta(hours=2)
    c.execute("""INSERT INTO trade_memory(
      trade_id,strategy,symbol,direction,status,entry_ts,exit_ts,net_result,gross_result,realized_pl,realized_r,fees_total,
      financing,entry_slippage_pips,exit_slippage_pips,approved_risk,entry_session,market_regime_entry,deployment_version,
      execution_quality_compromised,risk_config_version,runtime_code_hash)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (f"T{i}",strategy,symbol,direction,"CLOSED",iso(entry_ts),iso(exit_ts),pnl,pnl+0.1,pnl,r,.05,0.0,slip/2,slip/2,
       risk,"NY",regime,deployment,comp,"config_v1","hash1"))

def seed_common(path,degrading=True,high_latency=False,data_bad=False,correlated=False,regime_shift=False,
                overblocking=False,bad_candidates=False,good_candidate=False):
    c=sqlite3.connect(path)
    # Historical A good; recent A bad in degradation scenario. B stable.
    i=0
    for d in range(90,30,-1):
        i+=1;trade(c,i,d,"A",1.0,1.0,regime="BULLISH_TREND",slip=.2)
        i+=1;trade(c,i,d,"B",.6,.6,regime="BULLISH_TREND",slip=.2)
    for d in range(30,0,-1):
        i+=1
        a=(-.8 if degrading else 1.1)
        trade(c,i,d,"A",a,a,regime="HIGH_VOLATILITY" if regime_shift else "BULLISH_TREND",
              slip=2.0 if high_latency else .25,comp=1 if data_bad else 0)
        i+=1
        b=(-.8 if correlated and degrading else (.9 if correlated else .5))
        trade(c,i,d,"B",b,b,regime="HIGH_VOLATILITY" if regime_shift else "BULLISH_TREND",
              slip=2.0 if high_latency else .25,comp=1 if data_bad else 0)

    # Heartbeats / metrics.
    for n in range(30):
        ts=iso(ASOF-timedelta(days=n,hours=1))
        c.execute("INSERT INTO observability_heartbeats(ts,module_name,status,latency_ms,details_json) VALUES(?,?,?,?,?)",
                  (ts,"Broker Connection","OK",50,"{}"))
        c.execute("""INSERT INTO observability_metrics(ts,processing_time_ms,broker_latency_ms,market_data_latency_ms,details_json)
                     VALUES(?,?,?,?,?)""",(ts,100,2800 if high_latency else 80,50,"{}"))
    c.execute("""INSERT INTO observability_capital_history(ts,source,equity,drawdown,exposure,margin_usage,open_risk,details_json)
                 VALUES(?,?,?,?,?,?,?,?)""",(iso(ASOF-timedelta(hours=1)),"BROKER",10000,.04,.6,.2,.03,"{}"))
    if high_latency:
        c.execute("""INSERT INTO observability_alert_history(ts,alert_key,severity,status,module,event_type,message,details_json)
                     VALUES(?,?,?,?,?,?,?,?)""",(iso(ASOF-timedelta(days=1)),"e1","HIGH","ACTIVE","Execution","ORDER_REJECTED","x","{}"))
    if data_bad:
        for n in range(20):
            c.execute("""INSERT INTO observability_alert_history(ts,alert_key,severity,status,module,event_type,message,details_json)
                         VALUES(?,?,?,?,?,?,?,?)""",(iso(ASOF-timedelta(days=n+1)),f"st{n}","HIGH","ACTIVE","Market Data","MARKET_DATA_STALE","stale","{}"))

    # Risk engine.
    for n in range(60):
        ts=iso(ASOF-timedelta(hours=n*4))
        blocked=1 if overblocking and n<40 else 0
        c.execute("""INSERT INTO adaptive_risk_decisions(
          ts,instrument,setup_variant,risk_multiplier,allow_new_trades,reduce_existing_positions,emergency_stop,
          hard_limit_triggered,reason,metrics_json,shadow_mode)
          VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
          (ts,"EUR_USD","A",.5 if blocked else 1.0,0 if blocked else 1,blocked,0,0,"test","{}",1))
        c.execute("""INSERT INTO signals(ts,instrument,signal,technical,score,blocked,executed,setup_variant,features_json,filters_json)
                     VALUES(?,?,?,?,?,?,?,?,?,?)""",(ts,"EUR_USD","BUY",1,80,blocked,0,"A","{}","{}"))
        sid=c.execute("SELECT last_insert_rowid()").fetchone()[0]
        # If overblocking, most blocked trades would have won -> false positives.
        label=1 if (blocked and n%4!=0) else 0
        c.execute("""INSERT INTO learning_samples(signal_id,created_ts,instrument,direction,entry,stop,target,technical,score,
          blocked,executed,features_json,status,label,resolved_ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (sid,ts,"EUR_USD","LONG",1.1,1.09,1.12,1,80,blocked,0,"{}","DONE",label,ts))

    # Director decisions.
    for n in range(40):
        ts=iso(ASOF-timedelta(hours=n*8))
        state="ACTIVE" if n%3 else "PAUSED";label=1 if state=="ACTIVE" else 0
        c.execute("""INSERT INTO ai_strategy_director_decisions(
          ts,instrument,setup_variant,recommended_state,confidence,observation_only,
          score_components_json,reasons_json,historical_samples,recent_samples)
          VALUES(?,?,?,?,?,?,?,?,?,?)""",(ts,"EUR_USD","A",state,.8,1,"{}","[]",100,20))
        did=c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.execute("""INSERT INTO ai_strategy_director_outcomes(director_decision_id,resolved_label,executed,blocked,resolved_ts)
                     VALUES(?,?,?,?,?)""",(did,label,1 if state=="ACTIVE" else 0,1 if state=="PAUSED" else 0,ts))

    # Regime history; last 50 can differ.
    for n in range(200):
        rg="HIGH_VOLATILITY" if regime_shift and n<50 else "BULLISH_TREND"
        ts=iso(ASOF-timedelta(hours=n))
        c.execute("""INSERT INTO market_regime_history(ts,candle_ts,instrument,market_regime,confidence,volatility_state,
          trend_strength,abnormality_score,supporting_metrics_json) VALUES(?,?,?,?,?,?,?,?,?)""",
          (ts,ts,"EUR_USD",rg,.8,"HIGH" if rg=="HIGH_VOLATILITY" else "NORMAL",.7,0,"{}"))

    # Candidates.
    total=10 if bad_candidates else 1
    for n in range(total):
        cid=f"C{n}"
        c.execute("INSERT INTO candidate_strategies(candidate_id,generated_at) VALUES(?,?)",(cid,iso(ASOF-timedelta(days=20+n))))
    if good_candidate:
        cid="C0"
        oos={"candidate":{"expectancy":.7}}
        paper={"candidate":{"expectancy":.65}}
        c.execute("""INSERT INTO candidate_validation_runs(validation_id,candidate_id,completed_ts,final_status,oos_results_json,paper_results_json)
                     VALUES(?,?,?,?,?,?)""",("V0",cid,iso(ASOF-timedelta(days=10)),"READY_FOR_REVIEW",json.dumps(oos),json.dumps(paper)))
        c.execute("INSERT INTO candidate_paper_trades(candidate_id,created_ts) VALUES(?,?)",(cid,iso(ASOF-timedelta(days=8))))
        for stage,days in [("CANARY_LIVE",6),("LIMITED_PRODUCTION",3),("FULL_PRODUCTION_ELIGIBLE",1)]:
            c.execute("INSERT INTO deployment_events(ts,candidate_id,new_stage,event_type) VALUES(?,?,?,?)",
                      (iso(ASOF-timedelta(days=days)),cid,stage,"PROMOTION"))
        for n in range(12):
            c.execute("INSERT INTO deployment_live_trades(candidate_id,realized_r,opened_ts,closed_ts) VALUES(?,?,?,?)",
                      (cid,.6,iso(ASOF-timedelta(hours=20+n)),iso(ASOF-timedelta(hours=19+n))))
    c.commit();c.close()

def evaluate(**kw):
    path=db();seed_common(path,**kw)
    e=SystemEvaluationEngine(path,"3.20",min_samples=10,report_period_hours=24)
    r=e.evaluate(iso(ASOF))
    return path,e,r

def test_strategy_degrades_and_regime_shifts():
    path,e,r=evaluate(degrading=True,regime_shift=True)
    assert "STRATEGY_DEGRADATION" in r["degradation"]["types"]
    assert "MARKET_REGIME_SHIFT" in r["degradation"]["types"]
    assert r["system_status"] in ("WATCH","DEGRADING","HIGH_RISK","CRITICAL")
    assert any(x["recommendation"].startswith("REVIEW_") for x in r["recommendations"])
    os.remove(path)

def test_strategy_improves():
    path,e,r=evaluate(degrading=False)
    assert "STRATEGY_DEGRADATION" not in r["degradation"]["types"]
    assert r["dimensions"]["trading_score"]>=50
    os.remove(path)

def test_execution_costs_and_latency_degrade():
    path,e,r=evaluate(degrading=True,high_latency=True)
    assert "EXECUTION_DEGRADATION" in r["degradation"]["types"]
    assert r["operational"]["p95_broker_latency_ms"]>=2000
    assert any(x["recommendation"]=="REVIEW_EXECUTION_LATENCY" for x in r["recommendations"])
    os.remove(path)

def test_hidden_correlation():
    path,e,r=evaluate(degrading=True,correlated=True)
    assert r["diversification"]["status"]=="HIDDEN_CONCENTRATION_RISK"
    assert r["diversification"]["hidden_concentration_pairs"]
    os.remove(path)

def test_data_quality_degrades():
    path,e,r=evaluate(degrading=True,data_bad=True)
    assert "DATA_QUALITY_DEGRADATION" in r["degradation"]["types"]
    assert r["confidence"]["data_quality"]<.75
    os.remove(path)

def test_risk_engine_overblocking():
    path,e,r=evaluate(degrading=False,overblocking=True)
    assert r["risk_engine"]["block_rate"]>.5
    assert r["risk_engine"]["efficiency"]=="OVER_RESTRICTIVE"
    assert "RISK_DEGRADATION" in r["degradation"]["types"]
    assert any(x["recommendation"]=="REVIEW_RISK_ENGINE_RESTRICTIVENESS" for x in r["recommendations"])
    assert r["trading"]["activity_efficiency"]["possible_overfiltering"] is True
    assert any(x["recommendation"]=="POSSIBLE_OVERFILTERING" for x in r["recommendations"])
    os.remove(path)

def test_bad_candidate_funnel():
    path,e,r=evaluate(degrading=False,bad_candidates=True)
    assert r["adaptive_learning"]["funnel"]["GENERATED"]==10
    assert r["adaptive_learning"]["assessment"]=="CANDIDATE_QUALITY_LOW"
    assert any(x["recommendation"]=="REVIEW_ADAPTIVE_LEARNING_FUNNEL" for x in r["recommendations"])
    os.remove(path)

def test_good_candidate_survives_and_model_gap_small():
    path,e,r=evaluate(degrading=False,good_candidate=True)
    f=r["adaptive_learning"]["funnel"]
    assert f["PRODUCTION"]==1 and f["CANARY"]==1 and f["PAPER"]==1
    assert "C0" in r["adaptive_learning"]["successful_live_candidates"]
    assert r["adaptive_learning"]["live_effectiveness"]["C0"]["expectancy_r"]>0
    assert r["model_reality_gap"]["status"]=="NORMAL"
    os.remove(path)


def test_possible_overtrading():
    path=db();seed_common(path,degrading=False)
    c=sqlite3.connect(path)
    base=10000
    for n in range(100):
        # Many recent marginal trades with fee drag dominating gross edge.
        trade(c,base+n,1+(n%10)/20,"A",.02,.02,slip=.3)
        c.execute("UPDATE trade_memory SET gross_result=.08,fees_total=.06 WHERE trade_id=?",(f"T{base+n}",))
    c.commit();c.close()
    e=SystemEvaluationEngine(path,"3.20",min_samples=10)
    r=e.evaluate(iso(ASOF))
    assert r["trading"]["activity_efficiency"]["possible_overtrading"] is True
    assert any(x["recommendation"]=="POSSIBLE_OVERTRADING" for x in r["recommendations"])
    os.remove(path)

def test_history_is_immutable_and_asof_has_no_future():
    path=db();seed_common(path,degrading=False)
    c=sqlite3.connect(path)
    # Future catastrophic trade must not enter a historical evaluation.
    trade(c,999,-10,"A",-100,-100)
    c.commit();c.close()
    e=SystemEvaluationEngine(path,"3.20",min_samples=10)
    r=e.evaluate(iso(ASOF))
    assert "T999" not in r["data_snapshot"]["trade_ids"]
    assert r["data_snapshot"]["future_data_used"] is False
    c=e.conn()
    try:
        c.execute("UPDATE system_evaluations SET system_score=0 WHERE evaluation_id=?",(r["evaluation_id"],))
        raise AssertionError("historical evaluation was mutable")
    except sqlite3.DatabaseError:
        pass
    c.close();os.remove(path)

if __name__=="__main__":
    test_strategy_degrades_and_regime_shifts()
    test_strategy_improves()
    test_execution_costs_and_latency_degrade()
    test_hidden_correlation()
    test_data_quality_degrades()
    test_risk_engine_overblocking()
    test_bad_candidate_funnel()
    test_good_candidate_survives_and_model_gap_small()
    test_possible_overtrading()
    test_history_is_immutable_and_asof_has_no_future()
    print("system evaluation tests: OK")

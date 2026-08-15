
import os, tempfile, sqlite3, asyncio
from deployment_runtime import DeploymentManager

def seed(db):
    c=sqlite3.connect(db);c.row_factory=sqlite3.Row
    c.executescript("""
    CREATE TABLE candidate_strategies(candidate_id TEXT PRIMARY KEY,strategy_id TEXT,candidate_version TEXT,
      production_version TEXT,change_type TEXT,proposed_value_json TEXT);
    CREATE TABLE candidate_registry(candidate_id TEXT PRIMARY KEY,current_state TEXT,historical_validation_status TEXT,
      validation_score REAL,paper_trade_count INTEGER,paper_regime_count INTEGER,paper_days REAL,
      divergence_status TEXT,latest_validation_id TEXT,final_reason TEXT,auto_deploy INTEGER DEFAULT 0,updated_ts TEXT);
    CREATE TABLE candidate_validation_runs(validation_id TEXT PRIMARY KEY,oos_results_json TEXT,paper_results_json TEXT);
    CREATE TABLE trade_memory_degradation(scope_key TEXT,status TEXT,reason TEXT,strategy TEXT);
    CREATE TABLE concept_drift_alerts(scope_key TEXT,status TEXT,reason TEXT,strategy_id TEXT);
    """)
    c.execute("INSERT INTO candidate_strategies VALUES(?,?,?,?,?,?)",
              ("C1","S1","S1_candidate_v2","S1_v1","MIN_CONFIDENCE","0.75"))
    c.execute("INSERT INTO candidate_registry VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
              ("C1","READY_FOR_REVIEW","PASSED",.85,35,2,16,"CONSISTENT","V1","paper passed",0,"x"))
    c.execute("INSERT INTO candidate_validation_runs VALUES(?,?,?)",
              ("V1",'{"candidate":{"expectancy_r":0.30}}',
                    '{"candidate":{"expectancy_r":0.25,"win_rate":0.60}}'))
    c.commit();c.close()

def run():
    db=tempfile.mktemp(suffix=".db");seed(db)
    m=DeploymentManager(db,"https://example.invalid","acct","tok",True,
                        ["EUR_USD"],["BULL_TREND","RANGE"])
    m.ensure_schema()

    # Mandatory readiness and explicit approval.
    assert m.readiness("C1")["ready"]
    assert m.approve("C1","human-review","approved after review")["stage"]=="APPROVED_FOR_CANARY"

    health={"broker_ok":True,"data_ok":True,"system_abnormal":False,
            "nav":10000,"margin_usage":.02,"current_drawdown":0,"open_instruments":[]}
    started=asyncio.run(m.start("C1","human-review",health))
    assert started["stage"]=="CANARY_LIVE" and started["allocation_fraction"]==.05

    # Restart recovers state but never fail-opens.
    assert m.mark_restart()==1
    dep=m.live("S1")[0]
    assert dep["current_stage"]=="CANARY_LIVE"
    assert dep["resume_required"]==1 and dep["new_trades_enabled"]==0

    # Cannot trade before post-restart health check.
    ctx={"instrument":"EUR_USD","strategy_confidence_entry":.80,"director_confidence_entry":.80,
         "director_state":"ACTIVE","market_regime_entry":"BULL_TREND","volatility_state_entry":"NORMAL"}
    risk={"allow_new_trades":True,"emergency_stop":False}
    gate=m.signal_gate(dep,ctx,risk,health)
    assert not gate["allow"] and "RESTART_HEALTH_CHECK_REQUIRED" in gate["reasons"]

    assert asyncio.run(m.resume("C1","human-review",health))["ok"]
    dep=m.live("S1")[0]

    # Risk Engine remains a hard veto.
    blocked=m.signal_gate(dep,ctx,{"allow_new_trades":False,"emergency_stop":False},health)
    assert not blocked["allow"] and "RISK_ENGINE_BLOCK" in blocked["reasons"]

    # Regime failure and corrupt broker/data state fail closed.
    badctx=dict(ctx);badctx["market_regime_entry"]=None
    assert not m.signal_gate(dep,badctx,risk,health)["allow"]
    badhealth=dict(health);badhealth["data_ok"]=False
    assert not m.signal_gate(dep,ctx,risk,badhealth)["allow"]

    # Candidate kill switch.
    m.set_kill("CANDIDATE:C1",True,"test","unit")
    assert not m.signal_gate(dep,ctx,risk,health)["allow"]
    m.set_kill("CANDIDATE:C1",False,"clear","unit")

    # Attempt to skip promotion gate with no live evidence => HOLD.
    hold=m.promote("C1","human-review")
    assert hold["action"]=="HOLD"

    # Rollback with an open position does not blindly close it.
    c=m.conn()
    c.execute("""INSERT INTO deployment_live_trades(
      candidate_id,candidate_version,strategy_id,trade_id,stage,allocation_fraction,approved_risk_fraction,
      instrument,direction,units,expected_entry,stop_loss,take_profit,status,opened_ts,created_ts,updated_ts)
      VALUES('C1','S1_candidate_v2','S1','T1','CANARY_LIVE',.05,.00025,
      'EUR_USD','LONG',5,1.1,1.09,1.12,'OPEN','x','x','x')""")
    c.commit();c.close()
    rb=m.rollback("C1","drawdown limit","unit")
    assert rb["stage"]=="ROLLED_BACK" and rb["open_positions_left_protected"]==1
    c=m.conn()
    assert c.execute("SELECT status FROM deployment_live_trades WHERE trade_id='T1'").fetchone()["status"]=="OPEN"
    # Blue/green candidate version preserved.
    assert c.execute("SELECT candidate_version FROM candidate_strategies WHERE candidate_id='C1'").fetchone()["candidate_version"]=="S1_candidate_v2"
    # Audit exists.
    assert c.execute("SELECT COUNT(*) n FROM deployment_events WHERE candidate_id='C1'").fetchone()["n"]>=4
    c.close()
    os.remove(db)

if __name__=="__main__":
    run()
    print("deployment manager integration tests: OK")

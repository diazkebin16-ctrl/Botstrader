
import os, sqlite3, tempfile, json
from datetime import datetime, timezone, timedelta
from governance_engine import GovernanceEngine

NOW=datetime(2026,8,15,16,0,tzinfo=timezone.utc)
def iso(dt): return dt.isoformat()

def make_db():
    path=tempfile.mktemp(suffix=".db")
    c=sqlite3.connect(path)
    c.executescript("""
    CREATE TABLE system_evaluations(
      evaluation_id TEXT PRIMARY KEY,generated_at TEXT,as_of_ts TEXT,system_status TEXT,system_score REAL,
      data_quality_score REAL,degradation_json TEXT,risk_json TEXT,operational_json TEXT,
      model_reality_gap_json TEXT,diversification_json TEXT,director_json TEXT,risk_engine_json TEXT,
      stability_json TEXT,trading_json TEXT);
    CREATE TABLE security_audit_log(
      seq INTEGER PRIMARY KEY AUTOINCREMENT,timestamp TEXT,action TEXT,resource TEXT);
    CREATE TABLE deployment_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,candidate_id TEXT,event_type TEXT,new_stage TEXT);
    CREATE TABLE ai_strategy_director_decisions(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,setup_variant TEXT,recommended_state TEXT,confidence REAL);
    CREATE TABLE adaptive_risk_decisions(
      id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,setup_variant TEXT,allow_new_trades INTEGER,risk_multiplier REAL);
    CREATE TABLE recovery_incidents(
      incident_id TEXT PRIMARY KEY,started_ts TEXT,recovered_ts TEXT,status TEXT,severity TEXT,incident_type TEXT);
    CREATE TABLE concept_drift_alerts(
      scope_key TEXT PRIMARY KEY,ts TEXT,strategy_id TEXT,status TEXT);
    CREATE TABLE deployment_registry(
      candidate_id TEXT PRIMARY KEY,current_stage TEXT);
    CREATE TABLE trade_memory(
      id INTEGER PRIMARY KEY AUTOINCREMENT,status TEXT,strategy_confidence_entry REAL,realized_r REAL,
      execution_quality_compromised INTEGER DEFAULT 0);
    """)
    c.commit();c.close()
    return path

def insert_eval(path,status="HEALTHY",score=85,dq=.95,draw=.25,deg=None,gaps=0,stable=85,net=10,gross=12):
    c=sqlite3.connect(path)
    c.execute("""INSERT INTO system_evaluations(
      evaluation_id,generated_at,as_of_ts,system_status,system_score,data_quality_score,degradation_json,
      risk_json,operational_json,model_reality_gap_json,diversification_json,director_json,risk_engine_json,
      stability_json,trading_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      ("E"+str(c.execute("SELECT COUNT(*) FROM system_evaluations").fetchone()[0]+1),
       iso(NOW),iso(NOW),status,score,dq,
       json.dumps({"types":deg or []}),
       json.dumps({"drawdown_utilization":draw}),
       json.dumps({"active_critical_alerts":0}),
       json.dumps({"material_gaps":[{"x":1} for _ in range(gaps)]}),
       json.dumps({}),json.dumps({}),json.dumps({}),
       json.dumps({"score":stable}),json.dumps({"net_pnl":net,"gross_pnl":gross})))
    c.commit();c.close()

def eng(path,mode="SHADOW",**pol):
    e=GovernanceEngine(path,"3.21",mode=mode,policies=pol)
    e.ensure_schema();e.set_runtime(mode=mode,policies=pol,config_version=1)
    return e

def test_adaptive_learning_too_many_changes_and_budget_exhaustion():
    path=make_db();insert_eval(path)
    c=sqlite3.connect(path)
    for n in range(5):
        c.execute("INSERT INTO security_audit_log(timestamp,action,resource) VALUES(?,?,?)",
                  (iso(NOW-timedelta(hours=n+1)),"CONFIG_CHANGED",f"strategy.A.param{n}"))
    c.commit();c.close()
    e=eng(path,MAX_MAJOR_CHANGES_PER_WEEK=2,MAX_PARAMETER_CHANGES_PER_WEEK=3)
    meta=e.meta_risk(NOW)
    assert meta["change_budget"]["exhausted_any"]
    r=e.evaluate("adaptive_learning_excess")
    assert r["would_block"] is True
    assert r["decision"]=="WOULD_FREEZE"
    os.remove(path)

def test_strategy_churn_and_adaptation_loop():
    path=make_db();insert_eval(path)
    c=sqlite3.connect(path)
    states=["ACTIVE","PAUSED","ACTIVE","REDUCED","ACTIVE","PAUSED","ACTIVE","PAUSED"]
    for n,st in enumerate(states):
        c.execute("INSERT INTO ai_strategy_director_decisions(ts,setup_variant,recommended_state,confidence) VALUES(?,?,?,?)",
                  (iso(NOW-timedelta(hours=2*n)),"A",st,.6))
    for n in range(7):
        c.execute("INSERT INTO security_audit_log(timestamp,action,resource) VALUES(?,?,?)",
                  (iso(NOW-timedelta(hours=n+1)),"CONFIG_CHANGED","strategy.A.threshold"))
    c.commit();c.close()
    e=eng(path,STRATEGY_CHURN_TRANSITIONS_7D=4,PARAMETER_CHURN_CHANGES_7D=4,ADAPTATION_LOOP_EVENT_THRESHOLD=5)
    meta=e.meta_risk(NOW)
    assert meta["strategy_churn"]["detected"]
    assert meta["parameter_churn"]["detected"]
    assert meta["adaptation_loop"]["detected"]
    os.remove(path)

def test_candidate_promotion_during_degradation_is_would_blocked_in_shadow():
    path=make_db();insert_eval(path,status="DEGRADING",score=62,deg=["STRATEGY_DEGRADATION"])
    e=eng(path)
    r=e.check_action("DEPLOYMENT_PROMOTION","C1",
                     {"validation_state":"READY_FOR_REVIEW","magnitude":"MAJOR"})
    assert r["would_block"] is True
    assert r["enforced"] is False
    assert "System Evaluation status is DEGRADING" in r["reason"]
    os.remove(path)

def test_risk_engine_and_director_conflict():
    path=make_db();insert_eval(path)
    c=sqlite3.connect(path)
    c.execute("INSERT INTO ai_strategy_director_decisions(ts,setup_variant,recommended_state,confidence) VALUES(?,?,?,?)",
              (iso(NOW),"A","ACTIVE",.8))
    c.execute("INSERT INTO adaptive_risk_decisions(ts,setup_variant,allow_new_trades,risk_multiplier) VALUES(?,?,?,?)",
              (iso(NOW),"A",0,.3))
    c.execute("INSERT INTO ai_strategy_director_decisions(ts,setup_variant,recommended_state,confidence) VALUES(?,?,?,?)",
              (iso(NOW),"B","ACTIVE",.55))
    c.execute("INSERT INTO adaptive_risk_decisions(ts,setup_variant,allow_new_trades,risk_multiplier) VALUES(?,?,?,?)",
              (iso(NOW),"B",1,.5))
    c.commit();c.close()
    e=eng(path)
    meta=e.meta_risk(NOW)
    assert len(meta["conflicts"])>=2
    assert meta["model_disagreement"]["status"]=="HIGH_MODEL_DISAGREEMENT"
    assert all(x["resolution"]=="RISK_ENGINE_MORE_RESTRICTIVE" for x in meta["conflicts"])
    os.remove(path)

def test_low_data_quality_and_critical_system_freeze():
    path=make_db();insert_eval(path,status="CRITICAL",score=25,dq=.4,draw=.9,deg=["DATA_QUALITY_DEGRADATION"])
    e=eng(path)
    r=e.evaluate("critical")
    assert r["decision"]=="WOULD_FREEZE"
    assert r["meta_risk_state"] in ("HIGH","CRITICAL")
    assert "SYSTEM_CRITICAL" in r["reason"] and "LOW_DATA_QUALITY" in r["reason"]
    os.remove(path)

def test_critical_change_enforced_when_frozen_full_mode():
    path=make_db();insert_eval(path,status="CRITICAL",score=20,dq=.5)
    e=eng(path,mode="FULL_POLICY_ENFORCEMENT")
    e.evaluate("critical")
    r=e.check_action("CHANGE_APPLY","risk.max_trade_fraction",
                     {"component":"risk.max_trade_fraction","risk_level":"CRITICAL",
                      "current_value":.01,"proposed_value":.009})
    assert r["would_block"] and r["enforced"]
    assert r["decision"]=="BLOCK"
    os.remove(path)

def test_governance_lock_persists_restart():
    path=make_db();insert_eval(path)
    e=eng(path);e.set_lock(True,"incident uncertainty","risk-manager")
    e2=eng(path)
    st=e2.state()
    assert st["governance_lock"]==1
    assert st["adaptation_state"]=="ADAPTATION_FROZEN"
    r=e2.check_action("DEPLOYMENT_PROMOTION","C1",{"validation_state":"READY_FOR_REVIEW","magnitude":"MAJOR"})
    assert r["enforced"] is True
    os.remove(path)

def test_stability_window_blocks_premature_major_change():
    path=make_db();insert_eval(path)
    c=sqlite3.connect(path)
    c.execute("INSERT INTO security_audit_log(timestamp,action,resource) VALUES(?,?,?)",
              (iso(datetime.now(timezone.utc)-timedelta(hours=1)),"CONFIG_CHANGED","strategy.A.threshold"))
    c.commit();c.close()
    e=eng(path,MIN_STABILITY_HOURS=72)
    r=e.check_action("CHANGE_APPLY","strategy.A.threshold",
                     {"component":"strategy.A.threshold","risk_level":"HIGH_RISK",
                      "current_value":.7,"proposed_value":.9,"magnitude":"MAJOR"})
    assert r["would_block"]
    assert "Stability observation window has not completed" in r["reason"]
    os.remove(path)

def test_safe_change_allowed_after_stability():
    path=make_db();insert_eval(path,status="HEALTHY",score=88,dq=.95)
    c=sqlite3.connect(path)
    c.execute("INSERT INTO security_audit_log(timestamp,action,resource) VALUES(?,?,?)",
              (iso(datetime.now(timezone.utc)-timedelta(days=10)),"CONFIG_CHANGED","strategy.A.threshold"))
    c.commit();c.close()
    e=eng(path,MIN_STABILITY_HOURS=72)
    r=e.check_action("CHANGE_APPLY","strategy.A.threshold",
                     {"component":"strategy.A.threshold","risk_level":"HIGH_RISK",
                      "current_value":.70,"proposed_value":.72,"magnitude":"SMALL"})
    assert r["would_block"] is False
    assert r["decision"]=="ALLOW"
    os.remove(path)

def test_freeze_recovery_requires_explicit_reviews():
    path=make_db();insert_eval(path,status="HEALTHY",score=90,dq=.95)
    e=eng(path,MIN_STABILITY_HOURS=1,LIMITED_ADAPTATION_REVIEW_HOURS=1)
    e.set_lock(True,"manual emergency governance lock","admin")
    e.set_lock(False,"incident cleared","admin")
    # Lock clearing does not auto-resume adaptation.
    assert e.state()["adaptation_state"]=="ADAPTATION_FROZEN"
    r1=e.review_transition("risk-manager","health verified")
    assert r1["to_state"]=="REVIEW"
    c=e.conn();c.execute("UPDATE governance_state SET last_review_ts=? WHERE singleton=1",
                         (iso(datetime.now(timezone.utc)-timedelta(hours=2)),));c.commit();c.close()
    r2=e.review_transition("risk-manager","review observation complete")
    assert r2["to_state"]=="LIMITED_ADAPTATION"
    c=e.conn();c.execute("UPDATE governance_state SET limited_started_ts=? WHERE singleton=1",
                         (iso(datetime.now(timezone.utc)-timedelta(hours=2)),));c.commit();c.close()
    r3=e.review_transition("risk-manager","limited adaptation stable")
    assert r3["to_state"]=="NORMAL_ADAPTATION"
    os.remove(path)

def test_policy_conflict_resolves_conservatively():
    path=make_db();e=eng(path)
    x=e.resolve_policy_conflict([
        {"policy":"A","decision":"ALLOW"},
        {"policy":"B","decision":"BLOCK"}])
    assert x["decision"]=="BLOCK"
    assert x["event"]=="POLICY_CONFLICT_RESOLVED_CONSERVATIVELY"
    os.remove(path)

def test_confidence_miscalibration_detected():
    path=make_db();insert_eval(path)
    c=sqlite3.connect(path)
    for n in range(30):
        c.execute("INSERT INTO trade_memory(status,strategy_confidence_entry,realized_r,execution_quality_compromised) VALUES(?,?,?,0)",
                  ("CLOSED",.9,1 if n<12 else -1))
    c.commit();c.close()
    e=eng(path,CONFIDENCE_CALIBRATION_MIN_SAMPLES=20,CONFIDENCE_CALIBRATION_MAX_ERROR=.2)
    cal=e.meta_risk(NOW)["confidence_calibration"]
    assert cal["detected"]
    assert cal["status"]=="CONFIDENCE_MIS_CALIBRATION"
    os.remove(path)

if __name__=="__main__":
    test_adaptive_learning_too_many_changes_and_budget_exhaustion()
    test_strategy_churn_and_adaptation_loop()
    test_candidate_promotion_during_degradation_is_would_blocked_in_shadow()
    test_risk_engine_and_director_conflict()
    test_low_data_quality_and_critical_system_freeze()
    test_critical_change_enforced_when_frozen_full_mode()
    test_governance_lock_persists_restart()
    test_stability_window_blocks_premature_major_change()
    test_safe_change_allowed_after_stability()
    test_freeze_recovery_requires_explicit_reviews()
    test_policy_conflict_resolves_conservatively()
    test_confidence_miscalibration_detected()
    print("governance engine tests: OK")

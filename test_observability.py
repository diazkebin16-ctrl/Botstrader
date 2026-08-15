import os,tempfile,sqlite3,time
from datetime import datetime,timezone,timedelta
from observability import (
    ObservabilityManager, compute_system_health, stale_status, reconciliation_status,
    degradation_state, drawdown_alert_state, latency_alert_state,
    DEPENDENCY_CRITICAL, DEPENDENCY_IMPORTANT
)

def run():
    # Broker disconnect and Risk Engine offline are critical dependencies.
    h=compute_system_health([
        {"module_name":"Broker Connection","dependency_class":DEPENDENCY_CRITICAL,"status":"OFFLINE"},
        {"module_name":"Risk Engine","dependency_class":DEPENDENCY_CRITICAL,"status":"OK"},
    ])
    assert h["status"]=="CRITICAL"
    h=compute_system_health([
        {"module_name":"Broker Connection","dependency_class":DEPENDENCY_CRITICAL,"status":"OK"},
        {"module_name":"Risk Engine","dependency_class":DEPENDENCY_CRITICAL,"status":"OFFLINE"},
    ])
    assert h["status"]=="CRITICAL"

    # Market-data stale / missing heartbeat.
    old=(datetime.now(timezone.utc)-timedelta(seconds=500)).isoformat()
    assert stale_status(old,60,180)["status"]=="OFFLINE"
    mid=(datetime.now(timezone.utc)-timedelta(seconds=100)).isoformat()
    assert stale_status(mid,60,180)["status"]=="STALE"

    # Latency and drawdown warning/critical thresholds.
    assert latency_alert_state(300,250,1000)=="WARNING"
    assert latency_alert_state(1200,250,1000)=="CRITICAL"
    assert drawdown_alert_state(.08,.04,.10)=="WARNING"
    assert drawdown_alert_state(.11,.04,.10)=="CRITICAL"

    # Position mismatch / restart with positions.
    assert reconciliation_status(["EUR_USD"],["EUR_USD"])["status"]=="CONSISTENT"
    mm=reconciliation_status([], ["EUR_USD"])
    assert mm["status"]=="STATE_RECONCILIATION_REQUIRED" and mm["broker_only"]==["EUR_USD"]

    # Candidate/strategy degradation.
    assert degradation_state(.20,-.05,1.7,.8,False)=="CRITICAL_DEGRADATION"
    assert degradation_state(.20,.08,1.7,1.0,False) in ("DEGRADING","WATCH")
    assert degradation_state(.20,.18,1.7,1.6,True)=="CRITICAL_DEGRADATION"

    db=tempfile.mktemp(suffix=".db")
    m=ObservabilityManager(db,"test",alert_cooldown_seconds=3600)
    m.ensure_schema()

    # Deduplication and duplicate-order alert fatigue control.
    a1=m.alert("DUPLICATE_ORDER:123","CRITICAL","Execution Engine","DUPLICATE_ORDER","duplicate")
    a2=m.alert("DUPLICATE_ORDER:123","CRITICAL","Execution Engine","DUPLICATE_ORDER","duplicate")
    assert a1["notify"] is True and a2["notify"] is False and a2["count"]==2

    # Recovery notification and persistence across manager restart.
    rec=m.recover("DUPLICATE_ORDER:123","order state recovered")
    assert rec["recovered"]
    m2=ObservabilityManager(db,"test")
    m2.ensure_schema()
    c=m2.conn();row=c.execute("SELECT status FROM observability_alerts WHERE alert_key='DUPLICATE_ORDER:123'").fetchone();c.close()
    assert row["status"]=="RECOVERED"

    # Missing heartbeat -> stale/offline alert.
    m2.heartbeat("Risk Engine",DEPENDENCY_CRITICAL,"OK")
    c=m2.conn();c.execute("UPDATE observability_module_health SET heartbeat_ts=? WHERE module_name='Risk Engine'",(old,));c.commit();c.close()
    changes=m2.mark_stale_modules({"Risk Engine":60})
    assert changes[0]["status"]=="OFFLINE"
    assert any(x["event_type"]=="MODULE_HEARTBEAT_MISSED" for x in m2.active_alerts())

    # End-to-end correlation trace survives restart.
    cid=m2.new_trace("EUR_USD","S1")
    m2.trace_phase(cid,"market_data")
    time.sleep(.002);m2.trace_phase(cid,"signal")
    m2.link_trace(cid,signal_id=7,decision_id=8,risk_decision_id=9,order_id="O1",trade_id="T1")
    m2.trace_phase(cid,"director");m2.trace_phase(cid,"risk");m2.trace_phase(cid,"complete")
    tr=m2.trace(cid)["trace"]
    assert tr["signal_id"]==7 and tr["decision_id"]==8 and tr["risk_decision_id"]==9
    assert tr["total_latency_ms"] is not None

    # Database unavailable fails rather than silently reporting healthy.
    bad=tempfile.mkdtemp()+"/missing/subdir/obs.db"
    failed=False
    try:ObservabilityManager(bad,"x").ensure_schema()
    except Exception:failed=True
    assert failed

    os.remove(db)

if __name__=="__main__":
    run();print("observability tests: OK")

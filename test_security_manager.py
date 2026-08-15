
import json, os, sqlite3, tempfile, hashlib
from pathlib import Path

from security_manager import SecurityManager, sanitize
from observability import ObservabilityManager

def h(x): return hashlib.sha256(x.encode()).hexdigest()

def actors_json():
    return json.dumps({
        "admin":{"role":"ADMIN","token_sha256":h("admin-token")},
        "risk1":{"role":"RISK_MANAGER","token_sha256":h("risk1-token")},
        "risk2":{"role":"RISK_MANAGER","token_sha256":h("risk2-token")},
        "strategy":{"role":"STRATEGY_MANAGER","token_sha256":h("strategy-token")},
        "operator":{"role":"OPERATOR","token_sha256":h("operator-token")},
        "viewer":{"role":"VIEWER","token_sha256":h("viewer-token")},
    })

SCHEMA={
    "risk.max_trade_fraction":{"type":"float","min":0.001,"max":0.01,"hard_ceiling":0.01,"risk_level":"CRITICAL"},
    "risk.max_strategy_fraction":{"type":"float","min":0.001,"max":0.03,"hard_ceiling":0.03,"risk_level":"CRITICAL"},
    "risk.max_portfolio_fraction":{"type":"float","min":0.001,"max":0.06,"hard_ceiling":0.06,"risk_level":"CRITICAL"},
    "risk.drawdown_warning":{"type":"float","min":0.001,"max":0.05,"hard_ceiling":0.05,"risk_level":"HIGH_RISK"},
    "risk.drawdown_stop":{"type":"float","min":0.002,"max":0.10,"hard_ceiling":0.10,"risk_level":"CRITICAL"},
    "execution.trade_units":{"type":"int","min":1,"max":100,"hard_ceiling":100,"risk_level":"CRITICAL"},
    "strategy.*":{"type":"any","risk_level":"HIGH_RISK"},
    "broker.credentials":{"type":"str","secret":True,"risk_level":"CRITICAL"},
}
INITIAL={
    "risk.max_trade_fraction":0.01,
    "risk.max_strategy_fraction":0.03,
    "risk.max_portfolio_fraction":0.06,
    "risk.drawdown_warning":0.05,
    "risk.drawdown_stop":0.10,
    "execution.trade_units":100,
}

def manager(db,root=None,env="PAPER",actors=None):
    m=SecurityManager(db,"3.19",env,actors_json() if actors is None else actors,
                      allow_unauthenticated_reads=False)
    m.configure(SCHEMA,INITIAL,code_root=root,dependency_file=(str(Path(root)/"requirements.txt") if root else None))
    return m

def test_rbac_change_control_and_rollback():
    db=tempfile.mktemp(suffix=".db")
    m=manager(db)
    admin=m.authenticate("Bearer admin-token")
    risk1=m.authenticate("Bearer risk1-token")
    risk2=m.authenticate("Bearer risk2-token")
    strategy=m.authenticate("Bearer strategy-token")
    viewer=m.authenticate("Bearer viewer-token")

    # Viewer cannot request a risk change.
    try:
        m.create_change_request(viewer,"risk.engine","risk.max_trade_fraction",0.009,
                                "test","lower risk","rollback")
        raise AssertionError("viewer was allowed to request risk change")
    except PermissionError:
        pass

    # Adaptive Learning/automation may REQUEST, never authorize/apply.
    ai=m.internal_actor("ADAPTIVE_LEARNING_ENGINE","SYSTEM_RECOMMENDER")
    ai_req=m.create_change_request(ai,"risk.engine","risk.max_trade_fraction",0.009,
                                   "AI recommends less risk","lower exposure","restore prior snapshot")
    cid=ai_req["change"]["change_id"]
    assert ai_req["change"]["status"]=="PENDING_REVIEW"
    try:
        m.review_change(ai,cid,"APPROVE","self approval")
        raise AssertionError("automation self-approved")
    except PermissionError:
        pass
    try:
        m.apply_change(ai,cid)
        raise AssertionError("automation applied change")
    except PermissionError:
        pass

    # Same human requester cannot self-approve a CRITICAL change.
    human_req=m.create_change_request(risk1,"risk.engine","risk.max_trade_fraction",0.008,
                                      "reduce max trade risk","lower capital risk","rollback previous config")
    hid=human_req["change"]["change_id"]
    try:
        m.review_change(risk1,hid,"APPROVE","self")
        raise AssertionError("critical self approval accepted")
    except PermissionError:
        pass

    # Two distinct authorized reviewers required.
    x=m.review_change(risk2,hid,"APPROVE","risk review")
    assert x["change"]["status"]=="PENDING_REVIEW"
    x=m.review_change(admin,hid,"APPROVE","admin review")
    assert x["change"]["status"]=="APPROVED"
    before=m.current_version()
    applied=m.apply_change(admin,hid)
    assert applied["change"]["status"]=="APPLIED"
    assert m.current_config()["risk.max_trade_fraction"]==0.008
    after=m.current_version()
    assert after>before

    # Rollback is configuration-only and requires risk authority.
    try:
        m.rollback_config(strategy,1,"strategy manager attempted global rollback")
        raise AssertionError("strategy manager rolled back global config")
    except PermissionError:
        pass
    rb=m.rollback_config(admin,1,"restore baseline")
    assert rb["new_config_version"]>after
    assert m.current_config()["risk.max_trade_fraction"]==0.01

    # Audit is hash-chained and append-only.
    integ=m.verify_audit_chain()
    assert integ["verified"] and integ["records"]>0
    c=m.conn()
    try:
        c.execute("DELETE FROM security_audit_log")
        raise AssertionError("audit delete unexpectedly allowed")
    except sqlite3.DatabaseError:
        pass
    try:
        c.execute("UPDATE security_audit_log SET actor='tampered' WHERE seq=1")
        raise AssertionError("audit update unexpectedly allowed")
    except sqlite3.DatabaseError:
        pass
    c.close()
    os.remove(db)

def test_invalid_critical_change_and_secret_redaction():
    db=tempfile.mktemp(suffix=".db")
    m=manager(db)
    risk=m.authenticate("Bearer risk1-token")
    # Existing hard ceiling is 1%; V3.19 cannot increase it.
    bad=m.create_change_request(risk,"risk.engine","risk.max_trade_fraction",0.50,
                                "increase aggressively","more exposure","rollback")
    assert bad["change"]["status"]=="REJECTED"
    val=json.loads(bad["change"]["validation_json"])
    assert val["reason"] in ("ABOVE_MAX","HARD_CEILING_EXCEEDED")

    # Broker secrets are not managed through the application.
    sec=m.create_change_request(risk,"broker","broker.credentials","super-secret-token",
                                "rotate key","credential rotation","restore secret externally")
    assert sec["change"]["status"]=="REJECTED"

    # Audit record must not contain the secret.
    c=m.conn()
    rows=[dict(x) for x in c.execute("SELECT * FROM security_audit_log ORDER BY seq").fetchall()]
    c.close()
    joined=json.dumps(rows)
    assert "super-secret-token" not in joined
    assert "[REDACTED]" in joined

    # Operational observability also sanitizes bearer tokens and secret-key fields.
    obs=ObservabilityManager(db,"3.19")
    obs.ensure_schema()
    obs.structured_log("ERROR","Security Test","SECRET_TEST",
                       "Authorization: Bearer abcdefghijklmnopqrstuvwxyz123456",
                       metrics={"token":"plaintext-token","safe":"ok"})
    c=sqlite3.connect(db);c.row_factory=sqlite3.Row
    row=c.execute("SELECT * FROM observability_structured_logs ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    assert "abcdefghijklmnopqrstuvwxyz123456" not in row["message"]
    assert "plaintext-token" not in row["metrics_json"]
    assert "[REDACTED]" in row["message"] and "[REDACTED]" in row["metrics_json"]
    os.remove(db)

def test_emergency_reset_and_environment_guards():
    db=tempfile.mktemp(suffix=".db")
    m=manager(db)
    viewer=m.authenticate("Bearer viewer-token")
    risk=m.authenticate("Bearer risk1-token")
    try:
        m.authorize_emergency_reset(viewer,True,True,"viewer reset")
        raise AssertionError("viewer reset emergency stop")
    except PermissionError:
        pass
    assert m.authorize_emergency_reset(risk,False,True,"health not ready")["authorized"] is False
    assert m.authorize_emergency_reset(risk,True,True,"verified recovery")["authorized"] is True

    # Tests/Paper can never authorize real orders.
    t=SecurityManager(db,"3.19","TEST",actors_json(),False)
    t.configure(SCHEMA,INITIAL)
    env=t.validate_environment(True,"live",running_under_test=True)
    assert not env["valid"]
    guard=t.real_order_guard(broker_account_verified=True,risk_engine_ready=True,
                             reconciliation_complete=True,emergency_stop=False,
                             deployment_authorized=True,runtime_verified=True,
                             running_under_test=True)
    assert not guard["allow"]
    assert "TEST_PROCESS_REAL_ORDER_FORBIDDEN" in guard["reasons"]
    os.remove(db)

def test_runtime_and_config_integrity():
    root=Path(tempfile.mkdtemp())
    (root/"server.py").write_text("VERSION='3.19'\n")
    (root/"security_manager.py").write_text("x=1\n")
    (root/"requirements.txt").write_text("httpx==0.28.1\n")
    db=str(root/"state.db")
    m=manager(db,str(root))
    first=m.runtime_integrity_check()
    assert first["verified"]
    # Same version + changed critical code => unverified.
    (root/"server.py").write_text("VERSION='3.19'\nCHANGED=True\n")
    second=m.runtime_integrity_check()
    assert not second["verified"] and second["reason"]=="UNVERIFIED_RUNTIME_STATE"
    third=m.runtime_integrity_check()
    assert not third["verified"] and third["reason"]=="UNVERIFIED_RUNTIME_STATE"

    # Corrupt a config snapshot in storage and verify restart check detects it.
    c=m.conn()
    current=m.current_version()
    # Simulate external/database corruption by explicitly removing the application
    # immutability trigger first; normal application code cannot perform this update.
    c.execute("DROP TRIGGER security_config_versions_no_update")
    c.execute("UPDATE security_config_versions SET config_json='{}' WHERE config_version=?",(current,))
    c.commit();c.close()
    assert not m.verify_config_integrity()["verified"]

    check=m.startup_security_check(
        secrets_available=True,canary_live_enabled=False,canary_env="practice",
        risk_limits_valid=True,deployment_state_valid=True,audit_available=True,
        running_under_test=False)
    assert check["status"]=="SECURITY_FAILED"
    assert check["checks"]["config_integrity"] is False

def test_production_requires_actor_config():
    db=tempfile.mktemp(suffix=".db")
    m=manager(db,env="PRODUCTION",actors="{}")
    check=m.startup_security_check(
        secrets_available=True,canary_live_enabled=False,canary_env="practice",
        risk_limits_valid=True,deployment_state_valid=True,audit_available=True,
        running_under_test=False)
    assert check["status"]=="SECURITY_FAILED"
    assert check["checks"]["authorization_config"] is False
    os.remove(db)

if __name__=="__main__":
    test_rbac_change_control_and_rollback()
    test_invalid_critical_change_and_secret_redaction()
    test_emergency_reset_and_environment_guards()
    test_runtime_and_config_integrity()
    test_production_requires_actor_config()
    print("security manager tests: OK")

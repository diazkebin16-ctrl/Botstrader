import os, sys, subprocess, tempfile

def run_subprocess(code, extra_env=None):
    env=os.environ.copy()
    env.update({
        "DB_PATH":tempfile.mktemp(suffix=".db"),
        "AUTO_TRADE":"false",
        "OANDA_ACCOUNT_ID":"dummy-practice-account",
        "OANDA_TOKEN":"dummy-practice-token",
        "SECURITY_ACTORS_JSON":"{}",
    })
    if extra_env: env.update(extra_env)
    r=subprocess.run([sys.executable,"-c",code],cwd=os.path.dirname(__file__),
                     env=env,text=True,capture_output=True,timeout=120)
    if r.returncode:
        raise AssertionError(r.stderr)
    return r.stdout.strip()

def test_test_environment_cannot_enable_live_canary():
    out=run_subprocess(
        "import server; "
        "print(server.VERSION_TAG);"
        "print(server.deployment_manager.live_enabled);"
        "print(server.AUTO_PROMOTE_RESEARCH);"
        "print(server.security_manager.real_order_guard("
        "broker_account_verified=True,risk_engine_ready=True,reconciliation_complete=True,"
        "emergency_stop=False,deployment_authorized=True,runtime_verified=True,running_under_test=True)['allow'])",
        {
            "TRADING_ENVIRONMENT":"TEST",
            "DEPLOYMENT_LIVE_EXECUTION_ENABLED":"true",
            "CANARY_OANDA_ENV":"live",
            "OANDA_CANARY_ACCOUNT_ID":"dummy-live-account",
            "OANDA_CANARY_TOKEN":"dummy-live-token",
        })
    lines=out.splitlines()
    assert lines[-4:] == ["3.27","False","False","False"]

def test_unknown_strategy_version_fails_security_startup():
    code = '''
import server
server.conn().close()
server.deployment_manager.ensure_schema()
c=server.conn()
c.execute(\"\"\"INSERT INTO deployment_registry(
candidate_id,strategy_id,candidate_version,production_version,current_stage,
allocation_fraction,eligible_allocation_fraction,resume_required,new_trades_enabled,
created_ts,updated_ts)
VALUES('ghost','S_GHOST','ghost_v99','ghost_v98','CANARY_PAUSED',0,0,1,0,?,?)\"\"\",
(server.now_iso(),server.now_iso()))
c.commit();c.close()
x=server.security_startup_check()
print(x["status"])
print(x["checks"]["deployment_state_valid"])
'''
    out=run_subprocess(code,{"TRADING_ENVIRONMENT":"PAPER","DEPLOYMENT_LIVE_EXECUTION_ENABLED":"false","CANARY_OANDA_ENV":"practice"})
    lines=out.splitlines()
    assert lines[-2:] == ["SECURITY_FAILED","False"]

if __name__=="__main__":
    test_test_environment_cannot_enable_live_canary()
    test_unknown_strategy_version_fails_security_startup()
    print("security integration tests: OK")

import json, os, sqlite3, tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from production_readiness import ProductionReadinessGate


def make_env():
    root=Path(tempfile.mkdtemp(prefix='prodread_'));db=str(root/'test.db')
    c=sqlite3.connect(db)
    c.executescript('''
    CREATE TABLE system_evaluations(
      evaluation_id TEXT PRIMARY KEY,generated_at TEXT,system_status TEXT,system_score REAL,data_quality_score REAL);
    CREATE TABLE governance_state(
      singleton INTEGER PRIMARY KEY,adaptation_state TEXT,governance_lock INTEGER,updated_ts TEXT);
    CREATE TABLE recovery_state(
      account_scope TEXT PRIMARY KEY,last_reconciliation_status TEXT,last_reconciliation_ts TEXT,state TEXT,
      safe_mode INTEGER,emergency_stop INTEGER,last_market_data_ts TEXT,updated_ts TEXT);
    CREATE TABLE trade_memory(id INTEGER PRIMARY KEY AUTOINCREMENT,trade_id TEXT,status TEXT);
    ''')
    now=datetime.now(timezone.utc).isoformat()
    c.execute("INSERT INTO system_evaluations VALUES(?,?,?,?,?)",('E1',now,'HEALTHY',90,.95))
    c.execute("INSERT INTO governance_state VALUES(?,?,?,?)",(1,'NORMAL_ADAPTATION',0,now))
    c.execute("INSERT INTO recovery_state VALUES(?,?,?,?,?,?,?,?)",('PRIMARY','MATCHED',now,'READY',0,0,now,now))
    c.commit();c.close()
    report={
      'framework_version':'3.23','critical_failures':0,'safety_violations':0,
      'pass_fail_gate':{'ready_for_step15':True,'gates':{
        'zero_critical_safety_failures':True,'zero_risk_limit_bypasses':True,
        'zero_duplicate_order_vulnerabilities':True,'restart_recovery_successful':True,
        'reconciliation_passes':True,'database_failure_recovery_passes':True,
        'emergency_stop_survives_restart':True,'governance_protections_pass':True,
        'data_leakage_pass':True,'canary_rollback_pass':True,'full_extreme_simulation_pass':True
      }}}
    report_path=root/'step14.json';report_path.write_text(json.dumps(report))
    code=root/'server.py';code.write_text('VERSION=3.23\n')
    req=root/'requirements.txt';req.write_text('httpx==0\n')
    gate=ProductionReadinessGate(db,'3.23')
    gate.ensure_schema()
    cfg={'risk.max_trade_fraction':.01,'risk.max_portfolio_fraction':.06,'risk.drawdown_stop':.10}
    versions={'system_release':'3.23','dependencies':{'requirements':'hash'}}
    rc=gate.create_release_candidate(files=[str(code),str(req)],config=cfg,versions=versions,step14_report_path=str(report_path),actor='TEST')
    return root,gate,rc,cfg,versions,[str(code),str(req)]


def base_context(**over):
    x={
      'environment':'PRODUCTION','production_authorized':True,'account_scope':'PRIMARY',
      'risk_engine_ready':True,'risk_engine_shadow_mode':False,'broker_reconciled':True,
      'market_data_fresh':True,'audit_ready':True,'deployment_state_consistent':True,
      'no_state_corruption':True,'canary_controls_ready':True,'recovery_tests_pass':True,
      'security_tests_pass':True,'change_management_ready':True,'monitoring_ready':True,
      'no_risk_bypass_known':True,'no_duplicate_order_vulnerability':True,
      'emergency_stop_test_pass':True,'hard_limits':{
        'max_trade_risk_fraction':.01,'max_portfolio_exposure_fraction':.06,'max_drawdown_fraction':.10
      }
    }
    x.update(over);return x


def satisfy_account_paper_dry(gate,rc):
    gate.verify_account(rc['release_id'],'PRODUCTION',
        {'broker':'OANDA','account_id':'A1','account_type':'live','currency':'USD'},
        {'broker':'OANDA','account_id':'A1','account_type':'live','currency':'USD','permissions_ok':True,
         'market_access_ok':True,'leverage_ok':True,'margin_settings_ok':True,'balance_within_expected_range':True})
    p=gate.record_final_paper(rc['release_id'],{
        'trades':20,'days':5,'regimes':2,'execution_parity':True,'config_match':True,'code_match':True,
        'risk_match':True,'governance_match':True,'critical_incidents':0})
    assert p['passed']
    d=gate.record_dry_run(rc['release_id'],{
        'market_data':True,'signal':True,'director':True,'risk':True,'governance':True,'execution_prepared':True
    },{'instrument':'EUR_USD','side':'BUY','units':5},True,0)
    assert d['passed']


def test_step14_pass_does_not_equal_production_ready():
    root,g,rc,cfg,versions,files=make_env()
    # No account/final-paper/dry-run and risk is explicitly shadow only.
    result=g.certify(base_context(risk_engine_shadow_mode=True,risk_engine_ready=True,
                                  production_authorized=False),rc['release_id'])
    assert result['go_no_go']=='NO_GO'
    assert result['readiness_state']=='BLOCKED'
    assert 'RISK_ENGINE_READY' in result['blockers']
    assert 'BROKER_ACCOUNT_VERIFIED' in result['blockers']
    assert 'FINAL_PAPER_PASS' in result['blockers']
    assert 'PRODUCTION_DRY_RUN_PASS' in result['blockers']
    assert 'PRODUCTION_AUTHORIZATION_PRESENT' in result['blockers']


def test_exact_release_can_reach_ready_for_minimal_live_only_with_all_evidence():
    root,g,rc,cfg,versions,files=make_env();satisfy_account_paper_dry(g,rc)
    result=g.certify(base_context(),rc['release_id'])
    assert result['go_no_go']=='GO'
    assert result['readiness_state']=='READY_FOR_MINIMAL_LIVE'
    assert not result['blockers']
    limits=result['minimal_live_limits']
    assert limits['risk_cap_multiplier']==.05
    assert limits['max_trade_risk_fraction']<=.0005
    assert limits['max_trade_risk_fraction']<=base_context()['hard_limits']['max_trade_risk_fraction']


def test_release_change_requires_new_release_candidate():
    root,g,rc,cfg,versions,files=make_env();satisfy_account_paper_dry(g,rc)
    check=g.verify_release_unchanged(rc['release_id'],files,cfg,versions);assert check['passed']
    Path(files[0]).write_text('VERSION=3.23\nMATERIAL_CHANGE=True\n')
    check=g.verify_release_unchanged(rc['release_id'],files,cfg,versions)
    assert not check['passed'] and 'CODE_CHANGED' in check['mismatches']
    st=g.invalidate_certification('MAJOR_CODE_CHANGE',release_id=rc['release_id'])
    assert st['readiness_state']=='BLOCKED'


def test_minimal_live_requires_explicit_authorization_and_health():
    root,g,rc,cfg,versions,files=make_env();satisfy_account_paper_dry(g,rc)
    assert g.certify(base_context(),rc['release_id'])['go_no_go']=='GO'
    bad=g.activate_minimal_live(base_context(production_authorized=False,risk_ready=True,broker_ready=True,
        data_ready=True,reconciliation_ok=True,governance_ok=True,system_ready=True), 'risk','test')
    assert not bad['ok']
    ok=g.activate_minimal_live(base_context(risk_ready=True,broker_ready=True,data_ready=True,
        reconciliation_ok=True,governance_ok=True,system_ready=True), 'risk','minimal activation')
    assert ok['ok'] and ok['stage']=='MINIMAL_LIVE'
    pre=g.pretrade_health_gate(base_context(risk_ready=True,broker_ready=True,data_ready=True,
        reconciliation_ok=True,governance_ok=True,system_ready=True,emergency_stop=False,governance_lock=False))
    assert pre['allow_new_real_order']


def test_profit_only_cannot_promote():
    root,g,rc,cfg,versions,files=make_env();satisfy_account_paper_dry(g,rc);g.certify(base_context(),rc['release_id'])
    g.activate_minimal_live(base_context(risk_ready=True,broker_ready=True,data_ready=True,reconciliation_ok=True,
        governance_ok=True,system_ready=True), 'risk','go')
    # Five profitable trades are insufficient in both time and sample size.
    for i in range(5):
        g.record_live_execution({'trade_id':f'T{i}','slippage_pips':.2,'latency_ms':50,'fees':.01,
          'reconciliation_ok':True,'protection_ok':True,'audit_ok':True,'trade_memory_ok':True,'realized_r':1})
    ctx=base_context(reconciliation_ok=True,risk_ready=True,governance_state='NORMAL_ADAPTATION',system_status='HEALTHY',
                     data_quality=.95,drawdown=.001,operational_reliability=True,risk_consistent=True,
                     max_slippage_pips=2.0,open_p0=0,open_p1=0,realized_pnl=10,reconciliation_status='MATCHED')
    hold=g.promotion_gate('LIMITED_LIVE',ctx)
    assert hold['action']=='HOLD_CURRENT_STAGE'
    assert 'INSUFFICIENT_LIVE_TRADES' in hold['reasons']
    assert 'INSUFFICIENT_LIVE_DAYS' in hold['reasons']


def test_evidence_based_promotion_and_deescalation():
    root,g,rc,cfg,versions,files=make_env();satisfy_account_paper_dry(g,rc);g.certify(base_context(),rc['release_id'])
    g.activate_minimal_live(base_context(risk_ready=True,broker_ready=True,data_ready=True,reconciliation_ok=True,
        governance_ok=True,system_ready=True), 'risk','go')
    for i in range(12):
        g.record_live_execution({'trade_id':f'T{i}','slippage_pips':.2,'latency_ms':60,'fees':.01,
          'reconciliation_ok':True,'protection_ok':True,'audit_ok':True,'trade_memory_ok':True,'realized_r':.2})
    c=g.conn();c.execute("UPDATE production_state SET stage_started_ts=? WHERE singleton=1",
                         ((datetime.now(timezone.utc)-timedelta(days=6)).isoformat(),));c.commit();c.close()
    ctx=base_context(reconciliation_ok=True,risk_ready=True,governance_state='NORMAL_ADAPTATION',system_status='HEALTHY',
                     data_quality=.95,drawdown=.001,operational_reliability=True,risk_consistent=True,
                     max_slippage_pips=2.0,open_p0=0,open_p1=0,realized_pnl=2,reconciliation_status='MATCHED')
    prom=g.promotion_gate('LIMITED_LIVE',ctx)
    assert prom['action']=='PROMOTE'
    down=g.automatic_safety_downgrade({'risk_ready':False,'broker_stable':True,'reconciliation_ok':True,'data_quality_ok':True})
    assert down['action']=='SUSPEND'
    assert g.state()['readiness_state']=='SUSPENDED'
    resume=g.resume_gate({'incident_resolved':True,'reconciliation_ok':True,'health_ok':True,'risk_ready':True,
                          'broker_ready':True,'data_ready':True,'governance_ok':True})
    assert resume['action']=='LIMITED_RESTART'
    assert resume['stage']=='MINIMAL_LIVE'


def test_position_mismatch_is_p0_and_suspends():
    root,g,rc,cfg,versions,files=make_env()
    iid=g.open_incident('P0','POSITION_MISMATCH','broker 0.6 vs internal 1.0')
    assert g.state()['readiness_state']=='SUSPENDED'
    assert g.pretrade_health_gate(base_context(risk_ready=True,broker_ready=True,data_ready=True,
        reconciliation_ok=False,governance_ok=True,system_ready=True))['allow_new_real_order'] is False
    g.resolve_incident(iid,'stale internal position',['reconcile from broker','verify protection'])


def test_continuous_certification_invalidates_major_change():
    root,g,rc,cfg,versions,files=make_env()
    out=g.continuous_certification({'major_code_change':True})
    assert out['status']=='CERTIFICATION_INVALIDATED'
    assert g.state()['readiness_state']=='BLOCKED'


def test_dry_run_never_sends_real_order():
    root,g,rc,cfg,versions,files=make_env()
    good=g.record_dry_run(rc['release_id'],{'market_data':True,'signal':True,'director':True,'risk':True,
        'governance':True,'execution_prepared':True},{'units':5},True,0)
    bad=g.record_dry_run(rc['release_id'],{'market_data':True,'signal':True,'director':True,'risk':True,
        'governance':True,'execution_prepared':True},{'units':5},True,1)
    assert good['passed'] and not bad['passed']


if __name__=='__main__':
    test_step14_pass_does_not_equal_production_ready()
    test_exact_release_can_reach_ready_for_minimal_live_only_with_all_evidence()
    test_release_change_requires_new_release_candidate()
    test_minimal_live_requires_explicit_authorization_and_health()
    test_profit_only_cannot_promote()
    test_evidence_based_promotion_and_deescalation()
    test_position_mismatch_is_p0_and_suspends()
    test_continuous_certification_invalidates_major_change()
    test_dry_run_never_sends_real_order()
    print('production readiness tests: OK')

from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
from pathlib import Path
import hashlib, json, math, os, sqlite3, uuid

READINESS_STATES=(
    "NOT_READY","BLOCKED","CONDITIONALLY_READY","READY_FOR_MINIMAL_LIVE",
    "MINIMAL_LIVE","LIMITED_LIVE","CONTROLLED_LIVE","PRODUCTION_APPROVED","SUSPENDED"
)
PRODUCTION_STAGES=("CERTIFICATION","MINIMAL_LIVE","LIMITED_LIVE","CONTROLLED_LIVE","PRODUCTION_APPROVED","SUSPENDED")
INCIDENT_SEVERITIES=("P0","P1","P2","P3")

DEFAULT_STAGE_LIMITS={
    "MINIMAL_LIVE":{
        "risk_cap_multiplier":0.05,
        "max_trade_risk_fraction":0.0005,
        "max_portfolio_exposure_fraction":0.005,
        "max_daily_loss_fraction":0.0025,
        "max_drawdown_fraction":0.005,
        "min_trades_for_promotion":10,
        "min_days_for_promotion":5,
        "max_slippage_pips":2.0,
        "max_reconciliation_mismatches":0,
    },
    "LIMITED_LIVE":{
        "risk_cap_multiplier":0.10,
        "max_trade_risk_fraction":0.0010,
        "max_portfolio_exposure_fraction":0.010,
        "max_daily_loss_fraction":0.005,
        "max_drawdown_fraction":0.010,
        "min_trades_for_promotion":25,
        "min_days_for_promotion":10,
        "max_slippage_pips":2.0,
        "max_reconciliation_mismatches":0,
    },
    "CONTROLLED_LIVE":{
        "risk_cap_multiplier":0.25,
        "max_trade_risk_fraction":0.0025,
        "max_portfolio_exposure_fraction":0.025,
        "max_daily_loss_fraction":0.010,
        "max_drawdown_fraction":0.020,
        "min_trades_for_promotion":50,
        "min_days_for_promotion":20,
        "max_slippage_pips":2.5,
        "max_reconciliation_mismatches":0,
    },
    "PRODUCTION_APPROVED":{
        # This is still bounded by Risk Engine hard limits. The readiness layer never enlarges them.
        "risk_cap_multiplier":1.0,
        "max_trade_risk_fraction":None,
        "max_portfolio_exposure_fraction":None,
        "max_daily_loss_fraction":None,
        "max_drawdown_fraction":None,
        "min_trades_for_promotion":0,
        "min_days_for_promotion":0,
        "max_slippage_pips":2.5,
        "max_reconciliation_mismatches":0,
    }
}

CRITICAL_CHECKS=(
    "STEP14_NO_CRITICAL_FAILURES","NO_RISK_BYPASS_KNOWN","NO_DUPLICATE_ORDER_VULNERABILITY",
    "RECONCILIATION_READY","EMERGENCY_STOP_READY","RISK_ENGINE_READY","BROKER_ACCOUNT_VERIFIED",
    "MARKET_DATA_FRESH","AUDIT_READY","GOVERNANCE_READY","DEPLOYMENT_STATE_CONSISTENT",
    "NO_STATE_CORRUPTION","FINAL_PAPER_PASS","PRODUCTION_DRY_RUN_PASS","CANARY_CONTROLS_READY",
    "RECOVERY_TESTS_PASS","SECURITY_TESTS_PASS","CHANGE_MANAGEMENT_READY","RELEASE_CANDIDATE_FROZEN",
    "PRODUCTION_AUTHORIZATION_PRESENT"
)


def now_iso(): return datetime.now(timezone.utc).isoformat()
def j(x): return json.dumps(x,separators=(",",":"),sort_keys=True,default=str)
def sha256_bytes(b:bytes): return hashlib.sha256(b).hexdigest()
def sha256_obj(x): return sha256_bytes(j(x).encode())
def f(x,default=None):
    try:
        v=float(x);return v if math.isfinite(v) else default
    except Exception:return default

def parse_ts(x):
    if not x:return None
    try:return datetime.fromisoformat(str(x).replace("Z","+00:00"))
    except Exception:return None

class ProductionReadinessGate:
    """
    Production certification/control plane.

    This module never submits orders. It certifies a frozen release, records paper/dry-run evidence,
    controls readiness/stage state, generates promotion evidence packages, and provides a pre-trade
    health gate for the execution layer.
    """
    def __init__(self,db_path:str,release_version:str,stage_limits:Optional[Dict[str,Dict[str,Any]]]=None):
        self.db_path=db_path;self.release_version=release_version
        self.stage_limits={k:{**v} for k,v in DEFAULT_STAGE_LIMITS.items()}
        for stage,overrides in (stage_limits or {}).items():
            if stage in self.stage_limits:self.stage_limits[stage].update(overrides or {})

    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30,isolation_level=None)
        c.row_factory=sqlite3.Row;c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA synchronous=FULL")
        c.execute("PRAGMA busy_timeout=5000");return c

    def ensure_schema(self):
        c=self.conn();c.executescript("""
        CREATE TABLE IF NOT EXISTS production_release_candidates(
          release_id TEXT PRIMARY KEY,created_ts TEXT NOT NULL,release_version TEXT NOT NULL,status TEXT NOT NULL,
          code_fingerprint TEXT NOT NULL,config_fingerprint TEXT NOT NULL,dependency_fingerprint TEXT,
          files_json TEXT NOT NULL,config_json TEXT NOT NULL,versions_json TEXT NOT NULL,step14_report_hash TEXT,
          step14_report_path TEXT,freeze_reason TEXT,invalidated_ts TEXT,invalidation_reason TEXT,
          certified_ts TEXT,certification_id TEXT
        );
        CREATE TABLE IF NOT EXISTS production_certifications(
          certification_id TEXT PRIMARY KEY,release_id TEXT NOT NULL,created_ts TEXT NOT NULL,readiness_state TEXT NOT NULL,
          go_no_go TEXT NOT NULL,checklist_json TEXT NOT NULL,blockers_json TEXT NOT NULL,warnings_json TEXT NOT NULL,
          capital_limits_json TEXT NOT NULL,suspension_conditions_json TEXT NOT NULL,promotion_conditions_json TEXT NOT NULL,
          release_fingerprint TEXT NOT NULL,expires_ts TEXT,invalidated_ts TEXT,invalidation_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS production_state(
          singleton INTEGER PRIMARY KEY CHECK(singleton=1),readiness_state TEXT NOT NULL,production_stage TEXT NOT NULL,
          release_id TEXT,certification_id TEXT,production_authorized INTEGER NOT NULL DEFAULT 0,
          production_suspended INTEGER NOT NULL DEFAULT 0,suspension_reason TEXT,last_stage_change_ts TEXT,
          stage_started_ts TEXT,last_certification_ts TEXT,last_reconciliation_ts TEXT,last_reconciliation_status TEXT,
          last_health_check_ts TEXT,last_health_status TEXT,last_broker_verified_ts TEXT,last_data_fresh_ts TEXT,
          actual_exposure_fraction REAL DEFAULT 0,updated_ts TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS production_account_verification(
          verification_id TEXT PRIMARY KEY,release_id TEXT,ts TEXT NOT NULL,environment TEXT NOT NULL,
          expected_json TEXT NOT NULL,observed_json TEXT NOT NULL,passed INTEGER NOT NULL,reasons_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS production_final_paper_runs(
          paper_run_id TEXT PRIMARY KEY,release_id TEXT NOT NULL,started_ts TEXT NOT NULL,completed_ts TEXT,
          status TEXT NOT NULL,trades INTEGER NOT NULL DEFAULT 0,days REAL NOT NULL DEFAULT 0,
          regimes INTEGER NOT NULL DEFAULT 0,execution_parity INTEGER NOT NULL DEFAULT 0,config_match INTEGER NOT NULL DEFAULT 0,
          code_match INTEGER NOT NULL DEFAULT 0,risk_match INTEGER NOT NULL DEFAULT 0,governance_match INTEGER NOT NULL DEFAULT 0,
          results_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS production_dry_runs(
          dry_run_id TEXT PRIMARY KEY,release_id TEXT NOT NULL,ts TEXT NOT NULL,status TEXT NOT NULL,
          pipeline_json TEXT NOT NULL,expected_order_json TEXT,blocked_before_send INTEGER NOT NULL,
          real_broker_request_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS production_live_evidence(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,release_id TEXT NOT NULL,stage TEXT NOT NULL,
          trade_id TEXT,expected_order_json TEXT,actual_order_json TEXT,fill_json TEXT,
          slippage_pips REAL,latency_ms REAL,fees REAL,partial_fill INTEGER DEFAULT 0,rejected INTEGER DEFAULT 0,
          reconciliation_ok INTEGER,protection_ok INTEGER,audit_ok INTEGER,trade_memory_ok INTEGER,
          realized_r REAL,incident_id TEXT,details_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS production_incidents(
          incident_id TEXT PRIMARY KEY,opened_ts TEXT NOT NULL,resolved_ts TEXT,severity TEXT NOT NULL,
          incident_type TEXT NOT NULL,status TEXT NOT NULL,summary TEXT NOT NULL,timeline_json TEXT NOT NULL,
          impact_json TEXT NOT NULL,root_cause TEXT,controls_worked_json TEXT,controls_failed_json TEXT,
          corrective_actions_json TEXT,capital_impact REAL DEFAULT 0,release_id TEXT,certification_id TEXT
        );
        CREATE TABLE IF NOT EXISTS production_stage_events(
          event_id TEXT PRIMARY KEY,ts TEXT NOT NULL,release_id TEXT,certification_id TEXT,previous_stage TEXT,
          new_stage TEXT NOT NULL,event_type TEXT NOT NULL,actor TEXT NOT NULL,reason TEXT NOT NULL,
          evidence_json TEXT NOT NULL,automatic INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS production_readiness_audit(
          audit_id TEXT PRIMARY KEY,ts TEXT NOT NULL,release_id TEXT,certification_id TEXT,action TEXT NOT NULL,
          actor TEXT NOT NULL,result TEXT NOT NULL,reason TEXT NOT NULL,payload_json TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS prod_rc_no_update_fingerprint
          BEFORE UPDATE OF code_fingerprint,config_fingerprint,dependency_fingerprint,files_json,config_json,versions_json
          ON production_release_candidates
          WHEN OLD.status IN ('FROZEN','CERTIFIED')
          BEGIN SELECT RAISE(ABORT,'frozen release candidate fingerprints are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS prod_cert_no_update_core
          BEFORE UPDATE OF checklist_json,blockers_json,warnings_json,capital_limits_json,release_fingerprint
          ON production_certifications
          BEGIN SELECT RAISE(ABORT,'production certification evidence is immutable'); END;
        CREATE INDEX IF NOT EXISTS idx_prod_evidence_stage ON production_live_evidence(stage,ts);
        CREATE INDEX IF NOT EXISTS idx_prod_incidents_status ON production_incidents(status,severity,opened_ts);
        """)
        if not c.execute("SELECT 1 FROM production_state WHERE singleton=1").fetchone():
            c.execute("""INSERT INTO production_state(singleton,readiness_state,production_stage,updated_ts)
                         VALUES(1,'NOT_READY','CERTIFICATION',?)""",(now_iso(),))
        # Extend Trade Memory only if it already exists; no duplicate storage system is introduced.
        tables={x[0] for x in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "trade_memory" in tables:
            cols={x[1] for x in c.execute("PRAGMA table_info(trade_memory)").fetchall()}
            for col,ddl in (
                ("release_id","ALTER TABLE trade_memory ADD COLUMN release_id TEXT"),
                ("production_certification_id","ALTER TABLE trade_memory ADD COLUMN production_certification_id TEXT"),
                ("production_stage","ALTER TABLE trade_memory ADD COLUMN production_stage TEXT")
            ):
                if col not in cols:c.execute(ddl)
        c.commit();c.close()

    def audit(self,action,actor,result,reason,payload=None,release_id=None,certification_id=None):
        self.ensure_schema();aid="pra_"+uuid.uuid4().hex;c=self.conn()
        c.execute("""INSERT INTO production_readiness_audit(
          audit_id,ts,release_id,certification_id,action,actor,result,reason,payload_json)
          VALUES(?,?,?,?,?,?,?,?,?)""",
          (aid,now_iso(),release_id,certification_id,action,actor,result,reason,j(payload or {})))
        c.commit();c.close();return aid

    def state(self):
        self.ensure_schema();c=self.conn();r=c.execute("SELECT * FROM production_state WHERE singleton=1").fetchone();c.close()
        return dict(r)

    @staticmethod
    def hash_files(paths:List[str])->Tuple[str,List[Dict[str,Any]]]:
        rows=[]
        for raw in sorted(set(paths)):
            p=Path(raw)
            if not p.exists() or not p.is_file():
                rows.append({"path":str(p),"exists":False,"sha256":None,"size":None});continue
            b=p.read_bytes();rows.append({"path":str(p),"exists":True,"sha256":sha256_bytes(b),"size":len(b)})
        return sha256_obj(rows),rows

    def create_release_candidate(self,*,files:List[str],config:Dict[str,Any],versions:Dict[str,Any],
                                 step14_report_path:Optional[str],actor:str="SYSTEM")->Dict[str,Any]:
        self.ensure_schema();code_hash,file_rows=self.hash_files(files);config_hash=sha256_obj(config)
        dep_hash=sha256_obj(versions.get("dependencies") or {})
        step14_hash=None
        if step14_report_path and Path(step14_report_path).exists(): step14_hash=sha256_bytes(Path(step14_report_path).read_bytes())
        rid=f"rc_{self.release_version}_{code_hash[:10]}_{config_hash[:10]}"
        c=self.conn();existing=c.execute("SELECT * FROM production_release_candidates WHERE release_id=?",(rid,)).fetchone()
        if not existing:
            c.execute("""INSERT INTO production_release_candidates(
              release_id,created_ts,release_version,status,code_fingerprint,config_fingerprint,dependency_fingerprint,
              files_json,config_json,versions_json,step14_report_hash,step14_report_path,freeze_reason)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (rid,now_iso(),self.release_version,"FROZEN",code_hash,config_hash,dep_hash,j(file_rows),j(config),j(versions),
               step14_hash,step14_report_path,"certification release candidate frozen"))
        c.execute("""UPDATE production_state SET release_id=?,readiness_state='NOT_READY',production_stage='CERTIFICATION',
                     production_authorized=0,updated_ts=? WHERE singleton=1""",(rid,now_iso()))
        c.commit();row=c.execute("SELECT * FROM production_release_candidates WHERE release_id=?",(rid,)).fetchone();c.close()
        self.audit("RELEASE_CANDIDATE_FROZEN",actor,"APPLIED","release candidate frozen",{"release_id":rid},rid)
        return dict(row)

    def release_candidate(self,release_id:Optional[str]=None):
        self.ensure_schema();rid=release_id or self.state().get("release_id");c=self.conn()
        r=c.execute("SELECT * FROM production_release_candidates WHERE release_id=?",(rid,)).fetchone() if rid else None;c.close()
        return dict(r) if r else None

    def verify_release_unchanged(self,release_id:str,files:List[str],config:Dict[str,Any],versions:Dict[str,Any])->Dict[str,Any]:
        rc=self.release_candidate(release_id)
        if not rc:return {"passed":False,"reason":"RELEASE_CANDIDATE_NOT_FOUND"}
        code,_=self.hash_files(files);cfg=sha256_obj(config);deps=sha256_obj(versions.get("dependencies") or {})
        mismatches=[]
        if code!=rc["code_fingerprint"]:mismatches.append("CODE_CHANGED")
        if cfg!=rc["config_fingerprint"]:mismatches.append("CONFIG_CHANGED")
        if deps!=rc["dependency_fingerprint"]:mismatches.append("DEPENDENCIES_CHANGED")
        return {"passed":not mismatches,"mismatches":mismatches,"expected":{
            "code":rc["code_fingerprint"],"config":rc["config_fingerprint"],"dependencies":rc["dependency_fingerprint"]},
            "actual":{"code":code,"config":cfg,"dependencies":deps}}

    def invalidate_certification(self,reason:str,actor:str="SYSTEM",release_id:Optional[str]=None):
        st=self.state();rid=release_id or st.get("release_id");ts=now_iso();c=self.conn()
        c.execute("UPDATE production_release_candidates SET invalidated_ts=?,invalidation_reason=?,status='INVALIDATED' WHERE release_id=?",
                  (ts,reason,rid))
        c.execute("UPDATE production_certifications SET invalidated_ts=?,invalidation_reason=? WHERE release_id=? AND invalidated_ts IS NULL",
                  (ts,reason,rid))
        c.execute("""UPDATE production_state SET readiness_state='BLOCKED',production_authorized=0,
                     production_suspended=CASE WHEN production_stage!='CERTIFICATION' THEN 1 ELSE production_suspended END,
                     suspension_reason=?,updated_ts=? WHERE singleton=1""",(f"CERTIFICATION_INVALIDATED:{reason}",ts))
        c.commit();c.close();self.audit("CERTIFICATION_INVALIDATED",actor,"APPLIED",reason,{},rid,st.get("certification_id"))
        return self.state()

    def verify_account(self,release_id:str,environment:str,expected:Dict[str,Any],observed:Dict[str,Any],actor="SYSTEM"):
        reasons=[]
        if str(environment).upper()!="PRODUCTION":reasons.append("ENVIRONMENT_NOT_PRODUCTION")
        fields=("broker","account_id","account_type","currency")
        for key in fields:
            if expected.get(key) is not None and str(expected.get(key))!=str(observed.get(key)):reasons.append(f"{key.upper()}_MISMATCH")
        for key in ("permissions_ok","market_access_ok","leverage_ok","margin_settings_ok","balance_within_expected_range"):
            if observed.get(key) is not True:reasons.append(key.upper()+"_FALSE")
        passed=not reasons;vid="acct_"+uuid.uuid4().hex;c=self.conn()
        c.execute("""INSERT INTO production_account_verification(
          verification_id,release_id,ts,environment,expected_json,observed_json,passed,reasons_json)
          VALUES(?,?,?,?,?,?,?,?)""",(vid,release_id,now_iso(),str(environment).upper(),j(expected),j(observed),int(passed),j(reasons)))
        if passed:c.execute("UPDATE production_state SET last_broker_verified_ts=?,updated_ts=? WHERE singleton=1",(now_iso(),now_iso()))
        c.commit();c.close();self.audit("PRODUCTION_ACCOUNT_VERIFIED",actor,"PASS" if passed else "FAIL",";".join(reasons) or "account verified",{"verification_id":vid},release_id)
        return {"verification_id":vid,"passed":passed,"reasons":reasons}

    def record_final_paper(self,release_id:str,results:Dict[str,Any],actor="SYSTEM"):
        # Exact-release parity checks are mandatory.
        required=("trades","days","regimes","execution_parity","config_match","code_match","risk_match","governance_match")
        missing=[k for k in required if k not in results]
        passed=(not missing and int(results["trades"])>=10 and float(results["days"])>=3 and int(results["regimes"])>=1 and
                all(bool(results[k]) for k in ("execution_parity","config_match","code_match","risk_match","governance_match")) and
                results.get("critical_incidents",0)==0)
        pid="paper_"+uuid.uuid4().hex;c=self.conn()
        c.execute("""INSERT INTO production_final_paper_runs(
          paper_run_id,release_id,started_ts,completed_ts,status,trades,days,regimes,execution_parity,
          config_match,code_match,risk_match,governance_match,results_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (pid,release_id,results.get("started_ts") or now_iso(),now_iso(),"PASS" if passed else "FAIL",
           int(results.get("trades",0)),float(results.get("days",0)),int(results.get("regimes",0)),
           int(bool(results.get("execution_parity"))),int(bool(results.get("config_match"))),int(bool(results.get("code_match"))),
           int(bool(results.get("risk_match"))),int(bool(results.get("governance_match"))),j(results)))
        c.commit();c.close();self.audit("FINAL_PAPER_RUN",actor,"PASS" if passed else "FAIL","final paper certification run",{"paper_run_id":pid,"missing":missing},release_id)
        return {"paper_run_id":pid,"passed":passed,"missing":missing}

    def record_dry_run(self,release_id:str,pipeline:Dict[str,Any],expected_order:Optional[Dict[str,Any]],
                       blocked_before_send:bool,real_broker_request_count:int,actor="SYSTEM"):
        # Dry run is successful only if the full decision pipeline reaches prepared execution and the real send is blocked.
        required=("market_data","signal","director","risk","governance","execution_prepared")
        passed=all(bool(pipeline.get(k)) for k in required) and bool(blocked_before_send) and int(real_broker_request_count)==0
        did="dry_"+uuid.uuid4().hex;c=self.conn()
        c.execute("""INSERT INTO production_dry_runs(
          dry_run_id,release_id,ts,status,pipeline_json,expected_order_json,blocked_before_send,real_broker_request_count)
          VALUES(?,?,?,?,?,?,?,?)""",
          (did,release_id,now_iso(),"PASS" if passed else "FAIL",j(pipeline),j(expected_order or {}),int(blocked_before_send),int(real_broker_request_count)))
        c.commit();c.close();self.audit("PRODUCTION_DRY_RUN",actor,"PASS" if passed else "FAIL","production dry run",{"dry_run_id":did},release_id)
        return {"dry_run_id":did,"passed":passed}

    def _step14(self,rc:Dict[str,Any])->Dict[str,Any]:
        path=rc.get("step14_report_path")
        if not path or not Path(path).exists():return {"available":False,"passed":False,"reason":"STEP14_REPORT_MISSING"}
        try:r=json.loads(Path(path).read_text())
        except Exception as e:return {"available":False,"passed":False,"reason":"STEP14_REPORT_INVALID","error":str(e)}
        current_hash=sha256_bytes(Path(path).read_bytes())
        unchanged=current_hash==rc.get("step14_report_hash")
        gate=(r.get("pass_fail_gate") or {}).get("ready_for_step15") is True
        return {"available":True,"passed":bool(unchanged and gate and r.get("critical_failures",1)==0 and r.get("safety_violations",1)==0),
                "unchanged":unchanged,"report":r}

    def _latest(self,table,where="",params=()):
        c=self.conn()
        try:r=c.execute(f"SELECT * FROM {table} {where}",params).fetchone()
        except sqlite3.OperationalError:r=None
        c.close();return dict(r) if r else None

    def build_checklist(self,context:Dict[str,Any],release_id:Optional[str]=None)->Dict[str,Dict[str,Any]]:
        st=self.state();rid=release_id or st.get("release_id");rc=self.release_candidate(rid)
        if not rc:return {k:{"passed":False,"reason":"NO_RELEASE_CANDIDATE"} for k in CRITICAL_CHECKS}
        step14=self._step14(rc)
        paper=self._latest("production_final_paper_runs","WHERE release_id=? ORDER BY completed_ts DESC LIMIT 1",(rid,))
        dry=self._latest("production_dry_runs","WHERE release_id=? ORDER BY ts DESC LIMIT 1",(rid,))
        acct=self._latest("production_account_verification","WHERE release_id=? ORDER BY ts DESC LIMIT 1",(rid,))
        evalr=self._latest("system_evaluations","ORDER BY generated_at DESC LIMIT 1")
        gov=self._latest("governance_state","WHERE singleton=1")
        rec=self._latest("recovery_state","WHERE account_scope=?",(context.get("account_scope","PRIMARY"),))
        open_p0=self._latest("production_incidents","WHERE status!='RESOLVED' AND severity='P0' ORDER BY opened_ts DESC LIMIT 1")
        audit_available=context.get("audit_ready",False)
        security_pass=context.get("security_tests_pass",False)
        risk_shadow=bool(context.get("risk_engine_shadow_mode",True))
        risk_ready=bool(context.get("risk_engine_ready",False)) and not risk_shadow
        broker_reconciled=bool(context.get("broker_reconciled",False))
        gov_ready=(gov is not None and gov.get("adaptation_state") not in ("ADAPTATION_FROZEN",) and not int(gov.get("governance_lock") or 0))
        system_eval_ok=(evalr is not None and evalr.get("system_status") not in ("CRITICAL","HIGH_RISK","PAUSED","DEGRADING") and f(evalr.get("data_quality_score"),0)>=.75)
        checks={
          "STEP14_NO_CRITICAL_FAILURES":{"passed":step14.get("passed",False),"evidence":step14.get("reason") or "Step14 immutable report"},
          "NO_RISK_BYPASS_KNOWN":{"passed":bool(step14.get("passed",False) and context.get("no_risk_bypass_known",False)),"evidence":"Step14 + runtime declaration"},
          "NO_DUPLICATE_ORDER_VULNERABILITY":{"passed":bool(step14.get("passed",False) and context.get("no_duplicate_order_vulnerability",False)),"evidence":"idempotency/concurrency tests"},
          "RECONCILIATION_READY":{"passed":broker_reconciled and (rec is None or rec.get("last_reconciliation_status") in ("MATCHED","MINOR_MISMATCH","READY",None)),"evidence":rec or {}},
          "EMERGENCY_STOP_READY":{"passed":bool(context.get("emergency_stop_test_pass",False)),"evidence":"restart persistence + reset procedure"},
          "RISK_ENGINE_READY":{"passed":risk_ready,"evidence":{"reported_ready":context.get("risk_engine_ready"),"shadow_mode":risk_shadow},"blocker":"RISK_ENGINE_SHADOW_ONLY" if risk_shadow else None},
          "BROKER_ACCOUNT_VERIFIED":{"passed":bool(acct and acct.get("passed")),"evidence":acct or {}},
          "MARKET_DATA_FRESH":{"passed":bool(context.get("market_data_fresh",False)),"evidence":{"last_data_ts":context.get("last_data_ts")}},
          "AUDIT_READY":{"passed":bool(audit_available),"evidence":"audit subsystem/integrity"},
          "GOVERNANCE_READY":{"passed":bool(gov_ready),"evidence":gov or {}},
          "DEPLOYMENT_STATE_CONSISTENT":{"passed":bool(context.get("deployment_state_consistent",False)),"evidence":context.get("deployment_state")},
          "NO_STATE_CORRUPTION":{"passed":bool(context.get("no_state_corruption",False) and not open_p0),"evidence":{"open_p0":open_p0}},
          "FINAL_PAPER_PASS":{"passed":bool(paper and paper.get("status")=="PASS"),"evidence":paper or {}},
          "PRODUCTION_DRY_RUN_PASS":{"passed":bool(dry and dry.get("status")=="PASS" and int(dry.get("real_broker_request_count") or 0)==0),"evidence":dry or {}},
          "CANARY_CONTROLS_READY":{"passed":bool(context.get("canary_controls_ready",False)),"evidence":"Step8/14 canary rollback & limits"},
          "RECOVERY_TESTS_PASS":{"passed":bool(context.get("recovery_tests_pass",False)),"evidence":"Step10/14"},
          "SECURITY_TESTS_PASS":{"passed":bool(security_pass),"evidence":"Step11/14"},
          "CHANGE_MANAGEMENT_READY":{"passed":bool(context.get("change_management_ready",False)),"evidence":"RBAC/change-request/audit"},
          "RELEASE_CANDIDATE_FROZEN":{"passed":rc.get("status")=="FROZEN" and not rc.get("invalidated_ts"),"evidence":{"release_id":rid,"status":rc.get("status")}},
          "PRODUCTION_AUTHORIZATION_PRESENT":{"passed":bool(context.get("production_authorized",False)),"evidence":{"environment":context.get("environment"),"production_authorized":context.get("production_authorized")}},
          "SYSTEM_EVALUATION_STABLE":{"passed":bool(system_eval_ok),"evidence":evalr or {}},
          "MONITORING_READY":{"passed":bool(context.get("monitoring_ready",False)),"evidence":"observability/alerts"},
        }
        return checks

    def certify(self,context:Dict[str,Any],release_id:Optional[str]=None,actor="RISK_MANAGER"):
        self.ensure_schema();rid=release_id or self.state().get("release_id");rc=self.release_candidate(rid)
        if not rc:return {"readiness_state":"NOT_READY","go_no_go":"NO_GO","blockers":["NO_RELEASE_CANDIDATE"]}
        checks=self.build_checklist(context,rid)
        mandatory_fail=[k for k in CRITICAL_CHECKS if not checks.get(k,{}).get("passed")]
        extra_fail=[k for k in ("SYSTEM_EVALUATION_STABLE","MONITORING_READY") if not checks.get(k,{}).get("passed")]
        blockers=mandatory_fail+extra_fail
        warnings=[]
        if context.get("environment")!="PRODUCTION":blockers.append("ENVIRONMENT_NOT_PRODUCTION")
        if context.get("open_p1_incidents",0)>0:warnings.append("OPEN_P1_INCIDENTS")
        if blockers:
            state="BLOCKED" if any(x in blockers for x in ("RISK_ENGINE_READY","BROKER_ACCOUNT_VERIFIED","NO_STATE_CORRUPTION","RECONCILIATION_READY","PRODUCTION_AUTHORIZATION_PRESENT")) else "NOT_READY"
            go="NO_GO"
        elif warnings:
            state="CONDITIONALLY_READY";go="CONDITIONAL_GO"
        else:
            state="READY_FOR_MINIMAL_LIVE";go="GO"
        cid="cert_"+uuid.uuid4().hex;fingerprint=sha256_obj({"release":rc["code_fingerprint"],"config":rc["config_fingerprint"],"checks":checks})
        limits=self.effective_stage_limits("MINIMAL_LIVE",context.get("hard_limits") or {})
        suspension=["P0_INCIDENT","ACCOUNT_MISMATCH","RECONCILIATION_CRITICAL","RISK_ENGINE_UNAVAILABLE","MARKET_DATA_UNRELIABLE",
                    "EMERGENCY_STOP","STATE_CORRUPTION","AUDIT_INTEGRITY_FAILURE","GOVERNANCE_LOCK","CRITICAL_SYSTEM_EVALUATION","CERTIFICATION_INVALIDATED"]
        promotion=["MIN_TRADES","MIN_DAYS","EXECUTION_QUALITY_OK","DRAWDOWN_OK","RECONCILIATION_CLEAN","NO_CRITICAL_INCIDENTS",
                   "SYSTEM_STABLE","GOVERNANCE_NORMAL","RISK_READY","DATA_QUALITY_OK","LIVE_EXECUTION_DIVERGENCE_FALSE"]
        expires=(datetime.now(timezone.utc)+timedelta(days=30)).isoformat()
        c=self.conn();c.execute("""INSERT INTO production_certifications(
          certification_id,release_id,created_ts,readiness_state,go_no_go,checklist_json,blockers_json,warnings_json,
          capital_limits_json,suspension_conditions_json,promotion_conditions_json,release_fingerprint,expires_ts)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (cid,rid,now_iso(),state,go,j(checks),j(blockers),j(warnings),j(limits),j(suspension),j(promotion),fingerprint,expires))
        c.execute("""UPDATE production_state SET certification_id=?,readiness_state=?,production_stage='CERTIFICATION',
                     production_authorized=?,last_certification_ts=?,last_health_check_ts=?,last_health_status=?,updated_ts=?
                     WHERE singleton=1""",
                  (cid,state,int(state=="READY_FOR_MINIMAL_LIVE" and bool(context.get("production_authorized"))),now_iso(),now_iso(),state,now_iso()))
        if state=="READY_FOR_MINIMAL_LIVE":
            c.execute("UPDATE production_release_candidates SET certified_ts=?,certification_id=?,status='CERTIFIED' WHERE release_id=?",
                      (now_iso(),cid,rid))
        c.commit();c.close();self.audit("PRODUCTION_CERTIFICATION",actor,go,";".join(blockers) or "all readiness gates passed",{"checklist":checks,"warnings":warnings},rid,cid)
        return {"certification_id":cid,"release_id":rid,"readiness_state":state,"go_no_go":go,"checklist":checks,
                "blockers":blockers,"warnings":warnings,"minimal_live_limits":limits,"suspension_conditions":suspension,"promotion_conditions":promotion}

    def effective_stage_limits(self,stage:str,hard_limits:Dict[str,Any]):
        base={**self.stage_limits.get(stage,{})}
        # Never enlarge Risk Engine hard ceilings; stage caps are min(stage, hard).
        if base.get("max_trade_risk_fraction") is None:base["max_trade_risk_fraction"]=hard_limits.get("max_trade_risk_fraction")
        elif hard_limits.get("max_trade_risk_fraction") is not None:base["max_trade_risk_fraction"]=min(base["max_trade_risk_fraction"],float(hard_limits["max_trade_risk_fraction"]))
        if base.get("max_portfolio_exposure_fraction") is None:base["max_portfolio_exposure_fraction"]=hard_limits.get("max_portfolio_exposure_fraction")
        elif hard_limits.get("max_portfolio_exposure_fraction") is not None:base["max_portfolio_exposure_fraction"]=min(base["max_portfolio_exposure_fraction"],float(hard_limits["max_portfolio_exposure_fraction"]))
        if base.get("max_drawdown_fraction") is None:base["max_drawdown_fraction"]=hard_limits.get("max_drawdown_fraction")
        elif hard_limits.get("max_drawdown_fraction") is not None:base["max_drawdown_fraction"]=min(base["max_drawdown_fraction"],float(hard_limits["max_drawdown_fraction"]))
        base["principle"]="STAGE_CAP_NEVER_EXCEEDS_RISK_ENGINE_HARD_LIMIT"
        return base

    def pretrade_health_gate(self,context:Dict[str,Any])->Dict[str,Any]:
        st=self.state();reasons=[]
        if context.get("environment")!="PRODUCTION":reasons.append("ENVIRONMENT_NOT_PRODUCTION")
        if not bool(context.get("production_authorized")):reasons.append("PRODUCTION_NOT_AUTHORIZED")
        if st.get("readiness_state") not in ("MINIMAL_LIVE","LIMITED_LIVE","PRODUCTION_APPROVED"):reasons.append("READINESS_NOT_LIVE")
        if st.get("production_stage") not in ("MINIMAL_LIVE","LIMITED_LIVE","CONTROLLED_LIVE","PRODUCTION_APPROVED"):reasons.append("PRODUCTION_STAGE_NOT_LIVE")
        if int(st.get("production_suspended") or 0):reasons.append("PRODUCTION_SUSPENDED")
        for key,label in (("system_ready","SYSTEM_NOT_READY"),("risk_ready","RISK_NOT_READY"),("broker_ready","BROKER_NOT_READY"),
                          ("data_ready","DATA_NOT_READY"),("reconciliation_ok","RECONCILIATION_NOT_OK"),("governance_ok","GOVERNANCE_NOT_OK")):
            if not bool(context.get(key)):reasons.append(label)
        if context.get("emergency_stop"):reasons.append("EMERGENCY_STOP")
        if context.get("governance_lock"):reasons.append("GOVERNANCE_LOCK")
        return {"allow_new_real_order":not reasons,"reasons":reasons,"stage":st.get("production_stage"),"readiness":st.get("readiness_state")}

    def activate_minimal_live(self,context:Dict[str,Any],actor:str,reason:str):
        st=self.state();gate=self.pretrade_health_gate({**context,"system_ready":True,"risk_ready":True,"broker_ready":True,
                                                       "data_ready":True,"reconciliation_ok":True,"governance_ok":True})
        # Activation is special: readiness must be READY_FOR_MINIMAL_LIVE before pretrade becomes live.
        if st.get("readiness_state")!="READY_FOR_MINIMAL_LIVE":return {"ok":False,"reason":"READINESS_NOT_READY_FOR_MINIMAL_LIVE","state":st}
        if context.get("environment")!="PRODUCTION" or not context.get("production_authorized"):
            return {"ok":False,"reason":"EXPLICIT_PRODUCTION_AUTHORIZATION_REQUIRED"}
        if not all(context.get(k) for k in ("risk_ready","broker_ready","data_ready","reconciliation_ok","governance_ok","system_ready")):
            return {"ok":False,"reason":"PRETRADE_HEALTH_NOT_READY"}
        if context.get("emergency_stop") or context.get("governance_lock"):return {"ok":False,"reason":"SAFETY_LOCK_ACTIVE"}
        ts=now_iso();c=self.conn();c.execute("""UPDATE production_state SET readiness_state='MINIMAL_LIVE',production_stage='MINIMAL_LIVE',
          production_authorized=1,production_suspended=0,suspension_reason=NULL,last_stage_change_ts=?,stage_started_ts=?,updated_ts=? WHERE singleton=1""",(ts,ts,ts));c.commit();c.close()
        self._stage_event(st.get("production_stage"),"MINIMAL_LIVE","ACTIVATE_MINIMAL_LIVE",actor,reason,{"context":context},False)
        return {"ok":True,"stage":"MINIMAL_LIVE","limits":self.effective_stage_limits("MINIMAL_LIVE",context.get("hard_limits") or {}),"no_auto_scale":True}

    def _stage_event(self,previous,new,event_type,actor,reason,evidence,automatic=False):
        st=self.state();eid="pse_"+uuid.uuid4().hex;c=self.conn()
        c.execute("""INSERT INTO production_stage_events(event_id,ts,release_id,certification_id,previous_stage,new_stage,event_type,actor,reason,evidence_json,automatic)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                  (eid,now_iso(),st.get("release_id"),st.get("certification_id"),previous,new,event_type,actor,reason,j(evidence),int(automatic)))
        c.commit();c.close();return eid

    def record_live_execution(self,record:Dict[str,Any]):
        st=self.state();c=self.conn()
        c.execute("""INSERT INTO production_live_evidence(
          ts,release_id,stage,trade_id,expected_order_json,actual_order_json,fill_json,slippage_pips,latency_ms,fees,
          partial_fill,rejected,reconciliation_ok,protection_ok,audit_ok,trade_memory_ok,realized_r,incident_id,details_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (record.get("ts") or now_iso(),st.get("release_id"),st.get("production_stage"),record.get("trade_id"),j(record.get("expected_order") or {}),
           j(record.get("actual_order") or {}),j(record.get("fill") or {}),record.get("slippage_pips"),record.get("latency_ms"),record.get("fees"),
           int(bool(record.get("partial_fill"))),int(bool(record.get("rejected"))),int(bool(record.get("reconciliation_ok"))),
           int(bool(record.get("protection_ok"))),int(bool(record.get("audit_ok"))),int(bool(record.get("trade_memory_ok"))),record.get("realized_r"),
           record.get("incident_id"),j(record.get("details") or {})))
        c.commit();c.close()

    def live_metrics(self,stage:Optional[str]=None):
        st=self.state();stage=stage or st.get("production_stage");c=self.conn()
        rows=[dict(x) for x in c.execute("SELECT * FROM production_live_evidence WHERE stage=? ORDER BY ts",(stage,)).fetchall()];c.close()
        n=len(rows);fills=[x for x in rows if not x.get("rejected")];slip=[f(x.get("slippage_pips")) for x in rows if f(x.get("slippage_pips")) is not None]
        lat=[f(x.get("latency_ms")) for x in rows if f(x.get("latency_ms")) is not None];rr=[f(x.get("realized_r")) for x in rows if f(x.get("realized_r")) is not None]
        mismatches=sum(1 for x in rows if not int(x.get("reconciliation_ok") or 0));protection_fail=sum(1 for x in rows if not int(x.get("protection_ok") or 0))
        return {"stage":stage,"trades":n,"fill_rate":len(fills)/n if n else None,"rejection_rate":sum(int(x.get("rejected") or 0) for x in rows)/n if n else None,
                "avg_slippage_pips":sum(slip)/len(slip) if slip else None,"avg_latency_ms":sum(lat)/len(lat) if lat else None,
                "expectancy_r":sum(rr)/len(rr) if rr else None,"reconciliation_mismatches":mismatches,"protection_failures":protection_fail,
                "fees":sum(f(x.get("fees"),0) or 0 for x in rows),"partial_fills":sum(int(x.get("partial_fill") or 0) for x in rows)}

    def paper_vs_real_comparison(self,release_id:Optional[str]=None,stage:Optional[str]=None)->Dict[str,Any]:
        st=self.state();rid=release_id or st.get("release_id");stage=stage or st.get("production_stage")
        paper=self._latest("production_final_paper_runs","WHERE release_id=? AND status='PASS' ORDER BY completed_ts DESC LIMIT 1",(rid,))
        live=self.live_metrics(stage)
        if not paper:return {"status":"INSUFFICIENT_PAPER_DATA","material":True,"reasons":["NO_FINAL_PAPER_BASELINE"]}
        try:pres=json.loads(paper.get("results_json") or "{}")
        except Exception:pres={}
        reasons=[];diffs={}
        expected_slip=f(pres.get("avg_slippage_pips"));real_slip=f(live.get("avg_slippage_pips"))
        if expected_slip is not None and real_slip is not None:
            diffs["slippage_pips"]={"paper":expected_slip,"real":real_slip,"delta":real_slip-expected_slip}
            if real_slip>max(expected_slip*2,expected_slip+1.0):reasons.append("SLIPPAGE_DIVERGENCE")
        expected_fill=f(pres.get("fill_rate"));real_fill=f(live.get("fill_rate"))
        if expected_fill is not None and real_fill is not None:
            diffs["fill_rate"]={"paper":expected_fill,"real":real_fill,"delta":real_fill-expected_fill}
            if real_fill<expected_fill-.10:reasons.append("FILL_RATE_DIVERGENCE")
        expected_exp=f(pres.get("expectancy_r"));real_exp=f(live.get("expectancy_r"))
        if expected_exp is not None and real_exp is not None and live.get("trades",0)>=5:
            diffs["expectancy_r"]={"paper":expected_exp,"real":real_exp,"delta":real_exp-expected_exp}
            if expected_exp>0 and real_exp<0:reasons.append("EXPECTANCY_SIGN_DIVERGENCE")
        expected_freq=f(pres.get("trades_per_day"));
        if expected_freq is not None and live.get("trades",0)>0:
            # time in stage is used only for comparison, never for promotion by itself.
            started=parse_ts(st.get("stage_started_ts"));days=max(1/24,(datetime.now(timezone.utc)-started).total_seconds()/86400) if started else None
            if days:
                real_freq=live["trades"]/days;diffs["trades_per_day"]={"paper":expected_freq,"real":real_freq,"ratio":real_freq/max(expected_freq,1e-9)}
                if real_freq>expected_freq*2.5 or real_freq<expected_freq*.35:reasons.append("TRADE_FREQUENCY_DIVERGENCE")
        return {"status":"LIVE_EXECUTION_DIVERGENCE" if reasons else "CONSISTENT",
                "material":bool(reasons),"reasons":reasons,"differences":diffs,
                "paper_run_id":paper.get("paper_run_id"),"stage":stage}

    def live_stability_score(self,context:Dict[str,Any])->Dict[str,Any]:
        m=self.live_metrics();score=100.0;components={}
        rel=1.0 if context.get("operational_reliability",False) else 0.0;score-=30*(1-rel);components["operational_reliability"]=rel
        execution=1.0
        if m.get("rejection_rate") is not None:execution-=min(.5,m["rejection_rate"]*2)
        if m.get("avg_slippage_pips") is not None and m["avg_slippage_pips"]>context.get("max_slippage_pips",2.0):execution-=.4
        score-=25*(1-max(0,execution));components["execution_quality"]=max(0,execution)
        risk_cons=1.0 if context.get("risk_consistent",False) else 0.0;score-=20*(1-risk_cons);components["risk_consistency"]=risk_cons
        recon=1.0 if m.get("reconciliation_mismatches",0)==0 and context.get("reconciliation_ok",False) else 0.0;score-=15*(1-recon);components["reconciliation_accuracy"]=recon
        incidents=max(0,1-min(1,context.get("open_p0",0)+.5*context.get("open_p1",0)));score-=10*(1-incidents);components["incident_reliability"]=incidents
        score=max(0,min(100,score));state="STABLE" if score>=80 else "WATCH" if score>=65 else "UNSTABLE"
        return {"score":score,"state":state,"components":components,"not_pnl_only":True}

    def promotion_evidence(self,context:Dict[str,Any]):
        st=self.state();m=self.live_metrics();started=parse_ts(st.get("stage_started_ts"));days=(datetime.now(timezone.utc)-started).total_seconds()/86400 if started else 0
        stability=self.live_stability_score(context);limits=self.effective_stage_limits(st.get("production_stage"),context.get("hard_limits") or {})
        return {"current_stage":st.get("production_stage"),"time_in_stage_days":days,"trades":m["trades"],"pnl":context.get("realized_pnl"),
                "drawdown":context.get("drawdown"),"expectancy":m.get("expectancy_r"),"slippage":m.get("avg_slippage_pips"),
                "execution_quality":{"fill_rate":m.get("fill_rate"),"rejection_rate":m.get("rejection_rate"),"latency_ms":m.get("avg_latency_ms")},
                "incidents":{"p0":context.get("open_p0",0),"p1":context.get("open_p1",0)},"reconciliation_status":context.get("reconciliation_status"),
                "system_evaluation":context.get("system_evaluation"),"governance":context.get("governance"),"risk":context.get("risk"),
                "live_stability":stability,"limits":limits}

    def promotion_gate(self,target_stage:str,context:Dict[str,Any],actor="RISK_MANAGER"):
        st=self.state();current=st.get("production_stage");order=["MINIMAL_LIVE","LIMITED_LIVE","CONTROLLED_LIVE","PRODUCTION_APPROVED"]
        if current not in order or target_stage not in order:return {"action":"HOLD_CURRENT_STAGE","reasons":["INVALID_STAGE"]}
        if order.index(target_stage)!=order.index(current)+1:return {"action":"HOLD_CURRENT_STAGE","reasons":["NON_SEQUENTIAL_PROMOTION_FORBIDDEN"]}
        ev=self.promotion_evidence(context);limits=self.effective_stage_limits(current,context.get("hard_limits") or {});reasons=[]
        if ev["trades"]<int(limits.get("min_trades_for_promotion") or 0):reasons.append("INSUFFICIENT_LIVE_TRADES")
        if ev["time_in_stage_days"]<float(limits.get("min_days_for_promotion") or 0):reasons.append("INSUFFICIENT_LIVE_DAYS")
        if context.get("open_p0",0)>0 or context.get("open_p1",0)>0:reasons.append("OPEN_HIGH_SEVERITY_INCIDENT")
        if not context.get("reconciliation_ok"):reasons.append("RECONCILIATION_NOT_CLEAN")
        if not context.get("risk_ready"):reasons.append("RISK_NOT_READY")
        if context.get("governance_state")!="NORMAL_ADAPTATION":reasons.append("GOVERNANCE_NOT_NORMAL")
        if context.get("system_status") not in ("HEALTHY","EXCELLENT"):reasons.append("SYSTEM_NOT_STABLE")
        if context.get("data_quality",0)<.8:reasons.append("DATA_QUALITY_LOW")
        if context.get("drawdown",0)>=float(limits.get("max_drawdown_fraction") or 1):reasons.append("DRAWDOWN_TOO_HIGH")
        if ev["live_stability"]["score"]<80:reasons.append("LIVE_STABILITY_TOO_LOW")
        comparison=self.paper_vs_real_comparison(stage=current)
        if context.get("live_execution_divergence") or comparison.get("material"):reasons.append("LIVE_EXECUTION_DIVERGENCE")
        ev["paper_vs_real"]=comparison
        if reasons:return {"action":"HOLD_CURRENT_STAGE","reasons":reasons,"evidence":ev}
        ts=now_iso();c=self.conn();readiness="LIMITED_LIVE" if target_stage=="LIMITED_LIVE" else "CONTROLLED_LIVE" if target_stage=="CONTROLLED_LIVE" else "PRODUCTION_APPROVED"
        c.execute("UPDATE production_state SET production_stage=?,readiness_state=?,last_stage_change_ts=?,stage_started_ts=?,updated_ts=? WHERE singleton=1",
                  (target_stage,readiness,ts,ts,ts));c.commit();c.close();self._stage_event(current,target_stage,"PROMOTION",actor,"evidence gate passed",ev,False)
        return {"action":"PROMOTE","new_stage":target_stage,"evidence":ev,"limits":self.effective_stage_limits(target_stage,context.get("hard_limits") or {})}

    def automatic_safety_downgrade(self,context:Dict[str,Any],actor="SYSTEM"):
        st=self.state();current=st.get("production_stage");critical=[]
        if context.get("p0_incident"):critical.append("P0_INCIDENT")
        if not context.get("risk_ready",True):critical.append("RISK_ENGINE_UNAVAILABLE")
        if not context.get("broker_stable",True):critical.append("BROKER_INSTABILITY")
        if not context.get("reconciliation_ok",True):critical.append("RECONCILIATION_CRITICAL")
        if not context.get("data_quality_ok",True):critical.append("DATA_QUALITY_FAILURE")
        if context.get("emergency_stop"):critical.append("EMERGENCY_STOP")
        if context.get("account_mismatch"):critical.append("ACCOUNT_MISMATCH")
        if not critical:return {"action":"HOLD","stage":current}
        if any(x in critical for x in ("P0_INCIDENT","RISK_ENGINE_UNAVAILABLE","RECONCILIATION_CRITICAL","EMERGENCY_STOP","ACCOUNT_MISMATCH")):
            return self.suspend(";".join(critical),actor,automatic=True)
        order=["MINIMAL_LIVE","LIMITED_LIVE","CONTROLLED_LIVE","PRODUCTION_APPROVED"]
        new=order[max(0,order.index(current)-1)] if current in order else "MINIMAL_LIVE"
        ts=now_iso();c=self.conn();c.execute("UPDATE production_state SET production_stage=?,readiness_state=?,last_stage_change_ts=?,stage_started_ts=?,updated_ts=? WHERE singleton=1",
                                            (new,"MINIMAL_LIVE" if new=="MINIMAL_LIVE" else "LIMITED_LIVE",ts,ts,ts));c.commit();c.close();self._stage_event(current,new,"AUTOMATIC_SAFETY_DOWNGRADE",actor,";".join(critical),{"triggers":critical},True)
        return {"action":"DOWNGRADE","new_stage":new,"triggers":critical}

    def suspend(self,reason:str,actor="SYSTEM",automatic=False):
        st=self.state();ts=now_iso();c=self.conn();c.execute("""UPDATE production_state SET readiness_state='SUSPENDED',production_stage='SUSPENDED',
          production_suspended=1,suspension_reason=?,last_stage_change_ts=?,updated_ts=? WHERE singleton=1""",(reason,ts,ts));c.commit();c.close();self._stage_event(st.get("production_stage"),"SUSPENDED","PRODUCTION_SUSPENDED",actor,reason,{},automatic)
        return {"action":"SUSPEND","readiness_state":"SUSPENDED","reason":reason,"new_entries":False,
                "continue":["POSITION_MONITORING","RECONCILIATION","RISK_MANAGEMENT","MONITORING"],"promotions":False}

    def resume_gate(self,context:Dict[str,Any],actor="RISK_MANAGER"):
        st=self.state();reasons=[]
        if st.get("production_stage")!="SUSPENDED":reasons.append("NOT_SUSPENDED")
        for k,label in (("incident_resolved","INCIDENT_NOT_RESOLVED"),("reconciliation_ok","RECONCILIATION_NOT_OK"),
                        ("health_ok","HEALTH_CHECK_FAILED"),("risk_ready","RISK_NOT_READY"),("broker_ready","BROKER_NOT_READY"),
                        ("data_ready","DATA_NOT_READY"),("governance_ok","GOVERNANCE_NOT_READY")):
            if not context.get(k):reasons.append(label)
        if reasons:return {"action":"HOLD_SUSPENDED","reasons":reasons}
        ts=now_iso();c=self.conn();c.execute("""UPDATE production_state SET readiness_state='MINIMAL_LIVE',production_stage='MINIMAL_LIVE',
          production_suspended=0,suspension_reason=NULL,last_stage_change_ts=?,stage_started_ts=?,updated_ts=? WHERE singleton=1""",(ts,ts,ts));c.commit();c.close();self._stage_event("SUSPENDED","MINIMAL_LIVE","LIMITED_RESTART",actor,"incident resolved + reconciliation + health checks",context,False)
        return {"action":"LIMITED_RESTART","stage":"MINIMAL_LIVE","promotion_requires_new_observation":True}

    def open_incident(self,severity:str,incident_type:str,summary:str,release_id:Optional[str]=None):
        if severity not in INCIDENT_SEVERITIES:severity="P2"
        iid="pinc_"+uuid.uuid4().hex;st=self.state();c=self.conn();c.execute("""INSERT INTO production_incidents(
          incident_id,opened_ts,severity,incident_type,status,summary,timeline_json,impact_json,controls_worked_json,
          controls_failed_json,corrective_actions_json,release_id,certification_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (iid,now_iso(),severity,incident_type,"OPEN",summary,"[]","{}","[]","[]","[]",release_id or st.get("release_id"),st.get("certification_id")))
        c.commit();c.close()
        if severity=="P0":self.suspend(f"P0:{incident_type}:{summary}","SYSTEM",automatic=True)
        return iid

    def resolve_incident(self,incident_id:str,root_cause:str,corrective_actions:List[str],controls_worked=None,controls_failed=None):
        c=self.conn();c.execute("""UPDATE production_incidents SET status='RESOLVED',resolved_ts=?,root_cause=?,corrective_actions_json=?,
          controls_worked_json=?,controls_failed_json=? WHERE incident_id=?""",
          (now_iso(),root_cause,j(corrective_actions),j(controls_worked or []),j(controls_failed or []),incident_id));c.commit();c.close()

    def continuous_certification(self,context:Dict[str,Any]):
        st=self.state();triggers=[]
        if context.get("major_code_change"):triggers.append("MAJOR_CODE_CHANGE")
        if context.get("major_strategy_change"):triggers.append("MAJOR_STRATEGY_CHANGE")
        if context.get("risk_framework_change"):triggers.append("RISK_FRAMEWORK_CHANGE")
        if context.get("broker_integration_change"):triggers.append("BROKER_INTEGRATION_CHANGE")
        if context.get("critical_incident"):triggers.append("CRITICAL_INCIDENT")
        if context.get("long_inactivity"):triggers.append("LONG_INACTIVITY")
        cert=self._latest("production_certifications","WHERE certification_id=?",(st.get("certification_id"),)) if st.get("certification_id") else None
        if cert and parse_ts(cert.get("expires_ts")) and datetime.now(timezone.utc)>=parse_ts(cert.get("expires_ts")):
            triggers.append("CERTIFICATION_EXPIRED")
        if triggers:
            self.invalidate_certification(";".join(triggers),"CONTINUOUS_CERTIFICATION")
            return {"status":"CERTIFICATION_INVALIDATED","triggers":triggers}
        downgrade=self.automatic_safety_downgrade(context)
        return {"status":"VALID" if downgrade.get("action")=="HOLD" else "DEGRADED","safety_action":downgrade}

    def dashboard(self,context:Optional[Dict[str,Any]]=None):
        st=self.state();context=context or {};cert=None
        if st.get("certification_id"):cert=self._latest("production_certifications","WHERE certification_id=?",(st["certification_id"],))
        c=self.conn();inc=[dict(x) for x in c.execute("SELECT * FROM production_incidents WHERE status!='RESOLVED' ORDER BY opened_ts DESC").fetchall()];c.close()
        stage=st.get("production_stage");limits=self.effective_stage_limits(stage,context.get("hard_limits") or {}) if stage in self.stage_limits else {}
        return {"current_production_stage":stage,"readiness_state":st.get("readiness_state"),"certified_release":st.get("release_id"),
                "certification_id":st.get("certification_id"),"capital_exposure_allowed":limits,"actual_exposure":st.get("actual_exposure_fraction"),
                "live_stability_score":self.live_stability_score(context) if stage in self.stage_limits else None,
                "current_drawdown":context.get("drawdown"),"broker_status":context.get("broker_status"),
                "reconciliation_status":context.get("reconciliation_status"),"governance_status":context.get("governance_status"),
                "risk_status":context.get("risk_status"),"open_incidents":inc,"production_suspended":bool(st.get("production_suspended")),
                "suspension_reason":st.get("suspension_reason"),"next_promotion_requirements":self.promotion_evidence(context) if stage in self.stage_limits else None,
                "certification":cert}

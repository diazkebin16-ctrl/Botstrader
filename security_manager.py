
from __future__ import annotations

from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
import hashlib
import hmac
import json
import logging
import os
import re
import sqlite3
import uuid

RISK_LEVELS=("LOW_RISK","MEDIUM_RISK","HIGH_RISK","CRITICAL")
CHANGE_STATES=("DRAFT","PENDING_REVIEW","APPROVED","REJECTED","SCHEDULED","APPLYING","APPLIED","FAILED","ROLLED_BACK")
ROLES=("VIEWER","OPERATOR","STRATEGY_MANAGER","RISK_MANAGER","ADMIN","SYSTEM_RECOMMENDER")

ROLE_PERMISSIONS={
    "VIEWER":{"read"},
    "OPERATOR":{"read","manual_pause","manual_reconcile","activate_kill_switch"},
    "STRATEGY_MANAGER":{"read","request_strategy_change","review_strategy_change","apply_strategy_change",
                        "run_research","candidate_review","manual_pause"},
    "RISK_MANAGER":{"read","request_risk_change","review_risk_change","apply_risk_change",
                    "activate_kill_switch","reset_emergency_stop","manual_pause","manual_reconcile"},
    "ADMIN":{"*"},
    "SYSTEM_RECOMMENDER":{"read","recommend_change","request_non_authoritative_change"},
}

SECRET_KEY_RE=re.compile(r"(authorization|api[_-]?key|token|password|passwd|secret|credential|private[_-]?key|access[_-]?key)",re.I)
BEARER_RE=re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{8,}")
LONG_SECRET_RE=re.compile(r"\b[A-Za-z0-9_\-]{32,}\b")
SECRET_ASSIGN_RE=re.compile(r"(?i)\b(token|password|passwd|api[_-]?key|secret|credential|authorization)\b\s*[:=]\s*([^\s,;]+)")

def now_iso(): return datetime.now(timezone.utc).isoformat()

def canonical(v:Any)->str:
    return json.dumps(v,separators=(",",":"),sort_keys=True,default=str)

def sha256_text(s:str)->str:
    return hashlib.sha256(s.encode()).hexdigest()

def sanitize(value:Any)->Any:
    if isinstance(value,dict):
        out={}
        for k,v in value.items():
            if SECRET_KEY_RE.search(str(k)):
                out[k]="[REDACTED]"
            else:
                out[k]=sanitize(v)
        return out
    if isinstance(value,list):
        return [sanitize(x) for x in value]
    if isinstance(value,tuple):
        return [sanitize(x) for x in value]
    if isinstance(value,str):
        x=BEARER_RE.sub("Bearer [REDACTED]",value)
        x=SECRET_ASSIGN_RE.sub(lambda m: f"{m.group(1)}=[REDACTED]",x)
        return x
    return value

class RedactingFilter(logging.Filter):
    def filter(self,record):
        try:
            msg=record.getMessage()
            msg=BEARER_RE.sub("Bearer [REDACTED]",msg)
            msg=SECRET_ASSIGN_RE.sub(lambda m: f"{m.group(1)}=[REDACTED]",msg)
            record.msg=msg
            record.args=()
        except Exception:
            pass
        return True

def permission_for_component(component:str,risk_level:str,action:str)->str:
    c=str(component or "").lower()
    if action=="request":
        if c.startswith("risk.") or c.startswith("execution.") or c.startswith("security.") or c.startswith("deployment.") or c.startswith("broker."):
            return "request_risk_change"
        return "request_strategy_change"
    if action=="review":
        if c.startswith("risk.") or c.startswith("execution.") or c.startswith("security.") or c.startswith("deployment.") or c.startswith("broker."):
            return "review_risk_change"
        return "review_strategy_change"
    if action=="apply":
        if c.startswith("risk.") or c.startswith("execution.") or c.startswith("security.") or c.startswith("deployment.") or c.startswith("broker."):
            return "apply_risk_change"
        return "apply_strategy_change"
    return "read"

class SecurityManager:
    def __init__(self,db_path:str,version:str,environment:str,
                 actors_json:str="",allow_unauthenticated_reads:bool=True):
        self.db_path=db_path
        self.version=version
        self.environment=str(environment or "PAPER").upper()
        self.allow_unauthenticated_reads=allow_unauthenticated_reads
        self.actors=self._parse_actors(actors_json)
        self.schema:Dict[str,Dict[str,Any]]={}
        self.initial_config:Dict[str,Any]={}
        self.code_root:Optional[str]=None
        self.dependency_file:Optional[str]=None
        self.last_integrity:Dict[str,Any]={"verified":False,"reason":"not_checked"}

    def _parse_actors(self,raw:str)->Dict[str,Dict[str,str]]:
        try:
            data=json.loads(raw or "{}")
        except Exception:
            data={}
        out={}
        for actor,v in (data or {}).items():
            if not isinstance(v,dict): continue
            role=str(v.get("role") or "").upper()
            th=str(v.get("token_sha256") or "").lower()
            if role in ROLES and re.fullmatch(r"[0-9a-f]{64}",th):
                out[str(actor)]={"actor":str(actor),"role":role,"token_sha256":th}
        return out

    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30,isolation_level=None)
        c.row_factory=sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=FULL")
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("PRAGMA busy_timeout=5000")
        return c

    def ensure_schema(self):
        c=self.conn()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS security_change_requests(
          change_id TEXT PRIMARY KEY,
          component TEXT NOT NULL,
          config_key TEXT NOT NULL,
          current_value_json TEXT NOT NULL,
          proposed_value_json TEXT NOT NULL,
          reason TEXT NOT NULL,
          requested_by TEXT NOT NULL,
          requester_role TEXT NOT NULL,
          requester_type TEXT NOT NULL,
          requested_ts TEXT NOT NULL,
          risk_level TEXT NOT NULL,
          expected_impact TEXT,
          rollback_plan TEXT NOT NULL,
          status TEXT NOT NULL,
          validation_json TEXT NOT NULL DEFAULT '{}',
          required_approvals INTEGER NOT NULL DEFAULT 1,
          base_config_version INTEGER,
          scheduled_ts TEXT,
          applied_ts TEXT,
          failure_reason TEXT,
          updated_ts TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS security_change_approvals(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          change_id TEXT NOT NULL,
          reviewer TEXT NOT NULL,
          reviewer_role TEXT NOT NULL,
          decision TEXT NOT NULL,
          reason TEXT,
          ts TEXT NOT NULL,
          UNIQUE(change_id,reviewer)
        );
        CREATE TABLE IF NOT EXISTS security_config_versions(
          config_version INTEGER PRIMARY KEY AUTOINCREMENT,
          created_ts TEXT NOT NULL,
          created_by TEXT NOT NULL,
          reason TEXT NOT NULL,
          parent_version INTEGER,
          change_id TEXT,
          config_json TEXT NOT NULL,
          config_hash TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS security_config_state(
          singleton INTEGER PRIMARY KEY CHECK(singleton=1),
          current_version INTEGER NOT NULL,
          updated_ts TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS security_audit_log(
          seq INTEGER PRIMARY KEY AUTOINCREMENT,
          audit_id TEXT NOT NULL UNIQUE,
          timestamp TEXT NOT NULL,
          actor TEXT NOT NULL,
          actor_role TEXT NOT NULL,
          action TEXT NOT NULL,
          resource TEXT NOT NULL,
          old_value_json TEXT NOT NULL,
          new_value_json TEXT NOT NULL,
          reason TEXT NOT NULL,
          result TEXT NOT NULL,
          correlation_id TEXT,
          prev_hash TEXT NOT NULL,
          record_hash TEXT NOT NULL UNIQUE
        );
        CREATE TRIGGER IF NOT EXISTS security_audit_no_update
        BEFORE UPDATE ON security_audit_log
        BEGIN SELECT RAISE(ABORT,'security_audit_log is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS security_audit_no_delete
        BEFORE DELETE ON security_audit_log
        BEGIN SELECT RAISE(ABORT,'security_audit_log is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS security_config_versions_no_update
        BEFORE UPDATE ON security_config_versions
        BEGIN SELECT RAISE(ABORT,'security_config_versions are immutable snapshots'); END;
        CREATE TRIGGER IF NOT EXISTS security_config_versions_no_delete
        BEFORE DELETE ON security_config_versions
        BEGIN SELECT RAISE(ABORT,'security_config_versions are immutable snapshots'); END;
        CREATE TABLE IF NOT EXISTS security_runtime_manifests(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT NOT NULL,
          environment TEXT NOT NULL,
          created_ts TEXT NOT NULL,
          code_hash TEXT NOT NULL,
          dependency_hash TEXT,
          role_config_hash TEXT,
          file_hashes_json TEXT NOT NULL,
          verified INTEGER NOT NULL,
          details_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS security_startup_checks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          status TEXT NOT NULL,
          checks_json TEXT NOT NULL,
          details_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_security_changes_status ON security_change_requests(status,risk_level,requested_ts);
        CREATE INDEX IF NOT EXISTS idx_security_audit_ts ON security_audit_log(timestamp,action);
        """)
        c.close()

    def protect_existing_history_tables(self):
        c=self.conn()
        tables={x["name"] for x in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "deployment_events" in tables:
            c.executescript("""
            CREATE TRIGGER IF NOT EXISTS deployment_events_no_update
            BEFORE UPDATE ON deployment_events
            BEGIN SELECT RAISE(ABORT,'deployment history is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS deployment_events_no_delete
            BEFORE DELETE ON deployment_events
            BEGIN SELECT RAISE(ABORT,'deployment history is immutable'); END;
            """)
        c.commit();c.close()

    def configure(self,schema:Dict[str,Dict[str,Any]],initial_config:Dict[str,Any],
                  code_root:Optional[str]=None,dependency_file:Optional[str]=None):
        self.schema=dict(schema)
        self.initial_config=dict(initial_config)
        self.code_root=code_root
        self.dependency_file=dependency_file
        self.ensure_schema()
        self._bootstrap_config()
        self._ensure_missing_defaults()

    def _bootstrap_config(self):
        c=self.conn()
        st=c.execute("SELECT current_version FROM security_config_state WHERE singleton=1").fetchone()
        if not st:
            payload=canonical(self.initial_config)
            h=sha256_text(payload)
            c.execute("""INSERT INTO security_config_versions(
              created_ts,created_by,reason,parent_version,change_id,config_json,config_hash)
              VALUES(?,?,?,NULL,NULL,?,?)""",(now_iso(),"SYSTEM_BOOTSTRAP","initial configuration",payload,h))
            ver=c.execute("SELECT last_insert_rowid() id").fetchone()["id"]
            c.execute("INSERT INTO security_config_state(singleton,current_version,updated_ts) VALUES(1,?,?)",(ver,now_iso()))
        c.commit();c.close()

    def _ensure_missing_defaults(self):
        """
        Versioned schema evolution for newly introduced managed settings.
        Existing values are never overwritten; only missing keys receive the
        safe defaults shipped with the new software version.
        """
        c=self.conn()
        st=c.execute("SELECT current_version FROM security_config_state WHERE singleton=1").fetchone()
        if not st:
            c.close();return
        row=c.execute("SELECT * FROM security_config_versions WHERE config_version=?",(st["current_version"],)).fetchone()
        if not row:
            c.close();return
        cfg=json.loads(row["config_json"])
        missing={k:v for k,v in self.initial_config.items() if k not in cfg}
        if not missing:
            c.close();return
        old_version=int(st["current_version"])
        merged={**cfg,**missing};payload=canonical(merged);h=sha256_text(payload)
        c.execute("BEGIN IMMEDIATE")
        c.execute("""INSERT INTO security_config_versions(
          created_ts,created_by,reason,parent_version,change_id,config_json,config_hash)
          VALUES(?,?,?,?,NULL,?,?)""",
          (now_iso(),"SYSTEM_UPGRADE","add safe defaults for newly introduced managed configuration keys",
           old_version,payload,h))
        new_version=c.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        c.execute("UPDATE security_config_state SET current_version=?,updated_ts=? WHERE singleton=1",
                  (new_version,now_iso()))
        c.commit();c.close()
        self.audit({"actor":"SYSTEM_UPGRADE","role":"SYSTEM_RECOMMENDER"},
                   "CONFIG_SCHEMA_DEFAULTS_ADDED",f"config:v{old_version}->v{new_version}",
                   {"config_version":old_version},{"config_version":new_version,"added_keys":sorted(missing)},
                   "software upgrade added missing managed keys using safe defaults","APPLIED")

    # ---------- auth / RBAC ----------
    def authenticate(self,authorization:Optional[str],allow_anonymous_read:bool=False)->Dict[str,str]:
        if allow_anonymous_read and self.allow_unauthenticated_reads and not authorization:
            return {"actor":"anonymous","role":"VIEWER","actor_type":"HUMAN"}
        if not authorization or not str(authorization).lower().startswith("bearer "):
            raise PermissionError("AUTHENTICATION_REQUIRED")
        token=str(authorization).split(None,1)[1].strip()
        th=sha256_text(token)
        for actor,meta in self.actors.items():
            if hmac.compare_digest(th,meta["token_sha256"]):
                return {"actor":actor,"role":meta["role"],"actor_type":"HUMAN"}
        raise PermissionError("INVALID_CREDENTIALS")

    def internal_actor(self,name:str,role:str="SYSTEM_RECOMMENDER")->Dict[str,str]:
        return {"actor":str(name),"role":str(role).upper(),"actor_type":"AUTOMATION"}

    def has_permission(self,actor:Dict[str,str],permission:str)->bool:
        role=str(actor.get("role") or "").upper()
        perms=ROLE_PERMISSIONS.get(role,set())
        return "*" in perms or permission in perms

    def require(self,actor:Dict[str,str],permission:str):
        if not self.has_permission(actor,permission):
            raise PermissionError(f"PERMISSION_DENIED:{permission}")

    # ---------- audit ----------
    def audit(self,actor:Dict[str,str],action:str,resource:str,old_value:Any,new_value:Any,
              reason:str,result:str,correlation_id:Optional[str]=None)->Dict[str,Any]:
        actor=actor or {"actor":"SYSTEM","role":"SYSTEM_RECOMMENDER"}
        old_s=sanitize(old_value);new_s=sanitize(new_value)
        c=self.conn()
        prev=c.execute("SELECT record_hash FROM security_audit_log ORDER BY seq DESC LIMIT 1").fetchone()
        prev_hash=prev["record_hash"] if prev else "GENESIS"
        row={
            "audit_id":"aud_"+uuid.uuid4().hex,
            "timestamp":now_iso(),
            "actor":str(actor.get("actor") or "unknown"),
            "actor_role":str(actor.get("role") or "unknown"),
            "action":str(action),
            "resource":str(resource),
            "old_value":old_s,
            "new_value":new_s,
            "reason":str(reason),
            "result":str(result),
            "correlation_id":correlation_id,
            "prev_hash":prev_hash,
        }
        record_hash=sha256_text(canonical(row))
        c.execute("""INSERT INTO security_audit_log(
          audit_id,timestamp,actor,actor_role,action,resource,old_value_json,new_value_json,
          reason,result,correlation_id,prev_hash,record_hash)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (row["audit_id"],row["timestamp"],row["actor"],row["actor_role"],row["action"],
           row["resource"],canonical(old_s),canonical(new_s),row["reason"],row["result"],
           correlation_id,prev_hash,record_hash))
        c.commit();c.close()
        return {**row,"record_hash":record_hash}

    def verify_audit_chain(self)->Dict[str,Any]:
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM security_audit_log ORDER BY seq").fetchall()];c.close()
        prev="GENESIS"
        for r in rows:
            payload={
                "audit_id":r["audit_id"],"timestamp":r["timestamp"],"actor":r["actor"],
                "actor_role":r["actor_role"],"action":r["action"],"resource":r["resource"],
                "old_value":json.loads(r["old_value_json"] or "null"),
                "new_value":json.loads(r["new_value_json"] or "null"),
                "reason":r["reason"],"result":r["result"],"correlation_id":r["correlation_id"],
                "prev_hash":r["prev_hash"]
            }
            if r["prev_hash"]!=prev:
                return {"verified":False,"reason":"PREV_HASH_MISMATCH","seq":r["seq"]}
            calc=sha256_text(canonical(payload))
            if calc!=r["record_hash"]:
                return {"verified":False,"reason":"RECORD_HASH_MISMATCH","seq":r["seq"]}
            prev=r["record_hash"]
        return {"verified":True,"records":len(rows),"head_hash":prev}

    # ---------- config / versioning ----------
    def current_config(self)->Dict[str,Any]:
        c=self.conn()
        st=c.execute("SELECT current_version FROM security_config_state WHERE singleton=1").fetchone()
        if not st:
            c.close();return dict(self.initial_config)
        row=c.execute("SELECT * FROM security_config_versions WHERE config_version=?",(st["current_version"],)).fetchone()
        c.close()
        return json.loads(row["config_json"]) if row else dict(self.initial_config)

    def current_version(self)->int:
        c=self.conn();r=c.execute("SELECT current_version FROM security_config_state WHERE singleton=1").fetchone();c.close()
        return int(r["current_version"]) if r else 0

    def current_hash(self)->str:
        c=self.conn()
        r=c.execute("""SELECT v.config_hash FROM security_config_state s
                       JOIN security_config_versions v ON v.config_version=s.current_version
                       WHERE s.singleton=1""").fetchone()
        c.close();return str(r["config_hash"]) if r else ""

    def get(self,key:str,default=None):
        return self.current_config().get(key,default)

    def _spec_for_key(self,key:str)->Optional[Dict[str,Any]]:
        if key in self.schema:
            return self.schema[key]
        for pattern,spec in self.schema.items():
            if pattern.endswith("*") and key.startswith(pattern[:-1]):
                return spec
        return None

    def _validate_value(self,key:str,value:Any)->Dict[str,Any]:
        spec=self._spec_for_key(key)
        if not spec:
            return {"valid":False,"reason":"UNKNOWN_CONFIG_KEY"}
        if spec.get("secret") or SECRET_KEY_RE.search(key):
            return {"valid":False,"reason":"SECRET_CONFIGURATION_NOT_MANAGED_IN_APP"}
        typ=spec.get("type")
        try:
            if typ=="float":
                v=float(value)
            elif typ=="int":
                if isinstance(value,bool): raise ValueError()
                v=int(value)
            elif typ=="bool":
                if not isinstance(value,bool): raise ValueError()
                v=value
            elif typ=="str":
                v=str(value)
            else:
                v=value
        except Exception:
            return {"valid":False,"reason":f"INVALID_TYPE:{typ}"}
        if "min" in spec and v<spec["min"]:
            return {"valid":False,"reason":"BELOW_MIN","min":spec["min"]}
        if "max" in spec and v>spec["max"]:
            return {"valid":False,"reason":"ABOVE_MAX","max":spec["max"]}
        if "allowed" in spec and v not in spec["allowed"]:
            return {"valid":False,"reason":"NOT_ALLOWED","allowed":spec["allowed"]}
        # hard_ceiling means managed config can never increase beyond bootstrap/current code ceiling.
        if "hard_ceiling" in spec and isinstance(v,(int,float)) and v>spec["hard_ceiling"]:
            return {"valid":False,"reason":"HARD_CEILING_EXCEEDED","hard_ceiling":spec["hard_ceiling"]}
        return {"valid":True,"normalized":v,"risk_level":spec.get("risk_level","MEDIUM_RISK")}

    def validate_change(self,component:str,key:str,proposed:Any)->Dict[str,Any]:
        val=self._validate_value(key,proposed)
        if not val.get("valid"): return val
        cfg=self.current_config()
        current=cfg.get(key)
        # Cross-config constraints: use most restrictive relationships.
        candidate=dict(cfg);candidate[key]=val["normalized"]
        if "risk.base_fraction" in candidate and "risk.max_trade_fraction" in candidate:
            if candidate["risk.base_fraction"]>candidate["risk.max_trade_fraction"]:
                return {"valid":False,"reason":"BASE_RISK_EXCEEDS_TRADE_CAP"}
        if candidate.get("risk.max_trade_fraction",0)>candidate.get("risk.max_strategy_fraction",1):
            return {"valid":False,"reason":"TRADE_RISK_EXCEEDS_STRATEGY_CAP"}
        if candidate.get("risk.max_strategy_fraction",0)>candidate.get("risk.max_portfolio_fraction",1):
            return {"valid":False,"reason":"STRATEGY_RISK_EXCEEDS_PORTFOLIO_CAP"}
        if candidate.get("risk.drawdown_warning",0)>candidate.get("risk.drawdown_stop",1):
            return {"valid":False,"reason":"DRAWDOWN_WARNING_EXCEEDS_STOP"}
        if candidate.get("governance.meta_risk_high",0)>candidate.get("governance.meta_risk_critical",100):
            return {"valid":False,"reason":"GOVERNANCE_META_HIGH_EXCEEDS_CRITICAL"}
        if candidate.get("production.minimal_risk_multiplier",0)>candidate.get("production.limited_risk_multiplier",1):
            return {"valid":False,"reason":"PRODUCTION_MINIMAL_EXCEEDS_LIMITED"}
        if candidate.get("production.limited_risk_multiplier",0)>candidate.get("production.controlled_risk_multiplier",1):
            return {"valid":False,"reason":"PRODUCTION_LIMITED_EXCEEDS_CONTROLLED"}
        return {**val,"current":current}

    def create_change_request(self,actor:Dict[str,str],component:str,key:str,proposed:Any,
                              reason:str,expected_impact:str,rollback_plan:str,
                              correlation_id:Optional[str]=None)->Dict[str,Any]:
        spec=self._spec_for_key(key) or {}
        perm=permission_for_component(key,spec.get("risk_level","MEDIUM_RISK"),"request")
        if actor.get("actor_type")=="AUTOMATION":
            self.require(actor,"request_non_authoritative_change")
        else:
            self.require(actor,perm)
        v=self.validate_change(component,key,proposed)
        cfg=self.current_config();current=cfg.get(key)
        spec=self._spec_for_key(key) or {}
        secret_field=bool(spec.get("secret") or SECRET_KEY_RE.search(key))
        stored_current="[REDACTED]" if secret_field else current
        stored_proposed="[REDACTED]" if secret_field else sanitize(proposed)
        risk_level=spec.get("risk_level","MEDIUM_RISK")
        cid="chg_"+uuid.uuid4().hex
        required=2 if risk_level=="CRITICAL" else 1
        status="PENDING_REVIEW" if v.get("valid") else "REJECTED"
        c=self.conn()
        c.execute("""INSERT INTO security_change_requests(
          change_id,component,config_key,current_value_json,proposed_value_json,reason,
          requested_by,requester_role,requester_type,requested_ts,risk_level,expected_impact,
          rollback_plan,status,validation_json,required_approvals,base_config_version,updated_ts)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (cid,component,key,canonical(stored_current),canonical(stored_proposed),reason,
           actor["actor"],actor["role"],actor.get("actor_type","HUMAN"),now_iso(),risk_level,
           expected_impact,rollback_plan,status,canonical(v),required,self.current_version(),now_iso()))
        c.commit();c.close()
        self.audit(actor,"CHANGE_REQUEST_CREATED",f"{component}:{key}",stored_current,stored_proposed,reason,status,correlation_id)
        if not v.get("valid"):
            self.audit(actor,"CHANGE_VALIDATION_FAILED",f"{component}:{key}",stored_current,stored_proposed,
                       v.get("reason","validation failed"),"REJECTED",correlation_id)
        return self.change_request(cid)

    def change_request(self,change_id:str)->Dict[str,Any]:
        c=self.conn();r=c.execute("SELECT * FROM security_change_requests WHERE change_id=?",(change_id,)).fetchone()
        approvals=[dict(x) for x in c.execute("SELECT * FROM security_change_approvals WHERE change_id=? ORDER BY id",(change_id,)).fetchall()]
        c.close()
        return {"change":dict(r) if r else None,"approvals":approvals}

    def review_change(self,actor:Dict[str,str],change_id:str,decision:str,reason:str="")->Dict[str,Any]:
        req=self.change_request(change_id)["change"]
        if not req: raise KeyError(change_id)
        perm=permission_for_component(req["config_key"],req["risk_level"],"review")
        self.require(actor,perm)
        dec=str(decision).upper()
        if dec not in ("APPROVE","REJECT"): raise ValueError("decision must be APPROVE or REJECT")
        if req["status"] not in ("PENDING_REVIEW","APPROVED"):
            raise ValueError(f"CHANGE_NOT_REVIEWABLE:{req['status']}")
        if req["risk_level"]=="CRITICAL" and actor["actor"]==req["requested_by"]:
            self.audit(actor,"CHANGE_APPROVAL_DENIED",f"{req['component']}:{req['config_key']}",
                       None,None,"critical requester cannot self-approve","DENIED")
            raise PermissionError("CRITICAL_CHANGE_SELF_APPROVAL_FORBIDDEN")
        if req["requester_type"]=="AUTOMATION" and actor.get("actor_type")!="HUMAN":
            raise PermissionError("AUTOMATION_CANNOT_AUTHORIZE_AUTOMATION_CHANGE")
        c=self.conn()
        try:
            c.execute("""INSERT INTO security_change_approvals(change_id,reviewer,reviewer_role,decision,reason,ts)
                         VALUES(?,?,?,?,?,?)""",(change_id,actor["actor"],actor["role"],dec,reason,now_iso()))
        except sqlite3.IntegrityError:
            c.close();raise ValueError("REVIEWER_ALREADY_VOTED")
        if dec=="REJECT":
            new_status="REJECTED"
        else:
            rows=[dict(x) for x in c.execute("""SELECT * FROM security_change_approvals
                                               WHERE change_id=? AND decision='APPROVE'""",(change_id,)).fetchall()]
            distinct=len({x["reviewer"] for x in rows})
            required=int(req["required_approvals"])
            if req["risk_level"]=="CRITICAL":
                privileged=any(x["reviewer_role"] in ("RISK_MANAGER","ADMIN") for x in rows)
            else:
                privileged=True
            new_status="APPROVED" if distinct>=required and privileged else "PENDING_REVIEW"
        c.execute("UPDATE security_change_requests SET status=?,updated_ts=? WHERE change_id=?",
                  (new_status,now_iso(),change_id))
        c.commit();c.close()
        self.audit(actor,"CHANGE_REVIEWED",f"{req['component']}:{req['config_key']}",
                   req["current_value_json"],req["proposed_value_json"],reason or dec,new_status)
        return self.change_request(change_id)

    def apply_change(self,actor:Dict[str,str],change_id:str,correlation_id:Optional[str]=None)->Dict[str,Any]:
        bundle=self.change_request(change_id);req=bundle["change"]
        if not req: raise KeyError(change_id)
        self.require(actor,permission_for_component(req["config_key"],req["risk_level"],"apply"))
        if req["status"]!="APPROVED":
            raise PermissionError(f"CHANGE_NOT_APPROVED:{req['status']}")
        if req["requester_type"]=="AUTOMATION" and actor.get("actor_type")!="HUMAN":
            raise PermissionError("AUTOMATION_CANNOT_APPLY_CHANGE")
        # Optimistic consistency: requested current config version must still be current.
        if int(req["base_config_version"] or 0)!=self.current_version():
            c=self.conn();c.execute("UPDATE security_change_requests SET status='FAILED',failure_reason=?,updated_ts=? WHERE change_id=?",
                                    ("CONFIG_VERSION_CHANGED_SINCE_REQUEST",now_iso(),change_id));c.commit();c.close()
            self.audit(actor,"CHANGE_APPLY_FAILED",f"{req['component']}:{req['config_key']}",
                       req["current_value_json"],req["proposed_value_json"],
                       "base config version changed","FAILED",correlation_id)
            return self.change_request(change_id)
        proposed=json.loads(req["proposed_value_json"])
        v=self.validate_change(req["component"],req["config_key"],proposed)
        if not v.get("valid"):
            c=self.conn();c.execute("UPDATE security_change_requests SET status='FAILED',failure_reason=?,updated_ts=? WHERE change_id=?",
                                    (v.get("reason"),now_iso(),change_id));c.commit();c.close()
            self.audit(actor,"CHANGE_APPLY_FAILED",f"{req['component']}:{req['config_key']}",
                       req["current_value_json"],proposed,v.get("reason","validation failed"),"FAILED",correlation_id)
            return self.change_request(change_id)
        cfg=self.current_config();old=cfg.get(req["config_key"]);cfg[req["config_key"]]=v["normalized"]
        parent=self.current_version();payload=canonical(cfg);h=sha256_text(payload)
        c=self.conn()
        c.execute("BEGIN IMMEDIATE")
        c.execute("UPDATE security_change_requests SET status='APPLYING',updated_ts=? WHERE change_id=?",(now_iso(),change_id))
        c.execute("""INSERT INTO security_config_versions(
          created_ts,created_by,reason,parent_version,change_id,config_json,config_hash)
          VALUES(?,?,?,?,?,?,?)""",(now_iso(),actor["actor"],req["reason"],parent,change_id,payload,h))
        ver=c.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        c.execute("UPDATE security_config_state SET current_version=?,updated_ts=? WHERE singleton=1",(ver,now_iso()))
        c.execute("UPDATE security_change_requests SET status='APPLIED',applied_ts=?,updated_ts=? WHERE change_id=?",
                  (now_iso(),now_iso(),change_id))
        c.commit();c.close()
        self.audit(actor,"CONFIG_CHANGED",f"{req['component']}:{req['config_key']}",old,v["normalized"],
                   req["reason"],"APPLIED",correlation_id)
        return {**self.change_request(change_id),"config_version":ver,"config_hash":h}

    def rollback_config(self,actor:Dict[str,str],target_version:int,reason:str,
                        correlation_id:Optional[str]=None)->Dict[str,Any]:
        # A global configuration snapshot may contain hard-risk values even when
        # the change being rolled back originated in a strategy component.
        # Therefore rollback requires risk-level authority.
        self.require(actor,"apply_risk_change")
        c=self.conn()
        target=c.execute("SELECT * FROM security_config_versions WHERE config_version=?",(int(target_version),)).fetchone()
        if not target:
            c.close();raise KeyError(target_version)
        oldv=self.current_version();oldcfg=self.current_config();targetcfg=json.loads(target["config_json"])
        # Revalidate every managed key under current schema/hard ceilings.
        for k,v in targetcfg.items():
            if k in self.schema:
                chk=self._validate_value(k,v)
                if not chk.get("valid"):
                    c.close();raise ValueError(f"ROLLBACK_TARGET_INVALID:{k}:{chk.get('reason')}")
        payload=canonical(targetcfg);h=sha256_text(payload)
        c.execute("BEGIN IMMEDIATE")
        c.execute("""INSERT INTO security_config_versions(
          created_ts,created_by,reason,parent_version,change_id,config_json,config_hash)
          VALUES(?,?,?,?,NULL,?,?)""",(now_iso(),actor["actor"],f"ROLLBACK to v{target_version}: {reason}",oldv,payload,h))
        newv=c.execute("SELECT last_insert_rowid() id").fetchone()["id"]
        c.execute("UPDATE security_config_state SET current_version=?,updated_ts=? WHERE singleton=1",(newv,now_iso()))
        c.commit();c.close()
        self.audit(actor,"CONFIG_ROLLBACK",f"config:v{oldv}->v{newv}",oldcfg,targetcfg,reason,"ROLLED_BACK",correlation_id)
        return {"rolled_back_from":oldv,"target_snapshot_version":target_version,"new_config_version":newv,
                "config":targetcfg}

    def versions(self,limit=100)->List[Dict[str,Any]]:
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM security_config_versions ORDER BY config_version DESC LIMIT ?",
                                                       (min(max(int(limit),1),1000),)).fetchall()];c.close()
        return rows

    def verify_config_integrity(self)->Dict[str,Any]:
        c=self.conn()
        row=c.execute("""SELECT v.config_version,v.config_json,v.config_hash FROM security_config_state s
                         JOIN security_config_versions v ON v.config_version=s.current_version
                         WHERE s.singleton=1""").fetchone()
        c.close()
        if not row:
            return {"verified":False,"reason":"CONFIG_STATE_MISSING"}
        calc=sha256_text(row["config_json"])
        return {"verified":calc==row["config_hash"],
                "reason":"MATCHED" if calc==row["config_hash"] else "CONFIGURATION_CORRUPTION",
                "config_version":row["config_version"],"stored_hash":row["config_hash"],"calculated_hash":calc}

    # ---------- runtime integrity / environment ----------
    def _file_hashes(self)->Dict[str,str]:
        out={}
        if not self.code_root:return out
        for name in ("server.py","security_manager.py","recovery_manager.py","order_state.py",
                     "observability.py","deployment_runtime.py","deployment_manager.py",
                     "adaptive_learning.py","validation_pipeline.py","system_evaluation.py","governance_engine.py","production_readiness.py","smart_execution.py"):
            p=os.path.join(self.code_root,name)
            if os.path.exists(p):
                with open(p,"rb") as fh: out[name]=hashlib.sha256(fh.read()).hexdigest()
        return out

    def dependency_hash(self)->Optional[str]:
        p=self.dependency_file
        if not p or not os.path.exists(p):return None
        with open(p,"rb") as fh:return hashlib.sha256(fh.read()).hexdigest()

    def role_config_hash(self)->str:
        payload={k:{"role":v["role"],"token_sha256":v["token_sha256"]} for k,v in sorted(self.actors.items())}
        return sha256_text(canonical(payload))

    def runtime_integrity_check(self)->Dict[str,Any]:
        files=self._file_hashes();code_hash=sha256_text(canonical(files))
        dep_hash=self.dependency_hash();role_hash=self.role_config_hash()
        c=self.conn()
        # Compare against the last VERIFIED baseline. An unverified runtime
        # can never bless itself by simply running the check again.
        prev=c.execute("""SELECT * FROM security_runtime_manifests
                          WHERE version=? AND environment=? AND verified=1
                          ORDER BY id DESC LIMIT 1""",
                       (self.version,self.environment)).fetchone()
        role_changed=False
        if prev:
            role_changed=(prev["role_config_hash"]!=role_hash)
            verified=(prev["code_hash"]==code_hash and prev["dependency_hash"]==dep_hash and not role_changed)
            reason="MATCHED" if verified else ("UNVERIFIED_ROLE_CONFIG_CHANGE" if role_changed else "UNVERIFIED_RUNTIME_STATE")
        else:
            verified=True;reason="BASELINE_REGISTERED"
        c.execute("""INSERT INTO security_runtime_manifests(
          version,environment,created_ts,code_hash,dependency_hash,role_config_hash,file_hashes_json,verified,details_json)
          VALUES(?,?,?,?,?,?,?,?,?)""",
          (self.version,self.environment,now_iso(),code_hash,dep_hash,role_hash,canonical(files),int(verified),
           canonical({"reason":reason})))
        c.commit();c.close()
        self.last_integrity={"verified":verified,"reason":reason,"code_hash":code_hash,
                             "dependency_hash":dep_hash,"role_config_hash":role_hash,
                             "role_config_changed":role_changed,"file_hashes":files}
        return self.last_integrity

    def validate_environment(self,canary_live_enabled:bool,canary_env:str,
                             running_under_test:bool=False)->Dict[str,Any]:
        reasons=[]
        if self.environment not in ("DEVELOPMENT","TEST","PAPER","CANARY","PRODUCTION"):
            reasons.append("INVALID_ENVIRONMENT")
        if running_under_test and (canary_live_enabled or str(canary_env).lower()=="live"):
            reasons.append("TEST_ENVIRONMENT_REAL_BROKER_FORBIDDEN")
        if self.environment in ("DEVELOPMENT","TEST","PAPER") and canary_live_enabled:
            reasons.append("NON_PRODUCTION_LIVE_EXECUTION_FORBIDDEN")
        if str(canary_env).lower()=="live" and self.environment!="PRODUCTION":
            reasons.append("LIVE_BROKER_REQUIRES_PRODUCTION_ENVIRONMENT")
        return {"valid":not reasons,"reasons":reasons,"environment":self.environment}

    def real_order_guard(self,*,broker_account_verified:bool,risk_engine_ready:bool,
                         reconciliation_complete:bool,emergency_stop:bool,
                         deployment_authorized:bool,runtime_verified:bool,
                         running_under_test:bool=False)->Dict[str,Any]:
        reasons=[]
        if self.environment!="PRODUCTION":reasons.append("ENVIRONMENT_NOT_PRODUCTION")
        if not broker_account_verified:reasons.append("BROKER_ACCOUNT_NOT_VERIFIED")
        if not risk_engine_ready:reasons.append("RISK_ENGINE_NOT_READY")
        if not reconciliation_complete:reasons.append("RECONCILIATION_INCOMPLETE")
        if emergency_stop:reasons.append("EMERGENCY_STOP_ACTIVE")
        if not deployment_authorized:reasons.append("DEPLOYMENT_NOT_AUTHORIZED")
        if not runtime_verified:reasons.append("UNVERIFIED_RUNTIME_STATE")
        if running_under_test:reasons.append("TEST_PROCESS_REAL_ORDER_FORBIDDEN")
        return {"allow":not reasons,"reasons":reasons}

    def startup_security_check(self,*,secrets_available:bool,canary_live_enabled:bool,
                               canary_env:str,risk_limits_valid:bool,deployment_state_valid:bool,
                               audit_available:bool,running_under_test:bool=False)->Dict[str,Any]:
        checks={}
        checks["config_schema"]=all(k in self.schema for k in self.initial_config)
        checks["authorization_config"]=bool(self.actors) if self.environment=="PRODUCTION" else True
        checks["secrets_available"]=bool(secrets_available)
        checks["risk_limits_valid"]=bool(risk_limits_valid)
        checks["deployment_state_valid"]=bool(deployment_state_valid)
        checks["audit_available"]=bool(audit_available)
        checks["audit_integrity"]=bool(self.verify_audit_chain().get("verified"))
        checks["config_integrity"]=bool(self.verify_config_integrity().get("verified"))
        integ=self.runtime_integrity_check()
        checks["runtime_integrity"]=bool(integ.get("verified"))
        env=self.validate_environment(canary_live_enabled,canary_env,running_under_test)
        checks["environment"]=env["valid"]
        status="SECURITY_READY" if all(checks.values()) else "SECURITY_FAILED"
        c=self.conn();c.execute("INSERT INTO security_startup_checks(ts,status,checks_json,details_json) VALUES(?,?,?,?)",
                                (now_iso(),status,canonical(checks),canonical({"environment":env,"integrity":integ})));c.commit();c.close()
        return {"status":status,"checks":checks,"environment":env,"integrity":integ}

    # ---------- emergency stop ----------
    def authorize_emergency_reset(self,actor:Dict[str,str],health_ok:bool,reconciliation_ok:bool,
                                  reason:str)->Dict[str,Any]:
        self.require(actor,"reset_emergency_stop")
        ok=bool(health_ok and reconciliation_ok)
        self.audit(actor,"EMERGENCY_STOP_RESET_REQUEST","emergency_stop",True,False,reason,
                   "AUTHORIZED" if ok else "DENIED")
        return {"authorized":ok,"reasons":[] if ok else ["HEALTH_OR_RECONCILIATION_NOT_READY"]}

    def dashboard(self)->Dict[str,Any]:
        c=self.conn()
        pending=[dict(x) for x in c.execute("""SELECT * FROM security_change_requests
          WHERE status IN ('DRAFT','PENDING_REVIEW','APPROVED','SCHEDULED','APPLYING')
          ORDER BY requested_ts DESC LIMIT 100""").fetchall()]
        applied=[dict(x) for x in c.execute("""SELECT * FROM security_change_requests
          WHERE status='APPLIED' ORDER BY applied_ts DESC LIMIT 50""").fetchall()]
        rollbacks=[dict(x) for x in c.execute("""SELECT * FROM security_audit_log
          WHERE action='CONFIG_ROLLBACK' ORDER BY seq DESC LIMIT 50""").fetchall()]
        critical=[dict(x) for x in c.execute("""SELECT * FROM security_audit_log
          WHERE action IN ('CONFIG_CHANGED','CHANGE_VALIDATION_FAILED','CONFIG_ROLLBACK',
                           'EMERGENCY_STOP_RESET_REQUEST','PRODUCTION_DEPLOYMENT_APPROVED',
                           'ADMIN_PERMISSION_CHANGED')
          ORDER BY seq DESC LIMIT 100""").fetchall()]
        c.close()
        return {
            "environment":self.environment,
            "config_version":self.current_version(),
            "config_hash":self.current_hash(),
            "pending_change_requests":pending,
            "applied_changes":applied,
            "recent_rollbacks":rollbacks,
            "recent_critical_audit_events":critical,
            "roles":{k:sorted(v) for k,v in ROLE_PERMISSIONS.items()},
            "configured_actors":[{"actor":k,"role":v["role"]} for k,v in sorted(self.actors.items())],
            "runtime_integrity":self.last_integrity,
            "adaptive_learning_boundaries":{
                "allowed":["create_candidates","propose_parameters","recommend_changes","recommend_risk_reduction"],
                "forbidden":["change_hard_risk_limits","disable_kill_switch","modify_credentials",
                             "promote_to_production","increase_max_capital","change_permissions","delete_audit_log"]
            }
        }

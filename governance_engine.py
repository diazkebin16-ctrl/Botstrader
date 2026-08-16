
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import sqlite3, json, math, statistics, uuid, hashlib

GOVERNANCE_MODES=("SHADOW","ADVISORY","PARTIAL_ENFORCEMENT","FULL_POLICY_ENFORCEMENT")
ADAPTATION_STATES=("NORMAL_ADAPTATION","LIMITED_ADAPTATION","REVIEW","ADAPTATION_FROZEN")
META_RISK_STATES=("LOW","MODERATE","HIGH","CRITICAL")
CHANGE_MAGNITUDES=("SMALL","MODERATE","MAJOR","CRITICAL")

AUTHORITY_MATRIX={
    "ADAPTIVE_LEARNING_ENGINE":{
        "can":["ANALYZE","PROPOSE","CREATE_CANDIDATE","REQUEST_NON_AUTHORITATIVE_CHANGE"],
        "cannot":["DEPLOY","CHANGE_HARD_RISK_LIMIT","ACTIVATE_PRODUCTION","SELF_APPROVE","RESET_KILL_SWITCH"]
    },
    "AI_STRATEGY_DIRECTOR":{
        "can":["SELECT_PERMITTED_STRATEGY","REDUCE_ACTIVITY","RECOMMEND_PAUSE"],
        "cannot":["SELF_APPROVE","CHANGE_HARD_RISK_LIMIT","BYPASS_RISK_ENGINE","DIRECT_DEPLOY"]
    },
    "RISK_ENGINE":{
        "can":["REDUCE_RISK","BLOCK_TRADES","ACTIVATE_PROTECTION","EMERGENCY_STOP"],
        "cannot":["INCREASE_HARD_RISK_LIMIT","BYPASS_CHANGE_MANAGEMENT","PROMOTE_CANDIDATE"]
    },
    "DEPLOYMENT_MANAGER":{
        "can":["EXECUTE_AUTHORIZED_PROMOTION","PAUSE","REDUCE","ROLLBACK"],
        "cannot":["SKIP_VALIDATION_PIPELINE","SELF_AUTHORIZE_CANDIDATE","BYPASS_RISK_ENGINE"]
    },
    "SYSTEM_EVALUATION_ENGINE":{
        "can":["MEASURE","COMPARE","DETECT","EXPLAIN","RECOMMEND"],
        "cannot":["TRADE","DEPLOY","CHANGE_RISK","APPLY_CONFIG"]
    },
    "ENSEMBLE_ENGINE":{
        "can":["COMBINE_SIGNALS","ABSTAIN","RECOMMEND","PROPOSE_CANDIDATE_WEIGHTS"],
        "cannot":["TRADE","BYPASS_AI_DIRECTOR","BYPASS_RISK_ENGINE","INCREASE_LEVERAGE","SELF_DEPLOY_WEIGHTS"]
    },
    "CHANGE_MANAGEMENT":{
        "can":["VALIDATE_CHANGE","ROUTE_APPROVAL","VERSION_CONFIG","ROLLBACK_CONFIG"],
        "cannot":["BYPASS_RBAC","SELF_APPROVE_AUTOMATION","REWRITE_TRADING_STATE"]
    },
    "GOVERNANCE_ENGINE":{
        "can":["COORDINATE","BLOCK_ADAPTATION_WHEN_ENFORCED","FREEZE_ADAPTATION_WHEN_ENFORCED",
               "DETECT_CONFLICT","ENFORCE_CHANGE_BUDGET","RECOMMEND"],
        "cannot":["BUY","SELL","SIZE_POSITION","INCREASE_HARD_RISK","APPROVE_OWN_CRITICAL_CHANGE",
                  "BYPASS_CHANGE_MANAGEMENT","RESET_EMERGENCY_STOP"]
    }
}

AUTHORITY_PRIORITY=[
    "HARD_SAFETY_RULES",
    "RISK_ENGINE",
    "DEPLOYMENT_LIMITS",
    "AI_STRATEGY_DIRECTOR",
    "ADAPTIVE_LEARNING_RECOMMENDATIONS"
]

DEFAULT_POLICIES={
    "NO_SELF_APPROVAL":True,
    "NO_DIRECT_PRODUCTION_DEPLOYMENT":True,
    "NO_AUTOMATIC_HARD_RISK_INCREASE":True,
    "NO_KILL_SWITCH_SELF_RESET":True,
    "MINIMUM_VALIDATION_REQUIRED":True,
    "MINIMUM_STABILITY_PERIOD":True,
    "MAX_CHANGE_FREQUENCY":True,
    "MIN_STABILITY_HOURS":72,
    "LIMITED_ADAPTATION_REVIEW_HOURS":48,
    "CHANGE_BUDGET_WINDOW_DAYS":7,
    "MAX_MAJOR_CHANGES_PER_WEEK":3,
    "MAX_STRATEGY_CHANGES_PER_WEEK":5,
    "MAX_PARAMETER_CHANGES_PER_WEEK":8,
    "MAX_DEPLOYMENTS_PER_WEEK":3,
    "MAX_PROMOTIONS_PER_WEEK":2,
    "MAX_GLOBAL_CHANGES_PER_WEEK":10,
    "STRATEGY_CHURN_TRANSITIONS_7D":6,
    "PARAMETER_CHURN_CHANGES_7D":6,
    "DEPLOYMENT_CHURN_EVENTS_7D":6,
    "ADAPTATION_LOOP_EVENT_THRESHOLD":7,
    "META_RISK_HIGH":65.0,
    "META_RISK_CRITICAL":82.0,
    "DATA_QUALITY_FREEZE_THRESHOLD":0.60,
    "SYSTEM_SCORE_FREEZE_THRESHOLD":40.0,
    "DRAWDOWN_UTILIZATION_FREEZE":0.85,
    "MODEL_GAP_FREEZE_COUNT":2,
    "MAX_ACTIVE_CANDIDATES":8,
    "CONFIDENCE_CALIBRATION_MIN_SAMPLES":20,
    "CONFIDENCE_CALIBRATION_MAX_ERROR":0.20,
    "MODULE_DECISION_FRESHNESS_HOURS":6,
    "ENSEMBLE_CHURN_WEIGHT_VERSIONS_7D":20,
}

def now_iso(): return datetime.now(timezone.utc).isoformat()
def parse_ts(x):
    if not x:return None
    try:return datetime.fromisoformat(str(x).replace("Z","+00:00"))
    except Exception:return None
def f(x,default=None):
    try:
        v=float(x)
        return v if math.isfinite(v) else default
    except Exception:return default
def j(x): return json.dumps(x,separators=(",",":"),sort_keys=True,default=str)
def clamp(x,a=0.0,b=1.0): return max(a,min(b,x))
def sha(x): return hashlib.sha256(j(x).encode()).hexdigest()
def safe_div(a,b,default=0.0):
    try:return float(a)/float(b) if float(b)!=0 else default
    except Exception:return default

class GovernanceEngine:
    def __init__(self,db_path:str,version:str,mode:str="SHADOW",policies:Optional[Dict[str,Any]]=None):
        self.db_path=db_path;self.version=version
        self.mode=mode if mode in GOVERNANCE_MODES else "SHADOW"
        self.policies={**DEFAULT_POLICIES,**(policies or {})}

    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30,isolation_level=None)
        c.row_factory=sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA synchronous=FULL")
        c.execute("PRAGMA foreign_keys=ON");c.execute("PRAGMA busy_timeout=5000")
        return c

    def ensure_schema(self):
        c=self.conn();c.executescript("""
        CREATE TABLE IF NOT EXISTS governance_state(
          singleton INTEGER PRIMARY KEY CHECK(singleton=1),
          governance_mode TEXT NOT NULL,adaptation_state TEXT NOT NULL,
          recommended_state TEXT NOT NULL,governance_lock INTEGER NOT NULL DEFAULT 0,
          lock_reason TEXT,lock_source TEXT,lock_ts TEXT,review_required INTEGER NOT NULL DEFAULT 0,
          freeze_started_ts TEXT,limited_started_ts TEXT,last_review_ts TEXT,
          latest_meta_risk_score REAL NOT NULL DEFAULT 0,latest_meta_risk_state TEXT NOT NULL DEFAULT 'LOW',
          policy_version TEXT,updated_ts TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS governance_policy_versions(
          policy_version TEXT PRIMARY KEY,created_ts TEXT NOT NULL,engine_version TEXT NOT NULL,
          config_version INTEGER,policy_hash TEXT NOT NULL,policies_json TEXT NOT NULL,
          authority_matrix_json TEXT NOT NULL,priority_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS governance_decisions(
          governance_decision_id TEXT PRIMARY KEY,timestamp TEXT NOT NULL,governance_mode TEXT NOT NULL,
          adaptation_state TEXT NOT NULL,recommended_state TEXT NOT NULL,system_state TEXT,
          meta_risk_score REAL NOT NULL,meta_risk_state TEXT NOT NULL,trigger TEXT NOT NULL,
          action_type TEXT,target TEXT,affected_modules_json TEXT NOT NULL,policies_applied_json TEXT NOT NULL,
          policy_version TEXT,decision TEXT NOT NULL,would_block INTEGER NOT NULL DEFAULT 0,
          enforced INTEGER NOT NULL DEFAULT 0,reason TEXT NOT NULL,review_time TEXT,context_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS governance_conflicts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,governance_decision_id TEXT NOT NULL,ts TEXT NOT NULL,
          conflict_type TEXT NOT NULL,modules_json TEXT NOT NULL,details_json TEXT NOT NULL,
          conservative_resolution TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS governance_reviews(
          review_id TEXT PRIMARY KEY,ts TEXT NOT NULL,actor TEXT NOT NULL,from_state TEXT NOT NULL,
          to_state TEXT NOT NULL,health_json TEXT NOT NULL,reason TEXT NOT NULL,result TEXT NOT NULL
        );
        CREATE TRIGGER IF NOT EXISTS governance_policy_versions_no_update
          BEFORE UPDATE ON governance_policy_versions
          BEGIN SELECT RAISE(ABORT,'governance policy versions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS governance_policy_versions_no_delete
          BEFORE DELETE ON governance_policy_versions
          BEGIN SELECT RAISE(ABORT,'governance policy versions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS governance_decisions_no_update
          BEFORE UPDATE ON governance_decisions
          BEGIN SELECT RAISE(ABORT,'governance decisions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS governance_decisions_no_delete
          BEFORE DELETE ON governance_decisions
          BEGIN SELECT RAISE(ABORT,'governance decisions are immutable'); END;
        CREATE INDEX IF NOT EXISTS idx_gov_decisions_ts ON governance_decisions(timestamp,decision);
        CREATE INDEX IF NOT EXISTS idx_gov_conflicts_ts ON governance_conflicts(ts,conflict_type);
        """)
        row=c.execute("SELECT 1 FROM governance_state WHERE singleton=1").fetchone()
        if not row:
            c.execute("""INSERT INTO governance_state(
              singleton,governance_mode,adaptation_state,recommended_state,updated_ts)
              VALUES(1,?,'NORMAL_ADAPTATION','NORMAL_ADAPTATION',?)""",(self.mode,now_iso()))
        # Link Deployment state to the exact Governance authorization/policy.
        tables={x["name"] for x in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "deployment_registry" in tables:
            cols={x["name"] for x in c.execute("PRAGMA table_info(deployment_registry)").fetchall()}
            for ddl,col in (
                ("ALTER TABLE deployment_registry ADD COLUMN governance_policy_version TEXT","governance_policy_version"),
                ("ALTER TABLE deployment_registry ADD COLUMN governance_decision_id TEXT","governance_decision_id"),
            ):
                if col not in cols:
                    c.execute(ddl)
        c.commit();c.close()

    def link_deployment_authorization(self,candidate_id:str,decision:Dict[str,Any]):
        c=self.conn()
        try:
            c.execute("""UPDATE deployment_registry SET governance_policy_version=?,governance_decision_id=?,updated_ts=updated_ts
                         WHERE candidate_id=?""",
                      (decision.get("policy_version"),decision.get("governance_decision_id"),candidate_id))
            c.commit()
        except sqlite3.OperationalError:
            pass
        c.close()

    def set_runtime(self,mode:Optional[str]=None,policies:Optional[Dict[str,Any]]=None,config_version:Optional[int]=None):
        self.ensure_schema()
        if mode in GOVERNANCE_MODES:self.mode=mode
        if policies:self.policies={**self.policies,**policies}
        pv=self.record_policy_version(config_version)
        c=self.conn();c.execute("""UPDATE governance_state SET governance_mode=?,policy_version=?,updated_ts=?
                                  WHERE singleton=1""",(self.mode,pv,now_iso()));c.commit();c.close()
        return pv

    def record_policy_version(self,config_version:Optional[int]=None)->str:
        payload={**self.policies,"GOVERNANCE_MODE":self.mode}
        h=sha(payload);pv=f"governance_policy_{self.version}_{str(config_version or 'runtime')}_{h[:12]}"
        c=self.conn()
        c.execute("""INSERT OR IGNORE INTO governance_policy_versions(
          policy_version,created_ts,engine_version,config_version,policy_hash,policies_json,
          authority_matrix_json,priority_json) VALUES(?,?,?,?,?,?,?,?)""",
          (pv,now_iso(),self.version,config_version,h,j(payload),j(AUTHORITY_MATRIX),j(AUTHORITY_PRIORITY)))
        c.commit();c.close();return pv

    def state(self)->Dict[str,Any]:
        self.ensure_schema();c=self.conn();r=c.execute("SELECT * FROM governance_state WHERE singleton=1").fetchone();c.close()
        return dict(r) if r else {}

    def _rows(self,table,where="",params=()):
        c=self.conn()
        try:rows=[dict(x) for x in c.execute(f"SELECT * FROM {table} {where}",params).fetchall()]
        except sqlite3.OperationalError:rows=[]
        c.close();return rows

    def classify_change_magnitude(self,component:str,current:Any,proposed:Any,risk_level:Optional[str]=None)->str:
        c=str(component or "").lower()
        if risk_level=="CRITICAL" or c.startswith(("risk.","security.","governance.","broker.")):
            return "CRITICAL"
        if "deployment" in c or "strategy_candidate" in c:return "MAJOR"
        a=f(current);b=f(proposed)
        if a is not None and b is not None:
            denom=max(abs(a),1e-9);delta=abs(b-a)/denom
            if delta<=.03:return "SMALL"
            if delta<=.15:return "MODERATE"
            return "MAJOR"
        if current!=proposed:return "MODERATE"
        return "SMALL"

    def _latest_eval(self)->Dict[str,Any]:
        rows=self._rows("system_evaluations","ORDER BY generated_at DESC LIMIT 1")
        if not rows:return {}
        r=rows[0]
        def load(name):
            try:return json.loads(r.get(name) or "{}")
            except Exception:return {}
        return {
            "evaluation_id":r.get("evaluation_id"),"system_status":r.get("system_status"),
            "system_score":f(r.get("system_score"),0.0) or 0.0,
            "data_quality_score":f(r.get("data_quality_score"),0.0) or 0.0,
            "degradation":load("degradation_json"),"risk":load("risk_json"),
            "operational":load("operational_json"),"model_reality_gap":load("model_reality_gap_json"),
            "diversification":load("diversification_json"),"director":load("director_json"),
            "risk_engine":load("risk_engine_json"),"stability":load("stability_json"),
            "trading":load("trading_json")
        }

    def _change_counts(self,now:datetime)->Dict[str,Any]:
        days=int(self.policies["CHANGE_BUDGET_WINDOW_DAYS"]);cut=(now-timedelta(days=days)).isoformat()
        audits=self._rows("security_audit_log","WHERE timestamp>=? ORDER BY timestamp",(cut,))
        changes=[x for x in audits if x.get("action") in (
            "CONFIG_CHANGED","CONFIG_ROLLBACK","STRATEGY_CONFIGURATION_APPLIED",
            "CANDIDATE_PROMOTION","PRODUCTION_DEPLOYMENT_APPROVED"
        )]
        params=[x for x in changes if x.get("action")=="CONFIG_CHANGED"]
        strategy=[x for x in changes if "strategy" in str(x.get("resource") or "").lower()]
        per_strategy={}
        for x in strategy:
            resource=str(x.get("resource") or "")
            lower=resource.lower()
            key="UNKNOWN"
            if "strategy." in lower:
                tail=resource[lower.index("strategy.")+len("strategy."):]
                key=tail.split(".",1)[0].split(":",1)[0] or "UNKNOWN"
            elif "candidate:" in lower:
                key=resource.split("candidate:",1)[1].split(":",1)[0]
            per_strategy[key]=per_strategy.get(key,0)+1
        global_changes=[x for x in changes if x not in strategy]
        dep=self._rows("deployment_events","WHERE ts>=? ORDER BY ts",(cut,))
        promotions=[x for x in dep if x.get("event_type")=="PROMOTION"]
        major=sum(1 for x in changes if any(k in str(x.get("resource") or "").lower() for k in ("risk","deployment","strategy")))
        return {"window_days":days,"major_changes":major,"strategy_changes":len(strategy),
                "strategy_changes_by_strategy":per_strategy,
                "parameter_changes":len(params),"deployments":len(dep),"promotions":len(promotions),
                "global_changes":len(global_changes),"audit_rows":changes,"deployment_rows":dep}

    def _strategy_churn(self,now:datetime)->Dict[str,Any]:
        cut=(now-timedelta(days=7)).isoformat()
        rows=self._rows("ai_strategy_director_decisions","WHERE ts>=? ORDER BY setup_variant,ts,id",(cut,))
        by={};transitions={}
        for x in rows:
            st=x.get("setup_variant") or "UNKNOWN";state=x.get("recommended_state")
            if st in by and by[st]!=state:transitions[st]=transitions.get(st,0)+1
            by[st]=state
        hot={k:v for k,v in transitions.items() if v>=int(self.policies["STRATEGY_CHURN_TRANSITIONS_7D"])}
        return {"transitions":transitions,"hot":hot,"detected":bool(hot)}

    def _parameter_churn(self,now:datetime)->Dict[str,Any]:
        cut=(now-timedelta(days=7)).isoformat()
        rows=self._rows("security_audit_log","WHERE timestamp>=? AND action='CONFIG_CHANGED' ORDER BY timestamp",(cut,))
        by={}
        for x in rows:by[x.get("resource") or "UNKNOWN"]=by.get(x.get("resource") or "UNKNOWN",0)+1
        hot={k:v for k,v in by.items() if v>=int(self.policies["PARAMETER_CHURN_CHANGES_7D"])}
        return {"changes":by,"hot":hot,"detected":bool(hot)}

    def _deployment_churn(self,now:datetime)->Dict[str,Any]:
        cut=(now-timedelta(days=7)).isoformat()
        rows=self._rows("deployment_events","WHERE ts>=? ORDER BY ts",(cut,))
        by={}
        for x in rows:
            cid=x.get("candidate_id") or "SYSTEM";by[cid]=by.get(cid,0)+1
        hot={k:v for k,v in by.items() if v>=int(self.policies["DEPLOYMENT_CHURN_EVENTS_7D"])}
        return {"events":len(rows),"by_candidate":by,"hot":hot,"detected":bool(hot)}

    def _ensemble_churn(self,now:datetime)->Dict[str,Any]:
        cut=(now-timedelta(days=7)).isoformat()
        rows=self._rows("ensemble_weight_versions","WHERE created_at>=? ORDER BY created_at",(cut,))
        candidates=self._rows("ensemble_policy_candidates","WHERE created_at>=? ORDER BY created_at",(cut,))
        detected=len(rows)>=int(self.policies.get("ENSEMBLE_CHURN_WEIGHT_VERSIONS_7D",20))
        return {"weight_versions":len(rows),"candidate_weight_configs":len(candidates),
                "detected":detected,"status":"ENSEMBLE_CHURN" if detected else "NORMAL"}

    def _confidence_calibration(self)->Dict[str,Any]:
        rows=self._rows("trade_memory","WHERE status='CLOSED' AND strategy_confidence_entry IS NOT NULL AND realized_r IS NOT NULL")
        buckets={}
        for x in rows:
            c=clamp(f(x.get("strategy_confidence_entry"),0.0) or 0.0)
            lo=math.floor(c*10)/10.0;key=f"{lo:.1f}-{min(1.0,lo+.1):.1f}"
            b=buckets.setdefault(key,{"n":0,"conf":[],"wins":0})
            b["n"]+=1;b["conf"].append(c);b["wins"]+=1 if (f(x.get("realized_r"),0.0) or 0)>0 else 0
        out={};weighted=0;total=0
        for k,b in buckets.items():
            pred=statistics.mean(b["conf"]);real=b["wins"]/b["n"];err=abs(pred-real)
            out[k]={"sample_size":b["n"],"predicted":pred,"realized_win_rate":real,"absolute_error":err}
            if b["n"]>=int(self.policies["CONFIDENCE_CALIBRATION_MIN_SAMPLES"]):
                weighted+=err*b["n"];total+=b["n"]
        e=weighted/total if total else None
        bad=e is not None and e>float(self.policies["CONFIDENCE_CALIBRATION_MAX_ERROR"])
        return {"buckets":out,"weighted_calibration_error":e,
                "status":"CONFIDENCE_MIS_CALIBRATION" if bad else "CALIBRATED_OR_INSUFFICIENT_DATA",
                "detected":bad}

    def _module_conflicts(self,latest_eval:Dict[str,Any],now:Optional[datetime]=None)->List[Dict[str,Any]]:
        conflicts=[]
        now=now or datetime.now(timezone.utc)
        cutoff=(now-timedelta(hours=float(self.policies["MODULE_DECISION_FRESHNESS_HOURS"]))).isoformat()
        dirs=self._rows("ai_strategy_director_decisions",
                        "WHERE ts>=? AND id IN (SELECT MAX(id) FROM ai_strategy_director_decisions WHERE ts>=? GROUP BY setup_variant)",
                        (cutoff,cutoff))
        risks=self._rows("adaptive_risk_decisions",
                         "WHERE ts>=? AND id IN (SELECT MAX(id) FROM adaptive_risk_decisions WHERE ts>=? GROUP BY setup_variant)",
                         (cutoff,cutoff))
        risk_by={x.get("setup_variant"):x for x in risks}
        for d in dirs:
            r=risk_by.get(d.get("setup_variant"))
            if not r:continue
            director=d.get("recommended_state")
            cautious=(not int(r.get("allow_new_trades") or 0)) or (f(r.get("risk_multiplier"),1.0) or 1.0)<.75
            if director=="ACTIVE" and cautious:
                conflicts.append({"type":"MODULE_DECISION_CONFLICT","strategy":d.get("setup_variant"),
                                  "modules":["AI_STRATEGY_DIRECTOR","RISK_ENGINE"],
                                  "director":director,"risk_allow":bool(r.get("allow_new_trades")),
                                  "risk_multiplier":r.get("risk_multiplier"),
                                  "resolution":"RISK_ENGINE_MORE_RESTRICTIVE"})
        # Cross-model directional disagreement where the persisted schema supports it.
        regimes=self._rows("market_regime_history",
            "WHERE ts>=? AND id IN (SELECT MAX(id) FROM market_regime_history WHERE ts>=? GROUP BY instrument)",
            (cutoff,cutoff))
        signals=self._rows("signals",
            "WHERE ts>=? AND id IN (SELECT MAX(id) FROM signals WHERE ts>=? GROUP BY instrument)",
            (cutoff,cutoff))
        sig_by={x.get("instrument"):x for x in signals}
        for rg in regimes:
            sig=sig_by.get(rg.get("instrument"))
            if not sig:continue
            regime=str(rg.get("market_regime") or "").upper()
            direction=str(sig.get("signal") or "").upper()
            disagree=(("BULL" in regime and direction in ("SELL","SHORT")) or
                      ("BEAR" in regime and direction in ("BUY","LONG")))
            if disagree:
                conflicts.append({"type":"HIGH_MODEL_DISAGREEMENT","instrument":rg.get("instrument"),
                                  "modules":["MARKET_REGIME_DETECTOR","STRATEGY_MODEL"],
                                  "market_regime":regime,"strategy_signal":direction,
                                  "resolution":"NO_FORCED_HIGH_CONFIDENCE_DECISION"})
        deg=(latest_eval.get("degradation") or {}).get("types") or []
        if "STRATEGY_DEGRADATION" in deg:
            for d in dirs:
                if d.get("recommended_state")=="ACTIVE":
                    conflicts.append({"type":"MODULE_DECISION_CONFLICT","strategy":d.get("setup_variant"),
                                      "modules":["AI_STRATEGY_DIRECTOR","SYSTEM_EVALUATION_ENGINE"],
                                      "director":"ACTIVE","system_evaluation":"STRATEGY_DEGRADATION",
                                      "resolution":"NO_AGGRESSIVE_ADAPTATION"})
        return conflicts

    def _adaptation_loop(self,counts,churn,param_churn,dep_churn)->Dict[str,Any]:
        score=0;factors=[]
        if churn["detected"]:score+=3;factors.append("STRATEGY_CHURN")
        if param_churn["detected"]:score+=3;factors.append("PARAMETER_CHURN")
        if dep_churn["detected"]:score+=3;factors.append("DEPLOYMENT_CHURN")
        if counts["major_changes"]>=int(self.policies["ADAPTATION_LOOP_EVENT_THRESHOLD"]):
            score+=3;factors.append("HIGH_MAJOR_CHANGE_FREQUENCY")
        detected=score>=6
        return {"detected":detected,"loop_score":score,"factors":factors,
                "status":"ADAPTATION_LOOP_DETECTED" if detected else "NORMAL"}

    def _objective_drift(self,ev:Dict[str,Any])->Dict[str,Any]:
        trading=ev.get("trading") or {};risk=ev.get("risk") or {};stab=ev.get("stability") or {}
        gross=f(trading.get("gross_pnl"),0.0) or 0.0;net=f(trading.get("net_pnl"),0.0) or 0.0
        fee_drag=abs(gross-net)/max(abs(gross),1e-9) if gross else 0.0
        draw_util=f(risk.get("drawdown_utilization"),0.0) or 0.0
        unstable=(f(stab.get("score"),100.0) or 100)<50
        detected=(net>0 and (draw_util>.8 or fee_drag>.35 or unstable))
        return {"detected":detected,"status":"OBJECTIVE_DRIFT_DETECTED" if detected else "NORMAL",
                "net_pnl":net,"gross_pnl":gross,"gross_to_net_drag":fee_drag,
                "drawdown_utilization":draw_util,"stability_score":stab.get("score"),
                "objectives":["capital_preservation","risk_adjusted_return","stability","robustness",
                              "operational_reliability","controlled_adaptation"]}

    def _learning_data_governance(self)->Dict[str,Any]:
        all_rows=self._rows("trade_memory","WHERE status='CLOSED'")
        compromised=[x for x in all_rows if int(x.get("execution_quality_compromised") or 0)]
        valid=[x for x in all_rows if not int(x.get("execution_quality_compromised") or 0)]
        return {"VALID_TRADING_DATA":len(valid),"COMPROMISED_DATA":len(compromised),
                "excluded_contexts":["broker_outage","corrupted_feed","severe_execution_error",
                                     "manual_override_when_tagged","emergency_liquidation_when_tagged"],
                "adaptive_learning_should_use_compromised":False}

    def _budget(self,counts)->Dict[str,Any]:
        limits={
            "major_changes":int(self.policies["MAX_MAJOR_CHANGES_PER_WEEK"]),
            "strategy_changes":int(self.policies["MAX_STRATEGY_CHANGES_PER_WEEK"]),
            "parameter_changes":int(self.policies["MAX_PARAMETER_CHANGES_PER_WEEK"]),
            "deployments":int(self.policies["MAX_DEPLOYMENTS_PER_WEEK"]),
            "promotions":int(self.policies["MAX_PROMOTIONS_PER_WEEK"]),
            "global_changes":int(self.policies["MAX_GLOBAL_CHANGES_PER_WEEK"])
        }
        remaining={k:max(0,limits[k]-int(counts.get(k,0))) for k in limits}
        exhausted=[k for k,v in remaining.items() if v<=0]
        per_strategy_used=counts.get("strategy_changes_by_strategy") or {}
        per_strategy_remaining={
            k:max(0,int(self.policies["MAX_STRATEGY_CHANGES_PER_WEEK"])-int(v))
            for k,v in per_strategy_used.items()
        }
        exhausted_strategies=[k for k,v in per_strategy_remaining.items() if v<=0]
        return {"limits":limits,"used":{k:int(counts.get(k,0)) for k in limits},
                "remaining":remaining,"per_strategy_used":per_strategy_used,
                "per_strategy_remaining":per_strategy_remaining,
                "exhausted_strategies":exhausted_strategies,
                "exhausted":exhausted,"exhausted_any":bool(exhausted or exhausted_strategies)}

    def _stability_window(self,now:datetime)->Dict[str,Any]:
        events=[]
        for x in self._rows("security_audit_log","WHERE action IN ('CONFIG_CHANGED','CONFIG_ROLLBACK','STRATEGY_CONFIGURATION_APPLIED','CANDIDATE_PROMOTION','PRODUCTION_DEPLOYMENT_APPROVED') ORDER BY timestamp DESC LIMIT 1"):
            events.append(parse_ts(x.get("timestamp")))
        for x in self._rows("deployment_events","WHERE event_type IN ('PROMOTION','ROLLBACK','REDUCTION') ORDER BY ts DESC LIMIT 1"):
            events.append(parse_ts(x.get("ts")))
        events=[x for x in events if x]
        last=max(events) if events else None
        hours=float(self.policies["MIN_STABILITY_HOURS"])
        ends=last+timedelta(hours=hours) if last else None
        complete=(not ends) or now>=ends
        return {"last_major_change":last.isoformat() if last else None,
                "required_hours":hours,"window_ends":ends.isoformat() if ends else None,
                "complete":complete}

    def meta_risk(self,now:Optional[datetime]=None)->Dict[str,Any]:
        now=now or datetime.now(timezone.utc);ev=self._latest_eval();counts=self._change_counts(now)
        churn=self._strategy_churn(now);pchurn=self._parameter_churn(now);dchurn=self._deployment_churn(now);echurn=self._ensemble_churn(now)
        conflicts=self._module_conflicts(ev,now);loop=self._adaptation_loop(counts,churn,pchurn,dchurn)
        budget=self._budget(counts);cal=self._confidence_calibration();obj=self._objective_drift(ev)
        active_candidates=len(self._rows("deployment_registry","WHERE current_stage IN ('APPROVED_FOR_CANARY','CANARY_LIVE','LIMITED_PRODUCTION')"))
        incidents=len(self._rows("recovery_incidents","WHERE status!='RECOVERED'"))
        concept=len(self._rows("concept_drift_alerts","WHERE status IN ('POSSIBLE_CONCEPT_DRIFT','ACTIVE','DEGRADING')"))
        dq=f(ev.get("data_quality_score"),1.0) if ev else 1.0
        score=0.0;parts={}
        parts["change_frequency"]=min(20.0,counts["major_changes"]*3.0+counts["parameter_changes"]*1.0)
        parts["strategy_churn"]=15.0 if churn["detected"] else min(10.0,sum(churn["transitions"].values())*1.5)
        parts["deployment_velocity"]=min(15.0,dchurn["events"]*2.0)
        parts["module_disagreement"]=min(15.0,len(conflicts)*5.0)
        parts["incidents"]=min(20.0,incidents*10.0)
        parts["data_quality"]=max(0.0,(1.0-(dq if dq is not None else 1.0))*20.0)
        draw=f((ev.get("risk") or {}).get("drawdown_utilization"),0.0) if ev else 0.0
        parts["drawdown"]=min(20.0,(draw or 0.0)*20.0)
        parts["active_candidates"]=min(10.0,max(0,active_candidates-int(self.policies["MAX_ACTIVE_CANDIDATES"]))*2.0)
        parts["concept_drift"]=min(10.0,concept*4.0)
        parts["adaptation_loop"]=15.0 if loop["detected"] else 0.0
        parts["objective_drift"]=10.0 if obj["detected"] else 0.0
        parts["ensemble_churn"]=10.0 if echurn["detected"] else min(5.0,echurn["weight_versions"]*.15)
        sys_status=(ev or {}).get("system_status")
        sys_score=f((ev or {}).get("system_score"),100.0) or 100.0
        parts["system_health"]=30.0 if sys_status in ("CRITICAL","PAUSED") else 20.0 if sys_status=="HIGH_RISK" else 12.0 if sys_status=="DEGRADING" else max(0.0,(70.0-sys_score)*0.4)
        raw=sum(parts.values());score=min(100.0,raw)
        if score>=float(self.policies["META_RISK_CRITICAL"]):state="CRITICAL"
        elif score>=float(self.policies["META_RISK_HIGH"]):state="HIGH"
        elif score>=35:state="MODERATE"
        else:state="LOW"
        model_disagreement={"count":len(conflicts),
                            "score":min(1.0,len(conflicts)/3.0),
                            "status":"HIGH_MODEL_DISAGREEMENT" if len(conflicts)>=2 else "NORMAL",
                            "conflicts":conflicts}
        return {"score":score,"state":state,"components":parts,"system_evaluation":ev,
                "change_counts":counts,"change_budget":budget,"strategy_churn":churn,
                "parameter_churn":pchurn,"deployment_churn":dchurn,"ensemble_churn":echurn,"conflicts":conflicts,
                "model_disagreement":model_disagreement,
                "adaptation_loop":loop,"confidence_calibration":cal,"objective_drift":obj,
                "active_candidates":active_candidates,"active_incidents":incidents,
                "concept_drift_alerts":concept,"learning_data_governance":self._learning_data_governance(),
                "stability_window":self._stability_window(now)}

    def _latest_anomaly_signal(self)->Dict[str,Any]:
        """Return the latest actionable anomaly state when Step 19 tables are present.

        Governance treats the anomaly engine as an advisory input only. In SHADOW mode
        this can change the *recommended* adaptation state, but never enforces a freeze.
        """
        c=self.conn()
        try:
            row=c.execute(
                "SELECT severity, context_json, ts FROM anomaly_composite_history ORDER BY id DESC LIMIT 1"
            ).fetchone()
        except sqlite3.OperationalError:
            row=None
        finally:
            c.close()
        if not row:
            return {"present":False,"actionable_score":0.0,"severity":"NORMAL","ts":None}
        try:
            ctx=json.loads(row["context_json"] or "{}")
        except Exception:
            ctx={}
        score=f(ctx.get("actionable_score"),None)
        if score is None:
            score=0.0
        sev=str(ctx.get("actionable_severity") or row["severity"] or "NORMAL").upper()
        return {"present":True,"actionable_score":clamp(score),"severity":sev,"ts":row["ts"]}

    def _freeze_triggers(self,meta)->List[str]:
        ev=meta.get("system_evaluation") or {};tr=[]
        if ev and (ev.get("system_status")=="CRITICAL" or
                   f(ev.get("system_score"),100.0)<float(self.policies["SYSTEM_SCORE_FREEZE_THRESHOLD"])):
            tr.append("SYSTEM_CRITICAL")
        if f(ev.get("data_quality_score"),1.0)<float(self.policies["DATA_QUALITY_FREEZE_THRESHOLD"]):tr.append("LOW_DATA_QUALITY")
        if meta["active_incidents"]>0:tr.append("ACTIVE_OPERATIONAL_INCIDENT")
        if len(meta["conflicts"])>0:tr.append("MODULE_DECISION_CONFLICT")
        if meta["adaptation_loop"]["detected"]:tr.append("ADAPTATION_LOOP_DETECTED")
        if meta.get("ensemble_churn",{}).get("detected"):tr.append("ENSEMBLE_CHURN")
        if meta["change_budget"]["exhausted_any"]:tr.append("CHANGE_BUDGET_EXHAUSTED")
        if meta["concept_drift_alerts"]>0:tr.append("UNRESOLVED_CONCEPT_DRIFT")
        if f((ev.get("risk") or {}).get("drawdown_utilization"),0.0)>=float(self.policies["DRAWDOWN_UTILIZATION_FREEZE"]):tr.append("DRAWDOWN_ELEVATED")
        if len((ev.get("model_reality_gap") or {}).get("material_gaps") or [])>=int(self.policies["MODEL_GAP_FREEZE_COUNT"]):tr.append("MODEL_REALITY_GAP")
        deg=(ev.get("degradation") or {}).get("types") or []
        if "EXECUTION_DEGRADATION" in deg and f(ev.get("system_score"),100.0)<75.0:
            tr.append("EXECUTION_QUALITY_DEGRADED")
        anomaly=self._latest_anomaly_signal()
        if anomaly.get("present"):
            score=f(anomaly.get("actionable_score"),0.0)
            sev=str(anomaly.get("severity") or "NORMAL").upper()
            if sev=="CRITICAL" or score>=0.85:
                tr.append("COMPOSITE_ANOMALY_CRITICAL")
            elif sev=="HIGH" or score>=0.70:
                tr.append("ANOMALY_RISK_HIGH")
        if meta["state"]=="CRITICAL":tr.append("META_RISK_CRITICAL")
        return list(dict.fromkeys(tr))

    def evaluate(self,trigger:str="periodic")->Dict[str,Any]:
        self.ensure_schema();now=datetime.now(timezone.utc);meta=self.meta_risk(now);st=self.state()
        freeze=self._freeze_triggers(meta)
        if int(st.get("governance_lock") or 0):
            recommended="ADAPTATION_FROZEN";freeze=["GOVERNANCE_LOCK"]+freeze
        elif freeze or meta["state"]=="CRITICAL":
            recommended="ADAPTATION_FROZEN"
        elif meta["state"]=="HIGH":
            recommended="LIMITED_ADAPTATION"
        else:
            recommended="NORMAL_ADAPTATION"
        actual=st.get("adaptation_state") or "NORMAL_ADAPTATION"
        # No automatic exit from frozen/review. Enforced modes may enter freeze/limited,
        # but only an explicit review can move frozen -> review -> limited -> normal.
        if self.mode in ("PARTIAL_ENFORCEMENT","FULL_POLICY_ENFORCEMENT"):
            if recommended=="ADAPTATION_FROZEN" and actual!="ADAPTATION_FROZEN":
                actual="ADAPTATION_FROZEN"
            elif actual=="NORMAL_ADAPTATION" and recommended=="LIMITED_ADAPTATION":
                actual="LIMITED_ADAPTATION"
        review_time=None
        if recommended=="ADAPTATION_FROZEN":
            review_time=(now+timedelta(hours=float(self.policies["MIN_STABILITY_HOURS"]))).isoformat()
        c=self.conn()
        c.execute("""UPDATE governance_state SET governance_mode=?,adaptation_state=?,recommended_state=?,
                     review_required=?,freeze_started_ts=CASE WHEN ?='ADAPTATION_FROZEN' AND freeze_started_ts IS NULL THEN ? ELSE freeze_started_ts END,
                     latest_meta_risk_score=?,latest_meta_risk_state=?,updated_ts=? WHERE singleton=1""",
                  (self.mode,actual,recommended,int(recommended=="ADAPTATION_FROZEN"),
                   actual,now_iso(),meta["score"],meta["state"],now_iso()))
        c.commit();c.close()
        policies=[]
        if freeze:policies+=["FREEZE_ADAPTATION_ON_CRITICAL_CONDITIONS"]
        if meta["change_budget"]["exhausted_any"]:policies.append("MAX_CHANGE_FREQUENCY")
        if meta["adaptation_loop"]["detected"]:policies.append("ADAPTATION_LOOP_CONTROL")
        if meta["conflicts"]:policies.append("CONSERVATIVE_AUTHORITY_PRIORITY")
        decision="WOULD_FREEZE" if recommended=="ADAPTATION_FROZEN" and self.mode=="SHADOW" else \
                 "WOULD_LIMIT" if recommended=="LIMITED_ADAPTATION" and self.mode=="SHADOW" else \
                 "FREEZE" if actual=="ADAPTATION_FROZEN" else \
                 "LIMIT" if actual=="LIMITED_ADAPTATION" else "ALLOW_NORMAL_ADAPTATION"
        return self._log_decision(trigger=trigger,action_type="SYSTEM_GOVERNANCE",target="SYSTEM",
            system_state=(meta.get("system_evaluation") or {}).get("system_status"),
            meta=meta,affected=["SYSTEM_EVALUATION_ENGINE","ADAPTIVE_LEARNING_ENGINE","AI_STRATEGY_DIRECTOR",
                                "RISK_ENGINE","DEPLOYMENT_MANAGER","CHANGE_MANAGEMENT"],
            policies=policies,decision=decision,would_block=recommended=="ADAPTATION_FROZEN",
            enforced=(self.mode in ("PARTIAL_ENFORCEMENT","FULL_POLICY_ENFORCEMENT") and actual=="ADAPTATION_FROZEN"),
            reason="; ".join(freeze) if freeze else f"meta_risk={meta['state']}",review_time=review_time,
            context={"recommended_state":recommended,"actual_state":actual,"freeze_triggers":freeze})

    def _log_decision(self,*,trigger,action_type,target,system_state,meta,affected,policies,
                      decision,would_block,enforced,reason,review_time=None,context=None)->Dict[str,Any]:
        did="gov_"+uuid.uuid4().hex;st=self.state();pv=st.get("policy_version") or self.record_policy_version()
        c=self.conn()
        c.execute("""INSERT INTO governance_decisions(
          governance_decision_id,timestamp,governance_mode,adaptation_state,recommended_state,system_state,
          meta_risk_score,meta_risk_state,trigger,action_type,target,affected_modules_json,
          policies_applied_json,policy_version,decision,would_block,enforced,reason,review_time,context_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (did,now_iso(),self.mode,st.get("adaptation_state","NORMAL_ADAPTATION"),
           (context or {}).get("recommended_state",st.get("recommended_state","NORMAL_ADAPTATION")),
           system_state,meta["score"],meta["state"],trigger,action_type,target,j(affected),j(policies),
           pv,decision,int(would_block),int(enforced),reason,review_time,j(context or {})))
        for cf in meta.get("conflicts") or []:
            c.execute("""INSERT INTO governance_conflicts(
              governance_decision_id,ts,conflict_type,modules_json,details_json,conservative_resolution)
              VALUES(?,?,?,?,?,?)""",(did,now_iso(),cf.get("type","MODULE_DECISION_CONFLICT"),
                                      j(cf.get("modules") or []),j(cf),cf.get("resolution","MOST_RESTRICTIVE_WINS")))
        if (context or {}).get("policy_conflict"):
            c.execute("""INSERT INTO governance_conflicts(
              governance_decision_id,ts,conflict_type,modules_json,details_json,conservative_resolution)
              VALUES(?,?,?,?,?,?)""",(did,now_iso(),"POLICY_CONFLICT_RESOLVED_CONSERVATIVELY",
                                      j(["VALIDATION_POLICY","SYSTEM_STABILITY_POLICY"]),
                                      j(context or {}),"MOST_RESTRICTIVE_APPLICABLE_POLICY_WINS"))
        c.commit();c.close()
        return {"governance_decision_id":did,"timestamp":now_iso(),"governance_mode":self.mode,
                "adaptation_state":st.get("adaptation_state"),"recommended_state":(context or {}).get("recommended_state"),
                "system_state":system_state,"meta_risk_score":meta["score"],"meta_risk_state":meta["state"],
                "trigger":trigger,"action_type":action_type,"target":target,"affected_modules":affected,
                "policies_applied":policies,"policy_version":pv,"decision":decision,
                "would_block":bool(would_block),"enforced":bool(enforced),"reason":reason,
                "review_time":review_time,"context":context or {},"meta":meta}

    def check_action(self,action_type:str,target:str="",context:Optional[Dict[str,Any]]=None)->Dict[str,Any]:
        context=context or {};meta=self.meta_risk();st=self.state();policies=[];reasons=[];conflicts=meta["conflicts"]
        fresh_freeze_triggers=self._freeze_triggers(meta)
        fresh_recommended_state="ADAPTATION_FROZEN" if (int(st.get("governance_lock") or 0) or fresh_freeze_triggers) else                                 "LIMITED_ADAPTATION" if meta["state"]=="HIGH" else "NORMAL_ADAPTATION"
        magnitude=context.get("magnitude") or self.classify_change_magnitude(
            context.get("component") or target,context.get("current_value"),context.get("proposed_value"),context.get("risk_level"))
        if int(st.get("governance_lock") or 0):
            policies.append("GOVERNANCE_LOCK");reasons.append("Persistent Governance Lock is active")
        if st.get("adaptation_state")=="ADAPTATION_FROZEN" or fresh_recommended_state=="ADAPTATION_FROZEN":
            if action_type in ("CHANGE_APPLY","DEPLOYMENT_APPROVAL","DEPLOYMENT_PROMOTION","CANDIDATE_CRITICAL_CREATE",
                               "ENSEMBLE_WEIGHT_CHANGE","ENSEMBLE_MODEL_ADD","ENSEMBLE_PROMOTION"):
                policies.append("ADAPTATION_FROZEN")
                reasons.append("Adaptation is frozen or would be frozen under current governance conditions")
        if not meta["stability_window"]["complete"] and magnitude in ("MAJOR","CRITICAL"):
            policies.append("MINIMUM_STABILITY_PERIOD");reasons.append("Stability observation window has not completed")
        if meta["change_budget"]["exhausted_any"] and action_type in ("CHANGE_APPLY","DEPLOYMENT_APPROVAL","DEPLOYMENT_PROMOTION"):
            policies.append("MAX_CHANGE_FREQUENCY");reasons.append("Change budget exhausted")
        strategy_key=context.get("strategy_id")
        if not strategy_key:
            comp=str(context.get("component") or "")
            low=comp.lower()
            if "strategy." in low:
                tail=comp[low.index("strategy.")+len("strategy."):]
                strategy_key=tail.split(".",1)[0].split(":",1)[0] or None
        if strategy_key and strategy_key in (meta["change_budget"].get("exhausted_strategies") or []):
            policies.append("PER_STRATEGY_CHANGE_BUDGET")
            reasons.append(f"Change budget exhausted for strategy {strategy_key}")
        policy_conflict=False
        if action_type=="EXECUTION_POLICY_DEPLOYMENT":
            ev=meta.get("system_evaluation") or {}
            deg=(ev.get("degradation") or {}).get("types") or []
            if "EXECUTION_DEGRADATION" in deg:
                policies.append("EXECUTION_POLICY_STABILITY_REQUIRED")
                reasons.append("Execution Quality is degraded; new execution-policy deployment is blocked/reviewed")
        if action_type in ("DEPLOYMENT_APPROVAL","DEPLOYMENT_PROMOTION"):
            eligible_validation=context.get("validation_state") in ("READY_FOR_REVIEW","APPROVED_FOR_CANARY","CANARY_LIVE","LIMITED_PRODUCTION")
            system_status=(meta.get("system_evaluation") or {}).get("system_status")
            if system_status in ("DEGRADING","HIGH_RISK","CRITICAL","PAUSED","UNDER_REVIEW"):
                policies.append("SYSTEM_STABILITY_REQUIRED")
                reasons.append(f"System Evaluation status is {system_status}")
                if eligible_validation:
                    policy_conflict=True
            if not eligible_validation:
                policies.append("MINIMUM_VALIDATION_REQUIRED");reasons.append("Validation state is not eligible for promotion")
            if conflicts:
                policies.append("CONSERVATIVE_AUTHORITY_PRIORITY");reasons.append("Module decision conflict requires conservative resolution")
            if meta["state"] in ("HIGH","CRITICAL"):
                policies.append("META_RISK_LIMIT");reasons.append(f"Meta risk is {meta['state']}")
        if action_type in ("ENSEMBLE_WEIGHT_CHANGE","ENSEMBLE_MODEL_ADD","ENSEMBLE_PROMOTION"):
            ensemble_eval=((meta.get("system_evaluation") or {}).get("ensemble_effectiveness") or {})
            if ensemble_eval.get("assessment")=="LOW_DIVERSITY":
                policies.append("ENSEMBLE_DIVERSITY_REQUIRED");reasons.append("Ensemble diversity is too low for promotion/change")
            if meta.get("ensemble_churn",{}).get("detected"):
                policies.append("ENSEMBLE_CHURN_CONTROL");reasons.append("Ensemble weight/model churn detected")
            if action_type=="ENSEMBLE_PROMOTION" and context.get("validation_state") not in ("VALIDATED","PAPER_PASS","CANARY_PASS","READY_FOR_REVIEW"):
                policies.append("MINIMUM_VALIDATION_REQUIRED");reasons.append("Ensemble promotion requires validation/paper/canary evidence")
        if action_type=="DIRECT_PRODUCTION_DEPLOYMENT":
            policies.append("NO_DIRECT_PRODUCTION_DEPLOYMENT");reasons.append("Direct production deployment is forbidden")
        if action_type=="HARD_RISK_INCREASE":
            policies.append("NO_AUTOMATIC_HARD_RISK_INCREASE");reasons.append("Automatic hard-risk increase is forbidden")
        if action_type=="KILL_SWITCH_RESET" and context.get("requested_by_automation"):
            policies.append("NO_KILL_SWITCH_SELF_RESET");reasons.append("Automation cannot reset a kill switch")
        if context.get("requester")==context.get("approver") and context.get("requester") is not None:
            policies.append("NO_SELF_APPROVAL");reasons.append("Requester cannot self-approve")
        if st.get("adaptation_state")=="LIMITED_ADAPTATION" and magnitude in ("MAJOR","CRITICAL"):
            if action_type in ("CANDIDATE_CREATE","CANDIDATE_CRITICAL_CREATE","CHANGE_APPLY","DEPLOYMENT_APPROVAL","DEPLOYMENT_PROMOTION"):
                policies.append("LIMITED_ADAPTATION")
                reasons.append("Limited adaptation permits analysis, recommendations, low-risk Candidates and paper testing only")
        if meta["objective_drift"]["detected"] and magnitude in ("MAJOR","CRITICAL"):
            policies.append("OBJECTIVE_DRIFT_CONTROL");reasons.append("Objective drift detected")
        if meta["confidence_calibration"]["detected"] and action_type in ("DEPLOYMENT_PROMOTION","CHANGE_APPLY"):
            policies.append("CONFIDENCE_CALIBRATION_CONTROL");reasons.append("Confidence scores are materially mis-calibrated")
        would_block=bool(reasons)
        deterministic_critical=any(p in policies for p in (
            "GOVERNANCE_LOCK","NO_DIRECT_PRODUCTION_DEPLOYMENT","NO_AUTOMATIC_HARD_RISK_INCREASE",
            "NO_KILL_SWITCH_SELF_RESET","NO_SELF_APPROVAL"
        ))
        enforce=False
        if int(st.get("governance_lock") or 0):enforce=True
        elif self.mode=="FULL_POLICY_ENFORCEMENT":enforce=would_block
        elif self.mode=="PARTIAL_ENFORCEMENT":enforce=would_block and deterministic_critical
        # SHADOW/ADVISORY never auto-enforce.
        decision=("BLOCK" if enforce else "WOULD_BLOCK" if would_block else "ALLOW")
        if len(set(policies))!=len(policies):
            policies=list(dict.fromkeys(policies))
        if policy_conflict:
            policies.append("POLICY_CONFLICT_RESOLVED_CONSERVATIVELY")
            reasons.append("Validation eligibility conflicted with higher-order stability/safety policy; restrictive policy won")
        return self._log_decision(trigger=context.get("trigger") or "ACTION_CHECK",action_type=action_type,target=target,
            system_state=(meta.get("system_evaluation") or {}).get("system_status"),meta=meta,
            affected=context.get("affected_modules") or [],policies=policies,decision=decision,
            would_block=would_block,enforced=enforce,reason="; ".join(reasons) if reasons else "No governance policy blocks this action",
            review_time=meta["stability_window"].get("window_ends"),
            context={**context,"magnitude":magnitude,"recommended_state":self.state().get("recommended_state"),
                     "authority_priority":AUTHORITY_PRIORITY,
                     "fresh_recommended_state":fresh_recommended_state,
                     "fresh_freeze_triggers":fresh_freeze_triggers,
                     "policy_conflict":policy_conflict})

    def resolve_policy_conflict(self,policy_votes:List[Dict[str,Any]])->Dict[str,Any]:
        """
        Deterministic conservative resolver. If any applicable safety/authority
        policy votes BLOCK, the combined decision is BLOCK. An explicit mixture
        of ALLOW and BLOCK is surfaced as POLICY_CONFLICT_RESOLVED_CONSERVATIVELY.
        """
        votes=[str(x.get("decision") or "").upper() for x in policy_votes]
        has_block="BLOCK" in votes;has_allow="ALLOW" in votes
        decision="BLOCK" if has_block else "ALLOW"
        return {"decision":decision,
                "policy_conflict":bool(has_block and has_allow),
                "event":"POLICY_CONFLICT_RESOLVED_CONSERVATIVELY" if has_block and has_allow else None,
                "votes":policy_votes,"principle":"MOST_RESTRICTIVE_APPLICABLE_POLICY_WINS"}

    def set_lock(self,active:bool,reason:str,source:str)->Dict[str,Any]:
        self.ensure_schema();c=self.conn();ts=now_iso()
        c.execute("""UPDATE governance_state SET governance_lock=?,lock_reason=?,lock_source=?,
                     lock_ts=CASE WHEN ?=1 THEN ? ELSE lock_ts END,
                     review_required=CASE WHEN ?=1 THEN 1 ELSE review_required END,
                     adaptation_state=CASE WHEN ?=1 THEN 'ADAPTATION_FROZEN' ELSE adaptation_state END,
                     recommended_state=CASE WHEN ?=1 THEN 'ADAPTATION_FROZEN' ELSE recommended_state END,
                     updated_ts=? WHERE singleton=1""",
                  (int(active),reason,source,int(active),ts,int(active),int(active),int(active),ts))
        c.commit();c.close()
        meta=self.meta_risk()
        return self._log_decision(trigger="GOVERNANCE_LOCK_ACTIVATED" if active else "GOVERNANCE_LOCK_RELEASE_REQUEST",
            action_type="GOVERNANCE_LOCK",target="SYSTEM",system_state=(meta.get("system_evaluation") or {}).get("system_status"),
            meta=meta,affected=["DEPLOYMENT_MANAGER","CHANGE_MANAGEMENT","ADAPTIVE_LEARNING_ENGINE"],
            policies=["GOVERNANCE_LOCK"],decision="LOCKED" if active else "LOCK_CLEARED_PENDING_REVIEW",
            would_block=active,enforced=active,reason=reason,context={"source":source,"recommended_state":"ADAPTATION_FROZEN" if active else self.state().get("recommended_state")})

    def review_transition(self,actor:str,reason:str)->Dict[str,Any]:
        self.ensure_schema();st=self.state();meta=self.meta_risk();now=datetime.now(timezone.utc)
        health={
            "system_status":(meta.get("system_evaluation") or {}).get("system_status"),
            "system_score":(meta.get("system_evaluation") or {}).get("system_score"),
            "data_quality":(meta.get("system_evaluation") or {}).get("data_quality_score"),
            "meta_risk":meta["state"],"active_incidents":meta["active_incidents"],
            "conflicts":len(meta["conflicts"]),"stability_window":meta["stability_window"],
            "governance_lock":bool(st.get("governance_lock"))
        }
        old=st.get("adaptation_state");new=old;result="DENIED"
        clean=(meta["state"] in ("LOW","MODERATE") and meta["active_incidents"]==0 and not meta["conflicts"]
               and f((meta.get("system_evaluation") or {}).get("data_quality_score"),1.0)>=.75
               and (meta.get("system_evaluation") or {}).get("system_status") not in ("CRITICAL","HIGH_RISK","PAUSED"))
        if int(st.get("governance_lock") or 0):
            result="DENIED_LOCK_ACTIVE"
        elif old=="ADAPTATION_FROZEN" and clean and meta["stability_window"]["complete"]:
            new="REVIEW";result="APPROVED"
        elif old=="REVIEW" and clean:
            last=parse_ts(st.get("last_review_ts"))
            if last and now-last>=timedelta(hours=float(self.policies["LIMITED_ADAPTATION_REVIEW_HOURS"])):
                new="LIMITED_ADAPTATION";result="APPROVED"
            else:result="DENIED_REVIEW_OBSERVATION_INCOMPLETE"
        elif old=="LIMITED_ADAPTATION" and clean:
            limited=parse_ts(st.get("limited_started_ts"))
            if limited and now-limited>=timedelta(hours=float(self.policies["MIN_STABILITY_HOURS"])):
                new="NORMAL_ADAPTATION";result="APPROVED"
            else:result="DENIED_LIMITED_OBSERVATION_INCOMPLETE"
        c=self.conn()
        if result=="APPROVED":
            c.execute("""UPDATE governance_state SET adaptation_state=?,recommended_state=?,
                         last_review_ts=?,limited_started_ts=CASE WHEN ?='LIMITED_ADAPTATION' THEN ? ELSE limited_started_ts END,
                         review_required=CASE WHEN ?='NORMAL_ADAPTATION' THEN 0 ELSE 1 END,updated_ts=?
                         WHERE singleton=1""",(new,new,now_iso(),new,now_iso(),new,now_iso()))
        rid="grev_"+uuid.uuid4().hex
        c.execute("""INSERT INTO governance_reviews(review_id,ts,actor,from_state,to_state,health_json,reason,result)
                     VALUES(?,?,?,?,?,?,?,?)""",(rid,now_iso(),actor,old,new,j(health),reason,result))
        c.commit();c.close()
        return {"review_id":rid,"from_state":old,"to_state":new,"result":result,"health":health,
                "automatic_resume":False}

    def effectiveness(self)->Dict[str,Any]:
        decisions=self._rows("governance_decisions","WHERE governance_mode='SHADOW' AND would_block=1 ORDER BY timestamp")
        evals=self._rows("system_evaluations","ORDER BY as_of_ts")
        bad_preventable=good_blocked=unknown=0;details=[]
        for d in decisions:
            dt=parse_ts(d.get("timestamp"))
            future=[e for e in evals if parse_ts(e.get("as_of_ts")) and dt and dt<parse_ts(e["as_of_ts"])<=dt+timedelta(days=7)]
            if not future:unknown+=1;continue
            worst=min(future,key=lambda x:f(x.get("system_score"),100.0))
            status=worst.get("system_status")
            if status in ("DEGRADING","HIGH_RISK","CRITICAL","PAUSED"):
                bad_preventable+=1;classification="POTENTIALLY_USEFUL_BLOCK"
            else:
                good_blocked+=1;classification="POSSIBLE_FALSE_POSITIVE"
            details.append({"governance_decision_id":d["governance_decision_id"],"classification":classification,
                            "later_status":status,"later_score":worst.get("system_score"),
                            "counterfactual_only":True})
        total=bad_preventable+good_blocked
        return {"shadow_decisions_evaluated":total,"potential_bad_changes_avoided":bad_preventable,
                "possible_good_changes_blocked":good_blocked,"unknown_outcomes":unknown,
                "estimated_precision":safe_div(bad_preventable,total,None) if total else None,
                "false_positive_estimate":safe_div(good_blocked,total,None) if total else None,
                "false_negative_estimate":"UNAVAILABLE_WITHOUT_CONTROLLED_GOVERNANCE_EXPERIMENT",
                "counterfactual_only":True,"details":details[-100:]}

    def due(self,interval_minutes:int=60)->bool:
        rows=self._rows("governance_decisions","WHERE action_type='SYSTEM_GOVERNANCE' ORDER BY timestamp DESC LIMIT 1")
        if not rows:return True
        ts=parse_ts(rows[0].get("timestamp"))
        return not ts or datetime.now(timezone.utc)-ts>=timedelta(minutes=max(1,int(interval_minutes)))

    def dashboard(self)->Dict[str,Any]:
        self.ensure_schema();st=self.state();meta=self.meta_risk()
        dec=self._rows("governance_decisions","ORDER BY timestamp DESC LIMIT 50")
        conflicts=self._rows("governance_conflicts","ORDER BY ts DESC LIMIT 50")
        return {"governance_mode":self.mode,"adaptation_state":st.get("adaptation_state"),
                "recommended_adaptation_state":st.get("recommended_state"),
                "meta_risk_score":meta["score"],"meta_risk_state":meta["state"],
                "current_stability_window":meta["stability_window"],
                "change_budget":meta["change_budget"],"active_governance_lock":bool(st.get("governance_lock")),
                "lock_reason":st.get("lock_reason"),"module_conflicts":meta["conflicts"],
                "strategy_churn":meta["strategy_churn"],"parameter_churn":meta["parameter_churn"],
                "deployment_churn":meta["deployment_churn"],"ensemble_churn":meta.get("ensemble_churn"),"adaptation_loop":meta["adaptation_loop"],
                "model_disagreement":meta["model_disagreement"],
                "confidence_calibration":meta["confidence_calibration"],"objective_drift":meta["objective_drift"],
                "learning_data_governance":meta["learning_data_governance"],
                "authority_matrix":AUTHORITY_MATRIX,"authority_priority":AUTHORITY_PRIORITY,
                "incident_governance_priority":["RISK_CONTAINMENT","STATE_RECONCILIATION","SYSTEM_RECOVERY","EVALUATION","ADAPTATION"],
                "policies":self.policies,"policy_version":st.get("policy_version"),
                "recent_governance_decisions":dec,"recent_conflicts":conflicts,
                "effectiveness":self.effectiveness(),"trading_signal_authority":False,
                "automatic_risk_increase_authority":False}

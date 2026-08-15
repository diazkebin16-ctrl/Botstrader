
from __future__ import annotations
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, timedelta
import sqlite3, json, math
import httpx
from deployment_manager import ALLOCATION_STEPS,next_allocation,stage_for_allocation,r_metrics,promotion_gate,fail_safe

def now_iso(): return datetime.now(timezone.utc).isoformat()

def f(v,default=None):
    try:
        x=float(v)
        return x if math.isfinite(x) else default
    except Exception:return default

def parse(ts):
    try:return datetime.fromisoformat(str(ts).replace("Z","+00:00"))
    except Exception:return None

class DeploymentManager:
    def __init__(self,db_path:str,base_url:str,account:str,token:str,live_enabled:bool,
                 allowed_symbols:List[str],allowed_regimes:List[str],
                 min_validation_score=.75,min_paper_trades=30,min_paper_days=14,
                 min_paper_regimes=2,min_live_trades=10,min_limited_trades=25,
                 min_live_days=3,min_live_regimes=1,promotion_cooldown_hours=72,
                 max_promotions_7d=2,max_exposure_increase=.25,max_daily_risk=.005,
                 max_drawdown=.02,max_consecutive_losses=3,max_stage_days=30,
                 max_slippage_pips=2.5,max_latency_seconds=4.0,base_risk_fraction=.005):
        self.db_path=db_path;self.base_url=base_url;self.account=account;self.token=token
        self.live_enabled=bool(live_enabled)
        self.allowed_symbols=tuple(allowed_symbols);self.allowed_regimes=tuple(allowed_regimes)
        self.min_validation_score=min_validation_score
        self.min_paper_trades=min_paper_trades;self.min_paper_days=min_paper_days;self.min_paper_regimes=min_paper_regimes
        self.min_live_trades=min_live_trades;self.min_limited_trades=min_limited_trades
        self.min_live_days=min_live_days;self.min_live_regimes=min_live_regimes
        self.promotion_cooldown_hours=promotion_cooldown_hours;self.max_promotions_7d=max_promotions_7d
        self.max_exposure_increase=max_exposure_increase
        self.max_daily_risk=max_daily_risk;self.max_drawdown=max_drawdown
        self.max_consecutive_losses=max_consecutive_losses;self.max_stage_days=max_stage_days
        self.max_slippage_pips=max_slippage_pips;self.max_latency_seconds=max_latency_seconds
        self.base_risk_fraction=base_risk_fraction

    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30,isolation_level=None)
        c.row_factory=sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA foreign_keys=ON")
        return c

    def ensure_schema(self):
        c=self.conn()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS deployment_registry(
          candidate_id TEXT PRIMARY KEY,strategy_id TEXT NOT NULL,candidate_version TEXT NOT NULL,
          production_version TEXT NOT NULL,current_stage TEXT NOT NULL,previous_stage TEXT,
          allocation_fraction REAL NOT NULL DEFAULT 0,eligible_allocation_fraction REAL NOT NULL DEFAULT 0,
          approval_source TEXT,approval_note TEXT,approved_ts TEXT,stage_started_ts TEXT,last_promotion_ts TEXT,
          resume_required INTEGER NOT NULL DEFAULT 1,new_trades_enabled INTEGER NOT NULL DEFAULT 0,
          rollback_reason TEXT,live_metrics_json TEXT NOT NULL DEFAULT '{}',expected_metrics_json TEXT NOT NULL DEFAULT '{}',
          paper_metrics_json TEXT NOT NULL DEFAULT '{}',next_gate_json TEXT NOT NULL DEFAULT '{}',
          last_health_check_ts TEXT,created_ts TEXT NOT NULL,updated_ts TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS deployment_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,candidate_id TEXT,strategy_version TEXT,
          event_type TEXT NOT NULL,previous_stage TEXT,new_stage TEXT,capital_allocation REAL,
          risk_allocation REAL,validation_score REAL,live_metrics_json TEXT NOT NULL DEFAULT '{}',
          reason TEXT NOT NULL,approval_source TEXT,details_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS deployment_live_trades(
          id INTEGER PRIMARY KEY AUTOINCREMENT,candidate_id TEXT NOT NULL,candidate_version TEXT NOT NULL,
          strategy_id TEXT NOT NULL,signal_id INTEGER,trade_id TEXT UNIQUE,order_id TEXT,stage TEXT NOT NULL,
          allocation_fraction REAL NOT NULL,approved_risk_fraction REAL NOT NULL,instrument TEXT NOT NULL,
          direction TEXT NOT NULL,units REAL NOT NULL,expected_entry REAL NOT NULL,fill_price REAL,
          stop_loss REAL NOT NULL,take_profit REAL NOT NULL,slippage_pips REAL,latency_seconds REAL,
          market_regime TEXT,volatility_state TEXT,director_state TEXT,director_confidence REAL,risk_multiplier REAL,
          status TEXT NOT NULL DEFAULT 'OPEN',opened_ts TEXT NOT NULL,closed_ts TEXT,close_price REAL,
          realized_pl REAL,realized_r REAL,exit_reason TEXT,operational_error TEXT,created_ts TEXT NOT NULL,updated_ts TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS deployment_kill_switches(
          scope TEXT PRIMARY KEY,active INTEGER NOT NULL DEFAULT 0,reason TEXT,source TEXT,
          activated_ts TEXT,cleared_ts TEXT,updated_ts TEXT NOT NULL,details_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS deployment_promotion_gates(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,candidate_id TEXT NOT NULL,from_stage TEXT NOT NULL,
          requested_allocation REAL,proposed_allocation REAL,gate_status TEXT NOT NULL,criteria_json TEXT NOT NULL DEFAULT '{}',
          metrics_json TEXT NOT NULL DEFAULT '{}',reason TEXT NOT NULL,approval_source TEXT);
        CREATE INDEX IF NOT EXISTS idx_deploy_stage ON deployment_registry(current_stage,updated_ts);
        CREATE INDEX IF NOT EXISTS idx_deploy_live ON deployment_live_trades(candidate_id,status,opened_ts);
        """)
        c.close()

    def event(self,candidate_id,event,prev,new,allocation,reason,source=None,details=None,risk=None):
        c=self.conn();version=None;score=None
        if candidate_id:
            row=c.execute("""SELECT cs.candidate_version,cr.validation_score FROM candidate_strategies cs
                             LEFT JOIN candidate_registry cr ON cr.candidate_id=cs.candidate_id
                             WHERE cs.candidate_id=?""",(candidate_id,)).fetchone()
            if row:version=row["candidate_version"];score=row["validation_score"]
        c.execute("""INSERT INTO deployment_events(ts,candidate_id,strategy_version,event_type,previous_stage,new_stage,
                    capital_allocation,risk_allocation,validation_score,live_metrics_json,reason,approval_source,details_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (now_iso(),candidate_id,version,event,prev,new,allocation,risk,score,
                   json.dumps(self.live_metrics(candidate_id) if candidate_id else {},separators=(",",":")),
                   reason,source,json.dumps(details or {},separators=(",",":"),default=str)))
        c.commit();c.close()

    def kill(self,scope):
        c=self.conn();r=c.execute("SELECT * FROM deployment_kill_switches WHERE scope=?",(scope,)).fetchone();c.close()
        return dict(r) if r else {"scope":scope,"active":0}

    def set_kill(self,scope,active,reason,source):
        c=self.conn();ts=now_iso()
        c.execute("""INSERT INTO deployment_kill_switches(scope,active,reason,source,activated_ts,cleared_ts,updated_ts)
                     VALUES(?,?,?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET active=excluded.active,reason=excluded.reason,
                     source=excluded.source,activated_ts=CASE WHEN excluded.active=1 THEN excluded.activated_ts ELSE deployment_kill_switches.activated_ts END,
                     cleared_ts=CASE WHEN excluded.active=0 THEN excluded.cleared_ts ELSE NULL END,updated_ts=excluded.updated_ts""",
                  (scope,int(active),reason,source,ts if active else None,ts if not active else None,ts))
        c.commit();c.close();self.event(None,"KILL_SWITCH_ON" if active else "KILL_SWITCH_OFF",None,None,None,f"{scope}: {reason}",source)
        return self.kill(scope)

    def candidate(self,candidate_id):
        c=self.conn();r=c.execute("""SELECT cs.*,cr.current_state registry_state,cr.historical_validation_status,
          cr.validation_score registry_validation_score,cr.paper_trade_count,cr.paper_regime_count,cr.paper_days,
          cr.divergence_status,cr.latest_validation_id,cr.final_reason registry_reason
          FROM candidate_strategies cs JOIN candidate_registry cr ON cr.candidate_id=cs.candidate_id
          WHERE cs.candidate_id=?""",(candidate_id,)).fetchone();c.close()
        return dict(r) if r else None

    def critical_alerts(self,strategy):
        c=self.conn()
        a=[dict(x) for x in c.execute("""SELECT scope_key,status,reason FROM trade_memory_degradation
                                         WHERE strategy=? AND status='DEGRADED'""",(strategy,)).fetchall()]
        b=[dict(x) for x in c.execute("""SELECT scope_key,status,reason FROM concept_drift_alerts
                                         WHERE strategy_id=? AND status='POSSIBLE_CONCEPT_DRIFT'""",(strategy,)).fetchall()]
        c.close();return a+b

    def readiness(self,candidate_id):
        x=self.candidate(candidate_id);reasons=[]
        if not x:return {"ready":False,"reasons":["CANDIDATE_NOT_FOUND"]}
        if x["registry_state"]!="READY_FOR_REVIEW":reasons.append("NOT_READY_FOR_REVIEW")
        if x["historical_validation_status"]!="PASSED":reasons.append("HISTORICAL_VALIDATION_NOT_PASSED")
        if float(x["registry_validation_score"] or 0)<self.min_validation_score:reasons.append("VALIDATION_SCORE_TOO_LOW")
        if int(x["paper_trade_count"] or 0)<self.min_paper_trades:reasons.append("PAPER_SAMPLE_TOO_SMALL")
        if float(x["paper_days"] or 0)<self.min_paper_days:reasons.append("PAPER_PERIOD_TOO_SHORT")
        if int(x["paper_regime_count"] or 0)<self.min_paper_regimes:reasons.append("PAPER_REGIME_COVERAGE_TOO_LOW")
        if x["divergence_status"]!="CONSISTENT":reasons.append("BACKTEST_LIVE_DIVERGENCE_OR_PAPER_FAILURE")
        if self.critical_alerts(x["strategy_id"]):reasons.append("CRITICAL_DEGRADATION_OR_CONCEPT_DRIFT")
        return {"ready":not reasons,"reasons":reasons,"candidate":x}

    def approve(self,candidate_id,source,note=""):
        if not source:return {"ok":False,"status":"APPROVAL_SOURCE_REQUIRED"}
        ready=self.readiness(candidate_id)
        if not ready["ready"]:return {"ok":False,"status":"REJECTED","reasons":ready["reasons"]}
        x=ready["candidate"];c=self.conn();old=c.execute("SELECT current_stage FROM deployment_registry WHERE candidate_id=?",(candidate_id,)).fetchone()
        prev=old["current_stage"] if old else "READY_FOR_REVIEW"
        c.execute("""INSERT INTO deployment_registry(candidate_id,strategy_id,candidate_version,production_version,current_stage,
          previous_stage,allocation_fraction,eligible_allocation_fraction,approval_source,approval_note,approved_ts,
          resume_required,new_trades_enabled,created_ts,updated_ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(candidate_id) DO UPDATE SET previous_stage=deployment_registry.current_stage,current_stage='APPROVED_FOR_CANARY',
          allocation_fraction=0,eligible_allocation_fraction=0,approval_source=excluded.approval_source,
          approval_note=excluded.approval_note,approved_ts=excluded.approved_ts,resume_required=1,new_trades_enabled=0,updated_ts=excluded.updated_ts""",
          (candidate_id,x["strategy_id"],x["candidate_version"],x["production_version"],"APPROVED_FOR_CANARY",prev,0,0,
           source,note,now_iso(),1,0,now_iso(),now_iso()))
        c.execute("UPDATE candidate_registry SET current_state='APPROVED_FOR_CANARY',auto_deploy=0,updated_ts=? WHERE candidate_id=?",(now_iso(),candidate_id))
        c.commit();c.close();self.event(candidate_id,"PROMOTION",prev,"APPROVED_FOR_CANARY",0,"explicit approval",source,{"note":note},0)
        return {"ok":True,"stage":"APPROVED_FOR_CANARY"}

    async def req(self,client,method,path,params=None,body=None):
        if not self.account or not self.token:raise RuntimeError("missing canary account/token")
        r=await client.request(method,self.base_url+path.replace("{account}",self.account),params=params,json=body,
                               headers={"Authorization":f"Bearer {self.token}","Content-Type":"application/json"},timeout=15)
        if r.status_code>=400:
            try:msg=r.json().get("errorMessage") or r.json().get("errorCode")
            except Exception:msg=r.text[:200]
            raise RuntimeError(f"canary broker HTTP {r.status_code}: {msg}")
        return r.json()

    async def account_health(self,client):
        out={"broker_ok":False,"data_ok":False,"nav":None,"margin_usage":None,"current_drawdown":None,
             "open_instruments":[],"system_abnormal":False,"errors":[]}
        try:
            d=await self.req(client,"GET","/v3/accounts/{account}/summary");a=d.get("account") or {}
            nav=f(a.get("NAV"));mu=f(a.get("marginUsed"),0.0)
            out["nav"]=nav;out["margin_usage"]=(mu/nav) if nav and nav>0 else None;out["broker_ok"]=nav is not None
            # Persist high-water NAV in kill-switch details row.
            c=self.conn();row=c.execute("SELECT details_json FROM deployment_kill_switches WHERE scope='CANARY_NAV_STATE'").fetchone();c.close()
            peak=nav
            if row and nav is not None:
                try:peak=max(nav,float(json.loads(row["details_json"]).get("peak_nav",nav)))
                except Exception:pass
            if nav and peak:out["current_drawdown"]=max(0.0,(peak-nav)/peak)
            c=self.conn();c.execute("""INSERT INTO deployment_kill_switches(scope,active,reason,source,updated_ts,details_json)
                                      VALUES('CANARY_NAV_STATE',0,'state','SYSTEM',?,?)
                                      ON CONFLICT(scope) DO UPDATE SET updated_ts=excluded.updated_ts,details_json=excluded.details_json""",
                                   (now_iso(),json.dumps({"peak_nav":peak})));c.commit();c.close()
        except Exception as e:out["errors"].append(f"summary:{e}")
        try:
            d=await self.req(client,"GET","/v3/accounts/{account}/openPositions")
            out["open_instruments"]=[x.get("instrument") for x in d.get("positions",[]) if x.get("instrument")]
        except Exception as e:out["errors"].append(f"positions:{e}")
        out["data_ok"]=not out["errors"];out["system_abnormal"]=bool(out["errors"])
        return out

    async def start(self,candidate_id,source,health_override=None):
        c=self.conn();d=c.execute("SELECT * FROM deployment_registry WHERE candidate_id=?",(candidate_id,)).fetchone();c.close()
        if not d or d["current_stage"]!="APPROVED_FOR_CANARY":return {"ok":False,"status":"INVALID_STAGE"}
        if not self.live_enabled:return {"ok":False,"status":"LIVE_EXECUTION_DISABLED"}
        health=health_override
        if health is None:
            async with httpx.AsyncClient() as client:health=await self.account_health(client)
        if not health.get("broker_ok") or not health.get("data_ok") or health.get("system_abnormal"):
            return {"ok":False,"status":"HEALTH_CHECK_FAILED","health":health}
        alloc=ALLOCATION_STEPS[0];c=self.conn()
        c.execute("""UPDATE deployment_registry SET previous_stage=current_stage,current_stage='CANARY_LIVE',
                     allocation_fraction=?,eligible_allocation_fraction=?,stage_started_ts=?,last_health_check_ts=?,
                     resume_required=0,new_trades_enabled=1,updated_ts=? WHERE candidate_id=?""",
                  (alloc,alloc,now_iso(),now_iso(),now_iso(),candidate_id))
        c.execute("UPDATE candidate_registry SET current_state='CANARY_LIVE',auto_deploy=0,updated_ts=? WHERE candidate_id=?",(now_iso(),candidate_id))
        c.commit();c.close();self.event(candidate_id,"PROMOTION","APPROVED_FOR_CANARY","CANARY_LIVE",alloc,"canary health check passed",source,risk=self.base_risk_fraction*alloc)
        return {"ok":True,"stage":"CANARY_LIVE","allocation_fraction":alloc}

    def mark_restart(self):
        c=self.conn();rows=c.execute("SELECT candidate_id,current_stage FROM deployment_registry WHERE current_stage IN ('CANARY_LIVE','LIMITED_PRODUCTION')").fetchall()
        for r in rows:c.execute("UPDATE deployment_registry SET resume_required=1,new_trades_enabled=0,updated_ts=? WHERE candidate_id=?",(now_iso(),r["candidate_id"]))
        c.commit();c.close()
        for r in rows:self.event(r["candidate_id"],"RESTART_HOLD",r["current_stage"],r["current_stage"],None,"restart requires health check","SYSTEM")
        return len(rows)

    async def resume(self,candidate_id,source,health_override=None):
        c=self.conn();d=c.execute("SELECT * FROM deployment_registry WHERE candidate_id=?",(candidate_id,)).fetchone();c.close()
        if not d or d["current_stage"] not in ("CANARY_LIVE","LIMITED_PRODUCTION"):return {"ok":False,"status":"INVALID_STAGE"}
        health=health_override
        if health is None:
            async with httpx.AsyncClient() as client:health=await self.account_health(client)
        if not health.get("broker_ok") or not health.get("data_ok") or health.get("system_abnormal"):
            return {"ok":False,"status":"HEALTH_CHECK_FAILED","health":health}
        c=self.conn();c.execute("UPDATE deployment_registry SET resume_required=0,new_trades_enabled=1,last_health_check_ts=?,updated_ts=? WHERE candidate_id=?",(now_iso(),now_iso(),candidate_id));c.commit();c.close()
        self.event(candidate_id,"RESUME",d["current_stage"],d["current_stage"],d["allocation_fraction"],"post-restart health check passed",source)
        return {"ok":True,"stage":d["current_stage"]}

    def live(self,strategy):
        c=self.conn();rows=c.execute("""SELECT dr.*,cs.change_type,cs.proposed_value_json FROM deployment_registry dr
                                       JOIN candidate_strategies cs ON cs.candidate_id=dr.candidate_id
                                       WHERE dr.strategy_id=? AND dr.current_stage IN ('CANARY_LIVE','LIMITED_PRODUCTION')""",(strategy,)).fetchall();c.close()
        return [dict(x) for x in rows]

    def candidate_passes(self,dep,ctx):
        try:pv=json.loads(dep["proposed_value_json"])
        except Exception:pv=dep["proposed_value_json"]
        typ=dep["change_type"]
        if typ=="MIN_CONFIDENCE":
            v=f(ctx.get("strategy_confidence_entry"));return v is not None and v>=float(pv)
        if typ=="MIN_DIRECTOR_CONFIDENCE":
            v=f(ctx.get("director_confidence_entry"));return v is not None and v>=float(pv)
        if typ=="EXCLUDE_REGIME":return str(ctx.get("market_regime_entry"))!=str(pv)
        if typ=="EXCLUDE_VOLATILITY":return str(ctx.get("volatility_state_entry"))!=str(pv)
        return False

    def signal_gate(self,dep,ctx,risk,health):
        system_kill=bool(self.kill("SYSTEM").get("active") or self.kill("ALL_CANDIDATES").get("active"))
        candidate_kill=bool(self.kill("CANDIDATE:"+dep["candidate_id"]).get("active"))
        base=fail_safe(stage=dep["current_stage"],resume_required=bool(dep["resume_required"]),
          system_kill=system_kill,candidate_kill=candidate_kill,
          regime_ok=ctx.get("market_regime_entry") in self.allowed_regimes,
          director_ok=ctx.get("director_state") not in (None,"PAUSED","DISABLED"),
          risk_ok=bool(risk and risk.get("allow_new_trades") and not risk.get("emergency_stop")),
          data_ok=bool(health.get("data_ok")),broker_ok=bool(health.get("broker_ok")),
          new_trades_enabled=bool(dep["new_trades_enabled"]))
        if ctx.get("instrument") not in self.allowed_symbols:
            base["allow"]=False;base["reasons"].append("SYMBOL_NOT_ALLOWED")
        if not self.candidate_passes(dep,ctx):
            base["allow"]=False;base["reasons"].append("CANDIDATE_OVERLAY_REJECTED_SIGNAL")
        return base

    def live_metrics(self,candidate_id):
        if not candidate_id:return {}
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM deployment_live_trades WHERE candidate_id=? ORDER BY opened_ts,id",(candidate_id,)).fetchall()];c.close()
        closed=[r for r in rows if r["status"]=="CLOSED" and r["realized_r"] is not None]
        base=r_metrics([r["realized_r"] for r in closed])
        ds=[parse(r["opened_ts"]) for r in rows if parse(r["opened_ts"])]
        base["days"]=(max(ds)-min(ds)).total_seconds()/86400 if len(ds)>=2 else 0.0
        base["regimes"]=len({r["market_regime"] for r in closed if r["market_regime"]})
        slips=[abs(float(r["slippage_pips"] or 0)) for r in rows if r["fill_price"] is not None]
        lats=[float(r["latency_seconds"] or 0) for r in rows if r["latency_seconds"] is not None]
        base["avg_slippage_pips"]=sum(slips)/len(slips) if slips else 0.0
        base["avg_latency_seconds"]=sum(lats)/len(lats) if lats else 0.0
        base["operational_errors"]=sum(1 for r in rows if r["operational_error"])
        vals=[float(r["realized_r"]) for r in closed]
        if len(vals)>=4:
            m=len(vals)//2;base["stability"]=(int((sum(vals[:m])/m)>0)+int((sum(vals[m:])/(len(vals)-m))>0))/2
        else:base["stability"]=1.0 if vals and (sum(vals)/len(vals))>0 else 0.0
        return base

    def refs(self,candidate_id):
        c=self.conn();reg=c.execute("SELECT latest_validation_id FROM candidate_registry WHERE candidate_id=?",(candidate_id,)).fetchone()
        vr=c.execute("SELECT oos_results_json,paper_results_json FROM candidate_validation_runs WHERE validation_id=?",(reg["latest_validation_id"],)).fetchone() if reg else None;c.close()
        expected={};paper={}
        if vr:
            try:
                x=json.loads(vr["oos_results_json"] or "{}");expected=(x.get("comparison") or {}).get("candidate") or x.get("candidate") or {}
            except Exception:pass
            try:
                x=json.loads(vr["paper_results_json"] or "{}");paper=x.get("candidate") or {}
            except Exception:pass
        if expected.get("realized_r_expectancy") is not None and expected.get("expectancy_r") is None:expected["expectancy_r"]=expected["realized_r_expectancy"]
        return expected,paper

    def evaluate(self,candidate_id,auto=True):
        c=self.conn();d=c.execute("SELECT * FROM deployment_registry WHERE candidate_id=?",(candidate_id,)).fetchone();c.close()
        if not d:return {"action":"NOT_DEPLOYED"}
        live=self.live_metrics(candidate_id);expected,paper=self.refs(candidate_id)
        reasons=[];hard=False;pause=False
        # hard candidate-local limits
        if live.get("max_drawdown_r",0)*self.base_risk_fraction>=self.max_drawdown:
            hard=True;reasons.append("CANARY_DRAWDOWN_LIMIT")
        if live.get("max_consecutive_losses",0)>=self.max_consecutive_losses:
            hard=True;reasons.append("CANARY_CONSECUTIVE_LOSS_LIMIT")
        if live.get("avg_slippage_pips",0)>self.max_slippage_pips:
            hard=True;reasons.append("EXCESSIVE_SLIPPAGE")
        if live.get("avg_latency_seconds",0)>self.max_latency_seconds:
            pause=True;reasons.append("UNEXPECTED_LATENCY")
        if live.get("operational_errors",0)>0:
            hard=True;reasons.append("OPERATIONAL_ERRORS")
        if live.get("trades",0)>=self.min_live_trades:
            lexp=live.get("expectancy_r");pexp=paper.get("expectancy_r");eexp=expected.get("expectancy_r")
            for name,ref in (("paper",pexp),("expected",eexp)):
                if ref is not None and lexp is not None and abs(lexp-ref)/max(abs(float(ref)),.1)>.60:
                    pause=True;reasons.append("BACKTEST_LIVE_DIVERGENCE_"+name.upper())
        action="ROLLBACK" if hard else "PAUSE" if pause else "HOLD_CURRENT_LEVEL"
        live["divergence_status"]="CONSISTENT" if not pause and not hard else "LIVE_DIVERGENCE"
        if auto and d["current_stage"] in ("CANARY_LIVE","LIMITED_PRODUCTION"):
            if action=="ROLLBACK":self.rollback(candidate_id,"; ".join(reasons),"AUTO_MONITOR")
            elif action=="PAUSE":self.pause(candidate_id,"; ".join(reasons),"AUTO_MONITOR")
        c=self.conn();c.execute("UPDATE deployment_registry SET live_metrics_json=?,expected_metrics_json=?,paper_metrics_json=?,updated_ts=? WHERE candidate_id=?",
                              (json.dumps(live),json.dumps(expected),json.dumps(paper),now_iso(),candidate_id));c.commit();c.close()
        return {"action":action,"reasons":reasons,"live":live,"expected":expected,"paper":paper}

    def pause(self,candidate_id,reason,source):
        c=self.conn();d=c.execute("SELECT * FROM deployment_registry WHERE candidate_id=?",(candidate_id,)).fetchone()
        if not d:c.close();return {"ok":False}
        prev=d["current_stage"];c.execute("UPDATE deployment_registry SET previous_stage=?,current_stage='CANARY_PAUSED',new_trades_enabled=0,rollback_reason=?,updated_ts=? WHERE candidate_id=?",(prev,reason,now_iso(),candidate_id))
        c.execute("UPDATE candidate_registry SET current_state='CANARY_PAUSED',auto_deploy=0,updated_ts=? WHERE candidate_id=?",(now_iso(),candidate_id));c.commit();c.close()
        self.event(candidate_id,"PAUSE",prev,"CANARY_PAUSED",d["allocation_fraction"],reason,source)
        return {"ok":True,"stage":"CANARY_PAUSED"}

    def rollback(self,candidate_id,reason,source):
        c=self.conn();d=c.execute("SELECT * FROM deployment_registry WHERE candidate_id=?",(candidate_id,)).fetchone()
        if not d:c.close();return {"ok":False}
        open_n=c.execute("SELECT COUNT(*) n FROM deployment_live_trades WHERE candidate_id=? AND status='OPEN'",(candidate_id,)).fetchone()["n"]
        prev=d["current_stage"];c.execute("UPDATE deployment_registry SET previous_stage=?,current_stage='ROLLED_BACK',new_trades_enabled=0,allocation_fraction=0,rollback_reason=?,updated_ts=? WHERE candidate_id=?",(prev,reason,now_iso(),candidate_id))
        c.execute("UPDATE candidate_registry SET current_state='ROLLED_BACK',auto_deploy=0,updated_ts=? WHERE candidate_id=?",(now_iso(),candidate_id));c.commit();c.close()
        self.event(candidate_id,"ROLLBACK",prev,"ROLLED_BACK",0,reason,source,{"open_positions_left_protected":open_n},0)
        return {"ok":True,"stage":"ROLLED_BACK","open_positions_left_protected":open_n,
                "open_position_policy":"KEEP_BROKER_SL_TP_AND_RECONCILE"}

    def cooldown(self,candidate_id):
        c=self.conn();d=c.execute("SELECT * FROM deployment_registry WHERE candidate_id=?",(candidate_id,)).fetchone()
        since=(datetime.now(timezone.utc)-timedelta(days=7)).isoformat()
        count=c.execute("SELECT COUNT(*) n FROM deployment_events WHERE candidate_id=? AND event_type='PROMOTION' AND ts>=?",(candidate_id,since)).fetchone()["n"];c.close()
        last=parse(d["last_promotion_ts"]) if d and d["last_promotion_ts"] else None
        hours=(datetime.now(timezone.utc)-last).total_seconds()/3600 if last else 999999
        return {"ok":hours>=self.promotion_cooldown_hours and count<self.max_promotions_7d,"hours":hours,"promotions_7d":count}

    def promote(self,candidate_id,source,risk_ok=True):
        if not source:return {"ok":False,"status":"APPROVAL_SOURCE_REQUIRED"}
        c=self.conn();d=c.execute("SELECT * FROM deployment_registry WHERE candidate_id=?",(candidate_id,)).fetchone();c.close()
        if not d or d["current_stage"] not in ("CANARY_LIVE","LIMITED_PRODUCTION"):return {"ok":False,"status":"INVALID_STAGE"}
        live=self.live_metrics(candidate_id);evalr=self.evaluate(candidate_id,auto=False);cd=self.cooldown(candidate_id)
        mintr=self.min_live_trades if d["current_stage"]=="CANARY_LIVE" else self.min_limited_trades
        nxt=next_allocation(float(d["allocation_fraction"]))
        maxinc=(nxt is None) or (nxt-float(d["allocation_fraction"])<=self.max_exposure_increase+1e-12)
        g=promotion_gate(live,mintr,self.min_live_days,self.min_live_regimes,cd["ok"],risk_ok and evalr["action"]=="HOLD_CURRENT_LEVEL",maxinc)
        new=d["current_stage"];alloc=float(d["allocation_fraction"])
        if g["action"]=="PROMOTE":
            if nxt is None:new="FULL_PRODUCTION_ELIGIBLE"  # allocation remains 50%; 100% requires later review
            else:new=stage_for_allocation(nxt);alloc=nxt
        c=self.conn();c.execute("""INSERT INTO deployment_promotion_gates(ts,candidate_id,from_stage,requested_allocation,proposed_allocation,gate_status,criteria_json,metrics_json,reason,approval_source)
                                  VALUES(?,?,?,?,?,?,?,?,?,?)""",
                               (now_iso(),candidate_id,d["current_stage"],d["allocation_fraction"],nxt,
                                "PASSED" if new!=d["current_stage"] else "HOLD",json.dumps({"cooldown":cd,"min_trades":mintr}),
                                json.dumps(live),"; ".join(g["reasons"]) if g["reasons"] else "passed",source))
        if new!=d["current_stage"]:
            c.execute("UPDATE deployment_registry SET previous_stage=current_stage,current_stage=?,allocation_fraction=?,eligible_allocation_fraction=?,stage_started_ts=?,last_promotion_ts=?,updated_ts=? WHERE candidate_id=?",
                      (new,alloc,1.0 if new=="FULL_PRODUCTION_ELIGIBLE" else alloc,now_iso(),now_iso(),now_iso(),candidate_id))
            c.execute("UPDATE candidate_registry SET current_state=?,auto_deploy=0,updated_ts=? WHERE candidate_id=?",(new,now_iso(),candidate_id))
        c.commit();c.close();self.event(candidate_id,"PROMOTION" if new!=d["current_stage"] else "HOLD",d["current_stage"],new,alloc,
                                       "promotion gate passed" if new!=d["current_stage"] else "; ".join(g["reasons"]),source)
        return {"ok":True,"action":"PROMOTION" if new!=d["current_stage"] else "HOLD","stage":new,"allocation_fraction":alloc,"gate":g}

    async def execute(self,client,dep,signal,units,approved_risk):
        d=3 if "JPY" in signal["instrument"] else 5
        signed=units if signal["signal"]=="BUY" else -units
        body={"order":{"instrument":signal["instrument"],"units":str(signed),"type":"MARKET","timeInForce":"FOK","positionFill":"DEFAULT",
                       "stopLossOnFill":{"price":f"{signal['stop']:.{d}f}","timeInForce":"GTC"},
                       "takeProfitOnFill":{"price":f"{signal.get('managed_target',signal['target']):.{d}f}","timeInForce":"GTC"}}}
        t0=datetime.now(timezone.utc);x=await self.req(client,"POST","/v3/accounts/{account}/orders",body=body)
        latency=(datetime.now(timezone.utc)-t0).total_seconds()
        fill=x.get("orderFillTransaction") or {};price=f(fill.get("price"),signal["entry"])
        tid=str((fill.get("tradeOpened") or {}).get("tradeID",""));oid=str(fill.get("id",""))
        pip=.01 if "JPY" in signal["instrument"] else .0001
        slip=(price-signal["entry"])/pip
        if signal["signal"]=="SELL":slip=-slip
        c=self.conn();c.execute("""INSERT INTO deployment_live_trades(candidate_id,candidate_version,strategy_id,signal_id,trade_id,order_id,stage,allocation_fraction,approved_risk_fraction,
          instrument,direction,units,expected_entry,fill_price,stop_loss,take_profit,slippage_pips,latency_seconds,market_regime,volatility_state,director_state,director_confidence,risk_multiplier,status,
          opened_ts,created_ts,updated_ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?,?)""",
          (dep["candidate_id"],dep["candidate_version"],dep["strategy_id"],signal.get("signal_id"),tid,oid,dep["current_stage"],dep["allocation_fraction"],approved_risk,
           signal["instrument"],"LONG" if signal["signal"]=="BUY" else "SHORT",abs(units),signal["entry"],price,signal["stop"],signal.get("managed_target",signal["target"]),
           slip,latency,signal.get("market_regime"),signal.get("volatility_state"),signal.get("director_state"),signal.get("director_confidence"),signal.get("risk_multiplier"),
           fill.get("time") or now_iso(),now_iso(),now_iso()))
        c.commit();c.close()
        if abs(slip)>self.max_slippage_pips:self.pause(dep["candidate_id"],f"slippage={slip:.2f}","EXECUTION_MONITOR")
        return {"candidate_id":dep["candidate_id"],"trade_id":tid,"units":units,"fill_price":price,"slippage_pips":slip,"latency_seconds":latency}

    async def reconcile(self,client):
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM deployment_live_trades WHERE status='OPEN' ORDER BY id LIMIT 50").fetchall()];c.close()
        closed=0;affected=set();errors=[]
        for r in rows:
            try:
                d=await self.req(client,"GET",f"/v3/accounts/{{account}}/trades/{r['trade_id']}");tr=d.get("trade") or {}
                if tr.get("state")!="CLOSED":continue
                close=f(tr.get("averageClosePrice"));risk=abs(r["expected_entry"]-r["stop_loss"])
                rr=None if close is None or risk<=0 else ((close-r["fill_price"])/risk if r["direction"]=="LONG" else (r["fill_price"]-close)/risk)
                c=self.conn();c.execute("UPDATE deployment_live_trades SET status='CLOSED',closed_ts=?,close_price=?,realized_pl=?,realized_r=?,exit_reason=?,updated_ts=? WHERE id=?",
                                      (tr.get("closeTime") or now_iso(),close,f(tr.get("realizedPL"),0),rr,"BROKER_CLOSED",now_iso(),r["id"]));c.commit();c.close()
                closed+=1;affected.add(r["candidate_id"])
            except Exception as e:
                errors.append({"trade_id":r["trade_id"],"error":str(e)})
                c=self.conn();c.execute("UPDATE deployment_live_trades SET operational_error=?,updated_ts=? WHERE id=?",(str(e),now_iso(),r["id"]));c.commit();c.close()
        for cid in affected:self.evaluate(cid,auto=True)
        return {"checked":len(rows),"closed":closed,"errors":errors}

    def dashboard(self):
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM deployment_registry ORDER BY updated_ts DESC").fetchall()]
        sw=[dict(x) for x in c.execute("SELECT * FROM deployment_kill_switches ORDER BY scope").fetchall()];c.close()
        for r in rows:
            r["live_metrics"]=self.live_metrics(r["candidate_id"]);r["cooldown"]=self.cooldown(r["candidate_id"])
        return {"deployments":rows,"kill_switches":sw,"live_enabled":self.live_enabled,"auto_promotion":False}

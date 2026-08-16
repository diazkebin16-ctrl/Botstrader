
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
import hashlib
import json
import math
import sqlite3
import statistics
import uuid

MODES=("SHADOW","PAPER","CANARY","LIMITED_EXECUTION","PRODUCTION_EXECUTION")
INTENT_STATES=("VALID","EXPIRING","EXPIRED","CANCELLED","COMPLETED","REJECTED")
EXECUTION_REGIMES=("NORMAL_EXECUTION","CAUTIOUS_EXECUTION","HIGH_VOLATILITY_EXECUTION",
                   "LOW_LIQUIDITY_EXECUTION","EXECUTION_SUSPENDED")

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def parse_ts(value: Any) -> Optional[datetime]:
    if value is None:return None
    if isinstance(value,datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        v=str(value).replace("Z","+00:00")
        d=datetime.fromisoformat(v)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:return None

def finite(value: Any, default: Optional[float]=None) -> Optional[float]:
    try:
        x=float(value)
        return x if math.isfinite(x) else default
    except Exception:return default

def clamp(x: float,lo: float,hi: float)->float:
    return max(lo,min(hi,x))

def canonical(x: Any)->str:
    return json.dumps(x,separators=(",",":"),sort_keys=True,default=str)

def side_adverse_slippage(side:str,expected:float,actual:float)->float:
    """Positive means unfavorable, negative favorable."""
    return (actual-expected) if str(side).upper()=="BUY" else (expected-actual)

def slippage_metrics(side:str,expected:float,actual:float,quantity:float)->Dict[str,float]:
    adverse=side_adverse_slippage(side,expected,actual)
    bps=(adverse/expected*10000.0) if expected else 0.0
    return {
        "slippage_absolute":adverse,
        "slippage_bps":bps,
        "slippage_cost":adverse*abs(quantity),
        "favorable":bool(adverse<0)
    }

@dataclass
class ExecutionIntent:
    execution_intent_id:str
    trade_id:Optional[str]
    decision_id:Optional[str]
    risk_decision_id:Optional[str]
    strategy_id:str
    symbol:str
    side:str
    target_quantity:float
    maximum_quantity:float
    risk_approved_quantity:float
    urgency:str
    expected_price:float
    maximum_slippage_bps:float
    time_limit_seconds:int
    signal_time:str
    created_at:str
    expires_at:str
    risk_approval_valid_until:Optional[str]
    mode:str="SHADOW"

class SmartExecutionEngine:
    """
    Step 16 Smart Execution Engine.

    Critical boundary:
      * It does not generate BUY/SELL.
      * It cannot increase risk-approved quantity.
      * In SHADOW it never changes the broker order. It only records what it
        would have done.
      * It reuses Recovery Manager for real idempotency/order state/reconciliation.
    """
    def __init__(self,db_path:str,version:str="3.24",mode:str="SHADOW",
                 min_history_samples:int=20,max_snapshot_age_seconds:int=5,
                 default_intent_ttl_seconds:int=60,liquidity_participation:float=.25,
                 slice_threshold_units:float=1000,slice_size_units:float=200,
                 wide_spread_multiplier:float=1.8,abnormal_spread_multiplier:float=3.0,
                 latency_warning_ms:float=1200,degradation_min_samples:int=10):
        self.db_path=db_path;self.version=version
        self.mode=mode if mode in MODES else "SHADOW"
        self.min_history_samples=max(5,int(min_history_samples))
        self.max_snapshot_age_seconds=max(1,int(max_snapshot_age_seconds))
        self.default_intent_ttl_seconds=max(5,int(default_intent_ttl_seconds))
        self.liquidity_participation=clamp(float(liquidity_participation),.01,1.0)
        self.slice_threshold_units=max(1.0,float(slice_threshold_units))
        self.slice_size_units=max(1.0,float(slice_size_units))
        self.wide_spread_multiplier=max(1.05,float(wide_spread_multiplier))
        self.abnormal_spread_multiplier=max(self.wide_spread_multiplier+0.1,float(abnormal_spread_multiplier))
        self.latency_warning_ms=max(1.0,float(latency_warning_ms))
        self.degradation_min_samples=max(5,int(degradation_min_samples))

    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30)
        c.row_factory=sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA synchronous=FULL")
        c.execute("PRAGMA busy_timeout=5000")
        return c

    def ensure_schema(self):
        c=self.conn();c.executescript("""
        CREATE TABLE IF NOT EXISTS smart_execution_intents(
          execution_intent_id TEXT PRIMARY KEY,trade_id TEXT,decision_id TEXT,risk_decision_id TEXT,
          strategy_id TEXT NOT NULL,symbol TEXT NOT NULL,side TEXT NOT NULL,target_quantity REAL NOT NULL,
          maximum_quantity REAL NOT NULL,risk_approved_quantity REAL NOT NULL,urgency TEXT NOT NULL,
          expected_price REAL NOT NULL,maximum_slippage_bps REAL NOT NULL,time_limit_seconds INTEGER NOT NULL,
          signal_time TEXT NOT NULL,created_at TEXT NOT NULL,expires_at TEXT NOT NULL,
          risk_approval_valid_until TEXT,mode TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'VALID',
          execution_regime TEXT,execution_confidence REAL,actual_policy TEXT,shadow_policy TEXT,
          actual_requested_quantity REAL,shadow_requested_quantity REAL,filled_quantity REAL NOT NULL DEFAULT 0,
          remaining_quantity REAL,market_snapshot_id TEXT,last_reason TEXT,last_revalidated_at TEXT,
          policy_version TEXT,engine_version TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS smart_execution_market_snapshots(
          snapshot_id TEXT PRIMARY KEY,execution_intent_id TEXT,symbol TEXT NOT NULL,ts TEXT NOT NULL,
          bid REAL,ask REAL,spread REAL,spread_bps REAL,mid REAL,last_price REAL,
          available_liquidity REAL,recent_volume REAL,volatility TEXT,market_regime TEXT,
          broker_health TEXT,broker_latency_ms REAL,data_age_seconds REAL,market_status TEXT,
          order_book_json TEXT NOT NULL DEFAULT '{}',metadata_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS smart_execution_decisions(
          decision_record_id TEXT PRIMARY KEY,execution_intent_id TEXT NOT NULL,ts TEXT NOT NULL,mode TEXT NOT NULL,
          action TEXT NOT NULL,order_type TEXT,execution_method TEXT,recommended_quantity REAL,
          slice_plan_json TEXT NOT NULL DEFAULT '[]',limit_price REAL,fill_probability REAL,
          expected_slippage_bps REAL,allowed_slippage_bps REAL,estimated_market_impact_bps REAL,
          execution_cost_budget REAL,expected_execution_cost REAL,expected_gross_edge REAL,expected_net_edge REAL,
          execution_regime TEXT,execution_confidence REAL,spread_state TEXT,liquidity_state TEXT,
          reasons_json TEXT NOT NULL DEFAULT '[]',inputs_json TEXT NOT NULL DEFAULT '{}',
          hypothetical_only INTEGER NOT NULL DEFAULT 1,policy_version TEXT,engine_version TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS smart_execution_fills(
          fill_record_id TEXT PRIMARY KEY,execution_intent_id TEXT NOT NULL,ts TEXT NOT NULL,broker_order_id TEXT,
          broker_fill_id TEXT,fill_quantity REAL NOT NULL,fill_price REAL NOT NULL,expected_price REAL NOT NULL,
          order_type TEXT,broker_ack_latency_ms REAL,first_fill_latency_ms REAL,fees REAL NOT NULL DEFAULT 0,
          rejected INTEGER NOT NULL DEFAULT 0,partial_fill INTEGER NOT NULL DEFAULT 0,
          duplicate_event INTEGER NOT NULL DEFAULT 0,broker_event_id TEXT,
          UNIQUE(execution_intent_id,broker_event_id));
        CREATE TABLE IF NOT EXISTS smart_execution_tca(
          tca_id TEXT PRIMARY KEY,execution_intent_id TEXT NOT NULL,ts TEXT NOT NULL,strategy_id TEXT,symbol TEXT,
          side TEXT,order_type TEXT,session TEXT,market_regime TEXT,volatility TEXT,liquidity_state TEXT,
          expected_price REAL,actual_fill_price REAL,filled_quantity REAL,fill_rate REAL,
          slippage_absolute REAL,slippage_bps REAL,slippage_cost REAL,spread_cost REAL,fees REAL,
          estimated_market_impact REAL,delay_cost REAL,total_execution_cost REAL,
          expected_gross_edge REAL,expected_net_edge REAL,execution_quality_score REAL,
          entry_execution_score REAL,exit_execution_score REAL,stop_slippage_bps REAL,
          adverse_selection_bps REAL,attribution TEXT,metadata_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS smart_execution_shadow_comparisons(
          comparison_id TEXT PRIMARY KEY,execution_intent_id TEXT NOT NULL,ts TEXT NOT NULL,
          actual_order_type TEXT,shadow_order_type TEXT,actual_quantity REAL,shadow_quantity REAL,
          actual_slippage_bps REAL,shadow_expected_slippage_bps REAL,shadow_fill_probability REAL,
          actual_cost REAL,shadow_expected_cost REAL,actual_fill_rate REAL,
          hypothetical_fill_not_assumed INTEGER NOT NULL DEFAULT 1,outcome TEXT,details_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS smart_execution_policy_candidates(
          candidate_id TEXT PRIMARY KEY,created_at TEXT NOT NULL,parent_policy TEXT NOT NULL,
          candidate_policy_json TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'RESEARCH_ONLY',
          evidence_json TEXT NOT NULL DEFAULT '{}',validation_state TEXT NOT NULL DEFAULT 'PENDING',
          auto_deploy INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS smart_execution_alerts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,event_type TEXT NOT NULL,severity TEXT NOT NULL,
          execution_intent_id TEXT,symbol TEXT,message TEXT,details_json TEXT NOT NULL DEFAULT '{}');
        CREATE INDEX IF NOT EXISTS idx_se_intent_created ON smart_execution_intents(created_at);
        CREATE INDEX IF NOT EXISTS idx_se_snapshot_symbol_ts ON smart_execution_market_snapshots(symbol,ts);
        CREATE INDEX IF NOT EXISTS idx_se_tca_symbol_ts ON smart_execution_tca(symbol,ts);
        """)
        c.commit();c.close()

    def set_mode(self,mode:str):
        if mode not in MODES:raise ValueError("INVALID_SMART_EXECUTION_MODE")
        self.mode=mode

    def _alert(self,event_type:str,severity:str,intent_id:Optional[str],symbol:Optional[str],message:str,details:Dict[str,Any]):
        c=self.conn();c.execute("""INSERT INTO smart_execution_alerts(
          ts,event_type,severity,execution_intent_id,symbol,message,details_json) VALUES(?,?,?,?,?,?,?)""",
          (now_iso(),event_type,severity,intent_id,symbol,message,canonical(details)));c.commit();c.close()

    def create_intent(self,*,strategy_id:str,symbol:str,side:str,target_quantity:float,
                      maximum_quantity:float,risk_approved_quantity:float,expected_price:float,
                      urgency:str="NORMAL",maximum_slippage_bps:float=5.0,time_limit_seconds:Optional[int]=None,
                      signal_time:Optional[str]=None,trade_id:Optional[str]=None,decision_id:Optional[str]=None,
                      risk_decision_id:Optional[str]=None,risk_approval_valid:bool=True,
                      risk_approval_valid_until:Optional[str]=None,emergency_stop:bool=False,
                      policy_version:str="execution_policy_v1")->Dict[str,Any]:
        side=str(side).upper()
        if side not in ("BUY","SELL"):raise ValueError("SMART_EXECUTION_CANNOT_GENERATE_OR_CHANGE_DIRECTION")
        target=abs(float(target_quantity));maximum=abs(float(maximum_quantity));approved=abs(float(risk_approved_quantity))
        if not risk_approval_valid or approved<=0:
            raise PermissionError("NO_EXECUTION_WITHOUT_VALID_RISK_APPROVAL")
        if emergency_stop:
            raise PermissionError("EMERGENCY_STOP_NO_NEW_ENTRY")
        if target<=0 or maximum<=0:raise ValueError("INVALID_EXECUTION_QUANTITY")
        # Safety clamp: execution may reduce, never increase.
        target=min(target,maximum,approved)
        ttl=max(5,int(time_limit_seconds or self.default_intent_ttl_seconds))
        created=datetime.now(timezone.utc);signal=parse_ts(signal_time) or created
        expires=created+timedelta(seconds=ttl)
        eid="se_"+uuid.uuid4().hex
        obj=ExecutionIntent(eid,trade_id,decision_id,risk_decision_id,strategy_id,symbol,side,target,
                            min(maximum,approved),approved,str(urgency).upper(),float(expected_price),
                            max(0.0,float(maximum_slippage_bps)),ttl,signal.isoformat(),created.isoformat(),
                            expires.isoformat(),risk_approval_valid_until,self.mode)
        c=self.conn();c.execute("""INSERT INTO smart_execution_intents(
          execution_intent_id,trade_id,decision_id,risk_decision_id,strategy_id,symbol,side,target_quantity,
          maximum_quantity,risk_approved_quantity,urgency,expected_price,maximum_slippage_bps,time_limit_seconds,
          signal_time,created_at,expires_at,risk_approval_valid_until,mode,status,remaining_quantity,
          policy_version,engine_version,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (obj.execution_intent_id,obj.trade_id,obj.decision_id,obj.risk_decision_id,obj.strategy_id,obj.symbol,
           obj.side,obj.target_quantity,obj.maximum_quantity,obj.risk_approved_quantity,obj.urgency,obj.expected_price,
           obj.maximum_slippage_bps,obj.time_limit_seconds,obj.signal_time,obj.created_at,obj.expires_at,
           obj.risk_approval_valid_until,obj.mode,"VALID",obj.target_quantity,policy_version,self.version,now_iso()))
        c.commit();c.close()
        return asdict(obj)

    def intent(self,eid:str)->Optional[Dict[str,Any]]:
        c=self.conn();r=c.execute("SELECT * FROM smart_execution_intents WHERE execution_intent_id=?",(eid,)).fetchone();c.close()
        return dict(r) if r else None

    def link_trade(self,eid:str,trade_id:str):
        c=self.conn();c.execute("UPDATE smart_execution_intents SET trade_id=?,updated_at=? WHERE execution_intent_id=?",
                                (str(trade_id),now_iso(),eid));c.commit();c.close()
        return self.intent(eid)

    def intent_state(self,eid:str,at:Optional[datetime]=None)->str:
        x=self.intent(eid)
        if not x:return "UNKNOWN"
        if x["status"] in ("CANCELLED","COMPLETED","REJECTED"):return x["status"]
        at=at or datetime.now(timezone.utc);exp=parse_ts(x["expires_at"])
        remaining=(exp-at).total_seconds() if exp else -1
        if remaining<=0:return "EXPIRED"
        if remaining<=max(2,x["time_limit_seconds"]*.2):return "EXPIRING"
        return "VALID"

    def capture_snapshot(self,eid:str,*,bid:Optional[float],ask:Optional[float],last_price:Optional[float],
                         available_liquidity:Optional[float],recent_volume:Optional[float],volatility:str,
                         market_regime:str,timestamp:Optional[str]=None,broker_health:str="OK",
                         broker_latency_ms:Optional[float]=None,market_status:str="tradeable",
                         order_book:Optional[Dict[str,Any]]=None,metadata:Optional[Dict[str,Any]]=None)->Dict[str,Any]:
        x=self.intent(eid)
        if not x:raise KeyError("UNKNOWN_EXECUTION_INTENT")
        ts=parse_ts(timestamp) or datetime.now(timezone.utc)
        age=max(0.0,(datetime.now(timezone.utc)-ts).total_seconds())
        b=finite(bid);a=finite(ask);last=finite(last_price)
        mid=((b+a)/2.0) if b is not None and a is not None else last
        spread=(a-b) if b is not None and a is not None else None
        spread_bps=(spread/mid*10000.0) if spread is not None and mid else None
        sid="snap_"+uuid.uuid4().hex
        rec={"snapshot_id":sid,"execution_intent_id":eid,"symbol":x["symbol"],"ts":ts.isoformat(),"bid":b,"ask":a,
             "spread":spread,"spread_bps":spread_bps,"mid":mid,"last_price":last,
             "available_liquidity":finite(available_liquidity),"recent_volume":finite(recent_volume),
             "volatility":str(volatility or "UNKNOWN").upper(),"market_regime":str(market_regime or "UNKNOWN").upper(),
             "broker_health":str(broker_health or "UNKNOWN").upper(),"broker_latency_ms":finite(broker_latency_ms),
             "data_age_seconds":age,"market_status":str(market_status or "UNKNOWN").lower(),
             "order_book_json":canonical(order_book or {}),"metadata_json":canonical(metadata or {})}
        c=self.conn();c.execute("""INSERT INTO smart_execution_market_snapshots(
          snapshot_id,execution_intent_id,symbol,ts,bid,ask,spread,spread_bps,mid,last_price,available_liquidity,
          recent_volume,volatility,market_regime,broker_health,broker_latency_ms,data_age_seconds,market_status,
          order_book_json,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",tuple(rec.values()))
        c.execute("UPDATE smart_execution_intents SET market_snapshot_id=?,updated_at=? WHERE execution_intent_id=?",
                  (sid,now_iso(),eid));c.commit();c.close()
        return {**rec,"order_book":order_book or {},"metadata":metadata or {}}

    def _historical_tca(self,symbol:str,order_type:Optional[str],before_ts:str)->List[Dict[str,Any]]:
        c=self.conn()
        if order_type:
            rows=c.execute("""SELECT * FROM smart_execution_tca WHERE symbol=? AND order_type=? AND ts<?
                              ORDER BY ts DESC LIMIT 500""",(symbol,order_type,before_ts)).fetchall()
        else:
            rows=c.execute("""SELECT * FROM smart_execution_tca WHERE symbol=? AND ts<?
                              ORDER BY ts DESC LIMIT 500""",(symbol,before_ts)).fetchall()
        c.close();return [dict(x) for x in rows]

    def expected_slippage(self,eid:str,snapshot:Dict[str,Any],order_type:str="MARKET")->Dict[str,Any]:
        x=self.intent(eid);before=x["created_at"]
        hist=self._historical_tca(x["symbol"],order_type,before)
        vals=[finite(r.get("slippage_bps")) for r in hist]
        vals=[max(0.0,v) for v in vals if v is not None]
        spread=max(0.0,finite(snapshot.get("spread_bps"),0.0) or 0.0)
        qty=x["target_quantity"];liq=finite(snapshot.get("available_liquidity"))
        participation=(qty/max(liq,qty)) if liq is not None and liq>0 else 0.25
        vol=str(snapshot.get("volatility") or "").upper()
        vol_mult=2.0 if "EXTREME" in vol else 1.5 if "HIGH" in vol else .75 if "LOW" in vol else 1.0
        latency=finite(snapshot.get("broker_latency_ms"),0.0) or 0.0
        latency_component=min(3.0,latency/2000.0)
        impact_component=max(0.0,participation-self.liquidity_participation)*8.0
        heuristic=max(.05,spread*.50)*vol_mult+latency_component+impact_component
        if len(vals)>=self.min_history_samples:
            med=statistics.median(vals)
            p75=sorted(vals)[min(len(vals)-1,int(.75*(len(vals)-1)))]
            estimate=.6*med+.4*p75
            source="HISTORICAL_EXPLAINABLE"
        else:
            estimate=heuristic;source="HEURISTIC_INSUFFICIENT_HISTORY"
        if order_type=="LIMIT":
            estimate*=.35
        return {"expected_slippage_bps":max(0.0,estimate),"source":source,"samples":len(vals),
                "components":{"spread_bps":spread,"participation":participation,"volatility_multiplier":vol_mult,
                              "latency_component_bps":latency_component,"impact_component_bps":impact_component}}

    def spread_state(self,symbol:str,snapshot:Dict[str,Any],before_ts:str)->Dict[str,Any]:
        current=max(0.0,finite(snapshot.get("spread_bps"),0.0) or 0.0)
        c=self.conn();rows=c.execute("""SELECT spread_bps FROM smart_execution_market_snapshots
                                      WHERE symbol=? AND ts<? AND spread_bps IS NOT NULL ORDER BY ts DESC LIMIT 200""",
                                    (symbol,before_ts)).fetchall();c.close()
        hist=[float(x["spread_bps"]) for x in rows if x["spread_bps"] is not None and x["spread_bps"]>=0]
        baseline=statistics.median(hist) if len(hist)>=10 else max(current,0.1)
        ratio=current/max(baseline,1e-9)
        state="ABNORMAL_SPREAD" if ratio>=self.abnormal_spread_multiplier else "WIDE_SPREAD" if ratio>=self.wide_spread_multiplier else "NORMAL_SPREAD"
        return {"state":state,"current_bps":current,"baseline_bps":baseline,"ratio":ratio,"samples":len(hist)}

    def estimate_market_impact_bps(self,quantity:float,snapshot:Dict[str,Any])->float:
        liq=finite(snapshot.get("available_liquidity"))
        vol=finite(snapshot.get("recent_volume"))
        q=abs(float(quantity))
        if liq and liq>0:
            p=q/liq
        elif vol and vol>0:
            p=q/vol
        else:
            return 0.5
        return max(0.0,(p**1.5)*10.0)

    def estimate_fill_probability(self,order_type:str,urgency:str,snapshot:Dict[str,Any],limit_price:Optional[float]=None)->float:
        if order_type=="MARKET":return .995 if str(snapshot.get("market_status"))=="tradeable" else .0
        spread=max(0.0,finite(snapshot.get("spread_bps"),0.0) or 0.0)
        vol=str(snapshot.get("volatility") or "").upper()
        p=.78
        p-=min(.25,spread/20.0)
        if "HIGH" in vol or "EXTREME" in vol:p-=.15
        if str(urgency).upper()=="HIGH":p+=.05
        liq=finite(snapshot.get("available_liquidity"))
        if liq is not None and liq<=0:p=0.0
        return clamp(p,.05,.95)

    def _execution_regime(self,x:Dict[str,Any],snapshot:Dict[str,Any],spread:Dict[str,Any],quantity:float)->str:
        if (finite(snapshot.get("data_age_seconds"),999) or 999)>self.max_snapshot_age_seconds:
            return "EXECUTION_SUSPENDED"
        if str(snapshot.get("broker_health")) not in ("OK","HEALTHY"):return "EXECUTION_SUSPENDED"
        if str(snapshot.get("market_status")) not in ("tradeable","open","active"):return "EXECUTION_SUSPENDED"
        vol=str(snapshot.get("volatility") or "").upper()
        if "HIGH" in vol or "EXTREME" in vol:return "HIGH_VOLATILITY_EXECUTION"
        liq=finite(snapshot.get("available_liquidity"))
        if liq is not None and liq<quantity:return "LOW_LIQUIDITY_EXECUTION"
        if spread["state"] in ("WIDE_SPREAD","ABNORMAL_SPREAD"):return "CAUTIOUS_EXECUTION"
        return "NORMAL_EXECUTION"

    def _execution_confidence(self,snapshot:Dict[str,Any],spread_state:str,expected_slip:float,allowed_slip:float,
                              quantity:float)->float:
        score=1.0
        age=finite(snapshot.get("data_age_seconds"),999) or 999
        score-=clamp(age/max(self.max_snapshot_age_seconds,1),0,2)*.20
        if spread_state=="WIDE_SPREAD":score-=.15
        if spread_state=="ABNORMAL_SPREAD":score-=.35
        liq=finite(snapshot.get("available_liquidity"))
        if liq is not None and liq<quantity:score-=.25
        if str(snapshot.get("broker_health")) not in ("OK","HEALTHY"):score-=.35
        lat=finite(snapshot.get("broker_latency_ms"),0) or 0
        if lat>self.latency_warning_ms:score-=min(.25,(lat/self.latency_warning_ms-1)*.15)
        if allowed_slip>0:score-=min(.35,max(0,expected_slip/allowed_slip-1)*.25)
        vol=str(snapshot.get("volatility") or "").upper()
        if "HIGH" in vol:score-=.12
        if "EXTREME" in vol:score-=.25
        return clamp(score,0.0,1.0)

    def _liquidity_quantity(self,x:Dict[str,Any],snapshot:Dict[str,Any])->float:
        base=min(float(x["target_quantity"]),float(x["maximum_quantity"]),float(x["risk_approved_quantity"]))
        liq=finite(snapshot.get("available_liquidity"))
        if liq is None or liq<=0:return base
        # If a broker explicitly reports immediately available liquidity below the
        # authorized size, never request more than that visible amount.
        return min(base,abs(liq))

    def _slice_plan(self,quantity:float,snapshot:Dict[str,Any])->List[float]:
        if quantity<self.slice_threshold_units:return [quantity]
        liq=finite(snapshot.get("available_liquidity"))
        slice_size=min(self.slice_size_units,quantity)
        if liq and liq>0:slice_size=min(slice_size,max(1.0,liq*self.liquidity_participation))
        out=[];left=quantity
        while left>1e-12:
            q=min(left,slice_size);out.append(q);left-=q
        return out

    def recommend(self,eid:str,snapshot:Dict[str,Any],*,risk_approval_valid:bool=True,strategy_intent_valid:bool=True,
                  position_state_valid:bool=True,emergency_stop:bool=False,expected_gross_edge:Optional[float]=None,
                  execution_cost_budget:Optional[float]=None,actual_order_type:str="MARKET",
                  actual_requested_quantity:Optional[float]=None)->Dict[str,Any]:
        x=self.intent(eid)
        if not x:raise KeyError("UNKNOWN_EXECUTION_INTENT")
        state=self.intent_state(eid)
        reasons=[]
        q=self._liquidity_quantity(x,snapshot)
        q=min(q,float(x["risk_approved_quantity"]))
        stale=(finite(snapshot.get("data_age_seconds"),999) or 999)>self.max_snapshot_age_seconds
        risk_expiry=parse_ts(x.get("risk_approval_valid_until"))
        if risk_expiry and datetime.now(timezone.utc)>=risk_expiry:risk_approval_valid=False
        signal_age=(datetime.now(timezone.utc)-(parse_ts(x["signal_time"]) or datetime.now(timezone.utc))).total_seconds()
        if state=="EXPIRED" or signal_age>x["time_limit_seconds"]:
            action="CANCEL_REMAINING_EXECUTION";reasons.append("STALE_EXECUTION_INTENT")
        elif emergency_stop:
            action="REJECT_EXECUTION";reasons.append("EMERGENCY_STOP")
        elif not risk_approval_valid:
            action="REJECT_EXECUTION";reasons.append("RISK_APPROVAL_INVALID_OR_EXPIRED")
        elif not strategy_intent_valid or not position_state_valid:
            action="REJECT_EXECUTION";reasons.append("INTENT_OR_POSITION_REVALIDATION_FAILED")
        elif stale:
            action="REJECT_EXECUTION";reasons.append("STALE_CRITICAL_DATA")
        else:
            action="EXECUTE"

        sp=self.spread_state(x["symbol"],snapshot,x["created_at"])
        slip_market=self.expected_slippage(eid,snapshot,"MARKET")
        slip_limit=self.expected_slippage(eid,snapshot,"LIMIT")
        allowed=float(x["maximum_slippage_bps"])
        regime=self._execution_regime(x,snapshot,sp,q)
        if regime=="EXECUTION_SUSPENDED":
            action="REJECT_EXECUTION";reasons.append("EXECUTION_SUSPENDED")

        liq=finite(snapshot.get("available_liquidity"))
        if liq is not None and liq<float(x["target_quantity"]):
            reasons.append("LOW_LIQUIDITY_REDUCE_SIZE")
        if q<=0:
            action="REJECT_EXECUTION";reasons.append("NO_LIQUIDITY")

        urgency=str(x["urgency"]).upper()
        # Explicit Market-vs-Limit selection.
        if urgency=="HIGH" and sp["state"]=="NORMAL_SPREAD" and slip_market["expected_slippage_bps"]<=allowed:
            order_type="MARKET";method="IMMEDIATE_EXECUTION";expected_slip=slip_market["expected_slippage_bps"];limit_price=None
        else:
            order_type="LIMIT";method="PASSIVE_LIMIT_EXECUTION";expected_slip=slip_limit["expected_slippage_bps"]
            mid=finite(snapshot.get("mid"),x["expected_price"]) or x["expected_price"]
            bid=finite(snapshot.get("bid"),mid);ask=finite(snapshot.get("ask"),mid)
            limit_price=(bid if x["side"]=="BUY" else ask) if bid is not None and ask is not None else mid

        if q>=self.slice_threshold_units:
            method="LIQUIDITY_AWARE_SLICED_EXECUTION"
        slices=self._slice_plan(q,snapshot)
        fill_prob=self.estimate_fill_probability(order_type,urgency,snapshot,limit_price)
        impact=self.estimate_market_impact_bps(q,snapshot)
        spread_cost_bps=max(0.0,finite(snapshot.get("spread_bps"),0.0) or 0.0)/2.0
        expected_cost_bps=expected_slip+spread_cost_bps+impact
        expected_cost=expected_cost_bps/10000.0*abs(q)*float(x["expected_price"])
        gross=finite(expected_gross_edge)
        net=(gross-expected_cost) if gross is not None else None

        if action=="EXECUTE" and expected_slip>allowed:
            if urgency=="LOW":
                action="DELAY";reasons.append("EXPECTED_SLIPPAGE_EXCEEDS_ALLOWED")
            elif q<float(x["target_quantity"]) and order_type=="LIMIT":
                action="REDUCE_SIZE";reasons.append("SLIPPAGE_GUARD_REDUCED_SIZE")
            else:
                action="REJECT_EXECUTION";reasons.append("EXPECTED_SLIPPAGE_EXCEEDS_ALLOWED")
        budget=finite(execution_cost_budget)
        if action in ("EXECUTE","REDUCE_SIZE","DELAY") and budget is not None and expected_cost>budget:
            action="REJECT_EXECUTION";reasons.append("EXECUTION_COST_TOO_HIGH")
        if gross is not None and net is not None and net<=0:
            action="REJECT_EXECUTION";reasons.append("NO_ECONOMIC_EDGE_AFTER_COSTS")

        conf=self._execution_confidence(snapshot,sp["state"],expected_slip,allowed,q)
        if conf<.25 and action in ("EXECUTE","REDUCE_SIZE"):
            action="REJECT_EXECUTION";reasons.append("EXECUTION_CONFIDENCE_TOO_LOW")
        elif conf<.50 and action=="EXECUTE":
            action="REDUCE_SIZE";q=min(q,float(x["target_quantity"])*.5);slices=self._slice_plan(q,snapshot)
            reasons.append("LOW_EXECUTION_CONFIDENCE_REDUCE_ONLY")

        # Absolute safety invariant.
        q=min(abs(q),float(x["risk_approved_quantity"]),float(x["maximum_quantity"]))
        if sum(slices)>q+1e-9:slices=self._slice_plan(q,snapshot)
        if q>float(x["risk_approved_quantity"])+1e-12:
            raise AssertionError("EXECUTED_QUANTITY_CAN_NEVER_EXCEED_RISK_APPROVAL")
        rec={
            "action":action,"order_type":order_type,"execution_method":method,"recommended_quantity":q,
            "slice_plan":slices,"limit_price":limit_price,"fill_probability":fill_prob,
            "expected_slippage_bps":expected_slip,"allowed_slippage_bps":allowed,
            "estimated_market_impact_bps":impact,"execution_cost_budget":budget,
            "expected_execution_cost":expected_cost,"expected_gross_edge":gross,"expected_net_edge":net,
            "execution_regime":regime,"execution_confidence":conf,"spread_state":sp["state"],
            "liquidity_state":"LOW_LIQUIDITY" if liq is not None and liq<float(x["target_quantity"]) else "NORMAL_LIQUIDITY",
            "reasons":reasons or ["EXECUTION_CONDITIONS_ACCEPTABLE"],"hypothetical_only":self.mode=="SHADOW",
            "risk_approved_quantity":float(x["risk_approved_quantity"]),"target_quantity":float(x["target_quantity"])
        }
        did="sed_"+uuid.uuid4().hex
        c=self.conn();c.execute("""INSERT INTO smart_execution_decisions(
          decision_record_id,execution_intent_id,ts,mode,action,order_type,execution_method,recommended_quantity,
          slice_plan_json,limit_price,fill_probability,expected_slippage_bps,allowed_slippage_bps,
          estimated_market_impact_bps,execution_cost_budget,expected_execution_cost,expected_gross_edge,
          expected_net_edge,execution_regime,execution_confidence,spread_state,liquidity_state,reasons_json,
          inputs_json,hypothetical_only,policy_version,engine_version)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (did,eid,now_iso(),self.mode,action,order_type,method,q,canonical(slices),limit_price,fill_prob,
           expected_slip,allowed,impact,budget,expected_cost,gross,net,regime,conf,sp["state"],rec["liquidity_state"],
           canonical(rec["reasons"]),canonical({"snapshot_id":snapshot.get("snapshot_id"),"risk_approval_valid":risk_approval_valid,
           "strategy_intent_valid":strategy_intent_valid,"position_state_valid":position_state_valid,
           "emergency_stop":emergency_stop}),int(self.mode=="SHADOW"),x.get("policy_version"),self.version))
        c.execute("""UPDATE smart_execution_intents SET execution_regime=?,execution_confidence=?,
          actual_policy=?,shadow_policy=?,actual_requested_quantity=?,shadow_requested_quantity=?,last_reason=?,
          last_revalidated_at=?,updated_at=? WHERE execution_intent_id=?""",
          (regime,conf,actual_order_type,order_type,actual_requested_quantity,q,"; ".join(rec["reasons"]),
           now_iso(),now_iso(),eid));c.commit();c.close()
        for event,condition,severity in (
            ("SLIPPAGE_HIGH",expected_slip>allowed,"HIGH"),
            ("SPREAD_ABNORMAL",sp["state"]=="ABNORMAL_SPREAD","HIGH"),
            ("LOW_LIQUIDITY",rec["liquidity_state"]=="LOW_LIQUIDITY","WARNING"),
            ("BROKER_LATENCY_DEGRADED",(finite(snapshot.get("broker_latency_ms"),0) or 0)>self.latency_warning_ms,"WARNING"),
            ("EXECUTION_COST_TOO_HIGH","EXECUTION_COST_TOO_HIGH" in reasons,"HIGH"),
            ("EXECUTION_INTENT_EXPIRED","STALE_EXECUTION_INTENT" in reasons,"HIGH"),
        ):
            if condition:self._alert(event,severity,eid,x["symbol"],event,rec)
        return {"decision_record_id":did,**rec}

    def record_fill(self,eid:str,*,fill_quantity:float,fill_price:float,broker_order_id:Optional[str]=None,
                    broker_fill_id:Optional[str]=None,broker_event_id:Optional[str]=None,order_type:str="MARKET",
                    broker_ack_latency_ms:Optional[float]=None,first_fill_latency_ms:Optional[float]=None,
                    fees:float=0.0,rejected:bool=False,expected_gross_edge:Optional[float]=None,
                    session:Optional[str]=None,adverse_selection_bps:Optional[float]=None,
                    stop_expected:Optional[float]=None,stop_trigger:Optional[float]=None,
                    stop_fill:Optional[float]=None)->Dict[str,Any]:
        x=self.intent(eid)
        if not x:raise KeyError("UNKNOWN_EXECUTION_INTENT")
        q=abs(float(fill_quantity))
        current=float(x.get("filled_quantity") or 0.0)
        approved=float(x["risk_approved_quantity"])
        if current+q>approved+1e-9:
            raise PermissionError("EXECUTED_QUANTITY_EXCEEDS_RISK_APPROVED_QUANTITY")
        c=self.conn()
        if broker_event_id:
            dup=c.execute("""SELECT 1 FROM smart_execution_fills WHERE execution_intent_id=? AND broker_event_id=?""",
                          (eid,broker_event_id)).fetchone()
            if dup:
                c.close();return {"duplicate_event":True,"execution_intent_id":eid,"filled_quantity":current}
        fill_id="sef_"+uuid.uuid4().hex
        partial=(current+q)<float(x["target_quantity"])-1e-9
        try:
            c.execute("""INSERT INTO smart_execution_fills(
              fill_record_id,execution_intent_id,ts,broker_order_id,broker_fill_id,fill_quantity,fill_price,
              expected_price,order_type,broker_ack_latency_ms,first_fill_latency_ms,fees,rejected,partial_fill,
              duplicate_event,broker_event_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (fill_id,eid,now_iso(),broker_order_id,broker_fill_id,q,float(fill_price),float(x["expected_price"]),
               order_type,broker_ack_latency_ms,first_fill_latency_ms,float(fees),int(rejected),int(partial),0,broker_event_id))
        except sqlite3.IntegrityError:
            c.rollback();c.close();return {"duplicate_event":True,"execution_intent_id":eid,"filled_quantity":current}
        new_filled=current+q
        remaining=max(0.0,float(x["target_quantity"])-new_filled)
        status="COMPLETED" if remaining<=1e-9 else "VALID"
        c.execute("""UPDATE smart_execution_intents SET filled_quantity=?,remaining_quantity=?,status=?,updated_at=?
                     WHERE execution_intent_id=?""",(new_filled,remaining,status,now_iso(),eid))
        c.commit();c.close()
        tca=self.compute_tca(eid,order_type=order_type,session=session,expected_gross_edge=expected_gross_edge,
                             adverse_selection_bps=adverse_selection_bps,stop_expected=stop_expected,
                             stop_trigger=stop_trigger,stop_fill=stop_fill)
        return {"duplicate_event":False,"fill_record_id":fill_id,"filled_quantity":new_filled,
                "remaining_quantity":remaining,"partial_fill":partial,"tca":tca}

    def compute_tca(self,eid:str,*,order_type:str,session:Optional[str]=None,
                    expected_gross_edge:Optional[float]=None,adverse_selection_bps:Optional[float]=None,
                    stop_expected:Optional[float]=None,stop_trigger:Optional[float]=None,
                    stop_fill:Optional[float]=None)->Dict[str,Any]:
        x=self.intent(eid)
        c=self.conn();fills=[dict(r) for r in c.execute("SELECT * FROM smart_execution_fills WHERE execution_intent_id=? ORDER BY ts",(eid,)).fetchall()]
        snap=c.execute("SELECT * FROM smart_execution_market_snapshots WHERE snapshot_id=?",(x.get("market_snapshot_id"),)).fetchone()
        dec=c.execute("SELECT * FROM smart_execution_decisions WHERE execution_intent_id=? ORDER BY ts DESC LIMIT 1",(eid,)).fetchone()
        c.close()
        if not fills:return {}
        total_q=sum(abs(float(r["fill_quantity"])) for r in fills)
        avg=sum(abs(float(r["fill_quantity"]))*float(r["fill_price"]) for r in fills)/max(total_q,1e-12)
        sm=slippage_metrics(x["side"],float(x["expected_price"]),avg,total_q)
        fill_rate=min(1.0,total_q/max(float(x["target_quantity"]),1e-12))
        spread_bps=finite(snap["spread_bps"],0.0) if snap else 0.0
        spread_cost=(spread_bps or 0.0)/20000.0*total_q*float(x["expected_price"])
        fees=sum(float(r["fees"] or 0) for r in fills)
        impact_bps=float(dec["estimated_market_impact_bps"] or 0) if dec else 0.0
        impact=impact_bps/10000.0*total_q*float(x["expected_price"])
        latency=[finite(r["broker_ack_latency_ms"]) for r in fills];latency=[z for z in latency if z is not None]
        latency_pen=min(25.0,(statistics.mean(latency)/self.latency_warning_ms*10.0) if latency else 0.0)
        slip_pen=min(35.0,max(0.0,sm["slippage_bps"])*4.0)
        fill_pen=(1-fill_rate)*25.0
        rejection_pen=20.0 if any(int(r["rejected"]) for r in fills) else 0.0
        spread_pen=min(15.0,max(0.0,spread_bps or 0.0)*2.0)
        fee_pen=min(10.0,abs(fees)/max(abs(expected_gross_edge or 0),1e-9)*10.0) if expected_gross_edge else min(10.0,fees)
        impact_pen=min(15.0,impact_bps*2)
        score=clamp(100.0-(slip_pen+fill_pen+latency_pen+rejection_pen+spread_pen+fee_pen+impact_pen),0,100)
        total_cost=max(0.0,sm["slippage_cost"])+spread_cost+fees+impact
        net=(expected_gross_edge-total_cost) if expected_gross_edge is not None else None
        stop_slip=None
        if stop_expected is not None and stop_fill is not None:
            stop_slip=abs(float(stop_fill)-float(stop_expected))/max(abs(float(stop_expected)),1e-12)*10000.0
        attribution="EXECUTION_LOSS" if expected_gross_edge is not None and expected_gross_edge>0 and net is not None and net<=0 else "MIXED_OR_STRATEGY"
        rec={"tca_id":"tca_"+uuid.uuid4().hex,"execution_intent_id":eid,"ts":now_iso(),"strategy_id":x["strategy_id"],
             "symbol":x["symbol"],"side":x["side"],"order_type":order_type,"session":session,
             "market_regime":snap["market_regime"] if snap else None,"volatility":snap["volatility"] if snap else None,
             "liquidity_state":dec["liquidity_state"] if dec else None,"expected_price":x["expected_price"],
             "actual_fill_price":avg,"filled_quantity":total_q,"fill_rate":fill_rate,
             "slippage_absolute":sm["slippage_absolute"],"slippage_bps":sm["slippage_bps"],
             "slippage_cost":sm["slippage_cost"],"spread_cost":spread_cost,"fees":fees,
             "estimated_market_impact":impact,"delay_cost":0.0,
             "total_execution_cost":total_cost,"expected_gross_edge":expected_gross_edge,"expected_net_edge":net,
             "execution_quality_score":score,"entry_execution_score":score,"exit_execution_score":None,
             "stop_slippage_bps":stop_slip,"adverse_selection_bps":adverse_selection_bps,
             "attribution":attribution,"metadata_json":canonical({"stop_trigger":stop_trigger})}
        c=self.conn();c.execute("""INSERT INTO smart_execution_tca(
          tca_id,execution_intent_id,ts,strategy_id,symbol,side,order_type,session,market_regime,volatility,
          liquidity_state,expected_price,actual_fill_price,filled_quantity,fill_rate,slippage_absolute,
          slippage_bps,slippage_cost,spread_cost,fees,estimated_market_impact,delay_cost,total_execution_cost,
          expected_gross_edge,expected_net_edge,execution_quality_score,entry_execution_score,exit_execution_score,
          stop_slippage_bps,adverse_selection_bps,attribution,metadata_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",tuple(rec.values()))
        c.commit();c.close()
        return {**rec,"metadata":json.loads(rec["metadata_json"])}

    def revalidate_remaining(self,eid:str,snapshot:Dict[str,Any],*,risk_approval_valid:bool,
                             strategy_intent_valid:bool,position_state_valid:bool,emergency_stop:bool=False,
                             expected_gross_edge:Optional[float]=None,execution_cost_budget:Optional[float]=None)->Dict[str,Any]:
        x=self.intent(eid)
        if not x:raise KeyError("UNKNOWN_EXECUTION_INTENT")
        remaining=max(0.0,float(x["target_quantity"])-float(x.get("filled_quantity") or 0))
        if remaining<=1e-9:return {"action":"NO_REMAINING_EXECUTION","remaining_quantity":0.0}
        # Never re-authorize more than original approval minus actual fills.
        remaining_cap=max(0.0,float(x["risk_approved_quantity"])-float(x.get("filled_quantity") or 0))
        c=self.conn();c.execute("UPDATE smart_execution_intents SET target_quantity=?,updated_at=? WHERE execution_intent_id=?",
                                (min(float(x["filled_quantity"])+remaining, float(x["risk_approved_quantity"])),now_iso(),eid));c.commit();c.close()
        rec=self.recommend(eid,snapshot,risk_approval_valid=risk_approval_valid,
                           strategy_intent_valid=strategy_intent_valid,position_state_valid=position_state_valid,
                           emergency_stop=emergency_stop,expected_gross_edge=expected_gross_edge,
                           execution_cost_budget=execution_cost_budget,actual_requested_quantity=remaining)
        rec["remaining_quantity"]=min(remaining,remaining_cap)
        if (rec.get("expected_slippage_bps",0)>rec.get("allowed_slippage_bps",float("inf"))) or (
            rec["action"] in ("REJECT_EXECUTION","DELAY") and (
            "STALE_EXECUTION_INTENT" in rec["reasons"] or
            "RISK_APPROVAL_INVALID_OR_EXPIRED" in rec["reasons"] or
            "STALE_CRITICAL_DATA" in rec["reasons"] or
            "EXPECTED_SLIPPAGE_EXCEEDS_ALLOWED" in rec["reasons"])):
            rec["action"]="CANCEL_REMAINING_EXECUTION"
            c=self.conn();c.execute("""UPDATE smart_execution_intents SET status='CANCELLED',
                                      last_reason=?,updated_at=? WHERE execution_intent_id=?""",
                                   ("; ".join(rec["reasons"]),now_iso(),eid));c.commit();c.close()
        return rec

    def shadow_compare(self,eid:str,*,actual_order_type:str,actual_quantity:float,
                       actual_slippage_bps:Optional[float],actual_cost:Optional[float],
                       actual_fill_rate:Optional[float])->Dict[str,Any]:
        c=self.conn();d=c.execute("""SELECT * FROM smart_execution_decisions WHERE execution_intent_id=?
                                    ORDER BY ts DESC LIMIT 1""",(eid,)).fetchone();c.close()
        if not d:return {}
        out={"comparison_id":"sec_"+uuid.uuid4().hex,"execution_intent_id":eid,"ts":now_iso(),
             "actual_order_type":actual_order_type,"shadow_order_type":d["order_type"],
             "actual_quantity":abs(float(actual_quantity)),"shadow_quantity":float(d["recommended_quantity"] or 0),
             "actual_slippage_bps":actual_slippage_bps,"shadow_expected_slippage_bps":d["expected_slippage_bps"],
             "shadow_fill_probability":d["fill_probability"],"actual_cost":actual_cost,
             "shadow_expected_cost":d["expected_execution_cost"],"actual_fill_rate":actual_fill_rate,
             "hypothetical_fill_not_assumed":1,
             "outcome":"OBSERVE_ONLY_NO_CAUSAL_CLAIM",
             "details_json":canonical({"note":"A hypothetical LIMIT fill is not assumed merely because the shadow policy recommended it."})}
        c=self.conn();c.execute("""INSERT INTO smart_execution_shadow_comparisons(
          comparison_id,execution_intent_id,ts,actual_order_type,shadow_order_type,actual_quantity,shadow_quantity,
          actual_slippage_bps,shadow_expected_slippage_bps,shadow_fill_probability,actual_cost,
          shadow_expected_cost,actual_fill_rate,hypothetical_fill_not_assumed,outcome,details_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",tuple(out.values()));c.commit();c.close()
        return out

    def degradation(self,days:int=7)->Dict[str,Any]:
        c=self.conn()
        now=datetime.now(timezone.utc);recent=(now-timedelta(days=days)).isoformat();hist=(now-timedelta(days=days*5)).isoformat()
        rr=[dict(x) for x in c.execute("SELECT * FROM smart_execution_tca WHERE ts>=?",(recent,)).fetchall()]
        hh=[dict(x) for x in c.execute("SELECT * FROM smart_execution_tca WHERE ts>=? AND ts<?",(hist,recent)).fetchall()]
        fills_recent=[dict(x) for x in c.execute("SELECT * FROM smart_execution_fills WHERE ts>=?",(recent,)).fetchall()]
        fills_hist=[dict(x) for x in c.execute("SELECT * FROM smart_execution_fills WHERE ts>=? AND ts<?",(hist,recent)).fetchall()]
        c.close()
        def m(rows):
            if not rows:return {"samples":0,"quality":None,"slippage":None,"fill_rate":None}
            return {"samples":len(rows),
                    "quality":statistics.mean([float(x["execution_quality_score"]) for x in rows if x["execution_quality_score"] is not None]) if any(x["execution_quality_score"] is not None for x in rows) else None,
                    "slippage":statistics.mean([float(x["slippage_bps"]) for x in rows if x["slippage_bps"] is not None]) if any(x["slippage_bps"] is not None for x in rows) else None,
                    "fill_rate":statistics.mean([float(x["fill_rate"]) for x in rows if x["fill_rate"] is not None]) if any(x["fill_rate"] is not None for x in rows) else None}
        a,b=m(rr),m(hh)
        def reject_rate(rows):return sum(int(x["rejected"]) for x in rows)/len(rows) if rows else None
        lat_recent=[finite(x.get("broker_ack_latency_ms")) for x in fills_recent];lat_recent=[x for x in lat_recent if x is not None]
        lat_hist=[finite(x.get("broker_ack_latency_ms")) for x in fills_hist];lat_hist=[x for x in lat_hist if x is not None]
        reasons=[]
        if a["samples"]>=self.degradation_min_samples and b["samples"]>=self.degradation_min_samples:
            if a["quality"] is not None and b["quality"] is not None and a["quality"]<b["quality"]-15:reasons.append("EXECUTION_QUALITY_SCORE_DOWN")
            if a["slippage"] is not None and b["slippage"] is not None and a["slippage"]>max(b["slippage"]*1.5,b["slippage"]+1):reasons.append("SLIPPAGE_UP")
            if a["fill_rate"] is not None and b["fill_rate"] is not None and a["fill_rate"]<b["fill_rate"]-.15:reasons.append("FILL_RATE_DOWN")
            if lat_recent and lat_hist and statistics.mean(lat_recent)>statistics.mean(lat_hist)*1.5:reasons.append("BROKER_LATENCY_DEGRADATION")
            rr=reject_rate(fills_recent);hr=reject_rate(fills_hist)
            if rr is not None and hr is not None and len(fills_recent)>=self.degradation_min_samples and rr>hr+.10:
                reasons.append("REJECTION_RATE_UP")
        return {"status":"EXECUTION_DEGRADATION_DETECTED" if reasons else "NORMAL",
                "reasons":reasons,"recent":a,"historical":b,
                "recent_rejection_rate":reject_rate(fills_recent),"historical_rejection_rate":reject_rate(fills_hist)}

    def latency_baseline(self,symbol:str,session:Optional[str]=None,order_type:Optional[str]=None)->Dict[str,Any]:
        c=self.conn()
        sql="""SELECT f.broker_ack_latency_ms,f.first_fill_latency_ms,t.session,t.order_type
               FROM smart_execution_fills f JOIN smart_execution_tca t
               ON t.execution_intent_id=f.execution_intent_id
               JOIN smart_execution_intents i ON i.execution_intent_id=f.execution_intent_id
               WHERE i.symbol=?"""
        params=[symbol]
        if session:
            sql+=" AND t.session=?";params.append(session)
        if order_type:
            sql+=" AND t.order_type=?";params.append(order_type)
        sql+=" ORDER BY f.ts DESC LIMIT 500"
        rows=[dict(x) for x in c.execute(sql,tuple(params)).fetchall()];c.close()
        ack=[finite(x.get("broker_ack_latency_ms")) for x in rows];ack=[x for x in ack if x is not None]
        ff=[finite(x.get("first_fill_latency_ms")) for x in rows];ff=[x for x in ff if x is not None]
        return {"symbol":symbol,"session":session,"order_type":order_type,"samples":len(rows),
                "ack_mean_ms":statistics.mean(ack) if ack else None,
                "ack_p95_ms":sorted(ack)[min(len(ack)-1,int(.95*(len(ack)-1)))] if ack else None,
                "first_fill_mean_ms":statistics.mean(ff) if ff else None,
                "degraded":bool(ack and statistics.mean(ack)>self.latency_warning_ms)}

    def record_adverse_selection(self,eid:str,post_fill_price:float,window_seconds:int=30)->Dict[str,Any]:
        x=self.intent(eid)
        if not x:raise KeyError("UNKNOWN_EXECUTION_INTENT")
        c=self.conn();tca=c.execute("SELECT * FROM smart_execution_tca WHERE execution_intent_id=? ORDER BY ts DESC LIMIT 1",(eid,)).fetchone();c.close()
        if not tca:return {"status":"NO_FILL_TCA"}
        fill=float(tca["actual_fill_price"]);post=float(post_fill_price)
        adverse=(post-fill) if x["side"]=="SELL" else (fill-post)
        bps=adverse/max(abs(fill),1e-12)*10000.0
        c=self.conn();meta=json.loads(tca["metadata_json"] or "{}")
        meta["adverse_selection_window_seconds"]=int(window_seconds);meta["post_fill_price"]=post
        c.execute("UPDATE smart_execution_tca SET adverse_selection_bps=?,metadata_json=? WHERE tca_id=?",
                  (bps,canonical(meta),tca["tca_id"]));c.commit();c.close()
        return {"status":"RECORDED","adverse_selection_bps":bps,"window_seconds":window_seconds}

    def record_exit_quality(self,eid:str,expected_exit:float,actual_exit:float,quantity:float,
                            fees:float=0.0,stop_expected:Optional[float]=None,
                            stop_trigger:Optional[float]=None)->Dict[str,Any]:
        x=self.intent(eid)
        if not x:raise KeyError("UNKNOWN_EXECUTION_INTENT")
        # Closing a BUY is a SELL execution and vice versa.
        exit_side="SELL" if x["side"]=="BUY" else "BUY"
        sm=slippage_metrics(exit_side,float(expected_exit),float(actual_exit),abs(float(quantity)))
        slip_pen=min(55.0,max(0.0,sm["slippage_bps"])*5.0)
        fee_pen=min(20.0,abs(float(fees))*10.0)
        score=clamp(100-slip_pen-fee_pen,0,100)
        stop_slip=None
        if stop_expected is not None:
            stop_slip=abs(float(actual_exit)-float(stop_expected))/max(abs(float(stop_expected)),1e-12)*10000.0
        c=self.conn();row=c.execute("SELECT * FROM smart_execution_tca WHERE execution_intent_id=? ORDER BY ts DESC LIMIT 1",(eid,)).fetchone()
        if row:
            meta=json.loads(row["metadata_json"] or "{}")
            meta["exit_expected_price"]=float(expected_exit);meta["exit_actual_price"]=float(actual_exit)
            meta["exit_slippage_bps"]=sm["slippage_bps"];meta["exit_fees"]=float(fees)
            meta["stop_trigger_price"]=stop_trigger
            c.execute("UPDATE smart_execution_tca SET exit_execution_score=?,stop_slippage_bps=COALESCE(?,stop_slippage_bps),metadata_json=? WHERE tca_id=?",
                      (score,stop_slip,canonical(meta),row["tca_id"]))
            c.commit()
        c.close()
        return {"exit_execution_score":score,"exit_slippage_bps":sm["slippage_bps"],
                "exit_slippage_cost":sm["slippage_cost"],"stop_slippage_bps":stop_slip}

    def execution_memory_analysis(self,min_samples:int=10)->Dict[str,Any]:
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM smart_execution_tca ORDER BY ts").fetchall()];c.close()
        groups={}
        for x in rows:
            key=(x.get("symbol"),x.get("strategy_id"),x.get("order_type"),x.get("session"),
                 x.get("market_regime"),x.get("volatility"),x.get("liquidity_state"))
            groups.setdefault(key,[]).append(x)
        out=[]
        for key,rs in groups.items():
            quality=[finite(x.get("execution_quality_score")) for x in rs];quality=[v for v in quality if v is not None]
            slip=[finite(x.get("slippage_bps")) for x in rs];slip=[v for v in slip if v is not None]
            fill=[finite(x.get("fill_rate")) for x in rs];fill=[v for v in fill if v is not None]
            status="INSUFFICIENT_DATA" if len(rs)<min_samples else "OBSERVED"
            out.append({"symbol":key[0],"strategy":key[1],"order_type":key[2],"session":key[3],
                        "market_regime":key[4],"volatility":key[5],"liquidity_state":key[6],
                        "samples":len(rs),"status":status,
                        "avg_execution_quality":statistics.mean(quality) if quality else None,
                        "avg_slippage_bps":statistics.mean(slip) if slip else None,
                        "avg_fill_rate":statistics.mean(fill) if fill else None,
                        "recommendation":"OBSERVE_ANALYZE_RECOMMEND_ONLY"})
        return {"groups":out,"learning_authority":False,"auto_policy_change":False}

    def daily_costs(self,day:Optional[str]=None)->Dict[str,Any]:
        day=day or datetime.now(timezone.utc).date().isoformat()
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM smart_execution_tca WHERE substr(ts,1,10)=?",(day,)).fetchall()];c.close()
        return {"day":day,"trades":len(rows),
                "total_fees":sum(float(x["fees"] or 0) for x in rows),
                "total_slippage":sum(float(x["slippage_cost"] or 0) for x in rows),
                "total_spread_cost":sum(float(x["spread_cost"] or 0) for x in rows),
                "estimated_market_impact":sum(float(x["estimated_market_impact"] or 0) for x in rows),
                "total_execution_cost":sum(float(x["total_execution_cost"] or 0) for x in rows),
                "expected_gross_edge":sum(float(x["expected_gross_edge"] or 0) for x in rows)}

    def dashboard(self)->Dict[str,Any]:
        c=self.conn()
        last=c.execute("SELECT * FROM smart_execution_decisions ORDER BY ts DESC LIMIT 1").fetchone()
        active=[dict(x) for x in c.execute("""SELECT * FROM smart_execution_intents
                    WHERE status IN ('VALID','EXPIRING') ORDER BY created_at DESC LIMIT 100""").fetchall()]
        partial=[dict(x) for x in c.execute("""SELECT * FROM smart_execution_intents
                    WHERE filled_quantity>0 AND remaining_quantity>0 ORDER BY updated_at DESC LIMIT 100""").fetchall()]
        expired=c.execute("SELECT COUNT(*) n FROM smart_execution_intents WHERE status='EXPIRED'").fetchone()["n"]
        tca=[dict(x) for x in c.execute("SELECT * FROM smart_execution_tca ORDER BY ts DESC LIMIT 100").fetchall()]
        fills=[dict(x) for x in c.execute("SELECT * FROM smart_execution_fills ORDER BY ts DESC LIMIT 100").fetchall()]
        c.close()
        quality=[float(x["execution_quality_score"]) for x in tca if x["execution_quality_score"] is not None]
        slip=[float(x["slippage_bps"]) for x in tca if x["slippage_bps"] is not None]
        fill_rate=[float(x["fill_rate"]) for x in tca if x["fill_rate"] is not None]
        rejected=sum(int(x["rejected"]) for x in fills)
        latency=[finite(x.get("broker_ack_latency_ms")) for x in fills];latency=[x for x in latency if x is not None]
        return {"enabled":True,"engine_version":self.version,"mode":self.mode,
                "signal_authority":False,"risk_increase_authority":False,
                "current_execution_regime":last["execution_regime"] if last else "NO_DATA",
                "execution_confidence":last["execution_confidence"] if last else None,
                "execution_quality_score":statistics.mean(quality) if quality else None,
                "average_slippage_bps":statistics.mean(slip) if slip else None,
                "fill_rate":statistics.mean(fill_rate) if fill_rate else None,
                "rejection_rate":rejected/len(fills) if fills else None,
                "broker_latency_ms":statistics.mean(latency) if latency else None,
                "active_execution_intents":active,"partial_fills":partial,"expired_intents":int(expired),
                "daily_costs":self.daily_costs(),"degradation":self.degradation(),
                "latency_model":self.latency_baseline(active[0]["symbol"] if active else (tca[0]["symbol"] if tca else "UNKNOWN")),
                "execution_memory":self.execution_memory_analysis(min_samples=self.min_history_samples),
                "activation_path":["SHADOW","PAPER","CANARY","LIMITED_EXECUTION","PRODUCTION_EXECUTION"],
                "production_policy_authority":self.mode!="SHADOW"}

    def candidate_execution_policy(self,parent_policy:str,proposal:Dict[str,Any],evidence:Dict[str,Any])->Dict[str,Any]:
        cid="exec_candidate_"+uuid.uuid4().hex
        c=self.conn();c.execute("""INSERT INTO smart_execution_policy_candidates(
          candidate_id,created_at,parent_policy,candidate_policy_json,status,evidence_json,validation_state,auto_deploy)
          VALUES(?,?,?,?,?,?,?,0)""",(cid,now_iso(),parent_policy,canonical(proposal),"RESEARCH_ONLY",
                                    canonical(evidence),"PENDING"));c.commit();c.close()
        return {"candidate_id":cid,"status":"RESEARCH_ONLY","auto_deploy":False,
                "required_path":["SIMULATION","PAPER","VALIDATION","CANARY"]}


from __future__ import annotations

from typing import Dict, Any, Optional, List, Callable, Awaitable
from datetime import datetime, timezone, timedelta
import asyncio
import hashlib
import json
import math
import random
import sqlite3
import time
import uuid

import httpx

from order_state import transition as validate_transition, is_terminal, conservative_filled_units

RECOVERY_STATES = (
    "NORMAL",
    "DEGRADED",
    "RECOVERING",
    "RECONCILING",
    "TRADING_PAUSED",
    "SAFE_MODE",
    "CRITICAL_FAILURE",
    "READY",
)

RECONCILIATION_STATES = (
    "MATCHED",
    "MINOR_MISMATCH",
    "RECONCILIATION_REQUIRED",
    "CRITICAL_MISMATCH",
)

CRITICAL_ORDER_STATES = {"UNKNOWN"}

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _f(v, default=None):
    try:
        x=float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default

def _dt(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z","+00:00"))
    except Exception:
        return None

def _j(v):
    return json.dumps(v, separators=(",",":"), sort_keys=True, default=str)

def deterministic_intent_key(account_scope: str, symbol: str, side: str,
                             strategy_id: str, market_time: str,
                             entry: float, stop: float, target: float) -> str:
    raw="|".join([
        str(account_scope),str(symbol),str(side),str(strategy_id),str(market_time),
        f"{float(entry):.10f}",f"{float(stop):.10f}",f"{float(target):.10f}"
    ])
    return hashlib.sha256(raw.encode()).hexdigest()

def client_order_id(intent_key: str) -> str:
    return "ri_"+str(intent_key)[:48]

class RecoveryManager:
    """
    Deterministic operational-safety layer.

    Broker-confirmed state is authoritative for money, orders, fills and positions.
    Unknown submission outcome is never retried automatically.
    """

    def __init__(
        self,
        db_path: str,
        base_url: str,
        account: str,
        token: str,
        account_scope: str = "PRIMARY",
        use_client_extensions: bool = True,
        circuit_failure_threshold: int = 3,
        circuit_open_seconds: float = 20.0,
        request_min_interval_ms: float = 80.0,
        max_read_retries: int = 4,
        backoff_base_seconds: float = 0.4,
        backoff_cap_seconds: float = 8.0,
        allow_orphan_quarantine: bool = False,
    ):
        self.db_path=db_path
        self.base_url=base_url.rstrip("/")
        self.account=account
        self.token=token
        self.account_scope=str(account_scope)
        self.use_client_extensions=bool(use_client_extensions)
        self.circuit_failure_threshold=max(1,int(circuit_failure_threshold))
        self.circuit_open_seconds=max(1.0,float(circuit_open_seconds))
        self.request_min_interval=max(0.0,float(request_min_interval_ms)/1000.0)
        self.max_read_retries=max(0,int(max_read_retries))
        self.backoff_base=max(0.05,float(backoff_base_seconds))
        self.backoff_cap=max(self.backoff_base,float(backoff_cap_seconds))
        # Practice-only escape hatch for stale local position rows. In live/production
        # this must remain False: an unexplained missing broker trade is reconciliation-required.
        self.allow_orphan_quarantine=bool(allow_orphan_quarantine)
        self._request_lock=asyncio.Lock()
        self._last_request_monotonic=0.0

    # ---------------- persistence ----------------
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
        CREATE TABLE IF NOT EXISTS recovery_state(
          account_scope TEXT PRIMARY KEY,
          state TEXT NOT NULL,
          safe_mode INTEGER NOT NULL DEFAULT 1,
          emergency_stop INTEGER NOT NULL DEFAULT 0,
          new_trades_allowed INTEGER NOT NULL DEFAULT 0,
          last_transaction_id TEXT,
          last_broker_success_ts TEXT,
          last_market_data_ts TEXT,
          last_reconciliation_ts TEXT,
          last_reconciliation_status TEXT,
          last_risk_verified_ts TEXT,
          incident_started_ts TEXT,
          broker_disconnect_started_ts TEXT,
          ready_ts TEXT,
          details_json TEXT NOT NULL DEFAULT '{}',
          updated_ts TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS recovery_order_intents(
          execution_intent_id TEXT PRIMARY KEY,
          idempotency_key TEXT NOT NULL,
          account_scope TEXT NOT NULL,
          correlation_id TEXT,
          decision_id TEXT,
          risk_decision_id TEXT,
          signal_id INTEGER,
          strategy_id TEXT,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          requested_units REAL NOT NULL,
          filled_units REAL NOT NULL DEFAULT 0,
          remaining_units REAL NOT NULL DEFAULT 0,
          entry_price REAL,
          stop_loss REAL,
          take_profit REAL,
          state TEXT NOT NULL,
          client_order_id TEXT NOT NULL,
          broker_order_id TEXT,
          broker_trade_id TEXT,
          broker_event_id TEXT,
          broker_transaction_cursor_before TEXT,
          request_json TEXT NOT NULL DEFAULT '{}',
          response_json TEXT NOT NULL DEFAULT '{}',
          metadata_json TEXT NOT NULL DEFAULT '{}',
          execution_quality_compromised INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          created_ts TEXT NOT NULL,
          submitted_ts TEXT,
          acknowledged_ts TEXT,
          filled_ts TEXT,
          updated_ts TEXT NOT NULL,
          UNIQUE(account_scope,idempotency_key),
          UNIQUE(account_scope,client_order_id)
        );

        CREATE TABLE IF NOT EXISTS recovery_event_journal(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          event_id TEXT NOT NULL UNIQUE,
          ts TEXT NOT NULL,
          account_scope TEXT NOT NULL,
          correlation_id TEXT,
          execution_intent_id TEXT,
          trade_id TEXT,
          order_id TEXT,
          strategy_id TEXT,
          event_type TEXT NOT NULL,
          broker_event_id TEXT,
          payload_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_recovery_broker_event_dedup
        ON recovery_event_journal(account_scope,broker_event_id)
        WHERE broker_event_id IS NOT NULL AND broker_event_id!='';

        CREATE INDEX IF NOT EXISTS idx_recovery_journal_corr
        ON recovery_event_journal(correlation_id,ts);

        CREATE INDEX IF NOT EXISTS idx_recovery_intent_state
        ON recovery_order_intents(account_scope,state,updated_ts);

        CREATE TABLE IF NOT EXISTS recovery_incidents(
          incident_id TEXT PRIMARY KEY,
          account_scope TEXT NOT NULL,
          started_ts TEXT NOT NULL,
          recovered_ts TEXT,
          status TEXT NOT NULL,
          severity TEXT NOT NULL,
          incident_type TEXT NOT NULL,
          correlation_id TEXT,
          execution_intent_id TEXT,
          reason TEXT NOT NULL,
          details_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS recovery_reconciliation_runs(
          reconciliation_id TEXT PRIMARY KEY,
          account_scope TEXT NOT NULL,
          started_ts TEXT NOT NULL,
          completed_ts TEXT,
          status TEXT NOT NULL,
          broker_transaction_id TEXT,
          broker_balance REAL,
          broker_nav REAL,
          broker_margin_used REAL,
          summary_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS recovery_reconciliation_items(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          reconciliation_id TEXT NOT NULL,
          item_type TEXT NOT NULL,
          entity_id TEXT,
          status TEXT NOT NULL,
          severity TEXT NOT NULL,
          internal_json TEXT NOT NULL DEFAULT '{}',
          broker_json TEXT NOT NULL DEFAULT '{}',
          reason TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS recovery_circuit_breakers(
          account_scope TEXT NOT NULL,
          service TEXT NOT NULL,
          state TEXT NOT NULL,
          failure_count INTEGER NOT NULL DEFAULT 0,
          opened_ts TEXT,
          half_open_ts TEXT,
          last_failure_ts TEXT,
          last_success_ts TEXT,
          last_error TEXT,
          updated_ts TEXT NOT NULL,
          PRIMARY KEY(account_scope,service)
        );

        CREATE TABLE IF NOT EXISTS recovery_metrics(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          account_scope TEXT NOT NULL,
          incidents_total INTEGER NOT NULL,
          reconciliation_failures INTEGER NOT NULL,
          recovery_success_rate REAL,
          mean_time_to_recovery_seconds REAL,
          duplicate_orders_prevented INTEGER NOT NULL,
          unknown_order_states INTEGER NOT NULL,
          position_mismatches INTEGER NOT NULL,
          broker_disconnect_duration_seconds REAL,
          safe_mode_seconds REAL,
          details_json TEXT NOT NULL DEFAULT '{}'
        );
        """)
        # Strengthen existing Position/Trade Memory tables without creating a second store.
        for ddl in (
            "ALTER TABLE active_trade_management ADD COLUMN current_units REAL",
            "ALTER TABLE trade_memory ADD COLUMN execution_quality_compromised INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE trade_memory ADD COLUMN operational_incident_id TEXT",
            "ALTER TABLE trade_memory ADD COLUMN strategy_version TEXT",
            "ALTER TABLE trade_memory ADD COLUMN risk_config_version TEXT",
            "ALTER TABLE trade_memory ADD COLUMN director_version TEXT",
            "ALTER TABLE trade_memory ADD COLUMN regime_model_version TEXT",
            "ALTER TABLE trade_memory ADD COLUMN deployment_version TEXT",
            "ALTER TABLE trade_memory ADD COLUMN runtime_code_hash TEXT",
            "ALTER TABLE trade_memory ADD COLUMN dependency_lock_hash TEXT",
            "ALTER TABLE trade_memory ADD COLUMN config_snapshot_hash TEXT",
        ):
            try:
                c.execute(ddl)
            except sqlite3.OperationalError as e:
                msg=str(e).lower()
                if "duplicate column" not in msg and "no such table" not in msg:
                    raise
        row=c.execute("SELECT 1 FROM recovery_state WHERE account_scope=?",(self.account_scope,)).fetchone()
        if not row:
            c.execute("""INSERT INTO recovery_state(
              account_scope,state,safe_mode,emergency_stop,new_trades_allowed,details_json,updated_ts)
              VALUES(?, 'SAFE_MODE',1,0,0,'{}',?)""",(self.account_scope,now_iso()))
        c.commit();c.close()

    # ---------------- journal/state ----------------
    def journal(self,event_type:str,correlation_id:Optional[str]=None,
                execution_intent_id:Optional[str]=None,trade_id:Optional[str]=None,
                order_id:Optional[str]=None,strategy_id:Optional[str]=None,
                payload:Optional[Dict[str,Any]]=None,broker_event_id:Optional[str]=None) -> Dict[str,Any]:
        event_id="evt_"+uuid.uuid4().hex
        c=self.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("""INSERT INTO recovery_event_journal(
              event_id,ts,account_scope,correlation_id,execution_intent_id,trade_id,order_id,
              strategy_id,event_type,broker_event_id,payload_json)
              VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
              (event_id,now_iso(),self.account_scope,correlation_id,execution_intent_id,
               trade_id,order_id,strategy_id,event_type,broker_event_id,_j(payload or {})))
            c.commit()
            return {"inserted":True,"event_id":event_id}
        except sqlite3.IntegrityError:
            c.rollback()
            return {"inserted":False,"duplicate":True,"broker_event_id":broker_event_id}
        finally:
            c.close()

    def state(self) -> Dict[str,Any]:
        try:
            c=self.conn();r=c.execute("SELECT * FROM recovery_state WHERE account_scope=?",(self.account_scope,)).fetchone();c.close()
            return dict(r) if r else {"state":"CRITICAL_FAILURE","safe_mode":1,"new_trades_allowed":0}
        except Exception as e:
            return {"state":"CRITICAL_FAILURE","safe_mode":1,"new_trades_allowed":0,"database_error":str(e)}

    def set_state(self,state:str,reason:str="",safe_mode:Optional[bool]=None,
                  new_trades_allowed:Optional[bool]=None,details:Optional[Dict[str,Any]]=None):
        if state not in RECOVERY_STATES:
            raise ValueError(f"Unknown recovery state {state}")
        c=self.conn()
        old=c.execute("SELECT * FROM recovery_state WHERE account_scope=?",(self.account_scope,)).fetchone()
        sm=int(bool(safe_mode)) if safe_mode is not None else int(old["safe_mode"] if old else 1)
        allow=int(bool(new_trades_allowed)) if new_trades_allowed is not None else int(old["new_trades_allowed"] if old else 0)
        c.execute("""UPDATE recovery_state SET state=?,safe_mode=?,new_trades_allowed=?,
          details_json=?,updated_ts=?,ready_ts=CASE WHEN ?='READY' THEN ? ELSE ready_ts END
          WHERE account_scope=?""",
          (state,sm,allow,_j({"reason":reason,**(details or {})}),now_iso(),state,now_iso(),self.account_scope))
        c.commit();c.close()
        if not old or old["state"]!=state:
            self.journal("RECOVERY_STATE_CHANGED",payload={"previous":old["state"] if old else None,
                         "current":state,"reason":reason,"safe_mode":bool(sm),"new_trades_allowed":bool(allow)})
        return self.state()

    def enter_safe_mode(self,reason:str,correlation_id:Optional[str]=None,
                        execution_intent_id:Optional[str]=None,severity:str="HIGH"):
        c=self.conn()
        old=c.execute("SELECT incident_started_ts FROM recovery_state WHERE account_scope=?",(self.account_scope,)).fetchone()
        started=(old["incident_started_ts"] if old else None) or now_iso()
        c.execute("""UPDATE recovery_state SET state='SAFE_MODE',safe_mode=1,new_trades_allowed=0,
                     incident_started_ts=?,details_json=?,updated_ts=? WHERE account_scope=?""",
                  (started,_j({"reason":reason}),now_iso(),self.account_scope))
        c.commit();c.close()
        inc=self.open_incident("SAFE_MODE",severity,reason,correlation_id,execution_intent_id)
        self.journal("SAFE_MODE_ENTERED",correlation_id,execution_intent_id,payload={"reason":reason,"incident_id":inc})
        return inc

    def exit_safe_mode(self,reason:str="reconciliation and risk verification passed"):
        c=self.conn()
        row=c.execute("SELECT incident_started_ts FROM recovery_state WHERE account_scope=?",(self.account_scope,)).fetchone()
        c.execute("""UPDATE recovery_state SET state='READY',safe_mode=0,new_trades_allowed=1,
                     incident_started_ts=NULL,ready_ts=?,details_json=?,updated_ts=?
                     WHERE account_scope=?""",
                  (now_iso(),_j({"reason":reason}),now_iso(),self.account_scope))
        c.commit();c.close()
        self.recover_open_incidents(reason)
        self.journal("SAFE_MODE_EXITED",payload={"reason":reason})
        self.journal("SYSTEM_READY",payload={"reason":reason})

    def set_emergency_stop(self,active:bool,reason:str):
        c=self.conn()
        c.execute("""UPDATE recovery_state SET emergency_stop=?,safe_mode=CASE WHEN ?=1 THEN 1 ELSE safe_mode END,
          new_trades_allowed=CASE WHEN ?=1 THEN 0 ELSE new_trades_allowed END,
          state=CASE WHEN ?=1 THEN 'TRADING_PAUSED' ELSE state END,details_json=?,updated_ts=?
          WHERE account_scope=?""",
          (int(active),int(active),int(active),int(active),_j({"emergency_stop_reason":reason}),now_iso(),self.account_scope))
        c.commit();c.close()
        self.journal("EMERGENCY_STOP" if active else "EMERGENCY_STOP_CLEARED",payload={"reason":reason})
        return self.state()

    def new_trades_allowed(self) -> bool:
        st=self.state()
        return bool(st.get("new_trades_allowed")) and not bool(st.get("safe_mode")) and not bool(st.get("emergency_stop")) and st.get("state") in ("READY","NORMAL")

    def open_incident(self,incident_type:str,severity:str,reason:str,
                      correlation_id:Optional[str]=None,execution_intent_id:Optional[str]=None) -> str:
        c=self.conn()
        # Deduplicate open incidents of same type/intent.
        row=c.execute("""SELECT incident_id FROM recovery_incidents
                         WHERE account_scope=? AND status='OPEN' AND incident_type=?
                         AND COALESCE(execution_intent_id,'')=COALESCE(?,'')
                         ORDER BY started_ts DESC LIMIT 1""",
                      (self.account_scope,incident_type,execution_intent_id)).fetchone()
        if row:
            c.close();return row["incident_id"]
        iid="inc_"+uuid.uuid4().hex
        c.execute("""INSERT INTO recovery_incidents(
          incident_id,account_scope,started_ts,status,severity,incident_type,correlation_id,
          execution_intent_id,reason,details_json) VALUES(?,?,?,'OPEN',?,?,?,?,?,?)""",
          (iid,self.account_scope,now_iso(),severity,incident_type,correlation_id,
           execution_intent_id,reason,'{}'))
        c.commit();c.close()
        return iid

    def recover_open_incidents(self,reason:str):
        c=self.conn()
        c.execute("""UPDATE recovery_incidents SET status='RECOVERED',recovered_ts=?,
          details_json=? WHERE account_scope=? AND status='OPEN'""",
          (now_iso(),_j({"recovery_reason":reason}),self.account_scope))
        c.commit();c.close()

    # ---------------- circuit breaker + request ----------------
    def circuit(self,service="BROKER") -> Dict[str,Any]:
        c=self.conn()
        row=c.execute("SELECT * FROM recovery_circuit_breakers WHERE account_scope=? AND service=?",
                      (self.account_scope,service)).fetchone()
        if not row:
            c.execute("""INSERT INTO recovery_circuit_breakers(account_scope,service,state,failure_count,updated_ts)
                         VALUES(?,?,'CLOSED',0,?)""",(self.account_scope,service,now_iso()))
            c.commit()
            row=c.execute("SELECT * FROM recovery_circuit_breakers WHERE account_scope=? AND service=?",
                          (self.account_scope,service)).fetchone()
        c.close()
        d=dict(row)
        if d["state"]=="OPEN":
            opened=_dt(d.get("opened_ts"))
            if opened and (datetime.now(timezone.utc)-opened).total_seconds()>=self.circuit_open_seconds:
                self._set_circuit(service,"HALF_OPEN",d["failure_count"],None)
                d=self.circuit(service)
        return d

    def _set_circuit(self,service,state,failure_count,error):
        c=self.conn();ts=now_iso()
        c.execute("""INSERT INTO recovery_circuit_breakers(
          account_scope,service,state,failure_count,opened_ts,half_open_ts,last_failure_ts,last_success_ts,last_error,updated_ts)
          VALUES(?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(account_scope,service) DO UPDATE SET
          state=excluded.state,failure_count=excluded.failure_count,
          opened_ts=CASE WHEN excluded.state='OPEN' THEN excluded.opened_ts ELSE recovery_circuit_breakers.opened_ts END,
          half_open_ts=CASE WHEN excluded.state='HALF_OPEN' THEN excluded.half_open_ts ELSE recovery_circuit_breakers.half_open_ts END,
          last_failure_ts=CASE WHEN excluded.last_error IS NOT NULL THEN excluded.last_failure_ts ELSE recovery_circuit_breakers.last_failure_ts END,
          last_success_ts=CASE WHEN excluded.last_error IS NULL THEN excluded.last_success_ts ELSE recovery_circuit_breakers.last_success_ts END,
          last_error=excluded.last_error,updated_ts=excluded.updated_ts""",
          (self.account_scope,service,state,failure_count,
           ts if state=="OPEN" else None,ts if state=="HALF_OPEN" else None,
           ts if error else None,ts if not error else None,error,ts))
        c.commit();c.close()

    def circuit_success(self,service="BROKER"):
        self._set_circuit(service,"CLOSED",0,None)

    def circuit_failure(self,error:str,service="BROKER"):
        d=self.circuit(service)
        n=int(d.get("failure_count") or 0)+1
        state="OPEN" if n>=self.circuit_failure_threshold else d.get("state","CLOSED")
        self._set_circuit(service,state,n,error)
        if state=="OPEN":
            self.enter_safe_mode(f"{service} circuit breaker opened: {error}",severity="CRITICAL")
            self.journal("CIRCUIT_BREAKER_OPEN",payload={"service":service,"failure_count":n,"error":error})

    async def _throttle(self,critical:bool):
        async with self._request_lock:
            elapsed=time.monotonic()-self._last_request_monotonic
            wait=max(0.0,self.request_min_interval-elapsed)
            # Critical reconciliation/risk reads are allowed a shorter interval, not zero.
            if critical:
                wait*=0.25
            if wait:
                await asyncio.sleep(wait)
            self._last_request_monotonic=time.monotonic()

    async def broker_request(self,client:httpx.AsyncClient,method:str,path:str,params=None,body=None,
                             *,critical:bool=False,allow_retry:Optional[bool]=None,timeout:float=15.0):
        if not self.account or not self.token:
            self.enter_safe_mode("broker credentials unavailable",severity="CRITICAL")
            raise RuntimeError("Missing broker account/token")
        cb=self.circuit("BROKER")
        if cb["state"]=="OPEN":
            raise RuntimeError("BROKER_CIRCUIT_OPEN")
        if allow_retry is None:
            allow_retry=method.upper()=="GET"
        attempts=(self.max_read_retries+1) if allow_retry else 1
        url=self.base_url+path.replace("{account}",self.account)

        last=None
        for attempt in range(attempts):
            await self._throttle(critical)
            try:
                r=await client.request(method,url,params=params,json=body,
                    headers={"Authorization":f"Bearer {self.token}","Content-Type":"application/json"},
                    timeout=timeout)
                if r.status_code==429:
                    retry_after=_f(r.headers.get("Retry-After"),None)
                    if allow_retry and attempt+1<attempts:
                        delay=retry_after if retry_after is not None else min(self.backoff_cap,self.backoff_base*(2**attempt))
                        await asyncio.sleep(max(0.05,delay))
                        continue
                if r.status_code>=500 and allow_retry and attempt+1<attempts:
                    delay=min(self.backoff_cap,self.backoff_base*(2**attempt))+random.uniform(0,self.backoff_base)
                    await asyncio.sleep(delay);continue
                if r.status_code>=400:
                    try:msg=r.json().get("errorMessage") or r.json().get("errorCode") or r.text[:200]
                    except Exception:msg=r.text[:200]
                    raise RuntimeError(f"BROKER_HTTP_{r.status_code}: {msg}")
                self.circuit_success("BROKER")
                self._broker_success()
                return r.json()
            except (httpx.TimeoutException,httpx.TransportError,asyncio.TimeoutError) as e:
                last=e
                self.circuit_failure(str(e),"BROKER")
                if method.upper()!="GET":
                    self.enter_safe_mode(f"Broker write outcome uncertain: {method} {path}: {e}",severity="CRITICAL")
                    self.journal("BROKER_WRITE_STATUS_UNKNOWN",payload={"method":method,"path":path,"error":str(e)})
                if not allow_retry or attempt+1>=attempts:
                    raise
                delay=min(self.backoff_cap,self.backoff_base*(2**attempt))+random.uniform(0,self.backoff_base)
                await asyncio.sleep(delay)
            except Exception as e:
                last=e
                # 4xx is not necessarily connection failure; 5xx/runtime transport is.
                if "BROKER_HTTP_5" in str(e):
                    self.circuit_failure(str(e),"BROKER")
                raise
        raise last or RuntimeError("broker request failed")

    def _broker_success(self):
        c=self.conn()
        row=c.execute("SELECT broker_disconnect_started_ts FROM recovery_state WHERE account_scope=?",(self.account_scope,)).fetchone()
        was_disconnected=bool(row and row["broker_disconnect_started_ts"])
        c.execute("""UPDATE recovery_state SET last_broker_success_ts=?,
                     broker_disconnect_started_ts=NULL,updated_ts=? WHERE account_scope=?""",
                  (now_iso(),now_iso(),self.account_scope))
        c.commit();c.close()
        if was_disconnected:
            self.journal("BROKER_RECONNECTED")

    def broker_disconnected(self,reason:str):
        c=self.conn()
        row=c.execute("SELECT broker_disconnect_started_ts FROM recovery_state WHERE account_scope=?",(self.account_scope,)).fetchone()
        started=(row["broker_disconnect_started_ts"] if row else None) or now_iso()
        c.execute("""UPDATE recovery_state SET broker_disconnect_started_ts=?,
                     state='DEGRADED',safe_mode=1,new_trades_allowed=0,updated_ts=? WHERE account_scope=?""",
                  (started,now_iso(),self.account_scope))
        c.commit();c.close()
        self.journal("BROKER_DISCONNECTED",payload={"reason":reason})
        self.open_incident("BROKER_DISCONNECTED","CRITICAL",reason)

    # ---------------- order intent / state machine ----------------
    def create_intent(self,*,idempotency_key:str,correlation_id:str,decision_id:Optional[str],
                      risk_decision_id:Optional[str],strategy_id:str,symbol:str,side:str,
                      requested_units:float,entry_price:float,stop_loss:float,take_profit:float,
                      request_body:Dict[str,Any],metadata:Optional[Dict[str,Any]]=None) -> Dict[str,Any]:
        self.ensure_schema()
        c=self.conn()
        existing=c.execute("""SELECT * FROM recovery_order_intents
                              WHERE account_scope=? AND idempotency_key=?""",
                           (self.account_scope,idempotency_key)).fetchone()
        if existing:
            c.close()
            self.journal("DUPLICATE_ORDER_PREVENTED",correlation_id,existing["execution_intent_id"],
                         strategy_id=strategy_id,payload={"state":existing["state"],"idempotency_key":idempotency_key})
            return {"created":False,"duplicate_prevented":True,"intent":dict(existing)}

        eid="intent_"+uuid.uuid4().hex
        coid=client_order_id(idempotency_key)
        cursor=self.state().get("last_transaction_id")
        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute("""INSERT INTO recovery_order_intents(
              execution_intent_id,idempotency_key,account_scope,correlation_id,decision_id,risk_decision_id,
              strategy_id,symbol,side,requested_units,filled_units,remaining_units,entry_price,stop_loss,take_profit,
              state,client_order_id,broker_transaction_cursor_before,request_json,metadata_json,created_ts,updated_ts)
              VALUES(?,?,?,?,?,?,?,?,?,?,0,?,?,?,?, 'CREATED',?,?,?,?,?,?)""",
              (eid,idempotency_key,self.account_scope,correlation_id,decision_id,risk_decision_id,
               strategy_id,symbol,side,abs(float(requested_units)),abs(float(requested_units)),
               entry_price,stop_loss,take_profit,coid,cursor,_j(request_body),_j(metadata or {}),now_iso(),now_iso()))
            c.execute("""INSERT INTO recovery_event_journal(
              event_id,ts,account_scope,correlation_id,execution_intent_id,strategy_id,event_type,payload_json)
              VALUES(?,?,?,?,?,?,?,?)""",
              ("evt_"+uuid.uuid4().hex,now_iso(),self.account_scope,correlation_id,eid,strategy_id,
               "ORDER_INTENT_CREATED",_j({"idempotency_key":idempotency_key,"client_order_id":coid,
                                          "requested_units":abs(float(requested_units))})))
            c.commit()
            row=c.execute("SELECT * FROM recovery_order_intents WHERE execution_intent_id=?",(eid,)).fetchone()
            c.close()
            return {"created":True,"duplicate_prevented":False,"intent":dict(row)}
        except sqlite3.IntegrityError:
            # Concurrent callers can both pass the pre-check before one wins the
            # UNIQUE(account_scope,idempotency_key) insert. Treat the loser as an
            # idempotent duplicate rather than surfacing an exception/crashing.
            try:c.rollback()
            except Exception:pass
            existing=c.execute("""SELECT * FROM recovery_order_intents
                                  WHERE account_scope=? AND idempotency_key=?""",
                               (self.account_scope,idempotency_key)).fetchone()
            c.close()
            if not existing:
                raise
            self.journal("DUPLICATE_ORDER_PREVENTED",correlation_id,existing["execution_intent_id"],
                         strategy_id=strategy_id,payload={"state":existing["state"],"idempotency_key":idempotency_key,
                                                         "concurrent_race":True})
            return {"created":False,"duplicate_prevented":True,"intent":dict(existing)}

    def intent(self,execution_intent_id:str) -> Optional[Dict[str,Any]]:
        c=self.conn();r=c.execute("SELECT * FROM recovery_order_intents WHERE execution_intent_id=?",
                                  (execution_intent_id,)).fetchone();c.close()
        return dict(r) if r else None

    def link_signal(self,execution_intent_id:str,signal_id:int):
        c=self.conn();c.execute("UPDATE recovery_order_intents SET signal_id=?,updated_ts=? WHERE execution_intent_id=?",
                                (signal_id,now_iso(),execution_intent_id));c.commit();c.close()

    def transition_intent(self,execution_intent_id:str,new_state:str,*,broker_order_id:Optional[str]=None,
                          broker_trade_id:Optional[str]=None,broker_event_id:Optional[str]=None,
                          filled_units:Optional[float]=None,response:Optional[Dict[str,Any]]=None,
                          error:Optional[str]=None,event_type:Optional[str]=None,
                          compromised:Optional[bool]=None) -> Dict[str,Any]:
        c=self.conn()
        row=c.execute("SELECT * FROM recovery_order_intents WHERE execution_intent_id=?",(execution_intent_id,)).fetchone()
        if not row:
            c.close();raise KeyError(execution_intent_id)
        current=row["state"]
        validate_transition(current,new_state)
        requested=float(row["requested_units"])
        fu=float(row["filled_units"] or 0) if filled_units is None else abs(float(filled_units))
        fu=min(requested,fu)
        rem=max(0.0,requested-fu)
        comp=int(row["execution_quality_compromised"])
        if compromised is not None:comp=max(comp,int(bool(compromised)))
        ts=now_iso()
        ack=ts if new_state in ("ACKNOWLEDGED","PARTIALLY_FILLED","FILLED") and not row["acknowledged_ts"] else row["acknowledged_ts"]
        fillts=ts if new_state in ("PARTIALLY_FILLED","FILLED") else row["filled_ts"]
        c.execute("BEGIN IMMEDIATE")
        c.execute("""UPDATE recovery_order_intents SET state=?,filled_units=?,remaining_units=?,
          broker_order_id=COALESCE(?,broker_order_id),broker_trade_id=COALESCE(?,broker_trade_id),
          broker_event_id=COALESCE(?,broker_event_id),response_json=?,last_error=?,
          acknowledged_ts=?,filled_ts=?,execution_quality_compromised=?,updated_ts=?
          WHERE execution_intent_id=?""",
          (new_state,fu,rem,broker_order_id,broker_trade_id,broker_event_id,
           _j(response or {}),error,ack,fillts,comp,ts,execution_intent_id))
        if event_type:
            try:
                c.execute("""INSERT INTO recovery_event_journal(
                  event_id,ts,account_scope,correlation_id,execution_intent_id,trade_id,order_id,
                  strategy_id,event_type,broker_event_id,payload_json)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                  ("evt_"+uuid.uuid4().hex,ts,self.account_scope,row["correlation_id"],execution_intent_id,
                   broker_trade_id or row["broker_trade_id"],broker_order_id or row["broker_order_id"],
                   row["strategy_id"],event_type,broker_event_id,
                   _j({"previous_state":current,"new_state":new_state,"filled_units":fu,
                       "remaining_units":rem,"error":error})))
            except sqlite3.IntegrityError:
                # Duplicate broker fill/event: state update above is idempotent, journal is not duplicated.
                pass
        c.commit()
        out=c.execute("SELECT * FROM recovery_order_intents WHERE execution_intent_id=?",(execution_intent_id,)).fetchone()
        c.close()
        return dict(out)

    async def submit_order(self,client:httpx.AsyncClient,*,idempotency_key:str,correlation_id:str,
                           decision_id:Optional[str],risk_decision_id:Optional[str],strategy_id:str,
                           symbol:str,side:str,requested_units:float,entry_price:float,stop_loss:float,
                           take_profit:float,order_body:Dict[str,Any],metadata:Optional[Dict[str,Any]]=None) -> Dict[str,Any]:
        # Idempotency is checked BEFORE the safe-mode gate. This is deliberate:
        # after a timeout the system is in SAFE_MODE, but a repeated caller still
        # needs to receive DUPLICATE_INTENT_PREVENTED instead of creating ambiguity.
        try:
            c=self.conn()
            existing=c.execute("""SELECT * FROM recovery_order_intents
                                  WHERE account_scope=? AND idempotency_key=?""",
                               (self.account_scope,idempotency_key)).fetchone()
            c.close()
        except Exception:
            existing=None
        if existing:
            self.journal("DUPLICATE_ORDER_PREVENTED",correlation_id,existing["execution_intent_id"],
                         strategy_id=strategy_id,payload={"state":existing["state"],"idempotency_key":idempotency_key})
            return {"skipped":"DUPLICATE_INTENT_PREVENTED","duplicate_prevented":True,"intent":dict(existing)}

        if not self.new_trades_allowed():
            return {"skipped":"RECOVERY_NOT_READY","recovery_state":self.state()}

        created=self.create_intent(
            idempotency_key=idempotency_key,correlation_id=correlation_id,decision_id=decision_id,
            risk_decision_id=risk_decision_id,strategy_id=strategy_id,symbol=symbol,side=side,
            requested_units=requested_units,entry_price=entry_price,stop_loss=stop_loss,take_profit=take_profit,
            request_body=order_body,metadata=metadata)
        intent=created["intent"]
        if not created["created"]:
            # Never resubmit an existing intent. UNKNOWN must be reconciled first.
            return {"skipped":"DUPLICATE_INTENT_PREVENTED","duplicate_prevented":True,"intent":intent}

        eid=intent["execution_intent_id"]
        body=json.loads(json.dumps(order_body))
        if self.use_client_extensions:
            body.setdefault("order",{})["clientExtensions"]={"id":intent["client_order_id"],"tag":"recovery-managed"}
            body["order"]["tradeClientExtensions"]={"id":intent["client_order_id"],"tag":"recovery-managed"}

        self.transition_intent(eid,"SUBMITTING",event_type="ORDER_SUBMITTING")
        self.journal("ORDER_SUBMITTED",correlation_id,eid,strategy_id=strategy_id,
                     payload={"client_order_id":intent["client_order_id"]})
        c=self.conn();c.execute("UPDATE recovery_order_intents SET submitted_ts=?,updated_ts=? WHERE execution_intent_id=?",
                                (now_iso(),now_iso(),eid));c.commit();c.close()

        url=self.base_url+f"/v3/accounts/{self.account}/orders"
        await self._throttle(critical=True)
        try:
            r=await client.request("POST",url,json=body,
                headers={"Authorization":f"Bearer {self.token}","Content-Type":"application/json"},timeout=15)
        except (httpx.TimeoutException,httpx.TransportError,asyncio.TimeoutError) as e:
            self.broker_disconnected(str(e))
            out=self.transition_intent(eid,"UNKNOWN",error=str(e),event_type="ORDER_STATUS_UNKNOWN",compromised=True)
            self.enter_safe_mode("Order submission outcome unknown; reconciliation required",correlation_id,eid,severity="CRITICAL")
            return {"status_unknown":True,"intent":out,"error":str(e)}

        try:
            payload=r.json()
        except Exception:
            payload={"raw_text":r.text[:1000]}

        # Broker answered; this is a confirmed response even if rejected.
        self.circuit_success("BROKER");self._broker_success()

        fill=payload.get("orderFillTransaction") or {}
        reject=payload.get("orderRejectTransaction") or payload.get("orderCancelTransaction") or {}
        create=payload.get("orderCreateTransaction") or {}

        if fill:
            broker_event_id=str(fill.get("id") or "")
            broker_order_id=str(fill.get("orderID") or fill.get("id") or "")
            self.journal("BROKER_ACKNOWLEDGED",correlation_id,eid,order_id=broker_order_id,
                         strategy_id=strategy_id,payload={"fill_transaction_id":broker_event_id})
            trade_opened=fill.get("tradeOpened") or {}
            broker_trade_id=str(trade_opened.get("tradeID") or "")
            broker_units=_f(trade_opened.get("units"))
            if broker_units is None:
                broker_units=_f(fill.get("units"))
            filled,remaining=conservative_filled_units(requested_units,broker_units)
            new_state="FILLED" if remaining<=1e-9 else "PARTIALLY_FILLED"
            out=self.transition_intent(
                eid,new_state,broker_order_id=broker_order_id,broker_trade_id=broker_trade_id,
                broker_event_id=broker_event_id,filled_units=filled,response=payload,
                event_type="FILL" if new_state=="FILLED" else "PARTIAL_FILL",
                compromised=(new_state=="PARTIALLY_FILLED"))
            if broker_trade_id:
                self.journal("POSITION_OPENED",correlation_id,eid,trade_id=broker_trade_id,
                             order_id=broker_order_id,strategy_id=strategy_id,
                             payload={"filled_units":filled,"unfilled_units":remaining})
            return {"response":payload,"intent":out,"orderFillTransaction":fill,
                    "partial_fill":new_state=="PARTIALLY_FILLED","filled_units":filled,"remaining_units":remaining}

        if reject or r.status_code>=400:
            broker_event_id=str(reject.get("id") or payload.get("lastTransactionID") or "")
            out=self.transition_intent(eid,"REJECTED",broker_event_id=broker_event_id,
                                       response=payload,error=str(payload),event_type="ORDER_REJECTED")
            return {"rejected":True,"response":payload,"intent":out}

        if create:
            broker_event_id=str(create.get("id") or "")
            broker_order_id=str(create.get("id") or "")
            out=self.transition_intent(eid,"ACKNOWLEDGED",broker_order_id=broker_order_id,
                                       broker_event_id=broker_event_id,response=payload,event_type="BROKER_ACKNOWLEDGED")
            return {"submitted":True,"response":payload,"intent":out}

        # Unknown/incomplete broker response. Do not resend.
        out=self.transition_intent(eid,"UNKNOWN",response=payload,error="Incomplete broker response",
                                   event_type="ORDER_STATUS_UNKNOWN",compromised=True)
        self.enter_safe_mode("Incomplete broker response; order status unknown",correlation_id,eid,severity="CRITICAL")
        return {"status_unknown":True,"intent":out,"response":payload}

    # ---------------- broker snapshot / reconciliation ----------------
    async def fetch_broker_snapshot(self,client:httpx.AsyncClient) -> Dict[str,Any]:
        self.set_state("RECOVERING","fetching authoritative broker state",safe_mode=True,new_trades_allowed=False)
        self.journal("BROKER_CONNECTED")
        account=await self.broker_request(client,"GET","/v3/accounts/{account}",critical=True)
        pending=await self.broker_request(client,"GET","/v3/accounts/{account}/pendingOrders",critical=True)
        trades=await self.broker_request(client,"GET","/v3/accounts/{account}/openTrades",critical=True)
        positions=await self.broker_request(client,"GET","/v3/accounts/{account}/openPositions",critical=True)
        a=account.get("account") or {}
        cursor=self.state().get("last_transaction_id")
        transactions=[]
        if cursor:
            try:
                tx=await self.broker_request(client,"GET","/v3/accounts/{account}/transactions/sinceid",
                                             params={"id":cursor},critical=True)
                transactions=tx.get("transactions") or []
            except Exception:
                # Snapshot remains useful; unresolved UNKNOWN intents stay UNKNOWN.
                transactions=[]
        last_id=str(account.get("lastTransactionID") or a.get("lastTransactionID") or
                    pending.get("lastTransactionID") or trades.get("lastTransactionID") or "")
        c=self.conn()
        if last_id:
            c.execute("UPDATE recovery_state SET last_transaction_id=?,updated_ts=? WHERE account_scope=?",
                      (last_id,now_iso(),self.account_scope))
        c.commit();c.close()
        return {
            "account":a,
            "pending_orders":pending.get("orders") or [],
            "open_trades":trades.get("trades") or [],
            "positions":positions.get("positions") or [],
            "transactions":transactions,
            "last_transaction_id":last_id,
        }

    @staticmethod
    def _contains_client_id(obj:Any,client_id:str) -> bool:
        try:
            return client_id in json.dumps(obj,separators=(",",":"),default=str)
        except Exception:
            return False

    def _find_broker_evidence(self,intent:Dict[str,Any],snapshot:Dict[str,Any]) -> Dict[str,Any]:
        cid=intent["client_order_id"]
        for tr in snapshot.get("open_trades",[]):
            if self._contains_client_id(tr,cid):
                return {"type":"TRADE","object":tr}
        for order in snapshot.get("pending_orders",[]):
            if self._contains_client_id(order,cid):
                return {"type":"ORDER","object":order}
        for tx in snapshot.get("transactions",[]):
            if self._contains_client_id(tx,cid):
                typ=str(tx.get("type") or "")
                if "REJECT" in typ:
                    return {"type":"REJECT","object":tx}
                if "FILL" in typ:
                    return {"type":"FILL","object":tx}
                if "CANCEL" in typ:
                    return {"type":"CANCEL","object":tx}
                return {"type":"TRANSACTION","object":tx}
        return {"type":"NONE","object":None}

    def _restore_trade_from_intent(self,intent:Dict[str,Any],broker_trade:Dict[str,Any],incident_id:Optional[str]=None):
        trade_id=str(broker_trade.get("id") or intent.get("broker_trade_id") or "")
        if not trade_id:
            return
        units=abs(_f(broker_trade.get("currentUnits"),intent.get("filled_units") or 0) or 0)
        entry=_f(broker_trade.get("price"),intent.get("entry_price")) or 0
        stop_order=broker_trade.get("stopLossOrder") or {}
        tp_order=broker_trade.get("takeProfitOrder") or {}
        stop=_f(stop_order.get("price"),intent.get("stop_loss"))
        target=_f(tp_order.get("price"),intent.get("take_profit"))
        side="BUY" if _f(broker_trade.get("currentUnits"),1)>=0 else "SELL"
        c=self.conn()
        # Restore Position Manager bookkeeping; never send a corrective order here.
        try:
            c.execute("""INSERT INTO active_trade_management(
              trade_id,instrument,side,entry,initial_stop,initial_target,current_stop,setup_variant,policy,
              opened_ts,last_r,last_action,closed,updated_ts,current_units)
              VALUES(?,?,?,?,?,?,?,?,?,?,0,'RECOVERED',0,?,?)
              ON CONFLICT(trade_id) DO UPDATE SET current_units=excluded.current_units,current_stop=excluded.current_stop,
              updated_ts=excluded.updated_ts,closed=0""",
              (trade_id,intent["symbol"],side,entry,stop,target,stop,intent.get("strategy_id"),
               "RECOVERY_SAFE",now_iso(),now_iso(),units))
        except sqlite3.OperationalError:
            # In case schema differs slightly, preserve recovery state rather than trade.
            pass

        # Restore minimal Trade Memory if the original process crashed before it could write.
        exists=c.execute("SELECT 1 FROM trade_memory WHERE trade_id=?",(trade_id,)).fetchone()
        if not exists:
            md=json.loads(intent.get("metadata_json") or "{}")
            direction="LONG" if side=="BUY" else "SHORT"
            try:
                c.execute("""INSERT INTO trade_memory(
                  trade_id,signal_id,order_id,strategy,symbol,direction,status,entry_ts,entry_price,
                  position_size,stop_loss,take_profit,market_regime_entry,volatility_state_entry,
                  trend_strength_entry,strategy_confidence_entry,director_state_entry,director_confidence_entry,
                  risk_multiplier_entry,requested_risk,approved_risk,entry_reasons_json,entry_context_json,
                  execution_context_json,risk_recommendation_json,data_quality_json,created_ts,updated_ts,
                  execution_quality_compromised,operational_incident_id)
                  VALUES(?,?,?,?,?,?,'OPEN',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (trade_id,intent.get("signal_id"),intent.get("broker_order_id"),intent.get("strategy_id"),
                   intent["symbol"],direction,broker_trade.get("openTime") or intent["created_ts"],entry,units,stop,target,
                   md.get("market_regime"),md.get("volatility_state"),md.get("trend_strength"),
                   md.get("strategy_confidence"),md.get("director_state"),md.get("director_confidence"),
                   md.get("risk_multiplier"),md.get("requested_risk"),md.get("approved_risk"),
                   _j(["RECOVERED_FROM_BROKER_AFTER_INCIDENT"]),_j(md),
                   _j({"recovered":True,"broker_trade":broker_trade}),_j(md.get("risk") or {}),
                   _j({"execution_quality_compromised":True,"recovered_from_broker":True}),
                   now_iso(),now_iso(),1,incident_id))
            except sqlite3.OperationalError:
                pass
        else:
            try:
                c.execute("""UPDATE trade_memory SET position_size=?,execution_quality_compromised=1,
                             operational_incident_id=COALESCE(operational_incident_id,?),updated_ts=?
                             WHERE trade_id=?""",(units,incident_id,now_iso(),trade_id))
            except sqlite3.OperationalError:
                pass
        try:
            md=json.loads(intent.get("metadata_json") or "{}")
            c.execute("""UPDATE trade_memory SET
                         strategy_version=COALESCE(strategy_version,?),
                         risk_config_version=COALESCE(risk_config_version,?),
                         director_version=COALESCE(director_version,?),
                         regime_model_version=COALESCE(regime_model_version,?),
                         deployment_version=COALESCE(deployment_version,?),
                         runtime_code_hash=COALESCE(runtime_code_hash,?),
                         dependency_lock_hash=COALESCE(dependency_lock_hash,?),
                         config_snapshot_hash=COALESCE(config_snapshot_hash,?)
                         WHERE trade_id=?""",
                      (md.get("strategy_version"),md.get("risk_config_version"),md.get("director_version"),
                       md.get("regime_model_version"),md.get("deployment_version"),md.get("runtime_code_hash"),
                       md.get("dependency_lock_hash"),md.get("config_snapshot_hash"),trade_id))
        except Exception:
            pass
        c.commit();c.close()
        self.journal("POSITION_STATE_RESTORED",intent.get("correlation_id"),
                     intent.get("execution_intent_id"),trade_id=trade_id,
                     order_id=intent.get("broker_order_id"),strategy_id=intent.get("strategy_id"),
                     payload={"current_units":units,"entry":entry,"stop":stop,"target":target,
                              "execution_quality_compromised":True})

    def _record_reconciliation_item(self,recon_id,item_type,entity_id,status,severity,internal,broker,reason):
        c=self.conn()
        c.execute("""INSERT INTO recovery_reconciliation_items(
          reconciliation_id,item_type,entity_id,status,severity,internal_json,broker_json,reason)
          VALUES(?,?,?,?,?,?,?,?)""",
          (recon_id,item_type,entity_id,status,severity,_j(internal or {}),_j(broker or {}),reason))
        c.commit();c.close()

    def _internal_open_positions(self) -> Dict[str,Dict[str,Any]]:
        c=self.conn()
        try:
            rows=[dict(x) for x in c.execute("""SELECT a.*,tm.position_size FROM active_trade_management a
               LEFT JOIN trade_memory tm ON tm.trade_id=a.trade_id WHERE a.closed=0""").fetchall()]
        except Exception:
            rows=[dict(x) for x in c.execute("SELECT * FROM active_trade_management WHERE closed=0").fetchall()]
        c.close()
        return {str(x["trade_id"]):x for x in rows}

    def _quarantine_missing_internal_trade(self, trade_id: str, internal: Dict[str,Any]) -> bool:
        """Quarantine a stale *local* open trade without inventing a broker close.

        This is deliberately available only when the caller configured
        ``allow_orphan_quarantine`` (practice accounts).  The record is not marked
        CLOSED and therefore cannot become a fabricated win/loss.  It is removed
        from active position/risk management while preserving the full audit trail.
        """
        if not self.allow_orphan_quarantine:
            return False
        c=self.conn()
        try:
            # Do not quarantine an unresolved submission/order state.  Those remain
            # safety-critical until broker evidence resolves them.
            unresolved=c.execute("""SELECT 1 FROM recovery_order_intents
                WHERE account_scope=? AND broker_trade_id=?
                  AND state IN ('SUBMITTING','SUBMITTED','ACKNOWLEDGED','PARTIALLY_FILLED','UNKNOWN')
                LIMIT 1""",(self.account_scope,str(trade_id))).fetchone()
            if unresolved:
                return False
            ts=now_iso()
            c.execute("UPDATE active_trade_management SET closed=1,last_action='BROKER_MISSING_QUARANTINED',updated_ts=? WHERE trade_id=? AND closed=0",
                      (ts,str(trade_id)))
            # BROKER_MISSING is intentionally non-CLOSED: learning/performance code
            # must not treat an unknown outcome as a resolved trade.
            try:
                row=c.execute("SELECT data_quality_json FROM trade_memory WHERE trade_id=?",(str(trade_id),)).fetchone()
                quality={}
                if row and row['data_quality_json']:
                    try: quality=json.loads(row['data_quality_json'])
                    except Exception: quality={}
                quality.update({
                    'broker_trade_missing':True,
                    'broker_exit_unverified':True,
                    'excluded_from_learning':True,
                    'quarantined_ts':ts,
                })
                c.execute("""UPDATE trade_memory SET status='BROKER_MISSING',
                    execution_quality_compromised=1,data_quality_json=?,updated_ts=?
                    WHERE trade_id=? AND status='OPEN'""",(_j(quality),ts,str(trade_id)))
            except sqlite3.OperationalError:
                pass
            c.commit()
            self.journal('POSITION_ORPHAN_QUARANTINED',trade_id=str(trade_id),
                         strategy_id=internal.get('setup_variant'),
                         payload={'reason':'internal open trade absent from authoritative broker open-trade snapshot',
                                  'learning_outcome_invented':False})
            return True
        finally:
            c.close()

    def reconcile_snapshot(self,snapshot:Dict[str,Any]) -> Dict[str,Any]:
        self.set_state("RECONCILING","comparing internal state with broker",safe_mode=True,new_trades_allowed=False)
        recon_id="recon_"+uuid.uuid4().hex
        a=snapshot.get("account") or {}
        c=self.conn()
        c.execute("""INSERT INTO recovery_reconciliation_runs(
          reconciliation_id,account_scope,started_ts,status,broker_transaction_id,broker_balance,broker_nav,broker_margin_used)
          VALUES(?,?,?,'RUNNING',?,?,?,?)""",
          (recon_id,self.account_scope,now_iso(),snapshot.get("last_transaction_id"),
           _f(a.get("balance")),_f(a.get("NAV")),_f(a.get("marginUsed"))))
        c.commit();c.close()
        self.journal("RECONCILIATION_STARTED",payload={"reconciliation_id":recon_id})

        severity_rank={"MATCHED":0,"MINOR_MISMATCH":1,"RECONCILIATION_REQUIRED":2,"CRITICAL_MISMATCH":3}
        worst="MATCHED";counts={x:0 for x in RECONCILIATION_STATES}
        recovered_intents=[]

        # Resolve UNKNOWN/SUBMITTING intents from broker evidence first.
        c=self.conn()
        intents=[dict(x) for x in c.execute("""SELECT * FROM recovery_order_intents
          WHERE account_scope=? AND state IN ('SUBMITTING','SUBMITTED','ACKNOWLEDGED','PARTIALLY_FILLED','UNKNOWN')
          ORDER BY created_ts""",(self.account_scope,)).fetchall()]
        c.close()

        for intent in intents:
            evidence=self._find_broker_evidence(intent,snapshot)
            typ=evidence["type"];obj=evidence["object"] or {}
            status="MATCHED";reason="broker evidence matches known intent"
            if typ=="TRADE":
                units=_f(obj.get("currentUnits"),intent["filled_units"])
                filled,remaining=conservative_filled_units(intent["requested_units"],units)
                st="FILLED" if remaining<=1e-9 else "PARTIALLY_FILLED"
                trade_id=str(obj.get("id") or "")
                out=self.transition_intent(intent["execution_intent_id"],st,broker_trade_id=trade_id,
                                           filled_units=filled,response=obj,event_type="ORDER_FOUND_FILLED",
                                           compromised=(st=="PARTIALLY_FILLED"))
                inc=self.open_incident("ORDER_RECOVERY","HIGH","Unknown/submitted order recovered from broker",
                                       intent.get("correlation_id"),intent["execution_intent_id"])
                self._restore_trade_from_intent(out,obj,inc)
                recovered_intents.append(out["execution_intent_id"])
                if st=="PARTIALLY_FILLED":
                    status="MINOR_MISMATCH";reason=f"partial fill confirmed: requested={intent['requested_units']} filled={filled} unfilled={remaining}"
            elif typ=="ORDER":
                oid=str(obj.get("id") or "")
                self.transition_intent(intent["execution_intent_id"],"ACKNOWLEDGED",broker_order_id=oid,
                                       response=obj,event_type="ORDER_FOUND_PENDING")
            elif typ=="FILL":
                units=_f(obj.get("units"))
                filled,remaining=conservative_filled_units(intent["requested_units"],units)
                st="FILLED" if remaining<=1e-9 else "PARTIALLY_FILLED"
                self.transition_intent(intent["execution_intent_id"],st,broker_event_id=str(obj.get("id") or ""),
                                       filled_units=filled,response=obj,event_type="ORDER_FOUND_FILL_TRANSACTION",
                                       compromised=(st=="PARTIALLY_FILLED"))
                status="MINOR_MISMATCH" if remaining>0 else "MATCHED"
                reason="fill recovered from transaction history"
            elif typ=="REJECT":
                self.transition_intent(intent["execution_intent_id"],"REJECTED",broker_event_id=str(obj.get("id") or ""),
                                       response=obj,event_type="ORDER_FOUND_REJECTED")
            elif typ=="CANCEL":
                self.transition_intent(intent["execution_intent_id"],"CANCELLED",broker_event_id=str(obj.get("id") or ""),
                                       response=obj,event_type="ORDER_FOUND_CANCELLED")
            elif typ=="NONE" and intent["state"]=="UNKNOWN":
                status="RECONCILIATION_REQUIRED";reason="unknown order not found in authoritative snapshot; do not resend"
            self._record_reconciliation_item(recon_id,"ORDER_INTENT",intent["execution_intent_id"],
                                             status,"HIGH" if status=="RECONCILIATION_REQUIRED" else "INFO",
                                             intent,obj,reason)
            counts[status]+=1
            if severity_rank[status]>severity_rank[worst]:worst=status

        # Position reconciliation. Broker open Trade is authoritative.
        internal=self._internal_open_positions()
        broker_trades={str(x.get("id")):x for x in snapshot.get("open_trades",[]) if x.get("id")}
        for tid,btr in broker_trades.items():
            intr=internal.get(tid)
            if not intr:
                status="CRITICAL_MISMATCH";reason="broker has open trade missing internally"
                self._record_reconciliation_item(recon_id,"POSITION",tid,status,"CRITICAL",{},btr,reason)
                counts[status]+=1;worst=status
                # Restore only if linked to a known intent; never create a corrective market order.
                linked=None
                c=self.conn()
                linked=c.execute("SELECT * FROM recovery_order_intents WHERE account_scope=? AND broker_trade_id=?",
                                 (self.account_scope,tid)).fetchone()
                c.close()
                if linked:
                    inc=self.open_incident("POSITION_MISMATCH","CRITICAL",reason,linked["correlation_id"],linked["execution_intent_id"])
                    self._restore_trade_from_intent(dict(linked),btr,inc)
            else:
                b_units=abs(_f(btr.get("currentUnits"),0) or 0)
                i_units=abs(_f(intr.get("current_units"),intr.get("position_size")) or 0)
                if i_units and abs(i_units-b_units)>1e-6:
                    status="RECONCILIATION_REQUIRED";reason=f"position units differ internal={i_units} broker={b_units}"
                    try:
                        c=self.conn();c.execute("UPDATE active_trade_management SET current_units=?,updated_ts=? WHERE trade_id=?",
                                                (b_units,now_iso(),tid));c.commit();c.close()
                    except Exception:
                        pass
                else:
                    status="MATCHED";reason="position matched"
                self._record_reconciliation_item(recon_id,"POSITION",tid,status,
                                                 "HIGH" if status!="MATCHED" else "INFO",intr,btr,reason)
                counts[status]+=1
                if severity_rank[status]>severity_rank[worst]:worst=status

            # Protective orders are mandatory for bot-managed trades.
            sl=bool(btr.get("stopLossOrder"));tp=bool(btr.get("takeProfitOrder"))
            if not sl or not tp:
                status="CRITICAL_MISMATCH";reason=f"protective order missing stop={sl} take_profit={tp}"
                self._record_reconciliation_item(recon_id,"PROTECTIVE_ORDER",tid,status,"CRITICAL",{},btr,reason)
                counts[status]+=1;worst="CRITICAL_MISMATCH"

        for tid,intr in internal.items():
            if tid not in broker_trades:
                # Never infer a profitable/loss-making close from absence alone.  On
                # practice accounts we may quarantine a stale local row so it cannot
                # poison risk/recovery forever; production keeps the hard block.
                quarantined=self._quarantine_missing_internal_trade(tid,intr)
                if quarantined:
                    status="MINOR_MISMATCH";reason="stale local trade quarantined; broker outcome remains unknown and excluded from learning"
                    severity="INFO"
                else:
                    status="RECONCILIATION_REQUIRED";reason="internal open trade absent from broker open-trade snapshot; broker close not proven"
                    severity="HIGH"
                self._record_reconciliation_item(recon_id,"POSITION",tid,status,severity,intr,{},reason)
                counts[status]+=1
                if severity_rank[status]>severity_rank[worst]:worst=status

        # Balance/equity reconciliation: broker is source of truth.
        c=self.conn()
        local=c.execute("SELECT * FROM portfolio_risk_state WHERE id=1").fetchone()
        c.close()
        if local:
            l=dict(local);bn=_f(a.get("NAV"));ln=_f(l.get("nav"))
            if bn is not None and ln is not None:
                diff=abs(bn-ln)/max(abs(bn),1e-9)
                status="RECONCILIATION_REQUIRED" if diff>0.02 else ("MINOR_MISMATCH" if diff>0.005 else "MATCHED")
                self._record_reconciliation_item(recon_id,"EQUITY","NAV",status,
                                                 "HIGH" if status=="RECONCILIATION_REQUIRED" else "INFO",
                                                 l,{"NAV":bn,"balance":a.get("balance"),"marginUsed":a.get("marginUsed")},
                                                 f"NAV relative difference={diff:.4f}")
                counts[status]+=1
                if severity_rank[status]>severity_rank[worst]:worst=status

        summary={"status":worst,"counts":counts,"recovered_intents":recovered_intents,
                 "broker_open_trades":len(broker_trades),"internal_open_trades":len(internal)}
        c=self.conn()
        c.execute("""UPDATE recovery_reconciliation_runs SET completed_ts=?,status=?,summary_json=?
                     WHERE reconciliation_id=?""",(now_iso(),worst,_j(summary),recon_id))
        c.execute("""UPDATE recovery_state SET last_reconciliation_ts=?,last_reconciliation_status=?,
                     updated_ts=? WHERE account_scope=?""",(now_iso(),worst,now_iso(),self.account_scope))
        c.commit();c.close()
        self.journal("RECONCILIATION_COMPLETED",payload={"reconciliation_id":recon_id,**summary})

        if worst in ("CRITICAL_MISMATCH","RECONCILIATION_REQUIRED"):
            self.enter_safe_mode(f"Reconciliation status {worst}",severity="CRITICAL" if worst=="CRITICAL_MISMATCH" else "HIGH")
        return {"reconciliation_id":recon_id,**summary}

    async def reconnect_and_reconcile(self,client:httpx.AsyncClient,max_attempts:int=5) -> Dict[str,Any]:
        self.set_state("RECOVERING","broker reconnect sequence",safe_mode=True,new_trades_allowed=False)
        last=None
        for attempt in range(max(1,max_attempts)):
            try:
                snap=await self.fetch_broker_snapshot(client)
                self.journal("BROKER_RECONNECTED",payload={"attempt":attempt+1})
                rec=self.reconcile_snapshot(snap)
                return {"connected":True,"snapshot":snap,"reconciliation":rec}
            except Exception as e:
                last=str(e);self.broker_disconnected(last)
                delay=min(self.backoff_cap,self.backoff_base*(2**attempt))+random.uniform(0,self.backoff_base)
                await asyncio.sleep(delay)
        self.set_state("CRITICAL_FAILURE",f"broker reconnect exhausted: {last}",safe_mode=True,new_trades_allowed=False)
        return {"connected":False,"error":last}

    # ---------------- market/risk/deployment startup stages ----------------
    def startup_stage(self,stage:str,status:str="OK",details:Optional[Dict[str,Any]]=None):
        self.journal(stage,payload={"status":status,**(details or {})})

    def market_data_update(self,timestamp:str,reliable:bool=True):
        c=self.conn()
        c.execute("UPDATE recovery_state SET last_market_data_ts=?,updated_ts=? WHERE account_scope=?",
                  (timestamp,now_iso(),self.account_scope));c.commit();c.close()
        if not reliable:
            self._set_circuit("MARKET_DATA","OPEN",self.circuit_failure_threshold,"stale/unreliable market data")
            self.enter_safe_mode("MARKET_DATA_UNRELIABLE",severity="CRITICAL")
            self.journal("MARKET_DATA_UNRELIABLE",payload={"timestamp":timestamp})
        else:
            cb=self.circuit("MARKET_DATA")
            if cb.get("state")!="CLOSED":
                self.circuit_success("MARKET_DATA")
                self.journal("MARKET_DATA_RECOVERED",payload={"timestamp":timestamp})

    def verify_risk(self,ok:bool,details:Optional[Dict[str,Any]]=None):
        c=self.conn()
        if ok:
            c.execute("UPDATE recovery_state SET last_risk_verified_ts=?,updated_ts=? WHERE account_scope=?",
                      (now_iso(),now_iso(),self.account_scope))
        c.commit();c.close()
        self.journal("RISK_VERIFIED" if ok else "RISK_VERIFICATION_FAILED",payload=details or {})
        if not ok:
            self.enter_safe_mode("Risk Engine verification failed",severity="CRITICAL")

    def mark_execution_compromised(self,execution_intent_id:str,incident_id:Optional[str]=None):
        c=self.conn()
        row=c.execute("SELECT * FROM recovery_order_intents WHERE execution_intent_id=?",(execution_intent_id,)).fetchone()
        if row:
            c.execute("UPDATE recovery_order_intents SET execution_quality_compromised=1,updated_ts=? WHERE execution_intent_id=?",
                      (now_iso(),execution_intent_id))
            if row["broker_trade_id"]:
                try:
                    c.execute("""UPDATE trade_memory SET execution_quality_compromised=1,
                                 operational_incident_id=COALESCE(operational_incident_id,?),updated_ts=?
                                 WHERE trade_id=?""",(incident_id,now_iso(),row["broker_trade_id"]))
                except sqlite3.OperationalError:
                    pass
        c.commit();c.close()

    # ---------------- metrics / timeline ----------------
    def metrics(self) -> Dict[str,Any]:
        c=self.conn()
        incidents=[dict(x) for x in c.execute("SELECT * FROM recovery_incidents WHERE account_scope=?",(self.account_scope,)).fetchall()]
        recons=[dict(x) for x in c.execute("SELECT * FROM recovery_reconciliation_runs WHERE account_scope=?",(self.account_scope,)).fetchall()]
        dup=c.execute("""SELECT COUNT(*) n FROM recovery_event_journal WHERE account_scope=?
                         AND event_type='DUPLICATE_ORDER_PREVENTED'""",(self.account_scope,)).fetchone()["n"]
        unk=c.execute("""SELECT COUNT(*) n FROM recovery_order_intents WHERE account_scope=? AND state='UNKNOWN'""",
                      (self.account_scope,)).fetchone()["n"]
        pm=c.execute("""SELECT COUNT(*) n FROM recovery_reconciliation_items ri JOIN recovery_reconciliation_runs rr
                        ON rr.reconciliation_id=ri.reconciliation_id WHERE rr.account_scope=?
                        AND ri.item_type='POSITION' AND ri.status!='MATCHED'""",(self.account_scope,)).fetchone()["n"]
        c.close()
        recovered=[x for x in incidents if x["status"]=="RECOVERED" and x.get("recovered_ts")]
        durations=[]
        for x in recovered:
            a=_dt(x["started_ts"]);b=_dt(x["recovered_ts"])
            if a and b:durations.append((b-a).total_seconds())
        rate=len(recovered)/len(incidents) if incidents else 1.0
        recon_fail=sum(1 for x in recons if x["status"] in ("RECONCILIATION_REQUIRED","CRITICAL_MISMATCH"))
        st=self.state()
        bdur=0.0
        if st.get("broker_disconnect_started_ts"):
            a=_dt(st["broker_disconnect_started_ts"])
            if a:bdur=(datetime.now(timezone.utc)-a).total_seconds()
        sdur=0.0
        if st.get("incident_started_ts"):
            a=_dt(st["incident_started_ts"])
            if a:sdur=(datetime.now(timezone.utc)-a).total_seconds()
        result={"incidents":len(incidents),"reconciliation_failures":recon_fail,
                "recovery_success_rate":rate,
                "mean_time_to_recovery_seconds":sum(durations)/len(durations) if durations else None,
                "duplicate_orders_prevented":int(dup),"unknown_order_states":int(unk),
                "position_mismatches":int(pm),"broker_disconnect_duration_seconds":bdur,
                "safe_mode_seconds":sdur}
        c=self.conn()
        c.execute("""INSERT INTO recovery_metrics(
          ts,account_scope,incidents_total,reconciliation_failures,recovery_success_rate,
          mean_time_to_recovery_seconds,duplicate_orders_prevented,unknown_order_states,
          position_mismatches,broker_disconnect_duration_seconds,safe_mode_seconds,details_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          (now_iso(),self.account_scope,result["incidents"],recon_fail,rate,
           result["mean_time_to_recovery_seconds"],dup,unk,pm,bdur,sdur,_j(result)))
        c.commit();c.close()
        return result

    def timeline(self,correlation_id:Optional[str]=None,execution_intent_id:Optional[str]=None,limit:int=500) -> List[Dict[str,Any]]:
        c=self.conn();where=["account_scope=?"];params=[self.account_scope]
        if correlation_id:
            where.append("correlation_id=?");params.append(correlation_id)
        if execution_intent_id:
            where.append("execution_intent_id=?");params.append(execution_intent_id)
        params.append(min(max(int(limit),1),2000))
        rows=[dict(x) for x in c.execute(
            "SELECT * FROM recovery_event_journal WHERE "+" AND ".join(where)+" ORDER BY id LIMIT ?",tuple(params)
        ).fetchall()]
        c.close();return rows

    def orders(self,limit=200):
        c=self.conn();rows=[dict(x) for x in c.execute("""SELECT * FROM recovery_order_intents
          WHERE account_scope=? ORDER BY created_ts DESC LIMIT ?""",(self.account_scope,min(max(limit,1),1000))).fetchall()]
        c.close();return rows

    def incidents(self,limit=200):
        c=self.conn();rows=[dict(x) for x in c.execute("""SELECT * FROM recovery_incidents
          WHERE account_scope=? ORDER BY started_ts DESC LIMIT ?""",(self.account_scope,min(max(limit,1),1000))).fetchall()]
        c.close();return rows

from __future__ import annotations
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timezone, timedelta
import sqlite3, json, os, time, math, shutil, resource, uuid, re

def sanitize_observability(value):
    secret_re=re.compile(r"(authorization|api[_-]?key|token|password|passwd|secret|credential|private[_-]?key|access[_-]?key)",re.I)
    bearer_re=re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{8,}")
    if isinstance(value,dict):
        return {k:("[REDACTED]" if secret_re.search(str(k)) else sanitize_observability(v)) for k,v in value.items()}
    if isinstance(value,list):
        return [sanitize_observability(x) for x in value]
    if isinstance(value,tuple):
        return [sanitize_observability(x) for x in value]
    if isinstance(value,str):
        return bearer_re.sub("Bearer [REDACTED]",value)
    return value


HEALTH_STATES=("HEALTHY","DEGRADED","WARNING","CRITICAL","TRADING_PAUSED","EMERGENCY_STOP")
MODULE_STATES=("OK","DEGRADED","STALE","ERROR","OFFLINE","PAUSED")
SEVERITIES=("INFO","WARNING","HIGH","CRITICAL")
DEPENDENCY_CRITICAL="CRITICAL"
DEPENDENCY_IMPORTANT="IMPORTANT"
DEPENDENCY_NON_CRITICAL="NON_CRITICAL"
SEVERITY_RANK={"INFO":0,"WARNING":1,"HIGH":2,"CRITICAL":3}
MODULE_BAD_RANK={"OK":0,"DEGRADED":1,"STALE":2,"PAUSED":2,"ERROR":3,"OFFLINE":4}


def utcnow(): return datetime.now(timezone.utc)
def now_iso(): return utcnow().isoformat()

def parse_ts(x):
    if not x:return None
    try:return datetime.fromisoformat(str(x).replace("Z","+00:00"))
    except Exception:return None

def safe_float(x,default=None):
    try:
        v=float(x); return v if math.isfinite(v) else default
    except Exception:return default


def stale_status(last_update: Optional[str], stale_after_seconds: float, offline_after_seconds: Optional[float]=None) -> Dict[str,Any]:
    dt=parse_ts(last_update)
    if not dt:return {"status":"OFFLINE","age_seconds":None}
    age=(utcnow()-dt).total_seconds()
    offline=float(offline_after_seconds if offline_after_seconds is not None else stale_after_seconds*3)
    if age>offline:return {"status":"OFFLINE","age_seconds":age}
    if age>stale_after_seconds:return {"status":"STALE","age_seconds":age}
    return {"status":"OK","age_seconds":age}


def compute_system_health(modules: List[Dict[str,Any]], trading_paused=False, emergency_stop=False) -> Dict[str,Any]:
    if emergency_stop:
        return {"status":"EMERGENCY_STOP","reasons":["emergency_stop_active"]}
    critical=[];important=[]
    for m in modules:
        st=str(m.get("status") or "OFFLINE")
        dep=str(m.get("dependency_class") or DEPENDENCY_NON_CRITICAL)
        if dep==DEPENDENCY_CRITICAL and st in ("ERROR","OFFLINE","STALE"):
            critical.append(f"{m.get('module_name')}:{st}")
        elif dep in (DEPENDENCY_CRITICAL,DEPENDENCY_IMPORTANT) and st in ("DEGRADED","PAUSED","ERROR","OFFLINE","STALE"):
            important.append(f"{m.get('module_name')}:{st}")
    if critical:return {"status":"CRITICAL","reasons":critical}
    if trading_paused:return {"status":"TRADING_PAUSED","reasons":["trading_paused"]+important}
    if important:return {"status":"WARNING","reasons":important}
    degraded=[f"{m.get('module_name')}:{m.get('status')}" for m in modules if m.get("status")!="OK"]
    if degraded:return {"status":"DEGRADED","reasons":degraded}
    return {"status":"HEALTHY","reasons":[]}


def reconciliation_status(internal_instruments: List[str], broker_instruments: List[str]) -> Dict[str,Any]:
    internal=set(x for x in internal_instruments if x)
    broker=set(x for x in broker_instruments if x)
    missing_internal=sorted(broker-internal)
    missing_broker=sorted(internal-broker)
    ok=not missing_internal and not missing_broker
    return {"status":"CONSISTENT" if ok else "STATE_RECONCILIATION_REQUIRED",
            "broker_only":missing_internal,"internal_only":missing_broker}


def degradation_state(hist_expectancy, recent_expectancy, hist_pf, recent_pf, concept_drift=False) -> str:
    if concept_drift:return "CRITICAL_DEGRADATION"
    he=safe_float(hist_expectancy);re=safe_float(recent_expectancy);hp=safe_float(hist_pf);rp=safe_float(recent_pf)
    if he is not None and re is not None and he>0 and re<0 and rp is not None and rp<1:return "CRITICAL_DEGRADATION"
    if (he is not None and re is not None and re<he*.5) or (hp is not None and rp is not None and rp<hp*.7):return "DEGRADING"
    if (he is not None and re is not None and re<he*.75) or (hp is not None and rp is not None and rp<hp*.85):return "WATCH"
    return "NORMAL"


def drawdown_alert_state(drawdown: Optional[float], warning: float, critical: float) -> str:
    d=safe_float(drawdown)
    if d is None:return "UNKNOWN"
    if d>=critical:return "CRITICAL"
    if d>=warning:return "WARNING"
    return "OK"

def latency_alert_state(latency_ms: Optional[float], warning_ms: float, critical_ms: float) -> str:
    v=safe_float(latency_ms)
    if v is None:return "UNKNOWN"
    if v>=critical_ms:return "CRITICAL"
    if v>=warning_ms:return "WARNING"
    return "OK"


class ObservabilityManager:
    def __init__(self,db_path:str,version:str,alert_cooldown_seconds:int=900,
                 heartbeat_retention:int=5000,metrics_retention:int=10000):
        self.db_path=db_path;self.version=version;self.alert_cooldown_seconds=max(10,int(alert_cooldown_seconds))
        self.heartbeat_retention=heartbeat_retention;self.metrics_retention=metrics_retention
        self.started=utcnow();self._cpu_prev=None;self._wall_prev=None
        self._event_loop_lag_ms=0.0

    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30)
        c.row_factory=sqlite3.Row;c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA synchronous=NORMAL")
        return c

    def ensure_schema(self):
        c=self.conn();c.executescript("""
        CREATE TABLE IF NOT EXISTS observability_module_health(
          module_name TEXT PRIMARY KEY,dependency_class TEXT NOT NULL,status TEXT NOT NULL,last_update TEXT,
          latency_ms REAL,errors_json TEXT NOT NULL DEFAULT '[]',warnings_json TEXT NOT NULL DEFAULT '[]',
          last_successful_operation TEXT,failure_count INTEGER NOT NULL DEFAULT 0,started_ts TEXT,uptime_seconds REAL,
          version TEXT,heartbeat_ts TEXT,details_json TEXT NOT NULL DEFAULT '{}',updated_ts TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS observability_heartbeats(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,module_name TEXT NOT NULL,status TEXT NOT NULL,
          latency_ms REAL,details_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS observability_alerts(
          alert_key TEXT PRIMARY KEY,first_seen TEXT NOT NULL,last_seen TEXT NOT NULL,last_notified TEXT,severity TEXT NOT NULL,
          status TEXT NOT NULL,module TEXT NOT NULL,event_type TEXT NOT NULL,count INTEGER NOT NULL DEFAULT 1,
          message TEXT NOT NULL,group_key TEXT,details_json TEXT NOT NULL DEFAULT '{}',correlation_id TEXT,recovered_ts TEXT);
        CREATE TABLE IF NOT EXISTS observability_alert_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,alert_key TEXT NOT NULL,severity TEXT NOT NULL,status TEXT NOT NULL,
          module TEXT NOT NULL,event_type TEXT NOT NULL,message TEXT NOT NULL,details_json TEXT NOT NULL DEFAULT '{}',correlation_id TEXT);
        CREATE TABLE IF NOT EXISTS observability_metrics(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,cpu_percent REAL,memory_rss_mb REAL,memory_percent REAL,
          disk_used_percent REAL,event_loop_lag_ms REAL,queue_depth INTEGER,processing_time_ms REAL,db_latency_ms REAL,
          broker_latency_ms REAL,market_data_latency_ms REAL,details_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS observability_traces(
          correlation_id TEXT PRIMARY KEY,signal_id INTEGER,decision_id INTEGER,risk_decision_id INTEGER,order_id TEXT,trade_id TEXT,
          candidate_id TEXT,strategy_id TEXT,symbol TEXT,created_ts TEXT NOT NULL,market_data_ts TEXT,signal_ts TEXT,director_ts TEXT,
          risk_ts TEXT,order_created_ts TEXT,order_sent_ts TEXT,broker_ack_ts TEXT,fill_ts TEXT,trade_memory_ts TEXT,completed_ts TEXT,
          signal_latency_ms REAL,decision_latency_ms REAL,risk_latency_ms REAL,execution_latency_ms REAL,broker_latency_ms REAL,
          total_latency_ms REAL,context_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS observability_structured_logs(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,level TEXT NOT NULL,module TEXT NOT NULL,event_type TEXT NOT NULL,
          strategy_id TEXT,trade_id TEXT,decision_id TEXT,correlation_id TEXT,symbol TEXT,message TEXT NOT NULL,
          metrics_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS observability_capital_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,source TEXT NOT NULL,equity REAL,cash REAL,unrealized_pnl REAL,
          realized_pnl REAL,daily_pnl REAL,weekly_pnl REAL,drawdown REAL,peak_equity REAL,exposure REAL,margin_usage REAL,
          open_risk REAL,remaining_risk_budget REAL,details_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS observability_startup_checks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,status TEXT NOT NULL,checks_json TEXT NOT NULL,
          reconciliation_json TEXT NOT NULL DEFAULT '{}',details_json TEXT NOT NULL DEFAULT '{}');
        CREATE INDEX IF NOT EXISTS idx_obs_hb_module_ts ON observability_heartbeats(module_name,ts);
        CREATE INDEX IF NOT EXISTS idx_obs_alert_status ON observability_alerts(status,severity,last_seen);
        CREATE INDEX IF NOT EXISTS idx_obs_metrics_ts ON observability_metrics(ts);
        CREATE INDEX IF NOT EXISTS idx_obs_logs_trace ON observability_structured_logs(correlation_id,ts);
        CREATE INDEX IF NOT EXISTS idx_obs_capital_ts ON observability_capital_history(ts);
        """);c.commit();c.close()

    def begin_session(self):
        ts=now_iso();c=self.conn();c.execute("UPDATE observability_module_health SET started_ts=?,uptime_seconds=0,updated_ts=?",(ts,ts));c.commit();c.close()
        self.started=utcnow();self._cpu_prev=None;self._wall_prev=None

    def structured_log(self,level,module,event_type,message,*,strategy_id=None,trade_id=None,decision_id=None,
                       correlation_id=None,symbol=None,metrics=None):
        message=sanitize_observability(message)
        metrics=sanitize_observability(metrics or {})
        c=self.conn();c.execute("""INSERT INTO observability_structured_logs(ts,level,module,event_type,strategy_id,trade_id,
          decision_id,correlation_id,symbol,message,metrics_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
          (now_iso(),level,module,event_type,strategy_id,trade_id,str(decision_id) if decision_id is not None else None,
           correlation_id,symbol,message,json.dumps(metrics,separators=(",",":"),default=str)))
        c.commit();c.close()

    def heartbeat(self,module_name,dependency_class,status="OK",latency_ms=None,errors=None,warnings=None,
                  last_successful_operation=None,details=None):
        ts=now_iso();errors=sanitize_observability(list(errors or []));warnings=sanitize_observability(list(warnings or []));details=sanitize_observability(details or {})
        c=self.conn();prev=c.execute("SELECT * FROM observability_module_health WHERE module_name=?",(module_name,)).fetchone()
        failures=int(prev["failure_count"] if prev else 0)
        if status in ("ERROR","OFFLINE","STALE"):failures+=1
        elif status=="OK":failures=0
        started=(prev["started_ts"] if prev and prev["started_ts"] else ts)
        sd=parse_ts(started);uptime=(utcnow()-sd).total_seconds() if sd else 0
        last_ok=last_successful_operation or (ts if status=="OK" else (prev["last_successful_operation"] if prev else None))
        c.execute("""INSERT INTO observability_module_health(module_name,dependency_class,status,last_update,latency_ms,errors_json,
          warnings_json,last_successful_operation,failure_count,started_ts,uptime_seconds,version,heartbeat_ts,details_json,updated_ts)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(module_name) DO UPDATE SET dependency_class=excluded.dependency_class,
          status=excluded.status,last_update=excluded.last_update,latency_ms=excluded.latency_ms,errors_json=excluded.errors_json,
          warnings_json=excluded.warnings_json,last_successful_operation=excluded.last_successful_operation,
          failure_count=excluded.failure_count,uptime_seconds=excluded.uptime_seconds,version=excluded.version,
          heartbeat_ts=excluded.heartbeat_ts,details_json=excluded.details_json,updated_ts=excluded.updated_ts""",
          (module_name,dependency_class,status,ts,latency_ms,json.dumps(errors),json.dumps(warnings),last_ok,failures,
           started,uptime,self.version,ts,json.dumps(details,separators=(",",":"),default=str),ts))
        c.execute("INSERT INTO observability_heartbeats(ts,module_name,status,latency_ms,details_json) VALUES(?,?,?,?,?)",
                  (ts,module_name,status,latency_ms,json.dumps(details,separators=(",",":"),default=str)))
        c.commit();c.close()
        return {"module_name":module_name,"status":status,"failure_count":failures,"last_update":ts}

    def mark_stale_modules(self,thresholds: Dict[str,float],offline_multiplier=3.0):
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM observability_module_health").fetchall()];c.close()
        out=[]
        for r in rows:
            th=thresholds.get(r["module_name"])
            if th is None:continue
            st=stale_status(r.get("heartbeat_ts"),th,th*offline_multiplier)
            if st["status"]!=r["status"] and st["status"] in ("STALE","OFFLINE"):
                self.heartbeat(r["module_name"],r["dependency_class"],st["status"],errors=["heartbeat missed"],
                               details={"heartbeat_age_seconds":st["age_seconds"]})
                sev="CRITICAL" if r["dependency_class"]==DEPENDENCY_CRITICAL else "HIGH"
                self.alert(f"HEARTBEAT:{r['module_name']}",sev,r["module_name"],"MODULE_HEARTBEAT_MISSED",
                           f"{r['module_name']} heartbeat is {st['status'].lower()}",
                           details={"age_seconds":st["age_seconds"],"threshold_seconds":th})
            out.append({"module_name":r["module_name"],**st})
        return out

    def alert(self,key,severity,module,event_type,message,*,group_key=None,details=None,correlation_id=None,cooldown_seconds=None):
        sev=severity if severity in SEVERITIES else "WARNING";now=utcnow();ts=now.isoformat();cool=int(cooldown_seconds or self.alert_cooldown_seconds)
        message=sanitize_observability(message);details=sanitize_observability(details or {})
        c=self.conn();prev=c.execute("SELECT * FROM observability_alerts WHERE alert_key=?",(key,)).fetchone()
        notify=True;status="ACTIVE";count=1;first=ts
        if prev:
            count=int(prev["count"])+1;first=prev["first_seen"]
            last_not=parse_ts(prev["last_notified"])
            same_active=prev["status"]=="ACTIVE"
            escalated=SEVERITY_RANK[sev]>SEVERITY_RANK.get(prev["severity"],0)
            notify=(not same_active) or escalated or (last_not is None) or ((now-last_not).total_seconds()>=cool)
        last_notified=ts if notify else (prev["last_notified"] if prev else ts)
        c.execute("""INSERT INTO observability_alerts(alert_key,first_seen,last_seen,last_notified,severity,status,module,event_type,
          count,message,group_key,details_json,correlation_id,recovered_ts) VALUES(?,?,?,?,?,'ACTIVE',?,?,?,?,?,?,?,NULL)
          ON CONFLICT(alert_key) DO UPDATE SET last_seen=excluded.last_seen,last_notified=excluded.last_notified,severity=excluded.severity,
          status='ACTIVE',module=excluded.module,event_type=excluded.event_type,count=excluded.count,message=excluded.message,
          group_key=excluded.group_key,details_json=excluded.details_json,correlation_id=excluded.correlation_id,recovered_ts=NULL""",
          (key,first,ts,last_notified,sev,module,event_type,count,message,group_key,
           json.dumps(details,separators=(",",":"),default=str),correlation_id))
        if notify:
            c.execute("INSERT INTO observability_alert_history(ts,alert_key,severity,status,module,event_type,message,details_json,correlation_id) VALUES(?,?,?,?,?,?,?,?,?)",
                      (ts,key,sev,"ACTIVE",module,event_type,message,json.dumps(details,separators=(",",":"),default=str),correlation_id))
        c.commit();c.close()
        if notify:self.structured_log(sev,module,event_type,message,correlation_id=correlation_id,metrics=details)
        return {"alert_key":key,"severity":sev,"notify":notify,"count":count,"status":"ACTIVE"}

    def recover(self,key,message=None,details=None):
        c=self.conn();prev=c.execute("SELECT * FROM observability_alerts WHERE alert_key=?",(key,)).fetchone()
        if not prev or prev["status"]!="ACTIVE":c.close();return {"recovered":False}
        ts=now_iso();msg=sanitize_observability(message or f"Recovered: {prev['message']}");details=sanitize_observability(details or {})
        c.execute("UPDATE observability_alerts SET status='RECOVERED',last_seen=?,recovered_ts=? WHERE alert_key=?",(ts,ts,key))
        c.execute("INSERT INTO observability_alert_history(ts,alert_key,severity,status,module,event_type,message,details_json,correlation_id) VALUES(?,?,?,?,?,?,?,?,?)",
                  (ts,key,"INFO","RECOVERED",prev["module"],"RECOVERY",msg,json.dumps(details or {}),prev["correlation_id"]))
        c.commit();c.close();self.structured_log("INFO",prev["module"],"RECOVERY",msg,correlation_id=prev["correlation_id"],metrics=details)
        return {"recovered":True,"alert_key":key,"duration_seconds":(parse_ts(ts)-parse_ts(prev["first_seen"])).total_seconds()}

    def new_trace(self,symbol,strategy_id=None,context=None):
        cid="trace_"+uuid.uuid4().hex
        ts=now_iso();c=self.conn();c.execute("INSERT INTO observability_traces(correlation_id,strategy_id,symbol,created_ts,context_json) VALUES(?,?,?,?,?)",
            (cid,strategy_id,symbol,ts,json.dumps(context or {},separators=(",",":"),default=str)));c.commit();c.close()
        return cid

    def trace_phase(self,cid,phase,ts=None,**ids):
        col={"market_data":"market_data_ts","signal":"signal_ts","director":"director_ts","risk":"risk_ts",
             "order_created":"order_created_ts","order_sent":"order_sent_ts","broker_ack":"broker_ack_ts",
             "fill":"fill_ts","trade_memory":"trade_memory_ts","complete":"completed_ts"}.get(phase)
        if not col:return
        stamp=ts or now_iso();allowed={"signal_id","decision_id","risk_decision_id","order_id","trade_id","candidate_id","strategy_id"}
        sets=[f"{col}=?"];vals=[stamp]
        for k,v in ids.items():
            if k in allowed:sets.append(f"{k}=?");vals.append(v)
        vals.append(cid);c=self.conn();c.execute(f"UPDATE observability_traces SET {','.join(sets)} WHERE correlation_id=?",tuple(vals));c.commit();c.close()
        self._recalc_trace(cid)

    def _recalc_trace(self,cid):
        c=self.conn();r=c.execute("SELECT * FROM observability_traces WHERE correlation_id=?",(cid,)).fetchone()
        if not r:c.close();return
        d=dict(r)
        def ms(a,b):
            x=parse_ts(d.get(a));y=parse_ts(d.get(b));return (y-x).total_seconds()*1000 if x and y else None
        vals={"signal_latency_ms":ms("market_data_ts","signal_ts"),"decision_latency_ms":ms("signal_ts","director_ts"),
              "risk_latency_ms":ms("director_ts","risk_ts"),"execution_latency_ms":ms("order_created_ts","broker_ack_ts"),
              "broker_latency_ms":ms("order_sent_ts","broker_ack_ts"),"total_latency_ms":ms("market_data_ts","completed_ts")}
        c.execute("""UPDATE observability_traces SET signal_latency_ms=?,decision_latency_ms=?,risk_latency_ms=?,execution_latency_ms=?,
                     broker_latency_ms=?,total_latency_ms=? WHERE correlation_id=?""",
                  (vals["signal_latency_ms"],vals["decision_latency_ms"],vals["risk_latency_ms"],vals["execution_latency_ms"],vals["broker_latency_ms"],vals["total_latency_ms"],cid))
        c.commit();c.close()

    def link_trace(self,cid,**ids):
        allowed={"signal_id","decision_id","risk_decision_id","order_id","trade_id","candidate_id","strategy_id","symbol"}
        sets=[];vals=[]
        for k,v in ids.items():
            if k in allowed and v is not None:
                sets.append(f"{k}=?");vals.append(v)
        if not sets:return
        vals.append(cid);c=self.conn();c.execute(f"UPDATE observability_traces SET {','.join(sets)} WHERE correlation_id=?",tuple(vals));c.commit();c.close()

    def trace(self,cid):
        c=self.conn();r=c.execute("SELECT * FROM observability_traces WHERE correlation_id=?",(cid,)).fetchone()
        logs=[dict(x) for x in c.execute("SELECT * FROM observability_structured_logs WHERE correlation_id=? ORDER BY id",(cid,)).fetchall()]
        c.close();return {"trace":dict(r) if r else None,"logs":logs}

    def record_capital(self,snapshot:Dict[str,Any],source="BROKER"):
        c=self.conn();c.execute("""INSERT INTO observability_capital_history(ts,source,equity,cash,unrealized_pnl,realized_pnl,
          daily_pnl,weekly_pnl,drawdown,peak_equity,exposure,margin_usage,open_risk,remaining_risk_budget,details_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (now_iso(),source,snapshot.get("equity"),snapshot.get("cash"),snapshot.get("unrealized_pnl"),snapshot.get("realized_pnl"),
           snapshot.get("daily_pnl"),snapshot.get("weekly_pnl"),snapshot.get("drawdown"),snapshot.get("peak_equity"),
           snapshot.get("exposure"),snapshot.get("margin_usage"),snapshot.get("open_risk"),snapshot.get("remaining_risk_budget"),
           json.dumps(snapshot,separators=(",",":"),default=str)));c.commit();c.close()

    def sample_system_metrics(self,processing_time_ms=None,broker_latency_ms=None,market_data_latency_ms=None,queue_depth=0,details=None):
        wall=time.monotonic();cpu=time.process_time();cpu_pct=None
        if self._cpu_prev is not None and self._wall_prev is not None:
            dw=max(wall-self._wall_prev,1e-9);cpu_pct=max(0.0,min(100.0,(cpu-self._cpu_prev)/dw*100.0))
        self._cpu_prev=cpu;self._wall_prev=wall
        rss_mb=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024.0
        mem_pct=None
        try:
            with open('/proc/self/status') as fh:
                vmrss=next((line for line in fh if line.startswith('VmRSS:')),None)
            if vmrss:rss_mb=float(vmrss.split()[1])/1024.0
            with open('/proc/meminfo') as fh:
                total_kb=float(next(line.split()[1] for line in fh if line.startswith('MemTotal:')))
            mem_pct=(rss_mb*1024/total_kb)*100
        except Exception:pass
        disk=shutil.disk_usage(os.path.dirname(self.db_path) or ".")
        disk_pct=(disk.used/disk.total*100) if disk.total else None
        t0=time.perf_counter();
        try:
            c=self.conn();c.execute("SELECT 1").fetchone();c.close();db_ms=(time.perf_counter()-t0)*1000
        except Exception:db_ms=None
        c=self.conn();c.execute("""INSERT INTO observability_metrics(ts,cpu_percent,memory_rss_mb,memory_percent,disk_used_percent,
          event_loop_lag_ms,queue_depth,processing_time_ms,db_latency_ms,broker_latency_ms,market_data_latency_ms,details_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          (now_iso(),cpu_pct,rss_mb,mem_pct,disk_pct,self._event_loop_lag_ms,int(queue_depth),processing_time_ms,db_ms,
           broker_latency_ms,market_data_latency_ms,json.dumps(details or {},separators=(",",":"),default=str)))
        c.commit();c.close()
        return {"cpu_percent":cpu_pct,"memory_rss_mb":rss_mb,"memory_percent":mem_pct,"disk_used_percent":disk_pct,
                "event_loop_lag_ms":self._event_loop_lag_ms,"db_latency_ms":db_ms,"broker_latency_ms":broker_latency_ms,
                "market_data_latency_ms":market_data_latency_ms}

    def set_event_loop_lag(self,lag_ms):self._event_loop_lag_ms=max(0.0,float(lag_ms))

    def prune(self):
        c=self.conn()
        for table,keep in (("observability_heartbeats",self.heartbeat_retention),("observability_metrics",self.metrics_retention),
                           ("observability_structured_logs",20000),("observability_alert_history",10000),("observability_capital_history",10000)):
            row=c.execute(f"SELECT MAX(id) m FROM {table}").fetchone();mx=int(row["m"] or 0)
            if mx>keep:c.execute(f"DELETE FROM {table} WHERE id<=?",(mx-keep,))
        c.commit();c.close()

    def module_rows(self):
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM observability_module_health ORDER BY module_name").fetchall()];c.close();return rows

    def active_alerts(self):
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM observability_alerts WHERE status='ACTIVE' ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'HIGH' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,last_seen DESC").fetchall()];c.close();return rows

    def global_health(self,trading_paused=False,emergency_stop=False):
        return compute_system_health(self.module_rows(),trading_paused,emergency_stop)

    def startup_record(self,status,checks,reconciliation=None,details=None):
        c=self.conn();c.execute("INSERT INTO observability_startup_checks(ts,status,checks_json,reconciliation_json,details_json) VALUES(?,?,?,?,?)",
          (now_iso(),status,json.dumps(checks,separators=(",",":"),default=str),json.dumps(reconciliation or {},separators=(",",":"),default=str),json.dumps(details or {},separators=(",",":"),default=str)))
        c.commit();c.close()

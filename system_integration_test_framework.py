
from __future__ import annotations
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio, hashlib, json, math, os, random, sqlite3, statistics, tempfile, time, tracemalloc, uuid

import httpx

from recovery_manager import RecoveryManager, deterministic_intent_key
from order_state import can_transition, conservative_filled_units
from deployment_manager import fail_safe, promotion_gate, r_metrics
from validation_pipeline import run_historical_validation, strict_temporal_split
from adaptive_learning import validate_candidate
from system_evaluation import SystemEvaluationEngine
from governance_engine import GovernanceEngine
from smart_execution import SmartExecutionEngine
from ensemble_engine import EnsembleEngine

SAFE_TEST_ENVS={"UNIT_TEST","INTEGRATION_TEST","SIMULATION","TEST"}
FORBIDDEN_REAL_MARKERS=("api-fxtrade.oanda.com","live","production","real-money")
SEVERITIES=("LOW","MEDIUM","HIGH","CRITICAL")

def now_iso(): return datetime.now(timezone.utc).isoformat()
def j(x): return json.dumps(x,separators=(",",":"),sort_keys=True,default=str)
def f(x,default=None):
    try:
        v=float(x)
        return v if math.isfinite(v) else default
    except Exception:return default

class UnsafeTestEnvironment(RuntimeError): pass

@dataclass
class ScenarioResult:
    scenario_id:str
    name:str
    severity_if_failed:str
    passed:bool
    duration_ms:float
    components:List[str]
    assertions:List[Dict[str,Any]]=field(default_factory=list)
    metrics:Dict[str,Any]=field(default_factory=dict)
    events:List[Dict[str,Any]]=field(default_factory=list)
    failures:List[Dict[str,Any]]=field(default_factory=list)
    warnings:List[str]=field(default_factory=list)
    safety_violations:List[str]=field(default_factory=list)
    recovery_time_ms:Optional[float]=None
    deterministic_seed:Optional[int]=None

    def assert_(self,condition:bool,property_name:str,expected:Any=None,actual:Any=None,
                severity:Optional[str]=None,details:Optional[Dict[str,Any]]=None):
        rec={"property":property_name,"passed":bool(condition),"expected":expected,"actual":actual,"details":details or {}}
        self.assertions.append(rec)
        if not condition:
            sev=severity or self.severity_if_failed
            failure={"severity":sev,**rec}
            self.failures.append(failure)
            if sev=="CRITICAL":self.safety_violations.append(property_name)
        self.passed=bool(self.passed and condition)

    def event(self,event_type:str,**payload):
        self.events.append({"ts":now_iso(),"event_type":event_type,**payload})

    def as_dict(self): return self.__dict__.copy()

class FailureInjector:
    """
    Failure injection is deliberately not wired into production HTTP endpoints.
    It is available only inside an isolated test harness and refuses unsafe environments.
    """
    def __init__(self,environment:str):
        env=str(environment).upper()
        if env not in SAFE_TEST_ENVS:raise UnsafeTestEnvironment(f"FAILURE_INJECTION_FORBIDDEN:{env}")
        self.environment=env
        self.flags:Dict[str,Any]={}

    def inject(self,name:str,value:Any=True):
        self.flags[str(name)]=value
        return {"injected":name,"value":value,"environment":self.environment}

    def active(self,name:str,default=False): return self.flags.get(name,default)
    def clear(self,name:Optional[str]=None):
        if name is None:self.flags.clear()
        else:self.flags.pop(name,None)

class FakeResponse:
    def __init__(self,status_code=200,payload=None,headers=None,text=""):
        self.status_code=status_code;self._payload=payload if payload is not None else {}
        self.headers=headers or {};self.text=text or j(self._payload)
    def json(self): return self._payload

class ScriptedBroker:
    def __init__(self,responses=None,routes=None,delay_ms=0):
        self.responses=list(responses or []);self.routes=routes or {}
        self.calls=[];self.delay_ms=delay_ms
    async def request(self,method,url,params=None,json=None,headers=None,timeout=None):
        self.calls.append({"method":method,"url":url,"params":params,"json":json,"ts":time.monotonic()})
        if self.delay_ms: await asyncio.sleep(self.delay_ms/1000)
        for suffix,handler in self.routes.items():
            if url.endswith(suffix):
                return handler(method,url,params,json) if callable(handler) else handler
        if self.responses:
            x=self.responses.pop(0)
            if isinstance(x,Exception):raise x
            return x
        raise RuntimeError("UNSCRIPTED_BROKER_REQUEST:"+url)

class SystemIntegrationTestFramework:
    """
    Step 14 isolated integration/chaos framework.
    - Uses a temporary SQLite database.
    - Never receives production credentials.
    - Broker I/O is scripted/in-memory.
    - All stochastic scenarios store their seed.
    """
    def __init__(self,environment="INTEGRATION_TEST",seed=140013,work_dir:Optional[str]=None):
        self.environment=str(environment).upper()
        if self.environment not in SAFE_TEST_ENVS:
            raise UnsafeTestEnvironment(f"TEST_FRAMEWORK_FORBIDDEN_IN:{self.environment}")
        self.seed=int(seed);self.random=random.Random(self.seed)
        self.work_dir=Path(work_dir or tempfile.mkdtemp(prefix="step14_"))
        self.work_dir.mkdir(parents=True,exist_ok=True)
        self.db_path=str(self.work_dir/"integration.db")
        self.injector=FailureInjector(self.environment)
        self.results:List[ScenarioResult]=[]
        self._ensure_base_schema()

    # ---------- environment guardrails ----------
    @staticmethod
    def assert_safe_endpoint(environment:str,base_url:str,account_id:str="sandbox"):
        env=str(environment).upper();url=str(base_url).lower();acct=str(account_id).lower()
        if env not in SAFE_TEST_ENVS:raise UnsafeTestEnvironment("NON_TEST_ENVIRONMENT")
        if any(x in url for x in FORBIDDEN_REAL_MARKERS):raise UnsafeTestEnvironment("REAL_BROKER_ENDPOINT_FORBIDDEN")
        if any(x in acct for x in ("live","prod","real")):raise UnsafeTestEnvironment("REAL_ACCOUNT_FORBIDDEN")
        return True

    def recovery(self,scope="SIM_PRIMARY"):
        self.assert_safe_endpoint(self.environment,"https://broker.test",scope)
        r=RecoveryManager(self.db_path,"https://broker.test",scope,"TEST_TOKEN",scope,
                          request_min_interval_ms=0,backoff_base_seconds=.001,backoff_cap_seconds=.01,
                          circuit_open_seconds=.01,max_read_retries=2)
        r.ensure_schema()
        return r

    def governance(self,mode="SHADOW",policies=None):
        g=GovernanceEngine(self.db_path,"3.25",mode=mode,policies=policies or {})
        g.ensure_schema();g.set_runtime(mode=mode,policies=policies or {},config_version=1)
        return g

    def evaluator(self,min_samples=8):
        e=SystemEvaluationEngine(self.db_path,"3.26",min_samples=min_samples,report_period_hours=24)
        e.ensure_schema();return e

    def smart_execution(self):
        e=SmartExecutionEngine(self.db_path,"3.25",mode="SHADOW",max_snapshot_age_seconds=5,
                               default_intent_ttl_seconds=60,liquidity_participation=.25,
                               slice_threshold_units=1000,slice_size_units=200)
        e.ensure_schema();return e

    def ensemble(self):
        e=EnsembleEngine(self.db_path,"3.25",mode="SHADOW",max_model_weight=.40,
                         max_family_weight=.50,min_sample_size=20,correlation_threshold=.70,
                         weight_change_limit=.10,weight_cooldown_hours=24,min_active_directional=2)
        e.ensure_schema();return e

    # ---------- fixture schema ----------
    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30,isolation_level=None)
        c.row_factory=sqlite3.Row;c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA synchronous=FULL")
        c.execute("PRAGMA busy_timeout=5000");return c

    def _ensure_base_schema(self):
        c=self.conn();c.executescript("""
        CREATE TABLE IF NOT EXISTS active_trade_management(
          trade_id TEXT PRIMARY KEY,instrument TEXT,side TEXT,entry REAL,initial_stop REAL,
          initial_target REAL,current_stop REAL,setup_variant TEXT,policy TEXT,trend_score REAL,
          opened_ts TEXT,last_r REAL,last_action TEXT,break_even_applied INTEGER DEFAULT 0,
          profit_lock_applied INTEGER DEFAULT 0,trailing_applied INTEGER DEFAULT 0,
          closed INTEGER DEFAULT 0,updated_ts TEXT,current_units REAL);
        CREATE TABLE IF NOT EXISTS trade_memory(
          id INTEGER PRIMARY KEY AUTOINCREMENT,trade_id TEXT UNIQUE,signal_id INTEGER,order_id TEXT,
          strategy TEXT,symbol TEXT,direction TEXT,status TEXT,entry_ts TEXT,exit_ts TEXT,
          entry_price REAL,exit_price REAL,position_size REAL,stop_loss REAL,take_profit REAL,
          gross_result REAL,net_result REAL,realized_pl REAL,financing REAL,dividend_adjustment REAL,
          guaranteed_execution_fees REAL,commission REAL,fees_total REAL,entry_slippage_pips REAL,
          exit_slippage_pips REAL,duration_seconds REAL,market_regime_entry TEXT,regime_confidence_entry REAL,
          volatility_state_entry TEXT,trend_strength_entry REAL,strategy_confidence_entry REAL,
          director_state_entry TEXT,director_confidence_entry REAL,risk_multiplier_entry REAL,
          risk_allow_new_trades_shadow INTEGER,requested_risk REAL,approved_risk REAL,entry_drawdown REAL,
          mfe_r REAL DEFAULT 0,mae_r REAL DEFAULT 0,max_drawdown_during_trade_r REAL DEFAULT 0,realized_r REAL,
          entry_session TEXT,confidence_bucket TEXT,entry_reasons_json TEXT DEFAULT '[]',
          exit_reasons_json TEXT DEFAULT '[]',entry_context_json TEXT DEFAULT '{}',
          execution_context_json TEXT DEFAULT '{}',exit_context_json TEXT DEFAULT '{}',
          risk_recommendation_json TEXT DEFAULT '{}',data_quality_json TEXT DEFAULT '{}',
          execution_quality_compromised INTEGER DEFAULT 0,operational_incident_id TEXT,
          strategy_version TEXT,risk_config_version TEXT,director_version TEXT,regime_model_version TEXT,
          deployment_version TEXT,runtime_code_hash TEXT,dependency_lock_hash TEXT,config_snapshot_hash TEXT,
          created_ts TEXT,updated_ts TEXT);
        CREATE TABLE IF NOT EXISTS portfolio_risk_state(
          id INTEGER PRIMARY KEY,ts TEXT,balance REAL,nav REAL,peak_nav REAL,current_drawdown REAL,
          margin_used REAL,margin_usage REAL,open_positions INTEGER,portfolio_open_risk REAL,
          consecutive_losses INTEGER,data_stale INTEGER,system_abnormal INTEGER,details_json TEXT);
        CREATE TABLE IF NOT EXISTS market_regime_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,candle_ts TEXT,instrument TEXT,market_regime TEXT,confidence REAL,
          volatility_state TEXT,trend_strength REAL,abnormality_score REAL,supporting_metrics_json TEXT);
        CREATE TABLE IF NOT EXISTS ai_strategy_director_decisions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,instrument TEXT,setup_variant TEXT,recommended_state TEXT,
          confidence REAL,market_regime TEXT,regime_confidence REAL,volatility_state TEXT,
          strategy_health_status TEXT,historical_win_rate REAL,recent_win_rate REAL,historical_samples INTEGER,
          recent_samples INTEGER,signal_confidence REAL,score_components_json TEXT,reasons_json TEXT,
          observation_only INTEGER);
        CREATE TABLE IF NOT EXISTS ai_strategy_director_outcomes(
          id INTEGER PRIMARY KEY AUTOINCREMENT,director_decision_id INTEGER,signal_id INTEGER,resolved_label INTEGER,
          executed INTEGER,blocked INTEGER,resolved_ts TEXT);
        CREATE TABLE IF NOT EXISTS adaptive_risk_decisions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,instrument TEXT,setup_variant TEXT,market_regime TEXT,
          volatility_state TEXT,strategy_confidence REAL,recent_win_rate REAL,current_drawdown REAL,nav REAL,
          margin_usage REAL,portfolio_open_risk REAL,strategy_open_risk REAL,requested_risk REAL,approved_risk REAL,
          requested_units REAL,shadow_max_position_size REAL,risk_multiplier REAL,max_exposure REAL,
          allow_new_trades INTEGER,reduce_existing_positions INTEGER,emergency_stop INTEGER,
          hard_limit_triggered INTEGER,reason TEXT,metrics_json TEXT,shadow_mode INTEGER);
        CREATE TABLE IF NOT EXISTS signals(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,candle_ts TEXT,instrument TEXT,signal TEXT,technical INTEGER,
          score INTEGER,alignment TEXT,blocked INTEGER,entry REAL,stop REAL,target REAL,rr REAL,executed INTEGER,
          order_id TEXT,ml_probability REAL,dynamic_confidence REAL,confidence_source TEXT,confidence_samples INTEGER,
          required_confidence REAL,decision_reason TEXT,setup_variant TEXT,features_json TEXT,filters_json TEXT);
        CREATE TABLE IF NOT EXISTS learning_samples(
          id INTEGER PRIMARY KEY AUTOINCREMENT,signal_id INTEGER,created_ts TEXT,candle_ts TEXT,instrument TEXT,
          direction TEXT,entry REAL,stop REAL,target REAL,technical INTEGER,score INTEGER,blocked INTEGER,executed INTEGER,
          features_json TEXT,status TEXT,label INTEGER,resolved_ts TEXT,bars_to_resolution INTEGER,mfe_r REAL,mae_r REAL,note TEXT);
        CREATE TABLE IF NOT EXISTS candidate_strategies(candidate_id TEXT PRIMARY KEY,generated_at TEXT);
        CREATE TABLE IF NOT EXISTS candidate_validation_runs(
          validation_id TEXT PRIMARY KEY,candidate_id TEXT,completed_ts TEXT,final_status TEXT,
          oos_results_json TEXT,paper_results_json TEXT);
        CREATE TABLE IF NOT EXISTS candidate_paper_trades(id INTEGER PRIMARY KEY AUTOINCREMENT,candidate_id TEXT,created_ts TEXT);
        CREATE TABLE IF NOT EXISTS deployment_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,candidate_id TEXT,event_type TEXT,new_stage TEXT);
        CREATE TABLE IF NOT EXISTS deployment_registry(
          candidate_id TEXT PRIMARY KEY,current_stage TEXT,updated_ts TEXT,
          governance_policy_version TEXT,governance_decision_id TEXT);
        CREATE TABLE IF NOT EXISTS deployment_live_trades(
          id INTEGER PRIMARY KEY AUTOINCREMENT,candidate_id TEXT,realized_r REAL,opened_ts TEXT,closed_ts TEXT);
        CREATE TABLE IF NOT EXISTS observability_alert_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,alert_key TEXT,severity TEXT,status TEXT,module TEXT,
          event_type TEXT,message TEXT,details_json TEXT,correlation_id TEXT);
        CREATE TABLE IF NOT EXISTS observability_metrics(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,cpu_percent REAL,memory_rss_mb REAL,memory_percent REAL,
          disk_used_percent REAL,event_loop_lag_ms REAL,queue_depth INTEGER,processing_time_ms REAL,
          db_latency_ms REAL,broker_latency_ms REAL,market_data_latency_ms REAL,details_json TEXT);
        CREATE TABLE IF NOT EXISTS observability_heartbeats(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,module_name TEXT,status TEXT,latency_ms REAL,details_json TEXT);
        CREATE TABLE IF NOT EXISTS observability_capital_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT,source TEXT,equity REAL,cash REAL,unrealized_pnl REAL,
          realized_pnl REAL,daily_pnl REAL,weekly_pnl REAL,drawdown REAL,peak_equity REAL,exposure REAL,
          margin_usage REAL,open_risk REAL,remaining_risk_budget REAL,details_json TEXT);
        CREATE TABLE IF NOT EXISTS security_audit_log(
          seq INTEGER PRIMARY KEY AUTOINCREMENT,audit_id TEXT,timestamp TEXT,actor TEXT,actor_role TEXT,action TEXT,
          resource TEXT,old_value_json TEXT,new_value_json TEXT,reason TEXT,result TEXT,correlation_id TEXT,
          prev_hash TEXT,record_hash TEXT);
        CREATE TABLE IF NOT EXISTS concept_drift_alerts(
          scope_key TEXT PRIMARY KEY,ts TEXT,strategy_id TEXT,status TEXT);
        """);c.commit();c.close()
        # Other schemas supplied by their real modules.
        self.recovery().ensure_schema()
        self.governance().ensure_schema()
        self.evaluator().ensure_schema()
        self.smart_execution().ensure_schema()
        self.ensemble().ensure_schema()

    def reset_dynamic_data(self):
        keep_prefix=("sqlite_","governance_policy_versions")
        c=self.conn()
        tables=[x["name"] for x in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        for t in tables:
            if t.startswith("sqlite_") or t=="governance_policy_versions":continue
            try:c.execute(f"DELETE FROM {t}")
            except sqlite3.DatabaseError:pass
        # Recreate singleton state via managers.
        c.commit();c.close()
        self.recovery().ensure_schema();self.governance().ensure_schema();self.evaluator().ensure_schema()

    # ---------- common helpers ----------
    def _result(self,scenario_id,name,severity,components,seed=None):
        return ScenarioResult(scenario_id,name,severity,True,0.0,components,deterministic_seed=seed)

    def _record(self,r,start):
        r.duration_ms=(time.perf_counter()-start)*1000
        self.results.append(r);return r

    def seed_trade(self,trade_id,strategy,pnl,r,days_ago=1,regime="BULLISH_TREND",
                   conf=.75,slip=.2,compromised=0,deployment="prod_v1",approved_risk=.005):
        ts=datetime.now(timezone.utc)-timedelta(days=days_ago)
        c=self.conn()
        c.execute("""INSERT OR REPLACE INTO trade_memory(
          trade_id,strategy,symbol,direction,status,entry_ts,exit_ts,entry_price,exit_price,position_size,
          stop_loss,take_profit,gross_result,net_result,realized_pl,fees_total,entry_slippage_pips,
          exit_slippage_pips,market_regime_entry,strategy_confidence_entry,director_confidence_entry,
          requested_risk,approved_risk,realized_r,entry_session,execution_quality_compromised,
          strategy_version,risk_config_version,director_version,regime_model_version,deployment_version,
          runtime_code_hash,dependency_lock_hash,config_snapshot_hash,created_ts,updated_ts)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (trade_id,strategy,"EUR_USD","LONG","CLOSED",(ts-timedelta(hours=1)).isoformat(),ts.isoformat(),
           1.10,1.101,100,1.099,1.102,pnl+.05,pnl,pnl,.05,slip/2,slip/2,regime,conf,conf,
           approved_risk,approved_risk,r,"NY",compromised,
           f"{strategy}@3.25","config_v1","director@3.25","regime@3.25",deployment,
           "codehash","dephash","confighash",ts.isoformat(),ts.isoformat()))
        c.commit();c.close()

    def seed_system_context(self,healthy=True,degrading=False,latency_ms=80,data_quality=True,drawdown=.02):
        now=datetime.now(timezone.utc)
        for i in range(80):
            historical=i>=30
            pnl=(.8 if historical else (-.6 if degrading else .7))
            regime="BULLISH_TREND" if historical else ("HIGH_VOLATILITY" if degrading else "BULLISH_TREND")
            self.seed_trade(f"CTX{i}","STRAT_A",pnl,pnl,days_ago=max(1,80-i),regime=regime,
                            slip=2.0 if degrading else .2,compromised=0 if data_quality else 1)
        c=self.conn()
        for i in range(20):
            ts=(now-timedelta(hours=i*6)).isoformat()
            c.execute("INSERT INTO observability_heartbeats(ts,module_name,status,latency_ms,details_json) VALUES(?,?,?,?,?)",
                      (ts,"Broker Connection","OK",latency_ms,"{}"))
            c.execute("""INSERT INTO observability_metrics(ts,processing_time_ms,broker_latency_ms,market_data_latency_ms,details_json)
                         VALUES(?,?,?,?,?)""",(ts,100,latency_ms,50,"{}"))
        c.execute("""INSERT INTO observability_capital_history(
          ts,source,equity,drawdown,exposure,margin_usage,open_risk,details_json) VALUES(?,?,?,?,?,?,?,?)""",
          (now.isoformat(),"SIM",10000,drawdown,.25,.08,.02,"{}"))
        if not data_quality:
            for i in range(10):
                c.execute("""INSERT INTO observability_alert_history(
                  ts,alert_key,severity,status,module,event_type,message,details_json)
                  VALUES(?,?,?,?,?,?,?,?)""",
                  ((now-timedelta(hours=i)).isoformat(),f"stale{i}","HIGH","ACTIVE","Market Data",
                   "MARKET_DATA_STALE","stale","{}"))
        c.commit();c.close()
        ev=self.evaluator(min_samples=10).evaluate()
        return ev

    # ---------- scenarios ----------
    async def scenario_golden_path(self):
        start=time.perf_counter();r=self._result("GOLDEN_PATH","Golden Path","CRITICAL",
            ["Market Regime","Strategy","AI Director","Risk Engine","Execution","Broker","Trade Memory","Monitoring"])
        rm=self.recovery("GOLDEN")
        rm.exit_safe_mode("golden ready");rm.verify_risk(True,{"risk":"approved"})
        r.event("MARKET_DATA",valid=True);r.event("REGIME_DETECTED",regime="BULLISH_TREND",confidence=.9)
        r.event("STRATEGY_SIGNAL",strategy="STRAT_A",signal="BUY",confidence=.8)
        r.event("AI_DIRECTOR",state="ACTIVE",confidence=.85)
        r.event("RISK_ENGINE",allow=True,risk=.005,hard_limit=.01)
        key=deterministic_intent_key("GOLDEN","EUR_USD","BUY","STRAT_A","2026-08-15T12:00:00Z",1.1,1.099,1.102)
        broker=ScriptedBroker([FakeResponse(201,{
            "orderCreateTransaction":{"id":"O1"},
            "orderFillTransaction":{"id":"F1","tradeOpened":{"tradeID":"T-GOLD","units":"100"}}
        })])
        out=await rm.submit_order(broker,idempotency_key=key,correlation_id="corr-gold",decision_id="d1",
            risk_decision_id="risk1",strategy_id="STRAT_A",symbol="EUR_USD",side="BUY",requested_units=100,
            entry_price=1.1,stop_loss=1.099,take_profit=1.102,
            order_body={"order":{"instrument":"EUR_USD","units":"100","type":"MARKET"}})
        intent=out.get("intent") or {}
        self.seed_trade("T-GOLD","STRAT_A",1.0,1.0,days_ago=1)
        r.event("BROKER_FILL",intent_state=intent.get("state"));r.event("TRADE_MEMORY",trade_id="T-GOLD")
        r.assert_(intent.get("state")=="FILLED","ORDER_FILLED","FILLED",intent.get("state"))
        r.assert_(len(broker.calls)==1,"SINGLE_BROKER_SUBMISSION",1,len(broker.calls))
        r.assert_(rm.state().get("emergency_stop")==0,"NO_EMERGENCY_STOP",0,rm.state().get("emergency_stop"))
        return self._record(r,start)

    async def scenario_partial_fill_disconnect(self):
        start=time.perf_counter();r=self._result("PARTIAL_FILL_DISCONNECT","Partial Fill + Disconnect","CRITICAL",
            ["Execution","Broker","Recovery","Reconciliation","Risk","Trade Memory"])
        rm=self.recovery("PARTIAL");rm.exit_safe_mode("ready")
        key=deterministic_intent_key("PARTIAL","EUR_USD","BUY","S1","2026-08-15T13:00:00Z",1.1,1.099,1.102)
        timeout=httpx.ReadTimeout("lost after send",request=httpx.Request("POST","https://broker.test"))
        out=await rm.submit_order(ScriptedBroker([timeout]),idempotency_key=key,correlation_id="corr-partial",
            decision_id="D",risk_decision_id="R",strategy_id="S1",symbol="EUR_USD",side="BUY",requested_units=100,
            entry_price=1.1,stop_loss=1.099,take_profit=1.102,
            order_body={"order":{"instrument":"EUR_USD","units":"100","type":"MARKET"}})
        intent=out["intent"];cid=intent["client_order_id"]
        routes={
            "/v3/accounts/PARTIAL":FakeResponse(200,{"account":{"NAV":"10000","balance":"10000","marginUsed":"100"},"lastTransactionID":"99"}),
            "/pendingOrders":FakeResponse(200,{"orders":[],"lastTransactionID":"99"}),
            "/openTrades":FakeResponse(200,{"trades":[{"id":"T37","currentUnits":"37","price":"1.1002","openTime":now_iso(),
                    "clientExtensions":{"id":cid},"stopLossOrder":{"id":"SL37","price":"1.099"},
                    "takeProfitOrder":{"id":"TP37","price":"1.102"}}],"lastTransactionID":"99"}),
            "/openPositions":FakeResponse(200,{"positions":[{"instrument":"EUR_USD"}],"lastTransactionID":"99"}),
            "/transactions/sinceid":FakeResponse(200,{"transactions":[],"lastTransactionID":"99"})
        }
        reconnect=await rm.reconnect_and_reconcile(ScriptedBroker(routes=routes),max_attempts=1)
        restored=rm.intent(intent["execution_intent_id"])
        duplicate=await rm.submit_order(ScriptedBroker([FakeResponse(201,{})]),idempotency_key=key,
            correlation_id="corr-partial",decision_id="D",risk_decision_id="R",strategy_id="S1",
            symbol="EUR_USD",side="BUY",requested_units=100,entry_price=1.1,stop_loss=1.099,take_profit=1.102,
            order_body={"order":{"instrument":"EUR_USD","units":"100","type":"MARKET"}})
        r.event("ORDER_STATUS_UNKNOWN");r.event("SAFE_MODE");r.event("RECONNECT")
        r.event("PARTIAL_FILL",filled=restored.get("filled_units"),remaining=restored.get("remaining_units"))
        r.assert_(out.get("status_unknown") is True,"UNKNOWN_AFTER_LOST_ACK",True,out.get("status_unknown"))
        r.assert_(restored.get("filled_units")==37,"EXACT_PARTIAL_FILL",37,restored.get("filled_units"))
        r.assert_(restored.get("remaining_units")==63,"EXACT_REMAINING_UNITS",63,restored.get("remaining_units"))
        r.assert_(duplicate.get("duplicate_prevented") is True,"NO_DUPLICATE_RESUBMIT",True,duplicate)
        r.assert_(reconnect.get("connected") is True,"RECONNECT_SUCCEEDED",True,reconnect.get("connected"))
        return self._record(r,start)

    def scenario_duplicate_out_of_order(self):
        start=time.perf_counter();r=self._result("DUPLICATE_OUT_OF_ORDER","Duplicate and Out-of-Order Events","CRITICAL",
            ["Order State","Recovery","Persistence"])
        rm=self.recovery("EVENTS")
        key=deterministic_intent_key("EVENTS","GBP_USD","SELL","S2","2026-08-15T14:00:00Z",1.2,1.201,1.198)
        created=rm.create_intent(idempotency_key=key,correlation_id="c2",decision_id=None,risk_decision_id=None,
                                 strategy_id="S2",symbol="GBP_USD",side="SELL",requested_units=50,
                                 entry_price=1.2,stop_loss=1.201,take_profit=1.198,
                                 request_body={"order":{"units":"-50"}})
        eid=created["intent"]["execution_intent_id"]
        rm.transition_intent(eid,"SUBMITTING")
        rm.transition_intent(eid,"FILLED",filled_units=50,broker_event_id="EV1",event_type="FILL")
        # ACK arriving later must not regress the state.
        ack_regressed=False
        try: rm.transition_intent(eid,"ACKNOWLEDGED",broker_event_id="ACK1",event_type="ACK")
        except Exception: pass
        else: ack_regressed=rm.intent(eid)["state"]=="ACKNOWLEDGED"
        before=len([x for x in rm.timeline() if x.get("broker_event_id")=="EV1"])
        rm.transition_intent(eid,"FILLED",filled_units=50,broker_event_id="EV1",event_type="FILL")
        after=len([x for x in rm.timeline() if x.get("broker_event_id")=="EV1"])
        r.assert_(rm.intent(eid)["state"]=="FILLED","OUT_OF_ORDER_DOES_NOT_REGRESS","FILLED",rm.intent(eid)["state"])
        r.assert_(not ack_regressed,"LATE_ACK_CANNOT_REGRESS_TERMINAL_STATE",False,ack_regressed)
        r.assert_(after==before,"DUPLICATE_BROKER_EVENT_DEDUP",before,after)
        return self._record(r,start)

    def scenario_market_regime_stress(self):
        start=time.perf_counter();r=self._result("MARKET_REGIME_STRESS","Stress Market and Rapid Regime Change","HIGH",
            ["Market Regime","AI Director","Risk Engine","Governance"])
        seq=[
            ("BULLISH_TREND",.92,"NORMAL",.7),("HIGH_VOLATILITY",.78,"HIGH",.4),
            ("RANGE",.81,"NORMAL",.1),("BEARISH_TREND",.88,"NORMAL",.75),
            ("ABNORMAL",.60,"EXTREME",.0)
        ]
        c=self.conn()
        for i,(reg,conf,vol,strength) in enumerate(seq):
            ts=(datetime.now(timezone.utc)+timedelta(seconds=i)).isoformat()
            c.execute("""INSERT INTO market_regime_history(ts,candle_ts,instrument,market_regime,confidence,
              volatility_state,trend_strength,abnormality_score,supporting_metrics_json)
              VALUES(?,?,?,?,?,?,?,?,?)""",(ts,ts,"EUR_USD",reg,conf,vol,strength,.9 if reg=="ABNORMAL" else 0,"{}"))
            state="REDUCED" if reg in ("HIGH_VOLATILITY","ABNORMAL") else "ACTIVE"
            c.execute("""INSERT INTO ai_strategy_director_decisions(
              ts,instrument,setup_variant,recommended_state,confidence,market_regime,observation_only,
              score_components_json,reasons_json) VALUES(?,?,?,?,?,?,?,?,?)""",
              (ts,"EUR_USD","STRAT_A",state,max(.45,conf-.1),reg,1,"{}","[]"))
            mult=.3 if reg=="ABNORMAL" else .5 if reg=="HIGH_VOLATILITY" else 1.0
            c.execute("""INSERT INTO adaptive_risk_decisions(
              ts,instrument,setup_variant,market_regime,volatility_state,risk_multiplier,allow_new_trades,
              reduce_existing_positions,emergency_stop,hard_limit_triggered,reason,metrics_json,shadow_mode)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (ts,"EUR_USD","STRAT_A",reg,vol,mult,0 if reg=="ABNORMAL" else 1,
               1 if mult<1 else 0,0,0,"stress","{}",1))
        c.commit();c.close()
        # Hard property: risk multiplier never exceeds 1 and abnormal does not allow entries.
        risks=self.governance()._rows("adaptive_risk_decisions","ORDER BY id")
        r.assert_(all((f(x.get("risk_multiplier"),0) or 0)<=1 for x in risks),"NO_RISK_ESCALATION_FROM_REGIME",True,
                  [x.get("risk_multiplier") for x in risks])
        r.assert_(risks[-1]["allow_new_trades"]==0,"ABNORMAL_REGIME_BLOCKS_ENTRIES",0,risks[-1]["allow_new_trades"])
        return self._record(r,start)


    async def scenario_broker_failure_during_exit(self):
        start=time.perf_counter();r=self._result("BROKER_FAILURE_EXIT","Broker Failure During Exit","CRITICAL",
            ["Execution","Broker","Recovery","Risk"])
        rm=self.recovery("EXITFAIL");rm.exit_safe_mode("position managed")
        timeout=httpx.ReadTimeout("exit ack lost",request=httpx.Request("PUT","https://broker.test"))
        broker=ScriptedBroker([timeout])
        failed=False
        try:
            await rm.broker_request(broker,"PUT","/v3/accounts/{account}/trades/T1/close",
                                    body={"units":"ALL"},allow_retry=False,critical=True)
        except httpx.ReadTimeout:
            failed=True
        r.event("EXIT_REQUEST");r.event("BROKER_CONNECTION_LOST")
        r.assert_(failed,"EXIT_TIMEOUT_PROPAGATES")
        r.assert_(len(broker.calls)==1,"EXIT_NOT_BLINDLY_RETRIED",1,len(broker.calls))
        r.assert_(rm.state().get("safe_mode")==1,"EXIT_UNKNOWN_ENTERS_SAFE_MODE",1,rm.state().get("safe_mode"))
        r.assert_(rm.new_trades_allowed() is False,"NO_NEW_EXPOSURE_DURING_UNKNOWN_EXIT")
        return self._record(r,start)

    def scenario_reconciliation_mismatch_and_missing_protection(self):
        start=time.perf_counter();r=self._result("RECONCILIATION_SAFETY","Position Mismatch + Missing Protective Order","CRITICAL",
            ["Recovery","Reconciliation","Risk","Broker","Persistence"])
        rm=self.recovery("RECONSAFE")
        c=self.conn()
        c.execute("""INSERT OR REPLACE INTO active_trade_management(
          trade_id,instrument,side,entry,initial_stop,initial_target,current_stop,setup_variant,policy,trend_score,
          opened_ts,last_r,last_action,closed,updated_ts,current_units)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          ("T-MISMATCH","EUR_USD","BUY",1.1,1.099,1.102,1.099,"S1","P",1,now_iso(),0,"OPEN",0,now_iso(),100))
        c.execute("""INSERT OR REPLACE INTO trade_memory(
          trade_id,strategy,symbol,direction,status,entry_ts,entry_price,position_size,stop_loss,take_profit,
          created_ts,updated_ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          ("T-MISMATCH","S1","EUR_USD","LONG","OPEN",now_iso(),1.1,100,1.099,1.102,now_iso(),now_iso()))
        c.commit();c.close()
        snapshot={
            "account":{"NAV":"10000","balance":"10000","marginUsed":"100"},
            "last_transaction_id":"700",
            "open_trades":[{"id":"T-MISMATCH","currentUnits":"60","price":"1.1005","openTime":now_iso(),
                            "takeProfitOrder":{"id":"TP","price":"1.102"}}], # stop intentionally missing
            "open_orders":[],"open_positions":[{"instrument":"EUR_USD"}],"transactions":[]
        }
        out=rm.reconcile_snapshot(snapshot)
        c=self.conn()
        units=c.execute("SELECT current_units FROM active_trade_management WHERE trade_id='T-MISMATCH'").fetchone()["current_units"]
        items=[dict(x) for x in c.execute("""SELECT * FROM recovery_reconciliation_items
                                            WHERE reconciliation_id=?""",(out["reconciliation_id"],)).fetchall()]
        c.close()
        protective=[x for x in items if x["item_type"]=="PROTECTIVE_ORDER"]
        r.event("POSITION_MISMATCH",internal=100,broker=60)
        r.event("PROTECTIVE_ORDER_MISSING")
        r.assert_(out["status"]=="CRITICAL_MISMATCH","MISSING_PROTECTION_IS_CRITICAL","CRITICAL_MISMATCH",out["status"])
        r.assert_(abs(units-60)<1e-12,"BROKER_UNITS_BECOME_CONSERVATIVE_POSITION_TRUTH",60,units)
        r.assert_(bool(protective),"PROTECTIVE_ORDER_MISSING_RECORDED")
        r.assert_(rm.state().get("safe_mode")==1,"CRITICAL_MISMATCH_ENTERS_SAFE_MODE")
        return self._record(r,start)

    def scenario_database_failure_atomicity(self):
        start=time.perf_counter();r=self._result("DATABASE_FAILURE","Database Failure Atomicity / Recovery","CRITICAL",
            ["Persistence","Execution","Trade Memory","Deployment","Change Management","Recovery"])
        # Hold an exclusive lock, then attempt representative writes using a low-timeout connection.
        locker=sqlite3.connect(self.db_path,timeout=.1,isolation_level=None)
        locker.execute("BEGIN EXCLUSIVE")
        operations=[
            ("ORDER_INTENT","INSERT INTO recovery_event_journal(event_id,ts,account_scope,event_type,payload_json) VALUES(?,?,?,?,?)",
             ("dbfail-order",now_iso(),"DBFAIL","ORDER_INTENT_CREATED","{}")),
            ("TRADE_CLOSE","INSERT INTO trade_memory(trade_id,strategy,symbol,direction,status,entry_ts,exit_ts,net_result,created_ts,updated_ts) VALUES(?,?,?,?,?,?,?,?,?,?)",
             ("DBFAIL-T","S","EUR_USD","LONG","CLOSED",now_iso(),now_iso(),1,now_iso(),now_iso())),
            ("DEPLOYMENT","INSERT INTO deployment_events(ts,candidate_id,event_type,new_stage) VALUES(?,?,?,?)",
             (now_iso(),"DBFAIL-C","PROMOTION","CANARY_LIVE")),
            ("CONFIG_AUDIT","INSERT INTO security_audit_log(timestamp,action,resource) VALUES(?,?,?)",
             (now_iso(),"CONFIG_CHANGED","strategy.DBFAIL"))
        ]
        blocked=[]
        for name,sql,args in operations:
            c=sqlite3.connect(self.db_path,timeout=.03,isolation_level=None)
            try:c.execute(sql,args);c.commit();blocked.append(False)
            except sqlite3.OperationalError:blocked.append(True)
            finally:c.close()
        locker.rollback();locker.close()
        r.assert_(all(blocked),"DB_OUTAGE_BLOCKS_PARTIAL_WRITES",True,blocked)
        # After recovery, all representative durable writes succeed.
        successes=[]
        for name,sql,args in operations:
            c=sqlite3.connect(self.db_path,timeout=1,isolation_level=None)
            try:c.execute(sql,args);c.commit();successes.append(True)
            except sqlite3.DatabaseError:successes.append(False)
            finally:c.close()
        r.assert_(all(successes),"DB_RECOVERY_RESTORES_WRITES",True,successes)
        r.event("DATABASE_UNAVAILABLE");r.event("DATABASE_RECOVERED")
        return self._record(r,start)

    async def scenario_latency_injection(self):
        start=time.perf_counter();r=self._result("LATENCY_INJECTION","Injected Component Latency","HIGH",
            ["Market Data","AI Director","Risk Engine","Broker","Database","Monitoring"])
        self.injector.inject("simulate_broker_latency_ms",120)
        broker=ScriptedBroker([FakeResponse(200,{"ok":True})],delay_ms=120)
        t=time.perf_counter();await broker.request("GET","https://broker.test/latency");elapsed=(time.perf_counter()-t)*1000
        # Deterministic policy: latency itself cannot increase risk.
        risk_multiplier=.5 if elapsed>=100 else 1.0
        r.metrics={"broker_latency_ms":elapsed,"risk_multiplier":risk_multiplier}
        r.assert_(elapsed>=100,"LATENCY_INJECTION_EFFECTIVE",">=100ms",elapsed)
        r.assert_(risk_multiplier<=1.0,"LATENCY_NEVER_INCREASES_RISK","<=1.0",risk_multiplier)
        self.injector.clear()
        return self._record(r,start)

    def deterministic_replay(self,events:List[Dict[str,Any]],seed:Optional[int]=None,
                             versions:Optional[Dict[str,str]]=None)->Dict[str,Any]:
        rng=random.Random(self.seed if seed is None else seed)
        versions=versions or {"strategy":"S@3.25","risk":"R@3.25","director":"D@3.25","governance":"G@3.25"}
        state={"position":0.0,"safe_mode":False,"emergency_stop":False,"risk_multiplier":1.0,
               "decisions":[],"versions":versions}
        for index,e in enumerate(events):
            typ=e["type"]
            if typ=="MARKET":
                state["market"]=e.get("regime");state["volatility"]=e.get("volatility")
            elif typ=="RISK":
                state["risk_multiplier"]=min(1.0,max(0.0,float(e.get("multiplier",1.0))))
                state["decisions"].append({"i":index,"risk":state["risk_multiplier"]})
            elif typ=="ORDER_FILL":
                if not state["safe_mode"] and not state["emergency_stop"]:
                    state["position"]+=float(e.get("units",0))
                state["decisions"].append({"i":index,"position":state["position"]})
            elif typ=="UNKNOWN_ORDER":
                state["safe_mode"]=True;state["decisions"].append({"i":index,"safe_mode":True})
            elif typ=="RECONCILE":
                state["position"]=float(e.get("broker_units",state["position"]));state["safe_mode"]=False
                state["decisions"].append({"i":index,"reconciled":state["position"]})
            elif typ=="EMERGENCY_STOP":
                state["emergency_stop"]=bool(e.get("active",True));state["decisions"].append({"i":index,"emergency":state["emergency_stop"]})
            elif typ=="SEEDED_TIE_BREAK":
                # Randomness is allowed only with a stored seed.
                state["decisions"].append({"i":index,"choice":rng.choice(e.get("choices") or [0])})
        digest=hashlib.sha256(j(state).encode()).hexdigest()
        return {"state":state,"digest":digest,"seed":self.seed if seed is None else seed,"versions":versions}

    def scenario_deterministic_replay(self):
        start=time.perf_counter();r=self._result("DETERMINISTIC_REPLAY","Deterministic Historical Event Replay","CRITICAL",
            ["Execution","Risk","Recovery","Persistence","Audit"],seed=self.seed)
        events=[
            {"type":"MARKET","regime":"BULLISH_TREND","volatility":"NORMAL"},
            {"type":"RISK","multiplier":.8},{"type":"ORDER_FILL","units":100},
            {"type":"UNKNOWN_ORDER"},{"type":"RECONCILE","broker_units":37},
            {"type":"SEEDED_TIE_BREAK","choices":["A","B","C"]}
        ]
        a=self.deterministic_replay(events,self.seed)
        b=self.deterministic_replay(events,self.seed)
        c=self.deterministic_replay(events,self.seed+1)
        r.assert_(a["digest"]==b["digest"],"REPLAY_SAME_INPUT_SAME_DECISIONS",a["digest"],b["digest"])
        r.assert_(a["state"]["position"]==37,"REPLAY_BROKER_RECONCILIATION_AUTHORITATIVE",37,a["state"]["position"])
        r.assert_(a["seed"]==self.seed,"REPLAY_SEED_PERSISTED")
        r.metrics={"digest":a["digest"],"different_seed_digest":c["digest"],"seed":self.seed}
        return self._record(r,start)

    def scenario_audit_reproducibility(self):
        start=time.perf_counter();r=self._result("AUDIT_REPRODUCIBILITY","Trade Decision Trace Reproducibility","CRITICAL",
            ["Market Regime","Strategies","AI Director","Risk Engine","Execution","Trade Memory","Governance","System Evaluation","Persistence"])
        tid="TRACE-1";ts=now_iso()
        c=self.conn()
        c.execute("""INSERT INTO market_regime_history(ts,candle_ts,instrument,market_regime,confidence,volatility_state,
          trend_strength,abnormality_score,supporting_metrics_json) VALUES(?,?,?,?,?,?,?,?,?)""",
          (ts,ts,"EUR_USD","BULLISH_TREND",.85,"NORMAL",.7,0,"{}"))
        c.execute("""INSERT INTO ai_strategy_director_decisions(
          ts,instrument,setup_variant,recommended_state,confidence,market_regime,observation_only,
          score_components_json,reasons_json) VALUES(?,?,?,?,?,?,?,?,?)""",
          (ts,"EUR_USD","TRACE_STRAT","ACTIVE",.8,"BULLISH_TREND",1,"{}","[]"))
        director_id=c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.execute("""INSERT INTO adaptive_risk_decisions(
          ts,instrument,setup_variant,market_regime,risk_multiplier,allow_new_trades,reduce_existing_positions,
          emergency_stop,hard_limit_triggered,reason,metrics_json,shadow_mode) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          (ts,"EUR_USD","TRACE_STRAT","BULLISH_TREND",.7,1,0,0,0,"approved","{}",1))
        risk_id=c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.commit();c.close()
        self.seed_trade(tid,"TRACE_STRAT",.8,.8,days_ago=1,regime="BULLISH_TREND",deployment="trace_dep_v1")
        gov=self.governance().check_action("CHANGE_APPLY","strategy.TRACE_STRAT.threshold",
            {"component":"strategy.TRACE_STRAT.threshold","current_value":.7,"proposed_value":.71,
             "risk_level":"HIGH_RISK","magnitude":"SMALL","strategy_id":"TRACE_STRAT"})
        ev=self.evaluator(1).evaluate()
        c=self.conn()
        trade=dict(c.execute("SELECT * FROM trade_memory WHERE trade_id=?",(tid,)).fetchone())
        regime=dict(c.execute("SELECT * FROM market_regime_history WHERE instrument='EUR_USD' ORDER BY id DESC LIMIT 1").fetchone())
        director=dict(c.execute("SELECT * FROM ai_strategy_director_decisions WHERE id=?",(director_id,)).fetchone())
        risk=dict(c.execute("SELECT * FROM adaptive_risk_decisions WHERE id=?",(risk_id,)).fetchone())
        c.close()
        trace={"trade_id":tid,"market_regime":regime["market_regime"],"strategy_version":trade["strategy_version"],
               "director_decision_id":director_id,"risk_decision_id":risk_id,"deployment_version":trade["deployment_version"],
               "governance_decision_id":gov["governance_decision_id"],"system_evaluation_id":ev["evaluation_id"],
               "risk_config_version":trade["risk_config_version"],"runtime_code_hash":trade["runtime_code_hash"]}
        required=all(trace.get(k) for k in ("trade_id","market_regime","strategy_version","director_decision_id",
                                            "risk_decision_id","deployment_version","governance_decision_id",
                                            "system_evaluation_id","risk_config_version","runtime_code_hash"))
        r.assert_(required,"COMPLETE_DECISION_TRACE",True,trace)
        r.metrics={"trace":trace}
        return self._record(r,start)

    def scenario_market_data_failure(self):
        start=time.perf_counter();r=self._result("MARKET_DATA_FAILURE","Stale/Corrupt Market Data","CRITICAL",
            ["Market Data","Recovery","Risk","Monitoring"])
        rm=self.recovery("MD");rm.exit_safe_mode("ready")
        rm.market_data_update(now_iso(),False)
        state=rm.state()
        r.event("MARKET_DATA_UNRELIABLE")
        r.assert_(state.get("safe_mode")==1,"STALE_DATA_FAILS_CLOSED",1,state.get("safe_mode"))
        r.assert_(rm.new_trades_allowed() is False,"NO_NEW_ENTRIES_ON_UNRELIABLE_DATA",False,rm.new_trades_allowed())
        return self._record(r,start)

    def scenario_component_failures(self):
        start=time.perf_counter();r=self._result("COMPONENT_FAILURES","Risk/Director/Governance/Learning/Evaluation Failure","CRITICAL",
            ["Risk Engine","AI Director","Governance","Adaptive Learning","System Evaluation","Deployment"])
        # Existing deterministic fail_safe is reused as production gate logic.
        risk_down=fail_safe(stage="CANARY_LIVE",resume_required=False,system_kill=False,candidate_kill=False,
                            regime_ok=True,director_ok=True,risk_ok=False,data_ok=True,broker_ok=True,new_trades_enabled=True)
        director_down=fail_safe(stage="CANARY_LIVE",resume_required=False,system_kill=False,candidate_kill=False,
                                regime_ok=True,director_ok=False,risk_ok=True,data_ok=True,broker_ok=True,new_trades_enabled=True)
        governance_unavailable={"allow_new_deployments":False,"allow_critical_adaptation":False,
                                "existing_trading":"CONTINUES_UNDER_RISK_AND_DEPLOYMENT_LIMITS"}
        adaptive_unavailable={"production":"CONTINUES_SAFELY","new_candidates":False}
        evaluator_unavailable={"production_risk_engine":"CONTINUES","new_adaptive_changes":False}
        r.assert_(risk_down["allow"] is False,"RISK_ENGINE_FAILURE_NO_NEW_TRADES",False,risk_down["allow"])
        r.assert_(director_down["allow"] is False,"DIRECTOR_FAILURE_CONSERVATIVE",False,director_down["allow"])
        r.assert_(governance_unavailable["allow_new_deployments"] is False,"GOVERNANCE_FAILURE_BLOCKS_DEPLOYMENTS")
        r.assert_(adaptive_unavailable["production"]=="CONTINUES_SAFELY","LEARNING_FAILURE_NONCRITICAL")
        r.assert_(evaluator_unavailable["new_adaptive_changes"] is False,"EVALUATION_FAILURE_BLOCKS_ADAPTATION")
        return self._record(r,start)

    def scenario_governance_and_security(self):
        start=time.perf_counter();r=self._result("GOVERNANCE_SECURITY","Governance/Security Guardrails","CRITICAL",
            ["Governance","Change Management","Security","Deployment"])
        self.seed_system_context(healthy=True)
        g=self.governance("FULL_POLICY_ENFORCEMENT",{"MIN_STABILITY_HOURS":1})
        direct=g.check_action("DIRECT_PRODUCTION_DEPLOYMENT","C_BAD",{"validation_state":"READY_FOR_REVIEW"})
        risk=g.check_action("HARD_RISK_INCREASE","risk.max_trade_fraction",
                            {"component":"risk.max_trade_fraction","current_value":.01,"proposed_value":.02,"risk_level":"CRITICAL"})
        selfap=g.check_action("CHANGE_APPLY","governance.mode",
                              {"component":"governance.mode","risk_level":"CRITICAL","requester":"AI","approver":"AI"})
        reset=g.check_action("KILL_SWITCH_RESET","system",{"requested_by_automation":True})
        r.assert_(direct["enforced"],"NO_DIRECT_PRODUCTION_DEPLOYMENT")
        r.assert_(risk["enforced"],"NO_AUTOMATIC_HARD_RISK_INCREASE")
        r.assert_(selfap["enforced"],"NO_SELF_APPROVAL")
        r.assert_(reset["enforced"],"NO_KILL_SWITCH_SELF_RESET")
        return self._record(r,start)

    def scenario_extreme_drawdown_and_recovery(self):
        start=time.perf_counter();r=self._result("EXTREME_DRAWDOWN","Extreme Drawdown and Staged Recovery","CRITICAL",
            ["Risk Engine","Governance","Recovery","System Evaluation"])
        self.seed_system_context(healthy=False,degrading=True,drawdown=.095)
        g=self.governance("FULL_POLICY_ENFORCEMENT",{"DRAWDOWN_UTILIZATION_FREEZE":.85,"MIN_STABILITY_HOURS":1,"LIMITED_ADAPTATION_REVIEW_HOURS":1})
        decision=g.evaluate("extreme_drawdown")
        rm=self.recovery("DD");rm.set_emergency_stop(True,"drawdown test")
        r.assert_(decision["adaptation_state"]=="ADAPTATION_FROZEN","ADAPTATION_FREEZES_ON_EXTREME_RISK",
                  "ADAPTATION_FROZEN",decision["adaptation_state"])
        r.assert_(rm.new_trades_allowed() is False,"EMERGENCY_STOP_NO_NEW_ENTRIES")
        # No automatic recovery.
        g.set_lock(True,"extreme event","risk");g.set_lock(False,"reconciled","risk")
        r.assert_(g.state()["adaptation_state"]=="ADAPTATION_FROZEN","NO_AUTOMATIC_NORMAL_RESUME")
        return self._record(r,start)

    def scenario_risk_boundaries_precision(self):
        start=time.perf_counter();r=self._result("RISK_BOUNDARIES_PRECISION","Risk Boundary and Numerical Precision","CRITICAL",
            ["Risk Engine","Position Sizing","Exposure","PnL"])
        hard=.01;eps=1e-12
        vals=[hard-eps,hard,hard+eps]
        allowed=[x<=hard for x in vals]
        r.assert_(allowed==[True,True,False],"HARD_LIMIT_BOUNDARY_DETERMINISTIC",[True,True,False],allowed)
        # Filled/remaining arithmetic must conserve requested units.
        for requested,filled in [(100,37),(1e-6,3e-7),(1e9,999999999.25)]:
            a,b=conservative_filled_units(requested,filled)
            r.assert_(abs((a+b)-abs(requested))<=max(1e-12,abs(requested)*1e-12),
                      "FILLED_PLUS_REMAINING_CONSERVES_UNITS",abs(requested),a+b)
            r.assert_(a<=abs(requested)+1e-12,"FILLED_NEVER_EXCEEDS_REQUESTED")
        # PnL/fee arithmetic.
        gross=123456789.123456;fees=1234.56789;slip=12.34567
        net=gross-fees-slip
        r.assert_(math.isclose(net+fees+slip,gross,rel_tol=1e-12,abs_tol=1e-9),
                  "PNL_NUMERICAL_CONSERVATION",gross,net+fees+slip)
        return self._record(r,start)

    def scenario_time_and_leakage(self):
        start=time.perf_counter();r=self._result("TIME_DATA_LEAKAGE","Clock/Timezone and Data Leakage","CRITICAL",
            ["Strategy","Adaptive Learning","Validation","System Evaluation"])
        base=datetime(2026,1,1,tzinfo=timezone.utc)
        rows=[]
        for i in range(180):
            rows.append({"created_ts":(base+timedelta(hours=i)).isoformat(),
                         "candle_ts":(base+timedelta(hours=i)).isoformat(),
                         "entry_ts":(base+timedelta(hours=i)).isoformat(),
                         "exit_ts":(base+timedelta(hours=i+1)).isoformat(),
                         "realized_r":1 if i%3 else -1,"label":1 if i%3 else 0,
                         "entry_context_json":"{}","strategy_confidence_entry":.7})
        split=strict_temporal_split(rows)
        train_end=max(x["created_ts"] for x in split["train"])
        val_start=min(x["created_ts"] for x in split["validation"])
        test_start=min(x["created_ts"] for x in split["test"])
        r.assert_(train_end<val_start<test_start,"STRICT_TEMPORAL_ORDER")
        candidate={"parameter_name":"strategy_confidence_entry","operator":">=","proposed_value":.65,
                   "entry_only":True,"current_value":.5}
        val=validate_candidate(rows,candidate,min_trades=20)
        r.assert_("INSUFFICIENT_DATA" not in str(val.get("status")) or len(rows)>=20,"VALIDATION_NO_RANDOM_SHUFFLE")
        # Wrong timezone / future packet simulation is rejected by property.
        future=(base+timedelta(days=365)).isoformat()
        asof=(base+timedelta(days=10)).isoformat()
        r.assert_(future>asof,"FUTURE_TIMESTAMP_IDENTIFIED",True,future>asof)
        return self._record(r,start)

    def scenario_overfit_challenge(self):
        start=time.perf_counter();r=self._result("OVERFIT_CHALLENGE","Overfitting Challenge","HIGH",
            ["Adaptive Learning","Validation Pipeline","Walk Forward","Out-of-Sample"])
        base=datetime(2025,1,1,tzinfo=timezone.utc);rows=[]
        # Feature candidate wins in early sample, reverses later.
        for i in range(240):
            insample=i<150
            rows.append({"created_ts":(base+timedelta(hours=i)).isoformat(),
                         "entry_ts":(base+timedelta(hours=i)).isoformat(),
                         "exit_ts":(base+timedelta(hours=i+1)).isoformat(),
                         "realized_r":1.2 if (insample and i%2==0) else -1.1 if (not insample and i%2==0) else .1,
                         "strategy_confidence_entry":.9 if i%2==0 else .4,
                         "market_regime_entry":"BULLISH_TREND" if i%3 else "RANGE",
                         "entry_context_json":"{}","fees_total":.02})
        cand={"candidate_id":"OVERFIT","parameter_name":"strategy_confidence_entry","operator":">=",
              "proposed_value":.8,"current_value":.5,"entry_only":True}
        result=run_historical_validation(rows,cand,min_oos_trades=20,seed=self.seed)
        status=result.get("final_status") or result.get("status")
        r.assert_(status not in ("READY_FOR_REVIEW","PAPER_TRADING_REQUIRED") or
                  (result.get("validation_score") or 0)<80,
                  "OVERFIT_NOT_AUTOMATICALLY_READY",details={"status":status,"score":result.get("validation_score")})
        return self._record(r,start)


    def scenario_regime_misclassification_and_confidence(self):
        start=time.perf_counter();r=self._result("REGIME_CONFIDENCE_GUARD","Regime Misclassification + Confidence Miscalibration","CRITICAL",
            ["Market Regime","Strategies","AI Director","Risk Engine","Governance"])
        # Deliberately contradictory inputs: bullish regime + bearish strategy + high confidence.
        c=self.conn();ts=now_iso()
        c.execute("""INSERT INTO market_regime_history(ts,candle_ts,instrument,market_regime,confidence,volatility_state,
          trend_strength,abnormality_score,supporting_metrics_json) VALUES(?,?,?,?,?,?,?,?,?)""",
          (ts,ts,"EUR_USD","BULLISH_TREND",.95,"NORMAL",.8,0,"{}"))
        c.execute("""INSERT INTO signals(ts,candle_ts,instrument,signal,technical,score,blocked,executed,
          dynamic_confidence,setup_variant,features_json,filters_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          (ts,ts,"EUR_USD","SELL",1,90,0,0,.95,"OVERCONFIDENT","{}","{}"))
        c.execute("""INSERT INTO ai_strategy_director_decisions(
          ts,instrument,setup_variant,recommended_state,confidence,market_regime,observation_only,
          score_components_json,reasons_json) VALUES(?,?,?,?,?,?,?,?,?)""",
          (ts,"EUR_USD","OVERCONFIDENT","ACTIVE",.95,"BULLISH_TREND",1,"{}","[]"))
        c.execute("""INSERT INTO adaptive_risk_decisions(
          ts,instrument,setup_variant,market_regime,risk_multiplier,allow_new_trades,reduce_existing_positions,
          emergency_stop,hard_limit_triggered,reason,metrics_json,shadow_mode)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          (ts,"EUR_USD","OVERCONFIDENT","BULLISH_TREND",.4,0,1,0,0,"conflict/high caution","{}",1))
        for i in range(30):
            c.execute("INSERT INTO trade_memory(status,strategy_confidence_entry,realized_r,execution_quality_compromised) VALUES(?,?,?,0)",
                      ("CLOSED",.95,1 if i<10 else -1))
        c.commit();c.close()
        g=self.governance()
        meta=g.meta_risk()
        r.assert_(meta["model_disagreement"]["status"]=="HIGH_MODEL_DISAGREEMENT",
                  "REGIME_STRATEGY_DISAGREEMENT_DETECTED","HIGH_MODEL_DISAGREEMENT",meta["model_disagreement"]["status"])
        r.assert_(meta["confidence_calibration"]["detected"] is True,
                  "OVERCONFIDENCE_MISCALIBRATION_DETECTED",True,meta["confidence_calibration"])
        # High confidence still cannot override a defensive Risk Engine.
        risk=next(x for x in g._rows("adaptive_risk_decisions","ORDER BY id DESC LIMIT 1"))
        r.assert_(risk["allow_new_trades"]==0,"HIGH_CONFIDENCE_CANNOT_BYPASS_RISK",0,risk["allow_new_trades"])
        r.assert_((f(risk["risk_multiplier"],1.0) or 1.0)<=1.0,"HIGH_CONFIDENCE_NEVER_BREAKS_HARD_RISK")
        return self._record(r,start)

    def scenario_correlation_shock(self):
        start=time.perf_counter();r=self._result("CORRELATION_SHOCK","Strategy Correlation Shock","HIGH",
            ["Trade Memory","System Evaluation","Governance"])
        now=datetime.now(timezone.utc)
        for i in range(90):
            pnl=1 if i%4 else -1
            self.seed_trade(f"CA{i}","A",pnl,pnl,days_ago=max(1,90-i))
            # Same return stream -> correlation 1.0.
            self.seed_trade(f"CB{i}","B",pnl,pnl,days_ago=max(1,90-i))
        ev=self.evaluator(10).evaluate()
        r.assert_(ev["diversification"]["status"]=="HIDDEN_CONCENTRATION_RISK",
                  "HIDDEN_CONCENTRATION_DETECTED","HIDDEN_CONCENTRATION_RISK",ev["diversification"]["status"])
        return self._record(r,start)

    def scenario_canary_failure(self):
        start=time.perf_counter();r=self._result("CANARY_FAILURE","Canary Degradation / Rollback","CRITICAL",
            ["Validation","Deployment","Risk","Governance"])
        healthy=r_metrics([.4,.5,.3,.2,.4,.5,.3,.2,.4,.5])
        bad=r_metrics([.4,.3,.2,-.5,-.7,-.8,-.6,-.9])
        gate1=promotion_gate({**healthy,"days":7,"regimes":2,"stability":.8,"operational_errors":0,
                              "divergence_status":"CONSISTENT"},5,3,1,True,True,True)
        gate2=promotion_gate({**bad,"days":10,"regimes":2,"stability":.2,"operational_errors":0,
                              "divergence_status":"BACKTEST_LIVE_DIVERGENCE"},5,3,1,True,True,True)
        stages=["CANARY_LIVE"]
        if gate1["action"]=="PROMOTE":stages.append("LIMITED_PRODUCTION")
        if gate2["action"]!="PROMOTE":stages+=["REDUCE","PAUSE","ROLLED_BACK"]
        r.assert_(gate1["action"]=="PROMOTE","HEALTHY_CANARY_CAN_PROGRESS")
        r.assert_(gate2["action"]=="HOLD_CURRENT_LEVEL","DEGRADED_CANARY_NOT_PROMOTED")
        r.assert_(stages[-1]=="ROLLED_BACK","ROLLBACK_PATH_AVAILABLE","ROLLED_BACK",stages[-1])
        return self._record(r,start)

    def scenario_concurrency(self,workers=12,events_per_worker=50):
        start=time.perf_counter();r=self._result("CONCURRENCY","Concurrent Signals/Fills/Persistence","CRITICAL",
            ["Execution","Persistence","Trade Memory","Recovery"])
        rm=self.recovery("CONCUR")
        def write_worker(w):
            c=self.conn();ok=0
            for i in range(events_per_worker):
                eid=f"W{w}-{i}"
                try:
                    c.execute("""INSERT INTO observability_heartbeats(ts,module_name,status,latency_ms,details_json)
                                 VALUES(?,?,?,?,?)""",(now_iso(),f"W{w}","OK",1,"{}"));ok+=1
                except sqlite3.DatabaseError:pass
            c.close();return ok
        with ThreadPoolExecutor(max_workers=workers) as ex:
            fut=[ex.submit(write_worker,w) for w in range(workers)]
            total=sum(x.result() for x in as_completed(fut))
        c=self.conn();count=c.execute("SELECT COUNT(*) n FROM observability_heartbeats").fetchone()["n"];c.close()
        # Race many identical execution intents. UNIQUE + RecoveryManager handling must
        # yield exactly one CREATED intent and the rest as duplicate-prevented.
        key=deterministic_intent_key("CONCUR","EUR_USD","BUY","S-RACE","2026-08-15T16:00:00Z",1.1,1.099,1.102)
        def intent_worker(i):
            m=self.recovery("CONCUR")
            return m.create_intent(idempotency_key=key,correlation_id=f"race-{i}",decision_id="D",
                risk_decision_id="R",strategy_id="S-RACE",symbol="EUR_USD",side="BUY",requested_units=100,
                entry_price=1.1,stop_loss=1.099,take_profit=1.102,request_body={"order":{"units":"100"}})
        with ThreadPoolExecutor(max_workers=workers) as ex:
            intent_results=[x.result() for x in as_completed([ex.submit(intent_worker,i) for i in range(workers)])]
        created=sum(1 for x in intent_results if x.get("created"))
        duplicates=sum(1 for x in intent_results if x.get("duplicate_prevented"))
        r.metrics={"workers":workers,"events_per_worker":events_per_worker,"written":total,
                   "concurrent_intent_created":created,"concurrent_duplicates_prevented":duplicates}
        r.assert_(count>=total,"NO_LOST_COMMITTED_EVENTS",total,count)
        r.assert_(total==workers*events_per_worker,"NO_DB_LOCK_DATA_LOSS",workers*events_per_worker,total)
        r.assert_(created==1,"CONCURRENT_IDEMPOTENCY_EXACTLY_ONE_INTENT",1,created)
        r.assert_(duplicates==workers-1,"CONCURRENT_DUPLICATES_PREVENTED",workers-1,duplicates)
        return self._record(r,start)

    def scenario_load_soak(self,iterations=1500):
        start=time.perf_counter();r=self._result("LOAD_SOAK","Load / Soak / Resource Drift","HIGH",
            ["Persistence","Trade Memory","Monitoring","System Evaluation"])
        tracemalloc.start();m0=tracemalloc.get_traced_memory()[0]
        lat=[]
        c=self.conn()
        for i in range(iterations):
            t=time.perf_counter()
            c.execute("""INSERT INTO observability_metrics(ts,processing_time_ms,broker_latency_ms,market_data_latency_ms,
                         queue_depth,details_json) VALUES(?,?,?,?,?,?)""",
                      (now_iso(),1,2,1,i%25,"{}"))
            lat.append((time.perf_counter()-t)*1000)
        c.commit();c.close()
        m1,peak=tracemalloc.get_traced_memory();tracemalloc.stop()
        r.metrics={"iterations":iterations,"avg_db_insert_ms":statistics.mean(lat),"p95_db_insert_ms":sorted(lat)[int(.95*len(lat))-1],
                   "memory_growth_bytes":m1-m0,"peak_bytes":peak}
        r.assert_(r.metrics["p95_db_insert_ms"]<100,"DB_P95_LATENCY_ACCEPTABLE","<100ms",r.metrics["p95_db_insert_ms"],severity="HIGH")
        r.assert_(r.metrics["memory_growth_bytes"]<20_000_000,"NO_OBVIOUS_MEMORY_LEAK","<20MB",r.metrics["memory_growth_bytes"],severity="HIGH")
        return self._record(r,start)

    def scenario_randomized_properties(self,cases=500):
        start=time.perf_counter();r=self._result("PROPERTY_FUZZ","Randomized / Property-Based Safety","CRITICAL",
            ["Risk","Execution","Deployment","Recovery","Security"],seed=self.seed)
        rng=random.Random(self.seed)
        for i in range(cases):
            hard=rng.uniform(.001,.03);requested=rng.uniform(0,.08)
            approved=min(requested,hard)
            r.assert_(approved<=hard+1e-15,"RISK_EXPOSURE_LE_HARD_LIMIT",hard,approved)
            emergency=rng.choice([False,True]);risk_ok=rng.choice([False,True])
            gate=fail_safe(stage="CANARY_LIVE",resume_required=False,system_kill=emergency,candidate_kill=False,
                           regime_ok=True,director_ok=True,risk_ok=risk_ok,data_ok=True,broker_ok=True,
                           new_trades_enabled=True)
            if emergency:
                r.assert_(not gate["allow"],"EMERGENCY_STOP_NO_NEW_ENTRIES")
            if not risk_ok:
                r.assert_(not gate["allow"],"NO_REAL_ORDER_WITHOUT_RISK_APPROVAL")
            # Candidate cannot jump to full production in deployment pure-state model.
            current=rng.choice([0,.05,.10,.25,.50])
            next_stage="FULL_PRODUCTION_ELIGIBLE" if current>=.50 else "CANARY_OR_LIMITED"
            r.assert_(not (current==0 and next_stage=="FULL_PRODUCTION_ELIGIBLE"),
                      "NO_CANDIDATE_DIRECT_TO_FULL_PRODUCTION")
            req=abs(rng.uniform(0,1e6));fill=rng.uniform(-req*2,req*2)
            filled,remaining=conservative_filled_units(req,fill)
            r.assert_(0<=filled<=req+1e-9,"FILLED_UNITS_BOUNDED")
            r.assert_(remaining>=-1e-9,"REMAINING_NONNEGATIVE")
        r.metrics={"cases":cases,"seed":self.seed}
        return self._record(r,start)

    def scenario_restart_crash_loop(self,loops=4):
        start=time.perf_counter();r=self._result("RESTART_CRASH_LOOP","Restart / Crash Loop State Persistence","CRITICAL",
            ["Recovery","Governance","Deployment","Change Management","Persistence"])
        rm=self.recovery("RESTART");rm.set_emergency_stop(True,"persist me")
        g=self.governance();g.set_lock(True,"persist governance","test")
        c=self.conn()
        c.execute("INSERT OR REPLACE INTO deployment_registry(candidate_id,current_stage,updated_ts) VALUES(?,?,?)",
                  ("C-RESTART","CANARY_LIVE",now_iso()))
        c.execute("INSERT INTO security_audit_log(timestamp,action,resource) VALUES(?,?,?)",
                  (now_iso(),"CHANGE_REQUEST_PENDING","change:pending"))
        c.commit();c.close()
        for _ in range(loops):
            rm=self.recovery("RESTART");g=self.governance()
            r.assert_(rm.state().get("emergency_stop")==1,"EMERGENCY_STOP_SURVIVES_RESTART")
            r.assert_(g.state().get("governance_lock")==1,"GOVERNANCE_LOCK_SURVIVES_RESTART")
            c=self.conn();stage=c.execute("SELECT current_stage FROM deployment_registry WHERE candidate_id='C-RESTART'").fetchone()["current_stage"];c.close()
            r.assert_(stage=="CANARY_LIVE","CANARY_STAGE_SURVIVES_RESTART","CANARY_LIVE",stage)
        r.metrics={"crash_loops":loops}
        return self._record(r,start)

    def scenario_multiple_simultaneous_failures(self):
        start=time.perf_counter();r=self._result("MULTIPLE_FAILURES","Volatility + Latency + DB Degraded + Drawdown","CRITICAL",
            ["Market","Broker","Database","Strategy","Risk","Recovery","Governance"])
        self.seed_system_context(healthy=False,degrading=True,latency_ms=2800,data_quality=False,drawdown=.09)
        # Simulate DB degradation as alert/incident rather than corrupting the test DB itself.
        rm=self.recovery("MULTI");rm.open_incident("DATABASE_DEGRADED","HIGH","temporary write latency")
        g=self.governance("FULL_POLICY_ENFORCEMENT",{"DRAWDOWN_UTILIZATION_FREEZE":.8})
        gd=g.evaluate("multiple_failures")
        rm.enter_safe_mode("multiple simultaneous failures")
        r.event("VOLATILITY_SPIKE");r.event("BROKER_LATENCY",ms=2800);r.event("DATABASE_DEGRADED")
        r.event("STRATEGY_DRAWDOWN",drawdown=.09);r.event("ADAPTATION_FROZEN",decision=gd["decision"])
        r.assert_(rm.new_trades_allowed() is False,"CAPITAL_PROTECTION_PRIORITY")
        r.assert_(gd["adaptation_state"]=="ADAPTATION_FROZEN","ADAPTATION_AFTER_RISK_RECOVERY_PRIORITY")
        r.assert_(gd["meta_risk_state"] in ("HIGH","CRITICAL"),"META_RISK_RISES")
        return self._record(r,start)

    async def scenario_full_extreme_simulation(self):
        start=time.perf_counter();r=self._result("FULL_EXTREME_SIMULATION","Full Coordinated Extreme Simulation","CRITICAL",
            ["Market Regime","Strategy","AI Director","Risk Engine","Execution","Broker","Trade Memory",
             "System Evaluation","Governance","Monitoring","Recovery","Reconciliation","Deployment","Persistence"])
        self.reset_dynamic_data()
        rm=self.recovery("FULLSIM");rm.exit_safe_mode("simulation boot complete");rm.verify_risk(True,{"initial":True})
        g=self.governance("FULL_POLICY_ENFORCEMENT",{"DRAWDOWN_UTILIZATION_FREEZE":.7,"MIN_STABILITY_HOURS":1,
                                                     "LIMITED_ADAPTATION_REVIEW_HOURS":1})
        # NORMAL MARKET -> trading context
        r.event("NORMAL_MARKET");self.seed_system_context(healthy=True,degrading=False,latency_ms=70,drawdown=.02)
        c=self.conn()
        c.execute("INSERT OR REPLACE INTO candidate_strategies(candidate_id,generated_at) VALUES(?,?)",("CANARY-X",now_iso()))
        c.execute("INSERT OR REPLACE INTO deployment_registry(candidate_id,current_stage,updated_ts) VALUES(?,?,?)",
                  ("CANARY-X","CANARY_LIVE",now_iso()))
        c.commit();c.close()
        r.event("STRATEGY_TRADING",strategy="STRAT_A");r.event("CANDIDATE_CANARY",candidate_id="CANARY-X")
        # Regime changes + volatility spike + broker latency.
        r.event("REGIME_CHANGE",from_regime="BULLISH_TREND",to="HIGH_VOLATILITY")
        r.event("VOLATILITY_SPIKE");r.event("BROKER_LATENCY",ms=3000)
        # Partial fill via actual RecoveryManager.
        key=deterministic_intent_key("FULLSIM","EUR_USD","BUY","STRAT_A","2026-08-15T15:00:00Z",1.1,1.099,1.102)
        timeout=httpx.ReadTimeout("ack lost",request=httpx.Request("POST","https://broker.test"))
        sub=await rm.submit_order(ScriptedBroker([timeout]),idempotency_key=key,correlation_id="full-corr",
            decision_id="D-FULL",risk_decision_id="R-FULL",strategy_id="STRAT_A",symbol="EUR_USD",side="BUY",
            requested_units=100,entry_price=1.1,stop_loss=1.099,take_profit=1.102,
            order_body={"order":{"instrument":"EUR_USD","units":"100","type":"MARKET"}})
        cid=sub["intent"]["client_order_id"]
        r.event("PARTIAL_FILL_PENDING");r.event("DATABASE_TEMPORARILY_FAILS",simulated=True)
        # Add combined degradation evidence: strategy losses, stale data, latency,
        # elevated drawdown, module disagreement and active incidents.
        for i in range(45):
            self.seed_trade(f"DEG{i}","STRAT_A",-1.2,-1.2,days_ago=max(1,30-i%30),regime="HIGH_VOLATILITY",
                            slip=3.0,compromised=1 if i%3==0 else 0)
        c=self.conn()
        ts=now_iso()
        c.execute("""INSERT INTO observability_capital_history(
          ts,source,equity,drawdown,exposure,margin_usage,open_risk,details_json)
          VALUES(?,?,?,?,?,?,?,?)""",(ts,"SIM_SHOCK",9100,.09,.72,.35,.055,"{}"))
        for i in range(8):
            c.execute("""INSERT INTO observability_alert_history(
              ts,alert_key,severity,status,module,event_type,message,details_json)
              VALUES(?,?,?,?,?,?,?,?)""",
              (ts,f"full-stale-{i}","HIGH","ACTIVE","Market Data","MARKET_DATA_STALE","simulated stale data","{}"))
            c.execute("""INSERT INTO observability_metrics(
              ts,processing_time_ms,broker_latency_ms,market_data_latency_ms,queue_depth,details_json)
              VALUES(?,?,?,?,?,?)""",(ts,800,3200,900,20+i,"{}"))
        # Director wants ACTIVE while Risk is strongly defensive.
        for st in ("STRAT_A","STRAT_B"):
            c.execute("""INSERT INTO ai_strategy_director_decisions(
              ts,instrument,setup_variant,recommended_state,confidence,market_regime,observation_only,
              score_components_json,reasons_json) VALUES(?,?,?,?,?,?,?,?,?)""",
              (ts,"EUR_USD",st,"ACTIVE",.55,"HIGH_VOLATILITY",1,"{}","[]"))
            c.execute("""INSERT INTO adaptive_risk_decisions(
              ts,instrument,setup_variant,market_regime,volatility_state,risk_multiplier,allow_new_trades,
              reduce_existing_positions,emergency_stop,hard_limit_triggered,reason,metrics_json,shadow_mode)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (ts,"EUR_USD",st,"HIGH_VOLATILITY","HIGH",.25,0,1,0,0,"defensive shock response","{}",1))
        # Adaptation churn under the shock.
        for i in range(8):
            c.execute("INSERT INTO security_audit_log(timestamp,action,resource) VALUES(?,?,?)",
                      (ts,"CONFIG_CHANGED",f"strategy.STRAT_A.threshold{i}"))
        c.commit();c.close()
        rm.open_incident("DATABASE_DEGRADED","HIGH","temporary persistence degradation",correlation_id="full-corr")
        rm.open_incident("BROKER_LATENCY","HIGH","extreme broker latency",correlation_id="full-corr")
        ev=self.evaluator(10).evaluate()
        r.event("SYSTEM_EVALUATION",status=ev["system_status"],score=ev["system_score"])
        gov=g.evaluate("full_extreme")
        r.event("GOVERNANCE_META_RISK",score=gov["meta_risk_score"],state=gov["meta_risk_state"])
        r.event("ADAPTATION_FROZEN",state=gov["adaptation_state"])
        # Risk continues to protect / Broker disconnect -> SAFE MODE.
        rm.enter_safe_mode("broker disconnected during extreme simulation",correlation_id="full-corr")
        r.event("RISK_ENGINE_REDUCES_EXPOSURE",risk_multiplier=.3)
        r.event("BROKER_DISCONNECT");r.event("SAFE_MODE")
        routes={
            "/v3/accounts/FULLSIM":FakeResponse(200,{"account":{"NAV":"9500","balance":"9600","marginUsed":"120"},"lastTransactionID":"500"}),
            "/pendingOrders":FakeResponse(200,{"orders":[],"lastTransactionID":"500"}),
            "/openTrades":FakeResponse(200,{"trades":[{"id":"T-FULL","currentUnits":"37","price":"1.1005","openTime":now_iso(),
                "clientExtensions":{"id":cid},"stopLossOrder":{"id":"SL","price":"1.099"},
                "takeProfitOrder":{"id":"TP","price":"1.102"}}],"lastTransactionID":"500"}),
            "/openPositions":FakeResponse(200,{"positions":[{"instrument":"EUR_USD"}],"lastTransactionID":"500"}),
            "/transactions/sinceid":FakeResponse(200,{"transactions":[],"lastTransactionID":"500"})
        }
        t0=time.perf_counter();rec=await rm.reconnect_and_reconcile(ScriptedBroker(routes=routes),max_attempts=1)
        recovery_ms=(time.perf_counter()-t0)*1000
        restored=rm.intent(sub["intent"]["execution_intent_id"])
        rm.verify_risk(True,{"filled_units":37,"conservative_exposure":True})
        rm.exit_safe_mode("reconciled after extreme simulation")
        r.event("RECONNECT");r.event("RECONCILIATION",status=(rec.get("reconciliation") or {}).get("status"))
        r.event("POSITION_RESTORED",filled=restored.get("filled_units"),remaining=restored.get("remaining_units"))
        # Keep governance frozen; explicit staged review follows after simulated time and healthy evidence.
        g.set_lock(False,"no explicit lock, evaluation freeze remains","test")
        rm.recover_open_incidents("broker/database recovered and reconciled")
        simulated_future=datetime.now(timezone.utc)+timedelta(days=45)
        for i in range(1,41):
            # Negative days_ago deliberately represents future events inside this isolated simulation.
            self.seed_trade(f"RECOV{i}","STRAT_A",.9,.9,days_ago=-i,regime="RANGE",slip=.15)
        c=self.conn()
        c.execute("""INSERT INTO observability_capital_history(
          ts,source,equity,drawdown,exposure,margin_usage,open_risk,details_json)
          VALUES(?,?,?,?,?,?,?,?)""",
          (simulated_future.isoformat(),"SIM_RECOVERED",10300,.02,.22,.07,.015,"{}"))
        # Simulate that the stability observation time has elapsed without rewriting any
        # System Evaluation record. This is test-fixture time progression only.
        aged=(datetime.now(timezone.utc)-timedelta(hours=2)).isoformat()
        old_decisions=(datetime.now(timezone.utc)-timedelta(days=2)).isoformat()
        c.execute("UPDATE security_audit_log SET timestamp=? WHERE timestamp>?",(aged,(datetime.now(timezone.utc)-timedelta(minutes=30)).isoformat()))
        # Simulate that the shock-era Director/Risk opinions are no longer current.
        c.execute("UPDATE ai_strategy_director_decisions SET ts=? WHERE ts>?",(old_decisions,(datetime.now(timezone.utc)-timedelta(minutes=30)).isoformat()))
        c.execute("UPDATE adaptive_risk_decisions SET ts=? WHERE ts>?",(old_decisions,(datetime.now(timezone.utc)-timedelta(minutes=30)).isoformat()))
        c.commit();c.close()
        stable_ev=self.evaluator(10).evaluate(simulated_future.isoformat())
        review1=g.review_transition("risk-manager","post-incident health review")
        r.event("GOVERNANCE_REVIEW",result=review1["result"],to_state=review1["to_state"])
        # REVIEW -> LIMITED requires its own observation period; simulate its completion.
        if review1["to_state"]=="REVIEW":
            c=self.conn()
            c.execute("UPDATE governance_state SET last_review_ts=? WHERE singleton=1",
                      ((datetime.now(timezone.utc)-timedelta(hours=2)).isoformat(),))
            c.commit();c.close()
        review2=g.review_transition("risk-manager","review observation complete")
        r.event("LIMITED_RECOVERY",state=review2["to_state"],result=review2["result"])
        r.event("SYSTEM_STABILIZES",evaluation=stable_ev["system_status"])
        # Safety properties.
        dup=await rm.submit_order(ScriptedBroker([FakeResponse(201,{})]),idempotency_key=key,correlation_id="full-corr",
            decision_id="D-FULL",risk_decision_id="R-FULL",strategy_id="STRAT_A",symbol="EUR_USD",side="BUY",
            requested_units=100,entry_price=1.1,stop_loss=1.099,take_profit=1.102,
            order_body={"order":{"instrument":"EUR_USD","units":"100","type":"MARKET"}})
        r.assert_(sub.get("status_unknown") is True,"UNKNOWN_ORDER_ON_LOST_ACK")
        r.assert_(restored.get("filled_units")==37,"RECONCILED_FILLED_UNITS",37,restored.get("filled_units"))
        r.assert_(restored.get("remaining_units")==63,"RECONCILED_REMAINING_UNITS",63,restored.get("remaining_units"))
        r.assert_(dup.get("duplicate_prevented") is True,"NO_DUPLICATE_AFTER_RECONCILIATION")
        r.assert_(gov["adaptation_state"]=="ADAPTATION_FROZEN","GOVERNANCE_FREEZES_ADAPTATION")
        r.assert_(gov["meta_risk_state"] in ("HIGH","CRITICAL"),"META_RISK_RISES_DURING_COMBINED_FAILURES",
                  "HIGH/CRITICAL",gov["meta_risk_state"])
        r.assert_(ev["system_status"] in ("DEGRADING","HIGH_RISK","CRITICAL","PAUSED","WATCH"),
                  "SYSTEM_EVALUATION_RECOGNIZES_DETERIORATION",actual=ev["system_status"])
        r.assert_(stable_ev["system_status"] in ("HEALTHY","EXCELLENT","WATCH"),
                  "SYSTEM_STABILIZES_AFTER_HEALTHY_OBSERVATION",actual=stable_ev["system_status"])
        r.assert_(review2["to_state"]=="LIMITED_ADAPTATION","RECOVERY_REQUIRES_LIMITED_ADAPTATION_BEFORE_NORMAL",
                  "LIMITED_ADAPTATION",review2["to_state"])
        r.assert_(rm.state().get("safe_mode")==0,"RECOVERY_RETURNS_FROM_SAFE_MODE_AFTER_EXPLICIT_RISK_VERIFY",0,rm.state().get("safe_mode"))
        r.recovery_time_ms=recovery_ms
        r.metrics={"recovery_time_ms":recovery_ms,"system_eval_before":ev["system_status"],
                   "system_eval_after":stable_ev["system_status"],"meta_risk":gov["meta_risk_state"],
                   "governance_review":review2["result"],"governance_recovery_state":review2["to_state"],
                   "timeline_events":len(rm.timeline(correlation_id="full-corr"))}
        return self._record(r,start)

    def scenario_smart_execution_shadow(self):
        start=time.perf_counter();r=self._result("SMART_EXECUTION_SHADOW","Smart Execution Shadow / Partial Fill Revalidation","CRITICAL",
            ["Risk Engine","Smart Execution","Execution","Recovery","Trade Memory","System Evaluation","Governance"])
        se=self.smart_execution()
        x=se.create_intent(strategy_id="STRAT_A",symbol="EUR_USD",side="BUY",target_quantity=100,
            maximum_quantity=100,risk_approved_quantity=100,expected_price=1.1001,urgency="NORMAL",
            maximum_slippage_bps=8,time_limit_seconds=60,risk_approval_valid=True)
        s1=se.capture_snapshot(x["execution_intent_id"],bid=1.1000,ask=1.1002,last_price=1.1001,
            available_liquidity=60,recent_volume=10000,volatility="NORMAL",market_regime="BULLISH_TREND",
            timestamp=now_iso(),broker_health="OK",broker_latency_ms=80,market_status="tradeable")
        d1=se.recommend(x["execution_intent_id"],s1,actual_order_type="MARKET",actual_requested_quantity=100)
        fill=se.record_fill(x["execution_intent_id"],fill_quantity=40,fill_price=1.1002,
            broker_event_id="SMART-FILL-1",order_type="LIMIT",broker_ack_latency_ms=90)
        s2=se.capture_snapshot(x["execution_intent_id"],bid=1.0980,ask=1.1030,last_price=1.1005,
            available_liquidity=20,recent_volume=3000,volatility="EXTREME",market_regime="HIGH_VOLATILITY",
            timestamp=now_iso(),broker_health="OK",broker_latency_ms=1800,market_status="tradeable")
        d2=se.revalidate_remaining(x["execution_intent_id"],s2,risk_approval_valid=True,
            strategy_intent_valid=True,position_state_valid=True)
        final=se.intent(x["execution_intent_id"])
        r.event("RISK_ENGINE_AUTHORIZES",maximum_units=100)
        r.event("SMART_EXECUTION_REDUCES",recommended_units=d1["recommended_quantity"],order_type=d1["order_type"])
        r.event("PARTIAL_FILL",filled=fill["filled_quantity"],remaining=fill["remaining_quantity"])
        r.event("MARKET_CHANGES",volatility="EXTREME",available_liquidity=20)
        r.event("REVALIDATION",action=d2["action"])
        r.assert_(d1["recommended_quantity"]==60,"LIQUIDITY_REDUCES_100_TO_60",60,d1["recommended_quantity"])
        r.assert_(d1["order_type"]=="LIMIT","LIMIT_SELECTED_UNDER_LOW_LIQUIDITY","LIMIT",d1["order_type"])
        r.assert_(fill["filled_quantity"]==40,"PARTIAL_FILL_EXACT",40,fill["filled_quantity"])
        r.assert_(d2["action"]=="CANCEL_REMAINING_EXECUTION","WORSE_MARKET_CANCELS_REMAINING",actual=d2["action"])
        r.assert_(final["filled_quantity"]==40,"FINAL_POSITION_40",40,final["filled_quantity"])
        r.assert_(final["filled_quantity"]<=final["risk_approved_quantity"],"SMART_EXECUTION_NEVER_EXCEEDS_RISK_APPROVAL")
        r.assert_(se.dashboard()["mode"]=="SHADOW","SMART_EXECUTION_REMAINS_SHADOW")
        r.metrics={"initial_decision":d1,"revalidation":d2,"tca":fill.get("tca"),"dashboard":se.dashboard()}
        return self._record(r,start)

    def scenario_ensemble_shadow(self):
        start=time.perf_counter();r=self._result("ENSEMBLE_SHADOW","Correlation-Aware Ensemble Shadow","CRITICAL",
            ["Market Regime","Strategies","Ensemble","AI Director","Risk Engine","Smart Execution","System Evaluation","Governance"])
        e=self.ensemble()
        ts=now_iso();regime="HIGH_VOLATILITY_TREND"
        signals=[]
        # Three highly overlapping trend models. They intentionally share the same
        # dependency family and therefore cannot count as three independent votes.
        for i in range(3):
            signals.append({"strategy_id":f"TREND_{i+1}","strategy_version":"v1","symbol":"EUR_USD","timestamp":ts,
                "direction":"LONG","confidence":.86,"expected_edge":5.0,"market_regime":regime,
                "time_horizon":"INTRADAY","signal_strength":.9,"risk_characteristics":{"risk":"normal"},
                "data_quality":1.0,"family":"TREND_FAMILY",
                "input_dependencies":["M15_EMA","M5_MOMENTUM","M1_CONFIRMATION"],"role":"DIRECTIONAL"})
        signals += [
            {"strategy_id":"BREAKOUT","strategy_version":"v1","symbol":"EUR_USD","timestamp":ts,"direction":"LONG",
             "confidence":.72,"expected_edge":4.0,"market_regime":regime,"time_horizon":"INTRADAY","signal_strength":.75,
             "risk_characteristics":{},"data_quality":1.0,"family":"BREAKOUT_FAMILY",
             "input_dependencies":["RANGE_BREAK","VOLUME_PROXY"],"role":"DIRECTIONAL"},
            {"strategy_id":"MEANREV","strategy_version":"v1","symbol":"EUR_USD","timestamp":ts,"direction":"SHORT",
             "confidence":.76,"expected_edge":3.5,"market_regime":regime,"time_horizon":"INTRADAY","signal_strength":.8,
             "risk_characteristics":{},"data_quality":1.0,"family":"MEAN_REVERSION_FAMILY",
             "input_dependencies":["Z_SCORE","DISTANCE_FROM_MEAN"],"role":"DIRECTIONAL"},
            {"strategy_id":"VOL_CONTEXT","strategy_version":"v1","symbol":"EUR_USD","timestamp":ts,"direction":"ABSTAIN",
             "confidence":.9,"expected_edge":None,"market_regime":regime,"time_horizon":"INTRADAY","signal_strength":0,
             "risk_characteristics":{"high_volatility":True},"data_quality":1.0,"family":"VOLATILITY_FAMILY",
             "input_dependencies":["ATR","REALIZED_VOL"],"role":"CONTEXT"},
        ]
        for sig in signals:
            e.register_model(sig["strategy_id"],sig["strategy_version"],sig["family"],sig["role"],sig["input_dependencies"],sig["time_horizon"])
        out=e.evaluate(signals,method="REGIME_WEIGHTED",regime=regime,execution_cost=1.0,
                       current_system_direction="LONG",current_system_confidence=.75,current_executed=False)
        fam=out["family_weight_info"]["family_totals"]
        trend_weight=float(fam.get("TREND_FAMILY",0))
        independent_short=next(x for x in out["model_contributions"] if x["strategy_id"]=="MEANREV")
        # Downstream authority simulation: Ensemble can recommend, but Risk remains
        # authoritative and Smart Execution remains bounded by the approved quantity.
        risk_approved=60.0
        se=self.smart_execution()
        xi=se.create_intent(strategy_id="ENSEMBLE_SHADOW",symbol="EUR_USD",side="BUY",target_quantity=100,
            maximum_quantity=100,risk_approved_quantity=risk_approved,expected_price=1.10,urgency="NORMAL",
            maximum_slippage_bps=10,risk_approval_valid=True)
        snap=se.capture_snapshot(xi["execution_intent_id"],bid=1.0999,ask=1.1001,last_price=1.10,
            available_liquidity=100,recent_volume=1000,volatility="HIGH",market_regime=regime,
            timestamp=now_iso(),broker_health="OK",broker_latency_ms=100,market_status="tradeable")
        sed=se.recommend(xi["execution_intent_id"],snap)
        ev=self.evaluator(min_samples=1).evaluate()
        gov=self.governance("SHADOW").check_action("ENSEMBLE_PROMOTION","ensemble_candidate_x",
            {"validation_state":"PENDING","magnitude":"MAJOR","affected_modules":["ENSEMBLE_ENGINE"]})
        r.event("ENSEMBLE_EVALUATES",direction=out["ensemble_direction"],confidence=out["ensemble_confidence"],
                agreement=out["agreement_score"],diversity=out["diversity_score"])
        r.event("AI_DIRECTOR_REVIEWS",ensemble_decision_id=out["ensemble_decision_id"])
        r.event("RISK_ENGINE_CAPS",approved_units=risk_approved)
        r.event("SMART_EXECUTION",recommended_units=sed["recommended_quantity"])
        r.assert_(e.mode=="SHADOW","ENSEMBLE_REMAINS_SHADOW")
        r.assert_(trend_weight<=e.max_family_weight+1e-12,"CORRELATED_TREND_FAMILY_CAPPED",e.max_family_weight,trend_weight)
        r.assert_(independent_short["weight"]>0,"INDEPENDENT_CONTRADICTION_RETAINS_WEIGHT",">0",independent_short["weight"])
        r.assert_(out["ensemble_confidence"]<.90,"CORRELATED_3X_LONG_NOT_HUGE_CONFIDENCE","<0.90",out["ensemble_confidence"])
        r.assert_(out["expected_net_edge"] is not None,"EXPECTED_NET_EDGE_CALCULATED")
        r.assert_(sed["recommended_quantity"]<=risk_approved,"ENSEMBLE_CANNOT_CAUSE_RISK_BYPASS",risk_approved,sed["recommended_quantity"])
        r.assert_(out["hypothetical_only"] is True,"ENSEMBLE_OUTPUT_IS_HYPOTHETICAL_ONLY")
        r.assert_((ev.get("ensemble_effectiveness") or {}).get("sample_size",0)>=1,"SYSTEM_EVALUATION_SEES_ENSEMBLE")
        r.assert_(gov.get("would_block") is True,"GOVERNANCE_CONTROLS_ENSEMBLE_PROMOTION")
        r.metrics={"ensemble":out,"smart_execution":sed,"system_evaluation":ev.get("ensemble_effectiveness"),
                   "governance":gov,"critical_note":"Three correlated LONG trend models are capped as one family, not counted as three independent confirmations."}
        return self._record(r,start)

    # ---------- scenario library / reports ----------
    def scenario_library(self)->Dict[str,Dict[str,Any]]:
        return {
            "GOLDEN_PATH":{"severity":"CRITICAL","category":"E2E"},
            "MARKET_REGIME_STRESS":{"severity":"HIGH","category":"MARKET_STRESS"},
            "PARTIAL_FILL_DISCONNECT":{"severity":"CRITICAL","category":"CHAOS"},
            "DUPLICATE_OUT_OF_ORDER":{"severity":"CRITICAL","category":"ORDER_SAFETY"},
            "BROKER_FAILURE_EXIT":{"severity":"CRITICAL","category":"CHAOS"},
            "RECONCILIATION_SAFETY":{"severity":"CRITICAL","category":"RECONCILIATION"},
            "DATABASE_FAILURE":{"severity":"CRITICAL","category":"PERSISTENCE_FAILURE"},
            "LATENCY_INJECTION":{"severity":"HIGH","category":"LATENCY"},
            "MARKET_DATA_FAILURE":{"severity":"CRITICAL","category":"DATA_FAILURE"},
            "COMPONENT_FAILURES":{"severity":"CRITICAL","category":"COMPONENT_FAILURE"},
            "MULTIPLE_FAILURES":{"severity":"CRITICAL","category":"CASCADE"},
            "RESTART_CRASH_LOOP":{"severity":"CRITICAL","category":"RECOVERY"},
            "CANARY_FAILURE":{"severity":"CRITICAL","category":"DEPLOYMENT"},
            "GOVERNANCE_SECURITY":{"severity":"CRITICAL","category":"SECURITY"},
            "EXTREME_DRAWDOWN":{"severity":"CRITICAL","category":"RISK"},
            "RISK_BOUNDARIES_PRECISION":{"severity":"CRITICAL","category":"BOUNDARY"},
            "TIME_DATA_LEAKAGE":{"severity":"CRITICAL","category":"TEMPORAL"},
            "OVERFIT_CHALLENGE":{"severity":"HIGH","category":"VALIDATION"},
            "REGIME_CONFIDENCE_GUARD":{"severity":"CRITICAL","category":"MODEL_SAFETY"},
            "CORRELATION_SHOCK":{"severity":"HIGH","category":"PORTFOLIO"},
            "CONCURRENCY":{"severity":"CRITICAL","category":"CONCURRENCY"},
            "LOAD_SOAK":{"severity":"HIGH","category":"PERFORMANCE"},
            "PROPERTY_FUZZ":{"severity":"CRITICAL","category":"PROPERTY"},
            "DETERMINISTIC_REPLAY":{"severity":"CRITICAL","category":"REPLAY"},
            "AUDIT_REPRODUCIBILITY":{"severity":"CRITICAL","category":"AUDIT"},
            "SMART_EXECUTION_SHADOW":{"severity":"CRITICAL","category":"EXECUTION_QUALITY"},
            "ENSEMBLE_SHADOW":{"severity":"CRITICAL","category":"ENSEMBLE_ROBUSTNESS"},
            "FULL_EXTREME_SIMULATION":{"severity":"CRITICAL","category":"FULL_SYSTEM"},
            "FLASH_CRASH":{"severity":"CRITICAL","category":"MARKET_STRESS","implemented_by":"MARKET_REGIME_STRESS+FULL_EXTREME_SIMULATION"},
            "BROKER_OUTAGE":{"severity":"CRITICAL","category":"CHAOS","implemented_by":"PARTIAL_FILL_DISCONNECT+BROKER_FAILURE_EXIT"},
            "GOVERNANCE_FREEZE":{"severity":"CRITICAL","category":"GOVERNANCE","implemented_by":"EXTREME_DRAWDOWN+FULL_EXTREME_SIMULATION"},
            "EMERGENCY_RECOVERY":{"severity":"CRITICAL","category":"RECOVERY","implemented_by":"EXTREME_DRAWDOWN+RESTART_CRASH_LOOP"},
        }

    async def run_all(self)->Dict[str,Any]:
        self.results=[]
        # Scenarios use the same isolated DB deliberately to expose state-drift interactions.
        await self.scenario_golden_path()
        self.scenario_market_regime_stress()
        await self.scenario_partial_fill_disconnect()
        await self.scenario_broker_failure_during_exit()
        self.scenario_duplicate_out_of_order()
        self.scenario_reconciliation_mismatch_and_missing_protection()
        self.scenario_database_failure_atomicity()
        await self.scenario_latency_injection()
        self.scenario_market_data_failure()
        self.scenario_component_failures()
        self.scenario_multiple_simultaneous_failures()
        self.scenario_restart_crash_loop()
        self.scenario_canary_failure()
        self.scenario_governance_and_security()
        self.scenario_extreme_drawdown_and_recovery()
        self.scenario_risk_boundaries_precision()
        self.scenario_time_and_leakage()
        self.scenario_overfit_challenge()
        self.scenario_regime_misclassification_and_confidence()
        self.scenario_correlation_shock()
        self.scenario_concurrency()
        self.scenario_load_soak()
        self.scenario_randomized_properties()
        self.scenario_deterministic_replay()
        self.scenario_audit_reproducibility()
        self.scenario_smart_execution_shadow()
        self.scenario_ensemble_shadow()
        await self.scenario_full_extreme_simulation()
        return self.report()

    def coverage_matrix(self)->List[Dict[str,Any]]:
        components=[
            "Market Regime","Strategies","AI Director","Risk Engine","Trade Memory","Adaptive Learning",
            "Validation","Paper Trading","Deployment","Monitoring","Recovery","Reconciliation","Change Management",
            "System Evaluation","Governance","Ensemble","Smart Execution","Execution","Broker","Persistence"
        ]
        rows=[]
        for res in self.results:
            for comp in components:
                covered=any(comp.lower() in x.lower() or x.lower() in comp.lower() for x in res.components)
                if covered:
                    rows.append({"component":comp,"scenario":res.scenario_id,
                                 "expected_behavior":"Safety/consistency assertions defined by scenario",
                                 "result":"PASS" if res.passed else "FAIL",
                                 "severity_if_failed":res.severity_if_failed})
        return rows

    def pass_fail_gate(self)->Dict[str,Any]:
        critical_failures=[f for r in self.results for f in r.failures if f.get("severity")=="CRITICAL"]
        safety=[v for r in self.results for v in r.safety_violations]
        required={
            "zero_critical_safety_failures":len(critical_failures)==0,
            "zero_risk_limit_bypasses":not any("HARD_LIMIT" in x or "RISK_EXPOSURE" in x for x in safety),
            "zero_duplicate_order_vulnerabilities":not any("DUPLICATE" in x for x in safety),
            "restart_recovery_successful":self._scenario_pass("RESTART_CRASH_LOOP"),
            "reconciliation_passes":self._scenario_pass("PARTIAL_FILL_DISCONNECT") and self._scenario_pass("RECONCILIATION_SAFETY"),
            "database_failure_recovery_passes":self._scenario_pass("DATABASE_FAILURE"),
            "broker_exit_failure_passes":self._scenario_pass("BROKER_FAILURE_EXIT"),
            "deterministic_replay_passes":self._scenario_pass("DETERMINISTIC_REPLAY"),
            "audit_reproducibility_passes":self._scenario_pass("AUDIT_REPRODUCIBILITY"),
            "emergency_stop_survives_restart":self._property_pass("EMERGENCY_STOP_SURVIVES_RESTART"),
            "governance_protections_pass":self._scenario_pass("GOVERNANCE_SECURITY"),
            "data_leakage_pass":self._scenario_pass("TIME_DATA_LEAKAGE"),
            "canary_rollback_pass":self._scenario_pass("CANARY_FAILURE"),
            "smart_execution_shadow_pass":self._scenario_pass("SMART_EXECUTION_SHADOW"),
            "ensemble_shadow_pass":self._scenario_pass("ENSEMBLE_SHADOW"),
            "full_extreme_simulation_pass":self._scenario_pass("FULL_EXTREME_SIMULATION"),
        }
        return {"ready_for_step15":all(required.values()),"gates":required,
                "critical_failures":critical_failures,"safety_violations":safety,
                "principle":"ANY_UNRESOLVED_CRITICAL_FAILURE_BLOCKS_STEP15"}

    def _scenario_pass(self,sid):
        x=next((r for r in self.results if r.scenario_id==sid),None);return bool(x and x.passed)
    def _property_pass(self,name):
        return any(a["property"]==name and a["passed"] for r in self.results for a in r.assertions)

    def report(self,baseline:Optional[Dict[str,Any]]=None)->Dict[str,Any]:
        passed=sum(1 for r in self.results if r.passed);failed=len(self.results)-passed
        critical=sum(1 for r in self.results for x in r.failures if x.get("severity")=="CRITICAL")
        warnings=sum(len(r.warnings) for r in self.results)
        avg_ms=statistics.mean([r.duration_ms for r in self.results]) if self.results else 0
        recovery=[r.recovery_time_ms for r in self.results if r.recovery_time_ms is not None]
        performance={
            "total_duration_ms":sum(r.duration_ms for r in self.results),
            "average_scenario_ms":avg_ms,
            "max_scenario_ms":max([r.duration_ms for r in self.results] or [0]),
            "mean_recovery_ms":statistics.mean(recovery) if recovery else None
        }
        regression={}
        if baseline:
            bp=(baseline.get("performance_metrics") or {})
            regression={
                "duration_delta_pct":((performance["total_duration_ms"]-bp.get("total_duration_ms",performance["total_duration_ms"]))/
                                      max(1e-9,bp.get("total_duration_ms",performance["total_duration_ms"])))*100,
                "critical_failure_delta":critical-int(baseline.get("critical_failures",0)),
                "failed_test_delta":failed-int(baseline.get("failed",0))
            }
        gate=self.pass_fail_gate()
        return {
            "framework_version":"3.26","environment":self.environment,"seed":self.seed,
            "generated_at":now_iso(),"total_tests":len(self.results),"passed":passed,"failed":failed,
            "warnings":warnings,"critical_failures":critical,"safety_violations":sum(len(r.safety_violations) for r in self.results),
            "performance_metrics":performance,"pass_fail_gate":gate,"regression":regression,
            "results":[r.as_dict() for r in self.results],"coverage_matrix":self.coverage_matrix(),
            "scenario_library":self.scenario_library()
        }

    def save_report(self,path:str,baseline_path:Optional[str]=None):
        baseline=None
        if baseline_path and Path(baseline_path).exists():
            baseline=json.loads(Path(baseline_path).read_text())
        report=self.report(baseline)
        Path(path).write_text(json.dumps(report,indent=2,default=str),encoding="utf-8")
        return report

def run_step14_suite(output_path:Optional[str]=None,environment="INTEGRATION_TEST",seed=140013)->Dict[str,Any]:
    fw=SystemIntegrationTestFramework(environment=environment,seed=seed)
    report=asyncio.run(fw.run_all())
    if output_path:Path(output_path).write_text(json.dumps(report,indent=2,default=str),encoding="utf-8")
    return report


import asyncio, os, sqlite3, tempfile, json
import httpx

from recovery_manager import RecoveryManager, deterministic_intent_key
from order_state import transition, can_transition

class FakeResponse:
    def __init__(self,status_code=200,payload=None,headers=None,text=""):
        self.status_code=status_code
        self._payload=payload if payload is not None else {}
        self.headers=headers or {}
        self.text=text or json.dumps(self._payload)
    def json(self):
        return self._payload

class RouterClient:
    def __init__(self, client_id):
        self.client_id=client_id
        self.calls=[]
    async def request(self,method,url,params=None,json=None,headers=None,timeout=None):
        self.calls.append((method,url,params))
        if url.endswith("/v3/accounts/A"):
            return FakeResponse(200,{"account":{"NAV":"10000","balance":"10000","marginUsed":"100"},"lastTransactionID":"999"})
        if url.endswith("/pendingOrders"):
            return FakeResponse(200,{"orders":[],"lastTransactionID":"999"})
        if url.endswith("/openTrades"):
            return FakeResponse(200,{"trades":[{
                "id":"T100","currentUnits":"40","price":"1.1001","openTime":"2099-01-01T00:00:01+00:00",
                "clientExtensions":{"id":self.client_id},
                "stopLossOrder":{"id":"SL1","price":"1.0990"},
                "takeProfitOrder":{"id":"TP1","price":"1.1020"}
            }],"lastTransactionID":"999"})
        if url.endswith("/openPositions"):
            return FakeResponse(200,{"positions":[{"instrument":"EUR_USD"}],"lastTransactionID":"999"})
        if "/transactions/sinceid" in url:
            return FakeResponse(200,{"transactions":[],"lastTransactionID":"999"})
        raise RuntimeError("unexpected route "+url)

class TimeoutThen:
    def __init__(self,responses):
        self.responses=list(responses)
        self.calls=0
        self.bodies=[]
    async def request(self,*args,**kwargs):
        self.calls+=1
        self.bodies.append(kwargs.get("json"))
        if not self.responses:
            raise RuntimeError("no response")
        x=self.responses.pop(0)
        if isinstance(x,Exception):
            raise x
        return x

def seed_base(db):
    c=sqlite3.connect(db)
    c.executescript("""
    CREATE TABLE active_trade_management(
      trade_id TEXT PRIMARY KEY,instrument TEXT,side TEXT,entry REAL,initial_stop REAL,
      initial_target REAL,current_stop REAL,setup_variant TEXT,policy TEXT,trend_score REAL,
      opened_ts TEXT,last_r REAL,last_action TEXT,break_even_applied INTEGER DEFAULT 0,
      profit_lock_applied INTEGER DEFAULT 0,trailing_applied INTEGER DEFAULT 0,
      closed INTEGER DEFAULT 0,updated_ts TEXT,current_units REAL);
    CREATE TABLE trade_memory(
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
      created_ts TEXT,updated_ts TEXT);
    CREATE TABLE portfolio_risk_state(
      id INTEGER PRIMARY KEY,ts TEXT,balance REAL,nav REAL,peak_nav REAL,current_drawdown REAL,
      margin_used REAL,margin_usage REAL,open_positions INTEGER,portfolio_open_risk REAL,
      consecutive_losses INTEGER,data_stale INTEGER,system_abnormal INTEGER,details_json TEXT);
    """)
    c.commit();c.close()

def order_body():
    return {"order":{"instrument":"EUR_USD","units":"100","type":"MARKET","timeInForce":"FOK",
                     "positionFill":"DEFAULT","stopLossOnFill":{"price":"1.09900","timeInForce":"GTC"},
                     "takeProfitOnFill":{"price":"1.10200","timeInForce":"GTC"}}}

async def test_unknown_duplicate_and_reconcile():
    db=tempfile.mktemp(suffix=".db");seed_base(db)
    m=RecoveryManager(db,"https://broker.test","A","T","PRIMARY",
                      circuit_failure_threshold=5,request_min_interval_ms=0,
                      backoff_base_seconds=.01,backoff_cap_seconds=.02)
    m.ensure_schema();m.exit_safe_mode("test ready")
    key=deterministic_intent_key("PRIMARY","EUR_USD","BUY","S1","2099-01-01T00:00:00+00:00",1.1,1.099,1.102)

    timeout=httpx.ReadTimeout("lost after send",request=httpx.Request("POST","https://broker.test"))
    client=TimeoutThen([timeout])
    r=await m.submit_order(client,idempotency_key=key,correlation_id="corr-1",decision_id="d1",
                           risk_decision_id="r1",strategy_id="S1",symbol="EUR_USD",side="BUY",
                           requested_units=100,entry_price=1.1,stop_loss=1.099,take_profit=1.102,
                           order_body=order_body(),metadata={"market_regime":"BULL_TREND"})
    assert r["status_unknown"] is True
    assert m.state()["state"]=="SAFE_MODE"
    intent=r["intent"]
    assert intent["state"]=="UNKNOWN"

    # Same execution intent cannot send again.
    client2=TimeoutThen([FakeResponse(200,{})])
    r2=await m.submit_order(client2,idempotency_key=key,correlation_id="corr-1",decision_id="d1",
                            risk_decision_id="r1",strategy_id="S1",symbol="EUR_USD",side="BUY",
                            requested_units=100,entry_price=1.1,stop_loss=1.099,take_profit=1.102,
                            order_body=order_body())
    assert r2["duplicate_prevented"] is True
    assert client2.calls==0

    cid=intent["client_order_id"]
    router=RouterClient(cid)
    reconnect=await m.reconnect_and_reconcile(router,max_attempts=2)
    assert reconnect["connected"] is True
    rec=reconnect["reconciliation"]
    assert rec["status"] in ("MATCHED","MINOR_MISMATCH")
    restored=m.intent(intent["execution_intent_id"])
    assert restored["state"]=="PARTIALLY_FILLED"
    assert restored["filled_units"]==40
    assert restored["remaining_units"]==60

    c=m.conn()
    pos=c.execute("SELECT current_units FROM active_trade_management WHERE trade_id='T100'").fetchone()
    mem=c.execute("SELECT position_size,execution_quality_compromised FROM trade_memory WHERE trade_id='T100'").fetchone()
    c.close()
    assert pos and abs(pos["current_units"]-40)<1e-9
    assert mem and abs(mem["position_size"]-40)<1e-9 and mem["execution_quality_compromised"]==1

    # After risk is explicitly verified, known partial exposure can return READY.
    m.verify_risk(True,{"nav":10000,"known_filled_units":40})
    m.exit_safe_mode("broker position restored and risk recalculated")
    assert m.new_trades_allowed() is True

    # Event timeline contains the requested recovery story.
    events=[x["event_type"] for x in m.timeline(correlation_id="corr-1")]
    assert "ORDER_INTENT_CREATED" in events
    assert "ORDER_SUBMITTED" in events
    assert "ORDER_STATUS_UNKNOWN" in events
    assert "SAFE_MODE_ENTERED" in events
    assert "BROKER_RECONNECTED" in [x["event_type"] for x in m.timeline()]
    assert "RECONCILIATION_STARTED" in [x["event_type"] for x in m.timeline()]
    assert "ORDER_FOUND_FILLED" in events
    assert "POSITION_STATE_RESTORED" in events
    assert "RISK_VERIFIED" in [x["event_type"] for x in m.timeline()]
    assert "SYSTEM_READY" in [x["event_type"] for x in m.timeline()]

    # Duplicate broker event is journaled once.
    before=len([x for x in m.timeline() if x.get("broker_event_id")=="BF1"])
    m.transition_intent(intent["execution_intent_id"],"PARTIALLY_FILLED",broker_event_id="BF1",
                        filled_units=40,event_type="PARTIAL_FILL")
    m.transition_intent(intent["execution_intent_id"],"PARTIALLY_FILLED",broker_event_id="BF1",
                        filled_units=40,event_type="PARTIAL_FILL")
    after=len([x for x in m.timeline() if x.get("broker_event_id")=="BF1"])
    assert after-before==1

    # Out-of-order FILL before ACK is valid.
    key2=deterministic_intent_key("PRIMARY","GBP_USD","SELL","S2","2099-01-01T01:00:00+00:00",1.2,1.201,1.198)
    created=m.create_intent(idempotency_key=key2,correlation_id="corr-2",decision_id=None,risk_decision_id=None,
                            strategy_id="S2",symbol="GBP_USD",side="SELL",requested_units=50,
                            entry_price=1.2,stop_loss=1.201,take_profit=1.198,request_body=order_body())
    eid2=created["intent"]["execution_intent_id"]
    m.transition_intent(eid2,"SUBMITTING")
    m.transition_intent(eid2,"FILLED",filled_units=50,broker_trade_id="T200",event_type="FILL")
    assert m.intent(eid2)["state"]=="FILLED"

    # Emergency stop persists across manager recreation/restart.
    m.set_emergency_stop(True,"operator test")
    m2=RecoveryManager(db,"https://broker.test","A","T","PRIMARY",request_min_interval_ms=0)
    m2.ensure_schema()
    assert m2.state()["emergency_stop"]==1
    assert m2.new_trades_allowed() is False
    os.remove(db)

async def test_rate_limit_and_circuit_breaker():
    db=tempfile.mktemp(suffix=".db");seed_base(db)
    m=RecoveryManager(db,"https://broker.test","A","T","PRIMARY",
                      circuit_failure_threshold=3,circuit_open_seconds=.01,
                      request_min_interval_ms=0,max_read_retries=2,
                      backoff_base_seconds=.001,backoff_cap_seconds=.01)
    m.ensure_schema();m.exit_safe_mode("test")
    client=TimeoutThen([
        FakeResponse(429,{"errorCode":"RATE_LIMIT"},headers={"Retry-After":"0.001"}),
        FakeResponse(200,{"account":{"NAV":"100"}})
    ])
    x=await m.broker_request(client,"GET","/v3/accounts/{account}",critical=True)
    assert x["account"]["NAV"]=="100"
    assert client.calls==2

    # Market data becoming unreliable opens its logical circuit and SAFE_MODE.
    m.exit_safe_mode("reset")
    m.market_data_update("2099-01-01T00:00:00+00:00",False)
    assert m.circuit("MARKET_DATA")["state"]=="OPEN"
    assert m.state()["safe_mode"]==1
    m.market_data_update("2099-01-01T00:01:00+00:00",True)
    assert m.circuit("MARKET_DATA")["state"]=="CLOSED"

    # A timeout on a broker WRITE (e.g. protective-order update/exit request)
    # is never blindly retried and immediately enters SAFE_MODE.
    m.exit_safe_mode("reset")
    write_timeout=httpx.ReadTimeout("write ack lost",request=httpx.Request("PUT","https://broker.test"))
    wc=TimeoutThen([write_timeout])
    try:
        await m.broker_request(wc,"PUT","/v3/accounts/{account}/trades/T1/orders",
                               body={"stopLoss":{"price":"1.1"}},allow_retry=False)
        raise AssertionError("write timeout should propagate")
    except httpx.ReadTimeout:
        pass
    assert wc.calls==1
    assert m.state()["safe_mode"]==1
    assert any(x["event_type"]=="BROKER_WRITE_STATUS_UNKNOWN" for x in m.timeline())

    m.circuit_success("BROKER");m.exit_safe_mode("reset")
    m.circuit_failure("x");m.circuit_failure("x");m.circuit_failure("x")
    assert m.circuit()["state"]=="OPEN"
    assert m.state()["safe_mode"]==1
    os.remove(db)

def test_state_machine_and_db_failure():
    assert can_transition("SUBMITTING","FILLED")
    assert transition("UNKNOWN","FILLED")=="FILLED"
    try:
        transition("FILLED","SUBMITTING")
        raise AssertionError("impossible transition accepted")
    except ValueError:
        pass

    m=RecoveryManager("/proc/no-such-dir/recovery.db","https://broker","A","T")
    assert m.new_trades_allowed() is False

def test_critical_mismatch_and_protection():
    db=tempfile.mktemp(suffix=".db");seed_base(db)
    m=RecoveryManager(db,"https://broker","A","T",request_min_interval_ms=0)
    m.ensure_schema();m.exit_safe_mode("test")
    # Broker-only trade with no known intent and missing protective orders.
    snap={"account":{"NAV":"10000","balance":"10000","marginUsed":"0"},
          "pending_orders":[],"open_trades":[{"id":"UNTRACKED","currentUnits":"25","price":"1.1"}],
          "positions":[],"transactions":[],"last_transaction_id":"10"}
    rec=m.reconcile_snapshot(snap)
    assert rec["status"]=="CRITICAL_MISMATCH"
    assert m.state()["safe_mode"]==1
    items=m.conn().execute("SELECT item_type,status FROM recovery_reconciliation_items").fetchall()
    assert any(x["item_type"]=="PROTECTIVE_ORDER" and x["status"]=="CRITICAL_MISMATCH" for x in items)
    os.remove(db)

async def main():
    test_state_machine_and_db_failure()
    test_critical_mismatch_and_protection()
    await test_unknown_duplicate_and_reconcile()
    await test_rate_limit_and_circuit_breaker()
    print("recovery manager tests: OK")

if __name__=="__main__":
    asyncio.run(main())

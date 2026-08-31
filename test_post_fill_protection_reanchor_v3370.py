import asyncio
import json
import sqlite3
from pathlib import Path

import httpx
import pytest

import server


def _geom(side, fill):
    return server.post_fill_protection_geometry(
        "EUR_USD", side, 100.0, 99.0 if side == "BUY" else 101.0,
        101.5 if side == "BUY" else 98.5, fill,
    )


def test_buy_favorable_fill_preserves_planned_geometry():
    g=_geom("BUY",99.5)
    assert g["applied_stop"]==pytest.approx(98.5)
    assert g["applied_target"]==pytest.approx(101.0)
    assert g["planned_risk_distance"]==pytest.approx(1.0)
    assert g["planned_reward_distance"]==pytest.approx(1.5)
    assert g["rr"]==pytest.approx(1.5)


def test_buy_adverse_fill_preserves_planned_geometry():
    g=_geom("BUY",100.5)
    assert g["applied_stop"]==pytest.approx(99.5)
    assert g["applied_target"]==pytest.approx(102.0)
    assert g["rr"]==pytest.approx(1.5)


def test_sell_favorable_fill_preserves_planned_geometry():
    g=_geom("SELL",100.5)
    assert g["applied_stop"]==pytest.approx(101.5)
    assert g["applied_target"]==pytest.approx(99.0)
    assert g["rr"]==pytest.approx(1.5)


def test_sell_adverse_fill_preserves_planned_geometry():
    g=_geom("SELL",99.5)
    assert g["applied_stop"]==pytest.approx(100.5)
    assert g["applied_target"]==pytest.approx(98.0)
    assert g["rr"]==pytest.approx(1.5)


def test_usd_jpy_real_case_reanchors_to_nine_pip_risk_and_1_5rr():
    planned_entry=160.041
    planned_stop=159.951
    planned_target=160.176
    fill=159.989
    before_risk=(fill-planned_stop)/server.pip_size("USD_JPY")
    before_reward=(planned_target-fill)/server.pip_size("USD_JPY")
    assert before_risk==pytest.approx(3.8)
    assert before_reward==pytest.approx(18.7)
    assert before_reward/before_risk==pytest.approx(4.9210526316)

    g=server.post_fill_protection_geometry(
        "USD_JPY","BUY",planned_entry,planned_stop,planned_target,fill)
    assert server.format_instrument_price("USD_JPY",g["applied_stop"])=="159.899"
    assert server.format_instrument_price("USD_JPY",g["applied_target"])=="160.124"
    assert g["risk_pips"]==pytest.approx(9.0)
    assert g["reward_pips"]==pytest.approx(13.5)
    assert g["rr"]==pytest.approx(1.5)

    one_r=server.adaptive_stop_price("BUY",fill,g["applied_stop"],160.079,"BE_PROFIT_TRAIL")
    assert one_r["r_multiple"]==pytest.approx(1.0)
    assert one_r["action"]=="BREAK_EVEN"


@pytest.mark.parametrize("instrument,expected",[
    ("EUR_USD",("1.09915","1.10140")),
    ("GBP_USD",("1.09915","1.10140")),
    ("AUD_USD",("1.09915","1.10140")),
    ("USD_CAD",("1.09915","1.10140")),
])
def test_five_decimal_pairs_use_registry_precision(instrument,expected):
    g=server.post_fill_protection_geometry(instrument,"BUY",1.10000,1.09910,1.10135,1.10005)
    assert server.format_instrument_price(instrument,g["applied_stop"])==expected[0]
    assert server.format_instrument_price(instrument,g["applied_target"])==expected[1]
    assert g["risk_pips"]==pytest.approx(9.0)
    assert g["reward_pips"]==pytest.approx(13.5)


def test_replace_trade_protection_sends_stop_and_target_in_one_put_body(monkeypatch):
    calls=[]
    async def fake_req(client,method,path,params=None,body=None):
        calls.append((method,path,params,body)); return {"ok":True}
    monkeypatch.setattr(server,"req",fake_req)
    asyncio.run(server.replace_trade_protection(object(),"T1","USD_JPY",159.899,160.124))
    assert calls==[("PUT","/v3/accounts/{account}/trades/T1/orders",None,{
        "stopLoss":{"price":"159.899","timeInForce":"GTC"},
        "takeProfit":{"price":"160.124","timeInForce":"GTC"},
    })]


def test_replace_trade_protection_uses_supplied_instrument_not_eur_fallback(monkeypatch):
    instruments=[]; calls=[]
    original=server.format_instrument_price
    def spy(inst,price):
        instruments.append(inst); return original(inst,price)
    async def fake_req(client,method,path,params=None,body=None):
        calls.append((method,body)); return {}
    monkeypatch.setattr(server,"format_instrument_price",spy)
    monkeypatch.setattr(server,"req",fake_req)
    asyncio.run(server.replace_trade_protection(object(),"JPY_TRADE","USD_JPY",159.899,160.124))
    assert instruments==["USD_JPY","USD_JPY"]
    assert calls[0][0]=="PUT"


def _trade_payload(stop=None,target=None):
    trade={}
    if stop is not None: trade["stopLossOrder"]={"price":str(stop)}
    if target is not None: trade["takeProfitOrder"]={"price":str(target)}
    return {"trade":trade}


def test_verify_protection_requires_exact_broker_grounded_prices(monkeypatch):
    async def correct(*args,**kwargs): return _trade_payload("159.899","160.124")
    monkeypatch.setattr(server,"req",correct)
    ok=asyncio.run(server.verify_trade_protection(object(),"T","USD_JPY",159.899,160.124))
    assert ok["status"]=="OK" and ok["stop_match"] and ok["target_match"]

    async def bad_stop(*args,**kwargs): return _trade_payload("159.900","160.124")
    monkeypatch.setattr(server,"req",bad_stop)
    bad=asyncio.run(server.verify_trade_protection(object(),"T","USD_JPY",159.899,160.124))
    assert bad["status"]=="PROTECTION_ERROR" and not bad["stop_match"] and bad["target_match"]

    async def bad_target(*args,**kwargs): return _trade_payload("159.899","160.125")
    monkeypatch.setattr(server,"req",bad_target)
    bad=asyncio.run(server.verify_trade_protection(object(),"T","USD_JPY",159.899,160.124))
    assert bad["status"]=="PROTECTION_ERROR" and bad["stop_match"] and not bad["target_match"]

    async def missing(*args,**kwargs): return _trade_payload("159.899",None)
    monkeypatch.setattr(server,"req",missing)
    missing_result=asyncio.run(server.verify_trade_protection(object(),"T","USD_JPY",159.899,160.124))
    assert missing_result["status"]=="PROTECTION_ERROR" and not missing_result["tp_ok"]


def test_verify_protection_uses_metadata_rounding_tolerance(monkeypatch):
    # JPY display precision=3 => half-quantum class differences are tolerated,
    # while a full display tick is not.
    async def within(*args,**kwargs): return _trade_payload("159.8994","160.1236")
    monkeypatch.setattr(server,"req",within)
    result=asyncio.run(server.verify_trade_protection(object(),"T","USD_JPY",159.899,160.124))
    assert result["status"]=="OK"


def test_verify_without_expected_prices_never_returns_false_ok(monkeypatch):
    async def present(*args,**kwargs): return _trade_payload("1.09900","1.10200")
    monkeypatch.setattr(server,"req",present)
    result=asyncio.run(server.verify_trade_protection(object(),"T"))
    assert result["status"]=="PROTECTION_PRESENT_UNVERIFIED"
    assert result["status"]!="OK"


def _candidate(inst="USD_JPY",side="BUY"):
    if inst=="USD_JPY":
        return {"instrument":inst,"signal":side,"entry":160.041,
                "stop":159.951 if side=="BUY" else 160.131,
                "target":160.176 if side=="BUY" else 159.906,"managed_target":160.176 if side=="BUY" else 159.906}
    return {"instrument":inst,"signal":side,"entry":1.1000,
            "stop":1.0991 if side=="BUY" else 1.1009,
            "target":1.10135 if side=="BUY" else 1.09865,"managed_target":1.10135 if side=="BUY" else 1.09865}


def test_reanchor_success_performs_one_put_then_get_and_confirms(monkeypatch):
    calls=[]
    async def fake_req(client,method,path,params=None,body=None):
        calls.append((method,path,params,body))
        if method=="PUT": return {"lastTransactionID":"1"}
        return _trade_payload("159.899","160.124")
    monkeypatch.setattr(server,"req",fake_req)
    monkeypatch.setattr(server,"RECOVERY_MANAGER_ENABLED",False)
    result=asyncio.run(server.reanchor_post_fill_protection(object(),_candidate(),"T1",159.989,"C1"))
    assert result["status"]=="OK" and result["confirmed"] is True
    assert [x[0] for x in calls]==["PUT","GET"]
    assert sum(1 for x in calls if x[0]=="PUT")==1


def test_timeout_is_not_retried_and_enters_safe_mode(monkeypatch):
    calls=[]; safe=[]; journals=[]
    async def fake_req(client,method,path,params=None,body=None):
        calls.append(method)
        if method=="PUT": raise httpx.ReadTimeout("unknown")
        return _trade_payload("159.951","160.176")
    monkeypatch.setattr(server,"req",fake_req)
    monkeypatch.setattr(server,"RECOVERY_MANAGER_ENABLED",True)
    monkeypatch.setattr(server.recovery_manager,"enter_safe_mode",lambda *a,**kw:safe.append((a,kw)))
    monkeypatch.setattr(server.recovery_manager,"journal",lambda *a,**kw:journals.append((a,kw)))
    result=asyncio.run(server.reanchor_post_fill_protection(object(),_candidate(),"T1",159.989,"C1"))
    assert result["status"]=="UNKNOWN" and result["confirmed"] is False
    assert calls.count("PUT")==1
    assert safe and safe[-1][1]["severity"]=="CRITICAL"
    assert any(x[0][0]=="POST_FILL_PROTECTION_REANCHOR_UNKNOWN" for x in journals)


def test_http_4xx_is_not_retried_and_does_not_false_confirm(monkeypatch):
    calls=[]; safe=[]
    async def fake_req(client,method,path,params=None,body=None):
        calls.append(method)
        if method=="PUT": raise RuntimeError("BROKER_HTTP_400: invalid protective order")
        return _trade_payload("159.951","160.176")
    monkeypatch.setattr(server,"req",fake_req)
    monkeypatch.setattr(server,"RECOVERY_MANAGER_ENABLED",True)
    monkeypatch.setattr(server.recovery_manager,"enter_safe_mode",lambda *a,**kw:safe.append((a,kw)))
    monkeypatch.setattr(server.recovery_manager,"journal",lambda *a,**kw:None)
    result=asyncio.run(server.reanchor_post_fill_protection(object(),_candidate(),"T1",159.989,"C1"))
    assert result["status"]=="FAILED" and result["confirmed"] is False
    assert calls.count("PUT")==1
    assert result["effective_stop"]==pytest.approx(159.951)
    assert result["effective_target"]==pytest.approx(160.176)
    assert safe


def test_verification_mismatch_enters_safe_mode_and_is_not_ok(monkeypatch):
    safe=[]
    async def fake_req(client,method,path,params=None,body=None):
        if method=="PUT": return {"ok":True}
        return _trade_payload("159.951","160.176")
    monkeypatch.setattr(server,"req",fake_req)
    monkeypatch.setattr(server,"RECOVERY_MANAGER_ENABLED",True)
    monkeypatch.setattr(server.recovery_manager,"enter_safe_mode",lambda *a,**kw:safe.append((a,kw)))
    monkeypatch.setattr(server.recovery_manager,"journal",lambda *a,**kw:None)
    result=asyncio.run(server.reanchor_post_fill_protection(object(),_candidate(),"T1",159.989,"C1"))
    assert result["status"]=="VERIFY_MISMATCH" and result["confirmed"] is False
    assert result["verification"]["status"]=="PROTECTION_ERROR"
    assert safe


def _management_db(path: Path):
    c=sqlite3.connect(path); c.row_factory=sqlite3.Row
    c.execute("""CREATE TABLE active_trade_management(
      trade_id TEXT PRIMARY KEY,instrument TEXT,side TEXT,entry REAL,initial_stop REAL,initial_target REAL,current_stop REAL,
      setup_variant TEXT,policy TEXT,trend_score REAL,opened_ts TEXT,last_r REAL,last_action TEXT,updated_ts TEXT,closed INTEGER DEFAULT 0,
      current_units REAL,break_even_applied INTEGER DEFAULT 0,profit_lock_applied INTEGER DEFAULT 0,trailing_applied INTEGER DEFAULT 0)""")
    c.execute("""CREATE TABLE trade_forward_observations(
      trade_id TEXT PRIMARY KEY,instrument TEXT,side TEXT,opened_ts TEXT,be_trigger_r REAL,be_lock_r REAL,max_r_seen REAL DEFAULT 0,
      be_activated_ts TEXT,be_activation_r REAL,max_r_after_be REAL,updated_ts TEXT)""")
    c.execute("""CREATE TABLE trade_forward_events(
      id INTEGER PRIMARY KEY AUTOINCREMENT,trade_id TEXT,ts TEXT,event TEXT,r_multiple REAL,detail_json TEXT DEFAULT '{}',
      UNIQUE(trade_id,event))""")
    c.commit(); c.close()


def _conn_factory(path):
    def f():
        c=sqlite3.connect(path); c.row_factory=sqlite3.Row; return c
    return f


def test_register_trade_management_uses_confirmed_reanchored_geometry(monkeypatch,tmp_path):
    db=tmp_path/"tm.db"; _management_db(db)
    monkeypatch.setattr(server,"conn",_conn_factory(db))
    monkeypatch.setattr(server,"trend_runner_score",lambda r:0.0)
    monkeypatch.setattr(server,"setup_variant",lambda r:"TEST")
    r={"instrument":"USD_JPY","signal":"BUY","entry":160.041,"stop":159.951,"score":80,"features":{},"filters":{}}
    server.register_trade_management("T",r,160.176,100,159.989,applied_stop=159.899,applied_target=160.124)
    c=_conn_factory(db)(); row=c.execute("SELECT * FROM active_trade_management WHERE trade_id='T'").fetchone(); c.close()
    assert row["entry"]==pytest.approx(159.989)
    assert row["initial_stop"]==pytest.approx(159.899)
    assert row["initial_target"]==pytest.approx(160.124)
    assert row["current_stop"]==pytest.approx(159.899)
    proposal=server.adaptive_stop_price("BUY",row["entry"],row["initial_stop"],160.079,"BE_PROFIT_TRAIL")
    assert proposal["r_multiple"]==pytest.approx(1.0)


def test_trade_memory_keeps_planned_and_effective_geometry(monkeypatch,tmp_path):
    db=tmp_path/"memory.db"
    monkeypatch.setattr(server,"DB",str(db))
    monkeypatch.setattr(server,"TRADE_MEMORY_ENABLED",True)
    c=server.conn()
    existing={row[1] for row in c.execute("PRAGMA table_info(trade_memory)").fetchall()}
    for name,kind in (("release_id","TEXT"),("production_certification_id","TEXT"),("production_stage","TEXT")):
        if name not in existing:
            c.execute(f"ALTER TABLE trade_memory ADD COLUMN {name} {kind}")
    c.commit(); c.close()
    r={"instrument":"USD_JPY","signal":"BUY","entry":160.041,"stop":159.951,"target":160.176,
       "managed_target":160.176,"score":80,"rr":1.5,"rr_raw":1.5,"features":{},"filters":{},
       "market_regime":{},"candle_ts":"2026-08-30T15:00:00+00:00"}
    reanchor={"status":"OK","confirmed":True,
              "geometry":{"applied_stop":159.899,"applied_target":160.124},
              "verification":{"status":"OK","broker_stop":159.899,"broker_target":160.124},
              "effective_stop":159.899,"effective_target":160.124}
    fill={"id":"F1","time":"2026-08-30T15:00:01+00:00","tradeOpened":{"tradeID":"T1","units":"100"}}
    server.record_trade_memory_entry("T1",1,"O1",r,{}, {}, {}, "TEST",fill,159.989,-5.2,reanchor)
    c=server.conn(); row=c.execute("SELECT * FROM trade_memory WHERE trade_id='T1'").fetchone(); c.close()
    entry=json.loads(row["entry_context_json"]); execution=json.loads(row["execution_context_json"])
    assert entry["planned_entry"]==pytest.approx(160.041)
    assert entry["planned_stop"]==pytest.approx(159.951)
    assert entry["planned_target"]==pytest.approx(160.176)
    assert row["entry_price"]==pytest.approx(159.989)
    assert row["stop_loss"]==pytest.approx(159.899)
    assert row["take_profit"]==pytest.approx(160.124)
    assert execution["actual_fill_price"]==pytest.approx(159.989)
    assert execution["initial_stop_on_fill"]==pytest.approx(159.951)
    assert execution["initial_target_on_fill"]==pytest.approx(160.176)
    assert execution["applied_stop"]==pytest.approx(159.899)
    assert execution["applied_target"]==pytest.approx(160.124)
    assert execution["protection_reanchor_confirmed"] is True


@pytest.mark.parametrize("entry,stop,target",[
    (1.1,1.1,1.2),
    (1.1,1.0,1.1),
])
def test_invalid_zero_risk_or_reward_fails_safe(entry,stop,target):
    with pytest.raises(ValueError):
        server.post_fill_protection_geometry("EUR_USD","BUY",entry,stop,target,entry)


def test_rounding_cannot_invert_protection_orientation():
    with pytest.raises(ValueError):
        server.post_fill_protection_geometry("EUR_USD","BUY",1.1000000,1.0999999,1.1000001,1.1000000)

def test_trade_memory_does_not_false_plan_geometry_when_reanchor_state_is_unknown(monkeypatch,tmp_path):
    db=tmp_path/"memory_unknown.db"
    monkeypatch.setattr(server,"DB",str(db))
    monkeypatch.setattr(server,"TRADE_MEMORY_ENABLED",True)
    c=server.conn()
    existing={row[1] for row in c.execute("PRAGMA table_info(trade_memory)").fetchall()}
    for name,kind in (("release_id","TEXT"),("production_certification_id","TEXT"),("production_stage","TEXT")):
        if name not in existing:
            c.execute(f"ALTER TABLE trade_memory ADD COLUMN {name} {kind}")
    c.commit(); c.close()
    r={"instrument":"USD_JPY","signal":"BUY","entry":160.041,"stop":159.951,"target":160.176,
       "managed_target":160.176,"score":80,"rr":1.5,"rr_raw":1.5,"features":{},"filters":{},
       "market_regime":{},"candle_ts":"2026-08-30T15:00:00+00:00"}
    reanchor={"status":"UNKNOWN","confirmed":False,
              "geometry":{"applied_stop":159.899,"applied_target":160.124},
              "verification":None,"effective_stop":None,"effective_target":None}
    fill={"id":"F2","time":"2026-08-30T15:00:01+00:00","tradeOpened":{"tradeID":"T2","units":"100"}}
    server.record_trade_memory_entry("T2",2,"O2",r,{}, {}, {}, "TEST",fill,159.989,-5.2,reanchor)
    c=server.conn(); row=c.execute("SELECT stop_loss,take_profit,execution_context_json FROM trade_memory WHERE trade_id='T2'").fetchone(); c.close()
    execution=json.loads(row["execution_context_json"])
    assert row["stop_loss"] is None
    assert row["take_profit"] is None
    assert execution["protection_reanchor_status"]=="UNKNOWN"
    assert execution["initial_stop_on_fill"]==pytest.approx(159.951)
    assert execution["initial_target_on_fill"]==pytest.approx(160.176)

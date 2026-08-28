import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
import server


def _management_db(path: Path):
    c=sqlite3.connect(path)
    c.row_factory=sqlite3.Row
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


def _conn_factory(path: Path):
    def _conn():
        c=sqlite3.connect(path)
        c.row_factory=sqlite3.Row
        return c
    return _conn


def _trade_row():
    return (
        "144","EUR_USD","SELL",1.16042,1.16132,1.15772,1.16132,"TEST","BE_PROFIT_TRAIL",0.0,
        "2026-08-28T15:32:18+00:00",0.9333333333333826,"OPEN","2026-08-28T16:04:03+00:00",0,100.0,0,0,0
    )


def test_replace_trade_stop_routes_payload_as_json_body(monkeypatch):
    captured={}
    async def fake_req(client,method,path,params=None,body=None):
        captured.update(method=method,path=path,params=params,body=body)
        return {"ok":True}
    monkeypatch.setattr(server,"req",fake_req)
    asyncio.run(server.replace_trade_stop(object(),"144",1.16042))
    assert captured["method"]=="PUT"
    assert captured["params"] is None
    assert captured["body"]=={"stopLoss":{"price":"1.16042","timeInForce":"GTC"}}


def test_register_trade_management_uses_actual_fill(monkeypatch,tmp_path):
    db=tmp_path/"tm.db"
    _management_db(db)
    monkeypatch.setattr(server,"conn",_conn_factory(db))
    monkeypatch.setattr(server,"trend_runner_score",lambda r:0.0)
    r={"instrument":"EUR_USD","signal":"SELL","entry":1.16042,"stop":1.16132,"score":80,"features":{},"filters":{}}
    monkeypatch.setattr(server,"setup_variant",lambda r:"TEST")
    server.register_trade_management("144",r,1.15772,100,1.16021)
    c=_conn_factory(db)(); row=c.execute("SELECT entry,initial_stop FROM active_trade_management WHERE trade_id='144'").fetchone(); c.close()
    assert row["entry"]==pytest.approx(1.16021)
    assert row["initial_stop"]==pytest.approx(1.16132)


def test_new_entry_gate_blocks_7_to_10_new_york_only():
    # August is EDT (UTC-4).
    blocked=server.new_entry_time_gate(datetime(2026,8,28,13,30,tzinfo=timezone.utc))  # 09:30 ET
    before=server.new_entry_time_gate(datetime(2026,8,28,10,59,tzinfo=timezone.utc))   # 06:59 ET
    after=server.new_entry_time_gate(datetime(2026,8,28,14,0,tzinfo=timezone.utc))     # 10:00 ET
    assert blocked["allowed"] is False
    assert before["allowed"] is True
    assert after["allowed"] is True


def test_manage_failure_is_persisted_observable_and_retried(monkeypatch,tmp_path):
    db=tmp_path/"tm_fail.db"
    _management_db(db)
    cf=_conn_factory(db)
    c=cf(); c.execute("INSERT INTO active_trade_management VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",_trade_row()); c.commit(); c.close()
    monkeypatch.setattr(server,"conn",cf)
    monkeypatch.setattr(server,"TREND_RUNNER_MIN_SCORE",0.62)
    monkeypatch.setattr(server,"OBSERVABILITY_ENABLED",True)
    monkeypatch.setattr(server,"RECOVERY_MANAGER_ENABLED",True)
    calls=[]; alerts=[]; journals=[]
    async def reject(client,trade_id,price):
        calls.append((trade_id,price))
        raise RuntimeError("BROKER_HTTP_400: simulated rejection")
    monkeypatch.setattr(server,"replace_trade_stop",reject)
    monkeypatch.setattr(server.observability_manager,"alert",lambda *a,**kw: alerts.append((a,kw)))
    monkeypatch.setattr(server.recovery_manager,"journal",lambda *a,**kw: journals.append((a,kw)))
    # 1.15951 is >1R favorable using the planned legacy entry and must attempt BE.
    changed=asyncio.run(server.manage_open_trades(object(),"EUR_USD",1.15951))
    assert changed==0
    assert calls and calls[0][0]=="144"
    c=cf(); row=c.execute("SELECT last_r,last_action,break_even_applied,current_stop FROM active_trade_management WHERE trade_id='144'").fetchone();
    evt=c.execute("SELECT event FROM trade_forward_events WHERE trade_id='144' AND event='REACHED_1_00R'").fetchone(); c.close()
    assert row["last_r"]>=1.0
    assert row["last_action"]=="OPEN"
    assert row["break_even_applied"]==0
    assert row["current_stop"]==pytest.approx(1.16132)
    assert evt is not None
    assert alerts and alerts[0][0][0].startswith("TRADE_MANAGEMENT_UPDATE_FAILED:144")
    assert journals and journals[0][0][0]=="PROTECTIVE_ORDER_UPDATE_FAILED"


def test_manage_success_applies_break_even(monkeypatch,tmp_path):
    db=tmp_path/"tm_ok.db"
    _management_db(db)
    cf=_conn_factory(db)
    c=cf(); c.execute("INSERT INTO active_trade_management VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",_trade_row()); c.commit(); c.close()
    monkeypatch.setattr(server,"conn",cf)
    monkeypatch.setattr(server,"TREND_RUNNER_MIN_SCORE",0.62)
    calls=[]
    async def ok(client,trade_id,price):
        calls.append((trade_id,price)); return {"ok":True}
    monkeypatch.setattr(server,"replace_trade_stop",ok)
    changed=asyncio.run(server.manage_open_trades(object(),"EUR_USD",1.15951))
    assert changed==1
    c=cf(); row=c.execute("SELECT current_stop,last_action,break_even_applied FROM active_trade_management WHERE trade_id='144'").fetchone(); c.close()
    assert row["current_stop"]==pytest.approx(1.16042)
    assert row["last_action"]=="BREAK_EVEN"
    assert row["break_even_applied"]==1


def test_new_entry_gate_blocks_3_to_7_pm_new_york():
    # August is EDT (UTC-4).
    before=server.new_entry_time_gate(datetime(2026,8,28,18,59,tzinfo=timezone.utc))   # 14:59 ET
    blocked=server.new_entry_time_gate(datetime(2026,8,28,19,0,tzinfo=timezone.utc))  # 15:00 ET
    still=server.new_entry_time_gate(datetime(2026,8,28,22,59,tzinfo=timezone.utc))   # 18:59 ET
    after=server.new_entry_time_gate(datetime(2026,8,28,23,0,tzinfo=timezone.utc))    # 19:00 ET
    assert before["allowed"] is True
    assert blocked["allowed"] is False and blocked["reason"]=="NY_ENTRY_BLACKOUT_15_19"
    assert still["allowed"] is False
    assert after["allowed"] is True


def test_daily_exit_cutoff_boundary_new_york():
    assert server.daily_exit_cutoff_reached(datetime(2026,8,28,20,49,tzinfo=timezone.utc)) is False  # Fri 16:49 ET
    assert server.daily_exit_cutoff_reached(datetime(2026,8,28,20,50,tzinfo=timezone.utc)) is True   # Fri 16:50 ET
    assert server.daily_exit_cutoff_reached(datetime(2026,8,28,22,59,tzinfo=timezone.utc)) is True   # Fri 18:59 ET
    assert server.daily_exit_cutoff_reached(datetime(2026,8,28,23,0,tzinfo=timezone.utc)) is False   # Fri 19:00 ET


def test_daily_exit_cutoff_does_not_kill_evening_or_sunday_reopen_trades():
    # 19:00 ET is the start of the explicitly allowed evening entry window.
    assert server.daily_exit_cutoff_reached(datetime(2026,8,27,23,0,tzinfo=timezone.utc)) is False   # Thu 19:00 ET
    # Sunday 17:30 ET is the weekly reopen and must not be treated as a daily rollover cutoff.
    assert server.daily_exit_cutoff_reached(datetime(2026,8,30,21,30,tzinfo=timezone.utc)) is False  # Sun 17:30 ET


def test_daily_cutoff_closes_managed_trade(monkeypatch,tmp_path):
    db=tmp_path/"tm_cutoff.db"
    _management_db(db)
    cf=_conn_factory(db)
    c=cf(); c.execute("INSERT INTO active_trade_management VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",_trade_row()); c.commit(); c.close()
    monkeypatch.setattr(server,"conn",cf)
    monkeypatch.setattr(server,"RECOVERY_MANAGER_ENABLED",False)
    monkeypatch.setattr(server,"OBSERVABILITY_ENABLED",False)
    calls=[]
    async def fake_req(client,method,path,params=None,body=None):
        calls.append((method,path,params,body)); return {"orderFillTransaction":{"id":"X"}}
    monkeypatch.setattr(server,"req",fake_req)
    n=asyncio.run(server.close_managed_trades_for_daily_cutoff(
        object(),"EUR_USD",datetime(2026,8,28,20,50,tzinfo=timezone.utc)))
    assert n==1
    assert calls==[("PUT","/v3/accounts/{account}/trades/144/close",None,{"units":"ALL"})]
    c=cf(); row=c.execute("SELECT closed,last_action FROM active_trade_management WHERE trade_id='144'").fetchone(); c.close()
    assert row["closed"]==1
    assert row["last_action"]=="DAILY_CUTOFF_CLOSE"


def test_daily_cutoff_does_not_close_before_1650(monkeypatch,tmp_path):
    db=tmp_path/"tm_cutoff_before.db"
    _management_db(db)
    cf=_conn_factory(db)
    c=cf(); c.execute("INSERT INTO active_trade_management VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",_trade_row()); c.commit(); c.close()
    monkeypatch.setattr(server,"conn",cf)
    calls=[]
    async def fake_req(*a,**kw): calls.append((a,kw)); return {}
    monkeypatch.setattr(server,"req",fake_req)
    n=asyncio.run(server.close_managed_trades_for_daily_cutoff(
        object(),"EUR_USD",datetime(2026,8,28,20,49,tzinfo=timezone.utc)))
    assert n==0 and calls==[]
    c=cf(); row=c.execute("SELECT closed FROM active_trade_management WHERE trade_id='144'").fetchone(); c.close()
    assert row["closed"]==0


def test_daily_cutoff_does_not_close_after_evening_window_reopens(monkeypatch,tmp_path):
    db=tmp_path/"tm_cutoff_evening.db"
    _management_db(db)
    cf=_conn_factory(db)
    c=cf(); c.execute("INSERT INTO active_trade_management VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",_trade_row()); c.commit(); c.close()
    monkeypatch.setattr(server,"conn",cf)
    calls=[]
    async def fake_req(*a,**kw): calls.append((a,kw)); return {}
    monkeypatch.setattr(server,"req",fake_req)
    n=asyncio.run(server.close_managed_trades_for_daily_cutoff(
        object(),"EUR_USD",datetime(2026,8,27,23,0,tzinfo=timezone.utc)))  # Thu 19:00 ET
    assert n==0 and calls==[]
    c=cf(); row=c.execute("SELECT closed FROM active_trade_management WHERE trade_id='144'").fetchone(); c.close()
    assert row["closed"]==0


def test_actual_fill_controls_break_even_threshold(monkeypatch,tmp_path):
    db=tmp_path/"tm_fill_threshold.db"
    _management_db(db)
    cf=_conn_factory(db)
    monkeypatch.setattr(server,"conn",cf)
    monkeypatch.setattr(server,"trend_runner_score",lambda r:0.0)
    monkeypatch.setattr(server,"setup_variant",lambda r:"TEST")
    monkeypatch.setattr(server,"TREND_RUNNER_MIN_SCORE",0.62)
    r={"instrument":"EUR_USD","signal":"SELL","entry":1.16042,"stop":1.16132,"score":80,"features":{},"filters":{}}
    # Broker fill is 1.16021, so initial risk is 11.1 pips rather than the planned 9 pips.
    server.register_trade_management("144",r,1.15772,100,1.16021)
    calls=[]
    async def ok(client,trade_id,price): calls.append((trade_id,price)); return {"ok":True}
    monkeypatch.setattr(server,"replace_trade_stop",ok)
    # This is beyond +1R from the planned entry but still below +1R from the actual fill.
    assert asyncio.run(server.manage_open_trades(object(),"EUR_USD",1.15920))==0
    assert calls==[]
    # Actual-fill +1R for the SELL is 1.15910.
    assert asyncio.run(server.manage_open_trades(object(),"EUR_USD",1.15910))==1
    assert calls and calls[-1][1]==pytest.approx(1.16021)


def test_adaptive_stop_actions_are_symmetric_for_buy_and_sell():
    # 10-pip initial risk on both sides.
    buy_lock=server.adaptive_stop_price("BUY",1.1000,1.0990,1.1015,"BE_PROFIT_TRAIL")
    sell_lock=server.adaptive_stop_price("SELL",1.1000,1.1010,1.0985,"BE_PROFIT_TRAIL")
    assert buy_lock["action"]==sell_lock["action"]=="PROFIT_LOCK"
    assert buy_lock["new_stop"]==pytest.approx(1.10075)
    assert sell_lock["new_stop"]==pytest.approx(1.09925)

    buy_trail=server.adaptive_stop_price("BUY",1.1000,1.0990,1.1020,"BE_PROFIT_TRAIL")
    sell_trail=server.adaptive_stop_price("SELL",1.1000,1.1010,1.0980,"BE_PROFIT_TRAIL")
    assert buy_trail["action"]==sell_trail["action"]=="TRAIL"
    assert buy_trail["new_stop"]>1.1000
    assert sell_trail["new_stop"]<1.1000

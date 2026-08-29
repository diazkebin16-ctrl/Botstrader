import asyncio
import json
from datetime import datetime, timezone

import pytest

import server
from instrument_registry import InstrumentRegistry
from recovery_manager import deterministic_intent_key


def _fresh_db(monkeypatch, tmp_path):
    db=str(tmp_path / "multi_asset.db")
    monkeypatch.setattr(server,"DB",db)
    c=server.conn(); c.close()
    return db


def _signal(instrument, candle="2026-08-28T20:00:00+00:00", side="BUY"):
    return {
        "instrument":instrument,"signal":side,"technical":80,"score":80,"alignment":"NEUTRAL",
        "blocked":False,"entry":1.2000,"stop":1.1990,"target":1.2020,"managed_target":1.2020,
        "rr":2.0,"rr_raw":2.0,"candle_ts":candle,
        "features":{"rr_raw":2.0,"hour_ny":12,"m15_gap_atr":0.2,"m15_slope_atr":0.1,
                    "m5_momentum":0.01,"m1_momentum":0.01,"extension_atr":0.5,"volatility_ratio":1.0},
        "filters":{"m5_structure":True,"second_pullback":True,"m1_confirmation":True,"not_extended":True,"volatility_ok":True},
    }


def test_registry_defaults_and_future_jpy_precision():
    reg=InstrumentRegistry()
    assert reg.get("EUR/USD").pip_size == pytest.approx(0.0001)
    assert reg.get("GBP_USD").display_precision == 5
    assert reg.get("USD_JPY").pip_size == pytest.approx(0.01)
    assert reg.get("USD_JPY").format_price(151.23456) == "151.235"


def test_oanda_metadata_overrides_fallback_and_unit_precision():
    reg=InstrumentRegistry()
    reg.update_from_oanda({"instruments":[{
        "name":"GBP_USD","displayPrecision":6,"pipLocation":-5,
        "tradeUnitsPrecision":1,"minimumTradeSize":"0.1","marginRate":"0.0333","type":"CURRENCY"
    }]})
    meta=reg.get("GBP_USD")
    assert meta.source == "OANDA"
    assert meta.pip_size == pytest.approx(0.00001)
    assert meta.format_price(1.2345678) == "1.234568"
    assert meta.format_units(10.27) == "10.2"


def test_defined_instrument_is_not_automatically_enabled(monkeypatch):
    monkeypatch.setattr(server,"INSTRUMENTS",["EUR_USD"])
    monkeypatch.setattr(server,"SHADOW_INSTRUMENTS",["GBP_USD"])
    assert server.instrument_mode("EUR_USD") == "ENABLED"
    assert server.instrument_mode("GBP_USD") == "SHADOW"
    assert server.instrument_mode("USD_JPY") == "DISABLED"


def test_sizing_never_increases_legacy_units_and_is_stop_aware(monkeypatch):
    monkeypatch.setattr(server,"UNITS",100)
    monkeypatch.setattr(server,"managed_value",lambda key,default: default)
    ctx={"nav":100.0,"account_currency":"USD"}
    tight=server.instrument_sizing("EUR_USD",100,1.2000,1.1990,ctx,1.0)
    wide=server.instrument_sizing("GBP_USD",100,1.2500,1.1500,ctx,1.0)
    assert 0 <= wide["effective_units"] <= tight["effective_units"] <= 100
    assert tight["stop_distance_pips"] == pytest.approx(10.0)
    assert wide["stop_distance_pips"] == pytest.approx(1000.0)
    assert tight["never_increases_legacy_units"] is True


def test_signal_dedup_is_namespaced_by_instrument(monkeypatch,tmp_path):
    _fresh_db(monkeypatch,tmp_path)
    monkeypatch.setattr(server,"RESEARCH_LAB_ENABLED",False)
    conf={"probability":0.7,"source":"TEST","samples":0,"required_confidence":0.65,"variant":"TEST"}
    eur=_signal("EUR_USD"); gbp=_signal("GBP_USD")
    eur1=server.save_signal(eur,0,"",None,conf,"test")
    eur2=server.save_signal(eur,0,"",None,conf,"test")
    gbp1=server.save_signal(gbp,0,"",None,conf,"test")
    assert eur1 == eur2
    assert gbp1 != eur1
    c=server.conn(); rows=c.execute("SELECT instrument,candle_ts FROM signals ORDER BY id").fetchall();c.close()
    assert [r["instrument"] for r in rows] == ["EUR_USD","GBP_USD"]


def test_learning_performance_is_instrument_scoped(monkeypatch,tmp_path):
    _fresh_db(monkeypatch,tmp_path)
    monkeypatch.setattr(server,"RESEARCH_LAB_ENABLED",False)
    conf={"probability":0.7,"source":"TEST","samples":0,"required_confidence":0.65,"variant":"TEST"}
    for i,(inst,label) in enumerate([("EUR_USD",1),("EUR_USD",1),("GBP_USD",0),("GBP_USD",0)]):
        r=_signal(inst,f"2026-08-28T20:0{i}:00+00:00")
        sid=server.save_signal(r,1,str(i),None,conf,"test")
        c=server.conn();c.execute("UPDATE learning_samples SET label=?,status=?,resolved_ts=? WHERE signal_id=?",
            (label,"WIN" if label else "LOSS","2026-08-28T21:00:00+00:00",sid));c.commit();c.close()
    assert server.recent_performance("EUR_USD")["win_rate"] == 1.0
    assert server.recent_performance("GBP_USD")["win_rate"] == 0.0
    assert server._strategy_performance_summary("TEST","EUR_USD")["historical_win_rate"] == 1.0
    assert server._strategy_performance_summary("TEST","GBP_USD")["historical_win_rate"] == 0.0


def test_ml_artifact_paths_are_separate():
    assert server.shadow_model_path("EUR_USD") == server.MODEL_PATH
    assert server.shadow_model_path("GBP_USD") != server.MODEL_PATH
    assert "GBP_USD" in server.shadow_model_path("GBP_USD")


def test_strategy_health_keys_do_not_collide():
    variant="SECOND_PULLBACK_NEWS_NEUTRAL_RR2_Q80"
    assert server._strategy_health_key(server.PRIMARY_INSTRUMENT,variant) == variant
    assert server._strategy_health_key("GBP_USD",variant) != variant
    assert server._strategy_health_key("GBP_USD",variant).startswith("GBP_USD::")


def test_market_data_requests_and_results_are_instrument_specific(monkeypatch):
    calls=[]
    async def fake_req(client,method,path,params=None,body=None):
        calls.append(path)
        base=1.10 if "EUR_USD" in path else 1.25
        candles=[]
        for i in range(60):
            px=base+i*0.00001
            candles.append({"complete":True,"time":f"2026-08-28T20:{i%60:02d}:00Z",
                            "mid":{"o":str(px),"h":str(px+0.0001),"l":str(px-0.0001),"c":str(px)},"volume":10})
        return {"candles":candles}
    monkeypatch.setattr(server,"req",fake_req)
    eur=asyncio.run(server.candles(object(),"EUR_USD","M1",60))
    gbp=asyncio.run(server.candles(object(),"GBP_USD","M1",60))
    assert "EUR_USD" in calls[0] and "GBP_USD" in calls[1]
    assert eur[-1]["c"] != gbp[-1]["c"]


def test_shadow_instrument_cannot_send_order(monkeypatch):
    monkeypatch.setattr(server,"INSTRUMENTS",["EUR_USD"])
    monkeypatch.setattr(server,"SHADOW_INSTRUMENTS",["GBP_USD"])
    out=asyncio.run(server.execute(object(),_signal("GBP_USD")))
    assert out["skipped"] == "INSTRUMENT_NOT_EXECUTION_ENABLED"
    assert out["instrument_mode"] == "SHADOW"


def test_enabled_gbp_order_uses_gbp_and_registry_formatting(monkeypatch):
    monkeypatch.setattr(server,"INSTRUMENTS",["EUR_USD","GBP_USD"])
    monkeypatch.setattr(server,"SHADOW_INSTRUMENTS",[])
    monkeypatch.setattr(server,"SINGLE",False)
    monkeypatch.setattr(server,"market_is_weekend_closed",lambda *a,**k:False)
    monkeypatch.setattr(server,"new_entry_time_gate",lambda *a,**k:{"allowed":True,"reason":"ALLOWED"})
    server.INSTRUMENT_REGISTRY.update_from_oanda({"instruments":[{
        "name":"GBP_USD","displayPrecision":5,"pipLocation":-4,"tradeUnitsPrecision":0,"minimumTradeSize":"1","type":"CURRENCY"
    }]})
    captured={}
    async def fake_req(client,method,path,params=None,body=None):
        captured.update({"method":method,"path":path,"body":body});return {"ok":True}
    monkeypatch.setattr(server,"req",fake_req)
    r=_signal("GBP_USD");r["stop"]=1.199876;r["target"]=1.202345;r["managed_target"]=1.202345
    out=asyncio.run(server.execute(object(),r))
    order=captured["body"]["order"]
    assert captured["method"] == "POST"
    assert order["instrument"] == "GBP_USD"
    assert order["stopLossOnFill"]["price"] == "1.19988"
    assert order["takeProfitOnFill"]["price"] == "1.20235"
    assert float(order["units"]) <= server.UNITS
    assert out["ok"] is True


def test_trade_management_isolated_by_instrument(monkeypatch,tmp_path):
    _fresh_db(monkeypatch,tmp_path)
    server.register_trade_management("EUR_T",_signal("EUR_USD"),1.2020,filled_units=100,entry_price=1.2000)
    g=_signal("GBP_USD");g.update(entry=1.2500,stop=1.2490,target=1.2520,managed_target=1.2520)
    server.register_trade_management("GBP_T",g,1.2520,filled_units=100,entry_price=1.2500)
    calls=[]
    async def fake_replace(client,trade_id,price):calls.append((trade_id,price));return {"ok":True}
    monkeypatch.setattr(server,"replace_trade_stop",fake_replace)
    changed=asyncio.run(server.manage_open_trades(object(),"EUR_USD",1.2011))
    assert changed == 1
    assert [x[0] for x in calls] == ["EUR_T"]
    c=server.conn();gbp=c.execute("SELECT last_r,current_stop FROM active_trade_management WHERE trade_id='GBP_T'").fetchone();c.close()
    assert gbp["last_r"] == pytest.approx(0.0)
    assert gbp["current_stop"] == pytest.approx(1.2490)


def test_break_even_can_trigger_independently_for_each_pair(monkeypatch,tmp_path):
    _fresh_db(monkeypatch,tmp_path)
    server.register_trade_management("EUR_T",_signal("EUR_USD"),1.2020,filled_units=100,entry_price=1.2000)
    g=_signal("GBP_USD");g.update(entry=1.2500,stop=1.2490,target=1.2520,managed_target=1.2520)
    server.register_trade_management("GBP_T",g,1.2520,filled_units=100,entry_price=1.2500)
    calls=[]
    async def fake_replace(client,trade_id,price):calls.append(trade_id);return {"ok":True}
    monkeypatch.setattr(server,"replace_trade_stop",fake_replace)
    assert asyncio.run(server.manage_open_trades(object(),"EUR_USD",1.2011)) == 1
    assert asyncio.run(server.manage_open_trades(object(),"GBP_USD",1.2495)) == 0
    assert calls == ["EUR_T"]
    assert asyncio.run(server.manage_open_trades(object(),"GBP_USD",1.2511)) == 1
    assert calls == ["EUR_T","GBP_T"]


@pytest.mark.parametrize("failed",["EUR_USD","GBP_USD"])
def test_scan_failure_isolated_to_one_instrument(monkeypatch,failed):
    monkeypatch.setattr(server,"SCAN_INSTRUMENTS",["EUR_USD","GBP_USD"])
    monkeypatch.setattr(server,"INSTRUMENTS",["EUR_USD"])
    monkeypatch.setattr(server,"SHADOW_INSTRUMENTS",["GBP_USD"])
    monkeypatch.setattr(server,"WEEKEND_RESEARCH_ENABLED",False)
    monkeypatch.setattr(server,"OBSERVABILITY_ENABLED",False)
    server.state["last_results"]={};server.state["instrument_state"]={}
    async def fake_scan(client,inst):
        if inst==failed:raise RuntimeError(f"{inst} unavailable")
        return {"instrument":inst,"candle_ts":"2026-08-28T20:00:00+00:00"}
    monkeypatch.setattr(server,"scan",fake_scan)
    ok=asyncio.run(server.scan_instruments_once(object()))
    survivor="GBP_USD" if failed=="EUR_USD" else "EUR_USD"
    assert ok is False
    assert server.state["instrument_state"][failed]["ok"] is False
    assert server.state["instrument_state"][survivor]["ok"] is True


def test_recovery_idempotency_keys_include_instrument():
    args=("PRIMARY",)
    eur=deterministic_intent_key(*args,"EUR_USD","BUY","S","2026-08-28T20:00:00+00:00",1.2,1.199,1.202)
    gbp=deterministic_intent_key(*args,"GBP_USD","BUY","S","2026-08-28T20:00:00+00:00",1.2,1.199,1.202)
    assert eur != gbp


def test_blackouts_and_daily_cutoff_remain_global_policy():
    morning=datetime(2026,8,28,13,30,tzinfo=timezone.utc)  # 09:30 ET
    afternoon=datetime(2026,8,28,19,30,tzinfo=timezone.utc) # 15:30 ET
    cutoff=datetime(2026,8,28,21,0,tzinfo=timezone.utc) # 17:00 ET
    assert server.new_entry_time_gate(morning)["allowed"] is False
    assert server.new_entry_time_gate(afternoon)["allowed"] is False
    assert server.daily_exit_cutoff_reached(cutoff) is True


def test_runtime_integrity_covers_instrument_registry():
    hashes=server.security_manager._file_hashes()
    assert "instrument_registry.py" in hashes


def test_model_runs_schema_has_instrument(monkeypatch,tmp_path):
    _fresh_db(monkeypatch,tmp_path)
    c=server.conn(); cols={x[1] for x in c.execute("PRAGMA table_info(model_runs)").fetchall()};c.close()
    assert "instrument" in cols


def test_restart_restores_both_symbols_without_cross_contamination(tmp_path):
    from recovery_manager import RecoveryManager
    from test_recovery_manager import seed_base
    db=str(tmp_path / "recovery_multi.db")
    seed_base(db)
    m=RecoveryManager(db,"https://broker.test","A","T","PRIMARY",request_min_interval_ms=0)
    m.ensure_schema()
    now="2026-08-28T20:00:00+00:00"
    def intent(symbol, trade_id, entry, stop, target):
        return {
            "execution_intent_id":f"I_{symbol}","correlation_id":f"C_{symbol}",
            "signal_id":None,"broker_order_id":f"O_{symbol}","broker_trade_id":trade_id,
            "strategy_id":"TEST","symbol":symbol,"created_ts":now,"filled_units":100.0,
            "entry_price":entry,"stop_loss":stop,"take_profit":target,"metadata_json":"{}",
        }
    eur=intent("EUR_USD","EUR_T",1.1000,1.0990,1.1020)
    gbp=intent("GBP_USD","GBP_T",1.2500,1.2490,1.2520)
    m._restore_trade_from_intent(eur,{"id":"EUR_T","instrument":"EUR_USD","currentUnits":"100","price":"1.1000",
        "stopLossOrder":{"price":"1.0990"},"takeProfitOrder":{"price":"1.1020"},"openTime":now})
    m._restore_trade_from_intent(gbp,{"id":"GBP_T","instrument":"GBP_USD","currentUnits":"100","price":"1.2500",
        "stopLossOrder":{"price":"1.2490"},"takeProfitOrder":{"price":"1.2520"},"openTime":now})
    # New manager instance represents process restart using the same persistent DB.
    restarted=RecoveryManager(db,"https://broker.test","A","T","PRIMARY",request_min_interval_ms=0)
    restarted.ensure_schema()
    positions=restarted._internal_open_positions()
    assert positions["EUR_T"]["instrument"] == "EUR_USD"
    assert positions["GBP_T"]["instrument"] == "GBP_USD"
    c=restarted.conn()
    memory={r["trade_id"]:r["symbol"] for r in c.execute("SELECT trade_id,symbol FROM trade_memory ORDER BY trade_id").fetchall()}
    c.close()
    assert memory == {"EUR_T":"EUR_USD","GBP_T":"GBP_USD"}


def test_global_hard_risk_caps_are_not_raised_for_second_instrument(monkeypatch):
    monkeypatch.setattr(server,"managed_value",lambda key,default: default)
    ctx={
        "current_drawdown":0.0,"margin_usage":0.0,"portfolio_open_risk":server.RISK_MAX_PORTFOLIO_FRACTION,
        "open_instruments":["EUR_USD"],"consecutive_losses":0,"data_stale":False,"system_abnormal":False,
    }
    regime={"market_regime":"TREND","volatility_state":"NORMAL","confidence":0.8}
    rec=server.adaptive_risk_recommendation("GBP_USD","TEST",regime,{"confidence":0.8},0.8,ctx,server.UNITS)
    assert rec["allow_new_trades"] is False
    assert "portfolio_risk_limit" in rec["reason"]
    assert rec["max_position_size"] <= server.UNITS
    assert server.RISK_MAX_TRADE_FRACTION <= 0.03
    assert server.RISK_MAX_PORTFOLIO_FRACTION <= 0.20


def test_secondary_instrument_cannot_inherit_primary_active_research_veto(monkeypatch):
    monkeypatch.setattr(server,"get_active_research_rules",lambda:[{"source":"INTERNAL","rule_key":"ext_le_0_8","status":"ACTIVE"}])
    gbp=_signal("GBP_USD"); gbp["features"]["extension_atr"]=2.0
    eur=_signal("EUR_USD"); eur["features"]["extension_atr"]=2.0
    gout=server.evaluate_active_research_rules(gbp)
    eout=server.evaluate_active_research_rules(eur)
    assert gout["ok"] is True and gout["active"] is False
    assert gout["reason"] == "instrument_scoped_research_not_validated"
    assert eout["ok"] is False and eout["active"] is True


def test_production_release_fingerprint_includes_registry():
    assert any(path.endswith("instrument_registry.py") for path in server.production_release_files())

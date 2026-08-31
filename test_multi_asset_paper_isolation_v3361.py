import asyncio
from datetime import datetime, timezone

import pytest

import server
from instrument_profiles import instrument_profile


def _risk_ctx(open_instruments=None, open_risk=0.0, margin=0.0, **extra):
    out={
        "portfolio_open_risk":open_risk,
        "margin_usage":margin,
        "open_instruments":list(open_instruments or []),
        "data_stale":False,
        "system_abnormal":False,
    }
    out.update(extra)
    return out


def _signal(symbol="EUR_USD", *, m1=True, room=.8, rr=1.2, ext=.5):
    entry=150.000 if symbol.endswith("JPY") else 1.20000
    stop=149.900 if symbol.endswith("JPY") else 1.19900
    target=150.200 if symbol.endswith("JPY") else 1.20200
    return {
        "instrument":symbol,"signal":"BUY","entry":entry,"stop":stop,"target":target,"managed_target":target,
        "rr":2.0,"rr_raw":rr,"barrier_class":"WEAK","blocked":False,
        "filters":{"m1_confirmation":m1,"barrier_room_ok":True},
        "safety_checks":{"minimum_rr":True,"barrier_room_ok":True},
        "features":{
            "room_to_barrier_r":room,"rr_raw":rr,"extension_atr":ext,"m1_momentum":0.001,
            "m1_ema9_side_ok":1,"m1_candle_color_ok":1,"m1_exception_shadow":1,
            "legacy_v331_buy_score":40.0,"legacy_v331_sell_score":20.0,
            "legacy_v331_directional_score":40.0,"legacy_v331_chosen_direction":"BUY",
        },
        "broker_risk_context":_risk_ctx(),
    }


def test_release_and_primary_instrument_contract():
    assert server.VERSION_TAG == "3.38.1"
    assert server.PRIMARY_INSTRUMENT == "EUR_USD"
    # V3.37 hardening changed only the config default: secondary instruments
    # remain profiled/PAPER-capable but now require explicit INSTRUMENTS config.
    assert server.CONFIGURED_INSTRUMENTS == [server.PRIMARY_INSTRUMENT]
    assert instrument_profile("GBP_USD").paper_execution_allowed is True
    assert instrument_profile("USD_JPY").paper_execution_allowed is True


def test_default_paper_profiles_enable_three_but_secondary_deny_live():
    assert instrument_profile("EUR_USD").paper_execution_allowed is True
    assert instrument_profile("GBP_USD").paper_execution_allowed is True
    assert instrument_profile("USD_JPY").paper_execution_allowed is True
    assert instrument_profile("GBP_USD").live_execution_allowed is False
    assert instrument_profile("USD_JPY").live_execution_allowed is False


def test_secondary_profiles_do_not_inherit_eur_specific_vetoes_or_exceptions():
    eur=instrument_profile("EUR_USD")
    for symbol in ("GBP_USD","USD_JPY"):
        p=instrument_profile(symbol)
        assert p.specific_vetoes == frozenset()
        assert p.specific_exceptions == frozenset()
        assert p.learned_research_veto_authority is False
    assert eur.specific_vetoes == frozenset({"LOW_ROOM_LOW_RR","LOW_ROOM_EXTENDED"})
    assert eur.has_exception("M1_ALTERNATIVE_ADMISSION")
    assert eur.learned_research_veto_authority is True


def test_forward_eur_veto_is_not_authoritative_for_gbp_or_jpy(monkeypatch):
    monkeypatch.setattr(server,"TRADING_ENVIRONMENT","PAPER")
    monkeypatch.setattr(server,"PRIMARY_OANDA_ENV","practice")
    monkeypatch.setattr(server,"OANDA","https://api-fxpractice.oanda.com")
    monkeypatch.setattr(server,"PAPER_FORWARD_FILTERS_ENABLED",True)
    conf={"probability":.5}
    eur=server.quality_entry_gate(_signal("EUR_USD",room=.3,rr=.9,ext=.5),conf)
    gbp=server.quality_entry_gate(_signal("GBP_USD",room=.3,rr=.9,ext=.5),conf)
    jpy=server.quality_entry_gate(_signal("USD_JPY",room=.3,rr=.9,ext=.5),conf)
    # EUR Phase2 forward opens its prior LOW_ROOM strategic vetoes; GBP/JPY
    # still do not inherit those EUR-specific vetoes.
    assert eur["ok"] is True
    assert gbp["ok"] is True and "PAPER_FORWARD_VETO" not in gbp["reason"]
    assert jpy["ok"] is True and "PAPER_FORWARD_VETO" not in jpy["reason"]
    # The pattern remains observable for all symbols without becoming authority.
    assert server.forward_entry_pattern_flags(_signal("GBP_USD",room=.3,rr=.9)["features"])["low_room_low_rr"] is True


def test_phase1_m1_open_is_forward_scoped_to_eur_and_gbp(monkeypatch):
    monkeypatch.setattr(server,"ENTRY_TIMING_ENABLED",False)
    conf={"probability":.5}
    eur=server.quality_entry_gate(_signal("EUR_USD",m1=False),conf)
    gbp=server.quality_entry_gate(_signal("GBP_USD",m1=False),conf)
    jpy=server.quality_entry_gate(_signal("USD_JPY",m1=False),conf)
    assert eur["ok"] is True
    assert gbp["ok"] is True
    assert jpy["ok"] is False and "excepción específica no autorizada" in jpy["reason"]


def test_minimum_rr_and_barrier_room_remain_global_strategy_base(monkeypatch):
    monkeypatch.setattr(server,"ENTRY_TIMING_ENABLED",False)
    conf={"probability":.5}
    for symbol in ("EUR_USD","GBP_USD","USD_JPY"):
        r=_signal(symbol)
        r["barrier_class"]="STRONG"; r["rr_raw"]=server.MIN_ENTRY_RR-0.01
        out=server.quality_entry_gate(r,conf)
        assert out["ok"] is False and "barrera fuerte" in out["reason"]


def test_global_time_blackouts_apply_independent_of_symbol():
    morning=datetime(2026,8,31,12,30,tzinfo=timezone.utc)  # 08:30 ET
    afternoon=datetime(2026,8,31,19,30,tzinfo=timezone.utc) # 15:30 ET
    for _symbol in ("EUR_USD","GBP_USD","USD_JPY"):
        assert server.new_entry_time_gate(morning)["allowed"] is False
        assert server.new_entry_time_gate(afternoon)["allowed"] is False


def test_daily_cutoff_is_global_weekday_control():
    at=datetime(2026,8,31,21,0,tzinfo=timezone.utc)  # 17:00 ET
    assert server.daily_exit_cutoff_reached(at) is True


def test_jpy_registry_precision_and_units():
    m=server.instrument_metadata("USD_JPY")
    assert m.display_precision == 3
    assert m.pip_location == -2
    assert m.pip_size == pytest.approx(.01)
    assert m.trade_units_precision == 0
    assert m.minimum_trade_size == pytest.approx(1.0)
    assert m.format_price(150.1236) == "150.124"
    assert m.format_units(100.9) == "100"


def test_secondary_execution_requires_broker_verified_metadata(monkeypatch):
    monkeypatch.setattr(server,"INSTRUMENTS",["EUR_USD","GBP_USD","USD_JPY"])
    monkeypatch.setattr(server,"SHADOW_INSTRUMENTS",[])
    monkeypatch.setattr(server,"SINGLE",False)
    monkeypatch.setattr(server,"market_is_weekend_closed",lambda *a,**k:False)
    monkeypatch.setattr(server,"new_entry_time_gate",lambda *a,**k:{"allowed":True,"reason":"ALLOWED"})
    # Reset secondary registry objects to conservative fallback sources.
    from instrument_registry import InstrumentRegistry
    monkeypatch.setattr(server,"INSTRUMENT_REGISTRY",InstrumentRegistry())
    async def fail_refresh(*a,**k):
        raise RuntimeError("metadata unavailable")
    monkeypatch.setattr(server,"refresh_instrument_metadata",fail_refresh)
    for symbol in ("GBP_USD","USD_JPY"):
        out=asyncio.run(server.execute(object(),_signal(symbol)))
        assert out["skipped"] == "INSTRUMENT_METADATA_UNVERIFIED"


def test_gbp_and_jpy_can_reach_paper_order_gate_after_verified_metadata(monkeypatch):
    monkeypatch.setattr(server,"INSTRUMENTS",["EUR_USD","GBP_USD","USD_JPY"])
    monkeypatch.setattr(server,"SHADOW_INSTRUMENTS",[])
    monkeypatch.setattr(server,"SINGLE",False)
    monkeypatch.setattr(server,"market_is_weekend_closed",lambda *a,**k:False)
    monkeypatch.setattr(server,"new_entry_time_gate",lambda *a,**k:{"allowed":True,"reason":"ALLOWED"})
    server.INSTRUMENT_REGISTRY.update_from_oanda({"instruments":[
        {"name":"GBP_USD","displayPrecision":5,"pipLocation":-4,"tradeUnitsPrecision":0,"minimumTradeSize":"1","type":"CURRENCY"},
        {"name":"USD_JPY","displayPrecision":3,"pipLocation":-2,"tradeUnitsPrecision":0,"minimumTradeSize":"1","type":"CURRENCY"},
    ]})
    sent=[]
    async def fake_req(client,method,path,params=None,body=None):
        sent.append(body["order"]); return {"ok":True}
    monkeypatch.setattr(server,"req",fake_req)
    for symbol in ("GBP_USD","USD_JPY"):
        out=asyncio.run(server.execute(object(),_signal(symbol)))
        assert out["ok"] is True
    assert [x["instrument"] for x in sent] == ["GBP_USD","USD_JPY"]
    assert sent[1]["stopLossOnFill"]["price"] == "149.900"


def test_secondary_profiles_are_disabled_in_live_even_if_configured(monkeypatch):
    monkeypatch.setattr(server,"TRADING_ENVIRONMENT","PRODUCTION")
    monkeypatch.setattr(server,"PRIMARY_OANDA_ENV","live")
    monkeypatch.setattr(server,"INSTRUMENTS",["EUR_USD","GBP_USD","USD_JPY"])
    assert server.instrument_mode("EUR_USD") == "ENABLED"
    assert server.instrument_mode("GBP_USD") == "DISABLED"
    assert server.instrument_mode("USD_JPY") == "DISABLED"


def test_global_portfolio_guard_uses_existing_caps_without_raising_them(monkeypatch):
    monkeypatch.setattr(server,"managed_value",lambda key,default:default)
    # Two USD-sharing instruments already open => third is blocked by the existing correlated-position cap.
    ctx=_risk_ctx(["EUR_USD","GBP_USD"],open_risk=2*server.RISK_MAX_TRADE_FRACTION)
    out=server.portfolio_execution_guard("USD_JPY",ctx)
    assert out["allow"] is False
    assert "CORRELATED_POSITION_LIMIT" in out["reasons"]
    assert out["portfolio_risk_cap"] == server.RISK_MAX_PORTFOLIO_FRACTION
    assert server.RISK_MAX_CORRELATED_POSITIONS == 2


def test_prospective_trade_cannot_cross_global_portfolio_cap(monkeypatch):
    monkeypatch.setattr(server,"managed_value",lambda key,default:default)
    ctx=_risk_ctx([],open_risk=server.RISK_MAX_PORTFOLIO_FRACTION-server.RISK_MAX_TRADE_FRACTION/2)
    out=server.portfolio_execution_guard("GBP_USD",ctx)
    assert out["allow"] is False
    assert "PORTFOLIO_RISK_LIMIT" in out["reasons"]


def test_sizing_never_increases_requested_units_for_all_three(monkeypatch):
    monkeypatch.setattr(server,"managed_value",lambda key,default:default)
    for symbol,entry,stop in (("EUR_USD",1.2,1.199),("GBP_USD",1.25,1.249),("USD_JPY",150.0,149.9)):
        out=server.instrument_sizing(symbol,123,entry,stop,risk_context={"nav":10000})
        assert out["effective_units"] <= 123


def test_research_veto_authority_is_eur_only(monkeypatch):
    monkeypatch.setattr(server,"get_active_research_rules",lambda:[{"source":"INTERNAL","rule_key":"ext_le_0_8","status":"ACTIVE"}])
    for symbol in ("GBP_USD","USD_JPY"):
        r=_signal(symbol);r["features"]["extension_atr"]=2.0
        out=server.evaluate_active_research_rules(r)
        assert out["ok"] is True and out["active"] is False
    eur=_signal("EUR_USD");eur["features"]["extension_atr"]=2.0
    assert server.evaluate_active_research_rules(eur)["ok"] is False


def test_forward_snapshot_keeps_pattern_observation_separate_from_authority(monkeypatch):
    monkeypatch.setattr(server,"TRADING_ENVIRONMENT","PAPER")
    monkeypatch.setattr(server,"PRIMARY_OANDA_ENV","practice")
    monkeypatch.setattr(server,"OANDA","https://api-fxpractice.oanda.com")
    gbp=_signal("GBP_USD",room=.3,rr=.9,ext=.9)
    snap=server.forward_observation_snapshot(gbp,{"probability":.5,"samples":0})
    assert snap["vetoes"]["low_room_low_rr"] is True
    assert snap["paper_forward_filters_active"] is False


def test_duplicate_intent_identity_is_namespaced_by_instrument():
    from recovery_manager import deterministic_intent_key
    args=("acct",)
    eur=deterministic_intent_key(*args,"EUR_USD","BUY","BASE","2026-08-29T00:00:00Z",1.2,1.199,1.202)
    gbp=deterministic_intent_key(*args,"GBP_USD","BUY","BASE","2026-08-29T00:00:00Z",1.2,1.199,1.202)
    assert eur != gbp


def test_runtime_integrity_and_release_fingerprint_include_profile_module():
    assert "instrument_profiles.py" in server.security_manager._file_hashes()
    assert any(path.endswith("instrument_profiles.py") for path in server.production_release_files())


def test_body_keyword_safety_still_present_in_order_paths():
    src=open(server.__file__,encoding="utf-8").read()
    assert 'req(client, "POST", "/v3/accounts/{account}/orders", body=body)' in src
    assert 'order_body=body' in src

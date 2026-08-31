import os
import server


def _base_signal(room=0.8, rr=1.2, ext=0.5, m1=True):
    return {
        "signal":"BUY",
        "rr_raw":rr,
        "barrier_class":"WEAK",
        "filters":{"m1_confirmation":m1},
        "features":{
            "room_to_barrier_r":room,
            "rr_raw":rr,
            "extension_atr":ext,
            "m1_momentum":0.0001,
            "m1_ema9_side_ok":1,
            "m1_candle_color_ok":1,
            "m1_exception_shadow":0,
            "legacy_v331_buy_score":40.0,
            "legacy_v331_sell_score":20.0,
            "legacy_v331_directional_score":40.0,
            "legacy_v331_chosen_direction":"BUY",
        },
    }


def test_release_constants():
    assert server.MIN_RR == 1.5
    assert server.MIN_ENTRY_RR == 0.4
    assert server.MAX_ENTRY_EXTENSION_ATR == 1.5
    assert server.BREAK_EVEN_TRIGGER_R == 1.0
    assert server.BREAK_EVEN_LOCK_R == 0.0


def test_break_even_exact_1r():
    pre=server.adaptive_stop_price("BUY",1.1000,1.0990,1.1009,"BE_PROFIT_TRAIL")
    at=server.adaptive_stop_price("BUY",1.1000,1.0990,1.1010,"BE_PROFIT_TRAIL")
    assert pre["action"] == "NONE"
    assert at["action"] == "BREAK_EVEN"
    assert abs(at["new_stop"]-1.1000)<1e-9


def test_paper_forward_filter_rules(monkeypatch):
    monkeypatch.setattr(server,"TRADING_ENVIRONMENT","PAPER")
    monkeypatch.setattr(server,"PRIMARY_OANDA_ENV","practice")
    monkeypatch.setattr(server,"OANDA","https://api-fxpractice.oanda.com")
    monkeypatch.setattr(server,"PAPER_FORWARD_FILTERS_ENABLED",True)
    conf={"probability":0.5}
    sig_a=_base_signal(room=.30,rr=.90,ext=.50); sig_a["instrument"]="EUR_USD"
    sig_b=_base_signal(room=.50,rr=1.20,ext=.90); sig_b["instrument"]="EUR_USD"
    a=server.quality_entry_gate(sig_a,conf)
    b=server.quality_entry_gate(sig_b,conf)
    normal=server.quality_entry_gate(_base_signal(),conf)
    # V3.38 EUR forward experiment preserves these patterns in observability but
    # Phase1 opened both strategic vetoes for the Phase2 test population.
    assert server.forward_entry_pattern_flags(sig_a["features"])["low_room_low_rr"] is True
    assert server.forward_entry_pattern_flags(sig_b["features"])["low_room_extended"] is True
    assert a["ok"] and b["ok"] and normal["ok"]


def test_forward_filters_have_no_production_authority(monkeypatch):
    monkeypatch.setattr(server,"TRADING_ENVIRONMENT","PRODUCTION")
    monkeypatch.setattr(server,"PRIMARY_OANDA_ENV","practice")
    monkeypatch.setattr(server,"PAPER_FORWARD_FILTERS_ENABLED",True)
    conf={"probability":0.5}
    assert server.paper_forward_filters_active() is False
    assert "PAPER_FORWARD_VETO" not in server.quality_entry_gate(_base_signal(room=.30,rr=.90,ext=.50),conf)["reason"]


def test_adaptive_observe_only_before_executed_sample_minimum(monkeypatch):
    # All earlier hard/quality/research/strategy/re-entry gates are stubbed to pass;
    # this test isolates the adaptive confidence authority boundary.
    r=_base_signal(room=2.0,rr=1.5,ext=.2,m1=True)
    r.update({"blocked":False,"safety_checks":{"minimum_rr":True},"instrument":"EUR_USD","candle_ts":"2099-01-01T00:00:00+00:00"})
    monkeypatch.setattr(server,"paper_forward_filters_active",lambda instrument=None:False)
    monkeypatch.setattr(server,"evaluate_active_research_rules",lambda r:{"ok":True})
    monkeypatch.setattr(server,"strategy_execution_gate",lambda r:{"ok":True})
    monkeypatch.setattr(server,"reentry_guard",lambda r:{"ok":True})
    conf={"probability":0.05,"required_confidence":0.65,"samples":0,"mature":False}
    out=server.execution_decision(r,conf)
    assert out["execute"] is True
    assert "OBSERVE_ONLY" in out["reason"]


def test_shadow_flags_not_ml_inputs():
    assert "low_room_low_rr_shadow" not in server.FEATURE_COLUMNS
    assert "low_room_extended_shadow" not in server.FEATURE_COLUMNS

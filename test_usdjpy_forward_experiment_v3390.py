from __future__ import annotations

import inspect

import forward_experiment
import server


def _paper_practice(monkeypatch):
    monkeypatch.setattr(server, "TRADING_ENVIRONMENT", "PAPER")
    monkeypatch.setattr(server, "PRIMARY_OANDA_ENV", "practice")
    monkeypatch.setattr(server, "OANDA", "https://api-fxpractice.oanda.com")


def _features(direction="BUY", buy=33.0, sell=40.0, extension=9.0):
    return {
        "chosen_direction": direction,
        "legacy_v331_buy_score": buy,
        "legacy_v331_sell_score": sell,
        "extension_atr": extension,
        "rr_raw": 1.5,
        "room_to_barrier_r": 1.0,
    }


def _row(direction="BUY", buy=33.0, sell=40.0, extension=9.0, *, blocked=False, m1=False):
    f=_features(direction,buy,sell,extension)
    f.pop("chosen_direction")
    return {
        "instrument":"USD_JPY",
        "signal":direction,
        "blocked":blocked,
        "rr":1.5,
        "rr_raw":1.5,
        "score":50,
        "barrier_class":"WEAK",
        "safety_checks":{"minimum_rr":not blocked,"barrier_room_ok":True},
        "filters":{"m1_confirmation":m1,"barrier_room_ok":True},
        "features":f,
    }


def test_usdjpy_policy_isolated_to_usdjpy():
    p=forward_experiment.forward_policy("USD_JPY")
    assert p == {
        "instrument":"USD_JPY",
        "experiment_id":"USDJPY_PHASE2_FORWARD_V1",
        "bypass_m1_confirmation":True,
        "bypass_low_room_vetoes":False,
        "bypass_quality_extension":True,
    }
    assert forward_experiment.forward_policy("AUD_USD")["experiment_id"] is None
    assert forward_experiment.forward_policy("USD_CAD")["experiment_id"] is None


def test_usdjpy_chosen_score_uses_buy_for_buy():
    out=forward_experiment.evaluate_forward_experiment("USD_JPY",_features("BUY",buy=33.0,sell=1.0))
    assert out["chosen_direction"]=="BUY"
    assert out["chosen_legacy_score"]==33.0
    assert out["ok"] is True


def test_usdjpy_chosen_score_uses_sell_for_sell():
    out=forward_experiment.evaluate_forward_experiment("USD_JPY",_features("SELL",buy=100.0,sell=33.0))
    assert out["chosen_direction"]=="SELL"
    assert out["chosen_legacy_score"]==33.0
    assert out["ok"] is True


def test_usdjpy_threshold_exact_boundary():
    assert forward_experiment.USDJPY_CHOSEN_LEGACY_SCORE_MIN==33.0
    assert forward_experiment.evaluate_forward_experiment("USD_JPY",_features("BUY",buy=32.999999999,sell=99.0))["ok"] is False
    assert forward_experiment.evaluate_forward_experiment("USD_JPY",_features("BUY",buy=33.0,sell=0.0))["ok"] is True
    assert forward_experiment.evaluate_forward_experiment("USD_JPY",_features("BUY",buy=34.0,sell=0.0))["ok"] is True


def test_usdjpy_runtime_gate_uses_live_direction_not_legacy_counterfactual(monkeypatch):
    _paper_practice(monkeypatch)
    r=_row("SELL",buy=100.0,sell=32.0,extension=0.2,m1=True)
    out=server.forward_experiment_gate(r)
    assert out["chosen_direction"]=="SELL"
    assert out["chosen_legacy_score"]==32.0
    assert out["ok"] is False


def test_usdjpy_m1_opening_is_paper_practice_scoped(monkeypatch):
    _paper_practice(monkeypatch)
    r=_row("BUY",buy=33.0,sell=0.0,extension=0.2,m1=False)
    assert server.quality_entry_gate(r,{})["ok"] is True
    monkeypatch.setattr(server,"TRADING_ENVIRONMENT","PRODUCTION")
    assert server.quality_entry_gate(r,{})["ok"] is False


def test_usdjpy_extension_opening_is_paper_practice_scoped(monkeypatch):
    _paper_practice(monkeypatch)
    r=_row("BUY",buy=33.0,sell=0.0,extension=9.0,m1=True)
    assert server.quality_entry_gate(r,{})["ok"] is True
    monkeypatch.setattr(server,"TRADING_ENVIRONMENT","PRODUCTION")
    assert server.quality_entry_gate(r,{})["ok"] is False


def test_global_entry_time_gate_windows_unchanged():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    ny=ZoneInfo("America/New_York")
    assert server.new_entry_time_gate(datetime(2026,8,31,6,59,tzinfo=ny))["allowed"] is True
    assert server.new_entry_time_gate(datetime(2026,8,31,7,0,tzinfo=ny))["allowed"] is False
    assert server.new_entry_time_gate(datetime(2026,8,31,10,0,tzinfo=ny))["allowed"] is True
    assert server.new_entry_time_gate(datetime(2026,8,31,14,59,tzinfo=ny))["allowed"] is True
    assert server.new_entry_time_gate(datetime(2026,8,31,15,0,tzinfo=ny))["allowed"] is False
    assert server.new_entry_time_gate(datetime(2026,8,31,19,0,tzinfo=ny))["allowed"] is True


def test_safety_still_precedes_usdjpy_experiment(monkeypatch):
    _paper_practice(monkeypatch)
    r=_row("BUY",buy=100.0,sell=0.0,extension=0.2,blocked=True,m1=False)
    out=server.execution_decision(r,{})
    assert out["execute"] is False
    assert out["reason"].startswith("Safety veto:")


def test_existing_eur_policy_unchanged():
    p=forward_experiment.forward_policy("EUR_USD")
    assert p["experiment_id"]=="EUR_PHASE2_FORWARD_V1"
    out=forward_experiment.evaluate_forward_experiment("EUR_USD",{
        "legacy_v331_buy_score":31.0,"legacy_v331_sell_score":30.0,
        "legacy_v331_directional_score":31.0,"legacy_v331_chosen_direction":"BUY"})
    assert out["ok"] is True and out["threshold"]==31.0


def test_existing_gbp_policy_and_buy_side_semantics_unchanged():
    p=forward_experiment.forward_policy("GBP_USD")
    assert p["experiment_id"]=="GBP_PHASE2_FORWARD_V1"
    out=forward_experiment.evaluate_forward_experiment("GBP_USD",{
        "extension_atr":forward_experiment.GBP_EXTENSION_ATR_MAX,
        "legacy_v331_buy_score":forward_experiment.GBP_LEGACY_BUY_SCORE_MIN,
        "legacy_v331_sell_score":-999.0})
    assert out["ok"] is True


def test_aud_cad_behavior_remains_no_forward_experiment(monkeypatch):
    _paper_practice(monkeypatch)
    for symbol in ("AUD_USD","USD_CAD"):
        assert server._forward_experiment_active(symbol) is False
        r=_row("BUY",extension=0.2,m1=False); r["instrument"]=symbol
        assert server.quality_entry_gate(r,{})["ok"] is False


def test_no_authority_expansion_outside_paper_practice(monkeypatch):
    for env,broker,url in [
        ("PRODUCTION","live","https://api-fxtrade.oanda.com"),
        ("SIMULATION","practice","https://api-fxpractice.oanda.com"),
        ("PAPER","live","https://api-fxtrade.oanda.com"),
    ]:
        monkeypatch.setattr(server,"TRADING_ENVIRONMENT",env)
        monkeypatch.setattr(server,"PRIMARY_OANDA_ENV",broker)
        monkeypatch.setattr(server,"OANDA",url)
        assert server._forward_experiment_active("USD_JPY") is False


def test_post_fill_hardening_functions_not_part_of_usdjpy_policy_module():
    src=inspect.getsource(forward_experiment)
    for name in ("reanchor_post_fill_protection","replace_trade_protection","verify_trade_protection"):
        assert name not in src


def test_usdjpy_observability_and_execution_evidence_identity(monkeypatch):
    _paper_practice(monkeypatch)
    r=_row("SELL",buy=99.0,sell=33.0,extension=9.0,m1=False)
    r["new_entry_time_gate"]={"allowed":True,"reason":"ALLOWED","window":None}
    conf={"probability":0.5,"required_confidence":0.5,"samples":0}
    snap=server.forward_observation_snapshot(r,conf,executed=False,final_reason="TEST_REJECT")
    assert snap["experiment_id"]=="USDJPY_PHASE2_FORWARD_V1"
    assert snap["chosen_direction"]=="SELL"
    assert snap["chosen_legacy_score"]==33.0
    assert snap["experiment_threshold"]==33.0
    assert snap["experiment_score_gate_pass"] is True
    assert snap["m1_bypass_active"] is True
    assert snap["extension_bypass_active"] is True
    assert snap["global_entry_time_gate"]["allowed"] is True
    assert snap["safety_pass"] is True
    assert snap["final_strategic_eligibility"] is True
    assert snap["rejection_reason"]=="TEST_REJECT"
    # Executed-trade memory path explicitly persists the pre-entry experiment gate.
    assert "forward_experiment_gate(r)" in inspect.getsource(server.record_trade_memory_entry)

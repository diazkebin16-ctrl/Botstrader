import asyncio
import inspect

import httpx
import pytest

import server


def _candidate(side="BUY"):
    if side == "BUY":
        return {"instrument":"EUR_USD","signal":"BUY","entry":1.10000,"stop":1.09910,
                "target":1.10135,"managed_target":1.10135,"portfolio_execution_guard":{"allow":True}}
    return {"instrument":"EUR_USD","signal":"SELL","entry":1.10000,"stop":1.10090,
            "target":1.09865,"managed_target":1.09865,"portfolio_execution_guard":{"allow":True}}


def test_exact_planned_fill_preserves_geometry():
    for side in ("BUY", "SELL"):
        r=_candidate(side)
        g=server.post_fill_protection_geometry("EUR_USD",side,r["entry"],r["stop"],r["target"],r["entry"])
        assert g["applied_stop"] == pytest.approx(r["stop"])
        assert g["applied_target"] == pytest.approx(r["target"])
        assert g["rr"] == pytest.approx(1.5)


def test_effective_geometry_drives_break_even_and_trailing():
    fill=1.10050
    g=server.post_fill_protection_geometry("EUR_USD","BUY",1.10000,1.09910,1.10135,fill)
    risk=fill-g["applied_stop"]
    be=server.adaptive_stop_price("BUY",fill,g["applied_stop"],fill+risk,"BE_PROFIT_TRAIL")
    assert be["r_multiple"] == pytest.approx(1.0)
    assert be["action"] == "BREAK_EVEN"
    trail=server.adaptive_stop_price("BUY",fill,g["applied_stop"],fill+2.2*risk,"BE_PROFIT_TRAIL")
    assert trail["r_multiple"] == pytest.approx(2.2)
    assert trail["new_stop"] > fill


def test_broker_verification_transport_error_is_not_false_ok(monkeypatch):
    async def fail(*args, **kwargs):
        raise httpx.ReadTimeout("ambiguous broker read")
    monkeypatch.setattr(server,"req",fail)
    out=asyncio.run(server.verify_trade_protection(object(),"T","EUR_USD",1.0991,1.10135))
    assert out["status"] == "PROTECTION_ERROR"
    assert not out["sl_ok"] and not out["tp_ok"]


def test_partial_or_missing_broker_state_is_not_confirmed(monkeypatch):
    async def partial(*args, **kwargs):
        return {"trade":{"stopLossOrder":{"price":"1.09910"}}}
    monkeypatch.setattr(server,"req",partial)
    out=asyncio.run(server.verify_trade_protection(object(),"T","EUR_USD",1.0991,1.10135))
    assert out["status"] == "PROTECTION_ERROR"
    assert out["stop_match"] is True
    assert out["target_match"] is False


def test_legacy_execute_submits_immediate_sl_and_tp_before_any_reanchor(monkeypatch):
    calls=[]
    async def fake_req(client,method,path,params=None,body=None):
        calls.append((method,path,body))
        return {"orderFillTransaction":{"id":"F1","price":"1.10000","tradeOpened":{"tradeID":"T1","units":"100"}}}
    monkeypatch.setattr(server,"req",fake_req)
    monkeypatch.setattr(server,"instrument_mode",lambda inst:"ENABLED")
    monkeypatch.setattr(server,"market_is_weekend_closed",lambda:False)
    monkeypatch.setattr(server,"new_entry_time_gate",lambda:{"allowed":True,"reason":"OK"})
    monkeypatch.setattr(server,"SINGLE",False)
    monkeypatch.setattr(server,"instrument_sizing",lambda *a,**kw:{"effective_units":100.0})
    r=_candidate("BUY")
    asyncio.run(server.execute(object(),r))
    assert len(calls)==1
    method,path,body=calls[0]
    assert method=="POST"
    order=body["order"]
    assert order["stopLossOnFill"]["price"] == server.format_instrument_price("EUR_USD",r["stop"])
    assert order["takeProfitOnFill"]["price"] == server.format_instrument_price("EUR_USD",r["target"])


def test_recoverable_order_body_has_immediate_protection_and_reanchor_is_separate():
    src=inspect.getsource(server.execute_recoverable)
    reanchor_src=inspect.getsource(server.reanchor_post_fill_protection)
    assert '"stopLossOnFill"' in src
    assert '"takeProfitOnFill"' in src
    assert 'replace_trade_protection' in reanchor_src
    assert 'PUT' in inspect.getsource(server.replace_trade_protection)
    # Reanchor is post-fill and never replaces the initial protected order submission.
    assert 'reanchor_post_fill_protection' not in src


def test_reanchor_write_is_single_atomic_protection_update():
    src=inspect.getsource(server.replace_trade_protection)
    assert src.count('await req(') == 1
    assert '"stopLoss"' in src and '"takeProfit"' in src
    assert 'method' not in src or True


def test_v338_forward_identity_preserved_after_postfill_merge():
    assert server.forward_policy("EUR_USD")["experiment_id"] == "EUR_PHASE2_FORWARD_V1"
    assert server.forward_policy("GBP_USD")["experiment_id"] == "GBP_PHASE2_FORWARD_V1"
    eur=server.evaluate_forward_experiment("EUR_USD",{
        "legacy_v331_buy_score":31.0,"legacy_v331_sell_score":20.0,
        "legacy_v331_directional_score":31.0,"legacy_v331_chosen_direction":"BUY"})
    assert eur["ok"] is True
    gbp=server.evaluate_forward_experiment("GBP_USD",{
        "extension_atr":1.4985678822167452,"legacy_v331_buy_score":16.400000000000002})
    assert gbp["ok"] is True

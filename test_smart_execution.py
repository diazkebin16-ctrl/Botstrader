
import os, tempfile, sqlite3
from datetime import datetime, timezone, timedelta
from smart_execution import SmartExecutionEngine

def eng(**kw):
    path=tempfile.mktemp(suffix=".db")
    e=SmartExecutionEngine(path,mode="SHADOW",**kw);e.ensure_schema()
    return e,path

def snap(e,eid,**kw):
    base=dict(bid=1.1000,ask=1.1002,last_price=1.1001,available_liquidity=1000,recent_volume=10000,
              volatility="NORMAL",market_regime="BULLISH_TREND",timestamp=datetime.now(timezone.utc).isoformat(),
              broker_health="OK",broker_latency_ms=80,market_status="tradeable")
    base.update(kw);return e.capture_snapshot(eid,**base)

def intent(e,qty=100,slip=10,urgency="NORMAL",ttl=60,side="BUY",risk_valid=True,risk_until=None):
    return e.create_intent(strategy_id="S1",symbol="EUR_USD",side=side,target_quantity=qty,maximum_quantity=qty,
                           risk_approved_quantity=qty,expected_price=1.1001,urgency=urgency,
                           maximum_slippage_bps=slip,time_limit_seconds=ttl,
                           risk_approval_valid=risk_valid,risk_approval_valid_until=risk_until)

def test_normal_liquidity_never_exceeds_risk():
    e,p=eng();x=intent(e,100,20,"HIGH");s=snap(e,x["execution_intent_id"])
    r=e.recommend(x["execution_intent_id"],s)
    assert r["recommended_quantity"]<=100
    assert r["order_type"]=="MARKET"
    os.remove(p)

def test_low_liquidity_reduces_size():
    e,p=eng();x=intent(e,100,20);s=snap(e,x["execution_intent_id"],available_liquidity=60)
    r=e.recommend(x["execution_intent_id"],s)
    assert r["recommended_quantity"]==60
    assert "LOW_LIQUIDITY_REDUCE_SIZE" in r["reasons"]
    os.remove(p)

def test_wide_spread_prefers_limit_or_blocks():
    e,p=eng()
    # seed normal spread history
    for i in range(12):
        x=intent(e,100,20);snap(e,x["execution_intent_id"],bid=1.1000,ask=1.1001)
    x=intent(e,100,20);s=snap(e,x["execution_intent_id"],bid=1.1000,ask=1.1010)
    r=e.recommend(x["execution_intent_id"],s)
    assert r["spread_state"] in ("WIDE_SPREAD","ABNORMAL_SPREAD")
    assert r["order_type"]=="LIMIT"
    os.remove(p)

def test_high_volatility_does_not_increase_size():
    e,p=eng();x=intent(e,100,20);s=snap(e,x["execution_intent_id"],volatility="HIGH")
    r=e.recommend(x["execution_intent_id"],s)
    assert r["execution_regime"]=="HIGH_VOLATILITY_EXECUTION"
    assert r["recommended_quantity"]<=100
    os.remove(p)

def test_broker_latency_reduces_confidence_not_raise_size():
    e,p=eng(latency_warning_ms=500);x=intent(e,100,20);s=snap(e,x["execution_intent_id"],broker_latency_ms=2500)
    r=e.recommend(x["execution_intent_id"],s)
    assert r["execution_confidence"]<1
    assert r["recommended_quantity"]<=100
    os.remove(p)

def test_partial_fill_then_revalidate_cancels_expensive_remaining():
    e,p=eng()
    x=intent(e,100,8)
    s1=snap(e,x["execution_intent_id"],available_liquidity=60)
    d1=e.recommend(x["execution_intent_id"],s1)
    assert d1["recommended_quantity"]==60
    e.record_fill(x["execution_intent_id"],fill_quantity=40,fill_price=1.1002,broker_event_id="F1",order_type="LIMIT")
    # market worsens sharply: large spread + high volatility
    s2=snap(e,x["execution_intent_id"],bid=1.0980,ask=1.1030,available_liquidity=20,
            volatility="EXTREME",broker_latency_ms=1800)
    d2=e.revalidate_remaining(x["execution_intent_id"],s2,risk_approval_valid=True,
                              strategy_intent_valid=True,position_state_valid=True)
    assert d2["action"]=="CANCEL_REMAINING_EXECUTION"
    assert e.intent(x["execution_intent_id"])["filled_quantity"]==40
    assert e.intent(x["execution_intent_id"])["filled_quantity"]<=100
    os.remove(p)

def test_no_fill_and_rejection_recordable():
    e,p=eng();x=intent(e,50,10);s=snap(e,x["execution_intent_id"])
    e.recommend(x["execution_intent_id"],s)
    out=e.record_fill(x["execution_intent_id"],fill_quantity=0,fill_price=1.1001,
                      broker_event_id="R1",order_type="LIMIT",rejected=True)
    assert out["filled_quantity"]==0
    os.remove(p)

def test_slippage_spike_blocks():
    e,p=eng();x=intent(e,100,.1);s=snap(e,x["execution_intent_id"],bid=1.0990,ask=1.1020,volatility="HIGH")
    r=e.recommend(x["execution_intent_id"],s)
    assert r["action"] in ("REJECT_EXECUTION","DELAY")
    assert "EXPECTED_SLIPPAGE_EXCEEDS_ALLOWED" in r["reasons"] or "EXECUTION_CONFIDENCE_TOO_LOW" in r["reasons"]
    os.remove(p)

def test_stale_intent_no_new_order():
    e,p=eng(default_intent_ttl_seconds=5)
    old=(datetime.now(timezone.utc)-timedelta(minutes=5)).isoformat()
    x=e.create_intent(strategy_id="S",symbol="EUR_USD",side="BUY",target_quantity=10,maximum_quantity=10,
                      risk_approved_quantity=10,expected_price=1.1,maximum_slippage_bps=10,
                      time_limit_seconds=5,signal_time=old)
    s=snap(e,x["execution_intent_id"])
    r=e.recommend(x["execution_intent_id"],s)
    assert r["action"]=="CANCEL_REMAINING_EXECUTION"
    assert "STALE_EXECUTION_INTENT" in r["reasons"]
    os.remove(p)

def test_risk_approval_expiry_blocks():
    e,p=eng();past=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
    x=intent(e,100,10,risk_until=past);s=snap(e,x["execution_intent_id"])
    r=e.recommend(x["execution_intent_id"],s)
    assert r["action"]=="REJECT_EXECUTION"
    assert "RISK_APPROVAL_INVALID_OR_EXPIRED" in r["reasons"]
    os.remove(p)

def test_stale_market_data_blocks():
    e,p=eng(max_snapshot_age_seconds=2);x=intent(e,100,10)
    old=(datetime.now(timezone.utc)-timedelta(seconds=10)).isoformat()
    s=snap(e,x["execution_intent_id"],timestamp=old)
    r=e.recommend(x["execution_intent_id"],s)
    assert r["action"]=="REJECT_EXECUTION"
    assert "STALE_CRITICAL_DATA" in r["reasons"] or "EXECUTION_SUSPENDED" in r["reasons"]
    os.remove(p)

def test_duplicate_broker_event_dedup():
    e,p=eng();x=intent(e,100,10);s=snap(e,x["execution_intent_id"]);e.recommend(x["execution_intent_id"],s)
    a=e.record_fill(x["execution_intent_id"],fill_quantity=40,fill_price=1.1002,broker_event_id="same")
    b=e.record_fill(x["execution_intent_id"],fill_quantity=40,fill_price=1.1002,broker_event_id="same")
    assert not a["duplicate_event"] and b["duplicate_event"]
    assert e.intent(x["execution_intent_id"])["filled_quantity"]==40
    os.remove(p)

def test_emergency_stop_blocks():
    e,p=eng();x=intent(e,100,10);s=snap(e,x["execution_intent_id"])
    r=e.recommend(x["execution_intent_id"],s,emergency_stop=True)
    assert r["action"]=="REJECT_EXECUTION"
    os.remove(p)

def test_fill_cannot_exceed_risk_approval():
    e,p=eng();x=intent(e,100,10);s=snap(e,x["execution_intent_id"]);e.recommend(x["execution_intent_id"],s)
    try:
        e.record_fill(x["execution_intent_id"],fill_quantity=101,fill_price=1.1,broker_event_id="too-large")
        raise AssertionError("risk ceiling bypassed")
    except PermissionError:
        pass
    os.remove(p)

def test_order_slicing_conserves_total_and_cap():
    e,p=eng(slice_threshold_units=500,slice_size_units=200)
    x=intent(e,1000,20);s=snap(e,x["execution_intent_id"],available_liquidity=5000)
    r=e.recommend(x["execution_intent_id"],s)
    assert abs(sum(r["slice_plan"])-r["recommended_quantity"])<1e-9
    assert sum(r["slice_plan"])<=1000
    assert all(q<=200 for q in r["slice_plan"])
    os.remove(p)

def test_transaction_cost_and_execution_loss_attribution():
    e,p=eng();x=intent(e,100,20);s=snap(e,x["execution_intent_id"])
    e.recommend(x["execution_intent_id"],s,expected_gross_edge=0.05)
    out=e.record_fill(x["execution_intent_id"],fill_quantity=100,fill_price=1.1020,
                      broker_event_id="F",fees=.02,order_type="MARKET",expected_gross_edge=.05)
    assert out["tca"]["total_execution_cost"]>0
    assert out["tca"]["execution_quality_score"]<=100
    assert out["tca"]["attribution"]=="EXECUTION_LOSS"
    os.remove(p)

def test_no_economic_edge_after_costs():
    e,p=eng();x=intent(e,100,20);s=snap(e,x["execution_intent_id"],bid=1.1,ask=1.101)
    r=e.recommend(x["execution_intent_id"],s,expected_gross_edge=.0001)
    assert r["action"]=="REJECT_EXECUTION"
    assert "NO_ECONOMIC_EDGE_AFTER_COSTS" in r["reasons"]
    os.remove(p)

def test_shadow_mode_never_claims_hypothetical_fill():
    e,p=eng();x=intent(e,100,10);s=snap(e,x["execution_intent_id"],available_liquidity=60)
    e.recommend(x["execution_intent_id"],s)
    e.record_fill(x["execution_intent_id"],fill_quantity=40,fill_price=1.1002,broker_event_id="F")
    c=e.shadow_compare(x["execution_intent_id"],actual_order_type="MARKET",actual_quantity=40,
                       actual_slippage_bps=1.0,actual_cost=.1,actual_fill_rate=.4)
    assert c["hypothetical_fill_not_assumed"]==1
    assert c["outcome"]=="OBSERVE_ONLY_NO_CAUSAL_CLAIM"
    os.remove(p)

def test_execution_candidate_never_autodeploys():
    e,p=eng();c=e.candidate_execution_policy("policy_v1",{"spread_limit":2},{"samples":100})
    assert c["auto_deploy"] is False
    assert "CANARY" in c["required_path"]
    os.remove(p)


def test_exit_quality_and_adverse_selection():
    e,p=eng();x=intent(e,100,20);s=snap(e,x["execution_intent_id"]);e.recommend(x["execution_intent_id"],s)
    e.record_fill(x["execution_intent_id"],fill_quantity=100,fill_price=1.1002,broker_event_id="ENTRY",order_type="MARKET")
    adv=e.record_adverse_selection(x["execution_intent_id"],post_fill_price=1.0995,window_seconds=30)
    ex=e.record_exit_quality(x["execution_intent_id"],expected_exit=1.1010,actual_exit=1.1007,quantity=100,fees=.01,stop_expected=1.0990)
    assert adv["status"]=="RECORDED" and adv["adverse_selection_bps"]>0
    assert 0<=ex["exit_execution_score"]<=100
    os.remove(p)

def test_execution_memory_and_latency_baseline():
    e,p=eng();x=intent(e,100,20);s=snap(e,x["execution_intent_id"]);e.recommend(x["execution_intent_id"],s)
    e.record_fill(x["execution_intent_id"],fill_quantity=100,fill_price=1.1002,broker_event_id="LAT",order_type="MARKET",broker_ack_latency_ms=123,first_fill_latency_ms=140,session="NY")
    lb=e.latency_baseline("EUR_USD","NY","MARKET")
    mem=e.execution_memory_analysis(min_samples=1)
    assert lb["samples"]>=1 and lb["ack_mean_ms"]==123
    assert mem["groups"] and mem["groups"][0]["status"]=="OBSERVED"
    assert mem["auto_policy_change"] is False
    os.remove(p)

if __name__=="__main__":
    tests=[v for k,v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:t()
    print(f"smart execution tests: OK ({len(tests)})")

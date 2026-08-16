
from __future__ import annotations
from typing import Dict,Any,List
from pathlib import Path
import json, math, random, statistics, tempfile
from smart_execution import SmartExecutionEngine

def max_drawdown(xs):
    equity=0.0;peak=0.0;dd=0.0
    for x in xs:
        equity+=x;peak=max(peak,equity);dd=max(dd,peak-equity)
    return dd

def run_simulation(seed:int=160024,n:int=500)->Dict[str,Any]:
    """
    Deterministic microstructure simulation, not a claim of historical alpha.

    The repository did not persist bid/ask/liquidity snapshots before Step 16,
    so a genuine BASE-vs-SMART historical microstructure backtest is not
    reconstructible without inventing data. This simulation deliberately uses
    synthetic but seeded market snapshots to validate execution mechanics.
    """
    rng=random.Random(seed)
    db=tempfile.mktemp(suffix=".db")
    e=SmartExecutionEngine(db,"3.24",mode="SHADOW",max_snapshot_age_seconds=5,
                           min_history_samples=20,liquidity_participation=.25,
                           slice_threshold_units=1000,slice_size_units=200)
    e.ensure_schema()

    base_rows=[];smart_rows=[]
    for i in range(n):
        mid=1.05+rng.random()*.15
        spread_bps=rng.choice([.5,1,1.5,2,4,8,15])
        spread=mid*spread_bps/10000
        bid=mid-spread/2;ask=mid+spread/2
        liquidity=rng.choice([40,60,100,150,300,1000,5000])
        vol=rng.choices(["LOW","NORMAL","HIGH","EXTREME"],[.15,.55,.22,.08])[0]
        latency=rng.choice([40,80,150,400,900,1800,3500])
        side=rng.choice(["BUY","SELL"])
        qty=rng.choice([50,100,200,500,1000,2000])
        gross_edge_per_unit=rng.uniform(.00002,.00025)
        gross_edge=gross_edge_per_unit*qty
        # BASE: current immediate MARKET execution, bounded by risk approved qty.
        base_slip_bps=(spread_bps*.55+
                       (1.2 if vol=="HIGH" else 3.0 if vol=="EXTREME" else 0)+
                       min(3,latency/2000)+
                       max(0,qty/max(liquidity,qty)-.25)*8)
        base_fill_rate=1.0 if liquidity>=qty else max(.1,liquidity/qty)
        base_filled=qty*base_fill_rate
        base_fees=base_filled*mid*.00002
        base_spread_cost=spread_bps/20000*base_filled*mid
        base_slip_cost=base_slip_bps/10000*base_filled*mid
        base_net=gross_edge*base_fill_rate-base_fees-base_spread_cost-base_slip_cost
        base_quality=max(0,100-min(40,base_slip_bps*4)-(1-base_fill_rate)*25-min(20,latency/150))
        base_rows.append(dict(net=base_net,gross=gross_edge*base_fill_rate,slippage=base_slip_bps,
                              fees=base_fees,fill_rate=base_fill_rate,quality=base_quality))

        intent=e.create_intent(strategy_id="SIM",symbol="EUR_USD",side=side,target_quantity=qty,
            maximum_quantity=qty,risk_approved_quantity=qty,expected_price=mid,urgency="NORMAL",
            maximum_slippage_bps=8,time_limit_seconds=60,risk_approval_valid=True)
        snap=e.capture_snapshot(intent["execution_intent_id"],bid=bid,ask=ask,last_price=mid,
            available_liquidity=liquidity,recent_volume=liquidity*20,volatility=vol,
            market_regime="RANGE" if vol in ("LOW","NORMAL") else "HIGH_VOLATILITY",
            broker_health="OK",broker_latency_ms=latency,market_status="tradeable")
        rec=e.recommend(intent["execution_intent_id"],snap,expected_gross_edge=gross_edge,
                        execution_cost_budget=max(gross_edge*.8,1e-8),
                        actual_order_type="MARKET",actual_requested_quantity=qty)

        # Smart hypothetical fill is NOT assumed. For simulation only, use the
        # model's fill probability with a deterministic seed to realize fill/no-fill.
        smart_qty=min(rec["recommended_quantity"],qty)
        if rec["action"] in ("REJECT_EXECUTION","CANCEL_REMAINING_EXECUTION"):
            smart_fill_rate=0.0
        elif rec["action"]=="DELAY":
            smart_fill_rate=0.0
        else:
            probability=float(rec["fill_probability"])
            filled_event=rng.random()<=probability
            liquidity_cap=min(1.0,liquidity/max(smart_qty,1e-9))
            smart_fill_rate=(liquidity_cap if filled_event else 0.0)
        smart_filled=smart_qty*smart_fill_rate
        smart_slip=float(rec["expected_slippage_bps"]) if smart_filled else 0.0
        smart_fees=smart_filled*mid*.00002
        smart_spread_cost=spread_bps/20000*smart_filled*mid
        smart_slip_cost=smart_slip/10000*smart_filled*mid
        smart_gross=gross_edge*(smart_filled/max(qty,1e-9))
        smart_net=smart_gross-smart_fees-smart_spread_cost-smart_slip_cost
        smart_quality=max(0,100-min(40,smart_slip*4)-(1-smart_fill_rate)*25-min(20,latency/150))
        smart_rows.append(dict(net=smart_net,gross=smart_gross,slippage=smart_slip,fees=smart_fees,
                               fill_rate=smart_fill_rate,quality=smart_quality,action=rec["action"],
                               order_type=rec["order_type"],requested=smart_qty,risk_approved=qty))
    def summarize(rows):
        return {
            "gross_pnl_proxy":sum(x["gross"] for x in rows),
            "net_pnl_proxy":sum(x["net"] for x in rows),
            "avg_slippage_bps":statistics.mean(x["slippage"] for x in rows),
            "fees":sum(x["fees"] for x in rows),
            "fill_rate":statistics.mean(x["fill_rate"] for x in rows),
            "max_drawdown_proxy":max_drawdown([x["net"] for x in rows]),
            "execution_quality":statistics.mean(x["quality"] for x in rows),
        }
    base=summarize(base_rows);smart=summarize(smart_rows)
    safety={
        "executed_or_requested_never_above_risk_approved":all(x["requested"]<=x["risk_approved"]+1e-12 for x in smart_rows),
        "shadow_only":e.mode=="SHADOW",
        "no_causal_fill_assumption":True,
    }
    improvement={
        "net_pnl_proxy_delta":smart["net_pnl_proxy"]-base["net_pnl_proxy"],
        "avg_slippage_bps_delta":smart["avg_slippage_bps"]-base["avg_slippage_bps"],
        "fill_rate_delta":smart["fill_rate"]-base["fill_rate"],
        "execution_quality_delta":smart["execution_quality"]-base["execution_quality"],
        "drawdown_delta":smart["max_drawdown_proxy"]-base["max_drawdown_proxy"],
    }
    return {
        "version":"3.24","seed":seed,"scenarios":n,
        "evidence_type":"SYNTHETIC_MICROSTRUCTURE_SIMULATION",
        "historical_microstructure_backtest_status":"INSUFFICIENT_HISTORICAL_BID_ASK_LIQUIDITY_DATA_BEFORE_STEP16",
        "base_execution":base,"smart_execution_shadow":smart,"comparison":improvement,
        "safety":safety,
        "interpretation":"Simulation validates mechanics only. It is not evidence that Smart Execution improves live profitability.",
        "production_activation":False,
    }

if __name__=="__main__":
    import sys
    out=run_simulation()
    if len(sys.argv)>1:
        Path(sys.argv[1]).write_text(json.dumps(out,indent=2),encoding="utf-8")
    print(json.dumps(out,indent=2))

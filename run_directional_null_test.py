#!/usr/bin/env python3
"""Run the pre-registered directional null diagnostic without opening TEST."""
import argparse,json,os,sys
sys.path.insert(0,os.path.dirname(__file__))
from historical_candles import load_bundle
from historical_execution import HistoricalExecutionConfig
from historical_replay import ReplayVariant,_dt
from directional_null_test import DirectionalNullConfig,evaluate_pairs,evaluate_shadow_test_b,evaluate_component_diagnostic,reconstruct_opportunities

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cache",required=True);ap.add_argument("--benchmark",required=True)
    ap.add_argument("--variant",default="V331_BASELINE");ap.add_argument("--output",default="directional_null_test.json")
    ap.add_argument("--simulations",type=int,default=20000);ap.add_argument("--bootstrap-samples",type=int,default=20000)
    args=ap.parse_args()
    with open(args.benchmark,encoding="utf-8") as f:bench=json.load(f)
    if not bench.get("certification_eligible",False):raise SystemExit("Benchmark is not certification_eligible; refusing diagnostic.")
    vr=bench["variants"][args.variant]; boundary=vr["holdout"]["boundaries"]["test"]
    # TEST remains sealed: only canonical independent episodes strictly before its frozen boundary.
    opportunities=[r for r in vr["episodes"] if _dt(r["candle_ts"])<_dt(boundary)]
    meth=bench.get("methodology") or {}
    execution=HistoricalExecutionConfig(entry_slippage_pips=float(meth.get("entry_slippage_pips",.10)),
        exit_slippage_pips=float(meth.get("exit_slippage_pips",.10)),latency_bars=int(meth.get("latency_bars",0)),require_bid_ask=True)
    horizon=180
    bundle=load_bundle(args.cache)
    os.environ.setdefault("TRADING_ENVIRONMENT","SIMULATION");os.environ.setdefault("AUTO_TRADE","false")
    import server
    variant=ReplayVariant(args.variant,"V331_BASELINE",0.0) if args.variant=="V331_BASELINE" else ReplayVariant(args.variant,"SESSION",1.0)
    pairs=reconstruct_opportunities(server,bundle,bench.get("instrument","EUR_USD"),opportunities,variant,horizon_bars=horizon,execution=execution)
    drift=sum(1 for x in pairs if x["direction_drift"])
    cfg=DirectionalNullConfig(simulations=args.simulations,bootstrap_samples=args.bootstrap_samples)
    result=evaluate_pairs(pairs,cfg)
    result["shadow_test_b"]=evaluate_shadow_test_b(pairs,cfg)
    result["component_diagnostic"]=evaluate_component_diagnostic(pairs)
    result.update({"protocol":"DIRECTIONAL_NULL_TEST_PROTOCOL.md","instrument":bench.get("instrument"),"variant":args.variant,
        "sample":"DISCOVERY_PLUS_VALIDATION_PRE_TEST_BOUNDARY","test_boundary":boundary,"test_holdout_opened":False,
        "validation_contamination_note":"Validation was previously inspected; this is diagnostic, not pristine confirmatory evidence.",
        "direction_reconstruction_drift_count":drift,
        "execution":{"entry_slippage_pips":execution.entry_slippage_pips,"exit_slippage_pips":execution.exit_slippage_pips,
                     "latency_bars":execution.latency_bars,"require_bid_ask":execution.require_bid_ask},
        "research_integrity":"No scoring weights, thresholds, execution assumptions, episode definitions, or sample boundaries changed."})
    with open(args.output,"w",encoding="utf-8") as f:json.dump(result,f,indent=2,default=str)
    p=result["paired_test"];e=result["economic_edge"];o=result["one_sided_robustness"]
    print(f"Guardado: {args.output}")
    print(f"paired={p['n']} bot_expR={p['bot_expectancy_r']} null_p95={p['null_p95']} percentile={p['bot_empirical_percentile']}")
    print(f"condition1={p['condition_1_directional_skill_pass']} condition2={e['condition_2_economic_edge_pass']} CI90={e['bootstrap_ci']}")
    print(f"one_sided={o['n']} unique_side_rate={o['selection_rate']} | {result['interpretation']}")
    b=result["shadow_test_b"]
    print(f"TEST_B n={b['n']} bot_expR={b['bot_expectancy_r']} opposite_expR={b['opposite_shadow_expectancy_r']} null_p95={b['null_p95']} percentile={b['bot_empirical_percentile']}")
    print(f"TEST_B condition1={b['condition_1_directional_skill_pass']} condition2={b['condition_2_economic_edge_pass']} CI90={b['bootstrap_ci']} oracle={b['oracle_expectancy_r']} | {b['interpretation']}")
    c=result["component_diagnostic"]
    print(f"COMPONENT_DIAG n={c['n']} bot_loss_opp_win={c['bot_loss_opposite_win']} bot_win_opp_loss={c['bot_win_opposite_loss']}")
    print("COMPONENT_DELTA:",c["mean_bot_minus_opposite"])
    print("WRONG_WAY_DELTA:",c["mean_delta_when_bot_loses_opposite_wins"])
if __name__=="__main__":main()

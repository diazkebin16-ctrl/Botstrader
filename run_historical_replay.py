#!/usr/bin/env python3
"""Run v3.34.1 independent historical candle replay. Read-only broker access."""
import argparse,asyncio,json,os,sys
from datetime import datetime
sys.path.insert(0,os.path.dirname(__file__))
from historical_candles import fetch_bundle,load_bundle,save_bundle
from historical_replay import ReplayConfig,ReplayVariant,replay_history

def dt(s):return datetime.fromisoformat(s.replace("Z","+00:00"))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--instrument",default="EUR_USD");ap.add_argument("--start",required=True);ap.add_argument("--end",required=True)
    ap.add_argument("--cache",default="");ap.add_argument("--output",default="historical_replay_v3.34.1.json")
    ap.add_argument("--cost-pips",type=float,default=1.0);ap.add_argument("--horizon",type=int,default=180)
    ap.add_argument("--session-scales",default="0.5,0.75,1.0,1.25,1.5")
    args=ap.parse_args();start,end=dt(args.start),dt(args.end)
    if args.cache and os.path.exists(args.cache):bundle=load_bundle(args.cache)
    else:
        bundle=asyncio.run(fetch_bundle(args.instrument,start,end,warmup_days=10,horizon_minutes=args.horizon+60))
        if args.cache:save_bundle(args.cache,bundle)
    # Local import keeps downloader broker-read-only and makes research intent explicit.
    os.environ.setdefault("TRADING_ENVIRONMENT","SIMULATION");os.environ.setdefault("AUTO_TRADE","false")
    import server
    variants=[ReplayVariant("V331_BASELINE","V331_BASELINE",0.0)]
    for s in [float(x) for x in args.session_scales.split(",") if x.strip()]:variants.append(ReplayVariant(f"SESSION_{s:g}X","SESSION",s))
    report=replay_history(server,bundle,args.instrument,start,end,variants,ReplayConfig(horizon_bars=args.horizon,round_trip_cost_pips=args.cost_pips))
    with open(args.output,"w",encoding="utf-8") as f:json.dump(report,f,indent=2,default=str)
    print(json.dumps({"output":args.output,"instrument":args.instrument,"variants":{k:v["metrics"] for k,v in report["variants"].items()}},indent=2))
if __name__=="__main__":main()

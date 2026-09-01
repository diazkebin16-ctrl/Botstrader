#!/usr/bin/env python3
"""Run independent historical bid/ask candle replay. Read-only broker access."""
import argparse,asyncio,json,os,sys
from datetime import datetime
sys.path.insert(0,os.path.dirname(__file__))
from historical_candles import fetch_bundle,load_bundle,save_bundle
from historical_replay import ReplayConfig,ReplayVariant,replay_history
from historical_execution import HistoricalExecutionConfig
from replay_validation import ReplayValidationConfig

def dt(s):return datetime.fromisoformat(s.replace("Z","+00:00"))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--instrument",default="EUR_USD");ap.add_argument("--start",required=True);ap.add_argument("--end",required=True)
    ap.add_argument("--cache",default="");ap.add_argument("--output",default="historical_replay_v3.35.json")
    ap.add_argument("--horizon",type=int,default=180)
    ap.add_argument("--entry-slippage-pips",type=float,default=0.10)
    ap.add_argument("--exit-slippage-pips",type=float,default=0.10)
    ap.add_argument("--latency-bars",type=int,default=0)
    ap.add_argument("--embargo-minutes",type=int,default=30)
    ap.add_argument("--integrity-artifact",default="",help="Passed data-integrity evidence for this exact cache")
    ap.add_argument("--session-scales",default="1.0")
    ap.add_argument("--allow-research-sweep",action="store_true",help="Allow multiple session scales; output is research-only and not certification evidence")
    args=ap.parse_args();start,end=dt(args.start),dt(args.end)
    integrity=None
    if args.integrity_artifact:
        with open(args.integrity_artifact,encoding="utf-8") as f:integrity=json.load(f)
        if integrity.get("status")!="PASS":raise SystemExit("Data integrity gate did not pass")
        import hashlib
        digest=hashlib.sha256()
        with open(args.cache,"rb") as f:
            for chunk in iter(lambda:f.read(1024*1024),b""):digest.update(chunk)
        if digest.hexdigest()!=integrity.get("input_sha256"):raise SystemExit("Cache SHA does not match integrity artifact")
        if str(integrity.get("instrument") or "").upper()!=args.instrument.upper():raise SystemExit("Integrity artifact instrument mismatch")
    if os.getenv("BOTS_RESEARCH_OFFLINE","").lower()=="true" and (not args.cache or not os.path.exists(args.cache)):
        raise SystemExit("Offline research requires an existing cache; network download is blocked")
    if args.cache and os.path.exists(args.cache):bundle=load_bundle(args.cache)
    else:
        bundle=asyncio.run(fetch_bundle(args.instrument,start,end,warmup_days=10,horizon_minutes=args.horizon+60))
        if args.cache:save_bundle(args.cache,bundle)
    scales=[float(x) for x in args.session_scales.split(",") if x.strip()]
    if len(scales)!=1 and not args.allow_research_sweep:
        raise SystemExit("Multiple session scales are blocked while experimental configuration is frozen. Use exactly --session-scales 1.0.")
    m1=bundle.get("M1") or []
    required={"bid_o","bid_h","bid_l","bid_c","ask_o","ask_h","ask_l","ask_c"}
    if not m1 or not required.issubset(m1[0]):
        raise SystemExit("Historical cache lacks bid/ask candles. Re-download it; midpoint-only cache is not valid for realistic replay.")
    # Local import keeps downloader broker-read-only and makes research intent explicit.
    os.environ.setdefault("TRADING_ENVIRONMENT","SIMULATION");os.environ.setdefault("AUTO_TRADE","false")
    import server
    variants=[ReplayVariant("V331_BASELINE","V331_BASELINE",0.0)]
    for scale in scales:variants.append(ReplayVariant(f"SESSION_{scale:g}X","SESSION",scale))
    execution=HistoricalExecutionConfig(entry_slippage_pips=args.entry_slippage_pips,exit_slippage_pips=args.exit_slippage_pips,latency_bars=args.latency_bars,require_bid_ask=True)
    validation=ReplayValidationConfig(embargo_minutes=args.embargo_minutes)
    report=replay_history(
        server,bundle,args.instrument,start,end,variants,
        ReplayConfig(
            horizon_bars=args.horizon,
            execution=execution,
            validation=validation,
            save_m1_rejection_shadow=True,
            save_target_population=True,
        )
    )
    report["certification_eligible"]=(len(scales)==1 and scales[0]==1.0)
    report["input_identity"]=(integrity or {"status":"NOT TESTED"})
    with open(args.output,"w",encoding="utf-8") as f:json.dump(report,f,indent=2,default=str)
    print(f"Replay guardado: {args.output}")
    for name,v in report["variants"].items():
        m=v["metrics"]
        h=v["holdout"]["test"]
        wf=v.get("walk_forward",[])
        positive=sum(1 for x in wf if (x["test_metrics"].get("expectancy_r") or 0)>0)
        print(f"{name}: episodes={m['episodes']} expR={m['expectancy_r']:.3f} PF={m['profit_factor']:.3f} | OOS expR={h['expectancy_r']:.3f} PF={h['profit_factor']:.3f} | WF+={positive}/{len(wf)}")
if __name__=="__main__":main()

#!/usr/bin/env python3
"""Compare stored labels with the current cost-aware resolver using an explicit candle bundle.

This is an offline audit only. It never writes to the trading database.
"""
import argparse, sqlite3
import research_evidence
from historical_candles import load_bundle
from historical_replay import CandleStore, _dt


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--db",default="/data/market_alert.db")
    ap.add_argument("--candles",required=True,help="Historical candle bundle containing M1")
    ap.add_argument("--horizon",type=int,default=180)
    ap.add_argument("--cost-pips",type=float,default=1.0)
    args=ap.parse_args()
    store=CandleStore(load_bundle(args.candles))
    c=sqlite3.connect(args.db); c.row_factory=sqlite3.Row
    rows=c.execute("SELECT * FROM learning_samples WHERE label IN (0,1) ORDER BY candle_ts,id").fetchall(); c.close()
    matrix={"WIN->WIN":0,"WIN->LOSS":0,"WIN->OTHER":0,"LOSS->LOSS":0,"LOSS->WIN":0,"LOSS->OTHER":0}
    by_direction={}
    by_rr={"<1.0":{},"1.0-1.5":{},"1.5-2.0":{},">=2.0":{}}
    resolved=0
    for row in rows:
        s=dict(row); ts=s.get("candle_ts") or s.get("created_ts")
        if not ts: continue
        bars=store.future_m1_after(_dt(ts),args.horizon)
        out=research_evidence.resolve_outcome(s,bars,horizon_bars=args.horizon,round_trip_cost_pips=args.cost_pips)
        if not out: continue
        old="WIN" if int(s["label"])==1 else "LOSS"
        new=out.get("status") if out.get("status") in ("WIN","LOSS") else "OTHER"
        matrix[f"{old}->{new}"]+=1; resolved+=1
        d=s.get("direction") or "UNKNOWN"; by_direction.setdefault(d,{})[f"{old}->{new}"]=by_direction.setdefault(d,{}).get(f"{old}->{new}",0)+1
        risk=abs(float(s["entry"])-float(s["stop"])); rr=abs(float(s["target"])-float(s["entry"]))/risk if risk else 0
        band="<1.0" if rr<1 else "1.0-1.5" if rr<1.5 else "1.5-2.0" if rr<2 else ">=2.0"
        by_rr[band][f"{old}->{new}"]=by_rr[band].get(f"{old}->{new}",0)+1
    matches=matrix["WIN->WIN"]+matrix["LOSS->LOSS"]
    print("=== RESOLVER SEMANTICS AUDIT ===")
    print("RESOLVED =",resolved)
    print("MATCH =",matches)
    print("MISMATCH =",resolved-matches)
    print("MATCH_RATE =",None if not resolved else round(matches/resolved*100,2))
    for k,v in matrix.items(): print(k,"=",v)
    print("\nBY_DIRECTION =",by_direction)
    print("BY_NOMINAL_RR =",by_rr)
    print("\nREADING: this quantifies the effect of changed resolution/cost semantics; it does not imply an old-resolver bug.")

if __name__=="__main__": main()

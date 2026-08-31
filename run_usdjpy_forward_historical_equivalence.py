#!/usr/bin/env python3
"""Offline equivalence check for frozen USDJPY_PHASE2_FORWARD_V1.
Consumes only the Phase-1 research package and pre-entry fields already frozen there.
No broker/network/runtime mutation.
"""
from __future__ import annotations
import argparse, hashlib, json, os, tempfile, zipfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from forward_experiment import evaluate_forward_experiment, forward_policy, USDJPY_CHOSEN_LEGACY_SCORE_MIN

EXPECTED_PHASE1_SHA="d4f8bdc49588d10e55a02030039424baf1329190fdc6ec9661dd64b0fdc776d3"
EXPECTED_PHASE2_SHA="623d6ccf4e54a0e150619bb4cd0deced49fb43d1162eb8b7421b135937f89b5e"
EXPECTED_CACHE_SHA="8732fbbbeb987de586f5344934bb6535fca6a68ec44bb9f01154e59394f00964"
NY=ZoneInfo("America/New_York")

def sha(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for b in iter(lambda:f.read(1<<20),b""): h.update(b)
    return h.hexdigest()

def dt(v): return datetime.fromisoformat(str(v).replace("Z","+00:00"))

def time_allowed(r):
    # Canonical Phase-2 research definition: decision after the signal candle closes.
    t=dt(r["candle_ts"])+timedelta(minutes=1)
    n=t.astimezone(NY); m=n.hour*60+n.minute
    return not (420<=m<600 or 900<=m<1140)

def event_end(r):
    return dt(r["exit_ts"]) if r.get("exit_ts") else dt(r["candle_ts"])+timedelta(minutes=240)

def counts(rows):
    out={k:0 for k in ("WIN","LOSS","TIMEOUT","AMBIGUOUS","PENDING")}
    for r in rows: out[r.get("outcome_status") or "PENDING"]+=1
    return out

def gate(r):
    direction=str(r["chosen_signal"]).upper()
    f=r["features"]
    out=evaluate_forward_experiment("USD_JPY",{
        "chosen_direction":direction,
        "legacy_v331_buy_score":float(f["buy_score"]),
        "legacy_v331_sell_score":float(f["sell_score"]),
    })
    expected=float(f["buy_score"] if direction=="BUY" else f["sell_score"])
    assert out["chosen_legacy_score"]==expected
    return out

def partition(rows, keep):
    base=counts(rows); kept=counts([r for r,k in zip(rows,keep) if k])
    return {
        "population":len(rows),
        "win_kept":kept["WIN"],"win_blocked":base["WIN"]-kept["WIN"],
        "loss_kept":kept["LOSS"],"loss_blocked":base["LOSS"]-kept["LOSS"],
        "timeout_kept":kept["TIMEOUT"],"timeout_blocked":base["TIMEOUT"]-kept["TIMEOUT"],
        "ambiguous_kept":kept["AMBIGUOUS"],"pending_kept":kept["PENDING"],
    }

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--phase1",required=True)
    ap.add_argument("--phase2",required=True)
    ap.add_argument("--output",required=True)
    a=ap.parse_args()
    assert sha(a.phase1)==EXPECTED_PHASE1_SHA
    assert sha(a.phase2)==EXPECTED_PHASE2_SHA
    with tempfile.TemporaryDirectory() as td:
        with zipfile.ZipFile(a.phase1) as z: z.extract("USDJPY_M1_ENRICHED_EPISODES.json",td)
        rows=json.load(open(os.path.join(td,"USDJPY_M1_ENRICHED_EPISODES.json")))
    eligible=sorted([r for r in rows if time_allowed(r)],key=lambda r:dt(r["candle_ts"]))
    assert counts(eligible)=={"WIN":76,"LOSS":139,"TIMEOUT":21,"AMBIGUOUS":0,"PENDING":0}
    idx=int(len(eligible)*.60); boundary=dt(eligible[idx]["candle_ts"])
    discovery=[r for r in eligible[:idx] if event_end(r)<boundary]
    holdout=[r for r in eligible[idx:] if dt(r["candle_ts"])>=boundary+timedelta(minutes=30)]
    dm=[bool(gate(r)["ok"]) for r in discovery]
    hm=[bool(gate(r)["ok"]) for r in holdout]
    d=partition(discovery,dm); h=partition(holdout,hm)
    expected_d={"population":141,"win_kept":44,"win_blocked":4,"loss_kept":69,"loss_blocked":10,"timeout_kept":14,"timeout_blocked":0,"ambiguous_kept":0,"pending_kept":0}
    expected_h={"population":93,"win_kept":24,"win_blocked":4,"loss_kept":46,"loss_blocked":12,"timeout_kept":7,"timeout_blocked":0,"ambiguous_kept":0,"pending_kept":0}
    assert d==expected_d, (d,expected_d)
    assert h==expected_h, (h,expected_h)
    policy=forward_policy("USD_JPY")
    assert policy["bypass_m1_confirmation"] is True
    assert policy["bypass_quality_extension"] is True
    assert policy["bypass_low_room_vetoes"] is False
    assert USDJPY_CHOSEN_LEGACY_SCORE_MIN==33.0
    result={
        "status":"PASS",
        "experiment_id":"USDJPY_PHASE2_FORWARD_V1",
        "phase1_package_sha256":EXPECTED_PHASE1_SHA,
        "phase2_package_sha256":EXPECTED_PHASE2_SHA,
        "input_cache_sha256":EXPECTED_CACHE_SHA,
        "instrument":"USD_JPY",
        "rule":"chosen_legacy_score >= 33.0",
        "semantics":"BUY uses legacy BUY score; SELL uses legacy SELL score corresponding to chosen direction",
        "phase1_policy":policy,
        "eligible_counts":counts(eligible),
        "boundary":boundary.isoformat(),
        "embargo_minutes":30,
        "outcome_horizon_minutes":240,
        "discovery":d,
        "holdout":h,
        "look_ahead":False,
        "outcome_used_as_feature":False,
        "mutable_historical_state":"NOT_HISTORICALLY_RECONSTRUCTABLE",
    }
    with open(a.output,"w") as f: json.dump(result,f,indent=2,sort_keys=True)
    print(json.dumps(result,indent=2,sort_keys=True))
if __name__=="__main__": main()

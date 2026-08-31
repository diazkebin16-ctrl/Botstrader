#!/usr/bin/env python3
"""Reproduce frozen EUR/GBP Phase 2 forward gates on historical replay datasets.

Research validation utility only. It has no broker/network/database authority.
Datasets are supplied by path and are not bundled in the candidate package.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from forward_experiment import evaluate_forward_experiment
from legacy_v331_scoring import choose_legacy_v331_direction

EUR_DATASET_SHA256 = "0d848028e64b8f9590100de73c048795857efb4ea7480cbfdf1a91ab49bf4474"
GBP_DATASET_SHA256 = "f7360988947b76743f5abdf594a22ccba22e2b4440684608b9c51809d6a35fef"
EUR_EXCLUDED_PHASE1_LOSS_ID = "f5cb2339fb04a3c5530cb79f6f9104af3e9e40c9fd406a31065a45dc39c562a2"


def sha256(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(1024*1024),b""):
            h.update(chunk)
    return h.hexdigest()


def count(rows: Iterable[Dict[str,Any]]) -> Dict[str,int]:
    rows=list(rows)
    return {"total":len(rows),"win":sum(r["outcome_status"]=="WIN" for r in rows),"loss":sum(r["outcome_status"]=="LOSS" for r in rows)}


def result(rows: List[Dict[str,Any]], evaluator) -> Dict[str,Any]:
    kept=[]; blocked=[]
    for row in rows:
        (kept if evaluator(row)["ok"] else blocked).append(row)
    return {
        "baseline":count(rows),
        "kept":count(kept),
        "blocked":count(blocked),
    }


def eur_features(row: Dict[str,Any]) -> Dict[str,Any]:
    f=row["features"]
    chosen, directional=choose_legacy_v331_direction(f["buy_score"],f["sell_score"])
    return {
        **f,
        "legacy_v331_buy_score":f["buy_score"],
        "legacy_v331_sell_score":f["sell_score"],
        "legacy_v331_chosen_direction":chosen,
        "legacy_v331_directional_score":directional,
    }


def gbp_features(row: Dict[str,Any]) -> Dict[str,Any]:
    f=row["features"]
    return {**f,"legacy_v331_buy_score":f["buy_score"]}


def main() -> int:
    ap=argparse.ArgumentParser()
    ap.add_argument("--eur",required=True)
    ap.add_argument("--gbp",required=True)
    ap.add_argument("--output")
    args=ap.parse_args()
    eur_path=Path(args.eur); gbp_path=Path(args.gbp)
    e_sha=sha256(eur_path); g_sha=sha256(gbp_path)
    if e_sha!=EUR_DATASET_SHA256: raise SystemExit(f"EUR SHA mismatch: {e_sha}")
    if g_sha!=GBP_DATASET_SHA256: raise SystemExit(f"GBP SHA mismatch: {g_sha}")
    E=json.load(eur_path.open()); G=json.load(gbp_path.open())
    if E.get("instrument")!="EUR_USD": raise SystemExit("EUR instrument mismatch")
    if G.get("instrument")!="GBP_USD": raise SystemExit("GBP instrument mismatch")

    eur=[r for r in E["variants"]["V331_BASELINE"]["m1_rejection_shadow"]["episodes"] if r.get("outcome_status") in {"WIN","LOSS"}]
    eur=[r for r in eur if (r.get("shadow") or {}).get("opportunity_id")!=EUR_EXCLUDED_PHASE1_LOSS_ID]
    ids=[(r.get("shadow") or {}).get("opportunity_id") for r in eur]
    if len(ids)!=len(set(ids)): raise SystemExit("EUR duplicate opportunity_id")
    chosen_mismatch=[]
    for r in eur:
        f=r["features"]; chosen,_=choose_legacy_v331_direction(f["buy_score"],f["sell_score"])
        if chosen!=r.get("chosen_signal"): chosen_mismatch.append((r.get("candle_ts"),chosen,r.get("chosen_signal")))
    if chosen_mismatch: raise SystemExit(f"EUR chosen-direction mismatch: {chosen_mismatch[:3]}")

    eur_disc=[r for r in eur if "2026-07-29T05:27:00+00:00"<=r["candle_ts"]<="2026-08-18T01:27:00+00:00"]
    eur_hold=[r for r in eur if "2026-08-18T08:19:00+00:00"<=r["candle_ts"]<="2026-08-28T14:34:00+00:00"]
    eur_eval=lambda r:evaluate_forward_experiment("EUR_USD",eur_features(r))
    eur_d=result(eur_disc,eur_eval); eur_h=result(eur_hold,eur_eval)

    gbp=[r for r in G["variants"]["V331_BASELINE"]["m1_rejection_shadow"]["episodes"] if r.get("outcome_status") in {"WIN","LOSS"}]
    gbp_disc=[r for r in gbp if "2026-07-29T00:59:00+00:00"<=r["candle_ts"]<="2026-08-18T00:58:59+00:00"]
    gbp_hold=[r for r in gbp if "2026-08-18T00:59:00+00:00"<=r["candle_ts"]<="2026-08-28T18:59:00+00:00"]
    gbp_eval=lambda r:evaluate_forward_experiment("GBP_USD",gbp_features(r))
    gbp_d=result(gbp_disc,gbp_eval); gbp_h=result(gbp_hold,gbp_eval)

    expected={
      "eur":{"discovery":{"kept":{"win":27,"loss":69},"blocked":{"win":0,"loss":10}},
             "holdout":{"kept":{"win":14,"loss":30},"blocked":{"win":2,"loss":7}}},
      "gbp":{"discovery":{"kept":{"win":20,"loss":43},"blocked":{"win":5,"loss":50}},
             "holdout":{"kept":{"win":22,"loss":38},"blocked":{"win":4,"loss":22}}},
    }
    actual={"eur":{"discovery":eur_d,"holdout":eur_h},"gbp":{"discovery":gbp_d,"holdout":gbp_h}}
    checks=[]
    for asset in ("eur","gbp"):
        for split in ("discovery","holdout"):
            for disposition in ("kept","blocked"):
                for outcome in ("win","loss"):
                    a=actual[asset][split][disposition][outcome]
                    e=expected[asset][split][disposition][outcome]
                    checks.append({"field":f"{asset}.{split}.{disposition}.{outcome}","actual":a,"expected":e,"pass":a==e})
    passed=all(c["pass"] for c in checks)
    evidence={
      "status":"PASS" if passed else "FAIL",
      "look_ahead":False,
      "outcome_used_as_feature":False,
      "datasets":{"EUR_USD":{"sha256":e_sha},"GBP_USD":{"sha256":g_sha}},
      "eur_phase2_population":{"total":len(eur),"win":sum(r["outcome_status"]=="WIN" for r in eur),"loss":sum(r["outcome_status"]=="LOSS" for r in eur),"duplicate_opportunity_ids":0},
      "actual":actual,
      "expected":expected,
      "checks":checks,
    }
    text=json.dumps(evidence,indent=2,sort_keys=True)
    print(text)
    if args.output: Path(args.output).write_text(text+"\n")
    return 0 if passed else 2

if __name__=="__main__": raise SystemExit(main())

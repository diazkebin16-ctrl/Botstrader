#!/usr/bin/env python3
import argparse, json, sqlite3
from pathlib import Path
from forward_audit import filter_overlap_audit, evidence_class

FILTERS = ["minimum_rr", "barrier_room_ok", "low_room_low_rr", "low_room_extended"]


def load_rows(db_path: str):
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    rows = c.execute("""
        SELECT d.id,d.ts,d.candle_ts,d.instrument,d.signal,d.executed,d.reason,d.forward_audit_json,
               ls.label
        FROM decision_log d
        LEFT JOIN signals s
          ON s.instrument=d.instrument AND s.candle_ts=d.candle_ts AND s.signal=d.signal
        LEFT JOIN learning_samples ls ON ls.signal_id=s.id
        WHERE d.forward_audit_json IS NOT NULL AND d.forward_audit_json <> '{}'
        GROUP BY d.id
        ORDER BY d.id
    """).fetchall()
    c.close()
    out=[]
    for row in rows:
        try: audit=json.loads(row["forward_audit_json"] or "{}")
        except Exception: continue
        if not audit.get("vetoes"): continue
        audit["label"]=row["label"]
        audit["decision_id"]=row["id"]
        audit["ts"]=row["candle_ts"] or row["ts"]
        audit["signal"]=row["signal"]
        out.append(audit)
    return out


def main():
    ap=argparse.ArgumentParser(description="Order-independent forward filter stacking audit")
    ap.add_argument("--db",default="/data/market_alert.db")
    args=ap.parse_args()
    rows=load_rows(args.db)
    report=filter_overlap_audit(rows,FILTERS)
    print("=== FILTER STACKING AUDIT ===")
    print("POPULATION =",report["population"])
    print("COMBINED_VETO =",report["combined_veto"])
    for name in FILTERS:
        x=report["filters"][name]
        exclusive=[r for r in rows if r["vetoes"].get(name) and not any(r["vetoes"].get(o) for o in FILTERS if o!=name)]
        labeled=[r for r in exclusive if r.get("label") in (0,1)]
        wins=sum(int(r["label"]==1) for r in labeled)
        losses=sum(int(r["label"]==0) for r in labeled)
        wr=(wins/len(labeled)) if labeled else None
        print(f"{name}: TOTAL={x['veto_total']} TRUE_UNIQUE={x['true_unique_veto']} REMOVE_ONE_DELTA={x['remove_one_delta']} EVIDENCE={evidence_class(len(labeled))} LABELED_N={len(labeled)} WIN={wins} LOSS={losses} WR={None if wr is None else round(wr*100,2)}")
    print("\n=== PAIR OVERLAP ===")
    for p in report["pairs"]:
        print(f"{p['filter_a']} x {p['filter_b']}: INTER={p['intersection']} UNION={p['union']} JACCARD={None if p['jaccard'] is None else round(p['jaccard'],4)} A_OVERLAP={None if p['overlap_of_a'] is None else round(p['overlap_of_a'],4)} B_OVERLAP={None if p['overlap_of_b'] is None else round(p['overlap_of_b'],4)}")
    print("\nNOTE: High Jaccard indicates redundancy, not that a veto is wrong or should be relaxed.")

if __name__=="__main__": main()

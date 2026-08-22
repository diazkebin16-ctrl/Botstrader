#!/usr/bin/env python3
"""Read-only CLI for v3.33 research validation."""
from __future__ import annotations
import argparse
import json
import sqlite3
from pathlib import Path
from research_validation import validate_trade_memory_session_policy


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--db", default="/data/market_alert.db")
    ap.add_argument("--output", default="research_validation_v3.33.json")
    args=ap.parse_args()
    db=Path(args.db)
    if not db.exists():
        raise SystemExit(f"database not found: {db}")
    c=sqlite3.connect(str(db)); c.row_factory=sqlite3.Row
    rows=[dict(x) for x in c.execute("""SELECT * FROM trade_memory
                                        WHERE status='CLOSED' AND realized_r IS NOT NULL
                                        ORDER BY entry_ts,id""").fetchall()]
    c.close()
    report=validate_trade_memory_session_policy(rows)
    Path(args.output).write_text(json.dumps(report,indent=2,default=str)+"\n")
    print(json.dumps({k:report.get(k) for k in ("status","method","scope","raw_closed_trades","independent_episodes","baseline")},indent=2,default=str))
    print(f"report: {args.output}")

if __name__=="__main__":
    main()

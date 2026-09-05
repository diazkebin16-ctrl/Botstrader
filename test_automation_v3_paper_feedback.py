import json
import os
import sqlite3
import tempfile
from pathlib import Path

from deployment_runtime import DeploymentManager
from managed_strategy_rules import non_v3_managed_strategy_identity


def ident(candidate,release,instrument="GBP_USD",confidence="STANDARD",experimental=False):
    return {"active":True,"instrument":instrument,"v3_candidate_id":candidate,
            "v3_candidate_definition_sha256":"a"*64,"v3_confidence_class":confidence,
            "v3_experimental":experimental,"v3_paper_only":True,"v3_managed_release_identity":release,
            "v3_source_code_sha":"b"*40,"production_authority":False}


def manager(db):
    m=DeploymentManager(db,"https://example.invalid","","",False,["GBP_USD","EUR_USD"],["BULL_TREND","BEAR_TREND","RANGE"],
                        max_drawdown=.02,max_consecutive_losses=3,base_risk_fraction=.005)
    m.ensure_schema();return m


def schema(db):
    c=sqlite3.connect(db);c.execute("""CREATE TABLE IF NOT EXISTS trade_memory(
      id INTEGER PRIMARY KEY AUTOINCREMENT,trade_id TEXT UNIQUE,status TEXT,symbol TEXT,entry_context_json TEXT,
      exit_context_json TEXT,exit_reasons_json TEXT,realized_r REAL,net_result REAL,exit_ts TEXT)""");c.commit();c.close()


def closed(db,tid,snapshot,rr,outcome=None):
    c=sqlite3.connect(db);ctx={} if outcome is None else {"outcome":outcome}
    c.execute("""INSERT INTO trade_memory(trade_id,status,symbol,entry_context_json,exit_context_json,exit_reasons_json,realized_r,net_result,exit_ts)
      VALUES(?,?,?,?,?,?,?,?,?)""",(tid,"CLOSED",snapshot.get("instrument","GBP_USD"),json.dumps({"v3_managed_strategy":snapshot}),json.dumps(ctx),"[]",rr,rr,"2026-09-05T00:00:00+00:00"));c.commit();c.close()


def run():
    db=tempfile.mktemp(suffix=".db");schema(db);m=manager(db)
    a=ident("A","v3paper_"+"1"*64,confidence="EXPERIMENTAL",experimental=True)
    b=ident("B","v3paper_"+"2"*64)
    eur=ident("E","v3paper_"+"3"*64,instrument="EUR_USD")
    ar=m.register_managed_paper(a,True);assert ar["confidence_class"]=="EXPERIMENTAL" and ar["experimental"]==1 and ar["paper_only"]==1 and ar["production_authority"]==0
    br=m.register_managed_paper(b,True);assert br["confidence_class"]=="STANDARD" and br["experimental"]==0 and br["production_authority"]==0
    assert m.managed_paper_release(a["v3_managed_release_identity"])["lifecycle_state"]=="PAPER_RETIRED"

    # A opened before B: immutable A snapshot wins at close; B is untouched.
    closed(db,"T_A",a,1.5);r=m.reconcile_managed_paper_trade_memory();assert r["attributed"]==1
    assert m.managed_paper_metrics(a["v3_managed_release_identity"])["outcomes"]["WIN"]==1
    assert m.managed_paper_metrics(b["v3_managed_release_identity"])["results"]==0
    assert m.reconcile_managed_paper_trade_memory()["processed"]==0

    closed(db,"T_B",b,-1.0);m.reconcile_managed_paper_trade_memory();m.reconcile_managed_paper_trade_memory()
    assert m.managed_paper_metrics(b["v3_managed_release_identity"])["outcomes"]["LOSS"]==1
    closed(db,"T_TIMEOUT",b,0.0,"TIMEOUT");closed(db,"T_AMBIG",b,0.0,"AMBIGUOUS");m.reconcile_managed_paper_trade_memory()
    bm=m.managed_paper_metrics(b["v3_managed_release_identity"]);assert bm["outcomes"]["TIMEOUT"]==1 and bm["outcomes"]["AMBIGUOUS"]==1 and bm["binary_resolved"]==1

    # Restart retains metadata/results and duplicate close cannot double-count.
    m2=manager(db);assert m2.managed_paper_release(b["v3_managed_release_identity"])["confidence_class"]=="STANDARD"
    assert m2.reconcile_managed_paper_trade_memory()["processed"]==0

    # Existing consecutive-loss threshold degrades exact release only and blocks NEW entries.
    c=ident("C","v3paper_"+"4"*64);m2.register_managed_paper(c,True)
    for i in range(3):m2.ingest_managed_paper_result(c,f"C{i}","LOSS",-1.0)
    cr=m2.managed_paper_release(c["v3_managed_release_identity"]);assert cr["lifecycle_state"]=="PAPER_DEGRADED" and cr["new_trades_enabled"]==0
    assert not m2.managed_paper_entry_gate(c)["allow"]
    assert m2.managed_paper_entry_gate(eur)["allow"]

    # Existing kill-switch table blocks exact candidate; no open-position mutation exists in this gate.
    d=ident("D","v3paper_"+"5"*64);assert m2.managed_paper_entry_gate(d)["allow"]
    m2.set_kill("CANDIDATE:D",True,"test","unit");assert not m2.managed_paper_entry_gate(d)["allow"]

    # Legacy row with no V3 snapshot is never guessed/backfilled.
    cdb=sqlite3.connect(db);cdb.execute("INSERT INTO trade_memory(trade_id,status,symbol,entry_context_json,exit_context_json,exit_reasons_json,realized_r,net_result,exit_ts) VALUES('OLD','CLOSED','GBP_USD','{}','{}','[]',1,1,'x')");cdb.commit();cdb.close()
    m2.reconcile_managed_paper_trade_memory();cdb=sqlite3.connect(db);assert cdb.execute("SELECT COUNT(*) FROM deployment_managed_paper_feedback WHERE trade_id='OLD'").fetchone()[0]==0;cdb.close()
    non=non_v3_managed_strategy_identity("GBP_USD");assert non["active"] is False and non["v3_candidate_id"] is None
    bad=dict(eur);bad["production_authority"]=True
    try:m2.managed_paper_entry_gate(bad);raise AssertionError("production authority accepted")
    except ValueError:pass

    src=Path("server.py").read_text(encoding="utf-8")
    assert '"v3_managed_strategy":dict(r.get("v3_managed_strategy")' in src
    assert 'deployment_manager.managed_paper_entry_gate(v3_identity)' in src
    assert 'deployment_manager.reconcile_managed_paper_trade_memory' in src
    assert Path("forward_experiment.py").is_file()
    os.remove(db)


def test_v3_paper_feedback_loop():
    run()


if __name__=="__main__":
    run();print("automation v3 paper feedback integration tests: OK")

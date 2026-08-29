import json
import sqlite3
from pathlib import Path

import pytest

import server
from counterfactual_tracker import CounterfactualTracker, evidence_grade
from instrument_profiles import instrument_profile
from opportunity_ranker import rank_opportunities
from slot_allocator import allocate_slots


def cand(inst, score, *, signal_id=1, t="2026-08-29T12:00:00Z", entry=1.1000, stop=1.0900, target=1.1100):
    return {"instrument":inst,"signal":"BUY","score":score,"dynamic_confidence":score/100,
            "rr_raw":abs(target-entry)/abs(entry-stop),"room_to_barrier_r":1.5,"spread_pips":1.0,
            "batch_selection_eligible":True,"entry":entry,"stop":stop,"target":target,"candle_ts":t,
            "setup_variant":"S","_batch_context":{"signal_id":signal_id,"director_id":f"d{signal_id}"}}


def tracker(tmp_path,horizon=3):
    return CounterfactualTracker(str(tmp_path/"cf.db"),horizon)


def record(tr,candidate,cycle="c1",rank=2,winner=None,reason="NO_SLOT"):
    winner=winner or {"instrument":"EUR_USD","rank":1,"rank_score":.91,"signal_id":99}
    return tr.record_selection_rejected(cycle_id=cycle,candidate=candidate,rank=rank,rank_score=.88,
        components={"signal_quality":.8,"confidence":.8,"rr_quality":.5,"room_quality":.5,"cost_quality":.8},
        slot_capacity=1,slots_available=1,cycle_size=3,winner=winner,rejection_reason=reason)


def test_counterfactual_idempotency_restart(tmp_path):
    tr=tracker(tmp_path); c=cand("USD_JPY",88,signal_id=2,entry=150,stop=149,target=151)
    a=record(tr,c); b=record(tr,c)
    tr2=tracker(tmp_path)
    rows=tr2.open_for_instrument("USD_JPY")
    assert a["created"] is True and b["created"] is False
    assert len(rows)==1 and rows[0]["counterfactual_id"]==a["counterfactual"]["counterfactual_id"]


def test_win_loss_and_target_r_exact(tmp_path):
    tr=tracker(tmp_path,3)
    record(tr,cand("GBP_USD",80,signal_id=2,entry=1.10,stop=1.09,target=1.12))
    record(tr,cand("AUD_USD",79,signal_id=3,entry=.70,stop=.69,target=.705),cycle="c2")
    assert tr.resolve_open("GBP_USD",[{"t":"2026-08-29T12:01:00Z","h":1.121,"l":1.099}])==1
    assert tr.resolve_open("AUD_USD",[{"t":"2026-08-29T12:01:00Z","h":.701,"l":.689}])==1
    c=tr.conn();g=dict(c.execute("select * from counterfactual_opportunities where instrument='GBP_USD'").fetchone());a=dict(c.execute("select * from counterfactual_opportunities where instrument='AUD_USD'").fetchone());c.close()
    assert g["status"]=="WIN" and g["result_r"]==pytest.approx(2.0)
    assert a["status"]=="LOSS" and a["result_r"]==-1.0


def test_same_bar_stop_and_target_is_ambiguous(tmp_path):
    tr=tracker(tmp_path);record(tr,cand("USD_CAD",80,signal_id=4,entry=1.35,stop=1.34,target=1.36))
    tr.resolve_open("USD_CAD",[{"t":"2026-08-29T12:01:00Z","h":1.361,"l":1.339}])
    row=tr.open_for_instrument("USD_CAD")
    assert row==[]
    c=tr.conn();r=dict(c.execute("select * from counterfactual_opportunities").fetchone());c.close()
    assert r["status"]=="AMBIGUOUS" and r["result_r"] is None


def test_no_lookahead_ignores_prior_and_entry_timestamp_bar(tmp_path):
    tr=tracker(tmp_path,3);record(tr,cand("AUD_USD",80,signal_id=5,entry=.70,stop=.69,target=.71))
    candles=[
      {"t":"2026-08-29T11:59:00Z","h":.72,"l":.68},
      {"t":"2026-08-29T12:00:00Z","h":.72,"l":.68},
      {"t":"2026-08-29T12:01:00Z","h":.705,"l":.695},
      {"t":"2026-08-29T12:02:00Z","h":.711,"l":.695},
    ]
    tr.resolve_open("AUD_USD",candles)
    c=tr.conn();r=dict(c.execute("select * from counterfactual_opportunities").fetchone());c.close()
    assert r["status"]=="WIN" and r["bars_observed"]==2
    assert json.loads(r["pre_entry_json"])["look_ahead"] is False


def test_timeout_is_not_loss(tmp_path):
    tr=tracker(tmp_path,2);record(tr,cand("GBP_USD",80,signal_id=6))
    tr.resolve_open("GBP_USD",[
      {"t":"2026-08-29T12:01:00Z","h":1.105,"l":1.095},
      {"t":"2026-08-29T12:02:00Z","h":1.106,"l":1.094},
    ])
    c=tr.conn();r=dict(c.execute("select * from counterfactual_opportunities").fetchone());c.close()
    assert r["status"]=="TIMEOUT" and r["result_r"] is None


def test_selection_vs_safety_rejection_separation(tmp_path):
    tr=tracker(tmp_path)
    with pytest.raises(ValueError):
        tr.record_selection_rejected(cycle_id="c",candidate=cand("GBP_USD",80),rank=2,rank_score=.8,components={},slot_capacity=1,slots_available=1,cycle_size=2,winner={},rejection_reason="PORTFOLIO_RISK")
    tr.record_non_counterfactual_rejection(cycle_id="c",instrument="GBP_USD",reason="CORRELATION",category="SAFETY_REJECTED")
    c=tr.conn();assert c.execute("select count(*) n from counterfactual_opportunities").fetchone()["n"]==0
    assert c.execute("select detail_json from counterfactual_tracker_events").fetchone() is not None;c.close()


def _trade_memory_schema(tr):
    c=tr.conn();c.execute("""CREATE TABLE IF NOT EXISTS trade_memory(id INTEGER PRIMARY KEY AUTOINCREMENT,trade_id TEXT UNIQUE,signal_id INTEGER,strategy TEXT,symbol TEXT,direction TEXT,status TEXT,entry_ts TEXT,exit_ts TEXT,entry_price REAL,position_size REAL,realized_r REAL)""");c.commit();c.close()


def test_selector_regret_and_head_to_head(tmp_path):
    tr=tracker(tmp_path);_trade_memory_schema(tr)
    winner={"instrument":"EUR_USD","rank":1,"rank_score":.91,"signal_id":1,"trade_id":"t1"}
    record(tr,cand("USD_JPY",88,signal_id=2,entry=150,stop=149,target=151),winner=winner)
    c=tr.conn();c.execute("insert into trade_memory(trade_id,signal_id,strategy,symbol,direction,status,entry_ts,position_size,realized_r) values('t1',1,'S','EUR_USD','BUY','CLOSED','2026-08-29T12:00:00Z',100,-1.0)");c.commit();c.close()
    tr.resolve_open("USD_JPY",[{"t":"2026-08-29T12:01:00Z","h":151.1,"l":149.5}])
    row=tr.compare_selector_decisions("c1")[0]
    assert row["regret_R"]==pytest.approx(2.0) and row["selector_outcome"]=="WRONG"
    h=tr.head_to_head_report("EUR_USD","USD_JPY")
    assert h["times_competed"]==1 and h["selector_wrong"]==1 and h["average_regret_R"]==pytest.approx(2.0)


def test_multi_candidate_cycle_reconstructs_correct_and_wrong(tmp_path):
    tr=tracker(tmp_path);_trade_memory_schema(tr)
    winner={"instrument":"EUR_USD","rank":1,"rank_score":.91,"signal_id":1,"trade_id":"eur1"}
    record(tr,cand("USD_JPY",88,signal_id=2,entry=150,stop=149,target=151),winner=winner,rank=2)
    record(tr,cand("GBP_USD",83,signal_id=3),winner=winner,rank=3)
    c=tr.conn();c.execute("insert into trade_memory(trade_id,signal_id,strategy,symbol,direction,status,entry_ts,position_size,realized_r) values('eur1',1,'S','EUR_USD','BUY','CLOSED','2026-08-29T12:00:00Z',100,-1.0)");c.commit();c.close()
    tr.resolve_open("USD_JPY",[{"t":"2026-08-29T12:01:00Z","h":151.1,"l":149.5}])
    tr.resolve_open("GBP_USD",[{"t":"2026-08-29T12:01:00Z","h":1.105,"l":1.089}])
    rows=tr.compare_selector_decisions("c1")
    by={x["rejected_instrument"]:x for x in rows}
    assert by["USD_JPY"]["selector_outcome"]=="WRONG"
    assert by["GBP_USD"]["selector_outcome"]=="TIE"  # both -1R


def test_two_slot_rejected_candidates_can_be_recorded_independently(tmp_path):
    tr=tracker(tmp_path)
    winner={"instrument":"AUD_USD","rank":1,"rank_score":.95,"signal_id":1}
    record(tr,cand("USD_JPY",90,signal_id=3,entry=150,stop=149,target=151),cycle="two",rank=3,winner=winner)
    record(tr,cand("EUR_USD",85,signal_id=4),cycle="two",rank=4,winner=winner)
    c=tr.conn();rows=c.execute("select instrument,rank,status from counterfactual_opportunities where cycle_id='two' order by rank").fetchall();c.close()
    assert [(x["instrument"],x["rank"],x["status"]) for x in rows]==[("USD_JPY",3,"OPEN"),("EUR_USD",4,"OPEN")]


def test_reliability_report_keeps_executed_and_shadow_separate_and_grades_n20(tmp_path):
    tr=tracker(tmp_path);_trade_memory_schema(tr)
    c=tr.conn()
    for i in range(20):
        c.execute("insert into trade_memory(trade_id,signal_id,strategy,symbol,direction,status,entry_ts,position_size,realized_r) values(?,?,?,?,?,?,?,?,?)",
                  (f'e{i}',i,'S','EUR_USD','BUY','CLOSED',f'2026-08-29T10:{i:02d}:00Z',100,1.0 if i<10 else -1.0))
    c.commit();c.close()
    for i in range(20):
        r=record(tr,cand("USD_JPY",80,signal_id=100+i,t=f"2026-08-29T12:{i:02d}:00Z",entry=150,stop=149,target=151),cycle=f'j{i}')
        row=r["counterfactual"]
        tr._terminal(row,"WIN" if i<12 else "LOSS",1.0 if i<12 else -1.0,1,"2026-08-29T13:00:00Z","synthetic")
    eur=tr.instrument_reliability_report("EUR_USD");jpy=tr.instrument_reliability_report("USD_JPY")
    assert eur["executed_win_rate"]==pytest.approx(.5) and eur["shadow_count"]==0
    assert jpy["shadow_win_rate"]==pytest.approx(.6) and jpy["executed_count"]==0
    assert eur["executed"]["evidence_grade"]=="WEAK_LIMITED_EVIDENCE"
    assert jpy["shadow"]["evidence_grade"]=="WEAK_LIMITED_EVIDENCE"
    assert evidence_grade(14)=="UNDERPOWERED" and evidence_grade(30)=="USABLE"


def test_tracker_statistics_have_no_ranking_or_selection_authority(tmp_path):
    rows=[cand("EUR_USD",91),cand("USD_JPY",88,signal_id=2,entry=150,stop=149,target=151),cand("AUD_USD",85,signal_id=3,entry=.7,stop=.69,target=.71)]
    before=[(x.instrument,x.rank_score) for x in rank_opportunities(rows)]
    selected_before=[x["instrument"] for x in allocate_slots(rows,nlv=3000,broker_guard=lambda *_:{"allow":True},portfolio_guard=lambda *_:{"allow":True})["selected"]]
    tr=tracker(tmp_path);_trade_memory_schema(tr)
    # Extreme shadow evidence intentionally conflicts with productive ranking.
    for i in range(30):
        r=record(tr,cand("USD_JPY",80,signal_id=200+i,t=f"2026-08-{1+i//24:02d}T{(i%24):02d}:00:00Z",entry=150,stop=149,target=151),cycle=f'x{i}')
        tr._terminal(r["counterfactual"],"WIN",1.0,1,"2026-09-01T00:00:00Z","synthetic")
    after=[(x.instrument,x.rank_score) for x in rank_opportunities(rows)]
    selected_after=[x["instrument"] for x in allocate_slots(rows,nlv=3000,broker_guard=lambda *_:{"allow":True},portfolio_guard=lambda *_:{"allow":True})["selected"]]
    assert before==after and selected_before==selected_after
    assert CounterfactualTracker.execution_authority is False and CounterfactualTracker.research_authority is False


def test_five_pair_profiles_and_ibkr_authority_unchanged():
    for inst in ("EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD"):
        assert instrument_profile(inst).paper_execution_allowed is True
    for inst in ("GBP_USD","USD_JPY","AUD_USD","USD_CAD"):
        assert instrument_profile(inst).live_execution_allowed is False
    from broker_risk import IbkrBrokerRiskAdapter
    assert IbkrBrokerRiskAdapter.execution_authority is False


@pytest.mark.asyncio
async def test_single_position_guard_shadow_rows_do_not_count_as_positions(tmp_path,monkeypatch):
    tr=tracker(tmp_path);record(tr,cand("EUR_USD",80,signal_id=8))
    monkeypatch.setattr(server,"SINGLE",True)
    monkeypatch.setattr(server,"market_is_weekend_closed",lambda:False)
    monkeypatch.setattr(server,"new_entry_time_gate",lambda:{"allowed":True,"reason":"OK"})
    async def haspos(client,inst): return True
    monkeypatch.setattr(server,"haspos",haspos)
    r=cand("EUR_USD",90);r["portfolio_execution_guard"]={"allow":True};r["broker_risk_context"]={"nav":3000}
    out=await server.execute(object(),r)
    assert out["skipped"]=="existing_position"
    # Counterfactual DB evidence is never supplied to productive position state.
    assert len(tr.open_for_instrument("EUR_USD"))==1


def test_cycle_hook_persists_only_safe_no_slot_and_links_winner(tmp_path,monkeypatch):
    monkeypatch.setattr(server,"DB",str(tmp_path/"server.db"))
    server._COUNTERFACTUAL_TRACKERS.clear()
    rows=[cand("EUR_USD",91,signal_id=1),cand("USD_JPY",88,signal_id=2,entry=150,stop=149,target=151),cand("GBP_USD",83,signal_id=3)]
    ranked=rank_opportunities(rows)
    selected={**ranked[0].candidate,"opportunity_rank_score":ranked[0].rank_score}
    allocation={"selected":[selected],"rejected":[
        {"instrument":"USD_JPY","reason":"NO_SLOT_AVAILABLE"},
        {"instrument":"GBP_USD","reason":"GLOBAL_PORTFOLIO_RISK_GUARD"},
    ]}
    cycle={"cycle_id":"hook","max_slots":1,"slots_available":1}
    monkeypatch.setattr(server,"_shadow_candidate_safety",lambda candidate,ctx:{"safe":candidate["instrument"]=="USD_JPY","reason":"PORTFOLIO_RISK"})
    out=server._record_counterfactual_cycle(cycle,ranked,allocation,
        [{"executed":True,"instrument":"EUR_USD","trade_id":"trade-eur","intent":{"execution_intent_id":"intent-eur"}}],{})
    assert out["created"]==1
    tr=server.counterfactual_tracker();c=tr.conn();cf=dict(c.execute("select * from counterfactual_opportunities").fetchone());events=[json.loads(x["detail_json"]) for x in c.execute("select detail_json from counterfactual_tracker_events where event_type='NON_COUNTERFACTUAL_REJECTION'").fetchall()];c.close()
    assert cf["instrument"]=="USD_JPY" and cf["winner_instrument"]=="EUR_USD"
    assert cf["winner_trade_id"]=="trade-eur" and cf["winner_intent_id"]=="intent-eur"
    assert any(x.get("instrument")=="GBP_USD" for x in events)

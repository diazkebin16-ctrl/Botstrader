import asyncio
from copy import deepcopy

import pytest

import server
from broker_risk import IbkrBrokerRiskAdapter, OandaBrokerRiskAdapter
from instrument_profiles import instrument_profile
from instrument_registry import InstrumentRegistry
from opportunity_ranker import rank_opportunities, opportunity_rank_score
from slot_allocator import allocate_slots, slot_policy


def c(inst, score, conf=None, rr=1.5, room=1.5, spread=1.0):
    return {
        "instrument": inst,
        "signal": "BUY",
        "score": score,
        "dynamic_confidence": conf if conf is not None else score / 100,
        "rr_raw": rr,
        "room_to_barrier_r": room,
        "spread_pips": spread,
        "batch_selection_eligible": True,
        "entry": 1.1,
        "stop": 1.09,
        "target": 1.12,
    }


def all5():
    return [
        c("EUR_USD",82), c("GBP_USD",91), c("USD_JPY",87),
        c("AUD_USD",85), c("USD_CAD",84),
    ]


def allow(*_): return {"allow": True}


def test_a_five_opportunities_one_slot_best_wins():
    out=allocate_slots(all5(),nlv=3000,broker_guard=allow,portfolio_guard=allow)
    assert [x["instrument"] for x in out["selected"]] == ["GBP_USD"]


def test_b_best_candidate_last_in_scan_still_wins():
    rows=[c("EUR_USD",80),c("GBP_USD",81),c("USD_JPY",82),c("AUD_USD",83),c("USD_CAD",99)]
    out=allocate_slots(rows,nlv=3000,broker_guard=allow,portfolio_guard=allow)
    assert out["selected"][0]["instrument"] == "USD_CAD"


def test_c_tie_score_is_deterministic_and_not_input_order():
    a=[c("USD_JPY",90),c("EUR_USD",90),c("GBP_USD",90)]
    b=list(reversed(a))
    assert [x.instrument for x in rank_opportunities(a)] == [x.instrument for x in rank_opportunities(b)]
    assert [x.instrument for x in rank_opportunities(a)] == ["EUR_USD","GBP_USD","USD_JPY"]


def test_d_five_opportunities_two_slots_best_compatible():
    out=allocate_slots(all5(),nlv=5000,broker_guard=allow,portfolio_guard=allow)
    assert [x["instrument"] for x in out["selected"]] == ["GBP_USD","USD_JPY"]


def test_e_second_rank_incompatible_selects_next_compatible():
    rows=[c("GBP_USD",95),c("EUR_USD",92),c("AUD_USD",88)]
    def portfolio(candidate,selected):
        if selected and candidate["instrument"]=="EUR_USD":
            return {"allow":False,"reasons":["CORRELATED_POSITION_LIMIT"]}
        return {"allow":True}
    out=allocate_slots(rows,nlv=5000,broker_guard=allow,portfolio_guard=portfolio)
    assert [x["instrument"] for x in out["selected"]] == ["GBP_USD","AUD_USD"]
    assert any(x["instrument"]=="EUR_USD" and x["reason"]=="PORTFOLIO_RISK_REJECTED" for x in out["rejected"])


def test_f_one_valid_with_two_slots_opens_only_one():
    out=allocate_slots([c("EUR_USD",90)],nlv=5000,broker_guard=allow,portfolio_guard=allow)
    assert len(out["selected"]) == 1


def test_g_none_valid_means_zero():
    assert allocate_slots([],nlv=5000,broker_guard=allow,portfolio_guard=allow)["selected"] == []


@pytest.mark.parametrize("nlv,slots",[(2999,1),(3000,1),(4999,1),(5000,2),(10000,2)])
def test_h_i_j_capital_tiers(nlv,slots):
    assert slot_policy(nlv)["max_slots"] == slots


def test_k_broker_margin_insufficient_rejects_candidate():
    adapter=OandaBrokerRiskAdapter()
    verdict=adapter.prospective_check(c("EUR_USD",90),[],{
        "environment":"PAPER","instrument_execution_allowed":True,
        "secondary_instrument":False,"metadata_verified":True,"available_margin_ok":False,
    })
    assert verdict.allow is False
    assert "BROKER_MARGIN_INSUFFICIENT" in verdict.reasons


def test_l_secondary_broker_metadata_unverified_fails_closed():
    adapter=OandaBrokerRiskAdapter()
    verdict=adapter.prospective_check(c("GBP_USD",90),[],{
        "environment":"PAPER","instrument_execution_allowed":True,
        "secondary_instrument":True,"metadata_verified":False,"available_margin_ok":True,
    })
    assert verdict.allow is False
    assert "BROKER_METADATA_UNVERIFIED" in verdict.reasons


def test_m_n_ibkr_adapter_is_inactive_and_never_executes_live_or_paper():
    adapter=IbkrBrokerRiskAdapter()
    assert adapter.execution_authority is False
    for env in ("PAPER","PRODUCTION","LIVE"):
        verdict=adapter.prospective_check(c("EUR_USD",90),[],{"environment":env,"NetLiquidation":100000})
        assert verdict.allow is False
        assert "IBKR_EXECUTION_AUTHORITY_FALSE" in verdict.reasons


def test_o_aud_cad_are_oanda_paper_enabled_but_live_denied():
    assert instrument_profile("EUR_USD").paper_execution_allowed is True
    assert instrument_profile("GBP_USD").paper_execution_allowed is True
    assert instrument_profile("USD_JPY").paper_execution_allowed is True
    assert instrument_profile("AUD_USD").paper_execution_allowed is True
    assert instrument_profile("USD_CAD").paper_execution_allowed is True
    assert instrument_profile("AUD_USD").live_execution_allowed is False
    assert instrument_profile("USD_CAD").live_execution_allowed is False



def test_five_pair_registry_has_conservative_analysis_metadata():
    registry=InstrumentRegistry()
    assert registry.get("AUD_USD").pip_size == pytest.approx(0.0001)
    assert registry.get("USD_CAD").pip_size == pytest.approx(0.0001)
    assert registry.get("USD_JPY").pip_size == pytest.approx(0.01)
    assert registry.get("AUD_USD").source == "FALLBACK"
    assert registry.get("USD_CAD").source == "FALLBACK"

def test_p_research_fields_cannot_change_rank_without_explicit_authority():
    base=c("EUR_USD",88,conf=.88)
    mutated={**base,"research_recommendation":"BUY","research_score":999,"historical_outcome":"WIN"}
    assert opportunity_rank_score(base)[0] == opportunity_rank_score(mutated)[0]


def test_q_ranking_is_permutation_invariant():
    rows=all5()
    expected=[x.instrument for x in rank_opportunities(rows)]
    for perm in (rows[1:]+rows[:1], list(reversed(rows)), [rows[i] for i in (2,4,0,3,1)]):
        assert [x.instrument for x in rank_opportunities(perm)] == expected


def test_r_three_simultaneous_fx_candidates_one_slot():
    rows=[c("EUR_USD",82),c("GBP_USD",91),c("USD_JPY",87)]
    out=allocate_slots(rows,nlv=3000,broker_guard=allow,portfolio_guard=allow)
    assert [x["instrument"] for x in out["selected"]] == ["GBP_USD"]


def test_s_five_simultaneous_candidates_respect_two_slot_ceiling():
    out=allocate_slots(all5(),nlv=50000,broker_guard=allow,portfolio_guard=allow)
    assert len(out["selected"]) == 2


def test_t_existing_portfolio_correlation_guard_remains_final_authority():
    ctx={"portfolio_open_risk":0.0,"margin_usage":0.0,"open_instruments":["EUR_USD","GBP_USD"],
         "system_abnormal":False,"data_stale":False}
    verdict=server.portfolio_execution_guard("AUD_USD",ctx,prospective_trade_risk=.001)
    assert verdict["allow"] is False
    assert "CORRELATED_POSITION_LIMIT" in verdict["reasons"]


def test_u_runtime_integrity_includes_new_critical_modules():
    names={p.split("/")[-1] for p in server.production_release_files()}
    assert {"opportunity_ranker.py","slot_allocator.py","broker_risk.py"} <= names
    hashes=server.security_manager._file_hashes()
    assert {"opportunity_ranker.py","slot_allocator.py","broker_risk.py"} <= set(hashes)


def test_v_recovery_idempotency_still_namespaces_instrument():
    a=server.deterministic_intent_key("PRIMARY","EUR_USD","BUY","S","2026-08-29T12:00:00Z",1.1,1.09,1.12)
    b=server.deterministic_intent_key("PRIMARY","GBP_USD","BUY","S","2026-08-29T12:00:00Z",1.1,1.09,1.12)
    assert a != b


def test_w_no_lookahead_outcome_fields_do_not_change_rank():
    row=c("USD_JPY",87)
    a=opportunity_rank_score(row)[0]
    row2={**row,"outcome":"LOSS","realized_r":-1,"future_high":999,"future_low":0}
    assert opportunity_rank_score(row2)[0] == a


def test_x_hard_risk_limits_and_strategy_thresholds_are_not_increased():
    assert server.RISK_MAX_TRADE_FRACTION == pytest.approx(.01)
    assert server.RISK_MAX_STRATEGY_FRACTION == pytest.approx(.03)
    assert server.RISK_MAX_PORTFOLIO_FRACTION == pytest.approx(.06)
    assert server.RISK_MAX_MARGIN_USAGE == pytest.approx(.50)
    assert server.RISK_MAX_CORRELATED_POSITIONS == 2
    assert server.MIN_ENTRY_RR == pytest.approx(.40)
    assert server.BREAK_EVEN_LOCK_R == pytest.approx(0.0)
    assert server.RISK_ENGINE_SHADOW_MODE is True


def test_batch_scan_cycle_collects_all_before_selection_and_order_is_not_loop_based(monkeypatch):
    universe=["EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD"]
    monkeypatch.setattr(server,"SCAN_INSTRUMENTS",universe)
    monkeypatch.setattr(server,"WEEKEND_RESEARCH_ENABLED",False)
    monkeypatch.setattr(server,"OBSERVABILITY_ENABLED",False)
    monkeypatch.setattr(server,"AUTO",True)
    seen=[];executed=[]
    scores={"EUR_USD":82,"GBP_USD":91,"USD_JPY":87,"AUD_USD":85,"USD_CAD":84}
    async def fake_scan(client,inst,**kwargs):
        seen.append(inst)
        return {**c(inst,scores[inst]),"instrument":inst,"candle_ts":"2026-08-29T12:00:00Z",
                "dynamic_confidence":scores[inst]/100,"batch_selection_eligible":True,
                "_batch_context":{},"correlation_id":inst}
    async def risk(_):
        return {"nav":3000,"open_positions":0,"open_instruments":[],"portfolio_open_risk":0.0,
                "margin_usage":0.0,"system_abnormal":False,"data_stale":False}
    def broker(cand,sel,ctx): return {"allow":True}
    def portfolio(cand,sel,ctx): return {"allow":True}
    async def execute(client,cand,cycle_id,**kwargs):
        assert seen == universe  # execution begins only after full collection
        executed.append(cand["instrument"])
        return {"executed":True,"instrument":cand["instrument"]}
    monkeypatch.setattr(server,"scan",fake_scan)
    monkeypatch.setattr(server,"build_broker_risk_context",risk)
    monkeypatch.setattr(server,"_oanda_batch_broker_guard",broker)
    monkeypatch.setattr(server,"_batch_portfolio_guard",portfolio)
    monkeypatch.setattr(server,"execute_ranked_candidate",execute)
    monkeypatch.setattr(server,"_persist_multi_asset_cycle",lambda cycle: None)
    server.state["last_results"]={};server.state["instrument_state"]={}
    assert asyncio.run(server.scan_instruments_once(object())) is True
    assert executed == ["GBP_USD"]

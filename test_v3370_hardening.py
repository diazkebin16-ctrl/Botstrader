import math
import os

import pytest

import server
from instrument_profiles import instrument_profile
from opportunity_ranker import opportunity_rank_score, rank_opportunities
from slot_allocator import allocate_slots


def candidate(inst="EUR_USD", score=80, confidence=0.8, rr=1.5, room=1.5, spread=1.0):
    return {
        "instrument": inst,
        "signal": "BUY",
        "score": score,
        "dynamic_confidence": confidence,
        "rr_raw": rr,
        "room_to_barrier_r": room,
        "spread_pips": spread,
    }


def allow(*_args):
    return {"allow": True}


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), None, "abc", "", [], {}, object()])
def test_critical_confidence_malformed_is_excluded_fail_safe(bad):
    rows = [candidate("USD_JPY", 90, bad), candidate("EUR_USD", 68, 0.5)]
    ranked = rank_opportunities(rows)
    assert [x.instrument for x in ranked] == ["EUR_USD"]
    assert all(math.isfinite(x.rank_score) for x in ranked)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), None, "abc", "", [], {}, object()])
def test_critical_signal_quality_malformed_is_excluded_fail_safe(bad):
    rows = [candidate("USD_JPY", bad, 0.9), candidate("EUR_USD", 68, 0.5)]
    ranked = rank_opportunities(rows)
    assert [x.instrument for x in ranked] == ["EUR_USD"]


@pytest.mark.parametrize("field", ["rr_raw", "room_to_barrier_r", "spread_pips"])
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), None, "abc", "", [], {}, object()])
def test_optional_malformed_inputs_are_conservative_finite_and_do_not_crash(field, bad):
    row = candidate("EUR_USD", 80, 0.8)
    row[field] = bad
    score, components, _ = opportunity_rank_score(row)
    assert math.isfinite(score)
    assert all(math.isfinite(x) for x in components.values())
    component = {"rr_raw": "rr_quality", "room_to_barrier_r": "room_quality", "spread_pips": "cost_quality"}[field]
    assert components[component] == 0.0


def test_ia2_exact_nan_reproduction_no_longer_promotes_usdjpy():
    rows = [
        candidate("USD_JPY", 65, float("nan")),
        candidate("EUR_USD", 68, 0.5),
    ]
    ranked = rank_opportunities(rows)
    assert [x.instrument for x in ranked] == ["EUR_USD"]
    selected = allocate_slots(rows, nlv=3000, broker_guard=allow, portfolio_guard=allow)["selected"]
    assert [x["instrument"] for x in selected] == ["EUR_USD"]


def test_one_corrupt_among_five_cannot_displace_valid_candidates():
    rows = [
        candidate("EUR_USD", 68, 0.5),
        candidate("GBP_USD", 70, 0.6),
        candidate("USD_JPY", 99, float("nan")),
        candidate("AUD_USD", 72, 0.7),
        candidate("USD_CAD", 71, 0.65),
    ]
    ranked = rank_opportunities(rows)
    assert "USD_JPY" not in [x.instrument for x in ranked]
    assert ranked[0].instrument == "AUD_USD"


def test_all_candidates_corrupt_means_zero_safe_selection():
    rows = [candidate(inst, float("nan"), float("nan")) for inst in server.ANALYSIS_INSTRUMENTS]
    assert rank_opportunities(rows) == []
    assert allocate_slots(rows, nlv=5000, broker_guard=allow, portfolio_guard=allow)["selected"] == []


def test_rank_score_and_components_are_always_finite_for_accepted_candidates():
    rows = [candidate("EUR_USD", 80, 0.8, rr=float("nan"), room=float("inf"), spread="abc")]
    ranked = rank_opportunities(rows)
    assert len(ranked) == 1
    assert math.isfinite(ranked[0].rank_score)
    assert all(math.isfinite(x) for x in ranked[0].components.values())


def test_instruments_unset_defaults_to_primary_only(monkeypatch):
    monkeypatch.delenv("INSTRUMENTS", raising=False)
    assert server.configured_instruments() == [server.PRIMARY_INSTRUMENT]


def test_instruments_explicit_eur_gbp(monkeypatch):
    monkeypatch.setenv("INSTRUMENTS", "EUR_USD,GBP_USD")
    assert server.configured_instruments() == ["EUR_USD", "GBP_USD"]


def test_instruments_empty_falls_back_to_primary(monkeypatch):
    monkeypatch.setenv("INSTRUMENTS", "")
    assert server.configured_instruments() == [server.PRIMARY_INSTRUMENT]


def test_malformed_instruments_do_not_activate_unexpected_symbols(monkeypatch):
    monkeypatch.setenv("INSTRUMENTS", "abc,EUR/USD,NOT_A_PAIR,,{}")
    assert server.configured_instruments() == ["EUR_USD"]


def test_aud_cad_configuration_does_not_grant_oanda_execution_authority(monkeypatch):
    monkeypatch.setenv("INSTRUMENTS", "EUR_USD,AUD_USD,USD_CAD")
    configured = server.configured_instruments()
    assert configured == ["EUR_USD", "AUD_USD", "USD_CAD"]
    assert instrument_profile("AUD_USD").paper_execution_allowed is True
    assert instrument_profile("USD_CAD").paper_execution_allowed is True
    assert instrument_profile("AUD_USD").live_execution_allowed is False
    assert instrument_profile("USD_CAD").live_execution_allowed is False
    enabled = [x for x in configured if instrument_profile(x).allows_execution("PAPER", "practice")]
    assert enabled == ["EUR_USD", "AUD_USD", "USD_CAD"]


def test_single_worker_operational_invariant_is_declared_not_distributed_locking():
    assert server.EXECUTION_WORKER_MODE == "SINGLE_PROCESS_SINGLE_ACTIVE_REPLICA"
    assert server.DISTRIBUTED_EXECUTION_COORDINATION is False


def test_multi_worker_configuration_is_detected_for_fail_closed_execution(monkeypatch):
    monkeypatch.setenv("WEB_CONCURRENCY", "2")
    assert server._configured_execution_worker_count() == 2
    monkeypatch.setenv("WEB_CONCURRENCY", "malformed")
    assert server._configured_execution_worker_count() is None

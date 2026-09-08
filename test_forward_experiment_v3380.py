from __future__ import annotations

import inspect

import historical_replay
import server
from forward_experiment import (
    EUR_LEGACY_DIRECTIONAL_SCORE_MIN,
    evaluate_forward_experiment,
    forward_policy,
)
from legacy_v331_scoring import choose_legacy_v331_direction, legacy_v331_score


def _eur_features(score: float):
    return {
        "legacy_v331_buy_score": score,
        "legacy_v331_sell_score": score - 1.0,
        "legacy_v331_directional_score": score,
        "legacy_v331_chosen_direction": "BUY",
        "extension_atr": 0.2,
    }


def _paper_practice(monkeypatch):
    monkeypatch.setattr(server, "TRADING_ENVIRONMENT", "PAPER")
    monkeypatch.setattr(server, "PRIMARY_OANDA_ENV", "practice")
    monkeypatch.setattr(server, "OANDA", "https://api-fxpractice.oanda.com")


def test_legacy_v331_score_exact_formula_reference_case():
    components = {
        "h1_support": True,
        "m15_support": True,
        "h1_opposes": False,
        "m15_opposes": False,
        "m5_structure": True,
        "m5_momentum": True,
        "confirm": True,
        "m1_momentum": True,
        "second": True,
        "pc": 2,
        "pr": True,
        "rr_raw": 2.0,
        "min_rr": 1.5,
        "vol": 1.0,
        "ext": 1.0,
        "session_ok": True,
        "broken": 3,
    }
    assert legacy_v331_score(components) == 100.0


def test_legacy_v331_score_buy_sell_symmetry():
    common = {
        "h1_support": False, "m15_support": False,
        "h1_opposes": False, "m15_opposes": False,
        "m5_structure": False, "m5_momentum": True,
        "confirm": False, "m1_momentum": True,
        "second": False, "pc": 1, "pr": True,
        "rr_raw": 1.5, "min_rr": 1.5,
        "vol": 1.0, "ext": 1.3, "session_ok": True, "broken": 1,
    }
    assert legacy_v331_score(common) == legacy_v331_score(dict(common))


def test_legacy_v331_tie_favors_buy():
    direction, score = choose_legacy_v331_direction(31.0, 31.0)
    assert direction == "BUY"
    assert score == 31.0


def test_runtime_legacy_score_matches_historical_replay_helper(monkeypatch):
    monkeypatch.setattr(server, "ema", lambda values, n: [1.0] * len(values))
    monkeypatch.setattr(server, "pullbacks", lambda m5, e5, sig: (1, True))
    hyp = {
        "filters": {
            "h1_context": True, "m15_context": False, "m5_structure": False,
            "m1_confirmation": False, "second_pullback": False,
        },
        "metrics": {
            "h1_gap_atr": 0.2, "h1_slope_atr": 0.1,
            "m15_gap_atr": 0.0, "m15_slope_atr": 0.0,
            "m5_momentum": 0.0003, "m1_momentum": 0.0002,
            "volatility_ratio": 1.0, "extension_atr": 1.3,
            "session": {"ok": True},
        },
        "rr_raw": 1.5,
        "structure_context": {"broken_levels": [1, 2]},
    }
    m5 = [{"c": 1.0}] * 30
    runtime = server._legacy_v331_runtime_score(hyp, m5, "BUY")
    replay = historical_replay._legacy_v331_score(server, hyp, m5, "BUY")[0]
    assert runtime == replay


def test_eur_threshold_boundary():
    fail = evaluate_forward_experiment("EUR_USD", _eur_features(30.999999999))
    passed = evaluate_forward_experiment("EUR_USD", _eur_features(31.0))
    assert EUR_LEGACY_DIRECTIONAL_SCORE_MIN == 31.0
    assert fail["ok"] is False
    assert passed["ok"] is True


def _gbp_collection_features(direction="BUY", extension=0.2, buy=0.0, sell=0.0):
    return {
        "chosen_direction": direction,
        "extension_atr": extension,
        "legacy_v331_buy_score": buy,
        "legacy_v331_sell_score": sell,
    }


def test_gbp_collection_admits_canonical_direction_without_profitability_claim():
    out = evaluate_forward_experiment("GBP_USD", _gbp_collection_features("SELL", 9.0, -999.0, -999.0))
    assert out["ok"] is True
    assert out["collection_only"] is True
    assert out["profitability_certified"] is False
    assert out["chosen_direction"] == "SELL"


def test_gbp_collection_rejects_missing_or_invalid_direction():
    missing = _gbp_collection_features()
    missing.pop("chosen_direction")
    assert evaluate_forward_experiment("GBP_USD", missing)["ok"] is False
    assert evaluate_forward_experiment("GBP_USD", _gbp_collection_features("WAIT"))["ok"] is False


def test_instrument_isolation_policy():
    assert forward_policy("EUR_USD")["experiment_id"] == "EUR_PHASE2_FORWARD_V1"
    gbp = forward_policy("GBP_USD")
    assert gbp["experiment_id"] == "GBP_PAPER_COLLECTION_V1"
    assert gbp["bypass_quality_extension"] is True
    assert gbp["collection_only"] is True
    usd = forward_policy("USD_JPY")
    assert usd["experiment_id"] == "USDJPY_PHASE2_FORWARD_V1"
    assert usd["bypass_m1_confirmation"] is True
    assert usd["bypass_low_room_vetoes"] is False
    assert usd["bypass_quality_extension"] is True
    for symbol in ("AUD_USD", "USD_CAD"):
        p = forward_policy(symbol)
        assert p["experiment_id"] is None
        assert p["bypass_m1_confirmation"] is False
        assert p["bypass_low_room_vetoes"] is False
        assert p["bypass_quality_extension"] is False
        assert evaluate_forward_experiment(symbol, {})["ok"] is True


def test_eur_phase1_opened_strategic_gates_are_paper_scoped(monkeypatch):
    _paper_practice(monkeypatch)
    r = {
        "instrument": "EUR_USD", "rr_raw": 1.5, "barrier_class": "WEAK",
        "filters": {"m1_confirmation": False},
        "features": {"room_to_barrier_r": 0.2, "rr_raw": 0.8, "extension_atr": 9.0},
    }
    assert server.quality_entry_gate(r, {})["ok"] is True
    monkeypatch.setattr(server, "TRADING_ENVIRONMENT", "PRODUCTION")
    assert server.quality_entry_gate(r, {})["ok"] is False


def test_gbp_collection_opens_m1_and_extension_only_in_paper_practice(monkeypatch):
    _paper_practice(monkeypatch)
    r = {
        "instrument": "GBP_USD", "rr_raw": 1.5, "barrier_class": "WEAK",
        "filters": {"m1_confirmation": False},
        "features": {"extension_atr": 9.0},
    }
    assert server.quality_entry_gate(r, {})["ok"] is True
    monkeypatch.setattr(server, "TRADING_ENVIRONMENT", "PRODUCTION")
    assert server.quality_entry_gate(r, {})["ok"] is False


def test_gbp_collection_never_bypasses_hard_safety(monkeypatch):
    _paper_practice(monkeypatch)
    r = {
        "instrument": "GBP_USD", "signal": "BUY", "blocked": True,
        "rr_raw": server.MIN_ENTRY_RR - 0.01,
        "safety_checks": {"minimum_rr": False},
        "filters": {"m1_confirmation": False},
        "features": _gbp_collection_features("BUY", 9.0),
    }
    out = server.execution_decision(r, {"samples": 0})
    assert out["execute"] is False
    assert out["reason"] == "Safety veto: minimum_rr"


def test_gbp_safe_canonical_signal_is_admitted_for_paper_collection(monkeypatch):
    _paper_practice(monkeypatch)
    monkeypatch.setattr(server, "evaluate_active_research_rules", lambda r: {"ok": True})
    monkeypatch.setattr(server, "strategy_execution_gate", lambda r: {"ok": True})
    monkeypatch.setattr(server, "reentry_guard", lambda r: {"ok": True})
    r = {
        "instrument": "GBP_USD", "signal": "SELL", "blocked": False,
        "rr": server.MIN_RR, "rr_raw": server.MIN_ENTRY_RR,
        "barrier_class": "WEAK",
        "safety_checks": {
            "minimum_rr": True, "minimum_stop_pips": True,
            "barrier_room_ok": True, "volatility_sane": True,
        },
        "filters": {"m1_confirmation": False},
        "features": _gbp_collection_features("SELL", 9.0, -999.0, -999.0),
    }
    out = server.execution_decision(
        r, {"probability": 0.0, "required_confidence": 0.65, "samples": 0}
    )
    assert out["execute"] is True
    assert "Adaptive OBSERVE_ONLY" in out["reason"]
    assert server.forward_experiment_gate(r)["experiment_id"] == "GBP_PAPER_COLLECTION_V1"


def test_non_target_instrument_keeps_canonical_m1(monkeypatch):
    _paper_practice(monkeypatch)
    r = {
        "instrument": "AUD_USD", "rr_raw": 1.5, "barrier_class": "WEAK",
        "filters": {"m1_confirmation": False}, "features": {"extension_atr": 0.2},
    }
    out = server.quality_entry_gate(r, {})
    assert out["ok"] is False
    assert "M1" in out["reason"]


def test_safety_veto_precedes_experimental_gate(monkeypatch):
    _paper_practice(monkeypatch)
    r = {
        "instrument": "EUR_USD", "signal": "BUY", "blocked": True,
        "safety_checks": {"minimum_rr": False}, "features": {}, "filters": {},
    }
    out = server.execution_decision(r, {})
    assert out["execute"] is False
    assert out["reason"].startswith("Safety veto:")


def test_forward_gate_has_no_outcome_or_future_dependency():
    src = inspect.getsource(evaluate_forward_experiment)
    forbidden = ("realized_r", "mfe_r", "mae_r", "exit_price", "exit_ts", "trade_id", "future_bars")
    for name in forbidden:
        assert name not in src


def test_observability_is_deterministic(monkeypatch):
    _paper_practice(monkeypatch)
    r = {
        "instrument": "EUR_USD", "signal": "BUY", "score": 40, "rr": 1.5, "rr_raw": 1.5,
        "barrier_class": "WEAK", "safety_checks": {"minimum_rr": True, "barrier_room_ok": True},
        "filters": {"barrier_room_ok": True, "m1_confirmation": False},
        "features": {
            **_eur_features(31.0), "rr_raw": 1.5, "room_to_barrier_r": 1.0,
        },
    }
    conf = {"probability": 0.5, "required_confidence": 0.5, "samples": 0}
    a = server.forward_observation_snapshot(r, conf)
    b = server.forward_observation_snapshot(r, conf)
    assert a == b
    assert a["forward_experiment"]["experiment_id"] == "EUR_PHASE2_FORWARD_V1"
    assert a["forward_experiment"]["pass"] is True

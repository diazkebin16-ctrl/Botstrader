import pytest

import server


def _signal(instrument="USD_JPY", probability=0.46, rr=1.5):
    row = {
        "instrument": instrument,
        "signal": "SELL",
        "blocked": False,
        "safety_checks": {},
        "rr_raw": rr,
        "features": {
            "rr_raw": rr,
            "m15_gap_atr": 0.0,
            "m15_slope_atr": 0.0,
            "extension_atr": 0.2,
        },
    }
    confidence = {
        "probability": probability,
        "required_confidence": 0.69,
        "samples": 60,
        "mature": True,
    }
    return row, confidence


@pytest.fixture
def execution_gates_pass(monkeypatch):
    monkeypatch.setattr(server, "TRADING_ENVIRONMENT", "PAPER")
    monkeypatch.setattr(server, "PRIMARY_OANDA_ENV", "practice")
    monkeypatch.setattr(server, "OANDA", "https://api-fxpractice.oanda.com")
    monkeypatch.setattr(server, "quality_entry_gate", lambda r, conf: {"ok": True})
    monkeypatch.setattr(server, "forward_experiment_gate", lambda r: {"ok": True})
    monkeypatch.setattr(server.deployment_manager, "managed_paper_entry_gate", lambda identity: {"allow": True})
    monkeypatch.setattr(server, "evaluate_active_research_rules", lambda r: {"ok": True, "vetoes": []})
    monkeypatch.setattr(server, "strategy_execution_gate", lambda r: {"ok": True})
    monkeypatch.setattr(server, "reentry_guard", lambda r: {"ok": True})


def test_mature_usdjpy_paper_signal_uses_rr15_collection_threshold(execution_gates_pass):
    row, confidence = _signal(probability=0.46, rr=1.5)
    result = server.execution_decision(row, confidence)
    assert result["execute"] is True
    assert "PAPER_COLLECTION" in result["reason"]
    assert result["paper_collection_confidence_gate"]["required_confidence"] == pytest.approx(0.45)
    assert result["paper_collection_confidence_gate"]["global_required_confidence"] == pytest.approx(0.69)
    assert result["paper_collection_confidence_gate"]["production_authority"] is False


def test_usdjpy_paper_signal_below_expectancy_threshold_remains_blocked(execution_gates_pass):
    row, confidence = _signal(probability=0.44, rr=1.5)
    result = server.execution_decision(row, confidence)
    assert result["execute"] is False
    assert "44.0% < 45.0%" in result["reason"]


def test_rr2_collection_threshold_has_a_40_percent_floor(execution_gates_pass):
    row, confidence = _signal(probability=0.41, rr=2.0)
    result = server.execution_decision(row, confidence)
    assert result["execute"] is True
    assert result["paper_collection_confidence_gate"]["required_confidence"] == pytest.approx(0.40)


def test_non_jpy_keeps_the_global_confidence_gate(execution_gates_pass):
    row, confidence = _signal(instrument="EUR_USD", probability=0.46, rr=1.5)
    result = server.execution_decision(row, confidence)
    assert result["execute"] is False
    assert "46.0% < 69.0%" in result["reason"]
    assert row["paper_collection_confidence_gate"]["active"] is False


def test_production_cannot_inherit_the_collection_threshold(execution_gates_pass, monkeypatch):
    monkeypatch.setattr(server, "TRADING_ENVIRONMENT", "PRODUCTION")
    row, confidence = _signal(probability=0.46, rr=1.5)
    result = server.execution_decision(row, confidence)
    assert result["execute"] is False
    assert "46.0% < 69.0%" in result["reason"]
    assert row["paper_collection_confidence_gate"]["active"] is False


def test_safety_veto_remains_authoritative_before_collection_gate(execution_gates_pass):
    row, confidence = _signal(probability=0.90, rr=2.0)
    row["blocked"] = True
    row["safety_checks"] = {"spread_ok": False}
    result = server.execution_decision(row, confidence)
    assert result["execute"] is False
    assert result["reason"] == "Safety veto: spread_ok"
    assert "paper_collection_confidence_gate" not in row


def test_collection_gate_is_included_in_forward_attribution(execution_gates_pass):
    row, confidence = _signal(probability=0.46, rr=1.5)
    result = server.execution_decision(row, confidence)
    snapshot = server.forward_observation_snapshot(
        row, confidence, executed=result["execute"], final_reason=result["reason"]
    )
    gate = snapshot["paper_collection_confidence_gate"]
    assert gate["active"] is True
    assert gate["required_confidence"] == pytest.approx(0.45)

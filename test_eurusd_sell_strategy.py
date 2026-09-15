import copy
import json
from pathlib import Path

import pytest

from eurusd_sell_strategy import (
    MAX_SIMULTANEOUS_EURUSD_POSITIONS,
    backtest_sell_candidate,
    candidate_definition_sha256,
    canonical_dataset_sha256,
    evaluate_sell_candidate,
    route_eurusd_directional_lane,
)


EVIDENCE_PATH = Path(__file__).with_name("EURUSD_SELL_ONLY_V1_EVIDENCE.json")


def row(direction="SELL", extension=0.9, strength=0.2):
    return {
        "instrument": "EUR_USD",
        "signal": direction,
        "features": {"extension_atr": extension, "session_strength": strength},
    }


def test_sell_candidate_is_strictly_sell_only():
    assert evaluate_sell_candidate(row("SELL"))["eligible"] is True
    buy = evaluate_sell_candidate(row("BUY"))
    assert buy["eligible"] is False
    assert buy["reason"] == "SELL_ONLY_DIRECTION"


def test_filters_are_independent_and_fail_closed():
    assert evaluate_sell_candidate(row(extension=0.8999))["eligible"] is False
    assert evaluate_sell_candidate(row(strength=0.1999))["eligible"] is False
    missing = evaluate_sell_candidate({"instrument": "EUR_USD", "signal": "SELL", "features": {}})
    assert missing["eligible"] is False
    assert all(check["reason"] == "REQUIRED_PRE_ENTRY_EVIDENCE_MISSING" for check in missing["checks"])
    assert evaluate_sell_candidate(row(extension=float("nan")))["eligible"] is False


def test_outcomes_and_future_fields_cannot_change_pre_entry_decision():
    base = row()
    win = {**copy.deepcopy(base), "outcome": "WIN", "exit_price": 99, "future_bars": [1, 2, 3]}
    loss = {**copy.deepcopy(base), "outcome": "LOSS", "exit_price": -99, "future_bars": [-1]}
    assert evaluate_sell_candidate(win) == evaluate_sell_candidate(loss)


def test_directional_router_selects_exactly_one_lane_and_respects_open_position():
    buy = route_eurusd_directional_lane(
        row("BUY"), buy_lane_eligible=True, buy_strategy_id="EURUSD_CURRENT"
    )
    sell = route_eurusd_directional_lane(
        row("SELL"), buy_lane_eligible=True, buy_strategy_id="EURUSD_CURRENT"
    )
    blocked = route_eurusd_directional_lane(
        row("SELL"), buy_lane_eligible=True, buy_strategy_id="EURUSD_CURRENT",
        instrument_has_open_position=True,
    )
    assert buy["selected"] == "EURUSD_CURRENT"
    assert sell["selected"] == "EURUSD_SELL_ONLY_V1"
    assert blocked["selected"] is None
    assert MAX_SIMULTANEOUS_EURUSD_POSITIONS == 1


def test_wrong_instrument_is_never_eligible():
    candidate = row()
    candidate["instrument"] = "GBP_USD"
    result = evaluate_sell_candidate(candidate)
    assert result["eligible"] is False
    assert result["reason"] == "INSTRUMENT_NOT_ALLOWED"
    routed = route_eurusd_directional_lane(
        candidate, buy_lane_eligible=True, buy_strategy_id="EURUSD_CURRENT"
    )
    assert routed["selected"] is None
    assert routed["reason"] == "INSTRUMENT_NOT_ALLOWED"


def test_frozen_evidence_reproduces_discovery_holdout_and_full_results():
    evidence = json.loads(EVIDENCE_PATH.read_text(encoding="utf-8"))
    rows = evidence["dataset"]["rows"]
    split = evidence["methodology"]["discovery_count"]
    assert candidate_definition_sha256() == evidence["candidate"]["candidate_definition_sha256"]
    assert canonical_dataset_sha256(rows) == evidence["dataset"]["sha256"]

    for name, subset in (
        ("discovery", rows[:split]),
        ("holdout", rows[split:]),
        ("full", rows),
    ):
        actual = backtest_sell_candidate(subset)
        expected = evidence["results"][name]
        assert actual["baseline"] == expected["baseline"]
        assert actual["candidate"] == expected["candidate"]
        assert actual["blocked_win"] == expected["blocked_win"]
        assert actual["blocked_loss"] == expected["blocked_loss"]

    assert evidence["candidate"]["paper_only"] is True
    assert evidence["candidate"]["research_only"] is True
    assert evidence["candidate"]["production_authority"] is False
    assert evidence["candidate"]["auto_activation"] is False


@pytest.mark.parametrize("direction", ["WAIT", "", "HOLD"])
def test_non_directional_inputs_select_nothing(direction):
    result = route_eurusd_directional_lane(
        row(direction), buy_lane_eligible=True, buy_strategy_id="EURUSD_CURRENT"
    )
    assert result["selected"] is None

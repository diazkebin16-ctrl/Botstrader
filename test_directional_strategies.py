import copy

import pytest

import server
from directional_strategies import (
    STRATEGY_IDS,
    all_strategy_definitions,
    directional_strategy_id,
    evaluate_directional_strategy,
)


PAIRS = ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD")


def signal(instrument="EUR_USD", direction="BUY", *, extension=1.0, strength=0.3):
    return {
        "instrument": instrument,
        "signal": direction,
        "features": {"extension_atr": extension, "session_strength": strength},
        "filters": {},
    }


def test_registry_exposes_exactly_ten_unique_paper_only_strategies():
    definitions = all_strategy_definitions()
    assert len(definitions) == len(STRATEGY_IDS) == 10
    assert len(set(STRATEGY_IDS)) == 10
    for pair in PAIRS:
        lanes = [item for item in definitions if item["instrument"] == pair]
        assert {item["direction"] for item in lanes} == {"BUY", "SELL"}
        assert all(item["paper_only"] is True for item in lanes)
        assert all(item["production_authority"] is False for item in lanes)


@pytest.mark.parametrize("instrument", PAIRS)
@pytest.mark.parametrize("direction", ("BUY", "SELL"))
def test_runtime_strategy_identity_is_bound_to_instrument_and_direction(instrument, direction):
    row = signal(instrument, direction)
    expected = f"{instrument.replace('_', '')}_{direction}_ONLY_V1"
    assert directional_strategy_id(instrument, direction) == expected
    assert server.setup_variant(row) == expected
    evaluated = evaluate_directional_strategy(row)
    assert evaluated["eligible"] is True
    assert evaluated["strategy_id"] == expected


def test_old_non_directional_runtime_identity_is_no_longer_selected():
    old_prefix = "SECOND_PULLBACK_"
    for pair in PAIRS:
        for direction in ("BUY", "SELL"):
            assert not server.setup_variant(signal(pair, direction)).startswith(old_prefix)
    assert server.setup_variant(signal("EUR_USD", "WAIT")) == "WAIT"


def test_eurusd_sell_uses_frozen_filters_and_other_lanes_remain_collecting():
    assert evaluate_directional_strategy(signal("EUR_USD", "SELL"))["eligible"] is True
    assert evaluate_directional_strategy(signal("EUR_USD", "SELL", extension=0.89))["eligible"] is False
    assert evaluate_directional_strategy(signal("EUR_USD", "SELL", strength=0.19))["eligible"] is False
    missing = evaluate_directional_strategy({"instrument": "EUR_USD", "signal": "SELL", "features": {}})
    assert missing["eligible"] is False
    assert all(check["reason"] == "REQUIRED_PRE_ENTRY_EVIDENCE_MISSING" for check in missing["checks"])
    for pair in PAIRS:
        for direction in ("BUY", "SELL"):
            if (pair, direction) != ("EUR_USD", "SELL"):
                assert evaluate_directional_strategy(signal(pair, direction))["eligible"] is True


def test_future_outcome_fields_cannot_change_directional_decision():
    base = signal("EUR_USD", "SELL")
    win = {**copy.deepcopy(base), "outcome": "WIN", "exit_price": 9, "future_bars": [1, 2]}
    loss = {**copy.deepcopy(base), "outcome": "LOSS", "exit_price": -9, "future_bars": [-1]}
    assert evaluate_directional_strategy(win) == evaluate_directional_strategy(loss)


def test_attach_records_lane_and_preserves_setup_subtype():
    row = signal("GBP_USD", "SELL")
    row.update({"alignment": "NEUTRAL", "rr_raw": 1.7, "score": 82})
    lane = server.attach_directional_strategy(row)
    assert lane["strategy_id"] == "GBPUSD_SELL_ONLY_V1"
    assert row["filters"]["directional_lane"] is True
    assert row["setup_pattern"] == "SECOND_PULLBACK_NEWS_NEUTRAL_RR15_Q80"


def test_empirical_confidence_evidence_is_isolated_by_direction(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "DB", str(tmp_path / "directional.db"))
    c = server.conn()
    for index, (direction, label) in enumerate(
        (("BUY", 1), ("BUY", 1), ("BUY", 1), ("SELL", 0), ("SELL", 0)), 1
    ):
        variant = directional_strategy_id("GBP_USD", direction)
        cur = c.execute(
            """INSERT INTO signals(ts,instrument,signal,technical,score,blocked,executed,
               setup_variant,features_json,filters_json) VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (f"2026-09-15T00:0{index}:00Z", "GBP_USD", direction, 80, 80, 0, 1, variant, "{}", "{}"),
        )
        c.execute(
            """INSERT INTO learning_samples(signal_id,created_ts,instrument,direction,entry,stop,target,
               technical,score,blocked,executed,features_json,status,label) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (cur.lastrowid, f"2026-09-15T00:0{index}:00Z", "GBP_USD", direction, 1.2, 1.1, 1.3,
             80, 80, 0, 1, "{}", "RESOLVED", label),
        )
    c.commit()
    c.close()

    buy = server.empirical_confidence(signal("GBP_USD", "BUY"))
    sell = server.empirical_confidence(signal("GBP_USD", "SELL"))
    assert buy["samples"] == 3
    assert buy["local_samples"] == 3
    assert buy["global_win_rate"] == 1.0
    assert sell["samples"] == 2
    assert sell["local_samples"] == 2
    assert sell["global_win_rate"] == 0.0


def test_public_inventory_reports_ten_lanes_and_single_position_limit():
    payload = __import__("asyncio").run(server.directional_strategies_api())
    assert payload["version"] == "3.40.0"
    assert payload["strategy_count"] == 10
    assert payload["max_simultaneous_positions_per_instrument"] == 1
    assert payload["production_authority"] is False


def test_directional_registry_is_covered_by_runtime_integrity_manifest():
    assert "directional_strategies.py" in server.security_manager._file_hashes()

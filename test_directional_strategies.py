import copy

import pytest

import server
from directional_strategies import (
    STRATEGY_IDS,
    all_strategy_definitions,
    directional_strategy_id,
    evaluate_directional_strategy,
    strategy_definition,
)


PAIRS = ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD")


def signal(
    instrument="EUR_USD", direction="BUY", *, extension=1.0, strength=0.25,
    session_momentum=0.0, rr_raw=1.5, role=None,
):
    features = {
        "extension_atr": extension,
        "session_strength": strength,
        "session_momentum_atr": session_momentum,
        "session_displacement_atr": 0.0,
        "room_to_barrier_r": 0.0,
        "h1_gap_atr": 2.0,
        "m15_gap_atr": 2.0,
        "buy_score": 40.0,
        "rr_raw": rr_raw,
    }
    if direction in {"BUY", "SELL"}:
        definition = strategy_definition(instrument, direction)
        rules = definition["filters"]
        if "conditional_rules" in definition:
            from major_trend import TREND_VERSION
            policies = definition["conditional_rules"]
            role = role or next(key for key, policy in policies.items() if policy["enabled"])
            regime = "LATERAL" if role == "LATERAL" else (
                "UP" if (direction == "BUY") == (role == "WITH") else "DOWN")
            features.update(major_trend_regime=regime, major_trend_version=TREND_VERSION)
            rules = policies[role]["filters"]
        for rule in rules:
            threshold = float(rule["threshold"])
            features[rule["feature"]] = threshold + 0.01 if rule["operator"] == ">=" else threshold - 0.01
    return {
        "instrument": instrument,
        "signal": direction,
        "features": features,
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
    version = "V2" if (instrument, direction) in {
        ("EUR_USD", "SELL"), ("USD_JPY", "SELL")
    } else "V1"
    expected = f"{instrument.replace('_', '')}_{direction}_ONLY_{version}"
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


def test_all_ten_paper_lanes_enforce_their_current_frozen_filters():
    for pair in PAIRS:
        for direction in ("BUY", "SELL"):
            definition = strategy_definition(pair, direction)
            assert definition["paper_only"] is True
            assert definition["production_authority"] is False
            policies = definition.get("conditional_rules") or {None: {"enabled": True, "filters": definition["filters"]}}
            for role, policy in policies.items():
                if not policy["enabled"]:
                    continue
                row = signal(pair, direction, role=role)
                assert evaluate_directional_strategy(row)["eligible"] is True
                for rule in policy["filters"]:
                    failing = copy.deepcopy(row)
                    threshold = float(rule["threshold"])
                    failing["features"][rule["feature"]] = (
                        threshold - 0.000001 if rule["operator"] == ">=" else threshold + 0.000001
                    )
                    assert evaluate_directional_strategy(failing)["eligible"] is False
            missing = evaluate_directional_strategy({"instrument": pair, "signal": direction, "features": {}})
            assert missing["eligible"] is False
            expected = "NO_QUALIFIED_TREND_EVIDENCE" if "conditional_rules" in definition else "REQUIRED_PRE_ENTRY_EVIDENCE_MISSING"
            assert missing["checks"][0]["reason"] == expected


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
    assert payload["version"] == "3.41.0"
    assert payload["strategy_count"] == 10
    assert payload["max_simultaneous_positions_per_instrument"] == 1
    assert payload["production_authority"] is False


def test_directional_registry_is_covered_by_runtime_integrity_manifest():
    hashes = server.security_manager._file_hashes()
    assert {"directional_strategies.py", "major_trend.py", "trend_paper_activation.py"} <= hashes.keys()

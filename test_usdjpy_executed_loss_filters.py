import hashlib
import json
from pathlib import Path

import managed_strategy_rules as managed


def _row(*, direction="BUY", gap=0.0, slope=0.0, extension=0.8):
    return {
        "instrument": "USD_JPY",
        "signal": direction,
        "features": {
            "m15_gap_atr": gap,
            "m15_slope_atr": slope,
            "extension_atr": extension,
        },
    }


def _veto_groups(result):
    return {item.get("group_id") for item in result["vetoes"]}


def test_usdjpy_managed_identity_is_experimental_paper_only():
    identity = managed.managed_strategy_identity("USD_JPY")
    assert identity["active"] is True
    assert identity["v3_candidate_id"] == "USDJPY_EXECUTED60_DUAL_VETO_V1"
    assert identity["v3_confidence_class"] == "EXPERIMENTAL"
    assert identity["v3_experimental"] is True
    assert identity["v3_paper_only"] is True
    assert identity["production_authority"] is False


def test_m15_late_buy_requires_both_conditions_and_buy_direction():
    blocked = managed.evaluate_managed_strategy_rules(
        _row(direction="BUY", gap=0.70, slope=0.25, extension=0.8)
    )
    assert blocked["ok"] is False
    assert _veto_groups(blocked) == {"M15_LATE_BUY"}

    assert managed.evaluate_managed_strategy_rules(
        _row(direction="BUY", gap=0.69, slope=0.25, extension=0.8)
    )["ok"] is True
    assert managed.evaluate_managed_strategy_rules(
        _row(direction="BUY", gap=0.70, slope=0.24, extension=0.8)
    )["ok"] is True
    assert managed.evaluate_managed_strategy_rules(
        _row(direction="SELL", gap=0.90, slope=0.50, extension=0.8)
    )["ok"] is True


def test_low_m1_extension_veto_is_independent_and_inclusive():
    blocked = managed.evaluate_managed_strategy_rules(
        _row(direction="SELL", gap=-0.2, slope=-0.1, extension=0.43)
    )
    assert blocked["ok"] is False
    assert _veto_groups(blocked) == {"LOW_M1_EXTENSION"}
    assert managed.evaluate_managed_strategy_rules(
        _row(direction="SELL", gap=-0.2, slope=-0.1, extension=0.4300001)
    )["ok"] is True


def test_both_vetoes_can_trigger_without_becoming_admission_rules():
    blocked = managed.evaluate_managed_strategy_rules(
        _row(direction="BUY", gap=1.0, slope=0.5, extension=0.3)
    )
    assert blocked["ok"] is False
    assert _veto_groups(blocked) == {"M15_LATE_BUY", "LOW_M1_EXTENSION"}


def test_required_pre_entry_evidence_fails_closed_for_usdjpy_only():
    missing = managed.evaluate_managed_strategy_rules(
        {"instrument": "USD_JPY", "signal": "BUY", "features": {}}
    )
    assert missing["ok"] is False
    assert all(item["reason"] == "REQUIRED_PRE_ENTRY_EVIDENCE_MISSING" for item in missing["vetoes"])
    assert managed.evaluate_managed_strategy_rules(
        {"instrument": "GBP_USD", "signal": "BUY", "features": {}}
    )["ok"] is True


def test_existing_eur_admission_rules_keep_their_original_semantics():
    accepted = managed.evaluate_managed_strategy_rules({
        "instrument": "EUR_USD",
        "features": {"session_displacement_atr": 0.0, "m15_slope_atr": 0.0},
    })
    rejected = managed.evaluate_managed_strategy_rules({
        "instrument": "EUR_USD",
        "features": {"session_displacement_atr": -2.0, "m15_slope_atr": 0.0},
    })
    assert accepted["ok"] is True
    assert rejected["ok"] is False
    assert all("rule_mode" not in item for item in accepted["rules"])


def test_evidence_artifact_matches_the_active_release():
    evidence = json.loads(Path("USDJPY_EXECUTED60_DUAL_FILTER_EVIDENCE.json").read_text())
    identity = managed.managed_strategy_identity("USD_JPY")
    assert evidence["population"] == {
        "end": "2026-09-04T14:15:00+00:00",
        "executed_resolved": 60,
        "losses": 34,
        "start": "2026-08-31T00:12:00+00:00",
        "wins": 26,
    }
    assert evidence["combined_observed"]["loss_blocked"] == 16
    assert evidence["combined_observed"]["win_blocked"] == 1
    assert evidence["combined_observed"]["wins_kept"] == 25
    assert evidence["combined_observed"]["losses_kept"] == 18
    assert evidence["candidate_definition_sha256"] == identity["v3_candidate_definition_sha256"]
    assert evidence["release_identity"] == identity["v3_managed_release_identity"]
    assert evidence["methodology"]["production_authority"] is False
    definition = {
        "instrument": evidence["instrument"],
        "source_code_sha": evidence["source_code_sha"],
        "evidence_sha256": evidence["evidence_sha256"],
        "population": evidence["population"],
        "veto_groups": [
            {
                "group_id": group["group_id"],
                **({"direction": group["direction"]} if group.get("direction") else {}),
                "mode": "VETO_WHEN_ALL",
                "conditions": group["conditions"],
            }
            for group in evidence["veto_groups"]
        ],
        "combined_observed": {
            key: evidence["combined_observed"][key]
            for key in ("loss_blocked", "win_blocked", "wins_kept", "losses_kept", "activity_kept", "win_rate")
        },
        "paper_only": evidence["paper_only"],
        "production_authority": evidence["methodology"]["production_authority"],
    }
    digest = hashlib.sha256(
        json.dumps(definition, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert digest == evidence["candidate_definition_sha256"]

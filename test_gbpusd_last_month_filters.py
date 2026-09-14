import hashlib
import json
from pathlib import Path

import managed_strategy_rules as managed


def row(*, slope, rr):
    return {
        "instrument": "GBP_USD",
        "signal": "BUY",
        "features": {"m15_slope_atr": slope, "rr_raw": rr},
    }


def test_gbpusd_release_is_experimental_paper_only():
    identity = managed.managed_strategy_identity("GBP_USD")
    assert identity["active"] is True
    assert identity["v3_candidate_id"] == "GBPUSD_LASTMONTH_ABS_SLOPE_ROOM_V1"
    assert identity["v3_confidence_class"] == "EXPERIMENTAL"
    assert identity["v3_experimental"] is True
    assert identity["v3_paper_only"] is True
    assert identity["production_authority"] is False


def test_gbpusd_requires_both_frozen_conditions():
    assert managed.evaluate_managed_strategy_rules(row(slope=0.20, rr=1.00))["ok"] is True
    assert managed.evaluate_managed_strategy_rules(row(slope=-0.20, rr=1.00))["ok"] is True
    assert managed.evaluate_managed_strategy_rules(row(slope=0.05, rr=1.00))["ok"] is False
    assert managed.evaluate_managed_strategy_rules(row(slope=0.20, rr=1.20))["ok"] is False


def test_gbpusd_missing_required_feature_fails_closed_without_cross_asset_leakage():
    assert managed.evaluate_managed_strategy_rules({"instrument": "GBP_USD", "signal": "BUY", "features": {}})["ok"] is False
    assert managed.evaluate_managed_strategy_rules({"instrument": "AUD_USD", "signal": "BUY", "features": {}})["ok"] is True
    assert managed.evaluate_managed_strategy_rules({"instrument": "USD_CAD", "signal": "SELL", "features": {}})["ok"] is True
    assert managed.evaluate_managed_strategy_rules({"instrument": "EUR_USD", "signal": "BUY", "features": {}})["ok"] is True


def test_evidence_matches_active_candidate_definition():
    evidence = json.loads(Path("GBPUSD_LASTMONTH_FILTER_EVIDENCE.json").read_text())
    raw = json.dumps(evidence["candidate_definition"], sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256(raw).hexdigest()
    identity = managed.managed_strategy_identity("GBP_USD")
    assert digest == evidence["candidate_definition_sha256"]
    assert digest == identity["v3_candidate_definition_sha256"]
    assert evidence["managed_release_identity"] == identity["v3_managed_release_identity"]
    assert evidence["methodology"]["holdout_retuning"] is False
    assert evidence["methodology"]["production_authority"] is False

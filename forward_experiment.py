"""Instrument-scoped PAPER forward experiment policy for EUR/USD, GBP/USD, and USD/JPY.

This module is deliberately pure: it has no broker, database, network, outcome,
or future-data dependency. Runtime authority is granted only by server.py after
verifying OANDA Practice/PAPER context.
"""
from __future__ import annotations

from typing import Any, Dict, Mapping

EUR_FORWARD_EXPERIMENT_ID = "EUR_PHASE2_FORWARD_V1"
GBP_FORWARD_EXPERIMENT_ID = "GBP_PAPER_COLLECTION_V1"
USDJPY_FORWARD_EXPERIMENT_ID = "USDJPY_PHASE2_FORWARD_V1"

EUR_LEGACY_DIRECTIONAL_SCORE_MIN = 31.0
GBP_EXTENSION_ATR_MAX = 1.4985678822167452
GBP_LEGACY_BUY_SCORE_MIN = 16.400000000000002
USDJPY_CHOSEN_LEGACY_SCORE_MIN = 33.0


def normalize_symbol(symbol: Any) -> str:
    return str(symbol or "").strip().upper().replace("/", "_")


def forward_policy(symbol: Any) -> Dict[str, Any]:
    instrument = normalize_symbol(symbol)
    if instrument == "EUR_USD":
        return {
            "instrument": instrument,
            "experiment_id": EUR_FORWARD_EXPERIMENT_ID,
            # Phase 1 opened these strategic gates for the Phase 2 population.
            # Safety checks are intentionally absent from this policy.
            "bypass_m1_confirmation": True,
            "bypass_low_room_vetoes": True,
            "bypass_quality_extension": True,
        }
    if instrument == "GBP_USD":
        return {
            "instrument": instrument,
            "experiment_id": GBP_FORWARD_EXPERIMENT_ID,
            # PAPER-only collection admits the canonical directional signal once
            # server.py has already enforced hard safety.  These exceptions do
            # not exist in PRODUCTION/live and do not change signal generation.
            "bypass_m1_confirmation": True,
            "bypass_low_room_vetoes": False,
            "bypass_quality_extension": True,
            "collection_only": True,
            "profitability_certified": False,
        }
    if instrument == "USD_JPY":
        return {
            "instrument": instrument,
            "experiment_id": USDJPY_FORWARD_EXPERIMENT_ID,
            # USD/JPY Phase 1 recovered its target strategic WIN population only
            # after opening canonical M1 confirmation and QUALITY:EXTENSION.
            # Safety/global protections are intentionally not represented here.
            "bypass_m1_confirmation": True,
            "bypass_low_room_vetoes": False,
            "bypass_quality_extension": True,
        }
    return {
        "instrument": instrument,
        "experiment_id": None,
        "bypass_m1_confirmation": False,
        "bypass_low_room_vetoes": False,
        "bypass_quality_extension": False,
    }


def evaluate_forward_experiment(symbol: Any, features: Mapping[str, Any]) -> Dict[str, Any]:
    """Evaluate only the frozen Phase 2 forward candidate.

    All accepted inputs are decision-time/pre-entry values. The function never
    reads outcome, realized R, exit data, timestamps, trade IDs, or future bars.
    """
    instrument = normalize_symbol(symbol)
    f = dict(features or {})

    if instrument == "EUR_USD":
        required = ("legacy_v331_buy_score", "legacy_v331_sell_score", "legacy_v331_directional_score", "legacy_v331_chosen_direction")
        missing = [k for k in required if k not in f or f.get(k) is None]
        if missing:
            return {
                "ok": False,
                "instrument": instrument,
                "experiment_id": EUR_FORWARD_EXPERIMENT_ID,
                "reason": "EUR_EXPERIMENTAL_DIRECTIONAL_SCORE_FEATURES_MISSING",
                "missing": missing,
            }
        buy = float(f["legacy_v331_buy_score"])
        sell = float(f["legacy_v331_sell_score"])
        directional = float(f["legacy_v331_directional_score"])
        chosen = str(f["legacy_v331_chosen_direction"]).upper()
        passed = directional >= EUR_LEGACY_DIRECTIONAL_SCORE_MIN
        return {
            "ok": passed,
            "instrument": instrument,
            "experiment_id": EUR_FORWARD_EXPERIMENT_ID,
            "reason": "EUR_EXPERIMENTAL_DIRECTIONAL_SCORE_PASS" if passed else "EUR_EXPERIMENTAL_DIRECTIONAL_SCORE",
            "legacy_v331_buy_score": buy,
            "legacy_v331_sell_score": sell,
            "legacy_v331_directional_score": directional,
            "legacy_v331_chosen_direction": chosen,
            "threshold": EUR_LEGACY_DIRECTIONAL_SCORE_MIN,
            "pass": passed,
        }

    if instrument == "USD_JPY":
        required = ("chosen_direction", "legacy_v331_buy_score", "legacy_v331_sell_score")
        missing = [k for k in required if k not in f or f.get(k) is None]
        if missing:
            return {
                "ok": False,
                "instrument": instrument,
                "experiment_id": USDJPY_FORWARD_EXPERIMENT_ID,
                "reason": "USDJPY_EXPERIMENTAL_CHOSEN_LEGACY_SCORE_FEATURES_MISSING",
                "missing": missing,
            }
        direction = str(f["chosen_direction"]).upper()
        buy_score = float(f["legacy_v331_buy_score"])
        sell_score = float(f["legacy_v331_sell_score"])
        if direction == "BUY":
            chosen_score = buy_score
        elif direction == "SELL":
            chosen_score = sell_score
        else:
            return {
                "ok": False,
                "instrument": instrument,
                "experiment_id": USDJPY_FORWARD_EXPERIMENT_ID,
                "reason": "USDJPY_EXPERIMENTAL_CHOSEN_DIRECTION_INVALID",
                "chosen_direction": direction,
                "legacy_v331_buy_score": buy_score,
                "legacy_v331_sell_score": sell_score,
                "threshold": USDJPY_CHOSEN_LEGACY_SCORE_MIN,
                "pass": False,
            }
        passed = chosen_score >= USDJPY_CHOSEN_LEGACY_SCORE_MIN
        return {
            "ok": passed,
            "instrument": instrument,
            "experiment_id": USDJPY_FORWARD_EXPERIMENT_ID,
            "reason": "USDJPY_EXPERIMENTAL_CHOSEN_LEGACY_SCORE_PASS" if passed else "USDJPY_EXPERIMENTAL_CHOSEN_LEGACY_SCORE",
            "chosen_direction": direction,
            "legacy_v331_buy_score": buy_score,
            "legacy_v331_sell_score": sell_score,
            "chosen_legacy_score": chosen_score,
            "threshold": USDJPY_CHOSEN_LEGACY_SCORE_MIN,
            "score_pass": passed,
            "pass": passed,
        }

    if instrument == "GBP_USD":
        required = (
            "chosen_direction",
            "extension_atr",
            "legacy_v331_buy_score",
            "legacy_v331_sell_score",
        )
        missing = [k for k in required if k not in f or f.get(k) is None]
        if missing:
            return {
                "ok": False,
                "instrument": instrument,
                "experiment_id": GBP_FORWARD_EXPERIMENT_ID,
                "reason": "GBP_PAPER_COLLECTION_FEATURES_MISSING",
                "missing": missing,
            }
        direction = str(f["chosen_direction"]).upper()
        extension = float(f["extension_atr"])
        buy_score = float(f["legacy_v331_buy_score"])
        sell_score = float(f["legacy_v331_sell_score"])
        direction_valid = direction in {"BUY", "SELL"}
        return {
            "ok": direction_valid,
            "instrument": instrument,
            "experiment_id": GBP_FORWARD_EXPERIMENT_ID,
            "reason": "GBP_PAPER_COLLECTION_ADMIT" if direction_valid else "GBP_PAPER_COLLECTION_DIRECTION_INVALID",
            "chosen_direction": direction,
            "extension_atr": extension,
            "legacy_v331_buy_score": buy_score,
            "legacy_v331_sell_score": sell_score,
            "collection_only": True,
            "profitability_certified": False,
            "pass": direction_valid,
        }

    return {
        "ok": True,
        "instrument": instrument,
        "experiment_id": None,
        "reason": "NO_INSTRUMENT_FORWARD_EXPERIMENT",
        "pass": True,
    }

"""PAPER-only directional strategy registry for the five OANDA FX pairs.

Each instrument owns one BUY lane and one SELL lane. The lanes share the
existing safety, quality, time, portfolio and broker gates, but have independent
identities, evidence histories and optional lane-specific admission filters.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Mapping


SUPPORTED_INSTRUMENTS = ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD")
SUPPORTED_DIRECTIONS = ("BUY", "SELL")
BASE_FILTER_PIPELINE = (
    "CORE_TECHNICAL",
    "INSTRUMENT_MANAGED_RULES",
    "GLOBAL_SAFETY",
    "GLOBAL_ENTRY_TIME",
    "PORTFOLIO_RISK",
    "BROKER_PREFLIGHT",
)

# Frozen from DIRECTIONAL_ONE_MONTH_MIN10_EVIDENCE.json for the single
# 2026-08-17T12:11Z..2026-09-16T12:11Z PAPER research window. These filters
# have no LIVE authority and retain every global safety/risk/broker veto.
ONE_MONTH_PAPER_FILTERS = {
    ("EUR_USD", "BUY"): [
        {"feature": "h1_gap_atr", "operator": ">=", "threshold": 1.58196289},
        {"feature": "m15_gap_atr", "operator": ">=", "threshold": 1.20277563},
    ],
    ("EUR_USD", "SELL"): [
        {"feature": "h1_gap_atr", "operator": ">=", "threshold": 0.81177932},
        {"feature": "session_momentum_atr", "operator": ">=", "threshold": -0.32319926},
    ],
    ("GBP_USD", "BUY"): [
        {"feature": "extension_atr", "operator": "<=", "threshold": 0.40566165},
        {"feature": "session_momentum_atr", "operator": "<=", "threshold": -0.89624724},
    ],
    ("GBP_USD", "SELL"): [
        {"feature": "h1_gap_atr", "operator": ">=", "threshold": -1.0078491},
        {"feature": "m15_gap_atr", "operator": ">=", "threshold": 0.13508471},
    ],
    ("USD_JPY", "BUY"): [
        {"feature": "m15_gap_atr", "operator": "<=", "threshold": 0.84085822},
        {"feature": "session_momentum_atr", "operator": "<=", "threshold": -1.54426238},
    ],
    ("USD_JPY", "SELL"): [
        {"feature": "rr_raw", "operator": "<=", "threshold": 1.02888889},
        {"feature": "session_strength", "operator": "<=", "threshold": 0.24427135},
    ],
    ("AUD_USD", "BUY"): [
        {"feature": "room_to_barrier_r", "operator": "<=", "threshold": 0.46666667},
        {"feature": "session_momentum_atr", "operator": "<=", "threshold": -0.68553545},
    ],
    ("AUD_USD", "SELL"): [
        {"feature": "session_displacement_atr", "operator": ">=", "threshold": -2.912},
        {"feature": "session_momentum_atr", "operator": ">=", "threshold": 1.86666667},
    ],
    ("USD_CAD", "BUY"): [
        {"feature": "rr_raw", "operator": "<=", "threshold": 0.53333333},
        {"feature": "buy_score", "operator": "<=", "threshold": 47.0},
    ],
    ("USD_CAD", "SELL"): [
        {"feature": "rr_raw", "operator": "<=", "threshold": 1.17777778},
        {"feature": "m15_gap_atr", "operator": "<=", "threshold": -2.08580719},
    ],
}


def _strategy_id(instrument: str, direction: str) -> str:
    version = "V2" if (instrument, direction) in {
        ("EUR_USD", "SELL"),
        ("USD_JPY", "SELL"),
    } else "V1"
    return f"{instrument.replace('_', '')}_{direction}_ONLY_{version}"


def _definition(instrument: str, direction: str) -> dict[str, Any]:
    filters = ONE_MONTH_PAPER_FILTERS[(instrument, direction)]
    return {
        "strategy_id": _strategy_id(instrument, direction),
        "instrument": instrument,
        "direction": direction,
        "filters": filters,
        "paper_only": True,
        "research_only": True,
        "production_authority": False,
        "auto_activation": False,
        "evidence_window": "2026-08-17T12:11:00Z/2026-09-16T12:11:00Z",
        "minimum_resolved": 10,
        "minimum_win_rate_exclusive": 0.50,
    }


STRATEGY_DEFINITIONS = {
    (instrument, direction): _definition(instrument, direction)
    for instrument in SUPPORTED_INSTRUMENTS
    for direction in SUPPORTED_DIRECTIONS
}
STRATEGY_IDS = tuple(
    STRATEGY_DEFINITIONS[(instrument, direction)]["strategy_id"]
    for instrument in SUPPORTED_INSTRUMENTS
    for direction in SUPPORTED_DIRECTIONS
)
# Historical two-month candidate retained for the standalone replay module.
# Runtime routing uses STRATEGY_DEFINITIONS and the new 30-day PAPER filters.
EURUSD_SELL_STRATEGY = {
    "strategy_id": "EURUSD_SELL_ONLY_V2",
    "instrument": "EUR_USD",
    "direction": "SELL",
    "filters": [
        {"feature": "extension_atr", "operator": ">=", "threshold": 0.9},
        {"feature": "session_strength", "operator": ">=", "threshold": 0.2},
        {"feature": "session_momentum_atr", "operator": ">=", "threshold": -0.21},
    ],
    "paper_only": True,
    "research_only": True,
    "production_authority": False,
    "auto_activation": False,
}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _normalize_instrument(value: Any) -> str:
    return str(value or "").strip().upper().replace("/", "_")


def _normalize_direction(value: Any) -> str:
    direction = str(value or "").strip().upper()
    if direction == "LONG":
        return "BUY"
    if direction == "SHORT":
        return "SELL"
    return direction


def directional_strategy_id(instrument: Any, direction: Any) -> str:
    symbol = _normalize_instrument(instrument)
    side = _normalize_direction(direction)
    definition = STRATEGY_DEFINITIONS.get((symbol, side))
    if definition is None:
        raise ValueError("unsupported directional strategy")
    return str(definition["strategy_id"])


def strategy_definition(instrument: Any, direction: Any) -> dict[str, Any]:
    symbol = _normalize_instrument(instrument)
    side = _normalize_direction(direction)
    definition = STRATEGY_DEFINITIONS.get((symbol, side))
    if definition is None:
        raise ValueError("unsupported directional strategy")
    return json.loads(_canonical_json(definition))


def candidate_definition_sha256(instrument: Any, direction: Any) -> str:
    definition = strategy_definition(instrument, direction)
    return hashlib.sha256(_canonical_json(definition).encode("utf-8")).hexdigest()


def all_strategy_definitions() -> list[dict[str, Any]]:
    return [
        {
            **strategy_definition(instrument, direction),
            "candidate_definition_sha256": candidate_definition_sha256(instrument, direction),
            "base_filter_pipeline": list(BASE_FILTER_PIPELINE),
        }
        for instrument in SUPPORTED_INSTRUMENTS
        for direction in SUPPORTED_DIRECTIONS
    ]


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def evaluate_directional_strategy(row: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate only lane-specific pre-entry filters.

    Existing core/instrument/global gates remain authoritative elsewhere in the
    runtime. Outcome, exit and future-bar fields are intentionally never read.
    """
    instrument = _normalize_instrument(row.get("instrument") or row.get("symbol"))
    direction = _normalize_direction(row.get("signal") or row.get("direction"))
    definition = STRATEGY_DEFINITIONS.get((instrument, direction))
    if definition is None:
        return {
            "eligible": False,
            "reason": "NO_DIRECTIONAL_STRATEGY",
            "instrument": instrument,
            "direction": direction,
            "strategy_id": None,
            "checks": [],
            "production_authority": False,
        }

    features = row.get("features") if isinstance(row.get("features"), Mapping) else {}
    checks = []
    for rule in definition["filters"]:
        feature = str(rule["feature"])
        operator = str(rule["operator"])
        threshold = float(rule["threshold"])
        value = _finite(features.get(feature))
        passed = False
        if value is not None:
            passed = value >= threshold if operator == ">=" else value <= threshold
        checks.append(
            {
                "feature": feature,
                "operator": operator,
                "threshold": threshold,
                "value": value,
                "passed": passed,
                "reason": None if value is not None else "REQUIRED_PRE_ENTRY_EVIDENCE_MISSING",
            }
        )

    eligible = all(check["passed"] for check in checks)
    return {
        "eligible": eligible,
        "reason": "DIRECTIONAL_FILTERS_PASS" if eligible else "DIRECTIONAL_FILTERS_REJECT",
        "instrument": instrument,
        "direction": direction,
        "strategy_id": definition["strategy_id"],
        "candidate_definition_sha256": candidate_definition_sha256(instrument, direction),
        "base_filter_pipeline": list(BASE_FILTER_PIPELINE),
        "lane_filters": json.loads(_canonical_json(definition["filters"])),
        "checks": checks,
        "paper_only": True,
        "research_only": True,
        "production_authority": False,
    }

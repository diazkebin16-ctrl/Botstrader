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


def _strategy_id(instrument: str, direction: str) -> str:
    return f"{instrument.replace('_', '')}_{direction}_ONLY_V1"


def _definition(instrument: str, direction: str) -> dict[str, Any]:
    filters: list[dict[str, Any]] = []
    if instrument == "EUR_USD" and direction == "SELL":
        filters = [
            {"feature": "extension_atr", "operator": ">=", "threshold": 0.9},
            {"feature": "session_strength", "operator": ">=", "threshold": 0.2},
        ]
    return {
        "strategy_id": _strategy_id(instrument, direction),
        "instrument": instrument,
        "direction": direction,
        "filters": filters,
        "paper_only": True,
        "research_only": True,
        "production_authority": False,
        "auto_activation": False,
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
EURUSD_SELL_STRATEGY = STRATEGY_DEFINITIONS[("EUR_USD", "SELL")]


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

"""Generated Automation V3 PAPER-only research rule surface.

Only the instrument assignment lines below are mutable by the governed V3 adapter.
Runtime code and secrets are intentionally outside this change surface.
"""
from __future__ import annotations

import json
import math
from typing import Any, Mapping

SUPPORTED_INSTRUMENTS = ("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY", "USD_CAD")
APPROVED_FEATURES = (
    "rr_raw", "room_to_barrier_r", "extension_atr", "volatility_ratio",
    "direction_edge", "session_strength", "session_displacement_atr",
    "session_momentum_atr", "h1_gap_atr", "h1_slope_atr",
    "m15_gap_atr", "m15_slope_atr",
)
APPROVED_OPERATORS = (">=", "<=")
MANAGED_RULES_JSON = {}
MANAGED_RULES_JSON["AUD_USD"] = "[]"
MANAGED_RULES_JSON["EUR_USD"] = "[]"
MANAGED_RULES_JSON["GBP_USD"] = "[]"
MANAGED_RULES_JSON["USD_JPY"] = "[]"
MANAGED_RULES_JSON["USD_CAD"] = "[]"


def managed_rule_assignment(instrument: str) -> str:
    symbol = str(instrument or "").upper()
    if symbol not in SUPPORTED_INSTRUMENTS:
        raise ValueError("unsupported instrument")
    prefix = f'MANAGED_RULES_JSON["{symbol}"] = '
    for line in __loader_text().splitlines():
        if line.startswith(prefix):
            return line + "\n"
    raise RuntimeError("managed assignment missing")


def __loader_text() -> str:
    from pathlib import Path
    return Path(__file__).read_text(encoding="utf-8")


def rules_for(instrument: str) -> list[dict[str, Any]]:
    symbol = str(instrument or "").upper()
    if symbol not in SUPPORTED_INSTRUMENTS:
        return []
    value = json.loads(MANAGED_RULES_JSON[symbol])
    if not isinstance(value, list):
        raise ValueError("managed rules must be a list")
    return value


def _finite(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def evaluate_managed_strategy_rules(row: Mapping[str, Any]) -> dict[str, Any]:
    symbol = str((row or {}).get("instrument") or "").upper()
    rules = rules_for(symbol)
    if not rules:
        return {"ok": True, "active": False, "rules": [], "vetoes": [], "instrument": symbol}
    features = (row or {}).get("features") or {}
    results = []
    vetoes = []
    for rule in rules:
        feature = str(rule.get("feature") or "")
        operator = str(rule.get("operator") or "")
        if feature not in APPROVED_FEATURES or operator not in APPROVED_OPERATORS:
            raise ValueError("managed rule outside approved surface")
        value = features.get(feature) if isinstance(features, Mapping) else None
        if value is None and feature == "rr_raw":
            value = (row or {}).get("rr_raw")
        number = _finite(value)
        threshold = _finite(rule.get("threshold"))
        passed = None if number is None or threshold is None else (number >= threshold if operator == ">=" else number <= threshold)
        item = {"source": "automation_v3_managed", "rule_key": rule.get("candidate_id"), "feature": feature,
                "operator": operator, "threshold": threshold, "passed": passed}
        results.append(item)
        if passed is False:
            vetoes.append(item)
    return {"ok": not vetoes, "active": True, "rules": results, "vetoes": vetoes, "instrument": symbol}

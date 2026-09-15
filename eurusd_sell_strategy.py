"""EUR/USD SELL-only strategy candidate.

The candidate is intentionally pure and dormant.  It cannot submit orders,
change risk, or activate itself.  Runtime integration may consume its decision
only after the existing PAPER lifecycle, portfolio, recovery, time and broker
gates have also passed.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Iterable, Mapping

from directional_strategies import (
    EURUSD_SELL_STRATEGY,
    candidate_definition_sha256 as directional_definition_sha256,
)


STRATEGY_DEFINITION = EURUSD_SELL_STRATEGY

MAX_SIMULTANEOUS_EURUSD_POSITIONS = 1


def _canonical_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def candidate_definition_sha256() -> str:
    return directional_definition_sha256("EUR_USD", "SELL")


def strategy_identity() -> dict[str, Any]:
    definition = json.loads(_canonical_json(STRATEGY_DEFINITION))
    return {
        **definition,
        "candidate_definition_sha256": candidate_definition_sha256(),
    }


def _normalize_direction(row: Mapping[str, Any]) -> str:
    value = str(row.get("signal") or row.get("direction") or "").upper()
    if value == "SHORT":
        return "SELL"
    if value == "LONG":
        return "BUY"
    return value


def _finite_feature(features: Mapping[str, Any], name: str) -> float | None:
    try:
        value = float(features.get(name))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def evaluate_sell_candidate(
    row: Mapping[str, Any], *, instrument_has_open_position: bool = False
) -> dict[str, Any]:
    """Evaluate pre-entry evidence for the frozen SELL-only candidate.

    Outcome, P/L, exit fields and future bars are deliberately never read.
    Missing required evidence blocks the candidate instead of being inferred.
    """
    instrument = str(row.get("instrument") or row.get("symbol") or "").upper().replace("/", "_")
    direction = _normalize_direction(row)
    identity = strategy_identity()
    if instrument != "EUR_USD":
        return {
            "eligible": False,
            "reason": "INSTRUMENT_NOT_ALLOWED",
            "instrument": instrument,
            "direction": direction,
            "strategy": identity,
            "checks": [],
        }
    if direction != "SELL":
        return {
            "eligible": False,
            "reason": "SELL_ONLY_DIRECTION",
            "instrument": instrument,
            "direction": direction,
            "strategy": identity,
            "checks": [],
        }
    if instrument_has_open_position:
        return {
            "eligible": False,
            "reason": "EURUSD_POSITION_ALREADY_OPEN",
            "instrument": instrument,
            "direction": direction,
            "strategy": identity,
            "checks": [],
        }

    features = row.get("features") if isinstance(row.get("features"), Mapping) else {}
    checks = []
    for rule in STRATEGY_DEFINITION["filters"]:
        feature = str(rule["feature"])
        value = _finite_feature(features, feature)
        threshold = float(rule["threshold"])
        passed = value is not None and value >= threshold
        checks.append(
            {
                "feature": feature,
                "operator": ">=",
                "threshold": threshold,
                "value": value,
                "passed": passed,
                "reason": None if value is not None else "REQUIRED_PRE_ENTRY_EVIDENCE_MISSING",
            }
        )
    eligible = all(check["passed"] for check in checks)
    return {
        "eligible": eligible,
        "reason": "SELL_FILTERS_PASS" if eligible else "SELL_FILTERS_REJECT",
        "instrument": instrument,
        "direction": direction,
        "strategy": identity,
        "checks": checks,
    }


def route_eurusd_directional_lane(
    row: Mapping[str, Any], *, buy_lane_eligible: bool,
    buy_strategy_id: str, instrument_has_open_position: bool = False
) -> dict[str, Any]:
    """Select at most one EUR/USD strategy lane for a directional signal.

    BUY remains assigned to the existing strategy and SELL is assigned solely
    to this new candidate.  The shared open-position check blocks both lanes.
    """
    instrument = str(row.get("instrument") or row.get("symbol") or "").upper().replace("/", "_")
    direction = _normalize_direction(row)
    if instrument != "EUR_USD":
        return {
            "selected": None,
            "reason": "INSTRUMENT_NOT_ALLOWED",
            "max_simultaneous_positions": MAX_SIMULTANEOUS_EURUSD_POSITIONS,
        }
    if instrument_has_open_position:
        return {
            "selected": None,
            "reason": "EURUSD_POSITION_ALREADY_OPEN",
            "max_simultaneous_positions": MAX_SIMULTANEOUS_EURUSD_POSITIONS,
        }
    if direction == "BUY":
        return {
            "selected": buy_strategy_id if buy_lane_eligible else None,
            "reason": "BUY_LANE_SELECTED" if buy_lane_eligible else "BUY_LANE_REJECTED",
            "max_simultaneous_positions": MAX_SIMULTANEOUS_EURUSD_POSITIONS,
        }
    if direction == "SELL":
        result = evaluate_sell_candidate(row)
        return {
            "selected": STRATEGY_DEFINITION["strategy_id"] if result["eligible"] else None,
            "reason": "SELL_LANE_SELECTED" if result["eligible"] else result["reason"],
            "sell_evaluation": result,
            "max_simultaneous_positions": MAX_SIMULTANEOUS_EURUSD_POSITIONS,
        }
    return {
        "selected": None,
        "reason": "NO_DIRECTIONAL_SIGNAL",
        "max_simultaneous_positions": MAX_SIMULTANEOUS_EURUSD_POSITIONS,
    }


def canonical_dataset_sha256(rows: Iterable[Mapping[str, Any]]) -> str:
    material = [dict(row) for row in rows]
    return hashlib.sha256(_canonical_json(material).encode("utf-8")).hexdigest()


def backtest_sell_candidate(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Score resolved SELL observations after making each pre-entry decision."""
    observations = list(rows)
    accepted = []
    for row in observations:
        # The frozen evidence stores instrument/direction once at dataset level;
        # explicit per-row values, when present, still take precedence and are
        # validated by the same runtime evaluator.
        decision_row = {"instrument": "EUR_USD", "direction": "SELL", **row}
        decision = evaluate_sell_candidate(decision_row)
        if decision["eligible"]:
            accepted.append(row)

    def metrics(items: list[Mapping[str, Any]]) -> dict[str, Any]:
        wins = sum(str(item.get("outcome")).upper() == "WIN" for item in items)
        losses = sum(str(item.get("outcome")).upper() == "LOSS" for item in items)
        resolved = wins + losses
        return {
            "trades": len(items),
            "win": wins,
            "loss": losses,
            "win_rate": wins / resolved if resolved else None,
            "net_result": round(sum(float(item.get("net_result") or 0.0) for item in items), 8),
        }

    baseline = metrics(observations)
    candidate = metrics(accepted)
    accepted_ids = {str(row.get("trade_id")) for row in accepted}
    return {
        "baseline": baseline,
        "candidate": candidate,
        "blocked_win": sum(
            str(row.get("outcome")).upper() == "WIN" and str(row.get("trade_id")) not in accepted_ids
            for row in observations
        ),
        "blocked_loss": sum(
            str(row.get("outcome")).upper() == "LOSS" and str(row.get("trade_id")) not in accepted_ids
            for row in observations
        ),
        "accepted_trade_ids": [str(row.get("trade_id")) for row in accepted],
    }


from __future__ import annotations
from typing import Dict, Set

ORDER_STATES = (
    "CREATED",
    "SUBMITTING",
    "SUBMITTED",
    "ACKNOWLEDGED",
    "PARTIALLY_FILLED",
    "FILLED",
    "REJECTED",
    "CANCEL_PENDING",
    "CANCELLED",
    "EXPIRED",
    "UNKNOWN",
)

TERMINAL_STATES = {"FILLED", "REJECTED", "CANCELLED", "EXPIRED"}

# Out-of-order broker evidence is intentionally allowed in a few places:
# a FILL may be received/reconciled before an ACK was observed locally.
ALLOWED_TRANSITIONS: Dict[str, Set[str]] = {
    "CREATED": {"SUBMITTING", "REJECTED", "UNKNOWN"},
    "SUBMITTING": {"SUBMITTED", "ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCELLED", "EXPIRED", "UNKNOWN"},
    "SUBMITTED": {"ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCEL_PENDING", "CANCELLED", "EXPIRED", "UNKNOWN"},
    "ACKNOWLEDGED": {"PARTIALLY_FILLED", "FILLED", "CANCEL_PENDING", "CANCELLED", "EXPIRED", "UNKNOWN"},
    "PARTIALLY_FILLED": {"FILLED", "CANCEL_PENDING", "CANCELLED", "EXPIRED", "UNKNOWN"},
    "CANCEL_PENDING": {"CANCELLED", "PARTIALLY_FILLED", "FILLED", "UNKNOWN"},
    "UNKNOWN": {"SUBMITTED", "ACKNOWLEDGED", "PARTIALLY_FILLED", "FILLED", "REJECTED", "CANCELLED", "EXPIRED", "UNKNOWN"},
    "FILLED": set(),
    "REJECTED": set(),
    "CANCELLED": set(),
    "EXPIRED": set(),
}


def can_transition(current: str, new: str) -> bool:
    current = str(current or "").upper()
    new = str(new or "").upper()
    if current == new:
        return True
    return new in ALLOWED_TRANSITIONS.get(current, set())


def transition(current: str, new: str) -> str:
    current = str(current or "").upper()
    new = str(new or "").upper()
    if not can_transition(current, new):
        raise ValueError(f"Invalid order-state transition: {current} -> {new}")
    return new


def is_terminal(state: str) -> bool:
    return str(state or "").upper() in TERMINAL_STATES


def conservative_filled_units(requested_units: float, broker_units: float | None) -> tuple[float, float]:
    requested = abs(float(requested_units or 0.0))
    if broker_units is None:
        return 0.0, requested
    filled = min(requested, abs(float(broker_units)))
    remaining = max(0.0, requested - filled)
    return filled, remaining

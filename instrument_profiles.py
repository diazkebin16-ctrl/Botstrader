from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, FrozenSet


@dataclass(frozen=True)
class InstrumentProfile:
    """Execution-authority and instrument-specific strategy exceptions.

    Global safety/strategy behavior stays in server.py.  This profile contains
    only permissions or exceptions that must not leak from one instrument to
    another.
    """

    symbol: str
    paper_execution_allowed: bool
    live_execution_allowed: bool
    specific_vetoes: FrozenSet[str] = frozenset()
    specific_exceptions: FrozenSet[str] = frozenset()
    learned_research_veto_authority: bool = False

    def allows_execution(self, trading_environment: str, oanda_environment: str) -> bool:
        env = str(trading_environment or "").strip().upper()
        broker_env = str(oanda_environment or "").strip().lower()
        if env == "PAPER":
            return self.paper_execution_allowed and broker_env == "practice"
        if env in {"TEST", "INTEGRATION_TEST", "SIMULATION"}:
            # Logical execution mode for deterministic tests/replay only. server.py's
            # EARLY_TEST_MODE independently forces the practice endpoint and tests do
            # not gain LIVE broker authority from this profile decision.
            return self.paper_execution_allowed
        if env == "PRODUCTION":
            return self.live_execution_allowed and broker_env == "live"
        return False

    def has_veto(self, name: str) -> bool:
        return str(name or "").upper() in self.specific_vetoes

    def has_exception(self, name: str) -> bool:
        return str(name or "").upper() in self.specific_exceptions


# The EUR-only entries below are the forward-derived exceptions/vetoes that
# must not be inherited by a new instrument simply because it shares the same
# strategy engine.  Global strategy geometry (minimum_rr, barrier_room_ok,
# direction score, extension limits, etc.) intentionally does NOT live here.
INSTRUMENT_PROFILES: Dict[str, InstrumentProfile] = {
    "EUR_USD": InstrumentProfile(
        symbol="EUR_USD",
        paper_execution_allowed=True,
        live_execution_allowed=True,  # existing EUR production path is preserved
        specific_vetoes=frozenset({"LOW_ROOM_LOW_RR", "LOW_ROOM_EXTENDED"}),
        specific_exceptions=frozenset({"M1_ALTERNATIVE_ADMISSION"}),
        learned_research_veto_authority=True,
    ),
    "GBP_USD": InstrumentProfile(
        symbol="GBP_USD",
        paper_execution_allowed=True,
        live_execution_allowed=False,
    ),
    "USD_JPY": InstrumentProfile(
        symbol="USD_JPY",
        paper_execution_allowed=True,
        live_execution_allowed=False,
    ),
    "AUD_USD": InstrumentProfile(
        symbol="AUD_USD",
        paper_execution_allowed=True,
        live_execution_allowed=False,
    ),
    "USD_CAD": InstrumentProfile(
        symbol="USD_CAD",
        paper_execution_allowed=True,
        live_execution_allowed=False,
    ),
}


def normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper().replace("/", "_")


def instrument_profile(symbol: str) -> InstrumentProfile:
    key = normalize_symbol(symbol)
    if key in INSTRUMENT_PROFILES:
        return INSTRUMENT_PROFILES[key]
    # Unknown instruments are intentionally inert until explicitly profiled.
    return InstrumentProfile(key, paper_execution_allowed=False, live_execution_allowed=False)

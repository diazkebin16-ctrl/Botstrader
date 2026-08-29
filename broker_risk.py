from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple


@dataclass(frozen=True)
class BrokerAccountSnapshot:
    broker: str
    net_liquidation: Optional[float] = None
    available_funds: Optional[float] = None
    excess_liquidity: Optional[float] = None
    init_margin_req: Optional[float] = None
    maint_margin_req: Optional[float] = None
    buying_power: Optional[float] = None
    source: str = "UNVERIFIED"

    @property
    def verified(self) -> bool:
        return self.source.upper() in {"OANDA", "IBKR"}


@dataclass(frozen=True)
class IbkrAccountSnapshot:
    net_liquidation: Optional[float] = None
    available_funds: Optional[float] = None
    excess_liquidity: Optional[float] = None
    initial_margin_requirement: Optional[float] = None
    maintenance_margin_requirement: Optional[float] = None
    buying_power: Optional[float] = None
    currency: Optional[str] = None
    timestamp: Optional[str] = None
    verified: bool = False


@dataclass(frozen=True)
class IbkrWhatIfMarginResult:
    instrument: str
    quantity: Optional[float] = None
    side: Optional[str] = None
    initial_margin_before: Optional[float] = None
    initial_margin_after: Optional[float] = None
    initial_margin_change: Optional[float] = None
    maintenance_margin_before: Optional[float] = None
    maintenance_margin_after: Optional[float] = None
    maintenance_margin_change: Optional[float] = None
    available_funds_after: Optional[float] = None
    excess_liquidity_after: Optional[float] = None
    verified: bool = False
    timestamp: Optional[str] = None


@dataclass(frozen=True)
class BrokerInstrumentMinimum:
    instrument: str
    minimum_quantity: Optional[float] = None
    quantity_increment: Optional[float] = None
    verification_status: str = "UNVERIFIED"
    source: str = "UNVERIFIED"

    @property
    def verified(self) -> bool:
        return self.verification_status.upper() == "VERIFIED" and self.source.upper() == "IBKR"


@dataclass(frozen=True)
class BrokerRiskVerdict:
    allow: bool
    broker: str
    reasons: Tuple[str, ...]
    details: Dict[str, Any]

    def as_dict(self) -> Dict[str, Any]:
        return {"allow": self.allow, "broker": self.broker, "reasons": list(self.reasons), "details": dict(self.details)}


class BrokerRiskAdapter:
    broker_name = "UNKNOWN"
    execution_authority = False

    def prospective_check(self, candidate: Mapping[str, Any], selected: Sequence[Mapping[str, Any]], context: Mapping[str, Any]) -> BrokerRiskVerdict:
        return BrokerRiskVerdict(False, self.broker_name, ("BROKER_ADAPTER_NO_AUTHORITY",), {})


class OandaBrokerRiskAdapter(BrokerRiskAdapter):
    broker_name = "OANDA"
    execution_authority = True

    def prospective_check(self, candidate: Mapping[str, Any], selected: Sequence[Mapping[str, Any]], context: Mapping[str, Any]) -> BrokerRiskVerdict:
        reasons = []
        if str(context.get("environment") or "").upper() not in {"PAPER", "TEST", "INTEGRATION_TEST", "SIMULATION"}:
            reasons.append("OANDA_PAPER_ONLY_FOR_MULTI_ASSET_SELECTOR")
        if not bool(context.get("instrument_execution_allowed")):
            reasons.append("INSTRUMENT_EXECUTION_NOT_ALLOWED")
        if bool(context.get("secondary_instrument")) and not bool(context.get("metadata_verified")):
            reasons.append("BROKER_METADATA_UNVERIFIED")
        if context.get("available_margin_ok") is False:
            reasons.append("BROKER_MARGIN_INSUFFICIENT")
        return BrokerRiskVerdict(not reasons, self.broker_name, tuple(reasons), {
            "metadata_verified": bool(context.get("metadata_verified")),
            "available_margin_ok": context.get("available_margin_ok"),
        })


class IbkrBrokerRiskAdapter(BrokerRiskAdapter):
    broker_name = "IBKR"
    execution_authority = False
    REQUIRED_ACCOUNT_FIELDS = (
        "NetLiquidation", "AvailableFunds", "ExcessLiquidity",
        "InitMarginReq", "MaintMarginReq",
    )

    def prospective_check(self, candidate: Mapping[str, Any], selected: Sequence[Mapping[str, Any]], context: Mapping[str, Any]) -> BrokerRiskVerdict:
        # V3.37.0 deliberately has no IBKR connection and no execution authority.
        # Environment variables, synthetic fields, or fallback metadata cannot
        # promote this adapter. A future connected version must consume real IBKR
        # account data plus an order-specific what-if margin result.
        supplied = {k: context.get(k) for k in self.REQUIRED_ACCOUNT_FIELDS}
        return BrokerRiskVerdict(False, self.broker_name, (
            "IBKR_ADAPTER_INACTIVE",
            "IBKR_EXECUTION_AUTHORITY_FALSE",
            "IBKR_BROKER_DATA_UNVERIFIED",
            "IBKR_WHAT_IF_MARGIN_REQUIRED",
        ), {
            "execution_authority": False,
            "required_account_fields": list(self.REQUIRED_ACCOUNT_FIELDS),
            "provided_account_fields": supplied,
            "requires_prospective_margin_impact": True,
            "requires_verified_instrument_minimums": True,
            "account_snapshot_contract": IbkrAccountSnapshot.__name__,
            "what_if_margin_contract": IbkrWhatIfMarginResult.__name__,
            "instrument_minimum_contract": BrokerInstrumentMinimum.__name__,
            "synthetic_data_grants_authority": False,
            "fixed_margin_rate_authority": False,
        })


def broker_adapter(name: str) -> BrokerRiskAdapter:
    key = str(name or "").strip().upper()
    if key == "OANDA":
        return OandaBrokerRiskAdapter()
    if key == "IBKR":
        return IbkrBrokerRiskAdapter()
    return BrokerRiskAdapter()

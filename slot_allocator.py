from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from opportunity_ranker import RankedOpportunity, rank_opportunities


@dataclass(frozen=True)
class CapitalTier:
    minimum_nlv: float
    max_slots: int
    name: str


CAPITAL_TIERS: Sequence[CapitalTier] = (
    CapitalTier(0.0, 1, "NLV_LT_5000"),
    CapitalTier(5000.0, 2, "NLV_GTE_5000"),
)


def slot_policy(nlv: float) -> Dict[str, Any]:
    nlv = max(0.0, float(nlv or 0.0))
    chosen = CAPITAL_TIERS[0]
    for tier in CAPITAL_TIERS:
        if nlv >= tier.minimum_nlv:
            chosen = tier
    return {
        "nlv": nlv,
        "tier": chosen.name,
        "max_slots": chosen.max_slots,
        "policy_limit_only": True,
        "broker_may_reduce": True,
    }


def allocate_slots(
    candidates: Iterable[Mapping[str, Any]],
    *,
    nlv: float,
    open_positions: int = 0,
    broker_guard: Optional[Callable[[Mapping[str, Any], Sequence[Mapping[str, Any]]], Mapping[str, Any]]] = None,
    portfolio_guard: Optional[Callable[[Mapping[str, Any], Sequence[Mapping[str, Any]]], Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    ranked: List[RankedOpportunity] = rank_opportunities(candidates)
    policy = slot_policy(nlv)
    slots_available = max(0, int(policy["max_slots"]) - max(0, int(open_positions or 0)))
    selected: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    for item in ranked:
        candidate = item.candidate
        if len(selected) >= slots_available:
            rejected.append({"instrument": item.instrument, "reason": "NO_SLOT_AVAILABLE", "rank_score": item.rank_score})
            continue

        if broker_guard is not None:
            verdict = dict(broker_guard(candidate, selected) or {})
            if not verdict.get("allow", False):
                rejected.append({"instrument": item.instrument, "reason": "BROKER_RISK_REJECTED", "details": verdict, "rank_score": item.rank_score})
                continue

        if portfolio_guard is not None:
            verdict = dict(portfolio_guard(candidate, selected) or {})
            if not verdict.get("allow", False):
                rejected.append({"instrument": item.instrument, "reason": "PORTFOLIO_RISK_REJECTED", "details": verdict, "rank_score": item.rank_score})
                continue

        selected.append({
            **candidate,
            "opportunity_rank_score": item.rank_score,
            "opportunity_rank_components": item.components,
            "selection_reason": "highest_ranked_candidate_passing_all_hard_guards",
        })

    return {
        "policy": policy,
        "slots_available": slots_available,
        "selected": selected,
        "rejected": rejected,
        "ranking": [
            {"rank": i + 1, "instrument": x.instrument, "rank_score": x.rank_score, "components": x.components}
            for i, x in enumerate(ranked)
        ],
    }

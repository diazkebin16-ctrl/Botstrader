from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple


@dataclass(frozen=True)
class RankedOpportunity:
    instrument: str
    rank_score: float
    components: Dict[str, float]
    reasons: Tuple[str, ...]
    candidate: Dict[str, Any]


# Conservative, non-optimized policy. Signal confidence and the strategy's own
# quality score dominate. RR/room/cost inputs are bounded so they cannot turn
# the ranker into a new strategy or reward extreme values.
_RANK_WEIGHTS = {
    "signal_quality": 0.45,
    "confidence": 0.35,
    "rr_quality": 0.10,
    "room_quality": 0.05,
    "cost_quality": 0.05,
}


def _finite_float(value: Any, default: float | None = None) -> float | None:
    """Return a finite float or a conservative default.

    NaN/inf/malformed inputs are never allowed to propagate into ranking math.
    """
    try:
        x = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return x if math.isfinite(x) else default


def _clamp01(value: Any, default: float = 0.0) -> float:
    x = _finite_float(value, None)
    if x is None:
        fallback = _finite_float(default, 0.0)
        x = 0.0 if fallback is None else fallback
    return max(0.0, min(1.0, x))


def _first_finite(candidate: Mapping[str, Any], names: Sequence[str], default: float = 0.0) -> float:
    for name in names:
        if name not in candidate:
            continue
        value = _finite_float(candidate.get(name), None)
        return default if value is None else value
    return default


def _normalized_components(candidate: Mapping[str, Any]) -> Tuple[Dict[str, float], Tuple[str, ...]]:
    # Critical comparison fields: corrupt/non-finite values invalidate only this
    # candidate. Missing dynamic_confidence may conservatively fall back to the
    # finite strategy score for backwards compatibility; an explicitly supplied
    # malformed/non-finite confidence is not treated as missing.
    raw_score = candidate.get("score")
    score_f = _finite_float(raw_score, None)
    invalid: List[str] = []
    if score_f is None:
        invalid.append("INVALID_SIGNAL_QUALITY")
        score = 0.0
    else:
        score = _clamp01(score_f / 100.0)

    if "dynamic_confidence" in candidate:
        confidence_f = _finite_float(candidate.get("dynamic_confidence"), None)
        if confidence_f is None:
            invalid.append("INVALID_CONFIDENCE")
            confidence = 0.0
        else:
            confidence = _clamp01(confidence_f)
    elif "confidence" in candidate:
        confidence_f = _finite_float(candidate.get("confidence"), None)
        if confidence_f is None:
            invalid.append("INVALID_CONFIDENCE")
            confidence = 0.0
        else:
            confidence = _clamp01(confidence_f)
    else:
        confidence = score

    # Optional fields use conservative fallbacks that cannot improve a corrupt
    # candidate: RR/room -> zero quality; spread/cost -> worst quality.
    rr = max(0.0, _first_finite(candidate, ("rr_raw", "rr"), 0.0))
    rr_quality = min(1.0, rr / 2.0)
    room = max(0.0, _first_finite(candidate, ("room_to_barrier_r", "barrier_room_r"), 0.0))
    room_quality = min(1.0, room / 2.0)
    spread = _first_finite(candidate, ("spread_pips", "spread"), math.inf)
    cost_quality = 0.0 if not math.isfinite(spread) else max(0.0, min(1.0, 1.0 - max(0.0, spread) / 5.0))

    components = {
        "signal_quality": score,
        "confidence": confidence,
        "rr_quality": rr_quality,
        "room_quality": room_quality,
        "cost_quality": cost_quality,
    }
    if not all(math.isfinite(v) for v in components.values()):
        invalid.append("NON_FINITE_COMPONENT")
    return components, tuple(dict.fromkeys(invalid))


def opportunity_rank_score(candidate: Mapping[str, Any]) -> Tuple[float, Dict[str, float], Tuple[str, ...]]:
    components, invalid = _normalized_components(candidate)
    if invalid:
        return 0.0, components, tuple(f"invalid={reason}" for reason in invalid)
    weighted = sum(components[k] * w for k, w in _RANK_WEIGHTS.items())
    if not math.isfinite(weighted):
        return 0.0, components, ("invalid=NON_FINITE_RANK_SCORE",)
    score = round(weighted * 100.0, 6)
    reasons = tuple(
        f"{name}={components[name]:.4f}*{weight:.2f}"
        for name, weight in _RANK_WEIGHTS.items()
    )
    return score, components, reasons


def rank_opportunities(candidates: Iterable[Mapping[str, Any]]) -> List[RankedOpportunity]:
    ranked: List[RankedOpportunity] = []
    for raw in candidates:
        try:
            candidate = dict(raw)
        except (TypeError, ValueError):
            continue
        instrument = str(candidate.get("instrument") or "").strip().upper().replace("/", "_")
        score, components, reasons = opportunity_rank_score(candidate)
        if any(reason.startswith("invalid=") for reason in reasons):
            continue
        if not math.isfinite(score):
            continue
        ranked.append(RankedOpportunity(instrument, score, components, reasons, candidate))
    # Instrument is the deterministic tie-break. Input/scan order is irrelevant.
    ranked.sort(key=lambda item: (-item.rank_score, item.instrument))
    return ranked


def ranking_snapshot(candidates: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    ranked = rank_opportunities(candidates)
    return {
        "policy": "V3370_CONSERVATIVE_PRE_ENTRY_V1",
        "weights": dict(_RANK_WEIGHTS),
        "look_ahead": False,
        "ranking": [
            {
                "rank": idx + 1,
                "instrument": item.instrument,
                "rank_score": item.rank_score,
                "components": item.components,
                "reasons": list(item.reasons),
            }
            for idx, item in enumerate(ranked)
        ],
    }

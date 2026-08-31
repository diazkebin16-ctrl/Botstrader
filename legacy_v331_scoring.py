"""Pure legacy V331 directional scoring formula shared by replay and runtime."""
from __future__ import annotations

from typing import Any, Mapping, Tuple


def legacy_v331_score(components: Mapping[str, Any]) -> float:
    x = components
    s = 0.0
    s += 16 if bool(x.get("h1_support")) else (-10 if bool(x.get("h1_opposes")) else 3)
    s += 20 if bool(x.get("m15_support")) else (-12 if bool(x.get("m15_opposes")) else 4)
    s += 18 if bool(x.get("m5_structure")) else (7 if bool(x.get("m5_momentum")) else 0)
    s += 16 if bool(x.get("confirm")) else (6 if bool(x.get("m1_momentum")) else 0)
    s += 8 if bool(x.get("second")) else (4 if int(x.get("pc", 0) or 0) >= 1 and bool(x.get("pr")) else 0)
    rr_raw = float(x.get("rr_raw", 0) or 0)
    min_rr = float(x.get("min_rr", 0) or 0)
    s += 8 if rr_raw >= 2 else (6 if rr_raw >= min_rr else 0)
    vol = float(x.get("vol", 0) or 0)
    s += 5 if 0.65 <= vol <= 2 else 0
    ext = float(x.get("ext", 0) or 0)
    s += 5 if ext <= 1.20 else (2 if ext <= 1.60 else 0)
    s += 4 if bool(x.get("session_ok")) else 0
    s += min(6, 2 * int(x.get("broken", 0) or 0))
    return float(max(0.0, min(100.0, s)))


def choose_legacy_v331_direction(buy_score: float, sell_score: float) -> Tuple[str, float]:
    """Historical V331 tie semantics: BUY wins ties."""
    buy = float(buy_score)
    sell = float(sell_score)
    if buy >= sell:
        return "BUY", buy
    return "SELL", sell

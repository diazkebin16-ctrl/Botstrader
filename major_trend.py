"""Causal H1/H4 regime shared by research and PAPER execution.

Thresholds are fixed before outcome selection. Each call sees only closed bars.
H4 has twice H1's weight; disagreement weakens or neutralizes the bias.
"""
from datetime import datetime, timedelta, timezone
import math

REGIMES = ("DOWN", "WEAK_DOWN", "LATERAL", "WEAK_UP", "UP")
BIAS = dict(zip(REGIMES, (-1.0, -0.5, 0.0, 0.5, 1.0)))
TREND_VERSION = "H1_H4_EMA20_50_ATR14_V1"


def utc(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)


def _ema(values, period):
    result = [values[0]]
    alpha = 2 / (period + 1)
    for value in values[1:]:
        result.append(alpha * value + (1 - alpha) * result[-1])
    return result


def timeframe_trend(rows, hours, decision_time):
    closed = [r for r in rows if utc(r["t"]) + timedelta(hours=hours) <= utc(decision_time)][-140:]
    if len(closed) < 55:
        return {"available": False}
    # At a market reopening, the last completed H4 can be from Friday.
    # It remains the last known context until a new H4 closes; freshness of
    # executable M1 prices is enforced separately by the runtime.
    values = [float(r["c"]) for r in closed]
    fast, slow = _ema(values, 20), _ema(values, 50)
    ranges = [max(float(closed[i]["h"]) - float(closed[i]["l"]),
                  abs(float(closed[i]["h"]) - values[i-1]),
                  abs(float(closed[i]["l"]) - values[i-1])) for i in range(1, len(closed))]
    atr = sum(ranges[-14:]) / 14
    if not math.isfinite(atr) or atr <= 0 or not all(math.isfinite(x) for x in values):
        return {"available": False}
    gap, slope = (fast[-1] - slow[-1]) / atr, (fast[-1] - fast[-4]) / atr
    score = 0.0
    if gap * slope > 0 and abs(gap) >= 0.15 and abs(slope) >= 0.03:
        score = math.copysign(1.0 if abs(gap) >= 0.5 and abs(slope) >= 0.10 else 0.5, gap)
    return {"available": True, "score": score, "gap_atr": gap, "slope_atr": slope,
            "last_closed_bar": utc(closed[-1]["t"]).isoformat()}


def major_trend(h1, h4, decision_time):
    hourly = timeframe_trend(h1, 1, decision_time)
    four_hourly = timeframe_trend(h4, 4, decision_time)
    result = {"version": TREND_VERSION, "h1": hourly, "h4": four_hourly,
              "available": hourly["available"] and four_hourly["available"]}
    if not result["available"]:
        return {**result, "regime": "UNKNOWN", "bias": None}
    h, f = hourly["score"], four_hourly["score"]
    weighted = (h + 2 * f) / 3
    if h == f and abs(h) == 1:
        regime = "UP" if h > 0 else "DOWN"
    elif weighted >= 0.25:
        regime = "WEAK_UP"
    elif weighted <= -0.25:
        regime = "WEAK_DOWN"
    else:
        regime = "LATERAL"
    return {**result, "regime": regime, "bias": BIAS[regime]}


def trend_features(trend):
    return {"major_trend_regime": trend["regime"], "major_trend_bias": trend["bias"],
            "major_trend_version": TREND_VERSION,
            "h4_gap_atr": trend["h4"].get("gap_atr"),
            "h4_slope_atr": trend["h4"].get("slope_atr")}


def directional_targets(exposure):
    """Allocate 54 outcomes by market-time exposure, never by winners or direction.

    Strong regimes use 36/18; weak regimes 31.5/22.5; lateral 27/27.
    Largest-remainder rounding preserves exactly 54 total across all cells.
    A reversal therefore swaps the favored side within its own regime.
    """
    total = sum(exposure.get(r, 0) for r in REGIMES)
    if total <= 0:
        raise ValueError("No eligible market-time exposure with known H1/H4 trend")
    exact = {(side, regime): (27 + (9 if side == "BUY" else -9) * BIAS[regime])
             * exposure.get(regime, 0) / total for side in ("BUY", "SELL") for regime in REGIMES}
    counts = {key: math.floor(value) for key, value in exact.items()}
    ranked = sorted(exact, key=lambda key: (-(exact[key] - counts[key]), key))
    for key in ranked[:54 - sum(counts.values())]:
        counts[key] += 1
    return {side: {regime: counts[(side, regime)] for regime in REGIMES} for side in ("BUY", "SELL")}

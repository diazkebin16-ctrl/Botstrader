from __future__ import annotations

from datetime import datetime, time
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

TOKYO = ZoneInfo("Asia/Tokyo")
LONDON = ZoneInfo("Europe/London")
NEW_YORK = ZoneInfo("America/New_York")


def _in_window(dt: datetime, tz: ZoneInfo, start: time, end: time) -> bool:
    local = dt.astimezone(tz)
    t = local.timetz().replace(tzinfo=None)
    return local.weekday() < 5 and start <= t <= end


def active_session(dt: datetime) -> str:
    """Return the dominant intraday FX session.

    Overlaps intentionally prefer New York over London and London over Asia,
    because the newer liquidity center should drive an intraday execution model.
    """
    if _in_window(dt, NEW_YORK, time(8, 0), time(17, 0)):
        return "NEW_YORK"
    if _in_window(dt, LONDON, time(8, 0), time(16, 30)):
        return "LONDON"
    if _in_window(dt, TOKYO, time(9, 0), time(15, 0)):
        return "ASIA"
    return "OFF_SESSION"


def _same_session_date(ts: datetime, ref: datetime, session: str) -> bool:
    tz = NEW_YORK if session == "NEW_YORK" else LONDON if session == "LONDON" else TOKYO
    return ts.astimezone(tz).date() == ref.astimezone(tz).date()


def _session_start_ok(ts: datetime, session: str) -> bool:
    if session == "NEW_YORK":
        local = ts.astimezone(NEW_YORK)
        return local.timetz().replace(tzinfo=None) >= time(8, 0)
    if session == "LONDON":
        local = ts.astimezone(LONDON)
        return local.timetz().replace(tzinfo=None) >= time(8, 0)
    if session == "ASIA":
        local = ts.astimezone(TOKYO)
        return local.timetz().replace(tzinfo=None) >= time(9, 0)
    return False


def session_regime(m5: List[Dict[str, Any]], atr_value: float) -> Dict[str, Any]:
    """Measure the trend developed by the *current session*, not the macro trend.

    Uses only bars available up to the current M5 candle. Direction requires both
    meaningful displacement from the session open and agreement from recent
    structure/momentum. It returns NEUTRAL when evidence is weak.
    """
    if not m5:
        return {"session":"OFF_SESSION","direction":"NEUTRAL","strength":0.0,"bars":0,
                "displacement_atr":0.0,"structure_bias":0.0,"momentum_atr":0.0}
    now = m5[-1]["t"]
    session = active_session(now)
    if session == "OFF_SESSION":
        return {"session":session,"direction":"NEUTRAL","strength":0.0,"bars":0,
                "displacement_atr":0.0,"structure_bias":0.0,"momentum_atr":0.0}

    bars=[x for x in m5 if _same_session_date(x["t"],now,session) and _session_start_ok(x["t"],session) and x["t"]<=now]
    if len(bars) < 4:
        return {"session":session,"direction":"NEUTRAL","strength":0.0,"bars":len(bars),
                "displacement_atr":0.0,"structure_bias":0.0,"momentum_atr":0.0}

    atr=max(float(atr_value or 0.0),1e-12)
    session_open=float(bars[0]["o"])
    close=float(bars[-1]["c"])
    displacement=(close-session_open)/atr

    recent=bars[-min(12,len(bars)):]
    momentum=(float(recent[-1]["c"])-float(recent[0]["c"]))/atr if len(recent)>=2 else 0.0

    half=max(2,len(bars)//2)
    first=bars[:half]
    second=bars[-half:]
    first_high=max(float(x["h"]) for x in first); first_low=min(float(x["l"]) for x in first)
    second_high=max(float(x["h"]) for x in second); second_low=min(float(x["l"]) for x in second)
    bullish_structure=second_high>first_high and second_low>first_low
    bearish_structure=second_high<first_high and second_low<first_low
    structure_bias=1.0 if bullish_structure else -1.0 if bearish_structure else 0.0

    bullish_votes=int(displacement>=0.35)+int(momentum>=0.20)+int(bullish_structure)
    bearish_votes=int(displacement<=-0.35)+int(momentum<=-0.20)+int(bearish_structure)
    if bullish_votes>=2 and bullish_votes>bearish_votes:
        direction="BUY"
    elif bearish_votes>=2 and bearish_votes>bullish_votes:
        direction="SELL"
    else:
        direction="NEUTRAL"

    strength=min(1.0,0.45*min(abs(displacement)/1.25,1.0)+0.30*min(abs(momentum)/0.75,1.0)+0.25*abs(structure_bias))
    if direction=="NEUTRAL":
        strength*=0.5
    return {"session":session,"direction":direction,"strength":strength,"bars":len(bars),
            "displacement_atr":displacement,"structure_bias":structure_bias,"momentum_atr":momentum,
            "session_open":session_open,"last_close":close}

"""Fixed operational time contract shared by historical research.

Research-only helpers mirror the production schedule without importing server.py.
The schedule is not candidate-tunable and is intentionally excluded from the
Phase 1/Phase 2 researchable gate surface.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
ENTRY_BLOCKED_WINDOWS = ((7 * 60, 10 * 60), (15 * 60, 19 * 60))
OPERATIONAL_CLOSE_MINUTE = 16 * 60 + 50
NON_RESEARCHABLE_OPERATIONAL_GATES = (
    "NY_ENTRY_BLACKOUT_07_10",
    "NY_ENTRY_BLACKOUT_15_19",
)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def fixed_entry_gate(at: datetime) -> Dict[str, Any]:
    """Return fixed NEW-entry eligibility using America/New_York DST rules."""
    ny = _aware(at).astimezone(NY)
    mins = ny.hour * 60 + ny.minute
    morning = 7 * 60 <= mins < 10 * 60
    afternoon = 15 * 60 <= mins < 19 * 60
    if morning:
        reason = "NY_ENTRY_BLACKOUT_07_10"
        window = "07:00-10:00 America/New_York"
    elif afternoon:
        reason = "NY_ENTRY_BLACKOUT_15_19"
        window = "15:00-19:00 America/New_York"
    else:
        reason = "ALLOWED"
        window = None
    return {
        "allowed": not (morning or afternoon),
        "reason": reason,
        "ny_time": ny.isoformat(),
        "window": window,
        "blocked_windows": ["07:00-10:00", "15:00-19:00"],
        "timezone": "America/New_York",
        "researchable": False,
    }


def planned_entry_time(signal_candle_start: datetime, latency_bars: int = 0) -> datetime:
    """First executable M1 open after a completed signal candle."""
    return _aware(signal_candle_start).astimezone(timezone.utc) + timedelta(minutes=1 + max(0, int(latency_bars)))


def operational_close_after(entry_at: datetime) -> datetime:
    """First weekday 16:50 ET operational close strictly after an entry time."""
    ny = _aware(entry_at).astimezone(NY)
    candidate = ny.replace(hour=16, minute=50, second=0, microsecond=0)
    if ny.weekday() < 5 and ny < candidate:
        return candidate.astimezone(timezone.utc)
    day = (ny + timedelta(days=1)).date()
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return datetime(day.year, day.month, day.day, 16, 50, tzinfo=NY).astimezone(timezone.utc)


def minutes_needed_through_close(signal_candle_start: datetime, latency_bars: int = 0) -> int:
    entry = planned_entry_time(signal_candle_start, latency_bars)
    close = operational_close_after(entry)
    seconds = max(0.0, (close - _aware(signal_candle_start).astimezone(timezone.utc)).total_seconds())
    return int(seconds // 60) + 2

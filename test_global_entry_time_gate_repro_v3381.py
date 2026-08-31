from datetime import datetime, timezone
import server


def _utc(hour, minute=0):
    return datetime(2026, 8, 31, hour, minute, tzinfo=timezone.utc)


def test_morning_blackout_boundaries_are_deterministic():
    assert server.new_entry_time_gate(_utc(10, 59))["allowed"] is True   # 06:59 ET
    at_0700 = server.new_entry_time_gate(_utc(11, 0))                    # 07:00 ET
    assert at_0700["allowed"] is False
    assert at_0700["reason"] == "NY_ENTRY_BLACKOUT_07_10"
    assert server.new_entry_time_gate(_utc(14, 0))["allowed"] is True    # 10:00 ET


def test_afternoon_blackout_boundaries_are_deterministic():
    assert server.new_entry_time_gate(_utc(18, 59))["allowed"] is True   # 14:59 ET
    at_1500 = server.new_entry_time_gate(_utc(19, 0))                    # 15:00 ET
    assert at_1500["allowed"] is False
    assert at_1500["reason"] == "NY_ENTRY_BLACKOUT_15_19"
    assert server.new_entry_time_gate(_utc(23, 0))["allowed"] is True    # 19:00 ET

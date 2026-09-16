from datetime import datetime, timedelta, timezone
import pytest

from major_trend import directional_targets, major_trend, timeframe_trend, utc
from directional_trend_optimizer import closed_nonoverlapping, optimize_pair, role_for
from run_directional_trend_optimization import frozen_window

START = datetime(2026, 7, 1, tzinfo=timezone.utc)
END = START + timedelta(days=60)


def candles(hours, slope):
    return [{"t": START + timedelta(hours=i * hours), "c": 100 + slope * i,
             "h": 101 + slope * i, "l": 99 + slope * i, "o": 100 + slope * i} for i in range(140)]


def test_strong_lateral_and_weak_budgets():
    up = directional_targets({"UP": 100})
    assert up["BUY"]["UP"] == 36 and up["SELL"]["UP"] == 18
    down = directional_targets({"DOWN": 100})
    assert down["BUY"]["DOWN"] == 18 and down["SELL"]["DOWN"] == 36
    lateral = directional_targets({"LATERAL": 100})
    assert lateral["BUY"]["LATERAL"] == lateral["SELL"]["LATERAL"] == 27
    weak = directional_targets({"WEAK_UP": 100})
    assert 27 < weak["BUY"]["WEAK_UP"] < 36
    assert sum(sum(side.values()) for side in weak.values()) == 54


def test_month_reversal_swaps_favored_direction_without_splitting_fit():
    targets = directional_targets({"UP": 1000, "DOWN": 1000})
    assert targets["BUY"]["UP"] == targets["SELL"]["DOWN"] == 18
    assert targets["SELL"]["UP"] == targets["BUY"]["DOWN"] == 9
    assert role_for("BUY", "UP") == role_for("SELL", "DOWN") == "WITH"


def test_future_or_unclosed_candles_cannot_change_trend():
    h1, h4 = candles(1, .3), candles(4, .3)
    now = START + timedelta(hours=560)
    # Align recent H1 with the same decision instant.
    for row in h1:
        row["t"] += timedelta(hours=420)
    before = major_trend(h1, h4, now)
    assert before["regime"] == "UP"
    for bars in (h1, h4):
        bars.append({"t": now, "c": -999, "o": -999, "h": 1e9, "l": -1e9})
    assert major_trend(h1, h4, now) == before


def test_missing_and_lateral_data_and_context_across_market_closure():
    assert major_trend([], [], END)["regime"] == "UNKNOWN"
    assert timeframe_trend(candles(1, 0), 1, END)["available"]
    assert timeframe_trend(candles(1, 0), 1, START + timedelta(hours=140))["score"] == 0


def rows(direction, regime, n, start, wins=None):
    return [{"instrument": "USD_JPY", "signal": direction,
             "candle_ts": (start + timedelta(hours=i*6)).isoformat(),
             "entry_ts": (start + timedelta(hours=i*6, minutes=1)).isoformat(),
             "exit_ts": (start + timedelta(hours=i*6, minutes=30)).isoformat(),
             "outcome_status": "WIN" if wins is None or i < wins else "LOSS",
             "realized_r": 1.2 if wins is None or i < wins else -1,
             "features": {"major_trend_regime": regime}}
            for i in range(n)]


def optimize(data, exposure):
    return optimize_pair(data, exposure, "USD_JPY", START, END)


def test_reversal_requires_evidence_in_both_regimes():
    data = rows("BUY", "UP", 18, START) + rows("SELL", "UP", 9, START + timedelta(days=8))
    report = optimize(data, {"UP": 100, "DOWN": 100})
    assert not report["pair_qualified"]
    assert not report["results"][0]["final_gates"]["regime_minima"]
    data += rows("BUY", "DOWN", 9, START + timedelta(days=30))
    data += rows("SELL", "DOWN", 18, START + timedelta(days=40))
    report = optimize(data, {"UP": 100, "DOWN": 100})
    assert report["pair_qualified"]
    assert all(r["metrics"]["resolved"] == 27 for r in report["results"])


def test_exact_fifty_percent_fails_and_timeouts_do_not_count():
    data = rows("BUY", "UP", 36, START, 18) + rows("SELL", "UP", 18, START + timedelta(days=30))
    assert not optimize(data, {"UP": 1})["pair_qualified"]
    data[0]["outcome_status"] = "TIMEOUT"
    report = optimize(data, {"UP": 1})
    assert not report["results"][0]["final_gates"]["regime_minima"]


def test_overlap_duplicate_and_post_window_closes_excluded():
    data = rows("BUY", "UP", 3, START)
    data[1]["entry_ts"] = data[0]["entry_ts"]
    data[2]["exit_ts"] = (END + timedelta(minutes=1)).isoformat()
    assert len(closed_nonoverlapping(data + [data[0]], START, END)) == 1


def test_window_is_exactly_sixty_days():
    start, end = frozen_window(END)
    assert end - start == timedelta(days=60)
    with pytest.raises(ValueError, match="60-day"):
        optimize_pair([], {"UP": 1}, "USD_JPY", START, START + timedelta(days=30))


def test_empty_exposure_fails():
    with pytest.raises(ValueError):
        directional_targets({})

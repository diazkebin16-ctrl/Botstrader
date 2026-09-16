from datetime import datetime, timedelta, timezone

from directional_month_optimizer import FEATURES, optimize_all_lanes, optimize_lane
from run_directional_month_optimization import frozen_window

START = datetime(2026, 8, 15, tzinfo=timezone.utc)
END = datetime(2026, 9, 14, 23, 59, 59, tzinfo=timezone.utc)


def _rows(*, instrument="GBP_USD", direction="BUY", count=20, wins=12):
    rows = []
    for index in range(count):
        win = index < wins
        signal_value = 1.0 if win else 0.0
        rows.append({
            "candle_ts": (START + timedelta(days=index, hours=8)).isoformat(),
            "instrument": instrument, "signal": direction,
            "outcome_status": "WIN" if win else "LOSS", "realized_r": 1.2 if win else -1.0,
            "features": {feature: signal_value + index / 10000 for feature in FEATURES},
        })
    return rows


def _optimize(rows, *, instrument="GBP_USD", direction="BUY"):
    return optimize_lane(rows, instrument=instrument, direction=direction, start=START, end=END)


def test_fewer_than_ten_resolved_is_never_approved():
    result = _optimize(_rows(count=9, wins=9))
    assert result["verdict"] == "INSUFFICIENT_RAW_EVIDENCE"
    assert result["unfiltered_baseline"]["resolved"] == 9
    assert result["holdout_opened"] is False


def test_minimum_counts_only_win_and_loss():
    rows = _rows(count=9, wins=6)
    for index in range(5):
        row = dict(rows[0])
        row["candle_ts"] = (START + timedelta(days=20, hours=index)).isoformat()
        row["outcome_status"] = "TIMEOUT" if index < 4 else "AMBIGUOUS"
        rows.append(row)
    result = _optimize(rows)
    assert result["unfiltered_baseline"]["episodes"] == 14
    assert result["unfiltered_baseline"]["resolved"] == 9
    assert result["verdict"] == "INSUFFICIENT_RAW_EVIDENCE"


def test_exactly_fifty_percent_is_rejected():
    assert _optimize(_rows(count=10, wins=5))["verdict"] == "NO_ONE_MONTH_CANDIDATE"


def test_single_window_has_no_split_holdout_or_temporal_comparison():
    result = _optimize(_rows())
    assert result["split"] is None and result["comparison_periods"] == []
    assert result["holdout_opened"] is False
    assert result["search"]["threshold_source"] == "SAME_FROZEN_30_DAY_WINDOW"
    assert result["search"]["temporal_comparison"] is False


def test_qualified_candidate_has_ten_resolved_and_above_fifty_percent():
    candidate = _optimize(_rows())["frozen_candidate"]
    assert candidate["metrics"]["resolved"] >= 10
    assert candidate["metrics"]["win_rate"] > 0.50
    assert candidate["final_gates"]["positive_expectancy_safety"] is True


def test_lane_isolation_and_all_ten_lanes_are_reported():
    rows = {instrument: _rows(instrument=instrument, direction="BUY") + _rows(instrument=instrument, direction="SELL")
            for instrument in ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD")}
    result = optimize_all_lanes(rows, start=START, end=END)
    assert len(result["results"]) == result["strategy_count"] == result["approved_count"] == 10
    assert result["all_ten_qualified"] is True and result["split"] is None


def test_current_eurusd_sell_rules_are_an_incumbent_not_a_search_floor():
    result = _optimize(_rows(instrument="EUR_USD", direction="SELL"), instrument="EUR_USD", direction="SELL")
    assert len(result["current_rules"]) == 3
    assert result["strategy_id"] == "EURUSD_SELL_ONLY_V2"
    assert result["search"]["candidate_base"] == "UNFILTERED_DIRECTIONAL_LANE"


def test_frozen_window_is_exactly_thirty_days_and_excludes_current_minute():
    start, end = frozen_window(datetime(2026, 9, 16, 11, 7, 42, tzinfo=timezone.utc))
    assert end == datetime(2026, 9, 16, 11, 6, tzinfo=timezone.utc)
    assert end - start == timedelta(days=30)

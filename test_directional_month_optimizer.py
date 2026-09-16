from datetime import datetime, timedelta, timezone

from directional_month_optimizer import optimize_all_lanes, optimize_lane
from eurusd_buy_target_optimizer import FEATURES


START = datetime(2026, 7, 15, tzinfo=timezone.utc)
SECOND = datetime(2026, 8, 15, tzinfo=timezone.utc)
END = datetime(2026, 9, 14, 23, 59, 59, tzinfo=timezone.utc)


def _month_rows(start, *, instrument="GBP_USD", direction="BUY", count=20, invert=False):
    rows = []
    for index in range(count):
        win = index % 5 < 3
        signal_value = 1.0 if win else 0.0
        if invert:
            signal_value = 1.0 - signal_value
        features = {feature: signal_value + index / 10000 for feature in FEATURES}
        rows.append({
            "candle_ts": (start + timedelta(days=index, hours=8)).isoformat(),
            "instrument": instrument,
            "signal": direction,
            "outcome_status": "WIN" if win else "LOSS",
            "realized_r": 1.2 if win else -1.0,
            "features": features,
        })
    return rows


def _rows(**kwargs):
    return _month_rows(START, **kwargs) + _month_rows(SECOND, **kwargs)


def _optimize(rows, *, instrument="GBP_USD", direction="BUY"):
    return optimize_lane(
        rows, instrument=instrument, direction=direction, start=START,
        second_month_start=SECOND, end=END,
    )


def test_fewer_than_ten_resolved_in_either_month_is_never_modified():
    rows = _month_rows(START, count=9) + _month_rows(SECOND, count=20)
    result = _optimize(rows)
    assert result["verdict"] == "INSUFFICIENT_BASELINE_EVIDENCE"
    assert result["baseline"]["first_month"]["resolved"] == 9
    assert result["baseline"]["second_month"]["resolved"] == 20
    assert result["holdout_opened"] is False


def test_monthly_minimum_counts_only_win_and_loss():
    rows = _month_rows(START, count=9) + _month_rows(SECOND, count=20)
    for index in range(5):
        row = dict(rows[0])
        row["candle_ts"] = (START + timedelta(days=20, hours=index)).isoformat()
        row["outcome_status"] = "TIMEOUT" if index < 4 else "AMBIGUOUS"
        rows.append(row)
    result = _optimize(rows)
    assert result["baseline"]["first_month"]["episodes"] == 14
    assert result["baseline"]["first_month"]["resolved"] == 9
    assert result["verdict"] == "INSUFFICIENT_BASELINE_EVIDENCE"


def test_lane_isolation_and_determinism():
    rows = _rows() + _rows(instrument="AUD_USD", direction="SELL")
    first = _optimize(rows)
    second = _optimize(rows)
    assert first == second
    assert first["baseline"]["total"]["resolved"] == 40


def test_second_month_cannot_change_the_frozen_candidate():
    normal = _optimize(_rows())
    inverted_rows = _month_rows(START) + _month_rows(SECOND, invert=True)
    inverted = _optimize(inverted_rows)
    assert normal["holdout_opened"] is True
    assert inverted["holdout_opened"] is True
    assert normal["frozen_candidate"] == inverted["frozen_candidate"]
    assert normal["search"]["threshold_source"] == "FIRST_MONTH_ONLY"


def test_qualified_candidate_has_ten_resolved_in_each_month():
    result = _optimize(_rows())
    assert result["verdict"] == "RESEARCH_CANDIDATE"
    assert result["frozen_candidate"]["first_month"]["resolved"] >= 10
    assert result["second_month"]["resolved"] >= 10
    assert result["frozen_candidate"]["first_month"]["win_rate"] > 0.50
    assert result["second_month"]["win_rate"] > 0.50


def test_all_ten_lanes_are_reported():
    rows = {instrument: [] for instrument in ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD")}
    result = optimize_all_lanes(
        rows, start=START, second_month_start=SECOND, end=END,
    )
    assert len(result["results"]) == 10
    assert result["approved_count"] == 0
    assert all(item["verdict"] == "INSUFFICIENT_BASELINE_EVIDENCE" for item in result["results"])


def test_current_eurusd_sell_rules_are_applied_before_search():
    rows = _rows(instrument="EUR_USD", direction="SELL")
    result = _optimize(rows, instrument="EUR_USD", direction="SELL")
    assert len(result["current_rules"]) == 3
    assert result["strategy_id"] == "EURUSD_SELL_ONLY_V2"

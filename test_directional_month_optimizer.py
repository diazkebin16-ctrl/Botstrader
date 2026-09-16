from datetime import datetime, timedelta, timezone

from directional_month_optimizer import MonthPolicy, optimize_all_lanes, optimize_lane
from eurusd_buy_target_optimizer import FEATURES


def _rows(instrument="GBP_USD", direction="BUY", count=40):
    start = datetime(2026, 8, 15, tzinfo=timezone.utc)
    rows = []
    for index in range(count):
        win = index % 3 != 0
        rows.append({
            "candle_ts": (start + timedelta(hours=18 * index)).isoformat(),
            "instrument": instrument,
            "signal": direction,
            "outcome_status": "WIN" if win else "LOSS",
            "realized_r": 1.2 if win else -1.0,
            "features": {feature: (1.0 if win else 0.0) + index / 10000 for feature in FEATURES},
        })
    return rows


def test_insufficient_baseline_is_never_modified():
    result = optimize_lane(
        _rows(count=19), instrument="GBP_USD", direction="BUY",
        start="2026-08-15T00:00:00Z", end="2026-09-14T23:59:59Z",
    )
    assert result["verdict"] == "INSUFFICIENT_BASELINE_EVIDENCE"
    assert result["holdout_opened"] is False


def test_lane_isolation_and_determinism():
    rows = _rows() + _rows(instrument="AUD_USD", direction="SELL")
    policy = MonthPolicy(
        minimum_discovery_resolved=8,
        minimum_holdout_resolved=5,
        minimum_discovery_bucket_resolved=2,
    )
    first = optimize_lane(
        rows, instrument="GBP_USD", direction="BUY",
        start="2026-08-15T00:00:00Z", end="2026-09-14T23:59:59Z", policy=policy,
    )
    second = optimize_lane(
        rows, instrument="GBP_USD", direction="BUY",
        start="2026-08-15T00:00:00Z", end="2026-09-14T23:59:59Z", policy=policy,
    )
    assert first == second
    assert first["baseline"]["total"]["resolved"] == 40


def test_all_ten_lanes_are_reported():
    rows = {instrument: [] for instrument in ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD")}
    result = optimize_all_lanes(
        rows, start="2026-08-15T00:00:00Z", end="2026-09-14T23:59:59Z",
    )
    assert len(result["results"]) == 10
    assert result["approved_count"] == 0
    assert all(item["verdict"] == "INSUFFICIENT_BASELINE_EVIDENCE" for item in result["results"])


def test_current_eurusd_sell_rules_are_applied_before_search():
    rows = _rows(instrument="EUR_USD", direction="SELL")
    result = optimize_lane(
        rows, instrument="EUR_USD", direction="SELL",
        start="2026-08-15T00:00:00Z", end="2026-09-14T23:59:59Z",
    )
    assert len(result["current_rules"]) == 3
    assert result["strategy_id"] == "EURUSD_SELL_ONLY_V2"

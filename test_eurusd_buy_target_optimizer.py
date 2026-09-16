from datetime import datetime, timedelta, timezone

from eurusd_buy_target_optimizer import FEATURES, TargetPolicy, optimize_eurusd_buy


def _rows():
    start = datetime(2026, 7, 15, tzinfo=timezone.utc)
    rows = []
    for index in range(60):
        timestamp = start + timedelta(days=index, hours=1)
        good = index % 3 == 0
        rows.append(
            {
                "candle_ts": timestamp.isoformat(),
                "instrument": "EUR_USD",
                "signal": "BUY",
                "outcome_status": "WIN" if good else "LOSS",
                "realized_r": 1.5 if good else -1.0,
                "features": {feature: (1.0 if good else 0.0) + index / 10000 for feature in FEATURES},
            }
        )
        rows.append({**rows[-1], "instrument": "GBP_USD"})
        rows.append({**rows[-1], "instrument": "EUR_USD", "signal": "SELL"})
    return rows


def test_optimizer_is_lane_isolated_and_excludes_directional_score():
    result = optimize_eurusd_buy(_rows(), start="2026-07-15T00:00:00Z", end="2026-09-14T23:59:59Z")
    assert result["baseline"]["total"]["resolved"] == 60
    assert result["search"]["directional_score_excluded"] is True
    assert "directional_score" not in result["search"]["features"]


def test_holdout_is_not_opened_when_no_candidate_survives_validation():
    policy = TargetPolicy(minimum_discovery_win_rate_gain=0.99)
    result = optimize_eurusd_buy(_rows(), start="2026-07-15T00:00:00Z", end="2026-09-14T23:59:59Z", policy=policy)
    assert result["frozen_candidate"] is None
    assert result["holdout_opened"] is False
    assert result["verdict"] == "NO_CANDIDATE_BEFORE_HOLDOUT"


def test_candidate_definition_is_deterministic():
    first = optimize_eurusd_buy(_rows(), start="2026-07-15T00:00:00Z", end="2026-09-14T23:59:59Z")
    second = optimize_eurusd_buy(_rows(), start="2026-07-15T00:00:00Z", end="2026-09-14T23:59:59Z")
    assert first == second

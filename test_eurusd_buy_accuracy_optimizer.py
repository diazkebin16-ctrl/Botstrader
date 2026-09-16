from datetime import datetime, timedelta, timezone

from eurusd_buy_accuracy_optimizer import AccuracyPolicy, _wilson_lower, optimize_accuracy
from eurusd_buy_target_optimizer import FEATURES


def _rows():
    start = datetime(2026, 7, 15, tzinfo=timezone.utc)
    rows = []
    for index in range(62):
        win = index % 3 != 0
        rows.append({
            "candle_ts": (start + timedelta(days=index)).isoformat(),
            "instrument": "EUR_USD",
            "signal": "BUY",
            "outcome_status": "WIN" if win else "LOSS",
            "realized_r": 1.2 if win else -1.0,
            "features": {feature: (1.0 if win else 0.0) + index / 10000 for feature in FEATURES},
        })
    return rows


def test_wilson_lower_rewards_sample_evidence():
    assert _wilson_lower(60, 100) > _wilson_lower(6, 10)


def test_prior_failed_mechanism_and_directional_score_are_excluded():
    result = optimize_accuracy(_rows(), start="2026-07-15T00:00:00Z", end="2026-09-14T23:59:59Z")
    assert result["search"]["directional_score_excluded"] is True
    assert result["search"]["prior_failed_mechanism_excluded"] == "session_momentum_atr <= threshold"
    if result["frozen_candidate"]:
        assert all(not (rule["feature"] == "session_momentum_atr" and rule["operator"] == "<=") for rule in result["frozen_candidate"]["rules"])


def test_deterministic_and_lane_isolated():
    rows = _rows() + [{**row, "instrument": "GBP_USD"} for row in _rows()]
    policy = AccuracyPolicy(minimum_discovery_resolved=5, minimum_holdout_resolved=5)
    first = optimize_accuracy(rows, start="2026-07-15T00:00:00Z", end="2026-09-14T23:59:59Z", policy=policy)
    second = optimize_accuracy(rows, start="2026-07-15T00:00:00Z", end="2026-09-14T23:59:59Z", policy=policy)
    assert first == second
    assert first["baseline"]["total"]["resolved"] == 62

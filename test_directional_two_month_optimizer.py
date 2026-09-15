from datetime import datetime, timedelta, timezone

from directional_two_month_optimizer import (
    OptimizationPolicy,
    apply_rules,
    metrics,
    optimize_lane,
)


START = datetime(2026, 7, 1, tzinfo=timezone.utc)
END = START + timedelta(days=60)


def row(index, outcome, quality, *, status=None, realized=None):
    direction = "BUY"
    return {
        "instrument": "GBP_USD",
        "signal": direction,
        "candle_ts": (START + timedelta(days=index)).isoformat(),
        "features": {"direction_edge": quality, "extension_atr": 1.0, "rr_raw": 2.0},
        "outcome_status": status or outcome,
        "realized_r": realized if realized is not None else (1.8 if outcome == "WIN" else -1.0 if outcome == "LOSS" else None),
    }


def permissive_policy():
    return OptimizationPolicy(
        minimum_total_baseline_resolved=12,
        minimum_discovery_baseline_resolved=4,
        minimum_validation_baseline_resolved=2,
        minimum_holdout_baseline_resolved=2,
        minimum_candidate_total_resolved=6,
        minimum_candidate_oos_resolved=3,
        minimum_candidate_holdout_resolved=1,
        minimum_resolved_retention=0.30,
        maximum_resolved_retention=0.90,
        maximum_trades_per_30_days=20,
        minimum_discovery_win_rate_gain=0.05,
        minimum_oos_win_rate_gain=0.03,
    )


def test_optimizer_finds_loss_filter_that_survives_validation_and_holdout():
    rows = []
    for index in range(60):
        high_quality = index % 3 != 0
        outcome = "WIN" if high_quality else "LOSS"
        rows.append(row(index, outcome, 0.8 if high_quality else 0.1))
    result = optimize_lane(
        rows,
        instrument="GBP_USD",
        direction="BUY",
        start=START,
        end=END,
        policy=permissive_policy(),
    )
    assert result["verdict"] == "RESEARCH_CANDIDATE"
    assert result["candidate"]["qualified"] is True
    assert result["candidate"]["splits"]["holdout"]["win_rate"] > result["baseline"]["holdout"]["win_rate"]
    assert len(result["candidate"]["rules"]) <= 2


def test_optimizer_refuses_small_sample_even_with_perfect_outcomes():
    rows = [row(index, "WIN", 0.9) for index in range(8)]
    result = optimize_lane(
        rows,
        instrument="GBP_USD",
        direction="BUY",
        start=START,
        end=END,
    )
    assert result["verdict"] == "INSUFFICIENT_EVIDENCE"
    assert result["candidate"] is None


def test_filters_cannot_turn_timeout_or_ambiguous_into_loss():
    rows = [
        row(1, "WIN", 0.9),
        row(2, "LOSS", 0.1),
        row(3, "TIMEOUT", 0.9, status="TIMEOUT"),
        row(4, "AMBIGUOUS", 0.9, status="AMBIGUOUS"),
    ]
    selected = apply_rules(rows, [{"feature": "direction_edge", "operator": ">=", "threshold": 0.5}])
    report = metrics(selected)
    assert report["wins"] == 1
    assert report["losses"] == 0
    assert report["statuses"]["TIMEOUT"] == 1
    assert report["statuses"]["AMBIGUOUS"] == 1


def test_candidate_is_frequency_reducing_not_trade_generating():
    rows = [row(index, "WIN" if index % 2 else "LOSS", float(index % 5)) for index in range(60)]
    result = optimize_lane(
        rows,
        instrument="GBP_USD",
        direction="BUY",
        start=START,
        end=END,
        policy=permissive_policy(),
    )
    if result["candidate"] is not None:
        assert result["candidate"]["splits"]["total"]["episodes"] <= result["baseline"]["total"]["episodes"]

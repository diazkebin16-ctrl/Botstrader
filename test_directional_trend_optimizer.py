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
    assert not report["results"][0]["final_gates"]["adaptive_role_minima"]
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
    assert not report["results"][0]["final_gates"]["adaptive_role_minima"]


def test_overlap_duplicate_and_post_window_closes_excluded():
    data = rows("BUY", "UP", 3, START)
    data[1]["entry_ts"] = data[0]["entry_ts"]
    data[2]["exit_ts"] = (END + timedelta(minutes=1)).isoformat()
    assert len(closed_nonoverlapping(data + [data[0]], START, END)) == 1


def test_invalid_entry_does_not_lock_the_pair_for_sixty_days():
    data = rows("BUY", "UP", 3, START)
    data[0]["outcome_status"] = "ENTRY_INVALIDATED"
    data[0].pop("exit_ts")
    assert len(closed_nonoverlapping(data, START, END)) == 2


def test_window_is_exactly_sixty_days():
    start, end = frozen_window(END)
    assert end - start == timedelta(days=60)
    with pytest.raises(ValueError, match="60-day"):
        optimize_pair([], {"UP": 1}, "USD_JPY", START, START + timedelta(days=30))


def test_empty_exposure_fails():
    with pytest.raises(ValueError):
        directional_targets({})


def activation_fixture():
    data = rows("BUY", "UP", 36, START) + rows("SELL", "UP", 18, START + timedelta(days=30))
    pair = optimize(data, {"UP": 1})
    from major_trend import TREND_VERSION
    return {"protocol": "ONE_60_DAY_CAUSAL_H1_H4_ADAPTIVE_PAPER", "production_authority": False,
            "split": None, "comparison_periods": [], "trend_version": TREND_VERSION,
            "window": {"start": START.isoformat(), "end": END.isoformat(), "days": 60},
            "execution_model": {"closed_within_window_only": True}, "pairs": [pair]}


def test_activation_recomputes_gates_and_rejects_tampering(tmp_path):
    from trend_paper_activation import qualified_definitions, activate_report
    import json
    report = activation_fixture()
    assert len(qualified_definitions(report)) == 2
    source, active = tmp_path / "evidence.json", tmp_path / "active.json"
    source.write_text(json.dumps(report))
    assert activate_report(source, active) == 2
    report["pairs"][0]["results"][0]["metrics"]["wins"] = 0
    with pytest.raises(ValueError):
        qualified_definitions(report)


def test_runtime_switches_role_when_trend_reverses_and_fails_closed(monkeypatch):
    import directional_strategies as registry
    from major_trend import TREND_VERSION
    definition = dict(registry.STRATEGY_DEFINITIONS[("USD_JPY", "BUY")])
    definition["conditional_rules"] = {
        "WITH": {"enabled": True, "filters": [{"feature": "rr_raw", "operator": ">=", "threshold": 1}]},
        "AGAINST": {"enabled": True, "filters": [{"feature": "rr_raw", "operator": ">=", "threshold": 2}]},
        "LATERAL": {"enabled": False, "filters": []},
    }
    monkeypatch.setitem(registry.STRATEGY_DEFINITIONS, ("USD_JPY", "BUY"), definition)
    row = {"instrument": "USD_JPY", "signal": "BUY", "features": {
        "major_trend_regime": "UP", "major_trend_version": TREND_VERSION, "rr_raw": 1.5}}
    assert registry.evaluate_directional_strategy(row)["eligible"]
    row["features"]["major_trend_regime"] = "DOWN"
    result = registry.evaluate_directional_strategy(row)
    assert result["major_trend_role"] == "AGAINST" and not result["eligible"]
    row["features"].pop("major_trend_version")
    assert not registry.evaluate_directional_strategy(row)["eligible"]


def test_adaptive_replay_never_calls_temporal_split_helpers(monkeypatch):
    import historical_replay as replay
    from types import SimpleNamespace
    def forbidden(*args, **kwargs):
        raise AssertionError("Temporal splitting was requested")
    monkeypatch.setattr(replay, "chronological_holdout", forbidden)
    monkeypatch.setattr(replay, "walk_forward_splits", forbidden)
    bundle = {}
    for tf, seconds in replay.BAR_SECONDS.items():
        bundle[tf] = [{"t": END - timedelta(seconds=(80-i)*seconds),
                       "o": 100, "c": 100, "h": 101, "l": 99} for i in range(80)]
    monkeypatch.setattr(replay, "replay_snapshot", lambda *args, **kwargs: {
        "candle_ts": (END - timedelta(minutes=1)).isoformat(), "features": {},
        "actionable": False, "signal": "WAIT", "decision_reason": "WAIT"})
    server = SimpleNamespace(_direction_hypothesis=lambda *args: {})
    result = replay.replay_history(server, bundle, "USD_JPY", END-timedelta(minutes=1), END,
        [replay.ReplayVariant("ADAPTIVE")],
        replay.ReplayConfig(adaptive_major_trend=True, temporal_validation=False))
    assert result["variants"]["ADAPTIVE"]["holdout"] is None
    assert result["variants"]["ADAPTIVE"]["walk_forward"] == []
    assert result["variants"]["ADAPTIVE"]["major_trend_exposure_minutes"] == {"LATERAL": 1}


def test_parallel_pair_aggregation_rejects_mixed_windows(tmp_path):
    import json
    from run_directional_trend_optimization import aggregate
    payload = {"instrument": "EUR_USD", "window": {
        "start": (START + timedelta(days=1)).isoformat(), "end": END.isoformat()}}
    (tmp_path / "EUR_USD_replay.json").write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="different windows"):
        aggregate(tmp_path, START, END)


def test_rejected_setup_does_not_occupy_a_position_before_filtering():
    data = rows("BUY", "UP", 36, START + timedelta(hours=1))
    data += rows("SELL", "UP", 18, START + timedelta(days=30))
    for row in data:
        row["features"]["rr_raw"] = 2.0
    rejected = rows("BUY", "UP", 1, START)[0]
    rejected.update(outcome_status="TIMEOUT", realized_r=None,
                    exit_ts=(START + timedelta(days=2)).isoformat())
    rejected["features"]["rr_raw"] = 0.1
    report = optimize([rejected]+data, {"UP":1})
    assert report["pair_qualified"]
    assert report["results"][0]["metrics"]["resolved"] == 36


def test_weak_and_strong_context_share_role_minima_without_reducing_total():
    data = rows("BUY", "WEAK_UP", 34, START)
    data += rows("SELL", "UP", 20, START + timedelta(days=30))
    report = optimize(data, {"UP":1,"WEAK_UP":1})
    assert report["pair_qualified"]
    buy, sell = report["results"]
    assert buy["minimum_by_role"]["WITH"] == 34
    assert sell["minimum_by_role"]["AGAINST"] == 20
    assert buy["resolved_by_regime"].get("UP",0) == 0
    assert buy["metrics"]["resolved"] + sell["metrics"]["resolved"] == 54

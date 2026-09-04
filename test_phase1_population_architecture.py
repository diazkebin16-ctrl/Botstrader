import json

import pytest

from automation_v3_discovery_pregate import classify_discovery_outcome
from historical_replay import build_research_target_episodes
from research_phase2 import _phase1_eligible_rows, candidate_analysis
from research_pipeline import _strategic_blocks, analyze_phase1, extract_target_population


def row(status, *, ts, blocks=None, actionable=False, feature=0.0, instrument="AUD_USD"):
    return {
        "instrument": instrument,
        "candle_ts": ts,
        "signal": "BUY" if actionable else "WAIT",
        "chosen_signal": "BUY",
        "research_direction": "BUY",
        "actionable": actionable,
        "decision_reason": "REPLAY_ACTIONABLE" if actionable else "WAIT_DIRECTION",
        "research_blocks": list(blocks or []),
        "research_episode_class": "|".join(sorted(set(blocks or []))) if blocks else "BASELINE_PASS",
        "safety_checks": {"valid_direction": actionable},
        "filters": {"m1_confirmation": True},
        "features": {"rr_raw": feature, "score": feature},
        "outcome_status": status,
        "label": 1 if status == "WIN" else 0 if status == "LOSS" else None,
        "realized_r": 1.5 if status == "WIN" else -1.0 if status == "LOSS" else None,
    }


def write_target(tmp_path, rows):
    path = tmp_path / "target.json"
    path.write_text(json.dumps({
        "instrument": "AUD_USD",
        "variant": "V331_BASELINE",
        "lookahead_protection": True,
        "episodes": rows,
    }), encoding="utf-8")
    return path


def test_raw_win_blocked_by_baseline_remains_phase1_target(tmp_path):
    out = analyze_phase1(str(write_target(tmp_path, [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"])])))
    assert out["phase1_target_wins"] == 1
    assert out["phase1_recovered_wins"] == 1


def test_phase1_target_not_limited_to_baseline_pass(tmp_path):
    rows = [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["DIRECTION_SELECTION"]), row("LOSS", ts="2026-01-02T00:00:00Z", actionable=True)]
    out = analyze_phase1(str(write_target(tmp_path, rows)))
    assert out["phase1_target_wins"] == 1
    assert out["baseline_pass_wins"] == 0
    assert out["baseline_blocked_wins"] == 1


def test_phase1_maximizes_all_recoverable_win_recall(tmp_path):
    rows = [
        row("WIN", ts="2026-01-01T00:00:00Z", blocks=["DIRECTION_SELECTION"]),
        row("WIN", ts="2026-01-02T00:00:00Z", blocks=["MINIMUM_RR"]),
        row("WIN", ts="2026-01-03T00:00:00Z", blocks=["M1_CONFIRMATION"]),
        row("WIN", ts="2026-01-04T00:00:00Z", blocks=["SAFETY:finite_prices"]),
    ]
    out = analyze_phase1(str(write_target(tmp_path, rows)))
    assert out["phase1_recovered_wins"] == 3
    assert out["phase1_unrecoverable_wins"] == 1
    assert out["win_recall_after"] == pytest.approx(0.75)


def test_phase1_may_admit_losses_to_recover_win(tmp_path):
    rows = [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"])]
    rows += [row("LOSS", ts=f"2026-01-{day:02d}T00:00:00Z", blocks=["M1_CONFIRMATION"]) for day in range(2, 7)]
    out = analyze_phase1(str(write_target(tmp_path, rows)))
    assert out["best_policy"]["opened_gates"] == ["M1_CONFIRMATION"]
    assert out["losses_admitted_after"] == 5
    assert out["phase1_recovered_wins"] == 1


def test_phase2_receives_recovered_wins_and_admitted_losses():
    rows = [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"]), row("LOSS", ts="2026-01-02T00:00:00Z", blocks=["M1_CONFIRMATION"])]
    spec = {"phase1_policy": {"opened_gates": ["M1_CONFIRMATION"]}}
    selected = _phase1_eligible_rows(spec, rows)
    assert [item["outcome_status"] for item in selected] == ["WIN", "LOSS"]


def test_phase2_can_remove_loss_while_retaining_recovered_win():
    win = row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"], feature=2.0)
    loss = row("LOSS", ts="2026-01-02T00:00:00Z", blocks=["M1_CONFIRMATION"], feature=0.2)
    candidate = {"id": "rr", "rules": [{"feature": "rr_raw", "operator": ">=", "threshold": 1.0}]}
    analysis = candidate_analysis(candidate, [win, loss])
    assert analysis["selected"]["wins"] == 1
    assert analysis["selected"]["losses"] == 0


def test_hard_safety_gates_remain_immutable():
    relaxable, immutable = _strategic_blocks({"research_blocks": ["SAFETY:finite_prices", "SAFETY:positive_risk"]})
    assert not relaxable
    assert immutable == {"SAFETY:finite_prices", "SAFETY:positive_risk"}


def test_strategy_gates_are_distinguishable_from_safety():
    relaxable, immutable = _strategic_blocks({"research_blocks": ["DIRECTION_SELECTION", "MINIMUM_RR", "LOW_ROOM", "SAFETY:minimum_stop_pips"]})
    assert {"DIRECTION_SELECTION", "MINIMUM_RR", "LOW_ROOM"} <= relaxable
    assert immutable == {"SAFETY:minimum_stop_pips"}


def test_baseline_episode_anchors_never_silently_shrink():
    rows = [
        row("WIN", ts="2026-01-01T00:00:00Z", actionable=True),
        row("LOSS", ts="2026-01-01T00:05:00Z", blocks=["M1_CONFIRMATION"]),
        row("WIN", ts="2026-01-01T00:20:00Z", actionable=True),
        row("LOSS", ts="2026-01-01T00:25:00Z", blocks=["MINIMUM_RR"]),
        row("WIN", ts="2026-01-01T00:40:00Z", actionable=True),
    ]
    episodes, evidence = build_research_target_episodes(rows, gap_minutes=15)
    keys = {(x["candle_ts"], x["research_direction"]) for x in episodes}
    assert evidence["baseline_subset_missing"] == 0
    for ts in ("2026-01-01T00:00:00Z", "2026-01-01T00:20:00Z", "2026-01-01T00:40:00Z"):
        assert (ts, "BUY") in keys


def test_funnel_accounting_balances(tmp_path):
    rows = [row("WIN", ts="2026-01-01T00:00:00Z"), row("LOSS", ts="2026-01-02T00:00:00Z"), row("TIMEOUT", ts="2026-01-03T00:00:00Z")]
    out = analyze_phase1(str(write_target(tmp_path, rows)))
    assert out["resolved_wins"] + out["resolved_losses"] + out["timeouts"] + out["ambiguous"] == out["total_research_episodes"]


def test_every_unrecovered_win_has_explicit_reason(tmp_path):
    out = analyze_phase1(str(write_target(tmp_path, [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["SAFETY:finite_prices"])])))
    item = out["unrecovered_target_wins"][0]
    assert item["immutable_blocks"] == ["SAFETY:finite_prices"]


def test_research_episode_identity_is_deterministic():
    rows = [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"]), row("LOSS", ts="2026-01-01T00:05:00Z", blocks=["MINIMUM_RR"])]
    first, first_evidence = build_research_target_episodes(rows, gap_minutes=15)
    second, second_evidence = build_research_target_episodes(rows, gap_minutes=15)
    assert first == second
    assert first_evidence == second_evidence


def test_no_lookahead_remains_enforced(tmp_path):
    replay = tmp_path / "replay.json"
    replay.write_text(json.dumps({"instrument":"AUD_USD","methodology":{"no_lookahead_decision":False,"future_bars_only_for_outcome":True},"variants":{"V331_BASELINE":{"target_population":{"enabled":True,"episodes":[]}}}}), encoding="utf-8")
    with pytest.raises(ValueError, match="look-ahead"):
        extract_target_population(str(replay), "V331_BASELINE")


def test_freeze_holdout_contract_remains_separate():
    assert "holdout_opened" not in analyze_phase1.__annotations__


def test_lookback_expansion_only_for_insufficient_broad_support():
    fallback = {"dominant_failure": "OTHER_METHODOLOGY_FAILURE", "recommended_action": "STOP"}
    insufficient = {"available": True, "pass_all_pre_gate": 0, "dominant_failure": "INSUFFICIENT_SUPPORT", "recommended_action": "EXPAND_LOOKBACK"}
    poor = {"available": True, "pass_all_pre_gate": 0, "dominant_failure": "NO_POSITIVE_EXPECTANCY", "recommended_action": "NO_VALID_CANDIDATE"}
    assert classify_discovery_outcome({}, insufficient, fallback)["recommended_action"] == "EXPAND_LOOKBACK"
    assert classify_discovery_outcome({}, poor, fallback)["recommended_action"] == "NO_VALID_CANDIDATE"


def test_audusd_regression_shape_preserves_baseline_subset():
    rows = []
    for index in range(38):
        minute = index * 20
        hour, mm = divmod(minute, 60)
        rows.append(row("WIN", ts=f"2026-01-{1 + hour // 24:02d}T{hour % 24:02d}:{mm:02d}:00Z", actionable=True))
        rows.append(row("LOSS", ts=f"2026-01-{1 + hour // 24:02d}T{hour % 24:02d}:{(mm + 5) % 60:02d}:00Z", blocks=["M1_CONFIRMATION"]))
    _, evidence = build_research_target_episodes(rows, gap_minutes=15)
    assert evidence["baseline_actionable_episodes"] == 38
    assert evidence["research_episodes"] >= 38


def test_multi_asset_isolation_in_episode_identity():
    rows = [row("WIN", ts="2026-01-01T00:00:00Z", actionable=True, instrument="AUD_USD"), row("WIN", ts="2026-01-01T00:00:00Z", actionable=True, instrument="EUR_USD")]
    episodes, _ = build_research_target_episodes(rows, gap_minutes=15)
    assert {x["instrument"] for x in episodes} == {"AUD_USD", "EUR_USD"}


def test_production_authority_false(tmp_path):
    out = analyze_phase1(str(write_target(tmp_path, [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"])])))
    assert out["production_authority"] is False

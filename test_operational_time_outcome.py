from datetime import datetime, timedelta, timezone
import json

import pytest

from automation_v3_candidate_mapping import CandidateNotDeployable, _canonical_rules
from historical_execution import HistoricalExecutionConfig, resolve_executed_outcome
from historical_replay import build_research_target_episodes
from operational_time import NY, fixed_entry_gate
from research_phase2 import _phase1_eligible_rows
from research_pipeline import RESEARCHABLE_STRATEGY_GATES, analyze_phase1

UTC = timezone.utc


def ny_dt(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute, tzinfo=NY)


def bar(t, bid=1.1000, high=None, low=None, spread=0.0002):
    high = bid + 0.0002 if high is None else high
    low = bid - 0.0002 if low is None else low
    return {
        "t": t, "o": bid + spread / 2, "h": high + spread / 2,
        "l": low + spread / 2, "c": bid + spread / 2,
        "bid_o": bid, "bid_h": high, "bid_l": low, "bid_c": bid,
        "ask_o": bid + spread, "ask_h": high + spread,
        "ask_l": low + spread, "ask_c": bid + spread, "v": 1,
    }


def sample(signal_ts, direction="BUY"):
    if direction == "BUY":
        return {"candle_ts": signal_ts.isoformat(), "direction":"BUY", "entry":1.1000, "stop":1.0980, "target":1.1020, "instrument":"EUR_USD"}
    return {"candle_ts": signal_ts.isoformat(), "direction":"SELL", "entry":1.1000, "stop":1.1020, "target":1.0980, "instrument":"EUR_USD"}


def flat_bars(start, end, bid=1.1000):
    rows=[]; t=start
    while t <= end:
        rows.append(bar(t,bid=bid))
        t += timedelta(minutes=1)
    return rows


def research_row(ts, status="WIN", blocks=None, operational=True, actionable=False, instrument="AUD_USD"):
    return {
        "instrument":instrument,"candle_ts":ts,"signal":"BUY" if actionable else "WAIT",
        "chosen_signal":"BUY","research_direction":"BUY","actionable":actionable,
        "decision_reason":"REPLAY_ACTIONABLE" if actionable else "WAIT_DIRECTION",
        "research_blocks":list(blocks or []),"research_episode_class":"|".join(blocks or []) if blocks else "BASELINE_PASS",
        "operational_entry_allowed":operational,"safety_checks":{},"features":{"rr_raw":1.0},
        "outcome_status":status,"label":1 if status=="WIN" else 0 if status=="LOSS" else None,
        "realized_r":1.5 if status=="WIN" else -1.0 if status=="LOSS" else None,
    }


def write_target(tmp_path, rows):
    p=tmp_path/"target.json"
    p.write_text(json.dumps({"instrument":"AUD_USD","variant":"V331_BASELINE","lookahead_protection":True,"episodes":rows}),encoding="utf-8")
    return p


def test_01_entry_0659_et_allowed(): assert fixed_entry_gate(ny_dt(2026,1,5,6,59))["allowed"] is True

def test_02_entry_0700_et_blocked(): assert fixed_entry_gate(ny_dt(2026,1,5,7,0))["allowed"] is False

def test_03_entry_0959_et_blocked(): assert fixed_entry_gate(ny_dt(2026,1,5,9,59))["allowed"] is False

def test_04_entry_1000_et_allowed(): assert fixed_entry_gate(ny_dt(2026,1,5,10,0))["allowed"] is True

def test_05_entry_1459_et_allowed(): assert fixed_entry_gate(ny_dt(2026,1,5,14,59))["allowed"] is True

def test_06_entry_1500_et_blocked(): assert fixed_entry_gate(ny_dt(2026,1,5,15,0))["allowed"] is False

def test_07_afternoon_window_blocked():
    assert all(not fixed_entry_gate(ny_dt(2026,1,5,h,m))["allowed"] for h,m in [(15,0),(16,0),(18,59)])


def test_08_existing_position_before_0700_managed_after_0700():
    entry=ny_dt(2026,1,5,6,59).astimezone(UTC); sig=entry-timedelta(minutes=1)
    rows=flat_bars(entry,ny_dt(2026,1,5,7,30).astimezone(UTC))
    rows[-1]=bar(rows[-1]["t"],high=1.1022,low=1.0998)
    out=resolve_executed_outcome(sample(sig),rows,horizon_bars=240,config=HistoricalExecutionConfig(entry_slippage_pips=0,exit_slippage_pips=0))
    assert out["status"]=="WIN"


def test_09_existing_position_before_1500_managed_after_1500():
    entry=ny_dt(2026,1,5,14,59).astimezone(UTC); sig=entry-timedelta(minutes=1)
    rows=flat_bars(entry,ny_dt(2026,1,5,15,30).astimezone(UTC))
    rows[-1]=bar(rows[-1]["t"],high=1.1022,low=1.0998)
    out=resolve_executed_outcome(sample(sig),rows,horizon_bars=240,config=HistoricalExecutionConfig(entry_slippage_pips=0,exit_slippage_pips=0))
    assert out["status"]=="WIN"


def test_10_tp_before_1650_is_win():
    entry=ny_dt(2026,1,5,10,0).astimezone(UTC); sig=entry-timedelta(minutes=1)
    out=resolve_executed_outcome(sample(sig),[bar(entry,high=1.1022,low=1.0998)],horizon_bars=240,config=HistoricalExecutionConfig(entry_slippage_pips=0,exit_slippage_pips=0))
    assert out["status"]=="WIN"


def test_11_sl_before_1650_is_loss():
    entry=ny_dt(2026,1,5,10,0).astimezone(UTC); sig=entry-timedelta(minutes=1)
    out=resolve_executed_outcome(sample(sig),[bar(entry,high=1.1002,low=1.0978)],horizon_bars=240,config=HistoricalExecutionConfig(entry_slippage_pips=0,exit_slippage_pips=0))
    assert out["status"]=="LOSS"


def test_12_unresolved_at_1650_is_timeout():
    entry=ny_dt(2026,1,5,15,0).astimezone(UTC) - timedelta(minutes=1)  # 14:59 allowed
    sig=entry-timedelta(minutes=1); close=ny_dt(2026,1,5,16,50).astimezone(UTC)
    rows=flat_bars(entry,close)
    out=resolve_executed_outcome(sample(sig),rows,horizon_bars=240,config=HistoricalExecutionConfig(entry_slippage_pips=0,exit_slippage_pips=0))
    assert out["status"]=="TIMEOUT" and out["label"] is None and out["realized_r"] is None


def test_13_timeout_not_counted_as_loss():
    entry=ny_dt(2026,1,5,14,59).astimezone(UTC); sig=entry-timedelta(minutes=1); close=ny_dt(2026,1,5,16,50).astimezone(UTC)
    out=resolve_executed_outcome(sample(sig),flat_bars(entry,close),horizon_bars=240,config=HistoricalExecutionConfig(entry_slippage_pips=0,exit_slippage_pips=0))
    assert out["status"]=="TIMEOUT" and out["label"] is None


def test_14_no_artificial_timeout_at_240_minutes():
    entry=ny_dt(2026,1,5,10,0).astimezone(UTC); sig=entry-timedelta(minutes=1)
    tp_at=entry+timedelta(minutes=300)
    rows=flat_bars(entry,tp_at)
    rows[-1]=bar(tp_at,high=1.1022,low=1.0998)
    out=resolve_executed_outcome(sample(sig),rows,horizon_bars=240,config=HistoricalExecutionConfig(entry_slippage_pips=0,exit_slippage_pips=0))
    assert out["status"]=="WIN" and out["bars"]>240


def test_15_data_coverage_horizon_is_separate_constant_contract():
    from research_integrity import validate_dataset
    assert "required_end = end_dt + timedelta(minutes=max(0, int(horizon_minutes)))" in __import__("inspect").getsource(validate_dataset)
    assert "horizon_minutes" in __import__("inspect").signature(validate_dataset).parameters


def test_16_dst_spring_0700_boundary():
    assert fixed_entry_gate(datetime(2026,3,8,10,59,tzinfo=UTC))["allowed"] is True
    assert fixed_entry_gate(datetime(2026,3,8,11,0,tzinfo=UTC))["allowed"] is False


def test_17_dst_fall_0700_boundary():
    assert fixed_entry_gate(datetime(2026,11,1,11,59,tzinfo=UTC))["allowed"] is True
    assert fixed_entry_gate(datetime(2026,11,1,12,0,tzinfo=UTC))["allowed"] is False


def test_18_same_bar_tp_sl_remains_ambiguous():
    entry=ny_dt(2026,1,5,10,0).astimezone(UTC); sig=entry-timedelta(minutes=1)
    out=resolve_executed_outcome(sample(sig),[bar(entry,high=1.1022,low=1.0978)],horizon_bars=240,config=HistoricalExecutionConfig(entry_slippage_pips=0,exit_slippage_pips=0))
    assert out["status"]=="AMBIGUOUS" and out["label"] is None


def test_19_midpoint_prohibited():
    entry=ny_dt(2026,1,5,10,0).astimezone(UTC); sig=entry-timedelta(minutes=1)
    out=resolve_executed_outcome(sample(sig),[{"t":entry,"o":1.1,"h":1.2,"l":1.0,"c":1.1}],horizon_bars=240)
    assert out["status"]=="DATA_INSUFFICIENT"


def test_20_phase1_cannot_open_fixed_schedule(tmp_path):
    assert all("SCHEDULE" not in gate and "BLACKOUT" not in gate for gate in RESEARCHABLE_STRATEGY_GATES)
    with pytest.raises(ValueError,match="fixed operational-entry"):
        analyze_phase1(str(write_target(tmp_path,[research_row("2026-01-05T12:00:00Z",operational=False)])))


def test_21_phase2_cannot_open_fixed_schedule():
    rows=[research_row("2026-01-05T12:00:00Z",operational=False)]
    assert _phase1_eligible_rows({"phase1_policy":{"opened_gates":["DIRECTION_SELECTION","MINIMUM_RR"]}},rows)==[]


def test_22_candidate_compiler_cannot_relax_schedule():
    with pytest.raises(CandidateNotDeployable,match="fixed operational schedule"):
        _canonical_rules({"rules":[{"feature":"entry_time_window","operator":">=","threshold":1}]})


def test_23_broad_population_invariant_remains_pass():
    rows=[research_row("2026-01-05T14:00:00Z",actionable=True),research_row("2026-01-05T14:05:00Z",blocks=["M1_CONFIRMATION"])]
    _,e=build_research_target_episodes(rows,gap_minutes=15)
    assert e["status"]=="PASS" and e["baseline_subset_missing"]==0


def test_24_phase1_win_recall_architecture_remains_pass(tmp_path):
    out=analyze_phase1(str(write_target(tmp_path,[research_row("2026-01-05T14:00:00Z",blocks=["MINIMUM_RR"])])))
    assert out["phase1_target_wins"]==1 and out["phase1_recovered_wins"]==1


def test_25_phase2_admitted_loss_contract_remains_pass():
    rows=[research_row("2026-01-05T14:00:00Z",status="WIN",blocks=["M1_CONFIRMATION"]),research_row("2026-01-05T14:05:00Z",status="LOSS",blocks=["M1_CONFIRMATION"])]
    selected=_phase1_eligible_rows({"phase1_policy":{"opened_gates":["M1_CONFIRMATION"]}},rows)
    assert [x["outcome_status"] for x in selected]==["WIN","LOSS"]


def test_26_operational_contract_does_not_touch_freeze_holdout():
    import operational_time
    text=__import__("inspect").getsource(operational_time)
    assert "freeze" not in text.lower() and "holdout" not in text.lower()


def test_27_production_authority_false_in_phase1(tmp_path):
    out=analyze_phase1(str(write_target(tmp_path,[research_row("2026-01-05T14:00:00Z",blocks=["MINIMUM_RR"])])))
    assert out["production_authority"] is False


def test_28_multi_asset_episode_isolation_preserved():
    rows=[research_row("2026-01-05T14:00:00Z",actionable=True,instrument="AUD_USD"),research_row("2026-01-05T14:00:00Z",actionable=True,instrument="EUR_USD")]
    episodes,_=build_research_target_episodes(rows,gap_minutes=15)
    assert {x["instrument"] for x in episodes}=={"AUD_USD","EUR_USD"}

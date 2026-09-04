from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise RuntimeError(f"expected exactly one match in {path}: {old[:80]!r}; got {text.count(old)}")
    p.write_text(text.replace(old, new), encoding="utf-8")


Path("operational_time.py").write_text(r'''"""Fixed operational time contract shared by historical research.

Research-only helpers mirror the production schedule without importing server.py.
The schedule is not candidate-tunable and is intentionally excluded from the
Phase 1/Phase 2 researchable gate surface.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
ENTRY_BLOCKED_WINDOWS = ((7 * 60, 10 * 60), (15 * 60, 19 * 60))
OPERATIONAL_CLOSE_MINUTE = 16 * 60 + 50
NON_RESEARCHABLE_OPERATIONAL_GATES = (
    "NY_ENTRY_BLACKOUT_07_10",
    "NY_ENTRY_BLACKOUT_15_19",
)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def fixed_entry_gate(at: datetime) -> Dict[str, Any]:
    """Return fixed NEW-entry eligibility using America/New_York DST rules."""
    ny = _aware(at).astimezone(NY)
    mins = ny.hour * 60 + ny.minute
    morning = 7 * 60 <= mins < 10 * 60
    afternoon = 15 * 60 <= mins < 19 * 60
    if morning:
        reason = "NY_ENTRY_BLACKOUT_07_10"
        window = "07:00-10:00 America/New_York"
    elif afternoon:
        reason = "NY_ENTRY_BLACKOUT_15_19"
        window = "15:00-19:00 America/New_York"
    else:
        reason = "ALLOWED"
        window = None
    return {
        "allowed": not (morning or afternoon),
        "reason": reason,
        "ny_time": ny.isoformat(),
        "window": window,
        "blocked_windows": ["07:00-10:00", "15:00-19:00"],
        "timezone": "America/New_York",
        "researchable": False,
    }


def planned_entry_time(signal_candle_start: datetime, latency_bars: int = 0) -> datetime:
    """First executable M1 open after a completed signal candle."""
    return _aware(signal_candle_start).astimezone(timezone.utc) + timedelta(minutes=1 + max(0, int(latency_bars)))


def operational_close_after(entry_at: datetime) -> datetime:
    """First weekday 16:50 ET operational close strictly after an entry time."""
    ny = _aware(entry_at).astimezone(NY)
    candidate = ny.replace(hour=16, minute=50, second=0, microsecond=0)
    if ny.weekday() < 5 and ny < candidate:
        return candidate.astimezone(timezone.utc)
    day = (ny + timedelta(days=1)).date()
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return datetime(day.year, day.month, day.day, 16, 50, tzinfo=NY).astimezone(timezone.utc)


def minutes_needed_through_close(signal_candle_start: datetime, latency_bars: int = 0) -> int:
    entry = planned_entry_time(signal_candle_start, latency_bars)
    close = operational_close_after(entry)
    seconds = max(0.0, (close - _aware(signal_candle_start).astimezone(timezone.utc)).total_seconds())
    return int(seconds // 60) + 2
''', encoding="utf-8")

# Historical execution: keep bid/ask and same-bar conservatism, but separate
# outcome lifetime from the data-coverage horizon argument.
replace_once(
    "historical_execution.py",
    "from research_evidence import pip_size\n",
    "from research_evidence import pip_size\nfrom operational_time import fixed_entry_gate, operational_close_after\n",
)
replace_once(
    "historical_execution.py",
    '''    if direction == "BUY":\n        entry_fill = ask_o + entry_slip\n        if not (stop < entry_fill < target):\n''',
    '''    entry_ts = _dt(first["t"])\n    operational_gate = fixed_entry_gate(entry_ts)\n    if operational_gate["allowed"] is not True:\n        out = _invalid(f"new entry blocked by fixed operational schedule: {operational_gate['reason']}", "ENTRY_BLOCKED_OPERATIONAL_TIME")\n        out.update({"entry_ts": entry_ts.isoformat(), "operational_entry_gate": operational_gate})\n        return out\n    operational_close = operational_close_after(entry_ts)\n\n    if direction == "BUY":\n        entry_fill = ask_o + entry_slip\n        if not (stop < entry_fill < target):\n''',
)
replace_once(
    "historical_execution.py",
    '''    max_bars = max(1, int(horizon_bars))\n\n    for idx, bar in enumerate(bars[:max_bars], start=1):\n        if config.require_bid_ask and not has_bid_ask(bar):\n''',
    '''    data_coverage_horizon_bars = max(1, int(horizon_bars))\n\n    for idx, bar in enumerate(bars, start=1):\n        bar_ts = _dt(bar["t"])\n        if bar_ts >= operational_close:\n            return {\n                "status": "TIMEOUT", "label": None, "bars": idx - 1,\n                "entry_ts": entry_ts.isoformat(), "exit_ts": operational_close.isoformat(),\n                "entry_fill": entry_fill, "planned_entry": planned_entry,\n                "entry_spread_pips": entry_spread_pips,\n                "entry_slippage_pips": float(config.entry_slippage_pips),\n                "exit_slippage_pips": float(config.exit_slippage_pips),\n                "mfe_r": mfe, "mae_r": mae, "realized_r": None,\n                "data_coverage_horizon_bars": data_coverage_horizon_bars,\n                "operational_close": operational_close.isoformat(),\n                "note": "Neither TP nor SL touched before 16:50 America/New_York operational close",\n            }\n        if config.require_bid_ask and not has_bid_ask(bar):\n''',
)
replace_once(
    "historical_execution.py",
    '''            "entry_ts": _dt(first["t"]).isoformat(),\n            "exit_ts": _dt(bar["t"]).isoformat(),\n''',
    '''            "entry_ts": entry_ts.isoformat(),\n            "exit_ts": bar_ts.isoformat(),\n''',
)
replace_once(
    "historical_execution.py",
    '''            "mae_r": mae,\n        }\n''',
    '''            "mae_r": mae,\n            "data_coverage_horizon_bars": data_coverage_horizon_bars,\n            "operational_close": operational_close.isoformat(),\n        }\n''',
)
replace_once(
    "historical_execution.py",
    '''    if len(bars) >= max_bars:\n        last = bars[max_bars - 1]\n        return {\n            "status": "TIMEOUT", "label": None, "bars": max_bars,\n            "entry_ts": _dt(first["t"]).isoformat(), "exit_ts": _dt(last["t"]).isoformat(),\n            "entry_fill": entry_fill, "planned_entry": planned_entry,\n            "entry_spread_pips": entry_spread_pips,\n            "entry_slippage_pips": float(config.entry_slippage_pips),\n            "exit_slippage_pips": float(config.exit_slippage_pips),\n            "mfe_r": mfe, "mae_r": mae, "realized_r": None,\n            "note": f"No resolution in {max_bars} executable M1 bars",\n        }\n    return None\n''',
    '''    # Exhausting the caller's available candles before operational close is\n    # not a trade TIMEOUT.  It is unresolved evidence and must remain PENDING so\n    # data coverage can be repaired independently of trade lifetime semantics.\n    return None\n''',
)

# Historical replay: operational schedule is applied before broad episode identity;
# it is never emitted as a researchable blocker.
replace_once(
    "historical_replay.py",
    "from historical_execution import HistoricalExecutionConfig, resolve_executed_outcome\n",
    "from historical_execution import HistoricalExecutionConfig, resolve_executed_outcome\nfrom operational_time import fixed_entry_gate, minutes_needed_through_close, planned_entry_time\n",
)
replace_once(
    "historical_replay.py",
    '''    for row in rows:\n        direction = str(row.get("signal") or "").upper()\n''',
    '''    for row in rows:\n        if row.get("operational_entry_allowed") is False:\n            continue\n        direction = str(row.get("signal") or "").upper()\n''',
)
replace_once(
    "historical_replay.py",
    '''    future=store.future_m1_after(_dt(row["candle_ts"]), config.horizon_bars + max(0, int(config.execution.latency_bars)) + 1)\n    out=resolve_executed_outcome(payload,future,horizon_bars=config.horizon_bars,config=config.execution)\n''',
    '''    signal_ts=_dt(row["candle_ts"])\n    operational_needed=minutes_needed_through_close(signal_ts, config.execution.latency_bars)\n    future_count=max(\n        config.horizon_bars + max(0, int(config.execution.latency_bars)) + 1,\n        operational_needed,\n    )\n    future=store.future_m1_after(signal_ts, future_count)\n    out=resolve_executed_outcome(payload,future,horizon_bars=config.horizon_bars,config=config.execution)\n''',
)
replace_once(
    "historical_replay.py",
    '''            row=replay_snapshot(server,h1,m15,m5,m1,inst,v,hypotheses=hypotheses)\n            if not row["actionable"]:\n''',
    '''            row=replay_snapshot(server,h1,m15,m5,m1,inst,v,hypotheses=hypotheses)\n            planned_entry=planned_entry_time(_dt(row["candle_ts"]), config.execution.latency_bars)\n            operational_gate=fixed_entry_gate(planned_entry)\n            row["planned_entry_ts"]=planned_entry.isoformat()\n            row["operational_entry_allowed"]=bool(operational_gate["allowed"])\n            row["operational_entry_gate"]=operational_gate\n            if not operational_gate["allowed"]:\n                row["actionable"]=False\n                row["decision_reason"]="OPERATIONAL:"+str(operational_gate["reason"])\n            if not row["actionable"]:\n''',
)
replace_once(
    "historical_replay.py",
    '''                           "require_bid_ask":config.execution.require_bid_ask,\n                           "validation":"CHRONOLOGICAL_HOLDOUT_PLUS_WALK_FORWARD_WITH_PURGING_AND_EMBARGO",\n''',
    '''                           "require_bid_ask":config.execution.require_bid_ask,\n                           "fixed_entry_windows_et":["07:00-10:00","15:00-19:00"],\n                           "operational_entry_schedule_researchable":False,\n                           "trade_outcome_lifetime":"TP_OR_SL_OR_16:50_AMERICA_NEW_YORK",\n                           "data_coverage_horizon_bars":config.horizon_bars,\n                           "data_coverage_horizon_is_trade_timeout":False,\n                           "validation":"CHRONOLOGICAL_HOLDOUT_PLUS_WALK_FORWARD_WITH_PURGING_AND_EMBARGO",\n''',
)

# Phase 1 and Phase 2 fail closed if a malformed target artifact contains a fixed
# schedule-blocked row.  Such rows are not targets to recover.
replace_once(
    "research_pipeline.py",
    '''    rows = list(population.get("episodes") or [])\n    _validate_outcomes(rows)\n''',
    '''    rows = list(population.get("episodes") or [])\n    if any(row.get("operational_entry_allowed") is False for row in rows):\n        raise ValueError("Target population contains fixed operational-entry blocked episode")\n    _validate_outcomes(rows)\n''',
)
replace_once(
    "research_pipeline.py",
    '''    all_rows = list(source.get("episodes") or [])\n    _validate_outcomes(all_rows)\n''',
    '''    all_rows = list(source.get("episodes") or [])\n    if any(row.get("operational_entry_allowed") is False for row in all_rows):\n        raise ValueError("Phase 1 cannot research fixed operational-entry blocked episodes")\n    _validate_outcomes(all_rows)\n''',
)
replace_once(
    "research_phase2.py",
    '''    rows = list(source.get("episodes") or [])\n    validate_rows(rows)\n''',
    '''    rows = list(source.get("episodes") or [])\n    if any(row.get("operational_entry_allowed") is False for row in rows):\n        raise ValueError("Phase 2 cannot research fixed operational-entry blocked episodes")\n    validate_rows(rows)\n''',
)
replace_once(
    "research_phase2.py",
    '''    opened = set((spec.get("phase1_policy") or {}).get("opened_gates") or [])\n    return [dict(row) for row in rows if _eligible(row, opened)]\n''',
    '''    opened = set((spec.get("phase1_policy") or {}).get("opened_gates") or [])\n    return [\n        dict(row) for row in rows\n        if row.get("operational_entry_allowed") is not False and _eligible(row, opened)\n    ]\n''',
)

# Candidate compiler gives an explicit fixed-schedule error before normal feature
# allow-list handling; candidates can never encode time-window relaxation.
replace_once(
    "automation_v3_candidate_mapping.py",
    'MAX_COMPOSITE_RULES = 3\n',
    'MAX_COMPOSITE_RULES = 3\nPROHIBITED_OPERATIONAL_FEATURE_MARKERS = ("entry_time", "entry_window", "schedule", "blackout", "operational_time")\n',
)
replace_once(
    "automation_v3_candidate_mapping.py",
    '''        feature = str(raw.get("feature") or "")\n        operator = str(raw.get("operator") or "")\n        if feature not in APPROVED_FEATURES:\n''',
    '''        feature = str(raw.get("feature") or "")\n        operator = str(raw.get("operator") or "")\n        if any(marker in feature.lower() for marker in PROHIBITED_OPERATIONAL_FEATURE_MARKERS):\n            raise CandidateNotDeployable("candidate cannot relax fixed operational schedule")\n        if feature not in APPROVED_FEATURES:\n''',
)

# Existing historical execution test must no longer expect fixed-horizon timeout;
# the other bid/ask and ambiguity tests remain unchanged.

Path("test_operational_time_outcome.py").write_text(r'''from datetime import datetime, timedelta, timezone
import json

import pytest

from automation_v3_candidate_mapping import CandidateNotDeployable, _canonical_rules
from historical_execution import HistoricalExecutionConfig, resolve_executed_outcome
from historical_replay import build_research_target_episodes
from operational_time import NY, fixed_entry_gate, operational_close_after
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
    import research_integrity
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
''', encoding="utf-8")

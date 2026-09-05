from datetime import datetime, timedelta, timezone
import json

from autonomous_asset_optimizer import (
    LOOKBACK_SEQUENCE, MAX_RESEARCH_LOOKBACK_MONTHS, aligned_research_end,
)
from automation_v3_integrity_recovery import build_integrity_diagnostic
from research_integrity import TIMEFRAME_SECONDS, validate_dataset


def _rows(tf, first, last_open):
    step = timedelta(seconds=TIMEFRAME_SECONDS[tf])
    out=[]; cur=first
    while cur <= last_open:
        row={"t":cur.isoformat(),"o":1.0,"h":1.1,"l":0.9,"c":1.0}
        if tf == "M1":
            row.update({"bid_o":1.0,"bid_h":1.1,"bid_l":0.9,"bid_c":1.0,"ask_o":1.01,"ask_h":1.11,"ask_l":0.91,"ask_c":1.01})
        out.append(row);cur+=step
    return out


def _bundle(first, ends):
    return {tf:_rows(tf,first,ends[tf]) for tf in TIMEFRAME_SECONDS}


def _validate(tmp_path, end, ends):
    tmp_path.mkdir(parents=True, exist_ok=True)
    first=end-timedelta(days=11)
    cache=tmp_path/"cache.json";cache.write_text(json.dumps(_bundle(first,ends)),encoding="utf-8")
    return validate_dataset(str(cache),instrument="AUD_USD",start=(end-timedelta(days=1)).isoformat(),end=end.isoformat(),warmup_days=10,horizon_minutes=240,repo=tmp_path)


def test_run_33925240916_shape_is_strictly_incomplete(tmp_path, monkeypatch):
    monkeypatch.setattr('research_integrity._git_head', lambda repo:'a'*40)
    end=datetime(2026,9,4,18,0,tzinfo=timezone.utc)
    ends={"H1":datetime(2026,9,4,20,0,tzinfo=timezone.utc),"M15":datetime(2026,9,4,20,45,tzinfo=timezone.utc),"M5":datetime(2026,9,4,20,55,tzinfo=timezone.utc),"M1":datetime(2026,9,4,20,59,tzinfo=timezone.utc)}
    out=_validate(tmp_path,end,ends)
    assert out["required_horizon_end"]=="2026-09-04T22:00:00+00:00"
    assert set(out["failures"]) >= {f"{tf}_HORIZON_COVERAGE_INCOMPLETE" for tf in TIMEFRAME_SECONDS}
    assert all(v["actual_complete_end"]=="2026-09-04T21:00:00+00:00" for v in out["coverage"].values())


def test_exact_completed_candle_coverage_passes_all_timeframes(tmp_path, monkeypatch):
    monkeypatch.setattr('research_integrity._git_head', lambda repo:'a'*40)
    end=datetime(2026,9,4,17,0,tzinfo=timezone.utc)
    ends={"H1":datetime(2026,9,4,20,0,tzinfo=timezone.utc),"M15":datetime(2026,9,4,20,45,tzinfo=timezone.utc),"M5":datetime(2026,9,4,20,55,tzinfo=timezone.utc),"M1":datetime(2026,9,4,20,59,tzinfo=timezone.utc)}
    out=_validate(tmp_path,end,ends)
    assert out["required_horizon_end"]=="2026-09-04T21:00:00+00:00"
    assert not [x for x in out["failures"] if "HORIZON_COVERAGE_INCOMPLETE" in x]
    assert all(v["horizon_covered"] for v in out["coverage"].values())


def test_one_candle_short_fails_each_timeframe(tmp_path, monkeypatch):
    monkeypatch.setattr('research_integrity._git_head', lambda repo:'a'*40)
    end=datetime(2026,9,4,17,0,tzinfo=timezone.utc);required=end+timedelta(minutes=240)
    for tf,seconds in TIMEFRAME_SECONDS.items():
        exact={"H1":datetime(2026,9,4,20,0,tzinfo=timezone.utc),"M15":datetime(2026,9,4,20,45,tzinfo=timezone.utc),"M5":datetime(2026,9,4,20,55,tzinfo=timezone.utc),"M1":datetime(2026,9,4,20,59,tzinfo=timezone.utc)}
        exact[tf]-=timedelta(seconds=seconds)
        out=_validate(tmp_path/tf,end,exact)
        assert f"{tf}_HORIZON_COVERAGE_INCOMPLETE" in out["failures"]
        assert out["coverage"][tf]["actual_complete_end"] < required.isoformat()


def test_friday_alignment_avoids_impossible_weekend_horizon():
    now=datetime(2026,9,4,22,23,tzinfo=timezone.utc)
    assert aligned_research_end(now,240)==datetime(2026,9,4,17,0,tzinfo=timezone.utc)


def test_sunday_before_four_hours_of_reopen_uses_friday_window():
    now=datetime(2026,9,6,23,0,tzinfo=timezone.utc)  # Sunday 19:00 ET, only 2h after reopen
    assert aligned_research_end(now,240)==datetime(2026,9,4,17,0,tzinfo=timezone.utc)


def test_sunday_after_four_hours_of_reopen_can_use_recent_window():
    now=datetime(2026,9,7,1,0,tzinfo=timezone.utc)  # Sunday 21:00 ET
    assert aligned_research_end(now,240)==datetime(2026,9,6,21,0,tzinfo=timezone.utc)


def test_max_lookback_policy_is_explicitly_three_months():
    assert LOOKBACK_SEQUENCE==(1,2,3)
    assert MAX_RESEARCH_LOOKBACK_MONTHS==3
    assert 6 not in LOOKBACK_SEQUENCE and 12 not in LOOKBACK_SEQUENCE


def test_retry_horizon_diagnostic_does_not_recommend_backward_expansion(tmp_path):
    artifact=tmp_path/"01.json"
    report={"status":"FAIL","failures":["M1_HORIZON_COVERAGE_INCOMPLETE"],"warmup_days":10,"horizon_minutes":240,"required_horizon_end":"2026-09-04T21:00:00+00:00","bid_ask_real":True,"midpoint_only":False,"gaps_present":False,"coverage":{"M1":{"warmup_covered":True,"horizon_covered":False}},"timeframes":{"M1":{"count":1,"first":"2026-09-04T20:58:00+00:00","last":"2026-09-04T20:59:00+00:00","coverage_end":"2026-09-04T21:00:00+00:00","non_weekend_gaps":0}}}
    artifact.write_text(json.dumps(report),encoding="utf-8")
    diag=build_integrity_diagnostic(report,artifact_path=artifact,cache_path=tmp_path/"cache.json",requested_start="2026-08-04T17:00:00+00:00",requested_end="2026-09-04T17:00:00+00:00",retry_count=1)
    assert diag["recommended_action"]=="DATA_COVERAGE_INSUFFICIENT"
    assert diag["required_horizon_end"]=="2026-09-04T21:00:00+00:00"
    assert diag["production_authority"] is False

import json

from research_pipeline import analyze_phase1, extract_target_population


def _row(status, reason="REPLAY_ACTIONABLE", safety=None, ts="2026-01-01T00:00:00Z"):
    return {
        "candle_ts":ts,"outcome_status":status,
        "label":1 if status=="WIN" else (0 if status=="LOSS" else None),
        "decision_reason":reason,"safety_checks":safety or {},"research_direction":"BUY",
    }


def test_extract_target_population_preserves_outcomes(tmp_path):
    replay=tmp_path/"replay.json"
    replay.write_text(json.dumps({
        "instrument":"AUD_USD",
        "methodology":{"no_lookahead_decision":True,"future_bars_only_for_outcome":True},
        "variants":{"V331_BASELINE":{"target_population":{"enabled":True,"scope":"RESEARCH_ONLY","episodes":[
            _row("WIN"),_row("TIMEOUT",ts="2026-01-02T00:00:00Z"),_row("AMBIGUOUS",ts="2026-01-03T00:00:00Z")
        ]}}},
    }),encoding="utf-8")
    out=extract_target_population(str(replay),"V331_BASELINE")
    assert out["outcomes"]=={"AMBIGUOUS":1,"TIMEOUT":1,"WIN":1}
    assert out["episodes"][1]["label"] is None


def test_phase1_prefers_minimum_losses_after_recovering_all_wins(tmp_path):
    source=tmp_path/"target.json"
    rows=[
        _row("WIN","QUALITY:M1_CONFIRMATION"),
        _row("WIN","QUALITY:EXTENSION",ts="2026-01-02T00:00:00Z"),
        _row("LOSS","QUALITY:M1_CONFIRMATION",ts="2026-01-03T00:00:00Z"),
        _row("LOSS","SAFETY:barrier_room_ok",{"barrier_room_ok":False},"2026-01-04T00:00:00Z"),
    ]
    source.write_text(json.dumps({"instrument":"AUD_USD","variant":"V331_BASELINE","lookahead_protection":True,"episodes":rows}),encoding="utf-8")
    out=analyze_phase1(str(source))
    assert out["status"]=="OK"
    assert out["all_target_wins_recovered"] is True
    assert out["best_policy"]["opened_gates"]==["M1_CONFIRMATION","QUALITY_EXTENSION"]
    assert out["best_policy"]["losses_released"]==1


def test_phase1_researches_minimum_rr_but_preserves_hard_safety(tmp_path):
    source=tmp_path/"target.json"
    source.write_text(json.dumps({
        "instrument":"AUD_USD","variant":"V331_BASELINE","lookahead_protection":True,
        "episodes":[
            _row("WIN","SAFETY:minimum_rr",{"minimum_rr":False}),
            _row("WIN","SAFETY:finite_prices",{"finite_prices":False},"2026-01-02T00:00:00Z"),
        ],
    }),encoding="utf-8")
    out=analyze_phase1(str(source))
    assert "MINIMUM_RR" in out["best_policy"]["opened_gates"]
    assert out["phase1_recovered_wins"]==1
    assert out["status"]=="REVIEW_REQUIRED"
    assert out["unrecovered_target_wins"][0]["immutable_blocks"]==["SAFETY:finite_prices"]

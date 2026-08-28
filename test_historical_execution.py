from datetime import datetime, timedelta, timezone

from historical_execution import HistoricalExecutionConfig, resolve_executed_outcome

UTC=timezone.utc

def bar(t,bid_o,bid_h,bid_l,bid_c,spread=0.0002):
    return {"t":t,"o":bid_o+spread/2,"h":bid_h+spread/2,"l":bid_l+spread/2,"c":bid_c+spread/2,
            "bid_o":bid_o,"bid_h":bid_h,"bid_l":bid_l,"bid_c":bid_c,
            "ask_o":bid_o+spread,"ask_h":bid_h+spread,"ask_l":bid_l+spread,"ask_c":bid_c+spread,"v":1}

def sample(direction="BUY"):
    if direction=="BUY":
        return {"candle_ts":"2026-01-01T10:00:00+00:00","direction":"BUY","entry":1.1000,"stop":1.0990,"target":1.1020,"instrument":"EUR_USD"}
    return {"candle_ts":"2026-01-01T10:00:00+00:00","direction":"SELL","entry":1.1000,"stop":1.1010,"target":1.0980,"instrument":"EUR_USD"}

def test_buy_crosses_ask_and_uses_bid_for_exit_without_double_cost():
    t=datetime(2026,1,1,10,1,tzinfo=UTC)
    rows=[bar(t,1.1000,1.1021,1.0998,1.1018,spread=0.0002)]
    cfg=HistoricalExecutionConfig(entry_slippage_pips=0.0,exit_slippage_pips=0.0)
    out=resolve_executed_outcome(sample("BUY"),rows,horizon_bars=10,config=cfg)
    assert out["status"]=="WIN"
    assert abs(out["entry_fill"]-1.1002)<1e-12
    assert abs(out["realized_r"]-1.8)<1e-9

def test_sell_crosses_bid_and_uses_ask_for_exit():
    t=datetime(2026,1,1,10,1,tzinfo=UTC)
    rows=[bar(t,1.1000,1.1001,1.0977,1.0980,spread=0.0002)]
    cfg=HistoricalExecutionConfig(entry_slippage_pips=0.0,exit_slippage_pips=0.0)
    out=resolve_executed_outcome(sample("SELL"),rows,horizon_bars=10,config=cfg)
    assert out["status"]=="WIN"
    assert abs(out["entry_fill"]-1.1000)<1e-12
    assert abs(out["realized_r"]-2.0)<1e-9

def test_adverse_slippage_is_applied_once_in_fill_prices():
    t=datetime(2026,1,1,10,1,tzinfo=UTC)
    rows=[bar(t,1.1000,1.1022,1.0998,1.1018,spread=0.0002)]
    cfg=HistoricalExecutionConfig(entry_slippage_pips=0.1,exit_slippage_pips=0.1)
    out=resolve_executed_outcome(sample("BUY"),rows,horizon_bars=10,config=cfg)
    assert out["status"]=="WIN"
    # ask entry 1.1002 + 0.1 pip; target fill 1.1020 - 0.1 pip => 1.78R
    assert abs(out["realized_r"]-1.78)<1e-9

def test_missing_bid_ask_is_explicit_data_failure_not_midpoint_fallback():
    t=datetime(2026,1,1,10,1,tzinfo=UTC)
    rows=[{"t":t,"o":1.1,"h":1.103,"l":1.098,"c":1.101,"v":1}]
    out=resolve_executed_outcome(sample("BUY"),rows,horizon_bars=10)
    assert out["status"]=="DATA_INSUFFICIENT"
    assert out["realized_r"] is None

def test_same_bar_tp_and_sl_is_ambiguous():
    t=datetime(2026,1,1,10,1,tzinfo=UTC)
    rows=[bar(t,1.1000,1.1022,1.0988,1.1005,spread=0.0002)]
    out=resolve_executed_outcome(sample("BUY"),rows,horizon_bars=10,config=HistoricalExecutionConfig(entry_slippage_pips=0,exit_slippage_pips=0))
    assert out["status"]=="AMBIGUOUS"
    assert out["realized_r"] is None

def test_entry_is_not_forced_when_market_has_moved_beyond_target():
    t=datetime(2026,1,1,10,1,tzinfo=UTC)
    rows=[bar(t,1.1021,1.1030,1.1020,1.1025,spread=0.0002)]
    out=resolve_executed_outcome(sample("BUY"),rows,horizon_bars=10,config=HistoricalExecutionConfig(entry_slippage_pips=0,exit_slippage_pips=0))
    assert out["status"]=="ENTRY_INVALIDATED"
    assert out["realized_r"] is None

from datetime import datetime, timezone, timedelta

from research_evidence import collapse_market_episodes, economic_realized_r, resolve_outcome


def bar(ts, o, h, l, c):
    return {"t": ts, "o": o, "h": h, "l": l, "c": c}


def test_episode_collapse_uses_first_opportunity_and_gap():
    base=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    rows=[
        {"candle_ts":(base+timedelta(minutes=i)).isoformat(),"instrument":"EUR_USD","signal":"BUY","id":i}
        for i in (0,1,2,20,21)
    ]
    out=collapse_market_episodes(rows,gap_minutes=15)
    assert [x["id"] for x in out]==[0,20]


def test_cost_aware_buy_moves_target_farther_and_stop_closer():
    start=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    sample={"candle_ts":start.isoformat(),"instrument":"EUR_USD","direction":"BUY",
            "entry":1.1000,"stop":1.0990,"target":1.1015}
    bars=[bar(start+timedelta(minutes=1),1.1000,1.10165,1.0995,1.1010)]
    out=resolve_outcome(sample,bars,horizon_bars=10,round_trip_cost_pips=1.0)
    assert out["status"]=="WIN"
    assert round(out["effective_target"],5)==1.1016
    assert round(out["effective_stop"],5)==1.0991
    assert round(out["cost_r"],3)==0.1


def test_timeout_is_explicit_unlabeled_terminal_state():
    start=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    sample={"candle_ts":start.isoformat(),"instrument":"EUR_USD","direction":"BUY",
            "entry":1.1000,"stop":1.0990,"target":1.1015}
    bars=[bar(start+timedelta(minutes=i+1),1.1000,1.1002,1.0998,1.1000) for i in range(3)]
    out=resolve_outcome(sample,bars,horizon_bars=3,round_trip_cost_pips=0)
    assert out["status"]=="TIMEOUT"
    assert out["label"] is None


def test_same_bar_tp_sl_is_ambiguous():
    start=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    sample={"candle_ts":start.isoformat(),"instrument":"EUR_USD","direction":"BUY",
            "entry":1.1000,"stop":1.0990,"target":1.1010}
    bars=[bar(start+timedelta(minutes=1),1.1000,1.1011,1.0989,1.1001)]
    out=resolve_outcome(sample,bars,horizon_bars=3,round_trip_cost_pips=0)
    assert out["status"]=="AMBIGUOUS"
    assert out["label"] is None

from research_evidence import annotate_market_episodes, split_episode_holdout


def test_episode_annotation_ignores_source_and_variant():
    base=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    rows=[
        {"candle_ts":base.isoformat(),"instrument":"EUR_USD","signal":"BUY","source":"CANONICAL","variant":None},
        {"candle_ts":(base+timedelta(minutes=1)).isoformat(),"instrument":"EUR_USD","signal":"BUY","source":"SHADOW","variant":"BASELINE"},
        {"candle_ts":(base+timedelta(minutes=2)).isoformat(),"instrument":"EUR_USD","signal":"BUY","source":"SHADOW","variant":"TARGET_2R"},
    ]
    out=annotate_market_episodes(rows,gap_minutes=15)
    assert len({x["episode_id"] for x in out})==1


def test_holdout_never_splits_an_episode_and_is_chronological():
    base=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    rows=[]
    for episode,minute in (("e1",0),("e2",30),("e3",60),("e4",90)):
        for source in ("CANONICAL","SHADOW"):
            rows.append({"episode_id":episode,"candle_ts":(base+timedelta(minutes=minute)).isoformat(),"source":source})
    discovery,validation=split_episode_holdout(rows,.25)
    d={x["episode_id"] for x in discovery};v={x["episode_id"] for x in validation}
    assert not (d & v)
    assert v=={"e4"}
    assert max(x["candle_ts"] for x in discovery) < min(x["candle_ts"] for x in validation)


def test_shifted_barrier_cost_is_counted_exactly_once_in_win_pnl():
    start=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    sample={"candle_ts":start.isoformat(),"instrument":"EUR_USD","direction":"BUY",
            "entry":1.1000,"stop":1.0990,"target":1.1015}
    bars=[bar(start+timedelta(minutes=1),1.1000,1.10161,1.0995,1.1010)]
    out=resolve_outcome(sample,bars,horizon_bars=10,round_trip_cost_pips=1.0)
    assert out["status"]=="WIN"
    assert round(out["cost_r"],3)==0.1
    # Gross midpoint move to the shifted TP is 1.6R; subtracting 0.1R cost once = 1.5R.
    assert round(economic_realized_r(sample,out),6)==1.5


def test_shifted_barrier_cost_is_counted_exactly_once_in_loss_pnl():
    start=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    sample={"candle_ts":start.isoformat(),"instrument":"EUR_USD","direction":"BUY",
            "entry":1.1000,"stop":1.0990,"target":1.1015}
    bars=[bar(start+timedelta(minutes=1),1.1000,1.1001,1.09905,1.0991)]
    out=resolve_outcome(sample,bars,horizon_bars=10,round_trip_cost_pips=1.0)
    assert out["status"]=="LOSS"
    # Gross midpoint loss to the shifted SL is -0.9R; subtracting 0.1R cost once = -1.0R.
    assert round(economic_realized_r(sample,out),6)==-1.0

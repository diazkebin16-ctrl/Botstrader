from datetime import datetime, timedelta, timezone
from research_validation import (
    normalize_trade_memory, annotate_trade_episodes, collapse_trade_episodes,
    performance_metrics, walk_forward_optimize, WalkForwardConfig,
    session_opposition_policy,
)


def _raw(i, r, direction="LONG", session_dir="BUY", strength=.8, minute=None):
    t=datetime(2026,1,1,tzinfo=timezone.utc)+timedelta(minutes=(minute if minute is not None else i*20))
    return {
        "trade_id":f"t{i}","symbol":"EUR_USD","direction":direction,"status":"CLOSED",
        "entry_ts":t.isoformat(),"exit_ts":(t+timedelta(minutes=5)).isoformat(),"realized_r":r,
        "entry_context_json":{"features":{"session_direction":session_dir,"session_strength":strength},"filters":{}},
    }


def test_normalization_hides_outcome_from_pretrade():
    x=normalize_trade_memory([_raw(1,1.5)])[0]
    assert "realized_r" not in x["pretrade"]
    assert x["pretrade"]["features"]["session_direction"]=="BUY"


def test_episode_gap_is_consecutive_not_from_first():
    rows=normalize_trade_memory([_raw(1,1,minute=0),_raw(2,-1,minute=10),_raw(3,1,minute=20),_raw(4,1,minute=40)])
    tagged=annotate_trade_episodes(rows,15)
    assert tagged[0]["episode_id"]==tagged[2]["episode_id"]
    assert tagged[3]["episode_id"]!=tagged[2]["episode_id"]
    assert len(collapse_trade_episodes(rows,15))==2


def test_session_opposition_uses_only_pretrade_features():
    buy_against_sell=normalize_trade_memory([_raw(1,-1,session_dir="SELL",strength=.8)])[0]
    assert session_opposition_policy({"direction":"BUY","pretrade":buy_against_sell["pretrade"]},.55) is False
    assert session_opposition_policy({"direction":"BUY","pretrade":buy_against_sell["pretrade"]},.85) is True


def test_walk_forward_never_splits_episode_between_train_test():
    rows=normalize_trade_memory([_raw(i,1 if i%3 else -1,minute=i*20) for i in range(1,81)])
    cfg=WalkForwardConfig(train_episodes=30,test_episodes=10,step_episodes=10,min_train_selected=10,min_test_selected=3)
    out=walk_forward_optimize(rows,[.25,.55,.85],session_opposition_policy,cfg)
    assert out["valid_windows"]>0
    for w in out["windows"]:
        if "train_episode_ids" in w:
            assert not (set(w["train_episode_ids"]) & set(w["test_episode_ids"]))


def test_parameter_selection_is_train_only():
    rows=[]
    # Train: session opposition loses -> low threshold blocks those losses.
    for i in range(1,31):
        sd="SELL" if i%2==0 else "BUY"
        rv=-1 if sd=="SELL" else 1.5
        rows.append(_raw(i,rv,session_dir=sd,strength=.8,minute=i*20))
    # Test is deliberately reversed. A leaky optimizer would prefer a high threshold.
    for i in range(31,41):
        sd="SELL" if i%2==0 else "BUY"
        rv=1.5 if sd=="SELL" else -1
        rows.append(_raw(i,rv,session_dir=sd,strength=.8,minute=i*20))
    norm=normalize_trade_memory(rows)
    cfg=WalkForwardConfig(train_episodes=30,test_episodes=10,step_episodes=10,min_train_selected=10,min_test_selected=3)
    out=walk_forward_optimize(norm,[.55,.85],session_opposition_policy,cfg)
    w=out["windows"][0]
    assert w["chosen_parameter"]==.55


def test_metrics_include_expectancy_pf_drawdown_ci():
    rows=normalize_trade_memory([_raw(1,1.5),_raw(2,-1),_raw(3,1.5),_raw(4,-1)])
    m=performance_metrics(rows)
    assert m["episodes"]==4
    assert abs(m["expectancy_r"]-.25)<1e-12
    assert abs(m["profit_factor"]-1.5)<1e-12
    assert m["max_drawdown_r"]>=1.0
    assert m["expectancy_95ci_low"] is not None

from datetime import datetime, timedelta, timezone
from replay_validation import ReplayValidationConfig, chronological_holdout, walk_forward_splits

UTC=timezone.utc

def rows(n=30):
    t=datetime(2026,1,1,0,0,tzinfo=UTC)
    out=[]
    for i in range(n):
        ts=t+timedelta(minutes=10*i)
        out.append({"candle_ts":ts.isoformat(),"exit_ts":(ts+timedelta(minutes=5)).isoformat(),"outcome_status":"WIN","realized_r":1.0})
    return out

def test_holdout_is_chronological_and_embargoed():
    cfg=ReplayValidationConfig(discovery_fraction=.6,validation_fraction=.2,embargo_minutes=10)
    split=chronological_holdout(rows(),horizon_bars=180,config=cfg)
    assert split["status"]=="OK"
    assert max(x["candle_ts"] for x in split["discovery"]) < min(x["candle_ts"] for x in split["validation"])
    assert max(x["candle_ts"] for x in split["validation"]) < min(x["candle_ts"] for x in split["test"])
    assert split["embargoed"]==2

def test_purge_removes_training_event_overlapping_next_partition():
    data=rows()
    # Last discovery event extends beyond validation boundary.
    data[17]["exit_ts"]=(datetime.fromisoformat(data[18]["candle_ts"])+timedelta(minutes=1)).isoformat()
    cfg=ReplayValidationConfig(discovery_fraction=.6,validation_fraction=.2,embargo_minutes=0)
    split=chronological_holdout(data,horizon_bars=180,config=cfg)
    assert split["purged"]>=1
    assert data[17] not in split["discovery"]

def test_walk_forward_uses_fixed_policy_splits_with_purge_and_embargo():
    cfg=ReplayValidationConfig(embargo_minutes=10,walk_forward_train_episodes=10,walk_forward_test_episodes=5,walk_forward_step_episodes=5)
    folds=walk_forward_splits(rows(30),horizon_bars=180,config=cfg)
    assert len(folds)==4
    assert all(f["train"] and f["test"] for f in folds)
    assert all(max(x["candle_ts"] for x in f["train"]) < min(x["candle_ts"] for x in f["test"]) for f in folds)

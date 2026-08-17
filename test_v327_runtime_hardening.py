import asyncio
from datetime import datetime, timezone, timedelta

from ensemble_engine import EnsembleEngine
import server


def test_ensemble_deduplicates_repeated_model_ids(tmp_path):
    db=str(tmp_path/'ensemble.db')
    e=EnsembleEngine(db_path=db, version='3.27', mode='SHADOW')
    e.ensure_schema()
    ts=datetime.now(timezone.utc).isoformat()
    repeated={
        'strategy_id':'CROSS_ASSET_GBP_USD','strategy_version':'1','symbol':'EUR_USD','timestamp':ts,
        'direction':'ABSTAIN','confidence':0.4,'expected_edge':None,'market_regime':'RANGE',
        'time_horizon':'M5','signal_strength':0.0,'risk_characteristics':{},'data_quality':1.0,
        'family':'CROSS_ASSET','role':'DIRECTIONAL','status':'ONLINE'
    }
    out=e.evaluate([repeated.copy() for _ in range(6)], regime='RANGE')
    assert out['abstaining_models']==['CROSS_ASSET_GBP_USD']


def test_market_closed_is_not_reported_as_fresh(monkeypatch):
    monkeypatch.setattr(server, 'market_is_weekend_closed', lambda at=None: True)
    monkeypatch.setattr(server, '_obs_module', lambda *a, **k: None)
    class Obs:
        def alert(self,*a,**k): pass
        def recover(self,*a,**k): pass
    monkeypatch.setattr(server, 'observability_manager', Obs())
    old=datetime.now(timezone.utc)-timedelta(hours=44)
    out=server.observability_market_data_update('EUR_USD',[{'t':old}],12.0)
    assert out['market_closed'] is True
    assert out['market_data_state']=='MARKET_CLOSED'
    assert out['fresh'] is False
    assert out['stale'] is False


def test_learning_status_separates_adaptive_learning_from_confidence_gate(monkeypatch, tmp_path):
    # Static source-level contract: the API must not claim Adaptive Learning can mutate production execution.
    src=open(server.__file__,encoding='utf-8').read()
    assert '"changes_execution":False' in src
    assert '"adaptive_learning_changes_production_execution":False' in src
    assert '"adaptive_confidence_gate_enabled":bool(ADAPTIVE_CONFIDENCE)' in src


def test_initial_supervisor_launch_is_not_counted_as_restart():
    src=open(server.__file__,encoding='utf-8').read()
    block=src[src.index('async def supervised_worker_loop():'):src.index('async def watchdog_loop():')]
    assert 'first_launch=True' in block
    assert 'if not first_launch:' in block
    assert 'state["worker_restarts"] += 1' in block


def test_market_closed_is_hard_execution_veto(monkeypatch):
    monkeypatch.setattr(server, 'market_is_weekend_closed', lambda at=None: True)
    class NeverClient:
        async def request(self, *a, **k):
            raise AssertionError('broker request must not be reached while market is closed')
    r={'instrument':'EUR_USD','signal':'BUY','entry':1.1,'stop':1.09,'target':1.12}
    out=asyncio.run(server.execute(NeverClient(), r))
    assert out['skipped']=='MARKET_CLOSED'
    assert out['market_closed'] is True


def test_recoverable_path_blocks_market_closed_before_preflight(monkeypatch):
    monkeypatch.setattr(server, 'market_is_weekend_closed', lambda at=None: True)
    called={'preflight':False}
    async def preflight(*a, **k):
        called['preflight']=True
        raise AssertionError('price preflight/broker path must not be reached')
    monkeypatch.setattr(server, 'recovery_price_preflight', preflight)
    class DummyRecovery:
        def journal(self,*a,**k): pass
    monkeypatch.setattr(server,'recovery_manager',DummyRecovery())
    r={'instrument':'EUR_USD','signal':'BUY','entry':1.1,'stop':1.09,'target':1.12}
    out=asyncio.run(server.execute_recoverable(None,r,'trace',1,1))
    assert out['skipped']=='MARKET_CLOSED'
    assert called['preflight'] is False


def test_numpy_is_available_for_shadow_training():
    assert hasattr(server, "np")
    arr = server.np.asarray([0, 1, 1])
    assert int(arr.sum()) == 2


def test_scanner_health_uses_heartbeat_for_liveness(monkeypatch):
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    monkeypatch.setitem(server.state, "last_scan", (now - timedelta(seconds=server.WATCHDOG_STALE_SECONDS + 30)).isoformat())
    monkeypatch.setitem(server.state, "worker_last_heartbeat", now.isoformat())
    snap = server.scanner_health_snapshot()
    assert snap["stale"] is False
    assert snap["last_scan_age_seconds"] > server.WATCHDOG_STALE_SECONDS
    assert snap["last_heartbeat_age_seconds"] < 5


def test_counterfactual_health_cannot_become_operational_watch(monkeypatch):
    rows = []
    # Strong historical counterfactual baseline followed by a bad recent block.
    labels = [1] * 100 + [0] * 30
    for i, label in enumerate(labels):
        rows.append({"label": label, "executed": 0, "blocked": 1, "ts": f"2026-01-01T00:{i%60:02d}:00+00:00",
                     "candle_ts": "", "setup_variant": "CF_TEST"})
    monkeypatch.setattr(server, "_strategy_rows",
                        lambda variant, executed_only=False, since_ts=None: [] if executed_only else rows)

    class DummyConn:
        def execute(self, sql, params=()):
            class Result:
                def fetchone(self): return None
            return Result()
        def commit(self): pass
        def close(self): pass

    # Capture DB writes without requiring a real persistent row for this unit check.
    monkeypatch.setattr(server, "conn", lambda: DummyConn())
    monkeypatch.setattr(server, "strategy_health_snapshot", lambda variant: None)
    monkeypatch.setattr(server, "_health_transition", lambda *a, **k: None)

    out = server._evaluate_one_strategy_health("CF_TEST")
    assert out["status"] == "LEARNING"
    assert "counterfactual" in out["reason"]

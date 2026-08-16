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

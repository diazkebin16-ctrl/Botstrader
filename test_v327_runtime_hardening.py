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


def test_shadow_training_dependencies_are_imported():
    required = [
        "np",
        "joblib",
        "TimeSeriesSplit",
        "Pipeline",
        "StandardScaler",
        "LogisticRegression",
        "accuracy_score",
        "roc_auc_score",
        "log_loss",
        "brier_score_loss",
    ]
    missing = [name for name in required if not hasattr(server, name)]
    assert missing == [], f"missing ML training imports: {missing}"


def test_timeseries_pipeline_can_fit_small_binary_dataset():
    X = server.np.asarray([
        [0.0, 0.1], [0.1, 0.2], [0.2, 0.1], [0.3, 0.4],
        [0.4, 0.3], [0.5, 0.6], [0.6, 0.5], [0.7, 0.8],
        [0.8, 0.7], [0.9, 1.0], [1.0, 0.9], [1.1, 1.2],
    ])
    y = server.np.asarray([0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1])
    splits = list(server.TimeSeriesSplit(n_splits=3).split(X))
    assert len(splits) == 3
    tr, te = splits[-1]
    model = server.Pipeline([
        ("scale", server.StandardScaler()),
        ("clf", server.LogisticRegression(max_iter=1000, class_weight="balanced")),
    ])
    model.fit(X[tr], y[tr])
    prob = model.predict_proba(X[te])[:, 1]
    assert len(prob) == len(te)
    assert all(0.0 <= float(p) <= 1.0 for p in prob)



def test_runtime_version_and_dashboard_are_v327():
    assert server.VERSION_TAG == "3.27"
    src=open(server.__file__,encoding="utf-8").read()
    assert "Market Alert V3.27 · Integrated Shadow Runtime" in src


def test_storage_status_never_calls_ephemeral_storage_persistent():
    st=server.storage_status()
    assert st["persistent"] == bool(server.DB_PERSISTENT and server.MODEL_PERSISTENT)
    assert st["action_required"] is (not st["persistent"])
    if not st["persistent"]:
        assert st["status"] in ("EPHEMERAL","ACTION_REQUIRED_RAILWAY_VOLUME")
        assert st["action"]


def test_learning_persistence_recommendation_semantics():
    stats=server.learning_stats()
    assert stats["persistent_db_recommended"] is (not stats["persistent_db_configured"])
    assert stats["db_persistence"]["persistent"] == stats["persistent_db_configured"]


def test_ml_without_model_is_waiting_not_offline(monkeypatch, tmp_path):
    monkeypatch.setattr(server,"MODEL_PATH",str(tmp_path/"not-trained.joblib"))
    r={
        "instrument":"EUR_USD","candle_ts":datetime.now(timezone.utc).isoformat(),"market_data_stale":False,
        "signal":"BUY","technical":60,"direction_edge":30,"direction_state":"TREND","rr":2.0,
        "entry":1.1,"stop":1.09,"target":1.12,"managed_target":1.12,"buy_score":60,"sell_score":10,
        "market_regime":{"market_regime":"RANGE"},"news_bias":"NEUTRAL","alignment":"N/A",
        "news_positive_hits":0,"news_negative_hits":0,"news_articles":[],"weekend_research":{},
        "external_research_collection":{"symbols":[]},"features":{},"filters":{}
    }
    conf={"probability":0.6}
    signals=server.build_ensemble_shadow_signals(r,conf,None)
    ml=next(x for x in signals if x["strategy_id"]=="ML_SUCCESS_CALIBRATOR")
    assert ml["status"]=="ONLINE"
    assert ml["metadata"]["lifecycle_state"]=="WAITING_FOR_EVIDENCE"
    assert ml["signal_strength"]==0.0


def test_single_directional_model_does_not_report_perfect_consensus(tmp_path):
    db=str(tmp_path/"ensemble-consensus.db")
    e=EnsembleEngine(db_path=db,version="3.27",mode="SHADOW",min_active_directional=2)
    e.ensure_schema()
    ts=datetime.now(timezone.utc).isoformat()
    sig={
        "strategy_id":"TECHNICAL_CORE","strategy_version":"technical@3.27","symbol":"EUR_USD","timestamp":ts,
        "direction":"LONG","confidence":.6,"expected_edge":2.0,"market_regime":"RANGE","time_horizon":"INTRADAY",
        "signal_strength":.8,"risk_characteristics":{},"data_quality":1.0,"family":"TREND_STRUCTURE",
        "input_dependencies":["PRICE"],"role":"DIRECTIONAL","ttl_seconds":300,"status":"ONLINE"
    }
    out=e.evaluate([sig],regime="RANGE")
    assert out["ensemble_direction"]=="ABSTAIN"
    assert out["consensus_evaluable"] is False
    assert out["directional_model_count"]==1
    assert out["agreement_score"]==0.0
    assert out["disagreement_score"]==0.0


def test_decision_hard_filters_include_quality_gate(monkeypatch,tmp_path):
    old_db=server.DB
    try:
        monkeypatch.setattr(server,"DB",str(tmp_path/"decision.db"))
        # Force a quality veto while safety is valid.
        monkeypatch.setattr(server,"quality_entry_gate",lambda r,conf:{"ok":False,"reason":"missing M1"})
        r={"instrument":"EUR_USD","candle_ts":"2026-08-17T12:00:00+00:00","signal":"BUY","score":50,"blocked":False}
        conf={"variant":"TEST","probability":.7,"source":"TEST","samples":10,"required_confidence":.65,
              "recent_win_rate":None,"performance_penalty":0}
        server.save_decision(r,conf,0,"Quality veto: missing M1")
        c=server.conn();row=c.execute("SELECT * FROM decision_log ORDER BY id DESC LIMIT 1").fetchone();c.close()
        assert row["safety_filters_ok"]==1
        assert row["quality_filters_ok"]==0
        assert row["hard_filters_ok"]==0
    finally:
        monkeypatch.setattr(server,"DB",old_db)


def test_status_learning_uses_unambiguous_sample_names(monkeypatch):
    async def run():
        out=await server.status()
        learn=out["learning"]
        assert "samples" not in learn
        assert "training_labeled_samples" in learn
        assert "research_samples_total" in learn
        assert "pending_samples" in learn
        assert out["version"]=="3.27"
    asyncio.run(run())

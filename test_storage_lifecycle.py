import os, shutil, sqlite3, tempfile
import storage_lifecycle
from storage_lifecycle import StorageLifecycleManager, RetentionPolicy


def _db():
    p=tempfile.mktemp(suffix='.db');c=sqlite3.connect(p)
    c.executescript('''
    CREATE TABLE ensemble_signals(signal_id TEXT PRIMARY KEY,ts TEXT);
    CREATE TABLE ensemble_alerts(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT);
    CREATE TABLE ensemble_outputs(ensemble_decision_id TEXT PRIMARY KEY,ts TEXT);
    CREATE TABLE external_research_observations(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT);
    CREATE TABLE observability_alerts(alert_key TEXT PRIMARY KEY,last_seen TEXT,status TEXT);
    CREATE TABLE multi_asset_decision_cycles(cycle_id TEXT PRIMARY KEY,ts TEXT,metadata_json TEXT);
    CREATE TABLE observability_traces(correlation_id TEXT PRIMARY KEY,created_ts TEXT);
    CREATE TABLE counterfactual_tracker_events(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT);
    CREATE TABLE counterfactual_opportunities(counterfactual_id TEXT PRIMARY KEY,market_time TEXT,status TEXT);
    CREATE TABLE recovery_state(id INTEGER PRIMARY KEY,payload TEXT);
    ''')
    for i in range(10):
        ts=f'2026-08-22T00:{i:02d}:00+00:00'
        c.execute('INSERT INTO ensemble_signals VALUES(?,?)',(f's{i}',ts))
        c.execute('INSERT INTO ensemble_alerts(ts) VALUES(?)',(ts,))
        c.execute('INSERT INTO ensemble_outputs VALUES(?,?)',(f'o{i}',ts))
        c.execute('INSERT INTO external_research_observations(ts) VALUES(?)',(ts,))
        c.execute('INSERT INTO observability_alerts VALUES(?,?,?)',(f'a{i}',ts,'ACTIVE' if i==0 else 'RECOVERED'))
        c.execute('INSERT INTO multi_asset_decision_cycles VALUES(?,?,?)',(f'c{i}',ts,'{}'))
        c.execute('INSERT INTO observability_traces VALUES(?,?)',(f'trace_{i}',ts))
        c.execute('INSERT INTO counterfactual_tracker_events(ts) VALUES(?)',(ts,))
        # Two of every five shadow opportunities are still awaiting an outcome.
        c.execute('INSERT INTO counterfactual_opportunities VALUES(?,?,?)',
                  (f'cf{i}',ts,'OPEN' if i%5<2 else 'RESOLVED'))
    c.execute("INSERT INTO recovery_state VALUES(1,'must_survive')")
    c.commit();c.close();return p


def _policy(**kw):
    base=dict(ensemble_signals=3,ensemble_alerts=4,ensemble_outputs=5,observability_recovered_alerts=2,
              external_research_observations=6,multi_asset_decision_cycles=2,observability_traces=3,
              counterfactual_tracker_events=4,counterfactual_resolved_opportunities=1)
    base.update(kw);return RetentionPolicy(**base)


def test_storage_lifecycle_bounds_only_non_authoritative_tables():
    p=_db();m=StorageLifecycleManager(p,_policy())
    out=m.prune();c=sqlite3.connect(p)
    assert c.execute('SELECT COUNT(*) FROM ensemble_signals').fetchone()[0]==3
    assert c.execute('SELECT COUNT(*) FROM ensemble_alerts').fetchone()[0]==4
    assert c.execute('SELECT COUNT(*) FROM ensemble_outputs').fetchone()[0]==5
    assert c.execute('SELECT COUNT(*) FROM external_research_observations').fetchone()[0]==6
    assert c.execute("SELECT COUNT(*) FROM observability_alerts WHERE status='ACTIVE'").fetchone()[0]==1
    assert c.execute("SELECT COUNT(*) FROM observability_alerts WHERE status!='ACTIVE'").fetchone()[0]==2
    assert c.execute('SELECT payload FROM recovery_state WHERE id=1').fetchone()[0]=='must_survive'
    c.close();os.remove(p)


def test_per_cycle_telemetry_tables_are_bounded():
    """The four tables that used to grow for ever now have a ceiling."""
    p=_db();m=StorageLifecycleManager(p,_policy())
    out=m.prune();c=sqlite3.connect(p)
    assert c.execute('SELECT COUNT(*) FROM multi_asset_decision_cycles').fetchone()[0]==2
    assert c.execute('SELECT COUNT(*) FROM observability_traces').fetchone()[0]==3
    assert c.execute('SELECT COUNT(*) FROM counterfactual_tracker_events').fetchone()[0]==4
    # The newest rows are the ones kept.
    assert c.execute('SELECT cycle_id FROM multi_asset_decision_cycles ORDER BY ts').fetchall()==[('c8',),('c9',)]
    assert out['multi_asset_decision_cycles']==8
    c.close();os.remove(p)


def test_unresolved_shadow_opportunities_are_never_deleted():
    """A counterfactual still waiting for its outcome outranks any cap."""
    p=_db();m=StorageLifecycleManager(p,_policy())
    m.prune();c=sqlite3.connect(p)
    assert c.execute("SELECT COUNT(*) FROM counterfactual_opportunities WHERE status='OPEN'").fetchone()[0]==4
    assert c.execute("SELECT COUNT(*) FROM counterfactual_opportunities WHERE status!='OPEN'").fetchone()[0]==1
    c.close();os.remove(p)


def test_compact_declines_when_the_volume_has_no_room(monkeypatch):
    """A nearly full disk turns compaction into a no-op, not an incident."""
    p=_db();m=StorageLifecycleManager(p)
    monkeypatch.setattr(storage_lifecycle.shutil,'disk_usage',
                        lambda _p: shutil._ntuple_diskusage(total=10**9,used=10**9,free=1024))
    out=m.compact(min_reclaim_bytes=0,headroom_bytes=0)
    assert out['compacted'] is False
    assert out['reason']=='INSUFFICIENT_FREE_SPACE'
    assert os.path.exists(p)
    os.remove(p)


def test_compact_declines_when_there_is_nothing_worth_reclaiming():
    p=_db();m=StorageLifecycleManager(p)
    out=m.compact(min_reclaim_bytes=256*1024*1024)
    assert out['compacted'] is False
    assert out['reason']=='NOT_ENOUGH_TO_RECLAIM'
    os.remove(p)


def test_compact_returns_deleted_pages_to_the_filesystem():
    """Pruning frees pages; only compaction gives the space back."""
    p=_db();c=sqlite3.connect(p)
    c.execute('CREATE TABLE ensemble_signals_bulk(id INTEGER PRIMARY KEY,payload TEXT)')
    c.executemany('INSERT INTO ensemble_signals_bulk(payload) VALUES(?)',[('x'*4000,) for _ in range(4000)])
    c.commit();c.close()
    grown=os.path.getsize(p)
    c=sqlite3.connect(p);c.execute('DELETE FROM ensemble_signals_bulk');c.commit();c.close()
    after_delete=os.path.getsize(p)
    assert after_delete>=grown*0.9, 'deleting rows must not shrink the file by itself'
    out=StorageLifecycleManager(p).compact(min_reclaim_bytes=1024,headroom_bytes=0)
    assert out['compacted'] is True
    assert os.path.getsize(p)<after_delete/2
    assert out['bytes_reclaimed']>0
    os.remove(p)

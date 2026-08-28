import os, sqlite3, tempfile
from storage_lifecycle import StorageLifecycleManager, RetentionPolicy


def _db():
    p=tempfile.mktemp(suffix='.db');c=sqlite3.connect(p)
    c.executescript('''
    CREATE TABLE ensemble_signals(signal_id TEXT PRIMARY KEY,ts TEXT);
    CREATE TABLE ensemble_alerts(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT);
    CREATE TABLE ensemble_outputs(ensemble_decision_id TEXT PRIMARY KEY,ts TEXT);
    CREATE TABLE external_research_observations(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT);
    CREATE TABLE observability_alerts(alert_key TEXT PRIMARY KEY,last_seen TEXT,status TEXT);
    CREATE TABLE recovery_state(id INTEGER PRIMARY KEY,payload TEXT);
    ''')
    for i in range(10):
        ts=f'2026-08-22T00:{i:02d}:00+00:00'
        c.execute('INSERT INTO ensemble_signals VALUES(?,?)',(f's{i}',ts))
        c.execute('INSERT INTO ensemble_alerts(ts) VALUES(?)',(ts,))
        c.execute('INSERT INTO ensemble_outputs VALUES(?,?)',(f'o{i}',ts))
        c.execute('INSERT INTO external_research_observations(ts) VALUES(?)',(ts,))
        c.execute('INSERT INTO observability_alerts VALUES(?,?,?)',(f'a{i}',ts,'ACTIVE' if i==0 else 'RECOVERED'))
    c.execute("INSERT INTO recovery_state VALUES(1,'must_survive')")
    c.commit();c.close();return p


def test_storage_lifecycle_bounds_only_non_authoritative_tables():
    p=_db();m=StorageLifecycleManager(p,RetentionPolicy(ensemble_signals=3,ensemble_alerts=4,ensemble_outputs=5,observability_recovered_alerts=2,external_research_observations=6))
    out=m.prune();c=sqlite3.connect(p)
    assert c.execute('SELECT COUNT(*) FROM ensemble_signals').fetchone()[0]==3
    assert c.execute('SELECT COUNT(*) FROM ensemble_alerts').fetchone()[0]==4
    assert c.execute('SELECT COUNT(*) FROM ensemble_outputs').fetchone()[0]==5
    assert c.execute('SELECT COUNT(*) FROM external_research_observations').fetchone()[0]==6
    assert c.execute("SELECT COUNT(*) FROM observability_alerts WHERE status='ACTIVE'").fetchone()[0]==1
    assert c.execute("SELECT COUNT(*) FROM observability_alerts WHERE status!='ACTIVE'").fetchone()[0]==2
    assert c.execute('SELECT payload FROM recovery_state WHERE id=1').fetchone()[0]=='must_survive'
    c.close();os.remove(p)

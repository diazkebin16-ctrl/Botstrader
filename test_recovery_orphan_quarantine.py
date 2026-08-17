import os, sqlite3, tempfile
from recovery_manager import RecoveryManager, now_iso


def _db():
    f=tempfile.NamedTemporaryFile(suffix='.db',delete=False); f.close()
    rm=RecoveryManager(f.name,'https://example.invalid','acct','token',allow_orphan_quarantine=True)
    rm.ensure_schema()
    c=rm.conn()
    c.execute("""CREATE TABLE IF NOT EXISTS active_trade_management(
      trade_id TEXT PRIMARY KEY,instrument TEXT,side TEXT,entry REAL,initial_stop REAL,initial_target REAL,
      current_stop REAL,setup_variant TEXT,policy TEXT,trend_score REAL,opened_ts TEXT,last_r REAL,
      last_action TEXT,closed INTEGER,updated_ts TEXT,current_units REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trade_memory(
      trade_id TEXT PRIMARY KEY,status TEXT,data_quality_json TEXT,execution_quality_compromised INTEGER DEFAULT 0,
      updated_ts TEXT,position_size REAL)""")
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_risk_state(id INTEGER PRIMARY KEY,nav REAL)""")
    c.commit(); c.close()
    return rm,f.name


def test_practice_orphan_is_quarantined_not_fabricated_closed():
    rm,path=_db()
    try:
        c=rm.conn(); ts=now_iso()
        c.execute("INSERT INTO active_trade_management VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  ('95','EUR_USD','BUY',1.1,1.09,1.12,1.09,'S','P',0,ts,0,'OPEN',0,ts,100))
        c.execute("INSERT INTO trade_memory VALUES(?,?,?,?,?,?)",('95','OPEN','{}',0,ts,100))
        c.commit(); c.close()
        out=rm.reconcile_snapshot({'account':{'NAV':'10000'},'open_trades':[],'pending_orders':[],
                                   'positions':[],'transactions':[],'last_transaction_id':'99'})
        assert out['status']=='MINOR_MISMATCH'
        c=rm.conn()
        a=c.execute("SELECT closed,last_action FROM active_trade_management WHERE trade_id='95'").fetchone()
        tm=c.execute("SELECT status,data_quality_json FROM trade_memory WHERE trade_id='95'").fetchone(); c.close()
        assert a['closed']==1 and a['last_action']=='BROKER_MISSING_QUARANTINED'
        assert tm['status']=='BROKER_MISSING'
        assert 'excluded_from_learning' in tm['data_quality_json']
    finally: os.unlink(path)


def test_live_semantics_keep_missing_trade_blocked():
    rm,path=_db(); rm.allow_orphan_quarantine=False
    try:
        c=rm.conn(); ts=now_iso()
        c.execute("INSERT INTO active_trade_management VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  ('95','EUR_USD','BUY',1.1,1.09,1.12,1.09,'S','P',0,ts,0,'OPEN',0,ts,100))
        c.execute("INSERT INTO trade_memory VALUES(?,?,?,?,?,?)",('95','OPEN','{}',0,ts,100))
        c.commit(); c.close()
        out=rm.reconcile_snapshot({'account':{'NAV':'10000'},'open_trades':[],'pending_orders':[],
                                   'positions':[],'transactions':[],'last_transaction_id':'99'})
        assert out['status']=='RECONCILIATION_REQUIRED'
        assert rm.state()['safe_mode']==1
    finally: os.unlink(path)

import sqlite3, tempfile, os
from observability import ObservabilityManager


def test_fresh_heartbeat_recovers_missed_alert_without_overwriting_business_status():
    path=tempfile.mktemp(suffix='.db')
    m=ObservabilityManager(path,'3.35.2');m.ensure_schema()
    m.heartbeat('X','CRITICAL','DEGRADED',details={'why':'business'})
    m.alert('HEARTBEAT:X','CRITICAL','X','MODULE_HEARTBEAT_MISSED','missed')
    out=m.mark_stale_modules({'X':180})
    assert out[0]['status']=='OK'
    c=sqlite3.connect(path);c.row_factory=sqlite3.Row
    assert c.execute("SELECT status FROM observability_alerts WHERE alert_key='HEARTBEAT:X'").fetchone()['status']=='RECOVERED'
    assert c.execute("SELECT status FROM observability_module_health WHERE module_name='X'").fetchone()['status']=='DEGRADED'
    c.close();os.remove(path)

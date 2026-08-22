from datetime import datetime, timedelta, timezone
from session_regime import active_session, session_regime


def m5_series(start, prices):
    out=[]
    for i,p in enumerate(prices):
        t=start+timedelta(minutes=5*i)
        o=p-0.00005
        out.append({"t":t,"o":o,"h":p+0.00010,"l":p-0.00010,"c":p})
    return out


def test_new_york_session_detected_in_overlap():
    # 13:00 UTC during summer is 09:00 New York and 14:00 London.
    dt=datetime(2026,8,21,13,0,tzinfo=timezone.utc)
    assert active_session(dt)=="NEW_YORK"


def test_session_regime_detects_bearish_intraday_move():
    start=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    prices=[1.1700,1.1698,1.1696,1.1694,1.1692,1.1690,1.1688,1.1686]
    out=session_regime(m5_series(start,prices),atr_value=0.0005)
    assert out["session"]=="NEW_YORK"
    assert out["direction"]=="SELL"
    assert out["strength"]>0.5


def test_session_regime_stays_neutral_without_displacement():
    start=datetime(2026,8,21,12,0,tzinfo=timezone.utc)
    prices=[1.1700,1.17002,1.16999,1.17001,1.17000,1.17001]
    out=session_regime(m5_series(start,prices),atr_value=0.0005)
    assert out["direction"]=="NEUTRAL"

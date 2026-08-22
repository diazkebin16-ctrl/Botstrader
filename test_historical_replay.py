from datetime import datetime,timedelta,timezone
from historical_replay import CandleStore,ReplayVariant,_choose

UTC=timezone.utc

def c(t):return {"t":t,"o":1,"h":1,"l":1,"c":1,"v":1}

def test_candle_store_never_exposes_incomplete_bar():
    t=datetime(2026,1,1,10,0,tzinfo=UTC)
    s=CandleStore({"M1":[c(t),c(t+timedelta(minutes=1))],"M5":[],"M15":[],"H1":[]})
    assert [x["t"] for x in s.history("M1",t+timedelta(seconds=59),10)]==[]
    assert [x["t"] for x in s.history("M1",t+timedelta(minutes=1),10)]==[t]
    assert [x["t"] for x in s.history("M1",t+timedelta(minutes=2),10)]==[t,t+timedelta(minutes=1)]

def test_future_bars_begin_strictly_after_signal_candle():
    t=datetime(2026,1,1,10,0,tzinfo=UTC);rows=[c(t+timedelta(minutes=i)) for i in range(4)]
    s=CandleStore({"M1":rows,"M5":[],"M15":[],"H1":[]})
    assert [x["t"] for x in s.future_m1_after(t+timedelta(minutes=1),2)]==[t+timedelta(minutes=2),t+timedelta(minutes=3)]

def test_variant_is_research_only_value_object():
    v=ReplayVariant("SESSION_1X","SESSION",1.0)
    assert v.mode=="SESSION" and v.session_weight_scale==1.0

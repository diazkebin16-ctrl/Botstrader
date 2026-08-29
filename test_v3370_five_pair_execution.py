import asyncio
from pathlib import Path

import pytest

import server
from broker_risk import IbkrAccountSnapshot, IbkrWhatIfMarginResult, BrokerInstrumentMinimum, IbkrBrokerRiskAdapter
from instrument_profiles import instrument_profile
from instrument_registry import InstrumentRegistry
from opportunity_ranker import rank_opportunities
from recovery_manager import RecoveryManager, deterministic_intent_key
from slot_allocator import allocate_slots


def c(inst, score, conf=None):
    return {"instrument":inst,"signal":"BUY","score":score,
            "dynamic_confidence":score/100 if conf is None else conf,
            "rr_raw":1.5,"room_to_barrier_r":1.5,"spread_pips":1.0,
            "batch_selection_eligible":True,"entry":1.1,"stop":1.09,"target":1.12,
            "candle_ts":"2026-08-29T12:00:00Z","_batch_context":{"signal_id":1}}


def test_five_profiles_paper_and_secondary_live_denied():
    for inst in ("EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD"):
        assert instrument_profile(inst).paper_execution_allowed is True
        assert instrument_profile(inst).allows_execution("PAPER","practice") is True
    for inst in ("GBP_USD","USD_JPY","AUD_USD","USD_CAD"):
        assert instrument_profile(inst).live_execution_allowed is False
        assert instrument_profile(inst).allows_execution("PRODUCTION","live") is False
    for inst in ("AUD_USD","USD_CAD"):
        p=instrument_profile(inst)
        assert not p.specific_vetoes and not p.specific_exceptions
        assert p.learned_research_veto_authority is False


def test_worker_count_all_variables_are_validated_fail_closed():
    cases=[
        ({},True,1),
        ({"WEB_CONCURRENCY":"1"},True,1),
        ({"WEB_CONCURRENCY":"1","UVICORN_WORKERS":"1","GUNICORN_WORKERS":"1"},True,1),
        ({"WEB_CONCURRENCY":"1","GUNICORN_WORKERS":"4"},False,4),
        ({"WEB_CONCURRENCY":"1","UVICORN_WORKERS":"2","GUNICORN_WORKERS":"1"},False,2),
        ({"WEB_CONCURRENCY":"4","UVICORN_WORKERS":"1","GUNICORN_WORKERS":"1"},False,4),
        ({"WEB_CONCURRENCY":"abc","UVICORN_WORKERS":"1"},False,None),
        ({"WEB_CONCURRENCY":"1","UVICORN_WORKERS":"abc"},False,None),
        ({"WEB_CONCURRENCY":"1","UVICORN_WORKERS":"1","GUNICORN_WORKERS":"abc"},False,None),
        ({"WEB_CONCURRENCY":"0","UVICORN_WORKERS":"1"},False,1),
        ({"WEB_CONCURRENCY":"-1","UVICORN_WORKERS":"1"},False,1),
        ({"WEB_CONCURRENCY":"","UVICORN_WORKERS":"1"},True,1),
    ]
    for env,safe,effective in cases:
        out=server.execution_worker_configuration(env)
        assert out["safe"] is safe
        assert out["effective_workers"] == effective


def test_aud_cad_metadata_fallback_cannot_authorize_secondary_order(monkeypatch):
    registry=InstrumentRegistry()
    monkeypatch.setattr(server,"INSTRUMENT_REGISTRY",registry)
    monkeypatch.setattr(server,"INSTRUMENTS",["EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD"])
    ctx={"margin_usage":0.0}
    for inst in ("AUD_USD","USD_CAD"):
        assert registry.get(inst).source == "FALLBACK"
        verdict=server._oanda_batch_broker_guard(c(inst,90),[],ctx)
        assert verdict["allow"] is False
        assert "BROKER_METADATA_UNVERIFIED" in verdict["reasons"]


def test_aud_cad_verified_oanda_metadata_can_pass_broker_gate(monkeypatch):
    registry=InstrumentRegistry()
    registry.update_from_oanda({"instruments":[
        {"name":"AUD_USD","displayPrecision":5,"pipLocation":-4,"tradeUnitsPrecision":0,"minimumTradeSize":"1","marginRate":"0.03","type":"CURRENCY"},
        {"name":"USD_CAD","displayPrecision":5,"pipLocation":-4,"tradeUnitsPrecision":0,"minimumTradeSize":"1","marginRate":"0.03","type":"CURRENCY"},
    ]})
    monkeypatch.setattr(server,"INSTRUMENT_REGISTRY",registry)
    monkeypatch.setattr(server,"INSTRUMENTS",["EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD"])
    for inst in ("AUD_USD","USD_CAD"):
        m=registry.get(inst)
        assert m.source=="OANDA" and m.pip_size==pytest.approx(.0001)
        verdict=server._oanda_batch_broker_guard(c(inst,90),[],{"margin_usage":0.0})
        assert verdict["allow"] is True


def test_five_pair_best_of_five_and_order_independence():
    rows=[c("EUR_USD",72),c("GBP_USD",81),c("USD_JPY",68),c("AUD_USD",89),c("USD_CAD",77)]
    assert allocate_slots(rows,nlv=3000,broker_guard=lambda *_:{"allow":True},portfolio_guard=lambda *_:{"allow":True})["selected"][0]["instrument"]=="AUD_USD"
    expected=[x.instrument for x in rank_opportunities(rows)]
    assert expected==[x.instrument for x in rank_opportunities(list(reversed(rows)))]


def test_cad_can_win_and_two_slot_safe_set_can_skip_incompatible():
    rows=[c("USD_CAD",99),c("AUD_USD",95),c("GBP_USD",93),c("EUR_USD",80),c("USD_JPY",70)]
    assert allocate_slots(rows,nlv=3000,broker_guard=lambda *_:{"allow":True},portfolio_guard=lambda *_:{"allow":True})["selected"][0]["instrument"]=="USD_CAD"
    def portfolio(candidate,selected):
        if selected and candidate["instrument"]=="AUD_USD": return {"allow":False,"reasons":["CORRELATED_POSITION_LIMIT"]}
        return {"allow":True}
    out=allocate_slots(rows,nlv=5000,broker_guard=lambda *_:{"allow":True},portfolio_guard=portfolio)
    assert [x["instrument"] for x in out["selected"]]==["USD_CAD","GBP_USD"]


def test_recovery_intent_idempotency_same_market_intent(tmp_path):
    rm=RecoveryManager(str(tmp_path/'r.db'),'https://api-fxpractice.oanda.com','acct','token')
    key=deterministic_intent_key('PRIMARY','AUD_USD','BUY','S','2026-08-29T12:00:00Z',1.1,1.09,1.12)
    kwargs=dict(idempotency_key=key,correlation_id='cycle-1',decision_id='d1',risk_decision_id='r1',strategy_id='S',symbol='AUD_USD',side='BUY',requested_units=100,entry_price=1.1,stop_loss=1.09,take_profit=1.12,request_body={'order':{}},metadata={'cycle_id':'cycle-1','rank':1,'slot_index':1,'broker':'OANDA','environment':'PAPER'})
    a=rm.create_intent(**kwargs); b=rm.create_intent(**kwargs)
    assert a['created'] is True
    assert b['created'] is False and b['duplicate_prevented'] is True
    assert a['intent']['execution_intent_id']==b['intent']['execution_intent_id']


def test_ibkr_contracts_exist_but_adapter_has_no_authority():
    snap=IbkrAccountSnapshot(net_liquidation=10000,verified=True)
    whatif=IbkrWhatIfMarginResult('EUR_USD',quantity=1000,side='BUY',verified=True)
    minimum=BrokerInstrumentMinimum('EUR_USD',minimum_quantity=1000,verification_status='VERIFIED',source='IBKR')
    assert snap.verified is True and whatif.verified is True and minimum.verified is True
    adapter=IbkrBrokerRiskAdapter()
    assert adapter.execution_authority is False
    assert adapter.prospective_check(c('EUR_USD',90),[],{'NetLiquidation':10000}).allow is False


def _patch_batch(monkeypatch, execute_results, nlv=3000):
    universe=["EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD"]
    scores={"EUR_USD":99,"GBP_USD":95,"USD_JPY":90,"AUD_USD":85,"USD_CAD":80}
    monkeypatch.setattr(server,'SCAN_INSTRUMENTS',universe)
    monkeypatch.setattr(server,'WEEKEND_RESEARCH_ENABLED',False)
    monkeypatch.setattr(server,'OBSERVABILITY_ENABLED',False)
    monkeypatch.setattr(server,'MULTI_WORKER_EXECUTION_BLOCKED',False)
    async def scan(client,inst,**kwargs): return c(inst,scores[inst])
    async def risk(client): return {'nav':nlv,'open_positions':0,'open_instruments':[],'portfolio_open_risk':0.0,'margin_usage':0.0,'system_abnormal':False,'data_stale':False}
    calls=[]
    async def execute(client,candidate,cycle_id,**kwargs):
        calls.append((candidate['instrument'],kwargs))
        return execute_results[len(calls)-1]
    monkeypatch.setattr(server,'scan',scan)
    monkeypatch.setattr(server,'build_broker_risk_context',risk)
    monkeypatch.setattr(server,'execute_ranked_candidate',execute)
    monkeypatch.setattr(server,'_persist_multi_asset_cycle',lambda cycle:None)
    server.state['last_results']={};server.state['instrument_state']={}
    return calls


def test_preexecution_rejection_falls_back_to_next_candidate(monkeypatch):
    calls=_patch_batch(monkeypatch,[
        {'executed':False,'reason':'BROKER_RISK_GUARD','pre_execution_rejection':True,'fallback_allowed':True},
        {'executed':True,'instrument':'GBP_USD'},
    ])
    assert asyncio.run(server.scan_instruments_once(object())) is True
    assert [x[0] for x in calls]==['EUR_USD','GBP_USD']


def test_explicit_broker_rejection_falls_back(monkeypatch):
    calls=_patch_batch(monkeypatch,[
        {'executed':False,'reason':'BROKER_EXPLICIT_REJECTION','explicit_rejection':True,'fallback_allowed':True},
        {'executed':True,'instrument':'GBP_USD'},
    ])
    asyncio.run(server.scan_instruments_once(object()))
    assert [x[0] for x in calls]==['EUR_USD','GBP_USD']


def test_unknown_submit_stops_fallback(monkeypatch):
    calls=_patch_batch(monkeypatch,[
        {'executed':False,'reason':'ORDER_STATUS_UNKNOWN','uncertain':True,'fallback_allowed':False,'intent_state':'UNKNOWN'},
    ])
    assert asyncio.run(server.scan_instruments_once(object())) is False
    assert [x[0] for x in calls]==['EUR_USD']


def test_second_slot_gets_new_slot_index_after_first_confirmed(monkeypatch):
    calls=_patch_batch(monkeypatch,[
        {'executed':True,'instrument':'EUR_USD'},
        {'executed':True,'instrument':'GBP_USD'},
    ],nlv=5000)
    asyncio.run(server.scan_instruments_once(object()))
    assert [x[1]['slot_index'] for x in calls[:2]]==[1,2]
    assert len(calls)==2


def test_execute_ranked_candidate_rebuilds_fresh_context(monkeypatch):
    contexts=[
        {'nav':5000,'open_positions':0,'open_instruments':[],'portfolio_open_risk':0.0,'margin_usage':0.0,'system_abnormal':False,'data_stale':False},
        {'nav':5000,'open_positions':2,'open_instruments':['EUR_USD','GBP_USD'],'portfolio_open_risk':0.02,'margin_usage':0.1,'system_abnormal':False,'data_stale':False},
    ]
    async def risk(client): return contexts.pop(0)
    monkeypatch.setattr(server,'build_broker_risk_context',risk)
    monkeypatch.setattr(server,'_batch_portfolio_guard',lambda *args:{'allow':True})
    monkeypatch.setattr(server,'_oanda_batch_broker_guard',lambda *args:{'allow':True})
    monkeypatch.setattr(server,'AUTO',False)
    monkeypatch.setattr(server.recovery_manager,'new_trades_allowed',lambda: True)
    first=asyncio.run(server.execute_ranked_candidate(object(),c('EUR_USD',90),'cycle',max_slots=2))
    second=asyncio.run(server.execute_ranked_candidate(object(),c('GBP_USD',89),'cycle',max_slots=2))
    assert first['reason']=='AUTO_TRADE=false'
    assert second['reason']=='NO_FRESH_SLOT_AVAILABLE'


def test_nan_inf_hardening_still_excludes_corrupt_critical_candidates():
    rows=[c('AUD_USD',99,float('nan')),c('USD_CAD',98,float('inf')),c('GBP_USD',80,.8)]
    ranked=rank_opportunities(rows)
    assert [x.instrument for x in ranked]==['GBP_USD']
    assert all(x.rank_score==x.rank_score and x.rank_score not in (float('inf'),float('-inf')) for x in ranked)


@pytest.mark.parametrize("inst",["AUD_USD","USD_CAD"])
def test_secondary_execute_path_refreshes_metadata_and_fails_closed_if_still_unverified(monkeypatch,inst):
    registry=InstrumentRegistry()
    monkeypatch.setattr(server,'INSTRUMENT_REGISTRY',registry)
    monkeypatch.setattr(server,'INSTRUMENTS',["EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD"])
    async def risk(client): return {'nav':3000,'open_positions':0,'open_instruments':[],'portfolio_open_risk':0.0,'margin_usage':0.0,'system_abnormal':False,'data_stale':False}
    async def refresh(client,symbols=None,force=False): return {'updated':[]}
    monkeypatch.setattr(server,'build_broker_risk_context',risk)
    monkeypatch.setattr(server,'_batch_portfolio_guard',lambda *args:{'allow':True})
    monkeypatch.setattr(server,'refresh_instrument_metadata',refresh)
    out=asyncio.run(server.execute_ranked_candidate(object(),c(inst,90),'cycle',max_slots=1))
    assert out['reason']=='INSTRUMENT_METADATA_UNVERIFIED'
    assert out['fallback_allowed'] is True


@pytest.mark.parametrize("inst",["AUD_USD","USD_CAD"])
def test_secondary_execute_path_can_continue_after_oanda_metadata_verification(monkeypatch,inst):
    registry=InstrumentRegistry()
    monkeypatch.setattr(server,'INSTRUMENT_REGISTRY',registry)
    monkeypatch.setattr(server,'INSTRUMENTS',["EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD"])
    async def risk(client): return {'nav':3000,'open_positions':0,'open_instruments':[],'portfolio_open_risk':0.0,'margin_usage':0.0,'system_abnormal':False,'data_stale':False}
    async def refresh(client,symbols=None,force=False):
        registry.update_from_oanda({'instruments':[{'name':inst,'displayPrecision':5,'pipLocation':-4,'tradeUnitsPrecision':0,'minimumTradeSize':'1','marginRate':'0.03','type':'CURRENCY'}]})
        return {'updated':[inst]}
    monkeypatch.setattr(server,'build_broker_risk_context',risk)
    monkeypatch.setattr(server,'_batch_portfolio_guard',lambda *args:{'allow':True})
    monkeypatch.setattr(server,'refresh_instrument_metadata',refresh)
    monkeypatch.setattr(server.recovery_manager,'new_trades_allowed',lambda: True)
    monkeypatch.setattr(server,'AUTO',False)
    out=asyncio.run(server.execute_ranked_candidate(object(),c(inst,90),'cycle',max_slots=1))
    assert registry.get(inst).source=='OANDA'
    assert out['reason']=='AUTO_TRADE=false'



def test_recovery_intents_accept_all_five_instrument_namespaces(tmp_path):
    rm=RecoveryManager(str(tmp_path/'five.db'),'https://api-fxpractice.oanda.com','acct','token')
    ids=[]
    for i,inst in enumerate(("EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD"),1):
        key=deterministic_intent_key('PRIMARY',inst,'BUY','S','2026-08-29T12:00:00Z',1.1,1.09,1.12)
        out=rm.create_intent(idempotency_key=key,correlation_id=f'c{i}',decision_id=None,risk_decision_id=None,
            strategy_id='S',symbol=inst,side='BUY',requested_units=100,entry_price=1.1,stop_loss=1.09,take_profit=1.12,
            request_body={'order':{'instrument':inst}},metadata={'broker':'OANDA','environment':'PAPER'})
        assert out['created'] is True and out['intent']['symbol']==inst
        ids.append(out['intent']['execution_intent_id'])
    assert len(set(ids))==5


def test_aud_cad_profiles_do_not_inherit_eur_specific_authority():
    eur=instrument_profile('EUR_USD')
    assert 'LOW_ROOM_LOW_RR' in eur.specific_vetoes
    for inst in ('AUD_USD','USD_CAD'):
        p=instrument_profile(inst)
        assert 'LOW_ROOM_LOW_RR' not in p.specific_vetoes
        assert 'LOW_ROOM_EXTENDED' not in p.specific_vetoes
        assert 'M1_ALTERNATIVE_ADMISSION' not in p.specific_exceptions
        assert p.learned_research_veto_authority is False



@pytest.mark.parametrize("inst",["AUD_USD","USD_CAD"])
def test_aud_cad_trade_management_registration_is_instrument_generic(monkeypatch,inst):
    calls=[]
    class FakeConn:
        def execute(self,sql,params=()): calls.append((sql,params)); return self
        def commit(self): pass
        def close(self): pass
    monkeypatch.setattr(server,'conn',lambda:FakeConn())
    monkeypatch.setattr(server,'trend_runner_score',lambda r:0.0)
    row=c(inst,90)
    server.register_trade_management('trade-1',row,row['target'],filled_units=100,entry_price=row['entry'])
    assert any(inst in params for _,params in calls)
    assert server.instrument_metadata(inst).pip_size==pytest.approx(.0001)

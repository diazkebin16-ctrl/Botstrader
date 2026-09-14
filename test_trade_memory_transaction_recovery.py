import asyncio
import json

import pytest

import server


def _memory(**overrides):
    row={
        "trade_id":"101","status":"BROKER_MISSING","symbol":"EUR_USD",
        "direction":"LONG","entry_ts":"2026-09-01T10:00:00Z",
        "entry_price":1.1000,"position_size":100,"stop_loss":1.0990,
        "take_profit":1.1020,"data_quality_json":"{}",
        "execution_quality_compromised":1,"order_id":"51","strategy":"TEST",
    }
    row.update(overrides)
    return row


def _fill(*,trade_id="101",units="100",price="1.1020",realized="0.20",
          transaction_id="201",reason="TAKE_PROFIT_ORDER",full=True):
    reduction={"tradeID":trade_id,"units":units,"price":price,"realizedPL":realized,
               "financing":"-0.01","guaranteedExecutionFee":"0.00"}
    tx={"id":transaction_id,"time":"2026-09-01T11:00:00Z","type":"ORDER_FILL",
        "reason":reason,"orderID":"151","commission":"0.00"}
    tx["tradesClosed" if full else "tradeReduced"]=[reduction] if full else reduction
    return tx


def test_transaction_history_reconstructs_exact_full_close():
    close=server._trade_memory_transaction_close(_memory(),[_fill()])
    assert close is not None
    assert close["source"] == "OANDA_TRANSACTION_HISTORY"
    assert close["exit_price"] == pytest.approx(1.1020)
    assert close["realized_pl"] == pytest.approx(0.20)
    assert close["financing"] == pytest.approx(-0.01)
    assert close["closing_transaction_ids"] == ["201"]
    assert "TAKE_PROFIT_ORDER" in close["exit_reasons"]


def test_partial_reduction_requires_complete_original_units():
    partial=_fill(units="40",price="1.1010",realized="0.04",full=False)
    assert server._trade_memory_transaction_close(_memory(),[partial]) is None
    remainder=_fill(units="60",price="1.1020",realized="0.12",
                    transaction_id="202",full=False)
    close=server._trade_memory_transaction_close(_memory(),[partial,remainder])
    assert close is not None
    assert close["reduced_units"] == pytest.approx(100)
    assert close["exit_price"] == pytest.approx(1.1016)
    assert close["realized_pl"] == pytest.approx(0.16)


def test_broker_missing_is_recovered_and_restored_for_learning(monkeypatch,tmp_path):
    monkeypatch.setattr(server,"DB",str(tmp_path/"recovery.db"))
    monkeypatch.setattr(server,"RECOVERY_MANAGER_ENABLED",False)
    monkeypatch.setattr(server,"DEPLOYMENT_MANAGER_ENABLED",False)
    c=server.conn()
    mem=_memory()
    c.execute("""INSERT INTO trade_memory(
      trade_id,order_id,strategy,symbol,direction,status,entry_ts,entry_price,
      position_size,stop_loss,take_profit,data_quality_json,
      execution_quality_compromised,created_ts,updated_ts)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (mem["trade_id"],mem["order_id"],mem["strategy"],mem["symbol"],mem["direction"],
       mem["status"],mem["entry_ts"],mem["entry_price"],mem["position_size"],
       mem["stop_loss"],mem["take_profit"],json.dumps({"excluded_from_learning":True,
       "broker_trade_missing":True,"broker_exit_unverified":True}),1,
       mem["entry_ts"],mem["entry_ts"]))
    c.execute("""INSERT INTO active_trade_management(
      trade_id,instrument,side,entry,initial_stop,initial_target,policy,opened_ts,closed,updated_ts)
      VALUES(?,?,?,?,?,?,?,?,?,?)""",
      (mem["trade_id"],mem["symbol"],"BUY",mem["entry_price"],mem["stop_loss"],
       mem["take_profit"],"TEST",mem["entry_ts"],1,mem["entry_ts"]))
    c.commit();c.close()

    async def fake_req(client,method,path,params=None,**kwargs):
        if path == "/v3/accounts/{account}":
            return {"account":{},"lastTransactionID":"201"}
        if path.endswith("/transactions/idrange"):
            return {"transactions":[_fill()]}
        raise AssertionError(path)
    monkeypatch.setattr(server,"req",fake_req)
    out=asyncio.run(server.reconcile_trade_memory(object(),None))
    assert out["closed"] == 1
    assert out["recovered_broker_missing"] == 1
    c=server.conn()
    row=dict(c.execute("SELECT * FROM trade_memory WHERE trade_id='101'").fetchone())
    managed=c.execute("SELECT closed FROM active_trade_management WHERE trade_id='101'").fetchone()
    c.close()
    quality=json.loads(row["data_quality_json"])
    assert row["status"] == "CLOSED"
    assert row["net_result"] == pytest.approx(0.19)
    assert row["realized_r"] == pytest.approx(2.0)
    assert quality["actual_broker_exit_reconciled"] is True
    assert quality["excluded_from_learning"] is False
    assert quality["broker_trade_missing"] is False
    assert row["execution_quality_compromised"] == 0
    assert managed["closed"] == 1


def test_global_trade_memory_reconciliation_precedes_orphan_check(monkeypatch):
    calls=[]
    async def memory(client,instrument=None):
        calls.append(("memory",instrument))
        return {"enabled":True,"checked":1,"closed":1,"errors":[]}
    async def recovery(client,max_attempts=3):
        calls.append(("recovery",max_attempts))
        return {"connected":True,"reconciliation":{"status":"MATCHED"}}
    monkeypatch.setattr(server,"RECOVERY_MANAGER_ENABLED",True)
    monkeypatch.setattr(server,"OBSERVABILITY_ENABLED",False)
    monkeypatch.setattr(server,"reconcile_trade_memory",memory)
    monkeypatch.setattr(server.recovery_manager,"reconnect_and_reconcile",recovery)
    out=asyncio.run(server.recovery_reconcile_primary(object(),"test"))
    assert calls == [("memory",None),("recovery",3)]
    assert out["trade_memory_pre_reconciliation"]["closed"] == 1

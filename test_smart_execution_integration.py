
from datetime import datetime, timezone, timedelta
import os, tempfile
from system_integration_test_framework import SystemIntegrationTestFramework

def test_smart_execution_tca_flows_into_system_evaluation_and_governance():
    fw=SystemIntegrationTestFramework("INTEGRATION_TEST",seed=160024)
    se=fw.smart_execution()
    now=datetime.now(timezone.utc)
    c=fw.conn()
    # Recent execution is intentionally poor. System Evaluation should see the
    # Step 16 TCA independently from strategy PnL.
    for i in range(20):
        ts=(now-timedelta(minutes=i)).isoformat()
        c.execute("""INSERT INTO smart_execution_tca(
          tca_id,execution_intent_id,ts,strategy_id,symbol,side,order_type,session,market_regime,volatility,
          liquidity_state,expected_price,actual_fill_price,filled_quantity,fill_rate,slippage_absolute,
          slippage_bps,slippage_cost,spread_cost,fees,estimated_market_impact,delay_cost,total_execution_cost,
          expected_gross_edge,expected_net_edge,execution_quality_score,entry_execution_score,exit_execution_score,
          stop_slippage_bps,adverse_selection_bps,attribution,metadata_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (f"bad{i}",f"intent{i}",ts,"S1","EUR_USD","BUY","MARKET","NY","HIGH_VOLATILITY","HIGH",
           "LOW_LIQUIDITY",1.1,1.102,100,.60,.002,18.0,.20,.05,.01,.04,0,.30,.20,-.10,
           35.0,35.0,None,None,8.0,"EXECUTION_LOSS","{}"))
    c.commit();c.close()

    # Populate enough ordinary trading/operational context so the evaluator has
    # a complete system view rather than only execution records.
    for i in range(30):
        fw.seed_trade(f"SEI{i}","S1",.3,.3,days_ago=max(1,30-i),regime="HIGH_VOLATILITY",slip=.2)
    ev=fw.evaluator(min_samples=10).evaluate()
    smart=(ev.get("execution_quality") or {}).get("smart_execution") or {}
    assert smart.get("samples",0)>=20
    assert smart.get("execution_quality_score",100)<60
    assert "EXECUTION_DEGRADATION" in (ev.get("degradation") or {}).get("types",[])

    g=fw.governance("FULL_POLICY_ENFORCEMENT")
    chk=g.check_action("EXECUTION_POLICY_DEPLOYMENT","exec_candidate_bad",
                       {"magnitude":"MAJOR","affected_modules":["SMART_EXECUTION","SYSTEM_EVALUATION"]})
    assert chk["would_block"] is True
    assert chk["enforced"] is True
    assert "execution" in str(chk.get("reason","")).lower()

def test_execution_policy_candidate_is_research_only():
    fw=SystemIntegrationTestFramework("SIMULATION",seed=22)
    se=fw.smart_execution()
    c=se.candidate_execution_policy("current_v1",{"order_selection":"passive_limit"},{"samples":100})
    assert c["status"]=="RESEARCH_ONLY"
    assert c["auto_deploy"] is False
    assert c["required_path"]==["SIMULATION","PAPER","VALIDATION","CANARY"]

if __name__=="__main__":
    test_smart_execution_tca_flows_into_system_evaluation_and_governance()
    test_execution_policy_candidate_is_research_only()
    print("smart execution integration tests: OK (2)")

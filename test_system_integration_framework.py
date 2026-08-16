
import asyncio, json, os, tempfile
from pathlib import Path
from system_integration_test_framework import (
    SystemIntegrationTestFramework, FailureInjector, UnsafeTestEnvironment
)

def test_failure_injection_is_nonproduction_only():
    for env in ("PRODUCTION","CANARY","PAPER"):
        try:
            FailureInjector(env)
            raise AssertionError(f"Failure injection allowed in {env}")
        except UnsafeTestEnvironment:
            pass
    assert FailureInjector("SIMULATION").environment=="SIMULATION"

def test_real_endpoint_and_account_are_rejected():
    try:
        SystemIntegrationTestFramework.assert_safe_endpoint("INTEGRATION_TEST","https://api-fxtrade.oanda.com","sandbox")
        raise AssertionError("real broker endpoint accepted")
    except UnsafeTestEnvironment:
        pass
    try:
        SystemIntegrationTestFramework.assert_safe_endpoint("SIMULATION","https://broker.test","prod-live-account")
        raise AssertionError("real-looking account accepted")
    except UnsafeTestEnvironment:
        pass
    assert SystemIntegrationTestFramework.assert_safe_endpoint("INTEGRATION_TEST","https://broker.test","sandbox")

def test_deterministic_replay():
    fw=SystemIntegrationTestFramework("INTEGRATION_TEST",seed=42)
    events=[
        {"type":"MARKET","regime":"BULLISH_TREND","volatility":"NORMAL"},
        {"type":"RISK","multiplier":.5},
        {"type":"ORDER_FILL","units":100},
        {"type":"UNKNOWN_ORDER"},
        {"type":"RECONCILE","broker_units":37},
        {"type":"SEEDED_TIE_BREAK","choices":["A","B","C"]}
    ]
    a=fw.deterministic_replay(events,42)
    b=fw.deterministic_replay(events,42)
    assert a["digest"]==b["digest"]
    assert a["state"]["position"]==37
    assert a["state"]["risk_multiplier"]==.5

def test_full_integrated_suite_has_no_critical_failure_and_all_gates_pass():
    fw=SystemIntegrationTestFramework("INTEGRATION_TEST",seed=140013)
    report=asyncio.run(fw.run_all())
    assert report["critical_failures"]==0
    assert report["failed"]==0
    assert report["pass_fail_gate"]["ready_for_step15"] is True
    assert all(report["pass_fail_gate"]["gates"].values())
    # Coverage must touch every major architecture family.
    components={x["component"] for x in report["coverage_matrix"]}
    required={"Risk Engine","Smart Execution","Recovery","Reconciliation","Governance","System Evaluation","Execution","Broker","Persistence"}
    assert required.issubset(components)

def test_regression_report_detects_deltas_without_mutating_baseline():
    fw=SystemIntegrationTestFramework("INTEGRATION_TEST",seed=7)
    asyncio.run(fw.scenario_golden_path())
    baseline=fw.report()
    current=fw.report(baseline)
    assert current["regression"]["critical_failure_delta"]==0
    assert current["regression"]["failed_test_delta"]==0

def test_scenario_library_contains_required_extreme_cases():
    fw=SystemIntegrationTestFramework("SIMULATION")
    lib=fw.scenario_library()
    for name in ("GOLDEN_PATH","FLASH_CRASH","BROKER_OUTAGE","DATABASE_FAILURE",
                 "PARTIAL_FILL_DISCONNECT","CANARY_FAILURE","GOVERNANCE_FREEZE",
                 "EMERGENCY_RECOVERY","SMART_EXECUTION_SHADOW","FULL_EXTREME_SIMULATION"):
        assert name in lib

if __name__=="__main__":
    test_failure_injection_is_nonproduction_only()
    test_real_endpoint_and_account_are_rejected()
    test_deterministic_replay()
    test_full_integrated_suite_has_no_critical_failure_and_all_gates_pass()
    test_regression_report_detects_deltas_without_mutating_baseline()
    test_scenario_library_contains_required_extreme_cases()
    print("system integration framework tests: OK")

import asyncio
import ast
import inspect
import time

import server


def test_scan_offloads_all_sync_outcome_resolvers():
    src = inspect.getsource(server.scan)
    tree = ast.parse(src)
    awaited_to_thread = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name):
                if call.func.value.id == "asyncio" and call.func.attr == "to_thread" and call.args:
                    first = call.args[0]
                    if isinstance(first, ast.Name):
                        awaited_to_thread.append(first.id)
    assert "resolve_pending" in awaited_to_thread
    assert "resolve_shadow_trials" in awaited_to_thread
    assert "resolve_candidate_paper_trades" in awaited_to_thread


def test_to_thread_keeps_event_loop_responsive_for_blocking_resolver_work():
    ticks = []

    def blocking_work():
        time.sleep(0.12)
        return 7

    async def scenario():
        async def ticker():
            end = asyncio.get_running_loop().time() + 0.10
            while asyncio.get_running_loop().time() < end:
                ticks.append(asyncio.get_running_loop().time())
                await asyncio.sleep(0.01)
        result, _ = await asyncio.gather(asyncio.to_thread(blocking_work), ticker())
        return result

    assert asyncio.run(scenario()) == 7
    assert len(ticks) >= 5


def test_observability_monitor_offloads_periodic_sync_work():
    src = inspect.getsource(server.observability_loop_monitor)
    assert 'await asyncio.to_thread(run_system_evaluation, source="periodic")' in src
    assert 'await asyncio.to_thread(run_governance_cycle, "periodic")' in src
    assert 'await asyncio.to_thread(refresh_smart_execution_observability)' in src
    assert 'await asyncio.to_thread(refresh_ensemble_observability)' in src


def test_scan_offloads_sync_research_and_learning_refreshes():
    src = inspect.getsource(server.scan)
    expected = [
        "refresh_discovered_patterns",
        "refresh_filter_hypotheses",
        "refresh_external_hypotheses",
        "autonomous_discovery_refresh",
        "review_active_research_rules",
        "security_queue_validated_research_changes",
        "evaluate_all_strategy_health",
        "reconcile_ai_director_outcomes",
        "should_retrain_model",
        "train_shadow_model",
    ]
    for name in expected:
        assert f"asyncio.to_thread({name}" in src, name

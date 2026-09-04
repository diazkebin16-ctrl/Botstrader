import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import automation_v3_remote_worker as remote_worker
from automation_v3_convergence import (
    checkpoint_matches,
    compact_pre_gate,
    discovery_checkpoint,
)
from autonomous_asset_optimizer import diagnose_discovery, integrity_artifact_failed
from cascade_optimizer import CascadeOptimizer, MethodologyViolation, Stage
from operational_time import fixed_entry_gate
from research_manager import sha256_file


def _discovery_report(*, dataset="dataset-a", phase2="phase2-a", status="NO_FREEZE_ELIGIBLE_CANDIDATE"):
    return {
        "stage": "discovery",
        "status": status,
        "instrument": "AUD_USD",
        "lookahead_protection": True,
        "dataset_identity": {"dataset_identity": dataset, "code_sha": "a" * 40},
        "input_sha256": "b" * 64,
        "phase2_sha256": phase2,
        "candidate_space": {"generated": 120, "evaluated_after_discovery_gate": 26, "freeze_eligible": 0},
        "discovery_metrics": {"episodes": 2085, "resolved_binary": 1618},
        "ranked_candidates": [
            {
                "candidate": {"id": "candidate-1"},
                "discovery": {"selected": {"resolved_binary": 100}},
                "validation": {
                    "selected": {"resolved_binary": 80},
                    "win_retention": 0.75,
                    "losses_rejected": 10,
                    "expectancy_delta_r": 0.10,
                },
                "overfitting_risk": {"severity": "HIGH"},
                "directional_stability": {"stable": True},
                "temporal_stability": {"stable": True},
            }
        ],
    }


def test_sufficient_support_high_overfitting_is_terminal_no_valid_candidate():
    diag = diagnose_discovery(_discovery_report(), min_resolved=10)
    assert diag["max_resolved"] >= 10
    assert diag["dominant_failure"] == "HIGH_OVERFITTING_RISK"
    assert diag["recommended_action"] == "NO_VALID_CANDIDATE"


def test_sufficient_support_negative_methodology_does_not_expand_lookback():
    diag = diagnose_discovery(_discovery_report(), min_resolved=10)
    assert diag["recommended_action"] != "EXPAND_LOOKBACK"


def test_insufficient_support_still_allows_lookback_expansion():
    report = _discovery_report()
    report["discovery_metrics"] = {"episodes": 9, "resolved_binary": 6}
    report["ranked_candidates"] = []
    report["candidate_space"] = {"generated": 120, "evaluated_after_discovery_gate": 0, "freeze_eligible": 0}
    diag = diagnose_discovery(report, min_resolved=10)
    assert diag["dominant_failure"] == "INSUFFICIENT_SUPPORT"
    assert diag["recommended_action"] == "EXPAND_LOOKBACK"


def test_integrity_pass_cannot_be_reclassified_from_stale_failure():
    assert integrity_artifact_failed({"status": "PASS", "failures": []}) is False


def test_current_recoverable_integrity_failure_remains_recoverable():
    assert integrity_artifact_failed({"status": "FAIL", "failures": ["M1_HORIZON_COVERAGE_INCOMPLETE"]}) is True


class FakeManager:
    def __init__(self, phases):
        self.state = {"assets": {"AUD_USD": {"phases": phases}}}
        self.update_calls = []

    def load(self):
        return self.state

    def update_phase(self, instrument, name, status, artifact=None, details=None):
        phase = self.state["assets"][instrument]["phases"][name]
        phase["status"] = status
        if artifact is not None:
            phase["artifact"] = artifact
            phase["artifact_sha256"] = sha256_file(artifact)
        if details is not None:
            phase["details"] = details
        self.update_calls.append((name, status))

    def active_phase1_best_viable_approval(self, instrument, artifact):
        return None


def _completed_phase(path: Path):
    path.write_text('{}\n', encoding="utf-8")
    return {"status": "COMPLETED", "artifact_sha256": sha256_file(path), "details": {}}


def test_same_discovery_identity_checkpoint_hit_avoids_reevaluation(tmp_path):
    names = ["data_integrity", "replay", "target_population", "phase_1", "phase_2", "discovery"]
    stages = []
    phases = {}
    for name in names[:-1]:
        artifact = tmp_path / f"{name}.json"
        phases[name] = _completed_phase(artifact)
        stages.append(Stage(name, ("python", name), artifact))
    discovery_path = tmp_path / "discovery.json"
    report = _discovery_report()
    discovery_path.write_text(json.dumps(report), encoding="utf-8")
    discovery_stage = Stage("discovery", ("python", "discovery", "--min-resolved", "10"), discovery_path)
    checkpoint = discovery_checkpoint(artifact_path=discovery_path, report=report, command=discovery_stage.command)
    phases["discovery"] = {
        "status": "BLOCKED",
        "artifact_sha256": sha256_file(discovery_path),
        "details": {"negative_checkpoint": checkpoint, "cache_hits": 0},
    }
    stages.append(discovery_stage)
    called = {"count": 0}

    def runner(*args, **kwargs):
        called["count"] += 1
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    manager = FakeManager(phases)
    with pytest.raises(MethodologyViolation, match="memoized"):
        CascadeOptimizer(manager, runner=runner).run("AUD_USD", stages, through="discovery")
    assert called["count"] == 0
    assert manager.state["assets"]["AUD_USD"]["phases"]["discovery"]["details"]["cache_hits"] == 1


def test_dataset_identity_change_invalidates_discovery_checkpoint(tmp_path):
    path = tmp_path / "discovery.json"
    report = _discovery_report(dataset="dataset-a")
    path.write_text(json.dumps(report), encoding="utf-8")
    command = ("python", "discovery")
    checkpoint = discovery_checkpoint(artifact_path=path, report=report, command=command)
    changed = _discovery_report(dataset="dataset-b")
    path.write_text(json.dumps(changed), encoding="utf-8")
    assert not checkpoint_matches(checkpoint, artifact_path=path, report=changed, command=command)


def test_code_or_methodology_command_change_invalidates_checkpoint(tmp_path):
    path = tmp_path / "discovery.json"
    report = _discovery_report()
    path.write_text(json.dumps(report), encoding="utf-8")
    checkpoint = discovery_checkpoint(artifact_path=path, report=report, command=("python", "discovery", "--code", "a"))
    assert not checkpoint_matches(
        checkpoint,
        artifact_path=path,
        report=report,
        command=("python", "discovery", "--code", "b"),
    )


def _paper_remote_env(monkeypatch, tmp_path):
    monkeypatch.setenv("GH_TOKEN", "g")
    monkeypatch.setenv("RAILWAY_TOKEN", "r")
    monkeypatch.setenv("OANDA_TOKEN", "o")
    monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY", "false")
    monkeypatch.setenv("TRADING_ENVIRONMENT", "PAPER")
    monkeypatch.setenv("PRIMARY_OANDA_ENV", "practice")
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "research"))
    monkeypatch.setenv("BOTS_V3_REMOTE_STATUS_PATH", str(tmp_path / "status.json"))
    monkeypatch.setenv("BOTS_V3_STATUS_INTERVAL_SECONDS", "1")


def test_remote_worker_drains_child_output_to_files_not_pipe(monkeypatch, tmp_path):
    _paper_remote_env(monkeypatch, tmp_path)
    seen = {}

    class Proc:
        returncode = 0
        def poll(self): return 0
        def communicate(self): return (None, None)

    def popen(cmd, **kwargs):
        seen["stdout"] = kwargs["stdout"]
        seen["stderr"] = kwargs["stderr"]
        kwargs["stdout"].write(json.dumps({"status": "NO_VALID_CANDIDATE"}))
        kwargs["stdout"].flush()
        return Proc()

    monkeypatch.setattr(remote_worker.subprocess, "Popen", popen)
    result = remote_worker.run_worker("AUD_USD")
    assert seen["stdout"] is not subprocess.PIPE
    assert seen["stderr"] is not subprocess.PIPE
    assert result["terminal_state"] == "NO_VALID_CANDIDATE"


def test_execution_budget_creates_resumable_checkpoint(monkeypatch, tmp_path):
    _paper_remote_env(monkeypatch, tmp_path)
    monkeypatch.setenv("BOTS_V3_EXECUTION_BUDGET_SECONDS", "60")
    root = tmp_path / "research"
    ledger = root / "AUD_USD" / "autonomous_v3" / "automation_v3_state.json"
    ledger.parent.mkdir(parents=True)
    ledger.write_text(json.dumps({"runs": {"AUD_USD": {"status": "RUNNING", "code_sha": "a" * 40, "lookback_attempts": [{"months": 1, "code_sha": "a" * 40}], "production_authority": False}}}), encoding="utf-8")

    class Proc:
        returncode = None
        stopped = False
        def poll(self): return -15 if self.stopped else None
        def terminate(self): self.stopped = True; self.returncode = -15
        def communicate(self): return (None, None)

    monkeypatch.setattr(remote_worker.subprocess, "Popen", lambda *a, **k: Proc())
    monkeypatch.setattr(remote_worker.time, "sleep", lambda *a: None)
    ticks = iter([0.0, 61.0, 62.0, 63.0, 64.0, 65.0])
    monkeypatch.setattr(remote_worker.time, "monotonic", lambda: next(ticks, 66.0))
    result = remote_worker.run_worker("AUD_USD")
    assert result["terminal_state"] == "EXECUTION_BUDGET_CHECKPOINT"
    persisted = json.loads(ledger.read_text())["runs"]["AUD_USD"]
    assert persisted["status"] == "EXECUTION_BUDGET_CHECKPOINT"
    assert persisted["final_outcome"] is None
    assert persisted["production_authority"] is False


def test_resume_checkpoint_preserves_completed_prior_stage_hashes(tmp_path):
    artifact = tmp_path / "phase.json"
    phase = _completed_phase(artifact)
    original = phase["artifact_sha256"]
    assert original == sha256_file(artifact)
    assert phase["status"] == "COMPLETED"


def test_console_pre_gate_summary_is_compact_but_full_artifact_can_remain_detailed():
    full = {
        "generated_candidates": 120,
        "pass_all_pre_gate": 26,
        "dominant_failure": "PRE_GATE_PASS_EXISTS",
        "recommended_action": "CONTINUE_EXISTING_EVALUATED_PATH",
        "production_authority": False,
        "candidate_results": [{"candidate_id": str(i), "large": "x" * 1000} for i in range(120)],
    }
    compact = compact_pre_gate(full)
    assert "candidate_results" not in compact
    assert compact["generated_candidates"] == 120
    assert len(full["candidate_results"]) == 120


def test_fixed_operational_schedule_remains_immutable():
    blocked = fixed_entry_gate(datetime(2026, 9, 4, 11, 0, tzinfo=timezone.utc))
    assert blocked["allowed"] is False
    assert blocked["researchable"] is False
    assert blocked["reason"] == "NY_ENTRY_BLACKOUT_07_10"


def test_production_authority_remains_false():
    report = _discovery_report()
    diag = diagnose_discovery(report)
    assert diag["production_authority"] is False
    assert discovery_checkpoint(artifact_path=Path(__file__), report=report, command=("python", "discovery"))["production_authority"] is False

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from automation_v3_phase1_continuation import run_with_phase1_autonomous_continuation
from automation_v3_remote_worker import _snapshot
from autonomous_asset_optimizer import (
    AUTOMATION_APPROVAL_TYPE,
    AUTOMATION_AUTHORITY,
    AUTOMATION_SCOPE,
    AutonomousResearchManager,
    V3Ledger,
    canonical_sha256,
    sha256_file,
    utc_now,
    validate_autonomous_phase1,
    write_json,
)
from research_manager import ResearchManager

CURRENT_SHA = "0a8cb00245f6c117e2fcd728b7a6b1084538879f"
OLD_SHA = "4bca8d1bf7aeb4fb15b91d5fb79f849bdf90288c"
INSTRUMENT = "AUD_USD"


def _target(tmp_path: Path, name: str = "03_target_population.json") -> Path:
    path = tmp_path / name
    write_json(path, {"stage": "target_population", "instrument": INSTRUMENT, "rows": 115})
    return path


def _phase1(target_sha: str, **overrides) -> dict:
    value = {
        "stage": "phase_1",
        "instrument": INSTRUMENT,
        "status": "REVIEW_REQUIRED",
        "all_target_wins_recovered": False,
        "selection_scope": "DISCOVERY_ONLY",
        "lookahead_protection": True,
        "input_sha256": target_sha,
        "best_policy": {
            "opened_gates": ["M1_CONFIRMATION", "QUALITY_EXTENSION", "LOW_ROOM"],
            "wins_recovered": 4,
            "losses_released": 7,
            "eligible_episodes": 16,
        },
        "candidates": [
            {"opened_gates": ["M1_CONFIRMATION", "QUALITY_EXTENSION", "LOW_ROOM"], "wins_recovered": 4, "losses_released": 7, "eligible_episodes": 16},
            {"opened_gates": ["M1_CONFIRMATION", "QUALITY_EXTENSION"], "wins_recovered": 3, "losses_released": 4, "eligible_episodes": 10},
            {"opened_gates": ["QUALITY_EXTENSION", "LOW_ROOM"], "wins_recovered": 3, "losses_released": 5, "eligible_episodes": 12},
            {"opened_gates": [], "wins_recovered": 1, "losses_released": 2, "eligible_episodes": 5},
        ],
        "unrecovered_target_wins": [
            {"direction": "BUY", "immutable_blocks": ["SAFETY:minimum_rr", "WAIT_DIRECTION"], "relaxable_blocks": []},
            {"direction": "SELL", "immutable_blocks": ["SAFETY:minimum_rr"], "relaxable_blocks": []},
        ],
    }
    value.update(overrides)
    return value


def _manager(tmp_path: Path, *, code_sha: str = CURRENT_SHA):
    ledger = V3Ledger(tmp_path / "automation_v3_state.json")
    state = tmp_path / "research_state.json"
    inner = ResearchManager(state)
    inner.register_asset(INSTRUMENT, code_sha=code_sha, start="2026-08-04T07:00:00+00:00", end="2026-09-04T07:00:00+00:00", warmup_days=10, horizon_minutes=240, data_sha256="data")
    target = _target(tmp_path)
    inner.update_phase(INSTRUMENT, "target_population", "COMPLETED", artifact=str(target))
    return AutonomousResearchManager(state, ledger), ledger, target


class ReviewThenContinueCascade:
    def __init__(self, phase1_path: Path, phase1_value: dict):
        self.phase1_path = phase1_path
        self.phase1_value = phase1_value
        self.calls = 0

    def run(self, instrument, stages, through):
        assert instrument == INSTRUMENT
        assert through == "prompts"
        self.calls += 1
        if self.calls == 1:
            write_json(self.phase1_path, self.phase1_value)
            raise RuntimeError("Artifact status is REVIEW_REQUIRED")
        return {"status": "CONTINUED"}


class AlwaysReviewCascade(ReviewThenContinueCascade):
    def run(self, instrument, stages, through):
        self.calls += 1
        write_json(self.phase1_path, self.phase1_value)
        raise RuntimeError("Artifact status is REVIEW_REQUIRED")


def _run_helper(tmp_path: Path, phase1_value: dict | None = None):
    manager, ledger, target = _manager(tmp_path)
    phase1 = tmp_path / "04_phase_1.json"
    value = phase1_value or _phase1(sha256_file(target))
    cascade = ReviewThenContinueCascade(phase1, value)
    approval = run_with_phase1_autonomous_continuation(
        cascade=cascade,
        manager=manager,
        ledger=ledger,
        instrument=INSTRUMENT,
        stages=[],
        through="prompts",
        phase1_artifact=phase1,
        load_json=lambda p: json.loads(Path(p).read_text()),
        utc_now=utc_now,
    )
    return approval, manager, ledger, target, phase1, cascade


def test_valid_review_required_creates_fresh_approval_and_continues(tmp_path):
    approval, _, _, _, _, cascade = _run_helper(tmp_path)
    assert approval and cascade.calls == 2


def test_approval_type_exact(tmp_path):
    approval, *_ = _run_helper(tmp_path)
    assert approval["approval_type"] == AUTOMATION_APPROVAL_TYPE


def test_approval_authority_exact(tmp_path):
    approval, *_ = _run_helper(tmp_path)
    assert approval["approval_authority"] == AUTOMATION_AUTHORITY


def test_authorization_scope_exact(tmp_path):
    approval, *_ = _run_helper(tmp_path)
    assert approval["authorization_scope"] == AUTOMATION_SCOPE


def test_ia1_approved_false(tmp_path):
    approval, *_ = _run_helper(tmp_path)
    assert approval["ia1_approved"] is False


def test_human_approval_false(tmp_path):
    approval, *_ = _run_helper(tmp_path)
    assert approval["human_approval"] is False


def test_production_authority_false(tmp_path):
    approval, *_ = _run_helper(tmp_path)
    assert approval["production_authority"] is False


def test_instrument_binding(tmp_path):
    approval, *_ = _run_helper(tmp_path)
    assert approval["instrument"] == INSTRUMENT


def test_current_code_sha_binding(tmp_path):
    approval, *_ = _run_helper(tmp_path)
    assert approval["code_sha"] == CURRENT_SHA


def test_current_dataset_identity_binding(tmp_path):
    approval, manager, *_ = _run_helper(tmp_path)
    assert approval["dataset_identity"] == manager.inner.load()["assets"][INSTRUMENT]["dataset_identity"]


def test_target_population_sha_binding(tmp_path):
    approval, _, _, target, *_ = _run_helper(tmp_path)
    assert approval["target_population_sha256"] == sha256_file(target)


def test_phase1_artifact_sha_binding(tmp_path):
    approval, _, _, _, phase1, _ = _run_helper(tmp_path)
    assert approval["phase1_artifact_sha256"] == sha256_file(phase1)


def test_best_policy_sha_binding(tmp_path):
    approval, _, _, _, phase1, _ = _run_helper(tmp_path)
    artifact = json.loads(phase1.read_text())
    assert approval["best_policy_sha256"] == canonical_sha256(artifact["best_policy"])


def test_only_approved_relaxable_gates_are_accepted(tmp_path):
    target = _target(tmp_path)
    assert validate_autonomous_phase1(_phase1(sha256_file(target)), instrument=INSTRUMENT, target_population_sha256=sha256_file(target))["wins_recovered"] == 4


def test_immutable_or_unknown_opened_gate_is_blocked(tmp_path):
    target = _target(tmp_path)
    artifact = _phase1(sha256_file(target))
    artifact["best_policy"] = {"opened_gates": ["WAIT_DIRECTION"], "wins_recovered": 99, "losses_released": 0}
    artifact["candidates"].append(dict(artifact["best_policy"]))
    with pytest.raises(ValueError, match="unknown/immutable gate"):
        validate_autonomous_phase1(artifact, instrument=INSTRUMENT, target_population_sha256=sha256_file(target))


def test_missing_immutable_blocker_is_blocked(tmp_path):
    target = _target(tmp_path)
    artifact = _phase1(sha256_file(target))
    artifact["unrecovered_target_wins"][0]["immutable_blocks"] = []
    with pytest.raises(ValueError, match="immutable blocker"):
        validate_autonomous_phase1(artifact, instrument=INSTRUMENT, target_population_sha256=sha256_file(target))


def test_invalid_review_required_remains_blocked(tmp_path):
    manager, ledger, target = _manager(tmp_path)
    phase1 = tmp_path / "04_phase_1.json"
    invalid = _phase1(sha256_file(target), lookahead_protection=False)
    cascade = AlwaysReviewCascade(phase1, invalid)
    with pytest.raises(ValueError, match="methodology/binding"):
        run_with_phase1_autonomous_continuation(cascade=cascade, manager=manager, ledger=ledger, instrument=INSTRUMENT, stages=[], through="prompts", phase1_artifact=phase1, load_json=lambda p: json.loads(Path(p).read_text()), utc_now=utc_now)


def test_stale_autonomous_approval_is_rejected(tmp_path):
    approval, manager, ledger, _, phase1, _ = _run_helper(tmp_path)
    state = ledger.load(); record = state["runs"][INSTRUMENT]["approvals"][-1]; record["code_sha"] = OLD_SHA; ledger.save(state)
    assert manager.active_phase1_best_viable_approval(INSTRUMENT, phase1) is None
    assert approval["code_sha"] == CURRENT_SHA


def test_old_approval_invalidated_on_code_sha_change(tmp_path):
    manager, ledger, _ = _manager(tmp_path, code_sha=OLD_SHA)
    ledger.mutate(INSTRUMENT, code_sha=OLD_SHA)
    ledger.append(INSTRUMENT, "approvals", {"active": False, "invalidated_reason": "CODE_SHA_CHANGED", "code_sha": OLD_SHA, "approval_type": AUTOMATION_APPROVAL_TYPE, "production_authority": False})
    old = ledger.run(INSTRUMENT)["approvals"][0]
    assert old["active"] is False and old["invalidated_reason"] == "CODE_SHA_CHANGED"


def test_fresh_approval_minted_after_code_sha_change(tmp_path):
    manager, ledger, target = _manager(tmp_path)
    ledger.append(INSTRUMENT, "approvals", {"active": False, "invalidated_reason": "CODE_SHA_CHANGED", "code_sha": OLD_SHA, "approval_type": AUTOMATION_APPROVAL_TYPE, "production_authority": False})
    phase1 = tmp_path / "04_phase_1.json"; write_json(phase1, _phase1(sha256_file(target)))
    fresh = manager.approve_phase1_autonomous(INSTRUMENT, phase1)
    history = ledger.run(INSTRUMENT)["approvals"]
    assert history[0]["active"] is False and fresh["code_sha"] == CURRENT_SHA and fresh["active"] is True


def test_resume_does_not_rerun_valid_earlier_stages(tmp_path):
    approval, manager, _, _, _, _ = _run_helper(tmp_path)
    phases = manager.inner.load()["assets"][INSTRUMENT]["phases"]
    assert phases["target_population"]["status"] == "COMPLETED"
    assert phases["phase_1"]["status"] == "COMPLETED"
    assert approval is not None


def test_exact_phase1_artifact_is_not_regenerated(tmp_path):
    approval, _, _, _, phase1, _ = _run_helper(tmp_path)
    before = hashlib.sha256(phase1.read_bytes()).hexdigest()
    assert before == approval["phase1_artifact_sha256"] == hashlib.sha256(phase1.read_bytes()).hexdigest()


def test_remote_worker_reports_phase2_after_continuation(tmp_path, monkeypatch):
    root = tmp_path / "root"; work = root / INSTRUMENT / "autonomous_v3"; work.mkdir(parents=True)
    ledger = V3Ledger(work / "automation_v3_state.json")
    ledger.mutate(INSTRUMENT, code_sha=CURRENT_SHA, status="RUNNING", phase1_status="REVIEW_REQUIRED", autonomous_approval="CREATED")
    ledger.append(INSTRUMENT, "lookback_attempts", {"months": 1, "code_sha": CURRENT_SHA})
    research_dir = work / f"lookback_01m_{CURRENT_SHA[:12]}"; research_dir.mkdir()
    rm = ResearchManager(research_dir / "research_state.json")
    rm.register_asset(INSTRUMENT, code_sha=CURRENT_SHA, start="s", end="e", warmup_days=10, horizon_minutes=240, data_sha256="d")
    target = research_dir / "03_target_population.json"; write_json(target, {"ok": True}); rm.update_phase(INSTRUMENT, "target_population", "COMPLETED", artifact=str(target))
    phase1 = research_dir / "04_phase_1.json"; write_json(phase1, {"status": "REVIEW_REQUIRED"}); rm.update_phase(INSTRUMENT, "phase_1", "COMPLETED", artifact=str(phase1), details={"review_status": "REVIEW_REQUIRED"})
    monkeypatch.delenv("GITHUB_SHA", raising=False); monkeypatch.delenv("GITHUB_WORKFLOW", raising=False)
    snap = _snapshot(root, INSTRUMENT, "33866217544")
    assert snap["current_stage"] == "phase_2"
    assert snap["phase1_status"] == "REVIEW_REQUIRED"
    assert snap["autonomous_approval"] == "CREATED"


def test_end_to_end_production_authority_false(tmp_path):
    approval, _, ledger, *_ = _run_helper(tmp_path)
    assert approval["production_authority"] is False
    assert ledger.run(INSTRUMENT)["production_authority"] is False


def test_real_run_33866217544_regression_fixture(tmp_path):
    manager, ledger, target = _manager(tmp_path)
    ledger.append(INSTRUMENT, "approvals", {
        "active": False,
        "approval_type": AUTOMATION_APPROVAL_TYPE,
        "approval_authority": AUTOMATION_AUTHORITY,
        "authorization_scope": AUTOMATION_SCOPE,
        "code_sha": OLD_SHA,
        "invalidated_reason": "CODE_SHA_CHANGED",
        "ia1_approved": False,
        "human_approval": False,
        "production_authority": False,
    })
    phase1 = tmp_path / "04_phase_1.json"
    artifact = _phase1(sha256_file(target))
    cascade = ReviewThenContinueCascade(phase1, artifact)
    approval = run_with_phase1_autonomous_continuation(cascade=cascade, manager=manager, ledger=ledger, instrument=INSTRUMENT, stages=[], through="prompts", phase1_artifact=phase1, load_json=lambda p: json.loads(Path(p).read_text()), utc_now=utc_now)
    history = ledger.run(INSTRUMENT)["approvals"]
    assert cascade.calls == 2
    assert history[0]["active"] is False and history[0]["code_sha"] == OLD_SHA
    assert approval["code_sha"] == CURRENT_SHA and approval["active"] is True
    assert approval["phase1_artifact_sha256"] == sha256_file(phase1)
    assert approval["target_population_sha256"] == sha256_file(target)
    assert approval["production_authority"] is False

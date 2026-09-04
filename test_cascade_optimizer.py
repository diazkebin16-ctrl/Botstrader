import json
import subprocess

import pytest

from cascade_optimizer import CASCADE, CascadeOptimizer, MethodologyViolation, Stage, validate_research_artifact
from research_manager import ResearchManager, sha256_file


def _registered(tmp_path):
    manager = ResearchManager(tmp_path / "state.json")
    manager.register_asset(
        "AUD_USD", code_sha="sha", start="start", end="end",
        warmup_days=10, horizon_minutes=240,
    )
    return manager


def test_full_cascade_records_artifacts_and_resumes(tmp_path):
    manager = _registered(tmp_path)
    calls = []
    stages = []
    for name in CASCADE:
        artifact = tmp_path / f"{name}.json"
        stages.append(Stage(name, ("research-tool", name), artifact))

    def runner(command, **kwargs):
        name = command[-1]
        payload = {"status": "OK"}
        if name == "data_integrity":payload["status"]="PASS"
        if name == "replay":payload["methodology"]={"no_lookahead_decision":True}
        if name == "target_population":payload["lookahead_protection"]=True
        if name == "phase_1":payload.update({"lookahead_protection":True,"all_target_wins_recovered":True,"selection_scope":"DISCOVERY_ONLY"})
        if name in {"phase_2", "discovery", "discovery_repeat", "freeze", "holdout", "pre_audit"}:
            payload["lookahead_protection"] = True
        if name in {"discovery","discovery_repeat"}:payload["status"]="OK"
        if name == "determinism":payload["status"]="PASS"
        if name == "freeze":payload.update({"immutable":True,"holdout_opened":False,"candidate_id":"c1","candidate_definition_sha256":"d1"})
        if name == "holdout":payload.update({"retuning_after_holdout":False,"candidate_definition_sha256":"d1"})
        if name == "audit":payload.update({"status":"PASS","production_modifications":"NONE"})
        if name == "report":payload={"LOOK-AHEAD":{"status":"PASS"}}
        if name == "pre_audit":payload.update({"verdict":"ACCEPT","severities":{}})
        (tmp_path / f"{name}.json").write_text(json.dumps(payload), encoding="utf-8")
        calls.append(name)
        return subprocess.CompletedProcess(command, 0, "", "")

    optimizer = CascadeOptimizer(manager, runner=runner)
    assert optimizer.run("AUD_USD", stages,through="prompts")["status"] == "COMPLETED"
    assert calls == list(CASCADE)
    assert optimizer.run("AUD_USD", stages,through="prompts")["status"] == "COMPLETED"
    assert calls == list(CASCADE)


@pytest.mark.parametrize("failed_stage", ["holdout", "pre_audit"])
def test_fail_artifact_blocks_cascade_and_later_stages(tmp_path, failed_stage):
    manager = _registered(tmp_path)
    calls = []
    stages = [Stage(name, ("research-tool", name), tmp_path / f"{name}.json") for name in CASCADE]

    def runner(command, **kwargs):
        name = command[-1]
        payload = {"status": "OK"}
        if name == "data_integrity":
            payload["status"] = "PASS"
        if name == "replay":
            payload["methodology"] = {"no_lookahead_decision": True}
        if name == "target_population":
            payload["lookahead_protection"] = True
        if name == "phase_1":
            payload.update({"lookahead_protection": True, "all_target_wins_recovered": True, "selection_scope": "DISCOVERY_ONLY"})
        if name in {"phase_2", "discovery", "discovery_repeat", "freeze", "holdout", "pre_audit"}:
            payload["lookahead_protection"] = True
        if name == "determinism":
            payload["status"] = "PASS"
        if name == "freeze":
            payload.update({"immutable": True, "holdout_opened": False, "candidate_id": "c1", "candidate_definition_sha256": "d1"})
        if name == "holdout":
            payload.update({"retuning_after_holdout": False, "candidate_definition_sha256": "d1"})
        if name == "audit":
            payload.update({"status": "PASS", "production_modifications": "NONE"})
        if name == "report":
            payload = {"LOOK-AHEAD": {"status": "PASS"}}
        if name == "pre_audit":
            payload.update({"verdict": "ACCEPT", "severities": {}})
        if name == failed_stage:
            payload["status"] = "FAIL"
        (tmp_path / f"{name}.json").write_text(json.dumps(payload), encoding="utf-8")
        calls.append(name)
        return subprocess.CompletedProcess(command, 0, "", "")

    with pytest.raises(MethodologyViolation, match="Artifact status is FAIL"):
        CascadeOptimizer(manager, runner=runner).run("AUD_USD", stages, through="prompts")

    phases = manager.load()["assets"]["AUD_USD"]["phases"]
    assert phases[failed_stage]["status"] == "BLOCKED"
    assert phases[failed_stage]["status"] != "COMPLETED"
    assert calls[-1] == failed_stage
    assert all(phases[name]["status"] == "PENDING" for name in CASCADE[CASCADE.index(failed_stage) + 1:])


@pytest.mark.parametrize("status", ["TIMEOUT", "AMBIGUOUS", "NOT_HISTORICALLY_RECONSTRUCTABLE"])
def test_non_binary_outcomes_cannot_be_labeled_as_loss(tmp_path, status):
    artifact = tmp_path / "bad.json"
    artifact.write_text(json.dumps({"rows": [{"status": status, "label": 0}]}), encoding="utf-8")
    with pytest.raises(MethodologyViolation):
        validate_research_artifact(artifact, "replay")


def test_dual_touch_must_be_ambiguous(tmp_path):
    artifact = tmp_path / "bad.json"
    artifact.write_text(json.dumps({"row": {"dual_touch_same_bar": True, "status": "LOSS"}}), encoding="utf-8")
    with pytest.raises(MethodologyViolation):
        validate_research_artifact(artifact, "replay")


def test_research_stage_requires_lookahead_guard(tmp_path):
    artifact = tmp_path / "phase.json"
    artifact.write_text('{"status":"OK"}', encoding="utf-8")
    with pytest.raises(MethodologyViolation):
        validate_research_artifact(artifact, "phase_1")


def test_production_commands_are_forbidden(tmp_path):
    manager = _registered(tmp_path)
    stages = [Stage(name, ("research-tool", name), tmp_path / f"{name}.json") for name in CASCADE]
    stages[0] = Stage("replay", ("railway", "up"), tmp_path / "replay.json")
    with pytest.raises(ValueError):
        CascadeOptimizer(manager).run("AUD_USD", stages)


def test_can_stop_after_phase1_with_a_prefix_manifest(tmp_path):
    manager = _registered(tmp_path)
    calls=[]
    stages=[Stage(name,("research-tool",name),tmp_path/f"{name}.json") for name in CASCADE[:4]]
    def runner(command,**kwargs):
        name=command[-1]
        payload={"status":"OK","lookahead_protection":True}
        if name=="data_integrity":payload["status"]="PASS"
        (tmp_path/f"{name}.json").write_text(json.dumps(payload),encoding="utf-8")
        calls.append(name)
        return subprocess.CompletedProcess(command,0,"","")
    result=CascadeOptimizer(manager,runner=runner).run("AUD_USD",stages,through="phase_1")
    assert result["phases"]==list(CASCADE[:4])
    assert calls==list(CASCADE[:4])


def _review_required_phase1_payload():
    return {
        "status": "REVIEW_REQUIRED",
        "stage": "phase_1",
        "instrument": "AUD_USD",
        "lookahead_protection": True,
        "selection_scope": "DISCOVERY_ONLY",
        "all_target_wins_recovered": False,
        "input_sha256": "target-sha",
        "best_policy": {
            "opened_gates": [],
            "wins_recovered": 2,
            "losses_released": 4,
            "eligible_episodes": 9,
        },
        "candidates": [
            {
                "opened_gates": [],
                "wins_recovered": 2,
                "losses_released": 4,
                "eligible_episodes": 9,
            },
            {
                "opened_gates": ["LOW_ROOM"],
                "wins_recovered": 2,
                "losses_released": 7,
                "eligible_episodes": 14,
            },
        ],
        "unrecovered_target_wins": [
            {
                "immutable_blocks": ["WAIT_DIRECTION"],
                "relaxable_blocks": [],
            }
        ],
    }


def test_phase1_review_required_still_blocks_without_best_viable_approval(tmp_path):
    manager = _registered(tmp_path)
    stages = [
        Stage(name, ("research-tool", name), tmp_path / f"{name}.json")
        for name in CASCADE
    ]

    def runner(command, **kwargs):
        name = command[-1]
        payload = {"status": "OK"}
        if name == "data_integrity":
            payload["status"] = "PASS"
        if name == "replay":
            payload["methodology"] = {"no_lookahead_decision": True}
        if name == "target_population":
            payload["lookahead_protection"] = True
        if name == "phase_1":
            payload = _review_required_phase1_payload()
        (tmp_path / f"{name}.json").write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, "", "")

    with pytest.raises(MethodologyViolation, match="REVIEW_REQUIRED"):
        CascadeOptimizer(manager, runner=runner).run("AUD_USD", stages, through="phase_1")

    assert manager.load()["assets"]["AUD_USD"]["phases"]["phase_1"]["status"] == "BLOCKED"


def test_approved_phase1_review_resumes_without_rerunning_phase1(tmp_path):
    manager = _registered(tmp_path)

    target = tmp_path / "target_population.json"
    target.write_text('{"target":1,"lookahead_protection":true}\n', encoding="utf-8")
    manager.update_phase("AUD_USD", "target_population", "COMPLETED", artifact=str(target))

    phase1_artifact = tmp_path / "phase_1.json"
    phase1_payload = _review_required_phase1_payload()
    phase1_payload["input_sha256"] = sha256_file(target)
    phase1_artifact.write_text(json.dumps(phase1_payload), encoding="utf-8")

    manager.update_phase(
        "AUD_USD", "phase_1", "BLOCKED", artifact=str(phase1_artifact),
        details={"error":"Artifact status is REVIEW_REQUIRED"},
    )

    manager.approve_phase1_best_viable(
        "AUD_USD", phase1_artifact, ia1_approved=True,
    )

    stages = []
    for name in CASCADE:
        artifact = phase1_artifact if name == "phase_1" else tmp_path / f"{name}.json"
        stages.append(Stage(name, ("research-tool", name), artifact))

    calls = []

    def runner(command, **kwargs):
        name = command[-1]
        calls.append(name)
        payload = {"status": "OK"}
        if name == "data_integrity":
            payload["status"] = "PASS"
        if name == "replay":
            payload["methodology"] = {"no_lookahead_decision": True}
        if name == "target_population":
            payload["lookahead_protection"] = True
        if name == "phase_2":
            payload["lookahead_protection"] = True
        (tmp_path / f"{name}.json").write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, "", "")

    result = CascadeOptimizer(manager, runner=runner).run(
        "AUD_USD", stages, through="phase_2", resume=True,
    )

    assert result["status"] == "COMPLETED"
    assert "phase_1" not in calls
    assert "phase_2" in calls
    phase1_state = manager.load()["assets"]["AUD_USD"]["phases"]["phase_1"]
    assert phase1_state["status"] == "COMPLETED"
    assert phase1_state["details"]["approval_type"] == "BEST_VIABLE_POLICY"
    assert phase1_state["details"]["production_authority"] is False


def test_generic_review_required_still_blocks_other_stage(tmp_path):
    manager = _registered(tmp_path)
    stages = [
        Stage(name, ("research-tool", name), tmp_path / f"{name}.json")
        for name in CASCADE
    ]

    def runner(command, **kwargs):
        name = command[-1]
        payload = {"status": "OK"}
        if name == "data_integrity":
            payload["status"] = "PASS"
        if name == "replay":
            payload["methodology"] = {"no_lookahead_decision": True}
        if name == "target_population":
            payload["lookahead_protection"] = True
        if name == "phase_1":
            payload.update({
                "lookahead_protection": True,
                "all_target_wins_recovered": True,
                "selection_scope": "DISCOVERY_ONLY",
            })
        if name == "phase_2":
            payload["lookahead_protection"] = True
        if name == "discovery":
            payload = {
                "status": "REVIEW_REQUIRED",
                "lookahead_protection": True,
            }
        (tmp_path / f"{name}.json").write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, "", "")

    with pytest.raises(MethodologyViolation, match="REVIEW_REQUIRED"):
        CascadeOptimizer(manager, runner=runner).run(
            "AUD_USD", stages, through="discovery",
        )

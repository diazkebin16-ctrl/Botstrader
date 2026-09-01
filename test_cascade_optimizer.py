import json
import subprocess

import pytest

from cascade_optimizer import CASCADE, CascadeOptimizer, MethodologyViolation, Stage, validate_research_artifact
from research_manager import ResearchManager


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
        if name == "holdout":payload["retuning_after_holdout"]=False
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

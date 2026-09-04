"""Offline, resumable research cascade orchestration.

Commands come from a reviewed JSON manifest.  The orchestrator records evidence
in :mod:`research_manager`; it has no broker, deployment, or trading authority.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from automation_v3_convergence import (
    checkpoint_matches,
    command_identity,
    discovery_checkpoint,
)
from research_manager import ResearchManager, sha256_file


CASCADE = (
    "data_integrity", "replay", "target_population", "phase_1", "phase_2",
    "discovery", "discovery_repeat", "determinism", "freeze", "holdout",
    "audit", "report", "pre_audit", "prompts",
)
FORBIDDEN_COMMAND_PARTS = (
    "railway", "server.py", "deployment_manager.py", "deployment_runtime.py",
    "--auto-trade", "auto_trade=true", "trading_environment=production",
)


class MethodologyViolation(RuntimeError):
    pass


@dataclass(frozen=True)
class Stage:
    name: str
    command: Sequence[str]
    artifact: Path


def _walk(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def validate_research_artifact(path: Path, phase: str) -> Dict[str, Any]:
    """Reject common semantic collapses and missing anti-look-ahead evidence."""
    with path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    for item in _walk(report):
        status = str(item.get("status") or "").upper()
        label = item.get("label")
        if status in {"TIMEOUT", "AMBIGUOUS", "NOT_HISTORICALLY_RECONSTRUCTABLE"} and label not in (None, ""):
            raise MethodologyViolation(f"{status} must not have a binary label")
        if item.get("dual_touch_same_bar") is True and status != "AMBIGUOUS":
            raise MethodologyViolation("dual-touch same-bar must be AMBIGUOUS")
    if report.get("lookahead_detected") is True:
        raise MethodologyViolation("artifact reports look-ahead")
    if phase in {"phase_1", "phase_2", "discovery", "discovery_repeat", "freeze", "holdout", "report", "pre_audit"}:
        protected = report.get("lookahead_protection") is True
        if phase == "report":
            protected = (report.get("LOOK-AHEAD") or {}).get("status") == "PASS"
        if not protected:
            raise MethodologyViolation(f"{phase} lacks explicit lookahead_protection=true")
    if phase == "data_integrity" and str(report.get("status") or "").upper() != "PASS":
        raise MethodologyViolation("dataset integrity gate did not pass")
    if phase == "determinism" and str(report.get("status") or "").upper() != "PASS":
        raise MethodologyViolation("determinism check did not pass")
    if phase == "audit" and str(report.get("status") or "").upper() != "PASS":
        raise MethodologyViolation("test/diff/package audit did not pass")
    if phase == "freeze" and report.get("immutable") is not True:
        raise MethodologyViolation("freeze artifact is not immutable")
    if phase == "holdout" and report.get("retuning_after_holdout") is not False:
        raise MethodologyViolation("holdout artifact does not prohibit retuning")
    return report


class CascadeOptimizer:
    def __init__(
        self, manager: ResearchManager,
        runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self.manager = manager
        self.runner = runner

    @staticmethod
    def _validate_stage(stage: Stage) -> None:
        if stage.name not in CASCADE:
            raise ValueError(f"Unknown cascade stage: {stage.name}")
        if not stage.command:
            raise ValueError(f"Empty command for {stage.name}")
        rendered = " ".join(str(x) for x in stage.command).lower()
        forbidden = next((x for x in FORBIDDEN_COMMAND_PARTS if x in rendered), None)
        if forbidden:
            raise ValueError(f"Offline cascade forbids command containing: {forbidden}")

    def _transition_gate(self, name: str, stages: Mapping[str, Stage], instrument: str) -> Optional[Dict[str, Any]]:
        if name not in {"phase_2", "holdout"}:
            return None
        from research_governance import DecisionGateEngine

        def read(stage_name: str) -> Dict[str, Any]:
            with stages[stage_name].artifact.open("r", encoding="utf-8") as handle:
                return json.load(handle)

        if name == "phase_2":
            phase1 = read("phase_1")
            state = self.manager.load()
            asset = state["assets"][instrument.upper()]
            approval = self.manager.active_phase1_best_viable_approval(
                instrument, stages["phase_1"].artifact,
            )
            result = DecisionGateEngine.evaluate(
                "PHASE_2", integrity=read("data_integrity"), replay=read("replay"),
                target_population=read("target_population"), phase1=phase1,
                phase1_approval=approval,
                instrument=instrument.upper(),
                dataset_identity=asset.get("dataset_identity"),
                code_sha=(asset.get("provenance") or {}).get("code_sha"),
                phase1_artifact_sha256=sha256_file(stages["phase_1"].artifact),
            )
        else:
            result = DecisionGateEngine.evaluate(
                "HOLDOUT", discovery=read("discovery"), frozen=read("freeze"),
            )
        if result["status"] != "ALLOWED":
            raise MethodologyViolation("; ".join(result["reasons"]))
        return result

    def _resume_negative_discovery(self, instrument: str, stage: Stage, current: Mapping[str, Any]) -> None:
        if current.get("status") != "BLOCKED" or not stage.artifact.is_file():
            return
        details = current.get("details") or {}
        checkpoint = details.get("negative_checkpoint") if isinstance(details, Mapping) else None
        if not isinstance(checkpoint, Mapping):
            return
        try:
            report = validate_research_artifact(stage.artifact, "discovery")
        except Exception:
            return
        if str(report.get("status") or "").upper() != "NO_FREEZE_ELIGIBLE_CANDIDATE":
            return
        if not checkpoint_matches(
            checkpoint,
            artifact_path=stage.artifact,
            report=report,
            command=stage.command,
        ):
            return
        cached_details = dict(details)
        cached_details["cache_hits"] = int(cached_details.get("cache_hits") or 0) + 1
        cached_details["last_cache_hit"] = "DISCOVERY_EVALUATED"
        self.manager.update_phase(
            instrument,
            "discovery",
            "BLOCKED",
            artifact=str(stage.artifact),
            details=cached_details,
        )
        raise MethodologyViolation("Artifact status is NO_FREEZE_ELIGIBLE_CANDIDATE (memoized)")

    def run(self, instrument: str, stages: Sequence[Stage], *, resume: bool = True,
            through: str = "report", holdout_reproduction_read_only: bool = False) -> Dict[str, Any]:
        instrument = instrument.upper()
        if through not in CASCADE:
            raise ValueError(f"Unknown terminal stage: {through}")
        requested = CASCADE[:CASCADE.index(through)+1]
        state = self.manager.load()
        if instrument not in state["assets"]:
            raise KeyError(f"Asset not registered: {instrument}")
        by_name = {stage.name: stage for stage in stages}
        missing = [name for name in requested if name not in by_name]
        if missing:
            raise ValueError(f"Cascade manifest missing stages: {', '.join(missing)}")

        env = dict(os.environ)
        env.update({
            "TRADING_ENVIRONMENT": "SIMULATION", "AUTO_TRADE": "false",
            "BOTS_RESEARCH_OFFLINE": "true",
        })
        completed: List[str] = []
        stage_timings: Dict[str, float] = {}
        for name in requested:
            stage = by_name[name]
            self._validate_stage(stage)
            current = self.manager.load()["assets"][instrument]["phases"][name]
            if resume and name == "discovery":
                self._resume_negative_discovery(instrument, stage, current)
            if resume and current.get("status") == "COMPLETED" and stage.artifact.is_file():
                expected = current.get("artifact_sha256")
                if expected and expected == sha256_file(stage.artifact):
                    completed.append(name)
                    continue
            if name == "phase_1" and resume and stage.artifact.is_file():
                approval = self.manager.active_phase1_best_viable_approval(instrument, stage.artifact)
                if approval is not None:
                    with stage.artifact.open("r", encoding="utf-8") as handle:
                        reviewed_phase1 = json.load(handle)
                    if (
                        reviewed_phase1.get("status") == "REVIEW_REQUIRED"
                        and reviewed_phase1.get("all_target_wins_recovered") is False
                    ):
                        self.manager.update_phase(
                            instrument, name, "COMPLETED", artifact=str(stage.artifact),
                            details={
                                "review_status": "REVIEW_REQUIRED",
                                "approval_type": "BEST_VIABLE_POLICY",
                                "approval_phase1_artifact_sha256": approval.get("phase1_artifact_sha256"),
                                "production_authority": False,
                                "checkpoint": {
                                    "name": "PHASE1_READY",
                                    "artifact_sha256": sha256_file(stage.artifact),
                                    "methodology_identity": command_identity(stage.command),
                                    "production_authority": False,
                                },
                            },
                        )
                        completed.append(name)
                        continue
            self.manager.update_phase(instrument, name, "RUNNING")
            started = time.monotonic()
            report: Dict[str, Any] | None = None
            try:
                transition_gate = self._transition_gate(name, by_name, instrument)
                holdout_authorization = None
                if name == "holdout":
                    with by_name["freeze"].artifact.open("r", encoding="utf-8") as handle:
                        frozen = json.load(handle)
                    try:
                        holdout_authorization = self.manager.begin_holdout(
                            instrument,
                            candidate_definition_sha256=str(frozen.get("candidate_definition_sha256") or ""),
                            freeze_sha256=sha256_file(by_name["freeze"].artifact),
                            read_only_reproduction=holdout_reproduction_read_only,
                        )
                        if (
                            holdout_authorization.get("mode") == "READ_ONLY_REPRODUCTION"
                            and Path(str(holdout_authorization.get("holdout_artifact") or "")).resolve() == stage.artifact.resolve()
                        ):
                            raise ValueError("Read-only reproduction requires a distinct holdout artifact path")
                    except ValueError as exc:
                        raise MethodologyViolation(str(exc)) from exc
                result = self.runner(
                    list(stage.command), check=False, text=True,
                    capture_output=True, env=env,
                )
                if result.returncode != 0:
                    raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "stage failed")
                if not stage.artifact.is_file():
                    raise RuntimeError(f"Expected artifact was not created: {stage.artifact}")
                report = validate_research_artifact(stage.artifact, name)
                stage_seconds = max(0.0, time.monotonic() - started)
                stage_timings[name] = stage_seconds
                blocking_statuses = {"FAIL", "FAILED", "BLOCKED", "REVIEW_REQUIRED", "NO_FREEZE_ELIGIBLE_CANDIDATE"}
                if str(report.get("status") or "OK").upper() in blocking_statuses:
                    raise MethodologyViolation(f"Artifact status is {report['status']}")
                if name == "holdout":
                    try:
                        self.manager.complete_holdout(
                            instrument,
                            candidate_definition_sha256=str(report.get("candidate_definition_sha256") or ""),
                            freeze_sha256=sha256_file(by_name["freeze"].artifact),
                            holdout_artifact=str(stage.artifact),
                            read_only_reproduction=(holdout_authorization or {}).get("mode") == "READ_ONLY_REPRODUCTION",
                        )
                    except ValueError as exc:
                        raise MethodologyViolation(str(exc)) from exc
                evidence_summary = {
                    key: report.get(key) for key in (
                        "dataset_identity", "partitions", "candidate_space", "candidate_id",
                        "freeze_status", "decision", "overfitting_risk", "verdict",
                        "OUTPUT SHA256",
                    ) if report.get(key) is not None
                }
                checkpoint_name = {
                    "data_integrity": "DATA_READY",
                    "target_population": "POPULATION_READY",
                    "phase_1": "PHASE1_READY",
                    "phase_2": "PHASE2_READY",
                    "discovery": "DISCOVERY_EVALUATED",
                    "freeze": "FREEZE_READY",
                    "holdout": "HOLDOUT_COMPLETE",
                    "audit": "AUDIT_COMPLETE",
                }.get(name, name.upper() + "_COMPLETE")
                self.manager.update_phase(
                    instrument, name, "COMPLETED", artifact=str(stage.artifact),
                    details={
                        "command": list(stage.command), "transition_gate": transition_gate,
                        "evidence_summary": evidence_summary,
                        "duration_seconds": stage_seconds,
                        "checkpoint": {
                            "name": checkpoint_name,
                            "artifact_sha256": sha256_file(stage.artifact),
                            "methodology_identity": command_identity(stage.command),
                            "production_authority": False,
                        },
                    },
                )
                if name == "freeze":
                    current_identity = self.manager.load()["assets"][instrument]["dataset_identity"]
                    self.manager.freeze_candidate(instrument, {
                        "candidate_id": report.get("candidate_id"),
                        "candidate_definition_sha256": report.get("candidate_definition_sha256"),
                        "dataset_identity": current_identity,
                        "artifact_dataset_identity": report.get("dataset_identity"),
                        "code_sha": report.get("code_sha"),
                        "target_population_sha256": report.get("target_population_sha256"),
                        "phase2_sha256": report.get("phase2_sha256"),
                        "discovery_sha256": report.get("discovery_sha256"),
                        "artifact": str(stage.artifact),
                        "artifact_sha256": sha256_file(stage.artifact),
                        "rule": report.get("rule"),
                        "threshold": report.get("threshold"),
                    })
                if name == "pre_audit":
                    self.manager.add_audit(instrument, report)
                    self.manager.record_risks_and_limitations(
                        instrument,
                        risks=[{"severity": key, "count": value} for key, value in (report.get("severities") or {}).items() if value],
                        limitations=[] if report.get("verdict") == "ACCEPT" else ["Independent IA #2 and IA #1 review remain required."],
                        forward_status="BLOCKED_HUMAN_IA1_REVIEW_REQUIRED",
                    )
                completed.append(name)
            except BaseException as exc:
                stage_seconds = max(0.0, time.monotonic() - started)
                stage_timings[name] = stage_seconds
                artifact_for_state = None
                details: Dict[str, Any] = {
                    "error": str(exc),
                    "duration_seconds": stage_seconds,
                }
                if name == "phase_1" and stage.artifact.is_file():
                    artifact_for_state = str(stage.artifact)
                if name == "discovery" and stage.artifact.is_file():
                    try:
                        negative_report = report or validate_research_artifact(stage.artifact, name)
                        if str(negative_report.get("status") or "").upper() == "NO_FREEZE_ELIGIBLE_CANDIDATE":
                            artifact_for_state = str(stage.artifact)
                            details["negative_checkpoint"] = discovery_checkpoint(
                                artifact_path=stage.artifact,
                                report=negative_report,
                                command=stage.command,
                            )
                            details["artifact_sha256"] = sha256_file(stage.artifact)
                            details["cache_hits"] = 0
                    except Exception:
                        pass
                self.manager.update_phase(
                    instrument, name, "BLOCKED" if isinstance(exc, MethodologyViolation) else "FAILED",
                    artifact=artifact_for_state,
                    details=details,
                )
                raise
        return {
            "instrument": instrument,
            "status": "COMPLETED",
            "phases": completed,
            "stage_timings_seconds": stage_timings,
        }


def load_manifest(path: os.PathLike[str] | str) -> List[Stage]:
    base = Path(path).resolve().parent
    with open(path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)
    stages = []
    for item in raw.get("stages", []):
        artifact = Path(item["artifact"])
        if not artifact.is_absolute():
            artifact = base / artifact
        stages.append(Stage(item["name"], tuple(item["command"]), artifact))
    return stages


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the offline BotsTrader research cascade")
    parser.add_argument("instrument")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--state", default="research_state.json")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--holdout-reproduction-read-only", action="store_true")
    parser.add_argument("--through", choices=CASCADE, default="report")
    args = parser.parse_args()
    optimizer = CascadeOptimizer(ResearchManager(args.state))
    result = optimizer.run(
        args.instrument, load_manifest(args.manifest),
        resume=not args.no_resume, through=args.through,
        holdout_reproduction_read_only=args.holdout_reproduction_read_only,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

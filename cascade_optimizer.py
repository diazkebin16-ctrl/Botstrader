"""Offline, resumable research cascade orchestration.

Commands come from a reviewed JSON manifest.  The orchestrator records evidence
in :mod:`research_manager`; it has no broker, deployment, or trading authority.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Sequence

from research_manager import ResearchManager, sha256_file


CASCADE = (
    "replay", "target_population", "phase_1", "phase_2",
    "discovery", "freeze", "holdout", "report",
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
    if phase in {"phase_1", "phase_2", "discovery", "freeze", "holdout"}:
        if report.get("lookahead_protection") is not True:
            raise MethodologyViolation(f"{phase} lacks explicit lookahead_protection=true")
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

    def run(self, instrument: str, stages: Sequence[Stage], *, resume: bool = True,
            through: str = "report") -> Dict[str, Any]:
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
        env.update({"TRADING_ENVIRONMENT": "SIMULATION", "AUTO_TRADE": "false"})
        completed: List[str] = []
        for name in requested:
            stage = by_name[name]
            self._validate_stage(stage)
            current = self.manager.load()["assets"][instrument]["phases"][name]
            if resume and current.get("status") == "COMPLETED" and stage.artifact.is_file():
                expected = current.get("artifact_sha256")
                if expected and expected == sha256_file(stage.artifact):
                    completed.append(name)
                    continue
            self.manager.update_phase(instrument, name, "RUNNING")
            try:
                result = self.runner(
                    list(stage.command), check=False, text=True,
                    capture_output=True, env=env,
                )
                if result.returncode != 0:
                    raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "stage failed")
                if not stage.artifact.is_file():
                    raise RuntimeError(f"Expected artifact was not created: {stage.artifact}")
                report = validate_research_artifact(stage.artifact, name)
                if str(report.get("status") or "OK").upper() in {"FAILED", "BLOCKED", "REVIEW_REQUIRED"}:
                    raise MethodologyViolation(f"Artifact status is {report['status']}")
                self.manager.update_phase(
                    instrument, name, "COMPLETED", artifact=str(stage.artifact),
                    details={"command": list(stage.command)},
                )
                completed.append(name)
            except BaseException as exc:
                self.manager.update_phase(
                    instrument, name, "BLOCKED" if isinstance(exc, MethodologyViolation) else "FAILED",
                    details={"error": str(exc)},
                )
                raise
        return {"instrument": instrument, "status": "COMPLETED", "phases": completed}


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
    parser.add_argument("--through", choices=CASCADE, default="report")
    args = parser.parse_args()
    optimizer = CascadeOptimizer(ResearchManager(args.state))
    result = optimizer.run(
        args.instrument, load_manifest(args.manifest),
        resume=not args.no_resume, through=args.through,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

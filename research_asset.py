#!/usr/bin/env python3
"""Master resumable command for one strictly offline research asset cycle."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from cascade_optimizer import CASCADE, CascadeOptimizer, Stage
from research_manager import ResearchManager, sha256_file


SUPPORTED_ASSETS = ("GBP_USD", "EUR_USD", "USD_JPY", "AUD_USD", "USD_CAD")


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=str(repo), check=True, text=True, capture_output=True)
    return result.stdout.strip()


def _stage(name: str, artifact: Path, command: list[str]) -> Stage:
    return Stage(name=name, artifact=artifact, command=tuple(command))


def build_stages(
    *, repo: Path, python: str, instrument: str, cache: Path, workspace: Path,
    start: str, end: str, warmup: int, horizon: int, variant: str,
    embargo: int, discovery_fraction: float, validation_fraction: float,
    min_resolved: int, code_sha: str, state: Path,
) -> list[Stage]:
    pipeline = str(repo / "research_pipeline.py")
    artifacts = {name: workspace / f"{index:02d}_{name}.json" for index, name in enumerate(CASCADE, 1)}
    common_split = [
        "--horizon", str(horizon), "--discovery-fraction", str(discovery_fraction),
        "--validation-fraction", str(validation_fraction), "--embargo-minutes", str(embargo),
    ]
    stages = [
        _stage("data_integrity", artifacts["data_integrity"], [
            python, pipeline, "data-integrity", "--cache", str(cache), "--instrument", instrument,
            "--start", start, "--end", end, "--warmup", str(warmup), "--horizon", str(horizon),
            "--repo", str(repo), "--data-sha256", sha256_file(cache), "--code-sha", code_sha,
            "--output", str(artifacts["data_integrity"]),
        ]),
        _stage("replay", artifacts["replay"], [
            python, str(repo / "run_historical_replay.py"), "--instrument", instrument,
            "--start", start, "--end", end, "--cache", str(cache), "--output", str(artifacts["replay"]),
            "--horizon", str(horizon), "--embargo-minutes", str(embargo), "--session-scales", "1.0",
            "--integrity-artifact", str(artifacts["data_integrity"]),
        ]),
        _stage("target_population", artifacts["target_population"], [
            python, pipeline, "target-population", "--replay", str(artifacts["replay"]),
            "--variant", variant, "--output", str(artifacts["target_population"]),
        ]),
        _stage("phase_1", artifacts["phase_1"], [
            python, pipeline, "phase1", "--input", str(artifacts["target_population"]),
            "--discovery-only", *common_split, "--output", str(artifacts["phase_1"]),
        ]),
        _stage("phase_2", artifacts["phase_2"], [
            python, pipeline, "phase2", "--input", str(artifacts["target_population"]),
            "--phase1", str(artifacts["phase_1"]), *common_split, "--output", str(artifacts["phase_2"]),
        ]),
        _stage("discovery", artifacts["discovery"], [
            python, pipeline, "discovery", "--input", str(artifacts["target_population"]),
            "--phase2", str(artifacts["phase_2"]), "--min-resolved", str(min_resolved),
            "--output", str(artifacts["discovery"]),
        ]),
        _stage("discovery_repeat", artifacts["discovery_repeat"], [
            python, pipeline, "discovery", "--input", str(artifacts["target_population"]),
            "--phase2", str(artifacts["phase_2"]), "--min-resolved", str(min_resolved),
            "--output", str(artifacts["discovery_repeat"]),
        ]),
        _stage("determinism", artifacts["determinism"], [
            python, pipeline, "determinism", "--first", str(artifacts["discovery"]),
            "--second", str(artifacts["discovery_repeat"]), "--output", str(artifacts["determinism"]),
        ]),
        _stage("freeze", artifacts["freeze"], [
            python, pipeline, "freeze", "--discovery", str(artifacts["discovery"]),
            "--output", str(artifacts["freeze"]),
        ]),
        _stage("holdout", artifacts["holdout"], [
            python, pipeline, "holdout", "--input", str(artifacts["target_population"]),
            "--phase2", str(artifacts["phase_2"]), "--freeze", str(artifacts["freeze"]),
            "--output", str(artifacts["holdout"]),
        ]),
        _stage("audit", artifacts["audit"], [
            python, pipeline, "audit", "--repo", str(repo), "--base-commit", code_sha,
            "--new-tests", "test_research_phase2.py,test_research_integrity.py,test_research_governance.py,test_research_audit.py,test_research_asset.py",
            "--regression-tests", "test_research_manager.py,test_research_pipeline.py,test_cascade_optimizer.py,test_replay_validation.py,test_historical_replay.py",
            "--output", str(artifacts["audit"]),
        ]),
        _stage("report", artifacts["report"], [
            python, pipeline, "report", "--integrity", str(artifacts["data_integrity"]),
            "--phase1", str(artifacts["phase_1"]), "--phase2", str(artifacts["phase_2"]),
            "--discovery", str(artifacts["discovery"]), "--freeze", str(artifacts["freeze"]),
            "--holdout", str(artifacts["holdout"]), "--determinism", str(artifacts["determinism"]),
            "--audit", str(artifacts["audit"]), "--output", str(artifacts["report"]),
        ]),
        _stage("pre_audit", artifacts["pre_audit"], [
            python, pipeline, "pre-audit", "--report", str(artifacts["report"]),
            "--output", str(artifacts["pre_audit"]),
        ]),
        _stage("prompts", artifacts["prompts"], [
            python, pipeline, "prompts", "--report", str(artifacts["report"]),
            "--pre-audit", str(artifacts["pre_audit"]), "--state", str(state),
            "--output", str(artifacts["prompts"]),
        ]),
    ]
    return stages


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one resumable BotsTrader offline research asset cycle")
    parser.add_argument("instrument", choices=SUPPORTED_ASSETS)
    parser.add_argument("--cache", required=True);parser.add_argument("--start", required=True);parser.add_argument("--end", required=True)
    parser.add_argument("--warmup", type=int, default=10);parser.add_argument("--horizon", type=int, default=240)
    parser.add_argument("--variant", default="V331_BASELINE");parser.add_argument("--embargo-minutes", type=int, default=30)
    parser.add_argument("--discovery-fraction", type=float, default=.60);parser.add_argument("--validation-fraction", type=float, default=.20)
    parser.add_argument("--min-resolved", type=int, default=10);parser.add_argument("--workspace")
    parser.add_argument("--through", choices=CASCADE, default="prompts");parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent
    if _git(repo, "branch", "--show-current") != "main":raise SystemExit("research_asset requires branch main")
    dirty = _git(repo, "status", "--short")
    if dirty:raise SystemExit("research_asset refuses a dirty working tree")
    code_sha = _git(repo, "rev-parse", "HEAD")
    cache = Path(args.cache).resolve()
    if not cache.is_file():raise SystemExit(f"cache not found: {cache}")
    default_root = repo.parent / "bots_research_runs"
    workspace = Path(args.workspace).resolve() if args.workspace else default_root / f"{args.instrument}_{args.start[:10]}_{args.end[:10]}"
    try:
        workspace.relative_to(repo)
    except ValueError:
        pass
    else:
        raise SystemExit("research workspace must be outside the git repository")
    workspace.mkdir(parents=True, exist_ok=True)
    state = workspace / "research_state.json"
    manager = ResearchManager(state)
    manager.register_asset(
        args.instrument, code_sha=code_sha, start=args.start, end=args.end,
        warmup_days=args.warmup, horizon_minutes=args.horizon,
        data_sha256=sha256_file(cache),
    )
    stages = build_stages(
        repo=repo, python=args.python, instrument=args.instrument, cache=cache,
        workspace=workspace, start=args.start, end=args.end, warmup=args.warmup,
        horizon=args.horizon, variant=args.variant, embargo=args.embargo_minutes,
        discovery_fraction=args.discovery_fraction, validation_fraction=args.validation_fraction,
        min_resolved=args.min_resolved, code_sha=code_sha, state=state,
    )
    manifest = workspace / "cascade_manifest.json"
    manifest.write_text(json.dumps({
        "schema_version": 2, "instrument": args.instrument, "code_sha": code_sha,
        "stages": [{"name": stage.name, "artifact": str(stage.artifact), "command": list(stage.command)} for stage in stages],
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.prepare_only:
        result = {"status": "READY", "instrument": args.instrument, "workspace": str(workspace), "manifest": str(manifest), "state": str(state)}
    else:
        result = CascadeOptimizer(manager).run(args.instrument, stages, through=args.through)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

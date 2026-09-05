#!/usr/bin/env python3
"""Governed user-controlled modes for Automation V3.

This module adds a review-before-holdout boundary without changing the certified
research population, incumbent comparison, lookback, or outcome semantics.
LIVE authority is never accepted here.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import unicodedata
from pathlib import Path
from typing import Any, Mapping, Sequence

from autonomous_asset_optimizer import (
    AutonomousAssetOptimizer,
    GovernedReleaseController,
    PRACTICE_OANDA_URL,
    SUPPORTED_INSTRUMENTS,
    V3Ledger,
    canonical_sha256,
    load_json,
    run_with_phase1_autonomous_continuation,
    sha256_file,
    utc_now,
    write_json,
)
from automation_v3_candidate_mapping import CandidateNotDeployable, compile_and_write_release_plan
from research_phase2 import evaluate_holdout, freeze_candidate

FULL_AUTO_TO_PAPER = "FULL_AUTO_TO_PAPER"
REVIEW_BEFORE_HOLDOUT_DEPLOY = "REVIEW_BEFORE_HOLDOUT_DEPLOY"
SELECT_REVIEW_CANDIDATE = "SELECT_REVIEW_CANDIDATE"
KEEP_INCUMBENT = "KEEP_INCUMBENT"
MODES = {FULL_AUTO_TO_PAPER, REVIEW_BEFORE_HOLDOUT_DEPLOY, SELECT_REVIEW_CANDIDATE, KEEP_INCUMBENT}
REVIEW_READY = "REVIEW_SHORTLIST_READY"
STALE_REVIEW_SHORTLIST = "STALE_REVIEW_SHORTLIST"
INCUMBENT_RETAINS_CONTROL = "INCUMBENT_RETAINS_CONTROL"
REVIEW_PAUSE_SENTINEL = "REVIEW_PRE_HOLDOUT_PAUSE"
SHORTLIST_POLICY = "PRE_HOLDOUT_TOP4_INCUMBENT_RELATIVE_V1"
METHODOLOGY_CONTRACT = "REVIEW_BEFORE_HOLDOUT_DEPLOY_V1"
MAX_SHORTLIST = 4
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _plain(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower().strip()


def normalize_instrument(value: str | None) -> str | None:
    if value is None:
        return None
    symbol = str(value).strip().upper().replace("/", "_").replace("-", "_")
    return symbol if symbol in SUPPORTED_INSTRUMENTS else None


def parse_natural_language_intent(text: str) -> dict[str, Any]:
    """Translate the supported Spanish command surface into a safe intent object.

    Selection phrases intentionally do not infer an instrument from conversation
    memory. The caller must bind the rank to an active backend shortlist artifact.
    """
    raw = text or ""
    plain = _plain(raw)
    instrument = None
    for symbol in SUPPORTED_INSTRUMENTS:
        slash = symbol.replace("_", "/").lower()
        if symbol.lower() in plain or slash in plain:
            instrument = symbol
            break
    if "quedate con la actual" in plain or "manten la actual" in plain or "mantener la actual" in plain:
        return {"instrument": instrument, "mode": KEEP_INCUMBENT}
    match = re.search(r"\b(?:despliega|desplegar|usa|usar)\s+(?:la\s+)?(?:estrategia\s+)?([1-4])\b", plain)
    if match:
        return {"instrument": instrument, "mode": SELECT_REVIEW_CANDIDATE, "rank": int(match.group(1))}
    optimize = "automatiza" in plain or "optimiza" in plain
    if optimize:
        review_markers = (
            "antes de desplegar", "no despliegues", "no desplegar", "dame los resultados",
            "ensename las mejores", "muestrame las mejores", "estrategias primero",
        )
        mode = REVIEW_BEFORE_HOLDOUT_DEPLOY if any(marker in plain for marker in review_markers) else FULL_AUTO_TO_PAPER
        return {"instrument": instrument, "mode": mode}
    raise ValueError("UNSUPPORTED_AUTOMATION_V3_INTENT")


def validate_structured_request(payload: Mapping[str, Any]) -> dict[str, Any]:
    mode = str(payload.get("mode") or "").upper()
    if mode not in MODES:
        raise ValueError("unsupported Automation V3 mode")
    instrument = normalize_instrument(payload.get("instrument"))
    if instrument is None:
        raise ValueError("unsupported or missing instrument")
    result: dict[str, Any] = {"instrument": instrument, "mode": mode}
    shortlist_sha = payload.get("shortlist_sha256")
    rank = payload.get("rank")
    if mode == SELECT_REVIEW_CANDIDATE:
        if not isinstance(shortlist_sha, str) or not SHA256_RE.fullmatch(shortlist_sha.lower()):
            raise ValueError("SELECT_REVIEW_CANDIDATE requires shortlist_sha256")
        try:
            rank = int(rank)
        except (TypeError, ValueError):
            raise ValueError("SELECT_REVIEW_CANDIDATE requires rank") from None
        if rank not in range(1, MAX_SHORTLIST + 1):
            raise ValueError("rank must be between 1 and 4")
        result.update(shortlist_sha256=shortlist_sha.lower(), rank=rank)
    elif mode == KEEP_INCUMBENT and shortlist_sha:
        if not isinstance(shortlist_sha, str) or not SHA256_RE.fullmatch(shortlist_sha.lower()):
            raise ValueError("invalid shortlist_sha256")
        result["shortlist_sha256"] = shortlist_sha.lower()
    return result


def _methodology_identity(phase2: Mapping[str, Any], discovery: Mapping[str, Any]) -> dict[str, Any]:
    incumbent = discovery.get("incumbent") or {}
    definition = incumbent.get("definition") or {}
    return {
        "contract": METHODOLOGY_CONTRACT,
        "shortlist_policy": SHORTLIST_POLICY,
        "selection_protocol": phase2.get("selection_protocol"),
        "partition_config": phase2.get("partition_config"),
        "lookahead_protection": phase2.get("lookahead_protection") is True,
        "incumbent_methodology_identity": definition.get("methodology_identity"),
        "holdout_policy": "ONE_EXACT_SELECTED_CANDIDATE__OPEN_ONCE__NO_FALLBACK",
        "ranking_policy": "EXPECTANCY_DELTA__PROFIT_FACTOR_DELTA__VALIDATION_EXPECTANCY__RESOLVED__CANDIDATE_ID",
        "production_authority": False,
    }


def _candidate_definition_sha(record: Mapping[str, Any]) -> str:
    candidate = record.get("candidate") or {}
    if not isinstance(candidate, Mapping) or not candidate.get("id"):
        raise ValueError("candidate definition missing")
    return canonical_sha256(candidate)


def _candidate_sort_key(record: Mapping[str, Any]) -> tuple[float, float, float, int, str]:
    comparison = (record.get("incumbent_comparison") or {}).get("validation") or {}
    selected = (record.get("validation") or {}).get("selected") or {}
    return (
        float(comparison.get("expectancy_delta_vs_incumbent") or -999.0),
        float(comparison.get("profit_factor_delta_vs_incumbent") or -999.0),
        float(selected.get("expectancy_r") or -999.0),
        int(selected.get("resolved_binary") or 0),
        str((record.get("candidate") or {}).get("id") or ""),
    )


def _eligible_records(discovery: Mapping[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    proposed = discovery.get("proposed_frozen_candidate")
    if isinstance(proposed, Mapping) and (proposed.get("decision_gate") or {}).get("decision") == "FREEZE_ELIGIBLE":
        records.append(dict(proposed))
    for item in discovery.get("ranked_candidates") or []:
        if not isinstance(item, Mapping):
            continue
        if (item.get("decision_gate") or {}).get("decision") == "FREEZE_ELIGIBLE":
            records.append(dict(item))
    unique: dict[str, dict[str, Any]] = {}
    for record in records:
        digest = _candidate_definition_sha(record)
        unique.setdefault(digest, record)
    return sorted(unique.values(), key=_candidate_sort_key, reverse=True)


def _short_candidate(record: Mapping[str, Any], rank: int) -> dict[str, Any]:
    candidate = dict(record.get("candidate") or {})
    validation = record.get("validation") or {}
    comparison = (record.get("incumbent_comparison") or {}).get("validation") or {}
    gate = record.get("decision_gate") or {}
    return {
        "rank": rank,
        "candidate_id": candidate.get("id"),
        "candidate_definition": candidate,
        "candidate_definition_sha256": canonical_sha256(candidate),
        "challenger_metrics": validation.get("selected"),
        "incumbent_metrics": comparison.get("incumbent"),
        "relative_improvement": comparison,
        "win_retention": validation.get("win_retention"),
        "loss_rejection": validation.get("loss_rejection"),
        "losses_rejected": validation.get("losses_rejected"),
        "temporal_stability": record.get("temporal_stability"),
        "directional_stability": record.get("directional_stability"),
        "sensitivity": record.get("sensitivity"),
        "walk_forward_stability": record.get("walk_forward_stability"),
        "overfitting_status": record.get("overfitting_risk"),
        "paper_candidate_classification": gate.get("paper_candidate_classification"),
        "pre_holdout_eligible": gate.get("decision") == "FREEZE_ELIGIBLE",
        "reason": gate.get("diagnostic_state") or "CHALLENGER_DEPLOYABLE",
        "production_authority": False,
    }


def build_review_shortlist(workspace: str | Path, *, run_id: str) -> tuple[Path, dict[str, Any]]:
    workspace = Path(workspace)
    target_path = workspace / "03_target_population.json"
    phase1_path = workspace / "04_phase_1.json"
    phase2_path = workspace / "05_phase_2.json"
    discovery_path = workspace / "06_discovery.json"
    determinism_path = workspace / "08_determinism.json"
    for path in (target_path, phase1_path, phase2_path, discovery_path, determinism_path):
        if not path.is_file():
            raise ValueError(f"review evidence missing: {path.name}")
    target = load_json(target_path)
    phase2 = load_json(phase2_path)
    discovery = load_json(discovery_path)
    determinism = load_json(determinism_path)
    if discovery.get("holdout_opened") is not False:
        raise ValueError("review requires unopened holdout")
    if str(determinism.get("status") or "").upper() not in {"PASS", "OK"}:
        raise ValueError("determinism did not pass")
    records = _eligible_records(discovery)[:MAX_SHORTLIST]
    if not records:
        raise ValueError("NO_PRE_HOLDOUT_ELIGIBLE_CHALLENGER")
    dataset_identity = phase2.get("dataset_identity")
    incumbent = discovery.get("incumbent") or {}
    if not isinstance(dataset_identity, Mapping) or not incumbent.get("incumbent_definition_sha256"):
        raise ValueError("review identity evidence missing")
    code_sha = str(dataset_identity.get("code_sha") or "")
    if len(code_sha) != 40:
        raise ValueError("review code identity missing")
    methodology = _methodology_identity(phase2, discovery)
    payload: dict[str, Any] = {
        "schema_version": 1,
        "run_id": str(run_id),
        "instrument": str(discovery.get("instrument") or "").upper(),
        "mode": REVIEW_BEFORE_HOLDOUT_DEPLOY,
        "code_sha": code_sha,
        "dataset_identity": dict(dataset_identity),
        "dataset_identity_sha256": canonical_sha256(dataset_identity),
        "incumbent_definition_sha256": incumbent.get("incumbent_definition_sha256"),
        "target_population_sha256": sha256_file(target_path),
        "phase1_artifact_sha256": sha256_file(phase1_path),
        "phase2_artifact_sha256": sha256_file(phase2_path),
        "discovery_artifact_sha256": sha256_file(discovery_path),
        "determinism_artifact_sha256": sha256_file(determinism_path),
        "methodology_identity": methodology,
        "methodology_identity_sha256": canonical_sha256(methodology),
        "holdout_opened": False,
        "selection_required_before_holdout": True,
        "candidates": [_short_candidate(record, rank) for rank, record in enumerate(records, 1)],
        "production_authority": False,
    }
    payload["shortlist_sha256"] = canonical_sha256(payload)
    path = workspace / f"review_shortlist_{payload['shortlist_sha256']}.json"
    if path.exists():
        existing = load_json(path)
        if existing != payload:
            raise ValueError("immutable shortlist collision")
        return path, existing
    write_json(path, payload)
    return path, payload


def _verify_shortlist_hash(shortlist: Mapping[str, Any]) -> None:
    stored = shortlist.get("shortlist_sha256")
    material = dict(shortlist)
    material.pop("shortlist_sha256", None)
    if not isinstance(stored, str) or canonical_sha256(material) != stored:
        raise ValueError("STALE_REVIEW_SHORTLIST: shortlist hash mismatch")


def verify_review_shortlist(shortlist_path: str | Path, *, current_code_sha: str) -> dict[str, Any]:
    path = Path(shortlist_path)
    shortlist = load_json(path)
    _verify_shortlist_hash(shortlist)
    if shortlist.get("production_authority") is not False or shortlist.get("mode") != REVIEW_BEFORE_HOLDOUT_DEPLOY:
        raise ValueError("STALE_REVIEW_SHORTLIST: invalid shortlist authority/mode")
    if shortlist.get("code_sha") != current_code_sha:
        raise ValueError("STALE_REVIEW_SHORTLIST: code identity changed")
    workspace = path.parent
    bindings = {
        "target_population_sha256": workspace / "03_target_population.json",
        "phase1_artifact_sha256": workspace / "04_phase_1.json",
        "phase2_artifact_sha256": workspace / "05_phase_2.json",
        "discovery_artifact_sha256": workspace / "06_discovery.json",
        "determinism_artifact_sha256": workspace / "08_determinism.json",
    }
    for key, artifact in bindings.items():
        if not artifact.is_file() or shortlist.get(key) != sha256_file(artifact):
            raise ValueError(f"STALE_REVIEW_SHORTLIST: {key} changed")
    phase2 = load_json(bindings["phase2_artifact_sha256"])
    discovery = load_json(bindings["discovery_artifact_sha256"])
    dataset_identity = phase2.get("dataset_identity") or {}
    if canonical_sha256(dataset_identity) != shortlist.get("dataset_identity_sha256"):
        raise ValueError("STALE_REVIEW_SHORTLIST: dataset identity changed")
    incumbent_sha = (discovery.get("incumbent") or {}).get("incumbent_definition_sha256")
    if incumbent_sha != shortlist.get("incumbent_definition_sha256"):
        raise ValueError("STALE_REVIEW_SHORTLIST: incumbent identity changed")
    methodology = _methodology_identity(phase2, discovery)
    if canonical_sha256(methodology) != shortlist.get("methodology_identity_sha256"):
        raise ValueError("STALE_REVIEW_SHORTLIST: methodology identity changed")
    return shortlist


def _find_shortlist(root: Path, instrument: str, shortlist_sha256: str | None) -> Path:
    matches: list[Path] = []
    for path in sorted((root / instrument / "autonomous_v3").glob("lookback_*/review_shortlist_*.json")):
        try:
            payload = load_json(path)
        except Exception:
            continue
        if payload.get("instrument") != instrument:
            continue
        if shortlist_sha256 and payload.get("shortlist_sha256") != shortlist_sha256:
            continue
        matches.append(path)
    if not matches:
        raise ValueError("STALE_REVIEW_SHORTLIST: shortlist not found")
    if shortlist_sha256:
        return matches[-1]
    active = []
    for path in matches:
        state = path.parent / f"review_selection_{load_json(path).get('shortlist_sha256')}.json"
        if not state.exists() or load_json(state).get("status") not in {"CANCELLED", "PAPER_DEPLOYED", "SELECTED_CHALLENGER_FAILED_HOLDOUT"}:
            active.append(path)
    if len(active) != 1:
        raise ValueError("review shortlist selection is ambiguous; shortlist_sha256 required")
    return active[0]


def _source_candidate(discovery: Mapping[str, Any], candidate_id: str, definition_sha: str) -> dict[str, Any]:
    candidates: list[Mapping[str, Any]] = []
    proposed = discovery.get("proposed_frozen_candidate")
    if isinstance(proposed, Mapping):
        candidates.append(proposed)
    candidates.extend(item for item in discovery.get("ranked_candidates") or [] if isinstance(item, Mapping))
    for item in candidates:
        definition = item.get("candidate") or {}
        if definition.get("id") == candidate_id and canonical_sha256(definition) == definition_sha:
            if (item.get("decision_gate") or {}).get("decision") != "FREEZE_ELIGIBLE":
                raise ValueError("selected candidate is not pre-holdout eligible")
            return dict(item)
    raise ValueError("STALE_REVIEW_SHORTLIST: candidate evidence changed")


def resolve_review_candidate(shortlist_path: str | Path, *, current_code_sha: str, rank: int) -> tuple[dict[str, Any], dict[str, Any]]:
    shortlist = verify_review_shortlist(shortlist_path, current_code_sha=current_code_sha)
    choices = [item for item in shortlist.get("candidates") or [] if isinstance(item, Mapping) and item.get("rank") == rank]
    if len(choices) != 1:
        raise ValueError("rank is not present in immutable shortlist")
    choice = dict(choices[0])
    if choice.get("pre_holdout_eligible") is not True:
        raise ValueError("selected candidate is not pre-holdout eligible")
    discovery = load_json(Path(shortlist_path).parent / "06_discovery.json")
    source = _source_candidate(discovery, str(choice.get("candidate_id") or ""), str(choice.get("candidate_definition_sha256") or ""))
    return shortlist, source


def _selection_state_path(shortlist_path: Path, shortlist_sha: str) -> Path:
    return shortlist_path.parent / f"review_selection_{shortlist_sha}.json"


def _bind_selection(state_path: Path, binding: Mapping[str, Any]) -> dict[str, Any]:
    if state_path.exists():
        existing = load_json(state_path)
        identity_keys = ("instrument", "shortlist_sha256", "rank", "candidate_id", "candidate_definition_sha256")
        if any(existing.get(key) != binding.get(key) for key in identity_keys):
            raise ValueError("HOLDOUT_ALREADY_BOUND_TO_DIFFERENT_CANDIDATE")
        return existing
    payload = dict(binding)
    payload.update(status="SELECTED_PRE_HOLDOUT", holdout_opened=False, production_authority=False, selected_at=utc_now())
    write_json(state_path, payload)
    return payload


def _copy_required_review_evidence(workspace: Path, release_dir: Path) -> None:
    release_dir.mkdir(parents=True, exist_ok=True)
    for name in ("01_data_integrity.json", "03_target_population.json", "04_phase_1.json", "05_phase_2.json", "08_determinism.json"):
        source = workspace / name
        if not source.is_file():
            raise ValueError(f"review evidence missing: {name}")
        target = release_dir / name
        if target.exists() and sha256_file(target) != sha256_file(source):
            raise ValueError(f"immutable release evidence collision: {name}")
        if not target.exists():
            shutil.copy2(source, target)


def _run_fixed(command: Sequence[str], *, cwd: Path) -> None:
    result = subprocess.run(list(command), cwd=str(cwd), text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError((result.stderr or result.stdout or "governed stage failed")[-4000:])


def _complete_audit_report(repo: Path, release_dir: Path, code_sha: str) -> None:
    pipeline = str(repo / "research_pipeline.py")
    python = sys.executable
    audit = release_dir / "11_audit.json"
    report = release_dir / "12_report.json"
    pre = release_dir / "13_pre_audit.json"
    if not audit.exists():
        _run_fixed([
            python, pipeline, "audit", "--repo", str(repo), "--base-commit", code_sha,
            "--new-tests", "test_research_phase2.py,test_research_integrity.py,test_research_governance.py,test_research_audit.py,test_research_asset.py,test_automation_v3_modes.py",
            "--regression-tests", "test_research_manager.py,test_research_pipeline.py,test_cascade_optimizer.py,test_replay_validation.py,test_historical_replay.py,test_automation_v3_incumbent_challenger.py",
            "--output", str(audit),
        ], cwd=repo)
    if not report.exists():
        _run_fixed([
            python, pipeline, "report", "--integrity", str(release_dir / "01_data_integrity.json"),
            "--phase1", str(release_dir / "04_phase_1.json"), "--phase2", str(release_dir / "05_phase_2.json"),
            "--discovery", str(release_dir / "06_discovery.json"), "--freeze", str(release_dir / "09_freeze.json"),
            "--holdout", str(release_dir / "10_holdout.json"), "--determinism", str(release_dir / "08_determinism.json"),
            "--audit", str(audit), "--output", str(report),
        ], cwd=repo)
    if not pre.exists():
        _run_fixed([python, pipeline, "pre-audit", "--report", str(report), "--output", str(pre)], cwd=repo)


class ReviewBeforeHoldoutOptimizer(AutonomousAssetOptimizer):
    """Run certified research only through determinism, then create a shortlist."""

    def _run_cascade(self, cascade: Any, manager: Any, ledger: V3Ledger, instrument: str, stages: Any, artifact_dir: Path) -> Any:
        run_with_phase1_autonomous_continuation(
            cascade=cascade, manager=manager, ledger=ledger, instrument=instrument,
            stages=stages, through="determinism", phase1_artifact=artifact_dir / "04_phase_1.json",
            load_json=load_json, utc_now=utc_now,
        )
        raise RuntimeError(REVIEW_PAUSE_SENTINEL)

    def _terminal(self, ledger: V3Ledger, instrument: str, status: str, reason: str, **extra: Any) -> dict[str, Any]:
        if status == "METHODOLOGY_BLOCKED" and reason == REVIEW_PAUSE_SENTINEL:
            run = ledger.run(instrument)
            attempts = run.get("lookback_attempts") or []
            if not attempts:
                return super()._terminal(ledger, instrument, "NO_VALID_CANDIDATE", "review workspace unavailable", **extra)
            months = int(attempts[-1]["months"])
            code_sha = str(run.get("code_sha") or "")
            workspace = Path(run.get("workspace") or "") / f"lookback_{months:02d}m_{code_sha[:12]}"
            try:
                path, shortlist = build_review_shortlist(workspace, run_id=os.getenv("GITHUB_RUN_ID", "local-review-run"))
            except ValueError as exc:
                return super()._terminal(ledger, instrument, "NO_VALID_CANDIDATE", str(exc), lookback_months=months, **extra)
            return ledger.mutate(
                instrument, status=REVIEW_READY, final_outcome=REVIEW_READY,
                stop_reason="awaiting exact pre-holdout candidate selection",
                lookback_months=months, review_shortlist=str(path),
                review_shortlist_sha256=shortlist["shortlist_sha256"],
                review_candidates=shortlist["candidates"], mode=REVIEW_BEFORE_HOLDOUT_DEPLOY,
            )
        return super()._terminal(ledger, instrument, status, reason, **extra)

    def optimize(self, instrument: str) -> dict[str, Any]:
        symbol = instrument.upper()
        root = Path(os.getenv("BOTS_RESEARCH_ROOT", str(self.repo.parent / "Botstrader_Research"))) / symbol / "autonomous_v3"
        ledger = V3Ledger(root / "automation_v3_state.json")
        run = ledger.run(symbol)
        current_sha = self.code_sha_provider() if self.code_sha_provider else self._git("rev-parse", "HEAD")
        if run.get("status") == REVIEW_READY and run.get("code_sha") == current_sha:
            shortlist_path = Path(str(run.get("review_shortlist") or ""))
            if shortlist_path.is_file():
                try:
                    verify_review_shortlist(shortlist_path, current_code_sha=current_sha)
                    return run
                except ValueError:
                    pass
        return super().optimize(symbol)


def _git_sha(repo: Path) -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(repo), check=True, text=True, capture_output=True).stdout.strip()


def _ledger_for(root: Path, instrument: str) -> V3Ledger:
    return V3Ledger(root / instrument / "autonomous_v3" / "automation_v3_state.json")


def select_review_candidate(*, repo: Path, root: Path, instrument: str, shortlist_sha256: str, rank: int, release: Any | None = None) -> dict[str, Any]:
    code_sha = _git_sha(repo)
    shortlist_path = _find_shortlist(root, instrument, shortlist_sha256)
    shortlist, source_record = resolve_review_candidate(shortlist_path, current_code_sha=code_sha, rank=rank)
    choice = next(item for item in shortlist["candidates"] if item["rank"] == rank)
    binding = {
        "instrument": instrument,
        "shortlist_sha256": shortlist["shortlist_sha256"],
        "rank": rank,
        "candidate_id": choice["candidate_id"],
        "candidate_definition_sha256": choice["candidate_definition_sha256"],
    }
    state_path = _selection_state_path(shortlist_path, shortlist["shortlist_sha256"])
    state = _bind_selection(state_path, binding)
    workspace = shortlist_path.parent
    release_dir = workspace / f"review_release_{shortlist['shortlist_sha256'][:12]}"
    _copy_required_review_evidence(workspace, release_dir)
    selected_discovery_path = release_dir / "06_discovery.json"
    if not selected_discovery_path.exists():
        discovery = load_json(workspace / "06_discovery.json")
        discovery["proposed_frozen_candidate"] = source_record
        discovery["review_selection"] = {**binding, "selection_before_holdout": True, "production_authority": False}
        write_json(selected_discovery_path, discovery)
    else:
        selected = load_json(selected_discovery_path)
        if canonical_sha256((selected.get("proposed_frozen_candidate") or {}).get("candidate") or {}) != choice["candidate_definition_sha256"]:
            raise ValueError("immutable selected discovery collision")
    freeze_path = release_dir / "09_freeze.json"
    if not freeze_path.exists():
        write_json(freeze_path, freeze_candidate(str(selected_discovery_path), freeze_path))
    frozen = load_json(freeze_path)
    if frozen.get("candidate_definition_sha256") != choice["candidate_definition_sha256"]:
        raise ValueError("frozen candidate differs from selection")
    holdout_path = release_dir / "10_holdout.json"
    if state.get("holdout_opened") is True:
        if not holdout_path.is_file():
            raise ValueError("HOLDOUT_STATE_UNCERTAIN: refusing to reopen holdout")
        holdout = load_json(holdout_path)
        if holdout.get("candidate_definition_sha256") != choice["candidate_definition_sha256"]:
            raise ValueError("holdout candidate identity mismatch")
    else:
        state = {**state, "status": "HOLDOUT_OPENING", "holdout_opened": True, "holdout_opened_at": utc_now(), "freeze_sha256": sha256_file(freeze_path)}
        write_json(state_path, state)
        holdout = evaluate_holdout(str(release_dir / "03_target_population.json"), str(release_dir / "05_phase_2.json"), str(selected_discovery_path), str(freeze_path))
        write_json(holdout_path, holdout)
        state = {**state, "status": "HOLDOUT_COMPLETE", "holdout_sha256": sha256_file(holdout_path), "holdout_status": holdout.get("status")}
        write_json(state_path, state)
    ledger = _ledger_for(root, instrument)
    if holdout.get("status") != "PASS":
        state = {**state, "status": "SELECTED_CHALLENGER_FAILED_HOLDOUT", "deployment": "NOT_ATTEMPTED", "production_authority": False}
        write_json(state_path, state)
        return ledger.mutate(instrument, status="SELECTED_CHALLENGER_FAILED_HOLDOUT", final_outcome="SELECTED_CHALLENGER_FAILED_HOLDOUT", stop_reason="selected immutable challenger failed final holdout; no fallback candidate tested", review_selection=state)
    _complete_audit_report(repo, release_dir, code_sha)
    audit = load_json(release_dir / "11_audit.json")
    pre = load_json(release_dir / "13_pre_audit.json")
    if audit.get("status") != "PASS" or pre.get("verdict") not in {"ACCEPT", "ACCEPT WITH LIMITATIONS"}:
        state = {**state, "status": "AUDIT_BLOCKED", "deployment": "NOT_ATTEMPTED", "production_authority": False}
        write_json(state_path, state)
        return ledger.mutate(instrument, status="AUDIT_BLOCKED", final_outcome="AUDIT_BLOCKED", stop_reason="selected challenger failed governed audit", review_selection=state)
    candidate = next((item for item in holdout.get("candidate_ranking") or [] if isinstance(item, Mapping) and item.get("status") == "RESEARCH_CANDIDATE"), None)
    if not candidate:
        raise ValueError("holdout PASS lacked deployable candidate record")
    plan = release_dir / "paper_release_plan.json"
    write_json(plan, {"instrument": instrument, "candidate": candidate, "source_code_sha": code_sha, "production_authority": False})
    controller = release or GovernedReleaseController(repo)
    if isinstance(controller, GovernedReleaseController):
        try:
            compiled = compile_and_write_release_plan(repo=repo, plan_path=plan, instrument=instrument, source_code_sha=code_sha)
        except CandidateNotDeployable as exc:
            state = {**state, "status": "CANDIDATE_NOT_DEPLOYABLE", "reason": str(exc), "production_authority": False}
            write_json(state_path, state)
            return ledger.mutate(instrument, status="CANDIDATE_NOT_DEPLOYABLE", final_outcome="CANDIDATE_NOT_DEPLOYABLE", stop_reason=str(exc), review_selection=state)
        ledger.mutate(instrument, status="PAPER_DEPLOYABLE_CANDIDATE", paper_release_plan=str(plan), paper_release_plan_sha256=canonical_sha256(compiled), review_selection=state)
    release_result = controller.prepare_test_merge(plan=plan, base_sha=code_sha, instrument=instrument)
    if release_result.get("status") != "PASS":
        state = {**state, "status": "DEPLOYMENT_FAILURE", "release": release_result, "production_authority": False}
        write_json(state_path, state)
        return ledger.mutate(instrument, status="DEPLOYMENT_FAILURE", final_outcome="DEPLOYMENT_FAILURE", stop_reason=str(release_result.get("reason")), release=release_result, review_selection=state)
    deployment = controller.deploy_paper(expected_sha=release_result["merged_main_sha"], environment={"TRADING_ENVIRONMENT": "PAPER", "PRIMARY_OANDA_ENV": "practice", "OANDA": PRACTICE_OANDA_URL})
    if deployment.get("status") != "PAPER_DEPLOYED":
        state = {**state, "status": "DEPLOYMENT_FAILURE", "deployment": deployment, "production_authority": False}
        write_json(state_path, state)
        return ledger.mutate(instrument, status="DEPLOYMENT_FAILURE", final_outcome="DEPLOYMENT_FAILURE", stop_reason=str(deployment.get("reason")), deployment=deployment, review_selection=state)
    state = {**state, "status": "PAPER_DEPLOYED", "deployment": deployment, "production_authority": False}
    write_json(state_path, state)
    return ledger.mutate(instrument, status="PAPER_DEPLOYED", final_outcome="PAPER_DEPLOYED", stop_reason="selected immutable challenger survived one final holdout and PAPER deployment verification", deployment=deployment, review_selection=state)


def keep_incumbent(*, repo: Path, root: Path, instrument: str, shortlist_sha256: str | None = None) -> dict[str, Any]:
    code_sha = _git_sha(repo)
    shortlist_path = _find_shortlist(root, instrument, shortlist_sha256)
    shortlist = verify_review_shortlist(shortlist_path, current_code_sha=code_sha)
    state_path = _selection_state_path(shortlist_path, shortlist["shortlist_sha256"])
    if state_path.exists() and load_json(state_path).get("holdout_opened") is True:
        raise ValueError("review selection already opened holdout")
    state = {
        "instrument": instrument, "shortlist_sha256": shortlist["shortlist_sha256"],
        "status": "CANCELLED", "decision": KEEP_INCUMBENT,
        "reason": "user retained incumbent before final holdout; no challenger deployment attempted",
        "holdout_opened": False, "production_authority": False, "cancelled_at": utc_now(),
    }
    write_json(state_path, state)
    return _ledger_for(root, instrument).mutate(
        instrument, status=INCUMBENT_RETAINS_CONTROL, final_outcome=INCUMBENT_RETAINS_CONTROL,
        stop_reason="review cancelled; incumbent retains control without a claim of absolute quality",
        review_selection=state, mode=KEEP_INCUMBENT,
    )


def _assert_remote_paper_boundary(mode: str) -> None:
    if os.getenv("BOTS_V3_PRODUCTION_AUTHORITY", "").strip().lower() != "false":
        raise RuntimeError("BOTS_V3_PRODUCTION_AUTHORITY must be false")
    if os.getenv("TRADING_ENVIRONMENT", "").strip().upper() != "PAPER":
        raise RuntimeError("TRADING_ENVIRONMENT must be PAPER")
    if os.getenv("PRIMARY_OANDA_ENV", "").strip().lower() != "practice":
        raise RuntimeError("PRIMARY_OANDA_ENV must be practice")
    if mode in {REVIEW_BEFORE_HOLDOUT_DEPLOY, SELECT_REVIEW_CANDIDATE} and not os.getenv("OANDA_TOKEN", "").strip():
        raise RuntimeError("DATA_SOURCE_UNAVAILABLE: OANDA_TOKEN missing")
    if mode == SELECT_REVIEW_CANDIDATE:
        if not os.getenv("GH_TOKEN", "").strip() or not os.getenv("RAILWAY_TOKEN", "").strip():
            raise RuntimeError("deployment credentials unavailable")


def execute_request(request: Mapping[str, Any], *, repo: Path | None = None, root: Path | None = None) -> dict[str, Any]:
    safe = validate_structured_request(request)
    repo = (repo or Path(__file__).resolve().parent).resolve()
    root = (root or Path(os.getenv("BOTS_RESEARCH_ROOT", str(repo.parent / "Botstrader_Research")))).resolve()
    mode = safe["mode"]
    instrument = safe["instrument"]
    _assert_remote_paper_boundary(mode)
    if mode == FULL_AUTO_TO_PAPER:
        from automation_v3_remote_worker import run_worker
        return run_worker(instrument)
    if mode == REVIEW_BEFORE_HOLDOUT_DEPLOY:
        result = ReviewBeforeHoldoutOptimizer(repo).optimize(instrument)
    elif mode == SELECT_REVIEW_CANDIDATE:
        result = select_review_candidate(repo=repo, root=root, instrument=instrument, shortlist_sha256=safe["shortlist_sha256"], rank=safe["rank"])
    else:
        result = keep_incumbent(repo=repo, root=root, instrument=instrument, shortlist_sha256=safe.get("shortlist_sha256"))
    status_path = Path(os.getenv("BOTS_V3_REMOTE_STATUS_PATH", str(root / instrument / "autonomous_v3" / "remote_status.json")))
    status = {
        "run_id": os.getenv("GITHUB_RUN_ID", "local-mode-run"),
        "instrument": instrument, "mode": mode,
        "terminal_state": result.get("status"),
        "shortlist_sha256": result.get("review_shortlist_sha256") or safe.get("shortlist_sha256"),
        "review_candidates": result.get("review_candidates"),
        "paper_deployment_status": (result.get("deployment") or {}).get("status") if isinstance(result.get("deployment"), Mapping) else None,
        "production_authority": False,
    }
    write_json(status_path, status)
    print("REMOTE_STATUS " + json.dumps(status, sort_keys=True), flush=True)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Governed Automation V3 mode controller; PAPER maximum authority")
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--mode", required=True, choices=sorted(MODES))
    parser.add_argument("--shortlist-sha256")
    parser.add_argument("--rank", type=int)
    args = parser.parse_args()
    payload = {"instrument": args.instrument, "mode": args.mode}
    if args.shortlist_sha256:
        payload["shortlist_sha256"] = args.shortlist_sha256
    if args.rank is not None:
        payload["rank"] = args.rank
    try:
        result = execute_request(payload)
    except Exception as exc:
        result = {"status": "MODE_REQUEST_FAILED", "reason": str(exc), "production_authority": False}
        print(json.dumps(result, indent=2, sort_keys=True))
        raise SystemExit(2)
    print(json.dumps(result, indent=2, sort_keys=True))
    accepted = {
        "PAPER_DEPLOYED", REVIEW_READY, INCUMBENT_RETAINS_CONTROL,
        "NO_VALID_CANDIDATE", "INSUFFICIENT_EVIDENCE", "DATA_COVERAGE_INSUFFICIENT",
        "CANDIDATE_NOT_DEPLOYABLE", "SELECTED_CHALLENGER_FAILED_HOLDOUT",
    }
    raise SystemExit(0 if result.get("status") in accepted else 2)


if __name__ == "__main__":
    main()

"""Deterministic convergence/checkpoint helpers for Automation V3.

This module is orchestration-only. It does not change research thresholds,
candidate generation, outcome semantics, or broker behavior.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_identity(command: Sequence[str]) -> str:
    return canonical_sha256([str(part) for part in command])


def candidate_set_sha256(report: Mapping[str, Any]) -> str:
    candidates = []
    for item in report.get("ranked_candidates") or []:
        if isinstance(item, Mapping):
            candidate = item.get("candidate") if isinstance(item.get("candidate"), Mapping) else item
            candidates.append(str(candidate.get("id") or candidate.get("candidate_id") or ""))
    if not candidates:
        space = report.get("candidate_space") or {}
        candidates = [f"generated:{int(space.get('generated') or 0)}"]
    return canonical_sha256(sorted(candidates))


def discovery_checkpoint(
    *,
    artifact_path: str | Path,
    report: Mapping[str, Any],
    command: Sequence[str],
) -> dict[str, Any]:
    """Bind a completed negative discovery result to all available identities."""
    dataset = report.get("dataset_identity") or {}
    return {
        "checkpoint": "DISCOVERY_EVALUATED",
        "stage": "discovery",
        "status": str(report.get("status") or "UNKNOWN"),
        "artifact_sha256": file_sha256(artifact_path),
        "instrument": str(report.get("instrument") or "").upper(),
        "dataset_identity_sha256": canonical_sha256(dataset),
        "target_population_sha256": report.get("input_sha256"),
        "phase2_sha256": report.get("phase2_sha256"),
        "candidate_set_sha256": candidate_set_sha256(report),
        "methodology_identity": command_identity(command),
        "production_authority": False,
    }


def checkpoint_matches(
    checkpoint: Mapping[str, Any],
    *,
    artifact_path: str | Path,
    report: Mapping[str, Any],
    command: Sequence[str],
) -> bool:
    if checkpoint.get("checkpoint") != "DISCOVERY_EVALUATED":
        return False
    current = discovery_checkpoint(artifact_path=artifact_path, report=report, command=command)
    return all(checkpoint.get(key) == current.get(key) for key in current)


def compact_pre_gate(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    keys = (
        "generated_candidates",
        "fail_resolved_lt_min",
        "fail_win_retention_lt_060",
        "fail_losses_rejected_lt_2",
        "fail_expectancy_delta_le_0",
        "pass_all_pre_gate",
        "dominant_failure",
        "recommended_action",
        "production_authority",
    )
    return {key: value.get(key) for key in keys if key in value}


def compact_discovery_diagnostic(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    keys = (
        "generated_candidates",
        "discovery_rows",
        "evaluated_after_discovery_gate",
        "freeze_eligible",
        "max_resolved",
        "min_resolved_required",
        "fail_support_count",
        "fail_win_retention_count",
        "fail_loss_rejection_count",
        "fail_expectancy_count",
        "fail_overfitting_count",
        "fail_instability_count",
        "dominant_failure",
        "recommended_action",
        "production_authority",
    )
    return {key: value.get(key) for key in keys if key in value}

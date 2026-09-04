"""Deterministic Automation V3 candidate -> managed PAPER code-change compiler."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from managed_strategy_rules import APPROVED_FEATURES, APPROVED_OPERATORS, SUPPORTED_INSTRUMENTS

MANAGED_PATH = "managed_strategy_rules.py"
ACCEPTED_AUDIT_VERDICTS = {"ACCEPT", "ACCEPT WITH LIMITATIONS"}
MAX_COMPOSITE_RULES = 3


class CandidateNotDeployable(ValueError):
    pass


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_object(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise CandidateNotDeployable(f"artifact is not an object: {Path(path).name}")
    return value


def _artifact(workspace: Path, name: str) -> Path:
    matches = sorted(workspace.glob(f"*_{name}.json"))
    if len(matches) != 1:
        raise CandidateNotDeployable(f"required artifact missing or ambiguous: {name}")
    return matches[0]


def _exact_assignment(path: Path, instrument: str) -> str:
    prefix = f'MANAGED_RULES_JSON["{instrument}"] = '
    matches = [line for line in path.read_text(encoding="utf-8").splitlines(keepends=True) if line.startswith(prefix)]
    if len(matches) != 1:
        raise CandidateNotDeployable("managed instrument assignment is not unique")
    return matches[0]


def _canonical_rules(definition: Mapping[str, Any]) -> list[dict[str, Any]]:
    rules = definition.get("rules")
    if not isinstance(rules, list) or not rules or len(rules) > MAX_COMPOSITE_RULES:
        raise CandidateNotDeployable("candidate rule form is unsupported")
    result = []
    for raw in rules:
        if not isinstance(raw, Mapping):
            raise CandidateNotDeployable("candidate rule is not declarative")
        feature = str(raw.get("feature") or "")
        operator = str(raw.get("operator") or "")
        if feature not in APPROVED_FEATURES:
            raise CandidateNotDeployable(f"candidate feature has no approved mapping: {feature}")
        if operator not in APPROVED_OPERATORS:
            raise CandidateNotDeployable(f"candidate operator is unsupported: {operator}")
        threshold = raw.get("threshold")
        if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
            raise CandidateNotDeployable("candidate threshold must be numeric")
        result.append({"feature": feature, "operator": operator, "threshold": threshold})
    return result


def _managed_payload(candidate_id: str, definition_sha: str, rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"candidate_definition_sha256": definition_sha, "candidate_id": candidate_id, **rule}
        for rule in rules
    ]


def _assignment(instrument: str, payload: list[dict[str, Any]]) -> str:
    inner = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f'MANAGED_RULES_JSON["{instrument}"] = {json.dumps(inner, ensure_ascii=True)}\n'


def compile_release_plan(
    *, repo: str | Path, plan_path: str | Path, instrument: str, source_code_sha: str,
) -> dict[str, Any]:
    """Compile an existing research release request into one bounded managed config edit.

    The compiler never derives code from prose.  It consumes only the immutable
    frozen candidate definition and refuses any unsupported feature/operator/form.
    """
    repo = Path(repo)
    plan_path = Path(plan_path)
    workspace = plan_path.parent
    symbol = str(instrument or "").upper()
    if symbol not in SUPPORTED_INSTRUMENTS:
        raise CandidateNotDeployable("unsupported instrument")
    if not source_code_sha or len(source_code_sha) != 40:
        raise CandidateNotDeployable("source code SHA missing")

    request = load_object(plan_path)
    freeze_path = _artifact(workspace, "freeze")
    holdout_path = _artifact(workspace, "holdout")
    phase2_path = _artifact(workspace, "phase_2")
    discovery_path = _artifact(workspace, "discovery")
    target_path = _artifact(workspace, "target_population")
    audit_path = _artifact(workspace, "audit")
    pre_audit_path = _artifact(workspace, "pre_audit")
    freeze = load_object(freeze_path)
    holdout = load_object(holdout_path)
    phase2 = load_object(phase2_path)
    discovery = load_object(discovery_path)
    target = load_object(target_path)
    pre_audit = load_object(pre_audit_path)

    if request.get("production_authority") is not False:
        raise CandidateNotDeployable("production authority must be false")
    if str(request.get("instrument") or "").upper() != symbol:
        raise CandidateNotDeployable("release request instrument mismatch")
    if request.get("source_code_sha") != source_code_sha:
        raise CandidateNotDeployable("stale release source code SHA")
    if freeze.get("freeze_status") != "FROZEN_IMMUTABLE" or freeze.get("immutable") is not True or freeze.get("holdout_opened") is not False:
        raise CandidateNotDeployable("candidate is not immutably frozen")
    if str(freeze.get("instrument") or "").upper() != symbol or str(holdout.get("instrument") or "").upper() != symbol:
        raise CandidateNotDeployable("cross-asset binding mismatch")

    definition = freeze.get("candidate_definition")
    if not isinstance(definition, Mapping):
        raise CandidateNotDeployable("frozen candidate definition missing")
    definition = dict(definition)
    definition_sha = canonical_sha256(definition)
    if freeze.get("candidate_definition_sha256") != definition_sha:
        raise CandidateNotDeployable("stale candidate definition hash")
    candidate_id = str(freeze.get("candidate_id") or "")
    if not candidate_id or definition.get("id") != candidate_id:
        raise CandidateNotDeployable("candidate identity mismatch")
    request_candidate = request.get("candidate") or {}
    if not isinstance(request_candidate, Mapping) or request_candidate.get("candidate_id") != candidate_id:
        raise CandidateNotDeployable("release candidate does not match freeze")

    freeze_sha = sha256_file(freeze_path)
    holdout_sha = sha256_file(holdout_path)
    phase2_sha = sha256_file(phase2_path)
    discovery_sha = sha256_file(discovery_path)
    target_sha = sha256_file(target_path)
    audit_sha = sha256_file(audit_path)
    pre_audit_sha = sha256_file(pre_audit_path)
    if holdout.get("status") != "PASS" or holdout.get("decision") != "RESEARCH_CANDIDATE_SURVIVED_HOLDOUT":
        raise CandidateNotDeployable("holdout did not approve candidate")
    if holdout.get("retuning_after_holdout") is not False or holdout.get("holdout_opened_once") is not True:
        raise CandidateNotDeployable("post-holdout retune/opening invariant failed")
    if holdout.get("candidate_definition_sha256") != definition_sha or holdout.get("freeze_sha256") != freeze_sha:
        raise CandidateNotDeployable("holdout/freeze candidate binding mismatch")
    if holdout.get("phase2_sha256") != phase2_sha or freeze.get("phase2_sha256") != phase2_sha:
        raise CandidateNotDeployable("phase2 binding mismatch")
    if freeze.get("discovery_sha256") != discovery_sha:
        raise CandidateNotDeployable("discovery/freeze binding mismatch")
    if holdout.get("input_sha256") != target_sha or freeze.get("target_population_sha256") != target_sha:
        raise CandidateNotDeployable("target population binding mismatch")
    if pre_audit.get("verdict") not in ACCEPTED_AUDIT_VERDICTS:
        raise CandidateNotDeployable("pre-audit rejected candidate")

    identities = [target.get("dataset_identity"), phase2.get("dataset_identity"), discovery.get("dataset_identity"), freeze.get("dataset_identity")]
    if any(not isinstance(value, Mapping) for value in identities):
        raise CandidateNotDeployable("dataset identity missing")
    identity_hashes = {canonical_sha256(value) for value in identities}
    if len(identity_hashes) != 1:
        raise CandidateNotDeployable("dataset identity mismatch")
    dataset_identity = dict(identities[0])
    if dataset_identity.get("code_sha") != source_code_sha or freeze.get("code_sha") != source_code_sha:
        raise CandidateNotDeployable("stale source code SHA binding")

    rules = _canonical_rules(definition)
    managed_path = repo / MANAGED_PATH
    if not managed_path.is_file():
        raise CandidateNotDeployable("approved managed change surface missing")
    old_text = _exact_assignment(managed_path, symbol)
    payload = _managed_payload(candidate_id, definition_sha, rules)
    new_text = _assignment(symbol, payload)
    if any(marker in new_text.upper() for marker in ("OANDA_TOKEN", "API_KEY", "SECRET_KEY", "PASSWORD", "API-FXTRADE.OANDA.COM")):
        raise CandidateNotDeployable("generated managed text contains prohibited marker")

    evidence_binding = {
        "instrument": symbol,
        "source_code_sha": source_code_sha,
        "dataset_identity": dataset_identity,
        "dataset_identity_sha256": canonical_sha256(dataset_identity),
        "candidate_id": candidate_id,
        "candidate_definition_sha256": definition_sha,
        "target_population_sha256": target_sha,
        "phase2_sha256": phase2_sha,
        "discovery_sha256": discovery_sha,
        "freeze_sha256": freeze_sha,
        "holdout_sha256": holdout_sha,
        "audit_sha256": audit_sha,
        "pre_audit_sha256": pre_audit_sha,
        "audit_verdict": pre_audit.get("verdict"),
        "retuning_after_holdout": False,
    }
    change = {
        "path": MANAGED_PATH,
        "operation": "replace_text",
        "expected_file_sha256": sha256_file(managed_path),
        "old_text": old_text,
        "new_text": new_text,
        "expected_occurrences": 1,
        "reason": "Apply exact frozen Automation V3 research veto for one PAPER instrument",
        "evidence_binding": evidence_binding,
    }
    compiled = {
        "schema_version": 2,
        "status": "PAPER_DEPLOYABLE_CANDIDATE",
        "instrument": symbol,
        "candidate_id": candidate_id,
        "candidate_definition_sha256": definition_sha,
        "source_code_sha": source_code_sha,
        "dataset_identity": dataset_identity,
        "production_authority": False,
        "freeze_artifact": {"path": freeze_path.name, "sha256": freeze_sha},
        "holdout_artifact": {"path": holdout_path.name, "sha256": holdout_sha},
        "audit_artifact": {"path": audit_path.name, "sha256": audit_sha},
        "pre_audit": {"path": pre_audit_path.name, "sha256": pre_audit_sha, "verdict": pre_audit.get("verdict")},
        "code_changes": [change],
    }
    compiled["plan_binding_sha256"] = canonical_sha256(compiled)
    return compiled


def compile_and_write_release_plan(**kwargs: Any) -> dict[str, Any]:
    plan_path = Path(kwargs["plan_path"])
    compiled = compile_release_plan(**kwargs)
    plan_path.write_text(json.dumps(compiled, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return compiled

"""Persistent, offline-only state for multi-asset research.

The manager is intentionally independent from broker and execution modules.  It
stores provenance and lifecycle state, but never decides that an experiment is
valid merely because a process completed successfully.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional


SCHEMA_VERSION = 3
PHASES = (
    "data_integrity", "replay", "target_population", "phase_1", "phase_2",
    "discovery", "discovery_repeat", "determinism", "freeze", "holdout",
    "audit", "report", "pre_audit", "prompts",
)
TERMINAL_OUTCOMES = (
    "WIN", "LOSS", "TIMEOUT", "AMBIGUOUS", "PENDING",
    "NOT_HISTORICALLY_RECONSTRUCTABLE",
)
PHASE_STATUSES = ("PENDING", "RUNNING", "COMPLETED", "FAILED", "BLOCKED")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: os.PathLike[str] | str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _empty_state() -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "updated_at": utc_now(),
        "assets": {},
    }


def _identity(provenance: Mapping[str, Any]) -> str:
    material = json.dumps(dict(provenance), sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(material.encode()).hexdigest()


def _migrate_state(state: Dict[str, Any]) -> Dict[str, Any]:
    version = state.get("schema_version")
    if version not in (1, 2, SCHEMA_VERSION):
        raise ValueError("Unsupported research state schema")
    for asset in (state.get("assets") or {}).values():
        phases = asset.setdefault("phases", {})
        for phase in PHASES:
            phases.setdefault(phase, {"status": "PENDING"})
        provenance = asset.setdefault("provenance", {})
        asset.setdefault("dataset_identity", _identity(provenance))
        dataset_identity = asset["dataset_identity"]
        for collection in ("candidates", "frozen_candidates", "audits"):
            for record in asset.setdefault(collection, []):
                record.setdefault("dataset_identity", dataset_identity)
                record.setdefault("active", record.get("dataset_identity") == dataset_identity)
        asset.setdefault("frozen_candidates", [])
        asset.setdefault("holdout_ledger", [])
        asset.setdefault("open_risks", asset.pop("risks", []))
        asset.setdefault("limitations", [])
        asset.setdefault("audit_verdict", "NOT TESTED")
        asset.setdefault("independent_audit_verdict", "NOT TESTED")
        asset.setdefault("forward_status", "BLOCKED_HUMAN_REVIEW_REQUIRED")
        asset.setdefault("lifecycle", {
            "last_valid_stage": None,
            "next_allowed_stage": "data_integrity",
            "status": "REGISTERED",
        })
    state["schema_version"] = SCHEMA_VERSION
    return state


class ResearchManager:
    """Atomic JSON-backed research ledger.

    A single writer replaces the state file atomically. This keeps the ledger
    usable from a phone where a process may be interrupted between commands.
    """

    def __init__(self, path: os.PathLike[str] | str = "research_state.json") -> None:
        self.path = Path(path)

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return _empty_state()
        with self.path.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
        if not isinstance(state.get("assets"), dict):
            raise ValueError("Invalid research state: assets must be an object")
        return _migrate_state(state)

    def save(self, state: Mapping[str, Any]) -> None:
        payload = _migrate_state(dict(state))
        payload["updated_at"] = utc_now()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=str(self.path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise

    def register_asset(
        self, instrument: str, *, code_sha: str, start: str, end: str,
        warmup_days: int, horizon_minutes: int, data_sha256: Optional[str] = None,
    ) -> Dict[str, Any]:
        instrument = instrument.strip().upper()
        if not instrument:
            raise ValueError("instrument is required")
        state = self.load()
        existing = state["assets"].get(instrument, {})
        provenance = {
            "code_sha": code_sha,
            "data_sha256": data_sha256,
            "window": {"start": start, "end": end},
            "warmup_days": int(warmup_days),
            "horizon_minutes": int(horizon_minutes),
        }
        identity = _identity(provenance)
        identity_changed = bool(existing) and existing.get("dataset_identity") != identity
        phases = existing.get("phases") or {phase: {"status": "PENDING"} for phase in PHASES}
        candidates = existing.get("candidates") or []
        frozen_candidates = existing.get("frozen_candidates") or []
        audits = existing.get("audits") or []
        if identity_changed:
            phases = {phase: {"status": "PENDING", "reason": "INPUT_IDENTITY_CHANGED"} for phase in PHASES}
            for record in [*candidates, *frozen_candidates, *audits]:
                record["active"] = False
                record["invalidated_reason"] = "INPUT_IDENTITY_CHANGED"
        asset = {
            "instrument": instrument,
            "provenance": provenance,
            "dataset_identity": identity,
            "phases": phases,
            "candidates": candidates,
            "frozen_candidates": frozen_candidates,
            "audits": audits,
            "holdout_ledger": existing.get("holdout_ledger") or [],
            "open_risks": existing.get("open_risks") or existing.get("risks") or [],
            "limitations": existing.get("limitations") or [],
            "audit_verdict": "NOT TESTED" if identity_changed else existing.get("audit_verdict") or "NOT TESTED",
            "independent_audit_verdict": "NOT TESTED" if identity_changed else existing.get("independent_audit_verdict") or "NOT TESTED",
            "forward_status": "BLOCKED_INPUT_IDENTITY_CHANGED" if identity_changed else existing.get("forward_status") or "BLOCKED_HUMAN_REVIEW_REQUIRED",
            "lifecycle": ({
                "last_valid_stage": None,
                "next_allowed_stage": "data_integrity",
                "status": "INPUT_CHANGED",
            } if identity_changed else existing.get("lifecycle") or {
                "last_valid_stage": None,
                "next_allowed_stage": "data_integrity",
                "status": "REGISTERED",
            }),
            "created_at": existing.get("created_at") or utc_now(),
            "updated_at": utc_now(),
        }
        if not identity_changed and existing.get("forward_candidate"):
            asset["forward_candidate"] = existing["forward_candidate"]
        state["assets"][instrument] = asset
        self.save(state)
        return asset

    def update_phase(
        self, instrument: str, phase: str, status: str, *,
        artifact: Optional[str] = None, details: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        phase, status = phase.lower(), status.upper()
        if phase not in PHASES:
            raise ValueError(f"Unknown phase: {phase}")
        if status not in PHASE_STATUSES:
            raise ValueError(f"Unknown phase status: {status}")
        state = self.load()
        asset = state["assets"].get(instrument.upper())
        if asset is None:
            raise KeyError(f"Asset not registered: {instrument}")
        record: Dict[str, Any] = {"status": status, "updated_at": utc_now()}
        if artifact:
            artifact_path = Path(artifact)
            record["artifact"] = str(artifact_path)
            if artifact_path.is_file():
                record["artifact_sha256"] = sha256_file(artifact_path)
        if details:
            record["details"] = dict(details)
        asset["phases"][phase] = record
        if status == "COMPLETED":
            index = PHASES.index(phase)
            asset["lifecycle"] = {
                "last_valid_stage": phase,
                "next_allowed_stage": PHASES[index + 1] if index + 1 < len(PHASES) else "HUMAN_IA1_REVIEW",
                "status": "IN_PROGRESS" if index + 1 < len(PHASES) else "AUTOMATION_COMPLETE_HUMAN_REVIEW_REQUIRED",
            }
        elif status in {"FAILED", "BLOCKED"}:
            asset["lifecycle"] = {
                "last_valid_stage": asset.get("lifecycle", {}).get("last_valid_stage"),
                "next_allowed_stage": phase,
                "status": status,
            }
        asset["updated_at"] = utc_now()
        self.save(state)
        return record

    def add_candidate(self, instrument: str, candidate: Mapping[str, Any]) -> Dict[str, Any]:
        state = self.load()
        asset = state["assets"].get(instrument.upper())
        if asset is None:
            raise KeyError(f"Asset not registered: {instrument}")
        record = dict(candidate)
        supplied_identity = record.get("dataset_identity")
        if supplied_identity not in (None, asset["dataset_identity"]):
            raise ValueError("Candidate dataset identity does not match current asset identity")
        record["dataset_identity"] = asset["dataset_identity"]
        record["active"] = True
        record.setdefault("recorded_at", utc_now())
        record.setdefault("frozen", False)
        asset["candidates"].append(record)
        self.save(state)
        return record

    def add_audit(self, instrument: str, audit: Mapping[str, Any]) -> Dict[str, Any]:
        state = self.load()
        asset = state["assets"].get(instrument.upper())
        if asset is None:
            raise KeyError(f"Asset not registered: {instrument}")
        record = dict(audit)
        supplied_identity = record.get("dataset_identity")
        if supplied_identity not in (None, asset["dataset_identity"]):
            raise ValueError("Audit dataset identity does not match current asset identity")
        record["dataset_identity"] = asset["dataset_identity"]
        record["active"] = True
        record.setdefault("recorded_at", utc_now())
        asset["audits"].append(record)
        asset["audit_verdict"] = record.get("verdict") or record.get("status") or "NOT TESTED"
        self.save(state)
        return record

    def freeze_candidate(self, instrument: str, candidate: Mapping[str, Any]) -> Dict[str, Any]:
        """Persist an immutable hypothesis definition; conflicting reuse is blocked."""
        state = self.load()
        asset = state["assets"].get(instrument.upper())
        if asset is None:
            raise KeyError(f"Asset not registered: {instrument}")
        definition = dict(candidate)
        supplied_identity = definition.get("dataset_identity")
        if supplied_identity not in (None, asset["dataset_identity"]):
            raise ValueError("Frozen candidate dataset identity does not match current asset identity")
        definition["dataset_identity"] = asset["dataset_identity"]
        definition["active"] = True
        candidate_id = str(definition.get("candidate_id") or definition.get("id") or "")
        definition_sha = str(definition.get("candidate_definition_sha256") or "")
        if not candidate_id or not definition_sha:
            raise ValueError("Frozen candidate requires candidate_id and candidate_definition_sha256")
        current_holdout = next((
            item for item in asset["holdout_ledger"]
            if item.get("dataset_identity") == asset["dataset_identity"]
        ), None)
        if current_holdout and current_holdout.get("candidate_definition_sha256") != definition_sha:
            raise ValueError("Holdout already exposed; a different candidate cannot be frozen for this dataset")
        for existing in asset["frozen_candidates"]:
            if existing.get("dataset_identity") != asset["dataset_identity"] or existing.get("active") is not True:
                continue
            if existing.get("candidate_id") == candidate_id:
                if existing.get("candidate_definition_sha256") != definition_sha:
                    raise ValueError("Frozen candidate id cannot be silently retuned")
                return existing
        definition.setdefault("frozen", True)
        definition.setdefault("frozen_at", utc_now())
        asset["frozen_candidates"].append(definition)
        self.save(state)
        return definition

    def begin_holdout(
        self, instrument: str, *, candidate_definition_sha256: str,
        freeze_sha256: str, read_only_reproduction: bool = False,
    ) -> Dict[str, Any]:
        """Persist holdout exposure before evaluation and reject candidate reuse."""
        state = self.load()
        asset = state["assets"].get(instrument.upper())
        if asset is None:
            raise KeyError(f"Asset not registered: {instrument}")
        frozen = next((
            item for item in asset["frozen_candidates"]
            if item.get("dataset_identity") == asset["dataset_identity"]
            and item.get("active") is True
            and item.get("candidate_definition_sha256") == candidate_definition_sha256
        ), None)
        if frozen is None:
            raise ValueError("Holdout requires a frozen candidate for the current dataset identity")
        if not freeze_sha256:
            raise ValueError("Holdout requires freeze artifact identity")
        existing = next((
            item for item in asset["holdout_ledger"]
            if item.get("dataset_identity") == asset["dataset_identity"]
        ), None)
        if existing:
            same_freeze = (
                existing.get("candidate_definition_sha256") == candidate_definition_sha256
                and existing.get("freeze_sha256") == freeze_sha256
            )
            if not same_freeze:
                raise ValueError("Holdout already exposed to a different candidate for this dataset")
            if not read_only_reproduction:
                raise ValueError("Holdout already consumed; reproduction must be explicitly read-only")
            if existing.get("status") != "OPENED":
                raise ValueError("Incomplete holdout exposure cannot be reopened")
            return {**existing, "mode": "READ_ONLY_REPRODUCTION"}
        if read_only_reproduction:
            raise ValueError("Read-only reproduction requires a prior completed holdout opening")
        record = {
            "dataset_identity": asset["dataset_identity"],
            "candidate_definition_sha256": candidate_definition_sha256,
            "freeze_sha256": freeze_sha256,
            "opened_at": utc_now(),
            "status": "OPENING",
            "mode": "FIRST_OPEN",
        }
        asset["holdout_ledger"].append(record)
        self.save(state)
        return record

    def complete_holdout(
        self, instrument: str, *, candidate_definition_sha256: str,
        freeze_sha256: str, holdout_artifact: str,
        read_only_reproduction: bool = False,
    ) -> Dict[str, Any]:
        state = self.load()
        asset = state["assets"].get(instrument.upper())
        if asset is None:
            raise KeyError(f"Asset not registered: {instrument}")
        artifact_path = Path(holdout_artifact)
        if not artifact_path.is_file():
            raise ValueError("Holdout artifact is missing")
        artifact_sha = sha256_file(artifact_path)
        record = next((
            item for item in asset["holdout_ledger"]
            if item.get("dataset_identity") == asset["dataset_identity"]
            and item.get("candidate_definition_sha256") == candidate_definition_sha256
            and item.get("freeze_sha256") == freeze_sha256
        ), None)
        if record is None:
            raise ValueError("Holdout opening was not persisted before evaluation")
        if read_only_reproduction:
            if record.get("status") != "OPENED" or record.get("holdout_artifact_sha256") != artifact_sha:
                raise ValueError("Read-only reproduction differs from the original holdout evidence")
            return {**record, "mode": "READ_ONLY_REPRODUCTION"}
        if record.get("status") != "OPENING":
            raise ValueError("Holdout ledger is not awaiting first-open evidence")
        record.update({
            "status": "OPENED",
            "holdout_artifact": str(artifact_path),
            "holdout_artifact_sha256": artifact_sha,
            "completed_at": utc_now(),
        })
        self.save(state)
        return record

    def record_risks_and_limitations(
        self, instrument: str, *, risks: list[Mapping[str, Any]], limitations: list[str],
        forward_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        state = self.load()
        asset = state["assets"].get(instrument.upper())
        if asset is None:
            raise KeyError(f"Asset not registered: {instrument}")
        asset["open_risks"] = [dict(item) for item in risks]
        asset["limitations"] = list(limitations)
        if forward_status:
            asset["forward_status"] = forward_status
        self.save(state)
        return asset

    def approve_forward_candidate(
        self, instrument: str, candidate_id: str, *, ia1_approved: bool,
        ia2_verdict: str,
    ) -> Dict[str, Any]:
        """Manual research promotion only; grants no production/trading authority."""
        if not ia1_approved:
            raise ValueError("IA #1 approval is required")
        ia2_verdict = ia2_verdict.upper()
        if ia2_verdict not in {"ACCEPT", "ACCEPT WITH LIMITATIONS"}:
            raise ValueError("Independent IA #2 audit must accept the candidate")
        state = self.load()
        asset = state["assets"].get(instrument.upper())
        if asset is None:
            raise KeyError(f"Asset not registered: {instrument}")
        frozen = next((
            item for item in asset["frozen_candidates"]
            if item.get("candidate_id") == candidate_id
            and item.get("dataset_identity") == asset["dataset_identity"]
            and item.get("active") is True
        ), None)
        if frozen is None:
            raise ValueError("Forward candidate must reference an active frozen candidate for the current dataset identity")
        accepted_audit = next((
            item for item in reversed(asset["audits"])
            if item.get("dataset_identity") == asset["dataset_identity"]
            and item.get("active") is True
            and item.get("verdict") in {"ACCEPT", "ACCEPT WITH LIMITATIONS"}
        ), None)
        if accepted_audit is None:
            raise ValueError("Automatic pre-audit did not accept the candidate")
        asset["independent_audit_verdict"] = ia2_verdict
        asset["forward_status"] = "FORWARD_CANDIDATE"
        asset["forward_candidate"] = {
            "candidate_id": candidate_id,
            "candidate_definition_sha256": frozen["candidate_definition_sha256"],
            "dataset_identity": asset["dataset_identity"],
            "status": "FORWARD_CANDIDATE",
            "ia1_approved": True,
            "ia2_verdict": ia2_verdict,
            "production_authority": False,
            "approved_at": utc_now(),
        }
        self.save(state)
        return asset["forward_candidate"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline BotsTrader research ledger")
    parser.add_argument("--state", default="research_state.json")
    sub = parser.add_subparsers(dest="command", required=True)
    register = sub.add_parser("register")
    register.add_argument("instrument")
    register.add_argument("--code-sha", required=True)
    register.add_argument("--start", required=True)
    register.add_argument("--end", required=True)
    register.add_argument("--warmup-days", type=int, default=10)
    register.add_argument("--horizon-minutes", type=int, default=240)
    register.add_argument("--data-sha256")
    phase = sub.add_parser("phase")
    phase.add_argument("instrument")
    phase.add_argument("phase", choices=PHASES)
    phase.add_argument("status", choices=PHASE_STATUSES)
    phase.add_argument("--artifact")
    forward = sub.add_parser("approve-forward")
    forward.add_argument("instrument");forward.add_argument("candidate_id")
    forward.add_argument("--ia1-approved",action="store_true")
    forward.add_argument("--ia2-verdict",choices=("ACCEPT","ACCEPT WITH LIMITATIONS","REJECT"),required=True)
    sub.add_parser("show").add_argument("instrument", nargs="?")
    args = parser.parse_args()
    manager = ResearchManager(args.state)
    if args.command == "register":
        result = manager.register_asset(
            args.instrument, code_sha=args.code_sha, start=args.start, end=args.end,
            warmup_days=args.warmup_days, horizon_minutes=args.horizon_minutes,
            data_sha256=args.data_sha256,
        )
    elif args.command == "phase":
        result = manager.update_phase(args.instrument, args.phase, args.status, artifact=args.artifact)
    elif args.command == "approve-forward":
        result = manager.approve_forward_candidate(
            args.instrument,args.candidate_id,ia1_approved=args.ia1_approved,
            ia2_verdict=args.ia2_verdict,
        )
    else:
        state = manager.load()
        result = state["assets"].get(args.instrument.upper()) if args.instrument else state
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

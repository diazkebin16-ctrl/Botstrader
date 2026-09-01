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


SCHEMA_VERSION = 1
PHASES = (
    "replay", "target_population", "phase_1", "phase_2",
    "discovery", "freeze", "holdout", "report", "audit",
)
TERMINAL_OUTCOMES = (
    "WIN", "LOSS", "TIMEOUT", "AMBIGUOUS",
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
        if state.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("Unsupported research state schema")
        if not isinstance(state.get("assets"), dict):
            raise ValueError("Invalid research state: assets must be an object")
        return state

    def save(self, state: Mapping[str, Any]) -> None:
        payload = dict(state)
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
        asset = {
            "instrument": instrument,
            "provenance": {
                "code_sha": code_sha,
                "data_sha256": data_sha256,
                "window": {"start": start, "end": end},
                "warmup_days": int(warmup_days),
                "horizon_minutes": int(horizon_minutes),
            },
            "phases": existing.get("phases") or {
                phase: {"status": "PENDING"} for phase in PHASES
            },
            "candidates": existing.get("candidates") or [],
            "audits": existing.get("audits") or [],
            "risks": existing.get("risks") or [],
            "created_at": existing.get("created_at") or utc_now(),
            "updated_at": utc_now(),
        }
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
        asset["updated_at"] = utc_now()
        self.save(state)
        return record

    def add_candidate(self, instrument: str, candidate: Mapping[str, Any]) -> Dict[str, Any]:
        state = self.load()
        asset = state["assets"].get(instrument.upper())
        if asset is None:
            raise KeyError(f"Asset not registered: {instrument}")
        record = dict(candidate)
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
        record.setdefault("recorded_at", utc_now())
        asset["audits"].append(record)
        self.save(state)
        return record


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
    else:
        state = manager.load()
        result = state["assets"].get(args.instrument.upper()) if args.instrument else state
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

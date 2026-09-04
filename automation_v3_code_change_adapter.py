#!/usr/bin/env python3
"""Declarative, fail-closed code-change adapter for Automation V3 PAPER candidates.

The adapter never interprets free-form research prose as code. It applies only
explicit replace_text operations embedded in the governed release plan and
binds every edit to the exact pre-edit file hash.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping

SUPPORTED_INSTRUMENTS = {"AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY", "USD_CAD"}
PROTECTED_FILES = {"server.py", "forward_experiment.py"}
BLOCKED_SUFFIXES = {".env", ".db", ".sqlite", ".zip", ".pyc"}
BLOCKED_MARKERS = (
    "api-fxtrade.oanda.com",
    "TRADING_ENVIRONMENT=PRODUCTION",
    "PRIMARY_OANDA_ENV=live",
    "OANDA_TOKEN=",
    "API_KEY=",
    "SECRET_KEY=",
    "PASSWORD=",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("release plan must be a JSON object")
    return value


def _safe_relative_path(repo: Path, raw: str) -> Path:
    rel = Path(raw)
    if rel.is_absolute() or ".." in rel.parts or not rel.parts:
        raise ValueError("unsafe change path")
    if rel.name in PROTECTED_FILES:
        raise ValueError("protected LIVE file cannot be changed")
    if rel.suffix.lower() in BLOCKED_SUFFIXES or "__pycache__" in rel.parts:
        raise ValueError("secret/transient file cannot be changed")
    target = (repo / rel).resolve()
    target.relative_to(repo.resolve())
    if not target.is_file():
        raise ValueError("replace_text target must already exist")
    return target


def apply_release_plan(repo: Path, plan_path: Path, *, base_sha: str, instrument: str) -> list[str]:
    if os.getenv("BOTS_V3_PRODUCTION_AUTHORITY", "").strip().lower() != "false":
        raise ValueError("production authority must be false")
    instrument = instrument.strip().upper()
    if instrument not in SUPPORTED_INSTRUMENTS:
        raise ValueError("unsupported instrument")
    plan = _load(plan_path)
    if str(plan.get("instrument") or "").upper() != instrument:
        raise ValueError("release plan instrument mismatch")
    if str(plan.get("source_code_sha") or "") != base_sha:
        raise ValueError("release plan base SHA mismatch")
    if plan.get("production_authority") is not False:
        raise ValueError("release plan does not explicitly deny production authority")

    changes = plan.get("code_changes")
    if not isinstance(changes, list) or not changes:
        raise ValueError("release plan contains no executable code_changes")

    changed: list[str] = []
    seen: set[str] = set()
    for item in changes:
        if not isinstance(item, dict) or item.get("operation") != "replace_text":
            raise ValueError("only replace_text changes are supported")
        raw_path = str(item.get("path") or "")
        if raw_path in seen:
            raise ValueError("duplicate change path")
        seen.add(raw_path)
        target = _safe_relative_path(repo, raw_path)
        expected = str(item.get("expected_sha256") or "")
        if len(expected) != 64 or _sha256(target) != expected:
            raise ValueError("pre-edit file hash mismatch")
        replacements = item.get("replacements")
        if not isinstance(replacements, list) or not replacements:
            raise ValueError("replace_text requires replacements")
        text = target.read_text(encoding="utf-8")
        for replacement in replacements:
            if not isinstance(replacement, dict):
                raise ValueError("malformed replacement")
            old = replacement.get("old")
            new = replacement.get("new")
            count = replacement.get("count", 1)
            if not isinstance(old, str) or not old or not isinstance(new, str):
                raise ValueError("replacement text must be explicit strings")
            if not isinstance(count, int) or count < 1:
                raise ValueError("replacement count must be a positive integer")
            if any(marker.upper() in new.upper() for marker in BLOCKED_MARKERS):
                raise ValueError("LIVE/secret marker rejected")
            if text.count(old) != count:
                raise ValueError("replacement occurrence count mismatch")
            text = text.replace(old, new, count)
        target.write_text(text, encoding="utf-8", newline="\n")
        changed.append(raw_path)
        print(f"Automation V3 code adapter changed allowlisted plan path: {raw_path}")
    return changed


def main() -> int:
    try:
        plan = Path(os.environ["BOTS_V3_RELEASE_PLAN"]).resolve()
        base_sha = os.environ["BOTS_V3_BASE_SHA"].strip()
        instrument = os.environ["BOTS_V3_INSTRUMENT"].strip().upper()
        repo = Path(__file__).resolve().parent
        changed = apply_release_plan(repo, plan, base_sha=base_sha, instrument=instrument)
        print(json.dumps({"status": "PASS", "changed_files": changed, "production_authority": False}))
        return 0
    except Exception as exc:
        print(json.dumps({"status": "FAIL", "reason": str(exc), "production_authority": False}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

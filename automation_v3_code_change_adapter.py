#!/usr/bin/env python3
"""Fail-closed deterministic code-change adapter for Automation V3 PAPER candidates."""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Mapping

SUPPORTED_INSTRUMENTS = {"AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY", "USD_CAD"}
MANAGED_PATH = "managed_strategy_rules.py"
PROTECTED_FILES = {"server.py", "forward_experiment.py"}
BLOCKED_SUFFIXES = {".env", ".db", ".sqlite", ".zip", ".pyc"}
BLOCKED_MARKERS = (
    "api-fxtrade.oanda.com", "TRADING_ENVIRONMENT=PRODUCTION", "PRIMARY_OANDA_ENV=live",
    "OANDA_TOKEN=", "API_KEY=", "SECRET_KEY=", "PASSWORD=",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


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
    if raw != MANAGED_PATH:
        raise ValueError("path outside approved Automation V3 managed surface")
    if rel.name in PROTECTED_FILES:
        raise ValueError("protected LIVE file cannot be changed")
    if rel.suffix.lower() in BLOCKED_SUFFIXES or "__pycache__" in rel.parts:
        raise ValueError("secret/transient file cannot be changed")
    target = (repo / rel).resolve()
    target.relative_to(repo.resolve())
    if not target.is_file():
        raise ValueError("replace_text target must already exist")
    return target


def _verify_artifact(plan_path: Path, descriptor: Mapping[str, Any], label: str) -> None:
    name = str(descriptor.get("path") or "")
    expected = str(descriptor.get("sha256") or "")
    if not name or Path(name).name != name or len(expected) != 64:
        raise ValueError(f"{label} artifact binding missing")
    path = plan_path.parent / name
    if not path.is_file() or _sha256(path) != expected:
        raise ValueError(f"{label} artifact binding mismatch")


def apply_release_plan(repo: Path, plan_path: Path, *, base_sha: str, instrument: str) -> list[str]:
    if os.getenv("BOTS_V3_PRODUCTION_AUTHORITY", "").strip().lower() != "false":
        raise ValueError("production authority must be false")
    instrument = instrument.strip().upper()
    if instrument not in SUPPORTED_INSTRUMENTS:
        raise ValueError("unsupported instrument")
    plan = dict(_load(plan_path))
    if plan.get("status") != "PAPER_DEPLOYABLE_CANDIDATE" or plan.get("schema_version") != 2:
        raise ValueError("release plan is not PAPER deployable")
    if str(plan.get("instrument") or "").upper() != instrument:
        raise ValueError("release plan instrument mismatch")
    if str(plan.get("source_code_sha") or "") != base_sha:
        raise ValueError("release plan base SHA mismatch")
    if plan.get("production_authority") is not False:
        raise ValueError("release plan does not explicitly deny production authority")
    plan_hash = str(plan.pop("plan_binding_sha256", ""))
    if len(plan_hash) != 64 or _canonical_sha256(plan) != plan_hash:
        raise ValueError("release plan binding hash mismatch")
    plan["plan_binding_sha256"] = plan_hash

    candidate_id = str(plan.get("candidate_id") or "")
    definition_sha = str(plan.get("candidate_definition_sha256") or "")
    dataset_identity = plan.get("dataset_identity")
    if not candidate_id or len(definition_sha) != 64 or not isinstance(dataset_identity, Mapping):
        raise ValueError("candidate identity binding missing")
    if dataset_identity.get("code_sha") != base_sha:
        raise ValueError("dataset/source SHA binding mismatch")
    _verify_artifact(plan_path, plan.get("freeze_artifact") or {}, "freeze")
    _verify_artifact(plan_path, plan.get("holdout_artifact") or {}, "holdout")
    _verify_artifact(plan_path, plan.get("audit_artifact") or {}, "audit")
    _verify_artifact(plan_path, plan.get("pre_audit") or {}, "pre-audit")
    if (plan.get("pre_audit") or {}).get("verdict") not in {"ACCEPT", "ACCEPT WITH LIMITATIONS"}:
        raise ValueError("audit verdict does not permit PAPER")

    changes = plan.get("code_changes")
    if not isinstance(changes, list) or len(changes) != 1:
        raise ValueError("release plan must contain exactly one managed code change")
    item = changes[0]
    if not isinstance(item, dict) or item.get("operation") != "replace_text":
        raise ValueError("only replace_text changes are supported")
    raw_path = str(item.get("path") or "")
    target = _safe_relative_path(repo, raw_path)
    expected = str(item.get("expected_file_sha256") or "")
    if len(expected) != 64 or _sha256(target) != expected:
        raise ValueError("pre-edit file hash mismatch")
    old = item.get("old_text")
    new = item.get("new_text")
    count = item.get("expected_occurrences")
    if not isinstance(old, str) or not old or not isinstance(new, str):
        raise ValueError("replacement text must be explicit strings")
    if not isinstance(count, int) or count != 1:
        raise ValueError("expected_occurrences must be exactly one")
    binding = item.get("evidence_binding")
    if not isinstance(binding, Mapping):
        raise ValueError("change evidence binding missing")
    if str(binding.get("instrument") or "").upper() != instrument or binding.get("source_code_sha") != base_sha:
        raise ValueError("change source/instrument binding mismatch")
    if binding.get("candidate_id") != candidate_id or binding.get("candidate_definition_sha256") != definition_sha:
        raise ValueError("change candidate binding mismatch")
    if binding.get("dataset_identity") != dataset_identity or binding.get("dataset_identity_sha256") != _canonical_sha256(dataset_identity):
        raise ValueError("change dataset binding mismatch")
    if binding.get("retuning_after_holdout") is not False:
        raise ValueError("post-holdout retune prohibited")
    if binding.get("freeze_sha256") != (plan.get("freeze_artifact") or {}).get("sha256") or binding.get("holdout_sha256") != (plan.get("holdout_artifact") or {}).get("sha256"):
        raise ValueError("freeze/holdout evidence mismatch")
    if binding.get("audit_sha256") != (plan.get("audit_artifact") or {}).get("sha256") or binding.get("pre_audit_sha256") != (plan.get("pre_audit") or {}).get("sha256"):
        raise ValueError("audit evidence mismatch")
    if binding.get("audit_verdict") != (plan.get("pre_audit") or {}).get("verdict"):
        raise ValueError("audit verdict binding mismatch")
    prefix = f'MANAGED_RULES_JSON["{instrument}"] = '
    if not old.startswith(prefix) or not new.startswith(prefix):
        raise ValueError("cross-asset managed assignment rejected")
    if candidate_id not in new or definition_sha not in new:
        raise ValueError("generated text is not bound to frozen candidate")
    if any(marker.upper() in new.upper() for marker in BLOCKED_MARKERS):
        raise ValueError("LIVE/secret marker rejected")
    text = target.read_text(encoding="utf-8")
    if text.count(old) != count:
        raise ValueError("replacement occurrence count mismatch")
    target.write_text(text.replace(old, new, count), encoding="utf-8", newline="\n")
    print(f"Automation V3 code adapter changed approved managed path: {raw_path}")
    return [raw_path]


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

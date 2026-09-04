#!/usr/bin/env python3
"""Remote, phone-triggered wrapper around autonomous_asset_optimizer.py.

This file does not implement research logic. It validates the remote execution
boundary, configures governed adapters, runs the existing V3 optimizer, and
persists a compact remotely-readable status artifact.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Mapping

from autonomous_asset_optimizer import SUPPORTED_INSTRUMENTS

EXPECTED_TERMINALS = {"PAPER_DEPLOYED", "CANDIDATE_NOT_DEPLOYABLE", "NO_VALID_CANDIDATE", "INSUFFICIENT_EVIDENCE", "DATA_COVERAGE_INSUFFICIENT"}
FAIL_TERMINALS = {
    "DATA_SOURCE_UNAVAILABLE", "DATA_INTEGRITY_FAILED", "METHODOLOGY_BLOCKED", "TEST_FAILURE",
    "DEPLOYMENT_FAILURE", "UNSUPPORTED_INSTRUMENT",
}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return value if isinstance(value, dict) else {}


def _require_remote_boundary() -> None:
    if os.getenv("BOTS_V3_PRODUCTION_AUTHORITY", "").strip().lower() != "false":
        raise RuntimeError("BOTS_V3_PRODUCTION_AUTHORITY must be false")
    if os.getenv("TRADING_ENVIRONMENT", "").strip().upper() != "PAPER":
        raise RuntimeError("TRADING_ENVIRONMENT must be PAPER")
    if os.getenv("PRIMARY_OANDA_ENV", "").strip().lower() != "practice":
        raise RuntimeError("PRIMARY_OANDA_ENV must be practice")
    if not os.getenv("GH_TOKEN", "").strip():
        raise RuntimeError("GITHUB_AUTH_UNAVAILABLE: GH_TOKEN missing")
    if not os.getenv("RAILWAY_TOKEN", "").strip():
        raise RuntimeError("RAILWAY_AUTH_UNAVAILABLE: RAILWAY_TOKEN missing")
    if not os.getenv("OANDA_TOKEN", "").strip():
        raise RuntimeError("DATA_SOURCE_UNAVAILABLE: OANDA_TOKEN missing")


def _current_checkout_sha() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parent, text=True).strip()
    except (OSError, subprocess.SubprocessError):
        return None


def _current_stage(root: Path, instrument: str) -> tuple[str | None, int | None]:
    ledger = _load(root / instrument / "autonomous_v3" / "automation_v3_state.json")
    run = ((ledger.get("runs") or {}).get(instrument) or {}) if isinstance(ledger.get("runs"), dict) else {}
    attempts = run.get("lookback_attempts") or []
    lookback = None
    if attempts and isinstance(attempts[-1], dict):
        lookback = attempts[-1].get("months")
    sha = str(run.get("code_sha") or "")
    if lookback and sha:
        research = root / instrument / "autonomous_v3" / f"lookback_{int(lookback):02d}m_{sha[:12]}" / "research_state.json"
        state = _load(research)
        asset = ((state.get("assets") or {}).get(instrument) or {}) if isinstance(state.get("assets"), dict) else {}
        lifecycle = asset.get("lifecycle") or {}
        if isinstance(lifecycle, dict):
            stage = lifecycle.get("next_allowed_stage") or lifecycle.get("last_valid_stage")
            if stage:
                return str(stage), int(lookback)
    return str(run.get("status") or "RUNNING"), int(lookback) if lookback else None


def _candidate(root: Path, instrument: str) -> Any:
    candidates = sorted((root / instrument / "autonomous_v3").glob("lookback_*/*paper_release_plan.json"))
    if not candidates:
        candidates = sorted((root / instrument / "autonomous_v3").glob("lookback_*/paper_release_plan.json"))
    if not candidates:
        return None
    plan = _load(candidates[-1])
    candidate = plan.get("candidate")
    if not isinstance(candidate, dict):
        return None
    return {
        "candidate_id": candidate.get("candidate_id") or candidate.get("id"),
        "candidate_rule": candidate.get("candidate_rule"),
        "status": candidate.get("status"),
    }


def _snapshot(root: Path, instrument: str, run_id: str, *, terminal: str | None = None, error: str | None = None) -> dict[str, Any]:
    ledger = _load(root / instrument / "autonomous_v3" / "automation_v3_state.json")
    run = ((ledger.get("runs") or {}).get(instrument) or {}) if isinstance(ledger.get("runs"), dict) else {}
    checkout_sha = _current_checkout_sha()
    run_sha = str(run.get("code_sha") or "")
    authoritative = not checkout_sha or not run_sha or run_sha == checkout_sha
    if authoritative:
        stage, lookback = _current_stage(root, instrument)
    else:
        stage, lookback = "STARTING", None
    deployment = run.get("paper_deployment") or run.get("deployment") or {}
    paper_status = deployment.get("status") if authoritative and isinstance(deployment, dict) else None
    stored_terminal = run.get("status") if authoritative and run.get("status") in EXPECTED_TERMINALS | FAIL_TERMINALS else None
    stored_error = run.get("stop_reason") if authoritative else None
    stored_integrity = run.get("integrity_diagnostic") if authoritative and isinstance(run.get("integrity_diagnostic"), dict) else None
    return {
        "run_id": run_id,
        "instrument": instrument,
        "current_stage": stage,
        "lookback": lookback,
        "terminal_state": terminal or stored_terminal,
        "candidate": _candidate(root, instrument) if authoritative else None,
        "paper_deployment_status": paper_status,
        "last_error": error or stored_error,
        "integrity_diagnostic": stored_integrity,
        "production_authority": False,
    }


def run_worker(instrument: str) -> dict[str, Any]:
    instrument = instrument.strip().upper()
    if instrument not in SUPPORTED_INSTRUMENTS:
        raise ValueError("unsupported instrument")
    _require_remote_boundary()
    repo = Path(__file__).resolve().parent
    root = Path(os.getenv("BOTS_RESEARCH_ROOT", str(repo.parent / "Botstrader_Research"))).resolve()
    status_path = Path(os.getenv("BOTS_V3_REMOTE_STATUS_PATH", str(root / instrument / "autonomous_v3" / "remote_status.json"))).resolve()
    run_id = os.getenv("GITHUB_RUN_ID", "local-remote-run")

    os.environ.setdefault("BOTS_V3_CODE_CHANGE_COMMAND", f"{sys.executable} {repo / 'automation_v3_code_change_adapter.py'}")
    os.environ.setdefault("BOTS_V3_PAPER_DEPLOY_COMMAND", f"{sys.executable} {repo / 'automation_v3_railway_adapter.py'} deploy")
    os.environ.setdefault("BOTS_V3_PAPER_VERIFY_COMMAND", f"{sys.executable} {repo / 'automation_v3_railway_adapter.py'} verify")
    os.environ.pop("BOTS_V3_PAPER_ROLLBACK_COMMAND", None)

    initial = _snapshot(root, instrument, run_id)
    _write_json(status_path, initial)
    print("REMOTE_STATUS " + json.dumps(initial, sort_keys=True), flush=True)

    proc = subprocess.Popen(
        [sys.executable, str(repo / "autonomous_asset_optimizer.py"), instrument],
        cwd=str(repo), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env=dict(os.environ),
    )
    while proc.poll() is None:
        time.sleep(max(1, int(os.getenv("BOTS_V3_STATUS_INTERVAL_SECONDS", "15"))))
        snapshot = _snapshot(root, instrument, run_id)
        _write_json(status_path, snapshot)
        print("REMOTE_STATUS " + json.dumps(snapshot, sort_keys=True), flush=True)

    stdout, stderr = proc.communicate()
    result: dict[str, Any] = {}
    try:
        result = json.loads(stdout) if stdout.strip() else {}
    except json.JSONDecodeError:
        result = {}
    terminal = str(result.get("status") or "") if isinstance(result, dict) else ""
    error = None
    if terminal in FAIL_TERMINALS or proc.returncode not in (0, None):
        error = str(result.get("stop_reason") or "") if isinstance(result, dict) else ""
        if not error:
            error = "remote optimizer failed"
    final = _snapshot(root, instrument, run_id, terminal=terminal or None, error=error)
    _write_json(status_path, final)
    print("REMOTE_STATUS " + json.dumps(final, sort_keys=True), flush=True)
    if stderr.strip():
        print("Automation V3 subprocess reported an error; inspect uploaded worker log.", file=sys.stderr)
    (status_path.parent / "worker_stdout.log").write_text(stdout, encoding="utf-8")
    (status_path.parent / "worker_stderr.log").write_text(stderr, encoding="utf-8")
    return final


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Automation V3 remotely with PAPER-only authority")
    parser.add_argument("instrument", choices=SUPPORTED_INSTRUMENTS)
    args = parser.parse_args()
    try:
        status = run_worker(args.instrument)
        print(json.dumps(status, indent=2, sort_keys=True))
        return 0 if status.get("terminal_state") in EXPECTED_TERMINALS else 2
    except Exception as exc:
        print(json.dumps({"status": "FAIL", "reason": str(exc), "production_authority": False}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

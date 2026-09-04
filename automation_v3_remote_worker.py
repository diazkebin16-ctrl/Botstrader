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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from autonomous_asset_optimizer import SUPPORTED_INSTRUMENTS
from automation_v3_convergence import compact_discovery_diagnostic, compact_pre_gate

EXPECTED_TERMINALS = {"PAPER_DEPLOYED", "CANDIDATE_NOT_DEPLOYABLE", "NO_VALID_CANDIDATE", "INSUFFICIENT_EVIDENCE", "DATA_COVERAGE_INSUFFICIENT"}
RESUMABLE_STATES = {"EXECUTION_BUDGET_CHECKPOINT"}
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
    if os.getenv("GITHUB_WORKFLOW", "").strip() != "Automation V3 Remote Optimizer":
        return None
    value = os.getenv("GITHUB_SHA", "").strip()
    return value or None


def _parse_time(value: Any) -> float | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
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


def _current_workspace(root: Path, instrument: str, run: Mapping[str, Any]) -> Path | None:
    attempts = run.get("lookback_attempts") or []
    if not attempts or not isinstance(attempts[-1], Mapping):
        return None
    months = attempts[-1].get("months")
    sha = str(run.get("code_sha") or "")
    if not months or not sha:
        return None
    return root / instrument / "autonomous_v3" / f"lookback_{int(months):02d}m_{sha[:12]}"


def _performance_snapshot(root: Path, instrument: str, run: Mapping[str, Any]) -> dict[str, Any]:
    workspace = _current_workspace(root, instrument, run)
    phases: Mapping[str, Any] = {}
    if workspace:
        state = _load(workspace / "research_state.json")
        asset = ((state.get("assets") or {}).get(instrument) or {}) if isinstance(state.get("assets"), dict) else {}
        phases = asset.get("phases") or {}
    durations: dict[str, float | None] = {}
    for name in ("data_integrity", "replay", "target_population", "phase_1", "phase_2", "discovery", "freeze", "holdout", "audit"):
        phase = phases.get(name) if isinstance(phases, Mapping) else None
        details = (phase or {}).get("details") if isinstance(phase, Mapping) else None
        value = (details or {}).get("duration_seconds") if isinstance(details, Mapping) else None
        durations[name] = round(float(value), 3) if isinstance(value, (int, float)) else None

    attempts = [x for x in run.get("lookback_attempts") or [] if isinstance(x, Mapping) and x.get("code_sha") == run.get("code_sha")]
    start_ts = _parse_time(attempts[0].get("at")) if attempts else None
    total_seconds = max(0.0, time.time() - start_ts) if start_ts is not None else None
    acquisition_seconds = None
    if attempts and isinstance(phases.get("data_integrity"), Mapping):
        phase = phases["data_integrity"]
        end_ts = _parse_time(phase.get("updated_at"))
        duration = durations.get("data_integrity")
        if end_ts is not None and duration is not None and start_ts is not None:
            acquisition_seconds = max(0.0, end_ts - duration - start_ts)

    target = _load(workspace / "03_target_population.json") if workspace else {}
    phase2 = _load(workspace / "05_phase_2.json") if workspace else {}
    discovery = _load(workspace / "06_discovery.json") if workspace else {}
    pre_gate = _load(workspace / "pre_gate_diagnostic.json") if workspace else {}
    support = _load(workspace / "support_diagnostic.json") if workspace else {}
    research_episode_count = len(target.get("episodes") or []) if isinstance(target.get("episodes"), list) else None
    phase1_input = ((phase2.get("phase1_input_population") or {}).get("discovery") or {}) if isinstance(phase2, Mapping) else {}
    resolved_binary = phase1_input.get("resolved_binary")
    candidate_space = discovery.get("candidate_space") or {}

    history = run.get("decision_history") or []
    current_history = []
    for event in reversed(history):
        if not isinstance(event, Mapping):
            continue
        if event.get("decision") == "CODE_SHA_CHANGED" and event.get("new") == run.get("code_sha"):
            current_history.append(event)
            break
        current_history.append(event)
    reacquire_count = sum(x.get("decision") == "DATA_REACQUIRE_REQUIRED" for x in current_history)
    discovery_phase = phases.get("discovery") if isinstance(phases, Mapping) else {}
    discovery_details = (discovery_phase or {}).get("details") if isinstance(discovery_phase, Mapping) else {}
    discovery_cache_hits = int((discovery_details or {}).get("cache_hits") or 0) if isinstance(discovery_details, Mapping) else 0
    cache_misses = sum(1 for value in durations.values() if value is not None)

    return {
        "data_acquisition_seconds": round(acquisition_seconds, 3) if acquisition_seconds is not None else None,
        "data_integrity_seconds": durations["data_integrity"],
        "population_build_seconds": durations["target_population"],
        "phase1_seconds": durations["phase_1"],
        "phase2_seconds": durations["phase_2"],
        "discovery_generation_seconds": None,
        "discovery_evaluation_seconds": durations["discovery"],
        "freeze_seconds": durations["freeze"],
        "holdout_seconds": durations["holdout"],
        "audit_seconds": durations["audit"],
        "total_seconds": round(total_seconds, 3) if total_seconds is not None else None,
        "lookback_months": attempts[-1].get("months") if attempts else None,
        "research_episode_count": research_episode_count,
        "resolved_binary": resolved_binary,
        "candidate_count": int(candidate_space.get("generated") or pre_gate.get("generated_candidates") or 0),
        "candidate_pre_gate_pass_count": int(pre_gate.get("pass_all_pre_gate") or 0),
        "candidate_full_evaluated_count": int(candidate_space.get("evaluated_after_discovery_gate") or 0),
        "freeze_eligible_count": int(candidate_space.get("freeze_eligible") or 0),
        "reacquire_count": reacquire_count,
        "population_rebuild_count": sum(1 for x in attempts if x.get("months") == (attempts[-1].get("months") if attempts else None)),
        "discovery_evaluation_count": 1 if discovery else 0,
        "cache_hits": discovery_cache_hits,
        "cache_misses": cache_misses,
        "dominant_failure": support.get("dominant_failure"),
        "production_authority": False,
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
    stored_terminal = run.get("status") if authoritative and run.get("status") in EXPECTED_TERMINALS | FAIL_TERMINALS | RESUMABLE_STATES else None
    stored_error = run.get("stop_reason") if authoritative else None
    stored_integrity = run.get("integrity_diagnostic") if authoritative and isinstance(run.get("integrity_diagnostic"), dict) else None
    stored_pre_gate = compact_pre_gate(run.get("pre_gate_diagnostic")) if authoritative else None
    stored_diagnostic = compact_discovery_diagnostic(run.get("diagnostic")) if authoritative else None
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
        "pre_gate_diagnostic": stored_pre_gate,
        "discovery_diagnostic": stored_diagnostic,
        "performance": _performance_snapshot(root, instrument, run) if authoritative else None,
        "phase1_status": run.get("phase1_status") if authoritative else None,
        "autonomous_approval": run.get("autonomous_approval") if authoritative else None,
        "production_authority": False,
    }


def _mark_budget_checkpoint(root: Path, instrument: str, reason: str) -> None:
    path = root / instrument / "autonomous_v3" / "automation_v3_state.json"
    ledger = _load(path)
    runs = ledger.setdefault("runs", {})
    run = runs.setdefault(instrument, {"instrument": instrument})
    stage, lookback = _current_stage(root, instrument)
    run["status"] = "EXECUTION_BUDGET_CHECKPOINT"
    run["final_outcome"] = None
    run["stop_reason"] = reason
    run["production_authority"] = False
    run["execution_checkpoint"] = {
        "checkpoint": "EXECUTION_BUDGET_CHECKPOINT",
        "current_stage": stage,
        "lookback_months": lookback,
        "code_sha": run.get("code_sha"),
        "production_authority": False,
    }
    _write_json(path, ledger)


def _stop_process(proc: Any, grace_seconds: float = 10.0) -> None:
    if proc.poll() is not None:
        return
    terminate = getattr(proc, "terminate", None)
    if callable(terminate):
        terminate()
    wait = getattr(proc, "wait", None)
    if callable(wait):
        try:
            wait(timeout=grace_seconds)
            return
        except (subprocess.TimeoutExpired, TypeError):
            pass
    kill = getattr(proc, "kill", None)
    if callable(kill) and proc.poll() is None:
        kill()


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

    status_path.parent.mkdir(parents=True, exist_ok=True)
    stdout_path = status_path.parent / "worker_stdout.log"
    stderr_path = status_path.parent / "worker_stderr.log"
    started = time.monotonic()
    budget_seconds = max(60, int(os.getenv("BOTS_V3_EXECUTION_BUDGET_SECONDS", "18000")))
    interval = max(1, int(os.getenv("BOTS_V3_STATUS_INTERVAL_SECONDS", "15")))
    terminal_grace = max(5, int(os.getenv("BOTS_V3_TERMINAL_GRACE_SECONDS", "30")))
    terminal_seen_at: float | None = None
    budget_checkpointed = False

    with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
        proc = subprocess.Popen(
            [sys.executable, str(repo / "autonomous_asset_optimizer.py"), instrument],
            cwd=str(repo), stdout=stdout_handle, stderr=stderr_handle, text=True,
            env=dict(os.environ),
        )
        while proc.poll() is None:
            time.sleep(interval)
            snapshot = _snapshot(root, instrument, run_id)
            _write_json(status_path, snapshot)
            print("REMOTE_STATUS " + json.dumps(snapshot, sort_keys=True), flush=True)
            terminal_now = snapshot.get("terminal_state")
            if terminal_now in EXPECTED_TERMINALS | FAIL_TERMINALS:
                if terminal_seen_at is None:
                    terminal_seen_at = time.monotonic()
                elif time.monotonic() - terminal_seen_at >= terminal_grace:
                    _stop_process(proc)
                    break
            if time.monotonic() - started >= budget_seconds:
                _stop_process(proc)
                _mark_budget_checkpoint(root, instrument, "execution budget reached; safe resumable checkpoint persisted")
                budget_checkpointed = True
                break
        communicated_stdout = ""
        communicated_stderr = ""
        communicate = getattr(proc, "communicate", None)
        if callable(communicate):
            try:
                communicated = communicate()
                if isinstance(communicated, tuple):
                    if isinstance(communicated[0], str):
                        communicated_stdout = communicated[0]
                    if len(communicated) > 1 and isinstance(communicated[1], str):
                        communicated_stderr = communicated[1]
            except ValueError:
                pass

    if budget_checkpointed:
        final = _snapshot(root, instrument, run_id, terminal="EXECUTION_BUDGET_CHECKPOINT", error="execution budget reached; resume on next run")
        _write_json(status_path, final)
        print("REMOTE_STATUS " + json.dumps(final, sort_keys=True), flush=True)
        return final

    stdout = stdout_path.read_text(encoding="utf-8") if stdout_path.exists() else ""
    stderr = stderr_path.read_text(encoding="utf-8") if stderr_path.exists() else ""
    if not stdout.strip():
        stdout = communicated_stdout
    if not stderr.strip():
        stderr = communicated_stderr
    result: dict[str, Any] = {}
    try:
        result = json.loads(stdout) if stdout.strip() else {}
    except json.JSONDecodeError:
        result = {}
    terminal = str(result.get("status") or "") if isinstance(result, dict) else ""
    error = None
    returncode = getattr(proc, "returncode", 0)
    if terminal in FAIL_TERMINALS or returncode not in (0, None):
        error = str(result.get("stop_reason") or "") if isinstance(result, dict) else ""
        if not error:
            error = "remote optimizer failed"
    final = _snapshot(root, instrument, run_id, terminal=terminal or None, error=error)
    _write_json(status_path, final)
    print("REMOTE_STATUS " + json.dumps(final, sort_keys=True), flush=True)
    if stderr.strip():
        print("Automation V3 subprocess reported an error; inspect uploaded worker log.", file=sys.stderr)
    return final


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Automation V3 remotely with PAPER-only authority")
    parser.add_argument("instrument", choices=SUPPORTED_INSTRUMENTS)
    args = parser.parse_args()
    try:
        status = run_worker(args.instrument)
        print(json.dumps(status, indent=2, sort_keys=True))
        return 0 if status.get("terminal_state") in EXPECTED_TERMINALS | RESUMABLE_STATES else 2
    except Exception as exc:
        print(json.dumps({"status": "FAIL", "reason": str(exc), "production_authority": False}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Railway PAPER deploy/verify adapters for Automation V3.

Designed for CI with a project-scoped RAILWAY_TOKEN. No LIVE fallback exists.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

PRACTICE_OANDA_URL = "https://api-fxpractice.oanda.com"
SUPPORTED_INSTRUMENTS = {"AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY", "USD_CAD"}
TERMINAL_FAILURES = {"FAILED", "CRASHED", "NEEDS_APPROVAL", "SLEEPING", "SKIPPED", "REMOVED", "REMOVING"}
IN_PROGRESS = {"WAITING", "QUEUED", "INITIALIZING", "BUILDING", "DEPLOYING"}


def _run(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True, capture_output=True, check=False)


def _load_json_text(text: str) -> Any:
    value = json.loads(text)
    return value


def _railway_json(args: list[str], *, cwd: Path | None = None) -> Any:
    result = _run(["railway", *args], cwd=cwd)
    if result.returncode:
        raise RuntimeError(f"Railway command failed: {' '.join(args[:2])}")
    return _load_json_text(result.stdout)


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"missing required environment: {name}")
    return value


def _validate_common() -> dict[str, str]:
    if os.getenv("BOTS_V3_PRODUCTION_AUTHORITY", "").strip().lower() != "false":
        raise ValueError("production authority must be false")
    if os.getenv("TRADING_ENVIRONMENT", "").strip().upper() != "PAPER":
        raise ValueError("TRADING_ENVIRONMENT must be PAPER")
    if os.getenv("PRIMARY_OANDA_ENV", "").strip().lower() != "practice":
        raise ValueError("PRIMARY_OANDA_ENV must be practice")
    if os.getenv("OANDA", "").strip() != PRACTICE_OANDA_URL:
        raise ValueError("OANDA endpoint must be practice")
    if not os.getenv("RAILWAY_TOKEN", "").strip():
        raise ValueError("RAILWAY_TOKEN is required")
    instrument = _required("BOTS_V3_INSTRUMENT").upper()
    if instrument not in SUPPORTED_INSTRUMENTS:
        raise ValueError("unsupported instrument")
    expected_sha = _required("BOTS_V3_EXPECTED_SHA")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
        raise ValueError("invalid expected SHA")
    return {
        "project": _required("BOTS_V3_RAILWAY_PROJECT_ID"),
        "environment": _required("BOTS_V3_RAILWAY_ENVIRONMENT_ID"),
        "service": _required("BOTS_V3_RAILWAY_SERVICE_ID"),
        "instrument": instrument,
        "expected_sha": expected_sha,
    }


def _deployment_list(ctx: Mapping[str, str]) -> list[Mapping[str, Any]]:
    value = _railway_json([
        "deployment", "list", "--project", ctx["project"], "--environment", ctx["environment"],
        "--service", ctx["service"], "--limit", "20", "--json",
    ])
    if isinstance(value, dict):
        value = value.get("deployments") or value.get("items") or []
    if not isinstance(value, list):
        raise RuntimeError("unexpected Railway deployment list format")
    return [item for item in value if isinstance(item, dict)]


def _deployment_id(item: Mapping[str, Any]) -> str:
    return str(item.get("id") or item.get("deploymentId") or "")


def _state_path() -> Path:
    return Path(_required("BOTS_V3_REMOTE_DEPLOY_STATE")).resolve()


def _write_state(payload: Mapping[str, Any]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def deploy(repo: Path) -> int:
    ctx = _validate_common()
    head = _run(["git", "rev-parse", "HEAD"], cwd=repo)
    dirty = _run(["git", "status", "--porcelain"], cwd=repo)
    if head.returncode or head.stdout.strip() != ctx["expected_sha"]:
        raise ValueError("deploy checkout does not match expected SHA")
    if dirty.returncode or dirty.stdout.strip():
        raise ValueError("deploy refuses dirty working tree")
    before_ids = {_deployment_id(item) for item in _deployment_list(ctx)}
    message = f"Automation V3 PAPER {ctx['instrument']} {ctx['expected_sha']}"
    result = _run([
        "railway", "up", "--project", ctx["project"], "--environment", ctx["environment"],
        "--service", ctx["service"], "--detach", "-m", message,
    ], cwd=repo)
    if result.returncode:
        raise RuntimeError("Railway PAPER deploy command failed")
    deadline = time.time() + 60
    deployment: Mapping[str, Any] | None = None
    while time.time() < deadline:
        for item in _deployment_list(ctx):
            item_id = _deployment_id(item)
            if item_id and item_id not in before_ids:
                deployment = item
                break
        if deployment:
            break
        time.sleep(3)
    if not deployment:
        raise RuntimeError("new Railway deployment could not be identified")
    payload = {
        "deployment_id": _deployment_id(deployment),
        "expected_sha": ctx["expected_sha"],
        "instrument": ctx["instrument"],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "production_authority": False,
    }
    _write_state(payload)
    print(json.dumps({"status": "QUEUED", **payload}, sort_keys=True))
    return 0


def _variables(ctx: Mapping[str, str]) -> dict[str, str]:
    value = _railway_json([
        "variable", "list", "--project", ctx["project"], "--environment", ctx["environment"],
        "--service", ctx["service"], "--json",
    ])
    if isinstance(value, dict):
        if isinstance(value.get("variables"), dict):
            value = value["variables"]
        return {str(k): str(v) for k, v in value.items() if not isinstance(v, (dict, list))}
    if isinstance(value, list):
        out: dict[str, str] = {}
        for item in value:
            if isinstance(item, dict) and item.get("name") is not None:
                out[str(item["name"])] = str(item.get("value") or "")
        return out
    raise RuntimeError("unexpected Railway variable format")


def _check_service_http() -> None:
    url = _required("BOTS_V3_HEALTHCHECK_URL")
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
    if status >= 500:
        raise RuntimeError(f"service health endpoint returned HTTP {status}")


def _check_oanda(vars_map: Mapping[str, str]) -> None:
    token = vars_map.get("OANDA_TOKEN", "").strip()
    account_id = vars_map.get("OANDA_ACCOUNT_ID", "").strip()
    if not token or not account_id:
        raise RuntimeError("OANDA Practice credentials unavailable for verification")
    request = urllib.request.Request(
        f"{PRACTICE_OANDA_URL}/v3/accounts/{account_id}/summary",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            if int(response.status) != 200:
                raise RuntimeError("OANDA Practice connectivity failed")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"OANDA Practice connectivity failed with HTTP {exc.code}") from None


def verify(repo: Path) -> int:
    ctx = _validate_common()
    state = json.loads(_state_path().read_text(encoding="utf-8"))
    if state.get("expected_sha") != ctx["expected_sha"] or state.get("instrument") != ctx["instrument"]:
        raise ValueError("deployment state identity mismatch")
    deployment_id = str(state.get("deployment_id") or "")
    if not deployment_id:
        raise ValueError("deployment state missing deployment_id")
    timeout = int(os.getenv("BOTS_V3_VERIFY_TIMEOUT_SECONDS", "900"))
    interval = max(2, int(os.getenv("BOTS_V3_VERIFY_INTERVAL_SECONDS", "10")))
    deadline = time.time() + timeout
    final: Mapping[str, Any] | None = None
    while time.time() < deadline:
        current = next((item for item in _deployment_list(ctx) if _deployment_id(item) == deployment_id), None)
        if current is None:
            raise RuntimeError("tracked Railway deployment disappeared")
        status = str(current.get("status") or "").upper()
        if status == "SUCCESS":
            final = current
            break
        if status in TERMINAL_FAILURES:
            raise RuntimeError(f"Railway deployment terminal state: {status}")
        if status not in IN_PROGRESS:
            raise RuntimeError(f"unknown Railway deployment state: {status}")
        time.sleep(interval)
    if final is None:
        raise RuntimeError("Railway deployment verification timed out")

    head = _run(["git", "rev-parse", "HEAD"], cwd=repo)
    dirty = _run(["git", "status", "--porcelain"], cwd=repo)
    if head.returncode or head.stdout.strip() != ctx["expected_sha"] or dirty.stdout.strip():
        raise RuntimeError("verified deployment source no longer matches expected SHA")

    vars_map = _variables(ctx)
    if vars_map.get("TRADING_ENVIRONMENT", "").upper() != "PAPER":
        raise RuntimeError("Railway service is not PAPER")
    if vars_map.get("PRIMARY_OANDA_ENV", "").lower() != "practice":
        raise RuntimeError("Railway service is not OANDA Practice")
    configured_oanda = vars_map.get("OANDA")
    if configured_oanda and configured_oanda != PRACTICE_OANDA_URL:
        raise RuntimeError("Railway service contains non-practice OANDA endpoint")
    instruments = {item.strip().upper() for item in vars_map.get("INSTRUMENTS", "").split(",") if item.strip()}
    if ctx["instrument"] not in instruments:
        raise RuntimeError("intended instrument is not active in Railway PAPER config")

    logs = _run([
        "railway", "logs", "--project", ctx["project"], "--environment", ctx["environment"],
        "--service", ctx["service"], "--latest", "--lines", "100", "--json",
    ], cwd=repo)
    if logs.returncode:
        raise RuntimeError("Railway runtime logs unavailable")
    lowered = logs.stdout.lower()
    if "traceback (most recent call last)" in lowered or "application startup failed" in lowered:
        raise RuntimeError("startup/runtime failure detected in Railway logs")
    _check_service_http()
    _check_oanda(vars_map)
    print(json.dumps({
        "status": "PAPER_DEPLOYED",
        "deployment_id": deployment_id,
        "verified_sha": ctx["expected_sha"],
        "instrument": ctx["instrument"],
        "production_authority": False,
    }, sort_keys=True))
    return 0


def export_runtime() -> int:
    project = _required("BOTS_V3_RAILWAY_PROJECT_ID")
    environment = _required("BOTS_V3_RAILWAY_ENVIRONMENT_ID")
    service = _required("BOTS_V3_RAILWAY_SERVICE_ID")
    if not os.getenv("RAILWAY_TOKEN", "").strip():
        raise ValueError("RAILWAY_TOKEN is required")
    ctx = {"project": project, "environment": environment, "service": service}
    vars_map = _variables(ctx)
    required = ("OANDA_TOKEN", "OANDA_ACCOUNT_ID", "PRIMARY_OANDA_ENV", "TRADING_ENVIRONMENT", "INSTRUMENTS")
    missing = [name for name in required if not vars_map.get(name, "").strip()]
    if missing:
        raise RuntimeError("required Railway PAPER variables missing: " + ",".join(missing))
    if vars_map["PRIMARY_OANDA_ENV"].lower() != "practice" or vars_map["TRADING_ENVIRONMENT"].upper() != "PAPER":
        raise RuntimeError("Railway service is not configured for PAPER/OANDA Practice")
    github_env = Path(_required("GITHUB_ENV"))
    with github_env.open("a", encoding="utf-8") as handle:
        for name in required:
            value = vars_map[name].replace("\n", "")
            print(f"::add-mask::{value}")
            handle.write(f"{name}={value}\n")
    print(json.dumps({"status": "PASS", "exported_names": list(required), "production_authority": False}))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("deploy", "verify", "export-runtime"))
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent
    try:
        if args.command == "deploy":
            return deploy(repo)
        if args.command == "verify":
            return verify(repo)
        return export_runtime()
    except Exception as exc:
        print(json.dumps({"status": "FAIL", "reason": str(exc), "production_authority": False}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

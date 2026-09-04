"""Structured offline test, diff and package cleanliness auditing."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any, Dict, Sequence


PROHIBITED_NAMES = re.compile(
    r"(^|/)(\.env($|\.)|.*\.pid$|.*\.pyc$|__pycache__($|/)|\.pytest_cache($|/)|"
    r".*\.(db|sqlite|sqlite3|zip|tar|gz|log)$|research_state\.json$|.*replay.*\.json$)",
    re.IGNORECASE,
)
SECRET_PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(r"(?i)(api[_-]?key|secret|token|password)\s*[:=]\s*['\"][^'\"]{8,}['\"]"),
)
PROTECTED_PRODUCTION_FILES = {"server.py", "forward_experiment.py"}


def _run(command: Sequence[str], repo: Path) -> Dict[str, Any]:
    result = subprocess.run(list(command), cwd=str(repo), text=True, capture_output=True, check=False)
    return {
        "command": list(command),
        "executed": True,
        "returncode": result.returncode,
        "status": "PASS" if result.returncode == 0 else "FAIL",
        "stdout": result.stdout[-12000:],
        "stderr": result.stderr[-12000:],
    }


def orchestrate_tests(
    repo: str | Path,
    *,
    new_test_commands: Sequence[Sequence[str]],
    regression_commands: Sequence[Sequence[str]],
    run_full_regression: bool = False,
) -> Dict[str, Any]:
    root = Path(repo).resolve()
    compile_result = _run(["python", "-m", "compileall", "-q", "."], root)
    new_results = [_run(command, root) for command in new_test_commands]
    regression_results = [_run(command, root) for command in regression_commands]
    full = _run(["python", "-m", "pytest", "-q"], root) if run_full_regression else {
        "executed": False, "status": "NOT TESTED", "reason": "full regression not requested",
    }
    required = [compile_result, *new_results, *regression_results]
    status = "PASS" if required and all(item["status"] == "PASS" for item in required) and (not run_full_regression or full["status"] == "PASS") else "FAIL"
    return {
        "status": status,
        "stage": "test_orchestrator",
        "compileall": compile_result,
        "new_module_tests": new_results,
        "historical_regression_tests": regression_results,
        "full_regression": full,
    }


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=str(repo), text=True, capture_output=True, check=True)
    return result.stdout


def audit_diff_package(repo: str | Path, *, base_commit: str, allow_production_changes: bool = False) -> Dict[str, Any]:
    root = Path(repo).resolve()
    raw = _git(root, "diff", "--name-status", "--find-renames", base_commit, "--")
    changes = []
    prohibited = []
    secret_hits = []
    permission_changes = []
    for line in raw.splitlines():
        parts = line.split("\t")
        status_code = parts[0]
        path = parts[-1]
        record = {"status": status_code, "path": path}
        changes.append(record)
        if PROHIBITED_NAMES.search(path):
            prohibited.append({"path": path, "reason": "PROHIBITED_ARTIFACT"})
        if path in PROTECTED_PRODUCTION_FILES and not allow_production_changes:
            prohibited.append({"path": path, "reason": "PRODUCTION_FILE_CHANGED"})
        local = root / path
        if local.is_file() and local.stat().st_size <= 2_000_000:
            try:
                text = local.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                text = ""
            for pattern in SECRET_PATTERNS:
                if pattern.search(text):
                    secret_hits.append({"path": path, "pattern": pattern.pattern})
                    break
    summary = _git(root, "diff", "--summary", base_commit, "--")
    for line in summary.splitlines():
        if "mode change" in line or "create mode 100755" in line:
            permission_changes.append(line.strip())
    untracked = [line[3:] for line in _git(root, "status", "--short", "--untracked-files=all").splitlines() if line.startswith("?? ")]
    for path in untracked:
        changes.append({"status": "?", "path": path})
        local = root / path
        if local.is_file() and local.stat().st_size <= 2_000_000:
            try:
                content = local.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                content = ""
            for pattern in SECRET_PATTERNS:
                if pattern.search(content):
                    secret_hits.append({"path": path, "pattern": pattern.pattern})
                    break
    contaminated_untracked = [path for path in untracked if PROHIBITED_NAMES.search(path)]
    failures = []
    if prohibited:
        failures.append("PROHIBITED_OR_PRODUCTION_FILES_IN_DIFF")
    if secret_hits:
        failures.append("POTENTIAL_SECRET_DETECTED")
    if permission_changes:
        failures.append("UNREVIEWED_PERMISSION_CHANGE")
    if contaminated_untracked:
        failures.append("WORKTREE_CONTAMINATED_BY_TRANSIENTS")
    return {
        "status": "PASS" if not failures else "FAIL",
        "stage": "diff_package_audit",
        "base_commit": base_commit,
        "head": _git(root, "rev-parse", "HEAD").strip(),
        "changes": changes,
        "permission_changes": permission_changes,
        "prohibited": prohibited,
        "secret_hits": secret_hits,
        "untracked": untracked,
        "contaminated_untracked": contaminated_untracked,
        "manifest_generation_allowed": not failures,
        "failures": failures,
    }


def combined_audit(
    repo: str | Path,
    *,
    base_commit: str,
    new_test_commands: Sequence[Sequence[str]],
    regression_commands: Sequence[Sequence[str]],
    run_full_regression: bool = False,
) -> Dict[str, Any]:
    tests = orchestrate_tests(
        repo, new_test_commands=new_test_commands,
        regression_commands=regression_commands,
        run_full_regression=run_full_regression,
    )
    package = audit_diff_package(repo, base_commit=base_commit)
    return {
        "status": "PASS" if tests["status"] == package["status"] == "PASS" else "FAIL",
        "stage": "audit",
        "tests": tests,
        "package": package,
        "production_modifications": "NONE" if not any(item["path"] in PROTECTED_PRODUCTION_FILES for item in package["changes"]) else "DETECTED",
    }

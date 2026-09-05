from pathlib import Path
import os
import subprocess
import sys

BASE_SHA = os.environ["BASE_SHA"]
ROOT = Path(__file__).resolve().parent
MODE_FILE = ROOT / "automation_v3_modes.py"
TEST_FILE = ROOT / "test_automation_v3_review_ledger_lookback.py"

text = MODE_FILE.read_text(encoding="utf-8")

old = '''        workspace, months = resolved
        discovery_path = workspace / "06_discovery.json"
'''
new = '''        workspace, months = resolved
        # REVIEW owns lookback identity from the persisted attempt/workspace.  A
        # base terminal may also pass lookback_months as context; consume and
        # validate it here so V3Ledger.mutate receives exactly one authoritative
        # value.
        inherited_lookback = extra.pop("lookback_months", None)
        if inherited_lookback is not None and int(inherited_lookback) != months:
            raise ValueError(
                f"review lookback identity mismatch: workspace={months}, terminal={inherited_lookback}"
            )
        discovery_path = workspace / "06_discovery.json"
'''
if old not in text:
    raise SystemExit("persist-review anchor not found")
text = text.replace(old, new, 1)

old = '''                stop_reason=f"REVIEW_REPORT_BUILD_FAILED: {safe_reason}",
                review_report_error={"code": "REVIEW_REPORT_BUILD_FAILED", "reason": safe_reason},
'''
new = '''                stop_reason=f"REVIEW_REPORT_BUILD_FAILED: {safe_reason}",
                lookback_months=months,
                review_report_error={"code": "REVIEW_REPORT_BUILD_FAILED", "reason": safe_reason},
'''
if old not in text:
    raise SystemExit("review-report error anchor not found")
text = text.replace(old, new, 1)

anchor = '''def execute_request(request: Mapping[str, Any], *, repo: Path | None = None, root: Path | None = None) -> dict[str, Any]:
'''
helper = '''def _write_mode_failure_status(
    request: Mapping[str, Any],
    exc: Exception,
    *,
    repo: Path | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    """Overwrite remote status with current-request failure authority.

    Historical review artifacts remain immutable on disk, but a failed request
    must never inherit shortlist or selection authority from a prior run.
    """
    repo = (repo or Path(__file__).resolve().parent).resolve()
    root = (root or Path(os.getenv("BOTS_RESEARCH_ROOT", str(repo.parent / "Botstrader_Research")))).resolve()
    instrument = normalize_instrument(request.get("instrument"))
    raw_instrument = str(request.get("instrument") or "").strip().upper().replace("/", "_")
    instrument = instrument or raw_instrument or "UNKNOWN"
    mode = str(request.get("mode") or "UNKNOWN").upper()
    run_id = os.getenv("GITHUB_RUN_ID", "local-mode-run")
    try:
        code_sha = _git_sha(repo)
    except Exception:
        code_sha = None
    safe_reason = str(exc).replace(str(root), "<research-root>")[:500]
    request_identity = {
        "run_id": run_id,
        "instrument": instrument,
        "mode": mode,
        "code_sha": code_sha,
    }
    status = {
        **request_identity,
        "request_identity_sha256": canonical_sha256(request_identity),
        "terminal_state": "MODE_REQUEST_FAILED",
        "reason": safe_reason,
        "status_source": "CURRENT_REQUEST_FAILURE",
        "current_request_failed": True,
        "current_request_selection_authority": False,
        "diagnostic_review_sha256": None,
        "shortlist_sha256": None,
        "review_candidates": [],
        "diagnostic_top_candidates": [],
        "deployable_candidates": [],
        "incumbent_metrics": None,
        "holdout_opened": False,
        "paper_deployment_status": None,
        "production_authority": False,
    }
    status_path = Path(
        os.getenv(
            "BOTS_V3_REMOTE_STATUS_PATH",
            str(root / instrument / "autonomous_v3" / "remote_status.json"),
        )
    )
    write_json(status_path, status)
    print("REMOTE_STATUS " + json.dumps(status, sort_keys=True), flush=True)
    return status


'''
if anchor not in text:
    raise SystemExit("execute_request anchor not found")
text = text.replace(anchor, helper + anchor, 1)

old = '''    except Exception as exc:
        result = {"status": "MODE_REQUEST_FAILED", "reason": str(exc), "production_authority": False}
        print(json.dumps(result, indent=2, sort_keys=True))
        raise SystemExit(2)
'''
new = '''    except Exception as exc:
        failed_status = _write_mode_failure_status(payload, exc)
        result = {
            "status": "MODE_REQUEST_FAILED",
            "reason": str(exc),
            "remote_status": failed_status,
            "production_authority": False,
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        raise SystemExit(2)
'''
if old not in text:
    raise SystemExit("main failure anchor not found")
text = text.replace(old, new, 1)
MODE_FILE.write_text(text, encoding="utf-8")

TEST_FILE.write_text(r'''import json
from pathlib import Path

import automation_v3_modes as modes
from autonomous_asset_optimizer import V3Ledger


def _fake_shortlist():
    return {
        "shortlist_sha256": "a" * 64,
        "diagnostic_review_sha256": "b" * 64,
        "diagnostic_top_candidates": [{"rank": 1, "deployment_eligible": True}],
        "deployable_candidates": [{"rank": 1, "deployment_eligible": True}],
        "incumbent_metrics": {"resolved_binary": 46},
        "production_authority": False,
    }


def test_review_pause_restored_lookback_is_owned_once(tmp_path, monkeypatch):
    root = tmp_path / "GBP_USD" / "autonomous_v3"
    root.mkdir(parents=True)
    ledger = V3Ledger(root / "automation_v3_state.json")
    code_sha = "1" * 40
    ledger.mutate(
        "GBP_USD",
        status="RUNNING",
        code_sha=code_sha,
        workspace=str(root),
        lookback_months=1,
        lookback_attempts=[{"months": 1, "status": "RUNNING"}],
    )
    workspace = root / f"lookback_01m_{code_sha[:12]}"
    workspace.mkdir()
    (workspace / "06_discovery.json").write_text("{}\n", encoding="utf-8")
    artifact = workspace / ("review_shortlist_" + "a" * 64 + ".json")
    payload = _fake_shortlist()
    monkeypatch.setattr(modes, "build_review_shortlist", lambda *args, **kwargs: (artifact, payload))

    optimizer = modes.ReviewBeforeHoldoutOptimizer(tmp_path, code_sha_provider=lambda: code_sha)
    result = optimizer._terminal(
        ledger,
        "GBP_USD",
        "METHODOLOGY_BLOCKED",
        modes.REVIEW_PAUSE_SENTINEL,
        lookback_months=1,
    )

    assert result["status"] == modes.REVIEW_READY
    assert result["lookback_months"] == 1
    assert result["review_shortlist_sha256"] == "a" * 64
    assert result["production_authority"] is False


def test_failed_mode_status_overwrites_stale_review_authority(tmp_path, monkeypatch):
    status_path = tmp_path / "remote_status.json"
    stale = {
        "run_id": "old-run",
        "terminal_state": modes.REVIEW_READY,
        "diagnostic_review_sha256": "c" * 64,
        "shortlist_sha256": "d" * 64,
        "diagnostic_top_candidates": [{"rank": 1}],
        "deployable_candidates": [{"rank": 1}],
        "incumbent_metrics": {"resolved_binary": 99},
        "production_authority": False,
    }
    status_path.write_text(json.dumps(stale), encoding="utf-8")
    monkeypatch.setenv("BOTS_V3_REMOTE_STATUS_PATH", str(status_path))
    monkeypatch.setenv("GITHUB_RUN_ID", "33962233273")

    status = modes._write_mode_failure_status(
        {"instrument": "GBP_USD", "mode": modes.REVIEW_BEFORE_HOLDOUT_DEPLOY},
        TypeError("V3Ledger.mutate() got multiple values for keyword argument 'lookback_months'"),
        repo=Path(__file__).resolve().parent,
        root=tmp_path,
    )
    persisted = json.loads(status_path.read_text(encoding="utf-8"))

    assert persisted == status
    assert status["run_id"] == "33962233273"
    assert status["terminal_state"] == "MODE_REQUEST_FAILED"
    assert status["status_source"] == "CURRENT_REQUEST_FAILURE"
    assert status["current_request_selection_authority"] is False
    assert status["diagnostic_review_sha256"] is None
    assert status["shortlist_sha256"] is None
    assert status["diagnostic_top_candidates"] == []
    assert status["deployable_candidates"] == []
    assert status["incumbent_metrics"] is None
    assert status["holdout_opened"] is False
    assert status["paper_deployment_status"] is None
    assert status["production_authority"] is False
    assert status["request_identity_sha256"]


def test_lookback_identity_mismatch_fails_instead_of_relabeling(tmp_path):
    root = tmp_path / "GBP_USD" / "autonomous_v3"
    root.mkdir(parents=True)
    ledger = V3Ledger(root / "automation_v3_state.json")
    code_sha = "2" * 40
    ledger.mutate(
        "GBP_USD",
        status="RUNNING",
        code_sha=code_sha,
        workspace=str(root),
        lookback_attempts=[{"months": 1, "status": "RUNNING"}],
    )
    workspace = root / f"lookback_01m_{code_sha[:12]}"
    workspace.mkdir()
    (workspace / "06_discovery.json").write_text("{}\n", encoding="utf-8")
    optimizer = modes.ReviewBeforeHoldoutOptimizer(tmp_path, code_sha_provider=lambda: code_sha)
    try:
        optimizer._persist_review_result(
            ledger,
            "GBP_USD",
            status=modes.REVIEW_READY,
            reason="test",
            final_outcome=modes.REVIEW_READY,
            lookback_months=2,
        )
    except ValueError as exc:
        assert "review lookback identity mismatch" in str(exc)
    else:
        raise AssertionError("lookback mismatch must fail closed")
''', encoding="utf-8")


def run(cmd, *, check=True):
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=ROOT, check=check, text=True)

run([sys.executable, "-m", "pytest", "-q", "test_automation_v3_review_ledger_lookback.py"])
run([sys.executable, "-m", "pytest", "-q", "test_automation_v3_modes.py", "test_automation_v3_review_ledger_lookback.py"])
run([sys.executable, "-m", "pytest", "-q", "test_automation_v3_integrity_recovery.py", "test_automation_v3_adaptive_paper_confidence.py", "test_automation_v3_incumbent_challenger.py"])
run([sys.executable, "-m", "pytest", "-q", "test_automation_v3_remote_runner.py", "test_automation_v3.py", "test_automation_v3_horizon_max3_lookback.py", "test_automation_v3_phase1_continuation.py"])
run([sys.executable, "-m", "pytest", "-q"])
run([sys.executable, "-m", "compileall", "-q", "."])
run(["pyflakes", "automation_v3_modes.py", "test_automation_v3_review_ledger_lookback.py"])
run(["git", "diff", "--check", BASE_SHA, "--"])

# Protected files must remain byte-identical to the accepted main baseline.
for protected in ("server.py", "forward_experiment.py"):
    before = subprocess.check_output(["git", "show", f"{BASE_SHA}:{protected}"], cwd=ROOT)
    after = (ROOT / protected).read_bytes()
    if before != after:
        raise SystemExit(f"protected file changed: {protected}")

# Focused secret/data check over the new diff.
diff = subprocess.check_output(["git", "diff", BASE_SHA, "--", "."], cwd=ROOT, text=True)
for needle in ("OANDA_TOKEN=", "GH_TOKEN=", "RAILWAY_TOKEN=", "PRIVATE KEY-----"):
    if needle in diff:
        raise SystemExit(f"secret-like material found in diff: {needle}")

# Remove temporary validation machinery from the final tree.
for path in (ROOT / "_tmp_v3_ledger_fix.py", ROOT / ".github/workflows/tmp-v3-ledger-lookback-fix.yml"):
    if path.exists():
        path.unlink()

run(["git", "config", "user.name", "automation-v3-ledger-fix"])
run(["git", "config", "user.email", "automation-v3-ledger-fix@users.noreply.github.com"])
run(["git", "add", "automation_v3_modes.py", "test_automation_v3_review_ledger_lookback.py", "_tmp_v3_ledger_fix.py", ".github/workflows/tmp-v3-ledger-lookback-fix.yml"])
run(["git", "commit", "-m", "Fix REVIEW ledger lookback ownership and failed status isolation"])
run(["git", "push", "origin", "HEAD:fix/automation-v3-review-ledger-lookback"])

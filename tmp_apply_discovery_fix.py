from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if old not in text:
        if new in text:
            return
        raise SystemExit(f"expected text not found in {path}: {old[:80]!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# 1) Current PASS integrity artifacts must never classify downstream discovery errors.
replace_once(
    "autonomous_asset_optimizer.py",
    'def diagnose_discovery(d:Mapping[str,Any],*,min_resolved=10):\n',
    'def integrity_artifact_failed(report:Mapping[str,Any]):\n return str(report.get("status") or "UNKNOWN").upper()!="PASS" or bool(report.get("failures") or [])\n\ndef diagnose_discovery(d:Mapping[str,Any],*,min_resolved=10):\n',
)

replace_once(
    "autonomous_asset_optimizer.py",
    '   r.update(status="NEW",paper_deployment=None,stop_reason=None);ledger.save(s)\n',
    '   r.update(status="NEW",paper_deployment=None,stop_reason=None,final_outcome=None,integrity_diagnostic=None,diagnostic=None,lookback_months=None);ledger.save(s)\n',
)

replace_once(
    "autonomous_asset_optimizer.py",
    '  end=aligned_research_end(self.now(),240);ledger.mutate(i,status="RUNNING",code_sha=sha,workspace=str(root),max_lookback_months=12)\n',
    '  end=aligned_research_end(self.now(),240);ledger.mutate(i,status="RUNNING",code_sha=sha,workspace=str(root),max_lookback_months=12,stop_reason=None,final_outcome=None,integrity_diagnostic=None,diagnostic=None)\n',
)

replace_once(
    "autonomous_asset_optimizer.py",
    '    if err:\n     ip=ad/"01_data_integrity.json"\n     if ip.is_file():\n',
    '    if err:\n     ip=ad/"01_data_integrity.json"\n     if ip.is_file() and not integrity_artifact_failed(load_json(ip)):\n      ledger.mutate(i,integrity_diagnostic=None,lookback_months=months)\n     if ip.is_file() and integrity_artifact_failed(load_json(ip)):\n',
)

replace_once(
    "autonomous_asset_optimizer.py",
    '       except Exception as x:\n        err=x\n        if ip.is_file():\n         diag=build_integrity_diagnostic(load_json(ip),artifact_path=ip,cache_path=cache,requested_start=start.isoformat(),requested_end=end.isoformat(),cache_preexisting=False,retry_count=1)\n         write_json(ad/"integrity_diagnostic.json",diag);ledger.mutate(i,integrity_diagnostic=diag,lookback_months=months);ledger.append(i,"decision_history",{"decision":"DATA_INTEGRITY_RETRY_FAILED","months":months,"diagnostic":diag,"at":utc_now()})\n       if err and diag.get("recoverable") is True:\n',
    '       except Exception as x:\n        err=x\n        if ip.is_file() and integrity_artifact_failed(load_json(ip)):\n         diag=build_integrity_diagnostic(load_json(ip),artifact_path=ip,cache_path=cache,requested_start=start.isoformat(),requested_end=end.isoformat(),cache_preexisting=False,retry_count=1)\n         write_json(ad/"integrity_diagnostic.json",diag);ledger.mutate(i,integrity_diagnostic=diag,lookback_months=months);ledger.append(i,"decision_history",{"decision":"DATA_INTEGRITY_RETRY_FAILED","months":months,"diagnostic":diag,"at":utc_now()})\n        else:\n         diag=None;ledger.mutate(i,integrity_diagnostic=None,lookback_months=months)\n       if err and isinstance(diag,Mapping) and diag.get("recoverable") is True:\n',
)

replace_once(
    "autonomous_asset_optimizer.py",
    '      if err and diag.get("recoverable") is not True:\n       return self._terminal(ledger,i,terminal_for_nonrecoverable(diag),str(err),lookback_months=months,integrity_diagnostic=diag)\n',
    '      if err and isinstance(diag,Mapping) and diag.get("recoverable") is not True:\n       return self._terminal(ledger,i,terminal_for_nonrecoverable(diag),str(err),lookback_months=months,integrity_diagnostic=diag)\n',
)

# 2) Phone-visible snapshot suppresses terminal/error/diagnostic from a different code SHA.
replace_once(
    "automation_v3_remote_worker.py",
    'def _current_stage(root: Path, instrument: str) -> tuple[str | None, int | None]:\n',
    'def _current_checkout_sha() -> str | None:\n    try:\n        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parent, text=True).strip()\n    except (OSError, subprocess.SubprocessError):\n        return None\n\n\ndef _current_stage(root: Path, instrument: str) -> tuple[str | None, int | None]:\n',
)

old_snapshot = '''def _snapshot(root: Path, instrument: str, run_id: str, *, terminal: str | None = None, error: str | None = None) -> dict[str, Any]:
    stage, lookback = _current_stage(root, instrument)
    ledger = _load(root / instrument / "autonomous_v3" / "automation_v3_state.json")
    run = ((ledger.get("runs") or {}).get(instrument) or {}) if isinstance(ledger.get("runs"), dict) else {}
    deployment = run.get("paper_deployment") or run.get("deployment") or {}
    if isinstance(deployment, dict):
        paper_status = deployment.get("status")
    else:
        paper_status = None
    return {
        "run_id": run_id,
        "instrument": instrument,
        "current_stage": stage,
        "lookback": lookback,
        "terminal_state": terminal or (run.get("status") if run.get("status") in EXPECTED_TERMINALS | FAIL_TERMINALS else None),
        "candidate": _candidate(root, instrument),
        "paper_deployment_status": paper_status,
        "last_error": error or run.get("stop_reason"),
        "integrity_diagnostic": run.get("integrity_diagnostic") if isinstance(run.get("integrity_diagnostic"), dict) else None,
        "production_authority": False,
    }
'''
new_snapshot = '''def _snapshot(root: Path, instrument: str, run_id: str, *, terminal: str | None = None, error: str | None = None) -> dict[str, Any]:
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
'''
replace_once("automation_v3_remote_worker.py", old_snapshot, new_snapshot)

# 3) Focused regressions using the existing V3 fake cascade/data harness.
Path("test_automation_v3_discovery_expansion.py").write_text(r'''import json

import automation_v3_remote_worker as remote
from autonomous_asset_optimizer import V3Ledger, integrity_artifact_failed
from test_automation_v3 import ScenarioCascade, _optimizer


class PassIntegrityCascade(ScenarioCascade):
    def run(self, instrument, stages, through="prompts"):
        wd = stages[0].artifact.parent
        (wd / "01_data_integrity.json").write_text(json.dumps({
            "status": "PASS", "failures": [], "bid_ask_real": True,
            "midpoint_only": False, "production_authority": False,
        }))
        return super().run(instrument, stages, through=through)


def _pass_integrity_optimizer(tmp_path, scenario, release=None):
    opt, data, rel = _optimizer(tmp_path, scenario, release=release)
    opt.cascade_factory = lambda m: PassIntegrityCascade(m, scenario)
    return opt, data, rel


def test_integrity_pass_discovery_failure_never_data_integrity_failed(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "research"))
    opt, _, _ = _pass_integrity_optimizer(tmp_path, {1: "bad"})
    out = opt.optimize("AUD_USD")
    assert out["status"] == "NO_VALID_CANDIDATE"
    assert out["status"] != "DATA_INTEGRITY_FAILED"


def test_discovery_insufficient_support_one_to_three(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "research"))
    opt, data, _ = _pass_integrity_optimizer(tmp_path, {1: "insufficient", 3: "bad"})
    assert opt.optimize("AUD_USD")["status"] == "NO_VALID_CANDIDATE"
    assert len(data.calls) == 2


def test_discovery_insufficient_support_three_to_six(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "research"))
    opt, data, _ = _pass_integrity_optimizer(tmp_path, {1: "insufficient", 3: "insufficient", 6: "bad"})
    assert opt.optimize("AUD_USD")["status"] == "NO_VALID_CANDIDATE"
    assert len(data.calls) == 3


def test_discovery_insufficient_support_six_to_twelve(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "research"))
    opt, data, _ = _pass_integrity_optimizer(tmp_path, {1: "insufficient", 3: "insufficient", 6: "insufficient", 12: "bad"})
    assert opt.optimize("AUD_USD")["status"] == "NO_VALID_CANDIDATE"
    assert len(data.calls) == 4


def test_discovery_twelve_month_insufficient_is_insufficient_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "research"))
    opt, data, _ = _pass_integrity_optimizer(tmp_path, {1: "insufficient", 3: "insufficient", 6: "insufficient", 12: "insufficient"})
    assert opt.optimize("AUD_USD")["status"] == "INSUFFICIENT_EVIDENCE"
    assert len(data.calls) == 4


def test_adequate_support_without_candidate_is_no_valid_candidate(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "research"))
    opt, data, _ = _pass_integrity_optimizer(tmp_path, {1: "bad"})
    assert opt.optimize("AUD_USD")["status"] == "NO_VALID_CANDIDATE"
    assert len(data.calls) == 1


def test_stale_previous_integrity_terminal_cannot_contaminate_new_sha(tmp_path, monkeypatch):
    root = tmp_path / "research"
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(root))
    ledger = V3Ledger(root / "AUD_USD" / "autonomous_v3" / "automation_v3_state.json")
    ledger.mutate("AUD_USD", status="DATA_INTEGRITY_FAILED", code_sha="old-sha", final_outcome="DATA_INTEGRITY_FAILED", stop_reason="old failure", integrity_diagnostic={"integrity_status": "FAIL"})
    opt, _, _ = _pass_integrity_optimizer(tmp_path, {1: "bad"})
    out = opt.optimize("AUD_USD")
    assert out["status"] == "NO_VALID_CANDIDATE"
    assert out.get("integrity_diagnostic") is None


def test_stale_integrity_diagnostic_cannot_override_current_pass(tmp_path, monkeypatch):
    root = tmp_path / "research"
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(root))
    ledger = V3Ledger(root / "AUD_USD" / "autonomous_v3" / "automation_v3_state.json")
    ledger.mutate("AUD_USD", status="RUNNING", code_sha="a" * 40, integrity_diagnostic={"integrity_status": "FAIL", "failed_checks": ["OLD"]})
    opt, _, _ = _pass_integrity_optimizer(tmp_path, {1: "bad"})
    out = opt.optimize("AUD_USD")
    assert out["status"] == "NO_VALID_CANDIDATE"
    assert out.get("integrity_diagnostic") is None


def test_cached_market_data_reused_across_code_sha_when_current_identity_rebuilt(tmp_path, monkeypatch):
    root = tmp_path / "research"
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(root))
    cache = root / "AUD_USD" / "autonomous_v3" / "data" / "AUD_USD_01m.json"
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text("{}")
    opt, data, _ = _pass_integrity_optimizer(tmp_path, {1: "bad"})
    out = opt.optimize("AUD_USD")
    assert out["status"] == "NO_VALID_CANDIDATE"
    assert data.calls == []


def test_remote_snapshot_hides_stale_terminal_and_diagnostic(tmp_path, monkeypatch):
    root = tmp_path / "research"
    ledger = V3Ledger(root / "AUD_USD" / "autonomous_v3" / "automation_v3_state.json")
    ledger.mutate("AUD_USD", status="DATA_INTEGRITY_FAILED", code_sha="old", stop_reason="old", integrity_diagnostic={"integrity_status": "FAIL"})
    monkeypatch.setattr(remote, "_current_checkout_sha", lambda: "new")
    snap = remote._snapshot(root, "AUD_USD", "run")
    assert snap["terminal_state"] is None
    assert snap["integrity_diagnostic"] is None
    assert snap["last_error"] is None
    assert snap["current_stage"] == "STARTING"
    assert snap["production_authority"] is False


def test_integrity_pass_helper_is_authoritative():
    assert integrity_artifact_failed({"status": "PASS", "failures": []}) is False
    assert integrity_artifact_failed({"status": "FAIL", "failures": ["X"]}) is True
''', encoding="utf-8")

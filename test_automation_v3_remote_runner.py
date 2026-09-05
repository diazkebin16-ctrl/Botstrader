import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import automation_v3_code_change_adapter as code_adapter
import automation_v3_railway_adapter as railway_adapter
import automation_v3_remote_worker as remote_worker


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _paper_env(monkeypatch):
    values = {
        "BOTS_V3_PRODUCTION_AUTHORITY": "false",
        "TRADING_ENVIRONMENT": "PAPER",
        "PRIMARY_OANDA_ENV": "practice",
        "OANDA": railway_adapter.PRACTICE_OANDA_URL,
        "RAILWAY_TOKEN": "railway-secret",
        "BOTS_V3_INSTRUMENT": "AUD_USD",
        "BOTS_V3_EXPECTED_SHA": "a" * 40,
        "BOTS_V3_RAILWAY_PROJECT_ID": "project",
        "BOTS_V3_RAILWAY_ENVIRONMENT_ID": "environment",
        "BOTS_V3_RAILWAY_SERVICE_ID": "service",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return values


def test_remote_trigger_accepts_supported_instrument():
    assert "AUD_USD" in remote_worker.SUPPORTED_INSTRUMENTS


def test_unsupported_instrument_rejected(monkeypatch):
    monkeypatch.setattr(remote_worker, "_require_remote_boundary", lambda: None)
    with pytest.raises(ValueError, match="unsupported instrument"):
        remote_worker.run_worker("AUD_USD;rm -rf /")


def test_no_arbitrary_shell_input():
    parser_values = set(remote_worker.SUPPORTED_INSTRUMENTS)
    assert all(" " not in value and ";" not in value and "/" not in value for value in parser_values)


def test_worker_starts_v3_correctly(monkeypatch, tmp_path):
    monkeypatch.setenv("GH_TOKEN", "g")
    monkeypatch.setenv("RAILWAY_TOKEN", "r")
    monkeypatch.setenv("OANDA_TOKEN", "o")
    monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY", "false")
    monkeypatch.setenv("TRADING_ENVIRONMENT", "PAPER")
    monkeypatch.setenv("PRIMARY_OANDA_ENV", "practice")
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "state"))
    monkeypatch.setenv("BOTS_V3_REMOTE_STATUS_PATH", str(tmp_path / "status.json"))
    seen = {}

    class P:
        returncode = 0
        def poll(self): return 0
        def communicate(self): return (json.dumps({"status": "NO_VALID_CANDIDATE"}), "")

    def popen(cmd, **kwargs):
        seen["cmd"] = cmd
        return P()

    monkeypatch.setattr(remote_worker.subprocess, "Popen", popen)
    result = remote_worker.run_worker("AUD_USD")
    assert seen["cmd"][-2:] == [str(Path(remote_worker.__file__).resolve().parent / "autonomous_asset_optimizer.py"), "AUD_USD"]
    assert result["terminal_state"] == "NO_VALID_CANDIDATE"


def test_worker_state_persistence(tmp_path):
    path = tmp_path / "status.json"
    remote_worker._write_json(path, {"run_id": "1", "production_authority": False})
    assert json.loads(path.read_text())["run_id"] == "1"


def test_code_change_adapter_unavailable_fails_closed(monkeypatch, tmp_path):
    plan = tmp_path / "plan.json"
    plan.write_text(json.dumps({"instrument": "AUD_USD", "source_code_sha": "b" * 40, "production_authority": False}))
    monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY", "false")
    with pytest.raises(ValueError, match="not PAPER deployable"):
        code_adapter.apply_release_plan(tmp_path, plan, base_sha="b" * 40, instrument="AUD_USD")


def test_code_change_adapter_success_path(monkeypatch, tmp_path):
    target = tmp_path / "managed_strategy_rules.py"
    old = 'MANAGED_RULES_JSON["AUD_USD"] = "[]"\n'
    target.write_text(old, encoding="utf-8")
    plan = tmp_path / "plan.json"
    artifact = tmp_path / "x.json"; artifact.write_text('{}\n')
    identity={"code_sha":"c"*40}; definition_sha="d"*64; candidate_id="candidate"
    binding={"instrument":"AUD_USD","source_code_sha":"c"*40,"dataset_identity":identity,"dataset_identity_sha256":code_adapter._canonical_sha256(identity),"candidate_id":candidate_id,"candidate_definition_sha256":definition_sha,"freeze_sha256":_sha(artifact),"holdout_sha256":_sha(artifact),"audit_sha256":_sha(artifact),"pre_audit_sha256":_sha(artifact),"audit_verdict":"ACCEPT","retuning_after_holdout":False}
    new='MANAGED_RULES_JSON["AUD_USD"] = "candidate dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"\n'
    payload={"schema_version":2,"status":"PAPER_DEPLOYABLE_CANDIDATE","instrument":"AUD_USD","candidate_id":candidate_id,"candidate_definition_sha256":definition_sha,"source_code_sha":"c"*40,"dataset_identity":identity,"production_authority":False,"freeze_artifact":{"path":"x.json","sha256":_sha(artifact)},"holdout_artifact":{"path":"x.json","sha256":_sha(artifact)},"audit_artifact":{"path":"x.json","sha256":_sha(artifact)},"pre_audit":{"path":"x.json","sha256":_sha(artifact),"verdict":"ACCEPT"},"code_changes":[{"operation":"replace_text","path":"managed_strategy_rules.py","expected_file_sha256":_sha(target),"old_text":old,"new_text":new,"expected_occurrences":1,"evidence_binding":binding}]}
    payload["plan_binding_sha256"]=code_adapter._canonical_sha256(payload);plan.write_text(json.dumps(payload))
    monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY", "false")
    assert code_adapter.apply_release_plan(tmp_path, plan, base_sha="c"*40, instrument="AUD_USD")==["managed_strategy_rules.py"]

def test_paper_deploy_env_validation(monkeypatch):
    _paper_env(monkeypatch)
    assert railway_adapter._validate_common()["instrument"] == "AUD_USD"


def test_live_env_rejected(monkeypatch):
    _paper_env(monkeypatch)
    monkeypatch.setenv("PRIMARY_OANDA_ENV", "live")
    with pytest.raises(ValueError, match="practice"):
        railway_adapter._validate_common()


def test_verify_exact_expected_sha(monkeypatch, tmp_path):
    _paper_env(monkeypatch)
    state = tmp_path / "deploy.json"
    state.write_text(json.dumps({"deployment_id": "d1", "expected_sha": "a" * 40, "instrument": "AUD_USD"}))
    monkeypatch.setenv("BOTS_V3_REMOTE_DEPLOY_STATE", str(state))
    monkeypatch.setenv("BOTS_V3_HEALTHCHECK_URL", "https://example.invalid/")
    monkeypatch.setattr(railway_adapter, "_deployment_list", lambda ctx: [{"id": "d1", "status": "SUCCESS"}])
    monkeypatch.setattr(railway_adapter, "_variables", lambda ctx: {"TRADING_ENVIRONMENT":"PAPER","PRIMARY_OANDA_ENV":"practice","INSTRUMENTS":"AUD_USD","OANDA_TOKEN":"x","OANDA_ACCOUNT_ID":"y"})
    monkeypatch.setattr(railway_adapter, "_check_service_http", lambda: None)
    monkeypatch.setattr(railway_adapter, "_check_oanda", lambda vars_map: None)
    def run(cmd, **kwargs):
        if cmd[:3] == ["git", "rev-parse", "HEAD"]: return SimpleNamespace(returncode=0, stdout="a"*40+"\n", stderr="")
        if cmd[:3] == ["git", "status", "--porcelain"]: return SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(returncode=0, stdout="{}", stderr="")
    monkeypatch.setattr(railway_adapter, "_run", run)
    assert railway_adapter.verify(tmp_path) == 0


def test_deploy_failure_produces_failure(monkeypatch, tmp_path):
    _paper_env(monkeypatch)
    monkeypatch.setenv("BOTS_V3_REMOTE_DEPLOY_STATE", str(tmp_path / "d.json"))
    monkeypatch.setattr(railway_adapter, "_deployment_list", lambda ctx: [])
    def run(cmd, **kwargs):
        if cmd[:3] == ["git", "rev-parse", "HEAD"]: return SimpleNamespace(returncode=0, stdout="a"*40+"\n", stderr="")
        if cmd[:3] == ["git", "status", "--porcelain"]: return SimpleNamespace(returncode=0, stdout="", stderr="")
        if cmd[:2] == ["railway", "up"]: return SimpleNamespace(returncode=1, stdout="", stderr="boom")
        return SimpleNamespace(returncode=0, stdout="", stderr="")
    monkeypatch.setattr(railway_adapter, "_run", run)
    with pytest.raises(RuntimeError, match="deploy command failed"):
        railway_adapter.deploy(tmp_path)


def test_verify_failure_has_no_unsafe_rollback(monkeypatch):
    assert "rollback" not in railway_adapter.__dict__


def test_missing_github_auth_explicit_failure(monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.setenv("RAILWAY_TOKEN", "r")
    monkeypatch.setenv("OANDA_TOKEN", "o")
    monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY", "false")
    monkeypatch.setenv("TRADING_ENVIRONMENT", "PAPER")
    monkeypatch.setenv("PRIMARY_OANDA_ENV", "practice")
    with pytest.raises(RuntimeError, match="GITHUB_AUTH_UNAVAILABLE"):
        remote_worker._require_remote_boundary()


def test_missing_railway_auth_explicit_failure(monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "g")
    monkeypatch.delenv("RAILWAY_TOKEN", raising=False)
    monkeypatch.setenv("OANDA_TOKEN", "o")
    monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY", "false")
    monkeypatch.setenv("TRADING_ENVIRONMENT", "PAPER")
    monkeypatch.setenv("PRIMARY_OANDA_ENV", "practice")
    with pytest.raises(RuntimeError, match="RAILWAY_AUTH_UNAVAILABLE"):
        remote_worker._require_remote_boundary()


def test_no_secrets_in_adapter_logs(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("OANDA_TOKEN", "supersecret")
    monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY", "false")
    plan=tmp_path/"plan.json";plan.write_text(json.dumps({"schema_version":2,"status":"PAPER_DEPLOYABLE_CANDIDATE","instrument":"AUD_USD","source_code_sha":"d"*40,"production_authority":False}))
    with pytest.raises(ValueError): code_adapter.apply_release_plan(tmp_path,plan,base_sha="d"*40,instrument="AUD_USD")
    assert "supersecret" not in capsys.readouterr().out

def test_no_datasets_in_git_policy():
    assert ".db" in code_adapter.BLOCKED_SUFFIXES and ".zip" in code_adapter.BLOCKED_SUFFIXES


def test_service_isolation_from_main_process():
    assert "server.py" in code_adapter.PROTECTED_FILES
    assert "forward_experiment.py" in code_adapter.PROTECTED_FILES


def test_worker_can_terminate_after_job():
    assert "CANDIDATE_NOT_DEPLOYABLE" in remote_worker.EXPECTED_TERMINALS and "PAPER_DEPLOYED" in remote_worker.EXPECTED_TERMINALS


def test_retrigger_terminal_status_is_readable(tmp_path):
    root = tmp_path / "research"
    ledger = root / "AUD_USD" / "autonomous_v3" / "automation_v3_state.json"
    ledger.parent.mkdir(parents=True)
    ledger.write_text(json.dumps({"runs":{"AUD_USD":{"status":"PAPER_DEPLOYED","production_authority":False}}}))
    snap = remote_worker._snapshot(root, "AUD_USD", "22")
    assert snap["terminal_state"] == "PAPER_DEPLOYED"
    assert snap["production_authority"] is False


def test_remote_status_readable(tmp_path):
    path = tmp_path / "status.json"
    payload = {"run_id":"x","instrument":"AUD_USD","current_stage":"discovery","lookback":3,"terminal_state":None,"candidate":None,"paper_deployment_status":None,"last_error":None,"production_authority":False}
    remote_worker._write_json(path, payload)
    assert json.loads(path.read_text()) == payload


def test_code_adapter_rejects_protected_file(monkeypatch, tmp_path):
    target=tmp_path/"managed_strategy_rules.py";target.write_text('MANAGED_RULES_JSON["AUD_USD"] = "[]"\n')
    plan=tmp_path/"plan.json";identity={"code_sha":"e"*40};artifact=tmp_path/"x.json";artifact.write_text('{}\n')
    binding={"instrument":"AUD_USD","source_code_sha":"e"*40,"dataset_identity":identity,"dataset_identity_sha256":code_adapter._canonical_sha256(identity),"candidate_id":"c","candidate_definition_sha256":"d"*64,"freeze_sha256":_sha(artifact),"holdout_sha256":_sha(artifact),"audit_sha256":_sha(artifact),"pre_audit_sha256":_sha(artifact),"audit_verdict":"ACCEPT","retuning_after_holdout":False}
    payload={"schema_version":2,"status":"PAPER_DEPLOYABLE_CANDIDATE","instrument":"AUD_USD","candidate_id":"c","candidate_definition_sha256":"d"*64,"source_code_sha":"e"*40,"dataset_identity":identity,"production_authority":False,"freeze_artifact":{"path":"x.json","sha256":_sha(artifact)},"holdout_artifact":{"path":"x.json","sha256":_sha(artifact)},"audit_artifact":{"path":"x.json","sha256":_sha(artifact)},"pre_audit":{"path":"x.json","sha256":_sha(artifact),"verdict":"ACCEPT"},"code_changes":[{"operation":"replace_text","path":"server.py","expected_file_sha256":_sha(target),"old_text":"x","new_text":"y","expected_occurrences":1,"evidence_binding":binding}]}
    payload["plan_binding_sha256"]=code_adapter._canonical_sha256(payload);plan.write_text(json.dumps(payload));monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY","false")
    with pytest.raises(ValueError,match="approved Automation V3 managed surface"):code_adapter.apply_release_plan(tmp_path,plan,base_sha="e"*40,instrument="AUD_USD")



def test_snapshot_surfaces_review_diagnostics_on_scientific_terminal(tmp_path, monkeypatch):
    import automation_v3_remote_worker as worker
    root = tmp_path
    base = root / "GBP_USD" / "autonomous_v3"
    base.mkdir(parents=True)
    sha = "c" * 40
    payload = {
        "runs": {"GBP_USD": {
            "instrument": "GBP_USD", "status": "NO_VALID_CANDIDATE", "final_outcome": "NO_VALID_CANDIDATE",
            "code_sha": sha, "lookback_attempts": [{"months": 1, "code_sha": sha}],
            "mode": "REVIEW_BEFORE_HOLDOUT_DEPLOY",
            "incumbent_metrics": {"resolved_binary": 46, "win_rate": 0.3478, "expectancy_r": -0.255, "profit_factor": 0.651},
            "diagnostic_top_candidates": [{"rank": 1, "candidate_id": "x", "deployment_eligible": False}],
            "deployable_candidates": [], "review_shortlist_sha256": "a" * 64,
            "production_authority": False,
        }}
    }
    worker._write_json(base / "automation_v3_state.json", payload)
    monkeypatch.delenv("GITHUB_WORKFLOW", raising=False)
    snap = worker._snapshot(root, "GBP_USD", "33938856618")
    assert snap["terminal_state"] == "NO_VALID_CANDIDATE"
    assert snap["incumbent_metrics"]["resolved_binary"] == 46
    assert len(snap["diagnostic_top_candidates"]) == 1
    assert snap["deployable_candidates"] == []
    assert snap["shortlist_sha256"] == "a" * 64
    assert snap["paper_deployment_status"] is None
    assert snap["production_authority"] is False



def test_snapshot_surfaces_diagnostic_review_identity_without_shortlist(tmp_path, monkeypatch):
    import automation_v3_remote_worker as worker
    root = tmp_path
    base = root / "GBP_USD" / "autonomous_v3"
    base.mkdir(parents=True)
    sha = "d" * 40
    worker._write_json(base / "automation_v3_state.json", {
        "runs": {"GBP_USD": {
            "instrument": "GBP_USD", "status": "NO_VALID_CANDIDATE", "final_outcome": "NO_VALID_CANDIDATE",
            "code_sha": sha, "lookback_attempts": [{"months": 1, "code_sha": sha}],
            "mode": "REVIEW_BEFORE_HOLDOUT_DEPLOY", "incumbent_metrics": {"resolved_binary": 46},
            "diagnostic_top_candidates": [{"rank": 1, "deployment_eligible": False}],
            "deployable_candidates": [], "review_shortlist_sha256": None,
            "diagnostic_review_sha256": "a" * 64, "production_authority": False,
        }}
    })
    monkeypatch.setattr(worker, "_current_checkout_sha", lambda: sha)
    snap = worker._snapshot(root, "GBP_USD", "33940485772")
    assert snap["terminal_state"] == "NO_VALID_CANDIDATE"
    assert snap["shortlist_sha256"] is None
    assert snap["diagnostic_review_sha256"] == "a" * 64
    assert snap["deployable_candidates"] == []
    assert snap["production_authority"] is False

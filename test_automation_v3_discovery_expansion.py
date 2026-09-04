import json

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

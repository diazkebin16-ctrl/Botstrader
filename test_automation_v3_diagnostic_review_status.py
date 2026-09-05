import json

import automation_v3_modes as modes


def _env(monkeypatch, tmp_path):
    monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY", "false")
    monkeypatch.setenv("TRADING_ENVIRONMENT", "PAPER")
    monkeypatch.setenv("PRIMARY_OANDA_ENV", "practice")
    monkeypatch.setenv("OANDA_TOKEN", "x")
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "research"))
    monkeypatch.setenv("BOTS_V3_REMOTE_STATUS_PATH", str(tmp_path / "remote_status.json"))
    monkeypatch.setenv("GITHUB_RUN_ID", "33941672359")
    return tmp_path / "remote_status.json"


def test_review_remote_status_exposes_exact_diagnostic_review_hash(monkeypatch, tmp_path):
    status_path = _env(monkeypatch, tmp_path)
    digest = "5d6d39c05217807080d4ae6af25d3be5543a792b1ec987f974237f49aa771b01"
    result = {
        "status": "NO_VALID_CANDIDATE",
        "diagnostic_review_sha256": digest,
        "review_shortlist_sha256": None,
        "incumbent_metrics": {"resolved_binary": 46, "wins": 16, "losses": 30},
        "diagnostic_top_candidates": [{"rank": 1}, {"rank": 2}, {"rank": 3}],
        "deployable_candidates": [],
        "holdout_opened": False,
        "production_authority": False,
    }
    monkeypatch.setattr(modes.ReviewBeforeHoldoutOptimizer, "optimize", lambda self, instrument: result)
    returned = modes.execute_request({"instrument": "GBP_USD", "mode": modes.REVIEW_BEFORE_HOLDOUT_DEPLOY}, repo=tmp_path)
    assert returned is result
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["terminal_state"] == "NO_VALID_CANDIDATE"
    assert status["diagnostic_review_sha256"] == digest
    assert len(status["diagnostic_review_sha256"]) == 64
    assert status["shortlist_sha256"] is None
    assert status["incumbent_metrics"] is not None
    assert len(status["diagnostic_top_candidates"]) == 3
    assert status["deployable_candidates"] == []
    assert status["paper_deployment_status"] is None
    assert status["production_authority"] is False


def test_review_remote_status_null_hash_when_no_review_identity(monkeypatch, tmp_path):
    status_path = _env(monkeypatch, tmp_path)
    result = {
        "status": "NO_VALID_CANDIDATE",
        "review_shortlist_sha256": None,
        "diagnostic_top_candidates": [],
        "deployable_candidates": [],
        "production_authority": False,
    }
    monkeypatch.setattr(modes.ReviewBeforeHoldoutOptimizer, "optimize", lambda self, instrument: result)
    modes.execute_request({"instrument": "GBP_USD", "mode": modes.REVIEW_BEFORE_HOLDOUT_DEPLOY}, repo=tmp_path)
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["diagnostic_review_sha256"] is None
    assert status["shortlist_sha256"] is None
    assert status["production_authority"] is False


def test_review_report_build_failed_does_not_invent_hash(monkeypatch, tmp_path):
    status_path = _env(monkeypatch, tmp_path)
    result = {
        "status": "REVIEW_REPORT_BUILD_FAILED",
        "review_report_error": {"code": "REVIEW_REPORT_BUILD_FAILED", "reason": "synthetic"},
        "production_authority": False,
    }
    monkeypatch.setattr(modes.ReviewBeforeHoldoutOptimizer, "optimize", lambda self, instrument: result)
    modes.execute_request({"instrument": "GBP_USD", "mode": modes.REVIEW_BEFORE_HOLDOUT_DEPLOY}, repo=tmp_path)
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["diagnostic_review_sha256"] is None
    assert status["shortlist_sha256"] is None
    assert status["production_authority"] is False


def test_full_auto_path_remains_delegated_to_remote_worker(monkeypatch, tmp_path):
    _env(monkeypatch, tmp_path)
    import automation_v3_remote_worker
    expected = {"terminal_state": "NO_VALID_CANDIDATE", "production_authority": False}
    monkeypatch.setattr(automation_v3_remote_worker, "run_worker", lambda instrument: expected)
    result = modes.execute_request({"instrument": "GBP_USD", "mode": modes.FULL_AUTO_TO_PAPER}, repo=tmp_path)
    assert result is expected

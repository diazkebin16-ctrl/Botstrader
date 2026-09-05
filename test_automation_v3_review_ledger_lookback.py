import json
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

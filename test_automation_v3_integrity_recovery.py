import json
from datetime import datetime, timezone
from pathlib import Path

from automation_v3_integrity_recovery import build_integrity_diagnostic


SHA = "a" * 40


def _integrity(failures, *, bid_ask=True, midpoint=False, non_weekend=0):
    return {
        "status": "FAIL",
        "stage": "data_integrity",
        "instrument": "AUD_USD",
        "start": "2026-08-01T00:00:00+00:00",
        "end": "2026-09-01T00:00:00+00:00",
        "warmup_days": 10,
        "horizon_minutes": 240,
        "bid_ask_real": bid_ask,
        "midpoint_only": midpoint,
        "gaps_present": bool(non_weekend),
        "failures": failures,
        "coverage": {"H1": {"warmup_covered": False, "horizon_covered": False}},
        "timeframes": {
            "H1": {"count": 10, "first": "2026-07-25T00:00:00+00:00", "last": "2026-09-01T04:00:00+00:00", "non_weekend_gaps": 0},
            "M15": {"count": 10, "first": "2026-07-25T00:00:00+00:00", "last": "2026-09-01T04:00:00+00:00", "non_weekend_gaps": 0},
            "M5": {"count": 10, "first": "2026-07-25T00:00:00+00:00", "last": "2026-09-01T04:00:00+00:00", "non_weekend_gaps": 0},
            "M1": {"count": 10, "first": "2026-07-25T00:00:00+00:00", "last": "2026-09-01T04:00:00+00:00", "non_weekend_gaps": non_weekend},
        },
    }


class FakeData:
    def __init__(self):
        self.calls = []

    async def acquire(self, instrument, start, end, cache, **kw):
        self.calls.append({"instrument": instrument, "start": start, "end": end, "kw": kw})
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text("{}", encoding="utf-8")
        return cache


class FakeManager:
    def __init__(self, state, ledger):
        self.state = state
        self.ledger = ledger

    def register_asset(self, *args, **kwargs):
        return {"dataset_identity": "d"}

    def approve_phase1_autonomous(self, *args, **kwargs):
        raise AssertionError("phase1 approval not expected")


class FakeStage:
    def __init__(self, name, artifact):
        self.name = name
        self.artifact = artifact
        self.command = ("fake", name)


def fake_stages(**kw):
    names = (
        "data_integrity", "replay", "target_population", "phase_1", "phase_2", "discovery",
        "discovery_repeat", "determinism", "freeze", "holdout", "audit", "report", "pre_audit", "prompts",
    )
    return [FakeStage(name, kw["workspace"] / f"{idx:02d}_{name}.json") for idx, name in enumerate(names, 1)]


class FakeRelease:
    def __init__(self):
        self.deploys = 0

    def prepare_test_merge(self, **kwargs):
        return {"status": "PASS", "merged_main_sha": "paper-sha"}

    def deploy_paper(self, **kwargs):
        self.deploys += 1
        return {"status": "PAPER_DEPLOYED", "production_authority": False}


class IntegrityScenarioCascade:
    def __init__(self, manager, scenarios, calls):
        self.scenarios = scenarios
        self.calls = calls

    def run(self, instrument, stages, through="prompts"):
        wd = stages[0].artifact.parent
        months = int(wd.name.split("_")[1][:-1])
        attempt = self.calls.get(months, 0)
        self.calls[months] = attempt + 1
        sequence = self.scenarios.get(months, [None])
        mode = sequence[min(attempt, len(sequence) - 1)]
        if mode:
            failures = {
                "coverage": ["H1_WARMUP_COVERAGE_INCOMPLETE", "M1_HORIZON_COVERAGE_INCOMPLETE"],
                "warmup": ["H1_WARMUP_COVERAGE_INCOMPLETE"],
                "horizon": ["M1_HORIZON_COVERAGE_INCOMPLETE"],
                "missing": ["M5_MISSING"],
                "midpoint": ["M1_MIDPOINT_ONLY_OR_INCOMPLETE_BID_ASK"],
                "bidask": ["M1_INVALID_BID_ASK_OHLC"],
                "gaps": ["M1_NON_WEEKEND_GAPS"],
                "instrument": ["CROSS_ASSET_DATASET_CONTAMINATION"],
            }[mode]
            report = _integrity(
                failures,
                bid_ask=mode not in {"midpoint", "bidask"},
                midpoint=mode == "midpoint",
                non_weekend=4 if mode == "gaps" else 0,
            )
            stages[0].artifact.write_text(json.dumps(report), encoding="utf-8")
            raise RuntimeError("dataset integrity gate did not pass")
        (wd / "10_holdout.json").write_text(json.dumps({
            "status": "PASS", "overfitting_risk": {"severity": "LOW"},
            "candidate_ranking": [{"status": "RESEARCH_CANDIDATE", "candidate_id": "c"}],
        }), encoding="utf-8")
        (wd / "13_pre_audit.json").write_text(json.dumps({"verdict": "ACCEPT"}), encoding="utf-8")
        return {"status": "COMPLETED"}


def _optimizer(tmp_path, scenarios):
    from autonomous_asset_optimizer import AutonomousAssetOptimizer

    repo = tmp_path / "repo"
    repo.mkdir()
    data = FakeData()
    release = FakeRelease()
    cascade_calls = {}
    optimizer = AutonomousAssetOptimizer(
        repo,
        data_source=data,
        release=release,
        now=lambda: datetime(2026, 9, 3, tzinfo=timezone.utc),
        cascade_factory=lambda manager: IntegrityScenarioCascade(manager, scenarios, cascade_calls),
        manager_factory=FakeManager,
        stage_builder=fake_stages,
        code_sha_provider=lambda: SHA,
    )
    return optimizer, data, release, cascade_calls


def _run(tmp_path, monkeypatch, scenarios):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "research"))
    optimizer, data, release, calls = _optimizer(tmp_path, scenarios)
    return optimizer.optimize("AUD_USD"), data, release, calls


def test_incomplete_coverage_auto_reacquire(tmp_path, monkeypatch):
    out, data, _, calls = _run(tmp_path, monkeypatch, {1: ["coverage", None]})
    assert out["status"] == "PAPER_DEPLOYED" and len(data.calls) == 2 and calls[1] == 2
    assert data.calls[-1]["kw"]["boundary_buffer_days"] == 3


def test_missing_warmup_auto_reacquire(tmp_path, monkeypatch):
    out, data, _, _ = _run(tmp_path, monkeypatch, {1: ["warmup", None]})
    assert out["status"] == "PAPER_DEPLOYED" and len(data.calls) == 2


def test_missing_horizon_auto_reacquire(tmp_path, monkeypatch):
    out, data, _, _ = _run(tmp_path, monkeypatch, {1: ["horizon", None]})
    assert out["status"] == "PAPER_DEPLOYED" and len(data.calls) == 2


def test_stale_cache_deleted_rebuilt_and_retried(tmp_path, monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT", str(tmp_path / "research"))
    optimizer, data, _, _ = _optimizer(tmp_path, {1: ["coverage", None]})
    cache = tmp_path / "research" / "AUD_USD" / "autonomous_v3" / "data" / "AUD_USD_01m.json"
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text("stale", encoding="utf-8")
    out = optimizer.optimize("AUD_USD")
    assert out["status"] == "PAPER_DEPLOYED" and len(data.calls) == 1
    history = json.loads((tmp_path / "research" / "AUD_USD" / "autonomous_v3" / "automation_v3_state.json").read_text())
    decisions = history["runs"]["AUD_USD"]["decision_history"]
    assert any("STALE_OR_PARTIAL_CACHE" in (item.get("diagnostic") or {}).get("failure_classes", []) for item in decisions)


def test_successful_retry_continues_cascade(tmp_path, monkeypatch):
    out, _, release, _ = _run(tmp_path, monkeypatch, {1: ["missing", None]})
    assert out["status"] == "PAPER_DEPLOYED" and release.deploys == 1


def test_repeated_recoverable_failure_expands_lookback(tmp_path, monkeypatch):
    out, data, _, calls = _run(tmp_path, monkeypatch, {1: ["coverage", "coverage"], 3: [None]})
    assert out["status"] == "PAPER_DEPLOYED" and calls[1] == 2 and calls[3] == 1 and len(data.calls) == 3


def test_twelve_month_repeated_coverage_has_safe_terminal(tmp_path, monkeypatch):
    out, data, _, calls = _run(tmp_path, monkeypatch, {
        1: ["coverage", "coverage"], 3: ["coverage", "coverage"],
        6: ["coverage", "coverage"], 12: ["coverage", "coverage"],
    })
    assert out["status"] == "DATA_COVERAGE_INSUFFICIENT" and calls[12] == 2 and len(data.calls) == 8
    assert out["production_authority"] is False


def test_midpoint_only_hard_block(tmp_path, monkeypatch):
    out, data, _, _ = _run(tmp_path, monkeypatch, {1: ["midpoint"]})
    assert out["status"] == "DATA_INTEGRITY_FAILED" and len(data.calls) == 1


def test_bid_ask_invalid_hard_block(tmp_path, monkeypatch):
    out, _, _, _ = _run(tmp_path, monkeypatch, {1: ["bidask"]})
    assert out["status"] == "DATA_INTEGRITY_FAILED"


def test_non_weekend_gaps_hard_block(tmp_path, monkeypatch):
    out, _, _, _ = _run(tmp_path, monkeypatch, {1: ["gaps"]})
    assert out["status"] == "DATA_INTEGRITY_FAILED"


def test_instrument_mismatch_hard_block(tmp_path, monkeypatch):
    out, _, _, _ = _run(tmp_path, monkeypatch, {1: ["instrument"]})
    assert out["status"] == "DATA_INTEGRITY_FAILED"


def test_remote_status_includes_exact_failed_checks(tmp_path):
    from automation_v3_remote_worker import _snapshot

    root = tmp_path / "root"
    state = root / "AUD_USD" / "autonomous_v3" / "automation_v3_state.json"
    state.parent.mkdir(parents=True)
    diagnostic = {"failed_checks": ["M1_HORIZON_COVERAGE_INCOMPLETE"], "recommended_action": "EXPAND_LOOKBACK", "production_authority": False}
    state.write_text(json.dumps({"runs": {"AUD_USD": {"status": "DATA_COVERAGE_INSUFFICIENT", "code_sha": SHA, "lookback_attempts": [{"months": 12}], "integrity_diagnostic": diagnostic, "production_authority": False}}}), encoding="utf-8")
    snap = _snapshot(root, "AUD_USD", "run-1")
    assert snap["integrity_diagnostic"]["failed_checks"] == ["M1_HORIZON_COVERAGE_INCOMPLETE"]
    assert snap["integrity_diagnostic"]["recommended_action"] == "EXPAND_LOOKBACK"


def test_integrity_diagnostic_redacts_secret_like_values(tmp_path):
    artifact = tmp_path / "01_data_integrity.json"
    report = _integrity(["H1_WARMUP_COVERAGE_INCOMPLETE"])
    artifact.write_text(json.dumps(report), encoding="utf-8")
    diag = build_integrity_diagnostic(
        report, artifact_path=artifact, cache_path=tmp_path / "OANDA_TOKEN=supersecret.json",
        requested_start="2026-08-01T00:00:00+00:00", requested_end="2026-09-01T00:00:00+00:00",
    )
    rendered = json.dumps(diag)
    assert "supersecret" not in rendered and "OANDA_TOKEN=" not in rendered


def test_production_authority_false_preserved(tmp_path):
    artifact = tmp_path / "01_data_integrity.json"
    report = _integrity(["M1_HORIZON_COVERAGE_INCOMPLETE"])
    artifact.write_text(json.dumps(report), encoding="utf-8")
    diag = build_integrity_diagnostic(
        report, artifact_path=artifact, cache_path=tmp_path / "cache.json",
        requested_start="2026-08-01T00:00:00+00:00", requested_end="2026-09-01T00:00:00+00:00",
    )
    assert diag["production_authority"] is False

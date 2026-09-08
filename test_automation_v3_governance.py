import hashlib
import json
import urllib.request
import zipfile

import pytest

from automation_v3_governance import (
    complete_holdout_record,
    holdout_population_identity,
    ledger_path,
    load_consumption_ledger,
    merge_recovered_holdout,
    record_holdout_opening,
)
import automation_v3_state_recovery as recovery
from automation_v3_state_recovery import recover_archive


def _evidence(code_sha="a" * 40, data_sha="d" * 64):
    dataset = {
        "instrument": "GBP_USD",
        "code_sha": code_sha,
        "input_sha256": data_sha,
        "start": "2026-08-01T00:00:00+00:00",
        "research_end": "2026-09-01T00:00:00+00:00",
        "required_horizon_end": "2026-09-01T04:00:00+00:00",
        "horizon_minutes": 240,
    }
    target = {
        "instrument": "GBP_USD",
        "start": dataset["start"],
        "end": dataset["research_end"],
        "dataset_identity": dataset,
    }
    phase2 = {
        "instrument": "GBP_USD",
        "dataset_identity": dataset,
        "partition_config": {"horizon_minutes": 240, "embargo_minutes": 30},
    }
    return target, phase2


def test_population_identity_ignores_code_but_not_new_market_data():
    target_a, phase2_a = _evidence(code_sha="a" * 40)
    target_b, phase2_b = _evidence(code_sha="b" * 40)
    first, _ = holdout_population_identity(target_a, phase2_a)
    second, _ = holdout_population_identity(target_b, phase2_b)
    assert first == second

    target_c, phase2_c = _evidence(code_sha="b" * 40, data_sha="e" * 64)
    third, _ = holdout_population_identity(target_c, phase2_c)
    assert third != first


def test_ledger_is_written_before_open_and_blocks_second_candidate(tmp_path):
    target, phase2 = _evidence()
    population, material = holdout_population_identity(target, phase2)
    path = ledger_path(tmp_path, "GBP_USD")
    opening = {
        "instrument": "GBP_USD",
        "holdout_population_identity_sha256": population,
        "holdout_population_identity": material,
        "candidate_definition_sha256": "1" * 64,
        "freeze_sha256": "2" * 64,
    }
    record_holdout_opening(path, opening)
    assert load_consumption_ledger(path, "GBP_USD")["records"][0]["status"] == "OPENING"
    with pytest.raises(ValueError, match="HOLDOUT_ALREADY_CONSUMED"):
        record_holdout_opening(path, {**opening, "candidate_definition_sha256": "3" * 64})

    complete_holdout_record(
        path,
        instrument="GBP_USD",
        population_identity=population,
        holdout_sha256="4" * 64,
        holdout_status="FAIL",
    )
    record = load_consumption_ledger(path, "GBP_USD")["records"][0]
    assert record["status"] == "OPENED"
    assert record["holdout_status"] == "FAIL"

    merge_recovered_holdout(path, {**opening, "status": "OPENING"})
    assert load_consumption_ledger(path, "GBP_USD")["records"][0]["status"] == "OPENED"


def test_recovery_imports_failed_holdout_and_is_idempotent(tmp_path):
    target, phase2 = _evidence()
    candidate_sha = "1" * 64
    freeze_sha = "2" * 64
    holdout = {
        "instrument": "GBP_USD",
        "status": "FAIL",
        "holdout_opened_once": True,
        "candidate_definition_sha256": candidate_sha,
        "freeze_sha256": freeze_sha,
        "production_authority": False,
    }
    freeze = {"candidate_definition_sha256": candidate_sha, "production_authority": False}
    archive_path = tmp_path / "artifact.zip"
    prefix = "GBP_USD/autonomous_v3/lookback_01m_a/review_release_x"
    holdout_bytes = (json.dumps(holdout, indent=2, sort_keys=True) + "\n").encode()
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(f"{prefix}/03_target_population.json", json.dumps(target))
        archive.writestr(f"{prefix}/05_phase_2.json", json.dumps(phase2))
        archive.writestr(f"{prefix}/09_freeze.json", json.dumps(freeze))
        archive.writestr(f"{prefix}/10_holdout.json", holdout_bytes)

    assert recover_archive(archive_path, root=tmp_path / "state", instrument="GBP_USD", artifact_id="42") == 1
    assert recover_archive(archive_path, root=tmp_path / "state", instrument="GBP_USD", artifact_id="42") == 1
    records = load_consumption_ledger(
        ledger_path(tmp_path / "state", "GBP_USD"), "GBP_USD"
    )["records"]
    assert len(records) == 1
    assert records[0]["holdout_status"] == "FAIL"
    assert records[0]["holdout_sha256"] == hashlib.sha256(holdout_bytes).hexdigest()
    assert records[0]["source_artifact_id"] == "42"


def test_recovery_rejects_cross_asset_evidence(tmp_path):
    target, phase2 = _evidence()
    holdout = {
        "instrument": "EUR_USD",
        "status": "FAIL",
        "holdout_opened_once": True,
        "candidate_definition_sha256": "1" * 64,
        "freeze_sha256": "2" * 64,
    }
    archive_path = tmp_path / "bad.zip"
    prefix = "GBP_USD/autonomous_v3/lookback/review_release_x"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(f"{prefix}/03_target_population.json", json.dumps(target))
        archive.writestr(f"{prefix}/05_phase_2.json", json.dumps(phase2))
        archive.writestr(
            f"{prefix}/09_freeze.json", json.dumps({"candidate_definition_sha256": "1" * 64})
        )
        archive.writestr(f"{prefix}/10_holdout.json", json.dumps(holdout))
    with pytest.raises(ValueError, match="cross-asset"):
        recover_archive(archive_path, root=tmp_path / "state", instrument="GBP_USD")


def test_recovery_preserves_opening_ledger_when_holdout_artifact_is_absent(tmp_path):
    target, phase2 = _evidence()
    population, material = holdout_population_identity(target, phase2)
    source = {
        "schema_version": 1,
        "instrument": "GBP_USD",
        "production_authority": False,
        "records": [{
            "instrument": "GBP_USD",
            "holdout_population_identity_sha256": population,
            "holdout_population_identity": material,
            "candidate_definition_sha256": "1" * 64,
            "freeze_sha256": "2" * 64,
            "status": "OPENING",
            "production_authority": False,
        }],
    }
    archive_path = tmp_path / "opening.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(
            "GBP_USD/autonomous_v3/holdout_consumption_ledger.json",
            json.dumps(source),
        )
    assert recover_archive(archive_path, root=tmp_path / "state", instrument="GBP_USD") == 1
    record = load_consumption_ledger(
        ledger_path(tmp_path / "state", "GBP_USD"), "GBP_USD"
    )["records"][0]
    assert record["status"] == "OPENING"
    with pytest.raises(ValueError, match="HOLDOUT_ALREADY_CONSUMED"):
        record_holdout_opening(
            ledger_path(tmp_path / "state", "GBP_USD"),
            {**record, "candidate_definition_sha256": "3" * 64},
        )


def test_workflow_uses_retry_unique_cache_and_recovery():
    workflow = open(".github/workflows/automation-v3-remote.yml", encoding="utf-8").read()
    key = "automation-v3-state-${{ env.INSTRUMENT }}-${{ github.run_id }}-${{ github.run_attempt }}"
    assert workflow.count(key) == 2
    assert "python automation_v3_state_recovery.py" in workflow


def test_artifact_redirect_drops_authorization_on_cross_host():
    handler = recovery._ArtifactRedirectHandler()
    request = urllib.request.Request(
        "https://api.github.com/repos/o/r/actions/artifacts/1/zip",
        headers={"Authorization": "Bearer secret", "User-Agent": "test"},
    )
    redirected = handler.redirect_request(
        request, None, 302, "Found", {}, "https://signed.example.invalid/artifact.zip"
    )
    assert redirected.get_header("Authorization") is None
    assert redirected.get_header("User-agent") == "test"


def test_recover_latest_skips_newer_artifact_without_governed_evidence(tmp_path, monkeypatch):
    empty = tmp_path / "empty.zip"
    with zipfile.ZipFile(empty, "w") as archive:
        archive.writestr("GBP_USD/autonomous_v3/automation_v3_state.json", "{}")
    governed = tmp_path / "governed.zip"
    target, phase2 = _evidence()
    candidate_sha = "1" * 64
    prefix = "GBP_USD/autonomous_v3/lookback/review_release_x"
    with zipfile.ZipFile(governed, "w") as archive:
        archive.writestr(f"{prefix}/03_target_population.json", json.dumps(target))
        archive.writestr(f"{prefix}/05_phase_2.json", json.dumps(phase2))
        archive.writestr(
            f"{prefix}/09_freeze.json", json.dumps({"candidate_definition_sha256": candidate_sha})
        )
        archive.writestr(f"{prefix}/10_holdout.json", json.dumps({
            "instrument": "GBP_USD", "status": "FAIL", "holdout_opened_once": True,
            "candidate_definition_sha256": candidate_sha, "freeze_sha256": "2" * 64,
        }))
    listing = {
        "artifacts": [
            {"id": 2, "name": "automation-v3-GBP_USD-new", "created_at": "2026-09-08", "expired": False, "archive_download_url": "new"},
            {"id": 1, "name": "automation-v3-GBP_USD-old", "created_at": "2026-09-07", "expired": False, "archive_download_url": "old"},
        ]
    }
    monkeypatch.setattr(recovery, "_request_json", lambda *_args: listing)
    payloads = {2: empty.read_bytes(), 1: governed.read_bytes()}
    monkeypatch.setattr(recovery, "_download_artifact", lambda artifact, _token: payloads[artifact["id"]])
    assert recovery.recover_latest(
        root=tmp_path / "state", instrument="GBP_USD", repository="o/r", token="token"
    ) == 1
    records = load_consumption_ledger(
        ledger_path(tmp_path / "state", "GBP_USD"), "GBP_USD"
    )["records"]
    assert records[0]["source_artifact_id"] == "1"

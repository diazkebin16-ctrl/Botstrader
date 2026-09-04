import json

import pytest

from research_manager import PHASES, ResearchManager, sha256_file


def test_register_and_update_phase_with_artifact_hash(tmp_path):
    state_path = tmp_path / "state.json"
    artifact = tmp_path / "replay.json"
    artifact.write_text('{"status":"OK"}\n', encoding="utf-8")
    manager = ResearchManager(state_path)

    asset = manager.register_asset(
        "aud_usd", code_sha="abc123", start="2026-07-29T00:00:00Z",
        end="2026-08-29T00:00:00Z", warmup_days=10, horizon_minutes=240,
    )
    assert tuple(asset["phases"]) == PHASES
    assert asset["provenance"]["code_sha"] == "abc123"

    record = manager.update_phase("AUD_USD", "replay", "COMPLETED", artifact=str(artifact))
    assert record["artifact_sha256"] == sha256_file(artifact)
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["assets"]["AUD_USD"]["phases"]["replay"]["status"] == "COMPLETED"


def test_preserves_but_invalidates_candidates_when_asset_identity_changes(tmp_path):
    manager = ResearchManager(tmp_path / "state.json")
    kwargs = dict(start="s", end="e", warmup_days=10, horizon_minutes=240)
    manager.register_asset("AUD_USD", code_sha="old", **kwargs)
    manager.add_candidate("AUD_USD", {"id": "c1", "frozen": True})
    manager.register_asset("AUD_USD", code_sha="new", **kwargs)
    candidate = manager.load()["assets"]["AUD_USD"]["candidates"][0]
    assert candidate["id"] == "c1"
    assert candidate["active"] is False


def test_rejects_unknown_phase_and_unregistered_asset(tmp_path):
    manager = ResearchManager(tmp_path / "state.json")
    with pytest.raises(ValueError):
        manager.update_phase("AUD_USD", "production", "COMPLETED")
    with pytest.raises(KeyError):
        manager.update_phase("AUD_USD", "replay", "COMPLETED")


def test_v2_freeze_is_immutable_and_identity_change_resets_phases(tmp_path):
    manager=ResearchManager(tmp_path/"state.json")
    kwargs=dict(start="s",end="e",warmup_days=10,horizon_minutes=240)
    manager.register_asset("AUD_USD",code_sha="a",data_sha256="d1",**kwargs)
    manager.update_phase("AUD_USD","data_integrity","COMPLETED")
    manager.freeze_candidate("AUD_USD",{"candidate_id":"c1","candidate_definition_sha256":"sha1"})
    with pytest.raises(ValueError,match="retuned"):
        manager.freeze_candidate("AUD_USD",{"candidate_id":"c1","candidate_definition_sha256":"sha2"})
    changed=manager.register_asset("AUD_USD",code_sha="a",data_sha256="d2",**kwargs)
    assert changed["phases"]["data_integrity"]["status"]=="PENDING"
    assert changed["lifecycle"]["status"]=="INPUT_CHANGED"


def test_forward_candidate_requires_manual_ia1_and_independent_ia2(tmp_path):
    manager=ResearchManager(tmp_path/"state.json")
    manager.register_asset("AUD_USD",code_sha="a",data_sha256="d",start="s",end="e",warmup_days=10,horizon_minutes=240)
    manager.freeze_candidate("AUD_USD",{"candidate_id":"c1","candidate_definition_sha256":"sha1"})
    manager.add_audit("AUD_USD",{"verdict":"ACCEPT"})
    with pytest.raises(ValueError,match="IA #1"):
        manager.approve_forward_candidate("AUD_USD","c1",ia1_approved=False,ia2_verdict="ACCEPT")
    result=manager.approve_forward_candidate("AUD_USD","c1",ia1_approved=True,ia2_verdict="ACCEPT WITH LIMITATIONS")
    assert result["status"]=="FORWARD_CANDIDATE"
    assert result["production_authority"] is False


def test_dataset_identity_change_invalidates_freeze_audit_and_forward_approval(tmp_path):
    manager = ResearchManager(tmp_path / "state.json")
    registration = dict(start="s", end="e", warmup_days=10, horizon_minutes=240)
    asset_a = manager.register_asset("AUD_USD", code_sha="code-a", data_sha256="data-a", **registration)
    frozen_a = manager.freeze_candidate("AUD_USD", {"candidate_id":"c-a","candidate_definition_sha256":"definition-a"})
    audit_a = manager.add_audit("AUD_USD", {"verdict":"ACCEPT"})
    approved_a = manager.approve_forward_candidate("AUD_USD", "c-a", ia1_approved=True, ia2_verdict="ACCEPT")
    assert frozen_a["dataset_identity"] == audit_a["dataset_identity"] == asset_a["dataset_identity"]
    assert approved_a["dataset_identity"] == asset_a["dataset_identity"]

    asset_b = manager.register_asset("AUD_USD", code_sha="code-b", data_sha256="data-b", **registration)
    assert asset_b["dataset_identity"] != asset_a["dataset_identity"]
    assert asset_b["audit_verdict"] == "NOT TESTED"
    assert asset_b["independent_audit_verdict"] == "NOT TESTED"
    assert asset_b["forward_status"].startswith("BLOCKED")
    assert "forward_candidate" not in asset_b
    assert frozen_a["dataset_identity"] != asset_b["dataset_identity"]

    with pytest.raises(ValueError, match="current dataset identity"):
        manager.approve_forward_candidate("AUD_USD", "c-a", ia1_approved=True, ia2_verdict="ACCEPT")
    manager.freeze_candidate("AUD_USD", {"candidate_id":"c-b","candidate_definition_sha256":"definition-b"})
    with pytest.raises(ValueError, match="pre-audit"):
        manager.approve_forward_candidate("AUD_USD", "c-b", ia1_approved=True, ia2_verdict="ACCEPT")
    manager.add_audit("AUD_USD", {"verdict":"ACCEPT WITH LIMITATIONS"})
    approved_b = manager.approve_forward_candidate("AUD_USD", "c-b", ia1_approved=True, ia2_verdict="ACCEPT")
    assert approved_b["dataset_identity"] == asset_b["dataset_identity"]


def test_holdout_ledger_allows_only_exact_read_only_reproduction(tmp_path):
    manager = ResearchManager(tmp_path / "state.json")
    registration = dict(start="s", end="e", warmup_days=10, horizon_minutes=240)
    asset_a = manager.register_asset("AUD_USD", code_sha="code-a", data_sha256="data-a", **registration)
    manager.freeze_candidate("AUD_USD", {"candidate_id":"c1","candidate_definition_sha256":"definition-1"})
    manager.freeze_candidate("AUD_USD", {"candidate_id":"c2","candidate_definition_sha256":"definition-2"})
    first = manager.begin_holdout(
        "AUD_USD", candidate_definition_sha256="definition-1", freeze_sha256="freeze-1",
    )
    assert first["status"] == "OPENING" and first["mode"] == "FIRST_OPEN"
    artifact = tmp_path / "holdout.json"
    artifact.write_text('{"status":"PASS"}\n', encoding="utf-8")
    opened = manager.complete_holdout(
        "AUD_USD", candidate_definition_sha256="definition-1", freeze_sha256="freeze-1",
        holdout_artifact=str(artifact),
    )
    assert opened["status"] == "OPENED"

    with pytest.raises(ValueError, match="explicitly read-only"):
        manager.begin_holdout("AUD_USD", candidate_definition_sha256="definition-1", freeze_sha256="freeze-1")
    reproduction = manager.begin_holdout(
        "AUD_USD", candidate_definition_sha256="definition-1", freeze_sha256="freeze-1",
        read_only_reproduction=True,
    )
    assert reproduction["mode"] == "READ_ONLY_REPRODUCTION"
    reproduced = manager.complete_holdout(
        "AUD_USD", candidate_definition_sha256="definition-1", freeze_sha256="freeze-1",
        holdout_artifact=str(artifact), read_only_reproduction=True,
    )
    assert reproduced["mode"] == "READ_ONLY_REPRODUCTION"
    with pytest.raises(ValueError, match="different candidate"):
        manager.begin_holdout("AUD_USD", candidate_definition_sha256="definition-2", freeze_sha256="freeze-2")
    with pytest.raises(ValueError, match="different candidate cannot be frozen"):
        manager.freeze_candidate("AUD_USD", {"candidate_id":"c-new","candidate_definition_sha256":"definition-new"})

    asset_b = manager.register_asset("AUD_USD", code_sha="code-b", data_sha256="data-b", **registration)
    manager.freeze_candidate("AUD_USD", {"candidate_id":"c3","candidate_definition_sha256":"definition-3"})
    new_open = manager.begin_holdout(
        "AUD_USD", candidate_definition_sha256="definition-3", freeze_sha256="freeze-3",
    )
    assert new_open["mode"] == "FIRST_OPEN"
    assert new_open["dataset_identity"] == asset_b["dataset_identity"]
    assert new_open["dataset_identity"] != asset_a["dataset_identity"]

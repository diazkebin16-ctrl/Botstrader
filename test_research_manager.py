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


def test_preserves_candidates_when_asset_provenance_is_refreshed(tmp_path):
    manager = ResearchManager(tmp_path / "state.json")
    kwargs = dict(start="s", end="e", warmup_days=10, horizon_minutes=240)
    manager.register_asset("AUD_USD", code_sha="old", **kwargs)
    manager.add_candidate("AUD_USD", {"id": "c1", "frozen": True})
    manager.register_asset("AUD_USD", code_sha="new", **kwargs)
    assert manager.load()["assets"]["AUD_USD"]["candidates"][0]["id"] == "c1"


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

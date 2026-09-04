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


def _phase1_review_artifact(tmp_path, *, instrument="AUD_USD", status="REVIEW_REQUIRED",
                            recovered=False, selection_scope="DISCOVERY_ONLY",
                            lookahead=True, immutable=True, best_override=None):
    manager = ResearchManager(tmp_path / "state.json")
    asset = manager.register_asset(
        instrument, code_sha="code-a", data_sha256="data-a",
        start="s", end="e", warmup_days=10, horizon_minutes=240,
    )
    target = tmp_path / "target.json"
    target.write_text('{"target":1}\n', encoding="utf-8")
    manager.update_phase(instrument, "target_population", "COMPLETED", artifact=str(target))
    target_sha = sha256_file(target)
    best = best_override or {
        "opened_gates": [], "wins_recovered": 2, "losses_released": 4, "eligible_episodes": 9,
    }
    candidates = [
        best,
        {"opened_gates":["LOW_ROOM"],"wins_recovered":2,"losses_released":7,"eligible_episodes":14},
    ]
    artifact = tmp_path / "phase1.json"
    artifact.write_text(json.dumps({
        "status":status,
        "stage":"phase_1",
        "instrument":instrument,
        "lookahead_protection":lookahead,
        "selection_scope":selection_scope,
        "all_target_wins_recovered":recovered,
        "input_sha256":target_sha,
        "best_policy":best,
        "candidates":candidates,
        "unrecovered_target_wins":[{
            "immutable_blocks":["WAIT_DIRECTION"] if immutable else [],
            "relaxable_blocks":["LOW_ROOM"],
        }],
    }), encoding="utf-8")
    return manager, asset, artifact


def test_best_viable_approval_persists_exact_bindings_and_preserves_artifact(tmp_path):
    manager, asset, artifact = _phase1_review_artifact(tmp_path)
    before = artifact.read_bytes()
    approval = manager.approve_phase1_best_viable(
        "AUD_USD", artifact, ia1_approved=True,
    )
    assert approval["approval_type"] == "BEST_VIABLE_POLICY"
    assert approval["instrument"] == "AUD_USD"
    assert approval["dataset_identity"] == asset["dataset_identity"]
    assert approval["code_sha"] == "code-a"
    assert approval["phase1_artifact_sha256"] == sha256_file(artifact)
    assert approval["best_policy_sha256"]
    assert approval["ia1_approved"] is True
    assert approval["production_authority"] is False
    assert approval["active"] is True
    assert artifact.read_bytes() == before
    active = manager.active_phase1_best_viable_approval("AUD_USD", artifact)
    assert active["phase1_artifact_sha256"] == approval["phase1_artifact_sha256"]


def test_best_viable_approval_requires_explicit_ia1(tmp_path):
    manager, _, artifact = _phase1_review_artifact(tmp_path)
    with pytest.raises(ValueError, match="IA #1"):
        manager.approve_phase1_best_viable("AUD_USD", artifact, ia1_approved=False)


@pytest.mark.parametrize("mutation,match", [
    ({"status":"OK"}, "REVIEW_REQUIRED"),
    ({"all_target_wins_recovered":True}, "unrecovered target WINs"),
    ({"selection_scope":"FULL_POPULATION_LEGACY"}, "discovery-only"),
    ({"lookahead_protection":False}, "look-ahead"),
])
def test_best_viable_approval_rejects_invalid_phase1_metadata(tmp_path, mutation, match):
    manager, _, artifact = _phase1_review_artifact(tmp_path)
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    payload.update(mutation)
    artifact.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match=match):
        manager.approve_phase1_best_viable("AUD_USD", artifact, ia1_approved=True)


def test_best_viable_approval_rejects_unrecovered_win_without_immutable_blocker(tmp_path):
    manager, _, artifact = _phase1_review_artifact(tmp_path, immutable=False)
    with pytest.raises(ValueError, match="immutable blocker"):
        manager.approve_phase1_best_viable("AUD_USD", artifact, ia1_approved=True)


def test_best_viable_approval_allows_immutable_plus_relaxable_blocker(tmp_path):
    manager, _, artifact = _phase1_review_artifact(tmp_path, immutable=True)
    approval = manager.approve_phase1_best_viable("AUD_USD", artifact, ia1_approved=True)
    assert approval["production_authority"] is False


def test_best_viable_approval_rejects_non_optimal_best_policy(tmp_path):
    bad_best = {
        "opened_gates":["LOW_ROOM"],"wins_recovered":2,"losses_released":7,"eligible_episodes":14,
    }
    manager, _, artifact = _phase1_review_artifact(tmp_path, best_override=bad_best)
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    payload["candidates"] = [
        {"opened_gates":[],"wins_recovered":2,"losses_released":4,"eligible_episodes":9},
        bad_best,
    ]
    artifact.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="deterministic ranking"):
        manager.approve_phase1_best_viable("AUD_USD", artifact, ia1_approved=True)


def test_best_viable_approval_rejects_unknown_or_immutable_opened_gate(tmp_path):
    bad_best = {
        "opened_gates":["WAIT_DIRECTION"],"wins_recovered":2,"losses_released":4,"eligible_episodes":9,
    }
    manager, _, artifact = _phase1_review_artifact(tmp_path, best_override=bad_best)
    with pytest.raises(ValueError, match="unknown or immutable gate"):
        manager.approve_phase1_best_viable("AUD_USD", artifact, ia1_approved=True)


def test_best_viable_approval_invalidated_by_dataset_identity_change(tmp_path):
    manager, asset_a, artifact = _phase1_review_artifact(tmp_path)
    approval = manager.approve_phase1_best_viable("AUD_USD", artifact, ia1_approved=True)
    assert approval["active"] is True
    asset_b = manager.register_asset(
        "AUD_USD", code_sha="code-b", data_sha256="data-b",
        start="s", end="e", warmup_days=10, horizon_minutes=240,
    )
    assert asset_b["dataset_identity"] != asset_a["dataset_identity"]
    approvals = manager.load()["assets"]["AUD_USD"]["phase1_best_viable_approvals"]
    assert approvals[-1]["active"] is False
    assert approvals[-1]["invalidated_reason"] == "INPUT_IDENTITY_CHANGED"
    assert manager.active_phase1_best_viable_approval("AUD_USD", artifact) is None


def test_best_viable_approval_mutated_artifact_is_not_reusable(tmp_path):
    manager, _, artifact = _phase1_review_artifact(tmp_path)
    manager.approve_phase1_best_viable("AUD_USD", artifact, ia1_approved=True)
    artifact.write_text(artifact.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    assert manager.active_phase1_best_viable_approval("AUD_USD", artifact) is None

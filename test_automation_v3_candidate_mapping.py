import hashlib
import json
from pathlib import Path

import pytest

import automation_v3_candidate_mapping as mapping
import automation_v3_code_change_adapter as adapter


SHA = "a" * 40


def _write(path: Path, value):
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _managed(repo: Path):
    repo.mkdir(exist_ok=True)
    (repo / "managed_strategy_rules.py").write_text(
        'MANAGED_RULES_JSON = {}\n'
        'MANAGED_RULES_JSON["AUD_USD"] = "[]"\n'
        'MANAGED_RULES_JSON["EUR_USD"] = "[]"\n'
        'MANAGED_RULES_JSON["GBP_USD"] = "[]"\n'
        'MANAGED_RULES_JSON["USD_JPY"] = "[]"\n'
        'MANAGED_RULES_JSON["USD_CAD"] = "[]"\n', encoding="utf-8"
    )


def _fixture(tmp_path, *, instrument="AUD_USD", rules=None, verdict="ACCEPT", retune=False):
    repo = tmp_path / "repo"; _managed(repo)
    ws = tmp_path / "ws"; ws.mkdir()
    rules = rules or [{"feature": "room_to_barrier_r", "operator": ">=", "threshold": 0.75}]
    definition = {
        "id": "candidate-1", "rules": rules, "filter": rules[0]["feature"], "subfilter": None,
        "old_rule": "NO_PHASE_2_THRESHOLD", "candidate_rule": "test", "threshold": rules[0]["threshold"],
        "direction_semantics": "SAME_NUMERIC_PREDICATE_FOR_BUY_AND_SELL", "entry_time_only": True,
    }
    definition_sha = mapping.canonical_sha256(definition)
    identity = {"instrument": instrument, "code_sha": SHA, "data_sha256": "d" * 64}
    target = _write(ws / "03_target_population.json", {"instrument": instrument, "dataset_identity": identity})
    phase2 = _write(ws / "05_phase_2.json", {"instrument": instrument, "dataset_identity": identity, "input_sha256": _sha(target)})
    discovery = _write(ws / "06_discovery.json", {"instrument": instrument, "dataset_identity": identity,
        "input_sha256": _sha(target), "phase2_sha256": _sha(phase2), "proposed_frozen_candidate": {"candidate": definition}})
    freeze = _write(ws / "09_freeze.json", {"status":"OK", "freeze_status":"FROZEN_IMMUTABLE", "immutable":True,
        "holdout_opened":False, "instrument":instrument, "candidate_id":"candidate-1", "candidate_definition":definition,
        "candidate_definition_sha256":definition_sha, "dataset_identity":identity, "code_sha":SHA,
        "target_population_sha256":_sha(target), "phase2_sha256":_sha(phase2), "discovery_sha256":_sha(discovery)})
    holdout = _write(ws / "10_holdout.json", {"status":"PASS", "stage":"holdout", "instrument":instrument,
        "decision":"RESEARCH_CANDIDATE_SURVIVED_HOLDOUT", "retuning_after_holdout":retune, "holdout_opened_once":True,
        "input_sha256":_sha(target), "phase2_sha256":_sha(phase2), "freeze_sha256":_sha(freeze),
        "candidate_definition_sha256":definition_sha})
    audit = _write(ws / "11_audit.json", {"status":"PASS", "production_authority":False})
    pre = _write(ws / "13_pre_audit.json", {"verdict":verdict, "production_authority":False})
    plan = _write(ws / "paper_release_plan.json", {"instrument":instrument,
        "candidate":{"candidate_id":"candidate-1"}, "source_code_sha":SHA, "production_authority":False})
    return {"repo":repo,"ws":ws,"plan":plan,"target":target,"phase2":phase2,"discovery":discovery,
            "freeze":freeze,"holdout":holdout,"audit":audit,"pre":pre,"definition":definition}


def _compile(f):
    return mapping.compile_release_plan(repo=f["repo"], plan_path=f["plan"], instrument="AUD_USD", source_code_sha=SHA)


def _save_compiled(f, plan):
    _write(f["plan"], plan)


def _rehash(plan):
    plan = dict(plan); plan.pop("plan_binding_sha256", None)
    plan["plan_binding_sha256"] = mapping.canonical_sha256(plan)
    return plan


def test_supported_single_threshold_candidate_deterministic_code_changes(tmp_path):
    f=_fixture(tmp_path); p=_compile(f); c=p["code_changes"][0]
    assert p["status"]=="PAPER_DEPLOYABLE_CANDIDATE" and c["path"]==mapping.MANAGED_PATH and c["expected_occurrences"]==1


def test_less_equal_candidate_mapping(tmp_path):
    f=_fixture(tmp_path,rules=[{"feature":"session_strength","operator":"<=","threshold":0.4}])
    assert '"operator":"<="' in _compile(f)["code_changes"][0]["new_text"]


def test_greater_equal_candidate_mapping(tmp_path):
    f=_fixture(tmp_path,rules=[{"feature":"session_strength","operator":">=","threshold":0.4}])
    assert '"operator":">="' in _compile(f)["code_changes"][0]["new_text"]


def test_composite_approved_candidate_mapping(tmp_path):
    rules=[{"feature":"room_to_barrier_r","operator":">=","threshold":.7},{"feature":"session_strength","operator":">=","threshold":.5}]
    assert _compile(_fixture(tmp_path,rules=rules))["code_changes"][0]["new_text"].count("candidate-1")==2


def test_exact_instrument_scope(tmp_path):
    p=_compile(_fixture(tmp_path)); c=p["code_changes"][0]
    assert c["old_text"].startswith('MANAGED_RULES_JSON["AUD_USD"]') and c["new_text"].startswith('MANAGED_RULES_JSON["AUD_USD"]')


def test_unsupported_feature_candidate_not_deployable(tmp_path):
    f=_fixture(tmp_path,rules=[{"feature":"future_price","operator":">=","threshold":1.0}])
    with pytest.raises(mapping.CandidateNotDeployable,match="no approved mapping"): _compile(f)


def test_unsupported_operator_blocked(tmp_path):
    f=_fixture(tmp_path,rules=[{"feature":"session_strength","operator":"==","threshold":.5}])
    with pytest.raises(mapping.CandidateNotDeployable,match="operator"): _compile(f)


def test_missing_mapping_blocked(tmp_path,monkeypatch):
    f=_fixture(tmp_path); monkeypatch.setattr(mapping,"APPROVED_FEATURES",())
    with pytest.raises(mapping.CandidateNotDeployable,match="no approved mapping"): _compile(f)


def test_stale_source_code_sha_blocked(tmp_path):
    f=_fixture(tmp_path); q=json.loads(f["plan"].read_text());q["source_code_sha"]="b"*40;_write(f["plan"],q)
    with pytest.raises(mapping.CandidateNotDeployable,match="stale release source"): _compile(f)


def test_stale_candidate_hash_blocked(tmp_path):
    f=_fixture(tmp_path); q=json.loads(f["freeze"].read_text());q["candidate_definition_sha256"]="0"*64;_write(f["freeze"],q)
    with pytest.raises(mapping.CandidateNotDeployable,match="candidate definition hash"): _compile(f)


def test_stale_freeze_blocked(tmp_path):
    f=_fixture(tmp_path); q=json.loads(f["holdout"].read_text());q["freeze_sha256"]="0"*64;_write(f["holdout"],q)
    with pytest.raises(mapping.CandidateNotDeployable,match="holdout/freeze"): _compile(f)


def test_stale_holdout_blocked(tmp_path):
    f=_fixture(tmp_path);q=json.loads(f["holdout"].read_text());q["candidate_definition_sha256"]="0"*64;_write(f["holdout"],q)
    with pytest.raises(mapping.CandidateNotDeployable,match="holdout/freeze"): _compile(f)


def test_post_holdout_retune_blocked(tmp_path):
    with pytest.raises(mapping.CandidateNotDeployable,match="retune"): _compile(_fixture(tmp_path,retune=True))


def test_rejected_audit_no_code_changes(tmp_path):
    with pytest.raises(mapping.CandidateNotDeployable,match="audit rejected"): _compile(_fixture(tmp_path,verdict="REJECT"))


def test_protected_file_mapping_impossible(tmp_path,monkeypatch):
    f=_fixture(tmp_path);monkeypatch.setattr(mapping,"MANAGED_PATH","server.py")
    with pytest.raises(mapping.CandidateNotDeployable,match="surface missing"): _compile(f)


def test_secret_like_generated_text_rejected(tmp_path,monkeypatch):
    f=_fixture(tmp_path);p=_compile(f);p["code_changes"][0]["new_text"] += "# OANDA_TOKEN=secret\n";p=_rehash(p);_save_compiled(f,p)
    monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY","false")
    with pytest.raises(ValueError,match="LIVE/secret"): adapter.apply_release_plan(f["repo"],f["plan"],base_sha=SHA,instrument="AUD_USD")


def test_expected_file_sha256_required(tmp_path,monkeypatch):
    f=_fixture(tmp_path);p=_compile(f);p["code_changes"][0].pop("expected_file_sha256");p=_rehash(p);_save_compiled(f,p);monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY","false")
    with pytest.raises(ValueError,match="pre-edit file hash"): adapter.apply_release_plan(f["repo"],f["plan"],base_sha=SHA,instrument="AUD_USD")


def test_expected_occurrences_required(tmp_path,monkeypatch):
    f=_fixture(tmp_path);p=_compile(f);p["code_changes"][0].pop("expected_occurrences");p=_rehash(p);_save_compiled(f,p);monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY","false")
    with pytest.raises(ValueError,match="expected_occurrences"): adapter.apply_release_plan(f["repo"],f["plan"],base_sha=SHA,instrument="AUD_USD")


def test_adapter_applies_exact_deterministic_plan(tmp_path,monkeypatch):
    f=_fixture(tmp_path);p=_compile(f);_save_compiled(f,p);monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY","false")
    assert adapter.apply_release_plan(f["repo"],f["plan"],base_sha=SHA,instrument="AUD_USD")==[mapping.MANAGED_PATH]
    assert p["candidate_definition_sha256"] in (f["repo"]/mapping.MANAGED_PATH).read_text()


def test_adapter_refuses_altered_source_file(tmp_path,monkeypatch):
    f=_fixture(tmp_path);p=_compile(f);_save_compiled(f,p);(f["repo"]/mapping.MANAGED_PATH).write_text((f["repo"]/mapping.MANAGED_PATH).read_text()+"# drift\n");monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY","false")
    with pytest.raises(ValueError,match="pre-edit file hash"): adapter.apply_release_plan(f["repo"],f["plan"],base_sha=SHA,instrument="AUD_USD")


def test_resulting_diff_equals_intended_candidate_only(tmp_path,monkeypatch):
    f=_fixture(tmp_path);before=(f["repo"]/mapping.MANAGED_PATH).read_text();p=_compile(f);_save_compiled(f,p);monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY","false");adapter.apply_release_plan(f["repo"],f["plan"],base_sha=SHA,instrument="AUD_USD");after=(f["repo"]/mapping.MANAGED_PATH).read_text()
    assert before.replace(p["code_changes"][0]["old_text"],p["code_changes"][0]["new_text"],1)==after


def test_multi_asset_isolation(tmp_path,monkeypatch):
    f=_fixture(tmp_path);p=_compile(f);_save_compiled(f,p);monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY","false");adapter.apply_release_plan(f["repo"],f["plan"],base_sha=SHA,instrument="AUD_USD")
    assert 'MANAGED_RULES_JSON["EUR_USD"] = "[]"' in (f["repo"]/mapping.MANAGED_PATH).read_text()


def test_repeated_plan_generation_deterministic(tmp_path):
    f=_fixture(tmp_path);request=f["plan"].read_text();a=_compile(f);f["plan"].write_text(request);b=_compile(f);assert a==b


def test_plan_hash_stable(tmp_path):
    p=_compile(_fixture(tmp_path));h=p.pop("plan_binding_sha256");assert mapping.canonical_sha256(p)==h


def test_remote_runner_happy_path_can_proceed_beyond_prior_high(tmp_path):
    assert _compile(_fixture(tmp_path))["status"]=="PAPER_DEPLOYABLE_CANDIDATE"


def test_paper_deploy_blocked_if_change_unrepresentable(tmp_path):
    f=_fixture(tmp_path,rules=[{"feature":"unknown","operator":">=","threshold":1.0}])
    with pytest.raises(mapping.CandidateNotDeployable): _compile(f)


def test_live_remains_impossible(tmp_path,monkeypatch):
    f=_fixture(tmp_path);p=_compile(f);_save_compiled(f,p);monkeypatch.setenv("BOTS_V3_PRODUCTION_AUTHORITY","true")
    with pytest.raises(ValueError,match="production authority"): adapter.apply_release_plan(f["repo"],f["plan"],base_sha=SHA,instrument="AUD_USD")

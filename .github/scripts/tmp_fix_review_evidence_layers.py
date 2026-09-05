from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text(encoding='utf-8')
    if old not in text:
        raise SystemExit(f'anchor not found in {path}')
    p.write_text(text.replace(old, new, 1), encoding='utf-8')


path = 'automation_v3_modes.py'
replace_once(path,
'''def _short_candidate(record: Mapping[str, Any], rank: int) -> dict[str, Any]:''',
'''def _short_candidate(record: Mapping[str, Any], rank: int, *, deployment_evidence_complete: bool = True) -> dict[str, Any]:''')
replace_once(path,
'''    deployment_eligible = gate.get("decision") == "FREEZE_ELIGIBLE"\n''',
'''    scientifically_eligible = gate.get("decision") == "FREEZE_ELIGIBLE"\n    deployment_eligible = scientifically_eligible and deployment_evidence_complete\n''')
replace_once(path,
'''        "deployment_eligible": deployment_eligible, "pre_holdout_eligible": deployment_eligible,\n        "status": status, "reason": _diagnostic_reason(record, status), "production_authority": False,\n''',
'''        "deployment_eligible": deployment_eligible, "pre_holdout_eligible": deployment_eligible,\n        "scientific_pre_holdout_eligible": scientifically_eligible,\n        "diagnostic_only": not deployment_eligible,\n        "status": status,\n        "reason": (\n            "DEPLOYMENT_EVIDENCE_INCOMPLETE: determinism evidence unavailable"\n            if scientifically_eligible and not deployment_evidence_complete\n            else _diagnostic_reason(record, status)\n        ),\n        "production_authority": False,\n''')

start = Path(path).read_text(encoding='utf-8').index('def build_review_shortlist(')
end = Path(path).read_text(encoding='utf-8').index('\ndef _verify_shortlist_hash', start)
text = Path(path).read_text(encoding='utf-8')
new_block = '''def build_review_shortlist(workspace: str | Path, *, run_id: str) -> tuple[Path, dict[str, Any]]:\n    workspace = Path(workspace)\n    target_path = workspace / "03_target_population.json"\n    phase1_path = workspace / "04_phase_1.json"\n    phase2_path = workspace / "05_phase_2.json"\n    discovery_path = workspace / "06_discovery.json"\n    determinism_path = workspace / "08_determinism.json"\n\n    # Diagnostic review evidence exists one layer earlier than deployment evidence.\n    # Discovery-terminal reporting requires only the scientific artifacts that\n    # already exist when candidates have been evaluated.\n    for required in (target_path, phase1_path, phase2_path, discovery_path):\n        if not required.is_file():\n            raise ValueError(f"review evidence missing: {required.name}")\n\n    phase2 = load_json(phase2_path)\n    discovery = load_json(discovery_path)\n    if discovery.get("holdout_opened") is not False:\n        raise ValueError("review requires unopened holdout")\n\n    determinism = load_json(determinism_path) if determinism_path.is_file() else {}\n    determinism_status = str(determinism.get("status") or "MISSING").upper()\n    deployment_evidence_complete = determinism_path.is_file() and determinism_status in {"PASS", "OK"}\n\n    evaluated = _evaluated_records(discovery)\n    ranked = [\n        _short_candidate(record, rank, deployment_evidence_complete=deployment_evidence_complete)\n        for rank, record in enumerate(evaluated, 1)\n    ]\n    diagnostic_top_candidates = ranked[:DEFAULT_DIAGNOSTIC_TOP]\n    deployable_candidates = [item for item in ranked if item.get("deployment_eligible") is True][:MAX_SHORTLIST]\n\n    dataset_identity = phase2.get("dataset_identity")\n    incumbent = discovery.get("incumbent") or {}\n    if not isinstance(dataset_identity, Mapping) or not incumbent.get("incumbent_definition_sha256"):\n        raise ValueError("review identity evidence missing")\n    code_sha = str(dataset_identity.get("code_sha") or "")\n    if len(code_sha) != 40:\n        raise ValueError("review code identity missing")\n\n    methodology = _methodology_identity(phase2, discovery)\n    payload: dict[str, Any] = {\n        "schema_version": 3,\n        "run_id": str(run_id),\n        "instrument": str(discovery.get("instrument") or "").upper(),\n        "mode": REVIEW_BEFORE_HOLDOUT_DEPLOY,\n        "code_sha": code_sha,\n        "dataset_identity": dict(dataset_identity),\n        "dataset_identity_sha256": canonical_sha256(dataset_identity),\n        "incumbent_definition_sha256": incumbent.get("incumbent_definition_sha256"),\n        "target_population_sha256": sha256_file(target_path),\n        "phase1_artifact_sha256": sha256_file(phase1_path),\n        "phase2_artifact_sha256": sha256_file(phase2_path),\n        "discovery_artifact_sha256": sha256_file(discovery_path),\n        "determinism_artifact_sha256": sha256_file(determinism_path) if determinism_path.is_file() else None,\n        "determinism_status": determinism_status,\n        "deployment_evidence_complete": deployment_evidence_complete,\n        "diagnostic_only": not deployment_evidence_complete,\n        "methodology_identity": methodology,\n        "methodology_identity_sha256": canonical_sha256(methodology),\n        "holdout_opened": False,\n        "selection_required_before_holdout": True,\n        "incumbent_metrics": (\n            (diagnostic_top_candidates[0].get("incumbent_metrics") if diagnostic_top_candidates else None)\n            or ((incumbent.get("validation") or {}).get("metrics") if isinstance(incumbent.get("validation"), Mapping) else None)\n            or (incumbent.get("validation") if isinstance(incumbent.get("validation"), Mapping) else None)\n            or {}\n        ),\n        "diagnostic_top_candidates": diagnostic_top_candidates,\n        "deployable_candidates": deployable_candidates,\n        "candidates": diagnostic_top_candidates,\n        "production_authority": False,\n    }\n\n    # This hash identifies the immutable diagnostic result and exists even before\n    # determinism. It is deliberately not deployment authority.\n    payload["diagnostic_review_sha256"] = canonical_sha256(payload)\n    if deployment_evidence_complete:\n        payload["shortlist_sha256"] = canonical_sha256(payload)\n        artifact_path = workspace / f"review_shortlist_{payload['shortlist_sha256']}.json"\n    else:\n        payload["shortlist_sha256"] = None\n        artifact_path = workspace / f"diagnostic_review_{payload['diagnostic_review_sha256']}.json"\n\n    if artifact_path.exists():\n        existing = load_json(artifact_path)\n        if existing != payload:\n            raise ValueError("immutable review artifact collision")\n        return artifact_path, existing\n    write_json(artifact_path, payload)\n    return artifact_path, payload\n\n\ndef _verify_diagnostic_review_hash(review: Mapping[str, Any]) -> None:\n    stored = review.get("diagnostic_review_sha256")\n    material = dict(review)\n    material.pop("diagnostic_review_sha256", None)\n    material.pop("shortlist_sha256", None)\n    if not isinstance(stored, str) or canonical_sha256(material) != stored:\n        raise ValueError("STALE_REVIEW_SHORTLIST: diagnostic review hash mismatch")\n\n'''
Path(path).write_text(text[:start] + new_block + text[end+1:], encoding='utf-8')

replace_once(path,
'''def verify_review_shortlist(shortlist_path: str | Path, *, current_code_sha: str) -> dict[str, Any]:\n    path = Path(shortlist_path)\n    shortlist = load_json(path)\n    _verify_shortlist_hash(shortlist)\n''',
'''def verify_review_shortlist(shortlist_path: str | Path, *, current_code_sha: str) -> dict[str, Any]:\n    path = Path(shortlist_path)\n    shortlist = load_json(path)\n    _verify_diagnostic_review_hash(shortlist)\n    if shortlist.get("deployment_evidence_complete") is not True or shortlist.get("diagnostic_only") is True:\n        raise CandidateNotDeployable(\n            "CANDIDATE_NOT_DEPLOYABLE: deployment evidence incomplete; determinism is required before selection"\n        )\n    _verify_shortlist_hash(shortlist)\n''')

replace_once(path,
'''        try:\n            path, shortlist = build_review_shortlist(\n                workspace, run_id=os.getenv("GITHUB_RUN_ID", "local-review-run")\n            )\n        except ValueError:\n            return None\n        return ledger.mutate(\n''',
'''        try:\n            path, shortlist = build_review_shortlist(\n                workspace, run_id=os.getenv("GITHUB_RUN_ID", "local-review-run")\n            )\n        except ValueError as exc:\n            safe_reason = str(exc).replace(str(workspace), "<workspace>")[:300]\n            return ledger.mutate(\n                instrument,\n                status="REVIEW_REPORT_BUILD_FAILED",\n                final_outcome=final_outcome,\n                scientific_terminal=status,\n                stop_reason=f"REVIEW_REPORT_BUILD_FAILED: {safe_reason}",\n                review_report_error={"code": "REVIEW_REPORT_BUILD_FAILED", "reason": safe_reason},\n                mode=REVIEW_BEFORE_HOLDOUT_DEPLOY,\n                production_authority=False,\n                **extra,\n            )\n        return ledger.mutate(\n''')
replace_once(path,
'''            review_shortlist_sha256=shortlist["shortlist_sha256"],\n''',
'''            review_shortlist_sha256=shortlist.get("shortlist_sha256"),\n            diagnostic_review_sha256=shortlist["diagnostic_review_sha256"],\n''')

# Remote status surfaces both evidence identities and explicit report failures.
path = 'automation_v3_remote_worker.py'
replace_once(path,
'''    "DEPLOYMENT_FAILURE", "UNSUPPORTED_INSTRUMENT",\n''',
'''    "DEPLOYMENT_FAILURE", "UNSUPPORTED_INSTRUMENT", "REVIEW_REPORT_BUILD_FAILED",\n''')
replace_once(path,
'''        "shortlist_sha256": run.get("review_shortlist_sha256") if authoritative else None,\n        "production_authority": False,\n''',
'''        "shortlist_sha256": run.get("review_shortlist_sha256") if authoritative else None,\n        "diagnostic_review_sha256": run.get("diagnostic_review_sha256") if authoritative else None,\n        "review_report_error": run.get("review_report_error") if authoritative else None,\n        "production_authority": False,\n''')

# Tests: make determinism optional in the real terminal fixture and add true acceptance-path coverage.
path = 'test_automation_v3_modes.py'
replace_once(path,
'''def _terminal_workspace(tmp_path: Path, *, status_kind="HIGH_OVERFITTING_RISK", count=30):''',
'''def _terminal_workspace(tmp_path: Path, *, status_kind="HIGH_OVERFITTING_RISK", count=30, include_determinism=True):''')
replace_once(path,
'''    write_json(workspace / "08_determinism.json", {"status": "PASS"})\n    ledger = V3Ledger(root / "automation_v3_state.json")\n''',
'''    if include_determinism:\n        write_json(workspace / "08_determinism.json", {"status": "PASS"})\n    ledger = V3Ledger(root / "automation_v3_state.json")\n''')

p = Path(path)
text = p.read_text(encoding='utf-8')
append = r'''


def test_real_33940485772_path_builds_diagnostic_top3_without_determinism(tmp_path):
    optimizer, ledger, root, workspace, sha = _terminal_workspace(
        tmp_path, status_kind="HIGH_OVERFITTING_RISK", count=30, include_determinism=False
    )
    result = optimizer._terminal(
        ledger, "GBP_USD", "NO_VALID_CANDIDATE", "HIGH_OVERFITTING_RISK",
        diagnostic={"generated_candidates": 120, "evaluated_after_discovery_gate": 30, "freeze_eligible": 0},
    )
    assert result["status"] == "NO_VALID_CANDIDATE"
    assert result["incumbent_metrics"]
    assert len(result["diagnostic_top_candidates"]) == 3
    assert result["deployable_candidates"] == []
    assert result["review_shortlist_sha256"] is None
    assert len(result["diagnostic_review_sha256"]) == 64
    review = load_json(Path(result["review_shortlist"]))
    assert review["diagnostic_only"] is True
    assert review["deployment_evidence_complete"] is False
    assert review["determinism_artifact_sha256"] is None
    assert review["shortlist_sha256"] is None
    assert review["diagnostic_review_sha256"] == result["diagnostic_review_sha256"]
    assert all(c["deployment_eligible"] is False for c in review["diagnostic_top_candidates"])
    assert all(c["diagnostic_only"] is True for c in review["diagnostic_top_candidates"])
    assert not (workspace / "09_freeze.json").exists()
    assert not (workspace / "10_holdout.json").exists()
    assert result.get("paper_deployment") is None
    assert result["production_authority"] is False


def test_missing_determinism_blocks_selection_even_if_scientifically_freeze_eligible(tmp_path):
    good = _candidate("good", 0.30, 0.20, eligible=True)
    sha = _workspace(tmp_path, [good], proposed=good)
    (tmp_path / "08_determinism.json").unlink()
    path, review = build_review_shortlist(tmp_path, run_id="33940485772")
    assert review["diagnostic_top_candidates"][0]["scientific_pre_holdout_eligible"] is True
    assert review["diagnostic_top_candidates"][0]["deployment_eligible"] is False
    assert review["deployable_candidates"] == []
    assert review["shortlist_sha256"] is None
    with pytest.raises(CandidateNotDeployable, match="determinism is required before selection"):
        resolve_review_candidate(path, current_code_sha=sha, rank=1)
    assert not (tmp_path / "09_freeze.json").exists()
    assert not (tmp_path / "10_holdout.json").exists()


def test_diagnostic_review_hash_binds_pre_determinism_top3(tmp_path):
    _workspace(tmp_path, [_candidate("a", 0.2, 0.1, eligible=False), _candidate("b", 0.1, 0.05, eligible=False)])
    (tmp_path / "08_determinism.json").unlink()
    _, review = build_review_shortlist(tmp_path, run_id="33940485772")
    material = dict(review)
    material.pop("diagnostic_review_sha256")
    material.pop("shortlist_sha256")
    assert review["diagnostic_review_sha256"] == canonical_sha256(material)
    assert review["shortlist_sha256"] is None


def test_unexpected_review_report_build_failure_is_explicit(tmp_path, monkeypatch):
    optimizer, ledger, *_ = _terminal_workspace(tmp_path, include_determinism=False)
    import automation_v3_modes as modes
    monkeypatch.setattr(modes, "build_review_shortlist", lambda *a, **k: (_ for _ in ()).throw(ValueError("synthetic review failure")))
    result = optimizer._terminal(ledger, "GBP_USD", "NO_VALID_CANDIDATE", "HIGH_OVERFITTING_RISK")
    assert result["status"] == "REVIEW_REPORT_BUILD_FAILED"
    assert result["final_outcome"] == "NO_VALID_CANDIDATE"
    assert result["review_report_error"]["code"] == "REVIEW_REPORT_BUILD_FAILED"
    assert "synthetic review failure" in result["review_report_error"]["reason"]
    assert result["production_authority"] is False
'''
if 'test_real_33940485772_path_builds_diagnostic_top3_without_determinism' not in text:
    p.write_text(text + append, encoding='utf-8')

# Remote status test for diagnostic identity without deployment shortlist authority.
path = 'test_automation_v3_remote_runner.py'
p = Path(path)
text = p.read_text(encoding='utf-8')
append = r'''


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
'''
if 'test_snapshot_surfaces_diagnostic_review_identity_without_shortlist' not in text:
    p.write_text(text + append, encoding='utf-8')

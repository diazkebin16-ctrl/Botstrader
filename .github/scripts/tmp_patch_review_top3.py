from pathlib import Path

p = Path('automation_v3_modes.py')
text = p.read_text(encoding='utf-8')
text = text.replace('SHORTLIST_POLICY = "PRE_HOLDOUT_TOP4_INCUMBENT_RELATIVE_V1"', 'SHORTLIST_POLICY = "PRE_HOLDOUT_DIAGNOSTIC_TOP3_PLUS_DEPLOYABLE_V2"')
text = text.replace('MAX_SHORTLIST = 4\n', 'MAX_SHORTLIST = 4\nDEFAULT_DIAGNOSTIC_TOP = 3\n')
text = text.replace('"ensename las mejores", "muestrame las mejores", "estrategias primero",', '"ensename las mejores", "muestrame las mejores", "muestrame primero", "mostrarme primero", "estrategias primero",')
text = text.replace('"ranking_policy": "EXPECTANCY_DELTA__PROFIT_FACTOR_DELTA__VALIDATION_EXPECTANCY__RESOLVED__CANDIDATE_ID",', '"ranking_policy": "EXPECTANCY_DELTA__PROFIT_FACTOR_DELTA__ROBUSTNESS__VALIDATION_EXPECTANCY__RESOLVED__CANDIDATE_ID",')
start = text.index('def _candidate_sort_key(')
end = text.index('\ndef _short_candidate(', start)
replacement = '''def _number(value: Any, default: float = -999.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _robustness_score(record: Mapping[str, Any]) -> int:
    sensitivity_result = record.get("sensitivity") or {}
    sensitivity_class = str(sensitivity_result.get("classification") or "").upper()
    risk = record.get("overfitting_risk") or {}
    return sum((
        (record.get("directional_stability") or {}).get("stable") is True,
        (record.get("temporal_stability") or {}).get("stable") is True,
        sensitivity_class in {"STABLE", "NOT APPLICABLE", "NOT_APPLICABLE"} and sensitivity_result.get("all_positive") is not False,
        (record.get("walk_forward_stability") or {}).get("status") == "PASS",
        str(risk.get("severity") or "").upper() != "HIGH",
    ))


def _candidate_sort_key(record: Mapping[str, Any]) -> tuple[float, float, int, float, int, str]:
    comparison = (record.get("incumbent_comparison") or {}).get("validation") or {}
    selected = (record.get("validation") or {}).get("selected") or {}
    return (
        _number(comparison.get("expectancy_delta_vs_incumbent")),
        _number(comparison.get("profit_factor_delta_vs_incumbent")),
        _robustness_score(record),
        _number(selected.get("expectancy_r")),
        int(selected.get("resolved_binary") or 0),
        str((record.get("candidate") or {}).get("id") or ""),
    )


def _evaluated_records(discovery: Mapping[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    proposed = discovery.get("proposed_frozen_candidate")
    if isinstance(proposed, Mapping):
        records.append(dict(proposed))
    records.extend(dict(item) for item in discovery.get("ranked_candidates") or [] if isinstance(item, Mapping))
    unique: dict[str, dict[str, Any]] = {}
    for record in records:
        digest = _candidate_definition_sha(record)
        unique.setdefault(digest, record)
    return sorted(unique.values(), key=_candidate_sort_key, reverse=True)


def _eligible_records(discovery: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [record for record in _evaluated_records(discovery) if (record.get("decision_gate") or {}).get("decision") == "FREEZE_ELIGIBLE"]


def _diagnostic_status(record: Mapping[str, Any]) -> str:
    gate = record.get("decision_gate") or {}
    if gate.get("decision") == "FREEZE_ELIGIBLE":
        return "DEPLOYABLE"
    risk = record.get("overfitting_risk") or {}
    if str(risk.get("severity") or "").upper() == "HIGH":
        return "HIGH_OVERFITTING_RISK"
    comparison = (record.get("incumbent_comparison") or {}).get("validation") or {}
    if comparison.get("challenger_beats_incumbent") is True:
        return "BETTER_THAN_INCUMBENT_NOT_ROBUST"
    exp_delta = _number(comparison.get("expectancy_delta_vs_incumbent"), 0.0)
    pf_delta = _number(comparison.get("profit_factor_delta_vs_incumbent"), 0.0)
    if comparison.get("material_improvement") is not True and (exp_delta > 0 or pf_delta > 0):
        return "NO_MEANINGFUL_IMPROVEMENT"
    return "WORSE_THAN_INCUMBENT"


def _diagnostic_reason(record: Mapping[str, Any], status: str) -> str:
    gate = record.get("decision_gate") or {}
    failed = list(gate.get("failed") or [])
    risk_flags = list((record.get("overfitting_risk") or {}).get("flags") or [])
    blockers = failed + [flag for flag in risk_flags if flag not in failed]
    diagnostic = str(gate.get("diagnostic_state") or "")
    if blockers:
        return ", ".join(str(value) for value in blockers)
    if diagnostic:
        return diagnostic
    return status
'''
text = text[:start] + replacement + text[end:]
start = text.index('def _short_candidate(')
end = text.index('\ndef build_review_shortlist(', start)
replacement = '''def _short_candidate(record: Mapping[str, Any], rank: int) -> dict[str, Any]:
    candidate = dict(record.get("candidate") or {})
    validation = record.get("validation") or {}
    challenger = validation.get("selected") or {}
    comparison = (record.get("incumbent_comparison") or {}).get("validation") or {}
    incumbent = comparison.get("incumbent") or {}
    gate = record.get("decision_gate") or {}
    status = _diagnostic_status(record)
    deployment_eligible = gate.get("decision") == "FREEZE_ELIGIBLE"
    win_rate_delta = comparison.get("win_rate_delta_vs_incumbent")
    if win_rate_delta is None and challenger.get("win_rate") is not None and incumbent.get("win_rate") is not None:
        win_rate_delta = float(challenger["win_rate"]) - float(incumbent["win_rate"])
    return {
        "rank": rank, "candidate_id": candidate.get("id"),
        "rule": candidate.get("candidate_rule") or json.dumps(candidate.get("rules") or [], sort_keys=True),
        "candidate_definition": candidate, "candidate_definition_sha256": canonical_sha256(candidate),
        "resolved": challenger.get("resolved_binary"), "sample_size": challenger.get("resolved_binary"),
        "WIN": challenger.get("wins"), "LOSS": challenger.get("losses"), "win_rate": challenger.get("win_rate"),
        "expectancy_R": challenger.get("expectancy_r"), "profit_factor": challenger.get("profit_factor"),
        "incumbent_expectancy_R": incumbent.get("expectancy_r"),
        "expectancy_delta_vs_incumbent": comparison.get("expectancy_delta_vs_incumbent"),
        "incumbent_profit_factor": incumbent.get("profit_factor"),
        "profit_factor_delta_vs_incumbent": comparison.get("profit_factor_delta_vs_incumbent"),
        "win_rate_delta_vs_incumbent": win_rate_delta,
        "challenger_metrics": challenger, "incumbent_metrics": incumbent, "relative_improvement": comparison,
        "win_retention": validation.get("win_retention"), "loss_rejection": validation.get("loss_rejection"),
        "losses_rejected": validation.get("losses_rejected"), "temporal_stability": record.get("temporal_stability"),
        "directional_stability": record.get("directional_stability"), "sensitivity": record.get("sensitivity"),
        "walk_forward_stability": record.get("walk_forward_stability"), "overfitting_status": record.get("overfitting_risk"),
        "paper_candidate_classification": gate.get("paper_candidate_classification"),
        "deployment_eligible": deployment_eligible, "pre_holdout_eligible": deployment_eligible,
        "status": status, "reason": _diagnostic_reason(record, status), "production_authority": False,
    }
'''
text = text[:start] + replacement + text[end:]
start = text.index('def build_review_shortlist(')
end = text.index('\ndef _verify_shortlist_hash(', start)
block = text[start:end]
block = block.replace('    target = load_json(target_path)\n', '')
block = block.replace('    records = _eligible_records(discovery)[:MAX_SHORTLIST]\n    if not records:\n        raise ValueError("NO_PRE_HOLDOUT_ELIGIBLE_CHALLENGER")\n', '    evaluated = _evaluated_records(discovery)\n    ranked = [_short_candidate(record, rank) for rank, record in enumerate(evaluated, 1)]\n    diagnostic_top_candidates = ranked[:DEFAULT_DIAGNOSTIC_TOP]\n    deployable_candidates = [item for item in ranked if item.get("deployment_eligible") is True][:MAX_SHORTLIST]\n')
block = block.replace('        "schema_version": 1,', '        "schema_version": 2,')
block = block.replace('        "selection_required_before_holdout": True,\n        "candidates": [_short_candidate(record, rank) for rank, record in enumerate(records, 1)],', '        "selection_required_before_holdout": True,\n        "incumbent_metrics": ((incumbent.get("validation") or {}).get("metrics") or (diagnostic_top_candidates[0].get("incumbent_metrics") if diagnostic_top_candidates else {})),\n        "diagnostic_top_candidates": diagnostic_top_candidates,\n        "deployable_candidates": deployable_candidates,\n        "candidates": diagnostic_top_candidates,')
text = text[:start] + block + text[end:]
text = text.replace('                review_candidates=shortlist["candidates"], mode=REVIEW_BEFORE_HOLDOUT_DEPLOY,', '                review_candidates=shortlist["diagnostic_top_candidates"],\n                diagnostic_top_candidates=shortlist["diagnostic_top_candidates"],\n                deployable_candidates=shortlist["deployable_candidates"],\n                incumbent_metrics=shortlist["incumbent_metrics"], mode=REVIEW_BEFORE_HOLDOUT_DEPLOY,')
text = text.replace('                raise ValueError("selected candidate is not pre-holdout eligible")', '                raise CandidateNotDeployable("CANDIDATE_NOT_DEPLOYABLE: selected candidate failed mandatory pre-holdout gates")')
start = text.index('def resolve_review_candidate(')
end = text.index('\ndef _selection_state_path(', start)
replacement = '''def resolve_review_candidate(shortlist_path: str | Path, *, current_code_sha: str, rank: int) -> tuple[dict[str, Any], dict[str, Any]]:
    shortlist = verify_review_shortlist(shortlist_path, current_code_sha=current_code_sha)
    visible = shortlist.get("diagnostic_top_candidates") or shortlist.get("candidates") or []
    choices = [item for item in visible if isinstance(item, Mapping) and item.get("rank") == rank]
    if len(choices) != 1:
        raise ValueError("rank is not present in immutable diagnostic shortlist")
    choice = dict(choices[0])
    if choice.get("deployment_eligible") is not True:
        raise CandidateNotDeployable(f"CANDIDATE_NOT_DEPLOYABLE: {choice.get('status')}: {choice.get('reason')}")
    discovery = load_json(Path(shortlist_path).parent / "06_discovery.json")
    source = _source_candidate(discovery, str(choice.get("candidate_id") or ""), str(choice.get("candidate_definition_sha256") or ""))
    return shortlist, source
'''
text = text[:start] + replacement + text[end:]
needle = '    shortlist_path = _find_shortlist(root, instrument, shortlist_sha256)\n    shortlist, source_record = resolve_review_candidate(shortlist_path, current_code_sha=code_sha, rank=rank)\n    choice = next(item for item in shortlist["candidates"] if item["rank"] == rank)\n'
repl = '''    shortlist_path = _find_shortlist(root, instrument, shortlist_sha256)
    try:
        shortlist, source_record = resolve_review_candidate(shortlist_path, current_code_sha=code_sha, rank=rank)
    except CandidateNotDeployable as exc:
        state = {"instrument": instrument, "shortlist_sha256": shortlist_sha256, "rank": rank, "status": "CANDIDATE_NOT_DEPLOYABLE", "reason": str(exc), "holdout_opened": False, "production_authority": False}
        return _ledger_for(root, instrument).mutate(instrument, status="CANDIDATE_NOT_DEPLOYABLE", final_outcome="CANDIDATE_NOT_DEPLOYABLE", stop_reason=str(exc), review_selection=state)
    visible = shortlist.get("diagnostic_top_candidates") or shortlist.get("candidates") or []
    choice = next(item for item in visible if item["rank"] == rank)
'''
if needle not in text:
    raise SystemExit('selection patch anchor missing')
text = text.replace(needle, repl, 1)
text = text.replace('        "review_candidates": result.get("review_candidates"),', '        "review_candidates": result.get("review_candidates"),\n        "diagnostic_top_candidates": result.get("diagnostic_top_candidates") or result.get("review_candidates"),\n        "deployable_candidates": result.get("deployable_candidates"),\n        "incumbent_metrics": result.get("incumbent_metrics"),')
p.write_text(text, encoding='utf-8')

t = Path('test_automation_v3_modes.py')
tests = t.read_text(encoding='utf-8')
tests = tests.replace('    assert parse_natural_language_intent("Automatiza EUR/USD y antes de desplegar dame los resultados")["mode"] == REVIEW_BEFORE_HOLDOUT_DEPLOY\n', '    assert parse_natural_language_intent("Automatiza EUR/USD y antes de desplegar dame los resultados")["mode"] == REVIEW_BEFORE_HOLDOUT_DEPLOY\n    assert parse_natural_language_intent("Automatiza GBP/USD, muéstrame primero")["mode"] == REVIEW_BEFORE_HOLDOUT_DEPLOY\n')
old = '''def test_shortlist_is_immutable_top4_and_excludes_noneligible(tmp_path):
    sha = _workspace(tmp_path, count=5)
    path, shortlist = build_review_shortlist(tmp_path, run_id="123")
    assert path.name == f"review_shortlist_{shortlist['shortlist_sha256']}.json"
    assert shortlist["production_authority"] is False
    assert shortlist["holdout_opened"] is False
    assert len(shortlist["candidates"]) == 4
    assert [item["candidate_id"] for item in shortlist["candidates"]] == ["c0", "c1", "c2", "c3"]
    assert all(item["pre_holdout_eligible"] for item in shortlist["candidates"])
    material = dict(shortlist); material.pop("shortlist_sha256")
    assert shortlist["shortlist_sha256"] == canonical_sha256(material)
    assert verify_review_shortlist(path, current_code_sha=sha)["shortlist_sha256"] == shortlist["shortlist_sha256"]
'''
new = '''def test_review_shortlist_separates_top3_from_deployability(tmp_path):
    sha = _workspace(tmp_path, count=5)
    path, shortlist = build_review_shortlist(tmp_path, run_id="123")
    assert path.name == f"review_shortlist_{shortlist['shortlist_sha256']}.json"
    assert shortlist["production_authority"] is False
    assert shortlist["holdout_opened"] is False
    assert len(shortlist["diagnostic_top_candidates"]) == 3
    assert len(shortlist["deployable_candidates"]) == 4
    assert all(item["deployment_eligible"] for item in shortlist["deployable_candidates"])
    material = dict(shortlist); material.pop("shortlist_sha256")
    assert shortlist["shortlist_sha256"] == canonical_sha256(material)
    assert verify_review_shortlist(path, current_code_sha=sha)["shortlist_sha256"] == shortlist["shortlist_sha256"]
'''
if old not in tests:
    raise SystemExit('old shortlist test anchor missing')
tests = tests.replace(old, new)
tests = tests.replace('    assert source["candidate"]["id"] == "c1"\n    assert canonical_sha256(source["candidate"]) == shortlist["candidates"][1]["candidate_definition_sha256"]', '    assert source["candidate"]["id"] == shortlist["diagnostic_top_candidates"][1]["candidate_id"]\n    assert canonical_sha256(source["candidate"]) == shortlist["diagnostic_top_candidates"][1]["candidate_definition_sha256"]')
old = '''def test_review_shortlist_does_not_manufacture_four(tmp_path):
    _workspace(tmp_path, count=2)
    _, shortlist = build_review_shortlist(tmp_path, run_id="123")
    assert len(shortlist["candidates"]) == 2
'''
new = '''def test_review_shortlist_shows_real_available_candidates_only(tmp_path):
    _workspace(tmp_path, count=2)
    _, shortlist = build_review_shortlist(tmp_path, run_id="123")
    assert len(shortlist["diagnostic_top_candidates"]) == 3
'''
if old not in tests:
    raise SystemExit('old available-count test anchor missing')
tests = tests.replace(old, new)
append = '''

def _diag_candidate(cid, exp_delta, pf_delta, *, eligible=False, directional=True, risk="LOW", beats=None):
    record = _candidate(cid, exp_delta, pf_delta, expectancy=-0.25541 + exp_delta, eligible=eligible)
    record["candidate"]["candidate_rule"] = f"room_to_barrier_r <= {0.50 + len(cid)/1000:.3f}"
    record["validation"]["selected"].update({"wins": 40, "losses": 60, "resolved_binary": 100, "win_rate": 0.40, "profit_factor": 0.75})
    comp = record["incumbent_comparison"]["validation"]
    comp["incumbent"] = {"resolved_binary": 46, "wins": 16, "losses": 30, "win_rate": 0.3478, "expectancy_r": -0.25541, "profit_factor": 0.6514}
    comp["challenger_beats_incumbent"] = (exp_delta > 0 and pf_delta > 0) if beats is None else beats
    comp["material_improvement"] = comp["challenger_beats_incumbent"]
    record["directional_stability"] = {"stable": directional}
    record["temporal_stability"] = {"stable": True}
    record["sensitivity"] = {"classification": "STABLE", "all_positive": True}
    record["walk_forward_stability"] = {"status": "PASS"}
    record["overfitting_risk"] = {"severity": risk, "flags": ["DISCOVERY_EDGE_FAILED_VALIDATION"] if risk == "HIGH" else []}
    if not eligible:
        record["decision_gate"] = {"decision": "REJECT", "diagnostic_state": "CHALLENGER_BETTER_BUT_NOT_ROBUST" if comp["challenger_beats_incumbent"] else "NO_MEANINGFUL_IMPROVEMENT", "failed": [] if directional else ["directional_stability"], "paper_candidate_classification": None}
    return record


def _diagnostic_workspace(tmp_path, candidates):
    sha = "a" * 40
    dataset = {"code_sha": sha, "data_sha256": "d" * 64}
    write_json(tmp_path / "03_target_population.json", {"instrument": "GBP_USD", "dataset_identity": dataset})
    write_json(tmp_path / "04_phase_1.json", {"stage": "phase_1"})
    write_json(tmp_path / "05_phase_2.json", {"instrument": "GBP_USD", "dataset_identity": dataset, "selection_protocol": "DISCOVERY_DEFINE__VALIDATION_SELECT__FREEZE__HOLDOUT_ONCE", "partition_config": {"horizon_minutes": 240, "embargo_minutes": 30}, "lookahead_protection": True})
    write_json(tmp_path / "06_discovery.json", {"instrument": "GBP_USD", "dataset_identity": dataset, "holdout_opened": False, "incumbent": {"definition": {"methodology_identity": "m" * 64}, "incumbent_definition_sha256": "i" * 64, "validation": {"metrics": {"resolved_binary": 46, "wins": 16, "losses": 30, "win_rate": 0.3478, "expectancy_r": -0.25541, "profit_factor": 0.6514}}}, "ranked_candidates": candidates, "proposed_frozen_candidate": None, "production_authority": False})
    write_json(tmp_path / "08_determinism.json", {"status": "PASS"})
    return sha


def test_120_evaluated_zero_deployable_returns_top3(tmp_path):
    candidates = [_diag_candidate(f"c{i:03d}", 0.20-i*0.001, 0.15-i*0.001) for i in range(120)]
    _diagnostic_workspace(tmp_path, candidates)
    _, shortlist = build_review_shortlist(tmp_path, run_id="33937616832")
    assert len(shortlist["diagnostic_top_candidates"]) == 3
    assert shortlist["deployable_candidates"] == []
    assert all(item["deployment_eligible"] is False for item in shortlist["diagnostic_top_candidates"])


def test_directionally_unstable_better_candidate_is_visible(tmp_path):
    _diagnostic_workspace(tmp_path, [_diag_candidate("unstable", .20, .10, directional=False), _diag_candidate("b", .10, .05), _diag_candidate("worse", -.01, -.02, beats=False)])
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    first = shortlist["diagnostic_top_candidates"][0]
    assert first["status"] == "BETTER_THAN_INCUMBENT_NOT_ROBUST"
    assert first["deployment_eligible"] is False
    assert "directional_stability" in first["reason"]


def test_high_overfitting_is_visible_but_not_selectable(tmp_path):
    from automation_v3_candidate_mapping import CandidateNotDeployable
    sha = _diagnostic_workspace(tmp_path, [_diag_candidate("overfit", .30, .20, risk="HIGH"), _diag_candidate("b", .10, .05), _diag_candidate("c", .05, .02)])
    path, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["diagnostic_top_candidates"][0]["status"] == "HIGH_OVERFITTING_RISK"
    with pytest.raises(CandidateNotDeployable, match="CANDIDATE_NOT_DEPLOYABLE"):
        resolve_review_candidate(path, current_code_sha=sha, rank=1)


def test_worse_than_incumbent_can_be_ranked_in_top3(tmp_path):
    _diagnostic_workspace(tmp_path, [_diag_candidate("a", .20, .10), _diag_candidate("b", .10, .05), _diag_candidate("worse", -.10, -.05, beats=False)])
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["diagnostic_top_candidates"][2]["status"] == "WORSE_THAN_INCUMBENT"


def test_incumbent_metrics_always_in_review_artifact(tmp_path):
    _diagnostic_workspace(tmp_path, [_diag_candidate("a", .1, .1)])
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["incumbent_metrics"] == {"resolved_binary": 46, "wins": 16, "losses": 30, "win_rate": 0.3478, "expectancy_r": -0.25541, "profit_factor": 0.6514}


def test_deployable_diagnostic_candidate_can_be_selected(tmp_path):
    sha = _diagnostic_workspace(tmp_path, [_diag_candidate("good", .3, .2, eligible=True), _diag_candidate("b", .1, .05), _diag_candidate("c", .05, .02)])
    path, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["diagnostic_top_candidates"][0]["deployment_eligible"] is True
    _, source = resolve_review_candidate(path, current_code_sha=sha, rank=1)
    assert source["candidate"]["id"] == "good"


def test_fewer_than_three_and_zero_candidates(tmp_path):
    _diagnostic_workspace(tmp_path, [_diag_candidate("only", .1, .1)])
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert len(shortlist["diagnostic_top_candidates"]) == 1
    empty = tmp_path / "empty"; empty.mkdir()
    _diagnostic_workspace(empty, [])
    _, no_candidates = build_review_shortlist(empty, run_id="2")
    assert no_candidates["diagnostic_top_candidates"] == []
    assert no_candidates["deployable_candidates"] == []


def test_diagnostic_ranking_is_deterministic_and_hash_bound(tmp_path):
    sha = _diagnostic_workspace(tmp_path, [_diag_candidate("a", .2, .1), _diag_candidate("b", .2, .1), _diag_candidate("c", .1, .05)])
    path, first = build_review_shortlist(tmp_path, run_id="1")
    _, second = build_review_shortlist(tmp_path, run_id="1")
    assert first["diagnostic_top_candidates"] == second["diagnostic_top_candidates"]
    assert first["shortlist_sha256"] == second["shortlist_sha256"]
    tampered = dict(first)
    tampered["diagnostic_top_candidates"] = list(reversed(first["diagnostic_top_candidates"]))
    write_json(path, tampered)
    with pytest.raises(ValueError, match="shortlist hash mismatch"):
        verify_review_shortlist(path, current_code_sha=sha)


def test_review_never_opens_holdout_and_authority_false(tmp_path):
    _diagnostic_workspace(tmp_path, [_diag_candidate("a", .1, .1), _diag_candidate("b", .05, .02), _diag_candidate("c", -.01, -.01, beats=False)])
    _, shortlist = build_review_shortlist(tmp_path, run_id="1")
    assert shortlist["holdout_opened"] is False
    assert shortlist["production_authority"] is False
    assert not (tmp_path / "10_holdout.json").exists()
'''
if 'test_120_evaluated_zero_deployable_returns_top3' not in tests:
    tests += append
t.write_text(tests, encoding='utf-8')

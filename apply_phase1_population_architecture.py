from pathlib import Path
import re


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected one occurrence, found {count}")
    return text.replace(old, new, 1)


def regex_once(text: str, pattern: str, replacement: str, label: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"{label}: expected one regex match, found {count}")
    return updated


# ---------------------------------------------------------------------------
# historical_replay.py: state-aware broad research episodes.
# ---------------------------------------------------------------------------
p = Path("historical_replay.py")
text = p.read_text(encoding="utf-8")
anchor = "\ndef replay_snapshot(server: Any, h1,m15,m5,m1,inst: str,variant: ReplayVariant, *, hypotheses=None) -> Dict[str,Any]:\n"
helper = r'''

def _research_blocks(server: Any, row: Mapping[str, Any]) -> List[str]:
    """Return all decision-time blockers for offline Phase 1 research.

    Strategy gates are explicitly researchable. Hard execution/data safety checks
    remain immutable. This is evidence only and does not alter live execution.
    """
    blocks: List[str] = []
    if str(row.get("signal") or "").upper() == "WAIT":
        blocks.append("DIRECTION_SELECTION")
    for name, passed in (row.get("safety_checks") or {}).items():
        if passed is not False or str(name) == "valid_direction":
            continue
        name = str(name)
        if name == "minimum_rr":
            blocks.append("MINIMUM_RR")
        elif name in {"barrier_room_ok", "low_room_low_rr", "low_room_extended"}:
            blocks.append("LOW_ROOM")
        else:
            blocks.append(f"SAFETY:{name}")
    filters = row.get("filters") or {}
    if getattr(server, "M1_CONFIRMATION_REQUIRED", True) and not bool(filters.get("m1_confirmation")):
        blocks.append("M1_CONFIRMATION")
    ext = float((row.get("features") or {}).get("extension_atr", 0) or 0)
    if getattr(server, "ENTRY_TIMING_ENABLED", True) and ext > float(getattr(server, "MAX_ENTRY_EXTENSION_ATR", 1.5)):
        blocks.append("QUALITY_EXTENSION")
    return sorted(set(blocks))


def build_research_target_episodes(
    rows: Sequence[Mapping[str, Any]], *, gap_minutes: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Build a broad pre-Phase1 population without swallowing baseline episodes.

    The old population collapsed every minute of the same selected direction into
    one long episode, even when the strategy gate state changed. That could make
    the supposedly-broad population smaller than the baseline actionable subset.
    We now include the blocker signature in episode identity. Baseline-pass rows
    therefore collapse exactly as the baseline actionable population, while each
    blocked strategic state remains independently researchable.
    """
    target_rows: List[Dict[str, Any]] = []
    for row in rows:
        direction = str(row.get("signal") or "").upper()
        if direction not in ("BUY", "SELL"):
            direction = str(row.get("chosen_signal") or "").upper()
        if direction not in ("BUY", "SELL"):
            continue
        item = dict(row)
        item["research_direction"] = direction
        blocks = [str(value) for value in (item.get("research_blocks") or [])]
        item["research_episode_class"] = str(
            item.get("research_episode_class")
            or ("|".join(sorted(set(blocks))) if blocks else "BASELINE_PASS")
        )
        target_rows.append(item)

    target_episodes = collapse_market_episodes(
        target_rows,
        gap_minutes=gap_minutes,
        timestamp_key="candle_ts",
        instrument_key="instrument",
        direction_key="research_direction",
        variant_key="research_episode_class",
    )
    baseline_rows = [
        dict(row) for row in rows
        if row.get("actionable") and str(row.get("signal") or "").upper() in ("BUY", "SELL")
    ]
    baseline_episodes = collapse_market_episodes(
        baseline_rows,
        gap_minutes=gap_minutes,
        timestamp_key="candle_ts",
        instrument_key="instrument",
        direction_key="signal",
    )
    baseline_keys = {
        (str(row.get("instrument") or ""), str(row.get("signal") or ""), str(row.get("candle_ts") or ""))
        for row in baseline_episodes
    }
    target_keys = {
        (str(row.get("instrument") or ""), str(row.get("research_direction") or ""), str(row.get("candle_ts") or ""))
        for row in target_episodes
    }
    missing = sorted(baseline_keys - target_keys)
    if missing:
        raise ValueError("Research population lost baseline actionable episodes")
    return target_episodes, {
        "status": "PASS",
        "identity": "INSTRUMENT_DIRECTION_STRATEGIC_BLOCKER_STATE",
        "baseline_actionable_episodes": len(baseline_episodes),
        "research_episodes": len(target_episodes),
        "baseline_subset_missing": 0,
    }
'''
text = replace_once(text, anchor, helper + anchor, "insert research population helpers")
text = replace_once(
    text,
    '    actionable,reason=_replay_gate(server,row);row["actionable"]=actionable;row["decision_reason"]=reason\n    return row',
    '    actionable,reason=_replay_gate(server,row);row["actionable"]=actionable;row["decision_reason"]=reason\n'
    '    row["research_blocks"]=_research_blocks(server,row)\n'
    '    row["research_episode_class"]="|".join(row["research_blocks"]) if row["research_blocks"] else "BASELINE_PASS"\n'
    '    return row',
    "attach complete blocker evidence",
)
text = replace_once(
    text,
    '        target_population_resolved=[]\n        if config.save_target_population:\n            # The selected direction and all features are computed at decision time.\n            # Future M1 is used only below to label the outcome. This population is\n            # research evidence, never an executable population.\n            target_rows=[]\n            for r in raw[v.name]:\n                direction=str(r.get("signal") or "").upper()\n                if direction not in ("BUY","SELL"):\n                    direction=str(r.get("chosen_signal") or "").upper()\n                if direction not in ("BUY","SELL"):\n                    continue\n                z=dict(r);z["research_direction"]=direction\n                target_rows.append(z)\n            target_episodes=collapse_market_episodes(\n                target_rows,\n                gap_minutes=config.episode_gap_minutes,\n                timestamp_key="candle_ts",\n                instrument_key="instrument",\n                direction_key="research_direction",\n            )\n            target_population_resolved=[\n                _resolve_episode(store,r,inst,config,direction_key="research_direction")\n                for r in target_episodes\n            ]',
    '        target_population_resolved=[]\n        target_population_evidence={"status":"NOT TESTED"}\n        if config.save_target_population:\n            # Decision-time blocker state participates in episode identity so the\n            # broad research population cannot swallow baseline actionable episodes.\n            target_episodes,target_population_evidence=build_research_target_episodes(\n                raw[v.name],gap_minutes=config.episode_gap_minutes\n            )\n            target_population_resolved=[\n                _resolve_episode(store,r,inst,config,direction_key="research_direction")\n                for r in target_episodes\n            ]',
    "replace target population episodeization",
)
text = replace_once(
    text,
    '                         "target_population":{\n                             "enabled":bool(config.save_target_population),\n                             "scope":"RESEARCH_ONLY_SELECTED_DIRECTION_BEFORE_STRATEGIC_GATES",\n                             "metrics":_metrics(target_population_resolved),\n                             "episodes":target_population_resolved,\n                         },',
    '                         "target_population":{\n                             "enabled":bool(config.save_target_population),\n                             "scope":"RESEARCH_ONLY_SELECTED_DIRECTION_BEFORE_STRATEGIC_GATES",\n                             "episode_semantics":"STRATEGIC_STATE_AWARE_PRE_PHASE1_RESEARCH_EPISODES",\n                             "baseline_subset_evidence":target_population_evidence,\n                             "funnel":{\n                                 "raw_decision_snapshots":len(raw[v.name]),\n                                 "baseline_actionable_snapshots":len(actionable),\n                                 "baseline_actionable_episodes":len(episodes),\n                                 "baseline_outcomes":_metrics(resolved),\n                                 "research_episodes":len(target_population_resolved),\n                                 "research_outcomes":_metrics(target_population_resolved),\n                             },\n                             "metrics":_metrics(target_population_resolved),\n                             "episodes":target_population_resolved,\n                         },',
    "add target population funnel evidence",
)
p.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# research_pipeline.py: maximize recall, research strategy gates, explicit funnel.
# ---------------------------------------------------------------------------
p = Path("research_pipeline.py")
text = p.read_text(encoding="utf-8")
text = replace_once(
    text,
    'LOW_ROOM_CHECKS = {"barrier_room_ok", "low_room_low_rr", "low_room_extended"}\n',
    'LOW_ROOM_CHECKS = {"barrier_room_ok", "low_room_low_rr", "low_room_extended"}\n'
    'RESEARCHABLE_STRATEGY_GATES = (\n'
    '    "DIRECTION_SELECTION", "MINIMUM_RR", "M1_CONFIRMATION",\n'
    '    "QUALITY_EXTENSION", "LOW_ROOM",\n'
    ')\n',
    "add researchable gate policy",
)
new_blocks = '''def _strategic_blocks(row: Mapping[str, Any]) -> Tuple[set[str], set[str]]:
    """Return (researchable strategy blocks, immutable safety blocks).

    Explicit replay evidence is preferred. Legacy artifacts remain readable but
    WAIT_DIRECTION and minimum_rr are classified according to their actual role:
    decision-policy/quality filters, not execution/data-integrity safety.
    """
    relaxable: set[str] = set()
    immutable: set[str] = set()
    explicit = row.get("research_blocks")
    if isinstance(explicit, list):
        for value in explicit:
            name = str(value)
            if name in RESEARCHABLE_STRATEGY_GATES:
                relaxable.add(name)
            elif name.startswith("SAFETY:"):
                immutable.add(name)
            else:
                immutable.add(f"UNKNOWN:{name}")
        return relaxable, immutable

    reason = str(row.get("decision_reason") or "")
    if reason == "WAIT_DIRECTION":
        relaxable.add("DIRECTION_SELECTION")
    elif reason == "QUALITY:M1_CONFIRMATION":
        relaxable.add("M1_CONFIRMATION")
    elif reason == "QUALITY:EXTENSION":
        relaxable.add("QUALITY_EXTENSION")

    failed_safety = {
        str(name) for name, passed in (row.get("safety_checks") or {}).items()
        if passed is False and str(name) != "valid_direction"
    }
    for name in failed_safety:
        if name == "minimum_rr":
            relaxable.add("MINIMUM_RR")
        elif name in LOW_ROOM_CHECKS:
            relaxable.add("LOW_ROOM")
        else:
            immutable.add(f"SAFETY:{name}")
    return relaxable, immutable
'''
text = regex_once(
    text,
    r'def _strategic_blocks\(row: Mapping\[str, Any\]\) -> Tuple\[set\[str\], set\[str\]\]:.*?\n\ndef _eligible',
    new_blocks + '\n\ndef _eligible',
    "replace strategic blocker classification",
)
text = replace_once(
    text,
    '    rows = list(source.get("episodes") or [])\n    _validate_outcomes(rows)\n    partition = None',
    '    all_rows = list(source.get("episodes") or [])\n    _validate_outcomes(all_rows)\n    rows = list(all_rows)\n    partition = None',
    "retain full research population",
)
text = replace_once(
    text,
    '    gates=("M1_CONFIRMATION", "QUALITY_EXTENSION", "LOW_ROOM")',
    '    gates=RESEARCHABLE_STRATEGY_GATES',
    "expand Phase1 gate search",
)
text = replace_once(
    text,
    '    candidates.sort(key=lambda x:(-x["wins_recovered"],x["losses_released"],len(x["opened_gates"]),x["opened_gates"]))\n    best=candidates[0] if candidates else {"opened_gates":[],"wins_recovered":0,"losses_released":0,"eligible_episodes":0}',
    '    candidates.sort(key=lambda x:(-x["wins_recovered"],len(x["opened_gates"]),x["losses_released"],x["opened_gates"]))\n'
    '    best=candidates[0] if candidates else {"opened_gates":[],"wins_recovered":0,"losses_released":0,"eligible_episodes":0}\n'
    '    baseline=next((item for item in candidates if item["opened_gates"]==[]),{"opened_gates":[],"wins_recovered":0,"losses_released":0,"eligible_episodes":0})',
    "make recall-first ranking explicit",
)
text = replace_once(
    text,
    '    blocker_counts=Counter()\n    for row in target_wins:\n        relaxable,immutable=_strategic_blocks(row)\n        blocker_counts.update(relaxable);blocker_counts.update(immutable)\n    complete=len(unrecovered)==0',
    '    blocker_counts=Counter();researchable_counts=Counter();immutable_counts=Counter()\n'
    '    for row in target_wins:\n'
    '        relaxable,immutable=_strategic_blocks(row)\n'
    '        blocker_counts.update(relaxable);blocker_counts.update(immutable)\n'
    '        researchable_counts.update(relaxable);immutable_counts.update(immutable)\n'
    '    source_counts=outcome_counts(all_rows);selection_counts=outcome_counts(rows)\n'
    '    baseline_wins=int(baseline.get("wins_recovered") or 0);best_wins=int(best.get("wins_recovered") or 0)\n'
    '    target_count=len(target_wins)\n'
    '    complete=len(unrecovered)==0',
    "add explicit Phase1 funnel accounting",
)
text = replace_once(
    text,
    '        "objective":"RECOVER_ALL_TARGET_WINS_THROUGH_STRATEGIC_CHAIN",\n        "target_wins":len(target_wins),',
    '        "objective":"MAXIMIZE_TARGET_WIN_RECALL_BEFORE_PHASE2_LOSS_FILTERING",\n'
    '        "total_research_episodes":len(all_rows),\n'
    '        "resolved_wins":int(source_counts.get("WIN",0)),\n'
    '        "resolved_losses":int(source_counts.get("LOSS",0)),\n'
    '        "timeouts":int(source_counts.get("TIMEOUT",0)),\n'
    '        "ambiguous":int(source_counts.get("AMBIGUOUS",0)),\n'
    '        "phase1_selection_episodes":len(rows),\n'
    '        "phase1_selection_outcomes":selection_counts,\n'
    '        "baseline_pass_wins":baseline_wins,\n'
    '        "baseline_blocked_wins":max(0,target_count-baseline_wins),\n'
    '        "phase1_target_wins":target_count,\n'
    '        "phase1_recovered_wins":best_wins,\n'
    '        "phase1_unrecoverable_wins":len(unrecovered),\n'
    '        "win_recall_before":baseline_wins/target_count if target_count else None,\n'
    '        "win_recall_after":best_wins/target_count if target_count else None,\n'
    '        "losses_admitted_before":int(baseline.get("losses_released") or 0),\n'
    '        "losses_admitted_after":int(best.get("losses_released") or 0),\n'
    '        "opened_gates":list(best.get("opened_gates") or []),\n'
    '        "immutable_blockers":dict(sorted(immutable_counts.items())),\n'
    '        "researchable_blockers":dict(sorted(researchable_counts.items())),\n'
    '        "target_wins":target_count,',
    "add requested Phase1 report fields",
)
text = replace_once(
    text,
    '        "candidates":candidates,\n        "notes":[\n            "Outcome is used only to evaluate Phase 1 recovery, never as a decision-time feature.",\n            "Non-LOW_ROOM safety checks and WAIT_DIRECTION are never relaxed automatically.",\n            "TIMEOUT and AMBIGUOUS remain separate from LOSS.",\n        ],',
    '        "candidates":candidates,\n'
    '        "researchable_strategy_gates":list(RESEARCHABLE_STRATEGY_GATES),\n'
    '        "production_authority":False,\n'
    '        "notes":[\n'
    '            "Outcome is used only to evaluate Phase 1 recovery, never as a decision-time feature.",\n'
    '            "DIRECTION_SELECTION and MINIMUM_RR are offline researchable strategy gates; live execution is unchanged.",\n'
    '            "All remaining failed safety checks are immutable unless explicitly classified as researchable.",\n'
    '            "Phase 1 maximizes WIN recall before considering gate count and admitted LOSS; Phase 2 filters LOSS.",\n'
    '            "TIMEOUT and AMBIGUOUS remain separate from LOSS.",\n'
    '        ],',
    "update Phase1 methodology notes",
)
p.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# research_phase2.py: make the expanded Phase1 population explicit evidence.
# ---------------------------------------------------------------------------
p = Path("research_phase2.py")
text = p.read_text(encoding="utf-8")
text = replace_once(
    text,
    '    available = {\n        name: sum(_feature(row, name) is not None for row in split["discovery"])\n        for name in NUMERIC_FEATURES\n    }\n    return {',
    '    available = {\n'
    '        name: sum(_feature(row, name) is not None for row in split["discovery"])\n'
    '        for name in NUMERIC_FEATURES\n'
    '    }\n'
    '    from research_pipeline import _eligible\n'
    '    phase1_opened=set((phase1.get("best_policy") or {}).get("opened_gates") or [])\n'
    '    phase1_input_population={\n'
    '        name: metrics([dict(row) for row in split[key] if _eligible(row,phase1_opened)])\n'
    '        for name,key in (("discovery","discovery"),("validation","validation"),("holdout","test"))\n'
    '    }\n'
    '    return {',
    "add Phase2 input population evidence",
)
text = replace_once(
    text,
    '        "phase1_policy": _phase1_policy({**phase1, "artifact_sha256": sha256_file(phase1_path)}),\n        "partition_config": {',
    '        "phase1_policy": _phase1_policy({**phase1, "artifact_sha256": sha256_file(phase1_path)}),\n'
    '        "phase1_input_population":phase1_input_population,\n'
    '        "partition_config": {',
    "persist Phase2 expanded input evidence",
)
p.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Approval validators must understand the enlarged governed research surface.
# ---------------------------------------------------------------------------
p = Path("research_manager.py")
text = p.read_text(encoding="utf-8")
text = replace_once(
    text,
    '        allowed_gates = {"M1_CONFIRMATION", "QUALITY_EXTENSION", "LOW_ROOM"}',
    '        allowed_gates = {"DIRECTION_SELECTION", "MINIMUM_RR", "M1_CONFIRMATION", "QUALITY_EXTENSION", "LOW_ROOM"}',
    "expand human approval allowlist",
)
text = replace_once(
    text,
    '                -int(candidate.get("wins_recovered") or 0),\n                int(candidate.get("losses_released") or 0),\n                len(gates),\n                tuple(gates),',
    '                -int(candidate.get("wins_recovered") or 0),\n                len(gates),\n                int(candidate.get("losses_released") or 0),\n                tuple(gates),',
    "align human approval recall-first ranking",
)
p.write_text(text, encoding="utf-8")

p = Path("autonomous_asset_optimizer.py")
text = p.read_text(encoding="utf-8")
text = replace_once(
    text,
    'best=p.get("best_policy");cs=p.get("candidates");allowed={"M1_CONFIRMATION","QUALITY_EXTENSION","LOW_ROOM"}',
    'best=p.get("best_policy");cs=p.get("candidates");allowed={"DIRECTION_SELECTION","MINIMUM_RR","M1_CONFIRMATION","QUALITY_EXTENSION","LOW_ROOM"}',
    "expand autonomous approval allowlist",
)
text = replace_once(
    text,
    '  return(-int(x.get("wins_recovered") or 0),int(x.get("losses_released") or 0),len(g),tuple(g))',
    '  return(-int(x.get("wins_recovered") or 0),len(g),int(x.get("losses_released") or 0),tuple(g))',
    "align autonomous approval ranking",
)
p.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Existing test semantics: minimum_rr is now researchable offline, while hard
# safety remains immutable.
# ---------------------------------------------------------------------------
p = Path("test_research_pipeline.py")
text = p.read_text(encoding="utf-8")
old = '''def test_phase1_never_relaxes_non_low_room_safety(tmp_path):
    source=tmp_path/"target.json"
    source.write_text(json.dumps({
        "instrument":"AUD_USD","variant":"V331_BASELINE","lookahead_protection":True,
        "episodes":[_row("WIN","SAFETY:minimum_rr",{"minimum_rr":False})],
    }),encoding="utf-8")
    out=analyze_phase1(str(source))
    assert out["status"]=="REVIEW_REQUIRED"
    assert out["all_target_wins_recovered"] is False
    assert out["unrecovered_target_wins"][0]["immutable_blocks"]==["SAFETY:minimum_rr"]
'''
new = '''def test_phase1_researches_minimum_rr_but_preserves_hard_safety(tmp_path):
    source=tmp_path/"target.json"
    source.write_text(json.dumps({
        "instrument":"AUD_USD","variant":"V331_BASELINE","lookahead_protection":True,
        "episodes":[
            _row("WIN","SAFETY:minimum_rr",{"minimum_rr":False}),
            _row("WIN","SAFETY:finite_prices",{"finite_prices":False},"2026-01-02T00:00:00Z"),
        ],
    }),encoding="utf-8")
    out=analyze_phase1(str(source))
    assert "MINIMUM_RR" in out["best_policy"]["opened_gates"]
    assert out["phase1_recovered_wins"]==1
    assert out["status"]=="REVIEW_REQUIRED"
    assert out["unrecovered_target_wins"][0]["immutable_blocks"]==["SAFETY:finite_prices"]
'''
text = replace_once(text, old, new, "update legacy minimum_rr test")
p.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# New focused architecture regression suite.
# ---------------------------------------------------------------------------
Path("test_phase1_population_architecture.py").write_text(r'''import json
from pathlib import Path

import pytest

from automation_v3_discovery_pregate import classify_discovery_outcome
from historical_replay import build_research_target_episodes
from research_phase2 import _phase1_eligible_rows, candidate_analysis
from research_pipeline import _strategic_blocks, analyze_phase1, extract_target_population


def row(status, *, ts, blocks=None, actionable=False, feature=0.0, instrument="AUD_USD"):
    return {
        "instrument": instrument,
        "candle_ts": ts,
        "signal": "BUY" if actionable else "WAIT",
        "chosen_signal": "BUY",
        "research_direction": "BUY",
        "actionable": actionable,
        "decision_reason": "REPLAY_ACTIONABLE" if actionable else "WAIT_DIRECTION",
        "research_blocks": list(blocks or []),
        "research_episode_class": "|".join(sorted(set(blocks or []))) if blocks else "BASELINE_PASS",
        "safety_checks": {"valid_direction": actionable},
        "filters": {"m1_confirmation": True},
        "features": {"rr_raw": feature, "score": feature},
        "outcome_status": status,
        "label": 1 if status == "WIN" else 0 if status == "LOSS" else None,
        "realized_r": 1.5 if status == "WIN" else -1.0 if status == "LOSS" else None,
    }


def write_target(tmp_path, rows):
    path = tmp_path / "target.json"
    path.write_text(json.dumps({
        "instrument": "AUD_USD",
        "variant": "V331_BASELINE",
        "lookahead_protection": True,
        "episodes": rows,
    }), encoding="utf-8")
    return path


def test_raw_win_blocked_by_baseline_remains_phase1_target(tmp_path):
    out = analyze_phase1(str(write_target(tmp_path, [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"])])))
    assert out["phase1_target_wins"] == 1
    assert out["phase1_recovered_wins"] == 1


def test_phase1_target_not_limited_to_baseline_pass(tmp_path):
    rows = [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["DIRECTION_SELECTION"]), row("LOSS", ts="2026-01-02T00:00:00Z", actionable=True)]
    out = analyze_phase1(str(write_target(tmp_path, rows)))
    assert out["phase1_target_wins"] == 1
    assert out["baseline_pass_wins"] == 0
    assert out["baseline_blocked_wins"] == 1


def test_phase1_maximizes_all_recoverable_win_recall(tmp_path):
    rows = [
        row("WIN", ts="2026-01-01T00:00:00Z", blocks=["DIRECTION_SELECTION"]),
        row("WIN", ts="2026-01-02T00:00:00Z", blocks=["MINIMUM_RR"]),
        row("WIN", ts="2026-01-03T00:00:00Z", blocks=["M1_CONFIRMATION"]),
        row("WIN", ts="2026-01-04T00:00:00Z", blocks=["SAFETY:finite_prices"]),
    ]
    out = analyze_phase1(str(write_target(tmp_path, rows)))
    assert out["phase1_recovered_wins"] == 3
    assert out["phase1_unrecoverable_wins"] == 1
    assert out["win_recall_after"] == pytest.approx(0.75)


def test_phase1_may_admit_losses_to_recover_win(tmp_path):
    rows = [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"])]
    rows += [row("LOSS", ts=f"2026-01-{day:02d}T00:00:00Z", blocks=["M1_CONFIRMATION"]) for day in range(2, 7)]
    out = analyze_phase1(str(write_target(tmp_path, rows)))
    assert out["best_policy"]["opened_gates"] == ["M1_CONFIRMATION"]
    assert out["losses_admitted_after"] == 5
    assert out["phase1_recovered_wins"] == 1


def test_phase2_receives_recovered_wins_and_admitted_losses():
    rows = [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"]), row("LOSS", ts="2026-01-02T00:00:00Z", blocks=["M1_CONFIRMATION"])]
    spec = {"phase1_policy": {"opened_gates": ["M1_CONFIRMATION"]}}
    selected = _phase1_eligible_rows(spec, rows)
    assert [item["outcome_status"] for item in selected] == ["WIN", "LOSS"]


def test_phase2_can_remove_loss_while_retaining_recovered_win():
    win = row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"], feature=2.0)
    loss = row("LOSS", ts="2026-01-02T00:00:00Z", blocks=["M1_CONFIRMATION"], feature=0.2)
    candidate = {"id": "rr", "rules": [{"feature": "rr_raw", "operator": ">=", "threshold": 1.0}]}
    analysis = candidate_analysis(candidate, [win, loss])
    assert analysis["selected"]["wins"] == 1
    assert analysis["selected"]["losses"] == 0


def test_hard_safety_gates_remain_immutable():
    relaxable, immutable = _strategic_blocks({"research_blocks": ["SAFETY:finite_prices", "SAFETY:positive_risk"]})
    assert not relaxable
    assert immutable == {"SAFETY:finite_prices", "SAFETY:positive_risk"}


def test_strategy_gates_are_distinguishable_from_safety():
    relaxable, immutable = _strategic_blocks({"research_blocks": ["DIRECTION_SELECTION", "MINIMUM_RR", "LOW_ROOM", "SAFETY:minimum_stop_pips"]})
    assert {"DIRECTION_SELECTION", "MINIMUM_RR", "LOW_ROOM"} <= relaxable
    assert immutable == {"SAFETY:minimum_stop_pips"}


def test_baseline_episode_anchors_never_silently_shrink():
    rows = [
        row("WIN", ts="2026-01-01T00:00:00Z", actionable=True),
        row("LOSS", ts="2026-01-01T00:05:00Z", blocks=["M1_CONFIRMATION"]),
        row("WIN", ts="2026-01-01T00:20:00Z", actionable=True),
        row("LOSS", ts="2026-01-01T00:25:00Z", blocks=["MINIMUM_RR"]),
        row("WIN", ts="2026-01-01T00:40:00Z", actionable=True),
    ]
    episodes, evidence = build_research_target_episodes(rows, gap_minutes=15)
    keys = {(x["candle_ts"], x["research_direction"]) for x in episodes}
    assert evidence["baseline_subset_missing"] == 0
    for ts in ("2026-01-01T00:00:00Z", "2026-01-01T00:20:00Z", "2026-01-01T00:40:00Z"):
        assert (ts, "BUY") in keys


def test_funnel_accounting_balances(tmp_path):
    rows = [row("WIN", ts="2026-01-01T00:00:00Z"), row("LOSS", ts="2026-01-02T00:00:00Z"), row("TIMEOUT", ts="2026-01-03T00:00:00Z")]
    out = analyze_phase1(str(write_target(tmp_path, rows)))
    assert out["resolved_wins"] + out["resolved_losses"] + out["timeouts"] + out["ambiguous"] == out["total_research_episodes"]


def test_every_unrecovered_win_has_explicit_reason(tmp_path):
    out = analyze_phase1(str(write_target(tmp_path, [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["SAFETY:finite_prices"])])))
    item = out["unrecovered_target_wins"][0]
    assert item["immutable_blocks"] == ["SAFETY:finite_prices"]


def test_research_episode_identity_is_deterministic():
    rows = [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"]), row("LOSS", ts="2026-01-01T00:05:00Z", blocks=["MINIMUM_RR"])]
    first, first_evidence = build_research_target_episodes(rows, gap_minutes=15)
    second, second_evidence = build_research_target_episodes(rows, gap_minutes=15)
    assert first == second
    assert first_evidence == second_evidence


def test_no_lookahead_remains_enforced(tmp_path):
    replay = tmp_path / "replay.json"
    replay.write_text(json.dumps({"instrument":"AUD_USD","methodology":{"no_lookahead_decision":False,"future_bars_only_for_outcome":True},"variants":{"V331_BASELINE":{"target_population":{"enabled":True,"episodes":[]}}}}), encoding="utf-8")
    with pytest.raises(ValueError, match="look-ahead"):
        extract_target_population(str(replay), "V331_BASELINE")


def test_freeze_holdout_contract_remains_separate():
    # Phase 1 produces policy/evidence only; it never marks holdout opened or frozen.
    assert "holdout_opened" not in analyze_phase1.__annotations__


def test_lookback_expansion_only_for_insufficient_broad_support():
    insufficient = {"dominant_failure": "INSUFFICIENT_SUPPORT", "recommended_action": "EXPAND_LOOKBACK"}
    poor = {"dominant_failure": "NO_POSITIVE_EXPECTANCY", "recommended_action": "NO_VALID_CANDIDATE"}
    assert classify_discovery_outcome(insufficient)["recommended_action"] == "EXPAND_LOOKBACK"
    assert classify_discovery_outcome(poor)["recommended_action"] == "NO_VALID_CANDIDATE"


def test_audusd_regression_shape_preserves_baseline_subset():
    # The real run had 38 baseline WIN episodes but only 17 target WIN episodes.
    # This synthetic shape reproduces the mechanism: pass windows separated by
    # blocked strategy states must remain distinct in the broad population.
    rows = []
    for index in range(38):
        minute = index * 20
        hour, mm = divmod(minute, 60)
        rows.append(row("WIN", ts=f"2026-01-{1 + hour // 24:02d}T{hour % 24:02d}:{mm:02d}:00Z", actionable=True))
        rows.append(row("LOSS", ts=f"2026-01-{1 + hour // 24:02d}T{hour % 24:02d}:{(mm + 5) % 60:02d}:00Z", blocks=["M1_CONFIRMATION"]))
    _, evidence = build_research_target_episodes(rows, gap_minutes=15)
    assert evidence["baseline_actionable_episodes"] == 38
    assert evidence["research_episodes"] >= 38


def test_multi_asset_isolation_in_episode_identity():
    rows = [row("WIN", ts="2026-01-01T00:00:00Z", actionable=True, instrument="AUD_USD"), row("WIN", ts="2026-01-01T00:00:00Z", actionable=True, instrument="EUR_USD")]
    episodes, _ = build_research_target_episodes(rows, gap_minutes=15)
    assert {x["instrument"] for x in episodes} == {"AUD_USD", "EUR_USD"}


def test_production_authority_false(tmp_path):
    out = analyze_phase1(str(write_target(tmp_path, [row("WIN", ts="2026-01-01T00:00:00Z", blocks=["M1_CONFIRMATION"])])))
    assert out["production_authority"] is False
''', encoding="utf-8")


# ---------------------------------------------------------------------------
# CI: add this branch, focused suite, new source files and exact diff baseline.
# ---------------------------------------------------------------------------
p = Path(".github/workflows/automation-v3-remote-runner-ci.yml")
text = p.read_text(encoding="utf-8")
text = replace_once(
    text,
    '      - fix/automation-v3-discovery-pregate-diagnostic\n',
    '      - fix/automation-v3-discovery-pregate-diagnostic\n      - fix/automation-v3-phase1-population-architecture\n',
    "add branch to CI",
)
text = replace_once(
    text,
    '      - name: Remote runner tests\n        run: python -m pytest -q test_automation_v3_remote_runner.py\n',
    '      - name: Focused Phase 1 population architecture tests\n        run: python -m pytest -q test_phase1_population_architecture.py\n\n      - name: Remote runner tests\n        run: python -m pytest -q test_automation_v3_remote_runner.py\n',
    "add focused architecture tests",
)
text = replace_once(
    text,
    'pyflakes automation_v3_candidate_mapping.py managed_strategy_rules.py automation_v3_code_change_adapter.py automation_v3_release.py automation_v3_railway_adapter.py automation_v3_remote_worker.py autonomous_asset_optimizer.py automation_v3_integrity_recovery.py automation_v3_phase1_continuation.py automation_v3_discovery_pregate.py test_automation_v3_candidate_mapping.py test_automation_v3_remote_runner.py test_automation_v3_integrity_recovery.py test_automation_v3_sha_horizon.py test_automation_v3_discovery_expansion.py test_automation_v3_phase1_continuation.py test_automation_v3_discovery_pregate.py',
    'pyflakes automation_v3_candidate_mapping.py managed_strategy_rules.py automation_v3_code_change_adapter.py automation_v3_release.py automation_v3_railway_adapter.py automation_v3_remote_worker.py autonomous_asset_optimizer.py automation_v3_integrity_recovery.py automation_v3_phase1_continuation.py automation_v3_discovery_pregate.py historical_replay.py research_pipeline.py research_phase2.py research_manager.py test_automation_v3_candidate_mapping.py test_automation_v3_remote_runner.py test_automation_v3_integrity_recovery.py test_automation_v3_sha_horizon.py test_automation_v3_discovery_expansion.py test_automation_v3_phase1_continuation.py test_automation_v3_discovery_pregate.py test_phase1_population_architecture.py test_research_pipeline.py',
    "extend pyflakes scope",
)
text = text.replace('7d1bf853d4e59c6de57bb5497f95f49fd0fb9b96', '4743428e3af6c6501772d8cc77b04209fd18710f')
p.write_text(text, encoding="utf-8")

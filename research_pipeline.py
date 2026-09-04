"""Pure artifact transforms for the BotsTrader offline research cascade."""
from __future__ import annotations

import argparse
import itertools
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Tuple

from research_manager import sha256_file


NON_BINARY_OUTCOMES = {"TIMEOUT", "AMBIGUOUS", "PENDING", "NOT_HISTORICALLY_RECONSTRUCTABLE"}
LOW_ROOM_CHECKS = {"barrier_room_ok", "low_room_low_rr", "low_room_extended"}
RESEARCHABLE_STRATEGY_GATES = (
    "DIRECTION_SELECTION", "MINIMUM_RR", "M1_CONFIRMATION",
    "QUALITY_EXTENSION", "LOW_ROOM",
)


def _validate_outcomes(rows: Iterable[Mapping[str, Any]]) -> None:
    for row in rows:
        status = str(row.get("outcome_status") or "PENDING").upper()
        if status in NON_BINARY_OUTCOMES and row.get("label") not in (None, ""):
            raise ValueError(f"{status} must not have a binary label")


def outcome_counts(rows: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts[str(row.get("outcome_status") or "PENDING").upper()] += 1
    return dict(sorted(counts.items()))


def extract_target_population(replay_path: str, variant: str) -> Dict[str, Any]:
    with open(replay_path, "r", encoding="utf-8") as handle:
        replay = json.load(handle)
    methodology = replay.get("methodology") or {}
    if methodology.get("no_lookahead_decision") is not True or methodology.get("future_bars_only_for_outcome") is not True:
        raise ValueError("Replay lacks required no-look-ahead evidence")
    variants = replay.get("variants") or {}
    if variant not in variants:
        raise KeyError(f"Replay variant not found: {variant}")
    population = variants[variant].get("target_population") or {}
    if population.get("enabled") is not True:
        raise ValueError("Replay target_population was not enabled")
    rows = list(population.get("episodes") or [])
    _validate_outcomes(rows)
    return {
        "status": "OK",
        "stage": "target_population",
        "instrument": replay.get("instrument"),
        "variant": variant,
        "input_sha256": sha256_file(replay_path),
        "lookahead_protection": True,
        "future_bars_used_only_for_outcome": True,
        "scope": population.get("scope"),
        "start": replay.get("start"),
        "end": replay.get("end"),
        "replay_methodology": methodology,
        "dataset_identity": replay.get("input_identity") or {"status":"NOT TESTED"},
        "outcomes": outcome_counts(rows),
        "episodes": rows,
    }


def _strategic_blocks(row: Mapping[str, Any]) -> Tuple[set[str], set[str]]:
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


def _eligible(row: Mapping[str, Any], opened: set[str]) -> bool:
    relaxable, immutable = _strategic_blocks(row)
    return not immutable and relaxable.issubset(opened)


def analyze_phase1(
    target_population_path: str, *, discovery_only: bool = False,
    horizon_minutes: int = 240, discovery_fraction: float = 0.60,
    validation_fraction: float = 0.20, embargo_minutes: int = 30,
) -> Dict[str, Any]:
    with open(target_population_path, "r", encoding="utf-8") as handle:
        source = json.load(handle)
    if source.get("lookahead_protection") is not True:
        raise ValueError("Target population lacks look-ahead protection")
    all_rows = list(source.get("episodes") or [])
    _validate_outcomes(all_rows)
    rows = list(all_rows)
    partition = None
    selection_scope = "FULL_POPULATION_LEGACY"
    if discovery_only:
        # Imported only for the Phase 1 discovery workflow.
        from replay_validation import ReplayValidationConfig, chronological_holdout
        from research_phase2 import _partition_hash
        split = chronological_holdout(
            rows, horizon_bars=horizon_minutes,
            config=ReplayValidationConfig(
                discovery_fraction=discovery_fraction,
                validation_fraction=validation_fraction,
                embargo_minutes=embargo_minutes,
            ),
        )
        if split["status"] != "OK":
            raise ValueError("Insufficient data for discovery-only Phase 1")
        rows = split["discovery"]
        selection_scope = "DISCOVERY_ONLY"
        partition = {
            "discovery_hash": _partition_hash(rows),
            "discovery_episodes": len(rows),
            "boundaries": split.get("boundaries") or {},
            "purged": split.get("purged"),
            "embargoed": split.get("embargoed"),
            "horizon_minutes": int(horizon_minutes),
            "discovery_fraction": float(discovery_fraction),
            "validation_fraction": float(validation_fraction),
            "embargo_minutes": int(embargo_minutes),
        }
    target_wins = [row for row in rows if str(row.get("outcome_status") or "").upper() == "WIN"]
    candidates=[]
    gates=RESEARCHABLE_STRATEGY_GATES
    for n in range(len(gates)+1):
        for combo in itertools.combinations(gates,n):
            opened=set(combo)
            kept=[row for row in rows if _eligible(row,opened)]
            wins=sum(str(r.get("outcome_status") or "").upper()=="WIN" for r in kept)
            losses=sum(str(r.get("outcome_status") or "").upper()=="LOSS" for r in kept)
            candidates.append({
                "opened_gates":list(combo), "wins_recovered":wins,
                "losses_released":losses, "eligible_episodes":len(kept),
            })
    candidates.sort(key=lambda x:(-x["wins_recovered"],len(x["opened_gates"]),x["losses_released"],x["opened_gates"]))
    best=candidates[0] if candidates else {"opened_gates":[],"wins_recovered":0,"losses_released":0,"eligible_episodes":0}
    baseline=next((item for item in candidates if item["opened_gates"]==[]),{"opened_gates":[],"wins_recovered":0,"losses_released":0,"eligible_episodes":0})
    unrecovered=[]
    opened=set(best["opened_gates"])
    for row in target_wins:
        if not _eligible(row,opened):
            relaxable,immutable=_strategic_blocks(row)
            unrecovered.append({
                "candle_ts":row.get("candle_ts"),
                "direction":row.get("research_direction") or row.get("chosen_signal"),
                "relaxable_blocks":sorted(relaxable-opened),
                "immutable_blocks":sorted(immutable),
            })
    blocker_counts=Counter();researchable_counts=Counter();immutable_counts=Counter()
    for row in target_wins:
        relaxable,immutable=_strategic_blocks(row)
        blocker_counts.update(relaxable);blocker_counts.update(immutable)
        researchable_counts.update(relaxable);immutable_counts.update(immutable)
    source_counts=outcome_counts(all_rows);selection_counts=outcome_counts(rows)
    baseline_wins=int(baseline.get("wins_recovered") or 0);best_wins=int(best.get("wins_recovered") or 0)
    target_count=len(target_wins)
    complete=len(unrecovered)==0
    return {
        "status":"OK" if complete else "REVIEW_REQUIRED",
        "stage":"phase_1",
        "instrument":source.get("instrument"),
        "variant":source.get("variant"),
        "input_sha256":sha256_file(target_population_path),
        "lookahead_protection":True,
        "selection_scope":selection_scope,
        "partition":partition,
        "objective":"MAXIMIZE_TARGET_WIN_RECALL_BEFORE_PHASE2_LOSS_FILTERING",
        "total_research_episodes":len(all_rows),
        "resolved_wins":int(source_counts.get("WIN",0)),
        "resolved_losses":int(source_counts.get("LOSS",0)),
        "timeouts":int(source_counts.get("TIMEOUT",0)),
        "ambiguous":int(source_counts.get("AMBIGUOUS",0)),
        "phase1_selection_episodes":len(rows),
        "phase1_selection_outcomes":selection_counts,
        "baseline_pass_wins":baseline_wins,
        "baseline_blocked_wins":max(0,target_count-baseline_wins),
        "phase1_target_wins":target_count,
        "phase1_recovered_wins":best_wins,
        "phase1_unrecoverable_wins":len(unrecovered),
        "win_recall_before":baseline_wins/target_count if target_count else None,
        "win_recall_after":best_wins/target_count if target_count else None,
        "losses_admitted_before":int(baseline.get("losses_released") or 0),
        "losses_admitted_after":int(best.get("losses_released") or 0),
        "opened_gates":list(best.get("opened_gates") or []),
        "immutable_blockers":dict(sorted(immutable_counts.items())),
        "researchable_blockers":dict(sorted(researchable_counts.items())),
        "target_wins":target_count,
        "best_policy":best,
        "all_target_wins_recovered":complete,
        "target_win_blockers":dict(sorted(blocker_counts.items())),
        "unrecovered_target_wins":unrecovered,
        "candidates":candidates,
        "researchable_strategy_gates":list(RESEARCHABLE_STRATEGY_GATES),
        "production_authority":False,
        "notes":[
            "Outcome is used only to evaluate Phase 1 recovery, never as a decision-time feature.",
            "DIRECTION_SELECTION and MINIMUM_RR are offline researchable strategy gates; live execution is unchanged.",
            "All remaining failed safety checks are immutable unless explicitly classified as researchable.",
            "Phase 1 maximizes WIN recall before considering gate count and admitted LOSS; Phase 2 filters LOSS.",
            "TIMEOUT and AMBIGUOUS remain separate from LOSS.",
        ],
    }


def _write(path: str, payload: Mapping[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(payload,indent=2,sort_keys=True)+"\n",encoding="utf-8")


def main() -> None:
    parser=argparse.ArgumentParser(description="BotsTrader offline research artifact stages")
    sub=parser.add_subparsers(dest="command",required=True)
    target=sub.add_parser("target-population")
    target.add_argument("--replay",required=True);target.add_argument("--variant",required=True);target.add_argument("--output",required=True)
    phase1=sub.add_parser("phase1")
    phase1.add_argument("--input",required=True);phase1.add_argument("--output",required=True)
    phase1.add_argument("--discovery-only",action="store_true")
    phase1.add_argument("--horizon",type=int,default=240)
    phase1.add_argument("--discovery-fraction",type=float,default=.60)
    phase1.add_argument("--validation-fraction",type=float,default=.20)
    phase1.add_argument("--embargo-minutes",type=int,default=30)
    integrity=sub.add_parser("data-integrity")
    integrity.add_argument("--cache",required=True);integrity.add_argument("--instrument",required=True)
    integrity.add_argument("--start",required=True);integrity.add_argument("--end",required=True)
    integrity.add_argument("--warmup",type=int,required=True);integrity.add_argument("--horizon",type=int,required=True)
    integrity.add_argument("--repo",default=".");integrity.add_argument("--data-sha256");integrity.add_argument("--code-sha")
    integrity.add_argument("--output",required=True)
    phase2=sub.add_parser("phase2")
    phase2.add_argument("--input",required=True);phase2.add_argument("--phase1",required=True);phase2.add_argument("--output",required=True)
    phase2.add_argument("--horizon",type=int,default=240);phase2.add_argument("--discovery-fraction",type=float,default=.60)
    phase2.add_argument("--validation-fraction",type=float,default=.20);phase2.add_argument("--embargo-minutes",type=int,default=30)
    discovery=sub.add_parser("discovery")
    discovery.add_argument("--input",required=True);discovery.add_argument("--phase2",required=True);discovery.add_argument("--min-resolved",type=int,default=10);discovery.add_argument("--output",required=True)
    freeze=sub.add_parser("freeze")
    freeze.add_argument("--discovery",required=True);freeze.add_argument("--output",required=True)
    holdout=sub.add_parser("holdout")
    holdout.add_argument("--input",required=True);holdout.add_argument("--phase2",required=True);holdout.add_argument("--discovery",required=True);holdout.add_argument("--freeze",required=True);holdout.add_argument("--output",required=True)
    report=sub.add_parser("report")
    report.add_argument("--integrity",required=True);report.add_argument("--phase1",required=True);report.add_argument("--phase2",required=True)
    report.add_argument("--discovery",required=True);report.add_argument("--freeze",required=True);report.add_argument("--holdout",required=True)
    report.add_argument("--determinism",required=True);report.add_argument("--audit",required=True);report.add_argument("--output",required=True)
    pre_audit=sub.add_parser("pre-audit")
    pre_audit.add_argument("--report",required=True);pre_audit.add_argument("--output",required=True)
    prompts=sub.add_parser("prompts")
    prompts.add_argument("--report",required=True);prompts.add_argument("--pre-audit",required=True);prompts.add_argument("--state");prompts.add_argument("--output",required=True)
    determinism=sub.add_parser("determinism")
    determinism.add_argument("--first",required=True);determinism.add_argument("--second",required=True);determinism.add_argument("--output",required=True)
    audit=sub.add_parser("audit")
    audit.add_argument("--repo",default=".");audit.add_argument("--base-commit",required=True)
    audit.add_argument("--new-tests",required=True,help="Comma-separated research test files")
    audit.add_argument("--regression-tests",required=True,help="Comma-separated related historical regression test files")
    audit.add_argument("--full-regression",action="store_true");audit.add_argument("--output",required=True)
    args=parser.parse_args()
    if args.command=="target-population":
        payload=extract_target_population(args.replay,args.variant)
    elif args.command=="phase1":
        payload=analyze_phase1(
            args.input,discovery_only=args.discovery_only,horizon_minutes=args.horizon,
            discovery_fraction=args.discovery_fraction,validation_fraction=args.validation_fraction,
            embargo_minutes=args.embargo_minutes,
        )
    elif args.command=="data-integrity":
        from research_integrity import validate_dataset
        payload=validate_dataset(
            args.cache,instrument=args.instrument,start=args.start,end=args.end,
            warmup_days=args.warmup,horizon_minutes=args.horizon,repo=args.repo,
            expected_data_sha256=args.data_sha256,expected_code_sha=args.code_sha,
        )
    else:
        # Heavy Phase 2 engines remain dormant for replay/Phase 1 commands.
        from research_phase2 import (
            automatic_pre_audit, automatic_report, discover_candidates,
            evaluate_holdout, freeze_candidate, generate_ai_prompts, prepare_phase2,
        )
        if args.command=="phase2":
            payload=prepare_phase2(
                args.input,args.phase1,horizon_minutes=args.horizon,
                discovery_fraction=args.discovery_fraction,validation_fraction=args.validation_fraction,
                embargo_minutes=args.embargo_minutes,
            )
        elif args.command=="discovery":payload=discover_candidates(args.input,args.phase2,min_resolved=args.min_resolved)
        elif args.command=="freeze":payload=freeze_candidate(args.discovery,args.output)
        elif args.command=="holdout":payload=evaluate_holdout(args.input,args.phase2,args.discovery,args.freeze)
        elif args.command=="report":payload=automatic_report(args.integrity,args.phase1,args.phase2,args.discovery,args.freeze,args.holdout,args.determinism,args.audit)
        elif args.command=="pre-audit":payload=automatic_pre_audit(args.report)
        elif args.command=="prompts":payload=generate_ai_prompts(args.report,args.pre_audit,args.state)
        elif args.command=="determinism":
            from research_integrity import compare_determinism
            payload=compare_determinism(args.first,args.second)
        else:
            from research_audit import combined_audit
            new_files=[item.strip() for item in args.new_tests.split(",") if item.strip()]
            regression_files=[item.strip() for item in args.regression_tests.split(",") if item.strip()]
            if not new_files or not regression_files:raise ValueError("Both new and regression test sets must be non-empty")
            payload=combined_audit(
                args.repo,base_commit=args.base_commit,
                new_test_commands=[["python","-m","pytest","-q",*new_files]],
                regression_commands=[["python","-m","pytest","-q",*regression_files]],
                run_full_regression=args.full_regression,
            )
    _write(args.output,payload)
    print(json.dumps({k:payload.get(k) for k in ("status","stage","instrument","variant")},sort_keys=True))


if __name__=="__main__":main()

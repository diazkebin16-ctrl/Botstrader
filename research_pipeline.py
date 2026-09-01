"""Pure artifact transforms for the BotsTrader offline research cascade."""
from __future__ import annotations

import argparse
import itertools
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

from research_manager import sha256_file


NON_BINARY_OUTCOMES = {"TIMEOUT", "AMBIGUOUS", "PENDING", "NOT_HISTORICALLY_RECONSTRUCTABLE"}
LOW_ROOM_CHECKS = {"barrier_room_ok", "low_room_low_rr", "low_room_extended"}


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
        "outcomes": outcome_counts(rows),
        "episodes": rows,
    }


def _strategic_blocks(row: Mapping[str, Any]) -> Tuple[set[str], set[str]]:
    """Return (relaxable strategic blocks, immutable/non-reconstructable blocks)."""
    relaxable: set[str] = set()
    immutable: set[str] = set()
    reason = str(row.get("decision_reason") or "")
    if reason == "WAIT_DIRECTION":
        immutable.add("WAIT_DIRECTION")
    elif reason == "QUALITY:M1_CONFIRMATION":
        relaxable.add("M1_CONFIRMATION")
    elif reason == "QUALITY:EXTENSION":
        relaxable.add("QUALITY_EXTENSION")

    failed_safety = {
        str(name) for name, passed in (row.get("safety_checks") or {}).items()
        if passed is False and str(name) != "valid_direction"
    }
    for name in failed_safety:
        if name in LOW_ROOM_CHECKS:
            relaxable.add("LOW_ROOM")
        else:
            immutable.add(f"SAFETY:{name}")
    return relaxable, immutable


def _eligible(row: Mapping[str, Any], opened: set[str]) -> bool:
    relaxable, immutable = _strategic_blocks(row)
    return not immutable and relaxable.issubset(opened)


def analyze_phase1(target_population_path: str) -> Dict[str, Any]:
    with open(target_population_path, "r", encoding="utf-8") as handle:
        source = json.load(handle)
    if source.get("lookahead_protection") is not True:
        raise ValueError("Target population lacks look-ahead protection")
    rows = list(source.get("episodes") or [])
    _validate_outcomes(rows)
    target_wins = [row for row in rows if str(row.get("outcome_status") or "").upper() == "WIN"]
    candidates=[]
    gates=("M1_CONFIRMATION", "QUALITY_EXTENSION", "LOW_ROOM")
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
    candidates.sort(key=lambda x:(-x["wins_recovered"],x["losses_released"],len(x["opened_gates"]),x["opened_gates"]))
    best=candidates[0] if candidates else {"opened_gates":[],"wins_recovered":0,"losses_released":0,"eligible_episodes":0}
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
    blocker_counts=Counter()
    for row in target_wins:
        relaxable,immutable=_strategic_blocks(row)
        blocker_counts.update(relaxable);blocker_counts.update(immutable)
    complete=len(unrecovered)==0
    return {
        "status":"OK" if complete else "REVIEW_REQUIRED",
        "stage":"phase_1",
        "instrument":source.get("instrument"),
        "variant":source.get("variant"),
        "input_sha256":sha256_file(target_population_path),
        "lookahead_protection":True,
        "objective":"RECOVER_ALL_TARGET_WINS_THROUGH_STRATEGIC_CHAIN",
        "target_wins":len(target_wins),
        "best_policy":best,
        "all_target_wins_recovered":complete,
        "target_win_blockers":dict(sorted(blocker_counts.items())),
        "unrecovered_target_wins":unrecovered,
        "candidates":candidates,
        "notes":[
            "Outcome is used only to evaluate Phase 1 recovery, never as a decision-time feature.",
            "Non-LOW_ROOM safety checks and WAIT_DIRECTION are never relaxed automatically.",
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
    args=parser.parse_args()
    if args.command=="target-population":payload=extract_target_population(args.replay,args.variant)
    else:payload=analyze_phase1(args.input)
    _write(args.output,payload)
    print(json.dumps({k:payload.get(k) for k in ("status","stage","instrument","variant")},sort_keys=True))


if __name__=="__main__":main()

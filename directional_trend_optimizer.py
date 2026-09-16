"""One 60-day in-sample selector with causal, reversal-aware evidence floors."""
from collections import Counter
from datetime import timedelta
from itertools import combinations
import hashlib
import json
import numpy as np

from directional_month_optimizer import _candidate_rules, metrics, _wilson_lower, rule_passes
from directional_strategies import SUPPORTED_INSTRUMENTS, directional_strategy_id
from major_trend import BIAS, REGIMES, TREND_VERSION, directional_targets, utc

ROLES = ("WITH", "AGAINST", "LATERAL")


def role_for(direction, regime):
    bias = BIAS.get(regime)
    if bias is None:
        return "UNKNOWN"
    if bias == 0:
        return "LATERAL"
    return "WITH" if (bias > 0) == (direction == "BUY") else "AGAINST"


def closed_nonoverlapping(rows, start, end):
    """One position per pair; no exits outside the frozen window count as resolved.

    Conservative base population: occupied intervals are removed before selection,
    so filtering never creates simultaneous trades or inflates sample counts.
    """
    selected, busy_until, seen = [], utc(start), set()
    for row in sorted(rows, key=lambda r: r["candle_ts"]):
        if row.get("outcome_status") in ("INVALID", "ENTRY_INVALIDATED", "ENTRY_BLOCKED_OPERATIONAL_TIME"):
            # A rejected order never opens a position and cannot occupy the pair.
            continue
        if row.get("outcome_status") in ("DATA_INSUFFICIENT", "DATA_INTEGRITY_ERROR"):
            raise ValueError("Incomplete executable bid/ask evidence")
        entry = utc(row.get("entry_ts") or row["candle_ts"])
        identity = (row["instrument"], row["candle_ts"])
        if identity in seen or entry < busy_until or not utc(start) <= entry < utc(end):
            continue
        seen.add(identity)
        exit_time = utc(row["exit_ts"]) if row.get("exit_ts") else utc(end)
        # TP/SL timestamps identify the start of the exit M1 candle.
        # Its high/low is only known at the close, so do not reuse that minute.
        release = exit_time + timedelta(minutes=1) if row.get("outcome_status") in ("WIN", "LOSS", "AMBIGUOUS") else exit_time
        busy_until = max(entry, release)
        if row.get("exit_ts") and entry <= exit_time <= utc(end):
            selected.append(dict(row))
    return selected


def _admitted(mask, entries, releases):
    busy = float("-inf")
    selected = []
    for index in np.flatnonzero(mask):
        if entries[index] >= busy:
            selected.append(index)
            busy = releases[index]
    return np.array(selected, dtype=int)


def _candidate_pool(rows, thresholds, minima):
    """Retain diverse rule alternatives; count occupancy only AFTER each filter."""
    if not rows:
        return [{"rules": [], "mask": np.zeros(0, dtype=bool), "score": 0, "count": 0}], 0
    entries, releases, resolved, wins, returns, states = _arrays(rows)
    masks = [np.array([rule_passes(row, rule) for row in rows]) for rule in thresholds]
    best_by_mask = {}
    evaluated = 0
    def screen(mask, rules):
        nonlocal evaluated
        evaluated += 1
        key = np.packbits(mask).tobytes()
        if key in best_by_mask:
            return
        chosen = _admitted(mask, entries, releases)
        closed = chosen[resolved[chosen]]
        n = len(closed)
        w = int(wins[closed].sum())
        counts = np.bincount(states[closed], minlength=len(REGIMES))
        minimum = sum(minima.values())
        deficit = max(0, minimum-n)
        excess = max(0,n-minimum)/max(minimum,1)
        expectancy = float(returns[closed].sum())/n if n else 0
        score = -1000*deficit + 10*_wilson_lower(w,n) + 2*expectancy - excess - .05*len(rules)
        best_by_mask[key] = {"rules": rules, "mask": mask, "score": score, "count": n}
    screen(np.ones(len(rows), dtype=bool), [])
    for rule, mask in zip(thresholds, masks):
        screen(mask, [rule])
    for left, right in combinations(range(len(thresholds)), 2):
        if thresholds[left]["feature"] != thresholds[right]["feature"]:
            screen(masks[left] & masks[right], [thresholds[left], thresholds[right]])
    ranked = sorted(best_by_mask.values(), key=lambda c: -c["score"])
    # Keep both strong fits and frequency reserves for cross-role/pair occupancy.
    reserve_minimum = 1.25 * sum(minima.values())
    reserve = [c for c in ranked if c["count"] >= reserve_minimum][:24]
    broad = sorted(ranked, key=lambda c: (-c["count"], -c["score"]))[:8]
    options = ranked[:40] + reserve + broad
    unique = {}
    for candidate in options:
        unique.setdefault(np.packbits(candidate["mask"]).tobytes(), candidate)
    return list(unique.values()), evaluated


def _arrays(rows):
    entries = np.array([utc(r.get("entry_ts") or r["candle_ts"]).timestamp() for r in rows])
    releases = np.array([utc(r["occupancy_release"]).timestamp() for r in rows])
    resolved = np.array([r["outcome_status"] in ("WIN", "LOSS") and r["closed_in_window"] for r in rows], dtype=bool)
    wins = np.array([r["outcome_status"] == "WIN" for r in rows], dtype=bool)
    returns = np.array([float(r.get("realized_r") or 0) if resolved[i] else 0 for i,r in enumerate(rows)])
    states = np.array([REGIMES.index(r["features"]["major_trend_regime"]) for r in rows],dtype=int)
    return entries, releases, resolved, wins, returns, states


def _population(rows, instrument, start, end):
    result, seen = [], set()
    for source in sorted(rows, key=lambda r: r.get("entry_ts") or r["candle_ts"]):
        row = dict(source)
        if row["instrument"] != instrument or row.get("signal") not in ("BUY", "SELL"):
            continue
        if row.get("outcome_status") in ("INVALID", "ENTRY_INVALIDATED", "ENTRY_BLOCKED_OPERATIONAL_TIME"):
            continue
        if row.get("outcome_status") in ("DATA_INSUFFICIENT", "DATA_INTEGRITY_ERROR"):
            raise ValueError("Incomplete executable bid/ask evidence")
        entry = utc(row.get("entry_ts") or row["candle_ts"])
        identity = (instrument, row["candle_ts"])
        if identity in seen or not utc(start) <= entry < utc(end):
            continue
        if row.get("features",{}).get("major_trend_regime") not in REGIMES:
            raise ValueError("Missing causal trend on a research entry")
        seen.add(identity)
        exit_time = utc(row["exit_ts"]) if row.get("exit_ts") else utc(end)
        row["closed_in_window"] = bool(row.get("exit_ts")) and entry <= exit_time <= utc(end)
        release = exit_time + timedelta(minutes=1) if row["outcome_status"] in ("WIN","LOSS","AMBIGUOUS") else exit_time
        row["occupancy_release"] = max(entry, release).isoformat()
        result.append(row)
    return result


def optimize_pair(rows, exposure, instrument, start, end):
    if abs((utc(end)-utc(start)).total_seconds()/86400-60) > 1e-9:
        raise ValueError("Research must use exactly one 60-day window")
    targets = directional_targets(exposure)
    role_targets = {direction:{role:sum(n for regime,n in targets[direction].items() if role_for(direction,regime)==role)
                               for role in ROLES} for direction in ("BUY","SELL")}
    population = _population(rows, instrument, start, end)
    entries, releases, resolved, wins, returns, states = _arrays(population)
    buy_mask = np.array([r["signal"] == "BUY" for r in population], dtype=bool)
    slots, pools, searches = [], [], 0
    for direction in ("BUY", "SELL"):
        lane = [r for r in population if r["signal"] == direction]
        thresholds = _candidate_rules(lane)
        for role in ROLES:
            indices = [i for i,r in enumerate(population) if r["signal"] == direction
                       and role_for(direction,r["features"]["major_trend_regime"]) == role]
            subset = [population[i] for i in indices]
            minima = {regime:n for regime,n in targets[direction].items() if role_for(direction,regime)==role}
            options, count = _candidate_pool(subset, thresholds, minima)
            searches += count
            for option in options:
                mask = np.zeros(len(population), dtype=bool)
                mask[indices] = option["mask"]
                option["mask"] = mask
            slots.append((direction,role))
            pools.append(options)

    def assess(choice):
        mask = np.zeros(len(population),dtype=bool)
        for pool,index in zip(pools,choice):
            mask |= pool[index]["mask"]
        admitted = _admitted(mask,entries,releases)
        closed = admitted[resolved[admitted]]
        score = 0.0
        for direction, side in (("BUY",buy_mask),("SELL",~buy_mask)):
            selected = closed[side[closed]]
            n, w = len(selected), int(wins[selected].sum())
            counts = np.bincount(states[selected],minlength=len(REGIMES))
            role_counts = {role:sum(int(counts[i]) for i,regime in enumerate(REGIMES) if role_for(direction,regime)==role) for role in ROLES}
            deficit = sum(max(0,target-role_counts[role]) for role,target in role_targets[direction].items())
            excess = sum(max(0,role_counts[role]-target)/max(target,1) for role,target in role_targets[direction].items())
            net = float(returns[selected].sum())
            score -= 1000*(deficit + max(0,n-2*w+1) + (1+abs(net) if net <= 0 else 0))
            score += 10*_wilson_lower(w,n) + 2*net/max(n,1) - excess
        score -= .05*sum(len(pool[index]["rules"]) for pool,index in zip(pools,choice))
        return score, admitted

    starts = [tuple(0 for _ in pools),
              tuple(max(range(len(pool)),key=lambda i:pool[i]["count"]) for pool in pools)]
    best_choice, best_score = starts[0], float("-inf")
    pair_evaluations = 0
    for initial in starts:
        choice = list(initial)
        score,_ = assess(choice)
        for _ in range(4):
            improved = False
            for slot,pool in enumerate(pools):
                selected = choice[slot]
                for index in range(len(pool)):
                    trial = choice[:]
                    trial[slot] = index
                    candidate_score,_ = assess(trial)
                    pair_evaluations += 1
                    if candidate_score > score + 1e-9:
                        score,selected,improved = candidate_score,index,True
                choice[slot] = selected
            if not improved:
                break
        if score > best_score:
            best_choice,best_score = tuple(choice),score
    _, admitted = assess(best_choice)
    chosen = [population[i] for i in admitted if population[i]["closed_in_window"]]
    results = []
    for direction in ("BUY", "SELL"):
        lane = [r for r in population if r["signal"] == direction and r["closed_in_window"]]
        selected = [r for r in chosen if r["signal"] == direction]
        rules = {role:{"enabled":bool(pools[i][best_choice[i]]["mask"].any()),
                       "filters":pools[i][best_choice[i]]["rules"]}
                 for i,(side,role) in enumerate(slots) if side==direction}
        counts = Counter(r["features"]["major_trend_regime"] for r in selected if r["outcome_status"] in ("WIN","LOSS"))
        summary = metrics(selected,60)
        role_counts = {role:sum(counts[regime] for regime in REGIMES if role_for(direction,regime)==role) for role in ROLES}
        gates = {"adaptive_role_minima":all(role_counts[role]>=n for role,n in role_targets[direction].items()),
                 "win_rate_strictly_above_50":(summary["win_rate"] or 0)>.5,
                 "positive_expectancy_safety":(summary["expectancy_r"] or 0)>0}
        definition = {"instrument":instrument,"direction":direction,"conditional_rules":rules,
                      "trend_version":TREND_VERSION,"minimum_by_regime":targets[direction],
                      "minimum_by_role":role_targets[direction],
                      "minimum_resolved":sum(targets[direction].values()),"minimum_win_rate_exclusive":.5,
                      "paper_only":True,"production_authority":False,
                      "window":{"start":utc(start).isoformat(),"end":utc(end).isoformat(),"days":60}}
        definition["definition_sha256"] = hashlib.sha256(json.dumps(definition,sort_keys=True).encode()).hexdigest()
        results.append({"strategy_id":directional_strategy_id(instrument,direction),"instrument":instrument,
                        "direction":direction,"verdict":"PAPER_CANDIDATE" if all(gates.values()) else "INSUFFICIENT_60_DAY_EVIDENCE",
                        "minimum_by_regime":targets[direction],"resolved_by_regime":dict(counts),
                        "minimum_by_role":role_targets[direction],"resolved_by_role":role_counts,
                        "raw_resolved_by_regime":dict(Counter(r["features"]["major_trend_regime"] for r in lane if r["outcome_status"] in ("WIN","LOSS"))),
                        "baseline":metrics(lane,60),"metrics":summary,"final_gates":gates,"candidate":definition,
                        "evaluated_candidates":searches,"pair_occupancy_evaluations":pair_evaluations,
                        "role_metrics":{role:metrics([r for r in selected if role_for(direction,r["features"]["major_trend_regime"])==role],60) for role in ROLES}})
    return {"instrument":instrument,"exposure_minutes":exposure,"targets":targets,"results":results,
            "occupancy_protocol":"FILTER_THEN_CHRONOLOGICAL_SINGLE_POSITION",
            "minimum_unit":"DIRECTION_AND_RELATIVE_TREND_ROLE",
            "pair_qualified":all(r["verdict"]=="PAPER_CANDIDATE" for r in results)}


def optimize_all(rows_by_instrument, exposure_by_instrument, start, end):
    pairs = [optimize_pair(rows_by_instrument.get(inst, []), exposure_by_instrument[inst], inst, start, end)
             for inst in SUPPORTED_INSTRUMENTS]
    results = [r for pair in pairs for r in pair["results"]]
    approved_pairs = [p["instrument"] for p in pairs if p["pair_qualified"]]
    return {"schema_version": 2, "protocol": "ONE_60_DAY_CAUSAL_H1_H4_ADAPTIVE_PAPER",
            "window": {"start": utc(start).isoformat(), "end": utc(end).isoformat(), "days": 60},
            "trend_version": TREND_VERSION, "split": None, "comparison_periods": [],
            "selection_is_out_of_sample": False, "production_authority": False,
            "strategy_count": 10, "pairs": pairs, "results": results,
            "approved_pairs": approved_pairs, "approved_count": len(approved_pairs) * 2,
            "all_ten_qualified": len(approved_pairs) == 5}

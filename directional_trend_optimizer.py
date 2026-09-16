"""One 60-day in-sample selector with causal, reversal-aware evidence floors."""
from collections import Counter
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
        entry = utc(row.get("entry_ts") or row["candle_ts"])
        identity = (row["instrument"], row["candle_ts"])
        if identity in seen or entry < busy_until or not utc(start) <= entry < utc(end):
            continue
        seen.add(identity)
        exit_time = utc(row["exit_ts"]) if row.get("exit_ts") else utc(end)
        busy_until = max(entry, exit_time)
        if row.get("exit_ts") and entry <= exit_time <= utc(end):
            selected.append(dict(row))
    return selected


def _select_role(rows, thresholds, minima):
    if not rows:
        return None, 0
    resolved = np.array([r["outcome_status"] in ("WIN", "LOSS") for r in rows])
    wins = np.array([r["outcome_status"] == "WIN" for r in rows])
    returns = np.array([float(r.get("realized_r") or 0) if resolved[i] else 0 for i, r in enumerate(rows)])
    regimes = {regime: np.array([r["features"]["major_trend_regime"] == regime for r in rows])
               & resolved for regime in minima}
    masks = [np.array([rule_passes(r, rule) for r in rows]) for rule in thresholds]
    best, evaluated = None, 0

    def screen(mask, rules):
        nonlocal best, evaluated
        evaluated += 1
        if any(int(np.count_nonzero(mask & regimes[r])) < minimum for r, minimum in minima.items()):
            return
        n = int(np.count_nonzero(mask & resolved))
        if n == 0:
            return
        w = int(np.count_nonzero(mask & wins))
        expectancy = float(returns[mask].sum()) / n
        # Floors are hard constraints; role win rates are reported, not extra gates.
        score = 10 * _wilson_lower(w, n) + 2 * expectancy + .01 * np.log1p(n) - .05 * len(rules)
        if best is None or score > best[0]:
            best = (score, {"rules": rules, "rows": [r for r, keep in zip(rows, mask) if keep]})

    screen(np.ones(len(rows), dtype=bool), [])
    for rule, mask in zip(thresholds, masks):
        screen(mask, [rule])
    for left, right in combinations(range(len(thresholds)), 2):
        if thresholds[left]["feature"] != thresholds[right]["feature"]:
            screen(masks[left] & masks[right], [thresholds[left], thresholds[right]])
    return best[1] if best else None, evaluated


def optimize_pair(rows, exposure, instrument, start, end):
    if abs((utc(end) - utc(start)).total_seconds() / 86400 - 60) > 1e-9:
        raise ValueError("Research must use exactly one 60-day window")
    targets = directional_targets(exposure)
    population = closed_nonoverlapping(rows, start, end)
    results = []
    for direction in ("BUY", "SELL"):
        lane = [r for r in population if r["signal"] == direction
                and r.get("features", {}).get("major_trend_regime") in REGIMES]
        # Thresholds use every available date in this lane, not a first-month fit.
        thresholds = _candidate_rules(lane)
        chosen, role_rules, searches = [], {}, 0
        for role in ROLES:
            minima = {regime: value for regime, value in targets[direction].items()
                      if role_for(direction, regime) == role}
            subset = [r for r in lane if role_for(direction, r["features"]["major_trend_regime"]) == role]
            best, count = _select_role(subset, thresholds, minima)
            searches += count
            if best:
                chosen.extend(best["rows"])
                role_rules[role] = {"enabled": True, "filters": best["rules"]}
            else:
                role_rules[role] = {"enabled": False, "filters": []}
        chosen.sort(key=lambda r: r["candle_ts"])
        counts = Counter(r["features"]["major_trend_regime"] for r in chosen if r["outcome_status"] in ("WIN", "LOSS"))
        summary = metrics(chosen, 60)
        gates = {
            "regime_minima": all(counts[r] >= n for r, n in targets[direction].items()),
            "win_rate_strictly_above_50": (summary["win_rate"] or 0) > .5,
            "positive_expectancy_safety": (summary["expectancy_r"] or 0) > 0,
        }
        definition = {"instrument": instrument, "direction": direction, "conditional_rules": role_rules,
                      "trend_version": TREND_VERSION, "minimum_by_regime": targets[direction],
                      "minimum_resolved": sum(targets[direction].values()),
                      "minimum_win_rate_exclusive": .5, "paper_only": True, "production_authority": False,
                      "window": {"start": utc(start).isoformat(), "end": utc(end).isoformat(), "days": 60}}
        definition["definition_sha256"] = hashlib.sha256(json.dumps(definition, sort_keys=True).encode()).hexdigest()
        results.append({"strategy_id": directional_strategy_id(instrument, direction),
                        "instrument": instrument, "direction": direction,
                        "verdict": "PAPER_CANDIDATE" if all(gates.values()) else "INSUFFICIENT_60_DAY_EVIDENCE",
                        "minimum_by_regime": targets[direction], "resolved_by_regime": dict(counts),
                        "baseline": metrics(lane, 60), "metrics": summary, "final_gates": gates,
                        "candidate": definition, "evaluated_candidates": searches,
                        "role_metrics": {role: metrics([r for r in chosen if role_for(direction, r["features"]["major_trend_regime"]) == role], 60) for role in ROLES}})
    return {"instrument": instrument, "exposure_minutes": exposure, "targets": targets, "results": results,
            "pair_qualified": all(r["verdict"] == "PAPER_CANDIDATE" for r in results)}


def optimize_all(rows_by_instrument, exposure_by_instrument, start, end):
    pairs = [optimize_pair(rows_by_instrument.get(inst, []), exposure_by_instrument[inst], inst, start, end)
             for inst in SUPPORTED_INSTRUMENTS]
    results = [r for pair in pairs for r in pair["results"]]
    approved_pairs = [p["instrument"] for p in pairs if p["pair_qualified"]]
    return {"schema_version": 1, "protocol": "ONE_60_DAY_CAUSAL_H1_H4_ADAPTIVE_PAPER",
            "window": {"start": utc(start).isoformat(), "end": utc(end).isoformat(), "days": 60},
            "trend_version": TREND_VERSION, "split": None, "comparison_periods": [],
            "selection_is_out_of_sample": False, "production_authority": False,
            "strategy_count": 10, "pairs": pairs, "results": results,
            "approved_pairs": approved_pairs, "approved_count": len(approved_pairs) * 2,
            "all_ten_qualified": len(approved_pairs) == 5}

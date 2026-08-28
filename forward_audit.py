from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Sequence

POWER_MIN_USABLE = 30
POWER_MIN_WEAK = 15


def evidence_class(n: int) -> str:
    n = int(n)
    if n < POWER_MIN_WEAK:
        return "INCONCLUSIVE_UNDERPOWERED"
    if n < POWER_MIN_USABLE:
        return "WEAK_EVIDENCE"
    return "USABLE_EVIDENCE"


def _bool_map(record: Mapping[str, Any], names: Sequence[str]) -> Dict[str, bool]:
    raw = record.get("vetoes") or {}
    return {name: bool(raw.get(name, False)) for name in names}


def filter_overlap_audit(records: Iterable[Mapping[str, Any]], filter_names: Sequence[str]) -> Dict[str, Any]:
    rows = [dict(x) for x in records]
    names = list(filter_names)
    total = len(rows)
    per_filter: Dict[str, Any] = {}

    vectors = [_bool_map(r, names) for r in rows]

    for name in names:
        hit_idx = [i for i, v in enumerate(vectors) if v[name]]
        unique_idx = [
            i for i, v in enumerate(vectors)
            if v[name] and not any(v[o] for o in names if o != name)
        ]
        # Removing only this filter changes the combined veto iff this is the only
        # active veto on that observation. This is deliberately order-independent.
        remove_one_idx = unique_idx[:]
        per_filter[name] = {
            "veto_total": len(hit_idx),
            "veto_rate": (len(hit_idx) / total) if total else None,
            "true_unique_veto": len(unique_idx),
            "true_unique_rate": (len(unique_idx) / total) if total else None,
            "remove_one_delta": len(remove_one_idx),
            "exclusive_evidence_class": evidence_class(len(unique_idx)),
        }

    pairs: List[Dict[str, Any]] = []
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            a_set = {j for j, v in enumerate(vectors) if v[a]}
            b_set = {j for j, v in enumerate(vectors) if v[b]}
            inter = len(a_set & b_set)
            union = len(a_set | b_set)
            pairs.append({
                "filter_a": a,
                "filter_b": b,
                "intersection": inter,
                "union": union,
                "jaccard": (inter / union) if union else None,
                "overlap_of_a": (inter / len(a_set)) if a_set else None,
                "overlap_of_b": (inter / len(b_set)) if b_set else None,
            })

    combined_veto = sum(1 for v in vectors if any(v.values()))
    return {
        "population": total,
        "combined_veto": combined_veto,
        "combined_veto_rate": (combined_veto / total) if total else None,
        "filters": per_filter,
        "pairs": pairs,
        "interpretation": {
            "high_jaccard_means": "mechanical_redundancy_only",
            "high_jaccard_does_not_mean": "overfiltering_or_bad_veto",
            "exclusive_subsets_below_15": "INCONCLUSIVE_UNDERPOWERED",
        },
    }


def attach_outcomes(records: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize optional outcome fields for downstream reports without inventing labels."""
    out = []
    for r in records:
        x = dict(r)
        label = x.get("label")
        if label in (1, "1", "WIN"):
            x["outcome"] = "WIN"
        elif label in (0, "0", "LOSS"):
            x["outcome"] = "LOSS"
        else:
            x["outcome"] = None
        out.append(x)
    return out

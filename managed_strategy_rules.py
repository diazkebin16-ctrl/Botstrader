"""Generated Automation V3 PAPER-only research rule surface.

Only the instrument assignment lines below are mutable by the governed V3 adapter.
Runtime code and secrets are intentionally outside this change surface.
"""
from __future__ import annotations

import json
import math
from typing import Any, Mapping

SUPPORTED_INSTRUMENTS = ("AUD_USD", "EUR_USD", "GBP_USD", "USD_JPY", "USD_CAD")
APPROVED_FEATURES = (
    "rr_raw", "room_to_barrier_r", "extension_atr", "volatility_ratio",
    "direction_edge", "session_strength", "session_displacement_atr",
    "session_momentum_atr", "h1_gap_atr", "h1_slope_atr",
    "m15_gap_atr", "m15_slope_atr",
)
APPROVED_OPERATORS = (">=", "<=")
MANAGED_RULES_JSON = {}
MANAGED_RULES_JSON["AUD_USD"] = "[]"
MANAGED_RULES_JSON["EUR_USD"] = "[]"
MANAGED_RULES_JSON["GBP_USD"] = "[]"
MANAGED_RULES_JSON["USD_JPY"] = "[]"
MANAGED_RULES_JSON["USD_CAD"] = "[]"


def managed_rule_assignment(instrument: str) -> str:
    symbol=str(instrument or "").upper()
    if symbol not in SUPPORTED_INSTRUMENTS:raise ValueError("unsupported instrument")
    prefix=f'MANAGED_RULES_JSON["{symbol}"] = '
    for line in __loader_text().splitlines():
        if line.startswith(prefix):return line+"\n"
    raise RuntimeError("managed assignment missing")


def __loader_text() -> str:
    from pathlib import Path
    return Path(__file__).read_text(encoding="utf-8")


def rules_for(instrument: str) -> list[dict[str,Any]]:
    symbol=str(instrument or "").upper()
    if symbol not in SUPPORTED_INSTRUMENTS:return []
    value=json.loads(MANAGED_RULES_JSON[symbol])
    if not isinstance(value,list):raise ValueError("managed rules must be a list")
    return value


def non_v3_managed_strategy_identity(instrument: str) -> dict[str,Any]:
    return {"active":False,"instrument":str(instrument or "").upper(),"v3_candidate_id":None,
            "v3_candidate_definition_sha256":None,"v3_confidence_class":None,"v3_experimental":None,
            "v3_paper_only":None,"v3_managed_release_identity":None,"v3_source_code_sha":None,
            "production_authority":False}


def managed_strategy_identity(instrument: str) -> dict[str,Any]:
    symbol=str(instrument or "").upper();rules=rules_for(symbol)
    if not rules:return non_v3_managed_strategy_identity(symbol)
    required=("candidate_id","candidate_definition_sha256","confidence_class","experimental","paper_only",
              "managed_release_identity","source_code_sha","production_authority")
    first=rules[0]
    missing=[k for k in required if k not in first]
    if missing:raise ValueError("managed V3 identity incomplete: "+",".join(missing))
    ident={k:first.get(k) for k in required}
    for rule in rules[1:]:
        if any(rule.get(k)!=ident[k] for k in required):raise ValueError("managed V3 rules carry mixed release identities")
    cid=str(ident["candidate_id"] or "");dsha=str(ident["candidate_definition_sha256"] or "").lower()
    cls=str(ident["confidence_class"] or "").upper();rid=str(ident["managed_release_identity"] or "")
    code=str(ident["source_code_sha"] or "").lower();exp=ident["experimental"] is True
    if not cid or len(dsha)!=64 or any(c not in "0123456789abcdef" for c in dsha):raise ValueError("managed V3 candidate identity invalid")
    if cls not in ("STANDARD","EXPERIMENTAL") or exp!=(cls=="EXPERIMENTAL"):raise ValueError("managed V3 confidence metadata invalid")
    if ident["paper_only"] is not True or ident["production_authority"] is not False:raise ValueError("managed V3 release is not PAPER-only")
    if not rid.startswith("v3paper_") or len(code)!=40 or any(c not in "0123456789abcdef" for c in code):raise ValueError("managed V3 release/source identity invalid")
    return {"active":True,"instrument":symbol,"v3_candidate_id":cid,"v3_candidate_definition_sha256":dsha,
            "v3_confidence_class":cls,"v3_experimental":exp,"v3_paper_only":True,
            "v3_managed_release_identity":rid,"v3_source_code_sha":code,"production_authority":False}


def _finite(value: Any) -> float|None:
    try:
        result=float(value);return result if math.isfinite(result) else None
    except (TypeError,ValueError):return None


def evaluate_managed_strategy_rules(row: Mapping[str,Any]) -> dict[str,Any]:
    symbol=str((row or {}).get("instrument") or "").upper();rules=rules_for(symbol);identity=managed_strategy_identity(symbol)
    if not rules:return {"ok":True,"active":False,"rules":[],"vetoes":[],"instrument":symbol,"managed_strategy":identity}
    features=(row or {}).get("features") or {};results=[];vetoes=[]
    for rule in rules:
        feature=str(rule.get("feature") or "");operator=str(rule.get("operator") or "")
        if feature not in APPROVED_FEATURES or operator not in APPROVED_OPERATORS:raise ValueError("managed rule outside approved surface")
        value=features.get(feature) if isinstance(features,Mapping) else None
        if value is None and feature=="rr_raw":value=(row or {}).get("rr_raw")
        number=_finite(value);threshold=_finite(rule.get("threshold"))
        passed=None if number is None or threshold is None else (number>=threshold if operator==">=" else number<=threshold)
        item={"source":"automation_v3_managed","rule_key":rule.get("candidate_id"),"feature":feature,"operator":operator,"threshold":threshold,"passed":passed}
        results.append(item)
        if passed is False:vetoes.append(item)
    return {"ok":not vetoes,"active":True,"rules":results,"vetoes":vetoes,"instrument":symbol,"managed_strategy":identity}

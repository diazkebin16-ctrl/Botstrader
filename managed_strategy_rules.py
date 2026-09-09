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
APPROVED_RULE_MODES = ("ADMISSION", "VETO_WHEN_ALL")
MANAGED_RULES_JSON = {}
MANAGED_RULES_JSON["AUD_USD"] = "[]"
MANAGED_RULES_JSON["EUR_USD"] = "[{\"candidate_definition_sha256\":\"0deb30e90db521d0e53eed1b358d7e3c4991360ecad043c7feced32cb5c802ed\",\"candidate_id\":\"COMPOSITE:0666c65d5412\",\"confidence_class\":\"EXPERIMENTAL\",\"experimental\":true,\"feature\":\"session_displacement_atr\",\"managed_release_identity\":\"v3paper_a68db3ab70d0f383f74b2713b722d1a5996ff067f2cacf44ede496bdd7cf32eb\",\"operator\":\">=\",\"paper_only\":true,\"production_authority\":false,\"source_code_sha\":\"6d85a16776ae9c2b7dfce7c9a3cbbaa72edadaa7\",\"threshold\":-1.2592592592595422},{\"candidate_definition_sha256\":\"0deb30e90db521d0e53eed1b358d7e3c4991360ecad043c7feced32cb5c802ed\",\"candidate_id\":\"COMPOSITE:0666c65d5412\",\"confidence_class\":\"EXPERIMENTAL\",\"experimental\":true,\"feature\":\"m15_slope_atr\",\"managed_release_identity\":\"v3paper_a68db3ab70d0f383f74b2713b722d1a5996ff067f2cacf44ede496bdd7cf32eb\",\"operator\":\">=\",\"paper_only\":true,\"production_authority\":false,\"source_code_sha\":\"6d85a16776ae9c2b7dfce7c9a3cbbaa72edadaa7\",\"threshold\":-0.1589937827394618},{\"candidate_definition_sha256\":\"0deb30e90db521d0e53eed1b358d7e3c4991360ecad043c7feced32cb5c802ed\",\"candidate_id\":\"COMPOSITE:0666c65d5412\",\"confidence_class\":\"EXPERIMENTAL\",\"experimental\":true,\"feature\":\"m15_slope_atr\",\"managed_release_identity\":\"v3paper_a68db3ab70d0f383f74b2713b722d1a5996ff067f2cacf44ede496bdd7cf32eb\",\"operator\":\">=\",\"paper_only\":true,\"production_authority\":false,\"source_code_sha\":\"6d85a16776ae9c2b7dfce7c9a3cbbaa72edadaa7\",\"threshold\":-0.37804495081897455}]"
MANAGED_RULES_JSON["GBP_USD"] = "[]"
MANAGED_RULES_JSON["USD_JPY"] = "[{\"candidate_definition_sha256\":\"69077fda3300c984bf0558eeab7a424f3996c204a0bd5d7f3153bafbf6082f30\",\"candidate_id\":\"USDJPY_EXECUTED60_M15_LATE_BUY_V2\",\"confidence_class\":\"EXPERIMENTAL\",\"direction\":\"BUY\",\"evidence_sha256\":\"3cfad5cdbd0218591d3b7589c09056ac8d77bd5f012769b61f2d93b8c72fefc8\",\"experimental\":true,\"feature\":\"m15_gap_atr\",\"group_id\":\"M15_LATE_BUY\",\"managed_release_identity\":\"v3paper_cb3f46d5222193f7ef352e53b9a016bc3ac05bb396d236debae7fc250c7c79b0\",\"operator\":\">=\",\"paper_only\":true,\"production_authority\":false,\"rule_mode\":\"VETO_WHEN_ALL\",\"source_code_sha\":\"c40f2ff1f57b0d1103c3e0afbe4bee02813a8584\",\"threshold\":0.7},{\"candidate_definition_sha256\":\"69077fda3300c984bf0558eeab7a424f3996c204a0bd5d7f3153bafbf6082f30\",\"candidate_id\":\"USDJPY_EXECUTED60_M15_LATE_BUY_V2\",\"confidence_class\":\"EXPERIMENTAL\",\"direction\":\"BUY\",\"evidence_sha256\":\"3cfad5cdbd0218591d3b7589c09056ac8d77bd5f012769b61f2d93b8c72fefc8\",\"experimental\":true,\"feature\":\"m15_slope_atr\",\"group_id\":\"M15_LATE_BUY\",\"managed_release_identity\":\"v3paper_cb3f46d5222193f7ef352e53b9a016bc3ac05bb396d236debae7fc250c7c79b0\",\"operator\":\">=\",\"paper_only\":true,\"production_authority\":false,\"rule_mode\":\"VETO_WHEN_ALL\",\"source_code_sha\":\"c40f2ff1f57b0d1103c3e0afbe4bee02813a8584\",\"threshold\":0.25}]"
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


def _rule_predicate(rule: Mapping[str,Any], features: Mapping[str,Any], row: Mapping[str,Any]) -> tuple[float|None,float|None,bool|None]:
    feature=str(rule.get("feature") or "");operator=str(rule.get("operator") or "")
    if feature not in APPROVED_FEATURES or operator not in APPROVED_OPERATORS:raise ValueError("managed rule outside approved surface")
    value=features.get(feature) if isinstance(features,Mapping) else None
    if value is None and feature=="rr_raw":value=(row or {}).get("rr_raw")
    number=_finite(value);threshold=_finite(rule.get("threshold"))
    matched=None if number is None or threshold is None else (number>=threshold if operator==">=" else number<=threshold)
    return number,threshold,matched


def evaluate_managed_strategy_rules(row: Mapping[str,Any]) -> dict[str,Any]:
    symbol=str((row or {}).get("instrument") or "").upper();rules=rules_for(symbol);identity=managed_strategy_identity(symbol)
    if not rules:return {"ok":True,"active":False,"rules":[],"vetoes":[],"instrument":symbol,"managed_strategy":identity}
    features=(row or {}).get("features") or {};results=[];vetoes=[];veto_groups={}
    row_direction=str((row or {}).get("signal") or (row or {}).get("direction") or "").upper()
    for rule in rules:
        mode=str(rule.get("rule_mode") or "ADMISSION").upper()
        if mode not in APPROVED_RULE_MODES:raise ValueError("managed rule mode outside approved surface")
        _,threshold,matched=_rule_predicate(rule,features,row)
        feature=str(rule.get("feature") or "");operator=str(rule.get("operator") or "")
        if mode=="ADMISSION":
            item={"source":"automation_v3_managed","rule_key":rule.get("candidate_id"),"feature":feature,"operator":operator,"threshold":threshold,"passed":matched}
            results.append(item)
            if matched is False:vetoes.append(item)
            continue
        group_id=str(rule.get("group_id") or "")
        if not group_id:raise ValueError("managed veto rule missing group_id")
        direction=str(rule.get("direction") or "").upper()
        if direction and direction not in ("BUY","SELL"):raise ValueError("managed veto direction invalid")
        group=veto_groups.setdefault(group_id,{"direction":direction,"conditions":[],"candidate_id":rule.get("candidate_id")})
        if group["direction"]!=direction:raise ValueError("managed veto group carries mixed directions")
        group["conditions"].append({"feature":feature,"operator":operator,"threshold":threshold,"matched":matched})
    for group_id,group in veto_groups.items():
        applicable=not group["direction"] or row_direction==group["direction"]
        missing=applicable and any(x["matched"] is None for x in group["conditions"])
        matched=bool(applicable and not missing and all(x["matched"] is True for x in group["conditions"]))
        item={"source":"automation_v3_managed","rule_key":f'{group["candidate_id"]}:{group_id}',
              "rule_mode":"VETO_WHEN_ALL","group_id":group_id,"direction":group["direction"] or None,
              "applicable":applicable,"conditions":group["conditions"],"passed":not matched and not missing}
        if missing:item["reason"]="REQUIRED_PRE_ENTRY_EVIDENCE_MISSING"
        elif matched:item["reason"]="VETO_CONDITIONS_MATCHED"
        results.append(item)
        if matched or missing:vetoes.append(item)
    return {"ok":not vetoes,"active":True,"rules":results,"vetoes":vetoes,"instrument":symbol,"managed_strategy":identity}

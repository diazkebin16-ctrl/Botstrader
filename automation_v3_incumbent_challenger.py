
"""Governed incumbent-vs-challenger identity and economic comparison helpers.

This module contains no trading execution. It binds the deployed incumbent identity
and compares already-computed metrics on identical chronological populations.
"""
from __future__ import annotations
import hashlib
import json
from typing import Any, Mapping, Sequence

def canonical_sha256(value: Any) -> str:
    raw=json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def build_incumbent_definition(*, instrument: str, code_sha: str, dataset_identity: Mapping[str,Any], managed_rules: Sequence[Mapping[str,Any]], methodology: Mapping[str,Any]) -> dict[str,Any]:
    symbol=str(instrument or "").upper()
    if not symbol or not code_sha or len(code_sha)!=40:
        raise ValueError("unknown incumbent identity")
    if not isinstance(dataset_identity,Mapping) or dataset_identity.get("code_sha")!=code_sha:
        raise ValueError("incumbent dataset/code identity mismatch")
    rules=[dict(rule) for rule in managed_rules]
    strategy_identity={"baseline_gate":"REPLAY_ACTIONABLE_CURRENT_STRATEGY","managed_rules":rules}
    definition={
        "instrument":symbol,
        "baseline_gate":"REPLAY_ACTIONABLE_CURRENT_STRATEGY",
        "managed_rules":rules,
        "code_sha":code_sha,
        "strategy_config_identity":canonical_sha256(strategy_identity),
        "dataset_identity":dict(dataset_identity),
        "dataset_identity_sha256":canonical_sha256(dataset_identity),
        "methodology_identity":canonical_sha256(methodology),
        "production_authority":False,
    }
    definition["incumbent_definition_sha256"]=canonical_sha256({k:v for k,v in definition.items() if k!="incumbent_definition_sha256"})
    return definition

def compare_metrics(*, incumbent: Mapping[str,Any], challenger: Mapping[str,Any], evaluation_population_sha256: str) -> dict[str,Any]:
    if not evaluation_population_sha256:
        raise ValueError("evaluation population identity missing")
    def delta(name: str):
        left=challenger.get(name); right=incumbent.get(name)
        return None if left is None or right is None else float(left)-float(right)
    exp=delta("expectancy_r"); pf=delta("profit_factor"); wr=delta("win_rate")
    resolved=int(challenger.get("resolved_binary") or 0)-int(incumbent.get("resolved_binary") or 0)
    beats=exp is not None and pf is not None and exp>0 and pf>0
    return {
        "incumbent":dict(incumbent),"challenger":dict(challenger),
        "evaluation_population_sha256":evaluation_population_sha256,
        "expectancy_delta_vs_incumbent":exp,
        "win_rate_delta_vs_incumbent":wr,
        "profit_factor_delta_vs_incumbent":pf,
        "drawdown_delta_vs_incumbent":None,
        "resolved_delta_vs_incumbent":resolved,
        "challenger_beats_incumbent":beats,
        "material_improvement":beats,
        "materiality_basis":"EXPECTANCY_AND_PROFIT_FACTOR_IMPROVE_WITH_EXISTING_ROBUSTNESS_GATES",
        "production_authority":False,
    }

def diagnostic_state(*, discovery_comparison: Mapping[str,Any], validation_comparison: Mapping[str,Any], robust: bool, deployable: bool) -> str:
    both=discovery_comparison.get("challenger_beats_incumbent") is True and validation_comparison.get("challenger_beats_incumbent") is True
    if deployable:return "CHALLENGER_DEPLOYABLE"
    if both and not robust:return "CHALLENGER_BETTER_BUT_NOT_ROBUST"
    return "NO_MEANINGFUL_IMPROVEMENT"

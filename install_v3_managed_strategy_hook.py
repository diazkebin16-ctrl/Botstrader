#!/usr/bin/env python3
"""One-time branch migration for the Automation V3 managed strategy hook."""
from pathlib import Path


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if new in text:
        return
    if text.count(old) != 1:
        raise SystemExit(f"expected exactly one integration point in {path}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8", newline="\n")


repo = Path(__file__).resolve().parent
server = repo / "server.py"
optimizer = repo / "autonomous_asset_optimizer.py"

replace_once(
    server,
    "from instrument_profiles import instrument_profile\n",
    "from instrument_profiles import instrument_profile\nfrom managed_strategy_rules import evaluate_managed_strategy_rules\n",
)
replace_once(
    server,
    '''    rules=get_active_research_rules()\n    if not rules:return {"ok":True,"active":False,"rules":[],"vetoes":[]}\n    results=[];vetoes=[]\n    for rule in rules:\n        passed=_rule_match_for_dict(rule["source"],rule["rule_key"],r)\n        item={"source":rule["source"],"rule_key":rule["rule_key"],"status":rule["status"],"passed":passed}\n        results.append(item)\n        if passed is False:vetoes.append(item)\n    return {"ok":not vetoes,"active":True,"rules":results,"vetoes":vetoes}\n''',
    '''    managed=evaluate_managed_strategy_rules(r or {})\n    rules=get_active_research_rules()\n    if not rules and not managed.get("active"):\n        return {"ok":True,"active":False,"rules":[],"vetoes":[],"instrument":instrument}\n    results=list(managed.get("rules") or []);vetoes=list(managed.get("vetoes") or [])\n    for rule in rules:\n        passed=_rule_match_for_dict(rule["source"],rule["rule_key"],r)\n        item={"source":rule["source"],"rule_key":rule["rule_key"],"status":rule["status"],"passed":passed}\n        results.append(item)\n        if passed is False:vetoes.append(item)\n    return {"ok":not vetoes,"active":True,"rules":results,"vetoes":vetoes,"instrument":instrument}\n''',
)
replace_once(
    optimizer,
    "from automation_v3_release import ReleaseController as GovernedReleaseController\n",
    "from automation_v3_release import ReleaseController as GovernedReleaseController\nfrom automation_v3_candidate_mapping import CandidateNotDeployable,compile_and_write_release_plan\n",
)
replace_once(
    optimizer,
    'TERMINAL_STATES={"PAPER_DEPLOYED","NO_VALID_CANDIDATE","INSUFFICIENT_EVIDENCE","DATA_SOURCE_UNAVAILABLE","METHODOLOGY_BLOCKED","TEST_FAILURE","DEPLOYMENT_FAILURE","UNSUPPORTED_INSTRUMENT"}\n',
    'TERMINAL_STATES={"PAPER_DEPLOYED","PAPER_DEPLOYABLE_CANDIDATE","CANDIDATE_NOT_DEPLOYABLE","NO_VALID_CANDIDATE","INSUFFICIENT_EVIDENCE","DATA_SOURCE_UNAVAILABLE","METHODOLOGY_BLOCKED","TEST_FAILURE","DEPLOYMENT_FAILURE","UNSUPPORTED_INSTRUMENT"}\n',
)
old = '''   cand=next(x for x in ranking if isinstance(x,Mapping) and x.get("status")=="RESEARCH_CANDIDATE");plan=ad/"paper_release_plan.json";write_json(plan,{"instrument":i,"candidate":cand,"source_code_sha":sha,"production_authority":False,"created_at":utc_now()});rel=self.release.prepare_test_merge(plan=plan,base_sha=sha,instrument=i);ledger.mutate(i,release=rel)\n   if rel.get("status")!="PASS":return self._terminal(ledger,i,"TEST_FAILURE" if rel.get("reason") in {"TEST_FAILURE","GIT_DIFF_CHECK_FAILED","DIFF_POLICY_BLOCK"} else "DEPLOYMENT_FAILURE",str(rel.get("reason")),release=rel)\n'''
new = '''   cand=next(x for x in ranking if isinstance(x,Mapping) and x.get("status")=="RESEARCH_CANDIDATE");plan=ad/"paper_release_plan.json";write_json(plan,{"instrument":i,"candidate":cand,"source_code_sha":sha,"production_authority":False})\n   try:compiled=compile_and_write_release_plan(repo=self.repo,plan_path=plan,instrument=i,source_code_sha=sha)\n   except CandidateNotDeployable as exc:return self._terminal(ledger,i,"CANDIDATE_NOT_DEPLOYABLE",str(exc),lookback_months=months)\n   ledger.mutate(i,status="PAPER_DEPLOYABLE_CANDIDATE",paper_release_plan=str(plan),paper_release_plan_sha256=canonical_sha256(compiled))\n   rel=self.release.prepare_test_merge(plan=plan,base_sha=sha,instrument=i);ledger.mutate(i,release=rel)\n   if rel.get("status")=="CANDIDATE_NOT_DEPLOYABLE":return self._terminal(ledger,i,"CANDIDATE_NOT_DEPLOYABLE",str(rel.get("reason")),release=rel)\n   if rel.get("status")!="PASS":return self._terminal(ledger,i,"TEST_FAILURE" if rel.get("reason") in {"TEST_FAILURE","GIT_DIFF_CHECK_FAILED","DIFF_POLICY_BLOCK"} else "DEPLOYMENT_FAILURE",str(rel.get("reason")),release=rel)\n'''
replace_once(optimizer, old, new)
print("Automation V3 managed strategy hook installed")

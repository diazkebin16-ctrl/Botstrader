from pathlib import Path

def replace_once(path,old,new):
    p=Path(path);s=p.read_text(encoding='utf-8')
    if s.count(old)!=1:raise SystemExit(f'{path}: anchor count={s.count(old)}')
    p.write_text(s.replace(old,new,1),encoding='utf-8')

p='server.py'
replace_once(p,'from managed_strategy_rules import evaluate_managed_strategy_rules\n',
             'from managed_strategy_rules import evaluate_managed_strategy_rules, managed_strategy_identity, non_v3_managed_strategy_identity\n')
replace_once(p,
'''        "filters":r.get("filters") or {},\n        # Persist the pre-entry experimental identity/gate result with the trade\n''',
'''        "filters":r.get("filters") or {},\n        # Immutable V3 identity captured by the pre-order lifecycle gate; never re-read current strategy here.\n        "v3_managed_strategy":dict(r.get("v3_managed_strategy") or non_v3_managed_strategy_identity(r.get("instrument"))),\n        # Persist the pre-entry experimental identity/gate result with the trade\n''')
replace_once(p,
'''    research_gate=evaluate_active_research_rules(r)\n    if not research_gate["ok"]:\n''',
'''    try:\n        v3_identity=managed_strategy_identity(r.get("instrument"))\n        r["v3_managed_strategy"]=v3_identity\n        if v3_identity.get("active"):\n            v3_lifecycle_gate=deployment_manager.managed_paper_entry_gate(v3_identity)\n            r["v3_managed_paper_gate"]=v3_lifecycle_gate\n            if not v3_lifecycle_gate.get("allow"):\n                return {"execute":False,"reason":"V3 managed PAPER lifecycle veto: "+"; ".join(v3_lifecycle_gate.get("reasons") or []),\n                        "v3_managed_paper_gate":v3_lifecycle_gate}\n    except Exception as exc:\n        return {"execute":False,"reason":"V3 managed PAPER lifecycle unavailable (fail-closed): "+str(exc)}\n\n    research_gate=evaluate_active_research_rules(r)\n    if not research_gate["ok"]:\n''')
replace_once(p,
'''    if closed:\n        refresh_trade_memory_degradation()\n    return {"enabled":True,"checked":len(rows),"closed":closed,"errors":errors}\n''',
'''    if closed:\n        refresh_trade_memory_degradation()\n    v3_feedback={"checked":0,"processed":0,"attributed":0,"non_v3":0,"errors":[]}\n    if DEPLOYMENT_MANAGER_ENABLED:\n        try:v3_feedback=deployment_manager.reconcile_managed_paper_trade_memory(limit=max(200,TRADE_MEMORY_RECONCILE_LIMIT*4))\n        except Exception as exc:errors.append({"component":"v3_managed_paper_feedback","error":str(exc)})\n    return {"enabled":True,"checked":len(rows),"closed":closed,"errors":errors,"v3_paper_feedback":v3_feedback}\n''')

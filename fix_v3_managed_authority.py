#!/usr/bin/env python3
from pathlib import Path

def rep(path,old,new):
 p=Path(path);t=p.read_text(encoding='utf-8')
 if new in t:return
 if t.count(old)!=1:raise SystemExit(f'patch point mismatch {path}')
 p.write_text(t.replace(old,new,1),encoding='utf-8',newline='\n')

rep('server.py',
'''    instrument=InstrumentRegistry.normalize_symbol((r or {}).get("instrument") or PRIMARY_INSTRUMENT)\n    if not instrument_profile(instrument).learned_research_veto_authority:\n        return {"ok":True,"active":False,"rules":[],"vetoes":[],\n                "reason":"instrument_scoped_research_not_validated","instrument":instrument}\n    managed=evaluate_managed_strategy_rules(r or {})\n    rules=get_active_research_rules()\n''',
'''    instrument=InstrumentRegistry.normalize_symbol((r or {}).get("instrument") or PRIMARY_INSTRUMENT)\n    managed=evaluate_managed_strategy_rules(r or {})\n    legacy_authority=instrument_profile(instrument).learned_research_veto_authority\n    if not legacy_authority and not managed.get("active"):\n        return {"ok":True,"active":False,"rules":[],"vetoes":[],\n                "reason":"instrument_scoped_research_not_validated","instrument":instrument}\n    rules=get_active_research_rules() if legacy_authority else []\n''')
rep('autonomous_asset_optimizer.py',
'''def main():\n p=argparse.ArgumentParser(description="BotsTrader Automation V3 one-command optimizer; PAPER maximum authority");p.add_argument("instrument");a=p.parse_args();r=AutonomousAssetOptimizer(Path(__file__).resolve().parent).optimize(a.instrument);print(json.dumps(r,indent=2,sort_keys=True));raise SystemExit(0 if r.get("status") in {"PAPER_DEPLOYED","NO_VALID_CANDIDATE","INSUFFICIENT_EVIDENCE"} else 2)\n''',
'''def main():\n p=argparse.ArgumentParser(description="BotsTrader Automation V3 one-command optimizer; PAPER maximum authority");p.add_argument("instrument");a=p.parse_args();r=AutonomousAssetOptimizer(Path(__file__).resolve().parent).optimize(a.instrument);print(json.dumps(r,indent=2,sort_keys=True));raise SystemExit(0 if r.get("status") in {"PAPER_DEPLOYED","CANDIDATE_NOT_DEPLOYABLE","NO_VALID_CANDIDATE","INSUFFICIENT_EVIDENCE"} else 2)\n''')
print('managed authority isolation fixed')

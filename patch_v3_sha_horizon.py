from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text(encoding='utf-8')
    if text.count(old) != 1:
        raise SystemExit(f'{path}: expected 1 match, got {text.count(old)}')
    p.write_text(text.replace(old, new, 1), encoding='utf-8')


p='autonomous_asset_optimizer.py'
replace_once(p,
"def utc_now():return datetime.now(timezone.utc).isoformat().replace(\"+00:00\",\"Z\")\n",
"def utc_now():return datetime.now(timezone.utc).isoformat().replace(\"+00:00\",\"Z\")\n"
"def aligned_research_end(now,horizon_minutes=240):\n"
" d=now.astimezone(timezone.utc)-timedelta(minutes=int(horizon_minutes));return d.replace(minute=0,second=0,microsecond=0)\n")
replace_once(p,
"  end=self.now().astimezone(timezone.utc)-timedelta(minutes=240);ledger.mutate(i,status=\"RUNNING\",code_sha=sha,workspace=str(root),max_lookback_months=12)\n",
"  end=aligned_research_end(self.now(),240);ledger.mutate(i,status=\"RUNNING\",code_sha=sha,workspace=str(root),max_lookback_months=12)\n")
old='''        m.register_asset(i,code_sha=sha,start=start.isoformat(),end=end.isoformat(),warmup_days=10,horizon_minutes=240,data_sha256=sha256_file(cache));c.run(i,stages,through="prompts");err=None;ledger.append(i,"decision_history",{"decision":"DATA_REACQUIRE_SUCCEEDED","months":months,"at":utc_now()})\n'''
new='''        final_data_sha=sha256_file(cache);m.register_asset(i,code_sha=sha,start=start.isoformat(),end=end.isoformat(),warmup_days=10,horizon_minutes=240,data_sha256=final_data_sha);stages=builder(repo=self.repo,python=sys.executable,instrument=i,cache=cache,workspace=ad,start=start.isoformat(),end=end.isoformat(),warmup=10,horizon=240,variant="V331_BASELINE",embargo=30,discovery_fraction=.60,validation_fraction=.20,min_resolved=10,code_sha=sha,state=state);write_json(ad/"cascade_manifest.json",{"schema_version":3,"automation":"V3","instrument":i,"code_sha":sha,"data_sha256":final_data_sha,"production_authority":False,"stages":[{"name":x.name,"artifact":str(x.artifact),"command":list(x.command)} for x in stages]});c.run(i,stages,through="prompts");err=None;ledger.append(i,"decision_history",{"decision":"DATA_REACQUIRE_SUCCEEDED","months":months,"data_sha256":final_data_sha,"at":utc_now()})\n'''
replace_once(p,old,new)
# Initial manifest also binds the exact cache SHA used to build commands.
replace_once(p,
'''stages=builder(repo=self.repo,python=sys.executable,instrument=i,cache=cache,workspace=ad,start=start.isoformat(),end=end.isoformat(),warmup=10,horizon=240,variant="V331_BASELINE",embargo=30,discovery_fraction=.60,validation_fraction=.20,min_resolved=10,code_sha=sha,state=state);write_json(ad/"cascade_manifest.json",{"schema_version":3,"automation":"V3","instrument":i,"code_sha":sha,"production_authority":False,"stages":[{"name":x.name,"artifact":str(x.artifact),"command":list(x.command)} for x in stages]})\n''',
'''stages=builder(repo=self.repo,python=sys.executable,instrument=i,cache=cache,workspace=ad,start=start.isoformat(),end=end.isoformat(),warmup=10,horizon=240,variant="V331_BASELINE",embargo=30,discovery_fraction=.60,validation_fraction=.20,min_resolved=10,code_sha=sha,state=state);write_json(ad/"cascade_manifest.json",{"schema_version":3,"automation":"V3","instrument":i,"code_sha":sha,"data_sha256":sha256_file(cache),"production_authority":False,"stages":[{"name":x.name,"artifact":str(x.artifact),"command":list(x.command)} for x in stages]})\n''')

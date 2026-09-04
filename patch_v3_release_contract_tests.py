#!/usr/bin/env python3
from pathlib import Path

p=Path('test_automation_v3.py')
t=p.read_text(encoding='utf-8')
start=t.index('def _release_repo(')
end=t.index('\nclass UnavailableData:',start)
new=r'''def _release_repo(tmp_path, monkeypatch, change):
    import hashlib, json, subprocess, sys
    from automation_v3_candidate_mapping import canonical_sha256
    from automation_v3_release import TESTS, ReleaseController
    repo=tmp_path/'release_repo';repo.mkdir()
    subprocess.run(['git','init','-q','-b','main'],cwd=repo,check=True)
    subprocess.run(['git','config','user.email','v3@example.com'],cwd=repo,check=True)
    subprocess.run(['git','config','user.name','V3 Test'],cwd=repo,check=True)
    for name in TESTS:
        (repo/name).write_text('def test_ok(): assert True\n',encoding='utf-8')
    (repo/'safe.py').write_text('VALUE=1\n',encoding='utf-8')
    (repo/'managed_strategy_rules.py').write_text(
        'MANAGED_RULES_JSON = {}\n'
        'MANAGED_RULES_JSON["AUD_USD"] = "[]"\n'
        'MANAGED_RULES_JSON["EUR_USD"] = "[]"\n'
        'MANAGED_RULES_JSON["GBP_USD"] = "[]"\n'
        'MANAGED_RULES_JSON["USD_JPY"] = "[]"\n'
        'MANAGED_RULES_JSON["USD_CAD"] = "[]"\n',encoding='utf-8')
    changer=repo/'changer.py'
    prefix="""import json,os\nfrom pathlib import Path\nplan=json.loads(Path(os.environ['BOTS_V3_RELEASE_PLAN']).read_text())\nitem=plan['code_changes'][0]\np=Path(item['path']);text=p.read_text();assert text.count(item['old_text'])==item['expected_occurrences'];p.write_text(text.replace(item['old_text'],item['new_text'],1))\n"""
    changer.write_text(prefix+change,encoding='utf-8')
    subprocess.run(['git','add','-A'],cwd=repo,check=True);subprocess.run(['git','commit','-qm','base'],cwd=repo,check=True)
    base=subprocess.run(['git','rev-parse','HEAD'],cwd=repo,text=True,capture_output=True,check=True).stdout.strip()
    bare=tmp_path/'remote.git';subprocess.run(['git','init','-q','--bare',str(bare)],check=True)
    subprocess.run(['git','remote','add','origin',str(bare)],cwd=repo,check=True);subprocess.run(['git','push','-q','-u','origin','main'],cwd=repo,check=True)
    monkeypatch.setenv('BOTS_V3_CODE_CHANGE_COMMAND',f'{sys.executable} {changer}')
    def write(name,value):
        q=tmp_path/name;q.write_text(json.dumps(value,sort_keys=True)+'\n',encoding='utf-8');return q
    def sha(q): return hashlib.sha256(q.read_bytes()).hexdigest()
    definition={"id":"candidate-1","rules":[{"feature":"room_to_barrier_r","operator":">=","threshold":0.75}],"filter":"room_to_barrier_r","subfilter":None,"old_rule":"NO_PHASE_2_THRESHOLD","candidate_rule":"test","threshold":0.75,"direction_semantics":"SAME_NUMERIC_PREDICATE_FOR_BUY_AND_SELL","entry_time_only":True}
    dsha=canonical_sha256(definition);identity={"instrument":"AUD_USD","code_sha":base,"data_sha256":"d"*64}
    target=write('03_target_population.json',{"instrument":"AUD_USD","dataset_identity":identity})
    phase2=write('05_phase_2.json',{"instrument":"AUD_USD","dataset_identity":identity,"input_sha256":sha(target)})
    discovery=write('06_discovery.json',{"instrument":"AUD_USD","dataset_identity":identity,"input_sha256":sha(target),"phase2_sha256":sha(phase2),"proposed_frozen_candidate":{"candidate":definition}})
    freeze=write('09_freeze.json',{"status":"OK","freeze_status":"FROZEN_IMMUTABLE","immutable":True,"holdout_opened":False,"instrument":"AUD_USD","candidate_id":"candidate-1","candidate_definition":definition,"candidate_definition_sha256":dsha,"dataset_identity":identity,"code_sha":base,"target_population_sha256":sha(target),"phase2_sha256":sha(phase2),"discovery_sha256":sha(discovery)})
    write('10_holdout.json',{"status":"PASS","stage":"holdout","instrument":"AUD_USD","decision":"RESEARCH_CANDIDATE_SURVIVED_HOLDOUT","retuning_after_holdout":False,"holdout_opened_once":True,"input_sha256":sha(target),"phase2_sha256":sha(phase2),"freeze_sha256":sha(freeze),"candidate_definition_sha256":dsha})
    write('11_audit.json',{"status":"PASS","stage":"audit","production_authority":False})
    write('13_pre_audit.json',{"verdict":"ACCEPT","production_authority":False})
    plan=write('paper_release_plan.json',{"instrument":"AUD_USD","candidate":{"candidate_id":"candidate-1"},"source_code_sha":base,"production_authority":False})
    return repo,base,plan,ReleaseController(repo)

def test_release_controller_creates_branch_tests_and_ff_merges(tmp_path,monkeypatch):
    repo,base,plan,r=_release_repo(tmp_path,monkeypatch,"")
    out=r.prepare_test_merge(plan=plan,base_sha=base,instrument='AUD_USD')
    assert out['status']=='PASS' and out['merged_main_sha'] and out['branch'].startswith('automation-v3/')
    assert out['changed_files']==['managed_strategy_rules.py']

def test_release_controller_blocks_protected_live_file(tmp_path,monkeypatch):
    repo,base,plan,r=_release_repo(tmp_path,monkeypatch,"from pathlib import Path\nPath('server.py').write_text('x=1\\n')\n")
    out=r.prepare_test_merge(plan=plan,base_sha=base,instrument='AUD_USD')
    assert out['status']=='BLOCKED' and out['reason']=='DIFF_POLICY_BLOCK'

def test_release_controller_blocks_secret_marker(tmp_path,monkeypatch):
    repo,base,plan,r=_release_repo(tmp_path,monkeypatch,"from pathlib import Path\nPath('safe.py').write_text('OANDA_TOKEN=abc\\n')\n")
    out=r.prepare_test_merge(plan=plan,base_sha=base,instrument='AUD_USD')
    assert out['status']=='BLOCKED' and out['reason']=='DIFF_POLICY_BLOCK'

def test_release_controller_test_failure_prevents_merge(tmp_path,monkeypatch):
    repo,base,plan,r=_release_repo(tmp_path,monkeypatch,"from pathlib import Path\nPath('test_automation_v3.py').write_text('def test_bad(): assert False\\n')\n")
    out=r.prepare_test_merge(plan=plan,base_sha=base,instrument='AUD_USD')
    assert out['status']=='BLOCKED' and out['reason']=='TEST_FAILURE'

def test_release_controller_dirty_tree_prevents_branch(tmp_path,monkeypatch):
    repo,base,plan,r=_release_repo(tmp_path,monkeypatch,"")
    (repo/'safe.py').write_text('dirty=1\n',encoding='utf-8')
    out=r.prepare_test_merge(plan=plan,base_sha=base,instrument='AUD_USD')
    assert out['status']=='BLOCKED' and out['reason']=='BASE_OR_WORKTREE_MISMATCH'
'''
p.write_text(t[:start]+new+t[end:],encoding='utf-8',newline='\n')
print('release contract tests migrated')

import json
from pathlib import Path
import types
import pytest

from autonomous_asset_optimizer import (
    AUTOMATION_APPROVAL_TYPE, AUTOMATION_AUTHORITY, AUTOMATION_SCOPE,
    PRACTICE_OANDA_URL, ReleaseController, V3Ledger, canonical_sha256,
    diagnose_discovery, validate_autonomous_phase1,
)
from research_governance import DecisionGateEngine


def _phase1(**updates):
    best={"opened_gates":[],"wins_recovered":2,"losses_released":4,"eligible_episodes":9}
    p={"stage":"phase_1","instrument":"AUD_USD","status":"REVIEW_REQUIRED","all_target_wins_recovered":False,
       "selection_scope":"DISCOVERY_ONLY","lookahead_protection":True,"input_sha256":"target",
       "unrecovered_target_wins":[{"immutable_blocks":["WAIT_DIRECTION"],"relaxable_blocks":[]}],
       "best_policy":best,"candidates":[best,{"opened_gates":["LOW_ROOM"],"wins_recovered":2,"losses_released":7,"eligible_episodes":14}]}
    p.update(updates); return p


def _approval(phase1=None, **updates):
    p=phase1 or _phase1(); a={"approval_type":AUTOMATION_APPROVAL_TYPE,"approval_authority":AUTOMATION_AUTHORITY,
       "authorization_scope":AUTOMATION_SCOPE,"active":True,"ia1_approved":False,"human_approval":False,
       "production_authority":False,"instrument":"AUD_USD","dataset_identity":"d","code_sha":"c",
       "phase1_artifact_sha256":"a","best_policy_sha256":canonical_sha256(p["best_policy"])}; a.update(updates); return a


def _gate(approval):
    return DecisionGateEngine.evaluate("PHASE_2",integrity={"status":"PASS"},replay={"methodology":{"no_lookahead_decision":True}},
        target_population={"lookahead_protection":True},phase1=_phase1(),phase1_approval=approval,instrument="AUD_USD",
        dataset_identity="d",code_sha="c",phase1_artifact_sha256="a")


def test_phase1_autonomous_policy_validates_without_forging_human():
    best=validate_autonomous_phase1(_phase1(),instrument="AUD_USD",target_population_sha256="target")
    a=_approval(); assert best["opened_gates"]==[] and a["ia1_approved"] is False and a["human_approval"] is False

@pytest.mark.parametrize("change",[
    {"status":"OK"},{"all_target_wins_recovered":True},{"selection_scope":"FULL"},{"lookahead_protection":False},
    {"unrecovered_target_wins":[]},{"unrecovered_target_wins":[{"immutable_blocks":[]}]},{"instrument":"EUR_USD"},{"input_sha256":"bad"},
])
def test_phase1_autonomous_policy_fail_closed(change):
    with pytest.raises(ValueError): validate_autonomous_phase1(_phase1(**change),instrument="AUD_USD",target_population_sha256="target")

def test_phase1_autonomous_policy_rejects_non_best_and_immutable_gate():
    p=_phase1(); p["best_policy"]=p["candidates"][1]
    with pytest.raises(ValueError): validate_autonomous_phase1(p,instrument="AUD_USD",target_population_sha256="target")
    p=_phase1(); bad={"opened_gates":["WAIT_DIRECTION"],"wins_recovered":9,"losses_released":0}; p["best_policy"]=bad;p["candidates"]=[bad]
    with pytest.raises(ValueError): validate_autonomous_phase1(p,instrument="AUD_USD",target_population_sha256="target")

def test_governance_allows_exact_automation_authority(): assert _gate(_approval())["status"]=="ALLOWED"
@pytest.mark.parametrize("updates",[
    {"approval_type":"BEST_VIABLE_POLICY"},{"approval_authority":"OTHER"},{"authorization_scope":"PAPER"},{"ia1_approved":True},
    {"human_approval":True},{"production_authority":True},{"active":False},{"instrument":"EUR_USD"},{"dataset_identity":"x"},
    {"code_sha":"x"},{"phase1_artifact_sha256":"x"},{"best_policy_sha256":"x"},
])
def test_governance_rejects_invalid_automation_authority(updates): assert _gate(_approval(**updates))["status"]=="BLOCKED"

def test_governance_normal_path_still_allowed():
    p=_phase1(all_target_wins_recovered=True,unrecovered_target_wins=[])
    r=DecisionGateEngine.evaluate("PHASE_2",integrity={"status":"PASS"},replay={"methodology":{"no_lookahead_decision":True}},target_population={"lookahead_protection":True},phase1=p)
    assert r["status"]=="ALLOWED"

def test_governance_unrecovered_without_approval_blocked(): assert _gate(None)["status"]=="BLOCKED"

def test_support_diagnostic_audusd_mathematical_insufficiency():
    d={"candidate_space":{"generated":118,"evaluated_after_discovery_gate":0,"freeze_eligible":0},"discovery_metrics":{"total":9,"wins":2,"losses":4}}
    x=diagnose_discovery(d,min_resolved=10); assert x["dominant_failure"]=="INSUFFICIENT_SUPPORT" and x["recommended_action"]=="EXPAND_LOOKBACK"

def test_support_diagnostic_no_positive_expectancy():
    item={"discovery":{"selected":{"resolved_binary":20}},"validation":{"selected":{"resolved_binary":20},"win_retention":.8,"losses_rejected":4,"expectancy_delta_r":-.1},"overfitting_risk":{"severity":"LOW"},"directional_stability":{"stable":True},"temporal_stability":{"stable":True}}
    d={"candidate_space":{"generated":1,"evaluated_after_discovery_gate":1},"discovery_metrics":{"total":40},"ranked_candidates":[item]}
    assert diagnose_discovery(d)["dominant_failure"]=="NO_POSITIVE_EXPECTANCY"

def test_support_diagnostic_freeze_eligible_continues():
    assert diagnose_discovery({"candidate_space":{"generated":1,"freeze_eligible":1},"discovery_metrics":{"total":20}})["recommended_action"]=="CONTINUE"

def test_ledger_persists_terminal_state_and_multi_asset_independence(tmp_path):
    l=V3Ledger(tmp_path/"v3.json"); l.mutate("AUD_USD",status="INSUFFICIENT_EVIDENCE"); l.mutate("EUR_USD",status="RUNNING")
    assert l.load()["runs"]["AUD_USD"]["status"]=="INSUFFICIENT_EVIDENCE" and l.load()["runs"]["EUR_USD"]["status"]=="RUNNING"

def test_release_live_is_hard_rejected(tmp_path):
    r=ReleaseController(tmp_path)
    with pytest.raises(ValueError): r.assert_paper_only({"TRADING_ENVIRONMENT":"PRODUCTION","PRIMARY_OANDA_ENV":"live","OANDA":"https://api-fxtrade.oanda.com"})

def test_release_paper_is_allowed(tmp_path):
    ReleaseController(tmp_path).assert_paper_only({"TRADING_ENVIRONMENT":"PAPER","PRIMARY_OANDA_ENV":"practice","OANDA":PRACTICE_OANDA_URL})

def test_deploy_unconfigured_fails_closed(tmp_path,monkeypatch):
    monkeypatch.delenv("BOTS_V3_PAPER_DEPLOY_COMMAND",raising=False);monkeypatch.delenv("BOTS_V3_PAPER_VERIFY_COMMAND",raising=False)
    assert ReleaseController(tmp_path).deploy_paper(expected_sha="x",environment={"TRADING_ENVIRONMENT":"PAPER","PRIMARY_OANDA_ENV":"practice","OANDA":PRACTICE_OANDA_URL})["status"]=="DEPLOYMENT_FAILURE"

def test_deploy_verify_failure_attempts_rollback(tmp_path,monkeypatch):
    monkeypatch.setenv("BOTS_V3_PAPER_DEPLOY_COMMAND","deploy");monkeypatch.setenv("BOTS_V3_PAPER_VERIFY_COMMAND","verify");monkeypatch.setenv("BOTS_V3_PAPER_ROLLBACK_COMMAND","rollback")
    calls=[]
    def runner(cmd,**kw):
        calls.append(cmd[0]); return types.SimpleNamespace(returncode=1 if cmd[0]=="verify" else 0,stdout="",stderr="")
    out=ReleaseController(tmp_path,runner).deploy_paper(expected_sha="x",environment={"TRADING_ENVIRONMENT":"PAPER","PRIMARY_OANDA_ENV":"practice","OANDA":PRACTICE_OANDA_URL})
    assert out["status"]=="DEPLOYMENT_FAILURE" and out["rollback_attempted"] and "rollback" in calls

class FakeData:
    def __init__(self): self.calls=[]
    async def acquire(self,instrument,start,end,cache,**kw):
        self.calls.append((instrument,start,end,cache)); cache.parent.mkdir(parents=True,exist_ok=True); cache.write_text('{}'); return cache

class FakeManager:
    def __init__(self,state,ledger): self.state=state; self.ledger=ledger
    def register_asset(self,*a,**kw): return {"dataset_identity":"d"}
    def approve_phase1_autonomous(self,*a,**kw): return _approval()

class FakeStage:
    def __init__(self,name,artifact): self.name=name; self.artifact=artifact; self.command=("fake",name)

def fake_stages(**kw):
    names=("data_integrity","replay","target_population","phase_1","phase_2","discovery","discovery_repeat","determinism","freeze","holdout","audit","report","pre_audit","prompts")
    return [FakeStage(n,kw["workspace"]/f"{i:02d}_{n}.json") for i,n in enumerate(names,1)]

class FakeRelease:
    def __init__(self,deploy=True): self.merges=0;self.deploys=0;self.ok=deploy
    def prepare_test_merge(self,**kw): self.merges+=1; return {"status":"PASS","merged_main_sha":"paper-sha"}
    def deploy_paper(self,**kw): self.deploys+=1; return {"status":"PAPER_DEPLOYED" if self.ok else "DEPLOYMENT_FAILURE","production_authority":False,"reason":"verify" if not self.ok else None}

class ScenarioCascade:
    def __init__(self,manager,scenario): self.scenario=scenario
    def run(self,instrument,stages,through="prompts"):
        wd=stages[0].artifact.parent; months=int(wd.name.split('_')[1][:-1])
        if self.scenario.get(months)=="insufficient":
            (wd/"06_discovery.json").write_text(json.dumps({"candidate_space":{"generated":118,"evaluated_after_discovery_gate":0,"freeze_eligible":0},"discovery_metrics":{"total":9,"wins":2,"losses":4}}))
            raise RuntimeError("Artifact status is NO_FREEZE_ELIGIBLE_CANDIDATE")
        if self.scenario.get(months)=="bad":
            (wd/"06_discovery.json").write_text(json.dumps({"candidate_space":{"generated":2,"evaluated_after_discovery_gate":2,"freeze_eligible":0},"discovery_metrics":{"total":40},"ranked_candidates":[{"discovery":{"selected":{"resolved_binary":20}},"validation":{"selected":{"resolved_binary":20},"win_retention":.8,"losses_rejected":4,"expectancy_delta_r":-.1},"overfitting_risk":{"severity":"LOW"},"directional_stability":{"stable":True},"temporal_stability":{"stable":True}}]}))
            raise RuntimeError("no candidate")
        (wd/"10_holdout.json").write_text(json.dumps({"status":"PASS","overfitting_risk":{"severity":"LOW"},"candidate_ranking":[{"status":"RESEARCH_CANDIDATE","candidate_id":"c"}]}))
        (wd/"13_pre_audit.json").write_text(json.dumps({"verdict":"ACCEPT"}))
        return {"status":"COMPLETED"}

def _optimizer(tmp_path,scenario,release=None):
    from autonomous_asset_optimizer import AutonomousAssetOptimizer
    repo=tmp_path/"repo";repo.mkdir();data=FakeData();rel=release or FakeRelease()
    opt=AutonomousAssetOptimizer(repo,data_source=data,release=rel,now=lambda:__import__('datetime').datetime(2026,9,3,tzinfo=__import__('datetime').timezone.utc),
        cascade_factory=lambda m:ScenarioCascade(m,scenario),manager_factory=FakeManager,stage_builder=fake_stages,code_sha_provider=lambda:'a'*40)
    return opt,data,rel

def test_one_command_happy_path_to_paper(tmp_path,monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT",str(tmp_path/"research"));opt,data,rel=_optimizer(tmp_path,{1:"ok"})
    out=opt.optimize("AUD_USD"); assert out["status"]=="PAPER_DEPLOYED" and len(data.calls)==1 and rel.deploys==1

def test_auto_expands_one_to_three_months(tmp_path,monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT",str(tmp_path/"research"));opt,data,_=_optimizer(tmp_path,{1:"insufficient",3:"ok"})
    assert opt.optimize("AUD_USD")["status"]=="PAPER_DEPLOYED"; assert len(data.calls)==2

def test_auto_expands_three_to_six_months(tmp_path,monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT",str(tmp_path/"research"));opt,data,_=_optimizer(tmp_path,{1:"insufficient",3:"insufficient",6:"ok"})
    assert opt.optimize("AUD_USD")["status"]=="PAPER_DEPLOYED"; assert len(data.calls)==3

def test_auto_expands_six_to_twelve_months(tmp_path,monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT",str(tmp_path/"research"));opt,data,_=_optimizer(tmp_path,{1:"insufficient",3:"insufficient",6:"insufficient",12:"ok"})
    assert opt.optimize("AUD_USD")["status"]=="PAPER_DEPLOYED"; assert len(data.calls)==4

def test_twelve_months_insufficient_terminal(tmp_path,monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT",str(tmp_path/"research"));opt,data,_=_optimizer(tmp_path,{1:"insufficient",3:"insufficient",6:"insufficient",12:"insufficient"})
    assert opt.optimize("AUD_USD")["status"]=="INSUFFICIENT_EVIDENCE" and len(data.calls)==4

def test_sufficient_support_bad_candidate_does_not_expand_forever(tmp_path,monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT",str(tmp_path/"research"));opt,data,_=_optimizer(tmp_path,{1:"bad"})
    assert opt.optimize("AUD_USD")["status"]=="NO_VALID_CANDIDATE" and len(data.calls)==1

def test_terminal_rerun_does_not_duplicate_paper_deploy(tmp_path,monkeypatch):
    monkeypatch.setenv("BOTS_RESEARCH_ROOT",str(tmp_path/"research"));opt,_,rel=_optimizer(tmp_path,{1:"ok"})
    assert opt.optimize("AUD_USD")["status"]=="PAPER_DEPLOYED"; assert opt.optimize("AUD_USD")["status"]=="PAPER_DEPLOYED"; assert rel.deploys==1

def test_unsupported_instrument_terminal(tmp_path):
    opt,_,_=_optimizer(tmp_path,{1:"ok"}); out=opt.optimize("BTC_USD"); assert out["status"]=="UNSUPPORTED_INSTRUMENT" and out["production_authority"] is False

def _release_repo(tmp_path, monkeypatch, change):
    import subprocess, sys
    from automation_v3_release import TESTS, ReleaseController
    repo=tmp_path/'release_repo';repo.mkdir()
    subprocess.run(['git','init','-q','-b','main'],cwd=repo,check=True)
    subprocess.run(['git','config','user.email','v3@example.com'],cwd=repo,check=True)
    subprocess.run(['git','config','user.name','V3 Test'],cwd=repo,check=True)
    for name in TESTS:
        (repo/name).write_text('def test_ok(): assert True\n',encoding='utf-8')
    (repo/'safe.py').write_text('VALUE=1\n',encoding='utf-8')
    changer=repo/'changer.py';changer.write_text(change,encoding='utf-8')
    subprocess.run(['git','add','-A'],cwd=repo,check=True);subprocess.run(['git','commit','-qm','base'],cwd=repo,check=True)
    base=subprocess.run(['git','rev-parse','HEAD'],cwd=repo,text=True,capture_output=True,check=True).stdout.strip()
    bare=tmp_path/'remote.git';subprocess.run(['git','init','-q','--bare',str(bare)],check=True)
    subprocess.run(['git','remote','add','origin',str(bare)],cwd=repo,check=True);subprocess.run(['git','push','-q','-u','origin','main'],cwd=repo,check=True)
    monkeypatch.setenv('BOTS_V3_CODE_CHANGE_COMMAND',f'{sys.executable} {changer}')
    plan=tmp_path/'plan.json';plan.write_text('{}',encoding='utf-8')
    return repo,base,plan,ReleaseController(repo)

def test_release_controller_creates_branch_tests_and_ff_merges(tmp_path,monkeypatch):
    repo,base,plan,r=_release_repo(tmp_path,monkeypatch,"from pathlib import Path\nPath('safe_candidate.py').write_text('PAPER_ONLY=True\\n')\n")
    out=r.prepare_test_merge(plan=plan,base_sha=base,instrument='AUD_USD')
    assert out['status']=='PASS' and out['merged_main_sha'] and out['branch'].startswith('automation-v3/')

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
    repo,base,plan,r=_release_repo(tmp_path,monkeypatch,"from pathlib import Path\nPath('safe_candidate.py').write_text('x=1\\n')\n")
    (repo/'safe.py').write_text('dirty=1\n',encoding='utf-8')
    out=r.prepare_test_merge(plan=plan,base_sha=base,instrument='AUD_USD')
    assert out['status']=='BLOCKED' and out['reason']=='BASE_OR_WORKTREE_MISMATCH'

class UnavailableData:
    async def acquire(self,*a,**kw): raise RuntimeError('source offline')

def test_data_source_unavailable_terminal(tmp_path,monkeypatch):
    from autonomous_asset_optimizer import AutonomousAssetOptimizer
    monkeypatch.setenv('BOTS_RESEARCH_ROOT',str(tmp_path/'research'))
    repo=tmp_path/'repo';repo.mkdir();rel=FakeRelease()
    opt=AutonomousAssetOptimizer(repo,data_source=UnavailableData(),release=rel,now=lambda:__import__('datetime').datetime(2026,9,3,tzinfo=__import__('datetime').timezone.utc),manager_factory=FakeManager,stage_builder=fake_stages,code_sha_provider=lambda:'a'*40)
    assert opt.optimize('AUD_USD')['status']=='DATA_SOURCE_UNAVAILABLE'

def test_crash_after_merge_resumes_paper_without_research(tmp_path,monkeypatch):
    from autonomous_asset_optimizer import AutonomousAssetOptimizer,V3Ledger
    root=tmp_path/'research';monkeypatch.setenv('BOTS_RESEARCH_ROOT',str(root));repo=tmp_path/'repo';repo.mkdir();sha='c'*40;rel=FakeRelease();data=FakeData()
    ledger=V3Ledger(root/'AUD_USD'/'autonomous_v3'/'automation_v3_state.json');ledger.mutate('AUD_USD',status='RUNNING',code_sha=sha,release={'status':'PASS','merged_main_sha':sha})
    opt=AutonomousAssetOptimizer(repo,data_source=data,release=rel,code_sha_provider=lambda:sha)
    assert opt.optimize('AUD_USD')['status']=='PAPER_DEPLOYED' and rel.deploys==1 and data.calls==[]

def test_code_sha_change_invalidates_old_autonomous_approval(tmp_path,monkeypatch):
    from autonomous_asset_optimizer import AutonomousAssetOptimizer,V3Ledger
    root=tmp_path/'research';monkeypatch.setenv('BOTS_RESEARCH_ROOT',str(root));repo=tmp_path/'repo';repo.mkdir();ledger=V3Ledger(root/'AUD_USD'/'autonomous_v3'/'automation_v3_state.json')
    ledger.mutate('AUD_USD',status='RUNNING',code_sha='b'*40,approvals=[{'active':True,'approval_type':AUTOMATION_APPROVAL_TYPE,'production_authority':False}])
    data=FakeData();opt=AutonomousAssetOptimizer(repo,data_source=data,release=FakeRelease(),now=lambda:__import__('datetime').datetime(2026,9,3,tzinfo=__import__('datetime').timezone.utc),cascade_factory=lambda m:ScenarioCascade(m,{1:'bad'}),manager_factory=FakeManager,stage_builder=fake_stages,code_sha_provider=lambda:'a'*40)
    assert opt.optimize('AUD_USD')['status']=='NO_VALID_CANDIDATE'
    old=V3Ledger(root/'AUD_USD'/'autonomous_v3'/'automation_v3_state.json').run('AUD_USD')['approvals'][0]
    assert old['active'] is False and old['invalidated_reason']=='CODE_SHA_CHANGED'

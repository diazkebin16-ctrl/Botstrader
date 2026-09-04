"""Fail-closed Automation V3 code-change, merge and PAPER release controller."""
from __future__ import annotations
import os,subprocess,sys,shutil
from pathlib import Path
PRACTICE_OANDA_URL="https://api-fxpractice.oanda.com"
PROTECTED_LIVE_FILES={"server.py","forward_experiment.py"}
TESTS=("test_automation_v3.py","test_cascade_optimizer.py","test_research_manager.py","test_research_governance.py","test_research_phase2.py","test_research_integrity.py","test_research_pipeline.py","test_replay_validation.py","test_forward_paper_release.py")
class ReleaseController:
 def __init__(self,repo,runner=subprocess.run):self.repo=Path(repo);self.runner=runner
 @staticmethod
 def assert_paper_only(e):
  if str(e.get("TRADING_ENVIRONMENT") or "").upper()!="PAPER" or str(e.get("PRIMARY_OANDA_ENV") or "").lower()!="practice" or e.get("OANDA")!=PRACTICE_OANDA_URL:raise ValueError("LIVE/PRODUCTION target prohibited")
 def _run(self,cmd,**kw):return self.runner(cmd,check=False,text=True,capture_output=True,**kw)
 def _git(self,*a):return self._run(["git",*a],cwd=self.repo)
 def _blocked(self,reason,**extra):return {"status":"BLOCKED","reason":reason,"production_authority":False,**extra}
 def _scan(self,base,working=False):
  args=("diff","--name-only") if working else ("diff","--name-only",base,"HEAD");n=self._git(*args);names=[x.strip() for x in n.stdout.splitlines() if x.strip()]
  if n.returncode:return False,"DIFF_READ_FAILED",names
  if any(Path(x).name in PROTECTED_LIVE_FILES for x in names):return False,"PROTECTED_LIVE_FILE_CHANGED",names
  if any(x.endswith((".env",".db",".sqlite",".zip",".pyc")) or "__pycache__" in x for x in names):return False,"TRANSIENT_OR_SECRET_FILE",names
  d=self._git("diff" if working else "diff",*([] if working else [base,"HEAD"]));text=d.stdout
  if any(x in text for x in ("api-fxtrade.oanda.com","TRADING_ENVIRONMENT=PRODUCTION","PRIMARY_OANDA_ENV=live")):return False,"LIVE_MARKER_IN_DIFF",names
  if any(x in text.upper() for x in ("OANDA_TOKEN=","API_KEY=","SECRET_KEY=","PASSWORD=")):return False,"SECRET_MARKER_IN_DIFF",names
  return True,"PASS",names
 def prepare_test_merge(self,*,plan,base_sha,instrument):
  adapter=os.getenv("BOTS_V3_CODE_CHANGE_COMMAND","").strip()
  if not adapter:return self._blocked("CODE_CHANGE_ADAPTER_UNAVAILABLE")
  if self._git("rev-parse","HEAD").stdout.strip()!=base_sha or self._git("branch","--show-current").stdout.strip()!="main" or self._git("status","--porcelain").stdout.strip():return self._blocked("BASE_OR_WORKTREE_MISMATCH")
  branch=f"automation-v3/{instrument.lower()}-{base_sha[:12]}"
  if self._git("checkout","-b",branch).returncode:return self._blocked("BRANCH_CREATE_FAILED")
  env=dict(os.environ,BOTS_V3_RELEASE_PLAN=str(plan),BOTS_V3_BASE_SHA=base_sha,BOTS_V3_INSTRUMENT=instrument,BOTS_V3_PRODUCTION_AUTHORITY="false",TRADING_ENVIRONMENT="PAPER",PRIMARY_OANDA_ENV="practice",OANDA=PRACTICE_OANDA_URL)
  if self._run(adapter.split(),cwd=self.repo,env=env).returncode:return self._blocked("CODE_CHANGE_ADAPTER_FAILED",branch=branch)
  if not self._git("status","--porcelain").stdout.strip():return self._blocked("NO_CODE_CHANGE",branch=branch)
  ok,reason,names=self._scan(base_sha,working=True)
  if not ok:return self._blocked("DIFF_POLICY_BLOCK",detail=reason,branch=branch)
  t=self._run([sys.executable,"-m","pytest","-q",*TESTS],cwd=self.repo,env=env)
  if t.returncode:return self._blocked("TEST_FAILURE",branch=branch,test_output=(t.stdout+t.stderr)[-4000:])
  for d in self.repo.rglob("__pycache__"): shutil.rmtree(d,ignore_errors=True)
  shutil.rmtree(self.repo/".pytest_cache",ignore_errors=True)
  for f in self.repo.rglob("*.pyc"):
   try:f.unlink()
   except FileNotFoundError:pass
  ok,reason,names=self._scan(base_sha,working=True)
  if not ok:return self._blocked("DIFF_POLICY_BLOCK",detail=reason,branch=branch)
  if self._git("diff","--check").returncode:return self._blocked("GIT_DIFF_CHECK_FAILED",branch=branch)
  self._git("add","-A")
  if self._git("commit","-m",f"Apply Automation V3 PAPER candidate for {instrument}").returncode:return self._blocked("COMMIT_FAILED",branch=branch)
  candidate=self._git("rev-parse","HEAD").stdout.strip();ok,reason,names=self._scan(base_sha)
  if not ok:return self._blocked("DIFF_POLICY_BLOCK",detail=reason,branch=branch)
  if self._git("push","origin",branch).returncode:return self._blocked("BRANCH_PUSH_FAILED",branch=branch)
  if self._git("checkout","main").returncode or self._git("rev-parse","HEAD").stdout.strip()!=base_sha:return self._blocked("MAIN_ANCESTRY_CHANGED",branch=branch)
  if self._git("merge","--ff-only",branch).returncode:return self._blocked("FF_ONLY_MERGE_FAILED",branch=branch)
  merged=self._git("rev-parse","HEAD").stdout.strip()
  if merged!=candidate or self._git("push","origin","main").returncode:return self._blocked("MAIN_PUSH_FAILED",branch=branch)
  return {"status":"PASS","branch":branch,"candidate_sha":candidate,"merged_main_sha":merged,"changed_files":names,"production_authority":False}
 def deploy_paper(self,*,expected_sha,environment):
  self.assert_paper_only(environment);deploy=os.getenv("BOTS_V3_PAPER_DEPLOY_COMMAND","").strip();verify=os.getenv("BOTS_V3_PAPER_VERIFY_COMMAND","").strip();rollback=os.getenv("BOTS_V3_PAPER_ROLLBACK_COMMAND","").strip()
  if not deploy or not verify:return {"status":"DEPLOYMENT_FAILURE","reason":"PAPER_DEPLOY_ADAPTER_UNAVAILABLE","production_authority":False}
  env=dict(os.environ,**{k:str(v) for k,v in environment.items()},BOTS_V3_EXPECTED_SHA=expected_sha,BOTS_V3_PRODUCTION_AUTHORITY="false")
  if self._run(deploy.split(),cwd=self.repo,env=env).returncode:return {"status":"DEPLOYMENT_FAILURE","reason":"PAPER_DEPLOY_FAILED","production_authority":False}
  if not self._run(verify.split(),cwd=self.repo,env=env).returncode:return {"status":"PAPER_DEPLOYED","verified_sha":expected_sha,"production_authority":False}
  rb=False
  if rollback:rb=self._run(rollback.split(),cwd=self.repo,env=env).returncode==0
  return {"status":"DEPLOYMENT_FAILURE","reason":"PAPER_VERIFY_FAILED","rollback_attempted":bool(rollback),"rollback_succeeded":rb,"production_authority":False}

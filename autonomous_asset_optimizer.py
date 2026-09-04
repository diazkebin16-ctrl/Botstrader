#!/usr/bin/env python3
"""Automation V3: one command research -> governed PAPER; never LIVE."""
from __future__ import annotations
import argparse,asyncio,hashlib,json,os,subprocess,sys,tempfile
from datetime import datetime,timedelta,timezone
from pathlib import Path
from typing import Any,Mapping
from automation_v3_release import ReleaseController as GovernedReleaseController
from automation_v3_candidate_mapping import CandidateNotDeployable,compile_and_write_release_plan
from automation_v3_integrity_recovery import build_integrity_diagnostic,terminal_for_nonrecoverable

LOOKBACK_SEQUENCE=(1,3,6,12);MAX_RESEARCH_LOOKBACK_MONTHS=12
SUPPORTED_INSTRUMENTS=("AUD_USD","EUR_USD","GBP_USD","USD_JPY","USD_CAD")
TERMINAL_STATES={"PAPER_DEPLOYED","PAPER_DEPLOYABLE_CANDIDATE","CANDIDATE_NOT_DEPLOYABLE","NO_VALID_CANDIDATE","INSUFFICIENT_EVIDENCE","DATA_SOURCE_UNAVAILABLE","DATA_COVERAGE_INSUFFICIENT","DATA_INTEGRITY_FAILED","METHODOLOGY_BLOCKED","TEST_FAILURE","DEPLOYMENT_FAILURE","UNSUPPORTED_INSTRUMENT"}
PRACTICE_OANDA_URL="https://api-fxpractice.oanda.com"
AUTOMATION_APPROVAL_TYPE="AUTONOMOUS_RESEARCH_POLICY_APPROVAL";AUTOMATION_AUTHORITY="AUTOMATION_V3_POLICY";AUTOMATION_SCOPE="RESEARCH_CONTINUATION_ONLY"
PROTECTED_LIVE_FILES={"server.py","forward_experiment.py"}

def utc_now():return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
def aligned_research_end(now,horizon_minutes=240):
 d=now.astimezone(timezone.utc)-timedelta(minutes=int(horizon_minutes));return d.replace(minute=0,second=0,microsecond=0)
def canonical_sha256(v):return hashlib.sha256(json.dumps(dict(v),sort_keys=True,separators=(",",":"),default=str).encode()).hexdigest()
def sha256_file(p):
 d=hashlib.sha256()
 with Path(p).open("rb") as f:
  for c in iter(lambda:f.read(1048576),b""):d.update(c)
 return d.hexdigest()
def write_json(p,v):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True);fd,t=tempfile.mkstemp(prefix=".v3-",dir=str(p.parent))
 try:
  with os.fdopen(fd,"w",encoding="utf-8") as f:json.dump(v,f,indent=2,sort_keys=True);f.write("\n");f.flush();os.fsync(f.fileno())
  os.replace(t,p)
 except BaseException:
  try:os.unlink(t)
  except FileNotFoundError:pass
  raise
def load_json(p):
 with Path(p).open(encoding="utf-8") as f:v=json.load(f)
 if not isinstance(v,dict):raise ValueError("artifact must be object")
 return v

def integrity_artifact_failed(report:Mapping[str,Any]):
 return str(report.get("status") or "UNKNOWN").upper()!="PASS" or bool(report.get("failures") or [])

def diagnose_discovery(d:Mapping[str,Any],*,min_resolved=10):
 s=d.get("candidate_space") or {};rows=int((d.get("discovery_metrics") or {}).get("episodes") or (d.get("discovery_metrics") or {}).get("total") or 0);ranked=[x for x in d.get("ranked_candidates") or [] if isinstance(x,Mapping)]
 mx=fs=fr=fl=fe=fo=fi=0
 for x in ranked:
  a=x.get("discovery") or {};v=x.get("validation") or {};dr=int((a.get("selected") or {}).get("resolved_binary") or 0);vr=int((v.get("selected") or {}).get("resolved_binary") or 0);mx=max(mx,dr,vr)
  fs+=dr<min_resolved or vr<min_resolved;fr+=float(v.get("win_retention") or 0)<.60;fl+=int(v.get("losses_rejected") or 0)<2;fe+=float(v.get("expectancy_delta_r") or 0)<=0;fo+=(x.get("overfitting_risk") or {}).get("severity")=="HIGH";fi+=(x.get("directional_stability") or {}).get("stable") is False or (x.get("temporal_stability") or {}).get("stable") is False
 gen=int(s.get("generated") or 0);ev=int(s.get("evaluated_after_discovery_gate") or len(ranked));fz=int(s.get("freeze_eligible") or 0)
 if not ranked:
  m=d.get("discovery_metrics") or {};mx=max(mx,int(m.get("resolved_binary") or 0) or min(rows,int(m.get("wins") or 0)+int(m.get("losses") or 0)));fs=gen if mx<min_resolved else 0
 if fz:dom,act="NONE","CONTINUE"
 elif rows<min_resolved or mx<min_resolved:dom,act="INSUFFICIENT_SUPPORT","EXPAND_LOOKBACK"
 elif fo>=max(fr,fl,fe,1):dom,act="HIGH_OVERFITTING_RISK","NO_VALID_CANDIDATE"
 elif fi>=max(fr,fl,fe,1):dom,act="INSTABILITY","NO_VALID_CANDIDATE"
 elif fe>=max(fr,fl,1):dom,act="NO_POSITIVE_EXPECTANCY","NO_VALID_CANDIDATE"
 elif fr>=max(fl,1):dom,act="LOW_WIN_RETENTION","NO_VALID_CANDIDATE"
 elif fl:dom,act="INSUFFICIENT_LOSS_REJECTION","NO_VALID_CANDIDATE"
 else:dom,act="OTHER_METHODOLOGY_FAILURE","STOP"
 return {"generated_candidates":gen,"discovery_rows":rows,"evaluated_after_discovery_gate":ev,"freeze_eligible":fz,"max_resolved":mx,"min_resolved_required":min_resolved,"fail_support_count":fs,"fail_win_retention_count":fr,"fail_loss_rejection_count":fl,"fail_expectancy_count":fe,"fail_overfitting_count":fo,"fail_instability_count":fi,"dominant_failure":dom,"recommended_action":act,"production_authority":False}

def validate_autonomous_phase1(p:Mapping[str,Any],*,instrument,target_population_sha256):
 if p.get("stage")!="phase_1" or str(p.get("instrument") or "").upper()!=instrument.upper():raise ValueError("Phase 1 identity mismatch")
 if p.get("status")!="REVIEW_REQUIRED" or p.get("all_target_wins_recovered") is not False:raise ValueError("autonomous policy requires unrecovered REVIEW_REQUIRED")
 if p.get("selection_scope")!="DISCOVERY_ONLY" or p.get("lookahead_protection") is not True or p.get("input_sha256")!=target_population_sha256:raise ValueError("Phase 1 methodology/binding mismatch")
 u=p.get("unrecovered_target_wins")
 if not isinstance(u,list) or not u or any(not isinstance(x,Mapping) or not isinstance(x.get("immutable_blocks"),list) or not x.get("immutable_blocks") for x in u):raise ValueError("every unrecovered WIN needs immutable blocker")
 best=p.get("best_policy");cs=p.get("candidates");allowed={"M1_CONFIRMATION","QUALITY_EXTENSION","LOW_ROOM"}
 if not isinstance(best,Mapping) or not isinstance(cs,list) or not cs:raise ValueError("ranking evidence missing")
 def rank(x):
  g=x.get("opened_gates")
  if not isinstance(g,list) or any(y not in allowed for y in g):raise ValueError("unknown/immutable gate")
  return(-int(x.get("wins_recovered") or 0),int(x.get("losses_released") or 0),len(g),tuple(g))
 if any(not isinstance(x,Mapping) for x in cs) or canonical_sha256(best)!=canonical_sha256(sorted(cs,key=rank)[0]):raise ValueError("best policy ranking mismatch")
 rank(best);return dict(best)

class V3Ledger:
 def __init__(self,path):self.path=Path(path)
 def load(self):return load_json(self.path) if self.path.exists() else {"schema_version":1,"runs":{}}
 def save(self,s):write_json(self.path,s)
 def run(self,i):return self.load().get("runs",{}).get(i,{"instrument":i,"status":"NEW","lookback_attempts":[],"approvals":[],"decision_history":[],"production_authority":False})
 def mutate(self,i,**kw):
  s=self.load();r=s.setdefault("runs",{}).setdefault(i,self.run(i));r.update(kw);r["instrument"]=i;r["production_authority"]=False;r["updated_at"]=utc_now();self.save(s);return r
 def append(self,i,k,v):s=self.load();r=s.setdefault("runs",{}).setdefault(i,self.run(i));r.setdefault(k,[]).append(v);r["production_authority"]=False;self.save(s)

class AutonomousResearchManager:
 def __init__(self,state,ledger):
  from research_manager import ResearchManager
  self.inner=ResearchManager(state);self.ledger=ledger;self.instrument=None
 def __getattr__(self,n):return getattr(self.inner,n)
 def register_asset(self,i,**kw):self.instrument=i.upper();return self.inner.register_asset(i,**kw)
 def approve_phase1_autonomous(self,i,artifact):
  i=i.upper();asset=self.inner.load()["assets"][i];path=Path(artifact);p=load_json(path);target=((asset.get("phases") or {}).get("target_population") or {}).get("artifact_sha256")
  best=validate_autonomous_phase1(p,instrument=i,target_population_sha256=target);rec={"approval_type":AUTOMATION_APPROVAL_TYPE,"approval_authority":AUTOMATION_AUTHORITY,"authorization_scope":AUTOMATION_SCOPE,"active":True,"instrument":i,"dataset_identity":asset.get("dataset_identity"),"code_sha":(asset.get("provenance") or {}).get("code_sha"),"phase1_artifact_sha256":sha256_file(path),"target_population_sha256":target,"best_policy_sha256":canonical_sha256(best),"ia1_approved":False,"human_approval":False,"production_authority":False,"approved_at":utc_now()};self.ledger.append(i,"approvals",rec);return rec
 def active_phase1_best_viable_approval(self,i,artifact):
  h=self.inner.active_phase1_best_viable_approval(i,artifact)
  if h:return h
  i=i.upper();asset=self.inner.load()["assets"].get(i) or {};p=Path(artifact)
  if not p.is_file():return None
  try:b=load_json(p).get("best_policy")
  except Exception:return None
  for a in reversed(self.ledger.run(i).get("approvals",[])):
   if a.get("active") is True and a.get("approval_type")==AUTOMATION_APPROVAL_TYPE and a.get("approval_authority")==AUTOMATION_AUTHORITY and a.get("authorization_scope")==AUTOMATION_SCOPE and a.get("dataset_identity")==asset.get("dataset_identity") and a.get("code_sha")==((asset.get("provenance") or {}).get("code_sha")) and a.get("phase1_artifact_sha256")==sha256_file(p) and isinstance(b,Mapping) and a.get("best_policy_sha256")==canonical_sha256(b) and a.get("ia1_approved") is False and a.get("human_approval") is False and a.get("production_authority") is False:return dict(a)
  return None

class OandaPracticeDataSource:
 async def acquire(self,instrument,start,end,cache,**kw):
  if not os.getenv("OANDA_TOKEN","").strip():raise RuntimeError("DATA_SOURCE_UNAVAILABLE: OANDA_TOKEN missing")
  from historical_candles import fetch_oanda_candles,save_bundle
  fs=start-timedelta(days=int(kw.get("warmup_days",10))+int(kw.get("boundary_buffer_days",0)));fe=end+timedelta(minutes=int(kw.get("horizon_minutes",240))+int(kw.get("boundary_buffer_minutes",0)));tfs=("H1","M15","M5","M1")
  vals=await asyncio.gather(*(fetch_oanda_candles(instrument,tf,fs,fe,base_url=PRACTICE_OANDA_URL) for tf in tfs));bundle=dict(zip(tfs,vals))
  if any(not bundle[x] for x in tfs):raise RuntimeError("DATA_SOURCE_UNAVAILABLE: incomplete OANDA history")
  Path(cache).parent.mkdir(parents=True,exist_ok=True);save_bundle(str(cache),bundle);return Path(cache)

ReleaseController=GovernedReleaseController

class AutonomousAssetOptimizer:
 def __init__(self,repo,*,data_source=None,release=None,now=None,cascade_factory=None,manager_factory=None,stage_builder=None,code_sha_provider=None):
  self.repo=Path(repo);self.data_source=data_source or OandaPracticeDataSource();self.release=release or ReleaseController(repo);self.now=now or (lambda:datetime.now(timezone.utc));self.cascade_factory=cascade_factory;self.manager_factory=manager_factory or AutonomousResearchManager;self.stage_builder=stage_builder;self.code_sha_provider=code_sha_provider
 def _git(self,*a):return subprocess.run(["git",*a],cwd=self.repo,check=True,text=True,capture_output=True).stdout.strip()
 @staticmethod
 def _months_before(end,m):return end-timedelta(days=31*m)
 def _terminal(self,l,i,s,reason,**extra):return l.mutate(i,status=s,stop_reason=reason,final_outcome=s,**extra)
 def optimize(self,instrument):
  i=instrument.upper();root=Path(os.getenv("BOTS_RESEARCH_ROOT",str(self.repo.parent/"Botstrader_Research")))/i/"autonomous_v3";root.mkdir(parents=True,exist_ok=True);ledger=V3Ledger(root/"automation_v3_state.json")
  if i not in SUPPORTED_INSTRUMENTS:return self._terminal(ledger,i,"UNSUPPORTED_INSTRUMENT","instrument unsupported")
  sha=self.code_sha_provider() if self.code_sha_provider else self._git("rev-parse","HEAD");run=ledger.run(i)
  if run.get("status") in TERMINAL_STATES and run.get("code_sha")==sha:return run
  release_resume=run.get("release") or {}
  if run.get("status")=="RUNNING" and release_resume.get("merged_main_sha")==sha:
   dep=self.release.deploy_paper(expected_sha=sha,environment={"TRADING_ENVIRONMENT":"PAPER","PRIMARY_OANDA_ENV":"practice","OANDA":PRACTICE_OANDA_URL});ledger.mutate(i,paper_deployment=dep)
   return self._terminal(ledger,i,"PAPER_DEPLOYED" if dep.get("status")=="PAPER_DEPLOYED" else "DEPLOYMENT_FAILURE","resumed PAPER verification",deployment=dep)
  if run.get("code_sha") and run.get("code_sha")!=sha:
   s=ledger.load();r=s.setdefault("runs",{}).setdefault(i,run);r.setdefault("decision_history",[]).append({"decision":"CODE_SHA_CHANGED","old":r.get("code_sha"),"new":sha,"at":utc_now()});
   for a in r.setdefault("approvals",[]):
    if a.get("active") is True:a["active"]=False;a["invalidated_reason"]="CODE_SHA_CHANGED"
   r.update(status="NEW",paper_deployment=None,stop_reason=None,final_outcome=None,integrity_diagnostic=None,diagnostic=None,lookback_months=None);ledger.save(s)
  end=aligned_research_end(self.now(),240);ledger.mutate(i,status="RUNNING",code_sha=sha,workspace=str(root),max_lookback_months=12,stop_reason=None,final_outcome=None,integrity_diagnostic=None,diagnostic=None)
  for months in LOOKBACK_SEQUENCE:
   ad=root/f"lookback_{months:02d}m_{sha[:12]}";ad.mkdir(parents=True,exist_ok=True);start=self._months_before(end,months);cache=root/"data"/f"{i}_{months:02d}m.json";cache_preexisting=cache.is_file();ledger.append(i,"lookback_attempts",{"months":months,"code_sha":sha,"start":start.isoformat(),"end":end.isoformat(),"status":"RUNNING","at":utc_now()})
   try:
    if not cache.is_file():asyncio.run(self.data_source.acquire(i,start,end,cache,warmup_days=10,horizon_minutes=240))
   except Exception as e:return self._terminal(ledger,i,"DATA_SOURCE_UNAVAILABLE",str(e),lookback_months=months)
   state=ad/"research_state.json";m=self.manager_factory(state,ledger);m.register_asset(i,code_sha=sha,start=start.isoformat(),end=end.isoformat(),warmup_days=10,horizon_minutes=240,data_sha256=sha256_file(cache))
   if self.stage_builder is None:
    from research_asset import build_stages;builder=build_stages
   else:builder=self.stage_builder
   stages=builder(repo=self.repo,python=sys.executable,instrument=i,cache=cache,workspace=ad,start=start.isoformat(),end=end.isoformat(),warmup=10,horizon=240,variant="V331_BASELINE",embargo=30,discovery_fraction=.60,validation_fraction=.20,min_resolved=10,code_sha=sha,state=state);write_json(ad/"cascade_manifest.json",{"schema_version":3,"automation":"V3","instrument":i,"code_sha":sha,"data_sha256":sha256_file(cache),"production_authority":False,"stages":[{"name":x.name,"artifact":str(x.artifact),"command":list(x.command)} for x in stages]})
   if self.cascade_factory:c=self.cascade_factory(m)
   else:
    from cascade_optimizer import CascadeOptimizer;c=CascadeOptimizer(m)
   err=None
   try:c.run(i,stages,through="prompts")
   except Exception as e:
    err=e;p=ad/"04_phase_1.json"
    if p.is_file():
     q=load_json(p)
     if q.get("status")=="REVIEW_REQUIRED" and q.get("all_target_wins_recovered") is False:
      try:a=m.approve_phase1_autonomous(i,p);ledger.append(i,"decision_history",{"decision":"AUTONOMOUS_PHASE1_BEST_VIABLE","approval":a,"at":utc_now()});m.update_phase(i,"phase_1","COMPLETED",artifact=str(p),details={"review_status":"REVIEW_REQUIRED","approval_type":AUTOMATION_APPROVAL_TYPE,"approval_authority":AUTOMATION_AUTHORITY,"authorization_scope":AUTOMATION_SCOPE,"ia1_approved":False,"production_authority":False});c.run(i,stages,through="prompts");err=None
      except Exception as x:err=x
    if err:
     ip=ad/"01_data_integrity.json"
     if ip.is_file() and not integrity_artifact_failed(load_json(ip)):
      ledger.mutate(i,integrity_diagnostic=None,lookback_months=months)
     if ip.is_file() and integrity_artifact_failed(load_json(ip)):
      diag=build_integrity_diagnostic(load_json(ip),artifact_path=ip,cache_path=cache,requested_start=start.isoformat(),requested_end=end.isoformat(),cache_preexisting=cache_preexisting,retry_count=0)
      write_json(ad/"integrity_diagnostic.json",diag);ledger.mutate(i,integrity_diagnostic=diag,lookback_months=months);ledger.append(i,"decision_history",{"decision":"DATA_INTEGRITY_DIAGNOSTIC","months":months,"diagnostic":diag,"at":utc_now()})
      if diag.get("recoverable") is True:
       ledger.append(i,"decision_history",{"decision":"DATA_REACQUIRE_REQUIRED","months":months,"recommended_action":"REACQUIRE_SAME_LOOKBACK","at":utc_now()})
       try:
        if cache.exists():cache.unlink()
        asyncio.run(self.data_source.acquire(i,start,end,cache,warmup_days=10,horizon_minutes=240,boundary_buffer_days=3,boundary_buffer_minutes=60))
       except Exception as x:return self._terminal(ledger,i,"DATA_SOURCE_UNAVAILABLE",str(x),lookback_months=months,integrity_diagnostic=diag)
       try:
        final_data_sha=sha256_file(cache);m.register_asset(i,code_sha=sha,start=start.isoformat(),end=end.isoformat(),warmup_days=10,horizon_minutes=240,data_sha256=final_data_sha);stages=builder(repo=self.repo,python=sys.executable,instrument=i,cache=cache,workspace=ad,start=start.isoformat(),end=end.isoformat(),warmup=10,horizon=240,variant="V331_BASELINE",embargo=30,discovery_fraction=.60,validation_fraction=.20,min_resolved=10,code_sha=sha,state=state);write_json(ad/"cascade_manifest.json",{"schema_version":3,"automation":"V3","instrument":i,"code_sha":sha,"data_sha256":final_data_sha,"production_authority":False,"stages":[{"name":x.name,"artifact":str(x.artifact),"command":list(x.command)} for x in stages]});c.run(i,stages,through="prompts");err=None;ledger.append(i,"decision_history",{"decision":"DATA_REACQUIRE_SUCCEEDED","months":months,"data_sha256":final_data_sha,"at":utc_now()})
       except Exception as x:
        err=x
        if ip.is_file() and integrity_artifact_failed(load_json(ip)):
         diag=build_integrity_diagnostic(load_json(ip),artifact_path=ip,cache_path=cache,requested_start=start.isoformat(),requested_end=end.isoformat(),cache_preexisting=False,retry_count=1)
         write_json(ad/"integrity_diagnostic.json",diag);ledger.mutate(i,integrity_diagnostic=diag,lookback_months=months);ledger.append(i,"decision_history",{"decision":"DATA_INTEGRITY_RETRY_FAILED","months":months,"diagnostic":diag,"at":utc_now()})
        else:
         diag=None;ledger.mutate(i,integrity_diagnostic=None,lookback_months=months)
       if err and isinstance(diag,Mapping) and diag.get("recoverable") is True:
        if months==12:return self._terminal(ledger,i,"DATA_COVERAGE_INSUFFICIENT","recoverable data coverage exhausted maximum lookback",lookback_months=months,integrity_diagnostic=diag)
        ledger.append(i,"decision_history",{"decision":"EXPAND_LOOKBACK","months":months,"recommended_action":"EXPAND_LOOKBACK","at":utc_now()});continue
      if err and isinstance(diag,Mapping) and diag.get("recoverable") is not True:
       return self._terminal(ledger,i,terminal_for_nonrecoverable(diag),str(err),lookback_months=months,integrity_diagnostic=diag)
    if err:
     dp=ad/"06_discovery.json"
     if dp.is_file():
      diag=diagnose_discovery(load_json(dp));write_json(ad/"support_diagnostic.json",diag);ledger.append(i,"decision_history",{"decision":"DISCOVERY_DIAGNOSTIC","months":months,**diag,"at":utc_now()})
      if diag["recommended_action"]=="EXPAND_LOOKBACK":
       if months==12:return self._terminal(ledger,i,"INSUFFICIENT_EVIDENCE","maximum lookback exhausted",diagnostic=diag)
       continue
      if diag["recommended_action"]=="NO_VALID_CANDIDATE":return self._terminal(ledger,i,"NO_VALID_CANDIDATE",diag["dominant_failure"],diagnostic=diag)
     return self._terminal(ledger,i,"METHODOLOGY_BLOCKED",str(err),lookback_months=months)
   h=load_json(ad/"10_holdout.json");pre=load_json(ad/"13_pre_audit.json");ranking=h.get("candidate_ranking") or []
   if h.get("status")!="PASS" or (h.get("overfitting_risk") or {}).get("severity")=="HIGH" or pre.get("verdict") not in {"ACCEPT","ACCEPT WITH LIMITATIONS"} or not any(isinstance(x,Mapping) and x.get("status")=="RESEARCH_CANDIDATE" for x in ranking):return self._terminal(ledger,i,"NO_VALID_CANDIDATE","holdout/pre-audit did not establish PAPER candidate",lookback_months=months)
   cand=next(x for x in ranking if isinstance(x,Mapping) and x.get("status")=="RESEARCH_CANDIDATE");plan=ad/"paper_release_plan.json";write_json(plan,{"instrument":i,"candidate":cand,"source_code_sha":sha,"production_authority":False})
   if isinstance(self.release,GovernedReleaseController):
    try:compiled=compile_and_write_release_plan(repo=self.repo,plan_path=plan,instrument=i,source_code_sha=sha)
    except CandidateNotDeployable as exc:return self._terminal(ledger,i,"CANDIDATE_NOT_DEPLOYABLE",str(exc),lookback_months=months)
    ledger.mutate(i,status="PAPER_DEPLOYABLE_CANDIDATE",paper_release_plan=str(plan),paper_release_plan_sha256=canonical_sha256(compiled))
   rel=self.release.prepare_test_merge(plan=plan,base_sha=sha,instrument=i);ledger.mutate(i,release=rel)
   if rel.get("status")=="CANDIDATE_NOT_DEPLOYABLE":return self._terminal(ledger,i,"CANDIDATE_NOT_DEPLOYABLE",str(rel.get("reason")),release=rel)
   if rel.get("status")!="PASS":return self._terminal(ledger,i,"TEST_FAILURE" if rel.get("reason") in {"TEST_FAILURE","GIT_DIFF_CHECK_FAILED","DIFF_POLICY_BLOCK"} else "DEPLOYMENT_FAILURE",str(rel.get("reason")),release=rel)
   dep=self.release.deploy_paper(expected_sha=rel["merged_main_sha"],environment={"TRADING_ENVIRONMENT":"PAPER","PRIMARY_OANDA_ENV":"practice","OANDA":PRACTICE_OANDA_URL});ledger.mutate(i,paper_deployment=dep)
   if dep.get("status")!="PAPER_DEPLOYED":return self._terminal(ledger,i,"DEPLOYMENT_FAILURE",str(dep.get("reason")),deployment=dep)
   return self._terminal(ledger,i,"PAPER_DEPLOYED","PAPER release verified",deployment=dep)
  return self._terminal(ledger,i,"INSUFFICIENT_EVIDENCE","lookback policy exhausted")

def main():
 p=argparse.ArgumentParser(description="BotsTrader Automation V3 one-command optimizer; PAPER maximum authority");p.add_argument("instrument");a=p.parse_args();r=AutonomousAssetOptimizer(Path(__file__).resolve().parent).optimize(a.instrument);print(json.dumps(r,indent=2,sort_keys=True));raise SystemExit(0 if r.get("status") in {"PAPER_DEPLOYED","CANDIDATE_NOT_DEPLOYABLE","NO_VALID_CANDIDATE","INSUFFICIENT_EVIDENCE","DATA_COVERAGE_INSUFFICIENT"} else 2)
if __name__=="__main__":main()

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise SystemExit(f"expected one match in {path}, got {text.count(old)}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# Optimizer: import policy and expose explicit data terminal states.
replace_once(
    "autonomous_asset_optimizer.py",
    "from automation_v3_candidate_mapping import CandidateNotDeployable,compile_and_write_release_plan\n",
    "from automation_v3_candidate_mapping import CandidateNotDeployable,compile_and_write_release_plan\n"
    "from automation_v3_integrity_recovery import build_integrity_diagnostic,terminal_for_nonrecoverable\n",
)
replace_once(
    "autonomous_asset_optimizer.py",
    'TERMINAL_STATES={"PAPER_DEPLOYED","PAPER_DEPLOYABLE_CANDIDATE","CANDIDATE_NOT_DEPLOYABLE","NO_VALID_CANDIDATE","INSUFFICIENT_EVIDENCE","DATA_SOURCE_UNAVAILABLE","METHODOLOGY_BLOCKED","TEST_FAILURE","DEPLOYMENT_FAILURE","UNSUPPORTED_INSTRUMENT"}',
    'TERMINAL_STATES={"PAPER_DEPLOYED","PAPER_DEPLOYABLE_CANDIDATE","CANDIDATE_NOT_DEPLOYABLE","NO_VALID_CANDIDATE","INSUFFICIENT_EVIDENCE","DATA_SOURCE_UNAVAILABLE","DATA_COVERAGE_INSUFFICIENT","DATA_INTEGRITY_FAILED","METHODOLOGY_BLOCKED","TEST_FAILURE","DEPLOYMENT_FAILURE","UNSUPPORTED_INSTRUMENT"}',
)
replace_once(
    "autonomous_asset_optimizer.py",
    '  fs=start-timedelta(days=int(kw.get("warmup_days",10)));fe=end+timedelta(minutes=int(kw.get("horizon_minutes",240)));tfs=("H1","M15","M5","M1")',
    '  fs=start-timedelta(days=int(kw.get("warmup_days",10))+int(kw.get("boundary_buffer_days",0)));fe=end+timedelta(minutes=int(kw.get("horizon_minutes",240))+int(kw.get("boundary_buffer_minutes",0)));tfs=("H1","M15","M5","M1")',
)
replace_once(
    "autonomous_asset_optimizer.py",
    '   ad=root/f"lookback_{months:02d}m_{sha[:12]}";ad.mkdir(parents=True,exist_ok=True);start=self._months_before(end,months);cache=root/"data"/f"{i}_{months:02d}m.json";ledger.append(i,"lookback_attempts",{"months":months,"code_sha":sha,"start":start.isoformat(),"end":end.isoformat(),"status":"RUNNING","at":utc_now()})',
    '   ad=root/f"lookback_{months:02d}m_{sha[:12]}";ad.mkdir(parents=True,exist_ok=True);start=self._months_before(end,months);cache=root/"data"/f"{i}_{months:02d}m.json";cache_preexisting=cache.is_file();ledger.append(i,"lookback_attempts",{"months":months,"code_sha":sha,"start":start.isoformat(),"end":end.isoformat(),"status":"RUNNING","at":utc_now()})',
)
old_block = '''    if err:\n     ip=ad/"01_data_integrity.json"\n     if ip.is_file():\n      failures=" ".join(str(x) for x in (load_json(ip).get("failures") or [])).upper()\n      if any(x in failures for x in ("COVERAGE","WARMUP","HORIZON","MISSING")):\n       try:\n        if cache.exists():cache.unlink()\n        asyncio.run(self.data_source.acquire(i,start,end,cache,warmup_days=10,horizon_minutes=240));m.register_asset(i,code_sha=sha,start=start.isoformat(),end=end.isoformat(),warmup_days=10,horizon_minutes=240,data_sha256=sha256_file(cache));c.run(i,stages,through="prompts");err=None\n       except Exception as x:err=x\n'''
new_block = '''    if err:\n     ip=ad/"01_data_integrity.json"\n     if ip.is_file():\n      diag=build_integrity_diagnostic(load_json(ip),artifact_path=ip,cache_path=cache,requested_start=start.isoformat(),requested_end=end.isoformat(),cache_preexisting=cache_preexisting,retry_count=0)\n      write_json(ad/"integrity_diagnostic.json",diag);ledger.mutate(i,integrity_diagnostic=diag,lookback_months=months);ledger.append(i,"decision_history",{"decision":"DATA_INTEGRITY_DIAGNOSTIC","months":months,"diagnostic":diag,"at":utc_now()})\n      if diag.get("recoverable") is True:\n       ledger.append(i,"decision_history",{"decision":"DATA_REACQUIRE_REQUIRED","months":months,"recommended_action":"REACQUIRE_SAME_LOOKBACK","at":utc_now()})\n       try:\n        if cache.exists():cache.unlink()\n        asyncio.run(self.data_source.acquire(i,start,end,cache,warmup_days=10,horizon_minutes=240,boundary_buffer_days=3,boundary_buffer_minutes=60))\n       except Exception as x:return self._terminal(ledger,i,"DATA_SOURCE_UNAVAILABLE",str(x),lookback_months=months,integrity_diagnostic=diag)\n       try:\n        m.register_asset(i,code_sha=sha,start=start.isoformat(),end=end.isoformat(),warmup_days=10,horizon_minutes=240,data_sha256=sha256_file(cache));c.run(i,stages,through="prompts");err=None;ledger.append(i,"decision_history",{"decision":"DATA_REACQUIRE_SUCCEEDED","months":months,"at":utc_now()})\n       except Exception as x:\n        err=x\n        if ip.is_file():\n         diag=build_integrity_diagnostic(load_json(ip),artifact_path=ip,cache_path=cache,requested_start=start.isoformat(),requested_end=end.isoformat(),cache_preexisting=False,retry_count=1)\n         write_json(ad/"integrity_diagnostic.json",diag);ledger.mutate(i,integrity_diagnostic=diag,lookback_months=months);ledger.append(i,"decision_history",{"decision":"DATA_INTEGRITY_RETRY_FAILED","months":months,"diagnostic":diag,"at":utc_now()})\n       if err and diag.get("recoverable") is True:\n        if months==12:return self._terminal(ledger,i,"DATA_COVERAGE_INSUFFICIENT","recoverable data coverage exhausted maximum lookback",lookback_months=months,integrity_diagnostic=diag)\n        ledger.append(i,"decision_history",{"decision":"EXPAND_LOOKBACK","months":months,"recommended_action":"EXPAND_LOOKBACK","at":utc_now()});continue\n      if err and diag.get("recoverable") is not True:\n       return self._terminal(ledger,i,terminal_for_nonrecoverable(diag),str(err),lookback_months=months,integrity_diagnostic=diag)\n'''
replace_once("autonomous_asset_optimizer.py", old_block, new_block)
replace_once(
    "autonomous_asset_optimizer.py",
    'p=argparse.ArgumentParser(description="BotsTrader Automation V3 one-command optimizer; PAPER maximum authority");p.add_argument("instrument");a=p.parse_args();r=AutonomousAssetOptimizer(Path(__file__).resolve().parent).optimize(a.instrument);print(json.dumps(r,indent=2,sort_keys=True));raise SystemExit(0 if r.get("status") in {"PAPER_DEPLOYED","CANDIDATE_NOT_DEPLOYABLE","NO_VALID_CANDIDATE","INSUFFICIENT_EVIDENCE"} else 2)',
    'p=argparse.ArgumentParser(description="BotsTrader Automation V3 one-command optimizer; PAPER maximum authority");p.add_argument("instrument");a=p.parse_args();r=AutonomousAssetOptimizer(Path(__file__).resolve().parent).optimize(a.instrument);print(json.dumps(r,indent=2,sort_keys=True));raise SystemExit(0 if r.get("status") in {"PAPER_DEPLOYED","CANDIDATE_NOT_DEPLOYABLE","NO_VALID_CANDIDATE","INSUFFICIENT_EVIDENCE","DATA_COVERAGE_INSUFFICIENT"} else 2)',
)

# Remote status: publish structured integrity evidence and distinguish safe coverage terminal.
replace_once(
    "automation_v3_remote_worker.py",
    'EXPECTED_TERMINALS = {"PAPER_DEPLOYED", "CANDIDATE_NOT_DEPLOYABLE", "NO_VALID_CANDIDATE", "INSUFFICIENT_EVIDENCE"}',
    'EXPECTED_TERMINALS = {"PAPER_DEPLOYED", "CANDIDATE_NOT_DEPLOYABLE", "NO_VALID_CANDIDATE", "INSUFFICIENT_EVIDENCE", "DATA_COVERAGE_INSUFFICIENT"}',
)
replace_once(
    "automation_v3_remote_worker.py",
    '    "DATA_SOURCE_UNAVAILABLE", "METHODOLOGY_BLOCKED", "TEST_FAILURE",\n    "DEPLOYMENT_FAILURE", "UNSUPPORTED_INSTRUMENT",',
    '    "DATA_SOURCE_UNAVAILABLE", "DATA_INTEGRITY_FAILED", "METHODOLOGY_BLOCKED", "TEST_FAILURE",\n    "DEPLOYMENT_FAILURE", "UNSUPPORTED_INSTRUMENT",',
)
replace_once(
    "automation_v3_remote_worker.py",
    '        "last_error": error or run.get("stop_reason"),\n        "production_authority": False,',
    '        "last_error": error or run.get("stop_reason"),\n        "integrity_diagnostic": run.get("integrity_diagnostic") if isinstance(run.get("integrity_diagnostic"), dict) else None,\n        "production_authority": False,',
)

# CI: ensure the focused recovery tests are part of the branch gate.
ci = Path(".github/workflows/automation-v3-remote-runner-ci.yml")
text = ci.read_text(encoding="utf-8")
needle = "      - name: Focused candidate contract tests\n        run: python -m pytest -q test_automation_v3_candidate_mapping.py\n"
replacement = needle + "      - name: Focused integrity recovery tests\n        run: python -m pytest -q test_automation_v3_integrity_recovery.py\n"
if text.count(needle) != 1:
    raise SystemExit("CI insertion point missing")
ci.write_text(text.replace(needle, replacement, 1), encoding="utf-8")

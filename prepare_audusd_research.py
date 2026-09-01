#!/usr/bin/env python3
"""Prepare the portable AUD/USD replay-to-Phase-1 offline cascade."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from research_manager import ResearchManager, sha256_file


START="2026-07-29T00:00:00Z"
END="2026-08-29T23:59:59Z"
HORIZON=240


def git_head() -> str:
    result=subprocess.run(["git","rev-parse","HEAD"],check=True,text=True,capture_output=True)
    return result.stdout.strip()


def main() -> None:
    parser=argparse.ArgumentParser(description="Prepare AUD/USD offline research from an existing BID/ASK cache")
    parser.add_argument("--cache",required=True,help="Existing AUD/USD historical BID/ASK cache")
    parser.add_argument("--workspace",default="research_runs/AUD_USD_20260729_20260829")
    parser.add_argument("--python",default=sys.executable)
    args=parser.parse_args()
    cache=Path(args.cache).resolve()
    if not cache.is_file():
        raise SystemExit(f"cache not found: {cache}")
    workspace=Path(args.workspace).resolve();workspace.mkdir(parents=True,exist_ok=True)
    replay=workspace/"01_replay.json"
    target=workspace/"02_target_population.json"
    phase1=workspace/"03_phase1.json"
    state=workspace/"research_state.json"
    manager=ResearchManager(state)
    manager.register_asset(
        "AUD_USD",code_sha=git_head(),start=START,end=END,warmup_days=10,
        horizon_minutes=HORIZON,data_sha256=sha256_file(cache),
    )
    script_root=Path(__file__).resolve().parent
    stages=[
        {"name":"replay","artifact":str(replay),"command":[args.python,str(script_root/"run_historical_replay.py"),
         "--instrument","AUD_USD","--start",START,"--end",END,"--cache",str(cache),
         "--output",str(replay),"--horizon",str(HORIZON),"--session-scales","1.0"]},
        {"name":"target_population","artifact":str(target),"command":[args.python,str(script_root/"research_pipeline.py"),
         "target-population","--replay",str(replay),"--variant","V331_BASELINE","--output",str(target)]},
        {"name":"phase_1","artifact":str(phase1),"command":[args.python,str(script_root/"research_pipeline.py"),
         "phase1","--input",str(target),"--output",str(phase1)]},
    ]
    manifest=workspace/"cascade_manifest.json"
    manifest.write_text(json.dumps({"schema_version":1,"instrument":"AUD_USD","stages":stages},indent=2)+"\n",encoding="utf-8")
    print(json.dumps({
        "status":"READY","instrument":"AUD_USD","state":str(state),"manifest":str(manifest),
        "next_command":[args.python,str(script_root/"cascade_optimizer.py"),"AUD_USD","--state",str(state),
                        "--manifest",str(manifest),"--through","phase_1"],
    },indent=2))


if __name__=="__main__":main()

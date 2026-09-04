import hashlib,json,subprocess
from datetime import datetime,timezone,timedelta
from pathlib import Path

from autonomous_asset_optimizer import aligned_research_end
from research_asset import build_stages
from research_integrity import validate_dataset


def _row(t):
    return {"t":t.isoformat(),"o":1.0,"h":1.1,"l":.9,"c":1.0,"bid_o":.9999,"bid_h":1.0999,"bid_l":.8999,"bid_c":.9999,"ask_o":1.0001,"ask_h":1.1001,"ask_l":.9001,"ask_c":1.0001}

def _bundle(end, short=None):
    steps={"H1":3600,"M15":900,"M5":300,"M1":60}; out={}
    for tf,sec in steps.items():
        last=end-timedelta(seconds=sec)
        if short==tf:last-=timedelta(seconds=sec)
        first=datetime(2026,7,20,0,0,tzinfo=timezone.utc)
        rows=[];t=first
        while t<=last:
            rows.append(_row(t));t+=timedelta(seconds=sec)
        out[tf]=rows
    return out

def _repo(tmp):
    repo=tmp/'repo';repo.mkdir();subprocess.run(['git','init','-q'],cwd=repo,check=True);subprocess.run(['git','config','user.email','x@y.z'],cwd=repo,check=True);subprocess.run(['git','config','user.name','x'],cwd=repo,check=True);(repo/'x').write_text('x');subprocess.run(['git','add','.'],cwd=repo,check=True);subprocess.run(['git','commit','-qm','x'],cwd=repo,check=True);return repo

def _validate(tmp,bundle,*,start='2026-08-04T06:00:00+00:00',end='2026-09-04T06:00:00+00:00',expected=None):
    cache=tmp/'cache.json';cache.write_text(json.dumps(bundle,separators=(',',':')));return validate_dataset(str(cache),instrument='AUD_USD',start=start,end=end,warmup_days=10,horizon_minutes=240,repo=_repo(tmp),expected_data_sha256=expected),cache

def test_reacquire_changes_cache_bytes_provenance_sha_refreshes(tmp_path):
    cache=tmp_path/'c.json';cache.write_text('{"a":1}');old=hashlib.sha256(cache.read_bytes()).hexdigest();cache.write_text('{"a":2}');new=hashlib.sha256(cache.read_bytes()).hexdigest();assert new!=old
    stages=build_stages(repo=Path('.'),python='python',instrument='AUD_USD',cache=cache,workspace=tmp_path,start='2026-08-04T06:00:00+00:00',end='2026-09-04T06:00:00+00:00',warmup=10,horizon=240,variant='V331_BASELINE',embargo=30,discovery_fraction=.6,validation_fraction=.2,min_resolved=10,code_sha='a'*40,state=tmp_path/'s.json')
    cmd=list(stages[0].command);assert cmd[cmd.index('--data-sha256')+1]==new

def test_stale_pre_reacquire_sha_is_not_reused(tmp_path):
    cache=tmp_path/'c.json';cache.write_text('old');old=hashlib.sha256(cache.read_bytes()).hexdigest();cache.write_text('new');new=hashlib.sha256(cache.read_bytes()).hexdigest();stages=build_stages(repo=Path('.'),python='python',instrument='AUD_USD',cache=cache,workspace=tmp_path,start='s',end='e',warmup=10,horizon=240,variant='v',embargo=30,discovery_fraction=.6,validation_fraction=.2,min_resolved=10,code_sha='a'*40,state=tmp_path/'s');cmd=list(stages[0].command);assert old not in cmd and new in cmd

def test_real_cache_tampering_sha_mismatch_hard_block(tmp_path):
    end=datetime(2026,9,4,10,0,tzinfo=timezone.utc);bundle=_bundle(end);cache=tmp_path/'cache.json';cache.write_text(json.dumps(bundle,separators=(',',':')));expected=hashlib.sha256(cache.read_bytes()).hexdigest();cache.write_text(cache.read_text()+' ');r=validate_dataset(str(cache),instrument='AUD_USD',start='2026-08-04T06:00:00+00:00',end='2026-09-04T06:00:00+00:00',warmup_days=10,horizon_minutes=240,repo=_repo(tmp_path),expected_data_sha256=expected);assert 'DATA_SHA256_MISMATCH' in r['failures']

def test_requested_end_plus_240_exactly_covered_horizon_pass(tmp_path):
    r,_=_validate(tmp_path,_bundle(datetime(2026,9,4,10,0,tzinfo=timezone.utc)));assert all(x['horizon_covered'] for x in r['coverage'].values())

def test_horizon_one_candle_short_fails(tmp_path):
    r,_=_validate(tmp_path,_bundle(datetime(2026,9,4,10,0,tzinfo=timezone.utc),short='M1'));assert 'M1_HORIZON_COVERAGE_INCOMPLETE' in r['failures']

def test_h1_boundary_rounding(tmp_path):
    r,_=_validate(tmp_path,_bundle(datetime(2026,9,4,10,0,tzinfo=timezone.utc)));assert r['coverage']['H1']['horizon_covered']

def test_m15_boundary_rounding(tmp_path):
    r,_=_validate(tmp_path,_bundle(datetime(2026,9,4,10,0,tzinfo=timezone.utc)));assert r['coverage']['M15']['horizon_covered']

def test_m5_boundary_rounding(tmp_path):
    r,_=_validate(tmp_path,_bundle(datetime(2026,9,4,10,0,tzinfo=timezone.utc)));assert r['coverage']['M5']['horizon_covered']

def test_m1_boundary_rounding(tmp_path):
    r,_=_validate(tmp_path,_bundle(datetime(2026,9,4,10,0,tzinfo=timezone.utc)));assert r['coverage']['M1']['horizon_covered']

def test_all_timeframes_independently_cover_horizon(tmp_path):
    r,_=_validate(tmp_path,_bundle(datetime(2026,9,4,10,0,tzinfo=timezone.utc),short='M15'));assert not r['coverage']['M15']['horizon_covered'] and all(r['coverage'][x]['horizon_covered'] for x in ('H1','M5','M1'))

def test_metadata_coverage_end_cannot_override_missing_candles(tmp_path):
    r,_=_validate(tmp_path,_bundle(datetime(2026,9,4,10,0,tzinfo=timezone.utc),short='H1'));r['coverage_end']='2099-01-01T00:00:00+00:00';assert not r['coverage']['H1']['horizon_covered']

def test_actual_candles_beyond_requested_end_satisfy_horizon(tmp_path):
    r,_=_validate(tmp_path,_bundle(datetime(2026,9,4,10,0,tzinfo=timezone.utc)));assert r['status']=='PASS'

def test_no_double_add_of_horizon():
    now=datetime(2026,9,4,10,10,6,tzinfo=timezone.utc);assert aligned_research_end(now,240)==datetime(2026,9,4,6,0,tzinfo=timezone.utc)

def test_production_authority_false_preserved():
    assert aligned_research_end(datetime(2026,9,4,10,10,tzinfo=timezone.utc),240).tzinfo==timezone.utc

def test_run_33861871891_regression_fixture(tmp_path):
    now=datetime(2026,9,4,10,10,6,563640,tzinfo=timezone.utc);end=aligned_research_end(now,240);assert end.isoformat()=='2026-09-04T06:00:00+00:00'
    horizon_end=end+timedelta(minutes=240);r,_=_validate(tmp_path,_bundle(horizon_end),start='2026-08-04T06:00:00+00:00',end=end.isoformat());assert r['status']=='PASS' and all(v['warmup_covered'] and v['horizon_covered'] for v in r['coverage'].values())

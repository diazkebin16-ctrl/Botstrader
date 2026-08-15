"""Advanced Candidate Strategy Validation Pipeline (Step 7).

Pure historical validation utilities. This module has no broker write calls,
no production-strategy mutation, and no deployment authority.
"""
from __future__ import annotations
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
import hashlib
import json
import math
import random

from adaptive_learning import (
    candidate_passes,
    candidate_uses_entry_only,
    dataset_fingerprint,
    metrics as base_metrics,
)


def _f(v, default=None):
    try:
        x=float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _dt(v):
    if not v:return None
    try:return datetime.fromisoformat(str(v).replace('Z','+00:00'))
    except Exception:return None


def _sorted(rows):
    return sorted(rows,key=lambda r:(_dt(r.get('entry_ts')) or datetime.min.replace(tzinfo=timezone.utc),str(r.get('trade_id'))))


def _period(rows):
    rr=_sorted(rows)
    return {
        'start':rr[0].get('entry_ts') if rr else None,
        'end':rr[-1].get('exit_ts') or rr[-1].get('entry_ts') if rr else None,
        'trades':len(rr)
    }


def strict_temporal_split(rows: List[Dict[str,Any]], train_fraction: float=0.60,
                          validation_fraction: float=0.20,
                          embargo_minutes: int=30) -> Dict[str,Any]:
    """Chronological train/validation/test. No shuffle. Purges overlap before each boundary."""
    rows=_sorted(rows); n=len(rows)
    if n<30:return {'status':'INSUFFICIENT_DATA','samples':n}
    train_end=max(10,int(n*train_fraction))
    validation_end=max(train_end+5,int(n*(train_fraction+validation_fraction)))
    validation_end=min(validation_end,n-5)
    raw_train=rows[:train_end]
    raw_val=rows[train_end:validation_end]
    test=rows[validation_end:]
    if not raw_val or not test:return {'status':'INSUFFICIENT_DATA','samples':n}
    val_start=_dt(raw_val[0].get('entry_ts'))
    test_start=_dt(test[0].get('entry_ts'))
    emb=timedelta(minutes=max(0,int(embargo_minutes)))
    train=[r for r in raw_train if (_dt(r.get('exit_ts')) and val_start and _dt(r.get('exit_ts'))<val_start-emb)]
    val=[r for r in raw_val if (_dt(r.get('exit_ts')) and test_start and _dt(r.get('exit_ts'))<test_start-emb)]
    return {
        'status':'OK','train':train,'validation':val,'test':test,
        'periods':{'train':_period(train),'validation':_period(val),'test':_period(test)},
        'config':{'train_fraction':train_fraction,'validation_fraction':validation_fraction,
                  'test_fraction':1-train_fraction-validation_fraction,'embargo_minutes':embargo_minutes}
    }


def extended_metrics(rows: List[Dict[str,Any]], basis: Optional[str]=None) -> Dict[str,Any]:
    m=dict(base_metrics(rows,basis))
    vals=[]
    key='net_result' if m.get('basis')=='NET_ACCOUNT_UNITS' else 'realized_r'
    for r in rows:
        x=_f(r.get(key))
        if x is not None:vals.append(x)
    m['average_trade']=sum(vals)/len(vals) if vals else None
    rr=[_f(r.get('realized_r')) for r in rows]
    rr=[x for x in rr if x is not None]
    m['average_realized_r']=sum(rr)/len(rr) if rr else None
    if rr:
        rw=[x for x in rr if x>0]; rl=[x for x in rr if x<0]
        rgp=sum(rw); rgl=abs(sum(rl))
        m['realized_r_expectancy']=sum(rr)/len(rr)
        m['realized_r_profit_factor']=rgp/rgl if rgl>0 else (999.0 if rgp>0 else None)
        curve=peak=rdd=0.0
        for x in rr:
            curve+=x;peak=max(peak,curve);rdd=max(rdd,peak-curve)
        m['realized_r_max_drawdown']=rdd
    else:
        m['realized_r_expectancy']=None;m['realized_r_profit_factor']=None;m['realized_r_max_drawdown']=None
    return m


def candidate_rows(rows,candidate):
    return [r for r in rows if candidate_passes(candidate,r)]


def compare_candidate_vs_production(rows: List[Dict[str,Any]], candidate: Dict[str,Any]) -> Dict[str,Any]:
    b=extended_metrics(rows)
    crows=candidate_rows(rows,candidate)
    c=extended_metrics(crows,b.get('basis'))
    def delta(k):
        a=c.get(k); z=b.get(k)
        return None if a is None or z is None else a-z
    return {
        'production':b,'candidate':c,'candidate_coverage':len(crows)/len(rows) if rows else 0.0,
        'delta_expectancy':delta('expectancy'),'delta_profit_factor':delta('profit_factor'),
        'delta_win_rate':delta('win_rate'),'delta_drawdown':delta('max_drawdown'),
        'delta_sharpe':delta('sharpe'),'delta_sortino':delta('sortino'),
        'delta_stability':delta('stability')
    }


def out_of_sample_test(rows: List[Dict[str,Any]], candidate: Dict[str,Any],
                       embargo_minutes: int=30, min_oos_trades: int=20) -> Dict[str,Any]:
    split=strict_temporal_split(rows,0.60,0.20,embargo_minutes)
    if split.get('status')!='OK':return split
    test=split['test']; comp=compare_candidate_vs_production(test,candidate)
    status='OK' if comp['candidate']['samples']>=min_oos_trades else 'INSUFFICIENT_DATA'
    return {'status':status,'periods':split['periods'],'comparison':comp,'test_trade_ids':[r.get('trade_id') for r in test]}


def walk_forward_analysis(rows: List[Dict[str,Any]], candidate: Dict[str,Any],
                          train_window: int=60, test_window: int=20, step_size: int=20,
                          min_windows: int=3, embargo_minutes: int=30) -> Dict[str,Any]:
    rows=_sorted(rows); windows=[]; start=train_window
    emb=timedelta(minutes=max(0,int(embargo_minutes)))
    while start+test_window<=len(rows):
        test=rows[start:start+test_window]
        test_start=_dt(test[0].get('entry_ts')) if test else None
        raw_train=rows[max(0,start-train_window):start]
        train=[r for r in raw_train if (_dt(r.get('exit_ts')) and test_start and _dt(r.get('exit_ts'))<test_start-emb)]
        comp=compare_candidate_vs_production(test,candidate)
        windows.append({
            'window':len(windows)+1,'train_period':_period(train),'test_period':_period(test),
            'train_samples':len(train),'test_samples':len(test),'comparison':comp
        })
        start+=max(1,step_size)
    valid=[w for w in windows if w['comparison']['candidate']['samples']>=max(5,test_window//4)]
    if len(valid)<min_windows:
        return {'status':'INSUFFICIENT_DATA','windows':windows,'valid_windows':len(valid),'minimum_windows':min_windows}
    exps=[w['comparison']['candidate'].get('expectancy') for w in valid]
    exps=[x for x in exps if x is not None]
    pfs=[w['comparison']['candidate'].get('profit_factor') for w in valid]
    positive=sum(1 for x in exps if x>0)/len(exps) if exps else 0.0
    pf_positive=sum(1 for x in pfs if x is not None and x>=1.0)/len(pfs) if pfs else 0.0
    if len(exps)>1:
        mean=sum(exps)/len(exps); sd=math.sqrt(sum((x-mean)**2 for x in exps)/(len(exps)-1))
        dispersion=sd/(abs(mean)+1e-9)
    else:dispersion=999.0
    stability=max(0.0,min(1.0,0.55*positive+0.30*pf_positive+0.15*(1/(1+dispersion))))
    return {'status':'OK','windows':windows,'valid_windows':len(valid),
            'positive_expectancy_window_fraction':positive,'pf_ge_1_window_fraction':pf_positive,
            'expectancy_dispersion':dispersion,'window_stability_score':stability}


def _stress_values(rows: List[Dict[str,Any]], candidate: Dict[str,Any], extra_cost_r: float=0.0,
                   drop_fraction: float=0.0, seed: int=1, only_volatility: Optional[str]=None):
    selected=candidate_rows(rows,candidate)
    if only_volatility:
        selected=[r for r in selected if str(r.get('volatility_state_entry'))==only_volatility]
    rng=random.Random(seed)
    vals=[]
    for r in selected:
        rv=_f(r.get('realized_r'))
        if rv is None:continue
        if drop_fraction>0 and rng.random()<drop_fraction:continue
        vals.append(rv-extra_cost_r)
    return vals


def _metrics_from_values(vals: List[float]) -> Dict[str,Any]:
    n=len(vals);wins=[x for x in vals if x>0];loss=[x for x in vals if x<0]
    gp=sum(wins);gl=abs(sum(loss));pf=gp/gl if gl>0 else (999.0 if gp>0 else None)
    curve=peak=dd=0.0
    for x in vals:curve+=x;peak=max(peak,curve);dd=max(dd,peak-curve)
    return {'samples':n,'net_profit_r':sum(vals),'expectancy_r':sum(vals)/n if n else None,
            'profit_factor':pf,'win_rate':len(wins)/n if n else None,'max_drawdown_r':dd}


def stress_test(rows: List[Dict[str,Any]], candidate: Dict[str,Any], seed: int=1) -> Dict[str,Any]:
    """Adverse execution proxies in R; they never create edge, only subtract cost/drop fills."""
    scenarios={
        'higher_fees':dict(extra_cost_r=0.05),
        'higher_slippage':dict(extra_cost_r=0.10),
        'wider_spread':dict(extra_cost_r=0.08),
        'latency':dict(extra_cost_r=0.05,drop_fraction=0.05),
        'worse_entry_and_exit':dict(extra_cost_r=0.15),
        'lower_liquidity':dict(extra_cost_r=0.12,drop_fraction=0.08),
        'extreme_volatility':dict(extra_cost_r=0.15,only_volatility='HIGH'),
    }
    results={}
    viable=[]
    for i,(name,cfg) in enumerate(scenarios.items()):
        vals=_stress_values(rows,candidate,seed=seed+i,**cfg)
        m=_metrics_from_values(vals)
        status='INSUFFICIENT_DATA' if m['samples']<8 else ('PASS' if (m['expectancy_r'] or -999)>0 and (m['profit_factor'] or 0)>=1 else 'FAIL')
        results[name]={**m,'status':status,'assumption':cfg}
        if status!='INSUFFICIENT_DATA':viable.append(status=='PASS')
    # Loss-streak stress: reorder all losses first, preserving outcomes; tests drawdown sensitivity only.
    vals=_stress_values(rows,candidate,seed=seed)
    reordered=sorted(vals)
    results['adverse_loss_sequence']={**_metrics_from_values(reordered),'status':'PASS' if reordered else 'INSUFFICIENT_DATA',
                                      'assumption':{'reorder':'losses-first; total edge unchanged'}}
    pass_fraction=sum(viable)/len(viable) if viable else 0.0
    return {'status':'OK' if viable else 'INSUFFICIENT_DATA','scenarios':results,'stress_pass_fraction':pass_fraction}


def parameter_sensitivity(test_rows: List[Dict[str,Any]], candidate: Dict[str,Any]) -> Dict[str,Any]:
    typ=candidate.get('change_type'); pv=candidate.get('proposed_value')
    if typ in ('MIN_CONFIDENCE','MIN_DIRECTOR_CONFIDENCE'):
        pv=float(pv); step=0.02
        vals=sorted(set(max(0.0,min(0.99,pv+d)) for d in (-2*step,-step,0,step,2*step)))
        points=[]
        for v in vals:
            c=dict(candidate);c['proposed_value']=v
            m=extended_metrics(candidate_rows(test_rows,c))
            points.append({'value':v,'metrics':m})
        valid=[x for x in points if x['metrics']['samples']>=5]
        good=sum(1 for x in valid if (x['metrics'].get('expectancy') or -999)>0 and (x['metrics'].get('profit_factor') or 0)>=1)
        robust=good/len(valid) if valid else 0.0
        center=next((x for x in points if abs(x['value']-pv)<1e-9),None)
        center_exp=(center or {}).get('metrics',{}).get('expectancy')
        neighbors=[x['metrics'].get('expectancy') for x in valid if abs(x['value']-pv)>1e-9 and x['metrics'].get('expectancy') is not None]
        cliff=False
        if center_exp is not None and center_exp>0 and neighbors:
            cliff=sum(1 for x in neighbors if x<=0)>=max(1,len(neighbors)//2)
        return {'status':'OK' if valid else 'INSUFFICIENT_DATA','points':points,
                'robustness_score':robust*(0.4 if cliff else 1.0),'parameter_cliff':cliff}
    # Categorical candidates: use coverage + performance across remaining regimes/volatility states.
    selected=candidate_rows(test_rows,candidate);m=extended_metrics(selected)
    coverage=len(selected)/len(test_rows) if test_rows else 0.0
    robustness=max(0.0,min(1.0,(m.get('regime_positive_fraction') or 0.0)*0.7+min(1.0,coverage/0.5)*0.3))
    return {'status':'OK' if len(selected)>=8 else 'INSUFFICIENT_DATA','points':[],
            'robustness_score':robustness,'coverage':coverage,'parameter_cliff':False}


def regime_analysis(rows: List[Dict[str,Any]], candidate: Dict[str,Any], min_samples: int=8) -> Dict[str,Any]:
    selected=candidate_rows(rows,candidate)
    groups={}
    for r in selected:
        rg=str(r.get('market_regime_entry') or 'UNKNOWN')
        groups.setdefault('REGIME:'+rg,[]).append(r)
        vol=str(r.get('volatility_state_entry') or 'UNKNOWN')
        groups.setdefault('VOLATILITY:'+vol,[]).append(r)
    out={}
    for k,rr in groups.items():
        m=extended_metrics(rr)
        out[k]={**m,'evidence_status':'OK' if m['samples']>=min_samples else 'INSUFFICIENT_DATA'}
    return out


def monte_carlo(rows: List[Dict[str,Any]], candidate: Dict[str,Any], simulations: int=300,
                seed: int=1) -> Dict[str,Any]:
    vals=[_f(r.get('realized_r')) for r in candidate_rows(rows,candidate)]
    vals=[x for x in vals if x is not None]
    if len(vals)<15:return {'status':'INSUFFICIENT_DATA','samples':len(vals)}
    rng=random.Random(seed); finals=[];dds=[]
    for _ in range(max(50,int(simulations))):
        seq=list(vals);rng.shuffle(seq)
        curve=peak=dd=0.0
        for x in seq:
            stressed=x-rng.uniform(0.0,0.05) # only worsens execution
            curve+=stressed;peak=max(peak,curve);dd=max(dd,peak-curve)
        finals.append(curve);dds.append(dd)
    finals.sort();dds.sort()
    def pct(a,p):return a[min(len(a)-1,max(0,int((len(a)-1)*p)))]
    return {'status':'OK','samples':len(vals),'simulations':len(finals),'seed':seed,
            'p05_final_r':pct(finals,.05),'median_final_r':pct(finals,.50),'p95_final_r':pct(finals,.95),
            'median_max_drawdown_r':pct(dds,.50),'p95_max_drawdown_r':pct(dds,.95),
            'probability_of_loss':sum(1 for x in finals if x<0)/len(finals)}


def validation_score(oos: Dict[str,Any], wf: Dict[str,Any], stress: Dict[str,Any],
                     sensitivity: Dict[str,Any], regimes: Dict[str,Any], mc: Dict[str,Any]) -> Dict[str,Any]:
    if oos.get('status')!='OK' or wf.get('status')!='OK':
        return {'score':0.0,'status':'NEEDS_MORE_DATA','components':{}}
    comp=oos['comparison']; cm=comp['candidate']; bm=comp['production']
    exp_delta=comp.get('delta_expectancy') or 0.0
    scale=abs(bm.get('expectancy') or 0)+abs(cm.get('average_loss') or 1)+1e-9
    oos_component=max(0,min(1,0.5+exp_delta/(2*scale)))
    wf_component=max(0,min(1,wf.get('window_stability_score') or 0))
    stress_component=max(0,min(1,stress.get('stress_pass_fraction') or 0))
    sens_component=max(0,min(1,sensitivity.get('robustness_score') or 0))
    dd_c=cm.get('max_drawdown');dd_b=bm.get('max_drawdown')
    if dd_c is None:dd_component=0
    elif not dd_b:dd_component=1 if dd_c<=0 else .5
    else:dd_component=max(0,min(1,1-(dd_c/dd_b-0.8)))
    regime_ok=[v for v in regimes.values() if v.get('evidence_status')=='OK']
    regime_component=(sum(1 for v in regime_ok if (v.get('expectancy') or -999)>0)/len(regime_ok)) if regime_ok else .4
    mc_component=.4 if mc.get('status')!='OK' else max(0,min(1,1-(mc.get('probability_of_loss') or 0)))
    sample_component=max(0,min(1,(cm.get('samples') or 0)/60))
    score=(.20*oos_component+.18*wf_component+.14*stress_component+.12*sens_component+
           .12*dd_component+.10*regime_component+.08*mc_component+.06*sample_component)
    return {'score':score,'status':'PROMISING' if score>=.65 else 'REJECTED',
            'components':{'oos':oos_component,'walk_forward':wf_component,'stress':stress_component,
                          'sensitivity':sens_component,'drawdown':dd_component,'regimes':regime_component,
                          'monte_carlo':mc_component,'sample_size':sample_component}}


def run_historical_validation(rows: List[Dict[str,Any]], candidate: Dict[str,Any],
                              train_window: int=60,test_window: int=20,step_size: int=20,
                              min_windows: int=3,embargo_minutes: int=30,min_oos_trades: int=20,
                              monte_carlo_sims: int=300,seed: int=1) -> Dict[str,Any]:
    if not candidate_uses_entry_only(candidate):
        return {'status':'FAILED','reason':'candidate predicate requires non-entry information'}
    split=strict_temporal_split(rows,.60,.20,embargo_minutes)
    if split.get('status')!='OK':return {'status':'INSUFFICIENT_DATA','reason':'temporal split unavailable','split':split}
    backtest=compare_candidate_vs_production(split['train']+split['validation'],candidate)
    validation=compare_candidate_vs_production(split['validation'],candidate)
    oos=out_of_sample_test(rows,candidate,embargo_minutes,min_oos_trades)
    wf=walk_forward_analysis(rows,candidate,train_window,test_window,step_size,min_windows,embargo_minutes)
    test_rows=split['test']
    stress=stress_test(test_rows,candidate,seed)
    sensitivity=parameter_sensitivity(test_rows,candidate)
    regimes=regime_analysis(test_rows,candidate)
    mc=monte_carlo(test_rows,candidate,monte_carlo_sims,seed)
    score=validation_score(oos,wf,stress,sensitivity,regimes,mc)

    reasons=[]
    if oos.get('status')!='OK':reasons.append('out-of-sample evidence insufficient')
    if wf.get('status')!='OK':reasons.append('walk-forward evidence insufficient')
    if wf.get('status')=='OK' and (wf.get('positive_expectancy_window_fraction') or 0)<.60:reasons.append('walk-forward inconsistency')
    if stress.get('status')=='OK' and (stress.get('stress_pass_fraction') or 0)<.50:reasons.append('stress tests fail too frequently')
    if sensitivity.get('status')=='OK' and (sensitivity.get('robustness_score') or 0)<.50:reasons.append('parameter sensitivity / cliff risk')
    comp=(oos.get('comparison') or {})
    cm=comp.get('candidate') or {}; bm=comp.get('production') or {}
    # Higher profit cannot excuse extreme drawdown.
    if (cm.get('net_profit') or 0)>(bm.get('net_profit') or 0) and bm.get('max_drawdown') not in (None,0) and cm.get('max_drawdown') is not None and cm['max_drawdown']>bm['max_drawdown']*1.35:
        reasons.append('candidate profit higher but drawdown materially worse')
    if (cm.get('samples') or 0)<min_oos_trades:reasons.append('candidate OOS sample below minimum')
    # Absolute return/risk sanity: an attractive net profit cannot hide a very deep
    # drawdown and poor payoff geometry. This is deliberately multi-metric.
    cnet=abs(cm.get('net_profit') or 0.0); cdd=cm.get('max_drawdown'); cexp=cm.get('expectancy'); cal=cm.get('average_loss')
    if cnet>0 and cdd is not None and cdd>cnet*0.50 and cal is not None and cexp is not None and abs(cal)>max(abs(cexp)*5.0,1e-9):
        reasons.append('candidate drawdown extreme relative to OOS profit/payoff')

    hard_fail=any(x in reasons for x in ['walk-forward inconsistency','stress tests fail too frequently','parameter sensitivity / cliff risk','candidate profit higher but drawdown materially worse','candidate drawdown extreme relative to OOS profit/payoff'])
    if oos.get('status')!='OK' or wf.get('status')!='OK':final='INSUFFICIENT_DATA'
    elif hard_fail or score['score']<.58:final='FAILED'
    elif score['score']<.65:final='PROMISING'
    else:final='PAPER_TRADING_REQUIRED'
    decision={'INSUFFICIENT_DATA':'NEEDS_MORE_DATA','FAILED':'REJECTED','PROMISING':'PROMISING',
              'PAPER_TRADING_REQUIRED':'PAPER_TRADING_REQUIRED'}.get(final,final)
    return {
        'status':final,'decision':decision,'validation_score':score['score'],'score_components':score['components'],
        'reasons':reasons or ['historical validation robust enough for mandatory paper phase'],
        'dataset_hash':dataset_fingerprint(rows),'period':_period(rows),'split_periods':split['periods'],
        'backtest':backtest,'validation':validation,'out_of_sample':oos,'walk_forward':wf,
        'stress_test':stress,'parameter_sensitivity':sensitivity,'regime_analysis':regimes,
        'monte_carlo':mc,'auto_deploy':False
    }

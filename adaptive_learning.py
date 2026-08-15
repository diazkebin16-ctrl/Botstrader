"""
Adaptive Learning Engine - pure research/validation utilities.

IMPORTANT:
- No broker calls.
- No production-strategy mutation.
- No execution or risk authority.
- Candidate predicates use entry-time fields only.
"""
from __future__ import annotations
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
import math
import hashlib
import json


ENTRY_ONLY_FIELDS = frozenset({
    "trade_id","strategy","symbol","direction","entry_ts","entry_price","position_size",
    "stop_loss","take_profit","market_regime_entry","regime_confidence_entry",
    "volatility_state_entry","trend_strength_entry","strategy_confidence_entry",
    "director_state_entry","director_confidence_entry","risk_multiplier_entry",
    "risk_allow_new_trades_shadow","requested_risk","approved_risk","entry_drawdown",
    "entry_session","confidence_bucket","entry_reasons_json","entry_context_json"
})


def _f(x, default=None):
    try:
        v=float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _dt(x):
    if not x:
        return None
    try:
        return datetime.fromisoformat(str(x).replace("Z","+00:00"))
    except Exception:
        return None


def dataset_fingerprint(rows: List[Dict[str, Any]]) -> str:
    """Stable fingerprint for reproducibility."""
    material=[
        (
            str(r.get("trade_id")),
            str(r.get("entry_ts")),
            str(r.get("exit_ts")),
            _f(r.get("net_result")),
            _f(r.get("realized_r"))
        )
        for r in rows
    ]
    raw=json.dumps(material,separators=(",",":"),sort_keys=False,default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def candidate_uses_entry_only(candidate: Dict[str, Any]) -> bool:
    typ=candidate.get("change_type")
    if typ=="MIN_CONFIDENCE":
        return "strategy_confidence_entry" in ENTRY_ONLY_FIELDS
    if typ=="EXCLUDE_REGIME":
        return "market_regime_entry" in ENTRY_ONLY_FIELDS
    if typ=="EXCLUDE_VOLATILITY":
        return "volatility_state_entry" in ENTRY_ONLY_FIELDS
    if typ=="MIN_DIRECTOR_CONFIDENCE":
        return "director_confidence_entry" in ENTRY_ONLY_FIELDS
    return False


def candidate_passes(candidate: Dict[str, Any], row: Dict[str, Any]) -> bool:
    """
    Candidate filter uses only information known at entry.
    It never touches exit/P&L/MFE/MAE to decide whether a trade qualifies.
    """
    typ=candidate.get("change_type")
    pv=candidate.get("proposed_value")
    if typ=="MIN_CONFIDENCE":
        v=_f(row.get("strategy_confidence_entry"))
        return v is not None and v>=float(pv)
    if typ=="MIN_DIRECTOR_CONFIDENCE":
        v=_f(row.get("director_confidence_entry"))
        return v is not None and v>=float(pv)
    if typ=="EXCLUDE_REGIME":
        return str(row.get("market_regime_entry") or "UNKNOWN")!=str(pv)
    if typ=="EXCLUDE_VOLATILITY":
        return str(row.get("volatility_state_entry") or "UNKNOWN")!=str(pv)
    return False


def metric_basis(rows: List[Dict[str, Any]]) -> str:
    if rows and all(_f(r.get("net_result")) is not None for r in rows):
        return "NET_ACCOUNT_UNITS"
    return "REALIZED_R"


def values(rows: List[Dict[str, Any]], basis: Optional[str]=None) -> List[float]:
    basis=basis or metric_basis(rows)
    key="net_result" if basis=="NET_ACCOUNT_UNITS" else "realized_r"
    return [float(v) for r in rows if (v:=_f(r.get(key))) is not None]


def metrics(rows: List[Dict[str, Any]], basis: Optional[str]=None) -> Dict[str, Any]:
    basis=basis or metric_basis(rows)
    usable=[r for r in rows if _f(r.get("net_result" if basis=="NET_ACCOUNT_UNITS" else "realized_r")) is not None]
    vals=values(usable,basis)
    n=len(vals)
    wins=[x for x in vals if x>0]
    losses=[x for x in vals if x<0]
    gp=sum(wins); gl=abs(sum(losses))
    pf=gp/gl if gl>0 else (999.0 if gp>0 else None)
    exp=sum(vals)/n if n else None
    aw=sum(wins)/len(wins) if wins else None
    al=sum(losses)/len(losses) if losses else None

    curve=peak=0.0; maxdd=0.0
    for x in vals:
        curve+=x; peak=max(peak,curve); maxdd=max(maxdd,peak-curve)

    sharpe=sortino=None
    if n>=2:
        mean=sum(vals)/n
        var=sum((x-mean)**2 for x in vals)/(n-1)
        sd=math.sqrt(var)
        sharpe=(mean/sd)*math.sqrt(n) if sd>0 else None
        downside=[min(0.0,x) for x in vals]
        dvar=sum(x*x for x in downside)/n
        ddev=math.sqrt(dvar)
        sortino=(mean/ddev)*math.sqrt(n) if ddev>0 else None

    fees=sum(abs(_f(r.get("fees_total"),0.0) or 0.0) for r in usable)
    slips=[abs(_f(r.get("entry_slippage_pips"),0.0) or 0.0) for r in usable]
    avg_slip=sum(slips)/len(slips) if slips else 0.0
    approved=[_f(r.get("approved_risk")) for r in usable]
    approved=[x for x in approved if x is not None]
    avg_approved_risk=sum(approved)/len(approved) if approved else None

    # Period stability: monthly expectancy dispersion.
    monthly={}
    for r in usable:
        dt=_dt(r.get("exit_ts") or r.get("entry_ts"))
        if not dt: continue
        monthly.setdefault(f"{dt.year:04d}-{dt.month:02d}",[]).append(r)
    pexps=[]
    for rr in monthly.values():
        vv=values(rr,basis)
        if vv: pexps.append(sum(vv)/len(vv))
    stability=None
    if pexps:
        pos=sum(1 for x in pexps if x>0)/len(pexps)
        if len(pexps)>1:
            mean=sum(pexps)/len(pexps)
            sd=math.sqrt(sum((x-mean)**2 for x in pexps)/(len(pexps)-1))
            dispersion=sd/(abs(mean)+1e-9)
            stability=max(0.0,min(1.0,0.65*pos+0.35*(1.0/(1.0+dispersion))))
        else:
            stability=pos

    regimes={}
    for r in usable:
        rg=str(r.get("market_regime_entry") or "UNKNOWN")
        regimes.setdefault(rg,[]).append(r)
    regime_expectancies={}
    for rg,rr in regimes.items():
        vv=values(rr,basis)
        regime_expectancies[rg]=sum(vv)/len(vv) if vv else None
    regime_positive_fraction=(
        sum(1 for x in regime_expectancies.values() if x is not None and x>0)/
        max(1,sum(1 for x in regime_expectancies.values() if x is not None))
    ) if regime_expectancies else None

    return {
        "samples":n,"basis":basis,"net_profit":sum(vals) if vals else 0.0,
        "win_rate":len(wins)/n if n else None,"profit_factor":pf,"expectancy":exp,
        "average_win":aw,"average_loss":al,"max_drawdown":maxdd,
        "sharpe":sharpe,"sortino":sortino,"fees_total":fees,
        "avg_entry_slippage_pips":avg_slip,"avg_approved_risk":avg_approved_risk,
        "stability":stability,
        "periods":len(monthly),"regime_expectancies":regime_expectancies,
        "regime_positive_fraction":regime_positive_fraction
    }


def purge_before_test(train: List[Dict[str, Any]], test_start: datetime,
                      embargo_minutes: int=30) -> List[Dict[str, Any]]:
    """
    Purge overlapping trades and apply an embargo before test start.
    A training trade must have CLOSED before (test_start - embargo).
    """
    cutoff=test_start-timedelta(minutes=max(0,int(embargo_minutes)))
    out=[]
    for r in train:
        exit_dt=_dt(r.get("exit_ts"))
        if exit_dt and exit_dt<cutoff:
            out.append(r)
    return out


def chronological_splits(rows: List[Dict[str, Any]], folds: int=3,
                         embargo_minutes: int=30) -> List[Dict[str, Any]]:
    """
    Expanding-window walk-forward. Each test fold is chronologically after train.
    """
    rows=sorted(rows,key=lambda r:(_dt(r.get("entry_ts")) or datetime.min.replace(tzinfo=timezone.utc)))
    n=len(rows)
    if n<20:return []
    folds=max(2,min(int(folds),5))
    initial=max(10,int(n*0.45))
    remaining=n-initial
    test_size=max(5,remaining//folds)
    out=[]
    for i in range(folds):
        test_start_i=initial+i*test_size
        test_end=n if i==folds-1 else min(n,test_start_i+test_size)
        if test_start_i>=n or test_end-test_start_i<5:continue
        test=rows[test_start_i:test_end]
        t0=_dt(test[0].get("entry_ts"))
        train=purge_before_test(rows[:test_start_i],t0,embargo_minutes) if t0 else rows[:test_start_i]
        if len(train)<10:continue
        out.append({"fold":i+1,"train":train,"test":test})
    return out


def _norm_pf(x):
    if x is None:return 0.0
    if x>=999:return 1.0
    return max(0.0,min(1.0,(x-0.5)/2.0))


def candidate_score(candidate_metrics: Dict[str, Any],
                    baseline_metrics: Dict[str, Any],
                    sample_target: int=80) -> float:
    """
    Multi-objective score. No single metric can dominate.
    Higher profit with materially worse drawdown/stability is penalized.
    """
    cm=candidate_metrics; bm=baseline_metrics
    exp_c=cm.get("expectancy") or 0.0; exp_b=bm.get("expectancy") or 0.0
    scale=abs(exp_b)+abs(cm.get("average_loss") or 1.0)+1e-9
    expectancy_component=max(0.0,min(1.0,0.5+(exp_c-exp_b)/(2*scale)))
    pf_component=_norm_pf(cm.get("profit_factor"))

    dd_c=cm.get("max_drawdown")
    dd_b=bm.get("max_drawdown")
    if dd_c is None: dd_component=0.0
    elif not dd_b or dd_b<=0: dd_component=1.0 if dd_c<=0 else 0.5
    else: dd_component=max(0.0,min(1.0,1.0-(dd_c/dd_b-0.7)/1.0))

    sharpe=cm.get("sharpe");sortino=cm.get("sortino")
    sharpe_component=max(0.0,min(1.0,0.5+(sharpe or 0.0)/6.0))
    sortino_component=max(0.0,min(1.0,0.5+(sortino or 0.0)/8.0))
    return_quality_component=(sharpe_component+sortino_component)/2.0
    stability_component=max(0.0,min(1.0,cm.get("stability") if cm.get("stability") is not None else 0.35))
    sample_component=max(0.0,min(1.0,cm.get("samples",0)/max(1,sample_target)))
    regime_component=max(0.0,min(1.0,cm.get("regime_positive_fraction")
                                      if cm.get("regime_positive_fraction") is not None else 0.35))

    # Costs: net_result already includes available fees; explicit slippage comparison
    # adds an additional penalty when candidate trades are more expensive to enter.
    slip_c=cm.get("avg_entry_slippage_pips") or 0.0
    slip_b=bm.get("avg_entry_slippage_pips") or 0.0
    cost_component=max(0.0,min(1.0,0.65+(slip_b-slip_c)/max(1.0,abs(slip_b)+1.0)))

    # Risk assumed: candidate never receives credit for requiring materially more
    # approved risk than baseline. Missing risk data remains neutral.
    rc=cm.get("avg_approved_risk"); rb=bm.get("avg_approved_risk")
    if rc is None or rb is None:
        risk_component=0.5
    else:
        risk_component=max(0.0,min(1.0,0.5+(rb-rc)/(2*max(abs(rb),0.001))))

    return (
        0.18*expectancy_component+
        0.15*pf_component+
        0.16*dd_component+
        0.10*return_quality_component+
        0.14*stability_component+
        0.08*sample_component+
        0.07*regime_component+
        0.06*cost_component+
        0.06*risk_component
    )


def validate_candidate(rows: List[Dict[str, Any]], candidate: Dict[str, Any],
                       min_trades: int=20, folds: int=3,
                       embargo_minutes: int=30) -> Dict[str, Any]:
    """
    Walk-forward counterfactual validation for ENTRY-TIME GATING candidates only.
    """
    if not candidate_uses_entry_only(candidate):
        return {"status":"UNVALIDATABLE_WITH_TRADE_MEMORY",
                "reason":"candidate requires fields unavailable at decision time"}

    rows=sorted(rows,key=lambda r:(_dt(r.get("entry_ts")) or datetime.min.replace(tzinfo=timezone.utc)))
    if len(rows)<max(min_trades*2,40):
        return {"status":"INSUFFICIENT_DATA","samples":len(rows)}

    splits=chronological_splits(rows,folds,embargo_minutes)
    if len(splits)<2:
        return {"status":"INSUFFICIENT_DATA","samples":len(rows),"reason":"not enough temporal folds"}

    fold_results=[]
    train_improvements=[]; test_improvements=[]
    for sp in splits:
        train=sp["train"];test=sp["test"]
        train_c=[r for r in train if candidate_passes(candidate,r)]
        test_c=[r for r in test if candidate_passes(candidate,r)]
        btr=metrics(train); ctr=metrics(train_c,btr["basis"])
        bte=metrics(test); cte=metrics(test_c,bte["basis"])

        if cte["samples"]<max(5,min_trades//3):
            fold_results.append({"fold":sp["fold"],"status":"INSUFFICIENT_TEST_COVERAGE",
                                 "baseline_test":bte,"candidate_test":cte})
            continue

        tr_imp=(ctr.get("expectancy") or 0)-(btr.get("expectancy") or 0)
        te_imp=(cte.get("expectancy") or 0)-(bte.get("expectancy") or 0)
        train_improvements.append(tr_imp);test_improvements.append(te_imp)
        fold_results.append({
            "fold":sp["fold"],"status":"OK",
            "train_samples":len(train),"test_samples":len(test),
            "candidate_train_samples":len(train_c),"candidate_test_samples":len(test_c),
            "baseline_train":btr,"candidate_train":ctr,
            "baseline_test":bte,"candidate_test":cte,
            "train_expectancy_improvement":tr_imp,
            "test_expectancy_improvement":te_imp
        })

    ok=[x for x in fold_results if x["status"]=="OK"]
    if len(ok)<2:
        return {"status":"INSUFFICIENT_DATA","folds":fold_results,
                "reason":"too few valid out-of-sample folds"}

    # Aggregate only test rows from valid folds.
    oos_baseline=[];oos_candidate=[]
    for sp,res in zip(splits,fold_results):
        if res["status"]!="OK":continue
        oos_baseline.extend(sp["test"])
        oos_candidate.extend([r for r in sp["test"] if candidate_passes(candidate,r)])
    bm=metrics(oos_baseline)
    cm=metrics(oos_candidate,bm["basis"])

    # Overfit detection: good train / bad OOS or very unstable fold behavior.
    avg_train=sum(train_improvements)/len(train_improvements)
    avg_test=sum(test_improvements)/len(test_improvements)
    positive_test_fraction=sum(1 for x in test_improvements if x>0)/len(test_improvements)
    gap=avg_train-avg_test
    scale=abs(bm.get("expectancy") or 0)+abs(cm.get("average_loss") or 1.0)+1e-9
    overfit=(avg_train>0 and avg_test<=0) or (gap/scale>0.75) or positive_test_fraction<0.50

    score=candidate_score(cm,bm,max(min_trades*3,60))

    # Explicit rejection when profit rises but risk/stability deteriorates materially.
    higher_profit=(cm.get("net_profit") or 0)>(bm.get("net_profit") or 0)
    worse_dd=(
        cm.get("max_drawdown") is not None and bm.get("max_drawdown") not in (None,0)
        and cm["max_drawdown"]>bm["max_drawdown"]*1.20
    )
    worse_stability=(
        cm.get("stability") is not None and bm.get("stability") is not None
        and cm["stability"]<bm["stability"]-0.20
    )
    risk_reject=higher_profit and (worse_dd or worse_stability)

    if overfit:
        status="OVERFIT_REJECTED"
        reason="train improvement did not survive temporal out-of-sample validation"
    elif risk_reject:
        status="RISK_STABILITY_REJECTED"
        reason="higher profit accompanied by materially worse drawdown/stability"
    elif cm["samples"]<min_trades:
        status="INSUFFICIENT_DATA"
        reason="out-of-sample candidate sample below minimum"
    elif score>=0.62 and avg_test>0 and positive_test_fraction>=2/3:
        status="ACCEPTED_AS_CANDIDATE"
        reason="multi-objective OOS score passed"
    else:
        status="REJECTED_AS_CANDIDATE"
        reason="candidate did not meet robust OOS acceptance criteria"

    return {
        "status":status,"reason":reason,"candidate_score":score,
        "dataset_hash":dataset_fingerprint(rows),
        "oos_baseline":bm,"oos_candidate":cm,
        "avg_train_expectancy_improvement":avg_train,
        "avg_oos_expectancy_improvement":avg_test,
        "positive_oos_fold_fraction":positive_test_fraction,
        "overfit_detected":overfit,
        "risk_stability_reject":risk_reject,
        "folds":fold_results
    }


def concept_drift(rows: List[Dict[str, Any]], recent: int=20,
                  min_history: int=30) -> Dict[str, Any]:
    """
    Requires TWO recent chronological windows to weaken consistently.
    This is an alert, never a production change.
    """
    rows=sorted(rows,key=lambda r:(_dt(r.get("entry_ts")) or datetime.min.replace(tzinfo=timezone.utc)))
    if len(rows)<min_history+2*recent:
        return {"status":"INSUFFICIENT_DATA","samples":len(rows)}

    hist=rows[:-(2*recent)]
    prev=rows[-(2*recent):-recent]
    curr=rows[-recent:]
    hm=metrics(hist);pm=metrics(prev,hm["basis"]);cm=metrics(curr,hm["basis"])

    he=hm.get("expectancy");pe=pm.get("expectancy");ce=cm.get("expectancy")
    hp=hm.get("profit_factor");pp=pm.get("profit_factor");cp=cm.get("profit_factor")

    weakening_exp=(he is not None and he>0 and pe is not None and ce is not None and pe<he*0.6 and ce<he*0.6)
    weakening_pf=(hp is not None and hp>=1.2 and pp is not None and cp is not None and pp<hp*0.75 and cp<hp*0.75)
    lost_edge=((pe is not None and pe<=0) and (ce is not None and ce<=0)) or (
        (pp is not None and pp<1.0) and (cp is not None and cp<1.0)
    )
    drift=(weakening_exp or weakening_pf) and lost_edge

    return {
        "status":"POSSIBLE_CONCEPT_DRIFT" if drift else "NO_DRIFT_DETECTED",
        "historical":hm,"previous_window":pm,"current_window":cm,
        "consistent_weakening":bool(weakening_exp or weakening_pf),
        "lost_edge":bool(lost_edge),
        "samples":len(rows)
    }

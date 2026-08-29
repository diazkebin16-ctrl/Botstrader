"""Pre-registered directional-selector null diagnostic.

Research-only: reconstructs BUY and SELL geometry independently at frozen
strategy opportunity timestamps, resolves both with the historical bid/ask
execution model, and compares the selected direction with a 50/50 Monte Carlo
null. The final TEST holdout is excluded by its pre-existing boundary.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from random import Random
from math import comb, sqrt
from statistics import mean, median
from typing import Any, Dict, Mapping, Sequence

from historical_execution import HistoricalExecutionConfig, resolve_executed_outcome
from historical_replay import CandleStore, ReplayVariant, _dt, replay_snapshot

COMPARABLE = {"WIN", "LOSS"}
NONCOMPARABLE = {
    "TIMEOUT", "AMBIGUOUS", "INVALID", "PENDING", "DATA_INSUFFICIENT",
    "DATA_INTEGRITY_ERROR", "ENTRY_INVALIDATED", "GEOMETRY_INVALID",
}

@dataclass(frozen=True)
class DirectionalNullConfig:
    simulations: int = 20_000
    bootstrap_samples: int = 20_000
    rng_seed: int = 3352
    bootstrap_seed: int = 3353
    ci_level: float = 0.90


def _quantile(values: Sequence[float], q: float) -> float:
    if not values:
        raise ValueError("quantile requires data")
    xs=sorted(float(x) for x in values)
    pos=(len(xs)-1)*max(0.0,min(1.0,float(q)))
    lo=int(pos); hi=min(lo+1,len(xs)-1); w=pos-lo
    return xs[lo]*(1.0-w)+xs[hi]*w


def _side_geometry_valid(hyp: Mapping[str,Any]) -> tuple[bool,str|None]:
    """Use frozen safety geometry, not direction-scoring/quality gates."""
    safety=hyp.get("safety_checks") or {}
    required=("finite_prices","positive_risk","minimum_rr","minimum_tp_pips",
              "minimum_stop_pips","barrier_room_ok","volatility_sane")
    failed=[k for k in required if not bool(safety.get(k,False))]
    return (not failed, None if not failed else "SAFETY:"+",".join(failed))


SHADOW_IGNORED_SAFETY = {"minimum_rr", "barrier_room_ok"}

def _side_shadow_geometry_valid(hyp: Mapping[str,Any]) -> tuple[bool,str|None]:
    """Research-only Test B geometry.

    Ignore ONLY minimum_rr and barrier_room_ok. Every other frozen safety
    requirement must remain valid. This never changes production decisions.
    """
    safety=hyp.get("safety_checks") or {}
    required=("finite_prices","positive_risk","minimum_rr","minimum_tp_pips",
              "minimum_stop_pips","barrier_room_ok","volatility_sane")
    failed=[k for k in required if not bool(safety.get(k,False))]
    blocking=[k for k in failed if k not in SHADOW_IGNORED_SAFETY]
    return (not blocking, None if not blocking else "SAFETY:"+",".join(blocking))


def _resolve_side_shadow(hyp: Mapping[str,Any], direction: str, instrument: str,
                         future: Sequence[Mapping[str,Any]], horizon_bars: int,
                         execution: HistoricalExecutionConfig) -> Dict[str,Any]:
    """Resolve frozen entry/stop/target while ignoring only Test B safety gates."""
    valid,reason=_side_shadow_geometry_valid(hyp)
    if not valid:
        return {"status":"GEOMETRY_INVALID","realized_r":None,"note":reason,
                "shadow_test_b":True}
    payload={"direction":direction,"entry":hyp["entry"],"stop":hyp["stop"],
             "target":hyp["target"],"instrument":instrument}
    out=resolve_executed_outcome(payload,future,horizon_bars=horizon_bars,config=execution)
    result=dict(out) if out else {"status":"PENDING","realized_r":None,
                                  "note":"insufficient future bars"}
    result["shadow_test_b"]=True
    return result


def _resolve_side(hyp: Mapping[str,Any], direction: str, instrument: str,
                  future: Sequence[Mapping[str,Any]], horizon_bars: int,
                  execution: HistoricalExecutionConfig) -> Dict[str,Any]:
    valid,reason=_side_geometry_valid(hyp)
    if not valid:
        return {"status":"GEOMETRY_INVALID","realized_r":None,"note":reason}
    payload={"direction":direction,"entry":hyp["entry"],"stop":hyp["stop"],
             "target":hyp["target"],"instrument":instrument}
    out=resolve_executed_outcome(payload,future,horizon_bars=horizon_bars,config=execution)
    return dict(out) if out else {"status":"PENDING","realized_r":None,"note":"insufficient future bars"}


def _bootstrap_ci(values: Sequence[float], *, samples: int, seed: int, level: float) -> tuple[float|None,float|None]:
    vals=[float(x) for x in values]
    if not vals:return None,None
    rng=Random(seed); n=len(vals)
    sims=[mean(vals[rng.randrange(n)] for _ in range(n)) for _ in range(max(1,int(samples)))]
    alpha=(1.0-max(0.0,min(1.0,float(level))))/2.0
    return _quantile(sims,alpha),_quantile(sims,1.0-alpha)


def evaluate_pairs(rows: Sequence[Mapping[str,Any]], config: DirectionalNullConfig=DirectionalNullConfig()) -> Dict[str,Any]:
    paired=[]; one_sided=[]
    counts={"total_opportunities":len(rows),"both_sides_comparable":0,"buy_only_valid":0,
            "sell_only_valid":0,"neither_comparable":0,"timeouts":0,"ambiguous":0,
            "invalid_or_data_states":0}
    status_counts={}
    geometry_invalid_reasons={}
    for r in rows:
        b,s=r["buy"],r["sell"]
        for x in (b,s):
            st=str(x.get("status") or "PENDING");status_counts[st]=status_counts.get(st,0)+1
            if st=="GEOMETRY_INVALID":
                reason=str(x.get("note") or "UNKNOWN")
                geometry_invalid_reasons[reason]=geometry_invalid_reasons.get(reason,0)+1
            if st=="TIMEOUT":counts["timeouts"]+=1
            elif st=="AMBIGUOUS":counts["ambiguous"]+=1
            elif st not in COMPARABLE:counts["invalid_or_data_states"]+=1
        bc=b.get("status") in COMPARABLE and b.get("realized_r") is not None
        sc=s.get("status") in COMPARABLE and s.get("realized_r") is not None
        if bc and sc:
            counts["both_sides_comparable"]+=1;paired.append(r)
        elif bc:
            counts["buy_only_valid"]+=1;one_sided.append((r,"BUY"))
        elif sc:
            counts["sell_only_valid"]+=1;one_sided.append((r,"SELL"))
        else:counts["neither_comparable"]+=1

    bot_vals=[]; pairs=[]
    for r in paired:
        br=float(r["buy"]["realized_r"]); sr=float(r["sell"]["realized_r"])
        pairs.append((br,sr));bot_vals.append(br if r["bot_direction"]=="BUY" else sr)
    bot_exp=mean(bot_vals) if bot_vals else None
    null=[]
    if pairs:
        rng=Random(config.rng_seed)
        for _ in range(max(1,int(config.simulations))):
            null.append(mean((br if rng.getrandbits(1)==0 else sr) for br,sr in pairs))
    q95=_quantile(null,.95) if null else None
    # Empirical percentile: fraction of null strategies at or below the bot.
    pct=(sum(x<=bot_exp for x in null)/len(null)) if null and bot_exp is not None else None
    ci_lo,ci_hi=_bootstrap_ci(bot_vals,samples=config.bootstrap_samples,seed=config.bootstrap_seed,level=config.ci_level)
    oracle=mean(max(br,sr) for br,sr in pairs) if pairs else None
    unique_hits=sum(1 for r,side in one_sided if r.get("bot_direction")==side)
    unique_rate=unique_hits/len(one_sided) if one_sided else None
    c1=bool(bot_exp is not None and q95 is not None and bot_exp>q95)
    c2=bool(ci_lo is not None and ci_lo>0.0)
    return {
        "counts":counts,"status_counts":status_counts,
        "geometry_invalid_reasons":geometry_invalid_reasons,
        "paired_test":{"n":len(paired),"bot_expectancy_r":bot_exp,
            "null_mean":mean(null) if null else None,"null_median":median(null) if null else None,
            "null_p95":q95,"bot_empirical_percentile":pct,"simulations":len(null),
            "condition_1_directional_skill_pass":c1},
        "economic_edge":{"n":len(bot_vals),"expectancy_r":bot_exp,"bootstrap_ci_level":config.ci_level,
            "bootstrap_ci":[ci_lo,ci_hi],"bootstrap_samples":max(1,int(config.bootstrap_samples)),
            "condition_2_economic_edge_pass":c2},
        "one_sided_robustness":{"n":len(one_sided),"bot_selected_unique_valid_side":unique_hits,
            "selection_rate":unique_rate},
        "oracle":{"n":len(pairs),"expectancy_r":oracle,"warning":"EX_POST_DIAGNOSTIC_NOT_A_STRATEGY"},
        "interpretation":("DIRECTIONAL_SKILL_AND_ECONOMIC_EDGE" if c1 and c2 else
                          "DIRECTIONAL_SKILL_BUT_NO_DEMONSTRATED_ECONOMIC_EDGE" if c1 else
                          "DIRECTIONAL_SKILL_NOT_SUPPORTED")
    }


def _paired_statistics(rows: Sequence[Mapping[str,Any]], *, buy_key: str, sell_key: str,
                       config: DirectionalNullConfig, rng_seed: int,
                       bootstrap_seed: int) -> Dict[str,Any]:
    paired=[]; excluded={}
    for r in rows:
        b,s=r[buy_key],r[sell_key]
        bc=b.get("status") in COMPARABLE and b.get("realized_r") is not None
        sc=s.get("status") in COMPARABLE and s.get("realized_r") is not None
        if bc and sc:
            paired.append(r)
        else:
            key=f"{b.get('status','PENDING')}|{s.get('status','PENDING')}"
            excluded[key]=excluded.get(key,0)+1

    bot_vals=[]; opposite_vals=[]; pairs=[]
    for r in paired:
        br=float(r[buy_key]["realized_r"]); sr=float(r[sell_key]["realized_r"])
        pairs.append((br,sr))
        if r["bot_direction"]=="BUY":
            bot_vals.append(br); opposite_vals.append(sr)
        else:
            bot_vals.append(sr); opposite_vals.append(br)

    bot_exp=mean(bot_vals) if bot_vals else None
    opp_exp=mean(opposite_vals) if opposite_vals else None
    null=[]
    if pairs:
        rng=Random(rng_seed)
        for _ in range(max(1,int(config.simulations))):
            null.append(mean((br if rng.getrandbits(1)==0 else sr) for br,sr in pairs))
    q95=_quantile(null,.95) if null else None
    pct=(sum(x<=bot_exp for x in null)/len(null)) if null and bot_exp is not None else None
    ci_lo,ci_hi=_bootstrap_ci(bot_vals,samples=config.bootstrap_samples,
                              seed=bootstrap_seed,level=config.ci_level)
    oracle=mean(max(br,sr) for br,sr in pairs) if pairs else None
    c1=bool(bot_exp is not None and q95 is not None and bot_exp>q95)
    c2=bool(ci_lo is not None and ci_lo>0.0)
    return {
        "n":len(paired),
        "bot_expectancy_r":bot_exp,
        "opposite_shadow_expectancy_r":opp_exp,
        "bot_minus_opposite_r":(bot_exp-opp_exp) if bot_exp is not None and opp_exp is not None else None,
        "null_mean":mean(null) if null else None,
        "null_median":median(null) if null else None,
        "null_p95":q95,
        "bot_empirical_percentile":pct,
        "simulations":len(null),
        "bootstrap_ci_level":config.ci_level,
        "bootstrap_ci":[ci_lo,ci_hi],
        "bootstrap_samples":max(1,int(config.bootstrap_samples)),
        "condition_1_directional_skill_pass":c1,
        "condition_2_economic_edge_pass":c2,
        "oracle_expectancy_r":oracle,
        "excluded_status_pairs":excluded,
        "interpretation":("DIRECTIONAL_SKILL_AND_ECONOMIC_EDGE" if c1 and c2 else
                          "DIRECTIONAL_SKILL_BUT_NO_DEMONSTRATED_ECONOMIC_EDGE" if c1 else
                          "DIRECTIONAL_SKILL_NOT_SUPPORTED")
    }


def evaluate_shadow_test_b(rows: Sequence[Mapping[str,Any]],
                           config: DirectionalNullConfig=DirectionalNullConfig()) -> Dict[str,Any]:
    """Extended research-only paired test.

    Uses normal outcomes where available and shadow outcomes only where the
    side was blocked exclusively by minimum_rr and/or barrier_room_ok.
    Test A remains untouched and is reported separately.
    """
    eligible=0; shadow_used=0
    for r in rows:
        for normal_key,shadow_key in (("buy","buy_shadow_b"),("sell","sell_shadow_b")):
            normal=r[normal_key]
            shadow=r[shadow_key]
            if normal.get("status")=="GEOMETRY_INVALID":
                note=str(normal.get("note") or "")
                failed=set(note.removeprefix("SAFETY:").split(",")) if note.startswith("SAFETY:") else set()
                if failed and failed.issubset(SHADOW_IGNORED_SAFETY):
                    eligible+=1
                    if shadow.get("status") in COMPARABLE and shadow.get("realized_r") is not None:
                        shadow_used+=1

    stats=_paired_statistics(rows,buy_key="buy_shadow_b",sell_key="sell_shadow_b",
                             config=config,rng_seed=config.rng_seed+1000,
                             bootstrap_seed=config.bootstrap_seed+1000)
    stats.update({
        "test":"B_EXTENDED_SHADOW",
        "research_only":True,
        "ignored_safety_checks":sorted(SHADOW_IGNORED_SAFETY),
        "eligible_invalid_sides":eligible,
        "eligible_shadow_sides_resolved_comparable":shadow_used,
        "warning":"COUNTERFACTUAL_DIAGNOSTIC_NOT_A_PRODUCTION_RULE"
    })
    return stats


def _direction_component_snapshot(server: Any, hyp: Mapping[str,Any],
                                  m5: Sequence[Mapping[str,Any]], sig: str) -> Dict[str,Any]:
    """Research-only reconstruction of the frozen V331_BASELINE scorer."""
    from historical_replay import _common_components, _legacy_v331_score

    x=_common_components(server,hyp,m5,sig)

    h1_c = 16.0 if x["h1_support"] else (-10.0 if x["h1_opposes"] else 3.0)
    m15_c = 20.0 if x["m15_support"] else (-12.0 if x["m15_opposes"] else 4.0)
    m5_c = 18.0 if x["m5_structure"] else (7.0 if x["m5_momentum"] else 0.0)
    m1_c = 16.0 if x["confirm"] else (6.0 if x["m1_momentum"] else 0.0)
    pullback_c = 8.0 if x["second"] else (4.0 if x["pc"]>=1 and x["pr"] else 0.0)
    rr_c = 8.0 if x["rr_raw"]>=2 else (6.0 if x["rr_raw"]>=server.MIN_RR else 0.0)
    volatility_c = 5.0 if .65<=x["vol"]<=2 else 0.0
    extension_c = 5.0 if x["ext"]<=1.20 else (2.0 if x["ext"]<=1.60 else 0.0)
    session_hours_c = 4.0 if x["session_ok"] else 0.0
    broken_barrier_c = float(min(6,2*x["broken"]))

    preclamp=(
        h1_c+m15_c+m5_c+m1_c+pullback_c+rr_c+
        volatility_c+extension_c+session_hours_c+broken_barrier_c
    )
    reconstructed=float(server.clamp(preclamp,0,100))

    legacy_score,legacy_countertrend,legacy_transition = \
        _legacy_v331_score(server,hyp,m5,sig)

    return {
        "direction_score":float(legacy_score),
        "h1_score_contribution":h1_c,
        "m15_score_contribution":m15_c,
        "m5_score_contribution":m5_c,
        "m1_score_contribution":m1_c,
        "pullback_score_contribution":pullback_c,
        "rr_score_contribution":rr_c,
        "volatility_score_contribution":volatility_c,
        "extension_score_contribution":extension_c,
        "session_hours_score_contribution":session_hours_c,
        "broken_barrier_score_contribution":broken_barrier_c,
        "preclamp_score":preclamp,
        "reconstructed_direction_score":reconstructed,
        "score_reconstruction_error":float(legacy_score)-reconstructed,
        "rr_raw":float(x["rr_raw"]),
        "h1_support":bool(x["h1_support"]),
        "h1_opposes":bool(x["h1_opposes"]),
        "m15_support":bool(x["m15_support"]),
        "m15_opposes":bool(x["m15_opposes"]),
        "m5_structure":bool(x["m5_structure"]),
        "m5_momentum_support":bool(x["m5_momentum"]),
        "m1_confirmation":bool(x["confirm"]),
        "m1_momentum_support":bool(x["m1_momentum"]),
        "second_pullback":bool(x["second"]),
        "pullback_count":int(x["pc"]),
        "pullback_ready":bool(x["pr"]),
        "volatility_ratio":float(x["vol"]),
        "extension_atr":float(x["ext"]),
        "session_hours_ok":bool(x["session_ok"]),
        "broken_barrier_count":int(x["broken"]),
        "session_direction":str((hyp.get("metrics") or {}).get("session_regime",{}).get("direction","NEUTRAL")),
        "session_strength":float((hyp.get("metrics") or {}).get("session_regime",{}).get("strength",0) or 0),
        "countertrend":bool(legacy_countertrend),
        "transition":bool(legacy_transition),
        "scorer":"V331_BASELINE_LEGACY",
    }


def evaluate_component_diagnostic(rows: Sequence[Mapping[str,Any]]) -> Dict[str,Any]:
    numeric=(
        "direction_score",
        "h1_score_contribution",
        "m15_score_contribution",
        "m5_score_contribution",
        "m1_score_contribution",
        "pullback_score_contribution",
        "rr_score_contribution",
        "volatility_score_contribution",
        "extension_score_contribution",
        "session_hours_score_contribution",
        "broken_barrier_score_contribution",
        "rr_raw",
    )
    all_d={k:[] for k in numeric}
    wrong={k:[] for k in numeric}
    right={k:[] for k in numeric}
    episodes=[]
    lw=wl=0
    max_reconstruction_error=0.0

    for r in rows:
        b,s=r["buy_shadow_b"],r["sell_shadow_b"]
        if not (b.get("status") in COMPARABLE and
                s.get("status") in COMPARABLE and
                b.get("realized_r") is not None and
                s.get("realized_r") is not None):
            continue

        bc,sc=r["buy_components"],r["sell_components"]
        max_reconstruction_error=max(
            max_reconstruction_error,
            abs(float(bc.get("score_reconstruction_error") or 0.0)),
            abs(float(sc.get("score_reconstruction_error") or 0.0)),
        )

        if r["bot_direction"]=="BUY":
            sel,opp=bc,sc
            sr,orr=float(b["realized_r"]),float(s["realized_r"])
        else:
            sel,opp=sc,bc
            sr,orr=float(s["realized_r"]),float(b["realized_r"])

        d={k:float(sel[k])-float(opp[k]) for k in numeric}
        for k,v in d.items():
            all_d[k].append(v)

        label="OTHER"
        if sr<0 and orr>0:
            lw+=1
            label="BOT_LOSS_OPPOSITE_WIN"
            for k,v in d.items():
                wrong[k].append(v)
        elif sr>0 and orr<0:
            wl+=1
            label="BOT_WIN_OPPOSITE_LOSS"
            for k,v in d.items():
                right[k].append(v)

        episodes.append({
            "candle_ts":r["candle_ts"],
            "bot_direction":r["bot_direction"],
            "bot_realized_r":sr,
            "opposite_realized_r":orr,
            "classification":label,
            "bot_components":sel,
            "opposite_components":opp,
        })

    avg=lambda d:{k:(mean(v) if v else None) for k,v in d.items()}

    return {
        "n":len(episodes),
        "bot_loss_opposite_win":lw,
        "bot_win_opposite_loss":wl,
        "mean_bot_minus_opposite":avg(all_d),
        "mean_delta_when_bot_loses_opposite_wins":avg(wrong),
        "mean_delta_when_bot_wins_opposite_loses":avg(right),
        "max_score_reconstruction_error":max_reconstruction_error,
        "score_reconstruction_valid":max_reconstruction_error < 1e-9,
        "episodes":episodes,
        "warning":"DESCRIPTIVE_ONLY_NO_DECISIONS_CHANGED",
    }



# ---------------------------------------------------------------------------
# V3.35.3 research-only directional evidence audits.
# These functions consume reconstructed counterfactual rows only. They do not
# participate in scan(), order execution, risk, safety, or production scoring.
# ---------------------------------------------------------------------------
# Primary E1 components must not be hard inclusion gates of the replay sample.
# M1 confirmation is excluded because REPLAY_ACTIONABLE requires it on the
# bot-selected side; its benchmark contribution is selection-conditioned.
E1_PRIMARY_COMPONENTS = (
    "h1_score_contribution", "m15_score_contribution",
    "m5_score_contribution", "pullback_score_contribution",
    "broken_barrier_score_contribution", "direction_score",
)
# RR remains secondary only: rr_raw/minimum_rr are endogenous to directional
# geometry and Test B explicitly relaxes minimum_rr.
E1_SECONDARY_COMPONENTS = ("rr_score_contribution",)
E1_COMPONENTS = E1_PRIMARY_COMPONENTS + E1_SECONDARY_COMPONENTS
E1_EXCLUDED_COMPONENTS = {
    "m1_score_contribution": "NON_IDENTIFIABLE_SELECTION_CONDITIONED_REPLAY_ACTIONABLE_REQUIRES_M1_CONFIRMATION",
    "extension_score_contribution": "EXCLUDED_REPLAY_QUALITY_GATE_NOT_DIRECTIONAL_DIFFERENTIAL",
}

def _rankdata(values: Sequence[float]) -> list[float]:
    order=sorted(range(len(values)),key=lambda i:float(values[i]))
    ranks=[0.0]*len(values); i=0
    while i<len(order):
        j=i+1
        while j<len(order) and float(values[order[j]])==float(values[order[i]]): j+=1
        rank=(i+j-1)/2.0+1.0
        for k in range(i,j): ranks[order[k]]=rank
        i=j
    return ranks

def _pearson(a: Sequence[float], b: Sequence[float]) -> float|None:
    if len(a)!=len(b) or len(a)<3:return None
    ma=mean(a); mb=mean(b)
    da=[x-ma for x in a]; db=[x-mb for x in b]
    den=sqrt(sum(x*x for x in da)*sum(y*y for y in db))
    return (sum(x*y for x,y in zip(da,db))/den) if den else None

def _spearman(a: Sequence[float], b: Sequence[float]) -> float|None:
    return _pearson(_rankdata(a),_rankdata(b)) if len(a)>=3 else None

def _exact_binomial_greater(k: int,n: int,p: float=.5) -> float|None:
    if n<=0:return None
    return min(1.0,sum(comb(n,i)*(p**i)*((1-p)**(n-i)) for i in range(k,n+1)))

def _bootstrap_stat_ci(values: Sequence[float], stat, *, samples: int=5000, seed: int=3354, level: float=.90):
    vals=list(values)
    if not vals:return [None,None]
    rng=Random(seed); n=len(vals); sims=[]
    for _ in range(max(1,int(samples))):
        sample=[vals[rng.randrange(n)] for _ in range(n)]
        sims.append(float(stat(sample)))
    alpha=(1-level)/2
    return [_quantile(sims,alpha),_quantile(sims,1-alpha)]

def _bh_qvalues(items: Sequence[tuple[str,float]]) -> Dict[str,float]:
    valid=sorted(((k,float(p)) for k,p in items if p is not None),key=lambda x:x[1])
    m=len(valid); out={}; running=1.0
    for rank in range(m,0,-1):
        key,p=valid[rank-1]; running=min(running,p*m/rank); out[key]=min(1.0,running)
    return out


def _e1_evidence_sample_class(n: int) -> str:
    """Pre-registered E1 evidence-size classification; never data-tuned."""
    n=int(n)
    if n < 15:
        return "UNDERPOWERED"
    if n < 30:
        return "WEAK_LIMITED_EVIDENCE"
    return "USABLE"

def population_selection_audit(rows: Sequence[Mapping[str,Any]], *, shadow: bool=True) -> Dict[str,Any]:
    """Describe exactly which outcome pairs form the analytical population."""
    bk,sk=("buy_shadow_b","sell_shadow_b") if shadow else ("buy","sell")
    combos={}; exclusion={}; comparable=0
    for r in rows:
        b,s=r[bk],r[sk]; bs=str(b.get("status") or "PENDING"); ss=str(s.get("status") or "PENDING")
        key=f"{bs}/{ss}"; combos[key]=combos.get(key,0)+1
        bc=bs in COMPARABLE and b.get("realized_r") is not None
        sc=ss in COMPARABLE and s.get("realized_r") is not None
        if bc and sc: comparable+=1; continue
        reasons=[]
        for side,x,ok in (("BUY",b,bc),("SELL",s,sc)):
            if ok: continue
            st=str(x.get("status") or "PENDING"); note=str(x.get("note") or "")
            reasons.append(f"{side}:{st}"+(f":{note}" if note else ""))
        rk=" | ".join(reasons) or "UNKNOWN"; exclusion[rk]=exclusion.get(rk,0)+1
    return {"total_opportunities":len(rows),"both_sides_comparable":comparable,
            "excluded_from_paired":len(rows)-comparable,"status_pair_counts":dict(sorted(combos.items())),
            "exclusion_reasons":dict(sorted(exclusion.items(),key=lambda kv:(-kv[1],kv[0]))),
            "population":"TEST_B_SHADOW" if shadow else "FROZEN_GEOMETRY",
            "warning":"DESCRIPTIVE_SELECTION_AUDIT_ONLY"}

def differential_evidence_association_audit(rows: Sequence[Mapping[str,Any]], *,
        bootstrap_samples: int=5000, seed: int=3355) -> Dict[str,Any]:
    """E1: association of BUY-SELL evidence deltas with BUY_R-SELL_R. Diagnostic only."""
    data={k:[] for k in E1_COMPONENTS}; session=[]
    for r in rows:
        b,s=r["buy_shadow_b"],r["sell_shadow_b"]
        if not (b.get("status") in COMPARABLE and s.get("status") in COMPARABLE and
                b.get("realized_r") is not None and s.get("realized_r") is not None): continue
        diff=float(b["realized_r"])-float(s["realized_r"]); bc,sc=r["buy_components"],r["sell_components"]
        for k in E1_COMPONENTS: data[k].append((float(bc[k])-float(sc[k]),diff))
        sd=str(bc.get("session_direction") or "NEUTRAL").upper()
        session.append(((1.0 if sd=="BUY" else -1.0 if sd=="SELL" else 0.0),diff))
    tests=[]; report={}
    for idx,(name,pairs) in enumerate(list(data.items())+[("session_direction",session)]):
        usable=[(d,y) for d,y in pairs if d!=0 and y!=0]
        concord=sum(1 for d,y in usable if (d>0)==(y>0)); n=len(usable); rate=concord/n if n else None
        bp=_exact_binomial_greater(concord,n) if n else None
        all_nonzero=[(d,y) for d,y in pairs if not (d==0 and y==0)]
        rho=_spearman([x[0] for x in all_nonzero],[x[1] for x in all_nonzero]) if len(all_nonzero)>=3 else None
        # Bootstrap CI for concordance and rho; p-values remain descriptive/unadjusted until BH below.
        ci=_bootstrap_stat_ci([1.0 if (d>0)==(y>0) else 0.0 for d,y in usable],mean,
                              samples=bootstrap_samples,seed=seed+idx) if usable else [None,None]
        report[name]={"n_total":len(pairs),"n_effective":n,"concordance":rate,"concordance_ci90":ci,
                      "concordance_p_greater_0_5":bp,"spearman_rho":rho,
                      "evidence_sample_class":_e1_evidence_sample_class(n)}
        if bp is not None:tests.append((name+":concordance",bp))
    q=_bh_qvalues(tests)
    for name,v in report.items():
        v["concordance_fdr_q"]=q.get(name+":concordance")
        sample_class=v["evidence_sample_class"]
        if sample_class=="UNDERPOWERED":
            v["status"]="UNDERPOWERED"
        elif sample_class=="WEAK_LIMITED_EVIDENCE":
            v["status"]="WEAK_LIMITED_EVIDENCE"
        elif v["concordance_fdr_q"] is not None and v["concordance_fdr_q"]<.10 and (v["concordance"] or 0)>.5:
            v["status"]="EVIDENCE"
        else:
            v["status"]="NO_EVIDENCE"
    return {"name":"DIFFERENTIAL_EVIDENCE_ASSOCIATION_AUDIT","research_only":True,
            "dependent_variable":"BUY_realized_R_minus_SELL_realized_R","components":report,
            "primary_components":list(E1_PRIMARY_COMPONENTS),
            "secondary_components":list(E1_SECONDARY_COMPONENTS),
            "excluded_components":dict(E1_EXCLUDED_COMPONENTS),
            "multiple_testing":"Benjamini-Hochberg FDR q<0.10 on included E1 concordance tests",
            "warning":"ASSOCIATION_ONLY_SELECTION_CONDITIONED_NOT_CAUSAL"}

def score_margin_calibration_audit(rows: Sequence[Mapping[str,Any]]) -> Dict[str,Any]:
    """E3 diagnostic: does larger absolute legacy score margin imply more directional correctness?"""
    pts=[]
    for r in rows:
        b,s=r["buy_shadow_b"],r["sell_shadow_b"]
        if not (b.get("status") in COMPARABLE and s.get("status") in COMPARABLE and b.get("realized_r") is not None and s.get("realized_r") is not None):continue
        bs=float(r["buy_components"]["direction_score"]); ss=float(r["sell_components"]["direction_score"]); margin=abs(bs-ss)
        selected="BUY" if bs>=ss else "SELL"; diff=float(b["realized_r"])-float(s["realized_r"])
        correct=(diff>0 and selected=="BUY") or (diff<0 and selected=="SELL")
        if diff!=0:pts.append((margin,1 if correct else 0))
    if not pts:return {"n":0,"status":"NO_DATA","research_only":True}
    xs=sorted(x for x,_ in pts); cuts=[_quantile(xs,q) for q in (.2,.4,.6,.8)]
    bins=[[] for _ in range(5)]
    for x,y in pts:
        i=sum(x>c for c in cuts); bins[i].append(y)
    rates=[(sum(v)/len(v) if v else None) for v in bins]
    observed=[x for x in rates if x is not None]
    mono=all(observed[i]<=observed[i+1] for i in range(len(observed)-1))
    return {"n":len(pts),"score":"absolute_BUY_SELL_legacy_score_margin",
            "bin_cutpoints":cuts,"bin_n":[len(v) for v in bins],"accuracy_by_bin":rates,
            "monotonic_non_decreasing":mono,"research_only":True,
            "warning":"DESCRIPTIVE_ONLY_REQUIRES_PROSPECTIVE_VALIDATION"}

def session_regime_shadow_audit(rows: Sequence[Mapping[str,Any]]) -> Dict[str,Any]:
    """E2 frozen session-regime shadow on reconstructed rows; no production action."""
    n=correct=neutral=0; vals=[]
    for r in rows:
        b,s=r["buy_shadow_b"],r["sell_shadow_b"]
        if not (b.get("status") in COMPARABLE and s.get("status") in COMPARABLE and b.get("realized_r") is not None and s.get("realized_r") is not None):continue
        sd=str(r["buy_components"].get("session_direction") or "NEUTRAL").upper()
        if sd not in ("BUY","SELL"):neutral+=1;continue
        diff=float(b["realized_r"])-float(s["realized_r"]);
        if diff==0:continue
        n+=1; hit=(diff>0 and sd=="BUY") or (diff<0 and sd=="SELL"); correct+=int(hit)
        vals.append(float(b["realized_r"]) if sd=="BUY" else float(s["realized_r"]))
    return {"n_non_neutral":n,"neutral_or_abstain":neutral,"accuracy":correct/n if n else None,
            "expectancy_r":mean(vals) if vals else None,"research_only":True,
            "classification":"DIAGNOSTIC_ONLY_EXISTING_DATA_REQUIRES_NEW_PROSPECTIVE_DATA",
            "warning":"FROZEN_SESSION_SIGNAL_DOES_NOT_CHANGE_LIVE_DIRECTION"}

def reconstruct_opportunities(server: Any, candles_by_tf: Mapping[str,Sequence[Mapping[str,Any]]],
                              instrument: str, opportunity_rows: Sequence[Mapping[str,Any]],
                              variant: ReplayVariant, *, horizon_bars: int,
                              execution: HistoricalExecutionConfig) -> list[Dict[str,Any]]:
    store=CandleStore(candles_by_tf); out=[]
    for src in opportunity_rows:
        ts=_dt(src["candle_ts"]); decision_time=ts+timedelta(minutes=1)
        h1=store.history("H1",decision_time,140);m15=store.history("M15",decision_time,140)
        m5=store.history("M5",decision_time,130);m1=store.history("M1",decision_time,220)
        if min(len(h1),len(m15),len(m5),len(m1))<55:
            continue
        buy=server._direction_hypothesis(h1,m15,m5,m1,instrument,"BUY")
        sell=server._direction_hypothesis(h1,m15,m5,m1,instrument,"SELL")
        snap=replay_snapshot(server,h1,m15,m5,m1,instrument,variant,hypotheses=(buy,sell))
        # Guard against benchmark/code drift: opportunity direction must remain frozen.
        bot_direction=str(src.get("signal") or "").upper()
        future=store.future_m1_after(ts,horizon_bars+max(0,int(execution.latency_bars))+1)
        out.append({"candle_ts":src["candle_ts"],"bot_direction":bot_direction,
                    "reconstructed_direction":snap.get("signal"),
                    "direction_drift":snap.get("signal")!=bot_direction,
                    "buy":_resolve_side(buy,"BUY",instrument,future,horizon_bars,execution),
                    "sell":_resolve_side(sell,"SELL",instrument,future,horizon_bars,execution),
                    "buy_shadow_b":_resolve_side_shadow(buy,"BUY",instrument,future,horizon_bars,execution),
                    "sell_shadow_b":_resolve_side_shadow(sell,"SELL",instrument,future,horizon_bars,execution),
                    "buy_components":_direction_component_snapshot(server,buy,m5,"BUY"),
                    "sell_components":_direction_component_snapshot(server,sell,m5,"SELL")})
    return out

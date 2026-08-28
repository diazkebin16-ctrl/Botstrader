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


def _direction_component_snapshot(hyp: Mapping[str,Any]) -> Dict[str,Any]:
    """Research-only snapshot of frozen evidence; no decisions change."""
    m=hyp.get("metrics") or {}; f=hyp.get("filters") or {}
    keys=("h1_gap_atr","h1_slope_atr","m15_gap_atr","m15_slope_atr","m5_momentum","m1_momentum","extension_atr","volatility_ratio")
    out={"direction_score":float(hyp.get("direction_score") or 0.0),"rr_raw":float(hyp.get("rr_raw") or 0.0),"room_to_barrier_r":hyp.get("room_to_barrier_r")}
    out.update({k:float(m.get(k) or 0.0) for k in keys})
    out.update({k:bool(f.get(k)) for k in ("h1_context","m15_context","m5_structure","second_pullback","m1_confirmation","minimum_rr","barrier_room_ok")})
    return out

def evaluate_component_diagnostic(rows: Sequence[Mapping[str,Any]]) -> Dict[str,Any]:
    numeric=("direction_score","h1_gap_atr","h1_slope_atr","m15_gap_atr","m15_slope_atr","m5_momentum","m1_momentum","rr_raw","extension_atr","volatility_ratio")
    all_d={k:[] for k in numeric}; wrong={k:[] for k in numeric}; right={k:[] for k in numeric}; episodes=[]; lw=wl=0
    for r in rows:
        b,s=r["buy_shadow_b"],r["sell_shadow_b"]
        if not (b.get("status") in COMPARABLE and s.get("status") in COMPARABLE and b.get("realized_r") is not None and s.get("realized_r") is not None): continue
        bc,sc=r["buy_components"],r["sell_components"]
        if r["bot_direction"]=="BUY": sel,opp=bc,sc; sr,orr=float(b["realized_r"]),float(s["realized_r"])
        else: sel,opp=sc,bc; sr,orr=float(s["realized_r"]),float(b["realized_r"])
        d={k:float(sel[k])-float(opp[k]) for k in numeric}
        for k,v in d.items(): all_d[k].append(v)
        label="OTHER"
        if sr<0 and orr>0:
            lw+=1; label="BOT_LOSS_OPPOSITE_WIN"
            for k,v in d.items(): wrong[k].append(v)
        elif sr>0 and orr<0:
            wl+=1; label="BOT_WIN_OPPOSITE_LOSS"
            for k,v in d.items(): right[k].append(v)
        episodes.append({"candle_ts":r["candle_ts"],"bot_direction":r["bot_direction"],"bot_realized_r":sr,"opposite_realized_r":orr,"classification":label,"bot_components":sel,"opposite_components":opp})
    avg=lambda d:{k:(mean(v) if v else None) for k,v in d.items()}
    return {"n":len(episodes),"bot_loss_opposite_win":lw,"bot_win_opposite_loss":wl,"mean_bot_minus_opposite":avg(all_d),"mean_delta_when_bot_loses_opposite_wins":avg(wrong),"mean_delta_when_bot_wins_opposite_loses":avg(right),"episodes":episodes,"warning":"DESCRIPTIVE_ONLY_NO_DECISIONS_CHANGED"}

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
                    "buy_components":_direction_component_snapshot(buy),
                    "sell_components":_direction_component_snapshot(sell)})
    return out

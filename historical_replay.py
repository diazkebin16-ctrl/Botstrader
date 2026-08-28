"""Independent candle replay for Botstrader research (v3.35).

The module is intentionally isolated from live execution. It reconstructs each
strategy decision from candles that were fully closed at the decision time, then
uses future M1 candles *only* in the outcome resolver.

It supports two directional policies on the same market history:
- V331_BASELINE: pre-session-regime directional scoring used by v3.31.
- SESSION: current v3.32+ scoring, with a configurable session weight scale.

The replay is not a reconstruction of production ML/research/governance state.
"REPLAY_ACTIONABLE" means the deterministic strategy/safety/M1/timing gates pass;
confidence-dependent and mutable production gates are deliberately excluded.
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
import math

from research_evidence import collapse_market_episodes
from historical_execution import HistoricalExecutionConfig, resolve_executed_outcome
from replay_validation import ReplayValidationConfig, chronological_holdout, walk_forward_splits


BAR_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "H1": 3600}


def _dt(v: Any) -> datetime:
    if isinstance(v, datetime):
        d=v
    else:
        d=datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    if d.tzinfo is None:d=d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def normalize_candles(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    out=[]
    for r in rows:
        x=dict(r); x["t"]=_dt(x["t"])
        for k in ("o","h","l","c"):x[k]=float(x[k])
        for k in ("bid_o","bid_h","bid_l","bid_c","ask_o","ask_h","ask_l","ask_c"):
            if k in x and x[k] is not None:x[k]=float(x[k])
        x["v"]=int(x.get("v",0) or 0)
        out.append(x)
    out.sort(key=lambda x:x["t"])
    return out


class CandleStore:
    """Time-indexed candles with a strict completed-bar view."""
    def __init__(self, candles_by_tf: Mapping[str, Sequence[Mapping[str, Any]]]):
        self.data={tf:normalize_candles(rows) for tf,rows in candles_by_tf.items()}
        self.times={tf:[x["t"] for x in rows] for tf,rows in self.data.items()}

    def history(self, tf: str, decision_time: datetime, count: int) -> List[Dict[str, Any]]:
        """Return up to ``count`` candles fully complete at ``decision_time``.

        OANDA candle timestamps denote bar *start*. A bar is therefore visible only
        after timestamp + timeframe duration <= decision_time.
        """
        duration=timedelta(seconds=BAR_SECONDS[tf])
        cutoff=_dt(decision_time)-duration
        idx=bisect_right(self.times[tf],cutoff)
        return self.data[tf][max(0,idx-int(count)):idx]

    def future_m1_after(self, candle_start: datetime, count: int) -> List[Dict[str, Any]]:
        times=self.times["M1"]
        idx=bisect_right(times,_dt(candle_start))
        return self.data["M1"][idx:idx+int(count)]


@dataclass(frozen=True)
class ReplayVariant:
    name: str
    mode: str = "SESSION"  # SESSION or V331_BASELINE
    session_weight_scale: float = 1.0


@dataclass(frozen=True)
class ReplayConfig:
    h1_history: int = 140
    m15_history: int = 140
    m5_history: int = 130
    m1_history: int = 220
    horizon_bars: int = 180
    episode_gap_minutes: int = 15
    save_m1_rejection_shadow: bool = False
    execution: HistoricalExecutionConfig = HistoricalExecutionConfig()
    validation: ReplayValidationConfig = ReplayValidationConfig()


def _opposes(metrics: Mapping[str, Any], sig: str) -> Tuple[bool,bool]:
    sign=1.0 if sig=="BUY" else -1.0
    hg=float(metrics.get("h1_gap_atr",0) or 0); hs=float(metrics.get("h1_slope_atr",0) or 0)
    mg=float(metrics.get("m15_gap_atr",0) or 0); ms=float(metrics.get("m15_slope_atr",0) or 0)
    return (-sign*hg>.12 and -sign*hs>.05, -sign*mg>.15 and -sign*ms>.07)


def _common_components(server: Any, hyp: Mapping[str, Any], m5: Sequence[Mapping[str,Any]], sig: str) -> Dict[str,Any]:
    f=hyp["filters"]; m=hyp["metrics"]
    sign=1.0 if sig=="BUY" else -1.0
    h1_opp,m15_opp=_opposes(m,sig)
    m5mom=sign*float(m.get("m5_momentum",0) or 0)>0
    m1mom=sign*float(m.get("m1_momentum",0) or 0)>0
    e5=server.ema([x["c"] for x in m5],20)
    pc,pr=server.pullbacks(m5,e5,sig)
    return {"h1_support":bool(f.get("h1_context")),"m15_support":bool(f.get("m15_context")),
            "h1_opposes":h1_opp,"m15_opposes":m15_opp,
            "m5_structure":bool(f.get("m5_structure")),"m5_momentum":m5mom,
            "confirm":bool(f.get("m1_confirmation")),"m1_momentum":m1mom,
            "second":bool(f.get("second_pullback")),"pc":pc,"pr":pr,
            "rr_raw":float(hyp.get("rr_raw",0) or 0),
            "vol":float(m.get("volatility_ratio",0) or 0),"ext":float(m.get("extension_atr",0) or 0),
            "session_ok":bool((m.get("session") or {}).get("ok")),
            "broken":len((hyp.get("structure_context") or {}).get("broken_levels",[]))}


def _legacy_v331_score(server: Any, hyp: Mapping[str,Any], m5: Sequence[Mapping[str,Any]], sig: str) -> Tuple[float,bool,bool]:
    x=_common_components(server,hyp,m5,sig)
    s=0.0
    s += 16 if x["h1_support"] else (-10 if x["h1_opposes"] else 3)
    s += 20 if x["m15_support"] else (-12 if x["m15_opposes"] else 4)
    s += 18 if x["m5_structure"] else (7 if x["m5_momentum"] else 0)
    s += 16 if x["confirm"] else (6 if x["m1_momentum"] else 0)
    s += 8 if x["second"] else (4 if x["pc"]>=1 and x["pr"] else 0)
    s += 8 if x["rr_raw"]>=2 else (6 if x["rr_raw"]>=server.MIN_RR else 0)
    s += 5 if .65<=x["vol"]<=2 else 0
    s += 5 if x["ext"]<=1.20 else (2 if x["ext"]<=1.60 else 0)
    s += 4 if x["session_ok"] else 0
    s += min(6,2*x["broken"])
    s=float(server.clamp(s,0,100))
    countertrend=x["h1_opposes"] and x["m15_opposes"]
    transition=(x["h1_opposes"] or x["m15_opposes"]) and (x["m5_structure"] or x["confirm"])
    return s,countertrend,transition


def _session_score(server: Any, hyp: Mapping[str,Any], m5: Sequence[Mapping[str,Any]], sig: str, scale: float) -> Tuple[float,bool,bool]:
    x=_common_components(server,hyp,m5,sig); m=hyp["metrics"]
    intraday=m.get("session_regime") or {}
    sd=intraday.get("direction","NEUTRAL"); strength=float(intraday.get("strength",0) or 0)
    support=sd==sig; opposes=sd in ("BUY","SELL") and sd!=sig
    s=0.0
    s += 8 if x["h1_support"] else (-5 if x["h1_opposes"] else 2)
    s += 14 if x["m15_support"] else (-9 if x["m15_opposes"] else 3)
    if support:s += float(scale)*(12+12*strength)
    elif opposes:s -= float(scale)*(8+10*strength)
    else:s += float(scale)*2
    s += 18 if x["m5_structure"] else (7 if x["m5_momentum"] else 0)
    s += 16 if x["confirm"] else (6 if x["m1_momentum"] else 0)
    s += 8 if x["second"] else (4 if x["pc"]>=1 and x["pr"] else 0)
    s += 8 if x["rr_raw"]>=2 else (6 if x["rr_raw"]>=server.MIN_RR else 0)
    s += 5 if .65<=x["vol"]<=2 else 0
    s += 5 if x["ext"]<=1.20 else (2 if x["ext"]<=1.60 else 0)
    s += 4 if x["session_ok"] else 0
    s += min(6,2*x["broken"])
    s=float(server.clamp(s,0,100))
    countertrend=opposes and x["m15_opposes"]
    transition=(opposes or x["m15_opposes"] or x["h1_opposes"]) and (x["m5_structure"] or x["confirm"])
    return s,countertrend,transition


def _choose(server: Any, buy: Mapping[str,Any], sell: Mapping[str,Any], m5: Sequence[Mapping[str,Any]], variant: ReplayVariant) -> Dict[str,Any]:
    scorer=_legacy_v331_score if variant.mode=="V331_BASELINE" else None
    if scorer:
        bs,bc,bt=scorer(server,buy,m5,"BUY"); ss,sc,st=scorer(server,sell,m5,"SELL")
    else:
        bs,bc,bt=_session_score(server,buy,m5,"BUY",variant.session_weight_scale)
        ss,sc,st=_session_score(server,sell,m5,"SELL",variant.session_weight_scale)
    edge=abs(bs-ss); chosen=buy if bs>=ss else sell
    chosen_sig="BUY" if bs>=ss else "SELL"
    chosen_counter=bc if chosen_sig=="BUY" else sc
    chosen_transition=bt if chosen_sig=="BUY" else st
    sig=chosen_sig
    if max(bs,ss)<server.DIRECTION_MIN_SCORE or edge<server.DIRECTION_MIN_EDGE:sig="WAIT"
    if sig!="WAIT" and chosen_counter and max(bs,ss)<server.COUNTERTREND_EXECUTION_MIN_SCORE:sig="WAIT"
    return {"signal":sig,"chosen_signal":chosen_sig,"chosen":chosen,"buy_score":bs,"sell_score":ss,
            "direction_edge":edge,"countertrend":chosen_counter,"transition":chosen_transition}


def _replay_gate(server: Any, row: Mapping[str,Any]) -> Tuple[bool,str]:
    if row["signal"]=="WAIT":return False,"WAIT_DIRECTION"
    failed=[k for k,v in (row.get("safety_checks") or {}).items() if not v]
    if failed:return False,"SAFETY:"+",".join(failed)
    filters=row.get("filters") or {}
    if getattr(server,"M1_CONFIRMATION_REQUIRED",True) and not bool(filters.get("m1_confirmation")):
        return False,"QUALITY:M1_CONFIRMATION"
    ext=float((row.get("features") or {}).get("extension_atr",0) or 0)
    if getattr(server,"ENTRY_TIMING_ENABLED",True) and ext>float(getattr(server,"MAX_ENTRY_EXTENSION_ATR",1.5)):
        return False,"QUALITY:EXTENSION"
    return True,"REPLAY_ACTIONABLE"


def replay_snapshot(server: Any, h1,m15,m5,m1,inst: str,variant: ReplayVariant, *, hypotheses=None) -> Dict[str,Any]:
    if hypotheses is None:
        buy=server._direction_hypothesis(h1,m15,m5,m1,inst,"BUY")
        sell=server._direction_hypothesis(h1,m15,m5,m1,inst,"SELL")
    else:
        buy,sell=hypotheses
    sel=_choose(server,buy,sell,m5,variant); chosen=sel["chosen"]
    sig=sel["signal"]
    safety=dict(chosen["safety_checks"]); safety["valid_direction"]=sig in ("BUY","SELL")
    mt=chosen["metrics"]; sess=mt.get("session") or {}; intraday=mt.get("session_regime") or {}
    filters=dict(chosen["filters"]); filters["direction_edge_ok"]=sel["direction_edge"]>=server.DIRECTION_MIN_EDGE
    filters["countertrend_strength_ok"]=not sel["countertrend"] or max(sel["buy_score"],sel["sell_score"])>=server.COUNTERTREND_EXECUTION_MIN_SCORE
    features={"rr_raw":float(chosen["rr_raw"]),"room_to_barrier_r":chosen.get("room_to_barrier_r"),
              "extension_atr":float(mt.get("extension_atr",0) or 0),"volatility_ratio":float(mt.get("volatility_ratio",0) or 0),
              "m1_confirm":1 if mt.get("m1_confirm") else 0,"m1_shadow_confirm":1 if mt.get("m1_shadow_confirm") else 0,
              "buy_score":sel["buy_score"],"sell_score":sel["sell_score"],"direction_edge":sel["direction_edge"],
              "h1_gap_atr":float(mt.get("h1_gap_atr",0) or 0),"h1_slope_atr":float(mt.get("h1_slope_atr",0) or 0),
              "m15_gap_atr":float(mt.get("m15_gap_atr",0) or 0),"m15_slope_atr":float(mt.get("m15_slope_atr",0) or 0),
              "session_direction":intraday.get("direction","NEUTRAL"),"session_strength":float(intraday.get("strength",0) or 0),
              "session_displacement_atr":float(intraday.get("displacement_atr",0) or 0),"session_momentum_atr":float(intraday.get("momentum_atr",0) or 0),
              "session_name":intraday.get("session"),"session_ok":1 if sess.get("ok") else 0}
    row={"variant":variant.name,"instrument":inst,"signal":sig,"chosen_signal":sel["chosen_signal"],
         "entry":float(chosen["entry"]),"stop":float(chosen["stop"]),"target":float(chosen["target"]),
         "rr":float(chosen["rr"]),"rr_raw":float(chosen["rr_raw"]),"risk":float(chosen["risk"]),
         "filters":filters,"safety_checks":safety,"features":features,"barrier_class":chosen.get("barrier_class"),
         "candle_ts":m1[-1]["t"].isoformat(),"countertrend":sel["countertrend"],"transition":sel["transition"]}
    actionable,reason=_replay_gate(server,row);row["actionable"]=actionable;row["decision_reason"]=reason
    return row


def _metrics(rows: Sequence[Mapping[str,Any]]) -> Dict[str,Any]:
    statuses={k:0 for k in ("WIN","LOSS","TIMEOUT","AMBIGUOUS","INVALID","PENDING","DATA_INSUFFICIENT","DATA_INTEGRITY_ERROR","ENTRY_INVALIDATED")}
    vals=[]
    for r in rows:
        st=str(r.get("outcome_status") or "PENDING");statuses[st]=statuses.get(st,0)+1
        if r.get("realized_r") is not None:vals.append(float(r["realized_r"]))
    gp=sum(x for x in vals if x>0);gl=abs(sum(x for x in vals if x<0));curve=peak=dd=0.0
    for x in vals:curve+=x;peak=max(peak,curve);dd=max(dd,peak-curve)
    mean=sum(vals)/len(vals) if vals else None
    return {"episodes":len(rows),"resolved":len(vals),"resolved_rate":len(vals)/len(rows) if rows else None,
            "wins":sum(x>0 for x in vals),"losses":sum(x<0 for x in vals),
            "win_rate":sum(x>0 for x in vals)/len(vals) if vals else None,"expectancy_r":mean,
            "profit_factor":gp/gl if gl else (999.0 if gp else None),"net_r":sum(vals),"max_drawdown_r":dd,
            "statuses":statuses}


def replay_history(server: Any, candles_by_tf: Mapping[str,Sequence[Mapping[str,Any]]], inst: str,
                   start: datetime, end: datetime, variants: Sequence[ReplayVariant], config: ReplayConfig=ReplayConfig()) -> Dict[str,Any]:
    store=CandleStore(candles_by_tf); start=_dt(start);end=_dt(end)
    raw={v.name:[] for v in variants}; rejection={v.name:{} for v in variants}
    m1_rejected={v.name:[] for v in variants}
    m1_all=store.data["M1"]
    for bar in m1_all:
        ts=bar["t"]
        if ts<start or ts>end:continue
        decision_time=ts+timedelta(minutes=1)
        h1=store.history("H1",decision_time,config.h1_history);m15=store.history("M15",decision_time,config.m15_history)
        m5=store.history("M5",decision_time,config.m5_history);m1=store.history("M1",decision_time,config.m1_history)
        if min(len(h1),len(m15),len(m5),len(m1))<55:continue
        # Technical hypotheses are invariant across research variants; compute them once.
        hypotheses=(server._direction_hypothesis(h1,m15,m5,m1,inst,"BUY"),
                    server._direction_hypothesis(h1,m15,m5,m1,inst,"SELL"))
        for v in variants:
            row=replay_snapshot(server,h1,m15,m5,m1,inst,v,hypotheses=hypotheses)
            if not row["actionable"]:
                d=rejection[v.name];d[row["decision_reason"]]=d.get(row["decision_reason"],0)+1
                if config.save_m1_rejection_shadow and row["decision_reason"]=="QUALITY:M1_CONFIRMATION" and row.get("signal") in ("BUY","SELL"):
                    m1_rejected[v.name].append(row)
            raw[v.name].append(row)
    reports={}
    for v in variants:
        actionable=[r for r in raw[v.name] if r.get("actionable") and r.get("signal") in ("BUY","SELL")]
        episodes=collapse_market_episodes(actionable,gap_minutes=config.episode_gap_minutes,
                                         timestamp_key="candle_ts",instrument_key="instrument",direction_key="signal")
        resolved=[]
        m1_shadow_resolved=[]
        if config.save_m1_rejection_shadow:
            m1_shadow_episodes=collapse_market_episodes(
                m1_rejected[v.name],
                gap_minutes=config.episode_gap_minutes,
                timestamp_key="candle_ts",
                instrument_key="instrument",
                direction_key="signal"
            )
            for r in m1_shadow_episodes:
                payload={"candle_ts":r["candle_ts"],"direction":r["signal"],"entry":r["entry"],"stop":r["stop"],"target":r["target"],"instrument":inst}
                future=store.future_m1_after(_dt(r["candle_ts"]), config.horizon_bars + max(0, int(config.execution.latency_bars)) + 1)
                out=resolve_executed_outcome(payload,future,horizon_bars=config.horizon_bars,config=config.execution)
                z=dict(r)
                if out:
                    z.update({
                        "outcome_status":out["status"],
                        "label":out.get("label"),
                        "mfe_r":out.get("mfe_r"),
                        "mae_r":out.get("mae_r"),
                        "entry_ts":out.get("entry_ts"),
                        "exit_ts":out.get("exit_ts"),
                        "entry_fill":out.get("entry_fill"),
                        "exit_fill":out.get("exit_fill"),
                        "entry_spread_pips":out.get("entry_spread_pips"),
                        "entry_slippage_pips":out.get("entry_slippage_pips"),
                        "exit_slippage_pips":out.get("exit_slippage_pips"),
                        "execution_note":out.get("note"),
                        "realized_r":out.get("realized_r")
                    })
                else:
                    z.update({"outcome_status":"PENDING","realized_r":None})
                m1_shadow_resolved.append(z)

        for r in episodes:
            payload={"candle_ts":r["candle_ts"],"direction":r["signal"],"entry":r["entry"],"stop":r["stop"],"target":r["target"],"instrument":inst}
            future=store.future_m1_after(_dt(r["candle_ts"]), config.horizon_bars + max(0, int(config.execution.latency_bars)) + 1)
            out=resolve_executed_outcome(payload,future,horizon_bars=config.horizon_bars,config=config.execution)
            z=dict(r)
            if out:
                z.update({"outcome_status":out["status"],"label":out.get("label"),"mfe_r":out.get("mfe_r"),"mae_r":out.get("mae_r"),
                          "entry_ts":out.get("entry_ts"),"exit_ts":out.get("exit_ts"),"entry_fill":out.get("entry_fill"),
                          "exit_fill":out.get("exit_fill"),"entry_spread_pips":out.get("entry_spread_pips"),
                          "entry_slippage_pips":out.get("entry_slippage_pips"),"exit_slippage_pips":out.get("exit_slippage_pips"),
                          "execution_note":out.get("note"),"realized_r":out.get("realized_r")})
            else:z.update({"outcome_status":"PENDING","realized_r":None})
            resolved.append(z)

        holdout=chronological_holdout(resolved,horizon_bars=config.horizon_bars,config=config.validation)
        wf=walk_forward_splits(resolved,horizon_bars=config.horizon_bars,config=config.validation)
        holdout_report={k:_metrics(holdout[k]) for k in ("discovery","validation","test")}
        holdout_report.update({"status":holdout["status"],"purged":holdout["purged"],"embargoed":holdout["embargoed"],
                               "boundaries":holdout.get("boundaries",{})})
        wf_report=[]
        for i,fold in enumerate(wf,1):
            wf_report.append({"fold":i,"boundary":fold["boundary"],"purged":fold["purged"],"embargoed":fold["embargoed"],
                              "train_metrics":_metrics(fold["train"]),"test_metrics":_metrics(fold["test"])})
        reports[v.name]={"raw_snapshots":len(raw[v.name]),"actionable_snapshots":len(actionable),"independent_episodes":len(episodes),
                         "rejections":rejection[v.name],"metrics":_metrics(resolved),"holdout":holdout_report,
                         "walk_forward":wf_report,"episodes":resolved,
                         "m1_rejection_shadow":{
                             "enabled":bool(config.save_m1_rejection_shadow),
                             "episodes":m1_shadow_resolved,
                             "metrics":_metrics(m1_shadow_resolved)
                         }}
    return {"instrument":inst,"start":start.isoformat(),"end":end.isoformat(),
            "methodology":{"no_lookahead_decision":True,"future_bars_only_for_outcome":True,
                           "episode_gap_minutes":config.episode_gap_minutes,
                           "execution_model":"HISTORICAL_BID_ASK_MARKET_FILL_WITH_EXPLICIT_ADVERSE_SLIPPAGE",
                           "entry_slippage_pips":config.execution.entry_slippage_pips,
                           "exit_slippage_pips":config.execution.exit_slippage_pips,
                           "latency_bars":config.execution.latency_bars,
                           "require_bid_ask":config.execution.require_bid_ask,
                           "validation":"CHRONOLOGICAL_HOLDOUT_PLUS_WALK_FORWARD_WITH_PURGING_AND_EMBARGO",
                           "embargo_minutes":config.validation.embargo_minutes,
                           "scope":"DETERMINISTIC_STRATEGY_CORE_NOT_PRODUCTION_ML_OR_MUTABLE_GATES",
                           "limitations":["M1 OHLC cannot reconstruct intrabar tick ordering; dual TP/SL touches remain AMBIGUOUS",
                                          "Historical candles do not provide order-book depth, so partial-fill probability is not inferred"]},"variants":reports}

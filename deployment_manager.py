
from __future__ import annotations
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import math

ALLOCATION_STEPS=(0.05,0.10,0.25,0.50)

def next_allocation(current: float):
    for x in ALLOCATION_STEPS:
        if x>current+1e-12:return float(x)
    return None

def stage_for_allocation(x: float) -> str:
    return "CANARY_LIVE" if x<=0.10+1e-12 else "LIMITED_PRODUCTION"

def r_metrics(values: List[float]) -> Dict[str,Any]:
    vals=[float(x) for x in values if x is not None and math.isfinite(float(x))]
    wins=[x for x in vals if x>0];loss=[x for x in vals if x<0]
    gp=sum(wins);gl=abs(sum(loss));pf=gp/gl if gl else (999.0 if gp else None)
    curve=peak=dd=0.0;cw=cl=mw=ml=0
    for x in vals:
        curve+=x;peak=max(peak,curve);dd=max(dd,peak-curve)
        if x>0:cw+=1;cl=0;mw=max(mw,cw)
        elif x<0:cl+=1;cw=0;ml=max(ml,cl)
    return {"trades":len(vals),"net_r":sum(vals),"win_rate":len(wins)/len(vals) if vals else None,
            "expectancy_r":sum(vals)/len(vals) if vals else None,"profit_factor":pf,
            "max_drawdown_r":dd,"max_consecutive_wins":mw,"max_consecutive_losses":ml}

def promotion_gate(live: Dict[str,Any], min_trades:int,min_days:float,min_regimes:int,
                   cooldown_ok:bool,risk_ok:bool,max_increase_ok:bool) -> Dict[str,Any]:
    reasons=[]
    if live.get("trades",0)<min_trades:reasons.append("NOT_ENOUGH_LIVE_TRADES")
    if live.get("days",0)<min_days:reasons.append("NOT_ENOUGH_LIVE_DAYS")
    if live.get("regimes",0)<min_regimes:reasons.append("NOT_ENOUGH_LIVE_REGIMES")
    if not cooldown_ok:reasons.append("PROMOTION_COOLDOWN")
    if not risk_ok:reasons.append("RISK_ENGINE_VETO")
    if not max_increase_ok:reasons.append("MAX_EXPOSURE_INCREASE_EXCEEDED")
    if live.get("expectancy_r") is None or live["expectancy_r"]<=0:reasons.append("EXPECTANCY_NOT_POSITIVE")
    if live.get("profit_factor") is None or live["profit_factor"]<1.10:reasons.append("PROFIT_FACTOR_TOO_LOW")
    if live.get("stability",0)<0.50:reasons.append("LOW_STABILITY")
    if live.get("operational_errors",0)>0:reasons.append("OPERATIONAL_ERRORS")
    if live.get("divergence_status") not in (None,"CONSISTENT"):reasons.append("LIVE_DIVERGENCE")
    return {"action":"PROMOTE" if not reasons else "HOLD_CURRENT_LEVEL","reasons":reasons}

def fail_safe(*,stage,resume_required,system_kill,candidate_kill,regime_ok,director_ok,
              risk_ok,data_ok,broker_ok,new_trades_enabled):
    reasons=[]
    if stage not in ("CANARY_LIVE","LIMITED_PRODUCTION"):reasons.append("DEPLOYMENT_NOT_LIVE")
    if resume_required:reasons.append("RESTART_HEALTH_CHECK_REQUIRED")
    if system_kill:reasons.append("GLOBAL_KILL_SWITCH")
    if candidate_kill:reasons.append("CANDIDATE_KILL_SWITCH")
    if not regime_ok:reasons.append("REGIME_COMPONENT_UNAVAILABLE_OR_DISALLOWED")
    if not director_ok:reasons.append("DIRECTOR_UNAVAILABLE_OR_PAUSE")
    if not risk_ok:reasons.append("RISK_ENGINE_BLOCK")
    if not data_ok:reasons.append("DATA_HEALTH_FAILURE")
    if not broker_ok:reasons.append("BROKER_HEALTH_FAILURE")
    if not new_trades_enabled:reasons.append("NEW_TRADES_DISABLED")
    return {"allow":not reasons,"reasons":reasons}

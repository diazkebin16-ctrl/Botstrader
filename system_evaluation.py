
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import sqlite3, json, math, statistics, uuid, itertools

STATUSES=("EXCELLENT","HEALTHY","WATCH","DEGRADING","HIGH_RISK","CRITICAL","PAUSED","UNDER_REVIEW")
DEGRADATION_TYPES=(
    "STRATEGY_DEGRADATION","MARKET_REGIME_SHIFT","EXECUTION_DEGRADATION",
    "INFRASTRUCTURE_DEGRADATION","RISK_DEGRADATION","MODEL_DEGRADATION",
    "DATA_QUALITY_DEGRADATION"
)

def now_iso(): return datetime.now(timezone.utc).isoformat()
def parse_ts(x):
    if not x:return None
    try:return datetime.fromisoformat(str(x).replace("Z","+00:00"))
    except Exception:return None
def f(x,default=None):
    try:
        y=float(x)
        return y if math.isfinite(y) else default
    except Exception:return default
def j(x): return json.dumps(x,separators=(",",":"),sort_keys=True,default=str)

def percentile(vals,p):
    vals=sorted(float(x) for x in vals if f(x) is not None)
    if not vals:return None
    if len(vals)==1:return vals[0]
    k=(len(vals)-1)*p
    a=math.floor(k);b=math.ceil(k)
    if a==b:return vals[a]
    return vals[a]+(vals[b]-vals[a])*(k-a)

def safe_div(a,b,default=0.0):
    try:return float(a)/float(b) if float(b)!=0 else default
    except Exception:return default

def trade_metrics(rows:List[Dict[str,Any]])->Dict[str,Any]:
    closed=[r for r in rows if r.get("net_result") is not None or r.get("realized_r") is not None]
    pnl=[f(r.get("net_result"),f(r.get("realized_pl"),0.0)) or 0.0 for r in closed]
    rr=[f(r.get("realized_r")) for r in closed if f(r.get("realized_r")) is not None]
    vals=rr if rr else pnl
    wins=[x for x in vals if x>0];losses=[x for x in vals if x<0]
    gross_profit=sum(wins);gross_loss=abs(sum(losses))
    expectation=statistics.mean(vals) if vals else None
    pf=(gross_profit/gross_loss) if gross_loss>1e-12 else (999.0 if gross_profit>0 else None)
    win_rate=(len(wins)/len(vals)) if vals else None
    avg_win=statistics.mean(wins) if wins else None
    avg_loss=statistics.mean(losses) if losses else None
    stdev=statistics.pstdev(vals) if len(vals)>=2 else None
    sharpe=(statistics.mean(vals)/stdev*math.sqrt(len(vals))) if stdev and stdev>1e-12 else None
    downside=[min(0.0,x) for x in vals]
    downside_dev=math.sqrt(sum(x*x for x in downside)/len(downside)) if downside else None
    sortino=(statistics.mean(vals)/downside_dev*math.sqrt(len(vals))) if downside_dev and downside_dev>1e-12 else None
    equity=0.0;peak=0.0;maxdd=0.0
    for x in pnl:
        equity+=x;peak=max(peak,equity);maxdd=max(maxdd,peak-equity)
    fees=sum(abs(f(r.get("fees_total"),0.0) or 0.0) for r in closed)
    financing=sum(f(r.get("financing"),0.0) or 0.0 for r in closed)
    gross=sum(f(r.get("gross_result"),f(r.get("net_result"),0.0)) or 0.0 for r in closed)
    slips=[abs(f(r.get("entry_slippage_pips"),0.0) or 0.0)+abs(f(r.get("exit_slippage_pips"),0.0) or 0.0) for r in closed]
    return {
        "sample_size":len(vals),"net_pnl":sum(pnl),"gross_pnl":gross,"expectancy":expectation,
        "profit_factor":pf,"win_rate":win_rate,"average_win":avg_win,"average_loss":avg_loss,
        "sharpe":sharpe,"sortino":sortino,"max_drawdown_absolute":maxdd,
        "fees":fees,"financing":financing,"avg_total_slippage_pips":statistics.mean(slips) if slips else None,
        "return_to_drawdown":safe_div(sum(pnl),maxdd,None) if maxdd>0 else None
    }

def confidence(sample:int,data_quality:float,effect:float=0.0)->Dict[str,Any]:
    sample_score=min(1.0,max(0.0,sample/100.0))
    quality=max(0.0,min(1.0,data_quality))
    effect_score=min(1.0,abs(effect)*2.0)
    score=.55*sample_score+.35*quality+.10*effect_score
    level="HIGH" if score>=.75 else "MEDIUM" if score>=.5 else "LOW"
    return {"confidence_level":level,"confidence_score":score,"sample_size":sample,
            "data_quality":quality,"statistical_significance":"SUPPORTED" if sample>=50 and score>=.6 else "LIMITED"}

def pearson(xs,ys):
    if len(xs)!=len(ys) or len(xs)<3:return None
    mx=statistics.mean(xs);my=statistics.mean(ys)
    dx=[x-mx for x in xs];dy=[y-my for y in ys]
    den=math.sqrt(sum(x*x for x in dx)*sum(y*y for y in dy))
    return sum(a*b for a,b in zip(dx,dy))/den if den>1e-12 else None

class SystemEvaluationEngine:
    def __init__(self,db_path:str,version:str,min_samples:int=20,report_period_hours:int=24,
                 score_weights:Optional[Dict[str,float]]=None,risk_drawdown_limit:float=0.10):
        self.db_path=db_path;self.version=version
        self.min_samples=max(5,int(min_samples));self.report_period_hours=max(1,int(report_period_hours))
        defaults={"trading":0.30,"risk":0.30,"operational":0.25,"stability":0.15}
        raw=score_weights or defaults
        total=sum(max(0.0,float(v)) for v in raw.values())
        if total<=0:
            raw=defaults;total=sum(defaults.values())
        self.score_weights={k:max(0.0,float(raw.get(k,0.0)))/total for k in ("trading","risk","operational","stability")}
        self.risk_drawdown_limit=max(0.001,float(risk_drawdown_limit))

    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30,isolation_level=None)
        c.row_factory=sqlite3.Row;c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA foreign_keys=ON")
        return c

    def ensure_schema(self):
        c=self.conn();c.executescript("""
        CREATE TABLE IF NOT EXISTS system_evaluations(
          evaluation_id TEXT PRIMARY KEY,generated_at TEXT NOT NULL,as_of_ts TEXT NOT NULL,
          engine_version TEXT NOT NULL,system_status TEXT NOT NULL,system_score REAL NOT NULL,
          trading_score REAL NOT NULL,risk_score REAL NOT NULL,operational_score REAL NOT NULL,
          stability_score REAL NOT NULL,confidence_level TEXT NOT NULL,confidence_score REAL NOT NULL,
          sample_size INTEGER NOT NULL,data_quality_score REAL NOT NULL,main_degradation_factor TEXT,
          biggest_risk_contributor TEXT,executive_summary_json TEXT NOT NULL,
          trading_json TEXT NOT NULL,risk_json TEXT NOT NULL,operational_json TEXT NOT NULL,
          stability_json TEXT NOT NULL,baseline_json TEXT NOT NULL,rolling_windows_json TEXT NOT NULL,
          attribution_json TEXT NOT NULL,director_json TEXT NOT NULL,risk_engine_json TEXT NOT NULL,
          adaptive_learning_json TEXT NOT NULL,model_reality_gap_json TEXT NOT NULL,
          diversification_json TEXT NOT NULL,regime_coverage_json TEXT NOT NULL,
          change_impact_json TEXT NOT NULL,incident_impact_json TEXT NOT NULL,
          degradation_json TEXT NOT NULL,recommendations_json TEXT NOT NULL,
          data_snapshot_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS system_evaluation_recommendations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,evaluation_id TEXT NOT NULL,recommendation TEXT NOT NULL,
          priority TEXT NOT NULL,reason TEXT NOT NULL,confidence REAL NOT NULL,
          evidence_json TEXT NOT NULL DEFAULT '{}',status TEXT NOT NULL DEFAULT 'OPEN',
          FOREIGN KEY(evaluation_id) REFERENCES system_evaluations(evaluation_id)
        );
        CREATE TABLE IF NOT EXISTS system_evaluation_attribution(
          id INTEGER PRIMARY KEY AUTOINCREMENT,evaluation_id TEXT NOT NULL,dimension TEXT NOT NULL,
          key TEXT NOT NULL,sample_size INTEGER NOT NULL,net_pnl REAL,expectancy REAL,
          profit_factor REAL,drawdown REAL,risk_consumption REAL,contribution_score REAL,
          details_json TEXT NOT NULL DEFAULT '{}',FOREIGN KEY(evaluation_id) REFERENCES system_evaluations(evaluation_id)
        );
        CREATE TRIGGER IF NOT EXISTS system_evaluations_no_update BEFORE UPDATE ON system_evaluations
        BEGIN SELECT RAISE(ABORT,'historical system evaluations are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS system_evaluations_no_delete BEFORE DELETE ON system_evaluations
        BEGIN SELECT RAISE(ABORT,'historical system evaluations are immutable'); END;
        CREATE INDEX IF NOT EXISTS idx_system_eval_ts ON system_evaluations(as_of_ts,generated_at);
        CREATE INDEX IF NOT EXISTS idx_system_eval_rec ON system_evaluation_recommendations(evaluation_id,priority);
        """);c.close()

    def _rows(self,table:str,where:str="",params=())->List[Dict[str,Any]]:
        c=self.conn()
        try:rows=[dict(x) for x in c.execute(f"SELECT * FROM {table} {where}",params).fetchall()]
        except sqlite3.OperationalError:rows=[]
        c.close();return rows

    def _trades(self,as_of:datetime)->List[Dict[str,Any]]:
        rows=self._rows("trade_memory","WHERE status='CLOSED' AND exit_ts IS NOT NULL AND exit_ts<=? ORDER BY exit_ts,id",(as_of.isoformat(),))
        return rows

    def _window(self,rows,days=None,n=None,as_of=None):
        if n:return rows[-n:]
        if days is None:return rows
        cutoff=(as_of-timedelta(days=days))
        return [r for r in rows if (parse_ts(r.get("exit_ts")) or datetime.min.replace(tzinfo=timezone.utc))>=cutoff]

    def _quality(self,trades,as_of):
        if not trades:return {"score":0.2,"missing_rate":1.0,"compromised_rate":0.0,"stale_events":0,"outliers":0}
        missing=0;comp=0;outliers=0
        nets=[f(x.get("net_result")) for x in trades if f(x.get("net_result")) is not None]
        med=statistics.median(nets) if nets else 0
        mad=statistics.median([abs(x-med) for x in nets]) if nets else 0
        for r in trades:
            if not r.get("strategy") or not r.get("symbol") or not r.get("entry_ts") or not r.get("exit_ts"):missing+=1
            if int(r.get("execution_quality_compromised") or 0):comp+=1
            v=f(r.get("net_result"))
            if v is not None and mad>0 and abs(v-med)>8*mad:outliers+=1
        cutoff=(as_of-timedelta(days=30)).isoformat()
        stale=self._rows("observability_alert_history",
            "WHERE ts>=? AND event_type IN ('MARKET_DATA_STALE','MARKET_DATA_UNRELIABLE','DATA_QUALITY_DEGRADATION')",(cutoff,))
        score=max(0.0,1.0-(missing/len(trades))*.35-(comp/len(trades))*.35-min(.2,len(stale)*.02)-min(.1,outliers/max(1,len(trades))))
        return {"score":score,"missing_rate":missing/len(trades),"compromised_rate":comp/len(trades),
                "stale_events":len(stale),"outliers":outliers,
                "feed_disagreement":"UNAVAILABLE_SINGLE_PRIMARY_MARKET_FEED"}

    def _attribution(self,trades,dimension):
        field={"strategy":"strategy","market_regime":"market_regime_entry","asset":"symbol","direction":"direction",
               "session":"entry_session","deployment_version":"deployment_version"}[dimension]
        groups={}
        for r in trades:groups.setdefault(str(r.get(field) or "UNKNOWN"),[]).append(r)
        total_pnl=sum((f(r.get("net_result"),0.0) or 0.0) for r in trades)
        out={}
        for key,rs in groups.items():
            m=trade_metrics(rs)
            neg=sum(abs(min(0.0,f(r.get("net_result"),0.0) or 0.0)) for r in rs)
            risk=sum(f(r.get("approved_risk"),0.0) or 0.0 for r in rs)
            contribution=(safe_div(m["net_pnl"],abs(total_pnl),0.0) if total_pnl else 0.0)
            out[key]={**m,"loss_contribution":neg,"risk_consumption":risk,
                      "profit_contribution_fraction":contribution}
        return out

    def _marginal(self,trades):
        overall=trade_metrics(trades);out={}
        strategies=sorted({r.get("strategy") for r in trades if r.get("strategy")})
        for st in strategies:
            without=[r for r in trades if r.get("strategy")!=st];wm=trade_metrics(without)
            own=trade_metrics([r for r in trades if r.get("strategy")==st])
            out[st]={
                "with_portfolio_net":overall["net_pnl"],"without_strategy_net":wm["net_pnl"],
                "marginal_net":overall["net_pnl"]-wm["net_pnl"],
                "portfolio_drawdown":overall["max_drawdown_absolute"],
                "without_strategy_drawdown":wm["max_drawdown_absolute"],
                "drawdown_added":overall["max_drawdown_absolute"]-wm["max_drawdown_absolute"],
                "strategy_net":own["net_pnl"],"strategy_samples":own["sample_size"],
                "estimated_from_realized_trade_history":True
            }
        return out

    def _diversification(self,trades):
        daily={}
        for r in trades:
            dt=parse_ts(r.get("exit_ts"))
            if not dt or not r.get("strategy"):continue
            day=dt.date().isoformat();daily.setdefault(r["strategy"],{}).setdefault(day,0.0)
            daily[r["strategy"]][day]+=f(r.get("realized_r"),f(r.get("net_result"),0.0)) or 0.0
        pairs=[]
        for a,b in itertools.combinations(sorted(daily),2):
            common=sorted(set(daily[a])&set(daily[b]))
            if len(common)<5:continue
            corr=pearson([daily[a][d] for d in common],[daily[b][d] for d in common])
            if corr is not None:pairs.append({"strategy_a":a,"strategy_b":b,"correlation":corr,"common_days":len(common)})
        hidden=[x for x in pairs if abs(x["correlation"])>=.8]
        return {"pairs":pairs,"hidden_concentration_pairs":hidden,
                "status":"HIDDEN_CONCENTRATION_RISK" if hidden else "NORMAL"}

    def _regime_coverage(self,trades):
        regimes={}
        for rg in sorted({r.get("market_regime_entry") for r in trades if r.get("market_regime_entry")}):
            by={}
            for st in sorted({r.get("strategy") for r in trades if r.get("strategy")}):
                rs=[r for r in trades if r.get("market_regime_entry")==rg and r.get("strategy")==st]
                if rs:by[st]=trade_metrics(rs)
            robust=[st for st,m in by.items() if m["sample_size"]>=self.min_samples and (m["expectancy"] or 0)>0 and (m["profit_factor"] or 0)>=1.1]
            regimes[rg]={"strategies":by,"robust_strategies":robust,"coverage":"GOOD" if robust else "GAP"}
        gaps=[k for k,v in regimes.items() if v["coverage"]=="GAP"]
        return {"regimes":regimes,"gaps":gaps,"status":"REGIME_COVERAGE_GAP" if gaps else "COVERED"}

    def _director(self,as_of):
        rows=self._rows("ai_strategy_director_decisions","WHERE ts<=? ORDER BY ts,id",(as_of.isoformat(),))
        outcomes={x["director_decision_id"]:x for x in self._rows("ai_strategy_director_outcomes",
                                                                  "WHERE resolved_ts IS NULL OR resolved_ts<=?",(as_of.isoformat(),))}
        resolved=[]
        changes=0;prev={}
        for d in rows:
            o=outcomes.get(d["id"])
            if o and o.get("resolved_label") is not None:
                state=d["recommended_state"];label=int(o["resolved_label"])
                correct=(state in ("ACTIVE","REDUCED") and label==1) or (state in ("PAUSED","DISABLED") and label==0)
                resolved.append((d,o,correct))
            key=d.get("setup_variant")
            if key in prev and prev[key]!=d.get("recommended_state"):changes+=1
            prev[key]=d.get("recommended_state")
        active=[x for x in resolved if x[0]["recommended_state"] in ("ACTIVE","REDUCED")]
        paused=[x for x in resolved if x[0]["recommended_state"] in ("PAUSED","DISABLED")]
        return {
            "resolved_decisions":len(resolved),
            "decision_accuracy":safe_div(sum(1 for x in resolved if x[2]),len(resolved),None) if resolved else None,
            "active_profitable_rate":safe_div(sum(1 for d,o,c in active if int(o["resolved_label"])==1),len(active),None) if active else None,
            "pause_loss_avoidance_rate":safe_div(sum(1 for d,o,c in paused if int(o["resolved_label"])==0),len(paused),None) if paused else None,
            "false_pause_rate":safe_div(sum(1 for d,o,c in paused if int(o["resolved_label"])==1),len(paused),None) if paused else None,
            "state_changes":changes,"change_rate":safe_div(changes,len(rows),0.0),
            "counterfactual_basis":"resolved shadow outcomes; hypothetical results are not real PnL",
            "reaction_lag_metric":"UNAVAILABLE_WITH_CURRENT_DIRECTOR_EVENT_SCHEMA"
        }

    def _risk_engine(self,as_of,trades):
        cutoff=(as_of-timedelta(days=30)).isoformat()
        rows=self._rows("adaptive_risk_decisions","WHERE ts>=? AND ts<=? ORDER BY ts,id",(cutoff,as_of.isoformat()))
        if not rows:return {"sample_size":0,"block_rate":None,"reduction_rate":None,"efficiency":"INSUFFICIENT_DATA"}
        blocks=sum(1 for x in rows if not int(x.get("allow_new_trades") or 0))
        reductions=sum(1 for x in rows if (f(x.get("risk_multiplier"),1.0) or 1.0)<.999)
        emergencies=sum(int(x.get("emergency_stop") or 0) for x in rows)
        hard=sum(int(x.get("hard_limit_triggered") or 0) for x in rows)
        block_rate=blocks/len(rows)
        # Compare resolved learning samples with blocked/executed signal stream as a shadow efficiency proxy.
        signals=self._rows("signals","WHERE ts>=? AND ts<=?",(cutoff,as_of.isoformat()))
        labels={x["signal_id"]:x for x in self._rows("learning_samples","WHERE resolved_ts IS NOT NULL AND resolved_ts<=?",(as_of.isoformat(),))}
        blocked_resolved=[(x,labels.get(x["id"])) for x in signals if int(x.get("blocked") or 0) and labels.get(x["id"])]
        avoided=sum(1 for s,l in blocked_resolved if int(l.get("label") or 0)==0)
        false_pos=sum(1 for s,l in blocked_resolved if int(l.get("label") or 0)==1)
        efficiency=(avoided/(avoided+false_pos)) if avoided+false_pos else None
        return {"sample_size":len(rows),"blocks":blocks,"block_rate":block_rate,"reductions":reductions,
                "reduction_rate":reductions/len(rows),"emergency_stops":emergencies,"hard_limit_hits":hard,
                "estimated_avoided_losses":avoided,"estimated_false_positive_blocks":false_pos,
                "block_precision_estimate":efficiency,
                "efficiency":"OVER_RESTRICTIVE" if block_rate>.5 and (efficiency is not None and efficiency<.55)
                             else "EFFECTIVE" if efficiency is not None and efficiency>=.65 else "MIXED"}

    def _adaptive(self,as_of):
        generated_rows=self._rows("candidate_strategies","WHERE generated_at<=?",(as_of.isoformat(),))
        candidate_ids={x["candidate_id"] for x in generated_rows}
        validations=self._rows("candidate_validation_runs",
                               "WHERE completed_ts IS NOT NULL AND completed_ts<=?",(as_of.isoformat(),))
        validated_ids={x["candidate_id"] for x in validations if x.get("final_status") not in ("FAILED","REJECTED","INSUFFICIENT_DATA")}
        paper_rows=self._rows("candidate_paper_trades","WHERE created_ts<=?",(as_of.isoformat(),))
        paper_ids={x["candidate_id"] for x in paper_rows}
        events=self._rows("deployment_events","WHERE ts<=? ORDER BY ts,id",(as_of.isoformat(),))
        latest_stage={}
        for e in events:
            if e.get("candidate_id"):latest_stage[e["candidate_id"]]=e.get("new_stage")
        canary_ids={cid for cid,st in latest_stage.items() if st in ("CANARY_LIVE","CANARY_PAUSED","LIMITED_PRODUCTION","FULL_PRODUCTION_ELIGIBLE","ROLLED_BACK","CANARY_REJECTED")}
        limited_ids={cid for cid,st in latest_stage.items() if st in ("LIMITED_PRODUCTION","FULL_PRODUCTION_ELIGIBLE")}
        production_ids={cid for cid,st in latest_stage.items() if st=="FULL_PRODUCTION_ELIGIBLE"}
        rejected_ids={x["candidate_id"] for x in validations if x.get("final_status") in ("FAILED","REJECTED")}
        rejected_ids|={cid for cid,st in latest_stage.items() if st in ("CANARY_REJECTED","ROLLED_BACK")}
        live_rows=self._rows("deployment_live_trades",
                             "WHERE opened_ts<=? AND realized_r IS NOT NULL AND closed_ts<=?",
                             (as_of.isoformat(),as_of.isoformat()))
        live_by={}
        for x in live_rows:live_by.setdefault(x["candidate_id"],[]).append(f(x.get("realized_r"),0.0) or 0.0)
        live_effectiveness={
            cid:{"trades":len(vals),"expectancy_r":statistics.mean(vals) if vals else None,
                 "net_r":sum(vals),"successful":len(vals)>=5 and statistics.mean(vals)>0}
            for cid,vals in live_by.items()
        }
        successful_live=[cid for cid,x in live_effectiveness.items() if x["successful"]]
        funnel={"GENERATED":len(candidate_ids),"VALIDATED":len(validated_ids),"PAPER":len(paper_ids),
                "CANARY":len(canary_ids),"LIMITED":len(limited_ids),"PRODUCTION":len(production_ids)}
        generated=funnel["GENERATED"]
        survival={k:safe_div(v,generated,0.0) if generated else None for k,v in funnel.items()}
        quality="INSUFFICIENT_DATA" if generated<5 else "VALIDATION_TOO_WEAK" if safe_div(funnel["PRODUCTION"],generated,0)>.7 else "CANDIDATE_QUALITY_LOW" if safe_div(funnel["VALIDATED"],generated,0)<.1 else "NORMAL"
        return {"funnel":funnel,"survival_rates":survival,"latest_stages":latest_stage,
                "rejected":len(rejected_ids),"live_effectiveness":live_effectiveness,
                "successful_live_candidates":successful_live,
                "assessment":quality,"as_of_reconstructed":True}

    def _model_gap(self,as_of):
        runs=self._rows("candidate_validation_runs","WHERE completed_ts IS NOT NULL AND completed_ts<=? ORDER BY completed_ts DESC",(as_of.isoformat(),))
        deployments={x["candidate_id"]:x for x in self._rows("deployment_registry","")}
        live=self._rows("deployment_live_trades",
                        "WHERE opened_ts<=? AND (closed_ts IS NULL OR closed_ts<=?)",
                        (as_of.isoformat(),as_of.isoformat()))
        by_live={}
        for x in live:
            if x.get("realized_r") is not None:by_live.setdefault(x["candidate_id"],[]).append(f(x["realized_r"],0.0) or 0.0)
        gaps=[]
        for r in runs[:100]:
            try:oos=json.loads(r.get("oos_results_json") or "{}");paper=json.loads(r.get("paper_results_json") or "{}")
            except Exception:continue
            expected=((oos.get("comparison") or {}).get("candidate") or oos.get("candidate") or {})
            pap=paper.get("candidate") or paper
            exp=f(expected.get("expectancy"));pp=f(pap.get("expectancy"))
            lv=by_live.get(r["candidate_id"],[]);le=statistics.mean(lv) if lv else None
            vals=[x for x in (exp,pp,le) if x is not None]
            gap=(max(vals)-min(vals)) if len(vals)>=2 else None
            gaps.append({"candidate_id":r["candidate_id"],"expected_expectancy":exp,"paper_expectancy":pp,
                         "live_expectancy":le,"gap":gap})
        material=[x for x in gaps if x["gap"] is not None and x["gap"]>=.5]
        return {"candidates":gaps,"material_gaps":material,"status":"MODEL_REALITY_GAP" if material else "NORMAL"}

    def _operational(self,as_of):
        cutoff=(as_of-timedelta(days=30)).isoformat()
        metrics=self._rows("observability_metrics","WHERE ts>=? AND ts<=?",(cutoff,as_of.isoformat()))
        alerts=self._rows("observability_alert_history","WHERE ts>=? AND ts<=?",(cutoff,as_of.isoformat()))
        rec=self._rows("recovery_incidents","WHERE started_ts>=? AND started_ts<=?",(cutoff,as_of.isoformat()))
        recons=self._rows("recovery_reconciliation_runs","WHERE started_ts>=? AND started_ts<=?",(cutoff,as_of.isoformat()))
        lats=[f(x.get("broker_latency_ms")) for x in metrics if f(x.get("broker_latency_ms")) is not None]
        total=[f(x.get("processing_time_ms")) for x in metrics if f(x.get("processing_time_ms")) is not None]
        stale=sum(1 for x in alerts if x.get("event_type") in ("MARKET_DATA_STALE","MARKET_DATA_UNRELIABLE"))
        exec_errors=sum(1 for x in alerts if x.get("event_type") in ("ORDER_REJECTED","ORDER_REJECTED_OR_UNCONFIRMED","PARTIAL_FILL_UNEXPECTED","ORDER_STATUS_UNKNOWN"))
        broker_fail=sum(1 for x in alerts if "BROKER" in str(x.get("event_type") or "") and x.get("severity") in ("HIGH","CRITICAL"))
        critical=sum(1 for x in alerts if x.get("severity")=="CRITICAL" and x.get("status")!="RECOVERED")
        recon_fail=sum(1 for x in recons if x.get("status") in ("RECONCILIATION_REQUIRED","CRITICAL_MISMATCH"))
        heartbeats=self._rows("observability_heartbeats","WHERE ts>=? AND ts<=?",(cutoff,as_of.isoformat()))
        uptime_ok=sum(1 for x in heartbeats if x.get("status")=="OK")
        uptime_ratio=safe_div(uptime_ok,len(heartbeats),0.0) if heartbeats else 0.0
        return {"window_days":30,"uptime_health_ratio":uptime_ratio,
                "avg_broker_latency_ms":statistics.mean(lats) if lats else None,"p95_broker_latency_ms":percentile(lats,.95),
                "avg_processing_ms":statistics.mean(total) if total else None,"p95_processing_ms":percentile(total,.95),
                "broker_failures":broker_fail,"reconciliation_failures":recon_fail,"stale_data_events":stale,
                "execution_errors":exec_errors,"recovery_incidents":len(rec),"active_critical_alerts":critical}

    def _risk(self,as_of,trades):
        caps=self._rows("observability_capital_history","WHERE ts<=? ORDER BY ts DESC LIMIT 1000",(as_of.isoformat(),))
        latest=caps[0] if caps else {}
        draw=f(latest.get("drawdown"),0.0) or 0.0
        exposure=max([f(x.get("exposure"),0.0) or 0.0 for x in caps] or [0.0])
        margin=max([f(x.get("margin_usage"),0.0) or 0.0 for x in caps] or [0.0])
        by_st=self._attribution(trades,"strategy")
        total_risk=sum(v["risk_consumption"] for v in by_st.values())
        concentration=max([safe_div(v["risk_consumption"],total_risk,0.0) for v in by_st.values()] or [0.0])
        rr=[f(x.get("realized_r")) for x in trades if f(x.get("realized_r")) is not None]
        tail=percentile(rr,.05)
        cutoff=(as_of-timedelta(days=30)).isoformat()
        risk_rows=self._rows("adaptive_risk_decisions","WHERE ts>=? AND ts<=?",(cutoff,as_of.isoformat()))
        emergency_events=sum(int(x.get("emergency_stop") or 0) for x in risk_rows)
        return {"current_drawdown":draw,"drawdown_utilization":safe_div(draw,self.risk_drawdown_limit,0.0),
                "drawdown_limit":self.risk_drawdown_limit,"max_observed_exposure":exposure,
                "max_margin_usage":margin,"strategy_risk_concentration":concentration,
                "loss_tail_p05_r":tail,"emergency_stop_decisions_30d":emergency_events}

    def _stability(self,trades,as_of):
        last30=self._window(trades,30,as_of=as_of);last7=self._window(trades,7,as_of=as_of)
        m30=trade_metrics(last30);m7=trade_metrics(last7)
        changes=self._rows("security_audit_log","WHERE timestamp>=? AND timestamp<=? AND action IN ('CONFIG_CHANGED','CONFIG_ROLLBACK','CANDIDATE_PROMOTION','PRODUCTION_DEPLOYMENT_APPROVED')",
                          ((as_of-timedelta(days=30)).isoformat(),as_of.isoformat()))
        dep=self._rows("deployment_events","WHERE ts>=? AND ts<=?",((as_of-timedelta(days=30)).isoformat(),as_of.isoformat()))
        incidents=self._rows("recovery_incidents","WHERE started_ts>=? AND started_ts<=?",((as_of-timedelta(days=30)).isoformat(),as_of.isoformat()))
        exp_var=abs((m7.get("expectancy") or 0)-(m30.get("expectancy") or 0))
        score=max(0.0,100-min(35,exp_var*30)-min(20,len(changes)*3)-min(20,len(dep)*1.5)-min(25,len(incidents)*4))
        return {"score":score,"expectancy_variation":exp_var,"critical_changes_30d":len(changes),
                "deployment_events_30d":len(dep),"operational_incidents_30d":len(incidents)}

    def _activity_efficiency(self,trades,as_of):
        cur_start=(as_of-timedelta(days=30)).isoformat()
        hist_start=(as_of-timedelta(days=90)).isoformat()
        hist_end=(as_of-timedelta(days=30)).isoformat()
        signals_recent=self._rows("signals","WHERE ts>=? AND ts<=?",(cur_start,as_of.isoformat()))
        signals_hist=self._rows("signals","WHERE ts>=? AND ts<?",(hist_start,hist_end))
        trades_recent=self._window(trades,30,as_of=as_of)
        trades_hist=[r for r in trades if (parse_ts(r.get("exit_ts")) or as_of)>=as_of-timedelta(days=90)
                     and (parse_ts(r.get("exit_ts")) or as_of)<as_of-timedelta(days=30)]
        recent_rate=len(trades_recent)/30.0
        hist_rate=len(trades_hist)/60.0
        gross=sum(abs(f(r.get("gross_result"),0.0) or 0.0) for r in trades_recent)
        fees=sum(abs(f(r.get("fees_total"),0.0) or 0.0) for r in trades_recent)
        fee_drag=safe_div(fees,gross,0.0)
        valid_recent=sum(1 for x in signals_recent if int(x.get("technical") or 0))
        blocked_recent=sum(1 for x in signals_recent if int(x.get("blocked") or 0))
        executed_recent=sum(1 for x in signals_recent if int(x.get("executed") or 0))
        execution_ratio=safe_div(executed_recent,valid_recent,None) if valid_recent else None
        blocked_ratio=safe_div(blocked_recent,valid_recent,None) if valid_recent else None
        over=(hist_rate>0 and recent_rate>hist_rate*1.8 and fee_drag>.12)
        under=(valid_recent>=20 and execution_ratio is not None and execution_ratio<.15 and blocked_ratio is not None and blocked_ratio>.6)
        return {"recent_trades_per_day":recent_rate,"historical_trades_per_day":hist_rate,
                "fee_drag_vs_abs_gross":fee_drag,"valid_signals_30d":valid_recent,
                "blocked_signals_30d":blocked_recent,"executed_signals_30d":executed_recent,
                "execution_ratio":execution_ratio,"blocked_ratio":blocked_ratio,
                "possible_overtrading":over,"possible_overfiltering":under}

    def _execution_quality(self,trades,as_of,operational):
        cutoff=(as_of-timedelta(days=30)).isoformat()
        intents=self._rows("recovery_order_intents","WHERE created_ts>=? AND created_ts<=?",(cutoff,as_of.isoformat()))
        terminal=[x for x in intents if x.get("state") in ("FILLED","PARTIALLY_FILLED","REJECTED","CANCELLED","EXPIRED")]
        fills=sum(1 for x in intents if x.get("state") in ("FILLED","PARTIALLY_FILLED"))
        rejected=sum(1 for x in intents if x.get("state")=="REJECTED")
        partial=sum(1 for x in intents if x.get("state")=="PARTIALLY_FILLED")
        unknown=sum(1 for x in intents if x.get("state")=="UNKNOWN")
        recent=self._window(trades,30,as_of=as_of)
        tm=trade_metrics(recent)
        return {"order_intents_30d":len(intents),"fill_rate":safe_div(fills,len(terminal),None) if terminal else None,
                "rejections":rejected,"partial_fills":partial,"unknown_order_states":unknown,
                "avg_total_slippage_pips":tm.get("avg_total_slippage_pips"),
                "p95_broker_latency_ms":operational.get("p95_broker_latency_ms"),
                "spread_paid":"UNAVAILABLE_NO_PERSISTED_BID_ASK_SPREAD",
                "costs":{"gross_pnl":tm.get("gross_pnl"),"net_pnl":tm.get("net_pnl"),
                         "fees":tm.get("fees"),"financing":tm.get("financing")}}

    def _deployment_impact(self,trades,as_of):
        events=self._rows("deployment_events",
            "WHERE ts<=? AND event_type IN ('PROMOTION','ROLLBACK','REDUCTION','PAUSE','RESUME') ORDER BY ts DESC LIMIT 30",
            (as_of.isoformat(),))
        out=[]
        for e in events:
            ts=parse_ts(e.get("ts"))
            if not ts:continue
            before=[r for r in trades if ts-timedelta(days=14) <= (parse_ts(r.get("exit_ts")) or ts-timedelta(days=100)) < ts]
            after=[r for r in trades if ts <= (parse_ts(r.get("exit_ts")) or ts+timedelta(days=100)) <= min(as_of,ts+timedelta(days=14))]
            regimes_before={}
            regimes_after={}
            for rs,target in ((before,regimes_before),(after,regimes_after)):
                for r in rs:target[r.get("market_regime_entry") or "UNKNOWN"]=target.get(r.get("market_regime_entry") or "UNKNOWN",0)+1
            out.append({"candidate_id":e.get("candidate_id"),"event_type":e.get("event_type"),
                        "timestamp":e.get("ts"),"before":trade_metrics(before),"after":trade_metrics(after),
                        "regime_mix_before":regimes_before,"regime_mix_after":regimes_after,
                        "regime_control":"DESCRIPTIVE_STRATIFICATION_ONLY","causal_claim":False})
        return out

    def _change_impact(self,trades,as_of):
        changes=self._rows("security_audit_log",
            "WHERE timestamp<=? AND action IN ('CONFIG_CHANGED','CONFIG_ROLLBACK','STRATEGY_CONFIGURATION_APPLIED') ORDER BY timestamp DESC LIMIT 20",
            (as_of.isoformat(),))
        out=[]
        for c in changes:
            ts=parse_ts(c.get("timestamp"))
            if not ts:continue
            before=[r for r in trades if ts-timedelta(days=14) <= (parse_ts(r.get("exit_ts")) or ts-timedelta(days=100)) < ts]
            after=[r for r in trades if ts <= (parse_ts(r.get("exit_ts")) or ts+timedelta(days=100)) <= min(as_of,ts+timedelta(days=14))]
            out.append({"audit_id":c.get("audit_id"),"action":c.get("action"),"resource":c.get("resource"),
                        "timestamp":c.get("timestamp"),"before":trade_metrics(before),"after":trade_metrics(after),
                        "regime_control":"DESCRIPTIVE_ONLY","causal_claim":False})
        return out

    def _incident_impact(self,trades,as_of):
        inc=self._rows("recovery_incidents","WHERE started_ts<=? ORDER BY started_ts DESC LIMIT 50",(as_of.isoformat(),))
        out=[]
        for x in inc:
            a=parse_ts(x.get("started_ts"));b=parse_ts(x.get("recovered_ts")) or a
            if not a:continue
            affected=[r for r in trades if a-timedelta(hours=1) <= (parse_ts(r.get("exit_ts")) or a-timedelta(days=10)) <= b+timedelta(hours=4)]
            out.append({"incident_id":x.get("incident_id"),"type":x.get("incident_type"),"severity":x.get("severity"),
                        "started_ts":x.get("started_ts"),"recovered_ts":x.get("recovered_ts"),
                        "nearby_trade_count":len(affected),"nearby_net_pnl":trade_metrics(affected)["net_pnl"],
                        "impact_is_estimate":True})
        return out

    def evaluate(self,as_of:Optional[str]=None)->Dict[str,Any]:
        self.ensure_schema()
        asdt=parse_ts(as_of) if as_of else datetime.now(timezone.utc)
        if asdt is None:raise ValueError("invalid as_of")
        trades=self._trades(asdt)
        dq=self._quality(trades,asdt)
        valid=[r for r in trades if not int(r.get("execution_quality_compromised") or 0)]
        hist=trade_metrics(valid)
        w={"last_20_trades":trade_metrics(valid[-20:]),"last_50_trades":trade_metrics(valid[-50:]),
           "last_100_trades":trade_metrics(valid[-100:]),"last_day":trade_metrics(self._window(valid,1,as_of=asdt)),
           "last_7d":trade_metrics(self._window(valid,7,as_of=asdt)),"last_30d":trade_metrics(self._window(valid,30,as_of=asdt))}
        recent30=self._window(valid,30,as_of=asdt)
        historical_prior=[r for r in valid if (parse_ts(r.get("exit_ts")) or asdt)>=datetime.min.replace(tzinfo=timezone.utc)
                          and (parse_ts(r.get("exit_ts")) or asdt)<asdt-timedelta(days=30)]
        baseline={"historical":trade_metrics(historical_prior if historical_prior else valid),
                  "recent":w["last_30d"],"current":w["last_7d"]}
        attr={d:self._attribution(valid,d) for d in ("strategy","market_regime","asset","direction","session","deployment_version")}
        total_profit=sum(max(0.0,m.get("net_pnl",0.0)) for m in attr["strategy"].values())
        total_loss=sum(m.get("loss_contribution",0.0) for m in attr["strategy"].values())
        total_risk=sum(m.get("risk_consumption",0.0) for m in attr["strategy"].values())
        total_dd=sum(m.get("max_drawdown_absolute",0.0) for m in attr["strategy"].values())
        for st,m in attr["strategy"].items():
            profit_share=safe_div(max(0.0,m.get("net_pnl",0.0)),total_profit,0.0)
            loss_share=safe_div(m.get("loss_contribution",0.0),total_loss,0.0)
            risk_share=safe_div(m.get("risk_consumption",0.0),total_risk,0.0)
            dd_share=safe_div(m.get("max_drawdown_absolute",0.0),total_dd,0.0)
            m["profit_share"]=profit_share;m["loss_share"]=loss_share;m["risk_share"]=risk_share;m["drawdown_share"]=dd_share
            m["strategy_contribution_score"]=profit_share-.35*loss_share-.35*risk_share-.30*dd_share
            m["return_per_risk_unit"]=safe_div(m.get("net_pnl",0.0),m.get("risk_consumption",0.0),None) if m.get("risk_consumption",0.0)>0 else None
        marginal=self._marginal(valid)
        div=self._diversification(valid);coverage=self._regime_coverage(valid)
        director=self._director(asdt);riskeng=self._risk_engine(asdt,valid);adaptive=self._adaptive(asdt)
        gap=self._model_gap(asdt);oper=self._operational(asdt);risk=self._risk(asdt,valid);stab=self._stability(valid,asdt)
        activity=self._activity_efficiency(valid,asdt);execution_quality=self._execution_quality(valid,asdt,oper)
        change=self._change_impact(valid,asdt);deployment_impact=self._deployment_impact(valid,asdt)
        incident=self._incident_impact(trades,asdt)

        # Dimension scores. These intentionally contain hard penalties and are not a simple arithmetic average.
        cur=w["last_30d"];base=baseline["historical"]
        trading_score=50.0
        if cur["sample_size"]>=self.min_samples:
            trading_score+=15 if (cur["expectancy"] or 0)>0 else -20
            trading_score+=10 if (cur["profit_factor"] or 0)>=1.2 else -10 if (cur["profit_factor"] or 0)<1 else 0
            trading_score+=10 if (cur["sharpe"] or 0)>=1 else -5 if (cur["sharpe"] or 0)<0 else 0
            trading_score+=10 if (cur["win_rate"] or 0)>=.5 else -5
        else:trading_score-=15
        trading_score=max(0,min(100,trading_score))

        risk_score=100.0
        risk_score-=min(45,(risk.get("drawdown_utilization") or 0)*45)
        risk_score-=min(25,(risk.get("strategy_risk_concentration") or 0)*30)
        risk_score-=min(30,riskeng.get("hard_limit_hits",0)*5+riskeng.get("emergency_stops",0)*15)
        if riskeng.get("efficiency")=="OVER_RESTRICTIVE":risk_score-=15
        risk_score=max(0,min(100,risk_score))

        op_score=100.0
        op_score-=min(25,oper["reconciliation_failures"]*8)
        op_score-=min(25,oper["broker_failures"]*6)
        op_score-=min(20,oper["execution_errors"]*4)
        op_score-=min(15,oper["stale_data_events"]*3)
        op_score-=min(30,oper["active_critical_alerts"]*15)
        if (oper.get("p95_broker_latency_ms") or 0)>2000:op_score-=20
        op_score=max(0,min(100,op_score))

        stability_score=max(0,min(100,stab["score"]))
        degradation=[];factors=[]
        hist_exp=base.get("expectancy");cur_exp=cur.get("expectancy")
        hist_pf=base.get("profit_factor");cur_pf=cur.get("profit_factor")
        if cur["sample_size"]>=self.min_samples and hist_exp is not None and cur_exp is not None and cur_exp<hist_exp*.65:
            degradation.append("STRATEGY_DEGRADATION");factors.append({"factor":"expectancy_drop","historical":hist_exp,"current":cur_exp})
        latest_reg=self._rows("market_regime_history","WHERE ts<=? ORDER BY ts DESC LIMIT 200",(asdt.isoformat(),))
        if latest_reg:
            recent_rg=[x["market_regime"] for x in latest_reg[:50]];older_rg=[x["market_regime"] for x in latest_reg[50:200]]
            if older_rg:
                rmode=max(set(recent_rg),key=recent_rg.count);omode=max(set(older_rg),key=older_rg.count)
                if rmode!=omode:degradation.append("MARKET_REGIME_SHIFT");factors.append({"factor":"regime_shift","from":omode,"to":rmode})
        if (cur.get("avg_total_slippage_pips") or 0) > max(1.0,(base.get("avg_total_slippage_pips") or 0)*1.5):
            degradation.append("EXECUTION_DEGRADATION");factors.append({"factor":"slippage_increase","historical":base.get("avg_total_slippage_pips"),"current":cur.get("avg_total_slippage_pips")})
        if op_score<65:degradation.append("INFRASTRUCTURE_DEGRADATION");factors.append({"factor":"operational_score","value":op_score})
        if risk_score<65 or riskeng.get("efficiency")=="OVER_RESTRICTIVE":degradation.append("RISK_DEGRADATION");factors.append({"factor":"risk_score","value":risk_score,"risk_engine":riskeng.get("efficiency")})
        if gap["status"]=="MODEL_REALITY_GAP":degradation.append("MODEL_DEGRADATION");factors.append({"factor":"model_reality_gap","count":len(gap["material_gaps"])})
        if dq["score"]<.75:degradation.append("DATA_QUALITY_DEGRADATION");factors.append({"factor":"data_quality","value":dq["score"]})
        degradation=list(dict.fromkeys(degradation))

        # Critical dimensions dominate score by construction.
        dimension_values={"trading":max(.01,trading_score),"risk":max(.01,risk_score),
                          "operational":max(.01,op_score),"stability":max(.01,stability_score)}
        base_quality=math.exp(sum(self.score_weights[k]*math.log(dimension_values[k]) for k in dimension_values))
        hard_penalty=1.0
        paused=False
        rec_state=self._rows("recovery_state","WHERE account_scope='PRIMARY'")
        if rec_state and (rec_state[0].get("emergency_stop") or rec_state[0].get("safe_mode")):paused=True
        if op_score<35 or risk_score<35:hard_penalty=min(hard_penalty,.45)
        if oper["active_critical_alerts"]>0:hard_penalty=min(hard_penalty,.55)
        system_score=max(0,min(100,base_quality*hard_penalty))
        if paused:status="PAUSED"
        elif op_score<25 or risk_score<25:status="CRITICAL"
        elif op_score<45 or risk_score<45:status="HIGH_RISK"
        elif degradation and system_score<65:status="DEGRADING"
        elif degradation or system_score<75:status="WATCH"
        elif system_score>=90 and trading_score>=85:status="EXCELLENT"
        else:status="HEALTHY"

        recommendations=[]
        def rec(name,priority,reason,evidence):
            recommendations.append({"recommendation":name,"priority":priority,"reason":reason,
                                    "confidence":confidence(cur["sample_size"],dq["score"],safe_div((cur_exp or 0)-(hist_exp or 0),abs(hist_exp or 1),0))["confidence_score"],
                                    "evidence":evidence})
        if not degradation:rec("HOLD_CURRENT_CONFIGURATION","INFO","No material global degradation detected.",{"system_score":system_score})
        if "STRATEGY_DEGRADATION" in degradation:
            worst=min(attr["strategy"].items(),key=lambda kv:(kv[1].get("expectancy") if kv[1].get("expectancy") is not None else 999))[0] if attr["strategy"] else "UNKNOWN"
            rec(f"REVIEW_{worst}","HIGH","Strategy attribution and recent expectancy indicate degradation.",{"strategy":worst})
        if "EXECUTION_DEGRADATION" in degradation:rec("REVIEW_EXECUTION_LATENCY","HIGH","Execution costs/slippage deteriorated.",{"operational":oper,"trading":cur})
        if "DATA_QUALITY_DEGRADATION" in degradation:rec("INVESTIGATE_DATA_QUALITY","HIGH","Input/history quality reduced confidence in conclusions.",dq)
        if gap["status"]=="MODEL_REALITY_GAP":rec("REVALIDATE_CANDIDATE","HIGH","Backtest/paper/live expectations diverge materially.",gap)
        if div["status"]=="HIDDEN_CONCENTRATION_RISK":rec("REVIEW_HIDDEN_CONCENTRATION","HIGH","Strategies show highly correlated realized return streams.",div["hidden_concentration_pairs"])
        if coverage["status"]=="REGIME_COVERAGE_GAP":rec("REVIEW_REGIME_COVERAGE","MEDIUM","One or more observed regimes lack robust strategy evidence.",coverage["gaps"])
        if riskeng.get("efficiency")=="OVER_RESTRICTIVE":rec("REVIEW_RISK_ENGINE_RESTRICTIVENESS","MEDIUM","Block rate is high relative to estimated avoided-loss precision.",riskeng)
        if adaptive.get("assessment") in ("CANDIDATE_QUALITY_LOW","VALIDATION_TOO_WEAK"):rec("REVIEW_ADAPTIVE_LEARNING_FUNNEL","MEDIUM","Candidate survival funnel is statistically unusual.",adaptive)
        if activity.get("possible_overtrading"):
            rec("POSSIBLE_OVERTRADING","MEDIUM","Trade frequency and fee drag rose materially while marginal efficiency weakened.",activity)
        if activity.get("possible_overfiltering"):
            rec("POSSIBLE_OVERFILTERING","MEDIUM","A large share of technically valid signals is blocked and very few become trades.",activity)
        if op_score<50 or risk_score<50:rec("PAUSE_DEPLOYMENTS","CRITICAL","Risk or operational reliability is too weak for safe promotion activity.",{"risk_score":risk_score,"operational_score":op_score})

        # Biggest risk contributor = highest risk consumption, then loss contribution.
        biggest=None
        if attr["strategy"]:
            biggest=max(attr["strategy"].items(),key=lambda kv:(kv[1].get("risk_consumption",0),kv[1].get("loss_contribution",0)))[0]
        main=degradation[0] if degradation else None
        conf=confidence(cur["sample_size"],dq["score"],safe_div((cur_exp or 0)-(hist_exp or 0),abs(hist_exp or 1),0))

        executive={
            "SYSTEM_STATUS":status,"SYSTEM_SCORE":system_score,"P&L":cur.get("net_pnl"),
            "DRAWDOWN":risk.get("current_drawdown"),
            "TOP_CONTRIBUTOR":max(attr["strategy"].items(),key=lambda kv:kv[1].get("net_pnl",0))[0] if attr["strategy"] else None,
            "BIGGEST_RISK":biggest,
            "MAJOR_CHANGE":change[0] if change else (deployment_impact[0] if deployment_impact else None),
            "MAJOR_INCIDENT":incident[0] if incident else None,
            "CURRENT_DEGRADATION":degradation,
            "RECOMMENDED_ACTION":recommendations[0]["recommendation"] if recommendations else "HOLD_CURRENT_CONFIGURATION"
        }
        data_snapshot={
            "as_of":asdt.isoformat(),"trade_ids":[r.get("trade_id") for r in trades],
            "latest_market_regime_ids":[x.get("id") for x in latest_reg[:50]],
            "config_versions":sorted({r.get("risk_config_version") for r in trades if r.get("risk_config_version")}),
            "runtime_code_hashes":sorted({r.get("runtime_code_hash") for r in trades if r.get("runtime_code_hash")}),
            "future_data_used":False
        }
        result={
            "evaluation_id":"seval_"+uuid.uuid4().hex,"generated_at":now_iso(),"as_of_ts":asdt.isoformat(),
            "system_status":status,"system_score":system_score,
            "score_method":{"type":"weighted_geometric_with_hard_gates","weights":self.score_weights,
                            "hard_penalty":hard_penalty,"critical_dimensions_dominate":True},
            "dimensions":{"trading_score":trading_score,"risk_score":risk_score,
                          "operational_score":op_score,"stability_score":stability_score},
            "confidence":conf,
            "trading":{**cur,"execution_quality":execution_quality,"activity_efficiency":activity},
            "risk":risk,"operational":oper,"stability":stab,
            "baseline":baseline,"rolling_windows":w,"attribution":{**attr,"marginal_value":marginal},
            "ai_strategy_director":director,"risk_engine":riskeng,"adaptive_learning":adaptive,
            "model_reality_gap":gap,"diversification":div,"regime_coverage":coverage,
            "activity_efficiency":activity,"execution_quality":execution_quality,
            "change_impact":{"configuration_changes":change,"deployments":deployment_impact},
            "incident_impact":incident,
            "degradation":{"detected":bool(degradation),"types":degradation,"factors":factors,
                           "classification":"MULTIPLE_CONTRIBUTING_FACTORS" if len(degradation)>1 else degradation[0] if degradation else "NONE",
                           "root_cause_analysis":{"possible_contributing_factors":factors,
                                                  "interpretation":"correlational attribution, not causal proof",
                                                  "causality_claimed":False},
                           "causality_claimed":False},
            "recommendations":recommendations,"data_quality":dq,"executive_summary":executive,
            "data_snapshot":data_snapshot,"autonomous_actions":False
        }
        self._persist(result)
        return result

    def _persist(self,r):
        c=self.conn()
        c.execute("""INSERT INTO system_evaluations(
          evaluation_id,generated_at,as_of_ts,engine_version,system_status,system_score,
          trading_score,risk_score,operational_score,stability_score,confidence_level,confidence_score,
          sample_size,data_quality_score,main_degradation_factor,biggest_risk_contributor,
          executive_summary_json,trading_json,risk_json,operational_json,stability_json,baseline_json,
          rolling_windows_json,attribution_json,director_json,risk_engine_json,adaptive_learning_json,
          model_reality_gap_json,diversification_json,regime_coverage_json,change_impact_json,
          incident_impact_json,degradation_json,recommendations_json,data_snapshot_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (r["evaluation_id"],r["generated_at"],r["as_of_ts"],self.version,r["system_status"],r["system_score"],
           r["dimensions"]["trading_score"],r["dimensions"]["risk_score"],r["dimensions"]["operational_score"],
           r["dimensions"]["stability_score"],r["confidence"]["confidence_level"],r["confidence"]["confidence_score"],
           r["confidence"]["sample_size"],r["data_quality"]["score"],(r["degradation"]["types"] or [None])[0],
           r["executive_summary"].get("BIGGEST_RISK"),j(r["executive_summary"]),j(r["trading"]),j(r["risk"]),
           j(r["operational"]),j(r["stability"]),j(r["baseline"]),j(r["rolling_windows"]),j(r["attribution"]),
           j(r["ai_strategy_director"]),j(r["risk_engine"]),j(r["adaptive_learning"]),j(r["model_reality_gap"]),
           j(r["diversification"]),j(r["regime_coverage"]),j(r["change_impact"]),j(r["incident_impact"]),
           j(r["degradation"]),j(r["recommendations"]),j(r["data_snapshot"])))
        for x in r["recommendations"]:
            c.execute("""INSERT INTO system_evaluation_recommendations(
              evaluation_id,recommendation,priority,reason,confidence,evidence_json)
              VALUES(?,?,?,?,?,?)""",(r["evaluation_id"],x["recommendation"],x["priority"],x["reason"],x["confidence"],j(x["evidence"])))
        for dim in ("strategy","market_regime","asset","direction","session","deployment_version"):
            for key,m in r["attribution"].get(dim,{}).items():
                c.execute("""INSERT INTO system_evaluation_attribution(
                  evaluation_id,dimension,key,sample_size,net_pnl,expectancy,profit_factor,drawdown,
                  risk_consumption,contribution_score,details_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                  (r["evaluation_id"],dim,key,m.get("sample_size",0),m.get("net_pnl"),m.get("expectancy"),
                   m.get("profit_factor"),m.get("max_drawdown_absolute"),m.get("risk_consumption"),
                   m.get("profit_contribution_fraction"),j(m)))
        c.commit();c.close()

    def latest(self)->Optional[Dict[str,Any]]:
        c=self.conn();r=c.execute("SELECT * FROM system_evaluations ORDER BY generated_at DESC LIMIT 1").fetchone();c.close()
        if not r:return None
        d=dict(r)
        for key in [x for x in d if x.endswith("_json")]:
            try:d[key[:-5]]=json.loads(d[key])
            except Exception:pass
        return d

    def history(self,limit=100)->List[Dict[str,Any]]:
        c=self.conn();rows=[dict(x) for x in c.execute("""SELECT evaluation_id,generated_at,as_of_ts,system_status,system_score,
          trading_score,risk_score,operational_score,stability_score,confidence_level,sample_size,data_quality_score,
          main_degradation_factor,biggest_risk_contributor,executive_summary_json
          FROM system_evaluations ORDER BY generated_at DESC LIMIT ?""",(min(max(int(limit),1),1000),)).fetchall()];c.close()
        for x in rows:
            try:x["executive_summary"]=json.loads(x.pop("executive_summary_json"))
            except Exception:pass
        return rows

    def due(self)->bool:
        latest=self.latest()
        if not latest:return True
        dt=parse_ts(latest.get("generated_at"))
        return not dt or datetime.now(timezone.utc)-dt>=timedelta(hours=self.report_period_hours)

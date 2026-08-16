from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone, timedelta
import hashlib, json, math, sqlite3, statistics, uuid

ANOMALY_TYPES=(
 "MARKET_ANOMALY","DATA_ANOMALY","LIQUIDITY_ANOMALY","VOLATILITY_ANOMALY",
 "CORRELATION_ANOMALY","STRATEGY_ANOMALY","ENSEMBLE_ANOMALY","EXECUTION_ANOMALY",
 "PORTFOLIO_ANOMALY","BROKER_ANOMALY","SYSTEM_ANOMALY"
)
SEVERITIES=("NORMAL","WATCH","ELEVATED","HIGH","CRITICAL")
HORIZONS={"VERY_SHORT_TERM":5,"SHORT_TERM":30,"MEDIUM_TERM":180,"LONG_TERM":1440}

def now_iso(): return datetime.now(timezone.utc).isoformat()
def parse_ts(x):
    if not x:return None
    try:
        d=datetime.fromisoformat(str(x).replace("Z","+00:00"));return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:return None
def f(x,default=None):
    try:
        y=float(x);return y if math.isfinite(y) else default
    except Exception:return default
def clamp(x,lo=0.0,hi=1.0): return max(lo,min(hi,float(x)))
def j(x): return json.dumps(x,separators=(",",":"),sort_keys=True,default=str)
def percentile(xs,p):
    xs=sorted(float(x) for x in xs if f(x) is not None)
    if not xs:return None
    if len(xs)==1:return xs[0]
    k=(len(xs)-1)*p;a=math.floor(k);b=math.ceil(k)
    return xs[a] if a==b else xs[a]+(xs[b]-xs[a])*(k-a)
def mad(xs):
    if not xs:return None
    m=statistics.median(xs);return statistics.median([abs(x-m) for x in xs])
def robust_z(value,history):
    if value is None or len(history)<5:return 0.0
    m=statistics.median(history);d=mad(history)
    if d and d>1e-12:return 0.6745*(value-m)/d
    q25=percentile(history,.25);q75=percentile(history,.75)
    iqr=(q75-q25) if q25 is not None and q75 is not None else 0
    if iqr and iqr>1e-12:return (value-m)/max(iqr/1.349,1e-12)
    # Degenerate baselines are common for scheduled/session-specific metrics.
    # Use a conservative relative floor instead of treating tiny deviations as
    # infinite z-scores.
    floor=max(abs(m)*.05,abs(value)*.01,1e-9)
    return (value-m)/floor
def score_from_z(z): return clamp((abs(z)-1.0)/5.0)
def severity(score): return "CRITICAL" if score>=.85 else "HIGH" if score>=.70 else "ELEVATED" if score>=.50 else "WATCH" if score>=.30 else "NORMAL"
def fingerprint(features):
    canonical={k:round(float(v),4) if f(v) is not None else str(v) for k,v in sorted(features.items())}
    return hashlib.sha256(j(canonical).encode()).hexdigest()[:24]

class AnomalyDetectionEngine:
    """Shadow-only anomaly layer. It never generates signals, orders, or risk increases."""
    def __init__(self,db_path:str,version:str="3.27",mode:str="SHADOW",baseline_min_samples:int=20,
                 persistence_confirmations:int=2,recovery_confirmations:int=3,enter_threshold:float=.50,
                 exit_threshold:float=.25,ood_threshold:float=.70,structural_min_points:int=10,
                 rare_event_cluster_min_events:int=12,context_day_min_multiplier:int=2):
        self.db_path=db_path;self.version=version;self.mode="SHADOW"
        self.baseline_min_samples=max(8,int(baseline_min_samples));self.persistence_confirmations=max(1,int(persistence_confirmations))
        self.recovery_confirmations=max(2,int(recovery_confirmations));self.enter_threshold=clamp(enter_threshold,.2,.95)
        self.exit_threshold=min(self.enter_threshold-.05,clamp(exit_threshold,.05,.8));self.ood_threshold=clamp(ood_threshold,.4,.95)
        self.structural_min_points=max(6,int(structural_min_points))
        self.rare_event_cluster_min_events=max(6,int(rare_event_cluster_min_events))
        self.context_day_min_multiplier=max(2,int(context_day_min_multiplier))
    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30);c.row_factory=sqlite3.Row;c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA synchronous=FULL");c.execute("PRAGMA busy_timeout=5000");return c
    def ensure_schema(self):
        c=self.conn();c.executescript("""
        CREATE TABLE IF NOT EXISTS anomaly_observations(observation_id TEXT PRIMARY KEY,ts TEXT NOT NULL,symbol TEXT,session TEXT,market_regime TEXT,metrics_json TEXT NOT NULL,features_json TEXT NOT NULL,source_versions_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS anomaly_events(anomaly_id TEXT PRIMARY KEY,rare_event_id TEXT,anomaly_type TEXT NOT NULL,subtype TEXT NOT NULL,severity TEXT NOT NULL,anomaly_score REAL NOT NULL,confidence REAL NOT NULL,lifecycle_state TEXT NOT NULL,scope TEXT NOT NULL,affected_symbols_json TEXT NOT NULL,affected_strategies_json TEXT NOT NULL,detected_at TEXT NOT NULL,anomaly_start TEXT NOT NULL,last_update TEXT NOT NULL,recovered_at TEXT,duration_seconds REAL NOT NULL DEFAULT 0,peak_score REAL NOT NULL,current_score REAL NOT NULL,recovery_progress REAL NOT NULL DEFAULT 0,supporting_metrics_json TEXT NOT NULL,baseline_reference_json TEXT NOT NULL,horizons_json TEXT NOT NULL,context_json TEXT NOT NULL,recommendations_json TEXT NOT NULL,fingerprint TEXT,engine_version TEXT NOT NULL,mode TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS anomaly_transitions(id INTEGER PRIMARY KEY AUTOINCREMENT,anomaly_id TEXT NOT NULL,ts TEXT NOT NULL,from_state TEXT,to_state TEXT,from_severity TEXT,to_severity TEXT,score REAL,reason TEXT);
        CREATE TABLE IF NOT EXISTS anomaly_rare_events(rare_event_id TEXT PRIMARY KEY,started_at TEXT NOT NULL,ended_at TEXT,event_fingerprint TEXT NOT NULL,cluster_label TEXT NOT NULL DEFAULT 'UNKNOWN_EVENT_TYPE',peak_composite_score REAL NOT NULL,before_context_json TEXT NOT NULL,during_context_json TEXT NOT NULL,after_context_json TEXT NOT NULL DEFAULT '{}',affected_trades_json TEXT NOT NULL DEFAULT '[]',risk_actions_json TEXT NOT NULL DEFAULT '[]',execution_behavior_json TEXT NOT NULL DEFAULT '{}',portfolio_impact_json TEXT NOT NULL DEFAULT '{}',recovery_time_seconds REAL,outcome_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS anomaly_composite_history(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,global_score REAL NOT NULL,severity TEXT NOT NULL,confidence REAL NOT NULL,cross_domain_score REAL NOT NULL,active_anomalies_json TEXT NOT NULL,context_json TEXT NOT NULL,recommendations_json TEXT NOT NULL,rare_event_id TEXT);
        CREATE TABLE IF NOT EXISTS anomaly_shadow_evaluations(id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,anomaly_id TEXT,hypothetical_action TEXT NOT NULL,actual_outcome TEXT,classification TEXT,detection_delay_seconds REAL,details_json TEXT NOT NULL DEFAULT '{}');
        CREATE INDEX IF NOT EXISTS idx_anom_obs_symbol_ts ON anomaly_observations(symbol,ts);
        CREATE INDEX IF NOT EXISTS idx_anom_events_state ON anomaly_events(lifecycle_state,severity,last_update);
        CREATE INDEX IF NOT EXISTS idx_anom_obs_context ON anomaly_observations(symbol,session,market_regime,ts);
        """)
        # Backward-compatible Step 19 hardening migrations.
        def addcol(table,name,ddl):
            cols={r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()}
            if name not in cols:c.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")
        for name,ddl in (
            ("day_of_week","INTEGER"),("time_bucket","TEXT"),("volatility_regime","TEXT"),
            ("data_quality","TEXT"),("context_json","TEXT NOT NULL DEFAULT '{}'")):
            addcol("anomaly_observations",name,ddl)
        for name,ddl in (
            ("feature_vector_json","TEXT NOT NULL DEFAULT '{}'"),
            ("data_quality_json","TEXT NOT NULL DEFAULT '{}'"),
            ("affected_strategies_json","TEXT NOT NULL DEFAULT '[]'"),
            ("governance_response_json","TEXT NOT NULL DEFAULT '{}'"),
            ("recovery_process_json","TEXT NOT NULL DEFAULT '[]'")):
            addcol("anomaly_rare_events",name,ddl)
        c.execute("""CREATE TABLE IF NOT EXISTS anomaly_cluster_state(
          singleton INTEGER PRIMARY KEY CHECK(singleton=1),status TEXT NOT NULL,event_count INTEGER NOT NULL,
          min_events INTEGER NOT NULL,last_evaluated TEXT,clusters_json TEXT NOT NULL DEFAULT '{}')""")
        c.execute("""INSERT OR IGNORE INTO anomaly_cluster_state(singleton,status,event_count,min_events,last_evaluated,clusters_json)
                     VALUES(1,'INSUFFICIENT_EVENT_HISTORY',0,?,NULL,'{}')""",(self.rare_event_cluster_min_events,))
        c.commit();c.close()
    def _rows(self,sql,params=()):
        c=self.conn()
        try:r=[dict(x) for x in c.execute(sql,params).fetchall()]
        except sqlite3.OperationalError:r=[]
        c.close();return r
    def observe(self,snapshot):
        ts=parse_ts(snapshot.get("timestamp")) or datetime.now(timezone.utc);oid="aobs_"+uuid.uuid4().hex
        vol_regime=str(snapshot.get("volatility_regime") or snapshot.get("volatility_state") or
                       (snapshot.get("metrics") or {}).get("volatility_regime") or "UNKNOWN").upper()
        # Four-hour buckets reduce fragmentation versus exact-hour matching.
        time_bucket=f"{(ts.hour//4)*4:02d}-{((ts.hour//4)*4+4)%24:02d}"
        ctx={"day_name":ts.strftime("%A"),"hour":ts.hour,"time_bucket":time_bucket,
             "session":snapshot.get("session"),"market_regime":snapshot.get("market_regime"),
             "volatility_regime":vol_regime}
        c=self.conn();c.execute("""INSERT INTO anomaly_observations(
          observation_id,ts,symbol,session,market_regime,metrics_json,features_json,source_versions_json,
          day_of_week,time_bucket,volatility_regime,data_quality,context_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (oid,ts.isoformat(),snapshot.get("symbol"),snapshot.get("session"),snapshot.get("market_regime"),
           j(snapshot.get("metrics") or {}),j(snapshot.get("features") or {}),j(snapshot.get("source_versions") or {}),
           ts.weekday(),time_bucket,vol_regime,str(snapshot.get("data_quality") or "UNKNOWN").upper(),j(ctx)))
        c.commit();c.close();return oid
    def _context_candidates(self,snapshot,ts):
        """Most-specific to safest fallback. Day-of-week is only used with extra evidence."""
        symbol=snapshot.get("symbol");session=snapshot.get("session");regime=snapshot.get("market_regime")
        vol_regime=str(snapshot.get("volatility_regime") or snapshot.get("volatility_state") or
                       (snapshot.get("metrics") or {}).get("volatility_regime") or "UNKNOWN").upper()
        time_bucket=f"{(ts.hour//4)*4:02d}-{((ts.hour//4)*4+4)%24:02d}"
        day=ts.weekday()
        return [
          ("SYMBOL_SESSION_REGIME_VOL_TIME_DAY",
           {"symbol":symbol,"session":session,"market_regime":regime,"volatility_regime":vol_regime,
            "time_bucket":time_bucket,"day_of_week":day},self.baseline_min_samples*self.context_day_min_multiplier),
          ("SYMBOL_SESSION_REGIME_VOL_TIME",
           {"symbol":symbol,"session":session,"market_regime":regime,"volatility_regime":vol_regime,
            "time_bucket":time_bucket},self.baseline_min_samples),
          ("SYMBOL_SESSION_REGIME_VOL",
           {"symbol":symbol,"session":session,"market_regime":regime,"volatility_regime":vol_regime},
           self.baseline_min_samples),
          ("SYMBOL_SESSION_REGIME",{"symbol":symbol,"session":session,"market_regime":regime},self.baseline_min_samples),
          ("SYMBOL_SESSION",{"symbol":symbol,"session":session},self.baseline_min_samples),
          ("SYMBOL",{"symbol":symbol},self.baseline_min_samples),
          ("GLOBAL_SAFE_BASELINE",{},self.baseline_min_samples),
        ]

    def _context_rows(self,filters,before,minutes):
        cutoff=(before-timedelta(minutes=minutes)).isoformat()
        where=["ts>=?","ts<?"];params=[cutoff,before.isoformat()]
        for k,v in filters.items():
            if v is None or (isinstance(v,str) and not v):continue
            where.append(f"{k}=?");params.append(v)
        return self._rows("SELECT metrics_json,features_json,ts,session,market_regime,volatility_regime,time_bucket,day_of_week "
                          "FROM anomaly_observations WHERE "+" AND ".join(where)+" ORDER BY ts",tuple(params))

    def _history_metric(self,symbol,metric,before,minutes,session=None,regime=None,volatility_regime=None,
                        time_bucket=None,day_of_week=None):
        filters={"symbol":symbol,"session":session,"market_regime":regime,"volatility_regime":volatility_regime,
                 "time_bucket":time_bucket,"day_of_week":day_of_week}
        rows=self._context_rows(filters,before,minutes);out=[]
        for r in rows:
            try:v=f(json.loads(r["metrics_json"] or "{}").get(metric))
            except Exception:v=None
            if v is not None:out.append(v)
        return out

    def _baseline(self,snapshot,metric,ts,current):
        """
        Hierarchical contextual baseline. It tries the most specific context first
        and falls back when the sample is too fragmented.
        """
        horizons={};chosen_summary={}
        for hname,mins in HORIZONS.items():
            selected=None;fallback_with_data=None
            candidates=self._context_candidates(snapshot,ts)
            for scope,filters,min_n in candidates:
                rows=self._context_rows(filters,ts,mins)
                vals=[]
                for r in rows:
                    try:v=f(json.loads(r["metrics_json"] or "{}").get(metric))
                    except Exception:v=None
                    if v is not None:vals.append(v)
                if vals and fallback_with_data is None:
                    fallback_with_data=(scope,filters,min_n,vals)
                # Avoid short-horizon context mixing. GLOBAL_SAFE is only allowed
                # as a statistically safe last resort on the longest horizon.
                global_allowed=(scope!="GLOBAL_SAFE_BASELINE" or hname=="LONG_TERM")
                if global_allowed and len(vals)>=min_n:
                    selected=(scope,filters,min_n,vals);break
            if selected is None:
                # Keep the most-specific available context as LOW_EVIDENCE rather
                # than substituting a different session/regime and creating false alarms.
                selected=fallback_with_data or ("GLOBAL_SAFE_BASELINE",{},self.baseline_min_samples,[])
            scope,filters,min_n,vals=selected
            z=robust_z(current,vals)
            horizons[hname]={"n":len(vals),"minimum_required":min_n,
                "evidence":"SUFFICIENT" if len(vals)>=min_n else "LOW_EVIDENCE",
                "scope":scope,"filters":filters,
                "median":statistics.median(vals) if vals else None,"mad":mad(vals),
                "q05":percentile(vals,.05),"q25":percentile(vals,.25),"q75":percentile(vals,.75),
                "q95":percentile(vals,.95),"robust_z":z,"score":score_from_z(z)}
        viable=[v for v in horizons.values() if v["evidence"]=="SUFFICIENT"]
        score=max([v["score"] for v in viable],default=0.0)
        conf=clamp(max([v["n"] for v in viable],default=0)/100)
        if viable:
            # Use the longest viable horizon as primary reference to avoid reacting
            # only to a tiny local window.
            primary=viable[-1]
            chosen_summary={"used_scope":primary["scope"],"used_filters":primary["filters"],
                            "sample_size":primary["n"],"evidence":primary["evidence"]}
        else:
            primary=horizons["LONG_TERM"]
            chosen_summary={"used_scope":primary["scope"],"used_filters":primary["filters"],
                            "sample_size":primary["n"],"evidence":"LOW_EVIDENCE"}
        return {"horizons":horizons,"selected":chosen_summary},score,conf

    def _metric_anomaly(self,snapshot,metric,atype,subtype):
        ts=parse_ts(snapshot.get("timestamp")) or datetime.now(timezone.utc);v=f((snapshot.get("metrics") or {}).get(metric))
        if v is None:return None
        base,score,conf=self._baseline(snapshot,metric,ts,v)
        if score<.20:return None
        viable=[x for x in base["horizons"].values() if x.get("evidence")=="SUFFICIENT" and x.get("median") is not None]
        ref=(viable[-1]["median"] if viable else base["horizons"]["LONG_TERM"].get("median"))
        resolved=subtype;extra={"baseline_scope":base["selected"].get("used_scope")}
        if metric=="volume" and ref is not None:
            ratio=v/max(abs(ref),1e-12);extra["volume_ratio"]=ratio
            # Robust-z can explode for very stable baselines; require material
            # economic magnitude as well.
            magnitude=clamp(abs(math.log(max(ratio,1e-9)))/1.5)
            score=min(score,magnitude)
            resolved="ABNORMAL_VOLUME_SPIKE" if v>ref else "ABNORMAL_VOLUME_DROP"
        elif metric=="volatility" and ref is not None:
            ratio=v/max(abs(ref),1e-12);extra["volatility_ratio"]=ratio
            magnitude=clamp(abs(ratio-1.0)/2.5)
            score=min(score,magnitude)
            resolved="VOLATILITY_EXTREME" if ratio>=4 or score>=.90 else "VOLATILITY_SHOCK" if ratio>=2 else "VOLATILITY_EXPANDING"
        elif metric=="spread_bps" and ref is not None:
            ratio=v/max(abs(ref),1e-12);extra["spread_ratio"]=ratio
            magnitude=clamp(abs(ratio-1.0)/3.0)
            score=min(score,magnitude)
            extra["spread_state"]="EXTREME" if ratio>=4 else "WIDE" if ratio>=1.8 else "NORMAL"
        elif metric=="available_liquidity" and ref is not None:
            ratio=v/max(abs(ref),1e-12);extra["liquidity_ratio"]=ratio
            if ratio<=1:
                magnitude=clamp((1.0-ratio)/.75)
                score=min(score,magnitude)
            elif score<.5:return None
        elif metric=="return_abs" and ref is not None:
            ratio=v/max(abs(ref),1e-12);extra["return_ratio"]=ratio
            score=min(score,clamp(abs(ratio-1.0)/8.0))
        if score<.20:return None
        return {"anomaly_type":atype,"subtype":resolved,"score":score,"confidence":conf,
                "metrics":{metric:v,**extra},"baseline":base,
                "symbols":[snapshot.get("symbol")] if snapshot.get("symbol") else []}

    def _data_integrity(self,s):
        m=s.get("metrics") or {};issues=[];score=0;price=f(m.get("price"));bid=f(m.get("bid"));ask=f(m.get("ask"));vol=f(m.get("volume"));ts=parse_ts(s.get("timestamp"));now=datetime.now(timezone.utc)
        if price is not None and price<=0:issues.append("IMPOSSIBLE_PRICE");score=1
        if bid is not None and ask is not None and bid>ask:issues.append("INVALID_QUOTE");score=1
        if vol is not None and vol<0:issues.append("NEGATIVE_VOLUME");score=1
        if ts and ts>now+timedelta(seconds=2):issues.append("FUTURE_TIMESTAMP");score=1
        for key,label,val in (("duplicate_bar","DUPLICATE_BAR",.8),("timestamp_reversal","TIMESTAMP_REVERSAL",.9),("missing_bar","MISSING_BAR",.65),("frozen_price","FROZEN_PRICE",.6)):
            if m.get(key):issues.append(label);score=max(score,val)
        if abs(f(m.get("feed_divergence_bps"),0) or 0)>10:issues.append("DATA_FEED_DIVERGENCE");score=max(score,.75)
        return None if not issues else {"anomaly_type":"DATA_ANOMALY","subtype":"DATA_INTEGRITY_ANOMALY","score":score,"confidence":.95,"metrics":{"issues":issues},"baseline":{"deterministic":True},"symbols":[s.get("symbol")] if s.get("symbol") else []}
    def _ood(self,s):
        feats=s.get("features") or {};training=s.get("training_baseline") or {};scores={};supported=0
        for k,v in feats.items():
            x=f(v);b=training.get(k) or {};med=f(b.get("median"));md=f(b.get("mad"));q05=f(b.get("q05"));q95=f(b.get("q95"))
            if x is None or med is None:continue
            supported+=1
            z=abs(.6745*(x-med)/md) if md and md>1e-12 else abs(x-med)/max(((q95-q05)/3.29) if q05 is not None and q95 is not None else 1e9,1e-9)
            scores[k]=clamp((z-2)/6)
        if not scores:return None
        overall=1-math.prod(1-z for z in scores.values())
        return None if overall<.30 else {"anomaly_type":"DATA_ANOMALY","subtype":"OUT_OF_DISTRIBUTION_DATA","score":overall,"confidence":clamp(supported/max(3,len(feats))),"metrics":{"feature_scores":scores},"baseline":{"training_distribution":training},"symbols":[s.get("symbol")] if s.get("symbol") else []}
    def _correlation(self,s):
        cur=s.get("correlations") or {};base=s.get("correlation_baseline") or {};changes=[];conv=[]
        for pair,c in cur.items():
            cv=f(c);bv=f(base.get(pair))
            if cv is None or bv is None:continue
            d=abs(cv-bv)
            if d>=.35:changes.append((pair,d,bv,cv))
            if abs(cv)>=.80 and abs(bv)<.60:conv.append((pair,bv,cv))
        if not changes and not conv:return None
        return {"anomaly_type":"CORRELATION_ANOMALY","subtype":"CORRELATION_CONVERGENCE" if conv else "CORRELATION_REGIME_SHIFT","score":max(.75 if conv else 0,clamp(max([x[1] for x in changes],default=.35)/.7)),"confidence":.85,"metrics":{"changes":changes,"convergence":conv},"baseline":{"correlations":base},"symbols":[]}
    def _trade_history(self,strategy_id,ts,regime=None):
        c=self.conn()
        try:
            cols={r[1] for r in c.execute("PRAGMA table_info(trade_memory)").fetchall()}
            if not cols:
                c.close();return [],"NO_TRADE_MEMORY"
            where=["status='CLOSED'","strategy=?","COALESCE(exit_ts,entry_ts)<?"]
            params=[strategy_id,ts.isoformat()]
            if regime and "market_regime_entry" in cols:
                regime_rows=c.execute("SELECT COUNT(*) n FROM trade_memory WHERE "+" AND ".join(where)+
                                      " AND market_regime_entry=?",(strategy_id,ts.isoformat(),regime)).fetchone()["n"]
                if regime_rows>=self.baseline_min_samples:
                    where.append("market_regime_entry=?");params.append(regime);scope="STRATEGY_REGIME"
                else:scope="STRATEGY_GLOBAL_FALLBACK"
            else:scope="STRATEGY_GLOBAL"
            rows=[dict(x) for x in c.execute("SELECT * FROM trade_memory WHERE "+" AND ".join(where)+
                                             " ORDER BY COALESCE(exit_ts,entry_ts),id",tuple(params)).fetchall()]
        except sqlite3.OperationalError:
            rows=[];scope="NO_TRADE_MEMORY"
        c.close();return rows,scope

    @staticmethod
    def _trade_is_stop(row):
        try:
            reasons=json.loads(row.get("exit_reasons_json") or "[]")
        except Exception:reasons=[]
        text=" ".join(str(x).upper() for x in reasons)
        return int("STOP" in text or "SL" in text)

    @staticmethod
    def _tail_streak(values,target_positive):
        n=0
        for v in reversed(values):
            match=(v>0) if target_positive else (v<=0)
            if match:n+=1
            else:break
        return n

    def _strategy(self,s):
        """
        Strategy behavior anomaly combines real Trade Memory distributions with
        current signal-rate telemetry. A loss streak alone is not considered
        model failure; evidence/sample size determines severity.
        """
        issues=[];score=0.0;baselines={};affected=[]
        ts=parse_ts(s.get("timestamp")) or datetime.now(timezone.utc)
        regime=s.get("market_regime")
        strategy_metrics=s.get("strategy_metrics") or {}
        strategy_ids=set(strategy_metrics)
        strategy_ids.update(str(x) for x in (s.get("affected_strategies") or []))
        # If no explicit strategy list is supplied, recent Trade Memory strategies
        # can still be monitored without inventing model identities.
        if not strategy_ids:
            try:
                rows=self._rows("""SELECT DISTINCT strategy FROM trade_memory
                                   WHERE entry_ts<? ORDER BY strategy LIMIT 100""",(ts.isoformat(),))
                strategy_ids.update(r["strategy"] for r in rows if r.get("strategy"))
            except Exception:pass

        for sid in sorted(strategy_ids):
            m=strategy_metrics.get(sid) or {}
            normal=f(m.get("baseline_signal_rate"));cur=f(m.get("signal_rate"))
            if normal is not None and cur is not None:
                if normal>0 and cur>normal*10:
                    issues.append((sid,"SIGNAL_FLOOD",normal,cur));score=max(score,.90);affected.append(sid)
                elif normal>0 and cur==0 and m.get("opportunity_present"):
                    issues.append((sid,"UNEXPECTED_STRATEGY_SILENCE",normal,cur));score=max(score,.65);affected.append(sid)
            if (f(m.get("drawdown_acceleration"),0) or 0)>.7:
                issues.append((sid,"DRAWDOWN_ACCELERATION",m.get("drawdown_acceleration")));score=max(score,.75);affected.append(sid)

            trades,scope=self._trade_history(sid,ts,regime)
            if len(trades)<self.baseline_min_samples+5:
                baselines[sid]={"scope":scope,"samples":len(trades),"evidence":"LOW_EVIDENCE"}
                continue
            recent_n=max(5,min(20,len(trades)//3))
            hist=trades[:-recent_n];recent=trades[-recent_n:]
            if len(hist)<self.baseline_min_samples:
                baselines[sid]={"scope":scope,"samples":len(trades),"evidence":"LOW_EVIDENCE"}
                continue
            def nums(rows,key):
                return [f(r.get(key)) for r in rows if f(r.get(key)) is not None]
            hdur=nums(hist,"duration_seconds");rdur=nums(recent,"duration_seconds")
            hsize=[abs(x) for x in nums(hist,"position_size")];rsize=[abs(x) for x in nums(recent,"position_size")]
            hpnl=[f(r.get("net_result"),f(r.get("realized_pl"),0)) or 0 for r in hist]
            rpnl=[f(r.get("net_result"),f(r.get("realized_pl"),0)) or 0 for r in recent]
            hstop=[self._trade_is_stop(r) for r in hist];rstop=[self._trade_is_stop(r) for r in recent]
            hwin=sum(1 for x in hpnl if x>0)/len(hpnl) if hpnl else None
            rwin=sum(1 for x in rpnl if x>0)/len(rpnl) if rpnl else None
            hstop_rate=sum(hstop)/len(hstop) if hstop else None
            rstop_rate=sum(rstop)/len(rstop) if rstop else None
            details={"scope":scope,"historical_samples":len(hist),"recent_samples":len(recent),
                     "historical_win_rate":hwin,"recent_win_rate":rwin,
                     "historical_stop_hit_rate":hstop_rate,"recent_stop_hit_rate":rstop_rate}
            if hdur and rdur:
                rz=robust_z(statistics.median(rdur),hdur);details["duration_robust_z"]=rz
                if abs(rz)>=4:
                    issues.append((sid,"TRADE_DURATION_DISTRIBUTION_SHIFT",round(rz,3)));score=max(score,score_from_z(rz));affected.append(sid)
            if hsize and rsize:
                rz=robust_z(statistics.median(rsize),hsize);details["position_size_robust_z"]=rz
                if abs(rz)>=4:
                    issues.append((sid,"POSITION_SIZE_DISTRIBUTION_SHIFT",round(rz,3)));score=max(score,score_from_z(rz));affected.append(sid)
            if hstop_rate is not None and rstop_rate is not None and rstop_rate-hstop_rate>=.30 and len(recent)>=10:
                issues.append((sid,"STOP_HIT_FREQUENCY_SHIFT",round(hstop_rate,3),round(rstop_rate,3)))
                score=max(score,.65);affected.append(sid)
            if hwin is not None and rwin is not None and abs(rwin-hwin)>=.30 and len(recent)>=10:
                issues.append((sid,"WIN_LOSS_DISTRIBUTION_SHIFT",round(hwin,3),round(rwin,3)))
                score=max(score,.60);affected.append(sid)
            loss_streak=self._tail_streak(rpnl,False);win_streak=self._tail_streak(rpnl,True)
            details["recent_consecutive_losses"]=loss_streak;details["recent_consecutive_wins"]=win_streak
            # A streak requires a meaningful sample and is WATCH/ELEVATED evidence,
            # not an automatic declaration that the strategy is broken.
            if loss_streak>=6 and len(recent)>=10:
                issues.append((sid,"ABNORMAL_CONSECUTIVE_LOSSES",loss_streak))
                score=max(score,.55 if loss_streak<9 else .70);affected.append(sid)
            if win_streak>=7 and len(recent)>=10:
                issues.append((sid,"ABNORMAL_CONSECUTIVE_WINS",win_streak))
                score=max(score,.45 if win_streak<10 else .60);affected.append(sid)
            baselines[sid]={**details,"evidence":"SUFFICIENT"}
        if not issues:return None
        unique=sorted(set(affected))
        confidence=clamp(max((baselines.get(x,{}).get("historical_samples",0) for x in unique),default=0)/100)
        return {"anomaly_type":"STRATEGY_ANOMALY","subtype":"STRATEGY_BEHAVIOR_ANOMALY",
                "score":score,"confidence":max(.55,confidence),"metrics":{"issues":issues,"behavior":baselines},
                "baseline":{"strategy_behavior":baselines},"symbols":[],"strategies":unique}

    def _ensemble(self,s):
        e=s.get("ensemble") or {};dis=f(e.get("disagreement"));base=f(e.get("baseline_disagreement"));agr=f(e.get("agreement"));ba=f(e.get("baseline_agreement"))
        if dis is not None and base is not None and dis-base>=.35:return {"anomaly_type":"ENSEMBLE_ANOMALY","subtype":"ENSEMBLE_DISAGREEMENT_SHOCK","score":clamp((dis-base)/.6),"confidence":.85,"metrics":{"disagreement":dis,"baseline_disagreement":base},"baseline":{},"symbols":[]}
        if agr is not None and ba is not None and agr>=.95 and agr-ba>=.30:return {"anomaly_type":"ENSEMBLE_ANOMALY","subtype":"SUSPICIOUS_MODEL_CONVERGENCE","score":.7,"confidence":.75,"metrics":{"agreement":agr,"baseline_agreement":ba},"baseline":{},"symbols":[]}
    def _model_drift(self,s):
        out=[]
        for mid,m in (s.get("model_distributions") or {}).items():
            current=m.get("current") or {};baseline=m.get("baseline") or {}
            keys=set(current)|set(baseline)
            tv=.5*sum(abs((f(current.get(k),0) or 0)-(f(baseline.get(k),0) or 0)) for k in keys)
            if tv>=.30:
                out.append({"anomaly_type":"STRATEGY_ANOMALY","subtype":"PREDICTION_DISTRIBUTION_DRIFT",
                    "score":clamp(tv/.70),"confidence":.8,"metrics":{"model_id":mid,"total_variation":tv,
                    "current":current,"baseline":baseline},"baseline":baseline,"symbols":[],"strategies":[mid]})
        for mid,m in (s.get("confidence_distributions") or {}).items():
            cur=f(m.get("current_mean"));base=f(m.get("baseline_mean"));sd=f(m.get("baseline_mad"))
            if cur is None or base is None:continue
            delta=abs(cur-base);scale=max(sd or 0,.05)
            z=delta/scale
            if z>=3:
                out.append({"anomaly_type":"STRATEGY_ANOMALY","subtype":"CONFIDENCE_DISTRIBUTION_ANOMALY",
                    "score":clamp((z-2)/6),"confidence":.8,"metrics":{"model_id":mid,"current_mean":cur,
                    "baseline_mean":base,"robust_shift":z},"baseline":m,"symbols":[],"strategies":[mid]})
        return out
    def _execution(self,s):
        e=s.get("execution") or {};issues=[];score=0
        for k,bk,up in (("slippage_bps","baseline_slippage_bps",True),("latency_ms","baseline_latency_ms",True),("fill_rate","baseline_fill_rate",False),("rejection_rate","baseline_rejection_rate",True)):
            v=f(e.get(k));b=f(e.get(bk))
            if v is None or b is None:continue
            bad=v>max(b*2,b+1) if up else v<b-.25
            if bad:issues.append((k,b,v));score=max(score,.75)
        return None if not issues else {"anomaly_type":"EXECUTION_ANOMALY","subtype":"EXECUTION_ANOMALY","score":score,"confidence":.85,"metrics":{"issues":issues},"baseline":e,"symbols":[s.get("symbol")] if s.get("symbol") else []}
    def _portfolio(self,s):
        p=s.get("portfolio") or {};issues=[];score=0
        if (f(p.get("correlation_concentration"),0) or 0)>=.8:issues.append("DIVERSIFICATION_BREAKDOWN");score=max(score,.8)
        if (f(p.get("exposure_utilization"),0) or 0)>1:issues.append("EXPOSURE_UNEXPECTEDLY_HIGH");score=1
        if (f(p.get("portfolio_heat"),0) or 0)>=1:issues.append("PORTFOLIO_HEAT_HIGH");score=max(score,.9)
        if p.get("position_mismatch"):issues.append("POSITION_ANOMALY");score=1
        actual_pnl=f(p.get("actual_pnl"));expected_pnl=f(p.get("expected_pnl"));pnl_tol=abs(f(p.get("pnl_tolerance"),0.0) or 0.0)
        if actual_pnl is not None and expected_pnl is not None and abs(actual_pnl-expected_pnl)>max(pnl_tol,1e-9):
            issues.append("PNL_RECONCILIATION_ANOMALY");score=max(score,.9)
        subtype="DIVERSIFICATION_BREAKDOWN" if "DIVERSIFICATION_BREAKDOWN" in issues else "POSITION_ANOMALY" if "POSITION_ANOMALY" in issues else "PNL_RECONCILIATION_ANOMALY" if "PNL_RECONCILIATION_ANOMALY" in issues else "PORTFOLIO_ANOMALY"
        return None if not issues else {"anomaly_type":"PORTFOLIO_ANOMALY","subtype":subtype,"score":score,
        "confidence":.95 if p.get("position_mismatch") else .85,"metrics":{"issues":issues,**p},"baseline":{},"symbols":[]}
    def _broker_system(self,s):
        out=[];b=s.get("broker") or {};lat=f(b.get("latency_ms"));base=f(b.get("baseline_latency_ms"))
        if b.get("state_mismatch") or b.get("response_format_inconsistent"):out.append({"anomaly_type":"BROKER_ANOMALY","subtype":"BROKER_BEHAVIOR_ANOMALY","score":.9,"confidence":.95,"metrics":b,"baseline":{},"symbols":[]})
        if lat is not None and base and lat>max(base*3,base+1000):out.append({"anomaly_type":"BROKER_ANOMALY","subtype":"BROKER_LATENCY_ANOMALY","score":clamp((lat/base-1)/4),"confidence":.9,"metrics":{"latency_ms":lat,"baseline_latency_ms":base},"baseline":{},"symbols":[]})
        sys=s.get("system") or {};issues=[];score=0
        for k in ("cpu","memory","disk","queue_depth","db_latency_ms","event_rate"):
            v=f(sys.get(k));bv=f(sys.get("baseline_"+k))
            if v is not None and bv and v>bv*4:issues.append((k,bv,v));score=max(score,.75)
        if issues:out.append({"anomaly_type":"SYSTEM_ANOMALY","subtype":"SYSTEM_RESOURCE_ANOMALY","score":score,"confidence":.8,"metrics":{"issues":issues},"baseline":{},"symbols":[]})
        return out
    def _regime(self,s):
        rg=str(s.get("market_regime") or "UNKNOWN").upper();dist=f(s.get("regime_distance"));transition=f(s.get("regime_transition_score"))
        if rg in ("UNKNOWN","UNKNOWN_REGIME","OUT_OF_DISTRIBUTION_REGIME","UNCERTAIN","ABNORMAL_UNCERTAIN") or (dist is not None and dist>=.8):return {"anomaly_type":"MARKET_ANOMALY","subtype":"UNKNOWN_REGIME","score":max(.7,dist or 0),"confidence":.8,"metrics":{"regime":rg,"distance":dist},"baseline":{},"symbols":[s.get("symbol")] if s.get("symbol") else []}
        if transition is not None and transition>=.7:return {"anomaly_type":"MARKET_ANOMALY","subtype":"REGIME_TRANSITION","score":transition,"confidence":.75,"metrics":{"transition_score":transition,"regime":rg},"baseline":{},"symbols":[s.get("symbol")] if s.get("symbol") else []}
    def _change_point(self,s,metric):
        ts=parse_ts(s.get("timestamp")) or datetime.now(timezone.utc)
        vals=[]
        used_scope=None
        # Structural change must compare through time, so do not condition on
        # exact time-bucket/day scopes that could hide the transition itself.
        candidates=[x for x in self._context_candidates(s,ts)
                    if "TIME" not in x[0] and "DAY" not in x[0]
                    and x[0] not in ("SYMBOL","GLOBAL_SAFE_BASELINE")]
        for scope,filters,min_n in candidates:
            rows=self._context_rows(filters,ts,HORIZONS["LONG_TERM"])
            tmp=[]
            for r in rows:
                try:v=f(json.loads(r["metrics_json"] or "{}").get(metric))
                except Exception:v=None
                if v is not None:tmp.append(v)
            if len(tmp)>=self.structural_min_points*2:
                vals=tmp;used_scope=scope;break
        if len(vals)<self.structural_min_points*2:return None
        n=max(self.structural_min_points,min(len(vals)//4,30));b=vals[-n:]
        # Keep an embargo window between historical baseline and current window.
        # This reduces contamination when the structural shift has already persisted
        # for several observations.
        a=vals[:-2*n] if len(vals)>=3*n else vals[:-n]
        if len(a)<self.structural_min_points:return None
        ma=statistics.median(a);mb=statistics.median(b)
        # Same robust floor principle as robust_z, otherwise a stable context
        # with near-zero dispersion would flag harmless session transitions.
        scale=max(mad(a) or 0,
                  ((percentile(a,.75) or ma)-(percentile(a,.25) or ma))/1.349 if a else 0,
                  abs(ma)*.05,1e-9)
        shift=abs(mb-ma)/scale
        persistence=sum(1 for x in b if abs(x-ma)>2*scale)/len(b)
        if shift<3 or persistence<.7:return None
        return {"anomaly_type":"MARKET_ANOMALY","subtype":"STRUCTURAL_CHANGE_DETECTED",
                "score":clamp((shift-2)/6),"confidence":clamp(len(b)/30),
                "metrics":{"metric":metric,"previous_median":ma,"recent_median":mb,
                           "shift_robust":shift,"persistence":persistence,"baseline_scope":used_scope},
                "baseline":{"scope":used_scope,"previous_window":a[-10:]},
                "symbols":[s.get("symbol")] if s.get("symbol") else []}

    def _recommendations(self,events,composite):
        rec=["LOG_AND_INVESTIGATE"];types={e["subtype"] for e in events}
        symbols=sorted({x for e in events for x in (e.get("symbols") or []) if x})
        strategies=sorted({x for e in events for x in (e.get("strategies") or []) if x})
        domains={e["anomaly_type"] for e in events}
        if types & {"OUT_OF_DISTRIBUTION_DATA","UNKNOWN_REGIME","ENSEMBLE_DISAGREEMENT_SHOCK",
                    "CONFIDENCE_DISTRIBUTION_ANOMALY","PREDICTION_DISTRIBUTION_DRIFT"}:
            rec.append("REDUCE_MODEL_CONFIDENCE")
        if any("LIQUIDITY" in x or "SPREAD" in x for x in types):
            rec += ["CAPITAL_ALLOCATION_RISK_OFF_BIAS","SMART_EXECUTION_CAUTIOUS"]
        if any("CORRELATION" in x or "DIVERSIFICATION" in x for x in types):
            rec.append("REDUCE_CORRELATED_ALLOCATIONS")
        # Isolation is advisory. A local anomaly should not unnecessarily stop all symbols/strategies.
        systemic_domains={"SYSTEM_ANOMALY","BROKER_ANOMALY","DATA_ANOMALY"}
        if symbols and len(symbols)==1 and not (domains & systemic_domains) and len(domains)<=2:
            rec.append("ISOLATE_SYMBOL")
        if strategies and len(strategies)==1 and not (domains & systemic_domains) and len(domains)<=2:
            rec.append("ISOLATE_STRATEGY")
        if len(domains)>=3 or bool(domains & {"SYSTEM_ANOMALY","BROKER_ANOMALY"}):
            rec.append("SYSTEM_WIDE_RESPONSE")
        if composite>=.70:rec += ["RISK_ENGINE_REDUCE_OR_BLOCK","LIMITED_ADAPTATION"]
        if composite>=.85:rec += ["CAPITAL_ALLOCATION_RISK_OFF","GOVERNANCE_ADAPTATION_FROZEN"]
        return list(dict.fromkeys(rec))

    def _persist_event(self,event,s):
        scope=event["subtype"]+":"+(",".join(event.get("symbols") or []) or "SYSTEM");now=parse_ts(s.get("timestamp")) or datetime.now(timezone.utc);score=clamp(event["score"]);sev=severity(score);active=self._rows("SELECT * FROM anomaly_events WHERE subtype=? AND scope=? AND lifecycle_state IN ('DETECTED','ACTIVE','STABILIZING') ORDER BY last_update DESC LIMIT 1",(event["subtype"],scope))
        if active:
            a=active[0];ctx=json.loads(a["context_json"] or "{}");confirm=int(ctx.get("confirmations",1))+1;new="ACTIVE" if confirm>=self.persistence_confirmations and score>=self.enter_threshold else a["lifecycle_state"];c=self.conn();c.execute("UPDATE anomaly_events SET severity=?,current_score=?,peak_score=max(peak_score,?),confidence=?,lifecycle_state=?,last_update=?,duration_seconds=?,supporting_metrics_json=?,baseline_reference_json=?,context_json=? WHERE anomaly_id=?",(sev,score,score,event["confidence"],new,now.isoformat(),max(0,(now-(parse_ts(a["anomaly_start"]) or now)).total_seconds()),j(event["metrics"]),j(event["baseline"]),j({"confirmations":confirm,"normal_confirmations":0}),a["anomaly_id"]));
            if new!=a["lifecycle_state"] or sev!=a["severity"]:c.execute("INSERT INTO anomaly_transitions(anomaly_id,ts,from_state,to_state,from_severity,to_severity,score,reason) VALUES(?,?,?,?,?,?,?,?)",(a["anomaly_id"],now.isoformat(),a["lifecycle_state"],new,a["severity"],sev,score,"PERSISTENCE_OR_ESCALATION"))
            c.commit();c.close();return a["anomaly_id"]
        aid="anom_"+uuid.uuid4().hex;state="ACTIVE" if self.persistence_confirmations<=1 and score>=self.enter_threshold else "DETECTED";c=self.conn();c.execute("""INSERT INTO anomaly_events(anomaly_id,anomaly_type,subtype,severity,anomaly_score,confidence,lifecycle_state,scope,affected_symbols_json,affected_strategies_json,detected_at,anomaly_start,last_update,duration_seconds,peak_score,current_score,recovery_progress,supporting_metrics_json,baseline_reference_json,horizons_json,context_json,recommendations_json,fingerprint,engine_version,mode) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(aid,event["anomaly_type"],event["subtype"],sev,score,event["confidence"],state,scope,j(event.get("symbols") or []),j(event.get("strategies") or []),now.isoformat(),now.isoformat(),now.isoformat(),0,score,score,0,j(event["metrics"]),j(event["baseline"]),j(event.get("horizons") or {}),j({"confirmations":1,"normal_confirmations":0}),j([]),fingerprint(event["metrics"]),self.version,self.mode));c.commit();c.close();return aid
    def _recover_missing(self,seen,ts):
        rows=self._rows("SELECT * FROM anomaly_events WHERE lifecycle_state IN ('DETECTED','ACTIVE','STABILIZING')");c=self.conn()
        for a in rows:
            if a["subtype"] in seen:continue
            ctx=json.loads(a["context_json"] or "{}");normal=int(ctx.get("normal_confirmations",0))+1;old=a["lifecycle_state"];new="STABILIZING" if old in ("DETECTED","ACTIVE") else old
            if normal>=self.recovery_confirmations:new="RECOVERED"
            c.execute("UPDATE anomaly_events SET lifecycle_state=?,recovery_progress=?,last_update=?,recovered_at=?,context_json=? WHERE anomaly_id=?",(new,clamp(normal/self.recovery_confirmations),ts.isoformat(),ts.isoformat() if new=="RECOVERED" else None,j({**ctx,"normal_confirmations":normal}),a["anomaly_id"]))
            if new!=old:c.execute("INSERT INTO anomaly_transitions(anomaly_id,ts,from_state,to_state,from_severity,to_severity,score,reason) VALUES(?,?,?,?,?,?,?,?)",(a["anomaly_id"],ts.isoformat(),old,new,a["severity"],a["severity"],a["current_score"],"HYSTERESIS_RECOVERY"))
        c.commit();c.close()
    def evaluate(self,s):
        self.ensure_schema();ts=parse_ts(s.get("timestamp")) or datetime.now(timezone.utc);self.observe(s);events=[];d=self._data_integrity(s)
        if d:events.append(d)
        for metric,atype,sub in (("return_abs","MARKET_ANOMALY","PRICE_ANOMALY"),("volatility","VOLATILITY_ANOMALY","VOLATILITY_SHOCK"),("volume","MARKET_ANOMALY","ABNORMAL_VOLUME"),("spread_bps","LIQUIDITY_ANOMALY","SPREAD_ANOMALY"),("available_liquidity","LIQUIDITY_ANOMALY","LIQUIDITY_SHOCK")):
            a=self._metric_anomaly(s,metric,atype,sub)
            if a:events.append(a)
        for a in (self._ood(s),self._correlation(s),self._strategy(s),self._ensemble(s),self._execution(s),self._portfolio(s),self._regime(s)):
            if a:events.append(a)
        events.extend(self._model_drift(s))
        events.extend(self._broker_system(s))
        for metric in ("volatility","spread_bps","available_liquidity","correlation_mean"):
            a=self._change_point(s,metric)
            if a:events.append(a)
        ids=[];seen=set()
        for e in events:seen.add(e["subtype"]);ids.append(self._persist_event(e,s))
        self._recover_missing(seen,ts)
        scores=[clamp(e["score"]) for e in events if e["score"]>=.20];domains=len(set(e["anomaly_type"] for e in events))
        # Hysteresis: active/stabilizing anomalies continue contributing until enough
        # normal confirmations exist. One normal reading cannot instantly clear defense.
        lingering=self._rows("SELECT anomaly_type,current_score,recovery_progress,lifecycle_state FROM anomaly_events WHERE lifecycle_state IN ('DETECTED','ACTIVE','STABILIZING')")
        for a in lingering:
            decay=(1-.70*clamp(a.get("recovery_progress") or 0)) if a.get("lifecycle_state")=="STABILIZING" else 1.0
            v=clamp((f(a.get("current_score"),0) or 0)*decay)
            if v>=.20:scores.append(v)
        domains=max(domains,len(set(a.get("anomaly_type") for a in lingering if a.get("anomaly_type"))))
        ind=1-math.prod(1-x for x in scores) if scores else 0;bonus=min(.20,max(0,domains-1)*.04);composite=clamp(ind+bonus);conf=sum(e["confidence"] for e in events)/len(events) if events else (.65 if lingering else .5);rec=self._recommendations(events,composite);rare=None
        if composite>=.70:
            feats={"composite":composite,"domains":domains,**{e["subtype"]:e["score"] for e in events}};fp=fingerprint(feats)
            open_rare=self._rows("SELECT * FROM anomaly_rare_events WHERE ended_at IS NULL ORDER BY started_at DESC LIMIT 1")
            c=self.conn()
            rare_context={**s,"_anomaly_features":feats,"_anomaly_ids":ids,
                          "_anomaly_types":[{"type":e["anomaly_type"],"subtype":e["subtype"],"score":e["score"]} for e in events]}
            affected_strategies=sorted({x for e in events for x in (e.get("strategies") or []) if x})
            governance_response={"recommended":"ADAPTATION_FROZEN" if composite>=.85 else
                                 "LIMITED_ADAPTATION" if composite>=.70 else "NORMAL_ADAPTATION",
                                 "direct_action":False}
            data_quality={"status":str(s.get("data_quality") or "UNKNOWN").upper(),
                          "ood":any(e["subtype"]=="OUT_OF_DISTRIBUTION_DATA" for e in events),
                          "integrity_anomaly":any(e["subtype"]=="DATA_INTEGRITY_ANOMALY" for e in events)}
            if open_rare:
                rare=open_rare[0]["rare_event_id"]
                c.execute("""UPDATE anomaly_rare_events SET peak_composite_score=max(peak_composite_score,?),
                  during_context_json=?,risk_actions_json=?,execution_behavior_json=?,portfolio_impact_json=?,
                  feature_vector_json=?,data_quality_json=?,affected_strategies_json=?,governance_response_json=?
                  WHERE rare_event_id=?""",(composite,j(rare_context),j(rec),j(s.get("execution") or {}),
                  j(s.get("portfolio") or {}),j(feats),j(data_quality),j(affected_strategies),
                  j(governance_response),rare))
            else:
                similar=self.similar_events(feats,1)
                cluster=similar[0]["cluster_label"] if similar and similar[0]["similarity"]>=.8 else "UNKNOWN_EVENT_TYPE"
                rare="rare_"+uuid.uuid4().hex
                c.execute("""INSERT INTO anomaly_rare_events(
                  rare_event_id,started_at,event_fingerprint,cluster_label,peak_composite_score,
                  before_context_json,during_context_json,affected_trades_json,risk_actions_json,
                  execution_behavior_json,portfolio_impact_json,feature_vector_json,data_quality_json,
                  affected_strategies_json,governance_response_json,recovery_process_json)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (rare,ts.isoformat(),fp,cluster,composite,j(s.get("before_context") or {}),j(rare_context),
                   j(s.get("affected_trades") or []),j(rec),j(s.get("execution") or {}),
                   j(s.get("portfolio") or {}),j(feats),j(data_quality),j(affected_strategies),
                   j(governance_response),j([{"ts":ts.isoformat(),"state":"DETECTED"}])))
            for aid in ids:c.execute("UPDATE anomaly_events SET rare_event_id=? WHERE anomaly_id=?",(rare,aid))
            c.commit();c.close()
        # False-positive control: a single statistical domain does not immediately
        # receive full defensive authority. Multi-domain confirmation, deterministic
        # integrity failures, or an already ACTIVE/STABILIZING event can confirm immediately.
        states=self._rows("SELECT lifecycle_state FROM anomaly_events WHERE anomaly_id IN (%s)" %
                          (",".join("?" for _ in ids) if ids else "''"),tuple(ids)) if ids else []
        active_confirmed=any(x.get("lifecycle_state") in ("ACTIVE","STABILIZING") for x in states)
        deterministic_critical=any(e.get("baseline",{}).get("deterministic") and e.get("score",0)>=.85 for e in events)
        confirmed=bool(domains>=3 or deterministic_critical or active_confirmed)
        actionable=composite if confirmed else min(composite,.49)
        actionable_sev=severity(actionable)
        actionable_rec=self._recommendations(events,actionable)
        c=self.conn();c.execute("INSERT INTO anomaly_composite_history(ts,global_score,severity,confidence,cross_domain_score,active_anomalies_json,context_json,recommendations_json,rare_event_id) VALUES(?,?,?,?,?,?,?,?,?)",(ts.isoformat(),composite,severity(composite),conf,clamp(composite+bonus),j([{"type":e["anomaly_type"],"subtype":e["subtype"],"score":e["score"]} for e in events]),j({"market_regime":s.get("market_regime"),"symbol":s.get("symbol"),"mode":self.mode,"confirmed":confirmed,"actionable_score":actionable,"actionable_severity":actionable_sev}),j(actionable_rec),rare));c.commit();c.close()
        return {"mode":self.mode,"anomaly_score":max(scores,default=0),"composite_anomaly_score":composite,
                "severity":severity(composite),"actionable_anomaly_score":actionable,
                "actionable_severity":actionable_sev,"confirmed":confirmed,"confidence":conf,
                "anomalies":events,"anomaly_ids":ids,"rare_event_id":rare,"recommendations":actionable_rec,
                "signal_authority":False,"risk_increase_authority":False,"direct_control_authority":False}
    def integration_context(self,result):
        score=clamp((result or {}).get("actionable_anomaly_score",(result or {}).get("composite_anomaly_score",0)))
        sev=(result or {}).get("actionable_severity",(result or {}).get("severity","NORMAL"))
        return {
          "anomaly_score":score,"severity":sev,
          "ensemble_confidence_multiplier":1.0 if score<.30 else .85 if score<.50 else .65 if score<.70 else .40 if score<.85 else .20,
          "allocation_risk_off":bool(score>=.85),
          "allocation_reduce":bool(score>=.50),
          "risk_engine_recommendation":"EMERGENCY_REVIEW" if sev=="CRITICAL" else "BLOCK_OR_REDUCE" if sev=="HIGH" else "REDUCE" if sev=="ELEVATED" else "NORMAL",
          "smart_execution_recommendation":"BLOCK_AFFECTED" if score>=.85 else "CAUTIOUS" if score>=.50 else "NORMAL",
          "governance_recommendation":"ADAPTATION_FROZEN" if score>=.85 else "LIMITED_ADAPTATION" if score>=.70 else "NORMAL_ADAPTATION",
          "direct_actions":False
        }

    def clustering_status(self):
        rows=self._rows("SELECT COUNT(*) n FROM anomaly_rare_events")
        n=int(rows[0]["n"]) if rows else 0
        status="CLUSTERING_AVAILABLE" if n>=self.rare_event_cluster_min_events else "INSUFFICIENT_EVENT_HISTORY"
        state={"status":status,"event_count":n,"minimum_required":self.rare_event_cluster_min_events}
        c=self.conn();c.execute("""INSERT INTO anomaly_cluster_state(singleton,status,event_count,min_events,last_evaluated,clusters_json)
          VALUES(1,?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET status=excluded.status,event_count=excluded.event_count,
          min_events=excluded.min_events,last_evaluated=excluded.last_evaluated""",
          (status,n,self.rare_event_cluster_min_events,now_iso(),"{}"));c.commit();c.close()
        return state

    @staticmethod
    def _rare_family(feature_vector):
        keys=set(feature_vector or {})
        if any("LIQUIDITY" in k or "SPREAD" in k for k in keys):return "LIQUIDITY_SHOCK"
        if any("VOLATILITY" in k for k in keys):return "VOLATILITY_SHOCK"
        if any("CORRELATION" in k or "DIVERSIFICATION" in k for k in keys):return "CORRELATION_BREAKDOWN"
        if any("DATA_" in k or "OUT_OF_DISTRIBUTION" in k for k in keys):return "DATA_FAILURE"
        if any("BROKER_" in k for k in keys):return "BROKER_FAILURE"
        if len(keys)>=4:return "COMPOSITE_MARKET_STRESS"
        return "UNKNOWN_EVENT_TYPE"

    def cluster_rare_events(self):
        """
        Explainable, non-ML clustering readiness. It never invents clusters before
        the configured minimum history exists.
        """
        st=self.clustering_status()
        if st["status"]!="CLUSTERING_AVAILABLE":
            return {**st,"clusters":{},"activated":False}
        rows=self._rows("SELECT rare_event_id,feature_vector_json,cluster_label FROM anomaly_rare_events ORDER BY started_at")
        groups={}
        c=self.conn()
        for r in rows:
            try:fv=json.loads(r.get("feature_vector_json") or "{}")
            except Exception:fv={}
            label=self._rare_family(fv)
            groups.setdefault(label,[]).append(r["rare_event_id"])
            c.execute("UPDATE anomaly_rare_events SET cluster_label=? WHERE rare_event_id=?",(label,r["rare_event_id"]))
        c.execute("""UPDATE anomaly_cluster_state SET status='CLUSTERING_ACTIVE',event_count=?,
                     last_evaluated=?,clusters_json=? WHERE singleton=1""",(len(rows),now_iso(),j(groups)))
        c.commit();c.close()
        return {"status":"CLUSTERING_ACTIVE","event_count":len(rows),
                "minimum_required":self.rare_event_cluster_min_events,"clusters":groups,"activated":True}

    def similar_events(self,features,limit=5,min_similarity=.55):
        """
        Historical similarity only; never returned as a prediction.
        """
        rows=self._rows("SELECT * FROM anomaly_rare_events ORDER BY started_at DESC LIMIT 500")
        target={k:f(v) for k,v in features.items() if f(v) is not None};out=[]
        for r in rows:
            try:other={k:f(v) for k,v in json.loads(r.get("feature_vector_json") or "{}").items() if f(v) is not None}
            except Exception:other={}
            common=set(target)&set(other)
            if common:
                # normalized relative distance, capped per feature to prevent one
                # dimension from dominating.
                ds=[min(1.0,abs(target[k]-other[k])/(abs(target[k])+abs(other[k])+1e-6)) for k in common]
                coverage=len(common)/max(1,len(set(target)|set(other)))
                sim=clamp((1-statistics.mean(ds))*.7+coverage*.3)
            else:sim=0.0
            out.append({"rare_event_id":r["rare_event_id"],"similarity":sim,
                        "classification":"SIMILAR_HISTORICAL_EVENT" if sim>=min_similarity else "UNKNOWN_EVENT_TYPE",
                        "cluster_label":r["cluster_label"],"started_at":r["started_at"],
                        "market_context":json.loads(r.get("during_context_json") or "{}"),
                        "risk_response":json.loads(r.get("risk_actions_json") or "[]"),
                        "outcome":json.loads(r.get("outcome_json") or "{}"),
                        "prediction":False})
        ranked=sorted(out,key=lambda x:x["similarity"],reverse=True)[:limit]
        if not ranked or ranked[0]["similarity"]<min_similarity:
            return [{"classification":"UNKNOWN_EVENT_TYPE","similarity":ranked[0]["similarity"] if ranked else 0.0,
                     "prediction":False,"message":"No sufficiently similar historical event."}]
        return ranked

    def recover_event(self,rare_event_id,after_context,outcome,recovery_step="RECOVERED"):
        c=self.conn();r=c.execute("SELECT * FROM anomaly_rare_events WHERE rare_event_id=?",(rare_event_id,)).fetchone()
        if not r:c.close();return False
        end=datetime.now(timezone.utc);start=parse_ts(r["started_at"]) or end
        try:steps=json.loads(r["recovery_process_json"] or "[]")
        except Exception:steps=[]
        steps.append({"ts":end.isoformat(),"state":recovery_step,"context":after_context})
        c.execute("""UPDATE anomaly_rare_events SET ended_at=?,after_context_json=?,recovery_time_seconds=?,
                     outcome_json=?,recovery_process_json=? WHERE rare_event_id=?""",
                  (end.isoformat(),j(after_context),(end-start).total_seconds(),j(outcome),j(steps),rare_event_id))
        c.commit();c.close();return True

    def period_label(self,ts_value):
        """Classify learning data without rewriting historical trades."""
        ts=parse_ts(ts_value)
        if not ts:return {"label":"UNKNOWN_TIME","rare_event_ids":[],"anomaly_ids":[]}
        rare=self._rows("""SELECT rare_event_id FROM anomaly_rare_events
          WHERE started_at<=? AND COALESCE(ended_at,?)>=?""",(ts.isoformat(),ts.isoformat(),ts.isoformat()))
        if rare:
            return {"label":"RARE_EVENT_DATA","rare_event_ids":[x["rare_event_id"] for x in rare],"anomaly_ids":[]}
        anomalies=self._rows("""SELECT anomaly_id FROM anomaly_events
          WHERE severity IN ('HIGH','CRITICAL') AND anomaly_start<=?
            AND COALESCE(recovered_at,last_update)>=?""",(ts.isoformat(),ts.isoformat()))
        if anomalies:
            return {"label":"ANOMALOUS_PERIOD","rare_event_ids":[],"anomaly_ids":[x["anomaly_id"] for x in anomalies]}
        return {"label":"NORMAL_DATA","rare_event_ids":[],"anomaly_ids":[]}

    def record_shadow_outcome(self,anomaly_id,hypothetical_action,classification,actual_outcome=None,detection_delay_seconds=None,details=None):
        allowed={"TRUE_POSITIVE","FALSE_POSITIVE","FALSE_NEGATIVE","TRUE_NEGATIVE","UNRESOLVED"}
        if classification not in allowed:raise ValueError("INVALID_SHADOW_CLASSIFICATION")
        c=self.conn();c.execute("INSERT INTO anomaly_shadow_evaluations(ts,anomaly_id,hypothetical_action,actual_outcome,classification,detection_delay_seconds,details_json) VALUES(?,?,?,?,?,?,?)",(now_iso(),anomaly_id,hypothetical_action,actual_outcome,classification,detection_delay_seconds,j(details or {})));c.commit();c.close()
        return self.shadow_metrics()
    def shadow_metrics(self):
        rows=self._rows("SELECT * FROM anomaly_shadow_evaluations")
        counts={k:0 for k in ("TRUE_POSITIVE","FALSE_POSITIVE","FALSE_NEGATIVE","TRUE_NEGATIVE","UNRESOLVED")}
        for r in rows:counts[r.get("classification","UNRESOLVED")]=counts.get(r.get("classification","UNRESOLVED"),0)+1
        tp,fp,fn=counts["TRUE_POSITIVE"],counts["FALSE_POSITIVE"],counts["FALSE_NEGATIVE"]
        delays=[f(r.get("detection_delay_seconds")) for r in rows if f(r.get("detection_delay_seconds")) is not None]
        return {"samples":len(rows),"counts":counts,"precision":tp/max(1,tp+fp),"recall":tp/max(1,tp+fn),"false_positive_rate":fp/max(1,fp+counts["TRUE_NEGATIVE"]),"mean_detection_delay_seconds":statistics.mean(delays) if delays else None,"cost_sensitive_note":"Critical false negatives must be weighted more heavily than mild false positives during validation."}
    def dashboard(self):
        active=self._rows("SELECT * FROM anomaly_events WHERE lifecycle_state!='RECOVERED' ORDER BY current_score DESC,last_update DESC LIMIT 100");comp=self._rows("SELECT * FROM anomaly_composite_history ORDER BY ts DESC LIMIT 1");cur=comp[0] if comp else {}
        return {"enabled":True,"mode":self.mode,"engine_version":self.version,"global_anomaly_status":cur.get("severity","NORMAL"),"anomaly_score":max([f(x.get("current_score"),0) or 0 for x in active],default=0),"composite_score":f(cur.get("global_score"),0) or 0,"current_anomalies":[{"id":x["anomaly_id"],"type":x["anomaly_type"],"subtype":x["subtype"],"severity":x["severity"],"score":x["current_score"],"state":x["lifecycle_state"]} for x in active],"signal_authority":False,"risk_increase_authority":False,"direct_control_authority":False,"shadow_evaluation":self.shadow_metrics(),"activation_path":["SHADOW","VALIDATION","PAPER_REPLAY","CANARY_CONTROLS","LIMITED_INTEGRATION"]}
    def timeline(self,limit=200): return self._rows("SELECT t.*,e.anomaly_type,e.subtype FROM anomaly_transitions t LEFT JOIN anomaly_events e ON e.anomaly_id=t.anomaly_id ORDER BY t.ts DESC LIMIT ?",(int(limit),))

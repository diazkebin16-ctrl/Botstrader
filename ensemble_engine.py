from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
import json, math, sqlite3, statistics, uuid

DIRECTIONS=("LONG","SHORT","NEUTRAL","ABSTAIN")
MODES=("SHADOW","PAPER","CANARY","LIMITED_ENSEMBLE","PRODUCTION_ENSEMBLE")
BASELINES=("MAJORITY","WEIGHTED","CONFIDENCE_WEIGHTED","PERFORMANCE_WEIGHTED","REGIME_WEIGHTED")


def now_iso()->str:
    return datetime.now(timezone.utc).isoformat()

def parse_ts(v:Any)->Optional[datetime]:
    if v is None:return None
    if isinstance(v,datetime):return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    try:
        d=datetime.fromisoformat(str(v).replace("Z","+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:return None

def finite(v:Any,default:Optional[float]=None)->Optional[float]:
    try:
        x=float(v);return x if math.isfinite(x) else default
    except Exception:return default

def clamp(x:float,lo:float,hi:float)->float:return max(lo,min(hi,x))
def canonical(x:Any)->str:return json.dumps(x,separators=(",",":"),sort_keys=True,default=str)
def dir_sign(direction:str)->float:
    d=str(direction).upper();return 1.0 if d=="LONG" else -1.0 if d=="SHORT" else 0.0

@dataclass
class StandardSignal:
    strategy_id:str
    strategy_version:str
    symbol:str
    timestamp:str
    direction:str
    confidence:float
    expected_edge:Optional[float]
    market_regime:Optional[str]
    time_horizon:str
    signal_strength:float
    risk_characteristics:Dict[str,Any]
    data_quality:float
    family:str
    input_dependencies:List[str]
    role:str="DIRECTIONAL"  # DIRECTIONAL | CALIBRATOR | CONTEXT
    ttl_seconds:int=300
    status:str="ONLINE"
    metadata:Dict[str,Any]=None

class EnsembleEngine:
    """Step 17 Ensemble Engine.

    SHADOW by default. It has no order authority and no risk-increase authority.
    Correlated models and families are explicitly discounted before agreement is computed.
    """
    def __init__(self,db_path:str,version:str="3.25",mode:str="SHADOW",max_model_weight:float=.40,
                 max_family_weight:float=.55,min_sample_size:int=30,correlation_threshold:float=.75,
                 weight_change_limit:float=.10,weight_cooldown_hours:int=24,
                 min_observation_window_hours:int=24,min_active_directional:int=2,
                 default_signal_ttl_seconds:int=300):
        self.db_path=db_path;self.version=version;self.mode=mode if mode in MODES else "SHADOW"
        self.max_model_weight=clamp(float(max_model_weight),.05,.75)
        self.max_family_weight=clamp(float(max_family_weight),.10,.90)
        self.min_sample_size=max(5,int(min_sample_size))
        self.correlation_threshold=clamp(float(correlation_threshold),.40,.99)
        self.weight_change_limit=clamp(float(weight_change_limit),.01,.50)
        self.weight_cooldown_hours=max(1,int(weight_cooldown_hours))
        self.min_observation_window_hours=max(1,int(min_observation_window_hours))
        self.min_active_directional=max(1,int(min_active_directional))
        self.default_signal_ttl_seconds=max(10,int(default_signal_ttl_seconds))

    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30);c.row_factory=sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA synchronous=FULL");c.execute("PRAGMA busy_timeout=5000")
        return c

    def ensure_schema(self):
        c=self.conn();c.executescript("""
        CREATE TABLE IF NOT EXISTS ensemble_model_registry(
          strategy_id TEXT PRIMARY KEY,strategy_version TEXT NOT NULL,family TEXT NOT NULL,role TEXT NOT NULL,
          input_dependencies_json TEXT NOT NULL,time_horizon TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 1,
          max_weight REAL,registered_at TEXT NOT NULL,updated_at TEXT NOT NULL,metadata_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS ensemble_signals(
          signal_id TEXT PRIMARY KEY,ensemble_cycle_id TEXT,strategy_id TEXT NOT NULL,strategy_version TEXT NOT NULL,
          symbol TEXT NOT NULL,ts TEXT NOT NULL,direction TEXT NOT NULL,confidence REAL NOT NULL,expected_edge REAL,
          market_regime TEXT,time_horizon TEXT NOT NULL,signal_strength REAL NOT NULL,risk_characteristics_json TEXT NOT NULL,
          data_quality REAL NOT NULL,family TEXT NOT NULL,input_dependencies_json TEXT NOT NULL,role TEXT NOT NULL,
          ttl_seconds INTEGER NOT NULL,status TEXT NOT NULL,metadata_json TEXT NOT NULL DEFAULT '{}',resolved_label INTEGER,
          resolved_return REAL,resolved_ts TEXT);
        CREATE INDEX IF NOT EXISTS idx_ensemble_signal_model_ts ON ensemble_signals(strategy_id,ts);
        CREATE INDEX IF NOT EXISTS idx_ensemble_signal_symbol_ts ON ensemble_signals(symbol,ts);
        CREATE TABLE IF NOT EXISTS ensemble_weight_versions(
          ensemble_weight_version TEXT PRIMARY KEY,created_at TEXT NOT NULL,method TEXT NOT NULL,regime TEXT,
          weights_json TEXT NOT NULL,family_weights_json TEXT NOT NULL,evidence_json TEXT NOT NULL,
          parent_version TEXT,status TEXT NOT NULL DEFAULT 'ACTIVE_SHADOW',auto_deploy INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS ensemble_outputs(
          ensemble_decision_id TEXT PRIMARY KEY,ensemble_cycle_id TEXT,symbol TEXT NOT NULL,ts TEXT NOT NULL,mode TEXT NOT NULL,
          method TEXT NOT NULL,ensemble_direction TEXT NOT NULL,ensemble_confidence REAL NOT NULL,agreement_score REAL NOT NULL,
          disagreement_score REAL NOT NULL,diversity_score REAL NOT NULL,weighted_expected_edge REAL,
          expected_execution_cost REAL,expected_net_edge REAL,market_regime TEXT,data_quality REAL,
          participating_models_json TEXT NOT NULL,abstaining_models_json TEXT NOT NULL,offline_models_json TEXT NOT NULL,
          model_contributions_json TEXT NOT NULL,correlation_json TEXT NOT NULL,families_json TEXT NOT NULL,
          reasoning_summary_json TEXT NOT NULL,ensemble_weight_version TEXT,hypothetical_only INTEGER NOT NULL DEFAULT 1,
          director_review_json TEXT NOT NULL DEFAULT '{}',risk_review_json TEXT NOT NULL DEFAULT '{}');
        CREATE INDEX IF NOT EXISTS idx_ensemble_outputs_symbol_ts ON ensemble_outputs(symbol,ts);
        CREATE TABLE IF NOT EXISTS ensemble_shadow_comparisons(
          comparison_id TEXT PRIMARY KEY,ensemble_decision_id TEXT NOT NULL,ts TEXT NOT NULL,current_direction TEXT,
          current_confidence REAL,current_executed INTEGER,ensemble_direction TEXT,ensemble_confidence REAL,
          actual_result REAL,hypothetical_result REAL,real_result_source TEXT,hypothetical_result_source TEXT,
          details_json TEXT NOT NULL DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS ensemble_policy_candidates(
          candidate_id TEXT PRIMARY KEY,created_at TEXT NOT NULL,parent_weight_version TEXT,proposal_json TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'RESEARCH_ONLY',validation_state TEXT NOT NULL DEFAULT 'PENDING',
          evidence_json TEXT NOT NULL DEFAULT '{}',auto_deploy INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS ensemble_alerts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,event_type TEXT NOT NULL,severity TEXT NOT NULL,
          symbol TEXT,strategy_id TEXT,ensemble_decision_id TEXT,message TEXT,details_json TEXT NOT NULL DEFAULT '{}');
        """);c.commit();c.close()

    def register_model(self,strategy_id:str,strategy_version:str,family:str,role:str,input_dependencies:List[str],
                       time_horizon:str,max_weight:Optional[float]=None,metadata:Optional[Dict[str,Any]]=None):
        c=self.conn();c.execute("""INSERT INTO ensemble_model_registry(strategy_id,strategy_version,family,role,
          input_dependencies_json,time_horizon,enabled,max_weight,registered_at,updated_at,metadata_json)
          VALUES(?,?,?,?,?,?,1,?,?,?,?) ON CONFLICT(strategy_id) DO UPDATE SET strategy_version=excluded.strategy_version,
          family=excluded.family,role=excluded.role,input_dependencies_json=excluded.input_dependencies_json,
          time_horizon=excluded.time_horizon,max_weight=excluded.max_weight,updated_at=excluded.updated_at,
          metadata_json=excluded.metadata_json""",
          (strategy_id,strategy_version,family,role,canonical(input_dependencies),time_horizon,max_weight,now_iso(),now_iso(),canonical(metadata or {})))
        c.commit();c.close()

    def registry(self)->List[Dict[str,Any]]:
        c=self.conn();rows=[dict(x) for x in c.execute("SELECT * FROM ensemble_model_registry ORDER BY strategy_id").fetchall()];c.close();return rows

    def standardize(self,signal:Dict[str,Any])->StandardSignal:
        direction=str(signal.get("direction","ABSTAIN")).upper()
        if direction not in DIRECTIONS:direction="ABSTAIN"
        conf=clamp(finite(signal.get("confidence"),0.0) or 0.0,0,1)
        strength=clamp(finite(signal.get("signal_strength"),conf) or conf,0,1)
        dq=clamp(finite(signal.get("data_quality"),1.0) or 0.0,0,1)
        return StandardSignal(
            strategy_id=str(signal["strategy_id"]),strategy_version=str(signal.get("strategy_version") or "UNKNOWN"),
            symbol=str(signal["symbol"]),timestamp=str(signal.get("timestamp") or now_iso()),direction=direction,
            confidence=conf,expected_edge=finite(signal.get("expected_edge")),market_regime=signal.get("market_regime"),
            time_horizon=str(signal.get("time_horizon") or "UNKNOWN"),signal_strength=strength,
            risk_characteristics=dict(signal.get("risk_characteristics") or {}),data_quality=dq,
            family=str(signal.get("family") or "UNCLASSIFIED"),input_dependencies=list(signal.get("input_dependencies") or []),
            role=str(signal.get("role") or "DIRECTIONAL").upper(),ttl_seconds=max(1,int(signal.get("ttl_seconds") or self.default_signal_ttl_seconds)),
            status=str(signal.get("status") or "ONLINE").upper(),metadata=dict(signal.get("metadata") or {}))

    def record_signals(self,cycle_id:str,signals:List[Dict[str,Any]])->List[StandardSignal]:
        out=[];c=self.conn()
        for raw in signals:
            s=self.standardize(raw);out.append(s);sid="ens_sig_"+uuid.uuid4().hex
            c.execute("""INSERT INTO ensemble_signals(signal_id,ensemble_cycle_id,strategy_id,strategy_version,symbol,ts,
              direction,confidence,expected_edge,market_regime,time_horizon,signal_strength,risk_characteristics_json,
              data_quality,family,input_dependencies_json,role,ttl_seconds,status,metadata_json)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (sid,cycle_id,s.strategy_id,s.strategy_version,s.symbol,s.timestamp,s.direction,s.confidence,s.expected_edge,
               s.market_regime,s.time_horizon,s.signal_strength,canonical(s.risk_characteristics),s.data_quality,s.family,
               canonical(s.input_dependencies),s.role,s.ttl_seconds,s.status,canonical(s.metadata or {})))
        c.commit();c.close();return out

    def _fresh(self,s:StandardSignal,at:datetime)->bool:
        ts=parse_ts(s.timestamp)
        return bool(ts and 0 <= (at-ts).total_seconds() <= s.ttl_seconds)

    def resolve_signal(self,signal_id:str,label:int,realized_return:Optional[float]=None):
        c=self.conn();c.execute("UPDATE ensemble_signals SET resolved_label=?,resolved_return=?,resolved_ts=? WHERE signal_id=?",
                                (int(label),realized_return,now_iso(),signal_id));c.commit();c.close()

    def reliability(self,strategy_id:str,regime:Optional[str]=None,before_ts:Optional[str]=None)->Dict[str,Any]:
        c=self.conn();where=["strategy_id=?","resolved_label IN (0,1)"];params=[strategy_id]
        if regime:where.append("market_regime=?");params.append(regime)
        if before_ts:where.append("ts<?");params.append(before_ts)
        rows=[dict(x) for x in c.execute("SELECT * FROM ensemble_signals WHERE "+" AND ".join(where)+" ORDER BY ts",params).fetchall()]
        c.close();n=len(rows);wins=sum(int(x["resolved_label"]) for x in rows)
        if not n:return {"score":.50,"samples":0,"evidence":"LOW_EVIDENCE","calibration":None,"brier":None,"stability":.5}
        wr=wins/n
        confs=[float(x["confidence"]) for x in rows];labs=[float(x["resolved_label"]) for x in rows]
        brier=statistics.mean((p-y)**2 for p,y in zip(confs,labs))
        calibration=clamp(1.0-brier/.25,0,1)
        rets=[finite(x.get("resolved_return")) for x in rows];rets=[x for x in rets if x is not None]
        expectancy=statistics.mean(rets) if rets else (wr-.5)*2
        pos=sum(x for x in rets if x>0) if rets else wins
        neg=abs(sum(x for x in rets if x<0)) if rets else max(1,n-wins)
        pf=(pos/neg) if neg>0 else min(3.0,1+pos)
        stability=1.0;recent_degradation=0.0;recent_expectancy=None;historical_expectancy=None
        if len(rets)>=10:
            half=max(1,len(rets)//2);a=statistics.mean(rets[:half]);b=statistics.mean(rets[half:])
            stability=clamp(1-abs(a-b)/(abs(a)+abs(b)+1e-9),0,1)
            recent_n=max(5,min(20,len(rets)//3));recent=rets[-recent_n:];historical=rets[:-recent_n]
            if historical:
                recent_expectancy=statistics.mean(recent);historical_expectancy=statistics.mean(historical)
                if recent_expectancy<historical_expectancy:
                    recent_degradation=clamp((historical_expectancy-recent_expectancy)/(abs(historical_expectancy)+abs(recent_expectancy)+1e-9),0,1)
        sample_factor=clamp(n/self.min_sample_size,0,1)
        perf=clamp(.5+.20*math.tanh(expectancy)+.15*math.tanh(pf-1),0,1)
        score=clamp((.30*calibration+.25*stability+.30*perf+.15*sample_factor)*(1-.30*recent_degradation),0,1)
        return {"score":score,"samples":n,"evidence":"SUFFICIENT" if n>=self.min_sample_size else "LOW_EVIDENCE",
                "win_rate":wr,"expectancy":expectancy,"profit_factor":pf,"calibration":calibration,"brier":brier,"stability":stability,
                "recent_degradation":recent_degradation,"recent_expectancy":recent_expectancy,"historical_expectancy":historical_expectancy}

    def _history_by_model(self,model_ids:List[str],symbol:str,before_ts:str)->Dict[str,Dict[str,int]]:
        c=self.conn();out={m:{} for m in model_ids}
        for m in model_ids:
            rows=c.execute("""SELECT ts,direction FROM ensemble_signals WHERE strategy_id=? AND symbol=? AND ts<?
                              AND role='DIRECTIONAL' ORDER BY ts DESC LIMIT 300""",(m,symbol,before_ts)).fetchall()
            out[m]={str(x["ts"]):int(dir_sign(x["direction"])) for x in rows if x["direction"] in ("LONG","SHORT")}
        c.close();return out

    def _return_history_by_model(self,model_ids:List[str],symbol:str,before_ts:str)->Dict[str,Dict[str,float]]:
        c=self.conn();out={m:{} for m in model_ids}
        for m in model_ids:
            rows=c.execute("""SELECT ts,resolved_return FROM ensemble_signals WHERE strategy_id=? AND symbol=? AND ts<?
                              AND resolved_return IS NOT NULL ORDER BY ts DESC LIMIT 300""",(m,symbol,before_ts)).fetchall()
            out[m]={str(x["ts"]):float(x["resolved_return"]) for x in rows}
        c.close();return out

    def _regime_profile_by_model(self,model_ids:List[str],symbol:str,before_ts:str)->Dict[str,Dict[str,float]]:
        c=self.conn();out={m:{} for m in model_ids}
        for m in model_ids:
            rows=c.execute("""SELECT market_regime,AVG(resolved_return) avg_r,COUNT(*) n FROM ensemble_signals
                              WHERE strategy_id=? AND symbol=? AND ts<? AND resolved_return IS NOT NULL
                              GROUP BY market_regime""",(m,symbol,before_ts)).fetchall()
            out[m]={str(x["market_regime"] or "UNKNOWN"):float(x["avg_r"]) for x in rows if int(x["n"] or 0)>=3}
        c.close();return out

    @staticmethod
    def _pearson(a:List[float],b:List[float])->Optional[float]:
        if len(a)<5 or len(a)!=len(b):return None
        ma=statistics.mean(a);mb=statistics.mean(b)
        da=[x-ma for x in a];db=[x-mb for x in b]
        den=math.sqrt(sum(x*x for x in da)*sum(x*x for x in db))
        return sum(x*y for x,y in zip(da,db))/den if den>1e-12 else None

    def correlation_matrix(self,signals:List[StandardSignal],before_ts:str)->Dict[str,Any]:
        directional=[x for x in signals if x.role=="DIRECTIONAL"]
        ids=[x.strategy_id for x in directional];symbol=directional[0].symbol if directional else ""
        hist=self._history_by_model(ids,symbol,before_ts);returns=self._return_history_by_model(ids,symbol,before_ts)
        regimes=self._regime_profile_by_model(ids,symbol,before_ts)
        matrix={m:{m:1.0} for m in ids};details={};high=[]
        dep={x.strategy_id:set(x.input_dependencies) for x in directional}
        smap={x.strategy_id:x for x in directional}
        for i,a in enumerate(ids):
            for b in ids[i+1:]:
                common=sorted(set(hist[a])&set(hist[b]))
                signal_corr=self._pearson([hist[a][t] for t in common],[hist[b][t] for t in common])
                rcommon=sorted(set(returns[a])&set(returns[b]))
                return_corr=self._pearson([returns[a][t] for t in rcommon],[returns[b][t] for t in rcommon])
                ja=len(dep[a]&dep[b])/max(1,len(dep[a]|dep[b]))
                common_reg=sorted(set(regimes[a])&set(regimes[b]))
                regime_corr=self._pearson([regimes[a][t] for t in common_reg],[regimes[b][t] for t in common_reg]) if len(common_reg)>=5 else None
                # Position-overlap group can be provided by a strategy when available. Current V3.25
                # technical/context models do not expose independent position books, so it remains 0 unless explicit.
                ga=(smap[a].risk_characteristics or {}).get("position_overlap_group")
                gb=(smap[b].risk_characteristics or {}).get("position_overlap_group")
                position_overlap=1.0 if ga and gb and ga==gb else 0.0
                components=[abs(x) for x in (signal_corr,return_corr,regime_corr) if x is not None]+[ja,position_overlap]
                effective=max(components or [0.0])
                matrix.setdefault(a,{})[b]=effective;matrix.setdefault(b,{})[a]=effective
                detail={"signal_correlation":signal_corr,"return_correlation":return_corr,"feature_similarity":ja,
                        "regime_behavior_similarity":abs(regime_corr) if regime_corr is not None else None,
                        "position_overlap":position_overlap,"effective_correlation":effective,
                        "signal_samples":len(common),"return_samples":len(rcommon),"shared_regimes":len(common_reg)}
                details[f"{a}|{b}"]=detail
                if effective>=self.correlation_threshold:
                    source=max((("signal",abs(signal_corr) if signal_corr is not None else 0.0),
                                ("return",abs(return_corr) if return_corr is not None else 0.0),
                                ("feature_dependency",ja),("regime_behavior",abs(regime_corr) if regime_corr is not None else 0.0),
                                ("position_overlap",position_overlap)),key=lambda x:x[1])[0]
                    high.append({"a":a,"b":b,"correlation":effective,"source":source,"components":detail})
        return {"matrix":matrix,"pair_details":details,"high_pairs":high,
                "position_overlap_note":"Position overlap is applied when strategies expose an explicit overlap group; otherwise it is not invented."}

    def cluster_families(self,signals:List[StandardSignal],corr:Dict[str,Any])->Dict[str,List[str]]:
        families={}
        for s in signals:
            if s.role!="DIRECTIONAL":continue
            families.setdefault(s.family,[]).append(s.strategy_id)
        return families

    def diversity_score(self,signals:List[StandardSignal],corr:Dict[str,Any])->float:
        active=[x for x in signals if x.role=="DIRECTIONAL" and x.direction in ("LONG","SHORT")]
        if not active:return 0.0
        families=len(set(x.family for x in active));family_score=clamp(families/max(2,len(active)),0,1)
        horizons=len(set(x.time_horizon for x in active));horizon_score=clamp(horizons/max(2,len(active)),0,1)
        pairs=[]
        ids=[x.strategy_id for x in active]
        for i,a in enumerate(ids):
            for b in ids[i+1:]:pairs.append((corr.get("matrix",{}).get(a,{}).get(b,0) or 0))
        corr_score=1-statistics.mean(pairs) if pairs else .5
        deps=[]
        for i,a in enumerate(active):
            A=set(a.input_dependencies)
            for b in active[i+1:]:
                B=set(b.input_dependencies);deps.append(len(A&B)/max(1,len(A|B)))
        input_score=1-statistics.mean(deps) if deps else .5
        return clamp(.35*family_score+.30*corr_score+.20*input_score+.15*horizon_score,0,1)

    def _base_weights(self,signals:List[StandardSignal],method:str,regime:Optional[str],before_ts:str)->Tuple[Dict[str,float],Dict[str,Any]]:
        active=[x for x in signals if x.role=="DIRECTIONAL" and x.direction in ("LONG","SHORT")]
        info={};raw={}
        for s in active:
            rel_global=self.reliability(s.strategy_id,None,before_ts)
            rel_reg=self.reliability(s.strategy_id,regime,before_ts) if regime else rel_global
            evidence=min(rel_global["samples"],rel_reg["samples"] if regime else rel_global["samples"])
            if method=="MAJORITY":w=1.0
            elif method=="WEIGHTED":w=.75
            elif method=="CONFIDENCE_WEIGHTED":w=max(.05,s.confidence)
            elif method=="PERFORMANCE_WEIGHTED":w=max(.05,rel_global["score"])
            else:w=max(.05,.45*rel_global["score"]+.55*rel_reg["score"])
            # Low evidence is shrunk toward a neutral conservative baseline.
            if evidence<self.min_sample_size:
                alpha=evidence/self.min_sample_size;w=alpha*w+(1-alpha)*.35
            w*=s.data_quality
            raw[s.strategy_id]=w
            info[s.strategy_id]={"global":rel_global,"regime":rel_reg,"raw_weight":w}
        total=sum(raw.values()) or 1.0
        return {k:v/total for k,v in raw.items()},info

    def _apply_caps_and_correlation(self,weights:Dict[str,float],signals:List[StandardSignal],corr:Dict[str,Any])->Tuple[Dict[str,float],Dict[str,Any]]:
        smap={s.strategy_id:s for s in signals};w=dict(weights);discounts={}
        # correlation discount: multiple near-duplicates share evidence instead of multiplying it.
        for m in list(w):
            peers=[p for p,v in corr.get("matrix",{}).get(m,{}).items() if p!=m and p in w and (v or 0)>=self.correlation_threshold]
            factor=1/math.sqrt(1+len(peers))
            w[m]*=factor;discounts[m]={"correlated_peers":peers,"factor":factor}
        # model cap before normalization.
        for m in w:w[m]=min(w[m],self.max_model_weight)
        # family cap applied iteratively.
        for _ in range(4):
            fams={}
            for m,val in w.items():fams.setdefault(smap[m].family,[]).append(m)
            for fam,members in fams.items():
                s=sum(w[x] for x in members)
                if s>self.max_family_weight and s>0:
                    scale=self.max_family_weight/s
                    for x in members:w[x]*=scale
            total=sum(w.values())
            if total>0:
                for m in w:w[m]/=total
        # after normalization, enforce caps again conservatively and leave unused weight as abstention mass.
        for m in w:w[m]=min(w[m],self.max_model_weight)
        fam_tot={}
        for m,val in w.items():fam_tot[smap[m].family]=fam_tot.get(smap[m].family,0)+val
        for fam,total in list(fam_tot.items()):
            if total>self.max_family_weight:
                scale=self.max_family_weight/total
                for m in w:
                    if smap[m].family==fam:w[m]*=scale
        return w,{"correlation_discounts":discounts,"family_totals":{f:sum(v for m,v in w.items() if smap[m].family==f) for f in set(s.family for s in signals if s.role=='DIRECTIONAL')},"unused_weight":max(0,1-sum(w.values()))}

    def _stabilize_weights(self,weights:Dict[str,float])->Tuple[Dict[str,float],Dict[str,Any]]:
        """Apply cooldown and per-iteration weight-change limits before weights affect an output."""
        c=self.conn();prev=c.execute("SELECT * FROM ensemble_weight_versions ORDER BY created_at DESC LIMIT 1").fetchone();c.close()
        if not prev:return dict(weights),{"cooldown_active":False,"parent_version":None,"changes":{}}
        prevw=json.loads(prev["weights_json"] or "{}");now=datetime.now(timezone.utc)
        age=(now-(parse_ts(prev["created_at"]) or now)).total_seconds()/3600
        if age>=self.weight_cooldown_hours:
            return dict(weights),{"cooldown_active":False,"parent_version":prev["ensemble_weight_version"],"changes":{}}
        out={};changes={}
        keys=set(prevw)|set(weights)
        for k in keys:
            pv=float(prevw.get(k,0.0));nv=float(weights.get(k,0.0))
            av=clamp(nv,pv-self.weight_change_limit,pv+self.weight_change_limit)
            if av>0:out[k]=av
            changes[k]={"previous":pv,"requested":nv,"applied":av}
        # Do not renormalize to 1 here: unused mass is intentional abstention evidence.
        return out,{"cooldown_active":True,"parent_version":prev["ensemble_weight_version"],"changes":changes}

    def _calibrator_factor(self,signals:List[StandardSignal])->Tuple[float,List[Dict[str,Any]]]:
        factors=[]
        for s in signals:
            if s.role!="CALIBRATOR" or s.status!="ONLINE":continue
            # calibrator cannot create direction; it only scales confidence toward neutral.
            f=clamp(.5+s.confidence-.5,.5,1.25)*s.data_quality
            factors.append({"model":s.strategy_id,"factor":f,"confidence":s.confidence})
        if not factors:return 1.0,[]
        return clamp(statistics.mean(x["factor"] for x in factors),.5,1.15),factors

    def evaluate(self,signals:List[Dict[str,Any]],method:str="REGIME_WEIGHTED",regime:Optional[str]=None,
                 execution_cost:Optional[float]=None,target_horizon:Optional[str]=None,
                 current_system_direction:Optional[str]=None,current_system_confidence:Optional[float]=None,
                 current_executed:bool=False)->Dict[str,Any]:
        method=method if method in BASELINES else "REGIME_WEIGHTED";cycle="ens_cycle_"+uuid.uuid4().hex
        std=self.record_signals(cycle,signals);at=datetime.now(timezone.utc)
        fresh=[];abstain=[];offline=[];stale=[]
        for s in std:
            if s.status!="ONLINE":offline.append(s.strategy_id);continue
            if not self._fresh(s,at):stale.append(s.strategy_id);continue
            if target_horizon and s.role=="DIRECTIONAL" and s.time_horizon!=target_horizon:
                abstain.append(s.strategy_id);continue
            if s.direction in ("ABSTAIN","NEUTRAL") or s.role!="DIRECTIONAL":
                if s.direction in ("ABSTAIN","NEUTRAL"):abstain.append(s.strategy_id)
                fresh.append(s);continue
            fresh.append(s)
        active=[s for s in fresh if s.role=="DIRECTIONAL" and s.direction in ("LONG","SHORT")]
        before=min((s.timestamp for s in fresh),default=now_iso())
        corr=self.correlation_matrix(active,before)
        families=self.cluster_families(active,corr);div=self.diversity_score(active,corr)
        weights,rel=self._base_weights(active,method,regime,before)
        weights,capinfo=self._apply_caps_and_correlation(weights,active,corr)
        weights,stability_info=self._stabilize_weights(weights)
        # Recompute family totals after stability limiting so reported evidence is exactly what influenced the decision.
        smap={s.strategy_id:s for s in active}
        capinfo["family_totals"]={f:sum(v for m,v in weights.items() if smap.get(m) and smap[m].family==f)
                                  for f in set(s.family for s in active)}
        capinfo["unused_weight"]=max(0,1-sum(weights.values()))
        capinfo["weight_stability"]=stability_info
        sign_sum=sum(weights.get(s.strategy_id,0)*dir_sign(s.direction)*s.signal_strength for s in active)
        evidence_mass=sum(weights.get(s.strategy_id,0)*s.signal_strength for s in active)
        agreement=clamp(abs(sign_sum)/max(evidence_mass,1e-12),0,1) if active else 0
        disagreement=1-agreement
        weighted_conf=sum(weights.get(s.strategy_id,0)*s.confidence for s in active)
        dq=(sum(weights.get(s.strategy_id,0)*s.data_quality for s in active)/max(sum(weights.values()),1e-12)) if active else 0
        cal_factor,calibrators=self._calibrator_factor(fresh)
        conf=clamp(weighted_conf*agreement*(.50+.50*div)*dq*cal_factor,0,.97)
        direction="LONG" if sign_sum>0 else "SHORT" if sign_sum<0 else "ABSTAIN"
        reasons=[]
        if len(active)<self.min_active_directional:
            reasons.append("INSUFFICIENT_ENSEMBLE_INFORMATION");direction="ABSTAIN";conf=min(conf,.25)
        if disagreement>.60:
            reasons.append("ENSEMBLE_CONFLICT");direction="ABSTAIN";conf=min(conf,.35)
        if div<.25:
            reasons.append("LOW_MODEL_DIVERSITY");conf=min(conf,.40)
        if offline:reasons.append("MODEL_OFFLINE")
        if stale:reasons.append("SIGNAL_STALE")
        if len(offline)+len(stale)>=max(2,len(std)//2):
            reasons.append("INSUFFICIENT_ENSEMBLE_INFORMATION");direction="ABSTAIN";conf=min(conf,.20)
        # Expected edge respects opposition and costs.
        gross=0.0;edge_mass=0.0
        for s in active:
            if s.expected_edge is None:continue
            w=weights.get(s.strategy_id,0);gross+=w*float(s.expected_edge)*(1 if s.direction==direction else -1);edge_mass+=w
        weighted_edge=(gross/max(edge_mass,1e-12)) if edge_mass else None
        net_edge=(weighted_edge-float(execution_cost)) if weighted_edge is not None and execution_cost is not None else weighted_edge
        if net_edge is not None and net_edge<=0:
            reasons.append("NO_CLEAR_EDGE_AFTER_EXECUTION_COSTS");direction="ABSTAIN";conf=min(conf,.30)
        contributions=[]
        for s in active:
            contributions.append({"strategy_id":s.strategy_id,"family":s.family,"direction":s.direction,
              "weight":weights.get(s.strategy_id,0),"confidence":s.confidence,"signal_strength":s.signal_strength,
              "contribution":weights.get(s.strategy_id,0)*dir_sign(s.direction)*s.signal_strength,
              "reliability":rel.get(s.strategy_id,{})})
        if not reasons:reasons.append("ENSEMBLE_SHADOW_EVALUATION_COMPLETE")
        wversion=self._store_weight_version(method,regime,weights,capinfo,rel)
        eid="ens_dec_"+uuid.uuid4().hex
        record={"ensemble_decision_id":eid,"ensemble_cycle_id":cycle,"symbol":std[0].symbol if std else "UNKNOWN",
          "ts":now_iso(),"mode":self.mode,"method":method,"ensemble_direction":direction,"ensemble_confidence":conf,
          "agreement_score":agreement,"disagreement_score":disagreement,"diversity_score":div,
          "weighted_expected_edge":weighted_edge,"expected_execution_cost":execution_cost,"expected_net_edge":net_edge,
          "market_regime":regime,"data_quality":dq,"participating_models":[s.strategy_id for s in active],
          "abstaining_models":abstain+stale,"offline_models":offline,"model_contributions":contributions,
          "correlation":corr,"families":families,"reasoning_summary":reasons,"ensemble_weight_version":wversion,
          "hypothetical_only":True,"current_system_direction":current_system_direction,
          "current_system_confidence":current_system_confidence,"current_executed":current_executed,
          "weights":weights,"family_weight_info":capinfo,"calibrators":calibrators}
        c=self.conn();c.execute("""INSERT INTO ensemble_outputs(ensemble_decision_id,ensemble_cycle_id,symbol,ts,mode,
          method,ensemble_direction,ensemble_confidence,agreement_score,disagreement_score,diversity_score,
          weighted_expected_edge,expected_execution_cost,expected_net_edge,market_regime,data_quality,
          participating_models_json,abstaining_models_json,offline_models_json,model_contributions_json,correlation_json,
          families_json,reasoning_summary_json,ensemble_weight_version,hypothetical_only)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
          (eid,cycle,record["symbol"],record["ts"],self.mode,method,direction,conf,agreement,disagreement,div,
           weighted_edge,execution_cost,net_edge,regime,dq,canonical(record["participating_models"]),canonical(record["abstaining_models"]),
           canonical(offline),canonical(contributions),canonical(corr),canonical(families),canonical(reasons),wversion));c.commit();c.close()
        self._emit_alerts(record)
        return record

    def _store_weight_version(self,method:str,regime:Optional[str],weights:Dict[str,float],family_info:Dict[str,Any],evidence:Dict[str,Any])->str:
        c=self.conn();prev=c.execute("SELECT * FROM ensemble_weight_versions ORDER BY created_at DESC LIMIT 1").fetchone()
        ver="ens_w_"+uuid.uuid4().hex[:12]
        c.execute("""INSERT INTO ensemble_weight_versions(ensemble_weight_version,created_at,method,regime,weights_json,
          family_weights_json,evidence_json,parent_version,status,auto_deploy) VALUES(?,?,?,?,?,?,?,?,?,0)""",
          (ver,now_iso(),method,regime,canonical(weights),canonical(family_info.get("family_totals",{})),canonical(evidence),
           prev["ensemble_weight_version"] if prev else None,"ACTIVE_SHADOW"));c.commit();c.close();return ver

    def _emit_alerts(self,r:Dict[str,Any]):
        events=[]
        if "ENSEMBLE_CONFLICT" in r["reasoning_summary"]:events.append(("ENSEMBLE_CONFLICT","WARNING"))
        if "LOW_MODEL_DIVERSITY" in r["reasoning_summary"]:events.append(("LOW_MODEL_DIVERSITY","WARNING"))
        if r["correlation"].get("high_pairs"):events.append(("HIGH_MODEL_CORRELATION","WARNING"))
        if r["offline_models"]:events.append(("MODEL_OFFLINE","WARNING"))
        if "SIGNAL_STALE" in r["reasoning_summary"]:events.append(("SIGNAL_STALE","WARNING"))
        if "INSUFFICIENT_ENSEMBLE_INFORMATION" in r["reasoning_summary"]:events.append(("INSUFFICIENT_ENSEMBLE_INFORMATION","HIGH"))
        if any((x.get("reliability") or {}).get("global",{}).get("evidence")=="SUFFICIENT" and
               ((x.get("reliability") or {}).get("global",{}).get("calibration") or 1.0)<.50 for x in r.get("model_contributions",[])):
            events.append(("CONFIDENCE_MISCALIBRATION","WARNING"))
        c=self.conn()
        for event,sev in events:
            c.execute("INSERT INTO ensemble_alerts(ts,event_type,severity,symbol,ensemble_decision_id,message,details_json) VALUES(?,?,?,?,?,?,?)",
                      (now_iso(),event,sev,r["symbol"],r["ensemble_decision_id"],event,canonical(r)))
        c.commit();c.close()

    def correlation_audit(self)->Dict[str,Any]:
        reg=self.registry();out=[]
        for i,a in enumerate(reg):
            A=set(json.loads(a["input_dependencies_json"] or "[]"))
            for b in reg[i+1:]:
                B=set(json.loads(b["input_dependencies_json"] or "[]"));j=len(A&B)/max(1,len(A|B))
                out.append({"a":a["strategy_id"],"b":b["strategy_id"],"feature_similarity":j,
                            "same_family":a["family"]==b["family"],"likely_correlated":j>=self.correlation_threshold or a["family"]==b["family"]})
        return {"pairs":out,"high_similarity":[x for x in out if x["likely_correlated"]]}

    @staticmethod
    def _return_metrics(values:List[float])->Dict[str,Any]:
        xs=[float(x) for x in values if x is not None and math.isfinite(float(x))]
        if not xs:return {"samples":0,"expectancy":None,"profit_factor":None,"max_drawdown":None,"sharpe":None,"sortino":None,"stability":None}
        wins=sum(x for x in xs if x>0);losses=abs(sum(x for x in xs if x<0));pf=(wins/losses) if losses>1e-12 else None
        eq=0.0;peak=0.0;dd=0.0
        for x in xs:eq+=x;peak=max(peak,eq);dd=max(dd,peak-eq)
        mean=statistics.mean(xs);sd=statistics.stdev(xs) if len(xs)>1 else 0.0
        downside=[x for x in xs if x<0];ds=statistics.pstdev(downside) if len(downside)>1 else 0.0
        sharpe=(mean/sd*math.sqrt(len(xs))) if sd>1e-12 else None
        sortino=(mean/ds*math.sqrt(len(xs))) if ds>1e-12 else None
        stability=.5
        if len(xs)>=10:
            k=max(2,len(xs)//4);chunks=[xs[i:i+k] for i in range(0,len(xs),k) if xs[i:i+k]]
            means=[statistics.mean(c) for c in chunks]
            stability=clamp(1-(statistics.pstdev(means)/(abs(statistics.mean(means))+1e-9)),0,1) if len(means)>1 else .5
        return {"samples":len(xs),"expectancy":mean,"profit_factor":pf,"max_drawdown":dd,"sharpe":sharpe,"sortino":sortino,"stability":stability}

    def performance_metrics(self,days:int=30)->Dict[str,Any]:
        since=(datetime.now(timezone.utc)-timedelta(days=days)).isoformat();c=self.conn()
        rows=[dict(x) for x in c.execute("""SELECT c.*,o.market_regime,o.ensemble_confidence,o.ensemble_direction
          FROM ensemble_shadow_comparisons c LEFT JOIN ensemble_outputs o ON o.ensemble_decision_id=c.ensemble_decision_id
          WHERE c.ts>=? ORDER BY c.ts""",(since,)).fetchall()]
        sigs=[dict(x) for x in c.execute("""SELECT * FROM ensemble_signals WHERE ts>=? AND resolved_label IN (0,1)
                                             ORDER BY ts""",(since,)).fetchall()]
        versions=[dict(x) for x in c.execute("SELECT * FROM ensemble_weight_versions WHERE created_at>=? ORDER BY created_at",(since,)).fetchall()]
        c.close()
        hyp=[finite(x.get("hypothetical_result")) for x in rows];hyp=[x for x in hyp if x is not None]
        actual=[finite(x.get("actual_result")) for x in rows];actual=[x for x in actual if x is not None]
        # Ensemble calibration is evaluated only where a shadow counterfactual is actually resolvable.
        probs=[];labels=[]
        for x in rows:
            h=finite(x.get("hypothetical_result"));p=finite(x.get("ensemble_confidence"))
            if h is None or p is None:continue
            probs.append(clamp(p,0,1));labels.append(1.0 if h>0 else 0.0)
        brier=statistics.mean((p-y)**2 for p,y in zip(probs,labels)) if probs else None
        by_regime={}
        for x in rows:
            h=finite(x.get("hypothetical_result"));reg=x.get("market_regime") or "UNKNOWN"
            if h is not None:by_regime.setdefault(reg,[]).append(h)
        model_ids=sorted(set(x.get("strategy_id") for x in sigs if x.get("strategy_id")))
        model_metrics={m:self.reliability(m,None,None) for m in model_ids}
        turnover=[];prev=None
        for v in versions:
            try:w=json.loads(v.get("weights_json") or "{}")
            except Exception:w={}
            if prev is not None:
                keys=set(prev)|set(w);turnover.append(sum(abs(float(w.get(k,0))-float(prev.get(k,0))) for k in keys)/2)
            prev=w
        return {"window_days":days,"ensemble":self._return_metrics(hyp),"current_system":self._return_metrics(actual),
                "ensemble_brier":brier,"calibration_samples":len(probs),"performance_by_regime":{k:self._return_metrics(v) for k,v in by_regime.items()},
                "model_reliability":model_metrics,"weight_turnover":statistics.mean(turnover) if turnover else 0.0,
                "weight_versions":len(versions),"net_performance_note":"hypothetical_result is used only when the shadow counterfactual is explicitly resolvable; otherwise it remains absent."}

    def degradation(self,recent_days:int=7,historical_days:int=30)->Dict[str,Any]:
        now=datetime.now(timezone.utc);recent_cut=(now-timedelta(days=recent_days)).isoformat();hist_cut=(now-timedelta(days=historical_days)).isoformat()
        c=self.conn();recent=[dict(x) for x in c.execute("SELECT * FROM ensemble_shadow_comparisons WHERE ts>=?",(recent_cut,)).fetchall()]
        hist=[dict(x) for x in c.execute("SELECT * FROM ensemble_shadow_comparisons WHERE ts>=? AND ts<?",(hist_cut,recent_cut)).fetchall()];c.close()
        rv=[finite(x.get("hypothetical_result")) for x in recent];rv=[x for x in rv if x is not None]
        hv=[finite(x.get("hypothetical_result")) for x in hist];hv=[x for x in hv if x is not None]
        rm=self._return_metrics(rv);hm=self._return_metrics(hv);reasons=[]
        if len(rv)>=self.min_sample_size and len(hv)>=self.min_sample_size:
            if rm["expectancy"] is not None and hm["expectancy"] is not None and rm["expectancy"]<hm["expectancy"]-max(.10,abs(hm["expectancy"])*.35):reasons.append("ENSEMBLE_EXPECTANCY_DOWN")
            if rm["profit_factor"] is not None and hm["profit_factor"] is not None and rm["profit_factor"]<min(1.0,hm["profit_factor"]*.70):reasons.append("ENSEMBLE_PROFIT_FACTOR_DOWN")
            if rm["max_drawdown"] is not None and hm["max_drawdown"] is not None and rm["max_drawdown"]>hm["max_drawdown"]*1.5+1e-9:reasons.append("ENSEMBLE_DRAWDOWN_UP")
        status="ENSEMBLE_DEGRADATION_DETECTED" if reasons else "INSUFFICIENT_DATA" if len(rv)<self.min_sample_size or len(hv)<self.min_sample_size else "NORMAL"
        return {"status":status,"reasons":reasons,"recent":rm,"historical":hm,"causal_claim":False}

    def value_added(self,days:int=30)->Dict[str,Any]:
        since=(datetime.now(timezone.utc)-timedelta(days=days)).isoformat();c=self.conn()
        comps=[dict(x) for x in c.execute("SELECT * FROM ensemble_shadow_comparisons WHERE ts>=?",(since,)).fetchall()]
        c.close();real=[finite(x.get("actual_result")) for x in comps];real=[x for x in real if x is not None]
        hyp=[finite(x.get("hypothetical_result")) for x in comps];hyp=[x for x in hyp if x is not None]
        perf=self.performance_metrics(days);models=perf.get("model_reliability") or {}
        individual=[(m,finite(v.get("expectancy"))) for m,v in models.items() if finite(v.get("expectancy")) is not None]
        best=max(individual,key=lambda x:x[1]) if individual else None
        avg=statistics.mean(x[1] for x in individual) if individual else None
        base={"samples":len(hyp),"best_individual_model":best[0] if best else None,
              "best_individual_expectancy":best[1] if best else None,"average_model_expectancy":avg,
              "comparison_note":"Individual-model outcomes and Ensemble shadow counterfactuals are descriptive unless they refer to the same resolvable event set."}
        if len(hyp)<self.min_sample_size:
            return {"status":"NO_ENSEMBLE_ADVANTAGE_DETECTED","evidence":"INSUFFICIENT_DATA",**base}
        ens=statistics.mean(hyp);cur=statistics.mean(real) if real else None
        # Do not declare advantage merely for sophistication; require evidence against the current system
        # and, when comparable, against the best individual model.
        advantage=cur is not None and ens>cur and (best is None or ens>best[1])
        return {"status":"ENSEMBLE_ADVANTAGE_OBSERVED_SHADOW" if advantage else "NO_ENSEMBLE_ADVANTAGE_DETECTED",
                "ensemble_expectancy":ens,"current_expectancy":cur,**base}

    def shadow_compare(self,ensemble_decision_id:str,current_direction:str,current_confidence:Optional[float],current_executed:bool,
                       actual_result:Optional[float]=None,hypothetical_result:Optional[float]=None)->Dict[str,Any]:
        c=self.conn();o=c.execute("SELECT * FROM ensemble_outputs WHERE ensemble_decision_id=?",(ensemble_decision_id,)).fetchone()
        if not o:c.close();return {}
        cid="ens_cmp_"+uuid.uuid4().hex
        c.execute("""INSERT INTO ensemble_shadow_comparisons(comparison_id,ensemble_decision_id,ts,current_direction,
          current_confidence,current_executed,ensemble_direction,ensemble_confidence,actual_result,hypothetical_result,
          real_result_source,hypothetical_result_source,details_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (cid,ensemble_decision_id,now_iso(),current_direction,current_confidence,int(current_executed),o["ensemble_direction"],
           o["ensemble_confidence"],actual_result,hypothetical_result,"EXECUTED_SYSTEM_ONLY",
           "SHADOW_COUNTERFACTUAL_ONLY_WHEN_RESOLVABLE",canonical({"no_hypothetical_fill_assumption":True})))
        c.commit();c.close()
        deg=self.degradation()
        if deg["status"]=="ENSEMBLE_DEGRADATION_DETECTED":
            c=self.conn();c.execute("INSERT INTO ensemble_alerts(ts,event_type,severity,symbol,ensemble_decision_id,message,details_json) VALUES(?,?,?,?,?,?,?)",
                (now_iso(),"ENSEMBLE_DEGRADATION","HIGH",o["symbol"],ensemble_decision_id,"ENSEMBLE_DEGRADATION",canonical(deg)));c.commit();c.close()
        return {"comparison_id":cid,"no_hypothetical_fill_assumption":True,"degradation":deg}

    def candidate_weights(self,parent_weight_version:str,proposal:Dict[str,Any],evidence:Dict[str,Any])->Dict[str,Any]:
        cid="ens_candidate_"+uuid.uuid4().hex;c=self.conn();c.execute("""INSERT INTO ensemble_policy_candidates(
          candidate_id,created_at,parent_weight_version,proposal_json,status,validation_state,evidence_json,auto_deploy)
          VALUES(?,?,?,?,?,?,?,0)""",(cid,now_iso(),parent_weight_version,canonical(proposal),"RESEARCH_ONLY","PENDING",canonical(evidence)))
        c.commit();c.close();return {"candidate_id":cid,"status":"RESEARCH_ONLY","auto_deploy":False,
          "required_path":["VALIDATION","PAPER","CANARY","APPROVAL"]}

    def dashboard(self)->Dict[str,Any]:
        c=self.conn();last=c.execute("SELECT * FROM ensemble_outputs ORDER BY ts DESC LIMIT 1").fetchone();alerts=[dict(x) for x in c.execute("SELECT * FROM ensemble_alerts ORDER BY id DESC LIMIT 20").fetchall()];c.close()
        if not last:return {"enabled":True,"mode":self.mode,"status":"NO_DATA","signal_authority":False,"risk_increase_authority":False}
        return {"enabled":True,"mode":self.mode,"signal_authority":False,"risk_increase_authority":False,
          "ensemble_status":"ABSTAIN" if last["ensemble_direction"]=="ABSTAIN" else "SHADOW_OPINION",
          "ensemble_direction":last["ensemble_direction"],"ensemble_confidence":last["ensemble_confidence"],
          "agreement_score":last["agreement_score"],"disagreement_score":last["disagreement_score"],
          "diversity_score":last["diversity_score"],"expected_net_edge":last["expected_net_edge"],
          "current_weights":json.loads((self.conn().execute("SELECT weights_json FROM ensemble_weight_versions ORDER BY created_at DESC LIMIT 1").fetchone() or {"weights_json":"{}"})["weights_json"]),
          "active_models":json.loads(last["participating_models_json"]),"abstaining_models":json.loads(last["abstaining_models_json"]),
          "recent_alerts":alerts,"value_added":self.value_added(),"performance":self.performance_metrics(),"degradation":self.degradation(),
          "activation_path":["SHADOW","VALIDATION","PAPER","CANARY","LIMITED_ENSEMBLE","PRODUCTION_ENSEMBLE"]}

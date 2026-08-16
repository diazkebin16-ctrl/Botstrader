from __future__ import annotations
from dataclasses import dataclass,asdict
from datetime import datetime,timezone,timedelta
from typing import Any,Dict,List,Optional,Tuple
import json,math,sqlite3,statistics,uuid

MODES=("SHADOW","PAPER","CANARY","LIMITED_LIVE")
BASELINES=("EQUAL_RISK","FIXED_RISK","VOLATILITY_WEIGHTED","PERFORMANCE_WEIGHTED","DYNAMIC")

def now_iso(): return datetime.now(timezone.utc).isoformat()
def clamp(x,lo,hi): return max(lo,min(hi,float(x)))
def finite(x,d=0.0):
    try:
        y=float(x);return y if math.isfinite(y) else d
    except Exception:return d

def parse_ts(x):
    try:
        d=datetime.fromisoformat(str(x).replace("Z","+00:00"));return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:return None

@dataclass
class AllocationInput:
    strategy_id:str; family:str; symbol:str; asset_class:str; direction:str
    requested_risk:float; expected_net_edge:float; reliability:float; calibration:float
    sample_size:int; stability:float; volatility:float; drawdown:float; tail_risk:float
    execution_quality:float; ensemble_confidence:float; regime_compatibility:float
    recent_performance:float=0.0; sector:Optional[str]=None; asset:Optional[str]=None
    data_quality:float=1.0; degraded:bool=False; cluster:Optional[str]=None

class CapitalAllocationEngine:
    """Step 18: risk-budget allocator. SHADOW only by default.

    It never creates BUY/SELL signals, sends orders, changes Risk Engine limits, or
    turns unused budget into forced exposure. All outputs are recommendations capped
    by an externally supplied authorized_total_risk.
    """
    def __init__(self,db_path:str,version:str="3.26",mode:str="SHADOW",max_strategy_allocation=.25,
                 max_family_risk=.40,max_symbol_risk=.25,max_asset_risk=.40,max_directional_risk=.65,
                 max_cluster_risk=.35,min_sample_size=30,min_observation_hours=24,
                 max_change_per_cycle=.05,change_cooldown_hours=24,rebalance_threshold=.02,
                 heat_limit=.80,correlation_threshold=.70):
        self.db_path=db_path;self.version=version;self.mode=mode if mode in MODES else "SHADOW"
        self.max_strategy_allocation=clamp(max_strategy_allocation,.01,.50)
        self.max_family_risk=clamp(max_family_risk,.05,.80);self.max_symbol_risk=clamp(max_symbol_risk,.01,.60)
        self.max_asset_risk=clamp(max_asset_risk,.05,.80);self.max_directional_risk=clamp(max_directional_risk,.10,.90)
        self.max_cluster_risk=clamp(max_cluster_risk,.05,.80);self.min_sample_size=max(5,int(min_sample_size))
        self.min_observation_hours=max(1,int(min_observation_hours));self.max_change_per_cycle=clamp(max_change_per_cycle,.005,.25)
        self.change_cooldown_hours=max(1,int(change_cooldown_hours));self.rebalance_threshold=clamp(rebalance_threshold,.001,.20)
        self.heat_limit=clamp(heat_limit,.20,1.0);self.correlation_threshold=clamp(correlation_threshold,.40,.99)
    def conn(self):
        c=sqlite3.connect(self.db_path,timeout=30);c.row_factory=sqlite3.Row;c.execute("PRAGMA journal_mode=WAL");c.execute("PRAGMA synchronous=FULL");return c
    def ensure_schema(self):
        c=self.conn();c.executescript('''
        CREATE TABLE IF NOT EXISTS allocation_decisions(
          allocation_version TEXT PRIMARY KEY,ts TEXT NOT NULL,mode TEXT NOT NULL,policy TEXT NOT NULL,
          authorized_total_risk REAL NOT NULL,used_risk REAL NOT NULL,unused_risk REAL NOT NULL,
          portfolio_heat REAL NOT NULL,diversification_score REAL NOT NULL,efficiency_score REAL NOT NULL,
          allocation_turnover REAL NOT NULL,risk_off INTEGER NOT NULL,reason TEXT NOT NULL,
          allocations_json TEXT NOT NULL,limits_json TEXT NOT NULL,correlations_json TEXT NOT NULL,stress_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS allocation_strategy_history(
          id INTEGER PRIMARY KEY AUTOINCREMENT,allocation_version TEXT NOT NULL,ts TEXT NOT NULL,strategy_id TEXT NOT NULL,
          family TEXT,symbol TEXT,direction TEXT,requested_risk REAL,allocated_risk REAL,allocation_percentage REAL,
          allocation_confidence REAL,reliability_adjusted_edge REAL,marginal_risk_contribution REAL,reason TEXT,details_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS allocation_shadow_comparisons(
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts TEXT NOT NULL,allocation_version TEXT NOT NULL,current_json TEXT NOT NULL,
          dynamic_json TEXT NOT NULL,metrics_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS allocation_policy_candidates(
          candidate_id TEXT PRIMARY KEY,created_at TEXT NOT NULL,parent_policy TEXT NOT NULL,status TEXT NOT NULL,
          proposal_json TEXT NOT NULL,evidence_json TEXT NOT NULL,auto_deploy INTEGER NOT NULL DEFAULT 0);
        ''');c.commit();c.close()
    @staticmethod
    def _pearson(a,b):
        if len(a)<5 or len(a)!=len(b):return None
        ma=statistics.mean(a);mb=statistics.mean(b);aa=[x-ma for x in a];bb=[x-mb for x in b]
        den=math.sqrt(sum(x*x for x in aa)*sum(x*x for x in bb));return sum(x*y for x,y in zip(aa,bb))/den if den>1e-12 else None
    def correlation_matrix(self,items:List[AllocationInput],explicit:Optional[Dict[str,Dict[str,float]]]=None)->Dict[str,Any]:
        ids=[x.strategy_id for x in items];m={a:{a:1.0} for a in ids};high=[]
        for i,a in enumerate(items):
            for b in items[i+1:]:
                v=None
                if explicit:v=(explicit.get(a.strategy_id,{}) or {}).get(b.strategy_id)
                if v is None and a.cluster and b.cluster and a.cluster==b.cluster:v=.90
                if v is None and a.family==b.family:v=.75
                if v is None:v=0.0
                v=clamp(abs(v),0,1);m[a.strategy_id][b.strategy_id]=v;m[b.strategy_id][a.strategy_id]=v
                if v>=self.correlation_threshold:high.append({"a":a.strategy_id,"b":b.strategy_id,"correlation":v})
        return {"matrix":m,"high_pairs":high}
    def reliability_adjusted_edge(self,x:AllocationInput)->Tuple[float,Dict[str,float]]:
        evidence=clamp(x.sample_size/self.min_sample_size,0,1)
        rel=clamp(x.reliability,0,1);cal=clamp(x.calibration,0,1);stable=clamp(x.stability,0,1);reg=clamp(x.regime_compatibility,0,1)
        quality=clamp(x.data_quality,0,1)*clamp(x.execution_quality,0,1)
        certainty=(.28*rel+.18*cal+.18*stable+.18*reg+.18*evidence)*quality
        if x.degraded:certainty*=.45
        edge=max(0.0,x.expected_net_edge)*certainty
        return edge,{"evidence":evidence,"certainty":certainty,"quality":quality}
    def diversification_score(self,items,corr):
        if len(items)<2:return .25 if items else 0.0
        pairs=[]
        for i,a in enumerate(items):
            for b in items[i+1:]:pairs.append(corr["matrix"].get(a.strategy_id,{}).get(b.strategy_id,0.0))
        corr_div=1-statistics.mean(pairs) if pairs else .5
        fam=len(set(x.family for x in items))/len(items);asset=len(set(x.asset_class for x in items))/len(items)
        return clamp(.55*corr_div+.25*fam+.20*asset,0,1)
    def portfolio_heat(self,items,alloc,corr,portfolio_drawdown=0.0,stress_multiplier=1.0):
        used=sum(alloc.values());corr_load=0.0
        for i,a in enumerate(items):
            for b in items[i+1:]:corr_load+=min(alloc.get(a.strategy_id,0),alloc.get(b.strategy_id,0))*corr["matrix"].get(a.strategy_id,{}).get(b.strategy_id,0)
        vol=sum(alloc.get(x.strategy_id,0)*clamp(x.volatility,0,3) for x in items)/max(used,1e-9) if used else 0
        tail=sum(alloc.get(x.strategy_id,0)*clamp(x.tail_risk,0,3) for x in items)/max(used,1e-9) if used else 0
        return clamp((used+.50*corr_load)*(1+.25*vol+.25*tail+2*max(0,portfolio_drawdown))*stress_multiplier,0,10)
    def _caps(self,items,alloc,total):
        smap={x.strategy_id:x for x in items};out={k:max(0,float(v)) for k,v in alloc.items()};reasons=[]
        for sid in out:
            cap=total*self.max_strategy_allocation
            if out[sid]>cap:out[sid]=cap;reasons.append(f"MAX_STRATEGY:{sid}")
        for field,capf,label in (("family",self.max_family_risk,"FAMILY"),("symbol",self.max_symbol_risk,"SYMBOL"),("asset_class",self.max_asset_risk,"ASSET"),("direction",self.max_directional_risk,"DIRECTION")):
            groups={}
            for sid,v in out.items():groups.setdefault(getattr(smap[sid],field) or "UNKNOWN",[]).append(sid)
            for g,members in groups.items():
                s=sum(out[x] for x in members);cap=total*capf
                if s>cap and s>0:
                    scale=cap/s
                    for x in members:out[x]*=scale
                    reasons.append(f"MAX_{label}:{g}")
        return out,reasons
    def _previous(self):
        c=self.conn();r=c.execute("SELECT * FROM allocation_decisions ORDER BY ts DESC LIMIT 1").fetchone();c.close()
        if not r:return None
        d=dict(r);d["allocations"]=json.loads(d["allocations_json"]);return d
    def allocate(self,items_raw:List[Dict[str,Any]],authorized_total_risk:float,policy="DYNAMIC",regime="UNKNOWN",
                 explicit_correlations=None,portfolio_drawdown=0.0,system_quality=1.0,ensemble_disagreement=0.0,
                 execution_degraded=False,governance_frozen=False,reallocation_cost=0.0)->Dict[str,Any]:
        items=[x if isinstance(x,AllocationInput) else AllocationInput(**x) for x in items_raw];total=max(0,float(authorized_total_risk));corr=self.correlation_matrix(items,explicit_correlations)
        scored={};raw={}
        for x in items:
            edge,detail=self.reliability_adjusted_edge(x);vol=max(.20,finite(x.volatility,1.0));tail=1+clamp(x.tail_risk,0,3)
            score=edge/(vol*tail);score*=max(.20,1-clamp(x.drawdown,0,1));score*=max(.25,clamp(x.ensemble_confidence,0,1))
            scored[x.strategy_id]={"reliability_adjusted_edge":edge,"score":score,**detail};raw[x.strategy_id]=score
        # Correlation-aware evidence discount.
        for x in items:
            peers=sum(1 for p,v in corr["matrix"].get(x.strategy_id,{}).items() if p!=x.strategy_id and v>=self.correlation_threshold)
            raw[x.strategy_id]/=math.sqrt(1+peers)
        low_opportunity=not raw or max(raw.values(),default=0)<=1e-9
        risk_off=bool(governance_frozen or execution_degraded or system_quality<.45 or portfolio_drawdown>=.08 or ensemble_disagreement>=.80)
        usable=total*(0.0 if risk_off else clamp(system_quality*(1-.55*clamp(ensemble_disagreement,0,1))*(1-min(.75,portfolio_drawdown*5)),0,1))
        if low_opportunity:usable=0.0
        if policy=="EQUAL_RISK":weights={x.strategy_id:1/max(1,len(items)) for x in items}
        elif policy=="FIXED_RISK":weights={x.strategy_id:max(0,x.requested_risk) for x in items};s=sum(weights.values()) or 1;weights={k:v/s for k,v in weights.items()}
        elif policy=="VOLATILITY_WEIGHTED":weights={x.strategy_id:1/max(.20,x.volatility) for x in items};s=sum(weights.values()) or 1;weights={k:v/s for k,v in weights.items()}
        elif policy=="PERFORMANCE_WEIGHTED":weights={x.strategy_id:max(.01,x.recent_performance+1) for x in items};s=sum(weights.values()) or 1;weights={k:v/s for k,v in weights.items()}
        else:
            s=sum(raw.values());weights={k:(v/s if s>0 else 0) for k,v in raw.items()}
        alloc={x.strategy_id:min(max(0,x.requested_risk),usable*weights.get(x.strategy_id,0)) for x in items}
        alloc,cap_reasons=self._caps(items,alloc,total)
        # Correlated-cluster cap using graph components.
        ids=[x.strategy_id for x in items];seen=set()
        for sid in ids:
            if sid in seen:continue
            stack=[sid];comp=[]
            while stack:
                a=stack.pop()
                if a in seen:continue
                seen.add(a);comp.append(a)
                stack += [b for b,v in corr["matrix"].get(a,{}).items() if b not in seen and b!=a and v>=self.correlation_threshold]
            if len(comp)>1:
                s=sum(alloc.get(x,0) for x in comp);cap=total*self.max_cluster_risk
                if s>cap and s>0:
                    scale=cap/s
                    for x in comp:alloc[x]*=scale
                    cap_reasons.append("MAX_CORRELATED_CLUSTER:"+",".join(sorted(comp)))
        # Change smoothing / no performance chasing.
        prev=self._previous();turnover=0.0;churn=False
        if prev and policy=="DYNAMIC":
            old=prev.get("allocations",{});age=parse_ts(prev.get("ts"));cool=age and datetime.now(timezone.utc)-age<timedelta(hours=self.change_cooldown_hours)
            for sid in alloc:
                p=finite(old.get(sid),0);target=alloc[sid];maxchg=total*self.max_change_per_cycle
                if cool and target>p:target=p # no upward churn during cooldown
                target=max(p-maxchg,min(p+maxchg,target));alloc[sid]=max(0,target);turnover+=abs(target-p)
            churn=turnover>total*.20
        alloc,more=self._caps(items,alloc,total);cap_reasons+=more
        # Never exceed requested or authorized total after all transforms.
        for x in items:alloc[x.strategy_id]=min(alloc.get(x.strategy_id,0),max(0,x.requested_risk))
        used=min(total,sum(alloc.values()));
        if sum(alloc.values())>total and sum(alloc.values())>0:
            sc=total/sum(alloc.values());alloc={k:v*sc for k,v in alloc.items()};used=total
        div=self.diversification_score(items,corr);heat=self.portfolio_heat(items,alloc,corr,portfolio_drawdown)
        if heat>=self.heat_limit and used>0:
            for _ in range(12):
                if heat<=self.heat_limit+1e-9: break
                scale=max(0,min(.95,self.heat_limit/max(heat,1e-9)))
                alloc={k:v*scale for k,v in alloc.items()};used=sum(alloc.values());heat=self.portfolio_heat(items,alloc,corr,portfolio_drawdown)
            cap_reasons.append("PORTFOLIO_HEAT_REDUCTION")
        # Stress: correlations move toward 1 and volatility spikes.
        stress_corr={"matrix":{a:{b:(1.0 if a==b else max(.85,v)) for b,v in row.items()} for a,row in corr["matrix"].items()}}
        stress_heat=self.portfolio_heat(items,alloc,stress_corr,portfolio_drawdown,1.35)
        mrc={}
        base=heat
        eps=max(total*.01,.01)
        for x in items:
            trial=dict(alloc);trial[x.strategy_id]=min(x.requested_risk,trial.get(x.strategy_id,0)+eps)
            mrc[x.strategy_id]=max(0,(self.portfolio_heat(items,trial,corr,portfolio_drawdown)-base)/eps)
        efficiency=(sum(scored[s]["reliability_adjusted_edge"]*alloc.get(s,0) for s in alloc)/max(used,1e-9)) if used else 0.0
        unused=max(0,total-used);version=f"alloc-{self.version}-{uuid.uuid4().hex[:12]}";alerts=[]
        if corr["high_pairs"]:alerts.append("HIDDEN_CONCENTRATION_DETECTED")
        if stress_heat>max(self.heat_limit,heat*1.25):alerts.append("CORRELATION_SPIKE")
        if heat>=self.heat_limit*.90:alerts.append("PORTFOLIO_HEAT_HIGH")
        if churn:alerts.append("ALLOCATION_CHURN_DETECTED")
        if low_opportunity:alerts.append("LOW_OPPORTUNITY_ENVIRONMENT")
        if reallocation_cost>max(.01,efficiency*.25):alerts.append("REALLOCATION_COST_TOO_HIGH")
        if used>total+1e-9:alerts.append("RISK_BUDGET_EXCEEDED")
        reasons=[]
        if risk_off:reasons.append("RISK_OFF")
        if low_opportunity:reasons.append("KEEP_RISK_UNALLOCATED")
        reasons+=cap_reasons
        strategy_rows={}
        for x in items:
            a=alloc.get(x.strategy_id,0);strategy_rows[x.strategy_id]={"risk_allocated":a,"capital_allocated":None,
                "strategy_allocation_percentage":a/total if total else 0,"allocation_confidence":scored[x.strategy_id]["certainty"],
                "reliability_adjusted_edge":scored[x.strategy_id]["reliability_adjusted_edge"],"marginal_risk_contribution":mrc[x.strategy_id],
                "allocation_reason":";".join(reasons or ["DIVERSIFIED_RISK_BUDGET"]),"family":x.family,"symbol":x.symbol,"direction":x.direction}
        result={"enabled":True,"mode":self.mode,"hypothetical_only":self.mode=="SHADOW","signal_authority":False,"order_authority":False,"risk_limit_authority":False,
            "allocation_version":version,"policy":policy,"authorized_total_risk":total,"used_risk_budget":used,"unused_risk_budget":unused,
            "allocations":alloc,"strategies":strategy_rows,"portfolio_heat":heat,"stress_heat":stress_heat,"diversification_score":div,
            "allocation_efficiency_score":efficiency,"allocation_turnover":turnover,"risk_off":risk_off,"low_opportunity":low_opportunity,
            "correlations":corr,"alerts":sorted(set(alerts)),"reason":";".join(reasons or ["NORMAL_SHADOW_ALLOCATION"]),
            "limits":{"max_strategy":self.max_strategy_allocation,"max_family":self.max_family_risk,"max_symbol":self.max_symbol_risk,"max_asset":self.max_asset_risk,"max_directional":self.max_directional_risk,"max_cluster":self.max_cluster_risk,"heat_limit":self.heat_limit},
            "stress":{"correlation_stress_heat":stress_heat,"volatility_spike_multiplier":1.35}}
        self._persist(result,items);return result
    def _persist(self,r,items):
        c=self.conn();c.execute("""INSERT INTO allocation_decisions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (r["allocation_version"],now_iso(),r["mode"],r["policy"],r["authorized_total_risk"],r["used_risk_budget"],r["unused_risk_budget"],r["portfolio_heat"],r["diversification_score"],r["allocation_efficiency_score"],r["allocation_turnover"],int(r["risk_off"]),r["reason"],json.dumps(r["allocations"]),json.dumps(r["limits"]),json.dumps(r["correlations"]),json.dumps(r["stress"])))
        sm={x.strategy_id:x for x in items}
        for sid,d in r["strategies"].items():
            x=sm[sid];c.execute("""INSERT INTO allocation_strategy_history(allocation_version,ts,strategy_id,family,symbol,direction,requested_risk,allocated_risk,allocation_percentage,allocation_confidence,reliability_adjusted_edge,marginal_risk_contribution,reason,details_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
             (r["allocation_version"],now_iso(),sid,x.family,x.symbol,x.direction,x.requested_risk,d["risk_allocated"],d["strategy_allocation_percentage"],d["allocation_confidence"],d["reliability_adjusted_edge"],d["marginal_risk_contribution"],d["allocation_reason"],json.dumps(asdict(x))))
        c.commit();c.close()
    def baselines(self,items,authorized_total_risk,**kwargs):
        return {p:self.allocate(items,authorized_total_risk,policy=p,**kwargs) for p in BASELINES}
    def shadow_compare(self,allocation_version,current,metrics):
        c=self.conn();r=c.execute("SELECT allocations_json FROM allocation_decisions WHERE allocation_version=?",(allocation_version,)).fetchone()
        if not r:c.close();return False
        c.execute("INSERT INTO allocation_shadow_comparisons(ts,allocation_version,current_json,dynamic_json,metrics_json) VALUES(?,?,?,?,?)",(now_iso(),allocation_version,json.dumps(current),r["allocations_json"],json.dumps(metrics)));c.commit();c.close();return True
    def candidate_policy(self,parent,proposal,evidence):
        cid="alloc-candidate-"+uuid.uuid4().hex[:12];c=self.conn();c.execute("INSERT INTO allocation_policy_candidates VALUES(?,?,?,?,?,?,0)",(cid,now_iso(),parent,"VALIDATION_REQUIRED",json.dumps(proposal),json.dumps(evidence)));c.commit();c.close();return {"candidate_id":cid,"status":"VALIDATION_REQUIRED","path":["SHADOW","VALIDATION","PAPER","CANARY","LIMITED_LIVE"],"auto_deploy":False}
    def dashboard(self):
        c=self.conn();r=c.execute("SELECT * FROM allocation_decisions ORDER BY ts DESC LIMIT 1").fetchone();rows=c.execute("SELECT * FROM allocation_strategy_history ORDER BY id DESC LIMIT 100").fetchall();c.close()
        latest=dict(r) if r else None
        if latest:
            latest["allocations"]=json.loads(latest.pop("allocations_json"));latest["limits"]=json.loads(latest.pop("limits_json"));latest["correlations"]=json.loads(latest.pop("correlations_json"));latest["stress"]=json.loads(latest.pop("stress_json"))
        return {"enabled":True,"mode":self.mode,"latest":latest,"strategies":[dict(x) for x in rows],"production_authority":False,"martingale":False}

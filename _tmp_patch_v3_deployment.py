from pathlib import Path

def replace_once(path,old,new):
    p=Path(path);s=p.read_text(encoding='utf-8')
    if s.count(old)!=1:raise SystemExit(f'{path}: anchor count={s.count(old)}')
    p.write_text(s.replace(old,new,1),encoding='utf-8')

p='deployment_runtime.py'
schema='''        CREATE TABLE IF NOT EXISTS deployment_managed_paper_strategies(\n          managed_release_identity TEXT PRIMARY KEY,candidate_id TEXT NOT NULL,candidate_definition_sha256 TEXT NOT NULL,\n          source_code_sha TEXT NOT NULL,instrument TEXT NOT NULL,confidence_class TEXT NOT NULL,experimental INTEGER NOT NULL,\n          paper_only INTEGER NOT NULL DEFAULT 1,production_authority INTEGER NOT NULL DEFAULT 0,lifecycle_state TEXT NOT NULL,\n          new_trades_enabled INTEGER NOT NULL DEFAULT 1,metrics_json TEXT NOT NULL DEFAULT '{}',block_reason TEXT,\n          activated_ts TEXT,retired_ts TEXT,created_ts TEXT NOT NULL,updated_ts TEXT NOT NULL);\n        CREATE TABLE IF NOT EXISTS deployment_managed_paper_feedback(\n          trade_id TEXT PRIMARY KEY,managed_release_identity TEXT,candidate_id TEXT,candidate_definition_sha256 TEXT,\n          source_code_sha TEXT,instrument TEXT NOT NULL,outcome TEXT NOT NULL,realized_r REAL,attribution_status TEXT NOT NULL,\n          resolved_ts TEXT,details_json TEXT NOT NULL DEFAULT '{}',created_ts TEXT NOT NULL);\n        CREATE INDEX IF NOT EXISTS idx_managed_paper_instrument ON deployment_managed_paper_strategies(instrument,lifecycle_state,updated_ts);\n        CREATE INDEX IF NOT EXISTS idx_managed_paper_feedback_release ON deployment_managed_paper_feedback(managed_release_identity,resolved_ts);\n'''
replace_once(p,'        CREATE INDEX IF NOT EXISTS idx_deploy_stage ON deployment_registry(current_stage,updated_ts);\n',schema+'        CREATE INDEX IF NOT EXISTS idx_deploy_stage ON deployment_registry(current_stage,updated_ts);\n')
methods=r'''
    def _managed_paper_identity(self, identity):
        x=dict(identity or {})
        if x.get("active") is not True:raise ValueError("managed PAPER identity is not active")
        instrument=str(x.get("instrument") or "").upper();cid=str(x.get("v3_candidate_id") or "")
        dsha=str(x.get("v3_candidate_definition_sha256") or "").lower();cls=str(x.get("v3_confidence_class") or "").upper()
        rid=str(x.get("v3_managed_release_identity") or "");code=str(x.get("v3_source_code_sha") or "").lower();exp=x.get("v3_experimental") is True
        if instrument not in self.allowed_symbols:raise ValueError("managed PAPER instrument is not execution-authorized")
        if not cid or len(dsha)!=64 or cls not in ("STANDARD","EXPERIMENTAL"):raise ValueError("managed PAPER candidate identity invalid")
        if exp!=(cls=="EXPERIMENTAL"):raise ValueError("managed PAPER confidence metadata inconsistent")
        if x.get("v3_paper_only") is not True or x.get("production_authority") is not False:raise ValueError("managed PAPER identity lacks PAPER-only authority")
        if not rid.startswith("v3paper_") or len(code)!=40:raise ValueError("managed PAPER release identity invalid")
        return {"managed_release_identity":rid,"candidate_id":cid,"candidate_definition_sha256":dsha,"source_code_sha":code,
                "instrument":instrument,"confidence_class":cls,"experimental":exp,"paper_only":True,"production_authority":False}

    def register_managed_paper(self, identity, activate=True):
        x=self._managed_paper_identity(identity);c=self.conn();ts=now_iso()
        row=c.execute("SELECT * FROM deployment_managed_paper_strategies WHERE managed_release_identity=?",(x["managed_release_identity"],)).fetchone()
        if row:
            row=dict(row);immutable=("candidate_id","candidate_definition_sha256","source_code_sha","instrument","confidence_class")
            if any(str(row[k])!=str(x[k]) for k in immutable) or bool(row["experimental"])!=x["experimental"] or not bool(row["paper_only"]) or bool(row["production_authority"]):
                c.close();raise ValueError("managed PAPER release identity collision")
            state=row["lifecycle_state"]
        else:
            state="PAPER_ACTIVE" if activate else "PAPER_RETIRED"
            c.execute("""INSERT INTO deployment_managed_paper_strategies(managed_release_identity,candidate_id,candidate_definition_sha256,source_code_sha,
              instrument,confidence_class,experimental,paper_only,production_authority,lifecycle_state,new_trades_enabled,activated_ts,retired_ts,created_ts,updated_ts)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (x["managed_release_identity"],x["candidate_id"],x["candidate_definition_sha256"],x["source_code_sha"],x["instrument"],x["confidence_class"],
               int(x["experimental"]),1,0,state,1 if activate else 0,ts if activate else None,None if activate else ts,ts,ts))
        if activate:
            c.execute("""UPDATE deployment_managed_paper_strategies SET lifecycle_state='PAPER_RETIRED',new_trades_enabled=0,retired_ts=?,updated_ts=?
                         WHERE instrument=? AND managed_release_identity<>? AND lifecycle_state='PAPER_ACTIVE'""",
                      (ts,ts,x["instrument"],x["managed_release_identity"]))
            if state=="PAPER_RETIRED":
                c.execute("""UPDATE deployment_managed_paper_strategies SET lifecycle_state='PAPER_ACTIVE',new_trades_enabled=1,activated_ts=?,retired_ts=NULL,updated_ts=?
                             WHERE managed_release_identity=?""",(ts,ts,x["managed_release_identity"]))
        c.commit();out=c.execute("SELECT * FROM deployment_managed_paper_strategies WHERE managed_release_identity=?",(x["managed_release_identity"],)).fetchone();c.close()
        return dict(out)

    def managed_paper_release(self, release_id):
        c=self.conn();r=c.execute("SELECT * FROM deployment_managed_paper_strategies WHERE managed_release_identity=?",(release_id,)).fetchone();c.close()
        return dict(r) if r else None

    def managed_paper_metrics(self, release_id):
        c=self.conn();rows=[dict(r) for r in c.execute("""SELECT outcome,realized_r FROM deployment_managed_paper_feedback
          WHERE managed_release_identity=? AND attribution_status='ATTRIBUTED' ORDER BY resolved_ts,trade_id""",(release_id,)).fetchall()];c.close()
        counts={k:0 for k in ("WIN","LOSS","TIMEOUT","AMBIGUOUS")};vals=[]
        for r in rows:
            o=str(r.get("outcome") or "").upper()
            if o in counts:counts[o]+=1
            if o in ("WIN","LOSS") and r.get("realized_r") is not None:vals.append(float(r["realized_r"]))
        m=r_metrics(vals);m.update({"results":len(rows),"outcomes":counts,"binary_resolved":len(vals)})
        return m

    def evaluate_managed_paper(self, release_id):
        row=self.managed_paper_release(release_id)
        if not row:return {"action":"NOT_REGISTERED","reasons":[]}
        m=self.managed_paper_metrics(release_id);reasons=[]
        if m.get("max_drawdown_r",0)*self.base_risk_fraction>=self.max_drawdown:reasons.append("PAPER_DRAWDOWN_LIMIT")
        if m.get("max_consecutive_losses",0)>=self.max_consecutive_losses:reasons.append("PAPER_CONSECUTIVE_LOSS_LIMIT")
        if reasons and row["lifecycle_state"]=="PAPER_ACTIVE":
            c=self.conn();c.execute("""UPDATE deployment_managed_paper_strategies SET lifecycle_state='PAPER_DEGRADED',new_trades_enabled=0,
              metrics_json=?,block_reason=?,updated_ts=? WHERE managed_release_identity=?""",
              (json.dumps(m,separators=(",",":")),"; ".join(reasons),now_iso(),release_id));c.commit();c.close()
        else:
            c=self.conn();c.execute("UPDATE deployment_managed_paper_strategies SET metrics_json=?,updated_ts=? WHERE managed_release_identity=?",
                                  (json.dumps(m,separators=(",",":")),now_iso(),release_id));c.commit();c.close()
        return {"action":"BLOCK_NEW_ENTRIES" if reasons and row["lifecycle_state"]=="PAPER_ACTIVE" else "HOLD","reasons":reasons,
                "metrics":m,"release":self.managed_paper_release(release_id)}

    def managed_paper_entry_gate(self, identity):
        x=self._managed_paper_identity(identity);row=self.register_managed_paper(identity,activate=True);kills=[]
        for scope in ("SYSTEM","ALL_CANDIDATES",f"CANDIDATE:{x['candidate_id']}",f"V3_RELEASE:{x['managed_release_identity']}"):
            if self.kill(scope).get("active"):kills.append(scope)
        if kills and row["lifecycle_state"]=="PAPER_ACTIVE":
            c=self.conn();c.execute("""UPDATE deployment_managed_paper_strategies SET lifecycle_state='PAPER_KILLED',new_trades_enabled=0,block_reason=?,updated_ts=?
              WHERE managed_release_identity=?""",("kill switch: "+",".join(kills),now_iso(),x["managed_release_identity"]));c.commit();c.close()
            row=self.managed_paper_release(x["managed_release_identity"])
        blocked=row["lifecycle_state"] in ("PAPER_DEGRADED","PAPER_KILLED","PAPER_RETIRED") or not bool(row["new_trades_enabled"])
        return {"allow":not blocked and not kills,"state":row["lifecycle_state"],"reasons":kills or ([row.get("block_reason") or row["lifecycle_state"]] if blocked else []),
                "managed_release_identity":x["managed_release_identity"],"candidate_id":x["candidate_id"],"instrument":x["instrument"],"production_authority":False}

    def ingest_managed_paper_result(self, identity, trade_id, outcome, realized_r=None, resolved_ts=None, details=None):
        x=self._managed_paper_identity(identity);tid=str(trade_id or "");outcome=str(outcome or "").upper()
        if not tid:raise ValueError("trade_id required for managed PAPER feedback")
        if outcome not in ("WIN","LOSS","TIMEOUT","AMBIGUOUS"):raise ValueError("unsupported managed PAPER outcome")
        self.register_managed_paper(identity,activate=False);rr=f(realized_r);c=self.conn();old=c.execute("SELECT * FROM deployment_managed_paper_feedback WHERE trade_id=?",(tid,)).fetchone()
        if old:
            old=dict(old);c.close()
            if old.get("managed_release_identity")!=x["managed_release_identity"] or old.get("outcome")!=outcome or f(old.get("realized_r"))!=rr:raise ValueError("trade feedback identity conflict")
            return {"ingested":False,"duplicate":True,"trade_id":tid}
        c.execute("""INSERT INTO deployment_managed_paper_feedback(trade_id,managed_release_identity,candidate_id,candidate_definition_sha256,source_code_sha,
          instrument,outcome,realized_r,attribution_status,resolved_ts,details_json,created_ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          (tid,x["managed_release_identity"],x["candidate_id"],x["candidate_definition_sha256"],x["source_code_sha"],x["instrument"],outcome,rr,
           "ATTRIBUTED",resolved_ts or now_iso(),json.dumps(details or {},separators=(",",":"),default=str),now_iso()))
        c.commit();c.close();return {"ingested":True,"duplicate":False,"trade_id":tid,"outcome":outcome,"evaluation":self.evaluate_managed_paper(x["managed_release_identity"])}

    @staticmethod
    def _managed_paper_outcome(row):
        try:ctx=json.loads(row.get("exit_context_json") or "{}")
        except Exception:ctx={}
        explicit=str(ctx.get("outcome") or "").upper()
        if explicit in ("WIN","LOSS","TIMEOUT","AMBIGUOUS"):return explicit
        try:reasons=[str(x).upper() for x in json.loads(row.get("exit_reasons_json") or "[]")]
        except Exception:reasons=[]
        if any("TIMEOUT" in x for x in reasons):return "TIMEOUT"
        if any("AMBIGUOUS" in x for x in reasons):return "AMBIGUOUS"
        rr=f(row.get("realized_r"));net=f(row.get("net_result"));value=rr if rr is not None else net
        if value is None or value==0:return "AMBIGUOUS"
        return "WIN" if value>0 else "LOSS"

    def reconcile_managed_paper_trade_memory(self, limit=200):
        c=self.conn();rows=[dict(r) for r in c.execute("""SELECT tm.* FROM trade_memory tm LEFT JOIN deployment_managed_paper_feedback fb ON fb.trade_id=tm.trade_id
          WHERE tm.status='CLOSED' AND fb.trade_id IS NULL AND tm.entry_context_json LIKE '%\"v3_managed_strategy\"%' ORDER BY tm.id LIMIT ?""",(int(limit),)).fetchall()];c.close()
        processed=attributed=non_v3=0;errors=[]
        for row in rows:
            try:
                ctx=json.loads(row.get("entry_context_json") or "{}");identity=ctx.get("v3_managed_strategy") or {};outcome=self._managed_paper_outcome(row)
                if identity.get("active") is True:
                    self.ingest_managed_paper_result(identity,row["trade_id"],outcome,row.get("realized_r"),row.get("exit_ts"),
                                                     {"net_result":row.get("net_result"),"source":"trade_memory_reconciliation"});attributed+=1
                else:
                    c=self.conn();c.execute("""INSERT OR IGNORE INTO deployment_managed_paper_feedback(trade_id,instrument,outcome,realized_r,attribution_status,resolved_ts,details_json,created_ts)
                      VALUES(?,?,?,?,?,?,?,?)""",(row["trade_id"],str(row.get("symbol") or ""),outcome,f(row.get("realized_r")),"NON_V3_UNATTRIBUTED",
                       row.get("exit_ts") or now_iso(),json.dumps({"source":"trade_memory_reconciliation"}),now_iso()));c.commit();c.close();non_v3+=1
                processed+=1
            except Exception as exc:errors.append({"trade_id":row.get("trade_id"),"error":str(exc)})
        return {"checked":len(rows),"processed":processed,"attributed":attributed,"non_v3":non_v3,"errors":errors}

'''
replace_once(p,'    def dashboard(self):\n',methods+'    def dashboard(self):\n')
replace_once(p,
'''        sw=[dict(x) for x in c.execute("SELECT * FROM deployment_kill_switches ORDER BY scope").fetchall()];c.close()\n        for r in rows:\n            r["live_metrics"]=self.live_metrics(r["candidate_id"]);r["cooldown"]=self.cooldown(r["candidate_id"])\n        return {"deployments":rows,"kill_switches":sw,"live_enabled":self.live_enabled,"auto_promotion":False}\n''',
'''        sw=[dict(x) for x in c.execute("SELECT * FROM deployment_kill_switches ORDER BY scope").fetchall()]\n        managed=[dict(x) for x in c.execute("SELECT * FROM deployment_managed_paper_strategies ORDER BY updated_ts DESC").fetchall()];c.close()\n        for r in rows:r["live_metrics"]=self.live_metrics(r["candidate_id"]);r["cooldown"]=self.cooldown(r["candidate_id"])\n        for r in managed:r["paper_feedback_metrics"]=self.managed_paper_metrics(r["managed_release_identity"])\n        return {"deployments":rows,"managed_paper":managed,"kill_switches":sw,"live_enabled":self.live_enabled,"auto_promotion":False}\n''')

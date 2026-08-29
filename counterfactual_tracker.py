from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

TERMINAL = {"WIN", "LOSS", "TIMEOUT", "AMBIGUOUS", "INVALIDATED", "CANCELLED"}
SELECTION_REASONS = {"LOWER_RANK", "NO_SLOT", "BEST_SAFE_SET_NOT_SELECTED"}
SAFETY_REASONS = {
    "PORTFOLIO_RISK", "CORRELATION", "METADATA", "BROKER_RISK", "GLOBAL_GATE",
    "RECOVERY", "SECURITY", "SINGLE_EXECUTION_WORKER_REQUIRED",
}
EXECUTION_REASONS = {"BROKER_EXPLICIT_REJECTION", "UNKNOWN_AFTER_SUBMIT"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _finite(value: Any) -> Optional[float]:
    try:
        x = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return x if math.isfinite(x) else None


def evidence_grade(n: int) -> str:
    if int(n) < 15:
        return "UNDERPOWERED"
    if int(n) < 30:
        return "WEAK_LIMITED_EVIDENCE"
    return "USABLE"


def deterministic_counterfactual_id(cycle_id: str, instrument: str, signal_id: Any,
                                    market_time: Any, side: str, entry: Any,
                                    stop: Any, target: Any) -> str:
    payload = "|".join(map(str, (cycle_id, signal_id or "", instrument, side, market_time or "",
                                 entry, stop, target)))
    return "cf_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


class CounterfactualTracker:
    """SHADOW-only selector observability. It has no execution or research authority."""
    execution_authority = False
    research_authority = False
    look_ahead = False

    def __init__(self, db_path: str, horizon_bars: int = 180):
        self.db_path = db_path
        self.horizon_bars = max(1, int(horizon_bars))
        self.ensure_schema()

    def conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.db_path)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=5000")
        c.execute("PRAGMA synchronous=NORMAL")
        return c

    def ensure_schema(self) -> None:
        c = self.conn()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS counterfactual_opportunities(
          counterfactual_id TEXT PRIMARY KEY,
          cycle_id TEXT NOT NULL,
          signal_id INTEGER,
          decision_id TEXT,
          instrument TEXT NOT NULL,
          side TEXT NOT NULL,
          strategy TEXT,
          market_time TEXT NOT NULL,
          entry REAL NOT NULL,
          stop REAL NOT NULL,
          target REAL NOT NULL,
          initial_risk REAL NOT NULL,
          target_r REAL NOT NULL,
          rank INTEGER NOT NULL,
          rank_score REAL NOT NULL,
          slot_capacity INTEGER NOT NULL,
          slots_available INTEGER NOT NULL,
          cycle_size INTEGER NOT NULL,
          winner_instrument TEXT,
          winner_rank INTEGER,
          winner_score REAL,
          winner_signal_id INTEGER,
          winner_trade_id TEXT,
          winner_intent_id TEXT,
          rejection_reason TEXT NOT NULL,
          rejection_category TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'OPEN',
          result_r REAL,
          opened_at TEXT NOT NULL,
          resolved_at TEXT,
          duration_seconds REAL,
          bars_observed INTEGER NOT NULL DEFAULT 0,
          pre_entry_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(cycle_id,instrument,signal_id,market_time,entry,stop,target)
        );
        CREATE INDEX IF NOT EXISTS idx_counterfactual_status_instrument_time
          ON counterfactual_opportunities(status,instrument,market_time);
        CREATE INDEX IF NOT EXISTS idx_counterfactual_cycle ON counterfactual_opportunities(cycle_id);
        CREATE INDEX IF NOT EXISTS idx_counterfactual_winner ON counterfactual_opportunities(winner_instrument,status);
        CREATE INDEX IF NOT EXISTS idx_counterfactual_rejection ON counterfactual_opportunities(rejection_category,rejection_reason);

        CREATE TABLE IF NOT EXISTS counterfactual_tracker_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL,
          cycle_id TEXT,
          counterfactual_id TEXT,
          event_type TEXT NOT NULL,
          detail_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_counterfactual_events_cycle ON counterfactual_tracker_events(cycle_id,ts);
        """)
        c.commit(); c.close()

    def event(self, event_type: str, *, cycle_id: Optional[str] = None,
              counterfactual_id: Optional[str] = None, detail: Optional[Mapping[str, Any]] = None) -> None:
        c = self.conn()
        c.execute("INSERT INTO counterfactual_tracker_events(ts,cycle_id,counterfactual_id,event_type,detail_json) VALUES(?,?,?,?,?)",
                  (_now(), cycle_id, counterfactual_id, event_type, json.dumps(dict(detail or {}), sort_keys=True, default=str)))
        c.commit(); c.close()

    def record_selection_rejected(self, *, cycle_id: str, candidate: Mapping[str, Any], rank: int,
                                  rank_score: float, components: Mapping[str, Any], slot_capacity: int,
                                  slots_available: int, cycle_size: int, winner: Optional[Mapping[str, Any]],
                                  rejection_reason: str = "NO_SLOT") -> Dict[str, Any]:
        if rejection_reason not in SELECTION_REASONS:
            raise ValueError("counterfactual primary evidence accepts SELECTION_REJECTED reasons only")
        inst = str(candidate.get("instrument") or "")
        side = str(candidate.get("signal") or candidate.get("side") or "")
        market_time = candidate.get("candle_ts") or candidate.get("market_time")
        entry, stop = _finite(candidate.get("entry")), _finite(candidate.get("stop"))
        target = _finite(candidate.get("managed_target", candidate.get("target")))
        if not inst or side not in {"BUY", "SELL"} or not market_time or None in (entry, stop, target):
            raise ValueError("invalid counterfactual candidate geometry")
        risk = abs(entry - stop)
        if risk <= 0:
            raise ValueError("invalid counterfactual initial risk")
        target_r = abs(target - entry) / risk
        batch = candidate.get("_batch_context") or {}
        signal_id = batch.get("signal_id") or candidate.get("signal_id")
        cid = deterministic_counterfactual_id(cycle_id, inst, signal_id, market_time, side, entry, stop, target)
        winner = dict(winner or {})
        snapshot = {
            "rr": candidate.get("rr_raw", candidate.get("rr")),
            "rank_components": dict(components or {}),
            "score": candidate.get("score"),
            "confidence": candidate.get("dynamic_confidence", candidate.get("confidence")),
            "room_to_barrier_r": candidate.get("room_to_barrier_r"),
            "spread_pips": candidate.get("spread_pips", candidate.get("spread")),
            "features": candidate.get("features") or candidate.get("features_json"),
            "market_regime": candidate.get("market_regime"),
            "session": candidate.get("session"),
            "news_context": candidate.get("news_context"),
            "look_ahead": False,
            "execution_authority": False,
            "research_authority": False,
        }
        now = _now()
        c = self.conn()
        c.execute("""INSERT OR IGNORE INTO counterfactual_opportunities(
          counterfactual_id,cycle_id,signal_id,decision_id,instrument,side,strategy,market_time,
          entry,stop,target,initial_risk,target_r,rank,rank_score,slot_capacity,slots_available,cycle_size,
          winner_instrument,winner_rank,winner_score,winner_signal_id,winner_trade_id,winner_intent_id,
          rejection_reason,rejection_category,status,opened_at,pre_entry_json,created_at,updated_at)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'OPEN',?,?,?,?)""",
          (cid,cycle_id,signal_id,batch.get("director_id"),inst,side,candidate.get("setup_variant"),str(market_time),
           entry,stop,target,risk,target_r,int(rank),float(rank_score),int(slot_capacity),int(slots_available),int(cycle_size),
           winner.get("instrument"),winner.get("rank"),winner.get("rank_score"),winner.get("signal_id"),
           winner.get("trade_id"),winner.get("intent_id"),rejection_reason,"SELECTION_REJECTED",
           str(market_time),json.dumps(snapshot,sort_keys=True,default=str),now,now))
        created = c.total_changes > 0
        row = c.execute("SELECT * FROM counterfactual_opportunities WHERE counterfactual_id=?", (cid,)).fetchone()
        c.commit(); c.close()
        if created:
            self.event("COUNTERFACTUAL_OPENED",cycle_id=cycle_id,counterfactual_id=cid,
                       detail={"instrument":inst,"rank":rank,"winner":winner.get("instrument"),"reason":rejection_reason})
        return {"created": created, "counterfactual": dict(row), "execution_authority": False}

    def record_non_counterfactual_rejection(self, *, cycle_id: str, instrument: str, reason: str,
                                             category: str, detail: Optional[Mapping[str, Any]] = None) -> None:
        self.event("NON_COUNTERFACTUAL_REJECTION", cycle_id=cycle_id,
                   detail={"instrument":instrument,"reason":reason,"category":category,**dict(detail or {})})

    def link_winner(self, cycle_id: str, *, instrument: str, rank: int, rank_score: float,
                    signal_id: Any = None, trade_id: Any = None, intent_id: Any = None) -> None:
        c = self.conn()
        c.execute("""UPDATE counterfactual_opportunities SET winner_instrument=?,winner_rank=?,winner_score=?,
                     winner_signal_id=COALESCE(?,winner_signal_id),winner_trade_id=COALESCE(?,winner_trade_id),
                     winner_intent_id=COALESCE(?,winner_intent_id),updated_at=? WHERE cycle_id=?""",
                  (instrument,int(rank),float(rank_score),signal_id,trade_id,intent_id,_now(),cycle_id))
        c.commit(); c.close()

    def open_for_instrument(self, instrument: str) -> List[Dict[str, Any]]:
        c = self.conn()
        rows = [dict(x) for x in c.execute("""SELECT * FROM counterfactual_opportunities
          WHERE status='OPEN' AND instrument=? ORDER BY market_time,counterfactual_id""",(instrument,)).fetchall()]
        c.close(); return rows

    def resolve_open(self, instrument: str, candles: Iterable[Mapping[str, Any]]) -> int:
        rows = self.open_for_instrument(instrument)
        candles = list(candles or [])
        changed = 0
        for row in rows:
            market_dt = _dt(row["market_time"])
            if not market_dt:
                self._terminal(row,"INVALIDATED",None,0,"invalid market_time")
                changed += 1; continue
            # Strict no-look-ahead: only bars whose timestamp is strictly AFTER T.
            post = []
            for candle in candles:
                cdt = _dt(candle.get("t"))
                if cdt and cdt > market_dt:
                    post.append((cdt,candle))
            post.sort(key=lambda x:x[0])
            observed = post[:self.horizon_bars]
            terminal = None
            for idx,(cdt,candle) in enumerate(observed,1):
                high, low = _finite(candle.get("h")), _finite(candle.get("l"))
                if high is None or low is None:
                    continue
                if row["side"] == "BUY":
                    tp_hit, sl_hit = high >= row["target"], low <= row["stop"]
                else:
                    tp_hit, sl_hit = low <= row["target"], high >= row["stop"]
                if tp_hit and sl_hit:
                    terminal=("AMBIGUOUS",None,idx,cdt,"same bar touched stop and target; intrabar order unavailable")
                    break
                if tp_hit:
                    terminal=("WIN",float(row["target_r"]),idx,cdt,"target touched before stop")
                    break
                if sl_hit:
                    terminal=("LOSS",-1.0,idx,cdt,"stop touched before target")
                    break
            if terminal:
                self._terminal(row,*terminal)
                changed += 1
            elif len(post) >= self.horizon_bars:
                cdt = post[self.horizon_bars-1][0]
                self._terminal(row,"TIMEOUT",None,self.horizon_bars,cdt,"shadow horizon expired without stop/target")
                changed += 1
            elif observed:
                c = self.conn(); c.execute("UPDATE counterfactual_opportunities SET bars_observed=?,updated_at=? WHERE counterfactual_id=?",
                                           (len(observed),_now(),row["counterfactual_id"])); c.commit(); c.close()
        return changed

    def _terminal(self, row: Mapping[str, Any], status: str, result_r: Optional[float], bars: int,
                  resolved_at: Any, note: str = "") -> None:
        if status not in TERMINAL:
            raise ValueError(status)
        resolved_dt = resolved_at if isinstance(resolved_at, datetime) else _dt(resolved_at)
        opened = _dt(row.get("opened_at") or row.get("market_time"))
        duration = (resolved_dt-opened).total_seconds() if resolved_dt and opened else None
        resolved_iso = resolved_dt.isoformat() if resolved_dt else _now()
        c = self.conn()
        c.execute("""UPDATE counterfactual_opportunities SET status=?,result_r=?,resolved_at=?,duration_seconds=?,
                     bars_observed=?,updated_at=? WHERE counterfactual_id=? AND status='OPEN'""",
                  (status,result_r,resolved_iso,duration,int(bars),_now(),row["counterfactual_id"]))
        changed = c.total_changes > 0; c.commit(); c.close()
        if changed:
            self.event("COUNTERFACTUAL_RESOLVED",cycle_id=row.get("cycle_id"),counterfactual_id=row["counterfactual_id"],
                       detail={"status":status,"result_r":result_r,"bars":bars,"note":note,"look_ahead":False})

    def instrument_reliability_report(self, instrument: str, recent_n: int = 20) -> Dict[str, Any]:
        c = self.conn()
        shadow=[dict(x) for x in c.execute("""SELECT status,result_r,resolved_at FROM counterfactual_opportunities
          WHERE instrument=? AND rejection_category='SELECTION_REJECTED' ORDER BY market_time,counterfactual_id""",(instrument,)).fetchall()]
        executed=[dict(x) for x in c.execute("""SELECT status,realized_r,exit_ts FROM trade_memory
          WHERE symbol=? ORDER BY entry_ts,id""",(instrument,)).fetchall()]
        c.close()
        def metrics(rows: List[Dict[str,Any]], rkey: str) -> Dict[str,Any]:
            vals=[float(x[rkey]) for x in rows if x.get(rkey) is not None and x.get("status") in {"WIN","LOSS","CLOSED"}]
            wins=[x for x in vals if x>0]; losses=[x for x in vals if x<0]
            recent=vals[-recent_n:]
            return {"count":len(rows),"closed_count":len(vals),"wins":len(wins),"losses":len(losses),
                    "win_rate":len(wins)/len(vals) if vals else None,
                    "expectancy_R":sum(vals)/len(vals) if vals else None,
                    "average_win_R":sum(wins)/len(wins) if wins else None,
                    "average_loss_R":sum(losses)/len(losses) if losses else None,
                    "recent_sample_count":len(recent),"recent_expectancy_R":sum(recent)/len(recent) if recent else None,
                    "historical_sample_count":len(vals),"evidence_grade":evidence_grade(len(vals))}
        sm=metrics(shadow,"result_r"); em=metrics(executed,"realized_r")
        total_valid=em["count"]+sm["count"]
        return {"instrument":instrument,"executed":em,"shadow":sm,
                "executed_count":em["count"],"executed_wins":em["wins"],"executed_losses":em["losses"],
                "executed_win_rate":em["win_rate"],"executed_expectancy_R":em["expectancy_R"],
                "shadow_count":sm["count"],"shadow_wins":sm["wins"],"shadow_losses":sm["losses"],
                "shadow_timeouts":sum(x.get("status")=="TIMEOUT" for x in shadow),
                "shadow_ambiguous":sum(x.get("status")=="AMBIGUOUS" for x in shadow),
                "shadow_win_rate":sm["win_rate"],"shadow_expectancy_R":sm["expectancy_R"],
                "total_valid_opportunities":total_valid,
                "selection_rate":em["count"]/total_valid if total_valid else None,
                "rejection_by_better_rank_rate":sm["count"]/total_valid if total_valid else None,
                "evidence_grade":evidence_grade(em["closed_count"]+sm["closed_count"]),
                "combined_valid_count":total_valid,"combined_is_mixed_evidence":True,
                "research_authority":False,"execution_authority":False}

    def compare_selector_decisions(self, cycle_id: Optional[str] = None) -> List[Dict[str, Any]]:
        c=self.conn()
        q="""SELECT cf.*,tm.realized_r winner_result_r,tm.status winner_trade_status
             FROM counterfactual_opportunities cf
             LEFT JOIN trade_memory tm ON tm.trade_id=cf.winner_trade_id
             WHERE cf.rejection_category='SELECTION_REJECTED'"""
        args=[]
        if cycle_id:
            q += " AND cf.cycle_id=?"; args.append(cycle_id)
        q += " ORDER BY cf.cycle_id,cf.rank,cf.counterfactual_id"
        rows=[dict(x) for x in c.execute(q,args).fetchall()]; c.close()
        out=[]
        for r in rows:
            wr=_finite(r.get("winner_result_r")); rr=_finite(r.get("result_r"))
            if wr is None or rr is None or r.get("status") not in {"WIN","LOSS"}:
                regret=None; outcome="UNKNOWN"
            else:
                regret=rr-wr
                outcome="WRONG" if regret>0 else "CORRECT" if regret<0 else "TIE"
            out.append({"cycle_id":r["cycle_id"],"winner":r.get("winner_instrument"),"winner_score":r.get("winner_score"),
                        "winner_result_R":wr,"rejected_instrument":r["instrument"],"rejected_score":r["rank_score"],
                        "rejected_result_R":rr,"rejected_status":r["status"],"regret_R":regret,
                        "regret_unknown":regret is None,"selector_outcome":outcome})
        return out

    def head_to_head_report(self, instrument_a: str, instrument_b: str) -> Dict[str, Any]:
        rows=self.compare_selector_decisions()
        pairs=[r for r in rows if {r.get("winner"),r.get("rejected_instrument")}=={instrument_a,instrument_b}]
        known=[r for r in pairs if r["selector_outcome"] in {"CORRECT","WRONG","TIE"}]
        regrets=[r["regret_R"] for r in known if r["regret_R"] is not None]
        return {"instrument_a":instrument_a,"instrument_b":instrument_b,"times_competed":len(pairs),
                "A_selected":sum(r.get("winner")==instrument_a for r in pairs),
                "B_selected":sum(r.get("winner")==instrument_b for r in pairs),
                "A_win_when_selected":sum(r.get("winner")==instrument_a and _finite(r.get("winner_result_R")) is not None and float(r["winner_result_R"])>0 for r in pairs),
                "A_loss_when_selected":sum(r.get("winner")==instrument_a and _finite(r.get("winner_result_R")) is not None and float(r["winner_result_R"])<0 for r in pairs),
                "B_win_when_selected":sum(r.get("winner")==instrument_b and _finite(r.get("winner_result_R")) is not None and float(r["winner_result_R"])>0 for r in pairs),
                "B_loss_when_selected":sum(r.get("winner")==instrument_b and _finite(r.get("winner_result_R")) is not None and float(r["winner_result_R"])<0 for r in pairs),
                "A_counterfactual_win":sum(r.get("rejected_instrument")==instrument_a and r.get("rejected_result_R") is not None and float(r["rejected_result_R"])>0 for r in pairs),
                "A_counterfactual_loss":sum(r.get("rejected_instrument")==instrument_a and r.get("rejected_result_R") is not None and float(r["rejected_result_R"])<0 for r in pairs),
                "B_counterfactual_win":sum(r.get("rejected_instrument")==instrument_b and r.get("rejected_result_R") is not None and float(r["rejected_result_R"])>0 for r in pairs),
                "B_counterfactual_loss":sum(r.get("rejected_instrument")==instrument_b and r.get("rejected_result_R") is not None and float(r["rejected_result_R"])<0 for r in pairs),
                "selector_correct":sum(r["selector_outcome"]=="CORRECT" for r in pairs),
                "selector_wrong":sum(r["selector_outcome"]=="WRONG" for r in pairs),
                "ties":sum(r["selector_outcome"]=="TIE" for r in pairs),
                "ambiguous":sum(r.get("rejected_status")=="AMBIGUOUS" for r in pairs),
                "timeouts":sum(r.get("rejected_status")=="TIMEOUT" for r in pairs),
                "average_regret_R":sum(regrets)/len(regrets) if regrets else None,
                "execution_authority":False,"research_authority":False}

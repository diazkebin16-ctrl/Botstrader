import os
import asyncio
import sqlite3
import json
import logging
import math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

# PRACTICE ONLY. There is intentionally no OANDA live endpoint or environment switch.
OANDA = "https://api-fxpractice.oanda.com"
GDELT = "https://api.gdeltproject.org/api/v2/doc/doc"
ACCOUNT = os.getenv("OANDA_ACCOUNT_ID", "").strip()
TOKEN = os.getenv("OANDA_TOKEN", "").strip()
INSTRUMENTS = [x.strip().upper().replace("/", "_") for x in os.getenv("INSTRUMENTS", "EUR_USD").split(",") if x.strip()]
UNITS = max(1, int(os.getenv("TRADE_UNITS", "100")))
THRESH = max(0, min(100, int(os.getenv("QUALITY_THRESHOLD", "80"))))
AUTO = os.getenv("AUTO_TRADE", "false").lower() == "true"
SINGLE = os.getenv("SINGLE_POSITION_PER_INSTRUMENT", "true").lower() == "true"
SESSION = os.getenv("SESSION_FILTER", "true").lower() == "true"
NEWS = os.getenv("NEWS_FILTER", "true").lower() == "true"
MIN_RR = float(os.getenv("MIN_RR", "1.5"))
DB = os.getenv("DB_PATH", "/data/market_alert.db" if os.path.isdir("/data") else "market_alert.db")
MODEL_PATH = os.getenv("MODEL_PATH", "/data/market_alert_model.joblib" if os.path.isdir("/data") else "market_alert_model.joblib")
ML_SHADOW = os.getenv("ML_SHADOW", "true").lower() == "true"
ML_MIN_SAMPLES = max(50, int(os.getenv("ML_MIN_SAMPLES", "100")))
ML_RETRAIN_HOURS = max(1, int(os.getenv("ML_RETRAIN_HOURS", "24")))
OUTCOME_HORIZON_MIN = max(30, int(os.getenv("OUTCOME_HORIZON_MIN", "180")))
ADAPTIVE_CONFIDENCE = os.getenv("ADAPTIVE_CONFIDENCE", "true").lower() == "true"
CONFIDENCE_MIN_SAMPLES = max(20, int(os.getenv("CONFIDENCE_MIN_SAMPLES", "60")))
CONFIDENCE_LOCAL_MIN = max(10, int(os.getenv("CONFIDENCE_LOCAL_MIN", "25")))
EXECUTION_MIN_CONFIDENCE = max(0.50, min(0.95, float(os.getenv("EXECUTION_MIN_CONFIDENCE", "0.68"))))
BOOTSTRAP_SCORE_THRESHOLD = max(80, min(100, int(os.getenv("BOOTSTRAP_SCORE_THRESHOLD", "90"))))
RECENT_PERFORMANCE_WINDOW = max(20, int(os.getenv("RECENT_PERFORMANCE_WINDOW", "40")))
NY = ZoneInfo("America/New_York")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("market-alert")
app = FastAPI(title="Market Alert V1.8 — Validation & Execution Audit / OANDA Practice Only")
state: Dict[str, Any] = {
    "started": datetime.now(timezone.utc).isoformat(),
    "last_scan": None,
    "last_error": None,
    "cycles": 0,
    "last_results": {},
    "learning": {"last_train": None, "model_ready": False, "note": "Waiting for resolved samples"},
}

FEATURE_COLUMNS = [
    "direction_buy", "technical_score", "final_score", "m15_gap_atr", "m15_slope_atr",
    "m5_momentum", "pullbacks", "second_pullback", "m1_momentum", "m1_confirm",
    "extension_atr", "volatility_ratio", "rr_raw", "session_ok", "news_confirm",
    "news_contradict", "blocked", "hour_ny"
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("""
        CREATE TABLE IF NOT EXISTS signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            candle_ts TEXT,
            instrument TEXT NOT NULL,
            signal TEXT NOT NULL,
            technical INTEGER NOT NULL,
            score INTEGER NOT NULL,
            alignment TEXT,
            blocked INTEGER NOT NULL,
            entry REAL,
            stop REAL,
            target REAL,
            rr REAL,
            executed INTEGER DEFAULT 0,
            order_id TEXT,
            ml_probability REAL,
            dynamic_confidence REAL,
            confidence_source TEXT,
            confidence_samples INTEGER,
            required_confidence REAL,
            decision_reason TEXT,
            setup_variant TEXT,
            features_json TEXT NOT NULL,
            filters_json TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS learning_samples(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER NOT NULL UNIQUE,
            created_ts TEXT NOT NULL,
            candle_ts TEXT,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry REAL NOT NULL,
            stop REAL NOT NULL,
            target REAL NOT NULL,
            technical INTEGER NOT NULL,
            score INTEGER NOT NULL,
            blocked INTEGER NOT NULL,
            executed INTEGER NOT NULL,
            features_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            label INTEGER,
            resolved_ts TEXT,
            bars_to_resolution INTEGER,
            mfe_r REAL,
            mae_r REAL,
            note TEXT,
            FOREIGN KEY(signal_id) REFERENCES signals(id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS execution_audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, signal_id INTEGER,
            instrument TEXT NOT NULL, order_id TEXT, trade_id TEXT, expected_entry REAL,
            fill_price REAL, slippage_pips REAL, stop_loss_ok INTEGER, take_profit_ok INTEGER,
            protection_status TEXT NOT NULL, detail TEXT)
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS decision_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            candle_ts TEXT,
            instrument TEXT NOT NULL,
            signal TEXT NOT NULL,
            setup_variant TEXT,
            quality_score INTEGER,
            dynamic_confidence REAL,
            confidence_source TEXT,
            confidence_samples INTEGER,
            required_confidence REAL,
            recent_win_rate REAL,
            performance_penalty REAL,
            hard_filters_ok INTEGER NOT NULL,
            auto_trade INTEGER NOT NULL,
            executed INTEGER NOT NULL,
            reason TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS model_runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trained_ts TEXT NOT NULL,
            samples INTEGER NOT NULL,
            train_samples INTEGER NOT NULL,
            test_samples INTEGER NOT NULL,
            win_rate REAL,
            baseline_accuracy REAL,
            accuracy REAL,
            roc_auc REAL,
            log_loss REAL,
            accepted INTEGER NOT NULL,
            model_path TEXT,
            note TEXT
        )
    """)
    # Safe migration from V1.5 databases already stored on the Railway volume.
    existing = {row[1] for row in c.execute("PRAGMA table_info(signals)").fetchall()}
    migrations = {
        "candle_ts": "TEXT",
        "ml_probability": "REAL",
        "dynamic_confidence": "REAL",
        "confidence_source": "TEXT",
        "confidence_samples": "INTEGER",
        "required_confidence": "REAL",
        "decision_reason": "TEXT",
        "setup_variant": "TEXT",
        "features_json": "TEXT NOT NULL DEFAULT '{}'",
        "filters_json": "TEXT NOT NULL DEFAULT '{}'",
    }
    for name, ddl in migrations.items():
        if name not in existing:
            c.execute(f"ALTER TABLE signals ADD COLUMN {name} {ddl}")
    c.execute("CREATE INDEX IF NOT EXISTS idx_samples_pending ON learning_samples(status,instrument)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_decision_ts ON decision_log(ts)")
    c.commit()
    return c


def mean(a: List[float]) -> float:
    return sum(a) / len(a) if a else 0.0


def clamp(v: float, lo: float, hi: float) -> float:
    return min(hi, max(lo, v))


def ema(a: List[float], n: int) -> List[float]:
    if not a:
        return []
    k = 2 / (n + 1)
    out = [a[0]]
    for x in a[1:]:
        out.append(x * k + out[-1] * (1 - k))
    return out


def atr(c: List[Dict[str, Any]], n: int = 14) -> float:
    if len(c) < 2:
        return 0.0
    tr = [max(c[i]["h"] - c[i]["l"], abs(c[i]["h"] - c[i-1]["c"]), abs(c[i]["l"] - c[i-1]["c"])) for i in range(1, len(c))]
    return mean(tr[-n:])


def mom(c: List[Dict[str, Any]], n: int) -> float:
    return (c[-1]["c"] - c[-1-n]["c"]) / c[-1-n]["c"] if len(c) > n and c[-1-n]["c"] else 0.0


def swing(c: List[Dict[str, Any]], side: str, n: int) -> float:
    s = c[-n:]
    if not s:
        return float("nan")
    return min(x["l"] for x in s) if side == "l" else max(x["h"] for x in s)


def structure(c: List[Dict[str, Any]]) -> tuple[bool, bool]:
    a, b = c[-30:-15], c[-15:]
    if len(a) < 10 or len(b) < 10:
        return False, False
    ah, al = max(x["h"] for x in a), min(x["l"] for x in a)
    bh, bl = max(x["h"] for x in b), min(x["l"] for x in b)
    return bh > ah and bl > al, bh < ah and bl < al


def pullbacks(c: List[Dict[str, Any]], e: List[float], side: str) -> tuple[int, bool]:
    count, last = 0, -99
    armed = False
    for i in range(max(2, len(c) - 48), len(c)):
        x = c[i]
        d = (x["c"] - e[i]) / max(x["c"], 1e-9)
        if side == "BUY":
            if d > .0008:
                armed = True
            touch = x["l"] <= e[i] * 1.00035 and x["c"] >= e[i] * .99985
        else:
            if d < -.0008:
                armed = True
            touch = x["h"] >= e[i] * .99965 and x["c"] <= e[i] * 1.00015
        if armed and touch:
            count, last, armed = count + 1, i, False
    return count, last >= len(c) - 6


def session_info(dt: datetime) -> Dict[str, Any]:
    n = dt.astimezone(NY)
    mins = n.hour * 60 + n.minute
    weekday = n.weekday() < 5
    post_open = 9 * 60 + 35 <= mins <= 12 * 60 + 30
    news_window = 8 * 60 + 25 <= mins <= 8 * 60 + 40
    return {"weekday": weekday, "post_open": post_open, "news_window": news_window, "ok": weekday and post_open and not news_window, "hour": n.hour + n.minute / 60.0, "ny": n.isoformat()}


async def req(client: httpx.AsyncClient, method: str, path: str, params=None, body=None) -> Dict[str, Any]:
    if not ACCOUNT or not TOKEN:
        raise RuntimeError("Faltan OANDA_ACCOUNT_ID/OANDA_TOKEN")
    url = OANDA + path.replace("{account}", ACCOUNT)
    r = await client.request(method, url, params=params, json=body, headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}, timeout=15)
    if r.status_code >= 400:
        try:
            msg = r.json().get("errorMessage") or r.json().get("errorCode")
        except Exception:
            msg = r.text[:250]
        raise RuntimeError(f"OANDA Practice HTTP {r.status_code}: {msg}")
    return r.json()


async def candles(client: httpx.AsyncClient, inst: str, granularity: str, count: int) -> List[Dict[str, Any]]:
    d = await req(client, "GET", f"/v3/accounts/{{account}}/instruments/{inst}/candles", {"price": "M", "granularity": granularity, "count": count})
    out = []
    for x in d.get("candles", []):
        if not x.get("complete"):
            continue
        m = x.get("mid", {})
        out.append({"t": datetime.fromisoformat(x["time"].replace("Z", "+00:00")), "o": float(m["o"]), "h": float(m["h"]), "l": float(m["l"]), "c": float(m["c"]), "v": int(x.get("volume", 0))})
    if len(out) < 55:
        raise RuntimeError(f"{inst} {granularity}: velas completas insuficientes ({len(out)})")
    return out


def analyze(m15, m5, m1, inst) -> Dict[str, Any]:
    c15, c5, c1 = [x["c"] for x in m15], [x["c"] for x in m5], [x["c"] for x in m1]
    e20, e50, e5, e9, e1 = ema(c15, 20), ema(c15, 50), ema(c5, 20), ema(c1, 9), ema(c1, 20)
    a15, a1 = atr(m15), atr(m1)
    gap = (e20[-1] - e50[-1]) / max(a15, 1e-9)
    slope = (e20[-1] - e20[-5]) / max(a15, 1e-9)
    buy, sell = gap > .1 and slope > .05, gap < -.1 and slope < -.05
    sig = "BUY" if buy else "SELL" if sell else "WAIT"
    mb, ms = structure(m5)
    m5m = mom(m5, 6)
    aligned = (mb and m5m > 0) if sig == "BUY" else (ms and m5m < 0) if sig == "SELL" else False
    pc, pr = pullbacks(m5, e5, sig) if sig != "WAIT" else (0, False)
    second = pc >= 2 and pr
    last = m1[-1]
    ph, pl, mm = swing(m1[:-1], "h", 7), swing(m1[:-1], "l", 7), mom(m1, 4)
    cb = e9[-1] > e1[-1] and mm > 0 and last["c"] > last["o"]
    cs = e9[-1] < e1[-1] and mm < 0 and last["c"] < last["o"]
    confirm = (cb and (last["c"] > ph or mm > .00012)) if sig == "BUY" else (cs and (last["c"] < pl or mm < -.00012)) if sig == "SELL" else False
    ext = abs(last["c"] - e1[-1]) / max(a1, 1e-9)
    vols = [atr(m1[:len(m1)-i]) for i in range(18) if len(m1)-i > 15]
    vol = a1 / max(mean(vols), 1e-9)
    entry = last["c"]
    ss = swing(m1, "l" if sig == "BUY" else "h", 12)
    astop = entry - a1 * 1.25 if sig == "BUY" else entry + a1 * 1.25
    stop = min(ss, astop) if sig == "BUY" else max(ss, astop) if sig == "SELL" else entry
    risk = abs(entry - stop) or max(a1 * 1.25, entry * .00045)
    st = swing(m5[:-2], "h" if sig == "BUY" else "l", 28)
    reward = (st - entry) if sig == "BUY" else (entry - st) if sig == "SELL" else 0
    reward = reward if reward > 0 else risk * 2
    rr_raw = reward / risk
    rr = min(2, max(0, rr_raw))
    target = entry + risk * rr if sig == "BUY" else entry - risk * rr if sig == "SELL" else entry
    sess = session_info(last["t"])
    tech = min(85,
        (12 if sig != "WAIT" else 0) + (10 if aligned else 0) + (8 if confirm else 0) +
        (7 if pc >= 1 and pr else 0) + (10 if second else 5 if pc >= 1 and pr else 0) +
        (8 if confirm else 0) + (10 if rr_raw >= 2 else 7 if rr_raw >= MIN_RR else 0) +
        (5 if risk <= a1 * 2.2 else 0) + (5 if ext <= 1.35 else 0) +
        (5 if sess["ok"] else 0) + (5 if sess["ok"] and second else 0)
    )
    checks = {
        "m15_context": sig != "WAIT",
        "m5_structure": aligned,
        "second_pullback": second,
        "m1_confirmation": confirm,
        "minimum_rr": rr_raw >= MIN_RR,
        "not_extended": ext <= 1.35,
        "volatility_ok": .65 <= vol <= 2,
    }
    if SESSION:
        checks["ny_session"] = sess["ok"]
    features = {
        "direction_buy": 1 if sig == "BUY" else 0,
        "technical_score": int(tech),
        "final_score": int(tech),
        "m15_gap_atr": float(gap),
        "m15_slope_atr": float(slope),
        "m5_momentum": float(m5m),
        "pullbacks": int(pc),
        "second_pullback": 1 if second else 0,
        "m1_momentum": float(mm),
        "m1_confirm": 1 if confirm else 0,
        "extension_atr": float(ext),
        "volatility_ratio": float(vol),
        "rr_raw": float(rr_raw),
        "session_ok": 1 if sess["ok"] else 0,
        "news_confirm": 0,
        "news_contradict": 0,
        "blocked": 1 if not all(checks.values()) else 0,
        "hour_ny": float(sess["hour"]),
    }
    return {
        "instrument": inst, "signal": sig, "technical": int(tech), "score": int(tech),
        "entry": entry, "stop": stop, "target": target, "rr": rr, "rr_raw": rr_raw,
        "blocked": not all(checks.values()), "pullbacks": pc, "filters": checks,
        "features": features, "candle_ts": last["t"].isoformat(), "alignment": "N/A"
    }


async def news(client: httpx.AsyncClient, r: Dict[str, Any]) -> Dict[str, Any]:
    if not NEWS:
        return {**r, "alignment": "DISABLED"}
    base, quote = r["instrument"].split("_")
    q = f"({base} OR {quote}) (forex OR currency OR inflation OR rates OR central bank OR jobs OR GDP)"
    try:
        x = await client.get(GDELT, params={"query": q, "mode": "ArtList", "maxrecords": "12", "format": "json", "timespan": "180min", "sort": "HybridRel"}, timeout=8)
        x.raise_for_status()
        arts = x.json().get("articles", [])
        text = " ".join(a.get("title", "").lower() for a in arts)
        pos = sum(text.count(w) for w in ["hawkish", "rate hike", "strong jobs", "jobs beat", "growth beats", "currency gains"])
        neg = sum(text.count(w) for w in ["dovish", "rate cut", "weak jobs", "jobs miss", "recession", "currency falls"])
        bias = "BULLISH" if pos - neg >= 2 else "BEARISH" if neg - pos >= 2 else "NEUTRAL"
        if (r["signal"] == "BUY" and bias == "BULLISH") or (r["signal"] == "SELL" and bias == "BEARISH"):
            align = "CONFIRMA"
        elif (r["signal"] == "BUY" and bias == "BEARISH") or (r["signal"] == "SELL" and bias == "BULLISH"):
            align = "CONTRADICE"
        else:
            align = "NEUTRAL"
        score = min(100, r["technical"] + (15 if align == "CONFIRMA" else 0))
        features = dict(r["features"])
        features["final_score"] = int(score)
        features["news_confirm"] = 1 if align == "CONFIRMA" else 0
        features["news_contradict"] = 1 if align == "CONTRADICE" else 0
        blocked = r["blocked"] or align == "CONTRADICE"
        features["blocked"] = 1 if blocked else 0
        return {**r, "score": score, "alignment": align, "blocked": blocked, "features": features, "news_articles": [{"title": a.get("title", ""), "url": a.get("url", "")} for a in arts[:5]]}
    except Exception as e:
        return {**r, "alignment": "UNKNOWN", "news_error": str(e)}


def feature_vector(features: Dict[str, Any]) -> List[float]:
    return [float(features.get(k, 0) or 0) for k in FEATURE_COLUMNS]



def setup_variant(r: Dict[str, Any]) -> str:
    """Stable description of the current strategy variation. This is not a new trading strategy."""
    if r.get("signal") not in ("BUY", "SELL"):
        return "WAIT"
    align = r.get("alignment", "N/A")
    news_tag = "NEWS_CONFIRM" if align == "CONFIRMA" else "NEWS_CONTRA" if align == "CONTRADICE" else "NEWS_NEUTRAL"
    rr_tag = "RR2" if float(r.get("rr_raw", 0)) >= 2 else "RR15"
    score = int(r.get("score", 0))
    score_tag = "Q90" if score >= 90 else "Q85" if score >= 85 else "Q80" if score >= 80 else "QLOW"
    return f"SECOND_PULLBACK_{news_tag}_{rr_tag}_{score_tag}"


def wilson_lower_bound(wins: int, total: int, z: float = 1.28) -> float:
    """Conservative lower bound (~80% two-sided) so small samples do not look overconfident."""
    if total <= 0:
        return 0.0
    p = wins / total
    den = 1 + z*z/total
    center = p + z*z/(2*total)
    margin = z * math.sqrt((p*(1-p) + z*z/(4*total))/total)
    return max(0.0, (center - margin) / den)


def recent_performance() -> Dict[str, Any]:
    c = conn()
    rows = c.execute(
        "SELECT label FROM learning_samples WHERE executed=1 AND label IN (0,1) ORDER BY id DESC LIMIT ?",
        (RECENT_PERFORMANCE_WINDOW,)
    ).fetchall()
    c.close()
    n = len(rows)
    if not n:
        return {"samples": 0, "win_rate": None, "penalty": 0.0}
    wins = sum(int(x["label"]) for x in rows)
    wr = wins / n
    # Only penalize after a meaningful recent sample. Never rewards aggressiveness.
    penalty = 0.0
    if n >= 20:
        if wr < 0.35:
            penalty = 0.12
        elif wr < 0.45:
            penalty = 0.08
        elif wr < 0.52:
            penalty = 0.04
    return {"samples": n, "win_rate": wr, "penalty": penalty}


def empirical_confidence(r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Estimates win probability from resolved historical setups.
    Uses global + setup-variant evidence with shrinkage and a conservative lower bound.
    It intentionally refuses to report very high confidence from tiny samples.
    """
    c = conn()
    total = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE label IN (0,1)").fetchone()["n"]
    wins = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE label=1").fetchone()["n"]
    variant = setup_variant(r)

    # Variant is reconstructed from the signal row when available.
    rows = c.execute("""
        SELECT ls.label, s.setup_variant
        FROM learning_samples ls
        JOIN signals s ON s.id=ls.signal_id
        WHERE ls.label IN (0,1)
        ORDER BY ls.id DESC
    """).fetchall()
    c.close()

    local = [int(x["label"]) for x in rows if x["setup_variant"] == variant]
    local_n, local_w = len(local), sum(local)

    if total < CONFIDENCE_MIN_SAMPLES:
        # Bootstrap: quality is a heuristic, not a probability. Cap confidence below 80%.
        q = int(r.get("score", 0))
        proxy = 0.50 + max(0, q - 70) * 0.009
        proxy = min(proxy, 0.79)
        return {
            "probability": proxy,
            "source": "BOOTSTRAP_CONSERVATIVE",
            "samples": total,
            "local_samples": local_n,
            "variant": variant,
            "global_win_rate": (wins/total) if total else None,
            "local_win_rate": (local_w/local_n) if local_n else None,
            "lower_bound": None,
            "mature": False,
        }

    # Beta prior centered on global performance, equivalent to 20 pseudo-observations.
    global_rate = wins / total
    prior_strength = 20.0
    if local_n:
        posterior = (local_w + global_rate * prior_strength) / (local_n + prior_strength)
    else:
        posterior = global_rate

    lb = wilson_lower_bound(local_w, local_n) if local_n >= CONFIDENCE_LOCAL_MIN else wilson_lower_bound(wins, total)
    # Blend posterior with conservative bound. This suppresses unjustified 90% readings.
    calibrated = 0.65 * posterior + 0.35 * lb

    # Require substantial evidence before allowing an displayed estimate >= 90%.
    if total < 250 or local_n < 50:
        calibrated = min(calibrated, 0.89)
    calibrated = max(0.05, min(0.97, calibrated))
    return {
        "probability": calibrated,
        "source": "EMPIRICAL_VARIANT" if local_n >= CONFIDENCE_LOCAL_MIN else "EMPIRICAL_GLOBAL",
        "samples": total,
        "local_samples": local_n,
        "variant": variant,
        "global_win_rate": global_rate,
        "local_win_rate": (local_w/local_n) if local_n else None,
        "lower_bound": lb,
        "mature": True,
    }


def dynamic_confidence(r: Dict[str, Any], mlp: Optional[float]) -> Dict[str, Any]:
    emp = empirical_confidence(r)
    p = float(emp["probability"])
    source = emp["source"]

    # ML is still a secondary estimate. It can refine, not dominate, the empirical calibration.
    if emp["mature"] and mlp is not None:
        p = 0.75 * p + 0.25 * float(mlp)
        source += "+ML_SHADOW"

    perf = recent_performance()
    penalty = float(perf["penalty"])
    # Poor recent execution raises the gate and slightly reduces reported confidence.
    p = max(0.05, p - penalty * 0.5)
    required = min(0.90, EXECUTION_MIN_CONFIDENCE + penalty)

    return {
        **emp,
        "probability": min(0.97, p),
        "source": source,
        "recent_samples": perf["samples"],
        "recent_win_rate": perf["win_rate"],
        "performance_penalty": penalty,
        "required_confidence": required,
    }


def execution_decision(r: Dict[str, Any], conf: Dict[str, Any]) -> Dict[str, Any]:
    if r["signal"] == "WAIT":
        return {"execute": False, "reason": "WAIT: no hay señal direccional"}
    if r["blocked"]:
        failed = [k for k,v in r.get("filters", {}).items() if not v]
        return {"execute": False, "reason": "Hard filters: " + ", ".join(failed)}
    if not ADAPTIVE_CONFIDENCE:
        ok = r["score"] >= THRESH
        return {"execute": ok, "reason": "Adaptive confidence desactivada; usa Quality Score"}
    if not conf["mature"]:
        ok = r["score"] >= BOOTSTRAP_SCORE_THRESHOLD and conf["probability"] >= 0.65
        return {"execute": ok, "reason": "Bootstrap: exige Quality Score alto mientras aprende" if ok else f"Bootstrap insuficiente: Q={r['score']} < {BOOTSTRAP_SCORE_THRESHOLD} o confianza conservadora baja"}
    ok = conf["probability"] >= conf["required_confidence"] and r["score"] >= THRESH
    if ok:
        return {"execute": True, "reason": f"Confianza dinámica {conf['probability']:.1%} >= {conf['required_confidence']:.1%} y Q >= {THRESH}"}
    return {"execute": False, "reason": f"No alcanza gate: confianza {conf['probability']:.1%}/{conf['required_confidence']:.1%}, Q={r['score']}/{THRESH}"}


def save_decision(r: Dict[str, Any], conf: Dict[str, Any], executed: int, reason: str):
    c = conn()
    c.execute("""
        INSERT INTO decision_log(ts,candle_ts,instrument,signal,setup_variant,quality_score,dynamic_confidence,
          confidence_source,confidence_samples,required_confidence,recent_win_rate,performance_penalty,
          hard_filters_ok,auto_trade,executed,reason)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        now_iso(), r.get("candle_ts"), r["instrument"], r["signal"], conf.get("variant"),
        r.get("score"), conf.get("probability"), conf.get("source"), conf.get("samples"),
        conf.get("required_confidence"), conf.get("recent_win_rate"), conf.get("performance_penalty"),
        int(not r.get("blocked", True)), int(AUTO), int(executed), reason
    ))
    c.commit(); c.close()


def load_shadow_probability(features: Dict[str, Any]) -> Optional[float]:
    if not ML_SHADOW or not Path(MODEL_PATH).exists():
        return None
    try:
        import joblib
        model = joblib.load(MODEL_PATH)
        return float(model.predict_proba([feature_vector(features)])[0][1])
    except Exception as e:
        log.warning("shadow model prediction failed: %s", e)
        return None


async def haspos(client: httpx.AsyncClient, inst: str) -> bool:
    d = await req(client, "GET", "/v3/accounts/{account}/openPositions")
    return any(x.get("instrument") == inst for x in d.get("positions", []))



def pip_size(instrument: str) -> float:
    return 0.01 if "JPY" in instrument else 0.0001

def calibration_report():
    c=conn(); rows=[dict(x) for x in c.execute("""SELECT s.dynamic_confidence,ls.label FROM signals s
      JOIN learning_samples ls ON ls.signal_id=s.id WHERE s.dynamic_confidence IS NOT NULL AND ls.label IN (0,1)""")]; c.close()
    out=[]
    for lo,hi in [(0.5,.6),(.6,.7),(.7,.8),(.8,.9),(.9,1.01)]:
        x=[r for r in rows if lo<=r["dynamic_confidence"]<hi]
        out.append({"range":f"{int(lo*100)}-{int(min(hi,1)*100)}%","samples":len(x),
          "predicted":sum(r["dynamic_confidence"] for r in x)/len(x) if x else None,
          "actual_win_rate":sum(r["label"] for r in x)/len(x) if x else None})
    return out

def strategy_health():
    c=conn(); rows=[dict(x) for x in c.execute("""SELECT s.executed,s.rr,s.setup_variant,ls.label FROM signals s
      JOIN learning_samples ls ON ls.signal_id=s.id WHERE ls.label IN (0,1) ORDER BY s.id DESC LIMIT 1000""")]; c.close()
    x=[r for r in rows if r["executed"]]; wins=sum(r["label"] for r in x); losses=len(x)-wins
    win_r=sum(float(r["rr"] or 0) for r in x if r["label"]==1); loss_r=float(losses)
    variants={}
    for r in x:
        z=variants.setdefault(r["setup_variant"] or "UNKNOWN",[0,0]); z[0]+=1; z[1]+=r["label"]
    vr=sorted([{"setup":k,"samples":v[0],"win_rate":v[1]/v[0]} for k,v in variants.items()],
              key=lambda q:(q["samples"]>=10,q["win_rate"],q["samples"]),reverse=True)
    return {"resolved_executed":len(x),"wins":wins,"losses":losses,"win_rate":wins/len(x) if x else None,
      "profit_factor_r":win_r/loss_r if loss_r else (999.0 if win_r else None),
      "expectancy_r":(win_r-loss_r)/len(x) if x else None,
      "best_setup":vr[0] if vr else None,"weakest_setup":vr[-1] if vr else None,"calibration":calibration_report()}

def threshold_report():
    c=conn(); rows=[dict(x) for x in c.execute("""SELECT s.dynamic_confidence,ls.label FROM signals s
      JOIN learning_samples ls ON ls.signal_id=s.id WHERE s.dynamic_confidence IS NOT NULL AND ls.label IN (0,1)""")]; c.close()
    out=[]
    for t in [.60,.65,.68,.70,.75,.80,.85,.90]:
        x=[r for r in rows if r["dynamic_confidence"]>=t]
        out.append({"threshold":t,"samples":len(x),"precision_win_rate":sum(r["label"] for r in x)/len(x) if x else None})
    return out

async def verify_trade_protection(client, trade_id: str):
    if not trade_id:return {"status":"PROTECTION_ERROR","sl_ok":False,"tp_ok":False,"detail":"No trade ID returned"}
    try:
        d=await req(client,"GET",f"/v3/accounts/{{account}}/trades/{trade_id}")
        tr=d.get("trade",{}); sl=bool(tr.get("stopLossOrder")); tp=bool(tr.get("takeProfitOrder"))
        return {"status":"OK" if sl and tp else "PROTECTION_ERROR","sl_ok":sl,"tp_ok":tp,
                "detail":f"stopLossOrder={sl}; takeProfitOrder={tp}"}
    except Exception as e:
        return {"status":"PROTECTION_ERROR","sl_ok":False,"tp_ok":False,"detail":str(e)}

async def execute(client: httpx.AsyncClient, r: Dict[str, Any]):
    if SINGLE and await haspos(client, r["instrument"]):
        return {"skipped": "existing_position"}
    d = 3 if "JPY" in r["instrument"] else 5
    u = UNITS if r["signal"] == "BUY" else -UNITS
    body = {"order": {
        "instrument": r["instrument"], "units": str(u), "type": "MARKET", "timeInForce": "FOK", "positionFill": "DEFAULT",
        "stopLossOnFill": {"price": f"{r['stop']:.{d}f}", "timeInForce": "GTC"},
        "takeProfitOnFill": {"price": f"{r['target']:.{d}f}", "timeInForce": "GTC"}
    }}
    return await req(client, "POST", "/v3/accounts/{account}/orders", body=body)


def save_signal(r: Dict[str, Any], executed: int, order_id: str, ml_probability: Optional[float], conf: Dict[str, Any], decision_reason: str) -> int:
    c = conn()
    cur = c.execute("""
        INSERT INTO signals(ts,candle_ts,instrument,signal,technical,score,alignment,blocked,entry,stop,target,rr,executed,order_id,ml_probability,
          dynamic_confidence,confidence_source,confidence_samples,required_confidence,decision_reason,setup_variant,features_json,filters_json)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        now_iso(), r.get("candle_ts"), r["instrument"], r["signal"], r["technical"], r["score"], r.get("alignment"), int(r["blocked"]),
        r["entry"], r["stop"], r["target"], r["rr"], executed, order_id, ml_probability,
        conf.get("probability"), conf.get("source"), conf.get("samples"), conf.get("required_confidence"),
        decision_reason, conf.get("variant"), json.dumps(r["features"], separators=(",", ":")), json.dumps(r["filters"], separators=(",", ":"))
    ))
    signal_id = int(cur.lastrowid)
    # Learn from directional signals, including rejected ones. WAIT snapshots stay in signals for diagnostics.
    if r["signal"] in ("BUY", "SELL") and r["entry"] != r["stop"] and r["entry"] != r["target"]:
        c.execute("""
            INSERT OR IGNORE INTO learning_samples(signal_id,created_ts,candle_ts,instrument,direction,entry,stop,target,technical,score,blocked,executed,features_json,status)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?, 'PENDING')
        """, (signal_id, now_iso(), r.get("candle_ts"), r["instrument"], r["signal"], r["entry"], r["stop"], r["target"], r["technical"], r["score"], int(r["blocked"]), executed, json.dumps(r["features"], separators=(",", ":"))))
    c.commit()
    c.close()
    return signal_id


def resolve_one(sample: sqlite3.Row, m1: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    start = datetime.fromisoformat(sample["candle_ts"]) if sample["candle_ts"] else datetime.fromisoformat(sample["created_ts"])
    bars = [x for x in m1 if x["t"] > start]
    if not bars:
        return None
    direction, entry, stop, target = sample["direction"], float(sample["entry"]), float(sample["stop"]), float(sample["target"])
    risk = abs(entry - stop)
    if risk <= 0:
        return {"status": "INVALID", "label": None, "bars": 0, "mfe_r": 0, "mae_r": 0, "note": "zero risk"}
    mfe, mae = 0.0, 0.0
    max_bars = OUTCOME_HORIZON_MIN
    for idx, x in enumerate(bars[:max_bars], start=1):
        if direction == "BUY":
            mfe = max(mfe, (x["h"] - entry) / risk)
            mae = min(mae, (x["l"] - entry) / risk)
            hit_tp, hit_sl = x["h"] >= target, x["l"] <= stop
        else:
            mfe = max(mfe, (entry - x["l"]) / risk)
            mae = min(mae, (entry - x["h"]) / risk)
            hit_tp, hit_sl = x["l"] <= target, x["h"] >= stop
        if hit_tp and hit_sl:
            return {"status": "AMBIGUOUS", "label": None, "bars": idx, "mfe_r": mfe, "mae_r": mae, "note": "SL y TP tocados en la misma vela M1"}
        if hit_tp:
            return {"status": "WIN", "label": 1, "bars": idx, "mfe_r": mfe, "mae_r": mae, "note": None}
        if hit_sl:
            return {"status": "LOSS", "label": 0, "bars": idx, "mfe_r": mfe, "mae_r": mae, "note": None}
    if len(bars) >= max_bars:
        return {"status": "TIMEOUT", "label": None, "bars": max_bars, "mfe_r": mfe, "mae_r": mae, "note": f"No resolvió en {max_bars} min"}
    return None


def resolve_pending(inst: str, m1: List[Dict[str, Any]]) -> int:
    c = conn()
    rows = c.execute("SELECT * FROM learning_samples WHERE status='PENDING' AND instrument=? ORDER BY id ASC", (inst,)).fetchall()
    resolved = 0
    for s in rows:
        out = resolve_one(s, m1)
        if out:
            c.execute("""UPDATE learning_samples SET status=?,label=?,resolved_ts=?,bars_to_resolution=?,mfe_r=?,mae_r=?,note=? WHERE id=?""",
                      (out["status"], out["label"], now_iso(), out["bars"], out["mfe_r"], out["mae_r"], out["note"], s["id"]))
            resolved += 1
    c.commit(); c.close()
    return resolved


def learning_stats() -> Dict[str, Any]:
    c = conn()
    total = c.execute("SELECT COUNT(*) n FROM learning_samples").fetchone()["n"]
    resolved = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE label IS NOT NULL").fetchone()["n"]
    wins = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE label=1").fetchone()["n"]
    executed_resolved = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE label IS NOT NULL AND executed=1").fetchone()["n"]
    executed_wins = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE label=1 AND executed=1").fetchone()["n"]
    blocked_resolved = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE label IS NOT NULL AND blocked=1").fetchone()["n"]
    blocked_wins = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE label=1 AND blocked=1").fetchone()["n"]
    run = c.execute("SELECT * FROM model_runs ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    return {
        "samples_total": total, "resolved_labeled": resolved, "pending_or_unlabeled": total - resolved,
        "win_rate_all": (wins / resolved) if resolved else None,
        "executed_resolved": executed_resolved, "win_rate_executed": (executed_wins / executed_resolved) if executed_resolved else None,
        "blocked_resolved": blocked_resolved, "counterfactual_win_rate_blocked": (blocked_wins / blocked_resolved) if blocked_resolved else None,
        "ml_min_samples": ML_MIN_SAMPLES, "model_ready": Path(MODEL_PATH).exists(), "last_model_run": dict(run) if run else None,
        "mode": "ADAPTIVE_CONFIDENCE", "changes_execution": ADAPTIVE_CONFIDENCE, "ml_role": "secondary_refinement"
    }


def train_shadow_model(force: bool = False) -> Dict[str, Any]:
    c=conn(); rows=c.execute("SELECT features_json,label,resolved_at FROM learning_samples WHERE label IN (0,1) ORDER BY resolved_at,id").fetchall(); c.close()
    if len(rows)<ML_MIN_SAMPLES and not force:return {"trained":False,"reason":f"need {ML_MIN_SAMPLES}, have {len(rows)}","samples":len(rows)}
    if len(rows)<20:return {"trained":False,"reason":"need at least 20 resolved samples for temporal validation","samples":len(rows)}
    X=[]; y=[]
    for row in rows:
        f=json.loads(row["features_json"]); X.append([float(f.get(k,0) or 0) for k in FEATURE_NAMES]); y.append(int(row["label"]))
    X=np.asarray(X); y=np.asarray(y)
    if len(set(y.tolist()))<2:return {"trained":False,"reason":"need WIN and LOSS labels","samples":len(y)}
    folds=[]; splits=min(5,max(2,len(y)//20))
    for tr,te in TimeSeriesSplit(n_splits=splits).split(X):
        if len(set(y[tr].tolist()))<2 or len(set(y[te].tolist()))<2:continue
        model=Pipeline([("scale",StandardScaler()),("clf",LogisticRegression(max_iter=1000,class_weight="balanced"))])
        model.fit(X[tr],y[tr]); prob=model.predict_proba(X[te])[:,1]; pred=(prob>=.5).astype(int)
        folds.append({"train":len(tr),"test":len(te),"accuracy":float(accuracy_score(y[te],pred)),
          "auc":float(roc_auc_score(y[te],prob)),"log_loss":float(log_loss(y[te],prob)),
          "brier":float(brier_score_loss(y[te],prob)),"baseline":float(max(np.mean(y[te]),1-np.mean(y[te])))})
    if not folds:return {"trained":False,"reason":"insufficient class diversity across time folds","samples":len(y)}
    final=Pipeline([("scale",StandardScaler()),("clf",LogisticRegression(max_iter=1000,class_weight="balanced"))]); final.fit(X,y)
    avg={k:float(np.mean([f[k] for f in folds])) for k in ["accuracy","auc","log_loss","brier","baseline"]}
    joblib.dump({"model":final,"features":FEATURE_NAMES,"trained_at":now_iso(),"samples":len(y),"walk_forward":folds},MODEL_PATH)
    c=conn(); c.execute("INSERT INTO model_runs(ts,samples,train_samples,test_samples,accuracy,baseline_accuracy,auc,log_loss,detail) VALUES(?,?,?,?,?,?,?,?,?)",
      (now_iso(),len(y),folds[-1]["train"],folds[-1]["test"],avg["accuracy"],avg["baseline"],avg["auc"],avg["log_loss"],
       json.dumps({"validation":"TimeSeriesSplit_walk_forward","folds":folds,"brier":avg["brier"]}))); c.commit(); c.close()
    return {"trained":True,"samples":len(y),"validation":"TimeSeriesSplit_walk_forward","folds":folds,"average":avg}

async def scan(client: httpx.AsyncClient, inst: str) -> Dict[str, Any]:
    m15, m5, m1 = await asyncio.gather(
        candles(client, inst, "M15", 100),
        candles(client, inst, "M5", 130),
        candles(client, inst, "M1", max(220, OUTCOME_HORIZON_MIN + 30))
    )
    resolved = resolve_pending(inst, m1)
    r = analyze(m15, m5, m1, inst)
    r = await news(client, r) if r["signal"] != "WAIT" and r["technical"] >= 50 else {**r, "alignment": "N/A"}
    mlp = load_shadow_probability(r["features"]) if r["signal"] != "WAIT" else None
    conf = dynamic_confidence(r, mlp) if r["signal"] != "WAIT" else {
        "probability": None, "source": "WAIT", "samples": 0, "local_samples": 0,
        "variant": "WAIT", "mature": False, "recent_win_rate": None,
        "performance_penalty": 0.0, "required_confidence": EXECUTION_MIN_CONFIDENCE
    }
    decision = execution_decision(r, conf)
    executed, oid = 0, ""
    if AUTO and decision["execute"]:
        x = await execute(client, r)
        if x and not x.get("skipped"):
            executed = 1
            fill=(x.get("orderFillTransaction") or {})
            oid=str(fill.get("id","")); trade_id=str((fill.get("tradeOpened") or {}).get("tradeID",""))
            fill_price=float(fill.get("price") or r["entry"]); slippage=(fill_price-r["entry"])/pip_size(inst)
            if r["signal"]=="SELL": slippage=-slippage
            protection=await verify_trade_protection(client,trade_id)
            if protection["status"]!="OK": decision["reason"] += "; PROTECTION_ERROR"
            log.info("PRACTICE EXECUTED %s %s quality=%s confidence=%s order=%s slippage=%.2f protection=%s",
                     r["signal"], inst, r["score"], conf.get("probability"), oid, slippage, protection["status"])
        elif x and x.get("skipped"):
            decision["reason"] += "; no ejecutada: posición existente"
    elif decision["execute"] and not AUTO:
        decision["reason"] += "; AUTO_TRADE=false"

    signal_id = save_signal(r, executed, oid, mlp, conf, decision["reason"])
    if executed:
        c=conn(); c.execute("""INSERT INTO execution_audit(ts,signal_id,instrument,order_id,trade_id,expected_entry,fill_price,slippage_pips,
          stop_loss_ok,take_profit_ok,protection_status,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          (now_iso(),signal_id,inst,oid,trade_id,r["entry"],fill_price,slippage,int(protection["sl_ok"]),int(protection["tp_ok"]),protection["status"],protection["detail"]))
        c.commit(); c.close()
    save_decision(r, conf, executed, decision["reason"])
    return {
        **r,
        "executed": bool(executed), "order_id": oid, "signal_id": signal_id,
        "ml_probability": mlp, "dynamic_confidence": conf.get("probability"),
        "confidence_source": conf.get("source"), "confidence_samples": conf.get("samples"),
        "local_confidence_samples": conf.get("local_samples"),
        "required_confidence": conf.get("required_confidence"),
        "recent_win_rate": conf.get("recent_win_rate"),
        "performance_penalty": conf.get("performance_penalty"),
        "setup_variant": conf.get("variant"),
        "decision": decision,
        "learning_resolved_this_cycle": resolved
    }


async def worker():
    await asyncio.sleep(2)
    last_train_check = datetime.min.replace(tzinfo=timezone.utc)
    while True:
        now = datetime.now(timezone.utc)
        await asyncio.sleep(max(1, 60 - now.second - now.microsecond / 1e6 + 2.5))
        state["cycles"] += 1
        state["last_scan"] = now_iso()
        async with httpx.AsyncClient() as client:
            for inst in INSTRUMENTS:
                try:
                    state["last_results"][inst] = await scan(client, inst)
                    state["last_error"] = None
                except Exception as e:
                    state["last_results"][inst] = {"error": str(e)}
                    state["last_error"] = str(e)
                    log.exception("scan failed for %s", inst)
        if datetime.now(timezone.utc) - last_train_check >= timedelta(hours=1):
            result = train_shadow_model(force=False)
            state["learning"] = {**result, "last_train": now_iso(), "model_ready": Path(MODEL_PATH).exists()}
            last_train_check = datetime.now(timezone.utc)


@app.on_event("startup")
async def start():
    conn().close()
    asyncio.create_task(worker())
    log.info("24/7 PRACTICE ONLY. AUTO=%s. No daily operation-count limit. Adaptive confidence=%s; ML=%s shadow/refinement.", AUTO, ADAPTIVE_CONFIDENCE, ML_SHADOW)


@app.get("/health")
async def health():
    return {"ok": True, "practice_only": True, "auto_trade": AUTO, "last_scan": state["last_scan"], "last_error": state["last_error"], "learning_mode": "adaptive_confidence", "adaptive_confidence": ADAPTIVE_CONFIDENCE}


@app.get("/api/status")
async def status():
    return {**state, "practice_only": True, "operation_count_limit": None, "auto_trade": AUTO, "instruments": INSTRUMENTS, "trade_units": UNITS, "quality_threshold": THRESH, "bootstrap_score_threshold": BOOTSTRAP_SCORE_THRESHOLD, "execution_min_confidence": EXECUTION_MIN_CONFIDENCE, "confidence_min_samples": CONFIDENCE_MIN_SAMPLES, "single_position_per_instrument": SINGLE, "adaptive_confidence": ADAPTIVE_CONFIDENCE, "ml_shadow": ML_SHADOW, "ml_role": "secondary_refinement"}


@app.get("/api/signals")
async def signals(limit: int = 50):
    c = conn(); rows = [dict(x) for x in c.execute("SELECT * FROM signals ORDER BY id DESC LIMIT ?", (min(max(limit, 1), 200),))]; c.close()
    for r in rows:
        r.pop("features_json", None); r.pop("filters_json", None)
    return rows


@app.get("/api/health/strategy")
async def health_strategy(): return strategy_health()

@app.get("/api/health/thresholds")
async def health_thresholds(): return threshold_report()

@app.get("/api/execution-audit")
async def execution_audit(limit: int = 100):
    c=conn(); rows=[dict(x) for x in c.execute("SELECT * FROM execution_audit ORDER BY id DESC LIMIT ?",(min(max(limit,1),500),))]; c.close(); return rows

@app.get("/api/decisions")
async def decisions(limit: int = 100):
    c = conn()
    rows = [dict(x) for x in c.execute("SELECT * FROM decision_log ORDER BY id DESC LIMIT ?", (min(max(limit,1),500),))]
    c.close()
    return rows


@app.get("/api/learning")
async def learning():
    return learning_stats()


@app.get("/api/learning/samples")
async def learning_samples(limit: int = 100, status: Optional[str] = None):
    c = conn()
    if status:
        rows = c.execute("SELECT * FROM learning_samples WHERE status=? ORDER BY id DESC LIMIT ?", (status.upper(), min(max(limit,1),500))).fetchall()
    else:
        rows = c.execute("SELECT * FROM learning_samples ORDER BY id DESC LIMIT ?", (min(max(limit,1),500),)).fetchall()
    out = [dict(x) for x in rows]; c.close(); return out


@app.get("/api/learning/export.csv", response_class=PlainTextResponse)
async def export_csv():
    import csv, io
    c = conn(); rows = c.execute("SELECT * FROM learning_samples ORDER BY id ASC").fetchall(); c.close()
    buf = io.StringIO()
    cols = ["id","signal_id","created_ts","candle_ts","instrument","direction","entry","stop","target","technical","score","blocked","executed","status","label","resolved_ts","bars_to_resolution","mfe_r","mae_r"] + FEATURE_COLUMNS
    w = csv.DictWriter(buf, fieldnames=cols); w.writeheader()
    for r in rows:
        f = json.loads(r["features_json"])
        row = {k:r[k] if k in r.keys() else None for k in cols if k not in FEATURE_COLUMNS}
        row.update({k:f.get(k) for k in FEATURE_COLUMNS}); w.writerow(row)
    return buf.getvalue()


@app.post("/api/learning/train")
async def train_now():
    # Safe: training only. It cannot alter technical rules or order execution.
    return train_shadow_model(force=True)


@app.get("/", response_class=HTMLResponse)
async def home():
    return """<!doctype html><html lang='es'><meta name='viewport' content='width=device-width'><title>Market Alert V1.7</title>
<style>body{font-family:system-ui;background:#0b1020;color:#eef2ff;max-width:1050px;margin:auto;padding:24px}.c{background:#151c32;border:1px solid #2c3656;border-radius:16px;padding:18px;margin:12px 0}pre{white-space:pre-wrap;word-break:break-word;background:#080c17;padding:14px;border-radius:12px}.tag{display:inline-block;padding:5px 9px;border-radius:999px;background:#25304f;margin-right:6px}</style>
<h1>Market Alert V1.8 · Validation & Execution Audit</h1><div class=c><span class=tag>OANDA PRACTICE ONLY</span><span class=tag>24/7</span><span class=tag>Sin límite diario</span><span class=tag>Confianza calibrada</span>
<p><b>Quality Score ≠ probabilidad.</b> La confianza dinámica se calibra con resultados reales. Con poca muestra se limita deliberadamente y el 90% requiere evidencia sustancial.</p></div>
<div class=c><h2>Estado</h2><pre id=s>Cargando…</pre></div><div class=c><h2>Aprendizaje</h2><pre id=l>Cargando…</pre></div><div class=c><h2>Última decisión</h2><pre id=d>Cargando…</pre></div><div class=c><h2>Últimas señales</h2><pre id=h>Cargando…</pre></div>
<script>async function u(){s.textContent=JSON.stringify(await fetch('/api/status').then(r=>r.json()),null,2);l.textContent=JSON.stringify(await fetch('/api/learning').then(r=>r.json()),null,2);d.textContent=JSON.stringify(await fetch('/api/decisions?limit=5').then(r=>r.json()),null,2);h.textContent=JSON.stringify(await fetch('/api/signals?limit=15').then(r=>r.json()),null,2)}u();setInterval(u,15000)</script></html>"""

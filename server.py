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
from fastapi import FastAPI, HTTPException
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
BOOTSTRAP_SCORE_THRESHOLD = max(80, min(100, int(os.getenv("BOOTSTRAP_SCORE_THRESHOLD", "90"))))
RECENT_PERFORMANCE_WINDOW = max(20, int(os.getenv("RECENT_PERFORMANCE_WINDOW", "40")))
WATCHDOG_ENABLED = os.getenv("WATCHDOG_ENABLED", "true").lower() == "true"
WATCHDOG_STALE_SECONDS = max(120, int(os.getenv("WATCHDOG_STALE_SECONDS", "180")))
WATCHDOG_CHECK_SECONDS = max(15, int(os.getenv("WATCHDOG_CHECK_SECONDS", "30")))
WORKER_RESTART_BACKOFF_SECONDS = max(1, int(os.getenv("WORKER_RESTART_BACKOFF_SECONDS", "5")))
# V2.0: market factors are evidence, not all-or-nothing gates.
# Only execution-safety constraints remain hard.
DISCOVERY_MIN_SAMPLES = max(100, int(os.getenv("DISCOVERY_MIN_SAMPLES", "100")))
DISCOVERY_MIN_EDGE = max(0.01, min(0.30, float(os.getenv("DISCOVERY_MIN_EDGE", "0.08"))))
DISCOVERY_SHRINKAGE = max(10.0, float(os.getenv("DISCOVERY_SHRINKAGE", "40")))
BOOTSTRAP_MIN_CONFIDENCE = max(0.35, min(0.60, float(os.getenv("BOOTSTRAP_MIN_CONFIDENCE", "0.45"))))
BOOTSTRAP_MAX_CONFIDENCE = max(0.66, min(0.85, float(os.getenv("BOOTSTRAP_MAX_CONFIDENCE", "0.78"))))
BOOTSTRAP_BLEND_MIN_SAMPLES = max(10, int(os.getenv("BOOTSTRAP_BLEND_MIN_SAMPLES", "20")))
BREAK_EVEN_TRIGGER_R = max(0.5, float(os.getenv("BREAK_EVEN_TRIGGER_R", "1.0")))
BREAK_EVEN_LOCK_R = max(0.0, float(os.getenv("BREAK_EVEN_LOCK_R", "0.05")))
PROFIT_LOCK_TRIGGER_R = max(BREAK_EVEN_TRIGGER_R, float(os.getenv("PROFIT_LOCK_TRIGGER_R", "1.5")))
PROFIT_LOCK_R = max(BREAK_EVEN_LOCK_R, float(os.getenv("PROFIT_LOCK_R", "0.75")))
TRAIL_TRIGGER_R = max(PROFIT_LOCK_TRIGGER_R, float(os.getenv("TRAIL_TRIGGER_R", "2.0")))
TRAIL_DISTANCE_R = max(0.25, float(os.getenv("TRAIL_DISTANCE_R", "0.75")))
EXIT_POLICY_MIN_SAMPLES = max(100, int(os.getenv("EXIT_POLICY_MIN_SAMPLES", "100")))
TREND_RUNNER_ENABLED = os.getenv("TREND_RUNNER_ENABLED", "true").lower() == "true"
TREND_RUNNER_MIN_SCORE = max(0.0, float(os.getenv("TREND_RUNNER_MIN_SCORE", "0.62")))
TREND_RUNNER_TP_R = max(2.0, float(os.getenv("TREND_RUNNER_TP_R", "3.0")))
TREND_RUNNER_TRAIL_START_R = max(1.5, float(os.getenv("TREND_RUNNER_TRAIL_START_R", "1.75")))
TREND_RUNNER_TRAIL_DISTANCE_R = max(0.40, float(os.getenv("TREND_RUNNER_TRAIL_DISTANCE_R", "0.90")))
VERSION_TAG = "3.7"
ENTRY_TIMING_ENABLED = os.getenv("ENTRY_TIMING_ENABLED", "true").lower() == "true"
MAX_ENTRY_EXTENSION_ATR = max(0.5, float(os.getenv("MAX_ENTRY_EXTENSION_ATR", "1.20")))
MIN_ROOM_TO_BARRIER_R = max(1.0, float(os.getenv("MIN_ROOM_TO_BARRIER_R", "1.50")))
REENTRY_REQUIRE_NEW_CANDLE = os.getenv("REENTRY_REQUIRE_NEW_CANDLE", "true").lower() == "true"
REENTRY_REQUIRE_STRUCTURE_CHANGE = os.getenv("REENTRY_REQUIRE_STRUCTURE_CHANGE", "true").lower() == "true"
STRUCTURAL_ROOM_ENABLED = os.getenv("STRUCTURAL_ROOM_ENABLED", "true").lower() == "true"
STRUCTURAL_BARRIER_BUFFER_R = max(0.0, float(os.getenv("STRUCTURAL_BARRIER_BUFFER_R", "0.05")))
STRUCTURE_STRONG_SCORE = max(0.50, min(0.95, float(os.getenv("STRUCTURE_STRONG_SCORE", "0.72"))))
STRUCTURE_BLOCK_SCORE = max(STRUCTURE_STRONG_SCORE, min(0.99, float(os.getenv("STRUCTURE_BLOCK_SCORE", "0.82"))))
BREAKOUT_CONFIRM_ATR = max(0.05, float(os.getenv("BREAKOUT_CONFIRM_ATR", "0.18")))
BREAKOUT_RETEST_TOLERANCE_ATR = max(0.05, float(os.getenv("BREAKOUT_RETEST_TOLERANCE_ATR", "0.20")))
WEAK_BARRIER_CONFIDENCE_PENALTY = max(0.0, min(0.20, float(os.getenv("WEAK_BARRIER_CONFIDENCE_PENALTY", "0.04"))))
MEDIUM_BARRIER_CONFIDENCE_PENALTY = max(WEAK_BARRIER_CONFIDENCE_PENALTY, min(0.25, float(os.getenv("MEDIUM_BARRIER_CONFIDENCE_PENALTY", "0.08"))))
DIRECTION_MIN_SCORE = max(0.0, min(100.0, float(os.getenv("DIRECTION_MIN_SCORE", "30"))))
DIRECTION_MIN_EDGE = max(0.0, min(50.0, float(os.getenv("DIRECTION_MIN_EDGE", "6"))))
COUNTERTREND_EXECUTION_MIN_SCORE = max(0.0, min(100.0, float(os.getenv("COUNTERTREND_EXECUTION_MIN_SCORE", "82"))))
MIN_TAKE_PROFIT_PIPS = max(0.1, float(os.getenv("MIN_TAKE_PROFIT_PIPS", "7.0")))
MIN_STOP_PIPS = max(0.1, float(os.getenv("MIN_STOP_PIPS", "3.0")))
STOP_ATR_M1_MULT = max(0.1, float(os.getenv("STOP_ATR_M1_MULT", "1.50")))
STOP_ATR_M5_MULT = max(0.1, float(os.getenv("STOP_ATR_M5_MULT", "0.40")))
M1_CONFIRMATION_REQUIRED = os.getenv("M1_CONFIRMATION_REQUIRED", "true").lower() == "true"
DEDUP_SIGNAL_SNAPSHOTS = os.getenv("DEDUP_SIGNAL_SNAPSHOTS", "true").lower() == "true"
RESEARCH_LAB_ENABLED = os.getenv("RESEARCH_LAB_ENABLED", "true").lower() == "true"
RESEARCH_EVAL_MIN_SAMPLES = max(30, int(os.getenv("RESEARCH_EVAL_MIN_SAMPLES", "50")))
RESEARCH_VALIDATE_MIN_SAMPLES = max(RESEARCH_EVAL_MIN_SAMPLES, int(os.getenv("RESEARCH_VALIDATE_MIN_SAMPLES", "100")))
RESEARCH_MIN_EDGE = max(0.02, min(0.30, float(os.getenv("RESEARCH_MIN_EDGE", "0.08"))))
MODEL_MIN_NEW_LABELS = max(20, int(os.getenv("MODEL_MIN_NEW_LABELS", "50")))
SHADOW_MAX_VARIANTS_PER_SIGNAL = max(1, min(8, int(os.getenv("SHADOW_MAX_VARIANTS_PER_SIGNAL", "4"))))

EXTERNAL_RESEARCH_ENABLED = os.getenv("EXTERNAL_RESEARCH_ENABLED", "true").lower() == "true"
EXTERNAL_RESEARCH_MIN_SAMPLES = max(50, int(os.getenv("EXTERNAL_RESEARCH_MIN_SAMPLES", "50")))
EXTERNAL_RESEARCH_VALIDATE_SAMPLES = max(100, int(os.getenv("EXTERNAL_RESEARCH_VALIDATE_SAMPLES", "100")))
EXTERNAL_RESEARCH_MIN_EDGE = max(0.02, min(0.30, float(os.getenv("EXTERNAL_RESEARCH_MIN_EDGE", "0.08"))))
EXTERNAL_RESEARCH_AUTO_ACTIVATE = False
EXTERNAL_RESEARCH_SYMBOLS = [x.strip().upper().replace("/", "_") for x in os.getenv(
    "EXTERNAL_RESEARCH_SYMBOLS", "GBP_USD,USD_JPY,AUD_USD,USD_CAD"
).split(",") if x.strip()]
EXTERNAL_RESEARCH_GRANULARITY = os.getenv("EXTERNAL_RESEARCH_GRANULARITY", "M5").upper()
EXTERNAL_RESEARCH_CANDLE_COUNT = max(30, min(200, int(os.getenv("EXTERNAL_RESEARCH_CANDLE_COUNT", "80"))))
EXTERNAL_RESEARCH_MIN_MOVE_ATR = max(0.05, float(os.getenv("EXTERNAL_RESEARCH_MIN_MOVE_ATR", "0.20")))
EXTERNAL_NEWS_RESEARCH = os.getenv("EXTERNAL_NEWS_RESEARCH", "true").lower() == "true"
AUTO_PROMOTE_RESEARCH = os.getenv("AUTO_PROMOTE_RESEARCH", "true").lower() == "true"
AUTO_PROMOTE_MIN_SAMPLES = 100
AUTO_PROMOTE_MIN_EDGE = max(0.05, min(0.30, float(os.getenv("AUTO_PROMOTE_MIN_EDGE", "0.10"))))
AUTO_PROMOTE_REVIEW_SAMPLES = 50
AUTO_PROMOTE_ROLLBACK_DROP = max(0.03, min(0.25, float(os.getenv("AUTO_PROMOTE_ROLLBACK_DROP", "0.08"))))
AUTO_PROMOTE_RETRY_NEW_SAMPLES = max(30, int(os.getenv("AUTO_PROMOTE_RETRY_NEW_SAMPLES", "50")))
AUTO_PROMOTE_MAX_ACTIVE = 0  # 0 = no fixed cap; compatibility/evidence governs
AUTONOMOUS_DISCOVERY_ENABLED = os.getenv("AUTONOMOUS_DISCOVERY_ENABLED", "true").lower() == "true"
AUTONOMOUS_DISCOVERY_MIN_ROWS = max(80, int(os.getenv("AUTONOMOUS_DISCOVERY_MIN_ROWS", "100")))
AUTONOMOUS_DISCOVERY_HOLDOUT = max(0.25, min(0.50, float(os.getenv("AUTONOMOUS_DISCOVERY_HOLDOUT", "0.40"))))
AUTONOMOUS_DISCOVERY_MIN_COVERAGE = max(0.10, min(0.40, float(os.getenv("AUTONOMOUS_DISCOVERY_MIN_COVERAGE", "0.15"))))
AUTONOMOUS_DISCOVERY_MAX_COVERAGE = max(0.60, min(0.95, float(os.getenv("AUTONOMOUS_DISCOVERY_MAX_COVERAGE", "0.85"))))
AUTONOMOUS_DISCOVERY_MIN_EDGE = max(0.05, min(0.30, float(os.getenv("AUTONOMOUS_DISCOVERY_MIN_EDGE", "0.10"))))
AUTONOMOUS_DISCOVERY_MAX_FEATURES = max(6, min(30, int(os.getenv("AUTONOMOUS_DISCOVERY_MAX_FEATURES", "18"))))
AUTONOMOUS_DISCOVERY_MAX_PAIRWISE = max(5, min(100, int(os.getenv("AUTONOMOUS_DISCOVERY_MAX_PAIRWISE", "30"))))
AUTONOMOUS_SHADOW_WEIGHT = max(0.05, min(0.50, float(os.getenv("AUTONOMOUS_SHADOW_WEIGHT", "0.20"))))
AUTONOMOUS_PROMOTION_MIN_CANONICAL = max(20, int(os.getenv("AUTONOMOUS_PROMOTION_MIN_CANONICAL", "30")))
MULTI_FILTER_COMPAT_ENABLED = os.getenv("MULTI_FILTER_COMPAT_ENABLED", "true").lower() == "true"
MULTI_FILTER_MIN_JOINT_SAMPLES = max(10, int(os.getenv("MULTI_FILTER_MIN_JOINT_SAMPLES", "20")))
MULTI_FILTER_MIN_JOINT_COVERAGE = max(0.05, min(0.40, float(os.getenv("MULTI_FILTER_MIN_JOINT_COVERAGE", "0.10"))))
MULTI_FILTER_MAX_WR_DROP = max(0.00, min(0.20, float(os.getenv("MULTI_FILTER_MAX_WR_DROP", "0.05"))))
ACTIVE_RULE_HEALTH_BLOCK = 50
EXTERNAL_INCLUDE_SHADOW = os.getenv("EXTERNAL_INCLUDE_SHADOW", "true").lower() == "true"
EXTERNAL_SHADOW_BASELINE_WEIGHT = max(0.10, min(1.0, float(os.getenv("EXTERNAL_SHADOW_BASELINE_WEIGHT", "0.50"))))
EXTERNAL_SHADOW_VARIANT_WEIGHT = max(0.05, min(EXTERNAL_SHADOW_BASELINE_WEIGHT, float(os.getenv("EXTERNAL_SHADOW_VARIANT_WEIGHT", "0.25"))))
EXTERNAL_PROMOTION_MIN_CANONICAL = max(10, int(os.getenv("EXTERNAL_PROMOTION_MIN_CANONICAL", "20")))
EXECUTION_MIN_CONFIDENCE = max(0.50, min(0.95, float(os.getenv("EXECUTION_MIN_CONFIDENCE", "0.65"))))
NY = ZoneInfo("America/New_York")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("market-alert")
app = FastAPI(title="Market Alert V3.7 — Parallel Filter Evolution / OANDA Practice Only")
state: Dict[str, Any] = {
    "started": datetime.now(timezone.utc).isoformat(),
    "last_scan": None,
    "last_successful_scan": None,
    "last_error": None,
    "cycles": 0,
    "successful_cycles": 0,
    "worker_restarts": 0,
    "worker_running": False,
    "worker_started_at": None,
    "worker_last_heartbeat": None,
    "watchdog_last_check": None,
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
    # V2.0 learning schema migration.
    sample_cols = {row[1] for row in c.execute("PRAGMA table_info(learning_samples)").fetchall()}
    sample_migrations = {
        "resolved_ts": "TEXT",
        "bars_to_resolution": "INTEGER",
        "mfe_r": "REAL",
        "mae_r": "REAL",
        "note": "TEXT",
    }
    for name, ddl in sample_migrations.items():
        if name not in sample_cols:
            c.execute(f"ALTER TABLE learning_samples ADD COLUMN {name} {ddl}")

    c.execute("""
        CREATE TABLE IF NOT EXISTS discovered_patterns(
            pattern_key TEXT PRIMARY KEY,
            family TEXT NOT NULL,
            value TEXT NOT NULL,
            samples INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            win_rate REAL,
            global_win_rate REAL,
            edge REAL,
            weight REAL NOT NULL,
            validated INTEGER NOT NULL,
            updated_ts TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS external_research_observations(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, candle_ts TEXT,
            instrument TEXT NOT NULL, source_type TEXT NOT NULL, source_key TEXT NOT NULL,
            value_num REAL, value_text TEXT, metadata_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS external_hypotheses(
            hypothesis_key TEXT PRIMARY KEY, description TEXT NOT NULL, family TEXT NOT NULL,
            stage TEXT NOT NULL DEFAULT 'EXPERIMENTAL', total_samples INTEGER NOT NULL DEFAULT 0,
            aligned_samples INTEGER NOT NULL DEFAULT 0, aligned_wins INTEGER NOT NULL DEFAULT 0,
            aligned_win_rate REAL, control_samples INTEGER NOT NULL DEFAULT 0,
            control_wins INTEGER NOT NULL DEFAULT 0, control_win_rate REAL, edge REAL,
            recommendation TEXT NOT NULL DEFAULT 'KEEP_TESTING',
            automatic_live_activation INTEGER NOT NULL DEFAULT 0, updated_ts TEXT NOT NULL
        )
    """)
    ext_cols = {row[1] for row in c.execute("PRAGMA table_info(external_hypotheses)").fetchall()}
    ext_migrations = {
        "canonical_samples": "INTEGER NOT NULL DEFAULT 0",
        "shadow_samples": "INTEGER NOT NULL DEFAULT 0",
        "effective_samples": "REAL NOT NULL DEFAULT 0",
        "shadow_weighted_wins": "REAL NOT NULL DEFAULT 0"
    }
    for name, ddl in ext_migrations.items():
        if name not in ext_cols:
            c.execute(f"ALTER TABLE external_hypotheses ADD COLUMN {name} {ddl}")
    c.execute("""
        CREATE TABLE IF NOT EXISTS research_knowledge(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
            hypothesis_key TEXT NOT NULL, finding TEXT NOT NULL,
            evidence_json TEXT NOT NULL DEFAULT '{}', UNIQUE(hypothesis_key,finding)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS shadow_trials(
            id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id INTEGER NOT NULL, created_ts TEXT NOT NULL, candle_ts TEXT,
            instrument TEXT NOT NULL, direction TEXT NOT NULL, variant TEXT NOT NULL, entry REAL NOT NULL, stop REAL NOT NULL,
            target REAL NOT NULL, risk REAL NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING', label INTEGER, resolved_ts TEXT,
            bars_to_resolution INTEGER, mfe_r REAL, mae_r REAL, note TEXT, UNIQUE(signal_id, variant),
            FOREIGN KEY(signal_id) REFERENCES signals(id))
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS filter_hypotheses(
            filter_key TEXT PRIMARY KEY, description TEXT NOT NULL, stage TEXT NOT NULL, total_samples INTEGER NOT NULL,
            pass_samples INTEGER NOT NULL, pass_wins INTEGER NOT NULL, pass_win_rate REAL, fail_samples INTEGER NOT NULL,
            fail_wins INTEGER NOT NULL, fail_win_rate REAL, edge REAL, coverage REAL, recommendation TEXT, updated_ts TEXT NOT NULL)
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS autonomous_hypotheses(
            hypothesis_key TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            rule_json TEXT NOT NULL,
            family TEXT NOT NULL,
            stage TEXT NOT NULL,
            discovery_samples REAL NOT NULL DEFAULT 0,
            validation_samples REAL NOT NULL DEFAULT 0,
            canonical_validation_samples INTEGER NOT NULL DEFAULT 0,
            shadow_validation_samples INTEGER NOT NULL DEFAULT 0,
            pass_samples REAL NOT NULL DEFAULT 0,
            pass_wins REAL NOT NULL DEFAULT 0,
            pass_win_rate REAL,
            fail_samples REAL NOT NULL DEFAULT 0,
            fail_wins REAL NOT NULL DEFAULT 0,
            fail_win_rate REAL,
            edge REAL,
            coverage REAL,
            score REAL,
            recommendation TEXT,
            generation INTEGER NOT NULL DEFAULT 1,
            updated_ts TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS research_family_stats(
            family TEXT PRIMARY KEY,
            hypotheses_tested INTEGER NOT NULL DEFAULT 0,
            validated INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0,
            avg_abs_edge REAL,
            best_edge REAL,
            priority_score REAL NOT NULL DEFAULT 1.0,
            updated_ts TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS active_research_rules(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            rule_key TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            activated_ts TEXT NOT NULL,
            deactivated_ts TEXT,
            evidence_samples INTEGER NOT NULL,
            evidence_edge REAL,
            baseline_win_rate REAL,
            review_after_samples INTEGER NOT NULL,
            post_samples INTEGER NOT NULL DEFAULT 0,
            post_wins INTEGER NOT NULL DEFAULT 0,
            post_win_rate REAL,
            last_review_ts TEXT,
            reason TEXT,
            UNIQUE(source, rule_key, activated_ts)
        )
    """)
    active_cols = {row[1] for row in c.execute("PRAGMA table_info(active_research_rules)").fetchall()}
    active_migrations = {
        "reviewed_matches": "INTEGER NOT NULL DEFAULT 0",
        "confirmed_ts": "TEXT",
        "last_health_block_wr": "REAL"
    }
    for name, ddl in active_migrations.items():
        if name not in active_cols:
            c.execute(f"ALTER TABLE active_research_rules ADD COLUMN {name} {ddl}")
    c.execute("""
        CREATE TABLE IF NOT EXISTS research_rule_audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            action TEXT NOT NULL,
            source TEXT NOT NULL,
            rule_key TEXT NOT NULL,
            details_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS research_rule_compatibility(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_a TEXT NOT NULL,
            rule_a TEXT NOT NULL,
            source_b TEXT NOT NULL,
            rule_b TEXT NOT NULL,
            checked_ts TEXT NOT NULL,
            samples INTEGER NOT NULL,
            joint_pass INTEGER NOT NULL,
            joint_win_rate REAL,
            joint_coverage REAL,
            compatible INTEGER NOT NULL,
            reason TEXT,
            UNIQUE(source_a, rule_a, source_b, rule_b)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS active_trade_management(
            trade_id TEXT PRIMARY KEY,
            instrument TEXT NOT NULL,
            side TEXT NOT NULL,
            entry REAL NOT NULL,
            initial_stop REAL NOT NULL,
            initial_target REAL NOT NULL,
            current_stop REAL,
            setup_variant TEXT,
            policy TEXT NOT NULL,
            trend_score REAL,
            opened_ts TEXT NOT NULL,
            last_r REAL NOT NULL DEFAULT 0,
            last_action TEXT,
            break_even_applied INTEGER NOT NULL DEFAULT 0,
            profit_lock_applied INTEGER NOT NULL DEFAULT 0,
            trailing_applied INTEGER NOT NULL DEFAULT 0,
            closed INTEGER NOT NULL DEFAULT 0,
            updated_ts TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS strategy_version_stats(
            version_tag TEXT PRIMARY KEY,
            started_ts TEXT NOT NULL,
            resolved_trades INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            total_r REAL NOT NULL DEFAULT 0,
            avg_r REAL,
            win_rate REAL,
            note TEXT
        )
    """)
    c.execute("INSERT OR IGNORE INTO strategy_version_stats(version_tag,started_ts,note) VALUES(?,?,?)",
              (VERSION_TAG, now_iso(), "Quality Scalper: RR>=1.5, trend runner, active stop management"))
    c.execute("CREATE INDEX IF NOT EXISTS idx_patterns_validated ON discovered_patterns(validated,weight)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_shadow_pending ON shadow_trials(status,instrument)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_filter_stage ON filter_hypotheses(stage,total_samples)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_active_research_status ON active_research_rules(status,activated_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_research_rule_audit ON research_rule_audit(source,rule_key,ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_autonomous_stage ON autonomous_hypotheses(stage,validation_samples)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_autonomous_score ON autonomous_hypotheses(score,edge)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_rule_compat ON research_rule_compatibility(compatible,checked_ts)")
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




def pivot_levels(candles_: List[Dict[str, Any]], left: int = 2, right: int = 2) -> Dict[str, List[Dict[str, Any]]]:
    """Confirmed local pivots with metadata; no lookahead beyond supplied history."""
    highs, lows = [], []
    n = len(candles_)
    for i in range(left, n-right):
        h = float(candles_[i]["h"]); l = float(candles_[i]["l"])
        if all(h >= float(candles_[j]["h"]) for j in range(i-left, i+right+1) if j != i):
            highs.append({"price": h, "index": i, "ts": candles_[i].get("t")})
        if all(l <= float(candles_[j]["l"]) for j in range(i-left, i+right+1) if j != i):
            lows.append({"price": l, "index": i, "ts": candles_[i].get("t")})
    return {"highs": highs, "lows": lows}


def _atr_value(candles_: List[Dict[str, Any]], n: int = 14) -> float:
    if len(candles_) < 2:
        return 0.0
    trs = []
    prev = float(candles_[0]["c"])
    for x in candles_[1:]:
        h,l,c = float(x["h"]),float(x["l"]),float(x["c"])
        trs.append(max(h-l, abs(h-prev), abs(l-prev)))
        prev = c
    if not trs:
        return 0.0
    return sum(trs[-n:]) / min(n, len(trs))


def _touch_count(candles_: List[Dict[str, Any]], level: float, atr: float) -> int:
    """Count distinct reactions near a level; used as importance evidence."""
    tol = max(atr * 0.18, 0.00005)
    touches = 0
    last_i = -99
    for i,x in enumerate(candles_):
        if float(x["l"]) - tol <= level <= float(x["h"]) + tol and i-last_i >= 2:
            touches += 1
            last_i = i
    return touches


def _level_strength(level: float, timeframe: str, candles_: List[Dict[str, Any]], atr: float, current_price: float) -> Dict[str, Any]:
    touches = _touch_count(candles_, level, atr)
    tf_weight = 0.28 if timeframe == "H1" else 0.16
    touch_weight = min(0.42, max(0, touches-1) * 0.11)
    recency = 0.12  # pivots are already recent-history candidates
    distance_atr = abs(level-current_price) / max(atr, 1e-12)
    proximity = 0.08 if distance_atr <= 1.5 else 0.03 if distance_atr <= 3 else 0.0
    score = clamp(tf_weight + touch_weight + recency + proximity, 0.0, 1.0)
    return {
        "score": score,
        "touches": touches,
        "timeframe": timeframe,
        "distance_atr": distance_atr,
    }


def _breakout_status(level: float, signal: str, m15: List[Dict[str, Any]], atr15: float) -> Dict[str, Any]:
    """
    A barrier is considered broken only after a close beyond it by a volatility-scaled margin.
    A successful retest adds confidence. We do not treat a single wick as a breakout.
    """
    if len(m15) < 4:
        return {"broken": False, "confirmed": False, "retested": False, "break_strength": 0.0}
    margin = atr15 * BREAKOUT_CONFIRM_ATR
    retest_tol = atr15 * BREAKOUT_RETEST_TOLERANCE_ATR
    recent = m15[-5:]
    broken_idx = None
    for i,x in enumerate(recent):
        close = float(x["c"])
        if (signal=="BUY" and close > level + margin) or (signal=="SELL" and close < level - margin):
            broken_idx = i
            break
    if broken_idx is None:
        return {"broken": False, "confirmed": False, "retested": False, "break_strength": 0.0}

    subsequent = recent[broken_idx+1:]
    confirmed = bool(subsequent) and (
        any(float(x["c"]) > level for x in subsequent) if signal=="BUY"
        else any(float(x["c"]) < level for x in subsequent)
    )
    retested = False
    for x in subsequent:
        lo,hi,cl = float(x["l"]),float(x["h"]),float(x["c"])
        if signal=="BUY":
            if lo <= level + retest_tol and cl >= level:
                retested = True
        else:
            if hi >= level - retest_tol and cl <= level:
                retested = True
    break_strength = 0.55 + (0.20 if confirmed else 0.0) + (0.20 if retested else 0.0)
    return {
        "broken": True,
        "confirmed": confirmed,
        "retested": retested,
        "break_strength": min(1.0, break_strength),
    }


def structural_context(h1, m15, entry: float, signal: str) -> Dict[str, Any]:
    """
    Contextual support/resistance:
    - weak/medium levels reduce confidence;
    - only strong, unbroken barriers can veto for lack of room;
    - broken/confirmed barriers are skipped and the next relevant barrier is evaluated.
    """
    if signal not in ("BUY","SELL"):
        return {"active_barrier": None, "room_to_barrier_r": None, "levels": [], "broken_levels": []}

    h1_hist = h1[:-1]
    m15_hist = m15[:-1]
    atr15 = _atr_value(m15_hist, 14)
    atr1h = _atr_value(h1_hist, 14)

    hp = pivot_levels(h1_hist, 2, 2)
    mp = pivot_levels(m15_hist, 3, 3)
    candidates = []

    sidekey = "highs" if signal=="BUY" else "lows"
    for p in hp[sidekey]:
        px=float(p["price"])
        if (signal=="BUY" and px>entry) or (signal=="SELL" and px<entry):
            st=_level_strength(px,"H1",h1_hist,max(atr1h,atr15),entry)
            br=_breakout_status(px,signal,m15_hist,atr15)
            candidates.append({"price":px, **st, **br})
    for p in mp[sidekey]:
        px=float(p["price"])
        if (signal=="BUY" and px>entry) or (signal=="SELL" and px<entry):
            st=_level_strength(px,"M15",m15_hist,atr15,entry)
            br=_breakout_status(px,signal,m15_hist,atr15)
            candidates.append({"price":px, **st, **br})

    # Deduplicate close levels, keeping the stronger representation.
    candidates.sort(key=lambda x: x["price"])
    dedup=[]
    tol=max(atr15*0.12,0.00005)
    for c in candidates:
        if dedup and abs(c["price"]-dedup[-1]["price"]) <= tol:
            if c["score"] > dedup[-1]["score"]:
                dedup[-1]=c
        else:
            dedup.append(c)

    broken=[x for x in dedup if x["broken"] and x["confirmed"]]
    active_candidates=[x for x in dedup if not (x["broken"] and x["confirmed"])]

    if signal=="BUY":
        active_candidates.sort(key=lambda x:x["price"])
    else:
        active_candidates.sort(key=lambda x:x["price"], reverse=True)

    active = active_candidates[0] if active_candidates else None
    return {
        "active_barrier": active,
        "levels": dedup,
        "broken_levels": broken,
        "atr15": atr15,
    }


def structural_confidence_adjustment(ctx: Dict[str, Any]) -> Dict[str, Any]:
    active=ctx.get("active_barrier")
    if not active:
        return {"adjustment":0.0,"classification":"NONE","reason":"sin barrera activa relevante"}
    score=float(active["score"])
    if score >= STRUCTURE_BLOCK_SCORE:
        return {"adjustment":0.0,"classification":"STRONG","reason":f"barrera fuerte score={score:.2f}"}
    if score >= STRUCTURE_STRONG_SCORE:
        return {"adjustment":-MEDIUM_BARRIER_CONFIDENCE_PENALTY,"classification":"MEDIUM","reason":f"barrera media score={score:.2f}"}
    return {"adjustment":-WEAK_BARRIER_CONFIDENCE_PENALTY,"classification":"WEAK","reason":f"barrera débil score={score:.2f}"}


def pip_size(inst: str) -> float:
    return 0.01 if "_JPY" in inst.upper() or inst.upper().endswith("JPY") else 0.0001

def pips_between(a: float, b: float, inst: str) -> float:
    return abs(float(a)-float(b))/pip_size(inst)

def _direction_hypothesis(h1, m15, m5, m1, inst: str, sig: str) -> Dict[str, Any]:
    """Evaluate BUY or SELL independently. Higher timeframes are evidence, not a direction lock."""
    c60, c15, c5, c1 = ([x["c"] for x in h1], [x["c"] for x in m15],
                         [x["c"] for x in m5], [x["c"] for x in m1])
    h20,h50 = ema(c60,20),ema(c60,50)
    e20,e50,e5,e9,e1 = ema(c15,20),ema(c15,50),ema(c5,20),ema(c1,9),ema(c1,20)
    a60,a15,a1 = atr(h1),atr(m15),atr(m1)

    hgap=(h20[-1]-h50[-1])/max(a60,1e-9)
    hslope=(h20[-1]-h20[-5])/max(a60,1e-9)
    gap=(e20[-1]-e50[-1])/max(a15,1e-9)
    slope=(e20[-1]-e20[-5])/max(a15,1e-9)

    sign = 1.0 if sig=="BUY" else -1.0
    h1_support = sign*hgap > .08 and sign*hslope > .03
    m15_support = sign*gap > .10 and sign*slope > .05
    h1_opposes = -sign*hgap > .12 and -sign*hslope > .05
    m15_opposes = -sign*gap > .15 and -sign*slope > .07

    mb,ms=structure(m5)
    m5m=mom(m5,6)
    m5_structure = (mb and m5m>0) if sig=="BUY" else (ms and m5m<0)
    m5_momentum = sign*m5m > 0

    pc,pr=pullbacks(m5,e5,sig)
    second=pc>=2 and pr

    last=m1[-1]
    ph,pl,mm=swing(m1[:-1],"h",7),swing(m1[:-1],"l",7),mom(m1,4)
    cb=e9[-1]>e1[-1] and mm>0 and last["c"]>last["o"]
    cs=e9[-1]<e1[-1] and mm<0 and last["c"]<last["o"]
    confirm=(cb and (last["c"]>ph or mm>.00012)) if sig=="BUY" else (cs and (last["c"]<pl or mm<-.00012))
    m1_momentum = sign*mm > 0

    ext=abs(last["c"]-e1[-1])/max(a1,1e-9)
    vols=[atr(m1[:len(m1)-i]) for i in range(18) if len(m1)-i>15]
    vol=a1/max(mean(vols),1e-9)
    entry=last["c"]

    ss=swing(m1,"l" if sig=="BUY" else "h",12)
    a5=atr(m5)
    pip=pip_size(inst)
    volatility_risk=max(a1*STOP_ATR_M1_MULT, a5*STOP_ATR_M5_MULT, MIN_STOP_PIPS*pip)
    structure_risk=abs(entry-ss) if math.isfinite(float(ss)) else 0.0
    risk=max(volatility_risk, structure_risk)
    stop=entry-risk if sig=="BUY" else entry+risk

    ctx=structural_context(h1,m15,entry,sig) if STRUCTURAL_ROOM_ENABLED else {"active_barrier":None,"levels":[],"broken_levels":[]}
    active=ctx.get("active_barrier")
    barrier=float(active["price"]) if active else None
    room=((barrier-entry) if sig=="BUY" else (entry-barrier)) if barrier is not None else None
    room_r=room/risk if room is not None and risk>0 else None
    barrier_score=float(active["score"]) if active else 0.0
    barrier_class=("STRONG" if barrier_score>=STRUCTURE_BLOCK_SCORE else
                   "MEDIUM" if barrier_score>=STRUCTURE_STRONG_SCORE else
                   "WEAK" if active else "NONE")

    st=swing(m5[:-2],"h" if sig=="BUY" else "l",28)
    structural_reward=(st-entry) if sig=="BUY" else (entry-st)
    rr_raw=structural_reward/risk if structural_reward>0 else MIN_RR
    rr=MIN_RR
    min_tp_distance=max(risk*MIN_RR, MIN_TAKE_PROFIT_PIPS*pip)
    target=entry+min_tp_distance if sig=="BUY" else entry-min_tp_distance
    barrier_allows_target=True
    if active and barrier_class=="STRONG" and not bool(active.get("broken")):
        buffer=risk*STRUCTURAL_BARRIER_BUFFER_R
        cap=barrier-buffer if sig=="BUY" else barrier+buffer
        barrier_allows_target=(target<=cap) if sig=="BUY" else (target>=cap)
        rr_raw=min(rr_raw,max(0.0,room_r if room_r is not None else rr_raw))
    actual_rr=abs(target-entry)/max(risk,1e-12)
    tp_pips=pips_between(entry,target,inst)

    sess=session_info(last["t"])

    # Independent directional score. H1/M15 matter, but M5/M1 can build a reversal hypothesis.
    dscore=0.0
    dscore += 16 if h1_support else (-10 if h1_opposes else 3)
    dscore += 20 if m15_support else (-12 if m15_opposes else 4)
    dscore += 18 if m5_structure else (7 if m5_momentum else 0)
    dscore += 16 if confirm else (6 if m1_momentum else 0)
    dscore += 8 if second else 4 if pc>=1 and pr else 0
    dscore += 8 if rr_raw>=2 else 6 if rr_raw>=MIN_RR else 0
    dscore += 5 if .65<=vol<=2 else 0
    dscore += 5 if ext<=1.20 else 2 if ext<=1.60 else 0
    dscore += 4 if sess["ok"] else 0
    # Confirmed broken barriers in the trade direction are positive structural evidence.
    dscore += min(6, 2*len(ctx.get("broken_levels",[])))
    dscore=clamp(dscore,0,100)

    countertrend = (h1_opposes and m15_opposes)
    transition = (h1_opposes or m15_opposes) and (m5_structure or confirm)

    checks={
        "h1_context": h1_support,
        "m15_context": m15_support,
        "m5_structure": m5_structure,
        "second_pullback": second,
        "m1_confirmation": confirm,
        "minimum_rr": actual_rr>=MIN_RR and rr_raw>=MIN_RR,
        "minimum_tp_pips": tp_pips>=MIN_TAKE_PROFIT_PIPS-1e-9,
        "barrier_room_ok": barrier_allows_target,
        "not_extended": ext<=1.35,
        "volatility_ok": .65<=vol<=2,
    }
    if SESSION:
        checks["ny_session"]=sess["ok"]

    safety={
        "valid_direction": True,
        "finite_prices": all(math.isfinite(float(x)) for x in (entry,stop,target)),
        "positive_risk": abs(entry-stop)>0,
        "minimum_rr": actual_rr>=MIN_RR-1e-9 and rr_raw>=MIN_RR,
        "minimum_tp_pips": tp_pips>=MIN_TAKE_PROFIT_PIPS-1e-9,
        "minimum_stop_pips": pips_between(entry,stop,inst)>=MIN_STOP_PIPS-1e-9,
        "barrier_room_ok": barrier_allows_target,
        "volatility_sane": .35<=vol<=3.5,
    }

    return {
        "signal":sig,"direction_score":float(dscore),"entry":entry,"stop":stop,"target":target,
        "rr":actual_rr,"rr_raw":rr_raw,"risk":risk,"stop_pips":pips_between(entry,stop,inst),
        "target_pips":tp_pips,"structural_barrier":barrier,
        "room_to_barrier_r":room_r,"barrier_score":barrier_score,"barrier_class":barrier_class,
        "structure_context":ctx,"pullbacks":pc,"filters":checks,"safety_checks":safety,
        "countertrend":countertrend,"transition":transition,
        "metrics":{
            "h1_gap_atr":float(hgap),"h1_slope_atr":float(hslope),
            "m15_gap_atr":float(gap),"m15_slope_atr":float(slope),
            "m5_momentum":float(m5m),"m1_momentum":float(mm),
            "extension_atr":float(ext),"volatility_ratio":float(vol),
            "second_pullback":second,"m1_confirm":confirm,"session":sess,
        }
    }


def analyze(h1, m15, m5, m1, inst) -> Dict[str, Any]:
    buy=_direction_hypothesis(h1,m15,m5,m1,inst,"BUY")
    sell=_direction_hypothesis(h1,m15,m5,m1,inst,"SELL")

    buy_score=float(buy["direction_score"])
    sell_score=float(sell["direction_score"])
    edge=abs(buy_score-sell_score)

    if max(buy_score,sell_score)<DIRECTION_MIN_SCORE or edge<DIRECTION_MIN_EDGE:
        chosen=buy if buy_score>=sell_score else sell
        sig="WAIT"
    else:
        chosen=buy if buy_score>sell_score else sell
        sig=chosen["signal"]

    # A fully countertrend trade needs exceptional evidence; a transition is allowed to develop
    # without being suppressed merely because H1/M15 have not flipped yet.
    if sig!="WAIT" and chosen["countertrend"] and chosen["direction_score"]<COUNTERTREND_EXECUTION_MIN_SCORE:
        sig="WAIT"

    mt=chosen["metrics"]
    sess=mt["session"]
    tech=int(round(chosen["direction_score"])) if sig!="WAIT" else int(round(max(buy_score,sell_score)))
    checks=dict(chosen["filters"])
    checks["direction_edge_ok"]=edge>=DIRECTION_MIN_EDGE
    checks["countertrend_strength_ok"]=not chosen["countertrend"] or chosen["direction_score"]>=COUNTERTREND_EXECUTION_MIN_SCORE

    features={
        "direction_buy":1 if sig=="BUY" else 0,
        "technical_score":tech,"final_score":tech,
        "m15_gap_atr":mt["m15_gap_atr"],"m15_slope_atr":mt["m15_slope_atr"],
        "m5_momentum":mt["m5_momentum"],"pullbacks":int(chosen["pullbacks"]),
        "second_pullback":1 if mt["second_pullback"] else 0,
        "m1_momentum":mt["m1_momentum"],"m1_confirm":1 if mt["m1_confirm"] else 0,
        "extension_atr":mt["extension_atr"],"volatility_ratio":mt["volatility_ratio"],
        "rr_raw":float(chosen["rr_raw"]),
        "room_to_barrier_r":float(chosen["room_to_barrier_r"]) if chosen["room_to_barrier_r"] is not None else None,
        "barrier_score":chosen["barrier_score"],"barrier_class":chosen["barrier_class"],
        "broken_barriers":len(chosen["structure_context"].get("broken_levels",[])),
        "session_ok":1 if sess["ok"] else 0,"news_confirm":0,"news_contradict":0,
        "blocked":0,"hour_ny":float(sess["hour"]),
        # Diagnostic fields for both hypotheses:
        "buy_score":buy_score,"sell_score":sell_score,"direction_edge":edge,
        "h1_gap_atr":mt["h1_gap_atr"],"h1_slope_atr":mt["h1_slope_atr"],
        "transition_state":1 if chosen["transition"] else 0,
    }

    safety=dict(chosen["safety_checks"])
    safety["valid_direction"]=sig in ("BUY","SELL")

    return {
        "instrument":inst,"signal":sig,"technical":tech,"score":tech,
        "buy_score":buy_score,"sell_score":sell_score,"direction_edge":edge,
        "direction_state":"TRANSITION" if chosen["transition"] else "COUNTERTREND" if chosen["countertrend"] else "TREND",
        "entry":chosen["entry"],"stop":chosen["stop"],"target":chosen["target"],
        "rr":chosen["rr"],"rr_raw":chosen["rr_raw"],
        "stop_pips":chosen["stop_pips"],"target_pips":chosen["target_pips"],
        "structural_barrier":chosen["structural_barrier"],
        "room_to_barrier_r":chosen["room_to_barrier_r"],
        "barrier_score":chosen["barrier_score"],"barrier_class":chosen["barrier_class"],
        "structure_context":chosen["structure_context"],
        "blocked":sig=="WAIT" or not all(safety.values()),
        "pullbacks":chosen["pullbacks"],"filters":checks,"safety_checks":safety,
        "features":features,"candle_ts":m1[-1]["t"].isoformat(),"alignment":"N/A",
        "hypotheses":{
            "BUY":{"score":buy_score,"countertrend":buy["countertrend"],"transition":buy["transition"]},
            "SELL":{"score":sell_score,"countertrend":sell["countertrend"],"transition":sell["transition"]},
        }
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
        blocked = r["blocked"]
        features["blocked"] = 1 if blocked else 0
        return {**r, "score": score, "alignment": align, "blocked": blocked, "features": features,
                "news_bias": bias, "news_positive_hits": pos, "news_negative_hits": neg,
                "news_articles": [{"title": a.get("title", ""), "url": a.get("url", "")} for a in arts[:5]]}
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



def candidate_patterns(r: Dict[str, Any]) -> Dict[str, str]:
    """Generate interpretable candidate regimes. Nothing is trusted until >= DISCOVERY_MIN_SAMPLES."""
    f = r.get("features", {})
    hour = int(float(f.get("hour_ny", 0) or 0))
    vol = float(f.get("volatility_ratio", 0) or 0)
    ext = float(f.get("extension_atr", 0) or 0)
    mom5 = float(f.get("m5_momentum", 0) or 0)
    mom1 = float(f.get("m1_momentum", 0) or 0)
    slope = float(f.get("m15_slope_atr", 0) or 0)
    rr = float(f.get("rr_raw", 0) or 0)
    align = r.get("alignment", "N/A")
    filters = r.get("filters", {})

    session = "ASIA" if hour < 3 else "LONDON" if hour < 8 else "NY_AM" if hour < 12 else "NY_PM" if hour < 17 else "OFF_HOURS"
    vol_regime = "LOW" if vol < .75 else "NORMAL" if vol <= 1.35 else "HIGH"
    ext_regime = "FRESH" if ext <= .8 else "NORMAL" if ext <= 1.35 else "EXTENDED"
    momentum = "ALIGNED" if (r.get("signal") == "BUY" and mom5 > 0 and mom1 > 0) or (r.get("signal") == "SELL" and mom5 < 0 and mom1 < 0) else "MIXED"
    trend_strength = "STRONG" if abs(slope) >= .35 else "MEDIUM" if abs(slope) >= .15 else "WEAK"
    rr_bucket = "RR2+" if rr >= 2 else "RR15+" if rr >= 1.5 else "RRLOW"
    confirmations = sum(1 for k in ("m5_structure","second_pullback","m1_confirmation","not_extended","volatility_ok") if filters.get(k))

    return {
        "session": session,
        "volatility": vol_regime,
        "extension": ext_regime,
        "momentum": momentum,
        "trend_strength": trend_strength,
        "rr_bucket": rr_bucket,
        "news": align,
        "confirmations": str(confirmations),
        # Interaction patterns allow discovery beyond any single hand-written rule.
        "session_x_volatility": f"{session}|{vol_regime}",
        "trend_x_momentum": f"{trend_strength}|{momentum}",
        "volatility_x_extension": f"{vol_regime}|{ext_regime}",
        "direction_x_session": f"{r.get('signal')}|{session}",
    }


def refresh_discovered_patterns() -> Dict[str, Any]:
    """Re-evaluate candidate patterns from resolved samples; validates only after >=100 observations."""
    c = conn()
    rows = c.execute("""
        SELECT ls.label, s.signal, s.alignment, s.features_json, s.filters_json
        FROM learning_samples ls JOIN signals s ON s.id=ls.signal_id
        WHERE ls.label IN (0,1)
        ORDER BY ls.id ASC
    """).fetchall()
    if not rows:
        c.close()
        return {"resolved_samples": 0, "validated_patterns": 0}

    global_wr = sum(int(x["label"]) for x in rows) / len(rows)
    buckets: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        rr = {
            "signal": row["signal"], "alignment": row["alignment"],
            "features": json.loads(row["features_json"] or "{}"),
            "filters": json.loads(row["filters_json"] or "{}"),
        }
        for family, value in candidate_patterns(rr).items():
            key = f"{family}={value}"
            b = buckets.setdefault(key, {"family": family, "value": value, "samples": 0, "wins": 0})
            b["samples"] += 1
            b["wins"] += int(row["label"])

    validated = 0
    for key, b in buckets.items():
        n, wins = b["samples"], b["wins"]
        wr = wins / n
        edge = wr - global_wr
        # Shrink the observed edge toward zero to avoid overreacting.
        weight = edge * (n / (n + DISCOVERY_SHRINKAGE))
        is_valid = int(n >= DISCOVERY_MIN_SAMPLES and abs(edge) >= DISCOVERY_MIN_EDGE)
        if is_valid:
            validated += 1
        c.execute("""
            INSERT INTO discovered_patterns(pattern_key,family,value,samples,wins,win_rate,global_win_rate,edge,weight,validated,updated_ts)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(pattern_key) DO UPDATE SET
              samples=excluded.samples,wins=excluded.wins,win_rate=excluded.win_rate,
              global_win_rate=excluded.global_win_rate,edge=excluded.edge,weight=excluded.weight,
              validated=excluded.validated,updated_ts=excluded.updated_ts
        """, (key,b["family"],b["value"],n,wins,wr,global_wr,edge,weight,is_valid,now_iso()))
    c.commit(); c.close()
    return {"resolved_samples": len(rows), "validated_patterns": validated, "global_win_rate": global_wr}


def discovery_adjustment(r: Dict[str, Any]) -> Dict[str, Any]:
    """Apply only validated patterns. Positive and negative evidence can both change confidence."""
    pats = candidate_patterns(r)
    c = conn()
    rows = c.execute("SELECT * FROM discovered_patterns WHERE validated=1").fetchall()
    c.close()
    by_key = {x["pattern_key"]: dict(x) for x in rows}
    matches = []
    raw = 0.0
    for family, value in pats.items():
        key = f"{family}={value}"
        if key in by_key:
            p = by_key[key]
            raw += float(p["weight"])
            matches.append({"pattern": key, "samples": p["samples"], "win_rate": p["win_rate"], "weight": p["weight"]})
    # Multiple correlated patterns should not swing the probability wildly.
    adjustment = clamp(raw * 0.35, -0.15, 0.15)
    return {"adjustment": adjustment, "matches": matches, "candidate_patterns": pats}

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



def bootstrap_evidence_confidence(r: Dict[str, Any]) -> Dict[str, Any]:
    """
    Provisional confidence before enough labeled history exists.
    It is NOT treated as learned probability; it is a bounded evidence estimate
    so the bot can begin collecting executed demo trades.
    """
    f = r.get("features", {})
    filters = r.get("filters", {})
    sig = r.get("signal")
    score = 0.50
    detail = []

    def add(name, delta):
        nonlocal score
        score += delta
        detail.append({"factor": name, "delta": round(delta, 4)})

    # Trend/context evidence
    gap = abs(float(f.get("m15_gap_atr", 0) or 0))
    slope = abs(float(f.get("m15_slope_atr", 0) or 0))
    if filters.get("m15_context"):
        add("m15_context", 0.045)
    if gap >= 0.55:
        add("m15_gap_strength", 0.025)
    if slope >= 0.25:
        add("m15_slope_strength", 0.025)

    # Structure and confirmations
    if filters.get("m5_structure"):
        add("m5_structure", 0.055)
    if filters.get("second_pullback"):
        add("second_pullback", 0.060)
    elif int(f.get("pullbacks", 0) or 0) >= 1:
        add("first_pullback", 0.020)
    if filters.get("m1_confirmation"):
        add("m1_confirmation", 0.055)

    # Momentum alignment
    m5 = float(f.get("m5_momentum", 0) or 0)
    m1 = float(f.get("m1_momentum", 0) or 0)
    aligned = (sig == "BUY" and m5 > 0 and m1 > 0) or (sig == "SELL" and m5 < 0 and m1 < 0)
    if aligned:
        add("momentum_alignment", 0.045)
    elif m5 * m1 < 0:
        add("momentum_conflict", -0.035)

    # R:R
    rr = float(f.get("rr_raw", 0) or 0)
    if rr >= 2.0:
        add("rr_2_plus", 0.045)
    elif rr >= 1.5:
        add("rr_1_5_plus", 0.020)

    # Extension and volatility
    ext = float(f.get("extension_atr", 0) or 0)
    if ext <= 0.85:
        add("fresh_entry", 0.025)
    elif ext > 1.35:
        add("extended_price", -0.040)

    vol = float(f.get("volatility_ratio", 0) or 0)
    if 0.75 <= vol <= 1.35:
        add("normal_volatility", 0.020)
    elif vol > 1.8:
        add("high_volatility", -0.030)

    # Session is evidence, never a veto
    if filters.get("ny_session"):
        add("ny_session", 0.025)

    # News
    align = r.get("alignment", "N/A")
    if align == "CONFIRMA":
        add("news_confirm", 0.055)
    elif align == "CONTRADICE":
        add("news_contradict", -0.070)

    # Technical score contributes modestly; it is not itself a probability.
    tech = float(r.get("technical", r.get("score", 0)) or 0)
    if tech >= 70:
        add("technical_70_plus", 0.040)
    elif tech >= 50:
        add("technical_50_plus", 0.020)
    elif tech < 25:
        add("technical_very_low", -0.025)

    score = clamp(score, BOOTSTRAP_MIN_CONFIDENCE, BOOTSTRAP_MAX_CONFIDENCE)
    return {"probability": score, "detail": detail}

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
        boot = bootstrap_evidence_confidence(r)
        # As real labels accumulate, blend the provisional evidence estimate with
        # the observed global win rate. This prevents a sudden jump at sample 60.
        if total >= BOOTSTRAP_BLEND_MIN_SAMPLES and total > 0:
            global_rate = wins / total
            alpha = min(0.65, (total - BOOTSTRAP_BLEND_MIN_SAMPLES) / max(1, CONFIDENCE_MIN_SAMPLES - BOOTSTRAP_BLEND_MIN_SAMPLES) * 0.65)
            probability = (1 - alpha) * boot["probability"] + alpha * global_rate
            source = "BOOTSTRAP_EVIDENCE+EMPIRICAL_BLEND"
        else:
            probability = boot["probability"]
            source = "BOOTSTRAP_EVIDENCE"
        return {
            "probability": clamp(probability, BOOTSTRAP_MIN_CONFIDENCE, BOOTSTRAP_MAX_CONFIDENCE),
            "source": source,
            "samples": total,
            "local_samples": local_n,
            "variant": variant,
            "global_win_rate": (wins/total) if total else None,
            "local_win_rate": (local_w/local_n) if local_n else None,
            "lower_bound": None,
            "mature": False,
            "bootstrap_detail": boot["detail"],
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

    discovery = discovery_adjustment(r)
    p += float(discovery["adjustment"])
    structure_adj = structural_confidence_adjustment(r.get("structure_context", {}))
    p += float(structure_adj["adjustment"])
    if discovery["matches"]:
        source += "+DISCOVERY"

    # ML remains secondary and only refines after enough labeled history.
    if emp["mature"] and mlp is not None:
        p = 0.80 * p + 0.20 * float(mlp)
        source += "+ML"

    perf = recent_performance()
    penalty = float(perf["penalty"])
    p = max(0.05, p - penalty * 0.5)
    required = min(0.90, EXECUTION_MIN_CONFIDENCE + penalty)

    return {
        **emp,
        "probability": min(0.97, max(0.03, p)),
        "source": source,
        "recent_samples": perf["samples"],
        "recent_win_rate": perf["win_rate"],
        "performance_penalty": penalty,
        "required_confidence": required,
        "discovery_adjustment": discovery["adjustment"],
        "validated_pattern_matches": discovery["matches"],
        "structure_adjustment": structure_adj["adjustment"],
        "structure_classification": structure_adj["classification"],
        "structure_reason": structure_adj["reason"],
        "candidate_patterns": discovery["candidate_patterns"],
    }



def adaptive_stop_price(side: str, entry: float, initial_stop: float, current_price: float,
                        policy: str = "BE_PROFIT_TRAIL") -> Dict[str, Any]:
    """Calculate a tighter stop proposal in R multiples; never widen initial risk."""
    risk = abs(entry - initial_stop)
    if risk <= 0:
        return {"action": "NONE", "new_stop": initial_stop, "r_multiple": 0.0, "reason": "invalid_initial_risk"}

    signed_move = (current_price - entry) if side == "BUY" else (entry - current_price)
    r_mult = signed_move / risk
    new_stop, action = initial_stop, "NONE"

    def locked_level(lock_r):
        return entry + lock_r * risk if side == "BUY" else entry - lock_r * risk

    # Small epsilon avoids floating-point boundary misses exactly at 1R/1.5R/2R.
    eps = 1e-9
    if r_mult + eps >= BREAK_EVEN_TRIGGER_R:
        new_stop, action = locked_level(BREAK_EVEN_LOCK_R), "BREAK_EVEN"

    if policy in ("BE_PROFIT_LOCK", "BE_PROFIT_TRAIL") and r_mult + eps >= PROFIT_LOCK_TRIGGER_R:
        new_stop, action = locked_level(PROFIT_LOCK_R), "PROFIT_LOCK"

    if policy == "BE_PROFIT_TRAIL" and r_mult + eps >= TRAIL_TRIGGER_R:
        trailing = current_price - TRAIL_DISTANCE_R * risk if side == "BUY" else current_price + TRAIL_DISTANCE_R * risk
        new_stop = max(new_stop, trailing) if side == "BUY" else min(new_stop, trailing)
        action = "TRAIL"

    new_stop = max(initial_stop, new_stop) if side == "BUY" else min(initial_stop, new_stop)
    return {"action": action, "new_stop": new_stop, "r_multiple": r_mult, "reason": f"{action} at {r_mult:.2f}R"}


def exit_policy_for_setup(setup_variant: str) -> Dict[str, Any]:
    """Promote learned exit policy only after >=100 resolved observations."""
    default = {"policy": "BE_PROFIT_TRAIL", "source": "DEFAULT_CONSERVATIVE", "samples": 0}
    try:
        c = conn()
        c.execute("""CREATE TABLE IF NOT EXISTS exit_policy_stats (
            setup_variant TEXT NOT NULL,
            policy TEXT NOT NULL,
            samples INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            avg_r REAL NOT NULL DEFAULT 0,
            updated_ts TEXT,
            PRIMARY KEY(setup_variant, policy)
        )""")
        rows = c.execute(
            "SELECT policy,samples,wins,avg_r FROM exit_policy_stats WHERE setup_variant=? AND samples>=?",
            (setup_variant, EXIT_POLICY_MIN_SAMPLES)
        ).fetchall()
        c.commit(); c.close()
        if not rows:
            return default
        best = max(rows, key=lambda x: (float(x[3]), float(x[2]) / max(1, int(x[1]))))
        return {"policy": best[0], "source": "LEARNED_EXIT_POLICY",
                "samples": int(best[1]), "avg_r": float(best[3])}
    except Exception:
        return default


def trend_runner_score(r: Dict[str, Any]) -> float:
    f = r.get("features", {})
    sig = r.get("signal")
    score = 0.0
    slope = abs(float(f.get("m15_slope_atr", 0) or 0))
    gap = abs(float(f.get("m15_gap_atr", 0) or 0))
    m5 = float(f.get("m5_momentum", 0) or 0)
    m1 = float(f.get("m1_momentum", 0) or 0)
    vol = float(f.get("volatility_ratio", 0) or 0)
    ext = float(f.get("extension_atr", 0) or 0)
    filters = r.get("filters", {})
    if slope >= 0.30: score += 0.20
    elif slope >= 0.18: score += 0.10
    if gap >= 0.55: score += 0.10
    aligned = (sig == "BUY" and m5 > 0 and m1 > 0) or (sig == "SELL" and m5 < 0 and m1 < 0)
    if aligned: score += 0.25
    if filters.get("m5_structure"): score += 0.15
    if filters.get("m1_confirmation"): score += 0.10
    if 0.80 <= vol <= 1.50: score += 0.10
    if ext <= 1.20: score += 0.05
    if r.get("alignment") == "CONFIRMA": score += 0.10
    if r.get("alignment") == "CONTRADICE": score -= 0.20
    return clamp(score, 0.0, 1.0)

def desired_target_for_trade(r: Dict[str, Any]) -> Dict[str, Any]:
    entry,stop=float(r["entry"]),float(r["stop"])
    risk=abs(entry-stop)
    base_target=float(r["target"])
    tscore=trend_runner_score(r)
    if not TREND_RUNNER_ENABLED or tscore<TREND_RUNNER_MIN_SCORE or risk<=0:
        return {"target":base_target,"runner":False,"trend_score":tscore}

    desired=entry+TREND_RUNNER_TP_R*risk if r["signal"]=="BUY" else entry-TREND_RUNNER_TP_R*risk
    barrier=r.get("structural_barrier")
    if barrier is not None and r.get("barrier_class")=="STRONG":
        buffer=risk*STRUCTURAL_BARRIER_BUFFER_R
        cap=float(barrier)-buffer if r["signal"]=="BUY" else float(barrier)+buffer
        desired=min(desired,cap) if r["signal"]=="BUY" else max(desired,cap)

    extends=desired>base_target+1e-9 if r["signal"]=="BUY" else desired<base_target-1e-9
    return {"target":desired if extends else base_target,"runner":bool(extends),"trend_score":tscore}


def quality_entry_gate(r: Dict[str, Any], conf: Dict[str, Any]) -> Dict[str, Any]:
    rr = float(r.get("rr_raw",0) or 0)
    if rr < 1.5 and r.get("barrier_class")=="STRONG":
        return {"ok":False,"reason":f"barrera fuerte deja solo {rr:.2f}R < 1.50R"}

    # M1 is the execution trigger, not the directional thesis. We still record the
    # BUY/SELL hypothesis for learning, but do not put money behind it until M1 agrees.
    if M1_CONFIRMATION_REQUIRED and not bool((r.get("filters") or {}).get("m1_confirmation")):
        return {"ok":False,"reason":"falta confirmación M1; hipótesis registrada, entrada aplazada"}

    if not ENTRY_TIMING_ENABLED:
        return {"ok":True,"reason":"quality_ok"}

    f=r.get("features") or {}
    ext=float(f.get("extension_atr",0) or 0)
    p=float(conf.get("probability") or 0)

    if ext > MAX_ENTRY_EXTENSION_ATR:
        return {"ok":False,"reason":f"entrada tardía/chasing: {ext:.2f} ATR > {MAX_ENTRY_EXTENSION_ATR:.2f}"}

    if ext > 0.90 and p < 0.72:
        return {"ok":False,"reason":f"extensión {ext:.2f} ATR con confianza {p:.1%}; esperar mejor precio"}

    return {"ok":True,"reason":"quality_context_ok"}


def reentry_guard(r: Dict[str, Any]) -> Dict[str, Any]:
    """Avoid immediate duplicate/revenge-style re-entry, but never waits a fixed number of minutes."""
    if not REENTRY_REQUIRE_NEW_CANDLE:
        return {"ok": True, "reason": "disabled"}
    candle = r.get("candle_ts")
    if not candle:
        return {"ok": True, "reason": "no_candle"}
    c = conn()
    row = c.execute(
        """SELECT candle_ts, signal, executed FROM decision_log
           WHERE instrument=? ORDER BY id DESC LIMIT 1""",
        (r["instrument"],)
    ).fetchone()
    c.close()
    if row and row["executed"] and row["candle_ts"] == candle:
        return {"ok": False, "reason": "misma vela que la operación anterior; esperar una señal nueva"}
    return {"ok": True, "reason": "new_opportunity"}

def execution_decision(r: Dict[str, Any], conf: Dict[str, Any]) -> Dict[str, Any]:
    if r["signal"] == "WAIT":
        return {"execute": False, "reason": "WAIT: no hay señal direccional"}
    if r.get("blocked"):
        failed = [k for k,v in r.get("safety_checks", {}).items() if not v]
        return {"execute": False, "reason": "Safety veto: " + ", ".join(failed)}
    q = quality_entry_gate(r, conf)
    if not q["ok"]:
        return {"execute": False, "reason": "Quality veto: " + q["reason"]}

    research_gate=evaluate_active_research_rules(r)
    if not research_gate["ok"]:
        keys="; ".join(f"{x['source']}:{x['rule_key']}" for x in research_gate.get("vetoes",[])[:5])
        return {"execute":False,"reason":f"Research veto(s): {keys}"}

    rg = reentry_guard(r)
    if not rg["ok"]:
        return {"execute": False, "reason": "Re-entry veto: " + rg["reason"]}
    p = float(conf.get("probability") or 0)
    required = float(conf.get("required_confidence") or EXECUTION_MIN_CONFIDENCE)
    phase = "learned" if conf.get("mature") else "bootstrap"
    if p >= required:
        return {"execute": True, "reason": f"Adaptive gate ({phase}): confianza {p:.1%} >= {required:.1%}; RR={float(r.get('rr_raw',0)):.2f}"}
    return {"execute": False, "reason": f"Adaptive gate ({phase}): confianza {p:.1%} < {required:.1%}"}


def save_decision(r: Dict[str, Any], conf: Dict[str, Any], executed: int, reason: str):
    c = conn()
    if DEDUP_SIGNAL_SNAPSHOTS and not executed:
        prev = c.execute("""SELECT id FROM decision_log WHERE instrument=? AND candle_ts=? AND signal=? AND reason=?
                            ORDER BY id DESC LIMIT 1""",
                         (r["instrument"], r.get("candle_ts"), r["signal"], reason)).fetchone()
        if prev:
            c.close()
            return
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
        "takeProfitOnFill": {"price": f"{r.get('managed_target', r['target']):.{d}f}", "timeInForce": "GTC"}
    }}
    return await req(client, "POST", "/v3/accounts/{account}/orders", body=body)


def save_signal(r: Dict[str, Any], executed: int, order_id: str, ml_probability: Optional[float], conf: Dict[str, Any], decision_reason: str) -> int:
    c = conn()
    # One learning observation per market opportunity snapshot. The scanner may run
    # more than once against the same M1 candle; those repeats must not inflate the
    # empirical sample count or dominate the training set.
    if DEDUP_SIGNAL_SNAPSHOTS and not executed and r.get("candle_ts"):
        prev = c.execute("""SELECT id FROM signals WHERE instrument=? AND candle_ts=? AND signal=?
                            ORDER BY id DESC LIMIT 1""",
                         (r["instrument"], r.get("candle_ts"), r["signal"])).fetchone()
        if prev:
            signal_id = int(prev["id"])
            c.close()
            return signal_id
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
    create_shadow_trials(signal_id, r)
    return signal_id




def record_external_observation(instrument: str, source_type: str, source_key: str,
                                value_num=None, value_text=None, metadata=None, candle_ts=None):
    """Research-only ingestion. It cannot place or modify OANDA orders."""
    if not EXTERNAL_RESEARCH_ENABLED: return
    c=conn()
    c.execute("""INSERT INTO external_research_observations(
      ts,candle_ts,instrument,source_type,source_key,value_num,value_text,metadata_json)
      VALUES(?,?,?,?,?,?,?,?)""",
      (now_iso(),candle_ts,instrument,source_type,source_key,value_num,value_text,
       json.dumps(metadata or {},separators=(",",":"))))
    c.commit(); c.close()



def _external_momentum_snapshot(candles_: List[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    if len(candles_) < 14:
        return None
    closes=[float(x["c"]) for x in candles_]
    atr_v=atr(candles_)
    if atr_v <= 0:
        return None
    ret3=(closes[-1]-closes[-4])/atr_v
    ret8=(closes[-1]-closes[-9])/atr_v
    fast=ema(closes,5)[-1]
    slow=ema(closes,13)[-1]
    trend=(fast-slow)/atr_v
    composite=(ret3*0.40)+(ret8*0.35)+(trend*0.25)
    return {
        "ret3_atr":float(ret3),
        "ret8_atr":float(ret8),
        "trend_atr":float(trend),
        "composite":float(composite),
        "strength":float((abs(ret3)+abs(ret8)+abs(trend))/3.0),
    }


async def collect_cross_asset_research(client: httpx.AsyncClient, target_instrument: str,
                                       candle_ts: Optional[str]) -> Dict[str, Any]:
    """Fetch external pairs automatically. Research only; no execution effect."""
    if not EXTERNAL_RESEARCH_ENABLED:
        return {"enabled":False,"collected":0,"errors":[]}
    symbols=[x for x in EXTERNAL_RESEARCH_SYMBOLS if x != target_instrument]
    if not symbols:
        return {"enabled":True,"collected":0,"errors":[]}

    async def one(sym: str):
        try:
            cs=await candles(client,sym,EXTERNAL_RESEARCH_GRANULARITY,EXTERNAL_RESEARCH_CANDLE_COUNT)
            return sym,_external_momentum_snapshot(cs),None
        except Exception as e:
            return sym,None,str(e)

    results=await asyncio.gather(*(one(sym) for sym in symbols))
    collected=0; errors=[]
    for sym,snap,err in results:
        if err:
            errors.append({"symbol":sym,"error":err})
            continue
        if not snap:
            continue
        record_external_observation(
            instrument=target_instrument,
            source_type="CROSS_ASSET",
            source_key=sym,
            value_num=snap["composite"],
            value_text="UP" if snap["composite"]>0 else "DOWN" if snap["composite"]<0 else "FLAT",
            metadata={
                "granularity":EXTERNAL_RESEARCH_GRANULARITY,
                "ret3_atr":snap["ret3_atr"],
                "ret8_atr":snap["ret8_atr"],
                "trend_atr":snap["trend_atr"],
                "strength":snap["strength"],
            },
            candle_ts=candle_ts
        )
        collected += 1
    return {"enabled":True,"collected":collected,"errors":errors,"symbols":symbols}


def record_news_research(r: Dict[str, Any]) -> None:
    """Persist the GDELT news context for later hypothesis testing."""
    if not EXTERNAL_RESEARCH_ENABLED or not EXTERNAL_NEWS_RESEARCH:
        return
    bias=str(r.get("news_bias") or "UNKNOWN").upper()
    align=str(r.get("alignment") or "UNKNOWN").upper()
    val=1.0 if bias=="BULLISH" else -1.0 if bias=="BEARISH" else 0.0
    record_external_observation(
        instrument=r["instrument"],
        source_type="NEWS",
        source_key=f"{r['instrument']}_GDELT_180M",
        value_num=val,
        value_text=f"{bias}|{align}",
        metadata={
            "article_count":len(r.get("news_articles") or []),
            "titles":[x.get("title","") for x in (r.get("news_articles") or [])[:5]],
        },
        candle_ts=r.get("candle_ts")
    )


def external_research_candidates(signal_row, observations):
    out={}; direction=signal_row.get("signal")
    for o in observations:
        typ=str(o.get("source_type","")).upper()
        key=str(o.get("source_key","")).upper()
        val=o.get("value_num"); txt=str(o.get("value_text") or "").upper()
        if typ in ("CROSS_ASSET","ASSET_MOMENTUM") and val is not None:
            v=float(val)
            same=(direction=="BUY" and v>0) or (direction=="SELL" and v<0)
            inverse=(direction=="BUY" and v<0) or (direction=="SELL" and v>0)
            strong=abs(v)>=EXTERNAL_RESEARCH_MIN_MOVE_ATR
            out[f"cross_asset::{key}::same_direction"]={
                "family":"CROSS_ASSET","aligned":bool(same and strong),
                "description":f"{key} moves in the SAME direction as {signal_row.get('instrument')}"}
            out[f"cross_asset::{key}::inverse_direction"]={
                "family":"CROSS_ASSET","aligned":bool(inverse and strong),
                "description":f"{key} moves INVERSE to {signal_row.get('instrument')}"}
            out[f"cross_asset::{key}::strong_move"]={
                "family":"CROSS_ASSET","aligned":bool(strong),
                "description":f"{key} has a move >= {EXTERNAL_RESEARCH_MIN_MOVE_ATR:.2f} ATR"}
        elif typ in ("RISK_REGIME","VOLATILITY_REGIME") and val is not None:
            out[f"regime::{key}::normal"]={"family":"MARKET_REGIME","aligned":abs(float(val))<=1.0,
                "description":f"{key} inside normal research regime"}
        elif typ in ("NEWS","MACRO_EVENT"):
            neutral=("NEUTRAL" in txt or "NONE" in txt or not txt)
            out[f"news::{key}::low_conflict"]={"family":"NEWS_MACRO",
                "aligned":neutral and abs(float(val or 0))<1.0,
                "description":f"{key} low directional conflict around signal"}
    return out


def refresh_external_hypotheses():
    """
    Evaluate external hypotheses using BOTH:
      1) canonical resolved opportunities (executed or rejected/paper-followed), and
      2) resolved shadow simulations produced by the simulation engine.

    Shadow outcomes are down-weighted so one market opportunity cannot dominate the
    evidence simply because several stop/target variants were simulated.
    """
    if not EXTERNAL_RESEARCH_ENABLED:
        return {"enabled":False}

    c=conn()

    canonical_rows=c.execute("""SELECT ls.label,s.id signal_id,s.candle_ts,s.instrument,s.signal,
                                      s.executed,'CANONICAL' evidence_source,NULL shadow_variant
                               FROM learning_samples ls
                               JOIN signals s ON s.id=ls.signal_id
                               WHERE ls.label IN (0,1)
                               ORDER BY s.id""").fetchall()

    shadow_rows=[]
    if EXTERNAL_INCLUDE_SHADOW:
        shadow_rows=c.execute("""SELECT st.label,s.id signal_id,s.candle_ts,s.instrument,s.signal,
                                       s.executed,'SHADOW' evidence_source,st.variant shadow_variant
                                FROM shadow_trials st
                                JOIN signals s ON s.id=st.signal_id
                                WHERE st.label IN (0,1)
                                ORDER BY st.id""").fetchall()

    rows=list(canonical_rows)+list(shadow_rows)
    stats={}

    for row in rows:
        obs=c.execute("""SELECT * FROM external_research_observations
                         WHERE instrument=? AND candle_ts=?""",
                      (row["instrument"],row["candle_ts"])).fetchall()
        if not obs:
            continue

        source=row["evidence_source"]
        variant=row["shadow_variant"]
        if source=="CANONICAL":
            weight=1.0
        elif variant=="BASELINE":
            weight=EXTERNAL_SHADOW_BASELINE_WEIGHT
        else:
            weight=EXTERNAL_SHADOW_VARIANT_WEIGHT

        candidates=external_research_candidates(dict(row),[dict(x) for x in obs])
        for key,h in candidates.items():
            x=stats.setdefault(key,{
                "description":h["description"],"family":h["family"],
                "an":0,"aw":0,"cn":0,"cw":0,
                "anw":0.0,"aww":0.0,"cnw":0.0,"cww":0.0,
                "canonical":0,"shadow":0,"shadow_weighted_wins":0.0
            })

            if source=="CANONICAL":
                x["canonical"]+=1
            else:
                x["shadow"]+=1
                x["shadow_weighted_wins"] += float(row["label"])*weight

            if h["aligned"]:
                x["an"]+=1
                x["aw"]+=int(row["label"])
                x["anw"]+=weight
                x["aww"]+=float(row["label"])*weight
            else:
                x["cn"]+=1
                x["cw"]+=int(row["label"])
                x["cnw"]+=weight
                x["cww"]+=float(row["label"])*weight

    stages={"EXPERIMENTAL":0,"EVALUATING":0,"VALIDATED":0,"REJECTED":0}

    for key,x in stats.items():
        raw_total=x["canonical"]+x["shadow"]
        effective=x["anw"]+x["cnw"]
        awr=x["aww"]/x["anw"] if x["anw"] else None
        cwr=x["cww"]/x["cnw"] if x["cnw"] else None
        edge=(awr-cwr) if awr is not None and cwr is not None else None

        # Research can advance using simulations, but live promotion later also
        # requires a minimum amount of canonical evidence.
        if effective < EXTERNAL_RESEARCH_MIN_SAMPLES:
            stage="EXPERIMENTAL"
        elif effective < EXTERNAL_RESEARCH_VALIDATE_SAMPLES:
            stage="EVALUATING"
        elif x["anw"]>=20 and x["cnw"]>=20 and edge is not None and edge>=EXTERNAL_RESEARCH_MIN_EDGE:
            stage="VALIDATED"
        elif x["anw"]>=20 and x["cnw"]>=20 and edge is not None and edge<=0.02:
            stage="REJECTED"
        else:
            stage="EVALUATING"

        stages[stage]+=1
        rec="CANDIDATE_FOR_FUTURE_VERSION" if stage=="VALIDATED" else ("DO_NOT_USE" if stage=="REJECTED" else "KEEP_TESTING")

        c.execute("""INSERT INTO external_hypotheses(
          hypothesis_key,description,family,stage,total_samples,aligned_samples,aligned_wins,aligned_win_rate,
          control_samples,control_wins,control_win_rate,edge,recommendation,automatic_live_activation,updated_ts,
          canonical_samples,shadow_samples,effective_samples,shadow_weighted_wins)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(hypothesis_key) DO UPDATE SET
          description=excluded.description,family=excluded.family,stage=excluded.stage,
          total_samples=excluded.total_samples,aligned_samples=excluded.aligned_samples,
          aligned_wins=excluded.aligned_wins,aligned_win_rate=excluded.aligned_win_rate,
          control_samples=excluded.control_samples,control_wins=excluded.control_wins,
          control_win_rate=excluded.control_win_rate,edge=excluded.edge,
          recommendation=excluded.recommendation,automatic_live_activation=0,updated_ts=excluded.updated_ts,
          canonical_samples=excluded.canonical_samples,shadow_samples=excluded.shadow_samples,
          effective_samples=excluded.effective_samples,shadow_weighted_wins=excluded.shadow_weighted_wins""",
          (key,x["description"],x["family"],stage,raw_total,x["an"],x["aw"],awr,
           x["cn"],x["cw"],cwr,edge,rec,0,now_iso(),
           x["canonical"],x["shadow"],effective,x["shadow_weighted_wins"]))

        if stage=="VALIDATED":
            evidence={
                "raw_samples":raw_total,
                "effective_samples":effective,
                "canonical_samples":x["canonical"],
                "shadow_samples":x["shadow"],
                "edge":edge,
                "aligned_win_rate":awr,
                "control_win_rate":cwr,
                "shadow_weights":{
                    "baseline":EXTERNAL_SHADOW_BASELINE_WEIGHT,
                    "variant":EXTERNAL_SHADOW_VARIANT_WEIGHT
                }
            }
            c.execute("""INSERT OR IGNORE INTO research_knowledge(ts,hypothesis_key,finding,evidence_json)
                         VALUES(?,?,?,?)""",
                      (now_iso(),key,"Validated research candidate: "+x["description"],
                       json.dumps(evidence,separators=(",",":"))))

    c.commit()
    c.close()

    return {
        "enabled":True,
        "canonical_resolved":len(canonical_rows),
        "shadow_resolved":len(shadow_rows),
        "shadow_included":EXTERNAL_INCLUDE_SHADOW,
        "shadow_baseline_weight":EXTERNAL_SHADOW_BASELINE_WEIGHT,
        "shadow_variant_weight":EXTERNAL_SHADOW_VARIANT_WEIGHT,
        **{k.lower():v for k,v in stages.items()},
        "automatic_live_activation":False
    }




AUTONOMOUS_NUMERIC_FEATURES = (
    "technical_score","final_score","m15_gap_atr","m15_slope_atr","m5_momentum",
    "pullbacks","m1_momentum","extension_atr","volatility_ratio","rr_raw",
    "room_to_barrier_r","barrier_score","broken_barriers","hour_ny","buy_score",
    "sell_score","direction_edge","h1_gap_atr","h1_slope_atr","transition_state"
)

def _auto_quantile(values, q):
    vals=sorted(float(x) for x in values if x is not None and math.isfinite(float(x)))
    if not vals:return None
    if len(vals)==1:return vals[0]
    pos=(len(vals)-1)*q; lo=int(math.floor(pos)); hi=int(math.ceil(pos))
    if lo==hi:return vals[lo]
    f=pos-lo
    return vals[lo]*(1-f)+vals[hi]*f

def _auto_variance(vals):
    if len(vals)<2:return 0.0
    m=sum(vals)/len(vals)
    return sum((x-m)*(x-m) for x in vals)/len(vals)

def _auto_dataset():
    c=conn()
    canonical=c.execute("""SELECT ls.label,s.candle_ts,s.instrument,s.signal,s.features_json,
                                  'CANONICAL' source,NULL variant
                           FROM learning_samples ls JOIN signals s ON s.id=ls.signal_id
                           WHERE ls.label IN (0,1) ORDER BY s.id""").fetchall()
    shadow=c.execute("""SELECT st.label,s.candle_ts,s.instrument,s.signal,s.features_json,
                               'SHADOW' source,st.variant variant
                        FROM shadow_trials st JOIN signals s ON s.id=st.signal_id
                        WHERE st.label IN (0,1) ORDER BY st.id""").fetchall()
    c.close()
    out=[]
    for row in list(canonical)+list(shadow):
        try:f=json.loads(row["features_json"] or "{}")
        except Exception:f={}
        out.append({"label":int(row["label"]),"features":f,"signal":row["signal"],
                    "instrument":row["instrument"],"candle_ts":row["candle_ts"],
                    "source":row["source"],"variant":row["variant"],
                    "weight":1.0 if row["source"]=="CANONICAL" else AUTONOMOUS_SHADOW_WEIGHT})
    return out

def _auto_feature_names(rows):
    ranked=[]
    for name in AUTONOMOUS_NUMERIC_FEATURES:
        vals=[]
        for r in rows:
            try:
                v=float(r["features"].get(name))
                if math.isfinite(v):vals.append(v)
            except Exception:pass
        if len(vals)>=max(20,int(len(rows)*.45)) and len(set(round(v,10) for v in vals))>=4:
            ranked.append((_auto_variance(vals),name))
    ranked.sort(reverse=True)
    return [n for _,n in ranked[:AUTONOMOUS_DISCOVERY_MAX_FEATURES]]

def _auto_rule_passes(rule,r):
    conditions=rule.get("conditions") or []
    if not conditions:return None
    f=r.get("features") or {}
    for c in conditions:
        try:
            x=float(f.get(c["feature"])); t=float(c["value"])
        except Exception:return None
        if not math.isfinite(x) or not math.isfinite(t):return None
        if c["op"]=="<=" and not x<=t:return False
        if c["op"]==">=" and not x>=t:return False
    return True

def evaluate_autonomous_rule(rule_key,r):
    c=conn(); row=c.execute("SELECT rule_json FROM autonomous_hypotheses WHERE hypothesis_key=?",(rule_key,)).fetchone(); c.close()
    if not row:return None
    try:rule=json.loads(row["rule_json"])
    except Exception:return None
    return _auto_rule_passes(rule,r)

def _auto_eval(rule,rows):
    p=[];f=[];cp=sp=0
    for row in rows:
        ok=_auto_rule_passes(rule,{"features":row["features"]})
        if ok is None:continue
        if ok:
            p.append(row); cp+=int(row["source"]=="CANONICAL"); sp+=int(row["source"]=="SHADOW")
        else:f.append(row)
    def cnt(x):return sum(float(r["weight"]) for r in x)
    def wr(x):
        den=cnt(x)
        return sum(float(r["label"])*float(r["weight"]) for r in x)/den if den else None
    pn,fn=cnt(p),cnt(f); pwr,fwr=wr(p),wr(f)
    total=pn+fn
    return {"pn":pn,"fn":fn,"pwr":pwr,"fwr":fwr,
            "edge":(pwr-fwr) if pwr is not None and fwr is not None else None,
            "coverage":pn/total if total else 0.0,"total":total,
            "canonical_pass":cp,"shadow_pass":sp}

def _auto_key(rule):
    return "auto::"+"&".join(f"{c['feature']}{c['op']}{float(c['value']):.8g}" for c in rule["conditions"])

def _auto_desc(rule):
    return " AND ".join(f"{c['feature']} {c['op']} {float(c['value']):.4g}" for c in rule["conditions"])

def refresh_research_family_stats():
    c=conn(); rows=c.execute("SELECT family,stage,COALESCE(edge,0) edge FROM autonomous_hypotheses").fetchall()
    groups={}
    for r in rows:
        g=groups.setdefault(r["family"],{"tested":0,"validated":0,"rejected":0,"edges":[]})
        g["tested"]+=1;g["validated"]+=int(r["stage"]=="VALIDATED");g["rejected"]+=int(r["stage"]=="REJECTED");g["edges"].append(float(r["edge"] or 0))
    for fam,g in groups.items():
        avg=sum(abs(x) for x in g["edges"])/len(g["edges"]) if g["edges"] else 0
        best=max(g["edges"]) if g["edges"] else 0
        success=(g["validated"]+1)/(g["tested"]+3)
        priority=clamp(.5+1.5*success+2*max(0,best),.5,3.0)
        c.execute("""INSERT INTO research_family_stats(family,hypotheses_tested,validated,rejected,avg_abs_edge,best_edge,priority_score,updated_ts)
                     VALUES(?,?,?,?,?,?,?,?)
                     ON CONFLICT(family) DO UPDATE SET hypotheses_tested=excluded.hypotheses_tested,
                     validated=excluded.validated,rejected=excluded.rejected,avg_abs_edge=excluded.avg_abs_edge,
                     best_edge=excluded.best_edge,priority_score=excluded.priority_score,updated_ts=excluded.updated_ts""",
                  (fam,g["tested"],g["validated"],g["rejected"],avg,best,priority,now_iso()))
    c.commit();c.close()
    return {"families":len(groups)}

def autonomous_discovery_refresh():
    """
    Generates thresholds from observed data and validates them on a later holdout.
    It does not rely on a hand-written threshold list.
    """
    if not AUTONOMOUS_DISCOVERY_ENABLED:return {"enabled":False}
    rows=_auto_dataset()
    if len(rows)<AUTONOMOUS_DISCOVERY_MIN_ROWS:
        return {"enabled":True,"rows":len(rows),"reason":"not_enough_rows"}

    split=max(30,int(len(rows)*(1-AUTONOMOUS_DISCOVERY_HOLDOUT)))
    discovery,validation=rows[:split],rows[split:]
    if len(validation)<30:return {"enabled":True,"rows":len(rows),"reason":"not_enough_holdout"}

    names=_auto_feature_names(discovery)
    singles=[]
    for name in names:
        vals=[]
        for r in discovery:
            try:
                v=float(r["features"].get(name))
                if math.isfinite(v):vals.append(v)
            except Exception:pass
        for q in (.25,.50,.75):
            t=_auto_quantile(vals,q)
            if t is None:continue
            singles.extend([
                {"family":"AUTO_SINGLE","generation":1,"conditions":[{"feature":name,"op":"<=","value":t}]},
                {"family":"AUTO_SINGLE","generation":1,"conditions":[{"feature":name,"op":">=","value":t}]}
            ])

    ranked=[]
    for rule in singles:
        e=_auto_eval(rule,discovery)
        if e["edge"] is not None and AUTONOMOUS_DISCOVERY_MIN_COVERAGE<=e["coverage"]<=AUTONOMOUS_DISCOVERY_MAX_COVERAGE:
            ranked.append((abs(e["edge"])*math.sqrt(max(1,e["total"])),rule))
    ranked.sort(key=lambda x:x[0],reverse=True)

    top=[];used=set()
    for _,rule in ranked:
        feat=rule["conditions"][0]["feature"]
        if feat in used:continue
        used.add(feat);top.append(rule)
        if len(top)>=10:break

    pairs=[]
    for i in range(len(top)):
        for j in range(i+1,len(top)):
            pairs.append({"family":"AUTO_PAIR","generation":2,
                          "conditions":[dict(top[i]["conditions"][0]),dict(top[j]["conditions"][0])]})
            if len(pairs)>=AUTONOMOUS_DISCOVERY_MAX_PAIRWISE:break
        if len(pairs)>=AUTONOMOUS_DISCOVERY_MAX_PAIRWISE:break

    c=conn();saved=0;counts={k:0 for k in ("EXPERIMENTAL","EVALUATING","VALIDATED","REJECTED")}
    for rule in singles+pairs:
        de=_auto_eval(rule,discovery);ve=_auto_eval(rule,validation)
        if ve["edge"] is None or not (AUTONOMOUS_DISCOVERY_MIN_COVERAGE<=ve["coverage"]<=AUTONOMOUS_DISCOVERY_MAX_COVERAGE):continue
        if ve["total"]<RESEARCH_EVAL_MIN_SAMPLES:stage="EXPERIMENTAL"
        elif ve["total"]<AUTO_PROMOTE_MIN_SAMPLES:stage="EVALUATING"
        elif ve["edge"]>=AUTONOMOUS_DISCOVERY_MIN_EDGE and ve["pn"]>=20 and ve["fn"]>=20:stage="VALIDATED"
        elif ve["edge"]<=.02 and ve["pn"]>=20 and ve["fn"]>=20:stage="REJECTED"
        else:stage="EVALUATING"
        key=_auto_key(rule);desc=_auto_desc(rule)
        score=max(0,ve["edge"])*math.sqrt(max(1,ve["total"]))
        rec="AUTO_PROMOTION_CANDIDATE" if stage=="VALIDATED" else "DO_NOT_USE" if stage=="REJECTED" else "KEEP_TESTING"
        c.execute("""INSERT INTO autonomous_hypotheses(
                     hypothesis_key,description,rule_json,family,stage,discovery_samples,validation_samples,
                     canonical_validation_samples,shadow_validation_samples,pass_samples,pass_wins,pass_win_rate,
                     fail_samples,fail_wins,fail_win_rate,edge,coverage,score,recommendation,generation,updated_ts)
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                     ON CONFLICT(hypothesis_key) DO UPDATE SET description=excluded.description,rule_json=excluded.rule_json,
                     family=excluded.family,stage=excluded.stage,discovery_samples=excluded.discovery_samples,
                     validation_samples=excluded.validation_samples,canonical_validation_samples=excluded.canonical_validation_samples,
                     shadow_validation_samples=excluded.shadow_validation_samples,pass_samples=excluded.pass_samples,
                     pass_wins=excluded.pass_wins,pass_win_rate=excluded.pass_win_rate,fail_samples=excluded.fail_samples,
                     fail_wins=excluded.fail_wins,fail_win_rate=excluded.fail_win_rate,edge=excluded.edge,
                     coverage=excluded.coverage,score=excluded.score,recommendation=excluded.recommendation,
                     generation=excluded.generation,updated_ts=excluded.updated_ts""",
                  (key,desc,json.dumps(rule,separators=(",",":")),rule["family"],stage,de["total"],ve["total"],
                   ve["canonical_pass"],ve["shadow_pass"],ve["pn"],(ve["pwr"] or 0)*ve["pn"],ve["pwr"],
                   ve["fn"],(ve["fwr"] or 0)*ve["fn"],ve["fwr"],ve["edge"],ve["coverage"],score,rec,rule["generation"],now_iso()))
        counts[stage]+=1;saved+=1
        if stage=="VALIDATED":
            c.execute("""INSERT OR IGNORE INTO research_knowledge(ts,hypothesis_key,finding,evidence_json)
                         VALUES(?,?,?,?)""",
                      (now_iso(),key,"Autonomous discovery validated: "+desc,
                       json.dumps({"validation_samples":ve["total"],"edge":ve["edge"],"coverage":ve["coverage"],
                                   "canonical_pass":ve["canonical_pass"],"shadow_pass":ve["shadow_pass"],
                                   "holdout":AUTONOMOUS_DISCOVERY_HOLDOUT},separators=(",",":"))))
    c.commit();c.close()
    meta=refresh_research_family_stats()
    return {"enabled":True,"rows":len(rows),"discovery_rows":len(discovery),"validation_rows":len(validation),
            "features_considered":names,"rules_generated":len(singles)+len(pairs),"rules_saved":saved,
            **{k.lower():v for k,v in counts.items()},"meta_learning":meta}



def get_active_research_rules() -> List[Dict[str, Any]]:
    c=conn()
    rows=c.execute("""SELECT * FROM active_research_rules
                      WHERE status IN ('ACTIVE','CONFIRMED')
                      ORDER BY id ASC""").fetchall()
    c.close()
    return [dict(x) for x in rows]

def get_active_research_rule() -> Optional[Dict[str, Any]]:
    rules=get_active_research_rules()
    return rules[0] if rules else None

def _audit_research_rule(action,source,rule_key,details):
    c=conn()
    c.execute("""INSERT INTO research_rule_audit(ts,action,source,rule_key,details_json)
                 VALUES(?,?,?,?,?)""",
              (now_iso(),action,source,rule_key,json.dumps(details or {},separators=(",",":"))))
    c.commit();c.close()

def _rule_last_reverted_evidence(source,rule_key):
    c=conn()
    row=c.execute("""SELECT evidence_samples FROM active_research_rules
                     WHERE source=? AND rule_key=? AND status='REVERTED'
                     ORDER BY id DESC LIMIT 1""",(source,rule_key)).fetchone()
    c.close()
    return int(row["evidence_samples"]) if row else None

def _candidate_rule_definition(source,rule_key):
    if source=="AUTONOMOUS":
        c=conn();row=c.execute("SELECT rule_json FROM autonomous_hypotheses WHERE hypothesis_key=?",(rule_key,)).fetchone();c.close()
        if row:
            try:return json.loads(row["rule_json"])
            except Exception:return None
    if source=="EXTERNAL":
        parts=rule_key.split("::")
        if len(parts)>=3 and parts[0]=="cross_asset":
            return {"external_asset":parts[1],"external_mode":parts[2]}
    return None

def _logical_rules_compatible(source_a,key_a,source_b,key_b):
    if source_a==source_b and key_a==key_b:return False
    a=_candidate_rule_definition(source_a,key_a); b=_candidate_rule_definition(source_b,key_b)
    if a and b and "external_asset" in a and "external_asset" in b:
        if a["external_asset"]==b["external_asset"]:
            modes={a.get("external_mode"),b.get("external_mode")}
            if "same_direction" in modes and "inverse_direction" in modes:return False
    if a and b and a.get("conditions") and b.get("conditions"):
        bounds={}
        for rule in (a,b):
            for cond in rule.get("conditions",[]):
                try:v=float(cond.get("value"))
                except Exception:continue
                z=bounds.setdefault(cond.get("feature"),{"lo":-math.inf,"hi":math.inf})
                if cond.get("op")==">=":z["lo"]=max(z["lo"],v)
                if cond.get("op")=="<=":z["hi"]=min(z["hi"],v)
        if any(z["lo"]>z["hi"] for z in bounds.values()):return False
    return True

def _rule_match_for_dict(source,rule_key,r):
    if source=="INTERNAL":
        cand=experimental_filter_candidates(r).get(rule_key)
        return bool(cand["pass"]) if cand is not None else None
    if source=="AUTONOMOUS":
        return evaluate_autonomous_rule(rule_key,r)
    c=conn()
    obs=c.execute("""SELECT * FROM external_research_observations
                     WHERE instrument=? AND candle_ts=?""",(r.get("instrument"),r.get("candle_ts"))).fetchall()
    c.close()
    cand=external_research_candidates(r,[dict(x) for x in obs]).get(rule_key)
    return bool(cand["aligned"]) if cand is not None else None

def _canonical_rule_rows(limit=1000):
    c=conn()
    rows=c.execute("""SELECT ls.label,s.signal,s.features_json,s.filters_json,s.instrument,s.candle_ts,s.ts
                      FROM learning_samples ls JOIN signals s ON s.id=ls.signal_id
                      WHERE ls.label IN (0,1) ORDER BY s.id DESC LIMIT ?""",(limit,)).fetchall()
    c.close()
    return list(reversed(rows))

def _row_as_rule_context(row):
    return {"signal":row["signal"],"features":json.loads(row["features_json"] or "{}"),
            "filters":json.loads(row["filters_json"] or "{}"),"instrument":row["instrument"],
            "candle_ts":row["candle_ts"]}

def assess_rule_compatibility(candidate,active_rules=None):
    active_rules=active_rules if active_rules is not None else get_active_research_rules()
    if not active_rules or not MULTI_FILTER_COMPAT_ENABLED:
        return {"compatible":True,"reason":"no_active_rules_or_check_disabled","joint_samples":None,
                "joint_win_rate":None,"joint_coverage":None}
    for ar in active_rules:
        if not _logical_rules_compatible(candidate["source"],candidate["rule_key"],ar["source"],ar["rule_key"]):
            return {"compatible":False,"reason":f"logical_conflict_with:{ar['source']}:{ar['rule_key']}",
                    "joint_samples":0,"joint_win_rate":None,"joint_coverage":0.0}

    rows=_canonical_rule_rows(1000)
    candidate_pass=0;candidate_wins=0;joint=[]
    for row in rows:
        ctx=_row_as_rule_context(row)
        cp=_rule_match_for_dict(candidate["source"],candidate["rule_key"],ctx)
        if cp is not True:continue
        candidate_pass+=1;candidate_wins+=int(row["label"])
        all_ok=True
        for ar in active_rules:
            ap=_rule_match_for_dict(ar["source"],ar["rule_key"],ctx)
            if ap is False:
                all_ok=False;break
        if all_ok:joint.append(row)

    if not candidate_pass:
        return {"compatible":False,"reason":"candidate_has_no_historical_passes",
                "joint_samples":0,"joint_win_rate":None,"joint_coverage":0.0}
    jn=len(joint);jwr=sum(int(x["label"]) for x in joint)/jn if jn else None
    cwr=candidate_wins/candidate_pass
    jcov=jn/max(1,len(rows))
    ok=(jn>=MULTI_FILTER_MIN_JOINT_SAMPLES and jcov>=MULTI_FILTER_MIN_JOINT_COVERAGE
        and jwr is not None and jwr>=cwr-MULTI_FILTER_MAX_WR_DROP)
    reason="historically_compatible" if ok else f"weak_joint_history:n={jn},coverage={jcov:.3f}"

    c=conn()
    for ar in active_rules:
        sa,ka=candidate["source"],candidate["rule_key"];sb,kb=ar["source"],ar["rule_key"]
        if (sa,ka)>(sb,kb):sa,ka,sb,kb=sb,kb,sa,ka
        c.execute("""INSERT INTO research_rule_compatibility(
          source_a,rule_a,source_b,rule_b,checked_ts,samples,joint_pass,joint_win_rate,joint_coverage,compatible,reason)
          VALUES(?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(source_a,rule_a,source_b,rule_b) DO UPDATE SET checked_ts=excluded.checked_ts,
          samples=excluded.samples,joint_pass=excluded.joint_pass,joint_win_rate=excluded.joint_win_rate,
          joint_coverage=excluded.joint_coverage,compatible=excluded.compatible,reason=excluded.reason""",
          (sa,ka,sb,kb,now_iso(),len(rows),jn,jwr,jcov,int(ok),reason))
    c.commit();c.close()
    return {"compatible":ok,"reason":reason,"joint_samples":jn,"joint_win_rate":jwr,
            "candidate_win_rate":cwr,"joint_coverage":jcov}

def _validated_promotion_candidates():
    c=conn();out=[]
    for r in c.execute("""SELECT filter_key rule_key,description,total_samples,edge,pass_win_rate FROM filter_hypotheses
                          WHERE stage='VALIDATED' AND total_samples>=? AND edge>=?""",
                       (AUTO_PROMOTE_MIN_SAMPLES,AUTO_PROMOTE_MIN_EDGE)).fetchall():
        out.append({"source":"INTERNAL","rule_key":r["rule_key"],"description":r["description"],
                    "samples":float(r["total_samples"]),"effective_samples":float(r["total_samples"]),
                    "edge":float(r["edge"] or 0),"baseline":float(r["pass_win_rate"]) if r["pass_win_rate"] is not None else None})
    for r in c.execute("""SELECT hypothesis_key rule_key,description,effective_samples,canonical_samples,shadow_samples,
                                 edge,aligned_win_rate FROM external_hypotheses
                          WHERE stage='VALIDATED' AND effective_samples>=? AND canonical_samples>=? AND edge>=?""",
                       (AUTO_PROMOTE_MIN_SAMPLES,EXTERNAL_PROMOTION_MIN_CANONICAL,AUTO_PROMOTE_MIN_EDGE)).fetchall():
        out.append({"source":"EXTERNAL","rule_key":r["rule_key"],"description":r["description"],
                    "samples":float(r["effective_samples"] or 0),"effective_samples":float(r["effective_samples"] or 0),
                    "canonical_samples":int(r["canonical_samples"] or 0),"shadow_samples":int(r["shadow_samples"] or 0),
                    "edge":float(r["edge"] or 0),"baseline":float(r["aligned_win_rate"]) if r["aligned_win_rate"] is not None else None})
    for r in c.execute("""SELECT hypothesis_key rule_key,description,validation_samples,canonical_validation_samples,
                                 shadow_validation_samples,edge,pass_win_rate,score FROM autonomous_hypotheses
                          WHERE stage='VALIDATED' AND validation_samples>=? AND canonical_validation_samples>=? AND edge>=?""",
                       (AUTO_PROMOTE_MIN_SAMPLES,AUTONOMOUS_PROMOTION_MIN_CANONICAL,AUTO_PROMOTE_MIN_EDGE)).fetchall():
        out.append({"source":"AUTONOMOUS","rule_key":r["rule_key"],"description":r["description"],
                    "samples":float(r["validation_samples"] or 0),"effective_samples":float(r["validation_samples"] or 0),
                    "canonical_samples":int(r["canonical_validation_samples"] or 0),
                    "shadow_samples":int(r["shadow_validation_samples"] or 0),"edge":float(r["edge"] or 0),
                    "score":float(r["score"] or 0),"baseline":float(r["pass_win_rate"]) if r["pass_win_rate"] is not None else None})
    c.close();return out

def promote_validated_research_rules():
    if not AUTO_PROMOTE_RESEARCH:return {"promoted":[],"skipped":[],"reason":"disabled"}
    active=get_active_research_rules();active_keys={(x["source"],x["rule_key"]) for x in active}
    eligible=[]
    for x in _validated_promotion_candidates():
        if (x["source"],x["rule_key"]) in active_keys:continue
        prior=_rule_last_reverted_evidence(x["source"],x["rule_key"])
        if prior is not None and x["samples"]<prior+AUTO_PROMOTE_RETRY_NEW_SAMPLES:continue
        eligible.append(x)
    eligible.sort(key=lambda x:(x["edge"]*math.log1p(float(x.get("effective_samples",x["samples"]))),
                                float(x.get("effective_samples",x["samples"]))),reverse=True)
    promoted=[];skipped=[]
    for x in eligible:
        compat=assess_rule_compatibility(x,active)
        if not compat["compatible"]:
            skipped.append({"candidate":x,"compatibility":compat});continue
        c=conn()
        c.execute("""INSERT INTO active_research_rules(
          source,rule_key,description,status,activated_ts,evidence_samples,evidence_edge,baseline_win_rate,
          review_after_samples,post_samples,post_wins,reviewed_matches,reason)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
          (x["source"],x["rule_key"],x["description"],"ACTIVE",now_iso(),x["samples"],x["edge"],x["baseline"],
           AUTO_PROMOTE_REVIEW_SAMPLES,0,0,0,"Auto-promoted; multi-filter compatible"))
        c.commit();c.close()
        _audit_research_rule("PROMOTED",x["source"],x["rule_key"],{**x,"compatibility":compat})
        promoted.append({"rule":x,"compatibility":compat})
        active=get_active_research_rules()
    return {"promoted":promoted,"skipped":skipped,"active_count":len(get_active_research_rules())}

def promote_validated_research_rule():
    x=promote_validated_research_rules()
    return {"promoted":bool(x["promoted"]),"promoted_rules":x["promoted"],"skipped":x["skipped"],"active_count":x["active_count"]}

def evaluate_active_research_rules(r):
    rules=get_active_research_rules()
    if not rules:return {"ok":True,"active":False,"rules":[],"vetoes":[]}
    results=[];vetoes=[]
    for rule in rules:
        passed=_rule_match_for_dict(rule["source"],rule["rule_key"],r)
        item={"source":rule["source"],"rule_key":rule["rule_key"],"status":rule["status"],"passed":passed}
        results.append(item)
        if passed is False:vetoes.append(item)
    return {"ok":not vetoes,"active":True,"rules":results,"vetoes":vetoes}

def evaluate_active_research_rule(r):
    out=evaluate_active_research_rules(r)
    if out["ok"]:return out
    return {"ok":False,"active":True,"reason":"one or more learned filters vetoed","vetoes":out["vetoes"]}

def _signal_passes_rule_for_review(source,rule_key,row):
    return _rule_match_for_dict(source,rule_key,_row_as_rule_context(row))

def _matching_rows_since(rule):
    c=conn()
    rows=c.execute("""SELECT ls.label,s.signal,s.features_json,s.filters_json,s.instrument,s.candle_ts,s.ts
                      FROM learning_samples ls JOIN signals s ON s.id=ls.signal_id
                      WHERE ls.label IN (0,1) AND s.ts>=? ORDER BY s.id""",(rule["activated_ts"],)).fetchall()
    c.close()
    return [r for r in rows if _signal_passes_rule_for_review(rule["source"],rule["rule_key"],r) is True]

def review_one_active_research_rule(rule):
    matched=_matching_rows_since(rule);total=len(matched);reviewed=int(rule.get("reviewed_matches") or 0)
    if total<reviewed+ACTIVE_RULE_HEALTH_BLOCK:
        return {"reviewed":False,"source":rule["source"],"rule_key":rule["rule_key"],
                "status":rule["status"],"matches":total,"next_review_at":reviewed+ACTIVE_RULE_HEALTH_BLOCK}
    block=matched[reviewed:reviewed+ACTIVE_RULE_HEALTH_BLOCK]
    n=len(block);w=sum(int(x["label"]) for x in block);wr=w/n if n else None
    allwr=sum(int(x["label"]) for x in matched)/total if total else None
    c=conn()
    c.execute("""UPDATE active_research_rules SET post_samples=?,post_wins=?,post_win_rate=?,
                 reviewed_matches=?,last_health_block_wr=?,last_review_ts=? WHERE id=?""",
              (total,sum(int(x["label"]) for x in matched),allwr,reviewed+n,wr,now_iso(),rule["id"]))
    c.commit();c.close()
    baseline=rule.get("baseline_win_rate")
    if baseline is not None and wr is not None and wr<float(baseline):
        c=conn()
        c.execute("""UPDATE active_research_rules SET status='REVERTED',deactivated_ts=?,reason=? WHERE id=?""",
                  (now_iso(),f"Independent health rollback: {wr:.3f} < baseline {float(baseline):.3f}",rule["id"]))
        c.commit();c.close()
        d={"block_samples":n,"block_win_rate":wr,"baseline_win_rate":baseline,
           "reviewed_matches":reviewed+n,"phase":"initial" if reviewed==0 else "ongoing_health"}
        _audit_research_rule("REVERTED",rule["source"],rule["rule_key"],d)
        return {"reviewed":True,"status":"REVERTED","source":rule["source"],"rule_key":rule["rule_key"],**d}
    if rule["status"]=="ACTIVE":
        c=conn()
        c.execute("UPDATE active_research_rules SET status='CONFIRMED',confirmed_ts=?,reason=? WHERE id=?",
                  (now_iso(),"Passed first independent 50-evidence block",rule["id"]))
        c.commit();c.close();action="CONFIRMED"
    else:action="HEALTH_CONFIRMED"
    d={"block_samples":n,"block_win_rate":wr,"baseline_win_rate":baseline,
       "reviewed_matches":reviewed+n,"phase":"initial" if reviewed==0 else "ongoing_health"}
    _audit_research_rule(action,rule["source"],rule["rule_key"],d)
    return {"reviewed":True,"status":"CONFIRMED","source":rule["source"],"rule_key":rule["rule_key"],**d}

def review_active_research_rules():
    results=[review_one_active_research_rule(r) for r in get_active_research_rules()]
    return {"reviewed_rules":results,"reverted":[x for x in results if x.get("status")=="REVERTED"],
            "active_after":len(get_active_research_rules())}

def review_active_research_rule():
    return review_active_research_rules()



def create_shadow_trials(signal_id: int, r: Dict[str, Any]) -> int:
    """Research only. Simulates each signal and variants; never places an extra order."""
    if not RESEARCH_LAB_ENABLED or r.get("signal") not in ("BUY","SELL"): return 0
    entry=float(r["entry"]); stop=float(r["stop"]); target=float(r["target"]); risk=abs(entry-stop)
    if risk<=0:return 0
    pip=pip_size(r["instrument"]); direction=r["signal"]
    def tp_for(rr,rk):
        d=max(MIN_TAKE_PROFIT_PIPS*pip,rr*rk); return entry+d if direction=="BUY" else entry-d
    variants=[("BASELINE",stop,target),("WIDER_STOP_125",entry-risk*1.25 if direction=="BUY" else entry+risk*1.25,tp_for(MIN_RR,risk*1.25)),("WIDER_STOP_150",entry-risk*1.5 if direction=="BUY" else entry+risk*1.5,tp_for(MIN_RR,risk*1.5)),("TARGET_2R",stop,tp_for(2.0,risk))][:SHADOW_MAX_VARIANTS_PER_SIGNAL]
    c=conn(); made=0
    for name,st,tp in variants:
        before=c.total_changes
        c.execute("""INSERT OR IGNORE INTO shadow_trials(signal_id,created_ts,candle_ts,instrument,direction,variant,entry,stop,target,risk,status) VALUES(?,?,?,?,?,?,?,?,?,?, 'PENDING')""",(signal_id,now_iso(),r.get("candle_ts"),r["instrument"],direction,name,entry,float(st),float(tp),abs(entry-float(st))))
        made += int(c.total_changes>before)
    c.commit();c.close();return made

def resolve_shadow_trials(inst: str,m1: List[Dict[str,Any]])->int:
    if not RESEARCH_LAB_ENABLED:return 0
    c=conn();rows=c.execute("SELECT * FROM shadow_trials WHERE status='PENDING' AND instrument=? ORDER BY id",(inst,)).fetchall();n=0
    for row in rows:
        fake={"candle_ts":row["candle_ts"],"created_ts":row["created_ts"],"direction":row["direction"],"entry":row["entry"],"stop":row["stop"],"target":row["target"]}
        out=resolve_one(fake,m1)
        if out:
            c.execute("UPDATE shadow_trials SET status=?,label=?,resolved_ts=?,bars_to_resolution=?,mfe_r=?,mae_r=?,note=? WHERE id=?",(out["status"],out["label"],now_iso(),out["bars"],out["mfe_r"],out["mae_r"],out["note"],row["id"]));n+=1
    c.commit();c.close();return n

def experimental_filter_candidates(r: Dict[str,Any])->Dict[str,Dict[str,Any]]:
    f=r.get("features",{});flt=r.get("filters",{});ext=float(f.get("extension_atr",0) or 0);vol=float(f.get("volatility_ratio",0) or 0);slope=abs(float(f.get("m15_slope_atr",0) or 0));bar=float(f.get("barrier_score",0) or 0);edge=float(f.get("direction_edge",0) or 0)
    confs=sum(1 for k in ("m5_structure","second_pullback","m1_confirmation","not_extended","volatility_ok") if flt.get(k));m5=float(f.get("m5_momentum",0) or 0);m1=float(f.get("m1_momentum",0) or 0);aligned=(r.get("signal")=="BUY" and m5>0 and m1>0) or (r.get("signal")=="SELL" and m5<0 and m1<0)
    return {"ext_le_0_8":{"pass":ext<=.8,"description":"Extensión <= 0.8 ATR"},"ext_le_1_0":{"pass":ext<=1.0,"description":"Extensión <= 1.0 ATR"},"vol_normal":{"pass":.75<=vol<=1.35,"description":"Volatilidad 0.75–1.35"},"trend_abs_ge_0_30":{"pass":slope>=.30,"description":"Pendiente M15 >= 0.30 ATR"},"momentum_aligned":{"pass":aligned,"description":"Momentum M5 y M1 alineado"},"confirmations_ge_3":{"pass":confs>=3,"description":"Al menos 3 confirmaciones"},"barrier_score_lt_0_75":{"pass":bar<.75,"description":"Barrera estructural < 0.75"},"direction_edge_ge_15":{"pass":edge>=15,"description":"Ventaja BUY/SELL >= 15"}}

def refresh_filter_hypotheses()->Dict[str,Any]:
    c=conn();rows=c.execute("SELECT ls.label,s.signal,s.features_json,s.filters_json FROM learning_samples ls JOIN signals s ON s.id=ls.signal_id WHERE ls.label IN (0,1) ORDER BY ls.id").fetchall()
    if not rows:c.close();return {"samples":0,"experimental":0,"evaluating":0,"validated":0,"rejected":0}
    stats={}
    for row in rows:
        rr={"signal":row["signal"],"features":json.loads(row["features_json"] or "{}"),"filters":json.loads(row["filters_json"] or "{}")}
        for key,h in experimental_filter_candidates(rr).items():
            x=stats.setdefault(key,{"description":h["description"],"pn":0,"pw":0,"fn":0,"fw":0})
            if h["pass"]:x["pn"]+=1;x["pw"]+=int(row["label"])
            else:x["fn"]+=1;x["fw"]+=int(row["label"])
    counts={k:0 for k in ("EXPERIMENTAL","EVALUATING","VALIDATED","REJECTED")}
    for key,x in stats.items():
        total=x["pn"]+x["fn"];pwr=x["pw"]/x["pn"] if x["pn"] else None;fwr=x["fw"]/x["fn"] if x["fn"] else None;edge=(pwr-fwr) if pwr is not None and fwr is not None else None;cov=x["pn"]/total if total else 0
        if total<RESEARCH_EVAL_MIN_SAMPLES:stage="EXPERIMENTAL"
        elif total<RESEARCH_VALIDATE_MIN_SAMPLES:stage="EVALUATING"
        elif x["pn"]>=20 and x["fn"]>=20 and edge is not None and edge>=RESEARCH_MIN_EDGE and .15<=cov<=.85:stage="VALIDATED"
        elif x["pn"]>=20 and x["fn"]>=20 and edge is not None and edge<=.02:stage="REJECTED"
        else:stage="EVALUATING"
        counts[stage]+=1;rec="CANDIDATE_FOR_FUTURE_VERSION" if stage=="VALIDATED" else "DO_NOT_USE" if stage=="REJECTED" else "KEEP_TESTING"
        c.execute("""INSERT INTO filter_hypotheses(filter_key,description,stage,total_samples,pass_samples,pass_wins,pass_win_rate,fail_samples,fail_wins,fail_win_rate,edge,coverage,recommendation,updated_ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(filter_key) DO UPDATE SET description=excluded.description,stage=excluded.stage,total_samples=excluded.total_samples,pass_samples=excluded.pass_samples,pass_wins=excluded.pass_wins,pass_win_rate=excluded.pass_win_rate,fail_samples=excluded.fail_samples,fail_wins=excluded.fail_wins,fail_win_rate=excluded.fail_win_rate,edge=excluded.edge,coverage=excluded.coverage,recommendation=excluded.recommendation,updated_ts=excluded.updated_ts""",(key,x["description"],stage,total,x["pn"],x["pw"],pwr,x["fn"],x["fw"],fwr,edge,cov,rec,now_iso()))
    c.commit();c.close();return {"samples":len(rows),"experimental":counts["EXPERIMENTAL"],"evaluating":counts["EVALUATING"],"validated":counts["VALIDATED"],"rejected":counts["REJECTED"]}

def should_retrain_model()->Dict[str,Any]:
    c=conn();labeled=c.execute("SELECT COUNT(*) n FROM learning_samples WHERE label IN (0,1)").fetchone()["n"];last=c.execute("SELECT samples FROM model_runs WHERE accepted=1 ORDER BY id DESC LIMIT 1").fetchone();c.close();last_n=int(last["samples"]) if last else 0;threshold=ML_MIN_SAMPLES if not last else last_n+MODEL_MIN_NEW_LABELS;return {"ready":labeled>=threshold,"labeled":labeled,"last_model_samples":last_n,"next_training_at":threshold}

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
    pending = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE status='PENDING'").fetchone()["n"]
    ambiguous = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE status='AMBIGUOUS'").fetchone()["n"]
    timeouts = c.execute("SELECT COUNT(*) n FROM learning_samples WHERE status='TIMEOUT'").fetchone()["n"]
    run = c.execute("SELECT * FROM model_runs ORDER BY id DESC LIMIT 1").fetchone()
    shadow_total=c.execute("SELECT COUNT(*) n FROM shadow_trials").fetchone()["n"]
    shadow_resolved=c.execute("SELECT COUNT(*) n FROM shadow_trials WHERE label IN (0,1)").fetchone()["n"]
    shadow_pending=c.execute("SELECT COUNT(*) n FROM shadow_trials WHERE status='PENDING'").fetchone()["n"]
    stages={x["stage"]:x["n"] for x in c.execute("SELECT stage,COUNT(*) n FROM filter_hypotheses GROUP BY stage").fetchall()}
    c.close()
    retrain_policy=should_retrain_model()
    return {
        "samples_total": total, "resolved_labeled": resolved, "pending_or_unlabeled": total - resolved,
        "pending": pending, "ambiguous": ambiguous, "timeouts": timeouts,
        "db_path": DB, "persistent_db_recommended": DB.startswith("/data/"),
        "win_rate_all": (wins / resolved) if resolved else None,
        "executed_resolved": executed_resolved, "win_rate_executed": (executed_wins / executed_resolved) if executed_resolved else None,
        "blocked_resolved": blocked_resolved, "counterfactual_win_rate_blocked": (blocked_wins / blocked_resolved) if blocked_resolved else None,
        "ml_min_samples": ML_MIN_SAMPLES, "model_ready": Path(MODEL_PATH).exists(), "last_model_run": dict(run) if run else None,
        "mode":"CONTINUOUS_RESEARCH","changes_execution":ADAPTIVE_CONFIDENCE,"ml_role":"secondary_refinement","discovery_min_samples":DISCOVERY_MIN_SAMPLES,
        "shadow_lab":{"enabled":RESEARCH_LAB_ENABLED,"trials_total":shadow_total,"resolved_labeled":shadow_resolved,"pending":shadow_pending},
        "filter_research":{"experimental":stages.get("EXPERIMENTAL",0),"evaluating":stages.get("EVALUATING",0),"validated":stages.get("VALIDATED",0),"rejected":stages.get("REJECTED",0),"automatic_live_activation":False},
        "external_research":{"enabled":EXTERNAL_RESEARCH_ENABLED,"symbols":EXTERNAL_RESEARCH_SYMBOLS,
                             "granularity":EXTERNAL_RESEARCH_GRANULARITY,"news_research":EXTERNAL_NEWS_RESEARCH,
                             "automatic_live_activation":False},
        "retrain_policy":retrain_policy
    }


def train_shadow_model(force: bool = False) -> Dict[str, Any]:
    c=conn(); rows=c.execute("SELECT features_json,label,resolved_ts FROM learning_samples WHERE label IN (0,1) ORDER BY resolved_ts,id").fetchall(); c.close()
    if len(rows)<ML_MIN_SAMPLES and not force:return {"trained":False,"reason":f"need {ML_MIN_SAMPLES}, have {len(rows)}","samples":len(rows)}
    if len(rows)<20:return {"trained":False,"reason":"need at least 20 resolved samples for temporal validation","samples":len(rows)}
    X=[]; y=[]
    for row in rows:
        f=json.loads(row["features_json"]); X.append([float(f.get(k,0) or 0) for k in FEATURE_COLUMNS]); y.append(int(row["label"]))
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
    joblib.dump({"model":final,"features":FEATURE_COLUMNS,"trained_at":now_iso(),"samples":len(y),"walk_forward":folds},MODEL_PATH)
    c=conn(); c.execute("""INSERT INTO model_runs(trained_ts,samples,train_samples,test_samples,win_rate,baseline_accuracy,accuracy,roc_auc,log_loss,accepted,model_path,note)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
      (now_iso(),len(y),folds[-1]["train"],folds[-1]["test"],float(np.mean(y)),avg["baseline"],avg["accuracy"],avg["auc"],avg["log_loss"],1,MODEL_PATH,
       json.dumps({"validation":"TimeSeriesSplit_walk_forward","folds":folds,"brier":avg["brier"]}))); c.commit(); c.close()
    return {"trained":True,"samples":len(y),"validation":"TimeSeriesSplit_walk_forward","folds":folds,"average":avg}


async def replace_trade_stop(client: httpx.AsyncClient, trade_id: str, price: float) -> Dict[str, Any]:
    body = {"stopLoss": {"price": f"{price:.5f}", "timeInForce": "GTC"}}
    return await req(client, "PUT", f"/v3/accounts/{{account}}/trades/{trade_id}/orders", body)

def register_trade_management(trade_id: str, r: Dict[str, Any], target: float):
    if not trade_id:
        return
    tscore = trend_runner_score(r)
    policy = "BE_PROFIT_TRAIL"
    c = conn()
    c.execute("""INSERT OR REPLACE INTO active_trade_management(
        trade_id,instrument,side,entry,initial_stop,initial_target,current_stop,setup_variant,policy,trend_score,
        opened_ts,last_r,last_action,updated_ts,closed)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
        (trade_id,r["instrument"],r["signal"],float(r["entry"]),float(r["stop"]),float(target),
         float(r["stop"]),setup_variant(r),policy,tscore,now_iso(),0.0,"OPEN",now_iso()))
    c.commit(); c.close()

async def manage_open_trades(client: httpx.AsyncClient, instrument: str, current_price: float) -> int:
    c=conn()
    rows=[dict(x) for x in c.execute(
        "SELECT * FROM active_trade_management WHERE instrument=? AND closed=0",(instrument,)
    ).fetchall()]
    c.close()
    changed=0
    for tr in rows:
        proposal=adaptive_stop_price(tr["side"],float(tr["entry"]),float(tr["initial_stop"]),current_price,tr["policy"])
        if float(tr.get("trend_score") or 0) >= TREND_RUNNER_MIN_SCORE and proposal["r_multiple"] >= TREND_RUNNER_TRAIL_START_R:
            risk=abs(float(tr["entry"])-float(tr["initial_stop"]))
            runner_stop=current_price-TREND_RUNNER_TRAIL_DISTANCE_R*risk if tr["side"]=="BUY" else current_price+TREND_RUNNER_TRAIL_DISTANCE_R*risk
            proposal["new_stop"]=max(float(proposal["new_stop"]),runner_stop) if tr["side"]=="BUY" else min(float(proposal["new_stop"]),runner_stop)
            proposal["action"]="TREND_RUNNER_TRAIL"
        old=float(tr["current_stop"] or tr["initial_stop"])
        improves=proposal["new_stop"]>old+1e-7 if tr["side"]=="BUY" else proposal["new_stop"]<old-1e-7
        if proposal["action"]!="NONE" and improves:
            try:
                await replace_trade_stop(client,tr["trade_id"],float(proposal["new_stop"]))
                c=conn()
                c.execute("""UPDATE active_trade_management SET current_stop=?,last_r=?,last_action=?,
                  break_even_applied=MAX(break_even_applied,?),profit_lock_applied=MAX(profit_lock_applied,?),
                  trailing_applied=MAX(trailing_applied,?),updated_ts=? WHERE trade_id=?""",
                  (float(proposal["new_stop"]),float(proposal["r_multiple"]),proposal["action"],
                   int(proposal["action"] in ("BREAK_EVEN","PROFIT_LOCK","TRAIL","TREND_RUNNER_TRAIL")),
                   int(proposal["action"] in ("PROFIT_LOCK","TRAIL","TREND_RUNNER_TRAIL")),
                   int(proposal["action"] in ("TRAIL","TREND_RUNNER_TRAIL")),
                   now_iso(),tr["trade_id"]))
                c.commit(); c.close()
                changed += 1
            except Exception as e:
                log.exception("Trade management update failed: %s",e)
        else:
            c=conn()
            c.execute("UPDATE active_trade_management SET last_r=?,updated_ts=? WHERE trade_id=?",
                      (float(proposal["r_multiple"]),now_iso(),tr["trade_id"]))
            c.commit(); c.close()
    return changed

async def scan(client: httpx.AsyncClient, inst: str) -> Dict[str, Any]:
    h1, m15, m5, m1 = await asyncio.gather(
        candles(client, inst, "H1", 140),
        candles(client, inst, "M15", 140),
        candles(client, inst, "M5", 130),
        candles(client, inst, "M1", max(220, OUTCOME_HORIZON_MIN + 30))
    )
    current_price=float(m1[-1]["c"]) if m1 else 0.0
    managed_changes=await manage_open_trades(client,inst,current_price) if current_price else 0
    resolved = resolve_pending(inst, m1)
    shadow_resolved = resolve_shadow_trials(inst, m1)
    if resolved or shadow_resolved:
        refresh_discovered_patterns()
        refresh_filter_hypotheses()
        refresh_external_hypotheses()
        autonomous_discovery_refresh()
        review_active_research_rules()
        promote_validated_research_rules()
        # Close the learning loop as soon as enough labeled outcomes exist instead
        # of waiting for the hourly maintenance tick. Training still enforces its
        # own minimum sample and temporal-validation requirements.
        retrain=should_retrain_model()
        if retrain["ready"]:
            try: state["learning"]={**train_shadow_model(force=False),"last_train":now_iso(),"model_ready":Path(MODEL_PATH).exists(),"retrain_policy":retrain}
            except Exception as e: log.exception("evidence-gated learning refresh failed: %s",e)
    if AUTO_PROMOTE_RESEARCH:
        promote_validated_research_rules()

    r = analyze(h1, m15, m5, m1, inst)

    # Research brain runs independently of order execution.
    r["external_research_collection"] = await collect_cross_asset_research(client, inst, r.get("candle_ts"))

    r = await news(client, r) if r["signal"] != "WAIT" and r["technical"] >= 50 else {**r, "alignment": "N/A"}
    if r.get("news_articles") is not None:
        record_news_research(r)
    target_plan=desired_target_for_trade(r) if r["signal"]!="WAIT" else {"target":r.get("target"),"runner":False,"trend_score":0.0}
    if r["signal"]!="WAIT":
        r["managed_target"]=target_plan["target"]
        r["trend_runner"]=target_plan["runner"]
        r["trend_score"]=target_plan["trend_score"]
    mlp = load_shadow_probability(r["features"]) if r["signal"] != "WAIT" else None
    conf = dynamic_confidence(r, mlp) if r["signal"] != "WAIT" else {
        "probability": None, "source": "WAIT", "samples": 0, "local_samples": 0,
        "variant": "WAIT", "mature": False, "recent_win_rate": None,
        "performance_penalty": 0.0, "required_confidence": EXECUTION_MIN_CONFIDENCE
    }
    r["research_governance"]={
        "auto_promote":AUTO_PROMOTE_RESEARCH,
        "min_samples":AUTO_PROMOTE_MIN_SAMPLES,
        "review_next":AUTO_PROMOTE_REVIEW_SAMPLES,
        "min_edge":AUTO_PROMOTE_MIN_EDGE,
        "max_active":None,
        "active_rules":get_active_research_rules(),
        "active_count":len(get_active_research_rules()),
        "compatibility_required":MULTI_FILTER_COMPAT_ENABLED,
        "autonomous_discovery":AUTONOMOUS_DISCOVERY_ENABLED
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
            register_trade_management(trade_id,r,float(r.get("managed_target",r["target"])))
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
        "discovery_adjustment": conf.get("discovery_adjustment", 0.0),
        "validated_pattern_matches": conf.get("validated_pattern_matches", []),
        "bootstrap_detail": conf.get("bootstrap_detail", []),
        "recent_win_rate": conf.get("recent_win_rate"),
        "performance_penalty": conf.get("performance_penalty"),
        "setup_variant": conf.get("variant"),
        "decision": decision,
        "management_updates_this_cycle": managed_changes,
        "trend_runner": r.get("trend_runner",False),
        "trend_score": r.get("trend_score",0.0),
        "managed_target": r.get("managed_target",r.get("target")),
        "learning_resolved_this_cycle": resolved,
        "shadow_resolved_this_cycle": shadow_resolved
    }


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def scanner_health_snapshot() -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    last = _parse_iso(state.get("last_scan"))
    age = (now - last).total_seconds() if last else None
    stale = age is None or age > WATCHDOG_STALE_SECONDS
    return {
        "worker_running": bool(state.get("worker_running")),
        "last_scan_age_seconds": age,
        "stale": stale,
        "watchdog_enabled": WATCHDOG_ENABLED,
        "watchdog_stale_seconds": WATCHDOG_STALE_SECONDS,
        "worker_restarts": state.get("worker_restarts", 0),
        "successful_cycles": state.get("successful_cycles", 0),
        "cycles": state.get("cycles", 0),
    }


async def worker():
    state["worker_running"] = True
    state["worker_started_at"] = now_iso()
    last_train_check = datetime.min.replace(tzinfo=timezone.utc)
    log.info("Scanner worker started")
    try:
        while True:
            now = datetime.now(timezone.utc)
            await asyncio.sleep(max(1, 60 - now.second - now.microsecond / 1e6 + 2.5))
            state["cycles"] += 1
            state["last_scan"] = now_iso()
            state["worker_last_heartbeat"] = state["last_scan"]
            cycle_ok = True
            async with httpx.AsyncClient() as client:
                for inst in INSTRUMENTS:
                    try:
                        state["last_results"][inst] = await scan(client, inst)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        cycle_ok = False
                        state["last_results"][inst] = {"error": str(e)}
                        state["last_error"] = str(e)
                        log.exception("scan failed for %s", inst)
            if cycle_ok:
                state["successful_cycles"] += 1
                state["last_successful_scan"] = now_iso()
                state["last_error"] = None
            if datetime.now(timezone.utc) - last_train_check >= timedelta(hours=1):
                try:
                    retrain=should_retrain_model()
                    result=train_shadow_model(force=False) if retrain["ready"] else {"trained":False,"reason":f"waiting for evidence: {retrain['labeled']}/{retrain['next_training_at']}","samples":retrain["labeled"]}
                    state["learning"]={**result,"last_train":now_iso(),"model_ready":Path(MODEL_PATH).exists(),"retrain_policy":retrain}
                except Exception as e:
                    log.exception("learning cycle failed")
                    state["learning"] = {"trained": False, "last_train": now_iso(), "model_ready": Path(MODEL_PATH).exists(), "error": str(e)}
                last_train_check = datetime.now(timezone.utc)
    finally:
        state["worker_running"] = False
        log.warning("Scanner worker stopped")


async def supervised_worker_loop():
    while True:
        app.state.restart_requested = False
        state["worker_restarts"] += 1
        task = asyncio.create_task(worker(), name="scanner-worker")
        app.state.scanner_worker_task = task
        try:
            await task
            # A perpetual worker should never return normally.
            state["last_error"] = "SCANNER_EXITED: worker returned unexpectedly"
            log.error(state["last_error"])
        except asyncio.CancelledError:
            if getattr(app.state, "restart_requested", False):
                log.warning("Scanner worker cancelled by watchdog; restarting")
            else:
                task.cancel()
                raise
        except Exception as e:
            state["last_error"] = f"SCANNER_CRASHED: {e}"
            log.exception("Scanner worker crashed")
        await asyncio.sleep(WORKER_RESTART_BACKOFF_SECONDS)


async def watchdog_loop():
    if not WATCHDOG_ENABLED:
        return
    while True:
        await asyncio.sleep(WATCHDOG_CHECK_SECONDS)
        state["watchdog_last_check"] = now_iso()
        started = _parse_iso(state.get("started"))
        startup_age = (datetime.now(timezone.utc) - started).total_seconds() if started else 9999
        if startup_age < WATCHDOG_STALE_SECONDS:
            continue
        snap = scanner_health_snapshot()
        if snap["stale"]:
            age = snap["last_scan_age_seconds"]
            state["last_error"] = f"SCANNER_STALE: last scan age={age} seconds"
            log.error(state["last_error"])
            task = getattr(app.state, "scanner_worker_task", None)
            if task and not task.done():
                app.state.restart_requested = True
                task.cancel()


@app.on_event("startup")
async def start():
    conn().close()
    app.state.restart_requested = False
    app.state.worker_supervisor_task = asyncio.create_task(supervised_worker_loop(), name="scanner-supervisor")
    app.state.watchdog_task = asyncio.create_task(watchdog_loop(), name="scanner-watchdog")
    log.info("24/7 PRACTICE ONLY. AUTO=%s. Scanner supervisor/watchdog active.", AUTO)


@app.on_event("shutdown")
async def shutdown():
    for name in ("scanner_worker_task", "worker_supervisor_task", "watchdog_task"):
        task = getattr(app.state, name, None)
        if task and not task.done():
            task.cancel()


@app.get("/health")
async def health():
    snap = scanner_health_snapshot()
    started = _parse_iso(state.get("started"))
    startup_age = (datetime.now(timezone.utc) - started).total_seconds() if started else 9999
    stale_effective = snap["stale"] and startup_age > WATCHDOG_STALE_SECONDS
    ok = bool(state.get("worker_running")) and not stale_effective
    return {"ok": ok, "practice_only": True, "auto_trade": AUTO, "last_scan": state["last_scan"],
            "last_successful_scan": state["last_successful_scan"], "last_error": state["last_error"],
            "learning_mode": "adaptive_confidence", "adaptive_confidence": ADAPTIVE_CONFIDENCE,
            "scanner": {**snap, "stale_effective": stale_effective}}


@app.get("/api/status")
async def status():
    return {**state, "practice_only": True, "operation_count_limit": None, "auto_trade": AUTO,
            "instruments": INSTRUMENTS, "trade_units": UNITS, "quality_threshold": THRESH,
            "bootstrap_score_threshold": BOOTSTRAP_SCORE_THRESHOLD,
            "execution_min_confidence": EXECUTION_MIN_CONFIDENCE, "confidence_min_samples": CONFIDENCE_MIN_SAMPLES,
            "single_position_per_instrument": SINGLE, "adaptive_confidence": ADAPTIVE_CONFIDENCE,
            "ml_shadow": ML_SHADOW, "ml_role": "secondary_refinement",
            "scanner": scanner_health_snapshot()}


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



@app.get("/api/research/shadow")
async def research_shadow(limit:int=100,status:Optional[str]=None):
    c=conn();rows=c.execute("SELECT * FROM shadow_trials WHERE status=? ORDER BY id DESC LIMIT ?",(status.upper(),min(max(limit,1),500))).fetchall() if status else c.execute("SELECT * FROM shadow_trials ORDER BY id DESC LIMIT ?",(min(max(limit,1),500),)).fetchall();c.close();return {"execution_effect":"NONE_RESEARCH_ONLY","trials":[dict(x) for x in rows]}

@app.get("/api/research/filters")
async def research_filters():
    c=conn();rows=c.execute("SELECT * FROM filter_hypotheses ORDER BY CASE stage WHEN 'VALIDATED' THEN 1 WHEN 'EVALUATING' THEN 2 WHEN 'EXPERIMENTAL' THEN 3 ELSE 4 END,ABS(COALESCE(edge,0)) DESC,total_samples DESC").fetchall();c.close();return {"automatic_live_activation":False,"evaluation_min_samples":RESEARCH_EVAL_MIN_SAMPLES,"validation_min_samples":RESEARCH_VALIDATE_MIN_SAMPLES,"minimum_edge":RESEARCH_MIN_EDGE,"filters":[dict(x) for x in rows]}


@app.post("/api/research/external/observation")
async def research_external_observation(payload: Dict[str, Any]):
    instrument=str(payload.get("instrument") or "EUR_USD").upper()
    source_type=str(payload.get("source_type") or "").upper()
    source_key=str(payload.get("source_key") or "").upper()
    if not source_type or not source_key:
        raise HTTPException(status_code=400,detail="source_type and source_key are required")
    record_external_observation(instrument,source_type,source_key,payload.get("value_num"),
                                payload.get("value_text"),payload.get("metadata") or {},
                                payload.get("candle_ts"))
    return {"ok":True,"research_only":True,"automatic_live_activation":False}


@app.get("/api/research/external/hypotheses")
async def research_external_hypotheses():
    c=conn()
    rows=c.execute("""SELECT * FROM external_hypotheses ORDER BY
                      CASE stage WHEN 'VALIDATED' THEN 1 WHEN 'EVALUATING' THEN 2
                      WHEN 'EXPERIMENTAL' THEN 3 ELSE 4 END,
                      ABS(COALESCE(edge,0)) DESC,total_samples DESC""").fetchall()
    c.close()
    return {"enabled":EXTERNAL_RESEARCH_ENABLED,"automatic_live_activation":False,
            "min_evaluation_samples":EXTERNAL_RESEARCH_MIN_SAMPLES,
            "min_validation_samples":EXTERNAL_RESEARCH_VALIDATE_SAMPLES,
            "shadow_included":EXTERNAL_INCLUDE_SHADOW,
            "shadow_baseline_weight":EXTERNAL_SHADOW_BASELINE_WEIGHT,
            "shadow_variant_weight":EXTERNAL_SHADOW_VARIANT_WEIGHT,
            "promotion_min_canonical":EXTERNAL_PROMOTION_MIN_CANONICAL,
            "hypotheses":[dict(x) for x in rows]}


@app.get("/api/research/knowledge")
async def research_knowledge(limit: int = 100):
    c=conn()
    rows=c.execute("SELECT * FROM research_knowledge ORDER BY id DESC LIMIT ?",
                   (min(max(limit,1),500),)).fetchall()
    c.close()
    return {"research_only":True,"findings":[dict(x) for x in rows]}




@app.get("/api/research/autonomous")
async def research_autonomous(limit: int = 100):
    c=conn()
    rows=c.execute("""SELECT * FROM autonomous_hypotheses
                      ORDER BY CASE stage WHEN 'VALIDATED' THEN 1 WHEN 'EVALUATING' THEN 2
                      WHEN 'EXPERIMENTAL' THEN 3 ELSE 4 END,score DESC,validation_samples DESC
                      LIMIT ?""",(min(max(limit,1),500),)).fetchall()
    fam=c.execute("SELECT * FROM research_family_stats ORDER BY priority_score DESC").fetchall()
    c.close()
    return {"enabled":AUTONOMOUS_DISCOVERY_ENABLED,"holdout":AUTONOMOUS_DISCOVERY_HOLDOUT,
            "promotion_at":AUTO_PROMOTE_MIN_SAMPLES,"promotion_min_canonical":AUTONOMOUS_PROMOTION_MIN_CANONICAL,
            "hypotheses":[dict(x) for x in rows],"research_priorities":[dict(x) for x in fam]}

@app.post("/api/research/autonomous/refresh")
async def research_autonomous_refresh():
    return autonomous_discovery_refresh()


@app.get("/api/research/active-rule")
async def research_active_rule():
    c=conn()
    history=c.execute("SELECT * FROM active_research_rules ORDER BY id DESC LIMIT 20").fetchall()
    c.close()
    c2=conn()
    comp=c2.execute("SELECT * FROM research_rule_compatibility ORDER BY checked_ts DESC LIMIT 100").fetchall()
    c2.close()
    return {"auto_promote":AUTO_PROMOTE_RESEARCH,"min_samples":AUTO_PROMOTE_MIN_SAMPLES,
            "min_edge":AUTO_PROMOTE_MIN_EDGE,"parallel_research":True,
            "multiple_active_if_compatible":True,"fixed_active_limit":None,"veto_only":True,
            "active":get_active_research_rules(),"history":[dict(x) for x in history],
            "compatibility":[dict(x) for x in comp]}


@app.get("/api/research/compatibility")
async def research_compatibility(limit: int = 200):
    c=conn()
    rows=c.execute("SELECT * FROM research_rule_compatibility ORDER BY checked_ts DESC LIMIT ?",
                   (min(max(limit,1),1000),)).fetchall()
    c.close()
    return {"min_joint_samples":MULTI_FILTER_MIN_JOINT_SAMPLES,
            "min_joint_coverage":MULTI_FILTER_MIN_JOINT_COVERAGE,
            "max_joint_wr_drop":MULTI_FILTER_MAX_WR_DROP,
            "checks":[dict(x) for x in rows]}


@app.get("/api/research/rule-audit")
async def research_rule_audit(limit: int = 100):
    c=conn()
    rows=c.execute("SELECT * FROM research_rule_audit ORDER BY id DESC LIMIT ?",
                   (min(max(limit,1),500),)).fetchall()
    c.close()
    return {"events":[dict(x) for x in rows]}


@app.post("/api/research/promote")
async def research_promote():
    return promote_validated_research_rules()


@app.post("/api/research/review-active")
async def research_review_active():
    return review_active_research_rules()


@app.post("/api/research/refresh")
async def research_refresh():
    external_research = refresh_external_hypotheses()
    autonomous=autonomous_discovery_refresh()
    return {"external":external_research,"autonomous":autonomous,
            "patterns":refresh_discovered_patterns(),"filters":refresh_filter_hypotheses(),
            "retrain_policy":should_retrain_model(),
            "note":"Autonomous rules are discovered on older data and validated on a later holdout before the 100/50 cycle."}

@app.post("/api/learning/train")
async def train_now():
    # Safe: training only. It cannot alter technical rules or order execution.
    return train_shadow_model(force=True)


@app.get("/api/discovery")
async def discovery():
    c=conn()
    rows=[dict(x) for x in c.execute("SELECT * FROM discovered_patterns ORDER BY validated DESC, ABS(weight) DESC, samples DESC LIMIT 100").fetchall()]
    c.close()
    return {"minimum_samples": DISCOVERY_MIN_SAMPLES, "minimum_edge": DISCOVERY_MIN_EDGE, "patterns": rows}

@app.get("/", response_class=HTMLResponse)
async def home():
    return """<!doctype html><html lang='es'><meta name='viewport' content='width=device-width'><title>Market Alert V1.7</title>
<style>body{font-family:system-ui;background:#0b1020;color:#eef2ff;max-width:1050px;margin:auto;padding:24px}.c{background:#151c32;border:1px solid #2c3656;border-radius:16px;padding:18px;margin:12px 0}pre{white-space:pre-wrap;word-break:break-word;background:#080c17;padding:14px;border-radius:12px}.tag{display:inline-block;padding:5px 9px;border-radius:999px;background:#25304f;margin-right:6px}</style>
<h1>Market Alert V2.8 · Adaptive Risk Engine</h1><div class=c><span class=tag>OANDA PRACTICE ONLY</span><span class=tag>24/7</span><span class=tag>Sin límite diario</span><span class=tag>Confianza calibrada</span>
<p><b>Quality Score ≠ probabilidad.</b> La confianza dinámica se calibra con resultados reales. Con poca muestra se limita deliberadamente y el 90% requiere evidencia sustancial.</p></div>
<div class=c><h2>Estado</h2><pre id=s>Cargando…</pre></div><div class=c><h2>Aprendizaje</h2><pre id=l>Cargando…</pre></div><div class=c><h2>Última decisión</h2><pre id=d>Cargando…</pre></div><div class=c><h2>Últimas señales</h2><pre id=h>Cargando…</pre></div>
<script>async function u(){s.textContent=JSON.stringify(await fetch('/api/status').then(r=>r.json()),null,2);l.textContent=JSON.stringify(await fetch('/api/learning').then(r=>r.json()),null,2);d.textContent=JSON.stringify(await fetch('/api/decisions?limit=5').then(r=>r.json()),null,2);h.textContent=JSON.stringify(await fetch('/api/signals?limit=15').then(r=>r.json()),null,2)}u();setInterval(u,15000)</script></html>"""

@app.get("/api/trade-management")
def trade_management_status():
    return {
        "ok": True,
        "mode": "ADAPTIVE_TRADE_MANAGEMENT",
        "break_even_trigger_r": BREAK_EVEN_TRIGGER_R,
        "break_even_lock_r": BREAK_EVEN_LOCK_R,
        "profit_lock_trigger_r": PROFIT_LOCK_TRIGGER_R,
        "profit_lock_r": PROFIT_LOCK_R,
        "trail_trigger_r": TRAIL_TRIGGER_R,
        "trail_distance_r": TRAIL_DISTANCE_R,
        "exit_policy_min_samples": EXIT_POLICY_MIN_SAMPLES,
        "default_policy": "BE_PROFIT_TRAIL",
        "practice_only": True,
    }


@app.get("/api/open-trade-management")
def open_trade_management():
    c=conn()
    rows=[dict(x) for x in c.execute("SELECT * FROM active_trade_management WHERE closed=0 ORDER BY opened_ts DESC").fetchall()]
    c.close()
    return rows

@app.get("/api/version-stats")
def version_stats():
    c=conn()
    rows=[dict(x) for x in c.execute("SELECT * FROM strategy_version_stats ORDER BY started_ts DESC").fetchall()]
    c.close()
    return rows


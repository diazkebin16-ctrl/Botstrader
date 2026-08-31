import os
import asyncio
import sqlite3
import json
import logging
import math
import hashlib
import time
import statistics
import numpy as np
import joblib
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score, log_loss, brier_score_loss
from datetime import datetime, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional
from recovery_manager import RecoveryManager, deterministic_intent_key
from security_manager import SecurityManager, RedactingFilter, sanitize as security_sanitize
from system_evaluation import SystemEvaluationEngine
from governance_engine import GovernanceEngine, AUTHORITY_MATRIX, AUTHORITY_PRIORITY
from production_readiness import ProductionReadinessGate
from smart_execution import SmartExecutionEngine
from ensemble_engine import EnsembleEngine
from storage_lifecycle import StorageLifecycleManager
from capital_allocation import CapitalAllocationEngine
from research_evidence import (resolve_outcome as research_resolve_outcome, collapse_market_episodes,
                               annotate_market_episodes, split_episode_holdout)
from session_regime import session_regime as detect_session_regime
from instrument_registry import InstrumentRegistry
from instrument_profiles import instrument_profile
from slot_allocator import slot_policy
from opportunity_ranker import rank_opportunities
from broker_risk import OandaBrokerRiskAdapter
from counterfactual_tracker import CounterfactualTracker
from legacy_v331_scoring import legacy_v331_score, choose_legacy_v331_direction
from forward_experiment import forward_policy, evaluate_forward_experiment
from observability import (
    ObservabilityManager, DEPENDENCY_CRITICAL, DEPENDENCY_IMPORTANT, DEPENDENCY_NON_CRITICAL,
    stale_status as observability_stale_status, reconciliation_status as observability_reconciliation_status,
    degradation_state as observability_degradation_state,
)
from deployment_runtime import DeploymentManager
from adaptive_learning import (
    dataset_fingerprint as al_dataset_fingerprint,
    candidate_uses_entry_only as al_candidate_uses_entry_only,
    candidate_passes as al_candidate_passes,
    metrics as al_metrics,
    validate_candidate as al_validate_candidate,
    candidate_score as al_candidate_score,
    concept_drift as al_concept_drift,
)
from validation_pipeline import (
    run_historical_validation as vp_run_historical_validation,
    strict_temporal_split as vp_strict_temporal_split,
    candidate_passes as vp_candidate_passes,
    extended_metrics as vp_metrics,
    dataset_fingerprint as vp_dataset_fingerprint,
)

import httpx
from fastapi import FastAPI, HTTPException, Header, Body
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

# Primary broker environment remains PRACTICE by default. Live endpoint selection requires
# THREE independent conditions and is disabled in unit/integration test processes. The
# Production Readiness Gate must still separately authorize every real order.
TRADING_ENVIRONMENT = os.getenv("TRADING_ENVIRONMENT","PAPER").strip().upper()
EARLY_TEST_MODE = bool(os.getenv("PYTEST_CURRENT_TEST") or os.getenv("UNIT_TEST")=="1" or TRADING_ENVIRONMENT in ("TEST","INTEGRATION_TEST","SIMULATION"))
PRIMARY_OANDA_ENV = os.getenv("PRIMARY_OANDA_ENV","practice").strip().lower()
PRODUCTION_AUTHORIZED = os.getenv("PRODUCTION_AUTHORIZED","false").lower()=="true"
OANDA = "https://api-fxtrade.oanda.com" if (PRIMARY_OANDA_ENV=="live" and PRODUCTION_AUTHORIZED and TRADING_ENVIRONMENT=="PRODUCTION" and not EARLY_TEST_MODE) else "https://api-fxpractice.oanda.com"
CANARY_OANDA_ENV = os.getenv("CANARY_OANDA_ENV","practice").strip().lower()
CANARY_OANDA = "https://api-fxtrade.oanda.com" if CANARY_OANDA_ENV=="live" else "https://api-fxpractice.oanda.com"
CANARY_ACCOUNT = os.getenv("OANDA_CANARY_ACCOUNT_ID","").strip()
CANARY_TOKEN = os.getenv("OANDA_CANARY_TOKEN",os.getenv("OANDA_TOKEN","")).strip()
DEPLOYMENT_LIVE_EXECUTION_ENABLED = os.getenv("DEPLOYMENT_LIVE_EXECUTION_ENABLED","false").lower()=="true"
GDELT = "https://api.gdeltproject.org/api/v2/doc/doc"
ACCOUNT = os.getenv("OANDA_ACCOUNT_ID", "").strip()
TOKEN = os.getenv("OANDA_TOKEN", "").strip()
def _instrument_list(raw: str) -> List[str]:
    out=[]
    for value in str(raw or "").split(","):
        symbol=InstrumentRegistry.normalize_symbol(value)
        if symbol and symbol not in out:
            out.append(symbol)
    return out

# Configured instruments are filtered through their central profile before they
# receive broker order authority.  PAPER/practice defaults to the three approved
# forward-collection instruments; secondary profiles explicitly deny LIVE.
PRIMARY_INSTRUMENT = "EUR_USD"
ANALYSIS_INSTRUMENTS = ("EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD")

def configured_instruments(raw: Optional[str] = None) -> List[str]:
    """Resolve explicit analysis configuration; default/fallback is primary only."""
    if raw is None:
        raw = os.getenv("INSTRUMENTS", PRIMARY_INSTRUMENT)
    requested = _instrument_list(raw)
    allowed = [x for x in requested if x in ANALYSIS_INSTRUMENTS]
    return allowed or [PRIMARY_INSTRUMENT]

CONFIGURED_INSTRUMENTS = configured_instruments()
INSTRUMENTS = [x for x in CONFIGURED_INSTRUMENTS if instrument_profile(x).allows_execution(TRADING_ENVIRONMENT, PRIMARY_OANDA_ENV)]

# V3.37.0 execution coordination invariant: one active process/replica per broker
# account. Distributed execution locking is intentionally out of scope. Detect
# common local/process worker-count settings and fail closed for new batch orders
# when they explicitly request more than one worker.
EXECUTION_WORKER_MODE = "SINGLE_PROCESS_SINGLE_ACTIVE_REPLICA"
DISTRIBUTED_EXECUTION_COORDINATION = False

EXECUTION_WORKER_ENV_VARS = ("WEB_CONCURRENCY", "UVICORN_WORKERS", "GUNICORN_WORKERS")

def execution_worker_configuration(env: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Validate every known local worker-count setting; any unsafe value fails closed.

    Empty strings are treated as unset.  Horizontal execution scaling remains
    unsupported: every explicitly configured value must be exactly 1.
    """
    source = os.environ if env is None else env
    configured: Dict[str, int] = {}
    invalid: Dict[str, Any] = {}
    for name in EXECUTION_WORKER_ENV_VARS:
        raw = source.get(name)
        if raw is None or not str(raw).strip():
            continue
        try:
            count = int(str(raw).strip())
        except (TypeError, ValueError, OverflowError):
            invalid[name] = raw
            continue
        configured[name] = count
        if count != 1:
            invalid[name] = raw
    safe = not invalid
    effective = 1 if not configured else max(configured.values())
    if any(name in invalid and name not in configured for name in invalid):
        effective = None
    return {
        "safe": safe,
        "effective_workers": effective,
        "configured": configured,
        "invalid": invalid,
        "distributed_coordination": False,
    }

def _configured_execution_worker_count() -> Optional[int]:
    return execution_worker_configuration().get("effective_workers")

EXECUTION_WORKER_CONFIG = execution_worker_configuration()
EXECUTION_WORKER_COUNT = EXECUTION_WORKER_CONFIG.get("effective_workers")
MULTI_WORKER_EXECUTION_BLOCKED = not bool(EXECUTION_WORKER_CONFIG.get("safe"))
SHADOW_INSTRUMENTS = [x for x in _instrument_list(os.getenv("SHADOW_INSTRUMENTS", "")) if x not in INSTRUMENTS]
# Analysis universe is intentionally broader than OANDA execution authority.
# All five target FX pairs may execute only when explicitly configured and their
# profiles/broker metadata authorize OANDA Practice. Secondary LIVE remains denied.
SCAN_INSTRUMENTS = list(dict.fromkeys(CONFIGURED_INSTRUMENTS + SHADOW_INSTRUMENTS))
INSTRUMENT_REGISTRY = InstrumentRegistry()
_INSTRUMENT_METADATA_REFRESH_TS: Optional[datetime] = None
INSTRUMENT_METADATA_REFRESH_SECONDS = max(300, int(os.getenv("INSTRUMENT_METADATA_REFRESH_SECONDS", "21600")))

def instrument_mode(instrument: str) -> str:
    symbol=InstrumentRegistry.normalize_symbol(instrument)
    if symbol in INSTRUMENTS and instrument_profile(symbol).allows_execution(TRADING_ENVIRONMENT, PRIMARY_OANDA_ENV):
        return "ENABLED"
    if symbol in SHADOW_INSTRUMENTS:
        return "SHADOW"
    return "DISABLED"

def instrument_metadata(instrument: str):
    return INSTRUMENT_REGISTRY.get(instrument)

def format_instrument_price(instrument: str, price: float) -> str:
    return instrument_metadata(instrument).format_price(price)

def normalize_instrument_units(instrument: str, units: float, *, allow_zero: bool=False) -> float:
    return instrument_metadata(instrument).normalize_units(units, allow_zero=allow_zero)

def format_instrument_units(instrument: str, units: float, *, allow_zero: bool=False) -> str:
    return instrument_metadata(instrument).format_units(units,allow_zero=allow_zero)

UNITS = max(1, int(os.getenv("TRADE_UNITS", "100")))
THRESH = max(0, min(100, int(os.getenv("QUALITY_THRESHOLD", "80"))))
AUTO = os.getenv("AUTO_TRADE", "false").lower() == "true"
SINGLE = os.getenv("SINGLE_POSITION_PER_INSTRUMENT", "true").lower() == "true"
SESSION = os.getenv("SESSION_FILTER", "true").lower() == "true"
NEWS = os.getenv("NEWS_FILTER", "true").lower() == "true"
MIN_RR = float(os.getenv("MIN_RR", "1.5"))
# Entry admission can accept less structural room than the final managed target.
# The target remains MIN_RR (1.50R); this value only controls admission.
MIN_ENTRY_RR = max(0.10, min(MIN_RR, float(os.getenv("MIN_ENTRY_RR", "0.40"))))
# Storage resolution. A Railway volume cannot be created by application code;
# the runtime can only detect and use a mounted persistent path.  We therefore
# separate "configured" from "recommended" and never claim that the ephemeral
# container filesystem is persistent.
IS_RAILWAY = bool(os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_PROJECT_ID") or os.getenv("RAILWAY_SERVICE_ID"))
PERSISTENT_STORAGE_PATH = os.getenv("PERSISTENT_STORAGE_PATH","").strip()
RAILWAY_VOLUME_MOUNT_PATH = os.getenv("RAILWAY_VOLUME_MOUNT_PATH","").strip()
PERSISTENCE_REQUIRED = os.getenv(
    "PERSISTENCE_REQUIRED", "true" if TRADING_ENVIRONMENT=="PRODUCTION" else "false"
).lower()=="true"

def _persistent_base_dir() -> Optional[str]:
    candidates=[PERSISTENT_STORAGE_PATH,RAILWAY_VOLUME_MOUNT_PATH,"/data"]
    for raw in candidates:
        if not raw: continue
        path=os.path.abspath(os.path.expanduser(raw))
        if os.path.isdir(path) and os.access(path,os.W_OK):
            return path
    return None

_PERSISTENT_BASE=_persistent_base_dir()
DB = os.getenv("DB_PATH", os.path.join(_PERSISTENT_BASE,"market_alert.db") if _PERSISTENT_BASE else "market_alert.db")
MODEL_PATH = os.getenv("MODEL_PATH", os.path.join(_PERSISTENT_BASE,"market_alert_model.joblib") if _PERSISTENT_BASE else "market_alert_model.joblib")

def _path_within(path: str, base: Optional[str]) -> bool:
    if not base or not os.path.isabs(path): return False
    try:
        return os.path.commonpath([os.path.abspath(path),os.path.abspath(base)])==os.path.abspath(base)
    except ValueError:
        return False

DB_PERSISTENT = _path_within(DB,_PERSISTENT_BASE)
MODEL_PERSISTENT = _path_within(MODEL_PATH,_PERSISTENT_BASE)

# Explicit DB_PATH/MODEL_PATH parents are created when possible. This supports a
# mounted volume such as /data without silently manufacturing a fake "persistent"
# directory on an ephemeral filesystem.
for _storage_path in (DB,MODEL_PATH):
    _parent=os.path.dirname(os.path.abspath(_storage_path))
    if _parent and _parent!=os.getcwd() and (os.path.isabs(_storage_path) or os.path.dirname(_storage_path)):
        os.makedirs(_parent,exist_ok=True)

def storage_status() -> Dict[str,Any]:
    persistent=bool(DB_PERSISTENT and MODEL_PERSISTENT)
    railway_missing=bool(IS_RAILWAY and not persistent)
    if persistent:
        status="PERSISTENT"
        action=None
    elif railway_missing:
        status="ACTION_REQUIRED_RAILWAY_VOLUME"
        action="Attach a Railway Volume mounted at /data (or set PERSISTENT_STORAGE_PATH/DB_PATH to its mount path)."
    else:
        status="EPHEMERAL"
        action="Configure a persistent volume/path before relying on learning across restarts."
    return {
        "status":status,"persistent":persistent,"db_persistent":bool(DB_PERSISTENT),
        "model_persistent":bool(MODEL_PERSISTENT),"db_path":DB,"model_path":MODEL_PATH,
        "base":_PERSISTENT_BASE,"is_railway":IS_RAILWAY,
        "railway_volume_mount":RAILWAY_VOLUME_MOUNT_PATH or None,
        "persistent_storage_path":PERSISTENT_STORAGE_PATH or None,
        "persistence_required":bool(PERSISTENCE_REQUIRED),
        "action_required":bool(not persistent),"action":action,
    }
ML_SHADOW = os.getenv("ML_SHADOW", "true").lower() == "true"
ML_MIN_SAMPLES = max(50, int(os.getenv("ML_MIN_SAMPLES", "100")))
ML_RETRAIN_HOURS = max(1, int(os.getenv("ML_RETRAIN_HOURS", "24")))
OUTCOME_HORIZON_MIN = max(30, int(os.getenv("OUTCOME_HORIZON_MIN", "180")))
COUNTERFACTUAL_SHADOW_ENABLED = os.getenv("COUNTERFACTUAL_SHADOW_ENABLED", "true").lower() == "true"
COUNTERFACTUAL_HORIZON_BARS = max(1, int(os.getenv("COUNTERFACTUAL_HORIZON_BARS", str(OUTCOME_HORIZON_MIN))))
_COUNTERFACTUAL_TRACKERS: Dict[str, CounterfactualTracker] = {}

def counterfactual_tracker() -> CounterfactualTracker:
    tracker=_COUNTERFACTUAL_TRACKERS.get(DB)
    if tracker is None or tracker.horizon_bars != COUNTERFACTUAL_HORIZON_BARS:
        tracker=CounterfactualTracker(DB,COUNTERFACTUAL_HORIZON_BARS)
        _COUNTERFACTUAL_TRACKERS[DB]=tracker
    return tracker

RESEARCH_ROUND_TRIP_COST_PIPS = max(0.0, float(os.getenv("RESEARCH_ROUND_TRIP_COST_PIPS", "1.0")))
RESEARCH_EPISODE_GAP_MINUTES = max(1, int(os.getenv("RESEARCH_EPISODE_GAP_MINUTES", "15")))
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
BREAK_EVEN_LOCK_R = max(0.0, float(os.getenv("BREAK_EVEN_LOCK_R", "0.00")))
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
VERSION_TAG = "3.38.1"
ENTRY_TIMING_ENABLED = os.getenv("ENTRY_TIMING_ENABLED", "true").lower() == "true"
MAX_ENTRY_EXTENSION_ATR = max(0.5, float(os.getenv("MAX_ENTRY_EXTENSION_ATR", "1.50")))
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
MIN_STOP_PIPS = max(0.1, float(os.getenv("MIN_STOP_PIPS", "9.0")))
STOP_ATR_M1_MULT = max(0.1, float(os.getenv("STOP_ATR_M1_MULT", "1.50")))
STOP_ATR_M5_MULT = max(0.1, float(os.getenv("STOP_ATR_M5_MULT", "0.40")))
M1_CONFIRMATION_REQUIRED = os.getenv("M1_CONFIRMATION_REQUIRED", "true").lower() == "true"

# Forward-validation entry filters. These rules have execution authority only in
# PAPER + OANDA practice. Production cannot inherit them implicitly.
PAPER_FORWARD_FILTERS_ENABLED = os.getenv("PAPER_FORWARD_FILTERS_ENABLED", "true").lower() == "true"
LOW_ROOM_LOW_RR_MAX_ROOM_R = 0.40
LOW_ROOM_LOW_RR_MAX_ENTRY_RR = 1.00
LOW_ROOM_EXTENDED_MAX_ROOM_R = 0.60
LOW_ROOM_EXTENDED_MIN_EXTENSION_ATR = 0.80

def paper_forward_filters_active(instrument: Optional[str] = None) -> bool:
    symbol=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    profile=instrument_profile(symbol)
    return bool(
        PAPER_FORWARD_FILTERS_ENABLED
        and TRADING_ENVIRONMENT == "PAPER"
        and PRIMARY_OANDA_ENV == "practice"
        and OANDA.endswith("fxpractice.oanda.com")
        and (profile.has_veto("LOW_ROOM_LOW_RR") or profile.has_veto("LOW_ROOM_EXTENDED"))
    )

def forward_entry_pattern_flags(features: Dict[str, Any]) -> Dict[str, bool]:
    """Pure rule evaluation shared by telemetry and the PAPER execution gate."""
    try:
        room_raw = features.get("room_to_barrier_r")
        room = None if room_raw is None else float(room_raw)
        rr = float(features.get("rr_raw", 0) or 0)
        ext = float(features.get("extension_atr", 0) or 0)
    except (TypeError, ValueError):
        return {"low_room_low_rr": False, "low_room_extended": False}
    return {
        "low_room_low_rr": bool(
            room is not None
            and room < LOW_ROOM_LOW_RR_MAX_ROOM_R
            and rr < LOW_ROOM_LOW_RR_MAX_ENTRY_RR
        ),
        "low_room_extended": bool(
            room is not None
            and room < LOW_ROOM_EXTENDED_MAX_ROOM_R
            and ext > LOW_ROOM_EXTENDED_MIN_EXTENSION_ATR
        ),
    }

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
AUTO_PROMOTE_RESEARCH = False  # V3.19: research may recommend; activation requires Change Management approval
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
WEEKEND_RESEARCH_ENABLED = os.getenv("WEEKEND_RESEARCH_ENABLED", "true").lower() == "true"
WEEKEND_NEWS_INTERVAL_MIN = max(30, int(os.getenv("WEEKEND_NEWS_INTERVAL_MIN", "60")))
WEEKEND_SIGNAL_CONTEXT_HOURS = max(1, min(48, int(os.getenv("WEEKEND_SIGNAL_CONTEXT_HOURS", "24"))))
WEEKEND_REACTION_HORIZONS = (1, 4, 12, 24)
STRATEGY_SELF_EVAL_ENABLED = os.getenv("STRATEGY_SELF_EVAL_ENABLED", "true").lower() == "true"
STRATEGY_AUTO_PAUSE = os.getenv("STRATEGY_AUTO_PAUSE", "true").lower() == "true"
STRATEGY_BASELINE_WINDOW = max(40, int(os.getenv("STRATEGY_BASELINE_WINDOW", "100")))
STRATEGY_RECENT_WINDOW = max(20, int(os.getenv("STRATEGY_RECENT_WINDOW", "30")))
STRATEGY_MIN_EXECUTED_TOTAL = max(20, int(os.getenv("STRATEGY_MIN_EXECUTED_TOTAL", "50")))
STRATEGY_WATCH_DROP = max(0.03, min(0.25, float(os.getenv("STRATEGY_WATCH_DROP", "0.08"))))
STRATEGY_DEGRADED_DROP = max(0.08, min(0.35, float(os.getenv("STRATEGY_DEGRADED_DROP", "0.15"))))
STRATEGY_DEGRADED_MAX_WR = max(0.25, min(0.65, float(os.getenv("STRATEGY_DEGRADED_MAX_WR", "0.50"))))
STRATEGY_RECOVERY_SAMPLES = max(20, int(os.getenv("STRATEGY_RECOVERY_SAMPLES", "30")))
STRATEGY_RECOVERY_TOLERANCE = max(0.00, min(0.20, float(os.getenv("STRATEGY_RECOVERY_TOLERANCE", "0.05"))))
STRATEGY_MAX_LOSS_STREAK_WATCH = max(3, int(os.getenv("STRATEGY_MAX_LOSS_STREAK_WATCH", "5")))
AI_DIRECTOR_ENABLED = os.getenv("AI_DIRECTOR_ENABLED", "true").lower() == "true"
AI_DIRECTOR_OBSERVATION_ONLY = True
AI_DIRECTOR_MIN_HISTORY = max(10, int(os.getenv("AI_DIRECTOR_MIN_HISTORY", "20")))
AI_DIRECTOR_RECENT_WINDOW = max(10, int(os.getenv("AI_DIRECTOR_RECENT_WINDOW", "30")))
AI_DIRECTOR_REDUCED_THRESHOLD = max(0.30, min(0.80, float(os.getenv("AI_DIRECTOR_REDUCED_THRESHOLD", "0.58"))))
AI_DIRECTOR_ACTIVE_THRESHOLD = max(AI_DIRECTOR_REDUCED_THRESHOLD, min(0.95, float(os.getenv("AI_DIRECTOR_ACTIVE_THRESHOLD", "0.72"))))
AI_DIRECTOR_LOG_CHANGES_ONLY = os.getenv("AI_DIRECTOR_LOG_CHANGES_ONLY", "true").lower() == "true"
RISK_ENGINE_ENABLED = os.getenv("RISK_ENGINE_ENABLED", "true").lower() == "true"
RISK_ENGINE_SHADOW_MODE = True
RISK_BASE_FRACTION = max(0.001, min(0.03, float(os.getenv("RISK_BASE_FRACTION", "0.005"))))
RISK_MAX_TRADE_FRACTION = max(RISK_BASE_FRACTION, min(0.03, float(os.getenv("RISK_MAX_TRADE_FRACTION", "0.01"))))
RISK_MAX_STRATEGY_FRACTION = max(RISK_MAX_TRADE_FRACTION, min(0.10, float(os.getenv("RISK_MAX_STRATEGY_FRACTION", "0.03"))))
RISK_MAX_PORTFOLIO_FRACTION = max(RISK_MAX_STRATEGY_FRACTION, min(0.20, float(os.getenv("RISK_MAX_PORTFOLIO_FRACTION", "0.06"))))
RISK_MAX_MARGIN_USAGE = max(0.10, min(0.90, float(os.getenv("RISK_MAX_MARGIN_USAGE", "0.50"))))
RISK_DRAWDOWN_WARN = max(0.01, min(0.25, float(os.getenv("RISK_DRAWDOWN_WARN", "0.05"))))
RISK_DRAWDOWN_STOP = max(RISK_DRAWDOWN_WARN, min(0.50, float(os.getenv("RISK_DRAWDOWN_STOP", "0.10"))))
RISK_MAX_CONSECUTIVE_LOSSES = max(3, int(os.getenv("RISK_MAX_CONSECUTIVE_LOSSES", "6")))
RISK_MAX_CORRELATED_POSITIONS = max(1, int(os.getenv("RISK_MAX_CORRELATED_POSITIONS", "2")))
RISK_DATA_STALE_SECONDS = max(60, int(os.getenv("RISK_DATA_STALE_SECONDS", "300")))
RISK_MIN_MULTIPLIER = max(0.05, min(0.60, float(os.getenv("RISK_MIN_MULTIPLIER", "0.25"))))
RISK_ABNORMAL_ERROR_COUNT = max(1, int(os.getenv("RISK_ABNORMAL_ERROR_COUNT", "3")))
TRADE_MEMORY_ENABLED = os.getenv("TRADE_MEMORY_ENABLED", "true").lower() == "true"
TRADE_MEMORY_MIN_SAMPLE_SIZE = max(5, int(os.getenv("TRADE_MEMORY_MIN_SAMPLE_SIZE", "20")))
TRADE_MEMORY_DEGRADATION_RECENT = max(10, int(os.getenv("TRADE_MEMORY_DEGRADATION_RECENT", "20")))
TRADE_MEMORY_DEGRADATION_MIN_HISTORY = max(20, int(os.getenv("TRADE_MEMORY_DEGRADATION_MIN_HISTORY", "30")))
TRADE_MEMORY_DEGRADATION_PF_FLOOR = max(0.50, min(1.20, float(os.getenv("TRADE_MEMORY_DEGRADATION_PF_FLOOR", "1.00"))))
TRADE_MEMORY_DEGRADATION_MIN_PF_DROP = max(0.10, min(2.00, float(os.getenv("TRADE_MEMORY_DEGRADATION_MIN_PF_DROP", "0.30"))))
TRADE_MEMORY_RECONCILE_LIMIT = max(5, min(100, int(os.getenv("TRADE_MEMORY_RECONCILE_LIMIT", "25"))))
ADAPTIVE_LEARNING_ENABLED = os.getenv("ADAPTIVE_LEARNING_ENABLED", "true").lower() == "true"
ADAPTIVE_LEARNING_MIN_TRADES = max(20, int(os.getenv("ADAPTIVE_LEARNING_MIN_TRADES", "60")))
ADAPTIVE_LEARNING_MIN_OBSERVATION_DAYS = max(7, int(os.getenv("ADAPTIVE_LEARNING_MIN_OBSERVATION_DAYS", "14")))
ADAPTIVE_LEARNING_MIN_OOS_TRADES = max(10, int(os.getenv("ADAPTIVE_LEARNING_MIN_OOS_TRADES", "20")))
ADAPTIVE_LEARNING_WALK_FORWARD_FOLDS = max(2, min(5, int(os.getenv("ADAPTIVE_LEARNING_WALK_FORWARD_FOLDS", "3"))))
ADAPTIVE_LEARNING_EMBARGO_MINUTES = max(0, int(os.getenv("ADAPTIVE_LEARNING_EMBARGO_MINUTES", "30")))
ADAPTIVE_LEARNING_COOLDOWN_HOURS = max(24, int(os.getenv("ADAPTIVE_LEARNING_COOLDOWN_HOURS", "168")))
ADAPTIVE_LEARNING_MIN_NEW_TRADES = max(10, int(os.getenv("ADAPTIVE_LEARNING_MIN_NEW_TRADES", "20")))
ADAPTIVE_LEARNING_MAX_CONFIDENCE_STEP = max(0.01, min(0.10, float(os.getenv("ADAPTIVE_LEARNING_MAX_CONFIDENCE_STEP", "0.05"))))
ADAPTIVE_LEARNING_ACCEPT_SCORE = max(0.50, min(0.90, float(os.getenv("ADAPTIVE_LEARNING_ACCEPT_SCORE", "0.62"))))
ADAPTIVE_LEARNING_OBSERVATION_ONLY = True
VALIDATION_PIPELINE_ENABLED = os.getenv("VALIDATION_PIPELINE_ENABLED", "true").lower() == "true"
VALIDATION_TRAIN_WINDOW = max(30, int(os.getenv("VALIDATION_TRAIN_WINDOW", "60")))
VALIDATION_TEST_WINDOW = max(10, int(os.getenv("VALIDATION_TEST_WINDOW", "20")))
VALIDATION_STEP_SIZE = max(5, int(os.getenv("VALIDATION_STEP_SIZE", "20")))
VALIDATION_MIN_WINDOWS = max(2, int(os.getenv("VALIDATION_MIN_WINDOWS", "3")))
VALIDATION_MIN_OOS_TRADES = max(10, int(os.getenv("VALIDATION_MIN_OOS_TRADES", "20")))
VALIDATION_MONTE_CARLO_SIMS = max(100, min(2000, int(os.getenv("VALIDATION_MONTE_CARLO_SIMS", "300"))))
VALIDATION_PAPER_MIN_TRADES = max(10, int(os.getenv("VALIDATION_PAPER_MIN_TRADES", "30")))
VALIDATION_PAPER_MIN_DAYS = max(7, int(os.getenv("VALIDATION_PAPER_MIN_DAYS", "14")))
VALIDATION_PAPER_MIN_REGIMES = max(1, int(os.getenv("VALIDATION_PAPER_MIN_REGIMES", "2")))
VALIDATION_PAPER_MAX_ENTRY_DEVIATION_R = max(0.10, min(1.50, float(os.getenv("VALIDATION_PAPER_MAX_ENTRY_DEVIATION_R", "0.50"))))
VALIDATION_BACKTEST_LIVE_EXPECTANCY_TOL = max(0.10, min(1.00, float(os.getenv("VALIDATION_BACKTEST_LIVE_EXPECTANCY_TOL", "0.50"))))
VALIDATION_MAX_STATE = "READY_FOR_REVIEW"
VALIDATION_AUTO_DEPLOY = False
DEPLOYMENT_MANAGER_ENABLED = os.getenv("DEPLOYMENT_MANAGER_ENABLED","true").lower()=="true"
DEPLOYMENT_MIN_VALIDATION_SCORE = max(.50,min(.95,float(os.getenv("DEPLOYMENT_MIN_VALIDATION_SCORE",".75"))))
DEPLOYMENT_CANARY_MIN_TRADES = max(5,int(os.getenv("DEPLOYMENT_CANARY_MIN_TRADES","10")))
DEPLOYMENT_LIMITED_MIN_TRADES = max(10,int(os.getenv("DEPLOYMENT_LIMITED_MIN_TRADES","25")))
DEPLOYMENT_MIN_LIVE_DAYS = max(1,int(os.getenv("DEPLOYMENT_MIN_LIVE_DAYS","3")))
DEPLOYMENT_MIN_LIVE_REGIMES = max(1,int(os.getenv("DEPLOYMENT_MIN_LIVE_REGIMES","1")))
DEPLOYMENT_PROMOTION_COOLDOWN_HOURS = max(12,int(os.getenv("DEPLOYMENT_PROMOTION_COOLDOWN_HOURS","72")))
DEPLOYMENT_MAX_PROMOTIONS_PER_7D = max(1,int(os.getenv("DEPLOYMENT_MAX_PROMOTIONS_PER_7D","2")))
DEPLOYMENT_MAX_EXPOSURE_INCREASE = max(.05,min(.25,float(os.getenv("DEPLOYMENT_MAX_EXPOSURE_INCREASE",".25"))))
DEPLOYMENT_CANARY_MAX_DAILY_RISK = max(.001,min(.02,float(os.getenv("DEPLOYMENT_CANARY_MAX_DAILY_RISK",".005"))))
DEPLOYMENT_CANARY_MAX_DRAWDOWN = max(.005,min(.10,float(os.getenv("DEPLOYMENT_CANARY_MAX_DRAWDOWN",".02"))))
DEPLOYMENT_CANARY_MAX_CONSECUTIVE_LOSSES = max(2,int(os.getenv("DEPLOYMENT_CANARY_MAX_CONSECUTIVE_LOSSES","3")))
DEPLOYMENT_CANARY_MAX_STAGE_DAYS = max(3,int(os.getenv("DEPLOYMENT_CANARY_MAX_STAGE_DAYS","30")))
DEPLOYMENT_MAX_SLIPPAGE_PIPS = max(.5,float(os.getenv("DEPLOYMENT_MAX_SLIPPAGE_PIPS","2.5")))
DEPLOYMENT_MAX_LATENCY_SECONDS = max(.5,float(os.getenv("DEPLOYMENT_MAX_LATENCY_SECONDS","4.0")))
DEPLOYMENT_AUTO_PROMOTION = False
PRODUCTION_READINESS_ENABLED = os.getenv("PRODUCTION_READINESS_ENABLED","true").lower()=="true"
PRODUCTION_DRY_RUN_MODE = os.getenv("PRODUCTION_DRY_RUN_MODE","true").lower()=="true"
PRODUCTION_STEP14_REPORT_PATH = os.getenv("PRODUCTION_STEP14_REPORT_PATH","/mnt/data/market-alert-v3.25-ensemble-shadow/certification-evidence/step14-integration-report-v3.25.json")
PRODUCTION_MINIMAL_RISK_MULTIPLIER = max(0.01,min(0.10,float(os.getenv("PRODUCTION_MINIMAL_RISK_MULTIPLIER","0.05"))))
PRODUCTION_LIMITED_RISK_MULTIPLIER = max(PRODUCTION_MINIMAL_RISK_MULTIPLIER,min(0.25,float(os.getenv("PRODUCTION_LIMITED_RISK_MULTIPLIER","0.10"))))
PRODUCTION_CONTROLLED_RISK_MULTIPLIER = max(PRODUCTION_LIMITED_RISK_MULTIPLIER,min(0.50,float(os.getenv("PRODUCTION_CONTROLLED_RISK_MULTIPLIER","0.25"))))
PRODUCTION_MINIMAL_MIN_TRADES = max(10,int(os.getenv("PRODUCTION_MINIMAL_MIN_TRADES","10")))
PRODUCTION_MINIMAL_MIN_DAYS = max(3,int(os.getenv("PRODUCTION_MINIMAL_MIN_DAYS","5")))
PRODUCTION_LIMITED_MIN_TRADES = max(25,int(os.getenv("PRODUCTION_LIMITED_MIN_TRADES","25")))
PRODUCTION_LIMITED_MIN_DAYS = max(7,int(os.getenv("PRODUCTION_LIMITED_MIN_DAYS","10")))
PRODUCTION_CONTROLLED_MIN_TRADES = max(50,int(os.getenv("PRODUCTION_CONTROLLED_MIN_TRADES","50")))
PRODUCTION_CONTROLLED_MIN_DAYS = max(14,int(os.getenv("PRODUCTION_CONTROLLED_MIN_DAYS","20")))
SMART_EXECUTION_ENABLED = os.getenv("SMART_EXECUTION_ENABLED","true").lower()=="true"
SMART_EXECUTION_MODE = os.getenv("SMART_EXECUTION_MODE","SHADOW").strip().upper()
SMART_EXECUTION_SHADOW_MODE = True  # Step 16 enforcement boundary: recommendations only.
# Step 16 starts in SHADOW. No server-side live policy authority is granted here.
SMART_EXECUTION_POLICY_AUTHORITY = False
SMART_EXECUTION_MAX_SNAPSHOT_AGE_SECONDS = max(1,int(os.getenv("SMART_EXECUTION_MAX_SNAPSHOT_AGE_SECONDS","5")))
SMART_EXECUTION_INTENT_TTL_SECONDS = max(5,int(os.getenv("SMART_EXECUTION_INTENT_TTL_SECONDS","60")))
SMART_EXECUTION_DEFAULT_MAX_SLIPPAGE_BPS = max(.1,float(os.getenv("SMART_EXECUTION_DEFAULT_MAX_SLIPPAGE_BPS","8")))
SMART_EXECUTION_LIQUIDITY_PARTICIPATION = max(.01,min(1.0,float(os.getenv("SMART_EXECUTION_LIQUIDITY_PARTICIPATION","0.25"))))
SMART_EXECUTION_SLICE_THRESHOLD_UNITS = max(1,float(os.getenv("SMART_EXECUTION_SLICE_THRESHOLD_UNITS","1000")))
SMART_EXECUTION_SLICE_SIZE_UNITS = max(1,float(os.getenv("SMART_EXECUTION_SLICE_SIZE_UNITS","200")))
SMART_EXECUTION_MIN_HISTORY_SAMPLES = max(5,int(os.getenv("SMART_EXECUTION_MIN_HISTORY_SAMPLES","20")))
SMART_EXECUTION_DEGRADATION_MIN_SAMPLES = max(5,int(os.getenv("SMART_EXECUTION_DEGRADATION_MIN_SAMPLES","10")))
ENSEMBLE_ENABLED = os.getenv("ENSEMBLE_ENABLED","true").lower()=="true"
ENSEMBLE_MODE = "SHADOW"  # Step 17 enforcement boundary: observation only.
ENSEMBLE_POLICY_AUTHORITY = False
ENSEMBLE_MAX_MODEL_WEIGHT = max(.10,min(.60,float(os.getenv("ENSEMBLE_MAX_MODEL_WEIGHT","0.40"))))
ENSEMBLE_MAX_FAMILY_WEIGHT = max(ENSEMBLE_MAX_MODEL_WEIGHT,min(.80,float(os.getenv("ENSEMBLE_MAX_FAMILY_WEIGHT","0.55"))))
ENSEMBLE_MIN_SAMPLE_SIZE = max(10,int(os.getenv("ENSEMBLE_MIN_SAMPLE_SIZE","30")))
ENSEMBLE_CORRELATION_THRESHOLD = max(.50,min(.95,float(os.getenv("ENSEMBLE_CORRELATION_THRESHOLD","0.75"))))
ENSEMBLE_WEIGHT_CHANGE_LIMIT = max(.01,min(.25,float(os.getenv("ENSEMBLE_WEIGHT_CHANGE_LIMIT","0.10"))))
ENSEMBLE_WEIGHT_COOLDOWN_HOURS = max(6,int(os.getenv("ENSEMBLE_WEIGHT_COOLDOWN_HOURS","24")))
ENSEMBLE_MIN_OBSERVATION_HOURS = max(6,int(os.getenv("ENSEMBLE_MIN_OBSERVATION_HOURS","24")))
ENSEMBLE_SIGNAL_TTL_SECONDS = max(30,int(os.getenv("ENSEMBLE_SIGNAL_TTL_SECONDS","300")))
CAPITAL_ALLOCATION_ENABLED = os.getenv("CAPITAL_ALLOCATION_ENABLED","true").lower()=="true"
CAPITAL_ALLOCATION_SHADOW_MODE = True
CAPITAL_ALLOCATION_MAX_STRATEGY = max(.01,min(.25,float(os.getenv("CAPITAL_ALLOCATION_MAX_STRATEGY","0.25"))))
CAPITAL_ALLOCATION_MAX_FAMILY = max(.05,min(.40,float(os.getenv("CAPITAL_ALLOCATION_MAX_FAMILY","0.40"))))
CAPITAL_ALLOCATION_MAX_SYMBOL = max(.01,min(.25,float(os.getenv("CAPITAL_ALLOCATION_MAX_SYMBOL","0.25"))))
CAPITAL_ALLOCATION_MAX_ASSET = max(.05,min(.40,float(os.getenv("CAPITAL_ALLOCATION_MAX_ASSET","0.40"))))
CAPITAL_ALLOCATION_MAX_DIRECTIONAL = max(.10,min(.65,float(os.getenv("CAPITAL_ALLOCATION_MAX_DIRECTIONAL","0.65"))))
CAPITAL_ALLOCATION_MAX_CLUSTER = max(.05,min(.35,float(os.getenv("CAPITAL_ALLOCATION_MAX_CLUSTER","0.35"))))
CAPITAL_ALLOCATION_MAX_CHANGE = max(.005,min(.05,float(os.getenv("CAPITAL_ALLOCATION_MAX_CHANGE","0.05"))))
CAPITAL_ALLOCATION_COOLDOWN_HOURS = max(1,int(os.getenv("CAPITAL_ALLOCATION_COOLDOWN_HOURS","24")))
CAPITAL_ALLOCATION_REBALANCE_THRESHOLD = max(.001,min(.02,float(os.getenv("CAPITAL_ALLOCATION_REBALANCE_THRESHOLD","0.02"))))
CAPITAL_ALLOCATION_HEAT_LIMIT = max(.20,min(.80,float(os.getenv("CAPITAL_ALLOCATION_HEAT_LIMIT","0.80"))))
OBSERVABILITY_ENABLED = os.getenv("OBSERVABILITY_ENABLED","true").lower()=="true"
OBSERVABILITY_ALERT_COOLDOWN_SECONDS = max(30,int(os.getenv("OBSERVABILITY_ALERT_COOLDOWN_SECONDS","900")))
OBSERVABILITY_MARKET_STALE_SECONDS = max(60,int(os.getenv("OBSERVABILITY_MARKET_STALE_SECONDS","180")))
OBSERVABILITY_BROKER_STALE_SECONDS = max(60,int(os.getenv("OBSERVABILITY_BROKER_STALE_SECONDS","180")))
OBSERVABILITY_HEARTBEAT_STALE_SECONDS = max(60,int(os.getenv("OBSERVABILITY_HEARTBEAT_STALE_SECONDS","180")))
OBSERVABILITY_LOOP_INTERVAL_SECONDS = max(2,int(os.getenv("OBSERVABILITY_LOOP_INTERVAL_SECONDS","5")))
OBSERVABILITY_LOOP_LAG_WARNING_MS = max(50,float(os.getenv("OBSERVABILITY_LOOP_LAG_WARNING_MS","250")))
OBSERVABILITY_LOOP_LAG_CRITICAL_MS = max(OBSERVABILITY_LOOP_LAG_WARNING_MS,float(os.getenv("OBSERVABILITY_LOOP_LAG_CRITICAL_MS","1000")))
OBSERVABILITY_DB_LATENCY_WARNING_MS = max(10,float(os.getenv("OBSERVABILITY_DB_LATENCY_WARNING_MS","100")))
OBSERVABILITY_BROKER_LATENCY_WARNING_MS = max(100,float(os.getenv("OBSERVABILITY_BROKER_LATENCY_WARNING_MS","2000")))
OBSERVABILITY_SIGNAL_SILENCE_HOURS = max(1,float(os.getenv("OBSERVABILITY_SIGNAL_SILENCE_HOURS","24")))
OBSERVABILITY_REGIME_STATIC_HOURS = max(4,float(os.getenv("OBSERVABILITY_REGIME_STATIC_HOURS","36")))
OBSERVABILITY_STARTUP_BLOCK_TRADING = os.getenv("OBSERVABILITY_STARTUP_BLOCK_TRADING","true").lower()=="true"
OBSERVABILITY_CRITICAL_FAILSAFE_ENABLED = os.getenv("OBSERVABILITY_CRITICAL_FAILSAFE_ENABLED","false").lower()=="true"
RECOVERY_MANAGER_ENABLED = os.getenv("RECOVERY_MANAGER_ENABLED","true").lower()=="true"
RECOVERY_USE_CLIENT_EXTENSIONS = os.getenv("RECOVERY_USE_CLIENT_EXTENSIONS","true").lower()=="true"
RECOVERY_CIRCUIT_FAILURE_THRESHOLD = max(2,int(os.getenv("RECOVERY_CIRCUIT_FAILURE_THRESHOLD","3")))
RECOVERY_CIRCUIT_OPEN_SECONDS = max(5.0,float(os.getenv("RECOVERY_CIRCUIT_OPEN_SECONDS","20")))
RECOVERY_REQUEST_MIN_INTERVAL_MS = max(10.0,float(os.getenv("RECOVERY_REQUEST_MIN_INTERVAL_MS","80")))
RECOVERY_MAX_READ_RETRIES = max(0,min(8,int(os.getenv("RECOVERY_MAX_READ_RETRIES","4"))))
RECOVERY_BACKOFF_BASE_SECONDS = max(.05,float(os.getenv("RECOVERY_BACKOFF_BASE_SECONDS",".4")))
RECOVERY_BACKOFF_CAP_SECONDS = max(1.0,float(os.getenv("RECOVERY_BACKOFF_CAP_SECONDS","8")))
RECOVERY_RECONCILE_INTERVAL_SECONDS = max(30,int(os.getenv("RECOVERY_RECONCILE_INTERVAL_SECONDS","120")))
RECOVERY_MARKET_DATA_MAX_AGE_SECONDS = max(30,int(os.getenv("RECOVERY_MARKET_DATA_MAX_AGE_SECONDS","180")))
RECOVERY_BLOCK_ADAPTIVE_LEARNING_COMPROMISED = os.getenv("RECOVERY_BLOCK_ADAPTIVE_LEARNING_COMPROMISED","true").lower()=="true"
RECOVERY_PRACTICE_ORPHAN_QUARANTINE = os.getenv("RECOVERY_PRACTICE_ORPHAN_QUARANTINE", "true").lower() == "true"
RECOVERY_MAX_QUOTE_AGE_SECONDS = max(2,float(os.getenv("RECOVERY_MAX_QUOTE_AGE_SECONDS","10")))
RECOVERY_MAX_SPREAD_PIPS = max(.5,float(os.getenv("RECOVERY_MAX_SPREAD_PIPS","5")))
RECOVERY_MAX_PRICE_DEVIATION_PIPS = max(2,float(os.getenv("RECOVERY_MAX_PRICE_DEVIATION_PIPS","20")))
SECURITY_ACTORS_JSON = os.getenv("SECURITY_ACTORS_JSON","{}")
SECURITY_ALLOW_UNAUTHENTICATED_READS = os.getenv("SECURITY_ALLOW_UNAUTHENTICATED_READS","false").lower()=="true"
BROKER_ACCOUNT_VERIFIED = os.getenv("BROKER_ACCOUNT_VERIFIED","false").lower()=="true"
SECURITY_REQUIRE_TWO_CRITICAL_APPROVALS = True
SECURITY_STARTUP_FAIL_CLOSED = os.getenv("SECURITY_STARTUP_FAIL_CLOSED","true").lower()=="true"
SYSTEM_EVALUATION_ENABLED = os.getenv("SYSTEM_EVALUATION_ENABLED","true").lower()=="true"
SYSTEM_EVALUATION_MIN_SAMPLES = max(5,int(os.getenv("SYSTEM_EVALUATION_MIN_SAMPLES","20")))
SYSTEM_EVALUATION_PERIOD_HOURS = max(1,int(os.getenv("SYSTEM_EVALUATION_PERIOD_HOURS","24")))
SYSTEM_EVALUATION_TRADING_WEIGHT = max(0.0,float(os.getenv("SYSTEM_EVALUATION_TRADING_WEIGHT","0.30")))
SYSTEM_EVALUATION_RISK_WEIGHT = max(0.0,float(os.getenv("SYSTEM_EVALUATION_RISK_WEIGHT","0.30")))
SYSTEM_EVALUATION_OPERATIONAL_WEIGHT = max(0.0,float(os.getenv("SYSTEM_EVALUATION_OPERATIONAL_WEIGHT","0.25")))
SYSTEM_EVALUATION_STABILITY_WEIGHT = max(0.0,float(os.getenv("SYSTEM_EVALUATION_STABILITY_WEIGHT","0.15")))
GOVERNANCE_ENABLED = os.getenv("GOVERNANCE_ENABLED","true").lower()=="true"
GOVERNANCE_MODE = os.getenv("GOVERNANCE_MODE","SHADOW").strip().upper()
GOVERNANCE_EVALUATION_INTERVAL_MINUTES = max(5,int(os.getenv("GOVERNANCE_EVALUATION_INTERVAL_MINUTES","60")))
GOVERNANCE_MIN_STABILITY_HOURS = max(1,int(os.getenv("GOVERNANCE_MIN_STABILITY_HOURS","72")))
GOVERNANCE_LIMITED_REVIEW_HOURS = max(1,int(os.getenv("GOVERNANCE_LIMITED_REVIEW_HOURS","48")))
GOVERNANCE_MAX_MAJOR_CHANGES_7D = max(1,int(os.getenv("GOVERNANCE_MAX_MAJOR_CHANGES_7D","3")))
GOVERNANCE_MAX_STRATEGY_CHANGES_7D = max(1,int(os.getenv("GOVERNANCE_MAX_STRATEGY_CHANGES_7D","5")))
GOVERNANCE_MAX_PARAMETER_CHANGES_7D = max(1,int(os.getenv("GOVERNANCE_MAX_PARAMETER_CHANGES_7D","8")))
GOVERNANCE_MAX_DEPLOYMENTS_7D = max(1,int(os.getenv("GOVERNANCE_MAX_DEPLOYMENTS_7D","3")))
GOVERNANCE_MAX_PROMOTIONS_7D = max(1,int(os.getenv("GOVERNANCE_MAX_PROMOTIONS_7D","2")))
GOVERNANCE_MAX_GLOBAL_CHANGES_7D = max(1,int(os.getenv("GOVERNANCE_MAX_GLOBAL_CHANGES_7D","10")))
GOVERNANCE_META_RISK_HIGH = max(40.0,min(90.0,float(os.getenv("GOVERNANCE_META_RISK_HIGH","65"))))
GOVERNANCE_META_RISK_CRITICAL = max(GOVERNANCE_META_RISK_HIGH,min(100.0,float(os.getenv("GOVERNANCE_META_RISK_CRITICAL","82"))))
GOVERNANCE_DECISION_FRESHNESS_HOURS = max(1,int(os.getenv("GOVERNANCE_DECISION_FRESHNESS_HOURS","6")))
OBSERVABILITY_DRAWDOWN_WARNING_FRACTION = max(.001,min(RISK_DRAWDOWN_STOP,float(os.getenv("OBSERVABILITY_DRAWDOWN_WARNING_FRACTION",str(RISK_DRAWDOWN_WARN*.8)))))
OBSERVABILITY_RISK_CONSTANT_WINDOW = max(5,int(os.getenv("OBSERVABILITY_RISK_CONSTANT_WINDOW","20")))
MARKET_REGIME_ENABLED = os.getenv("MARKET_REGIME_ENABLED", "true").lower() == "true"
MARKET_REGIME_LOG_CHANGES_ONLY = os.getenv("MARKET_REGIME_LOG_CHANGES_ONLY", "true").lower() == "true"
MARKET_REGIME_MIN_CANDLES = max(40, int(os.getenv("MARKET_REGIME_MIN_CANDLES", "60")))
MARKET_REGIME_HIGH_VOL_RATIO = max(1.20, float(os.getenv("MARKET_REGIME_HIGH_VOL_RATIO", "1.45")))
MARKET_REGIME_LOW_VOL_RATIO = min(0.90, max(0.30, float(os.getenv("MARKET_REGIME_LOW_VOL_RATIO", "0.70"))))
MARKET_REGIME_ABNORMAL_VOL_RATIO = max(MARKET_REGIME_HIGH_VOL_RATIO + 0.5, float(os.getenv("MARKET_REGIME_ABNORMAL_VOL_RATIO", "2.60")))
MARKET_REGIME_TREND_THRESHOLD = max(0.25, min(0.80, float(os.getenv("MARKET_REGIME_TREND_THRESHOLD", "0.48"))))
MARKET_REGIME_RANGE_THRESHOLD = max(0.15, min(0.60, float(os.getenv("MARKET_REGIME_RANGE_THRESHOLD", "0.34"))))
MARKET_TZ = ZoneInfo("America/New_York")
EXTERNAL_INCLUDE_SHADOW = os.getenv("EXTERNAL_INCLUDE_SHADOW", "true").lower() == "true"
EXTERNAL_SHADOW_BASELINE_WEIGHT = max(0.10, min(1.0, float(os.getenv("EXTERNAL_SHADOW_BASELINE_WEIGHT", "0.50"))))
EXTERNAL_SHADOW_VARIANT_WEIGHT = max(0.05, min(EXTERNAL_SHADOW_BASELINE_WEIGHT, float(os.getenv("EXTERNAL_SHADOW_VARIANT_WEIGHT", "0.25"))))
EXTERNAL_PROMOTION_MIN_CANONICAL = max(10, int(os.getenv("EXTERNAL_PROMOTION_MIN_CANONICAL", "20")))
EXECUTION_MIN_CONFIDENCE = max(0.50, min(0.95, float(os.getenv("EXECUTION_MIN_CONFIDENCE", "0.65"))))
NY = ZoneInfo("America/New_York")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
for _handler in logging.getLogger().handlers:
    _handler.addFilter(RedactingFilter())
log = logging.getLogger("market-alert")
app = FastAPI(title="Market Alert V3.35 — Historical Execution + OOS Research Runtime")
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
    "system_ready": False,
    "startup_health": None,
    "observability": {"enabled": OBSERVABILITY_ENABLED, "version": VERSION_TAG, "last_refresh": None, "last_broker_snapshot": None},
}

FEATURE_COLUMNS = [
    "direction_buy", "technical_score", "final_score", "m15_gap_atr", "m15_slope_atr",
    "m5_momentum", "pullbacks", "second_pullback", "m1_momentum", "m1_confirm",
    "extension_atr", "volatility_ratio", "rr_raw", "session_ok", "news_confirm",
    "news_contradict", "blocked", "hour_ny"
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


SECURITY_CONFIG_SCHEMA = {
    # Hard risk limits: managed configuration can only stay equal or become MORE restrictive
    # than the current code/environment ceilings. V3.19 does not permit increases.
    "risk.base_fraction":{"type":"float","min":0.0001,"max":RISK_BASE_FRACTION,"hard_ceiling":RISK_BASE_FRACTION,"risk_level":"CRITICAL"},
    "risk.max_trade_fraction":{"type":"float","min":0.0001,"max":RISK_MAX_TRADE_FRACTION,"hard_ceiling":RISK_MAX_TRADE_FRACTION,"risk_level":"CRITICAL"},
    "risk.max_strategy_fraction":{"type":"float","min":0.0001,"max":RISK_MAX_STRATEGY_FRACTION,"hard_ceiling":RISK_MAX_STRATEGY_FRACTION,"risk_level":"CRITICAL"},
    "risk.max_portfolio_fraction":{"type":"float","min":0.0001,"max":RISK_MAX_PORTFOLIO_FRACTION,"hard_ceiling":RISK_MAX_PORTFOLIO_FRACTION,"risk_level":"CRITICAL"},
    "risk.max_margin_usage":{"type":"float","min":0.01,"max":RISK_MAX_MARGIN_USAGE,"hard_ceiling":RISK_MAX_MARGIN_USAGE,"risk_level":"CRITICAL"},
    "risk.drawdown_warning":{"type":"float","min":0.001,"max":RISK_DRAWDOWN_WARN,"hard_ceiling":RISK_DRAWDOWN_WARN,"risk_level":"HIGH_RISK"},
    "risk.drawdown_stop":{"type":"float","min":0.002,"max":RISK_DRAWDOWN_STOP,"hard_ceiling":RISK_DRAWDOWN_STOP,"risk_level":"CRITICAL"},
    "risk.max_consecutive_losses":{"type":"int","min":1,"max":RISK_MAX_CONSECUTIVE_LOSSES,"hard_ceiling":RISK_MAX_CONSECUTIVE_LOSSES,"risk_level":"HIGH_RISK"},
    "risk.max_correlated_positions":{"type":"int","min":1,"max":RISK_MAX_CORRELATED_POSITIONS,"hard_ceiling":RISK_MAX_CORRELATED_POSITIONS,"risk_level":"CRITICAL"},

    # Deployment gates. Changes cannot silently make gates looser than V3.18 defaults.
    "deployment.min_validation_score":{"type":"float","min":DEPLOYMENT_MIN_VALIDATION_SCORE,"max":0.99,"risk_level":"CRITICAL"},
    "deployment.canary_min_trades":{"type":"int","min":DEPLOYMENT_CANARY_MIN_TRADES,"max":10000,"risk_level":"HIGH_RISK"},
    "deployment.limited_min_trades":{"type":"int","min":DEPLOYMENT_LIMITED_MIN_TRADES,"max":10000,"risk_level":"HIGH_RISK"},
    "deployment.min_live_days":{"type":"int","min":DEPLOYMENT_MIN_LIVE_DAYS,"max":365,"risk_level":"HIGH_RISK"},
    "deployment.promotion_cooldown_hours":{"type":"int","min":DEPLOYMENT_PROMOTION_COOLDOWN_HOURS,"max":8760,"risk_level":"CRITICAL"},
    "deployment.max_promotions_7d":{"type":"int","min":1,"max":DEPLOYMENT_MAX_PROMOTIONS_PER_7D,"hard_ceiling":DEPLOYMENT_MAX_PROMOTIONS_PER_7D,"risk_level":"CRITICAL"},
    "deployment.max_exposure_increase":{"type":"float","min":0.01,"max":DEPLOYMENT_MAX_EXPOSURE_INCREASE,"hard_ceiling":DEPLOYMENT_MAX_EXPOSURE_INCREASE,"risk_level":"CRITICAL"},

    # Non-capital controls.
    "director.active_threshold":{"type":"float","min":0.50,"max":0.95,"risk_level":"MEDIUM_RISK"},
    "director.reduced_threshold":{"type":"float","min":0.30,"max":0.90,"risk_level":"MEDIUM_RISK"},
    "regime.trend_threshold":{"type":"float","min":0.25,"max":0.80,"risk_level":"MEDIUM_RISK"},
    "regime.range_threshold":{"type":"float","min":0.15,"max":0.60,"risk_level":"MEDIUM_RISK"},
    "adaptive_learning.min_trades":{"type":"int","min":ADAPTIVE_LEARNING_MIN_TRADES,"max":100000,"risk_level":"HIGH_RISK"},
    "adaptive_learning.cooldown_hours":{"type":"int","min":ADAPTIVE_LEARNING_COOLDOWN_HOURS,"max":8760,"risk_level":"HIGH_RISK"},
    "observability.alert_cooldown_seconds":{"type":"int","min":30,"max":3600,"risk_level":"LOW_RISK"},
    "observability.loop_interval_seconds":{"type":"int","min":2,"max":60,"risk_level":"LOW_RISK"},
    "system_evaluation.min_samples":{"type":"int","min":5,"max":1000,"risk_level":"LOW_RISK"},
    "system_evaluation.period_hours":{"type":"int","min":1,"max":168,"risk_level":"LOW_RISK"},
    "system_evaluation.trading_weight":{"type":"float","min":0.0,"max":1.0,"risk_level":"MEDIUM_RISK"},
    "system_evaluation.risk_weight":{"type":"float","min":0.0,"max":1.0,"risk_level":"MEDIUM_RISK"},
    "system_evaluation.operational_weight":{"type":"float","min":0.0,"max":1.0,"risk_level":"MEDIUM_RISK"},
    "system_evaluation.stability_weight":{"type":"float","min":0.0,"max":1.0,"risk_level":"MEDIUM_RISK"},
    "governance.mode":{"type":"str","allowed":["SHADOW","ADVISORY","PARTIAL_ENFORCEMENT","FULL_POLICY_ENFORCEMENT"],"risk_level":"CRITICAL"},
    "governance.min_stability_hours":{"type":"int","min":1,"max":720,"risk_level":"HIGH_RISK"},
    "governance.limited_review_hours":{"type":"int","min":1,"max":720,"risk_level":"HIGH_RISK"},
    "governance.max_major_changes_7d":{"type":"int","min":1,"max":12,"risk_level":"CRITICAL"},
    "governance.max_strategy_changes_7d":{"type":"int","min":1,"max":20,"risk_level":"HIGH_RISK"},
    "governance.max_parameter_changes_7d":{"type":"int","min":1,"max":30,"risk_level":"HIGH_RISK"},
    "governance.max_deployments_7d":{"type":"int","min":1,"max":10,"risk_level":"CRITICAL"},
    "governance.max_promotions_7d":{"type":"int","min":1,"max":8,"risk_level":"CRITICAL"},
    "governance.max_global_changes_7d":{"type":"int","min":1,"max":30,"risk_level":"HIGH_RISK"},
    "governance.meta_risk_high":{"type":"float","min":40.0,"max":90.0,"risk_level":"HIGH_RISK"},
    "governance.meta_risk_critical":{"type":"float","min":60.0,"max":100.0,"risk_level":"CRITICAL"},
    "governance.decision_freshness_hours":{"type":"int","min":1,"max":72,"risk_level":"MEDIUM_RISK"},
    "smart_execution.mode":{"type":"str","allowed":["SHADOW","PAPER","CANARY","LIMITED_EXECUTION","PRODUCTION_EXECUTION"],"risk_level":"CRITICAL"},
    "smart_execution.max_snapshot_age_seconds":{"type":"int","min":1,"max":60,"risk_level":"HIGH_RISK"},
    "smart_execution.intent_ttl_seconds":{"type":"int","min":5,"max":600,"risk_level":"HIGH_RISK"},
    "smart_execution.max_slippage_bps":{"type":"float","min":0.1,"max":50.0,"risk_level":"HIGH_RISK"},
    "smart_execution.liquidity_participation":{"type":"float","min":0.01,"max":1.0,"risk_level":"HIGH_RISK"},
    "smart_execution.slice_threshold_units":{"type":"float","min":1,"max":10000000,"risk_level":"MEDIUM_RISK"},
    "smart_execution.slice_size_units":{"type":"float","min":1,"max":10000000,"risk_level":"MEDIUM_RISK"},
    # Ensemble influence controls. Managed config may become more conservative but cannot
    # silently loosen the V3.25 correlation/evidence caps.
    "ensemble.max_model_weight":{"type":"float","min":0.05,"max":ENSEMBLE_MAX_MODEL_WEIGHT,"hard_ceiling":ENSEMBLE_MAX_MODEL_WEIGHT,"risk_level":"HIGH_RISK"},
    "ensemble.max_family_weight":{"type":"float","min":0.10,"max":ENSEMBLE_MAX_FAMILY_WEIGHT,"hard_ceiling":ENSEMBLE_MAX_FAMILY_WEIGHT,"risk_level":"HIGH_RISK"},
    "ensemble.min_sample_size":{"type":"int","min":ENSEMBLE_MIN_SAMPLE_SIZE,"max":100000,"risk_level":"HIGH_RISK"},
    "ensemble.correlation_threshold":{"type":"float","min":0.40,"max":ENSEMBLE_CORRELATION_THRESHOLD,"hard_ceiling":ENSEMBLE_CORRELATION_THRESHOLD,"risk_level":"HIGH_RISK"},
    "ensemble.weight_change_limit":{"type":"float","min":0.01,"max":ENSEMBLE_WEIGHT_CHANGE_LIMIT,"hard_ceiling":ENSEMBLE_WEIGHT_CHANGE_LIMIT,"risk_level":"HIGH_RISK"},
    "ensemble.weight_cooldown_hours":{"type":"int","min":ENSEMBLE_WEIGHT_COOLDOWN_HOURS,"max":8760,"risk_level":"HIGH_RISK"},
    "ensemble.min_observation_hours":{"type":"int","min":ENSEMBLE_MIN_OBSERVATION_HOURS,"max":8760,"risk_level":"HIGH_RISK"},
    "ensemble.signal_ttl_seconds":{"type":"int","min":30,"max":ENSEMBLE_SIGNAL_TTL_SECONDS,"hard_ceiling":ENSEMBLE_SIGNAL_TTL_SECONDS,"risk_level":"HIGH_RISK"},
    "allocation.max_strategy":{"type":"float","min":0.01,"max":CAPITAL_ALLOCATION_MAX_STRATEGY,"hard_ceiling":CAPITAL_ALLOCATION_MAX_STRATEGY,"risk_level":"CRITICAL"},
    "allocation.max_family":{"type":"float","min":0.05,"max":CAPITAL_ALLOCATION_MAX_FAMILY,"hard_ceiling":CAPITAL_ALLOCATION_MAX_FAMILY,"risk_level":"CRITICAL"},
    "allocation.max_symbol":{"type":"float","min":0.01,"max":CAPITAL_ALLOCATION_MAX_SYMBOL,"hard_ceiling":CAPITAL_ALLOCATION_MAX_SYMBOL,"risk_level":"CRITICAL"},
    "allocation.max_asset":{"type":"float","min":0.05,"max":CAPITAL_ALLOCATION_MAX_ASSET,"hard_ceiling":CAPITAL_ALLOCATION_MAX_ASSET,"risk_level":"CRITICAL"},
    "allocation.max_directional":{"type":"float","min":0.10,"max":CAPITAL_ALLOCATION_MAX_DIRECTIONAL,"hard_ceiling":CAPITAL_ALLOCATION_MAX_DIRECTIONAL,"risk_level":"CRITICAL"},
    "allocation.max_cluster":{"type":"float","min":0.05,"max":CAPITAL_ALLOCATION_MAX_CLUSTER,"hard_ceiling":CAPITAL_ALLOCATION_MAX_CLUSTER,"risk_level":"CRITICAL"},
    "allocation.max_change":{"type":"float","min":0.005,"max":CAPITAL_ALLOCATION_MAX_CHANGE,"hard_ceiling":CAPITAL_ALLOCATION_MAX_CHANGE,"risk_level":"HIGH_RISK"},
    "allocation.cooldown_hours":{"type":"int","min":CAPITAL_ALLOCATION_COOLDOWN_HOURS,"max":8760,"risk_level":"HIGH_RISK"},
    "allocation.rebalance_threshold":{"type":"float","min":CAPITAL_ALLOCATION_REBALANCE_THRESHOLD,"max":0.20,"risk_level":"HIGH_RISK"},
    "allocation.heat_limit":{"type":"float","min":0.20,"max":CAPITAL_ALLOCATION_HEAT_LIMIT,"hard_ceiling":CAPITAL_ALLOCATION_HEAT_LIMIT,"risk_level":"CRITICAL"},
    "production.minimal_risk_multiplier":{"type":"float","min":0.01,"max":PRODUCTION_MINIMAL_RISK_MULTIPLIER,"hard_ceiling":PRODUCTION_MINIMAL_RISK_MULTIPLIER,"risk_level":"CRITICAL"},
    "production.limited_risk_multiplier":{"type":"float","min":0.01,"max":PRODUCTION_LIMITED_RISK_MULTIPLIER,"hard_ceiling":PRODUCTION_LIMITED_RISK_MULTIPLIER,"risk_level":"CRITICAL"},
    "production.controlled_risk_multiplier":{"type":"float","min":0.01,"max":PRODUCTION_CONTROLLED_RISK_MULTIPLIER,"hard_ceiling":PRODUCTION_CONTROLLED_RISK_MULTIPLIER,"risk_level":"CRITICAL"},
    "production.minimal_min_trades":{"type":"int","min":PRODUCTION_MINIMAL_MIN_TRADES,"max":10000,"risk_level":"HIGH_RISK"},
    "production.minimal_min_days":{"type":"int","min":PRODUCTION_MINIMAL_MIN_DAYS,"max":365,"risk_level":"HIGH_RISK"},
    "production.limited_min_trades":{"type":"int","min":PRODUCTION_LIMITED_MIN_TRADES,"max":10000,"risk_level":"HIGH_RISK"},
    "production.limited_min_days":{"type":"int","min":PRODUCTION_LIMITED_MIN_DAYS,"max":365,"risk_level":"HIGH_RISK"},
    "production.controlled_min_trades":{"type":"int","min":PRODUCTION_CONTROLLED_MIN_TRADES,"max":10000,"risk_level":"HIGH_RISK"},
    "production.controlled_min_days":{"type":"int","min":PRODUCTION_CONTROLLED_MIN_DAYS,"max":365,"risk_level":"HIGH_RISK"},
    "execution.auto_trade":{"type":"bool","risk_level":"CRITICAL"},
    "execution.trade_units":{"type":"int","min":1,"max":UNITS,"hard_ceiling":UNITS,"risk_level":"CRITICAL"},

    # Dynamic strategy/research recommendations are versioned and reviewed,
    # but are not allowed to touch secrets, permissions, hard risk limits or deployment authority.
    "strategy.*":{"type":"any","risk_level":"HIGH_RISK"},
    "research_rule.*":{"type":"bool","risk_level":"HIGH_RISK"},

    # Secrets are explicitly outside in-app Change Management.
    "broker.credentials":{"type":"str","secret":True,"risk_level":"CRITICAL"},
    "security.credentials":{"type":"str","secret":True,"risk_level":"CRITICAL"},
}

SECURITY_INITIAL_CONFIG = {
    "risk.base_fraction":RISK_BASE_FRACTION,
    "risk.max_trade_fraction":RISK_MAX_TRADE_FRACTION,
    "risk.max_strategy_fraction":RISK_MAX_STRATEGY_FRACTION,
    "risk.max_portfolio_fraction":RISK_MAX_PORTFOLIO_FRACTION,
    "risk.max_margin_usage":RISK_MAX_MARGIN_USAGE,
    "risk.drawdown_warning":RISK_DRAWDOWN_WARN,
    "risk.drawdown_stop":RISK_DRAWDOWN_STOP,
    "risk.max_consecutive_losses":RISK_MAX_CONSECUTIVE_LOSSES,
    "risk.max_correlated_positions":RISK_MAX_CORRELATED_POSITIONS,
    "deployment.min_validation_score":DEPLOYMENT_MIN_VALIDATION_SCORE,
    "deployment.canary_min_trades":DEPLOYMENT_CANARY_MIN_TRADES,
    "deployment.limited_min_trades":DEPLOYMENT_LIMITED_MIN_TRADES,
    "deployment.min_live_days":DEPLOYMENT_MIN_LIVE_DAYS,
    "deployment.promotion_cooldown_hours":DEPLOYMENT_PROMOTION_COOLDOWN_HOURS,
    "deployment.max_promotions_7d":DEPLOYMENT_MAX_PROMOTIONS_PER_7D,
    "deployment.max_exposure_increase":DEPLOYMENT_MAX_EXPOSURE_INCREASE,
    "director.active_threshold":AI_DIRECTOR_ACTIVE_THRESHOLD,
    "director.reduced_threshold":AI_DIRECTOR_REDUCED_THRESHOLD,
    "regime.trend_threshold":MARKET_REGIME_TREND_THRESHOLD,
    "regime.range_threshold":MARKET_REGIME_RANGE_THRESHOLD,
    "adaptive_learning.min_trades":ADAPTIVE_LEARNING_MIN_TRADES,
    "adaptive_learning.cooldown_hours":ADAPTIVE_LEARNING_COOLDOWN_HOURS,
    "observability.alert_cooldown_seconds":OBSERVABILITY_ALERT_COOLDOWN_SECONDS,
    "observability.loop_interval_seconds":OBSERVABILITY_LOOP_INTERVAL_SECONDS,
    "system_evaluation.min_samples":SYSTEM_EVALUATION_MIN_SAMPLES,
    "system_evaluation.period_hours":SYSTEM_EVALUATION_PERIOD_HOURS,
    "system_evaluation.trading_weight":SYSTEM_EVALUATION_TRADING_WEIGHT,
    "system_evaluation.risk_weight":SYSTEM_EVALUATION_RISK_WEIGHT,
    "system_evaluation.operational_weight":SYSTEM_EVALUATION_OPERATIONAL_WEIGHT,
    "system_evaluation.stability_weight":SYSTEM_EVALUATION_STABILITY_WEIGHT,
    "governance.mode":GOVERNANCE_MODE if GOVERNANCE_MODE in ("SHADOW","ADVISORY","PARTIAL_ENFORCEMENT","FULL_POLICY_ENFORCEMENT") else "SHADOW",
    "governance.min_stability_hours":GOVERNANCE_MIN_STABILITY_HOURS,
    "governance.limited_review_hours":GOVERNANCE_LIMITED_REVIEW_HOURS,
    "governance.max_major_changes_7d":GOVERNANCE_MAX_MAJOR_CHANGES_7D,
    "governance.max_strategy_changes_7d":GOVERNANCE_MAX_STRATEGY_CHANGES_7D,
    "governance.max_parameter_changes_7d":GOVERNANCE_MAX_PARAMETER_CHANGES_7D,
    "governance.max_deployments_7d":GOVERNANCE_MAX_DEPLOYMENTS_7D,
    "governance.max_promotions_7d":GOVERNANCE_MAX_PROMOTIONS_7D,
    "governance.max_global_changes_7d":GOVERNANCE_MAX_GLOBAL_CHANGES_7D,
    "governance.meta_risk_high":GOVERNANCE_META_RISK_HIGH,
    "governance.meta_risk_critical":GOVERNANCE_META_RISK_CRITICAL,
    "governance.decision_freshness_hours":GOVERNANCE_DECISION_FRESHNESS_HOURS,
    "smart_execution.mode":"SHADOW",
    "smart_execution.max_snapshot_age_seconds":SMART_EXECUTION_MAX_SNAPSHOT_AGE_SECONDS,
    "smart_execution.intent_ttl_seconds":SMART_EXECUTION_INTENT_TTL_SECONDS,
    "smart_execution.max_slippage_bps":SMART_EXECUTION_DEFAULT_MAX_SLIPPAGE_BPS,
    "smart_execution.liquidity_participation":SMART_EXECUTION_LIQUIDITY_PARTICIPATION,
    "smart_execution.slice_threshold_units":SMART_EXECUTION_SLICE_THRESHOLD_UNITS,
    "smart_execution.slice_size_units":SMART_EXECUTION_SLICE_SIZE_UNITS,
    "ensemble.max_model_weight":ENSEMBLE_MAX_MODEL_WEIGHT,
    "ensemble.max_family_weight":ENSEMBLE_MAX_FAMILY_WEIGHT,
    "ensemble.min_sample_size":ENSEMBLE_MIN_SAMPLE_SIZE,
    "ensemble.correlation_threshold":ENSEMBLE_CORRELATION_THRESHOLD,
    "ensemble.weight_change_limit":ENSEMBLE_WEIGHT_CHANGE_LIMIT,
    "ensemble.weight_cooldown_hours":ENSEMBLE_WEIGHT_COOLDOWN_HOURS,
    "ensemble.min_observation_hours":ENSEMBLE_MIN_OBSERVATION_HOURS,
    "ensemble.signal_ttl_seconds":ENSEMBLE_SIGNAL_TTL_SECONDS,
    "production.minimal_risk_multiplier":PRODUCTION_MINIMAL_RISK_MULTIPLIER,
    "production.limited_risk_multiplier":PRODUCTION_LIMITED_RISK_MULTIPLIER,
    "production.controlled_risk_multiplier":PRODUCTION_CONTROLLED_RISK_MULTIPLIER,
    "production.minimal_min_trades":PRODUCTION_MINIMAL_MIN_TRADES,
    "production.minimal_min_days":PRODUCTION_MINIMAL_MIN_DAYS,
    "production.limited_min_trades":PRODUCTION_LIMITED_MIN_TRADES,
    "production.limited_min_days":PRODUCTION_LIMITED_MIN_DAYS,
    "production.controlled_min_trades":PRODUCTION_CONTROLLED_MIN_TRADES,
    "production.controlled_min_days":PRODUCTION_CONTROLLED_MIN_DAYS,
    "execution.auto_trade":AUTO,
    "execution.trade_units":UNITS,
}

security_manager = SecurityManager(
    DB,VERSION_TAG,TRADING_ENVIRONMENT,SECURITY_ACTORS_JSON,
    allow_unauthenticated_reads=SECURITY_ALLOW_UNAUTHENTICATED_READS
)
security_manager.configure(
    SECURITY_CONFIG_SCHEMA,SECURITY_INITIAL_CONFIG,
    code_root=str(Path(__file__).resolve().parent),
    dependency_file=str(Path(__file__).resolve().parent/"requirements.txt")
)

def managed_value(key: str, fallback):
    try:
        return security_manager.get(key,fallback)
    except Exception:
        return fallback

def _security_actor(authorization: Optional[str], permission: Optional[str]=None, allow_read: bool=False):
    try:
        actor=security_manager.authenticate(authorization,allow_anonymous_read=allow_read)
        if permission: security_manager.require(actor,permission)
        return actor
    except PermissionError as e:
        code=401 if "AUTHENTICATION" in str(e) or "INVALID_CREDENTIALS" in str(e) else 403
        raise HTTPException(code,str(e))

def sync_security_runtime_config():
    # The Change Manager never raises a hard limit above code/env ceilings.
    deployment_manager.min_validation_score=float(managed_value("deployment.min_validation_score",DEPLOYMENT_MIN_VALIDATION_SCORE))
    deployment_manager.min_live_trades=int(managed_value("deployment.canary_min_trades",DEPLOYMENT_CANARY_MIN_TRADES))
    deployment_manager.min_limited_trades=int(managed_value("deployment.limited_min_trades",DEPLOYMENT_LIMITED_MIN_TRADES))
    deployment_manager.min_live_days=int(managed_value("deployment.min_live_days",DEPLOYMENT_MIN_LIVE_DAYS))
    deployment_manager.promotion_cooldown_hours=int(managed_value("deployment.promotion_cooldown_hours",DEPLOYMENT_PROMOTION_COOLDOWN_HOURS))
    deployment_manager.max_promotions_7d=int(managed_value("deployment.max_promotions_7d",DEPLOYMENT_MAX_PROMOTIONS_PER_7D))
    deployment_manager.max_exposure_increase=float(managed_value("deployment.max_exposure_increase",DEPLOYMENT_MAX_EXPOSURE_INCREASE))
    try:
        observability_manager.alert_cooldown_seconds=int(managed_value("observability.alert_cooldown_seconds",OBSERVABILITY_ALERT_COOLDOWN_SECONDS))
    except Exception:
        pass
    # Ensemble is always SHADOW in Step 17, but its evidence/correlation caps are
    # managed/versioned through Change Management. Runtime changes cannot grant order or risk authority.
    try:
        ensemble_engine.max_model_weight=float(managed_value("ensemble.max_model_weight",ENSEMBLE_MAX_MODEL_WEIGHT))
        ensemble_engine.max_family_weight=float(managed_value("ensemble.max_family_weight",ENSEMBLE_MAX_FAMILY_WEIGHT))
        ensemble_engine.min_sample_size=int(managed_value("ensemble.min_sample_size",ENSEMBLE_MIN_SAMPLE_SIZE))
        ensemble_engine.correlation_threshold=float(managed_value("ensemble.correlation_threshold",ENSEMBLE_CORRELATION_THRESHOLD))
        ensemble_engine.weight_change_limit=float(managed_value("ensemble.weight_change_limit",ENSEMBLE_WEIGHT_CHANGE_LIMIT))
        ensemble_engine.weight_cooldown_hours=int(managed_value("ensemble.weight_cooldown_hours",ENSEMBLE_WEIGHT_COOLDOWN_HOURS))
        ensemble_engine.min_observation_window_hours=int(managed_value("ensemble.min_observation_hours",ENSEMBLE_MIN_OBSERVATION_HOURS))
        ensemble_engine.default_signal_ttl_seconds=int(managed_value("ensemble.signal_ttl_seconds",ENSEMBLE_SIGNAL_TTL_SECONDS))
        ensemble_engine.mode="SHADOW"
    except Exception:
        pass

def sync_governance_runtime_config():
    if not GOVERNANCE_ENABLED:
        return None
    return governance_engine.set_runtime(
        mode=str(managed_value("governance.mode","SHADOW")),
        policies=governance_policy_config(),
        config_version=security_manager.current_version()
    )

def sync_production_readiness_config():
    if not PRODUCTION_READINESS_ENABLED:
        return None
    production_readiness_gate.stage_limits.update(production_stage_limits())
    return production_readiness_gate.stage_limits

def security_version_context(r: Optional[Dict[str,Any]]=None) -> Dict[str,Any]:
    integrity=security_manager.last_integrity or {}
    strategy=setup_variant(r) if r else "UNKNOWN"
    cfgv=security_manager.current_version()
    try:prod=production_readiness_gate.state()
    except Exception:prod={}
    return {
        "strategy_version":f"{strategy}@{VERSION_TAG}",
        "risk_config_version":f"config_v{cfgv}",
        "director_version":f"director@{VERSION_TAG}:config_v{cfgv}",
        "regime_model_version":f"regime@{VERSION_TAG}:config_v{cfgv}",
        "deployment_version":f"deployment@{VERSION_TAG}",
        "runtime_code_hash":integrity.get("code_hash"),
        "dependency_lock_hash":integrity.get("dependency_hash"),
        "config_snapshot_hash":security_manager.current_hash(),
        "release_id":prod.get("release_id"),
        "production_certification_id":prod.get("certification_id"),
        "production_stage":prod.get("production_stage"),
    }


def running_under_test() -> bool:
    return bool(os.getenv("PYTEST_CURRENT_TEST") or os.getenv("UNIT_TEST")=="1" or TRADING_ENVIRONMENT=="TEST")

def security_risk_limits_valid() -> bool:
    cfg=security_manager.current_config()
    try:
        return (
            0 < float(cfg["risk.base_fraction"]) <= float(cfg["risk.max_trade_fraction"])
            <= float(cfg["risk.max_strategy_fraction"]) <= float(cfg["risk.max_portfolio_fraction"])
            and 0 < float(cfg["risk.drawdown_warning"]) <= float(cfg["risk.drawdown_stop"])
            and float(cfg["risk.max_trade_fraction"]) <= RISK_MAX_TRADE_FRACTION
            and float(cfg["risk.max_strategy_fraction"]) <= RISK_MAX_STRATEGY_FRACTION
            and float(cfg["risk.max_portfolio_fraction"]) <= RISK_MAX_PORTFOLIO_FRACTION
        )
    except Exception:
        return False

def security_startup_check() -> Dict[str,Any]:
    secrets_ok=bool(ACCOUNT and TOKEN)
    if DEPLOYMENT_LIVE_EXECUTION_ENABLED or CANARY_OANDA_ENV=="live":
        secrets_ok=secrets_ok and bool(CANARY_ACCOUNT and CANARY_TOKEN)
    try:
        deployments=deployment_manager.dashboard().get("deployments",[])
        stages_ok=all(x.get("current_stage") in (
            "READY_FOR_REVIEW","APPROVED_FOR_CANARY","CANARY_LIVE","LIMITED_PRODUCTION",
            "FULL_PRODUCTION_ELIGIBLE","CANARY_PAUSED","ROLLED_BACK","CANARY_REJECTED"
        ) for x in deployments)
        c=conn()
        bad_versions=c.execute("""SELECT COUNT(*) n FROM deployment_registry dr
          LEFT JOIN candidate_strategies cs ON cs.candidate_id=dr.candidate_id
          WHERE cs.candidate_id IS NULL
             OR dr.candidate_version!=cs.candidate_version
             OR dr.production_version!=cs.production_version""").fetchone()["n"]
        c.close()
        dep_state_valid=bool(stages_ok and int(bad_versions)==0)
    except Exception:
        dep_state_valid=False
    try:
        audit_ok=security_manager.verify_audit_chain().get("verified",False)
    except Exception:
        audit_ok=False
    result=security_manager.startup_security_check(
        secrets_available=secrets_ok,
        canary_live_enabled=DEPLOYMENT_LIVE_EXECUTION_ENABLED,
        canary_env=CANARY_OANDA_ENV,
        risk_limits_valid=security_risk_limits_valid(),
        deployment_state_valid=dep_state_valid,
        audit_available=audit_ok,
        running_under_test=running_under_test()
    )
    actor=security_manager.internal_actor("STARTUP_SECURITY","SYSTEM_RECOMMENDER")
    if result.get("integrity",{}).get("role_config_changed"):
        security_manager.audit(actor,"ADMIN_PERMISSION_CHANGED","security.actor_roles",None,
                               {"role_config_hash":result["integrity"].get("role_config_hash")},
                               "role configuration changed outside runtime change manager",
                               "UNVERIFIED")
    return result



deployment_manager = DeploymentManager(
    DB,CANARY_OANDA,CANARY_ACCOUNT,CANARY_TOKEN,
    live_enabled=bool(DEPLOYMENT_LIVE_EXECUTION_ENABLED and CANARY_OANDA_ENV=="live" and TRADING_ENVIRONMENT=="PRODUCTION"),
    allowed_symbols=INSTRUMENTS,
    allowed_regimes=("BULL_TREND","BEAR_TREND","RANGE"),
    min_validation_score=DEPLOYMENT_MIN_VALIDATION_SCORE,
    min_paper_trades=VALIDATION_PAPER_MIN_TRADES,
    min_paper_days=VALIDATION_PAPER_MIN_DAYS,
    min_paper_regimes=VALIDATION_PAPER_MIN_REGIMES,
    min_live_trades=DEPLOYMENT_CANARY_MIN_TRADES,
    min_limited_trades=DEPLOYMENT_LIMITED_MIN_TRADES,
    min_live_days=DEPLOYMENT_MIN_LIVE_DAYS,
    min_live_regimes=DEPLOYMENT_MIN_LIVE_REGIMES,
    promotion_cooldown_hours=DEPLOYMENT_PROMOTION_COOLDOWN_HOURS,
    max_promotions_7d=DEPLOYMENT_MAX_PROMOTIONS_PER_7D,
    max_exposure_increase=DEPLOYMENT_MAX_EXPOSURE_INCREASE,
    max_daily_risk=DEPLOYMENT_CANARY_MAX_DAILY_RISK,
    max_drawdown=DEPLOYMENT_CANARY_MAX_DRAWDOWN,
    max_consecutive_losses=DEPLOYMENT_CANARY_MAX_CONSECUTIVE_LOSSES,
    max_stage_days=DEPLOYMENT_CANARY_MAX_STAGE_DAYS,
    max_slippage_pips=DEPLOYMENT_MAX_SLIPPAGE_PIPS,
    max_latency_seconds=DEPLOYMENT_MAX_LATENCY_SECONDS,
    base_risk_fraction=RISK_BASE_FRACTION
)
observability_manager = ObservabilityManager(
    DB, VERSION_TAG, alert_cooldown_seconds=OBSERVABILITY_ALERT_COOLDOWN_SECONDS
)
storage_lifecycle_manager = StorageLifecycleManager(DB)
system_evaluation_engine = SystemEvaluationEngine(
    DB, VERSION_TAG,
    min_samples=SYSTEM_EVALUATION_MIN_SAMPLES,
    report_period_hours=SYSTEM_EVALUATION_PERIOD_HOURS,
    score_weights={
        "trading":SYSTEM_EVALUATION_TRADING_WEIGHT,
        "risk":SYSTEM_EVALUATION_RISK_WEIGHT,
        "operational":SYSTEM_EVALUATION_OPERATIONAL_WEIGHT,
        "stability":SYSTEM_EVALUATION_STABILITY_WEIGHT
    },
    risk_drawdown_limit=float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP))
)

def governance_policy_config() -> Dict[str,Any]:
    return {
        "MIN_STABILITY_HOURS":int(managed_value("governance.min_stability_hours",GOVERNANCE_MIN_STABILITY_HOURS)),
        "LIMITED_ADAPTATION_REVIEW_HOURS":int(managed_value("governance.limited_review_hours",GOVERNANCE_LIMITED_REVIEW_HOURS)),
        "MAX_MAJOR_CHANGES_PER_WEEK":int(managed_value("governance.max_major_changes_7d",GOVERNANCE_MAX_MAJOR_CHANGES_7D)),
        "MAX_STRATEGY_CHANGES_PER_WEEK":int(managed_value("governance.max_strategy_changes_7d",GOVERNANCE_MAX_STRATEGY_CHANGES_7D)),
        "MAX_PARAMETER_CHANGES_PER_WEEK":int(managed_value("governance.max_parameter_changes_7d",GOVERNANCE_MAX_PARAMETER_CHANGES_7D)),
        "MAX_DEPLOYMENTS_PER_WEEK":int(managed_value("governance.max_deployments_7d",GOVERNANCE_MAX_DEPLOYMENTS_7D)),
        "MAX_PROMOTIONS_PER_WEEK":int(managed_value("governance.max_promotions_7d",GOVERNANCE_MAX_PROMOTIONS_7D)),
        "MAX_GLOBAL_CHANGES_PER_WEEK":int(managed_value("governance.max_global_changes_7d",GOVERNANCE_MAX_GLOBAL_CHANGES_7D)),
        "META_RISK_HIGH":float(managed_value("governance.meta_risk_high",GOVERNANCE_META_RISK_HIGH)),
        "META_RISK_CRITICAL":float(managed_value("governance.meta_risk_critical",GOVERNANCE_META_RISK_CRITICAL)),
        "MODULE_DECISION_FRESHNESS_HOURS":int(managed_value("governance.decision_freshness_hours",GOVERNANCE_DECISION_FRESHNESS_HOURS)),
    }

governance_engine = GovernanceEngine(
    DB, VERSION_TAG,
    mode=str(managed_value("governance.mode","SHADOW")),
    policies=governance_policy_config()
)
governance_engine.ensure_schema()
governance_engine.set_runtime(
    mode=str(managed_value("governance.mode","SHADOW")),
    policies=governance_policy_config(),
    config_version=security_manager.current_version()
)

smart_execution_engine = SmartExecutionEngine(
    DB, VERSION_TAG,
    mode="SHADOW",  # Step 16 deliberately starts observation-only.
    min_history_samples=SMART_EXECUTION_MIN_HISTORY_SAMPLES,
    max_snapshot_age_seconds=SMART_EXECUTION_MAX_SNAPSHOT_AGE_SECONDS,
    default_intent_ttl_seconds=SMART_EXECUTION_INTENT_TTL_SECONDS,
    liquidity_participation=SMART_EXECUTION_LIQUIDITY_PARTICIPATION,
    slice_threshold_units=SMART_EXECUTION_SLICE_THRESHOLD_UNITS,
    slice_size_units=SMART_EXECUTION_SLICE_SIZE_UNITS,
    latency_warning_ms=OBSERVABILITY_BROKER_LATENCY_WARNING_MS,
    degradation_min_samples=SMART_EXECUTION_DEGRADATION_MIN_SAMPLES
)
smart_execution_engine.ensure_schema()

ensemble_engine = EnsembleEngine(
    DB, VERSION_TAG, mode="SHADOW",
    max_model_weight=ENSEMBLE_MAX_MODEL_WEIGHT,
    max_family_weight=ENSEMBLE_MAX_FAMILY_WEIGHT,
    min_sample_size=ENSEMBLE_MIN_SAMPLE_SIZE,
    correlation_threshold=ENSEMBLE_CORRELATION_THRESHOLD,
    weight_change_limit=ENSEMBLE_WEIGHT_CHANGE_LIMIT,
    weight_cooldown_hours=ENSEMBLE_WEIGHT_COOLDOWN_HOURS,
    min_observation_window_hours=ENSEMBLE_MIN_OBSERVATION_HOURS,
    default_signal_ttl_seconds=ENSEMBLE_SIGNAL_TTL_SECONDS
)
ensemble_engine.ensure_schema()
# Current model map. Price-derived technical subcomponents remain one family so
# they cannot be counted as independent confirmations.
ensemble_engine.register_model("TECHNICAL_CORE",f"technical@{VERSION_TAG}","TREND_STRUCTURE","DIRECTIONAL",
    ["H1_PRICE","M15_PRICE","M5_PRICE","M1_PRICE","EMA","ATR","STRUCTURE","PULLBACK","MOMENTUM"],"INTRADAY")
ensemble_engine.register_model("ML_SUCCESS_CALIBRATOR",f"ml@{VERSION_TAG}","TECHNICAL_CALIBRATION","CALIBRATOR",
    ["TECHNICAL_FEATURE_VECTOR","RESOLVED_LABELS"],"INTRADAY")
ensemble_engine.register_model("NEWS_CONTEXT",f"gdelt@{VERSION_TAG}","NEWS_MACRO","DIRECTIONAL",
    ["GDELT_180M","FX_CURRENCY_TERMS"],"INTRADAY")
ensemble_engine.register_model("MARKET_REGIME_CONTEXT",f"regime@{VERSION_TAG}","MARKET_REGIME","CONTEXT",
    ["H1_PRICE","M15_PRICE","M5_PRICE","M1_PRICE","ATR","EFFICIENCY_RATIO"],"INTRADAY")
ensemble_engine.register_model("WEEKEND_CONTEXT",f"weekend@{VERSION_TAG}","WEEKEND_CONTEXT","DIRECTIONAL",
    ["GDELT_WEEKEND","MARKET_REOPEN_REACTION"],"INTRADAY")

capital_allocation_engine = CapitalAllocationEngine(
    DB,VERSION_TAG,mode="SHADOW",max_strategy_allocation=CAPITAL_ALLOCATION_MAX_STRATEGY,
    max_family_risk=CAPITAL_ALLOCATION_MAX_FAMILY,max_symbol_risk=CAPITAL_ALLOCATION_MAX_SYMBOL,
    max_asset_risk=CAPITAL_ALLOCATION_MAX_ASSET,max_directional_risk=CAPITAL_ALLOCATION_MAX_DIRECTIONAL,
    max_cluster_risk=CAPITAL_ALLOCATION_MAX_CLUSTER,max_change_per_cycle=CAPITAL_ALLOCATION_MAX_CHANGE,
    change_cooldown_hours=CAPITAL_ALLOCATION_COOLDOWN_HOURS,rebalance_threshold=CAPITAL_ALLOCATION_REBALANCE_THRESHOLD,
    heat_limit=CAPITAL_ALLOCATION_HEAT_LIMIT,correlation_threshold=ENSEMBLE_CORRELATION_THRESHOLD)
capital_allocation_engine.ensure_schema()

def production_stage_limits() -> Dict[str,Dict[str,Any]]:
    return {
        "MINIMAL_LIVE":{
            "risk_cap_multiplier":float(managed_value("production.minimal_risk_multiplier",PRODUCTION_MINIMAL_RISK_MULTIPLIER)),
            "max_trade_risk_fraction":min(float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION)),0.0005),
            "max_portfolio_exposure_fraction":min(float(managed_value("risk.max_portfolio_fraction",RISK_MAX_PORTFOLIO_FRACTION)),0.005),
            "max_drawdown_fraction":min(float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP)),0.005),
            "min_trades_for_promotion":int(managed_value("production.minimal_min_trades",PRODUCTION_MINIMAL_MIN_TRADES)),
            "min_days_for_promotion":int(managed_value("production.minimal_min_days",PRODUCTION_MINIMAL_MIN_DAYS)),
        },
        "LIMITED_LIVE":{
            "risk_cap_multiplier":float(managed_value("production.limited_risk_multiplier",PRODUCTION_LIMITED_RISK_MULTIPLIER)),
            "max_trade_risk_fraction":min(float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION)),0.001),
            "max_portfolio_exposure_fraction":min(float(managed_value("risk.max_portfolio_fraction",RISK_MAX_PORTFOLIO_FRACTION)),0.01),
            "max_drawdown_fraction":min(float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP)),0.01),
            "min_trades_for_promotion":int(managed_value("production.limited_min_trades",PRODUCTION_LIMITED_MIN_TRADES)),
            "min_days_for_promotion":int(managed_value("production.limited_min_days",PRODUCTION_LIMITED_MIN_DAYS)),
        },
        "CONTROLLED_LIVE":{
            "risk_cap_multiplier":float(managed_value("production.controlled_risk_multiplier",PRODUCTION_CONTROLLED_RISK_MULTIPLIER)),
            "max_trade_risk_fraction":min(float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION)),0.0025),
            "max_portfolio_exposure_fraction":min(float(managed_value("risk.max_portfolio_fraction",RISK_MAX_PORTFOLIO_FRACTION)),0.025),
            "max_drawdown_fraction":min(float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP)),0.02),
            "min_trades_for_promotion":int(managed_value("production.controlled_min_trades",PRODUCTION_CONTROLLED_MIN_TRADES)),
            "min_days_for_promotion":int(managed_value("production.controlled_min_days",PRODUCTION_CONTROLLED_MIN_DAYS)),
        }
    }

production_readiness_gate = ProductionReadinessGate(DB,VERSION_TAG,stage_limits=production_stage_limits())
production_readiness_gate.ensure_schema()

sync_security_runtime_config()

OBSERVABILITY_DEPENDENCIES = {
    "Database":DEPENDENCY_CRITICAL,
    "Persistent Storage":DEPENDENCY_IMPORTANT,
    "Market Data":DEPENDENCY_CRITICAL,
    "Market Regime Detector":DEPENDENCY_IMPORTANT,
    "Broker Connection":DEPENDENCY_CRITICAL,
    "Risk Engine":DEPENDENCY_CRITICAL,
    "Execution Engine":DEPENDENCY_CRITICAL,
    "Smart Execution Engine":DEPENDENCY_IMPORTANT,
    "Ensemble Engine":DEPENDENCY_NON_CRITICAL,
    "Recovery Manager":DEPENDENCY_CRITICAL,
    "Security Manager":DEPENDENCY_CRITICAL,
    "System Evaluation Engine":DEPENDENCY_IMPORTANT,
    "Governance Engine":DEPENDENCY_IMPORTANT,
    "Production Readiness Gate":DEPENDENCY_CRITICAL,
    "Strategies":DEPENDENCY_IMPORTANT,
    "AI Strategy Director":DEPENDENCY_IMPORTANT,
    "Trade Memory":DEPENDENCY_IMPORTANT,
    "Adaptive Learning":DEPENDENCY_NON_CRITICAL,
    "Validation Pipeline":DEPENDENCY_NON_CRITICAL,
    "Paper Trading":DEPENDENCY_NON_CRITICAL,
    "Deployment Manager":DEPENDENCY_IMPORTANT,
}


recovery_manager = RecoveryManager(
    DB,OANDA,ACCOUNT,TOKEN,account_scope="PRIMARY",
    use_client_extensions=RECOVERY_USE_CLIENT_EXTENSIONS,
    circuit_failure_threshold=RECOVERY_CIRCUIT_FAILURE_THRESHOLD,
    circuit_open_seconds=RECOVERY_CIRCUIT_OPEN_SECONDS,
    request_min_interval_ms=RECOVERY_REQUEST_MIN_INTERVAL_MS,
    max_read_retries=RECOVERY_MAX_READ_RETRIES,
    backoff_base_seconds=RECOVERY_BACKOFF_BASE_SECONDS,
    backoff_cap_seconds=RECOVERY_BACKOFF_CAP_SECONDS,
    allow_orphan_quarantine=bool(PRIMARY_OANDA_ENV=="practice" and RECOVERY_PRACTICE_ORPHAN_QUARANTINE)
)

canary_recovery_manager = RecoveryManager(
    DB,CANARY_OANDA,CANARY_ACCOUNT,CANARY_TOKEN,account_scope="CANARY",
    use_client_extensions=RECOVERY_USE_CLIENT_EXTENSIONS,
    circuit_failure_threshold=RECOVERY_CIRCUIT_FAILURE_THRESHOLD,
    circuit_open_seconds=RECOVERY_CIRCUIT_OPEN_SECONDS,
    request_min_interval_ms=RECOVERY_REQUEST_MIN_INTERVAL_MS,
    max_read_retries=RECOVERY_MAX_READ_RETRIES,
    backoff_base_seconds=RECOVERY_BACKOFF_BASE_SECONDS,
    backoff_cap_seconds=RECOVERY_BACKOFF_CAP_SECONDS
)

def production_hard_limits() -> Dict[str,Any]:
    return {
        "max_trade_risk_fraction":float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION)),
        "max_portfolio_exposure_fraction":float(managed_value("risk.max_portfolio_fraction",RISK_MAX_PORTFOLIO_FRACTION)),
        "max_drawdown_fraction":float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP)),
    }

def production_runtime_context() -> Dict[str,Any]:
    rec={}
    try: rec=recovery_manager.state()
    except Exception: rec={}
    gov={}
    try: gov=governance_engine.state()
    except Exception: gov={}
    latest_eval=None
    try: latest_eval=system_evaluation_engine.latest()
    except Exception: latest_eval=None
    monitoring_ready=False
    try:
        dash=observability_manager.dashboard()
        monitoring_ready=(dash.get("system_health") not in ("CRITICAL","TRADING_PAUSED","EMERGENCY_STOP"))
    except Exception:
        monitoring_ready=False
    dep_state={}
    if DEPLOYMENT_MANAGER_ENABLED:
        try: dep_state=deployment_manager.dashboard()
        except Exception: dep_state={}
    return {
        "environment":TRADING_ENVIRONMENT,
        "production_authorized":bool(PRODUCTION_AUTHORIZED),
        "account_scope":"PRIMARY",
        "risk_engine_ready":bool(RISK_ENGINE_ENABLED),
        "risk_engine_shadow_mode":bool(RISK_ENGINE_SHADOW_MODE),
        "broker_reconciled":bool(rec.get("last_reconciliation_status") in ("MATCHED","MINOR_MISMATCH","READY")),
        "market_data_fresh":bool(rec.get("last_market_data_ts")) and not bool(rec.get("safe_mode")),
        "last_data_ts":rec.get("last_market_data_ts"),
        "audit_ready":bool((security_manager.verify_audit_chain() or {}).get("verified",False)) if hasattr(security_manager,"verify_audit_chain") else False,
        "deployment_state_consistent":True,
        "deployment_state":dep_state,
        "no_state_corruption":not bool(rec.get("state") in ("CRITICAL_FAILURE","RECONCILING")),
        "canary_controls_ready":bool(DEPLOYMENT_MANAGER_ENABLED),
        "recovery_tests_pass":False,  # Certification must bind immutable Step14 evidence; runtime does not self-assert tests.
        "security_tests_pass":False,
        "change_management_ready":True,
        "monitoring_ready":monitoring_ready,
        "no_risk_bypass_known":False,
        "no_duplicate_order_vulnerability":False,
        "emergency_stop_test_pass":False,
        "system_ready":bool(rec.get("state") in ("READY","NORMAL")) and not bool(rec.get("safe_mode")),
        "risk_ready":bool(RISK_ENGINE_ENABLED and not RISK_ENGINE_SHADOW_MODE),
        "broker_ready":bool(rec.get("state") in ("READY","NORMAL")) and not bool(rec.get("safe_mode")),
        "data_ready":bool(rec.get("last_market_data_ts")) and not bool(rec.get("safe_mode")),
        "reconciliation_ok":bool(rec.get("last_reconciliation_status") in ("MATCHED","MINOR_MISMATCH","READY")),
        "governance_ok":bool(gov and not int(gov.get("governance_lock") or 0) and gov.get("adaptation_state")!="ADAPTATION_FROZEN"),
        "governance_lock":bool(gov and int(gov.get("governance_lock") or 0)),
        "emergency_stop":bool(rec.get("emergency_stop")),
        "system_status":(latest_eval or {}).get("system_status"),
        "system_evaluation":latest_eval or {},
        "governance_state":gov.get("adaptation_state"),
        "data_quality":float((latest_eval or {}).get("data_quality_score") or 0.0),
        "hard_limits":production_hard_limits(),
    }

def production_certification_context() -> Dict[str,Any]:
    ctx=production_runtime_context()
    try:
        report=json.loads(Path(PRODUCTION_STEP14_REPORT_PATH).read_text())
    except Exception:
        report={}
    gates=(report.get("pass_fail_gate") or {}).get("gates") or {}
    ctx.update({
        "recovery_tests_pass":bool(gates.get("restart_recovery_successful") and gates.get("reconciliation_passes") and gates.get("database_failure_recovery_passes")),
        "security_tests_pass":bool(gates.get("governance_protections_pass") and gates.get("zero_risk_limit_bypasses")),
        "no_risk_bypass_known":bool(gates.get("zero_risk_limit_bypasses")),
        "no_duplicate_order_vulnerability":bool(gates.get("zero_duplicate_order_vulnerabilities")),
        "emergency_stop_test_pass":bool(gates.get("emergency_stop_survives_restart")),
        "canary_controls_ready":bool(gates.get("canary_rollback_pass")),
        "step14_report_version":report.get("framework_version"),
    })
    return ctx

def production_release_files() -> List[str]:
    root=Path(__file__).resolve().parent
    names=[
        "server.py","production_readiness.py","governance_engine.py","system_evaluation.py",
        "security_manager.py","recovery_manager.py","order_state.py","observability.py","smart_execution.py","ensemble_engine.py","capital_allocation.py",
        "adaptive_learning.py","validation_pipeline.py","deployment_manager.py","deployment_runtime.py",
        "instrument_registry.py","instrument_profiles.py","opportunity_ranker.py","slot_allocator.py","broker_risk.py","counterfactual_tracker.py","requirements.txt","Dockerfile"
    ]
    return [str(root/n) for n in names]

def production_release_versions() -> Dict[str,Any]:
    return {
        "system_release":VERSION_TAG,
        "strategy_versions":[f"{x}@{VERSION_TAG}" for x in sorted(INSTRUMENTS)],
        "risk_config_version":f"config_v{security_manager.current_version()}",
        "governance_version":f"governance@{VERSION_TAG}:config_v{security_manager.current_version()}",
        "deployment_version":f"deployment@{VERSION_TAG}",
        "execution_version":f"execution@{VERSION_TAG}",
        "smart_execution_version":f"smart-execution@{VERSION_TAG}:SHADOW",
        "ensemble_version":f"ensemble@{VERSION_TAG}:SHADOW",
        "capital_allocation_version":f"capital-allocation@{VERSION_TAG}:SHADOW",
        "broker_adapter_version":f"oanda-adapter@{VERSION_TAG}",
        "data_pipeline_version":f"market-data@{VERSION_TAG}",
        "dependencies":security_manager.last_integrity.get("dependency_hash") if getattr(security_manager,"last_integrity",None) else None,
    }

def freeze_current_release_candidate(actor: str="SYSTEM") -> Dict[str,Any]:
    return production_readiness_gate.create_release_candidate(
        files=production_release_files(),config=security_manager.current_config(),versions=production_release_versions(),
        step14_report_path=PRODUCTION_STEP14_REPORT_PATH,actor=actor)


def conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=5000")
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
            filters_json TEXT NOT NULL,
            ensemble_decision_id TEXT,
            ensemble_direction TEXT,
            ensemble_confidence REAL,
            ensemble_agreement REAL,
            ensemble_diversity REAL,
            ensemble_weight_version TEXT
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
            safety_filters_ok INTEGER,
            quality_filters_ok INTEGER,
            auto_trade INTEGER NOT NULL,
            executed INTEGER NOT NULL,
            reason TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS trade_forward_observations(
            trade_id TEXT PRIMARY KEY,
            instrument TEXT NOT NULL,
            side TEXT NOT NULL,
            opened_ts TEXT NOT NULL,
            be_trigger_r REAL NOT NULL,
            be_lock_r REAL NOT NULL,
            max_r_seen REAL NOT NULL DEFAULT 0,
            be_activated_ts TEXT,
            be_activation_r REAL,
            max_r_after_be REAL,
            updated_ts TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS trade_forward_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            event TEXT NOT NULL,
            r_multiple REAL,
            detail_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(trade_id,event)
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
    model_cols = {row[1] for row in c.execute("PRAGMA table_info(model_runs)").fetchall()}
    if "instrument" not in model_cols:
        c.execute("ALTER TABLE model_runs ADD COLUMN instrument TEXT NOT NULL DEFAULT 'EUR_USD'")
    c.execute("CREATE INDEX IF NOT EXISTS idx_model_runs_instrument_id ON model_runs(instrument,id)")

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
        "outcome_cost_r": "REAL",
        "effective_target": "REAL",
        "effective_stop": "REAL",
    }
    for name, ddl in sample_migrations.items():
        if name not in sample_cols:
            c.execute(f"ALTER TABLE learning_samples ADD COLUMN {name} {ddl}")

    # V3.27 decision telemetry: distinguish safety invariants from quality-entry gates.
    decision_cols = {row[1] for row in c.execute("PRAGMA table_info(decision_log)").fetchall()}
    for name, ddl in {
        "safety_filters_ok":"INTEGER",
        "quality_filters_ok":"INTEGER",
        "forward_audit_json":"TEXT NOT NULL DEFAULT '{}'",
    }.items():
        if name not in decision_cols:
            c.execute(f"ALTER TABLE decision_log ADD COLUMN {name} {ddl}")

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
        CREATE TABLE IF NOT EXISTS instrument_discovered_patterns(
            instrument TEXT NOT NULL,
            pattern_key TEXT NOT NULL,
            family TEXT NOT NULL,
            value TEXT NOT NULL,
            samples INTEGER NOT NULL,
            wins INTEGER NOT NULL,
            win_rate REAL,
            instrument_win_rate REAL,
            edge REAL,
            weight REAL NOT NULL,
            validated INTEGER NOT NULL,
            updated_ts TEXT NOT NULL,
            PRIMARY KEY(instrument,pattern_key)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_learning_samples_instrument_resolved ON learning_samples(instrument,resolved_ts,id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_signals_instrument_variant ON signals(instrument,setup_variant,id)")
    c.execute("""
        CREATE TABLE IF NOT EXISTS market_regime_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            candle_ts TEXT NOT NULL,
            instrument TEXT NOT NULL,
            market_regime TEXT NOT NULL,
            confidence REAL NOT NULL,
            volatility_state TEXT NOT NULL,
            trend_strength REAL NOT NULL,
            abnormality_score REAL NOT NULL,
            supporting_metrics_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(instrument,candle_ts)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS validation_datasets(
            dataset_version TEXT PRIMARY KEY,
            created_ts TEXT NOT NULL,
            strategy_id TEXT NOT NULL,
            dataset_hash TEXT NOT NULL,
            trade_count INTEGER NOT NULL,
            period_start TEXT,
            period_end TEXT,
            training_start TEXT, training_end TEXT,
            validation_start TEXT, validation_end TEXT,
            test_start TEXT, test_end TEXT,
            sealed INTEGER NOT NULL DEFAULT 1,
            details_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS validation_dataset_members(
            dataset_version TEXT NOT NULL,
            trade_id TEXT NOT NULL,
            partition TEXT NOT NULL,
            position INTEGER NOT NULL,
            PRIMARY KEY(dataset_version,trade_id),
            FOREIGN KEY(dataset_version) REFERENCES validation_datasets(dataset_version)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS candidate_registry(
            candidate_id TEXT PRIMARY KEY,
            strategy_id TEXT NOT NULL,
            candidate_version TEXT NOT NULL,
            current_state TEXT NOT NULL,
            historical_validation_status TEXT,
            validation_score REAL,
            dataset_version TEXT,
            paper_started_ts TEXT,
            paper_updated_ts TEXT,
            paper_trade_count INTEGER NOT NULL DEFAULT 0,
            paper_regime_count INTEGER NOT NULL DEFAULT 0,
            paper_days REAL NOT NULL DEFAULT 0,
            divergence_status TEXT,
            final_reason TEXT,
            latest_validation_id TEXT,
            auto_deploy INTEGER NOT NULL DEFAULT 0,
            created_ts TEXT NOT NULL,
            updated_ts TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS candidate_validation_runs(
            validation_id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL,
            strategy_id TEXT NOT NULL,
            candidate_version TEXT NOT NULL,
            code_version TEXT NOT NULL,
            code_hash TEXT NOT NULL,
            dataset_version TEXT NOT NULL,
            started_ts TEXT NOT NULL,
            completed_ts TEXT,
            state TEXT NOT NULL,
            training_period_json TEXT NOT NULL DEFAULT '{}',
            validation_period_json TEXT NOT NULL DEFAULT '{}',
            test_period_json TEXT NOT NULL DEFAULT '{}',
            walk_forward_config_json TEXT NOT NULL DEFAULT '{}',
            backtest_results_json TEXT NOT NULL DEFAULT '{}',
            oos_results_json TEXT NOT NULL DEFAULT '{}',
            walk_forward_results_json TEXT NOT NULL DEFAULT '{}',
            stress_results_json TEXT NOT NULL DEFAULT '{}',
            sensitivity_results_json TEXT NOT NULL DEFAULT '{}',
            regime_results_json TEXT NOT NULL DEFAULT '{}',
            monte_carlo_results_json TEXT NOT NULL DEFAULT '{}',
            paper_results_json TEXT NOT NULL DEFAULT '{}',
            validation_score REAL,
            final_status TEXT NOT NULL,
            final_reason TEXT NOT NULL,
            reproducibility_json TEXT NOT NULL DEFAULT '{}',
            auto_deploy INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(candidate_id) REFERENCES candidate_strategies(candidate_id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS validation_walk_forward_windows(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            validation_id TEXT NOT NULL,
            window_no INTEGER NOT NULL,
            train_start TEXT, train_end TEXT, test_start TEXT, test_end TEXT,
            production_metrics_json TEXT NOT NULL DEFAULT '{}',
            candidate_metrics_json TEXT NOT NULL DEFAULT '{}',
            comparison_json TEXT NOT NULL DEFAULT '{}',
            UNIQUE(validation_id,window_no)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS candidate_paper_trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id TEXT NOT NULL,
            signal_id INTEGER NOT NULL,
            created_ts TEXT NOT NULL,
            candle_ts TEXT,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry REAL NOT NULL, stop REAL NOT NULL, target REAL NOT NULL, risk REAL NOT NULL,
            observed_entry REAL, simulated_exit REAL, entry_deviation_r REAL, latency_seconds REAL,
            executable INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'PENDING',
            label INTEGER, resolved_ts TEXT, bars_to_resolution INTEGER,
            mfe_r REAL, mae_r REAL, realized_r REAL, note TEXT,
            market_regime TEXT, volatility_state TEXT, strategy_confidence REAL,
            director_confidence REAL, risk_multiplier REAL,
            UNIQUE(candidate_id,signal_id),
            FOREIGN KEY(candidate_id) REFERENCES candidate_strategies(candidate_id),
            FOREIGN KEY(signal_id) REFERENCES signals(id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS validation_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL, candidate_id TEXT NOT NULL, validation_id TEXT,
            stage TEXT NOT NULL, status TEXT NOT NULL, details_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS adaptive_learning_runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL UNIQUE,
            started_ts TEXT NOT NULL,
            completed_ts TEXT,
            code_version TEXT NOT NULL,
            dataset_hash TEXT,
            period_start TEXT,
            period_end TEXT,
            trade_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            config_json TEXT NOT NULL DEFAULT '{}',
            summary_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS candidate_strategies(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id TEXT NOT NULL UNIQUE,
            run_id TEXT NOT NULL,
            strategy_id TEXT NOT NULL,
            production_version TEXT NOT NULL,
            candidate_version TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            change_type TEXT NOT NULL,
            parameter_name TEXT NOT NULL,
            current_value_json TEXT NOT NULL,
            proposed_value_json TEXT NOT NULL,
            reason TEXT NOT NULL,
            supporting_evidence_json TEXT NOT NULL DEFAULT '{}',
            sample_size INTEGER NOT NULL DEFAULT 0,
            expected_improvement REAL,
            confidence REAL,
            risks_json TEXT NOT NULL DEFAULT '[]',
            original_parameters_json TEXT NOT NULL DEFAULT '{}',
            candidate_parameters_json TEXT NOT NULL DEFAULT '{}',
            validation_json TEXT NOT NULL DEFAULT '{}',
            candidate_score REAL,
            dataset_hash TEXT NOT NULL,
            period_start TEXT,
            period_end TEXT,
            generated_at TEXT NOT NULL,
            validated_at TEXT,
            cooldown_until TEXT,
            auto_deploy INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(run_id) REFERENCES adaptive_learning_runs(run_id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS adaptive_learning_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            stage TEXT NOT NULL,
            strategy_id TEXT,
            candidate_id TEXT,
            status TEXT NOT NULL,
            details_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS concept_drift_alerts(
            scope_key TEXT PRIMARY KEY,
            ts TEXT NOT NULL,
            strategy_id TEXT NOT NULL,
            market_regime TEXT,
            status TEXT NOT NULL,
            confidence REAL,
            historical_metrics_json TEXT NOT NULL DEFAULT '{}',
            previous_metrics_json TEXT NOT NULL DEFAULT '{}',
            recent_metrics_json TEXT NOT NULL DEFAULT '{}',
            reason TEXT NOT NULL,
            auto_action INTEGER NOT NULL DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS trade_memory(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id TEXT NOT NULL UNIQUE,
            signal_id INTEGER,
            order_id TEXT,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            entry_ts TEXT NOT NULL,
            exit_ts TEXT,
            entry_price REAL NOT NULL,
            exit_price REAL,
            position_size REAL NOT NULL,
            stop_loss REAL,
            take_profit REAL,
            gross_result REAL,
            net_result REAL,
            realized_pl REAL,
            financing REAL,
            dividend_adjustment REAL,
            guaranteed_execution_fees REAL,
            commission REAL,
            fees_total REAL,
            entry_slippage_pips REAL,
            exit_slippage_pips REAL,
            duration_seconds REAL,
            market_regime_entry TEXT,
            regime_confidence_entry REAL,
            volatility_state_entry TEXT,
            trend_strength_entry REAL,
            strategy_confidence_entry REAL,
            director_state_entry TEXT,
            director_confidence_entry REAL,
            risk_multiplier_entry REAL,
            risk_allow_new_trades_shadow INTEGER,
            requested_risk REAL,
            approved_risk REAL,
            entry_drawdown REAL,
            mfe_r REAL NOT NULL DEFAULT 0,
            mae_r REAL NOT NULL DEFAULT 0,
            max_drawdown_during_trade_r REAL NOT NULL DEFAULT 0,
            realized_r REAL,
            entry_session TEXT,
            confidence_bucket TEXT,
            entry_reasons_json TEXT NOT NULL DEFAULT '[]',
            exit_reasons_json TEXT NOT NULL DEFAULT '[]',
            entry_context_json TEXT NOT NULL DEFAULT '{}',
            execution_context_json TEXT NOT NULL DEFAULT '{}',
            exit_context_json TEXT NOT NULL DEFAULT '{}',
            risk_recommendation_json TEXT NOT NULL DEFAULT '{}',
            data_quality_json TEXT NOT NULL DEFAULT '{}',
            execution_quality_compromised INTEGER NOT NULL DEFAULT 0,
            operational_incident_id TEXT,
            strategy_version TEXT,
            risk_config_version TEXT,
            director_version TEXT,
            regime_model_version TEXT,
            deployment_version TEXT,
            runtime_code_hash TEXT,
            dependency_lock_hash TEXT,
            config_snapshot_hash TEXT,
            ensemble_decision_id TEXT,
            ensemble_direction TEXT,
            ensemble_confidence REAL,
            ensemble_agreement REAL,
            ensemble_diversity REAL,
            ensemble_weight_version TEXT,
            ensemble_context_json TEXT NOT NULL DEFAULT '{}',
            created_ts TEXT NOT NULL,
            updated_ts TEXT NOT NULL,
            FOREIGN KEY(signal_id) REFERENCES signals(id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS trade_memory_degradation(
            scope_key TEXT PRIMARY KEY,
            ts TEXT NOT NULL,
            scope_type TEXT NOT NULL,
            strategy TEXT NOT NULL,
            market_regime TEXT,
            status TEXT NOT NULL,
            historical_samples INTEGER NOT NULL,
            recent_samples INTEGER NOT NULL,
            historical_profit_factor REAL,
            recent_profit_factor REAL,
            historical_expectancy REAL,
            recent_expectancy REAL,
            historical_win_rate REAL,
            recent_win_rate REAL,
            change_score REAL,
            reason TEXT NOT NULL,
            details_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS adaptive_risk_decisions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            instrument TEXT NOT NULL,
            setup_variant TEXT NOT NULL,
            market_regime TEXT,
            volatility_state TEXT,
            strategy_confidence REAL,
            recent_win_rate REAL,
            current_drawdown REAL,
            nav REAL,
            margin_usage REAL,
            portfolio_open_risk REAL,
            strategy_open_risk REAL,
            requested_risk REAL,
            approved_risk REAL,
            requested_units REAL,
            shadow_max_position_size REAL,
            risk_multiplier REAL NOT NULL,
            max_exposure REAL,
            allow_new_trades INTEGER NOT NULL,
            reduce_existing_positions INTEGER NOT NULL,
            emergency_stop INTEGER NOT NULL,
            hard_limit_triggered INTEGER NOT NULL,
            reason TEXT NOT NULL,
            metrics_json TEXT NOT NULL DEFAULT '{}',
            shadow_mode INTEGER NOT NULL DEFAULT 1
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_risk_state(
            id INTEGER PRIMARY KEY CHECK(id=1),
            ts TEXT NOT NULL,
            balance REAL,
            nav REAL,
            peak_nav REAL,
            current_drawdown REAL,
            margin_used REAL,
            margin_usage REAL,
            open_positions INTEGER NOT NULL DEFAULT 0,
            portfolio_open_risk REAL,
            consecutive_losses INTEGER NOT NULL DEFAULT 0,
            data_stale INTEGER NOT NULL DEFAULT 0,
            system_abnormal INTEGER NOT NULL DEFAULT 0,
            details_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS multi_asset_decision_cycles(
            cycle_id TEXT PRIMARY KEY,
            ts TEXT NOT NULL,
            broker_mode TEXT NOT NULL,
            trading_environment TEXT NOT NULL,
            nlv REAL,
            slot_tier TEXT,
            max_slots INTEGER NOT NULL,
            slots_available INTEGER NOT NULL,
            open_positions INTEGER NOT NULL,
            candidates_json TEXT NOT NULL DEFAULT '[]',
            ranking_json TEXT NOT NULL DEFAULT '[]',
            selected_json TEXT NOT NULL DEFAULT '[]',
            rejected_json TEXT NOT NULL DEFAULT '[]',
            metadata_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_strategy_director_decisions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            instrument TEXT NOT NULL,
            setup_variant TEXT NOT NULL,
            recommended_state TEXT NOT NULL,
            confidence REAL NOT NULL,
            market_regime TEXT,
            regime_confidence REAL,
            volatility_state TEXT,
            strategy_health_status TEXT,
            historical_win_rate REAL,
            recent_win_rate REAL,
            historical_samples INTEGER NOT NULL DEFAULT 0,
            recent_samples INTEGER NOT NULL DEFAULT 0,
            signal_confidence REAL,
            score_components_json TEXT NOT NULL DEFAULT '{}',
            reasons_json TEXT NOT NULL DEFAULT '[]',
            observation_only INTEGER NOT NULL DEFAULT 1
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_strategy_director_outcomes(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            director_decision_id INTEGER NOT NULL,
            signal_id INTEGER,
            resolved_label INTEGER,
            executed INTEGER,
            blocked INTEGER,
            resolved_ts TEXT,
            FOREIGN KEY(director_decision_id) REFERENCES ai_strategy_director_decisions(id)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS strategy_health(
            setup_variant TEXT PRIMARY KEY, status TEXT NOT NULL, evidence_mode TEXT NOT NULL DEFAULT 'CANONICAL',
            total_resolved INTEGER NOT NULL DEFAULT 0, executed_resolved INTEGER NOT NULL DEFAULT 0,
            baseline_samples INTEGER NOT NULL DEFAULT 0, baseline_win_rate REAL,
            recent_samples INTEGER NOT NULL DEFAULT 0, recent_win_rate REAL, recent_drop REAL,
            recent_loss_streak INTEGER NOT NULL DEFAULT 0, paused_ts TEXT, pause_baseline_win_rate REAL,
            recovery_samples INTEGER NOT NULL DEFAULT 0, recovery_win_rate REAL,
            last_transition TEXT, reason TEXT, updated_ts TEXT NOT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS strategy_health_audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, setup_variant TEXT NOT NULL,
            old_status TEXT, new_status TEXT NOT NULL, evidence_mode TEXT, baseline_win_rate REAL,
            recent_win_rate REAL, recent_drop REAL, loss_streak INTEGER, details_json TEXT NOT NULL DEFAULT '{}'
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS weekend_context(
            id INTEGER PRIMARY KEY AUTOINCREMENT, weekend_id TEXT NOT NULL, instrument TEXT NOT NULL,
            bucket_ts TEXT NOT NULL, collected_ts TEXT NOT NULL, bias TEXT NOT NULL,
            positive_hits INTEGER NOT NULL DEFAULT 0, negative_hits INTEGER NOT NULL DEFAULT 0,
            article_count INTEGER NOT NULL DEFAULT 0, titles_json TEXT NOT NULL DEFAULT '[]',
            UNIQUE(weekend_id,instrument,bucket_ts)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS weekend_sessions(
            weekend_id TEXT NOT NULL, instrument TEXT NOT NULL, opened_ts TEXT NOT NULL, open_price REAL NOT NULL,
            context_bias TEXT NOT NULL, context_score REAL NOT NULL DEFAULT 0, article_count INTEGER NOT NULL DEFAULT 0,
            context_json TEXT NOT NULL DEFAULT '{}', reaction_1h_pips REAL, reaction_4h_pips REAL,
            reaction_12h_pips REAL, reaction_24h_pips REAL, updated_ts TEXT NOT NULL,
            PRIMARY KEY(weekend_id,instrument)
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
    shadow_cols = {row[1] for row in c.execute("PRAGMA table_info(shadow_trials)").fetchall()}
    for name, ddl in {
        "outcome_cost_r":"REAL",
        "effective_target":"REAL",
        "effective_stop":"REAL",
    }.items():
        if name not in shadow_cols:
            c.execute(f"ALTER TABLE shadow_trials ADD COLUMN {name} {ddl}")

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
            updated_ts TEXT NOT NULL,
            current_units REAL
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
    c.execute("CREATE INDEX IF NOT EXISTS idx_weekend_context ON weekend_context(weekend_id,instrument,bucket_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_weekend_sessions_open ON weekend_sessions(instrument,opened_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_active_research_status ON active_research_rules(status,activated_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_research_rule_audit ON research_rule_audit(source,rule_key,ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_autonomous_stage ON autonomous_hypotheses(stage,validation_samples)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_autonomous_score ON autonomous_hypotheses(score,edge)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_rule_compat ON research_rule_compatibility(compatible,checked_ts)")
    # Step 17 migrations for existing persistent databases.
    signal_cols={row[1] for row in c.execute("PRAGMA table_info(signals)").fetchall()}
    for name,ddl in {
        "ensemble_decision_id":"TEXT","ensemble_direction":"TEXT","ensemble_confidence":"REAL",
        "ensemble_agreement":"REAL","ensemble_diversity":"REAL","ensemble_weight_version":"TEXT"
    }.items():
        if name not in signal_cols:c.execute(f"ALTER TABLE signals ADD COLUMN {name} {ddl}")
    tm_cols={row[1] for row in c.execute("PRAGMA table_info(trade_memory)").fetchall()}
    for name,ddl in {
        "ensemble_decision_id":"TEXT","ensemble_direction":"TEXT","ensemble_confidence":"REAL",
        "ensemble_agreement":"REAL","ensemble_diversity":"REAL","ensemble_weight_version":"TEXT",
        "ensemble_context_json":"TEXT NOT NULL DEFAULT '{}'"
    }.items():
        if name not in tm_cols:c.execute(f"ALTER TABLE trade_memory ADD COLUMN {name} {ddl}")

    c.execute("CREATE INDEX IF NOT EXISTS idx_market_regime_history ON market_regime_history(instrument,candle_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_strategy_health_status ON strategy_health(status,updated_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_strategy_health_audit ON strategy_health_audit(setup_variant,ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ai_director_variant_ts ON ai_strategy_director_decisions(setup_variant,ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ai_director_state ON ai_strategy_director_decisions(recommended_state,ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ai_director_outcomes_decision ON ai_strategy_director_outcomes(director_decision_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_adaptive_risk_variant_ts ON adaptive_risk_decisions(setup_variant,ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_adaptive_risk_blocks ON adaptive_risk_decisions(emergency_stop,allow_new_trades,ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trade_memory_strategy ON trade_memory(strategy,status,exit_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trade_memory_regime ON trade_memory(market_regime_entry,status,exit_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trade_memory_symbol ON trade_memory(symbol,status,exit_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trade_memory_entry_ts ON trade_memory(entry_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_trade_memory_degradation_status ON trade_memory_degradation(status,ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_candidate_strategy_status ON candidate_strategies(status,generated_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_candidate_strategy_parent ON candidate_strategies(strategy_id,parameter_name,generated_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_adaptive_events_run ON adaptive_learning_events(run_id,stage,ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_concept_drift_status ON concept_drift_alerts(status,ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_candidate_registry_state ON candidate_registry(current_state,updated_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_validation_candidate ON candidate_validation_runs(candidate_id,started_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_paper_candidate_status ON candidate_paper_trades(candidate_id,status,created_ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_validation_events_candidate ON validation_events(candidate_id,ts)")
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


def structure_close_confirmed(c: List[Dict[str, Any]]) -> tuple[bool, bool]:
    """Research-only M5 structure using close-confirmed breakout of prior range."""
    if len(c) < 31:
        return False, False
    prior = c[-31:-16]
    recent = c[-16:]
    if len(prior) < 10 or len(recent) < 10:
        return False, False

    prior_high = max(float(x["h"]) for x in prior)
    prior_low = min(float(x["l"]) for x in prior)

    bull = any(float(x["c"]) > prior_high for x in recent)
    bear = any(float(x["c"]) < prior_low for x in recent)

    return bull and not bear, bear and not bull


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
    if RECOVERY_MANAGER_ENABLED:
        return await recovery_manager.broker_request(
            client,method,path,params=params,body=body,
            critical=path in (
                "/v3/accounts/{account}",
                "/v3/accounts/{account}/openTrades",
                "/v3/accounts/{account}/openPositions",
                "/v3/accounts/{account}/pendingOrders",
            ),
            allow_retry=(method.upper()=="GET")
        )
    if not ACCOUNT or not TOKEN:
        raise RuntimeError("Faltan OANDA_ACCOUNT_ID/OANDA_TOKEN")
    url=OANDA+path.replace("{account}",ACCOUNT)
    r=await client.request(method,url,params=params,json=body,
                           headers={"Authorization":f"Bearer {TOKEN}","Content-Type":"application/json"},
                           timeout=15)
    if r.status_code>=400:
        try: msg=r.json().get("errorMessage") or r.json().get("errorCode")
        except Exception: msg=r.text[:250]
        raise RuntimeError(f"OANDA Practice HTTP {r.status_code}: {msg}")
    return r.json()

async def refresh_instrument_metadata(client: httpx.AsyncClient, symbols: Optional[List[str]]=None, *, force: bool=False) -> Dict[str,Any]:
    """Refresh broker-owned instrument metadata without granting execution authority."""
    global _INSTRUMENT_METADATA_REFRESH_TS
    wanted=list(dict.fromkeys(symbols or SCAN_INSTRUMENTS))
    now=datetime.now(timezone.utc)
    if (not force and _INSTRUMENT_METADATA_REFRESH_TS is not None and
        (now-_INSTRUMENT_METADATA_REFRESH_TS).total_seconds() < INSTRUMENT_METADATA_REFRESH_SECONDS):
        return {"refreshed":False,"cached":True,"metadata":INSTRUMENT_REGISTRY.snapshot(wanted)}
    if not wanted:
        return {"refreshed":False,"cached":True,"metadata":{}}
    payload=await req(client,"GET","/v3/accounts/{account}/instruments",params={"instruments":",".join(wanted)})
    updated=INSTRUMENT_REGISTRY.update_from_oanda(payload)
    _INSTRUMENT_METADATA_REFRESH_TS=now
    missing=[x for x in wanted if x not in updated]
    return {"refreshed":True,"cached":False,"updated":sorted(updated),"missing":missing,
            "metadata":INSTRUMENT_REGISTRY.snapshot(wanted)}

def instrument_sizing(instrument: str, requested_units: float, entry: float, stop: float,
                      risk_context: Optional[Dict[str,Any]]=None,
                      quote_home_conversion: Optional[float]=None) -> Dict[str,Any]:
    """Conservative instrument-aware sizing. It may reduce legacy units, never increase them."""
    meta=instrument_metadata(instrument)
    hard_cap=max(0.0,min(abs(float(requested_units)),float(UNITS),float(managed_value("execution.trade_units",UNITS))))
    risk_context=risk_context or {}
    nav=_risk_float(risk_context.get("nav"))
    account_currency=str(risk_context.get("account_currency") or "").upper()
    parts=InstrumentRegistry.normalize_symbol(instrument).split("_")
    quote_currency=parts[-1] if len(parts)==2 else ""
    conversion=_risk_float(quote_home_conversion)
    if conversion is None and account_currency and quote_currency==account_currency:
        conversion=1.0
    stop_distance=abs(float(entry)-float(stop))
    risk_budget=(nav*float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION))) if nav and nav>0 else None
    risk_limited=None
    if risk_budget is not None and stop_distance>0 and conversion is not None and conversion>0:
        risk_limited=risk_budget/(stop_distance*conversion)
        hard_cap=min(hard_cap,risk_limited)
    normalized=abs(float(meta.normalize_units(hard_cap,allow_zero=True)))
    if normalized>abs(float(requested_units)):
        normalized=abs(float(requested_units))
    return {
        "instrument":InstrumentRegistry.normalize_symbol(instrument),
        "requested_units":abs(float(requested_units)),
        "effective_units":normalized,
        "risk_limited_units":risk_limited,
        "risk_budget_home":risk_budget,
        "stop_distance_price":stop_distance,
        "stop_distance_pips":stop_distance/max(meta.pip_size,1e-12),
        "quote_currency":quote_currency,
        "account_currency":account_currency or None,
        "quote_home_conversion":conversion,
        "metadata_source":meta.source,
        "never_increases_legacy_units":True,
    }

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
    return instrument_metadata(inst).pip_size

def pips_between(a: float, b: float, inst: str) -> float:
    return abs(float(a)-float(b))/pip_size(inst)

def _direction_hypothesis(h1, m15, m5, m1, inst: str, sig: str) -> Dict[str, Any]:
    """Evaluate BUY or SELL independently. Higher timeframes are evidence, not a direction lock."""
    c60, c15, c5, c1 = ([x["c"] for x in h1], [x["c"] for x in m15],
                         [x["c"] for x in m5], [x["c"] for x in m1])
    h20,h50 = ema(c60,20),ema(c60,50)
    e20,e50,e5,e9,e1 = ema(c15,20),ema(c15,50),ema(c5,20),ema(c1,9),ema(c1,20)
    a60,a15,a5,a1 = atr(h1),atr(m15),atr(m5),atr(m1)

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
    cbull,cbear=structure_close_confirmed(m5)
    m5m=mom(m5,6)
    m5_structure = (mb and m5m>0) if sig=="BUY" else (ms and m5m<0)
    m5_structure_close_confirmed = (cbull and m5m>0) if sig=="BUY" else (cbear and m5m<0)
    m5_momentum = sign*m5m > 0

    pc,pr=pullbacks(m5,e5,sig)
    second=pc>=2 and pr

    last=m1[-1]
    ph,pl,mm=swing(m1[:-1],"h",7),swing(m1[:-1],"l",7),mom(m1,4)
    m1_ema9_side_ok = (last["c"]>e9[-1]) if sig=="BUY" else (last["c"]<e9[-1])
    m1_candle_color_ok = (last["c"]>last["o"]) if sig=="BUY" else (last["c"]<last["o"])
    cb=m1_ema9_side_ok and mm>0 and m1_candle_color_ok
    cs=m1_ema9_side_ok and mm<0 and m1_candle_color_ok
    confirm=(cb and (last["c"]>ph or mm>.00012)) if sig=="BUY" else (cs and (last["c"]<pl or mm<-.00012))

    # Alternative M1 evidence retained separately from canonical confirmation.
    # Strong directional momentum may substitute for candle colour, but the flag
    # does not rewrite m1_confirmation itself.
    shadow_cb=last["c"]>e9[-1] and mm>0 and (last["c"]>last["o"] or mm>.00012)
    shadow_cs=last["c"]<e9[-1] and mm<0 and (last["c"]<last["o"] or mm<-.00012)
    m1_shadow_confirm=(shadow_cb and (last["c"]>ph or mm>.00012)) if sig=="BUY" else (shadow_cs and (last["c"]<pl or mm<-.00012))
    m1_exception_shadow = bool(m1_shadow_confirm and not confirm)

    m1_momentum = sign*mm > 0

    ext=abs(last["c"]-e1[-1])/max(a1,1e-9)
    vols=[atr(m1[:len(m1)-i]) for i in range(18) if len(m1)-i>15]
    vol=a1/max(mean(vols),1e-9)
    entry=last["c"]

    ss=swing(m1,"l" if sig=="BUY" else "h",12)
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
    rr_raw=structural_reward/risk if structural_reward>0 else 0.0
    rr=MIN_RR
    min_tp_distance=max(risk*MIN_RR, MIN_TAKE_PROFIT_PIPS*pip)
    target=entry+min_tp_distance if sig=="BUY" else entry-min_tp_distance
    barrier_allows_target=True
    if active and barrier_class=="STRONG" and not bool(active.get("broken")):
        buffer=risk*STRUCTURAL_BARRIER_BUFFER_R
        cap=barrier-buffer if sig=="BUY" else barrier+buffer
        barrier_allows_target=(target<=cap) if sig=="BUY" else (target>=cap)
        if active and barrier_class=="STRONG" and not bool(active.get("broken")) and room_r is not None:
            rr_raw=min(rr_raw,max(0.0,room_r))
    actual_rr=abs(target-entry)/max(risk,1e-12)
    tp_pips=pips_between(entry,target,inst)

    sess=session_info(last["t"])
    intraday=detect_session_regime(m5,a5)
    session_support=intraday.get("direction")==sig
    session_opposes=intraday.get("direction") in ("BUY","SELL") and intraday.get("direction")!=sig
    session_strength=float(intraday.get("strength") or 0.0)

    # Direction hierarchy for an intraday strategy:
    # H1 is slow context, while current-session regime and M15/M5 carry more weight.
    dscore=0.0
    dscore += 8 if h1_support else (-5 if h1_opposes else 2)
    dscore += 14 if m15_support else (-9 if m15_opposes else 3)
    if session_support:
        dscore += 12 + 12*session_strength
    elif session_opposes:
        dscore -= 8 + 10*session_strength
    else:
        dscore += 2
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

    countertrend = session_opposes and m15_opposes
    transition = (session_opposes or m15_opposes or h1_opposes) and (m5_structure or confirm)

    checks={
        "h1_context": h1_support,
        "m15_context": m15_support,
        "m5_structure": m5_structure,
        "second_pullback": second,
        "m1_confirmation": confirm,
        "minimum_rr": actual_rr>=MIN_RR and rr_raw>=MIN_ENTRY_RR,
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
        "minimum_rr": actual_rr>=MIN_RR-1e-9 and rr_raw>=MIN_ENTRY_RR,
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
            "m5_structure_close_confirmed":bool(m5_structure_close_confirmed),
            "extension_atr":float(ext),"volatility_ratio":float(vol),
            "second_pullback":second,"m1_confirm":confirm,"m1_shadow_confirm":m1_shadow_confirm,
            "m1_exception_shadow":m1_exception_shadow,
            "m1_ema9_side_ok":bool(m1_ema9_side_ok),
            "m1_candle_color_ok":bool(m1_candle_color_ok),
            "session":sess,"session_regime":intraday,
        }
    }



def _regime_closes(candles_: List[Dict[str, Any]]) -> List[float]:
    return [float(x["c"]) for x in candles_ if x.get("c") is not None]


def _regime_returns(candles_: List[Dict[str, Any]]) -> List[float]:
    closes=_regime_closes(candles_)
    out=[]
    for i in range(1,len(closes)):
        if closes[i-1] != 0:
            out.append((closes[i]-closes[i-1])/closes[i-1])
    return out


def _regime_efficiency_ratio(candles_: List[Dict[str, Any]], period: int = 30) -> float:
    closes=_regime_closes(candles_[-(period+1):])
    if len(closes)<3:
        return 0.0
    net=abs(closes[-1]-closes[0])
    path=sum(abs(closes[i]-closes[i-1]) for i in range(1,len(closes)))
    return float(net/path) if path>0 else 0.0


def _regime_directional_slope(candles_: List[Dict[str, Any]], period: int = 30) -> float:
    cs=candles_[-max(8,period):]
    closes=_regime_closes(cs)
    if len(closes)<8:
        return 0.0
    a=atr(cs)
    if a<=0:
        return 0.0
    fast=ema(closes,min(8,len(closes)))[-1]
    slow=ema(closes,min(21,len(closes)))[-1]
    return float((fast-slow)/a)


def _regime_return_zscore(candles_: List[Dict[str, Any]], lookback: int = 60) -> float:
    rs=_regime_returns(candles_[-(lookback+2):])
    if len(rs)<12:
        return 0.0
    last=rs[-1]
    hist=rs[:-1]
    mean=sum(hist)/len(hist)
    var=sum((x-mean)*(x-mean) for x in hist)/len(hist)
    sd=math.sqrt(var)
    return float((last-mean)/sd) if sd>1e-12 else 0.0


def _regime_range_compression(candles_: List[Dict[str, Any]], short: int = 12, long: int = 50) -> float:
    if len(candles_)<long:
        return 1.0
    def span(xs):
        return max(float(x["h"]) for x in xs)-min(float(x["l"]) for x in xs)
    s=span(candles_[-short:])
    l=span(candles_[-long:])
    return float(s/l) if l>1e-12 else 1.0


def detect_market_regime(h1: List[Dict[str, Any]], m15: List[Dict[str, Any]],
                         m5: List[Dict[str, Any]], m1: List[Dict[str, Any]],
                         instrument: str) -> Dict[str, Any]:
    """
    Multi-metric regime classifier. Observation only: it does NOT alter execution,
    confidence, strategy activation, or learned filters.

    Uses trend agreement across H1/M15/M5, efficiency ratio, ATR-normalized EMA
    slopes, realized volatility, range compression, short-term shock z-score and
    cross-timeframe disagreement.
    """
    ts=(m1[-1]["t"].isoformat() if m1 and hasattr(m1[-1].get("t"),"isoformat")
        else str(m1[-1].get("t")) if m1 else now_iso())

    if not MARKET_REGIME_ENABLED:
        return {"market_regime":"DISABLED","confidence":0.0,"volatility_state":"UNKNOWN",
                "trend_strength":0.0,"supporting_metrics":{},"timestamp":ts}

    if min(len(h1),len(m15),len(m5),len(m1)) < MARKET_REGIME_MIN_CANDLES:
        return {"market_regime":"UNCERTAIN","confidence":0.15,"volatility_state":"UNKNOWN",
                "trend_strength":0.0,
                "supporting_metrics":{"reason":"insufficient_candles",
                                      "counts":{"H1":len(h1),"M15":len(m15),"M5":len(m5),"M1":len(m1)}},
                "timestamp":ts}

    slopes={
        "h1":_regime_directional_slope(h1,30),
        "m15":_regime_directional_slope(m15,35),
        "m5":_regime_directional_slope(m5,40),
    }
    efficiencies={
        "h1":_regime_efficiency_ratio(h1,24),
        "m15":_regime_efficiency_ratio(m15,32),
        "m5":_regime_efficiency_ratio(m5,40),
    }

    # Volatility ratio: current ATR vs longer rolling ATR proxy on M15 and M5.
    atr15_now=atr(m15[-20:])
    atr15_base=atr(m15[-80:-20]) if len(m15)>=80 else atr(m15[:-20] or m15)
    atr5_now=atr(m5[-30:])
    atr5_base=atr(m5[-120:-30]) if len(m5)>=120 else atr(m5[:-30] or m5)
    v15=atr15_now/max(atr15_base,1e-12)
    v5=atr5_now/max(atr5_base,1e-12)

    # Current M1 realized volatility relative to its previous window.
    m1_now=atr(m1[-20:])
    m1_base=atr(m1[-80:-20]) if len(m1)>=80 else atr(m1)
    v1=m1_now/max(m1_base,1e-12)
    vol_ratio=float(0.50*v15 + 0.30*v5 + 0.20*v1)

    if vol_ratio>=MARKET_REGIME_ABNORMAL_VOL_RATIO:
        volatility_state="ABNORMAL"
    elif vol_ratio>=MARKET_REGIME_HIGH_VOL_RATIO:
        volatility_state="HIGH"
    elif vol_ratio<=MARKET_REGIME_LOW_VOL_RATIO:
        volatility_state="LOW"
    else:
        volatility_state="NORMAL"

    weights={"h1":0.45,"m15":0.35,"m5":0.20}
    signed_trend=sum(weights[k]*math.tanh(slopes[k]/1.25) for k in weights)
    efficiency=sum(weights[k]*efficiencies[k] for k in weights)
    trend_strength=float(clamp(abs(signed_trend)*0.62 + efficiency*0.38,0.0,1.0))

    signs=[]
    for k in ("h1","m15","m5"):
        signs.append(1 if slopes[k]>0.12 else -1 if slopes[k]<-0.12 else 0)
    nonzero=[x for x in signs if x]
    agreement=(abs(sum(nonzero))/len(nonzero)) if nonzero else 0.0
    disagreement=1.0-agreement

    compression=_regime_range_compression(m15,12,50)
    shock_z=abs(_regime_return_zscore(m1,80))
    extreme_candle=abs(float(m1[-1]["c"])-float(m1[-1]["o"]))/max(atr(m1[-30:]),1e-12)

    abnormality=float(clamp(
        0.38*min(1.0,shock_z/4.0) +
        0.27*min(1.0,extreme_candle/2.5) +
        0.20*min(1.0,max(0.0,vol_ratio-1.5)/1.5) +
        0.15*disagreement,
        0.0,1.0))

    # Separate regime candidates. Range is a positive diagnosis, not merely no-trend.
    bullish_score=float(clamp(max(0.0,signed_trend)*0.58 + trend_strength*0.27 + agreement*0.15,0,1))
    bearish_score=float(clamp(max(0.0,-signed_trend)*0.58 + trend_strength*0.27 + agreement*0.15,0,1))
    range_score=float(clamp((1.0-trend_strength)*0.55 + (1.0-efficiency)*0.25 +
                            max(0.0,0.45-compression)/0.45*0.20,0,1))

    if abnormality>=0.66 or volatility_state=="ABNORMAL":
        regime="ABNORMAL_UNCERTAIN"
        winning=max(abnormality,0.66)
        margin=winning-max(bullish_score,bearish_score,range_score)*0.35
    else:
        candidates={"BULL_TREND":bullish_score,"BEAR_TREND":bearish_score,"RANGE":range_score}
        ordered=sorted(candidates.items(),key=lambda x:x[1],reverse=True)
        regime,winning=ordered[0]
        margin=winning-ordered[1][1]
        if regime in ("BULL_TREND","BEAR_TREND") and trend_strength<float(managed_value("regime.trend_threshold",MARKET_REGIME_TREND_THRESHOLD)):
            regime="RANGE" if range_score>=float(managed_value("regime.range_threshold",MARKET_REGIME_RANGE_THRESHOLD)) else "UNCERTAIN"
        elif regime=="RANGE" and range_score<float(managed_value("regime.range_threshold",MARKET_REGIME_RANGE_THRESHOLD)):
            regime="UNCERTAIN"

    # Confidence combines score margin, timeframe agreement, data sufficiency and anomaly penalty.
    data_factor=min(1.0,min(len(h1),len(m15),len(m5),len(m1))/max(1.0,MARKET_REGIME_MIN_CANDLES))
    base_conf=0.38*max(0.0,min(1.0,winning)) + 0.27*agreement + 0.20*min(1.0,max(0.0,margin)*2.5) + 0.15*data_factor
    if regime=="RANGE":
        base_conf=0.50*range_score+0.20*(1.0-trend_strength)+0.15*data_factor+0.15*(1.0-abnormality)
    elif regime=="ABNORMAL_UNCERTAIN":
        base_conf=0.55*abnormality+0.20*min(1.0,shock_z/4.0)+0.15*disagreement+0.10*data_factor
    elif regime=="UNCERTAIN":
        base_conf=0.30+0.25*disagreement+0.20*abnormality
    confidence=float(clamp(base_conf,0.0,0.99))

    metrics={
        "signed_trend":float(signed_trend),
        "timeframe_agreement":float(agreement),
        "timeframe_disagreement":float(disagreement),
        "efficiency_ratio":float(efficiency),
        "slopes_atr":{k:float(v) for k,v in slopes.items()},
        "efficiency_by_tf":{k:float(v) for k,v in efficiencies.items()},
        "atr_ratio":{"m15":float(v15),"m5":float(v5),"m1":float(v1),"composite":float(vol_ratio)},
        "range_compression":float(compression),
        "return_shock_z":float(shock_z),
        "extreme_candle_atr":float(extreme_candle),
        "abnormality_score":float(abnormality),
        "candidate_scores":{"bull":bullish_score,"bear":bearish_score,"range":range_score},
        "candles":{"H1":len(h1),"M15":len(m15),"M5":len(m5),"M1":len(m1)},
    }

    return {
        "market_regime":regime,
        "confidence":confidence,
        "volatility_state":volatility_state,
        "trend_strength":trend_strength,
        "supporting_metrics":metrics,
        "timestamp":ts,
    }


def record_market_regime(instrument: str, candle_ts: str, regime: Dict[str, Any]) -> None:
    if not MARKET_REGIME_ENABLED or not candle_ts:
        return
    c=conn()
    c.execute("""INSERT INTO market_regime_history(
        ts,candle_ts,instrument,market_regime,confidence,volatility_state,
        trend_strength,abnormality_score,supporting_metrics_json)
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(instrument,candle_ts) DO UPDATE SET
        ts=excluded.ts,market_regime=excluded.market_regime,confidence=excluded.confidence,
        volatility_state=excluded.volatility_state,trend_strength=excluded.trend_strength,
        abnormality_score=excluded.abnormality_score,
        supporting_metrics_json=excluded.supporting_metrics_json""",
      (now_iso(),candle_ts,instrument,regime.get("market_regime","UNCERTAIN"),
       float(regime.get("confidence") or 0),regime.get("volatility_state","UNKNOWN"),
       float(regime.get("trend_strength") or 0),
       float((regime.get("supporting_metrics") or {}).get("abnormality_score") or 0),
       json.dumps(regime.get("supporting_metrics") or {},separators=(",",":"))))
    c.commit();c.close()


def log_market_regime(instrument: str, regime: Dict[str, Any]) -> None:
    previous=(state.get("market_regimes") or {}).get(instrument)
    changed=(not previous or previous.get("market_regime")!=regime.get("market_regime") or
             previous.get("volatility_state")!=regime.get("volatility_state"))
    if changed or not MARKET_REGIME_LOG_CHANGES_ONLY:
        m=regime.get("supporting_metrics") or {}
        log.info("REGIME %s regime=%s conf=%.3f vol=%s trend=%.3f agreement=%.3f shock_z=%.2f abnormal=%.3f",
                 instrument,regime.get("market_regime"),float(regime.get("confidence") or 0),
                 regime.get("volatility_state"),float(regime.get("trend_strength") or 0),
                 float(m.get("timeframe_agreement") or 0),float(m.get("return_shock_z") or 0),
                 float(m.get("abnormality_score") or 0))


def _legacy_v331_runtime_score(hyp: Dict[str, Any], m5: List[Dict[str, Any]], sig: str) -> float:
    """Reproduce the V331 replay score from decision-time hypothesis state only."""
    f=hyp.get("filters") or {}
    m=hyp.get("metrics") or {}
    sign=1.0 if sig=="BUY" else -1.0
    hg=float(m.get("h1_gap_atr",0) or 0); hs=float(m.get("h1_slope_atr",0) or 0)
    mg=float(m.get("m15_gap_atr",0) or 0); ms=float(m.get("m15_slope_atr",0) or 0)
    h1_opposes=(-sign*hg>.12 and -sign*hs>.05)
    m15_opposes=(-sign*mg>.15 and -sign*ms>.07)
    m5_momentum=sign*float(m.get("m5_momentum",0) or 0)>0
    m1_momentum=sign*float(m.get("m1_momentum",0) or 0)>0
    e5=ema([x["c"] for x in m5],20)
    pc,pr=pullbacks(m5,e5,sig)
    components={
        "h1_support":bool(f.get("h1_context")),"m15_support":bool(f.get("m15_context")),
        "h1_opposes":h1_opposes,"m15_opposes":m15_opposes,
        "m5_structure":bool(f.get("m5_structure")),"m5_momentum":m5_momentum,
        "confirm":bool(f.get("m1_confirmation")),"m1_momentum":m1_momentum,
        "second":bool(f.get("second_pullback")),"pc":pc,"pr":pr,
        "rr_raw":float(hyp.get("rr_raw",0) or 0),"min_rr":float(MIN_RR),
        "vol":float(m.get("volatility_ratio",0) or 0),"ext":float(m.get("extension_atr",0) or 0),
        "session_ok":bool((m.get("session") or {}).get("ok")),
        "broken":len((hyp.get("structure_context") or {}).get("broken_levels",[])),
    }
    return legacy_v331_score(components)


def _forward_experiment_active(instrument: str) -> bool:
    symbol=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    return bool(
        forward_policy(symbol).get("experiment_id")
        and TRADING_ENVIRONMENT=="PAPER"
        and PRIMARY_OANDA_ENV=="practice"
        and OANDA.endswith("fxpractice.oanda.com")
    )


def forward_experiment_gate(r: Dict[str, Any]) -> Dict[str, Any]:
    symbol=InstrumentRegistry.normalize_symbol((r or {}).get("instrument") or PRIMARY_INSTRUMENT)
    if not _forward_experiment_active(symbol):
        return {"ok":True,"active":False,"instrument":symbol,"experiment_id":None,"reason":"FORWARD_EXPERIMENT_INACTIVE"}
    out=evaluate_forward_experiment(symbol,(r or {}).get("features") or {})
    out=dict(out); out["active"]=True
    runtime_signal=str((r or {}).get("signal") or "").upper()
    out["direction"]=runtime_signal
    if symbol=="EUR_USD" and out.get("legacy_v331_chosen_direction"):
        out["legacy_direction_matches_runtime"]=bool(out.get("legacy_v331_chosen_direction")==runtime_signal)
    return out


def analyze(h1, m15, m5, m1, inst) -> Dict[str, Any]:
    regime=detect_market_regime(h1,m15,m5,m1,inst)
    buy=_direction_hypothesis(h1,m15,m5,m1,inst,"BUY")
    sell=_direction_hypothesis(h1,m15,m5,m1,inst,"SELL")

    legacy_v331_buy_score=_legacy_v331_runtime_score(buy,m5,"BUY")
    legacy_v331_sell_score=_legacy_v331_runtime_score(sell,m5,"SELL")
    legacy_v331_chosen_direction,legacy_v331_directional_score=choose_legacy_v331_direction(
        legacy_v331_buy_score,legacy_v331_sell_score
    )

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
        "m1_shadow_confirm":1 if mt.get("m1_shadow_confirm") else 0,
        "m1_exception_shadow":1 if mt.get("m1_exception_shadow") else 0,
        "m1_ema9_side_ok":1 if mt.get("m1_ema9_side_ok") else 0,
        "m1_candle_color_ok":1 if mt.get("m1_candle_color_ok") else 0,
        "extension_atr":mt["extension_atr"],"volatility_ratio":mt["volatility_ratio"],
        "rr_raw":float(chosen["rr_raw"]),
        "room_to_barrier_r":float(chosen["room_to_barrier_r"]) if chosen["room_to_barrier_r"] is not None else None,
        "barrier_score":chosen["barrier_score"],"barrier_class":chosen["barrier_class"],
        "broken_barriers":len(chosen["structure_context"].get("broken_levels",[])),
        "session_ok":1 if sess["ok"] else 0,"news_confirm":0,"news_contradict":0,
        "blocked":0,"hour_ny":float(sess["hour"]),
        # Diagnostic fields for both hypotheses:
        "buy_score":buy_score,"sell_score":sell_score,"direction_edge":edge,
        # Frozen V331 research semantics used only by instrument-scoped forward experiments.
        "legacy_v331_buy_score":float(legacy_v331_buy_score),
        "legacy_v331_sell_score":float(legacy_v331_sell_score),
        "legacy_v331_directional_score":float(legacy_v331_directional_score),
        "legacy_v331_chosen_direction":legacy_v331_chosen_direction,
        "h1_gap_atr":mt["h1_gap_atr"],"h1_slope_atr":mt["h1_slope_atr"],
        "transition_state":1 if chosen["transition"] else 0,
        "session_direction":mt.get("session_regime",{}).get("direction","NEUTRAL"),
        "session_strength":float(mt.get("session_regime",{}).get("strength",0) or 0),
        "session_displacement_atr":float(mt.get("session_regime",{}).get("displacement_atr",0) or 0),
        "session_momentum_atr":float(mt.get("session_regime",{}).get("momentum_atr",0) or 0),
    }

    forward_flags = forward_entry_pattern_flags(features)
    # Research telemetry only; these names are deliberately excluded from FEATURE_COLUMNS.
    features["low_room_low_rr_shadow"] = 1 if forward_flags["low_room_low_rr"] else 0
    features["low_room_extended_shadow"] = 1 if forward_flags["low_room_extended"] else 0

    safety=dict(chosen["safety_checks"])
    safety["valid_direction"]=sig in ("BUY","SELL")

    return {
        "instrument":inst,"signal":sig,"technical":tech,"score":tech,
        "buy_score":buy_score,"sell_score":sell_score,"direction_edge":edge,
        "legacy_v331_buy_score":float(legacy_v331_buy_score),
        "legacy_v331_sell_score":float(legacy_v331_sell_score),
        "legacy_v331_directional_score":float(legacy_v331_directional_score),
        "legacy_v331_chosen_direction":legacy_v331_chosen_direction,
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
        "market_regime":regime,
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


def refresh_discovered_patterns(instrument: Optional[str]=None) -> Dict[str, Any]:
    """Re-evaluate discovery evidence within one instrument namespace.

    The legacy global table remains untouched for historical diagnostics; execution
    confidence reads only the instrument-scoped table.
    """
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    c=conn()
    rows=c.execute("""
        SELECT ls.label, s.signal, s.alignment, s.features_json, s.filters_json
        FROM learning_samples ls JOIN signals s ON s.id=ls.signal_id
        WHERE ls.label IN (0,1) AND ls.instrument=? AND s.instrument=?
        ORDER BY ls.id ASC
    """,(instrument,instrument)).fetchall()
    if not rows:
        c.close(); return {"instrument":instrument,"resolved_samples":0,"validated_patterns":0}
    instrument_wr=sum(int(x["label"]) for x in rows)/len(rows)
    buckets: Dict[str,Dict[str,Any]]={}
    for row in rows:
        rr={"signal":row["signal"],"alignment":row["alignment"],
            "features":json.loads(row["features_json"] or "{}"),
            "filters":json.loads(row["filters_json"] or "{}")}
        for family,value in candidate_patterns(rr).items():
            key=f"{family}={value}"
            b=buckets.setdefault(key,{"family":family,"value":value,"samples":0,"wins":0})
            b["samples"]+=1; b["wins"]+=int(row["label"])
    validated=0
    for key,b in buckets.items():
        n,wins=b["samples"],b["wins"]; wr=wins/n; edge=wr-instrument_wr
        weight=edge*(n/(n+DISCOVERY_SHRINKAGE))
        is_valid=int(n>=DISCOVERY_MIN_SAMPLES and abs(edge)>=DISCOVERY_MIN_EDGE)
        validated+=is_valid
        c.execute("""INSERT INTO instrument_discovered_patterns(
          instrument,pattern_key,family,value,samples,wins,win_rate,instrument_win_rate,edge,weight,validated,updated_ts)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(instrument,pattern_key) DO UPDATE SET samples=excluded.samples,wins=excluded.wins,
          win_rate=excluded.win_rate,instrument_win_rate=excluded.instrument_win_rate,edge=excluded.edge,
          weight=excluded.weight,validated=excluded.validated,updated_ts=excluded.updated_ts""",
          (instrument,key,b["family"],b["value"],n,wins,wr,instrument_wr,edge,weight,is_valid,now_iso()))
    c.commit();c.close()
    return {"instrument":instrument,"resolved_samples":len(rows),"validated_patterns":validated,"instrument_win_rate":instrument_wr}

def discovery_adjustment(r: Dict[str, Any]) -> Dict[str, Any]:
    """Apply validated patterns from this instrument only."""
    pats=candidate_patterns(r); instrument=InstrumentRegistry.normalize_symbol(r.get("instrument") or PRIMARY_INSTRUMENT)
    c=conn(); rows=c.execute("SELECT * FROM instrument_discovered_patterns WHERE instrument=? AND validated=1",(instrument,)).fetchall();c.close()
    by_key={x["pattern_key"]:dict(x) for x in rows}; matches=[];raw=0.0
    for family,value in pats.items():
        key=f"{family}={value}"
        if key in by_key:
            item=by_key[key];raw+=float(item["weight"]);matches.append({"pattern":key,"instrument":instrument,"samples":item["samples"],"win_rate":item["win_rate"],"weight":item["weight"]})
    return {"adjustment":clamp(raw*0.35,-0.15,0.15),"matches":matches,"candidate_patterns":pats,"instrument":instrument}

def wilson_lower_bound(wins: int, total: int, z: float = 1.28) -> float:
    """Conservative lower bound (~80% two-sided) so small samples do not look overconfident."""
    if total <= 0:
        return 0.0
    p = wins / total
    den = 1 + z*z/total
    center = p + z*z/(2*total)
    margin = z * math.sqrt((p*(1-p) + z*z/(4*total))/total)
    return max(0.0, (center - margin) / den)


def recent_performance(instrument: Optional[str]=None) -> Dict[str, Any]:
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    c = conn()
    rows = c.execute(
        "SELECT label FROM learning_samples WHERE instrument=? AND executed=1 AND label IN (0,1) ORDER BY id DESC LIMIT ?",
        (instrument,RECENT_PERFORMANCE_WINDOW)
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
    instrument=InstrumentRegistry.normalize_symbol(r.get("instrument") or PRIMARY_INSTRUMENT)
    c = conn()
    # Adaptive execution confidence is intentionally based only on trades the bot
    # actually executed. Rejected/counterfactual samples remain research evidence.
    total = c.execute(
        "SELECT COUNT(*) n FROM learning_samples WHERE instrument=? AND executed=1 AND label IN (0,1)",(instrument,)
    ).fetchone()["n"]
    wins = c.execute(
        "SELECT COUNT(*) n FROM learning_samples WHERE instrument=? AND executed=1 AND label=1",(instrument,)
    ).fetchone()["n"]
    variant = setup_variant(r)

    rows = c.execute("""
        SELECT ls.label, s.setup_variant
        FROM learning_samples ls
        JOIN signals s ON s.id=ls.signal_id
        WHERE ls.instrument=? AND s.instrument=? AND ls.executed=1 AND ls.label IN (0,1)
        ORDER BY ls.id DESC
    """,(instrument,instrument)).fetchall()
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
            "instrument": instrument,
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
        "instrument": instrument,
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

    perf = recent_performance(r.get("instrument"))
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
    symbol=InstrumentRegistry.normalize_symbol(r.get("instrument") or PRIMARY_INSTRUMENT)
    experiment_policy=forward_policy(symbol) if _forward_experiment_active(symbol) else forward_policy("")
    if rr < MIN_ENTRY_RR and r.get("barrier_class")=="STRONG":
        return {"ok":False,"reason":f"barrera fuerte deja solo {rr:.2f}R < {MIN_ENTRY_RR:.2f}R de admisión"}

    # M1 remains the execution trigger, but validated admission evidence can
    # substitute for the stricter canonical swing/strong-momentum confirmation.
    if (M1_CONFIRMATION_REQUIRED and not experiment_policy.get("bypass_m1_confirmation")
        and not bool((r.get("filters") or {}).get("m1_confirmation"))):
        profile=instrument_profile(symbol)
        if not profile.has_exception("M1_ALTERNATIVE_ADMISSION"):
            return {"ok":False,"reason":"falta confirmación M1 canónica; excepción específica no autorizada para este instrumento"}
        f_m1 = r.get("features") or {}
        sig_m1 = str(r.get("signal") or "").upper()
        m1_raw = float(f_m1.get("m1_momentum",0) or 0)
        momentum_direction_ok = (
            m1_raw > 0 if sig_m1=="BUY"
            else m1_raw < 0 if sig_m1=="SELL"
            else False
        )
        exception_1 = bool(f_m1.get("m1_exception_shadow"))
        ema9_admission_exception = not bool(f_m1.get("m1_ema9_side_ok"))
        m1_base_admission = bool(
            f_m1.get("m1_ema9_side_ok")
            and momentum_direction_ok
            and f_m1.get("m1_candle_color_ok")
        )
        if not (exception_1 or ema9_admission_exception or m1_base_admission):
            return {"ok":False,"reason":"falta confirmación M1; hipótesis registrada, entrada aplazada"}

    f=r.get("features") or {}

    # The two forward filters are active only in PAPER/practice. The same pure
    # conditions also populate shadow telemetry for prospective audit.
    if paper_forward_filters_active(r.get("instrument")) and not experiment_policy.get("bypass_low_room_vetoes"):
        flags = forward_entry_pattern_flags(f)
        room_raw = f.get("room_to_barrier_r")
        room = None if room_raw is None else float(room_raw)
        rr_entry = float(f.get("rr_raw", r.get("rr_raw",0)) or 0)
        extension = float(f.get("extension_atr",0) or 0)
        if flags["low_room_low_rr"]:
            return {
                "ok":False,
                "reason":(
                    "PAPER_FORWARD_VETO: LOW_ROOM_LOW_RR "
                    f"room={room:.3f}R < {LOW_ROOM_LOW_RR_MAX_ROOM_R:.2f}R; "
                    f"rr={rr_entry:.3f} < {LOW_ROOM_LOW_RR_MAX_ENTRY_RR:.2f}"
                ),
            }
        if flags["low_room_extended"]:
            return {
                "ok":False,
                "reason":(
                    "PAPER_FORWARD_VETO: LOW_ROOM_EXTENDED "
                    f"room={room:.3f}R < {LOW_ROOM_EXTENDED_MAX_ROOM_R:.2f}R; "
                    f"extension={extension:.3f}ATR > {LOW_ROOM_EXTENDED_MIN_EXTENSION_ATR:.2f}ATR"
                ),
            }

    if not ENTRY_TIMING_ENABLED:
        return {"ok":True,"reason":"quality_ok"}

    ext=float(f.get("extension_atr",0) or 0)
    if ext > MAX_ENTRY_EXTENSION_ATR and not experiment_policy.get("bypass_quality_extension"):
        return {"ok":False,"reason":f"entrada tardía/chasing: {ext:.2f} ATR > {MAX_ENTRY_EXTENSION_ATR:.2f}"}

    # Confidence no longer creates an extra extension veto inside the normal
    # 1.50 ATR timing envelope. Hard chasing protection remains unchanged.
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


def _loss_streak(labels: List[int]) -> int:
    streak=0
    for x in reversed(labels):
        if int(x)==0: streak+=1
        else: break
    return streak

def _strategy_rows(variant: str, executed_only: bool=False, since_ts: Optional[str]=None, instrument: Optional[str]=None) -> List[sqlite3.Row]:
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    where=["ls.label IN (0,1)","s.setup_variant=?","ls.instrument=?","s.instrument=?"]; params=[variant,instrument,instrument]
    if executed_only: where.append("ls.executed=1")
    if since_ts: where.append("s.ts>=?"); params.append(since_ts)
    c=conn(); rows=c.execute(f"""SELECT ls.label,ls.executed,ls.blocked,s.ts,s.candle_ts,s.setup_variant
                                 FROM learning_samples ls JOIN signals s ON s.id=ls.signal_id
                                 WHERE {' AND '.join(where)} ORDER BY s.id ASC""",tuple(params)).fetchall(); c.close()
    return rows

def _health_transition(variant,old_status,new_status,evidence_mode,baseline_wr,recent_wr,recent_drop,loss_streak,details):
    if old_status==new_status:return
    c=conn(); c.execute("""INSERT INTO strategy_health_audit(ts,setup_variant,old_status,new_status,evidence_mode,
                           baseline_win_rate,recent_win_rate,recent_drop,loss_streak,details_json)
                           VALUES(?,?,?,?,?,?,?,?,?,?)""",
                        (now_iso(),variant,old_status,new_status,evidence_mode,baseline_wr,recent_wr,recent_drop,
                         loss_streak,json.dumps(details or {},separators=(",",":")))); c.commit(); c.close()

def _strategy_health_key(instrument: str, variant: str) -> str:
    instrument=InstrumentRegistry.normalize_symbol(instrument)
    return variant if instrument==PRIMARY_INSTRUMENT else f"{instrument}::{variant}"

def strategy_health_snapshot(variant: str, instrument: Optional[str]=None) -> Optional[Dict[str, Any]]:
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    key=_strategy_health_key(instrument,variant)
    c=conn(); row=c.execute("SELECT * FROM strategy_health WHERE setup_variant=?",(key,)).fetchone(); c.close()
    if not row:return None
    out=dict(row);out["instrument"]=instrument;out["strategy_variant"]=variant
    return out

def all_strategy_health() -> List[Dict[str, Any]]:
    c=conn(); rows=c.execute("""SELECT * FROM strategy_health ORDER BY CASE status
                                  WHEN 'PAUSED' THEN 1 WHEN 'DEGRADED' THEN 2 WHEN 'WATCH' THEN 3
                                  WHEN 'RECOVERING' THEN 4 WHEN 'HEALTHY' THEN 5 ELSE 6 END, updated_ts DESC""").fetchall(); c.close()
    return [dict(x) for x in rows]

def _evaluate_one_strategy_health(variant: str, instrument: Optional[str]=None) -> Dict[str, Any]:
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    health_key=_strategy_health_key(instrument,variant)
    if instrument==PRIMARY_INSTRUMENT:
        canonical=_strategy_rows(variant,False); executed=_strategy_rows(variant,True)
    else:
        canonical=_strategy_rows(variant,False,None,instrument); executed=_strategy_rows(variant,True,None,instrument)
    c=conn(); previous=c.execute("SELECT * FROM strategy_health WHERE setup_variant=?",(health_key,)).fetchone(); c.close()
    prev=dict(previous) if previous else None; old_status=prev['status'] if prev else None
    total=len(canonical); executed_total=len(executed)
    evidence_mode='EXECUTED' if executed_total>=STRATEGY_MIN_EXECUTED_TOTAL else 'CANONICAL_MONITOR'
    primary=executed if evidence_mode=='EXECUTED' else canonical
    labels=[int(x['label']) for x in primary]
    recent_n=min(STRATEGY_RECENT_WINDOW,len(labels)); recent_labels=labels[-recent_n:] if recent_n else []
    recent_wr=sum(recent_labels)/recent_n if recent_n else None; loss_streak=_loss_streak(labels)
    earlier=labels[:-recent_n] if recent_n else labels[:]; baseline_labels=earlier[-STRATEGY_BASELINE_WINDOW:]
    baseline_n=len(baseline_labels); baseline_wr=sum(baseline_labels)/baseline_n if baseline_n else None
    drop=(baseline_wr-recent_wr) if baseline_wr is not None and recent_wr is not None else None
    status='LEARNING'; reason='not_enough_health_evidence'
    if recent_n>=STRATEGY_RECENT_WINDOW and baseline_n>=STRATEGY_RECENT_WINDOW:
        if evidence_mode=='EXECUTED':
            status='HEALTHY'; reason='recent executed performance within historical range'
            if drop is not None and drop>=STRATEGY_WATCH_DROP:
                status='WATCH'; reason=f'executed recent win rate down {drop:.3f} vs baseline'
            if loss_streak>=STRATEGY_MAX_LOSS_STREAK_WATCH and status=='HEALTHY':
                status='WATCH'; reason=f'executed loss streak {loss_streak}'
            if (drop is not None and drop>=STRATEGY_DEGRADED_DROP
                and recent_wr is not None and recent_wr<=STRATEGY_DEGRADED_MAX_WR):
                status='PAUSED' if STRATEGY_AUTO_PAUSE else 'DEGRADED'
                reason=f'executed performance degraded: recent {recent_wr:.3f}, baseline {baseline_wr:.3f}, drop {drop:.3f}'
        else:
            # Canonical/counterfactual labels are useful research evidence but
            # cannot represent actual trading health when executed_resolved=0.
            # Keep the metrics visible without allowing them to trigger
            # operational WATCH/PAUSED or downstream risk reductions.
            status='LEARNING'
            if drop is not None and drop>=STRATEGY_WATCH_DROP:
                reason=f'counterfactual degradation observed ({drop:.3f}); awaiting executed evidence'
            elif loss_streak>=STRATEGY_MAX_LOSS_STREAK_WATCH:
                reason=f'counterfactual loss streak {loss_streak}; awaiting executed evidence'
            else:
                reason='counterfactual monitor only; awaiting executed evidence'
    paused_ts=prev.get('paused_ts') if prev else None; pause_baseline=prev.get('pause_baseline_win_rate') if prev else None
    recovery_n=0; recovery_wr=None
    if status=='PAUSED' and old_status not in ('PAUSED','RECOVERING'):
        paused_ts=now_iso(); pause_baseline=baseline_wr
    if old_status in ('PAUSED','RECOVERING') and paused_ts:
        post=(_strategy_rows(variant,False,paused_ts) if instrument==PRIMARY_INSTRUMENT
              else _strategy_rows(variant,False,paused_ts,instrument)); labs=[int(x['label']) for x in post]
        recovery_n=len(labs); recovery_wr=sum(labs)/recovery_n if recovery_n else None
        target=(float(pause_baseline)-STRATEGY_RECOVERY_TOLERANCE) if pause_baseline is not None else .50
        if recovery_n>=STRATEGY_RECOVERY_SAMPLES:
            if recovery_wr is not None and recovery_wr>=target:
                status='HEALTHY'; reason=f'paper recovery confirmed: {recovery_wr:.3f} over {recovery_n} post-pause outcomes'; paused_ts=None
            else:
                status='PAUSED'; reason=f'still degraded in recovery monitor: {recovery_wr:.3f} < target {target:.3f}'
        else:
            status='RECOVERING'; reason=f'paused; collecting recovery evidence {recovery_n}/{STRATEGY_RECOVERY_SAMPLES}'
    transition=('PAUSED' if status=='PAUSED' and old_status not in ('PAUSED','RECOVERING') else
                'RECOVERED' if status=='HEALTHY' and old_status in ('PAUSED','RECOVERING') else
                'WATCH' if status=='WATCH' and old_status!='WATCH' else
                'HEALTHY' if status=='HEALTHY' and old_status not in ('HEALTHY',None) else None)
    c=conn(); c.execute("""INSERT INTO strategy_health(setup_variant,status,evidence_mode,total_resolved,executed_resolved,
      baseline_samples,baseline_win_rate,recent_samples,recent_win_rate,recent_drop,recent_loss_streak,paused_ts,
      pause_baseline_win_rate,recovery_samples,recovery_win_rate,last_transition,reason,updated_ts)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(setup_variant) DO UPDATE SET
      status=excluded.status,evidence_mode=excluded.evidence_mode,total_resolved=excluded.total_resolved,
      executed_resolved=excluded.executed_resolved,baseline_samples=excluded.baseline_samples,
      baseline_win_rate=excluded.baseline_win_rate,recent_samples=excluded.recent_samples,recent_win_rate=excluded.recent_win_rate,
      recent_drop=excluded.recent_drop,recent_loss_streak=excluded.recent_loss_streak,paused_ts=excluded.paused_ts,
      pause_baseline_win_rate=excluded.pause_baseline_win_rate,recovery_samples=excluded.recovery_samples,
      recovery_win_rate=excluded.recovery_win_rate,last_transition=excluded.last_transition,reason=excluded.reason,
      updated_ts=excluded.updated_ts""",
      (health_key,status,evidence_mode,total,executed_total,baseline_n,baseline_wr,recent_n,recent_wr,drop,loss_streak,
       paused_ts,pause_baseline,recovery_n,recovery_wr,transition,reason,now_iso())); c.commit(); c.close()
    _health_transition(health_key,old_status,status,evidence_mode,baseline_wr,recent_wr,drop,loss_streak,
                       {'reason':reason,'recovery_samples':recovery_n,'recovery_win_rate':recovery_wr,'instrument':instrument,'strategy_variant':variant})
    snapshot=(strategy_health_snapshot(variant) if instrument==PRIMARY_INSTRUMENT else strategy_health_snapshot(variant,instrument))
    return snapshot or {'setup_variant':health_key,'strategy_variant':variant,'instrument':instrument,'status':status,'reason':reason}

def evaluate_all_strategy_health(instrument: Optional[str]=None) -> Dict[str, Any]:
    if not STRATEGY_SELF_EVAL_ENABLED:return {'enabled':False,'strategies':[]}
    c=conn()
    if instrument:
        symbols=[InstrumentRegistry.normalize_symbol(instrument)]
    else:
        symbols=[x['instrument'] for x in c.execute("""SELECT DISTINCT instrument FROM signals
                         WHERE instrument IS NOT NULL AND instrument!='' ORDER BY instrument""").fetchall()]
    pairs=[]
    for symbol in symbols:
        variants=[x['setup_variant'] for x in c.execute("""SELECT DISTINCT setup_variant FROM signals
                         WHERE instrument=? AND setup_variant IS NOT NULL AND setup_variant NOT IN ('','WAIT')""",(symbol,)).fetchall()]
        pairs.extend((symbol,v) for v in variants)
    c.close()
    results=[_evaluate_one_strategy_health(v,symbol) for symbol,v in pairs]
    return {'enabled':True,'strategies':results,
            'paused':[{'instrument':x.get('instrument'),'setup_variant':x.get('strategy_variant') or x.get('setup_variant')} for x in results if x.get('status')=='PAUSED'],
            'watch':[{'instrument':x.get('instrument'),'setup_variant':x.get('strategy_variant') or x.get('setup_variant')} for x in results if x.get('status')=='WATCH'],
            'recovering':[{'instrument':x.get('instrument'),'setup_variant':x.get('strategy_variant') or x.get('setup_variant')} for x in results if x.get('status')=='RECOVERING']}

def strategy_execution_gate(r: Dict[str, Any]) -> Dict[str, Any]:
    if not STRATEGY_SELF_EVAL_ENABLED:return {'ok':True,'reason':'self_eval_disabled'}
    variant=setup_variant(r); health=strategy_health_snapshot(variant,r.get("instrument"))
    if not health:return {'ok':True,'reason':'strategy_not_yet_evaluated','variant':variant}
    if health['status'] in ('PAUSED','DEGRADED','RECOVERING'):
        return {'ok':False,'reason':f"strategy health {health['status']}: {health.get('reason','')}",
                'variant':variant,'health':health}
    return {'ok':True,'reason':f"strategy health {health['status']}",'variant':variant,'health':health}


def _strategy_performance_summary(variant: str, instrument: Optional[str]=None) -> Dict[str, Any]:
    """
    Historical/recent canonical strategy performance.
    Uses resolved learning samples, preserving the current architecture.
    """
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    c=conn()
    rows=c.execute("""SELECT ls.label,ls.executed,s.ts
                      FROM learning_samples ls
                      JOIN signals s ON s.id=ls.signal_id
                      WHERE ls.label IN (0,1) AND s.setup_variant=? AND ls.instrument=? AND s.instrument=?
                      ORDER BY s.id ASC""",(variant,instrument,instrument)).fetchall()
    c.close()

    labels=[int(x["label"]) for x in rows]
    n=len(labels)
    hist_wr=(sum(labels)/n) if n else None
    recent_labels=labels[-AI_DIRECTOR_RECENT_WINDOW:] if labels else []
    recent_n=len(recent_labels)
    recent_wr=(sum(recent_labels)/recent_n) if recent_n else None

    return {
        "historical_samples":n,
        "historical_win_rate":hist_wr,
        "recent_samples":recent_n,
        "recent_win_rate":recent_wr,
        "executed_samples":sum(int(x["executed"] or 0) for x in rows)
    }


def _strategy_regime_affinity(variant: str, current_regime: Optional[str], instrument: Optional[str]=None) -> Dict[str, Any]:
    """
    Learn whether a strategy historically behaves better/worse in the CURRENT regime.
    Uses stored market_regime_history nearest to each signal candle.
    If there is not enough evidence, returns neutral 0.5.
    """
    if not current_regime:
        return {"score":0.5,"samples":0,"win_rate":None,"reason":"no_current_regime"}

    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    c=conn()
    rows=c.execute("""SELECT ls.label,s.candle_ts,s.instrument
                      FROM learning_samples ls
                      JOIN signals s ON s.id=ls.signal_id
                      WHERE ls.label IN (0,1) AND s.setup_variant=? AND ls.instrument=? AND s.instrument=?
                      ORDER BY s.id DESC LIMIT 300""",(variant,instrument,instrument)).fetchall()

    matched=[]
    for row in rows:
        rr=c.execute("""SELECT market_regime FROM market_regime_history
                        WHERE instrument=? AND candle_ts<=?
                        ORDER BY candle_ts DESC LIMIT 1""",
                     (row["instrument"],row["candle_ts"])).fetchone()
        if rr and rr["market_regime"]==current_regime:
            matched.append(int(row["label"]))
    c.close()

    if len(matched)<10:
        return {"score":0.5,"samples":len(matched),"win_rate":(sum(matched)/len(matched) if matched else None),
                "reason":"insufficient_regime_history"}

    wr=sum(matched)/len(matched)
    # Convert WR into 0..1 affinity centered around 0.5.
    score=clamp(0.5 + (wr-0.5)*1.5, 0.0, 1.0)
    return {"score":score,"samples":len(matched),"win_rate":wr,"reason":"historical_regime_performance"}


def _health_state_score(status: Optional[str]) -> float:
    return {
        "HEALTHY":1.0,
        "LEARNING":0.55,
        "WATCH":0.40,
        "RECOVERING":0.25,
        "PAUSED":0.05,
        "DEGRADED":0.0
    }.get(str(status or "LEARNING").upper(),0.5)


def _director_state_from_score(score: float, health_status: Optional[str]) -> str:
    hs=str(health_status or "").upper()
    if hs in ("PAUSED","DEGRADED"):
        return "DISABLED"
    if hs=="RECOVERING":
        return "PAUSED"
    if score>=float(managed_value("director.active_threshold",AI_DIRECTOR_ACTIVE_THRESHOLD)):
        return "ACTIVE"
    if score>=float(managed_value("director.reduced_threshold",AI_DIRECTOR_REDUCED_THRESHOLD)):
        return "REDUCED"
    if score>=0.35:
        return "PAUSED"
    return "DISABLED"


def ai_strategy_director_recommendation(
    instrument: str,
    variant: str,
    regime: Optional[Dict[str, Any]],
    signal_confidence: Optional[float] = None,
    ensemble_shadow: Optional[Dict[str,Any]] = None
) -> Dict[str, Any]:
    """
    OBSERVATION ONLY.
    Produces a recommendation but never changes execution, sizing, SL/TP or strategy code.
    """
    if not AI_DIRECTOR_ENABLED:
        return {
            "enabled":False,"observation_only":True,"setup_variant":variant,
            "recommended_state":"ACTIVE","confidence":0.0,
            "reasons":["AI Strategy Director disabled"]
        }

    perf=_strategy_performance_summary(variant,instrument)
    health=strategy_health_snapshot(variant,instrument) or {"status":"LEARNING"}

    market_regime=(regime or {}).get("market_regime")
    regime_conf=float((regime or {}).get("confidence") or 0.0)
    volatility_state=(regime or {}).get("volatility_state")
    trend_strength=float((regime or {}).get("trend_strength") or 0.0)

    affinity=_strategy_regime_affinity(variant,market_regime,instrument)

    hist_wr=perf["historical_win_rate"]
    recent_wr=perf["recent_win_rate"]

    hist_score=0.5 if hist_wr is None else clamp(0.5 + (hist_wr-0.5)*1.2,0.0,1.0)
    recent_score=0.5 if recent_wr is None else clamp(0.5 + (recent_wr-0.5)*1.5,0.0,1.0)
    health_score=_health_state_score(health.get("status"))
    signal_score=clamp(float(signal_confidence or 0.5),0.0,1.0)

    # Current regime confidence tells us how much to trust regime affinity.
    regime_score=(0.5*(1.0-regime_conf)) + (float(affinity["score"])*regime_conf)

    # Volatility sanity component: abnormal uncertainty should reduce recommendation confidence.
    vol_score=1.0
    if market_regime=="ABNORMAL_UNCERTAIN" or volatility_state=="ABNORMAL":
        vol_score=0.15
    elif volatility_state=="HIGH":
        vol_score=0.70
    elif volatility_state=="LOW":
        vol_score=0.80

    components={
        "historical_performance":hist_score,
        "recent_performance":recent_score,
        "strategy_health":health_score,
        "regime_affinity":regime_score,
        "signal_confidence":signal_score,
        "volatility_sanity":vol_score
    }

    # Weighted, explainable score.
    score=(
        hist_score*0.20 +
        recent_score*0.25 +
        health_score*0.25 +
        regime_score*0.15 +
        signal_score*0.10 +
        vol_score*0.05
    )

    # Confidence in the DIRECTOR'S recommendation, not confidence that a trade wins.
    history_factor=clamp(perf["historical_samples"]/max(AI_DIRECTOR_MIN_HISTORY,1),0.0,1.0)
    recent_factor=clamp(perf["recent_samples"]/max(AI_DIRECTOR_RECENT_WINDOW,1),0.0,1.0)
    affinity_factor=clamp(affinity["samples"]/30.0,0.0,1.0)
    recommendation_confidence=clamp(
        0.30*history_factor +
        0.25*recent_factor +
        0.25*regime_conf +
        0.20*affinity_factor,
        0.0,1.0
    )

    state=_director_state_from_score(score,health.get("status"))

    reasons=[]
    reasons.append(f"strategy_health={health.get('status','LEARNING')}")
    if hist_wr is not None:
        reasons.append(f"historical_wr={hist_wr:.3f} over {perf['historical_samples']}")
    else:
        reasons.append("historical_wr=insufficient")
    if recent_wr is not None:
        reasons.append(f"recent_wr={recent_wr:.3f} over {perf['recent_samples']}")
    if market_regime:
        reasons.append(f"regime={market_regime} conf={regime_conf:.3f}")
    reasons.append(f"regime_affinity={affinity['score']:.3f} samples={affinity['samples']}")
    reasons.append(f"signal_confidence={signal_score:.3f}")
    if market_regime=="ABNORMAL_UNCERTAIN" or volatility_state=="ABNORMAL":
        reasons.append("abnormal/uncertain market conditions penalized")
    if ensemble_shadow and ensemble_shadow.get("enabled"):
        reasons.append(
            f"ensemble_shadow={ensemble_shadow.get('ensemble_direction','ABSTAIN')} "
            f"conf={float(ensemble_shadow.get('ensemble_confidence') or 0):.3f} "
            f"agreement={float(ensemble_shadow.get('agreement_score') or 0):.3f} "
            f"diversity={float(ensemble_shadow.get('diversity_score') or 0):.3f}"
        )
    if state in ("PAUSED","DISABLED"):
        reasons.append("observation recommendation only; execution remains unchanged")

    return {
        "enabled":True,
        "observation_only":True,
        "instrument":instrument,
        "setup_variant":variant,
        "recommended_state":state,
        "confidence":float(recommendation_confidence),
        "director_score":float(score),
        "market_regime":market_regime,
        "regime_confidence":regime_conf,
        "volatility_state":volatility_state,
        "trend_strength":trend_strength,
        "strategy_health_status":health.get("status"),
        "historical_win_rate":hist_wr,
        "recent_win_rate":recent_wr,
        "historical_samples":perf["historical_samples"],
        "recent_samples":perf["recent_samples"],
        "signal_confidence":signal_score,
        "regime_affinity":affinity,
        "score_components":components,
        "ensemble_shadow":ensemble_shadow or {"enabled":False},
        "reasons":reasons,
        "timestamp":now_iso()
    }


def log_ai_director_decision(decision: Dict[str, Any]) -> Optional[int]:
    if not decision.get("enabled"):
        return None

    # Optional change-only logging to stdout; DB always records every decision.
    c=conn()
    prev=c.execute("""SELECT recommended_state FROM ai_strategy_director_decisions
                      WHERE instrument=? AND setup_variant=?
                      ORDER BY id DESC LIMIT 1""",
                   (decision["instrument"],decision["setup_variant"])).fetchone()

    c.execute("""INSERT INTO ai_strategy_director_decisions(
      ts,instrument,setup_variant,recommended_state,confidence,market_regime,
      regime_confidence,volatility_state,strategy_health_status,historical_win_rate,
      recent_win_rate,historical_samples,recent_samples,signal_confidence,
      score_components_json,reasons_json,observation_only)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
      (decision["timestamp"],decision["instrument"],decision["setup_variant"],
       decision["recommended_state"],decision["confidence"],decision.get("market_regime"),
       decision.get("regime_confidence"),decision.get("volatility_state"),
       decision.get("strategy_health_status"),decision.get("historical_win_rate"),
       decision.get("recent_win_rate"),decision.get("historical_samples",0),
       decision.get("recent_samples",0),decision.get("signal_confidence"),
       json.dumps(decision.get("score_components") or {},separators=(",",":")),
       json.dumps(decision.get("reasons") or [],separators=(",",":"))))
    did=c.execute("SELECT last_insert_rowid() id").fetchone()["id"]
    c.commit();c.close()

    changed=(not prev or prev["recommended_state"]!=decision["recommended_state"])
    if changed or not AI_DIRECTOR_LOG_CHANGES_ONLY:
        log.info(
            "AI_DIRECTOR %s %s state=%s conf=%.3f score=%.3f regime=%s regime_conf=%.3f health=%s",
            decision["instrument"],decision["setup_variant"],decision["recommended_state"],
            decision["confidence"],decision["director_score"],decision.get("market_regime"),
            decision.get("regime_confidence",0.0),decision.get("strategy_health_status")
        )
    return int(did)


def attach_ai_director_outcome(signal_id: int, label: int) -> int:
    """
    Link resolved real/paper outcome back to the most recent director observation
    for the same setup variant/instrument at or before the signal timestamp.
    """
    c=conn()
    sig=c.execute("""SELECT id,instrument,setup_variant,ts,executed,blocked
                     FROM signals WHERE id=?""",(signal_id,)).fetchone()
    if not sig:
        c.close()
        return 0

    dec=c.execute("""SELECT id FROM ai_strategy_director_decisions
                     WHERE instrument=? AND setup_variant=? AND ts<=?
                     ORDER BY id DESC LIMIT 1""",
                  (sig["instrument"],sig["setup_variant"],sig["ts"])).fetchone()
    if not dec:
        c.close()
        return 0

    exists=c.execute("""SELECT 1 FROM ai_strategy_director_outcomes
                        WHERE director_decision_id=? AND signal_id=?""",
                     (dec["id"],signal_id)).fetchone()
    if exists:
        c.close()
        return 0

    c.execute("""INSERT INTO ai_strategy_director_outcomes(
      director_decision_id,signal_id,resolved_label,executed,blocked,resolved_ts)
      VALUES(?,?,?,?,?,?)""",
      (dec["id"],signal_id,int(label),int(sig["executed"] or 0),
       int(sig["blocked"] or 0),now_iso()))
    c.commit();c.close()
    return 1


def ai_director_report(limit: int = 100) -> Dict[str, Any]:
    c=conn()
    decisions=c.execute("""SELECT * FROM ai_strategy_director_decisions
                           ORDER BY id DESC LIMIT ?""",(min(max(limit,1),500),)).fetchall()

    performance=c.execute("""
        SELECT d.recommended_state,
               COUNT(o.id) resolved,
               AVG(o.resolved_label) win_rate,
               SUM(CASE WHEN o.executed=1 THEN 1 ELSE 0 END) executed_resolved
        FROM ai_strategy_director_decisions d
        LEFT JOIN ai_strategy_director_outcomes o ON o.director_decision_id=d.id
        GROUP BY d.recommended_state
        ORDER BY d.recommended_state
    """).fetchall()
    c.close()

    return {
        "enabled":AI_DIRECTOR_ENABLED,
        "observation_only":True,
        "authority_over_execution":False,
        "authority_over_risk":False,
        "recent_decisions":[dict(x) for x in decisions],
        "outcome_summary":[dict(x) for x in performance]
    }

def reconcile_ai_director_outcomes(limit: int = 500) -> int:
    c=conn()
    rows=c.execute("""SELECT ls.signal_id,ls.label
                      FROM learning_samples ls
                      WHERE ls.label IN (0,1)
                      ORDER BY ls.signal_id DESC LIMIT ?""",(limit,)).fetchall()
    c.close()
    linked=0
    for row in rows:
        try:
            linked += attach_ai_director_outcome(int(row["signal_id"]),int(row["label"]))
        except Exception:
            continue
    return linked




def _risk_float(value, default=None):
    try:
        v=float(value)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _executed_loss_streak() -> int:
    c=conn()
    rows=c.execute("""SELECT label FROM learning_samples
                      WHERE label IN (0,1) AND executed=1
                      ORDER BY signal_id DESC LIMIT 50""").fetchall()
    c.close()
    streak=0
    for row in rows:
        if int(row["label"])==0:
            streak+=1
        else:
            break
    return streak


def _strategy_open_risk_proxy(variant: str, instrument: Optional[str]=None) -> float:
    """
    Conservative fraction-of-NAV proxy using REAL filled units when available.
    """
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    c=conn()
    try:
        rows=c.execute("""SELECT current_units FROM active_trade_management
                          WHERE setup_variant=? AND instrument=? AND closed=0""",(variant,instrument)).fetchall()
        raw=sum((abs(float(x["current_units"] or UNITS))/max(abs(float(UNITS)),1.0))*float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION))
                for x in rows)
    except sqlite3.OperationalError:
        n=c.execute("""SELECT COUNT(*) n FROM active_trade_management
                       WHERE setup_variant=? AND instrument=? AND closed=0""",(variant,instrument)).fetchone()["n"]
        raw=int(n)*float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION))
    c.close()
    return float(min(float(managed_value("risk.max_strategy_fraction",RISK_MAX_STRATEGY_FRACTION)),raw))

def _shared_currency_correlation(instrument: str, open_instruments: List[str]) -> Dict[str, Any]:
    """
    Conservative correlation protection before a full rolling-correlation engine exists.
    FX pairs sharing a currency are treated as correlated exposure.
    """
    try:
        a,b=instrument.split("_")
    except Exception:
        return {"count":0,"high":False,"method":"unknown_instrument"}
    count=0
    related=[]
    for other in open_instruments:
        try:
            x,y=other.split("_")
        except Exception:
            continue
        if other==instrument or {a,b}.intersection({x,y}):
            count+=1
            related.append(other)
    return {
        "count":count,
        "high":count>=RISK_MAX_CORRELATED_POSITIONS,
        "related":related,
        "method":"shared_currency_conservative_proxy"
    }


def portfolio_execution_guard(instrument: str, risk_context: Dict[str, Any],
                              prospective_trade_risk: Optional[float] = None) -> Dict[str, Any]:
    """Hard global multi-asset risk guard using existing approved ceilings only.

    The adaptive risk engine remains SHADOW.  This function promotes only the
    already-defined portfolio/margin/correlation hard ceilings into the real
    execution path so adding symbols cannot bypass aggregate controls.
    """
    symbol=InstrumentRegistry.normalize_symbol(instrument)
    ctx=risk_context or {}
    max_trade=float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION))
    max_portfolio=float(managed_value("risk.max_portfolio_fraction",RISK_MAX_PORTFOLIO_FRACTION))
    max_margin=float(managed_value("risk.max_margin_usage",RISK_MAX_MARGIN_USAGE))
    max_correlated=int(managed_value("risk.max_correlated_positions",RISK_MAX_CORRELATED_POSITIONS))
    open_risk=_risk_float(ctx.get("portfolio_open_risk"),0.0) or 0.0
    margin_usage=_risk_float(ctx.get("margin_usage"))
    prospective=max(0.0,min(max_trade,float(prospective_trade_risk if prospective_trade_risk is not None else max_trade)))
    corr=_shared_currency_correlation(symbol,list(ctx.get("open_instruments") or []))
    reasons=[]
    if ctx.get("system_abnormal") or ctx.get("data_stale"):
        reasons.append("RISK_CONTEXT_UNSAFE")
    if margin_usage is not None and margin_usage>=max_margin:
        reasons.append("MARGIN_USAGE_LIMIT")
    if open_risk+prospective>max_portfolio+1e-12:
        reasons.append("PORTFOLIO_RISK_LIMIT")
    if int(corr.get("count") or 0)>=max_correlated:
        reasons.append("CORRELATED_POSITION_LIMIT")
    return {
        "allow":not reasons,
        "instrument":symbol,
        "reasons":reasons,
        "portfolio_open_risk":open_risk,
        "prospective_trade_risk":prospective,
        "prospective_portfolio_risk":open_risk+prospective,
        "portfolio_risk_cap":max_portfolio,
        "margin_usage":margin_usage,
        "margin_cap":max_margin,
        "correlation":corr,
        "max_correlated_positions":max_correlated,
    }


async def build_broker_risk_context(client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    Read-only OANDA Practice risk context.
    It does not send, modify or close orders.
    """
    ctx={
        "balance":None,"nav":None,"peak_nav":None,"current_drawdown":None,
        "margin_used":None,"margin_usage":None,"open_positions":0,
        "portfolio_open_risk":0.0,"open_instruments":[],"account_currency":None,
        "consecutive_losses":_executed_loss_streak(),
        "data_stale":False,"system_abnormal":False,
        "source":"OANDA_PRACTICE_READ_ONLY"
    }
    errors=[]
    try:
        summary=await req(client,"GET","/v3/accounts/{account}/summary")
        account=summary.get("account") or {}
        ctx["balance"]=_risk_float(account.get("balance"))
        ctx["nav"]=_risk_float(account.get("NAV"))
        ctx["account_currency"]=str(account.get("currency") or "").upper() or None
        ctx["margin_used"]=_risk_float(account.get("marginUsed"),0.0)
        if ctx["nav"] and ctx["nav"]>0 and ctx["margin_used"] is not None:
            ctx["margin_usage"]=max(0.0,ctx["margin_used"]/ctx["nav"])
    except Exception as e:
        errors.append(f"account_summary:{e}")

    try:
        pos=await req(client,"GET","/v3/accounts/{account}/openPositions")
        positions=pos.get("positions") or []
        ctx["open_instruments"]=[x.get("instrument") for x in positions if x.get("instrument")]
        trades_payload=await req(client,"GET","/v3/accounts/{account}/openTrades")
        open_trades=trades_payload.get("trades") or []
        ctx["open_positions"]=len(open_trades)
        unit_factor=sum(abs(float(x.get("currentUnits") or 0))/max(abs(float(UNITS)),1.0) for x in open_trades)
        ctx["portfolio_open_risk"]=min(
            float(managed_value("risk.max_portfolio_fraction",RISK_MAX_PORTFOLIO_FRACTION)),
            unit_factor*float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION))
        )
        ctx["broker_open_trade_units"]={str(x.get("id")):abs(float(x.get("currentUnits") or 0)) for x in open_trades}
    except Exception as e:
        errors.append(f"open_positions:{e}")

    # Persist high-water NAV locally across scans/deployments if DB persists.
    c=conn()
    prev=c.execute("SELECT peak_nav FROM portfolio_risk_state WHERE id=1").fetchone()
    c.close()
    prev_peak=_risk_float(prev["peak_nav"]) if prev else None
    if ctx["nav"] is not None:
        ctx["peak_nav"]=max(ctx["nav"],prev_peak or ctx["nav"])
        if ctx["peak_nav"]>0:
            ctx["current_drawdown"]=max(0.0,(ctx["peak_nav"]-ctx["nav"])/ctx["peak_nav"])

    heartbeat=state.get("worker_last_heartbeat")
    if heartbeat:
        try:
            hb=datetime.fromisoformat(str(heartbeat).replace("Z","+00:00"))
            ctx["data_stale"]=(datetime.now(timezone.utc)-hb).total_seconds()>RISK_DATA_STALE_SECONDS
        except Exception:
            ctx["data_stale"]=True

    # System-abnormal is a conservative health check, not self-modifying logic.
    ctx["system_abnormal"]=bool(
        errors or
        (state.get("last_error") and int(state.get("worker_restarts") or 0)>=RISK_ABNORMAL_ERROR_COUNT)
    )
    ctx["errors"]=errors
    return ctx


def persist_portfolio_risk_context(ctx: Dict[str, Any]) -> None:
    c=conn()
    c.execute("""INSERT INTO portfolio_risk_state(
      id,ts,balance,nav,peak_nav,current_drawdown,margin_used,margin_usage,
      open_positions,portfolio_open_risk,consecutive_losses,data_stale,system_abnormal,details_json)
      VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(id) DO UPDATE SET
      ts=excluded.ts,balance=excluded.balance,nav=excluded.nav,peak_nav=excluded.peak_nav,
      current_drawdown=excluded.current_drawdown,margin_used=excluded.margin_used,
      margin_usage=excluded.margin_usage,open_positions=excluded.open_positions,
      portfolio_open_risk=excluded.portfolio_open_risk,
      consecutive_losses=excluded.consecutive_losses,data_stale=excluded.data_stale,
      system_abnormal=excluded.system_abnormal,details_json=excluded.details_json""",
      (now_iso(),ctx.get("balance"),ctx.get("nav"),ctx.get("peak_nav"),
       ctx.get("current_drawdown"),ctx.get("margin_used"),ctx.get("margin_usage"),
       int(ctx.get("open_positions") or 0),ctx.get("portfolio_open_risk"),
       int(ctx.get("consecutive_losses") or 0),int(bool(ctx.get("data_stale"))),
       int(bool(ctx.get("system_abnormal"))),json.dumps(ctx,separators=(",",":"))))
    c.commit();c.close()


def adaptive_risk_recommendation(
    instrument: str,
    variant: str,
    regime: Optional[Dict[str, Any]],
    director: Optional[Dict[str, Any]],
    signal_confidence: Optional[float],
    risk_context: Dict[str, Any],
    requested_units: float
) -> Dict[str, Any]:
    """
    Pure decision function. SHADOW ONLY in V3.12.
    The multiplier is capped at 1.0, so strong performance can never increase
    risk above the existing configured position size.
    """
    risk_base=float(managed_value("risk.base_fraction",RISK_BASE_FRACTION))
    max_trade=float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION))
    max_strategy=float(managed_value("risk.max_strategy_fraction",RISK_MAX_STRATEGY_FRACTION))
    max_portfolio=float(managed_value("risk.max_portfolio_fraction",RISK_MAX_PORTFOLIO_FRACTION))
    max_margin=float(managed_value("risk.max_margin_usage",RISK_MAX_MARGIN_USAGE))
    dd_warn=float(managed_value("risk.drawdown_warning",RISK_DRAWDOWN_WARN))
    dd_stop=float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP))
    max_losses=int(managed_value("risk.max_consecutive_losses",RISK_MAX_CONSECUTIVE_LOSSES))
    max_correlated=int(managed_value("risk.max_correlated_positions",RISK_MAX_CORRELATED_POSITIONS))

    market_regime=(regime or {}).get("market_regime")
    volatility_state=(regime or {}).get("volatility_state")
    regime_conf=_risk_float((regime or {}).get("confidence"),0.0) or 0.0

    director_conf=_risk_float((director or {}).get("confidence"))
    strategy_conf=director_conf if director_conf is not None else (_risk_float(signal_confidence,0.5) or 0.5)

    perf=_strategy_performance_summary(variant,instrument)
    recent_wr=perf.get("recent_win_rate")
    health=strategy_health_snapshot(variant,instrument) or {"status":"LEARNING"}
    health_status=str(health.get("status") or "LEARNING").upper()

    dd=_risk_float(risk_context.get("current_drawdown"))
    margin_usage=_risk_float(risk_context.get("margin_usage"))
    portfolio_open_risk=_risk_float(risk_context.get("portfolio_open_risk"),0.0) or 0.0
    strategy_open_risk=_strategy_open_risk_proxy(variant,instrument)
    open_instruments=list(risk_context.get("open_instruments") or [])
    corr=_shared_currency_correlation(instrument,open_instruments)
    loss_streak=int(risk_context.get("consecutive_losses") or 0)

    reasons=[]
    hard=False
    emergency=False
    reduce_existing=False
    allow=True

    # ---------- hard limits ----------
    if market_regime is None or volatility_state is None or strategy_conf is None:
        hard=True; allow=False
        reasons.append("missing_or_inconsistent_inputs")

    if risk_context.get("data_stale"):
        hard=True; allow=False; emergency=True
        reasons.append("stale_data")

    if risk_context.get("system_abnormal"):
        hard=True; allow=False; emergency=True
        reasons.append("abnormal_system_behavior")

    if dd is not None and dd>=dd_stop:
        hard=True; allow=False; emergency=True; reduce_existing=True
        reasons.append("drawdown_hard_limit")
    elif dd is not None and dd>=dd_warn:
        reasons.append("drawdown_warning")

    if loss_streak>=max_losses:
        hard=True; allow=False
        reasons.append("consecutive_loss_limit")

    if market_regime=="ABNORMAL_UNCERTAIN" or volatility_state=="ABNORMAL":
        hard=True; allow=False; emergency=True; reduce_existing=True
        reasons.append("abnormal_market_volatility")

    if portfolio_open_risk>=max_portfolio:
        hard=True; allow=False; reduce_existing=True
        reasons.append("portfolio_risk_limit")

    if strategy_open_risk>=max_strategy:
        hard=True; allow=False
        reasons.append("strategy_risk_limit")

    if margin_usage is not None and margin_usage>=max_margin:
        hard=True; allow=False; reduce_existing=True
        reasons.append("margin_usage_limit")

    if corr["high"]:
        hard=True; allow=False
        reasons.append("correlated_position_limit")

    # ---------- progressive reductions ----------
    mult=1.0

    # Confidence can only reduce; never lever up.
    if strategy_conf<0.45:
        conf_mult=RISK_MIN_MULTIPLIER
    elif strategy_conf<0.60:
        conf_mult=0.50
    elif strategy_conf<0.72:
        conf_mult=0.75
    else:
        conf_mult=1.0
    mult*=conf_mult
    if conf_mult<1.0: reasons.append("confidence_reduction")

    if recent_wr is not None:
        if recent_wr<0.40: perf_mult=0.40
        elif recent_wr<0.50: perf_mult=0.60
        elif recent_wr<0.60: perf_mult=0.80
        else: perf_mult=1.0
        mult*=perf_mult
        if perf_mult<1.0: reasons.append("recent_performance_reduction")

    health_mult={
        "HEALTHY":1.0,"LEARNING":0.70,"WATCH":0.55,
        "RECOVERING":0.25,"PAUSED":0.0,"DEGRADED":0.0
    }.get(health_status,0.60)
    mult*=health_mult
    if health_mult<1.0: reasons.append(f"strategy_health_{health_status.lower()}")

    vol_mult={"LOW":0.85,"NORMAL":1.0,"HIGH":0.60,"ABNORMAL":0.0}.get(str(volatility_state),0.70)
    mult*=vol_mult
    if vol_mult<1.0: reasons.append("volatility_reduction")

    # Progressive drawdown taper before hard stop.
    if dd is not None and 0<dd<dd_stop:
        if dd<dd_warn:
            dd_mult=max(0.75,1.0-0.25*(dd/dd_warn))
        else:
            span=max(dd_stop-dd_warn,1e-9)
            progress=(dd-dd_warn)/span
            dd_mult=max(0.20,0.75-0.55*progress)
        mult*=dd_mult
        if dd_mult<1.0: reasons.append("drawdown_progressive_reduction")

    if regime_conf<0.50:
        mult*=0.70
        reasons.append("low_regime_confidence")

    mult=clamp(mult,0.0,1.0)
    if hard:
        mult=0.0

    # ---------- separate risk layers ----------
    requested_risk=risk_base
    trade_cap=max_trade
    strategy_remaining=max(0.0,max_strategy-strategy_open_risk)
    portfolio_remaining=max(0.0,max_portfolio-portfolio_open_risk)

    approved_risk=0.0 if not allow else min(
        requested_risk*mult,
        trade_cap,
        strategy_remaining,
        portfolio_remaining
    )

    shadow_max_units=max(0.0,float(requested_units)*mult)

    if not reasons:
        reasons=["normal_risk_conditions"]

    return {
        "enabled":RISK_ENGINE_ENABLED,
        "shadow_mode":True,
        "risk_multiplier":float(mult),
        "max_position_size":float(shadow_max_units),
        "max_exposure":float(max_margin),
        "allow_new_trades":bool(allow),
        "reduce_existing_positions":bool(reduce_existing),
        "emergency_stop":bool(emergency),
        "hard_limit_triggered":bool(hard),
        "reason":"; ".join(reasons),
        "requested_risk":float(requested_risk),
        "approved_risk":float(approved_risk),
        "metrics":{
            "instrument":instrument,
            "setup_variant":variant,
            "market_regime":market_regime,
            "regime_confidence":regime_conf,
            "volatility_state":volatility_state,
            "strategy_confidence":strategy_conf,
            "recent_win_rate":recent_wr,
            "strategy_health_status":health_status,
            "current_drawdown":dd,
            "nav":risk_context.get("nav"),
            "margin_usage":margin_usage,
            "portfolio_open_risk":portfolio_open_risk,
            "strategy_open_risk":strategy_open_risk,
            "consecutive_losses":loss_streak,
            "correlation":corr,
            "requested_units":float(requested_units),
            "risk_per_trade_cap":trade_cap,
            "risk_per_strategy_cap":max_strategy,
            "risk_portfolio_cap":max_portfolio
        },
        "timestamp":now_iso()
    }


def log_adaptive_risk_decision(d: Dict[str, Any]) -> Optional[int]:
    if not d.get("enabled"):
        return None
    m=d.get("metrics") or {}
    c=conn()
    c.execute("""INSERT INTO adaptive_risk_decisions(
      ts,instrument,setup_variant,market_regime,volatility_state,strategy_confidence,
      recent_win_rate,current_drawdown,nav,margin_usage,portfolio_open_risk,
      strategy_open_risk,requested_risk,approved_risk,requested_units,
      shadow_max_position_size,risk_multiplier,max_exposure,allow_new_trades,
      reduce_existing_positions,emergency_stop,hard_limit_triggered,reason,metrics_json,shadow_mode)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
      (d["timestamp"],m.get("instrument"),m.get("setup_variant"),m.get("market_regime"),
       m.get("volatility_state"),m.get("strategy_confidence"),m.get("recent_win_rate"),
       m.get("current_drawdown"),m.get("nav"),m.get("margin_usage"),
       m.get("portfolio_open_risk"),m.get("strategy_open_risk"),
       d.get("requested_risk"),d.get("approved_risk"),m.get("requested_units"),
       d.get("max_position_size"),d.get("risk_multiplier"),d.get("max_exposure"),
       int(bool(d.get("allow_new_trades"))),int(bool(d.get("reduce_existing_positions"))),
       int(bool(d.get("emergency_stop"))),int(bool(d.get("hard_limit_triggered"))),
       d.get("reason",""),json.dumps(m,separators=(",",":"))))
    rid=c.execute("SELECT last_insert_rowid() id").fetchone()["id"]
    c.commit();c.close()
    log.info("RISK_SHADOW %s %s allow=%s mult=%.3f emergency=%s reason=%s",
             m.get("instrument"),m.get("setup_variant"),d.get("allow_new_trades"),
             d.get("risk_multiplier",0.0),d.get("emergency_stop"),d.get("reason"))
    return int(rid)


def adaptive_risk_report(limit: int = 200) -> Dict[str, Any]:
    c=conn()
    rows=c.execute("""SELECT * FROM adaptive_risk_decisions
                      ORDER BY id DESC LIMIT ?""",(min(max(limit,1),1000),)).fetchall()
    ps=c.execute("SELECT * FROM portfolio_risk_state WHERE id=1").fetchone()
    c.close()
    return {
        "enabled":RISK_ENGINE_ENABLED,
        "shadow_mode":True,
        "authority_over_execution":False,
        "authority_over_position_size":False,
        "rules_self_modifiable":False,
        "portfolio_state":dict(ps) if ps else None,
        "recent_decisions":[dict(x) for x in rows]
    }



def _tm_confidence_bucket(p: Optional[float]) -> str:
    if p is None:
        return "UNKNOWN"
    p=float(p)
    if p<0.55:return "LOW"
    if p<0.70:return "MEDIUM"
    if p<0.80:return "HIGH"
    return "VERY_HIGH"


def _tm_session(at: Optional[str]) -> str:
    try:
        dt=datetime.fromisoformat(str(at).replace("Z","+00:00")).astimezone(NY)
        h=dt.hour
        if 0<=h<3:return "ASIA_LATE"
        if 3<=h<8:return "LONDON"
        if 8<=h<12:return "NY_LONDON_OVERLAP"
        if 12<=h<17:return "NEW_YORK"
        return "AFTER_HOURS"
    except Exception:
        return "UNKNOWN"


def _tm_json(value, fallback):
    try:
        return json.dumps(value if value is not None else fallback,separators=(",",":"),default=str)
    except Exception:
        return json.dumps(fallback,separators=(",",":"))


def record_trade_memory_entry(
    trade_id: str,
    signal_id: int,
    order_id: str,
    r: Dict[str, Any],
    conf: Dict[str, Any],
    director: Dict[str, Any],
    risk_shadow: Dict[str, Any],
    pre_execution_reason: str,
    fill: Dict[str, Any],
    fill_price: float,
    entry_slippage_pips: Optional[float],
    protection_reanchor: Optional[Dict[str,Any]]=None
) -> Optional[int]:
    """
    Freeze PRE-TRADE context at entry.
    Post-entry fields (exit, P/L, MFE, MAE) are deliberately absent from entry_context_json.
    """
    if not TRADE_MEMORY_ENABLED or not trade_id:
        return None

    regime=r.get("market_regime") if isinstance(r.get("market_regime"),dict) else {}
    risk_metrics=(risk_shadow or {}).get("metrics") or {}
    trade_opened=fill.get("tradeOpened") or {}
    units=_risk_float(trade_opened.get("units"))
    if units is None:
        units=_risk_float(fill.get("units"),UNITS)
    units=float(abs(units or UNITS))
    direction="LONG" if r.get("signal")=="BUY" else "SHORT"

    # This JSON contains only data available before order submission.
    entry_context={
        "context_version":"v1_pre_trade_only",
        "candle_ts":r.get("candle_ts"),
        "strategy":setup_variant(r),
        "planned_entry":r.get("entry"),
        "planned_stop":r.get("stop"),
        "planned_target":r.get("managed_target",r.get("target")),
        "rr":r.get("rr"),
        "rr_raw":r.get("rr_raw"),
        "technical":r.get("technical"),
        "score":r.get("score"),
        "features":r.get("features") or {},
        "filters":r.get("filters") or {},
        "news_alignment":r.get("alignment"),
        "market_regime":regime,
        "strategy_confidence":{
            "probability":conf.get("probability"),
            "source":conf.get("source"),
            "samples":conf.get("samples"),
            "required":conf.get("required_confidence")
        },
        "ai_strategy_director":{
            "recommended_state":director.get("recommended_state"),
            "confidence":director.get("confidence"),
            "director_score":director.get("director_score"),
            "reasons":director.get("reasons") or []
        },
        "ensemble_shadow":r.get("ensemble_shadow") or {},
        "adaptive_risk_shadow":{
            "risk_multiplier":risk_shadow.get("risk_multiplier"),
            "allow_new_trades":risk_shadow.get("allow_new_trades"),
            "requested_risk":risk_shadow.get("requested_risk"),
            "approved_risk":risk_shadow.get("approved_risk"),
            "reason":risk_shadow.get("reason")
        },
        "decision_reason":pre_execution_reason,
        "account_drawdown":risk_metrics.get("current_drawdown"),
        "entry_session":_tm_session(r.get("candle_ts") or now_iso())
    }

    # Fill/protection information is entry execution data, separate from decision context.
    reanchor_supplied=protection_reanchor is not None
    protection_reanchor=protection_reanchor or {}
    reanchor_geometry=protection_reanchor.get("geometry") or {}
    reanchor_verification=protection_reanchor.get("verification") or {}
    effective_stop=protection_reanchor.get("effective_stop")
    effective_target=protection_reanchor.get("effective_target")
    persisted_stop=(float(effective_stop) if effective_stop is not None else
                    (None if reanchor_supplied else (float(r.get("stop")) if r.get("stop") is not None else None)))
    persisted_target=(float(effective_target) if effective_target is not None else
                      (None if reanchor_supplied else (float(r.get("managed_target",r.get("target"))) if r.get("target") is not None else None)))
    execution_context={
        "fill_transaction_id":fill.get("id"),
        "fill_time":fill.get("time"),
        "actual_fill_price":fill_price,
        "units":units,
        "entry_slippage_pips":entry_slippage_pips,
        "initial_stop_on_fill":r.get("stop"),
        "initial_target_on_fill":r.get("managed_target",r.get("target")),
        "protection_reanchor_status":protection_reanchor.get("status"),
        "protection_reanchor_confirmed":bool(protection_reanchor.get("confirmed")),
        "applied_stop":effective_stop,
        "applied_target":effective_target,
        "expected_reanchored_stop":reanchor_geometry.get("applied_stop"),
        "expected_reanchored_target":reanchor_geometry.get("applied_target"),
        "broker_verified_stop":reanchor_verification.get("broker_stop"),
        "broker_verified_target":reanchor_verification.get("broker_target"),
        "protection_verification_status":reanchor_verification.get("status"),
        "smart_execution_shadow":r.get("smart_execution_shadow") or {},
        "smart_execution_actual":r.get("smart_execution_actual") or {},
        "smart_execution_mode":"SHADOW" if SMART_EXECUTION_ENABLED else "DISABLED"
    }

    entry_reasons=[
        pre_execution_reason,
        *list(director.get("reasons") or []),
        "risk_shadow:"+str(risk_shadow.get("reason") or "")
    ]

    data_quality={
        "entry_context_frozen":True,
        "lookahead_fields_in_entry_context":False,
        "actual_broker_exit_pending":True,
        "mfe_mae_source":"observed_M1_candles_while_open",
        "realized_result_source":"OANDA_trade_reconciliation_when_available"
    }

    c=conn()
    c.execute("""INSERT OR IGNORE INTO trade_memory(
      trade_id,signal_id,order_id,strategy,symbol,direction,status,entry_ts,entry_price,
      position_size,stop_loss,take_profit,market_regime_entry,regime_confidence_entry,
      volatility_state_entry,trend_strength_entry,strategy_confidence_entry,
      director_state_entry,director_confidence_entry,risk_multiplier_entry,
      risk_allow_new_trades_shadow,requested_risk,approved_risk,entry_drawdown,
      entry_slippage_pips,entry_session,confidence_bucket,entry_reasons_json,
      entry_context_json,execution_context_json,risk_recommendation_json,data_quality_json,
      created_ts,updated_ts)
      VALUES(?,?,?,?,?,?,'OPEN',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (
       str(trade_id),int(signal_id),str(order_id or ""),setup_variant(r),r["instrument"],direction,
       fill.get("time") or now_iso(),float(fill_price),units,
       persisted_stop,persisted_target,
       regime.get("market_regime"),regime.get("confidence"),regime.get("volatility_state"),
       regime.get("trend_strength"),conf.get("probability"),
       director.get("recommended_state"),director.get("confidence"),
       risk_shadow.get("risk_multiplier"),int(bool(risk_shadow.get("allow_new_trades"))),
       risk_shadow.get("requested_risk"),risk_shadow.get("approved_risk"),
       risk_metrics.get("current_drawdown"),entry_slippage_pips,
       _tm_session(r.get("candle_ts") or now_iso()),_tm_confidence_bucket(conf.get("probability")),
       _tm_json(entry_reasons,[]),_tm_json(entry_context,{}),_tm_json(execution_context,{}),
       _tm_json(risk_shadow,{}),_tm_json(data_quality,{}),now_iso(),now_iso()
      ))
    version_ctx=security_version_context(r)
    c.execute("""UPDATE trade_memory SET strategy_version=?,risk_config_version=?,director_version=?,
                 regime_model_version=?,deployment_version=?,runtime_code_hash=?,dependency_lock_hash=?,
                 config_snapshot_hash=?,release_id=?,production_certification_id=?,production_stage=? WHERE trade_id=?""",
              (version_ctx["strategy_version"],version_ctx["risk_config_version"],version_ctx["director_version"],
               version_ctx["regime_model_version"],version_ctx["deployment_version"],version_ctx["runtime_code_hash"],
               version_ctx["dependency_lock_hash"],version_ctx["config_snapshot_hash"],version_ctx.get("release_id"),
               version_ctx.get("production_certification_id"),version_ctx.get("production_stage"),str(trade_id)))
    ens=r.get("ensemble_shadow") or {}
    if ens.get("ensemble_decision_id"):
        c.execute("""UPDATE trade_memory SET ensemble_decision_id=?,ensemble_direction=?,ensemble_confidence=?,
                     ensemble_agreement=?,ensemble_diversity=?,ensemble_weight_version=?,ensemble_context_json=? WHERE trade_id=?""",
                  (ens.get("ensemble_decision_id"),ens.get("ensemble_direction"),ens.get("ensemble_confidence"),
                   ens.get("agreement_score"),ens.get("diversity_score"),ens.get("ensemble_weight_version"),
                   _tm_json(ens,{}),str(trade_id)))
    row=c.execute("SELECT id FROM trade_memory WHERE trade_id=?",(str(trade_id),)).fetchone()
    c.commit();c.close()

    log.info("TRADE_MEMORY OPEN trade=%s strategy=%s %s %s regime=%s conf=%s",
             trade_id,setup_variant(r),r["instrument"],direction,
             regime.get("market_regime"),conf.get("probability"))
    return int(row["id"]) if row else None


def update_trade_memory_excursions(instrument: str, candles_m1: List[Dict[str, Any]]) -> int:
    """
    MFE/MAE use only candles whose timestamps are at/after entry.
    They update POST-TRADE columns, never entry_context_json.
    """
    if not TRADE_MEMORY_ENABLED or not candles_m1:
        return 0
    c=conn()
    rows=[dict(x) for x in c.execute(
        "SELECT * FROM trade_memory WHERE symbol=? AND status='OPEN'",(instrument,)
    ).fetchall()]
    c.close()
    updated=0

    for tr in rows:
        entry=float(tr["entry_price"])
        stop=tr.get("stop_loss")
        if stop is None:
            continue
        risk=abs(entry-float(stop))
        if risk<=0:
            continue
        entry_dt=_parse_iso(tr.get("entry_ts"))
        if not entry_dt:
            continue

        max_fav=float(tr.get("mfe_r") or 0.0)
        max_adv=float(tr.get("mae_r") or 0.0)

        for candle in candles_m1:
            cdt=_parse_iso(candle.get("t"))
            if not cdt or cdt<entry_dt:
                continue
            high=float(candle["h"]); low=float(candle["l"])
            if tr["direction"]=="LONG":
                fav=max(0.0,(high-entry)/risk)
                adv=max(0.0,(entry-low)/risk)
            else:
                fav=max(0.0,(entry-low)/risk)
                adv=max(0.0,(high-entry)/risk)
            max_fav=max(max_fav,fav)
            max_adv=max(max_adv,adv)

        c=conn()
        c.execute("""UPDATE trade_memory
                     SET mfe_r=?,mae_r=?,max_drawdown_during_trade_r=?,updated_ts=?
                     WHERE trade_id=?""",
                  (max_fav,max_adv,max_adv,now_iso(),tr["trade_id"]))
        c.commit();c.close()
        updated+=1
    return updated


async def _trade_memory_exit_reason(client: httpx.AsyncClient, trade: Dict[str, Any]) -> List[str]:
    reasons=[]
    ids=list(trade.get("closingTransactionIDs") or [])
    if ids:
        tid=str(ids[-1])
        try:
            payload=await req(client,"GET",f"/v3/accounts/{{account}}/transactions/{tid}")
            tx=payload.get("transaction") or {}
            if tx.get("reason"):reasons.append(str(tx.get("reason")))
            if tx.get("type"):reasons.append(str(tx.get("type")))
            if tx.get("orderID"):reasons.append("order_id:"+str(tx.get("orderID")))
        except Exception as e:
            reasons.append("closing_transaction_unavailable:"+str(e))
    if not reasons:
        reasons.append("BROKER_TRADE_CLOSED")
    return reasons


async def reconcile_trade_memory(client: httpx.AsyncClient, instrument: Optional[str]=None) -> Dict[str, Any]:
    """
    Read-only OANDA reconciliation of executed trades.
    OANDA trade state is the source of truth for actual exit price/time/P&L.
    """
    if not TRADE_MEMORY_ENABLED:
        return {"enabled":False,"checked":0,"closed":0}

    c=conn()
    if instrument:
        rows=[dict(x) for x in c.execute("""SELECT * FROM trade_memory
                                           WHERE status='OPEN' AND symbol=?
                                           ORDER BY id LIMIT ?""",
                                        (instrument,TRADE_MEMORY_RECONCILE_LIMIT)).fetchall()]
    else:
        rows=[dict(x) for x in c.execute("""SELECT * FROM trade_memory
                                           WHERE status='OPEN'
                                           ORDER BY id LIMIT ?""",
                                        (TRADE_MEMORY_RECONCILE_LIMIT,)).fetchall()]
    c.close()

    closed=0;errors=[]
    for mem in rows:
        try:
            payload=await req(client,"GET",f"/v3/accounts/{{account}}/trades/{mem['trade_id']}")
            tr=payload.get("trade") or {}
            if str(tr.get("state"))!="CLOSED":
                continue

            exit_price=_risk_float(tr.get("averageClosePrice"))
            exit_ts=tr.get("closeTime") or now_iso()
            realized=_risk_float(tr.get("realizedPL"),0.0) or 0.0
            financing=_risk_float(tr.get("financing"),0.0) or 0.0
            dividend=_risk_float(tr.get("dividendAdjustment"),0.0) or 0.0
            guaranteed=_risk_float(tr.get("guaranteedExecutionFee"),0.0) or 0.0
            commission=_risk_float(tr.get("commission"),0.0) or 0.0
            fees_total=financing+dividend+guaranteed+commission
            net=realized+fees_total

            entry=float(mem["entry_price"])
            risk=abs(entry-float(mem["stop_loss"])) if mem.get("stop_loss") is not None else 0.0
            realized_r=None
            if exit_price is not None and risk>0:
                raw=(exit_price-entry)/risk if mem["direction"]=="LONG" else (entry-exit_price)/risk
                realized_r=float(raw)

            ent=_parse_iso(mem.get("entry_ts")); ex=_parse_iso(exit_ts)
            duration=(ex-ent).total_seconds() if ent and ex else None
            exit_reasons=await _trade_memory_exit_reason(client,tr)

            exit_context={
                "broker_state":tr.get("state"),
                "closing_transaction_ids":tr.get("closingTransactionIDs") or [],
                "broker_realized_pl":realized,
                "financing":financing,
                "dividend_adjustment":dividend,
                "guaranteed_execution_fee":guaranteed,
                "commission":commission,
                "average_close_price":exit_price,
                "close_time":exit_ts
            }

            quality=json.loads(mem.get("data_quality_json") or "{}")
            quality["actual_broker_exit_pending"]=False
            quality["actual_broker_exit_reconciled"]=True

            c=conn()
            c.execute("""UPDATE trade_memory SET
              status='CLOSED',exit_ts=?,exit_price=?,gross_result=?,net_result=?,
              realized_pl=?,financing=?,dividend_adjustment=?,guaranteed_execution_fees=?,
              commission=?,fees_total=?,duration_seconds=?,realized_r=?,
              exit_reasons_json=?,exit_context_json=?,data_quality_json=?,updated_ts=?
              WHERE trade_id=?""",
              (exit_ts,exit_price,realized,net,realized,financing,dividend,guaranteed,
               commission,fees_total,duration,realized_r,_tm_json(exit_reasons,[]),
               _tm_json(exit_context,{}),_tm_json(quality,{}),now_iso(),mem["trade_id"]))
            c.execute("UPDATE active_trade_management SET closed=1,updated_ts=? WHERE trade_id=?",
                      (now_iso(),mem["trade_id"]))
            c.commit();c.close()
            closed+=1
            log.info("TRADE_MEMORY CLOSED trade=%s net=%s realized_r=%s reason=%s",
                     mem["trade_id"],net,realized_r,";".join(exit_reasons))
            if RECOVERY_MANAGER_ENABLED:
                recovery_manager.journal("POSITION_CLOSED",trade_id=mem["trade_id"],
                                         order_id=mem.get("order_id"),strategy_id=mem.get("strategy"),
                                         payload={"net_result":net,"realized_r":realized_r,
                                                  "exit_reasons":exit_reasons})
        except Exception as e:
            errors.append({"trade_id":mem["trade_id"],"error":str(e)})
            log.warning("TRADE_MEMORY reconcile failed trade=%s err=%s",mem["trade_id"],e)

    if closed:
        refresh_trade_memory_degradation()
    return {"enabled":True,"checked":len(rows),"closed":closed,"errors":errors}


def _tm_value(row: Dict[str, Any]) -> Optional[float]:
    """
    Prefer broker net result in account units.
    Fall back to realized R only when monetary result is unavailable.
    A single analysis never mixes the two bases.
    """
    return _risk_float(row.get("net_result"))


def _tm_metric_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    monetary=[r for r in rows if _risk_float(r.get("net_result")) is not None]
    if len(monetary)==len(rows) and rows:
        vals=[float(r["net_result"]) for r in monetary]
        basis="NET_ACCOUNT_UNITS"
    else:
        usable=[r for r in rows if _risk_float(r.get("realized_r")) is not None]
        vals=[float(r["realized_r"]) for r in usable]
        basis="REALIZED_R"
        rows=usable

    n=len(vals)
    wins=[x for x in vals if x>0]
    losses=[x for x in vals if x<0]
    gross_profit=sum(wins)
    gross_loss=abs(sum(losses))
    pf=(gross_profit/gross_loss) if gross_loss>0 else (999.0 if gross_profit>0 else None)
    expectancy=(sum(vals)/n) if n else None
    avg_win=(sum(wins)/len(wins)) if wins else None
    avg_loss=(sum(losses)/len(losses)) if losses else None

    # Trade-level Sharpe (sqrt(N) scaled, not annualized by time).
    sharpe=None
    if n>=2:
        mean=sum(vals)/n
        var=sum((x-mean)**2 for x in vals)/(n-1)
        sd=math.sqrt(var)
        sharpe=(mean/sd)*math.sqrt(n) if sd>0 else None

    # Cumulative P&L/R max drawdown.
    curve=0.0;peak=0.0;max_dd=0.0
    for x in vals:
        curve+=x
        peak=max(peak,curve)
        max_dd=max(max_dd,peak-curve)

    max_wins=max_losses=cur_w=cur_l=0
    for x in vals:
        if x>0:
            cur_w+=1;cur_l=0;max_wins=max(max_wins,cur_w)
        elif x<0:
            cur_l+=1;cur_w=0;max_losses=max(max_losses,cur_l)

    realized_rr=[_risk_float(r.get("realized_r")) for r in rows]
    realized_rr=[x for x in realized_rr if x is not None]

    return {
        "samples":n,
        "basis":basis,
        "win_rate":len(wins)/n if n else None,
        "profit_factor":pf,
        "expectancy":expectancy,
        "average_win":avg_win,
        "average_loss":avg_loss,
        "average_realized_r":sum(realized_rr)/len(realized_rr) if realized_rr else None,
        "max_drawdown":max_dd if n else None,
        "max_consecutive_wins":max_wins,
        "max_consecutive_losses":max_losses,
        "trade_sharpe":sharpe,
        "gross_profit":gross_profit,
        "gross_loss":gross_loss
    }


def trade_memory_metrics(rows: List[Dict[str, Any]], min_samples: Optional[int]=None) -> Dict[str, Any]:
    min_samples=int(min_samples or TRADE_MEMORY_MIN_SAMPLE_SIZE)
    metrics=_tm_metric_rows(rows)
    metrics["minimum_sample_size"]=min_samples
    metrics["evidence_status"]="OK" if metrics["samples"]>=min_samples else "INSUFFICIENT_DATA"
    return metrics


def _tm_closed_rows(
    strategy: Optional[str]=None,
    regime: Optional[str]=None,
    symbol: Optional[str]=None,
    direction: Optional[str]=None,
    volatility: Optional[str]=None,
    min_confidence: Optional[float]=None,
    since: Optional[str]=None
) -> List[Dict[str, Any]]:
    where=["status='CLOSED'"]
    if RECOVERY_BLOCK_ADAPTIVE_LEARNING_COMPROMISED:
        where.append("COALESCE(execution_quality_compromised,0)=0")
    params=[]
    if strategy:where.append("strategy=?");params.append(strategy)
    if regime:where.append("market_regime_entry=?");params.append(regime)
    if symbol:where.append("symbol=?");params.append(symbol)
    if direction:where.append("direction=?");params.append(direction)
    if volatility:where.append("volatility_state_entry=?");params.append(volatility)
    if min_confidence is not None:where.append("strategy_confidence_entry>=?");params.append(float(min_confidence))
    if since:where.append("exit_ts>=?");params.append(since)
    c=conn()
    rows=[dict(x) for x in c.execute(
        "SELECT * FROM trade_memory WHERE "+" AND ".join(where)+" ORDER BY exit_ts,id",tuple(params)
    ).fetchall()]
    c.close()
    return rows


def trade_memory_group_analysis(group_by: str, min_samples: Optional[int]=None,
                                period: str="month") -> Dict[str, Any]:
    rows=_tm_closed_rows()
    supported={"strategy","market_regime","symbol","direction","confidence",
               "volatility","session","period"}
    if group_by not in supported:
        return {"error":"unsupported_group_by","supported":sorted(supported)}

    groups={}
    for r in rows:
        if group_by=="strategy":key=r.get("strategy")
        elif group_by=="market_regime":key=r.get("market_regime_entry") or "UNKNOWN"
        elif group_by=="symbol":key=r.get("symbol")
        elif group_by=="direction":key=r.get("direction")
        elif group_by=="confidence":key=r.get("confidence_bucket") or "UNKNOWN"
        elif group_by=="volatility":key=r.get("volatility_state_entry") or "UNKNOWN"
        elif group_by=="session":key=r.get("entry_session") or "UNKNOWN"
        else:
            dt=_parse_iso(r.get("exit_ts") or r.get("entry_ts"))
            if not dt:key="UNKNOWN"
            elif period=="day":key=dt.date().isoformat()
            elif period=="week":key=f"{dt.isocalendar().year}-W{dt.isocalendar().week:02d}"
            else:key=f"{dt.year:04d}-{dt.month:02d}"
        groups.setdefault(str(key),[]).append(r)

    return {
        "group_by":group_by,
        "period":period if group_by=="period" else None,
        "groups":[{"key":k,**trade_memory_metrics(v,min_samples)} for k,v in sorted(groups.items())]
    }


def trade_memory_combination_analysis(
    strategy: Optional[str]=None,
    regime: Optional[str]=None,
    symbol: Optional[str]=None,
    direction: Optional[str]=None,
    volatility: Optional[str]=None,
    min_confidence: Optional[float]=None,
    min_samples: Optional[int]=None
) -> Dict[str, Any]:
    rows=_tm_closed_rows(strategy,regime,symbol,direction,volatility,min_confidence)
    metrics=trade_memory_metrics(rows,min_samples)
    return {
        "combination":{
            "strategy":strategy,"market_regime":regime,"symbol":symbol,
            "direction":direction,"volatility":volatility,
            "min_confidence":min_confidence
        },
        **metrics
    }


def _tm_degradation_for_rows(strategy: str, regime: Optional[str],
                             rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    recent_n=TRADE_MEMORY_DEGRADATION_RECENT
    if len(rows)<TRADE_MEMORY_DEGRADATION_MIN_HISTORY+recent_n:
        return {
            "status":"INSUFFICIENT_DATA",
            "historical_samples":max(0,len(rows)-min(recent_n,len(rows))),
            "recent_samples":min(recent_n,len(rows)),
            "reason":"minimum historical/recent sample size not reached"
        }

    historical=rows[:-recent_n]
    recent=rows[-recent_n:]
    hm=_tm_metric_rows(historical)
    rm=_tm_metric_rows(recent)

    hp=hm.get("profit_factor");rp=rm.get("profit_factor")
    he=hm.get("expectancy");rexp=rm.get("expectancy")
    hwr=hm.get("win_rate");rwr=rm.get("win_rate")

    status="STABLE";reason="recent behavior remains within historical range";score=0.0

    pf_drop=None
    if hp is not None and rp is not None and hp<900 and rp<900:
        pf_drop=hp-rp

    strong_pf_degradation=(
        hp is not None and rp is not None and
        hp>=1.20 and rp<TRADE_MEMORY_DEGRADATION_PF_FLOOR and
        (pf_drop is None or pf_drop>=TRADE_MEMORY_DEGRADATION_MIN_PF_DROP)
    )
    expectancy_flip=(he is not None and rexp is not None and he>0 and rexp<0)

    if strong_pf_degradation or expectancy_flip:
        status="DEGRADED"
        reason="recent profit factor/expectancy materially below historical behavior"
        score=1.0
    elif (hp is not None and rp is not None and hp<900 and rp<900 and rp<hp*0.75) or (
          he is not None and rexp is not None and he>0 and rexp<he*0.50):
        status="WATCH"
        reason="recent behavior weakening versus historical baseline"
        score=0.5

    return {
        "status":status,
        "historical_samples":hm["samples"],
        "recent_samples":rm["samples"],
        "historical_profit_factor":hp,
        "recent_profit_factor":rp,
        "historical_expectancy":he,
        "recent_expectancy":rexp,
        "historical_win_rate":hwr,
        "recent_win_rate":rwr,
        "change_score":score,
        "reason":reason
    }


def refresh_trade_memory_degradation() -> Dict[str, Any]:
    """
    Observe/report only. It never pauses strategies or modifies Director/Risk rules.
    """
    if not TRADE_MEMORY_ENABLED:
        return {"enabled":False}
    c=conn()
    pairs=[dict(x) for x in c.execute("""SELECT DISTINCT strategy,market_regime_entry
                                         FROM trade_memory WHERE status='CLOSED'""").fetchall()]
    strategies=[x["strategy"] for x in c.execute(
        "SELECT DISTINCT strategy FROM trade_memory WHERE status='CLOSED'"
    ).fetchall()]
    c.close()

    results=[]
    scopes=[("STRATEGY",st,None) for st in strategies]
    scopes += [("STRATEGY_REGIME",x["strategy"],x["market_regime_entry"]) for x in pairs]

    for scope_type,strategy,regime in scopes:
        rows=_tm_closed_rows(strategy=strategy,regime=regime)
        d=_tm_degradation_for_rows(strategy,regime,rows)
        scope_key=f"{scope_type}::{strategy}::{regime or 'ALL'}"
        c=conn()
        c.execute("""INSERT INTO trade_memory_degradation(
          scope_key,ts,scope_type,strategy,market_regime,status,historical_samples,recent_samples,
          historical_profit_factor,recent_profit_factor,historical_expectancy,recent_expectancy,
          historical_win_rate,recent_win_rate,change_score,reason,details_json)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(scope_key) DO UPDATE SET
          ts=excluded.ts,status=excluded.status,historical_samples=excluded.historical_samples,
          recent_samples=excluded.recent_samples,historical_profit_factor=excluded.historical_profit_factor,
          recent_profit_factor=excluded.recent_profit_factor,historical_expectancy=excluded.historical_expectancy,
          recent_expectancy=excluded.recent_expectancy,historical_win_rate=excluded.historical_win_rate,
          recent_win_rate=excluded.recent_win_rate,change_score=excluded.change_score,
          reason=excluded.reason,details_json=excluded.details_json""",
          (scope_key,now_iso(),scope_type,strategy,regime,d["status"],
           d.get("historical_samples",0),d.get("recent_samples",0),
           d.get("historical_profit_factor"),d.get("recent_profit_factor"),
           d.get("historical_expectancy"),d.get("recent_expectancy"),
           d.get("historical_win_rate"),d.get("recent_win_rate"),d.get("change_score"),
           d["reason"],_tm_json(d,{})))
        c.commit();c.close()
        results.append({"scope_key":scope_key,**d})
        if d["status"]=="DEGRADED":
            log.warning("TRADE_MEMORY DEGRADATION %s hist_pf=%s recent_pf=%s reason=%s",
                        scope_key,d.get("historical_profit_factor"),d.get("recent_profit_factor"),d["reason"])
    return {"enabled":True,"results":results}


def trade_memory_recent_degradation(strategy: Optional[str]=None) -> List[Dict[str, Any]]:
    c=conn()
    if strategy:
        rows=c.execute("""SELECT * FROM trade_memory_degradation
                          WHERE strategy=? ORDER BY ts DESC""",(strategy,)).fetchall()
    else:
        rows=c.execute("""SELECT * FROM trade_memory_degradation
                          ORDER BY CASE status WHEN 'DEGRADED' THEN 1 WHEN 'WATCH' THEN 2
                          WHEN 'STABLE' THEN 3 ELSE 4 END,ts DESC""").fetchall()
    c.close()
    return [dict(x) for x in rows]


def trade_memory_insights(query: str, strategy: Optional[str]=None,
                          regime: Optional[str]=None) -> Dict[str, Any]:
    """
    Stable internal read interface for future AI Strategy Director use.
    It does not currently alter Director decisions.
    """
    q=str(query or "").lower()
    if q=="performance_by_strategy":
        return trade_memory_group_analysis("strategy")
    if q=="performance_by_regime":
        return trade_memory_group_analysis("market_regime")
    if q=="recent_degradation":
        return {"query":q,"results":trade_memory_recent_degradation(strategy)}
    if q=="confidence_performance":
        return trade_memory_group_analysis("confidence")
    if q=="strategy_regime_edge":
        if not strategy:
            return {"query":q,"error":"strategy is required"}
        rows=_tm_closed_rows(strategy=strategy,regime=regime)
        return {"query":q,"strategy":strategy,"regime":regime,
                **trade_memory_metrics(rows)}
    if q=="recent_vs_historical_performance":
        if not strategy:
            return {"query":q,"error":"strategy is required"}
        rows=_tm_closed_rows(strategy=strategy,regime=regime)
        return {"query":q,"strategy":strategy,"regime":regime,
                **_tm_degradation_for_rows(strategy,regime,rows)}
    return {
        "error":"unsupported_query",
        "supported":[
            "performance_by_strategy","performance_by_regime","recent_degradation",
            "confidence_performance","strategy_regime_edge",
            "recent_vs_historical_performance"
        ]
    }


def trade_memory_example(trade_id: str) -> Optional[Dict[str, Any]]:
    c=conn()
    row=c.execute("SELECT * FROM trade_memory WHERE trade_id=?",(trade_id,)).fetchone()
    c.close()
    if not row:return None
    out=dict(row)
    for k in ("entry_reasons_json","exit_reasons_json","entry_context_json",
              "execution_context_json","exit_context_json","risk_recommendation_json",
              "data_quality_json"):
        try:out[k[:-5] if k.endswith("_json") else k]=json.loads(out.get(k) or "{}")
        except Exception:pass
    return out



def _al_event(run_id: str, stage: str, status: str,
              strategy_id: Optional[str]=None, candidate_id: Optional[str]=None,
              details: Optional[Dict[str,Any]]=None) -> None:
    c=conn()
    c.execute("""INSERT INTO adaptive_learning_events(
      run_id,ts,stage,strategy_id,candidate_id,status,details_json)
      VALUES(?,?,?,?,?,?,?)""",
      (run_id,now_iso(),stage,strategy_id,candidate_id,status,_tm_json(details or {},{})))
    c.commit();c.close()
    log.info("ADAPTIVE_LEARNING %s run=%s strategy=%s candidate=%s status=%s",
             stage,run_id,strategy_id,candidate_id,status)


def _al_trade_rows(strategy: Optional[str]=None, instrument: Optional[str]=None) -> List[Dict[str,Any]]:
    where=["status='CLOSED'","COALESCE(execution_quality_compromised,0)=0"];params=[]
    if strategy:where.append("strategy=?");params.append(strategy)
    if instrument:where.append("symbol=?");params.append(InstrumentRegistry.normalize_symbol(instrument))
    c=conn();rows=[dict(x) for x in c.execute(
        "SELECT * FROM trade_memory WHERE "+" AND ".join(where)+" ORDER BY entry_ts,id",tuple(params)).fetchall()];c.close()
    return rows


def _al_period(rows: List[Dict[str,Any]]) -> Dict[str,Optional[str]]:
    dates=[r.get("entry_ts") for r in rows if r.get("entry_ts")]
    return {"start":min(dates) if dates else None,"end":max(dates) if dates else None}


def _al_production_parameters(strategy: str) -> Dict[str,Any]:
    """
    Snapshot only. Candidate parameters never overwrite these values.
    """
    return {
        "strategy_id":strategy,
        "code_version":VERSION_TAG,
        "execution_min_confidence":EXECUTION_MIN_CONFIDENCE,
        "quality_threshold":THRESH,
        "min_rr":MIN_RR,
        "session_filter":SESSION,
        "news_filter":NEWS,
        "trade_units":UNITS
    }


def _al_next_candidate_version(strategy: str) -> str:
    c=conn()
    n=c.execute("SELECT COUNT(*) n FROM candidate_strategies WHERE strategy_id=?",(strategy,)).fetchone()["n"]
    c.close()
    return f"{strategy}_candidate_v{int(n)+2}"


def _al_recent_candidate(strategy: str, parameter_name: str) -> Optional[Dict[str,Any]]:
    c=conn()
    row=c.execute("""SELECT * FROM candidate_strategies
                     WHERE strategy_id=? AND parameter_name=?
                     ORDER BY id DESC LIMIT 1""",(strategy,parameter_name)).fetchone()
    c.close()
    return dict(row) if row else None


def _al_cooldown_allows(strategy: str, parameter_name: str,
                        rows: List[Dict[str,Any]]) -> Dict[str,Any]:
    prev=_al_recent_candidate(strategy,parameter_name)
    if not prev:return {"ok":True}
    try:
        generated=datetime.fromisoformat(prev["generated_at"].replace("Z","+00:00"))
        hours=(datetime.now(timezone.utc)-generated).total_seconds()/3600
    except Exception:
        hours=int(managed_value("adaptive_learning.cooldown_hours",ADAPTIVE_LEARNING_COOLDOWN_HOURS))+1
    new_count=sum(1 for r in rows if r.get("entry_ts") and r["entry_ts"]>str(prev.get("period_end") or ""))
    ok=hours>=int(managed_value("adaptive_learning.cooldown_hours",ADAPTIVE_LEARNING_COOLDOWN_HOURS)) and new_count>=ADAPTIVE_LEARNING_MIN_NEW_TRADES
    return {"ok":ok,"hours_since":hours,"new_trades":new_count,
            "required_hours":int(managed_value("adaptive_learning.cooldown_hours",ADAPTIVE_LEARNING_COOLDOWN_HOURS)),
            "required_new_trades":ADAPTIVE_LEARNING_MIN_NEW_TRADES}


def _al_candidate_confidence(validation: Dict[str,Any]) -> float:
    cm=validation.get("oos_candidate") or {}
    samples=float(cm.get("samples") or 0)
    folds=float(validation.get("positive_oos_fold_fraction") or 0)
    score=float(validation.get("candidate_score") or 0)
    return clamp(0.35*min(1.0,samples/100.0)+0.35*folds+0.30*score,0.0,1.0)


def _al_candidate_risks(change_type: str, validation: Dict[str,Any]) -> List[str]:
    risks=[
        "historical counterfactual only; production behavior remains unchanged",
        "candidate has not traded live",
        "candidate may reduce opportunity count"
    ]
    if change_type in ("MIN_CONFIDENCE","MIN_DIRECTOR_CONFIDENCE"):
        risks.append("threshold may become regime-specific over time")
    if change_type in ("EXCLUDE_REGIME","EXCLUDE_VOLATILITY"):
        risks.append("excluding a condition can miss future regime reversals")
    cm=validation.get("oos_candidate") or {}
    if (cm.get("samples") or 0)<50:
        risks.append("out-of-sample sample remains moderate")
    return risks


def _al_store_candidate(run_id: str, strategy: str, candidate: Dict[str,Any],
                        reason: str, evidence: Dict[str,Any],
                        validation: Dict[str,Any], rows: List[Dict[str,Any]]) -> Dict[str,Any]:
    candidate_id=f"cand_{hashlib.sha256((run_id+strategy+candidate['parameter_name']+json.dumps(candidate,sort_keys=True)).encode()).hexdigest()[:16]}"
    version=_al_next_candidate_version(strategy)
    period=_al_period(rows)
    dataset_hash=al_dataset_fingerprint(rows)
    score=validation.get("candidate_score")
    confidence=_al_candidate_confidence(validation)
    improvement=validation.get("avg_oos_expectancy_improvement")
    status=validation.get("status","REJECTED_AS_CANDIDATE")
    current_params=_al_production_parameters(strategy)
    candidate_params={**current_params,
                      "candidate_overlay":{
                          "change_type":candidate["change_type"],
                          "parameter_name":candidate["parameter_name"],
                          "value":candidate["proposed_value"]
                      }}

    cooldown_until=(datetime.now(timezone.utc)+timedelta(hours=int(managed_value("adaptive_learning.cooldown_hours",ADAPTIVE_LEARNING_COOLDOWN_HOURS)))).isoformat()
    c=conn()
    c.execute("""INSERT OR IGNORE INTO candidate_strategies(
      candidate_id,run_id,strategy_id,production_version,candidate_version,status,
      change_type,parameter_name,current_value_json,proposed_value_json,reason,
      supporting_evidence_json,sample_size,expected_improvement,confidence,risks_json,
      original_parameters_json,candidate_parameters_json,validation_json,candidate_score,
      dataset_hash,period_start,period_end,generated_at,validated_at,cooldown_until,auto_deploy)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
      (candidate_id,run_id,strategy,VERSION_TAG,version,status,candidate["change_type"],
       candidate["parameter_name"],_tm_json(candidate["current_value"],None),
       _tm_json(candidate["proposed_value"],None),reason,_tm_json(evidence,{}),len(rows),
       improvement,confidence,_tm_json(_al_candidate_risks(candidate["change_type"],validation),[]),
       _tm_json(current_params,{}),_tm_json(candidate_params,{}),_tm_json(validation,{}),score,
       dataset_hash,period["start"],period["end"],now_iso(),now_iso(),cooldown_until))
    c.commit()
    row=c.execute("SELECT * FROM candidate_strategies WHERE candidate_id=?",(candidate_id,)).fetchone()
    c.close()
    return dict(row) if row else {}


def _al_generate_proposals_for_strategy(run_id: str, strategy: str,
                                        rows: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    """
    Generate only changes reconstructible from entry-time Trade Memory.
    No blind parameter grid search.
    """
    if len(rows)<int(managed_value("adaptive_learning.min_trades",ADAPTIVE_LEARNING_MIN_TRADES)):
        _al_event(run_id,"ANALYZE","INSUFFICIENT_DATA",strategy,None,
                  {"samples":len(rows),"minimum":ADAPTIVE_LEARNING_MIN_TRADES})
        return []

    period=_al_period(rows)
    try:
        start_dt=datetime.fromisoformat(str(period["start"]).replace("Z","+00:00"))
        end_dt=datetime.fromisoformat(str(period["end"]).replace("Z","+00:00"))
        observation_days=max(0.0,(end_dt-start_dt).total_seconds()/86400.0)
    except Exception:
        observation_days=0.0
    if observation_days<ADAPTIVE_LEARNING_MIN_OBSERVATION_DAYS:
        _al_event(run_id,"ANALYZE","INSUFFICIENT_DATA",strategy,None,
                  {"samples":len(rows),"observation_days":observation_days,
                   "minimum_observation_days":ADAPTIVE_LEARNING_MIN_OBSERVATION_DAYS})
        return []

    proposals=[]
    baseline=al_metrics(rows)
    _al_event(run_id,"ANALYZE","OK",strategy,None,
              {"samples":len(rows),"baseline":baseline})

    # 1) Confidence threshold candidate: only ONE bounded step.
    current=float(EXECUTION_MIN_CONFIDENCE)
    lower=[r for r in rows if _risk_float(r.get("strategy_confidence_entry")) is not None
           and float(r["strategy_confidence_entry"])<current+ADAPTIVE_LEARNING_MAX_CONFIDENCE_STEP]
    high=[r for r in rows if _risk_float(r.get("strategy_confidence_entry")) is not None
          and float(r["strategy_confidence_entry"])>=current+ADAPTIVE_LEARNING_MAX_CONFIDENCE_STEP]
    if len(lower)>=TRADE_MEMORY_MIN_SAMPLE_SIZE and len(high)>=TRADE_MEMORY_MIN_SAMPLE_SIZE:
        lm=al_metrics(lower);hm=al_metrics(high)
        if (hm.get("expectancy") or 0)>(lm.get("expectancy") or 0) and (hm.get("profit_factor") or 0)>1.0:
            proposals.append({
                "change_type":"MIN_CONFIDENCE",
                "parameter_name":"execution_min_confidence",
                "current_value":current,
                "proposed_value":round(min(0.95,current+ADAPTIVE_LEARNING_MAX_CONFIDENCE_STEP),4),
                "reason":"higher-confidence trades show stronger expectancy than lower-confidence observations",
                "evidence":{"lower_confidence":lm,"higher_confidence":hm}
            })

    # 2) Regime exclusion only when degradation engine already flags it and sample is sufficient.
    c=conn()
    deg=[dict(x) for x in c.execute("""SELECT * FROM trade_memory_degradation
                                       WHERE strategy=? AND scope_type='STRATEGY_REGIME'
                                         AND status='DEGRADED'""",(strategy,)).fetchall()]
    c.close()
    for d in deg:
        regime=d.get("market_regime")
        rg=[r for r in rows if r.get("market_regime_entry")==regime]
        if len(rg)>=TRADE_MEMORY_MIN_SAMPLE_SIZE:
            proposals.append({
                "change_type":"EXCLUDE_REGIME",
                "parameter_name":"regime_exclusion",
                "current_value":"NONE",
                "proposed_value":regime,
                "reason":"Trade Memory reports persistent degradation in this strategy/regime combination",
                "evidence":{"degradation":d,"regime_metrics":al_metrics(rg)}
            })

    # 3) Volatility exclusion if one state has persistent negative expectancy/PF and enough observations.
    by_vol={}
    for r in rows:
        by_vol.setdefault(str(r.get("volatility_state_entry") or "UNKNOWN"),[]).append(r)
    for vol,rr in by_vol.items():
        if vol not in ("HIGH","LOW","ABNORMAL") or len(rr)<TRADE_MEMORY_MIN_SAMPLE_SIZE:
            continue
        vm=al_metrics(rr)
        if (vm.get("expectancy") is not None and vm["expectancy"]<0) and (
            vm.get("profit_factor") is not None and vm["profit_factor"]<1.0):
            proposals.append({
                "change_type":"EXCLUDE_VOLATILITY",
                "parameter_name":"volatility_exclusion",
                "current_value":"NONE",
                "proposed_value":vol,
                "reason":"this volatility state has negative expectancy and profit factor below 1",
                "evidence":{"volatility_metrics":vm}
            })

    # 4) Director-confidence candidate. No proposal unless the higher-confidence
    # subset actually shows stronger performance; this preserves NO_CHANGE_RECOMMENDED.
    vals=[_risk_float(r.get("director_confidence_entry")) for r in rows]
    vals=[v for v in vals if v is not None]
    if len(vals)>=int(managed_value("adaptive_learning.min_trades",ADAPTIVE_LEARNING_MIN_TRADES)):
        q=sorted(vals)[int(0.35*(len(vals)-1))]
        proposed=min(0.95,max(0.50,round(q,2)))
        low_dc=[r for r in rows if _risk_float(r.get("director_confidence_entry")) is not None
                and float(r["director_confidence_entry"])<proposed]
        high_dc=[r for r in rows if _risk_float(r.get("director_confidence_entry")) is not None
                 and float(r["director_confidence_entry"])>=proposed]
        if len(low_dc)>=TRADE_MEMORY_MIN_SAMPLE_SIZE and len(high_dc)>=TRADE_MEMORY_MIN_SAMPLE_SIZE:
            ldm=al_metrics(low_dc);hdm=al_metrics(high_dc)
            if (hdm.get("expectancy") or 0)>(ldm.get("expectancy") or 0) and (hdm.get("profit_factor") or 0)>1.0:
                proposals.append({
                    "change_type":"MIN_DIRECTOR_CONFIDENCE",
                    "parameter_name":"director_min_confidence_candidate",
                    "current_value":0.0,
                    "proposed_value":proposed,
                    "reason":"higher Director-confidence observations show stronger expectancy",
                    "evidence":{"lower_director_confidence":ldm,"higher_director_confidence":hdm,
                                "bounded_quantile":"35%"}
                })

    # De-duplicate by parameter/value; do not chase dozens of alternatives.
    seen=set();unique=[]
    for x in proposals:
        key=(x["parameter_name"],str(x["proposed_value"]))
        if key in seen:continue
        seen.add(key);unique.append(x)
    return unique[:4]


def detect_adaptive_concept_drift() -> Dict[str,Any]:
    c=conn()
    pairs=[dict(x) for x in c.execute(
        """SELECT DISTINCT symbol,strategy,market_regime_entry FROM trade_memory
           WHERE status='CLOSED'"""
    ).fetchall()]
    c.close()
    results=[]
    for x in pairs:
        instrument=InstrumentRegistry.normalize_symbol(x.get("symbol") or PRIMARY_INSTRUMENT);strategy=x["strategy"];regime=x["market_regime_entry"]
        rows=_tm_closed_rows(strategy=strategy,regime=regime,symbol=instrument)
        d=al_concept_drift(rows,TRADE_MEMORY_DEGRADATION_RECENT,
                           TRADE_MEMORY_DEGRADATION_MIN_HISTORY)
        scope=f"{instrument}::{strategy}::{regime or 'UNKNOWN'}"
        confidence=0.0
        if d["status"]=="POSSIBLE_CONCEPT_DRIFT":
            confidence=clamp(min(1.0,len(rows)/150.0)*0.7+0.3,0,1)
        c=conn()
        c.execute("""INSERT INTO concept_drift_alerts(
          scope_key,ts,strategy_id,market_regime,status,confidence,
          historical_metrics_json,previous_metrics_json,recent_metrics_json,reason,auto_action)
          VALUES(?,?,?,?,?,?,?,?,?,?,0)
          ON CONFLICT(scope_key) DO UPDATE SET ts=excluded.ts,status=excluded.status,
          confidence=excluded.confidence,historical_metrics_json=excluded.historical_metrics_json,
          previous_metrics_json=excluded.previous_metrics_json,recent_metrics_json=excluded.recent_metrics_json,
          reason=excluded.reason,auto_action=0""",
          (scope,now_iso(),f"{instrument}::{strategy}",regime,d["status"],confidence,
           _tm_json(d.get("historical") or {},{}),_tm_json(d.get("previous_window") or {},{}),
           _tm_json(d.get("current_window") or {},{}),
           "two consecutive recent windows materially weakened vs historical edge"
           if d["status"]=="POSSIBLE_CONCEPT_DRIFT" else d["status"]))
        c.commit();c.close()
        results.append({"scope_key":scope,"instrument":instrument,"strategy":strategy,**d,"confidence":confidence})
        if d["status"]=="POSSIBLE_CONCEPT_DRIFT":
            log.warning("ADAPTIVE_LEARNING POSSIBLE_CONCEPT_DRIFT %s confidence=%.3f",scope,confidence)
    return {"results":results,"auto_action":False}


def run_adaptive_learning(force: bool=False) -> Dict[str,Any]:
    """
    OBSERVE -> ANALYZE -> GENERATE CANDIDATE -> VALIDATE -> ACCEPT/REJECT AS CANDIDATE.
    Never DEPLOY TO PRODUCTION.
    """
    if not ADAPTIVE_LEARNING_ENABLED:
        return {"enabled":False}

    all_rows=_al_trade_rows()
    dataset_hash=al_dataset_fingerprint(all_rows)
    period=_al_period(all_rows)
    run_id=f"al_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}_{dataset_hash[:10]}"
    config={
        "min_trades":int(managed_value("adaptive_learning.min_trades",ADAPTIVE_LEARNING_MIN_TRADES)),
        "min_observation_days":ADAPTIVE_LEARNING_MIN_OBSERVATION_DAYS,
        "min_oos_trades":ADAPTIVE_LEARNING_MIN_OOS_TRADES,
        "folds":ADAPTIVE_LEARNING_WALK_FORWARD_FOLDS,
        "embargo_minutes":ADAPTIVE_LEARNING_EMBARGO_MINUTES,
        "cooldown_hours":int(managed_value("adaptive_learning.cooldown_hours",ADAPTIVE_LEARNING_COOLDOWN_HOURS)),
        "max_confidence_step":ADAPTIVE_LEARNING_MAX_CONFIDENCE_STEP,
        "observation_only":True
    }
    c=conn()
    c.execute("""INSERT OR IGNORE INTO adaptive_learning_runs(
      run_id,started_ts,code_version,dataset_hash,period_start,period_end,trade_count,status,config_json)
      VALUES(?,?,?,?,?,?,?,?,?)""",
      (run_id,now_iso(),VERSION_TAG,dataset_hash,period["start"],period["end"],
       len(all_rows),"RUNNING",_tm_json(config,{})))
    c.commit();c.close()

    _al_event(run_id,"OBSERVE","OK",None,None,
              {"trades":len(all_rows),"dataset_hash":dataset_hash,"period":period})

    refresh_trade_memory_degradation()
    drift=detect_adaptive_concept_drift()

    c=conn()
    strategy_pairs=[(InstrumentRegistry.normalize_symbol(x["symbol"] or PRIMARY_INSTRUMENT),x["strategy"]) for x in c.execute(
        "SELECT DISTINCT symbol,strategy FROM trade_memory WHERE status='CLOSED' AND strategy IS NOT NULL"
    ).fetchall()]
    c.close()

    accepted=[];rejected=[];insufficient=[];no_change=[]
    for instrument,base_strategy in strategy_pairs:
        strategy=base_strategy if instrument==PRIMARY_INSTRUMENT else f"{instrument}::{base_strategy}"
        rows=_al_trade_rows(base_strategy,instrument)
        if len(rows)<int(managed_value("adaptive_learning.min_trades",ADAPTIVE_LEARNING_MIN_TRADES)):
            insufficient.append({"strategy":strategy,"instrument":instrument,"samples":len(rows)})
            _al_event(run_id,"ANALYZE","INSUFFICIENT_DATA",strategy,None,
                      {"samples":len(rows)})
            continue

        proposals=_al_generate_proposals_for_strategy(run_id,strategy,rows)
        if not proposals:
            no_change.append({"strategy":strategy,"instrument":instrument,"status":"NO_CHANGE_RECOMMENDED"})
            _al_event(run_id,"GENERATE_CANDIDATE","NO_CHANGE_RECOMMENDED",strategy,None,
                      {"reason":"no robust bounded change identified"})
            continue

        generated_for_strategy=0
        for prop in proposals:
            cooldown=_al_cooldown_allows(strategy,prop["parameter_name"],rows)
            if not cooldown["ok"] and not force:
                _al_event(run_id,"GENERATE_CANDIDATE","COOLDOWN",strategy,None,
                          {"proposal":prop,"cooldown":cooldown})
                continue

            # No future data can enter selection predicate.
            if not al_candidate_uses_entry_only(prop):
                _al_event(run_id,"VALIDATE","UNVALIDATABLE",strategy,None,
                          {"proposal":prop,"reason":"candidate uses non-entry data"})
                continue

            _al_event(run_id,"GENERATE_CANDIDATE","GENERATED",strategy,None,prop)
            validation=al_validate_candidate(
                rows,prop,min_trades=ADAPTIVE_LEARNING_MIN_OOS_TRADES,
                folds=ADAPTIVE_LEARNING_WALK_FORWARD_FOLDS,
                embargo_minutes=ADAPTIVE_LEARNING_EMBARGO_MINUTES
            )
            if (validation.get("status")=="ACCEPTED_AS_CANDIDATE" and
                float(validation.get("candidate_score") or 0)<ADAPTIVE_LEARNING_ACCEPT_SCORE):
                validation["status"]="REJECTED_AS_CANDIDATE"
                validation["reason"]="candidate score below configured acceptance threshold"

            governance_candidate=None
            if GOVERNANCE_ENABLED and validation.get("status")=="ACCEPTED_AS_CANDIDATE":
                magnitude=governance_engine.classify_change_magnitude(
                    f"strategy.{strategy}.{prop['parameter_name']}",
                    prop.get("current_value"),prop.get("proposed_value"),"HIGH_RISK")
                governance_candidate=governance_engine.check_action(
                    "CANDIDATE_CRITICAL_CREATE" if magnitude in ("MAJOR","CRITICAL") else "CANDIDATE_CREATE",
                    target=strategy,
                    context={"trigger":"ADAPTIVE_LEARNING_CANDIDATE","component":f"strategy.{strategy}.{prop['parameter_name']}",
                             "current_value":prop.get("current_value"),"proposed_value":prop.get("proposed_value"),
                             "risk_level":"HIGH_RISK","magnitude":magnitude,
                             "affected_modules":["ADAPTIVE_LEARNING_ENGINE","VALIDATION_PIPELINE"]})
                if governance_candidate.get("enforced"):
                    _al_event(run_id,"GOVERNANCE","BLOCKED",strategy,None,
                              {"proposal":prop,"governance":governance_candidate})
                    rejected.append({"strategy":strategy,"status":"GOVERNANCE_BLOCKED",
                                     "proposal":prop,"governance":governance_candidate})
                    continue

            stored=_al_store_candidate(
                run_id,strategy,prop,prop["reason"],prop.get("evidence") or {},
                validation,rows
            )
            generated_for_strategy+=1
            cid=stored.get("candidate_id")
            _al_event(run_id,"VALIDATE",validation.get("status","UNKNOWN"),
                      strategy,cid,{**validation,"governance":governance_candidate})

            if validation.get("status")=="ACCEPTED_AS_CANDIDATE":
                accepted.append(stored)
                try:
                    _vp_registry_upsert(stored,"PENDING",reason="accepted by Adaptive Learning; awaiting Step 7 validation")
                except Exception as e:
                    log.warning("Candidate registry pending insert failed candidate=%s err=%s",cid,e)
                _al_event(run_id,"ACCEPT_AS_CANDIDATE","ACCEPTED",strategy,cid,
                          {"candidate_version":stored.get("candidate_version"),
                           "score":validation.get("candidate_score"),
                           "auto_deploy":False})
                try:
                    ai_actor=security_manager.internal_actor("ADAPTIVE_LEARNING_ENGINE","SYSTEM_RECOMMENDER")
                    change_key=f"strategy.{strategy}.{prop['parameter_name']}"
                    cr=security_manager.create_change_request(
                        ai_actor,component=f"strategy_candidate.{cid}",key=change_key,
                        proposed=prop["proposed_value"],reason=prop["reason"],
                        expected_impact=f"Candidate {stored.get('candidate_version')} validated in research only; deployment still forbidden until Validation Pipeline and human approval.",
                        rollback_plan=f"Keep {stored.get('production_version')} untouched; rollback configuration snapshot and/or Deployment Manager to previous production version.")
                    _al_event(run_id,"CHANGE_REQUEST","PENDING_REVIEW",strategy,cid,
                              {"change_id":(cr.get("change") or {}).get("change_id"),
                               "self_approval":False})
                except Exception as e:
                    log.warning("Adaptive candidate change request creation failed candidate=%s err=%s",cid,e)
            else:
                rejected.append(stored)
                _al_event(run_id,"REJECT_AS_CANDIDATE",validation.get("status","REJECTED"),
                          strategy,cid,{"reason":validation.get("reason")})

        if generated_for_strategy==0:
            no_change.append({"strategy":strategy,"status":"NO_CHANGE_RECOMMENDED"})

    summary={
        "accepted_candidates":len(accepted),"rejected_candidates":len(rejected),
        "insufficient_strategies":len(insufficient),
        "no_change_recommended":len(no_change),
        "concept_drift_alerts":sum(1 for x in drift["results"]
                                   if x["status"]=="POSSIBLE_CONCEPT_DRIFT"),
        "auto_deploy":False
    }
    c=conn()
    c.execute("""UPDATE adaptive_learning_runs SET completed_ts=?,status='COMPLETED',
                 summary_json=? WHERE run_id=?""",
              (now_iso(),_tm_json(summary,{}),run_id))
    c.commit();c.close()
    _al_event(run_id,"COMPLETE","COMPLETED",None,None,summary)

    return {
        "enabled":True,"observation_only":True,"run_id":run_id,
        "dataset_hash":dataset_hash,"period":period,"summary":summary,
        "accepted_candidates":accepted,"rejected_candidates":rejected,
        "insufficient_data":insufficient,"no_change_recommended":no_change,
        "concept_drift":drift
    }


def adaptive_learning_insights(query: str, strategy: Optional[str]=None) -> Dict[str,Any]:
    """
    Informational interface for AI Strategy Director. No activation authority.
    """
    q=str(query or "").lower()
    c=conn()
    if q=="candidate_strategies":
        if strategy:
            rows=c.execute("""SELECT * FROM candidate_strategies
                              WHERE strategy_id=? ORDER BY id DESC LIMIT 100""",(strategy,)).fetchall()
        else:
            rows=c.execute("""SELECT * FROM candidate_strategies
                              ORDER BY id DESC LIMIT 100""").fetchall()
        out={"query":q,"results":[dict(x) for x in rows],"activation_authority":False}
    elif q=="degradation_alerts":
        rows=c.execute("""SELECT * FROM trade_memory_degradation
                          WHERE status IN ('WATCH','DEGRADED')
                          ORDER BY ts DESC""").fetchall()
        out={"query":q,"results":[dict(x) for x in rows],"activation_authority":False}
    elif q=="parameter_recommendations":
        rows=c.execute("""SELECT * FROM candidate_strategies
                          WHERE status='ACCEPTED_AS_CANDIDATE'
                          ORDER BY candidate_score DESC,id DESC""").fetchall()
        out={"query":q,"results":[dict(x) for x in rows],"activation_authority":False}
    elif q=="concept_drift":
        rows=c.execute("""SELECT * FROM concept_drift_alerts
                          WHERE status='POSSIBLE_CONCEPT_DRIFT'
                          ORDER BY ts DESC""").fetchall()
        out={"query":q,"results":[dict(x) for x in rows],"activation_authority":False}
    elif q=="strategy_edge_by_regime":
        c.close()
        return trade_memory_group_analysis("market_regime")
    else:
        out={"error":"unsupported_query","supported":[
            "candidate_strategies","degradation_alerts","parameter_recommendations",
            "concept_drift","strategy_edge_by_regime"
        ]}
    c.close()
    return out



def _vp_event(candidate_id: str, stage: str, status: str,
              validation_id: Optional[str]=None, details: Optional[Dict[str,Any]]=None) -> None:
    c=conn();c.execute("""INSERT INTO validation_events(ts,candidate_id,validation_id,stage,status,details_json)
                         VALUES(?,?,?,?,?,?)""",
                      (now_iso(),candidate_id,validation_id,stage,status,_tm_json(details or {},{})))
    c.commit();c.close()
    log.info("VALIDATION_PIPELINE %s candidate=%s validation=%s status=%s",stage,candidate_id,validation_id,status)


def _vp_code_hash() -> str:
    h=hashlib.sha256()
    for name in ("server.py","adaptive_learning.py","validation_pipeline.py"):
        fp=Path(__file__).resolve().parent/name
        if fp.exists():h.update(fp.read_bytes())
    return h.hexdigest()


def _vp_candidate(candidate_id: str) -> Optional[Dict[str,Any]]:
    c=conn();row=c.execute("SELECT * FROM candidate_strategies WHERE candidate_id=?",(candidate_id,)).fetchone();c.close()
    if not row:return None
    out=dict(row)
    for k in ("current_value_json","proposed_value_json","candidate_parameters_json","original_parameters_json","validation_json"):
        try:out[k[:-5]]=json.loads(out.get(k) or "null")
        except Exception:out[k[:-5]]=None
    out["proposed_value"]=out.get("proposed_value")
    return out


def _vp_candidate_spec(row: Dict[str,Any]) -> Dict[str,Any]:
    pv=row.get("proposed_value")
    if pv is None:
        try:pv=json.loads(row.get("proposed_value_json") or "null")
        except Exception:pv=None
    cv=row.get("current_value")
    if cv is None:
        try:cv=json.loads(row.get("current_value_json") or "null")
        except Exception:cv=None
    return {"change_type":row["change_type"],"parameter_name":row["parameter_name"],
            "current_value":cv,"proposed_value":pv}


def _vp_rows(strategy: str) -> List[Dict[str,Any]]:
    if "::" in strategy:
        instrument,base=strategy.split("::",1)
        return _al_trade_rows(base,instrument)
    return _al_trade_rows(strategy,PRIMARY_INSTRUMENT)


def _vp_dataset_version(candidate: Dict[str,Any], rows: List[Dict[str,Any]], split: Dict[str,Any]) -> str:
    raw=(candidate["strategy_id"]+"|"+candidate["parameter_name"]+"|"+vp_dataset_fingerprint(rows)+"|"+
         json.dumps(split.get("periods") or {},sort_keys=True)).encode()
    return "ds_"+hashlib.sha256(raw).hexdigest()[:20]


def _vp_test_reused(candidate: Dict[str,Any], split: Dict[str,Any]) -> Optional[Dict[str,Any]]:
    test_start=(split.get("periods") or {}).get("test",{}).get("start")
    if not test_start:return None
    c=conn();row=c.execute("""SELECT d.dataset_version,d.test_start,d.test_end,r.candidate_id
                               FROM validation_datasets d
                               JOIN candidate_validation_runs r ON r.dataset_version=d.dataset_version
                               JOIN candidate_strategies cs ON cs.candidate_id=r.candidate_id
                               WHERE cs.strategy_id=? AND cs.parameter_name=? AND r.candidate_id<>?
                               ORDER BY d.test_end DESC LIMIT 1""",
                            (candidate["strategy_id"],candidate["parameter_name"],candidate["candidate_id"])).fetchone();c.close()
    if row and row["test_end"] and str(test_start)<=str(row["test_end"]):return dict(row)
    return None


def _vp_seal_dataset(candidate: Dict[str,Any], rows: List[Dict[str,Any]], split: Dict[str,Any]) -> str:
    version=_vp_dataset_version(candidate,rows,split);periods=split["periods"]
    c=conn();c.execute("""INSERT OR IGNORE INTO validation_datasets(
      dataset_version,created_ts,strategy_id,dataset_hash,trade_count,period_start,period_end,
      training_start,training_end,validation_start,validation_end,test_start,test_end,sealed,details_json)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)""",
      (version,now_iso(),candidate["strategy_id"],vp_dataset_fingerprint(rows),len(rows),
       rows[0].get("entry_ts") if rows else None,rows[-1].get("exit_ts") if rows else None,
       periods["train"].get("start"),periods["train"].get("end"),periods["validation"].get("start"),
       periods["validation"].get("end"),periods["test"].get("start"),periods["test"].get("end"),
       _tm_json({"candidate_id":candidate["candidate_id"],"sealed_test":True},{})))
    for part in ("train","validation","test"):
        for i,r in enumerate(split[part]):
            c.execute("""INSERT OR IGNORE INTO validation_dataset_members(dataset_version,trade_id,partition,position)
                         VALUES(?,?,?,?)""",(version,str(r.get("trade_id")),part,i))
    c.commit();c.close();return version


def _vp_registry_upsert(candidate: Dict[str,Any], state_name: str, validation_id: Optional[str]=None,
                        score: Optional[float]=None, dataset_version: Optional[str]=None,
                        reason: Optional[str]=None) -> None:
    c=conn();c.execute("""INSERT INTO candidate_registry(
      candidate_id,strategy_id,candidate_version,current_state,historical_validation_status,
      validation_score,dataset_version,final_reason,latest_validation_id,auto_deploy,created_ts,updated_ts)
      VALUES(?,?,?,?,?,?,?,?,?,0,?,?)
      ON CONFLICT(candidate_id) DO UPDATE SET current_state=excluded.current_state,
      historical_validation_status=COALESCE(excluded.historical_validation_status,candidate_registry.historical_validation_status),
      validation_score=COALESCE(excluded.validation_score,candidate_registry.validation_score),
      dataset_version=COALESCE(excluded.dataset_version,candidate_registry.dataset_version),
      final_reason=COALESCE(excluded.final_reason,candidate_registry.final_reason),
      latest_validation_id=COALESCE(excluded.latest_validation_id,candidate_registry.latest_validation_id),
      auto_deploy=0,updated_ts=excluded.updated_ts""",
      (candidate["candidate_id"],candidate["strategy_id"],candidate["candidate_version"],state_name,
       state_name,score,dataset_version,reason,validation_id,now_iso(),now_iso()))
    c.commit();c.close()


def validate_candidate_advanced(candidate_id: str) -> Dict[str,Any]:
    if not VALIDATION_PIPELINE_ENABLED:return {"enabled":False}
    cand=_vp_candidate(candidate_id)
    if not cand:return {"status":"FAILED","reason":"candidate not found"}
    # Only Adaptive Learning accepted candidates can enter Step 7.
    if cand.get("status")!="ACCEPTED_AS_CANDIDATE":
        _vp_registry_upsert(cand,"FAILED",reason="candidate was not accepted by Adaptive Learning")
        return {"status":"FAILED","reason":"candidate was not accepted by Adaptive Learning"}
    rows=_vp_rows(cand["strategy_id"]);spec=_vp_candidate_spec(cand)
    _vp_registry_upsert(cand,"VALIDATING",reason="historical validation started")
    _vp_event(candidate_id,"VALIDATING","STARTED",None,{"trades":len(rows)})
    split=vp_strict_temporal_split(rows,.60,.20,ADAPTIVE_LEARNING_EMBARGO_MINUTES)
    if split.get("status")!="OK":
        _vp_registry_upsert(cand,"INSUFFICIENT_DATA",reason="strict temporal split unavailable")
        return {"status":"INSUFFICIENT_DATA","reason":"strict temporal split unavailable"}
    reused=_vp_test_reused(cand,split)
    if reused:
        _vp_registry_upsert(cand,"INSUFFICIENT_DATA",reason="unseen test-set protection: prior OOS set already inspected")
        _vp_event(candidate_id,"OUT_OF_SAMPLE","TEST_SET_REUSE_BLOCKED",None,reused)
        return {"status":"INSUFFICIENT_DATA","reason":"TEST_SET_REUSE_BLOCKED","prior_test":reused}
    ds=_vp_seal_dataset(cand,rows,split)
    validation_id="val_"+hashlib.sha256((candidate_id+ds+_vp_code_hash()).encode()).hexdigest()[:20]
    c=conn();existing=c.execute("SELECT * FROM candidate_validation_runs WHERE validation_id=?",(validation_id,)).fetchone();c.close()
    if existing:return {"status":existing["final_status"],"validation_id":validation_id,"reproduced_existing":True}
    cfg={"train_window":VALIDATION_TRAIN_WINDOW,"test_window":VALIDATION_TEST_WINDOW,"step_size":VALIDATION_STEP_SIZE,
         "min_windows":VALIDATION_MIN_WINDOWS,"embargo_minutes":ADAPTIVE_LEARNING_EMBARGO_MINUTES,
         "min_oos_trades":VALIDATION_MIN_OOS_TRADES,"monte_carlo_sims":VALIDATION_MONTE_CARLO_SIMS}
    c=conn();c.execute("""INSERT INTO candidate_validation_runs(
      validation_id,candidate_id,strategy_id,candidate_version,code_version,code_hash,dataset_version,
      started_ts,state,walk_forward_config_json,final_status,final_reason,reproducibility_json,auto_deploy)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
      (validation_id,candidate_id,cand["strategy_id"],cand["candidate_version"],VERSION_TAG,_vp_code_hash(),ds,
       now_iso(),"VALIDATING",_tm_json(cfg,{}),"VALIDATING","in progress",
       _tm_json({"dataset_hash":vp_dataset_fingerprint(rows),"candidate_spec":spec,"config":cfg},{})))
    c.commit();c.close()
    result=vp_run_historical_validation(rows,spec,VALIDATION_TRAIN_WINDOW,VALIDATION_TEST_WINDOW,
                                        VALIDATION_STEP_SIZE,VALIDATION_MIN_WINDOWS,
                                        ADAPTIVE_LEARNING_EMBARGO_MINUTES,VALIDATION_MIN_OOS_TRADES,
                                        VALIDATION_MONTE_CARLO_SIMS,int(ds[-8:],16))
    # Store individual walk-forward windows.
    c=conn()
    for w in (result.get("walk_forward") or {}).get("windows",[]):
        comp=w.get("comparison") or {}
        c.execute("""INSERT OR REPLACE INTO validation_walk_forward_windows(
          validation_id,window_no,train_start,train_end,test_start,test_end,
          production_metrics_json,candidate_metrics_json,comparison_json)
          VALUES(?,?,?,?,?,?,?,?,?)""",
          (validation_id,int(w.get("window",0)),(w.get("train_period") or {}).get("start"),(w.get("train_period") or {}).get("end"),
           (w.get("test_period") or {}).get("start"),(w.get("test_period") or {}).get("end"),
           _tm_json(comp.get("production") or {},{}),_tm_json(comp.get("candidate") or {},{}),_tm_json(comp,{})))
    hist_status=result.get("status")
    if hist_status=="PAPER_TRADING_REQUIRED":registry_state="PAPER_TRADING"
    elif hist_status=="INSUFFICIENT_DATA":registry_state="INSUFFICIENT_DATA"
    elif hist_status=="PROMISING":registry_state="PROMISING"
    else:registry_state="FAILED"
    periods=result.get("split_periods") or {}
    c.execute("""UPDATE candidate_validation_runs SET completed_ts=?,state=?,training_period_json=?,
      validation_period_json=?,test_period_json=?,backtest_results_json=?,oos_results_json=?,
      walk_forward_results_json=?,stress_results_json=?,sensitivity_results_json=?,regime_results_json=?,
      monte_carlo_results_json=?,validation_score=?,final_status=?,final_reason=? WHERE validation_id=?""",
      (now_iso(),registry_state,_tm_json(periods.get("train") or {},{}),_tm_json(periods.get("validation") or {},{}),
       _tm_json(periods.get("test") or {},{}),_tm_json(result.get("backtest") or {},{}),
       _tm_json(result.get("out_of_sample") or {},{}),_tm_json(result.get("walk_forward") or {},{}),
       _tm_json(result.get("stress_test") or {},{}),_tm_json(result.get("parameter_sensitivity") or {},{}),
       _tm_json(result.get("regime_analysis") or {},{}),_tm_json(result.get("monte_carlo") or {},{}),
       result.get("validation_score"),registry_state,"; ".join(result.get("reasons") or []),validation_id))
    if registry_state=="PAPER_TRADING":
        c.execute("""INSERT INTO candidate_registry(candidate_id,strategy_id,candidate_version,current_state,
          historical_validation_status,validation_score,dataset_version,paper_started_ts,latest_validation_id,
          final_reason,auto_deploy,created_ts,updated_ts)
          VALUES(?,?,?,?,?,?,?,?,?,?,0,?,?)
          ON CONFLICT(candidate_id) DO UPDATE SET current_state='PAPER_TRADING',historical_validation_status='PASSED',
          validation_score=excluded.validation_score,dataset_version=excluded.dataset_version,
          paper_started_ts=COALESCE(candidate_registry.paper_started_ts,excluded.paper_started_ts),
          latest_validation_id=excluded.latest_validation_id,final_reason=excluded.final_reason,auto_deploy=0,
          updated_ts=excluded.updated_ts""",
          (candidate_id,cand["strategy_id"],cand["candidate_version"],"PAPER_TRADING","PASSED",result.get("validation_score"),
           ds,now_iso(),validation_id,"mandatory real-time paper trading",now_iso(),now_iso()))
    else:
        c.execute("""INSERT INTO candidate_registry(candidate_id,strategy_id,candidate_version,current_state,
          historical_validation_status,validation_score,dataset_version,latest_validation_id,final_reason,
          auto_deploy,created_ts,updated_ts) VALUES(?,?,?,?,?,?,?,?,?,0,?,?)
          ON CONFLICT(candidate_id) DO UPDATE SET current_state=excluded.current_state,
          historical_validation_status=excluded.historical_validation_status,validation_score=excluded.validation_score,
          dataset_version=excluded.dataset_version,latest_validation_id=excluded.latest_validation_id,
          final_reason=excluded.final_reason,auto_deploy=0,updated_ts=excluded.updated_ts""",
          (candidate_id,cand["strategy_id"],cand["candidate_version"],registry_state,registry_state,
           result.get("validation_score"),ds,validation_id,"; ".join(result.get("reasons") or []),now_iso(),now_iso()))
    c.commit();c.close()
    _vp_event(candidate_id,"HISTORICAL_VALIDATION",registry_state,validation_id,
              {"score":result.get("validation_score"),"reasons":result.get("reasons"),"dataset_version":ds})
    return {"status":registry_state,"validation_id":validation_id,"dataset_version":ds,"result":result,"auto_deploy":False}


def _vp_live_entry_row(r: Dict[str,Any],conf: Dict[str,Any],director: Dict[str,Any]) -> Dict[str,Any]:
    rg=r.get("market_regime") if isinstance(r.get("market_regime"),dict) else {}
    return {"strategy_confidence_entry":conf.get("probability"),"director_confidence_entry":director.get("confidence"),
            "market_regime_entry":rg.get("market_regime"),"volatility_state_entry":rg.get("volatility_state")}


def record_candidate_paper_signals(signal_id: int,r: Dict[str,Any],conf: Dict[str,Any],director: Dict[str,Any],
                                   risk_shadow: Dict[str,Any],observed_price: float) -> int:
    if r.get("signal") not in ("BUY","SELL") or not observed_price:return 0
    strategy=setup_variant(r);c=conn();regs=[dict(x) for x in c.execute("""SELECT cr.*,cs.change_type,cs.parameter_name,
      cs.current_value_json,cs.proposed_value_json FROM candidate_registry cr JOIN candidate_strategies cs
      ON cs.candidate_id=cr.candidate_id WHERE cr.strategy_id=? AND cr.current_state='PAPER_TRADING'""",(strategy,)).fetchall()];c.close()
    if not regs:return 0
    live=_vp_live_entry_row(r,conf,director);made=0
    for reg in regs:
        try:pv=json.loads(reg.get("proposed_value_json") or "null")
        except Exception:pv=None
        spec={"change_type":reg["change_type"],"parameter_name":reg["parameter_name"],"proposed_value":pv}
        if not vp_candidate_passes(spec,live):continue
        entry=float(r["entry"]);stop=float(r["stop"]);target=float(r.get("managed_target",r["target"]));risk=abs(entry-stop)
        if risk<=0:continue
        deviation=abs(float(observed_price)-entry)/risk
        executable=int(deviation<=VALIDATION_PAPER_MAX_ENTRY_DEVIATION_R)
        candle=_parse_iso(r.get("candle_ts"));latency=(datetime.now(timezone.utc)-candle).total_seconds() if candle else None
        rg=r.get("market_regime") if isinstance(r.get("market_regime"),dict) else {}
        c=conn();before=c.total_changes;c.execute("""INSERT OR IGNORE INTO candidate_paper_trades(
          candidate_id,signal_id,created_ts,candle_ts,instrument,direction,entry,stop,target,risk,observed_entry,
          entry_deviation_r,latency_seconds,executable,status,market_regime,volatility_state,strategy_confidence,
          director_confidence,risk_multiplier) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'PENDING',?,?,?,?,?)""",
          (reg["candidate_id"],signal_id,now_iso(),r.get("candle_ts"),r["instrument"],r["signal"],entry,stop,target,risk,
           float(observed_price),deviation,latency,executable,rg.get("market_regime"),rg.get("volatility_state"),
           conf.get("probability"),director.get("confidence"),risk_shadow.get("risk_multiplier")))
        made+=int(c.total_changes>before);c.commit();c.close()
    return made


def resolve_candidate_paper_trades(inst: str,m1: List[Dict[str,Any]]) -> int:
    c=conn();rows=c.execute("""SELECT * FROM candidate_paper_trades WHERE status='PENDING' AND instrument=?
                              ORDER BY id""",(inst,)).fetchall();n=0
    for row in rows:
        if not int(row["executable"]):
            c.execute("UPDATE candidate_paper_trades SET status='MISSED',resolved_ts=?,note=? WHERE id=?",
                      (now_iso(),"entry deviation exceeded paper execution tolerance",row["id"]));n+=1;continue
        fake={"candle_ts":row["candle_ts"],"created_ts":row["created_ts"],"direction":row["direction"],
              "entry":row["entry"],"stop":row["stop"],"target":row["target"]}
        out=resolve_one(fake,m1)
        if out:
            rr=abs(float(row["target"])-float(row["entry"]))/float(row["risk"]) if int(out["label"])==1 else -1.0
            sim_exit=float(row["target"]) if int(out["label"])==1 else float(row["stop"])
            c.execute("""UPDATE candidate_paper_trades SET status=?,label=?,resolved_ts=?,bars_to_resolution=?,
              mfe_r=?,mae_r=?,realized_r=?,simulated_exit=?,note=? WHERE id=?""",
              (out["status"],out["label"],now_iso(),out["bars"],out["mfe_r"],out["mae_r"],rr,sim_exit,out["note"],row["id"]));n+=1
    c.commit();c.close()
    if n:evaluate_all_candidate_paper_states()
    return n


def _vp_paper_metrics(candidate_id: str) -> Dict[str,Any]:
    c=conn();rows=[dict(x) for x in c.execute("""SELECT * FROM candidate_paper_trades WHERE candidate_id=?
                                                 ORDER BY created_ts,id""",(candidate_id,)).fetchall()];c.close()
    resolved=[r for r in rows if r.get("status") in ("DONE","WIN","LOSS") and r.get("realized_r") is not None]
    vals=[float(r["realized_r"]) for r in resolved]
    wins=[x for x in vals if x>0];loss=[x for x in vals if x<0];gp=sum(wins);gl=abs(sum(loss));pf=gp/gl if gl else (999 if gp else None)
    curve=peak=dd=0
    for x in vals:curve+=x;peak=max(peak,curve);dd=max(dd,peak-curve)
    dates=[_parse_iso(r.get("created_ts")) for r in rows if _parse_iso(r.get("created_ts"))]
    days=(max(dates)-min(dates)).total_seconds()/86400 if len(dates)>=2 else 0
    regimes={r.get("market_regime") for r in resolved if r.get("market_regime")}
    missed=sum(1 for r in rows if r.get("status")=="MISSED")
    recent=vals[-10:] if len(vals)>=10 else []
    prior=vals[:-10] if len(vals)>=20 else []
    recent_exp=sum(recent)/len(recent) if recent else None
    prior_exp=sum(prior)/len(prior) if prior else None
    degradation=bool(prior_exp is not None and prior_exp>0 and recent_exp is not None and recent_exp<0)
    observed=[float(r["observed_entry"]) for r in rows if r.get("observed_entry") is not None]
    exits=[float(r["simulated_exit"]) for r in resolved if r.get("simulated_exit") is not None]
    return {"trades":len(resolved),"signals":len(rows),"missed":missed,"missed_rate":missed/len(rows) if rows else 0,
            "days":days,"regimes":len(regimes),"win_rate":len(wins)/len(vals) if vals else None,
            "expectancy_r":sum(vals)/len(vals) if vals else None,"profit_factor":pf,"max_drawdown_r":dd,
            "recent_expectancy_r":recent_exp,"prior_expectancy_r":prior_exp,"paper_degradation":degradation,
            "avg_observed_entry":sum(observed)/len(observed) if observed else None,
            "avg_simulated_exit":sum(exits)/len(exits) if exits else None,
            "avg_entry_deviation_r":sum(float(r.get("entry_deviation_r") or 0) for r in rows)/len(rows) if rows else None,
            "avg_latency_seconds":sum(float(r.get("latency_seconds") or 0) for r in rows)/len(rows) if rows else None}


def _vp_production_paper_benchmark(strategy: str,paper_start: str) -> Dict[str,Any]:
    c=conn();rows=[dict(x) for x in c.execute("""SELECT ls.label,s.rr,s.ts FROM learning_samples ls
      JOIN signals s ON s.id=ls.signal_id WHERE ls.label IN (0,1) AND s.setup_variant=? AND s.ts>=?
      ORDER BY s.id""",(strategy,paper_start)).fetchall()];c.close()
    vals=[float(r.get("rr") or MIN_RR) if int(r["label"])==1 else -1.0 for r in rows]
    wins=[x for x in vals if x>0];loss=[x for x in vals if x<0];gp=sum(wins);gl=abs(sum(loss));pf=gp/gl if gl else (999 if gp else None)
    curve=peak=dd=0
    for x in vals:curve+=x;peak=max(peak,curve);dd=max(dd,peak-curve)
    return {"trades":len(vals),"win_rate":len(wins)/len(vals) if vals else None,"expectancy_r":sum(vals)/len(vals) if vals else None,
            "profit_factor":pf,"max_drawdown_r":dd}


def _vp_peer_paper_comparison(candidate_id: str, strategy: str) -> List[Dict[str,Any]]:
    c=conn();ids=[x["candidate_id"] for x in c.execute("""SELECT candidate_id FROM candidate_registry
      WHERE strategy_id=? AND candidate_id<>? AND current_state IN ('PAPER_TRADING','READY_FOR_REVIEW','CANDIDATE_NEEDS_REEVALUATION')""",
      (strategy,candidate_id)).fetchall()];c.close()
    return [{"candidate_id":cid,"metrics":_vp_paper_metrics(cid)} for cid in ids]


def evaluate_candidate_paper_state(candidate_id: str) -> Dict[str,Any]:
    c=conn();reg=c.execute("SELECT * FROM candidate_registry WHERE candidate_id=?",(candidate_id,)).fetchone();c.close()
    if not reg:return {"status":"FAILED","reason":"candidate not in registry"}
    reg=dict(reg);paper=_vp_paper_metrics(candidate_id)
    benchmark=_vp_production_paper_benchmark(reg["strategy_id"],reg.get("paper_started_ts") or now_iso())
    c=conn();vr=c.execute("SELECT * FROM candidate_validation_runs WHERE validation_id=?",(reg.get("latest_validation_id"),)).fetchone();c.close()
    expected={}
    if vr:
        try:expected=json.loads(vr["oos_results_json"] or "{}").get("comparison",{}).get("candidate",{})
        except Exception:expected={}
    peers=_vp_peer_paper_comparison(candidate_id,reg["strategy_id"])
    min_ok=paper["trades"]>=VALIDATION_PAPER_MIN_TRADES and paper["days"]>=VALIDATION_PAPER_MIN_DAYS and paper["regimes"]>=VALIDATION_PAPER_MIN_REGIMES
    if not min_ok:
        state_name="PAPER_TRADING";div="INSUFFICIENT_PAPER_DATA";reason="mandatory paper minimum not reached"
    else:
        exp_expected=expected.get("realized_r_expectancy")
        exp_paper=paper.get("expectancy_r")
        divergence=False
        if exp_expected is not None and exp_paper is not None:
            scale=max(abs(float(exp_expected)),0.10);divergence=abs(float(exp_paper)-float(exp_expected))/scale>VALIDATION_BACKTEST_LIVE_EXPECTANCY_TOL
        if paper.get("missed_rate",0)>.20:divergence=True
        severe=(paper.get("expectancy_r") is not None and paper["expectancy_r"]<0 and (paper.get("profit_factor") or 0)<1.0)
        if severe:
            state_name="CANDIDATE_REJECTED_AFTER_PAPER";div="CANDIDATE_REJECTED_AFTER_PAPER";reason="paper expectancy/PF lost edge"
        elif paper.get("paper_degradation"):
            state_name="CANDIDATE_NEEDS_REEVALUATION";div="PAPER_DEGRADATION";reason="recent paper expectancy turned negative after earlier positive paper behavior"
        elif divergence:
            state_name="CANDIDATE_NEEDS_REEVALUATION";div="BACKTEST_LIVE_DIVERGENCE";reason="paper behavior diverges materially from historical expectations"
        else:
            state_name="READY_FOR_REVIEW";div="CONSISTENT";reason="historical validation and mandatory paper evidence passed; manual review required"
    c=conn();c.execute("""UPDATE candidate_registry SET current_state=?,paper_updated_ts=?,paper_trade_count=?,paper_regime_count=?,
      paper_days=?,divergence_status=?,final_reason=?,auto_deploy=0,updated_ts=? WHERE candidate_id=?""",
      (state_name,now_iso(),paper["trades"],paper["regimes"],paper["days"],div,reason,now_iso(),candidate_id))
    if vr:c.execute("UPDATE candidate_validation_runs SET paper_results_json=?,final_status=?,final_reason=?,auto_deploy=0 WHERE validation_id=?",
                    (_tm_json({"candidate":paper,"production_benchmark":benchmark,"peer_candidates":peers,"expected_backtest":expected,"divergence":div},{}),state_name,reason,reg.get("latest_validation_id")))
    c.commit();c.close();_vp_event(candidate_id,"PAPER_EVALUATION",state_name,reg.get("latest_validation_id"),{"paper":paper,"benchmark":benchmark,"divergence":div})
    return {"status":state_name,"divergence":div,"reason":reason,"paper":paper,"production_benchmark":benchmark,
            "peer_candidates":peers,"expected_backtest":expected,"auto_deploy":False}


def evaluate_all_candidate_paper_states() -> Dict[str,Any]:
    c=conn();ids=[x["candidate_id"] for x in c.execute("SELECT candidate_id FROM candidate_registry WHERE current_state='PAPER_TRADING'").fetchall()];c.close()
    return {"results":[evaluate_candidate_paper_state(x) for x in ids],"auto_deploy":False}


def candidate_registry_snapshot() -> List[Dict[str,Any]]:
    c=conn();rows=c.execute("SELECT * FROM candidate_registry ORDER BY created_ts,id" if False else "SELECT * FROM candidate_registry ORDER BY created_ts,candidate_id").fetchall();c.close()
    return [dict(x) for x in rows]

def forward_observation_snapshot(r: Dict[str, Any], conf: Dict[str, Any]) -> Dict[str, Any]:
    """Observational-only snapshot for forward attribution and filter-stacking audits.

    This function has no execution authority. It records every relevant gate independently
    so later analysis does not depend on the order in which execution gates return.
    """
    f=r.get("features") or {}
    safety=r.get("safety_checks") or {}
    checks=r.get("filters") or {}
    flags=forward_entry_pattern_flags(f)
    rr_raw=float(f.get("rr_raw",r.get("rr_raw",0)) or 0)
    actual_rr=float(r.get("rr",0) or 0)
    room_raw=f.get("room_to_barrier_r",r.get("room_to_barrier_r"))
    room=None if room_raw is None else float(room_raw)
    extension=float(f.get("extension_atr",0) or 0)
    buy_score=float(r.get("buy_score",f.get("buy_score",0)) or 0)
    sell_score=float(r.get("sell_score",f.get("sell_score",0)) or 0)
    edge=float(r.get("direction_edge",f.get("direction_edge",abs(buy_score-sell_score))) or 0)
    score_pass=max(buy_score,sell_score)>=DIRECTION_MIN_SCORE
    edge_pass=edge>=DIRECTION_MIN_EDGE
    current_rr_pass=bool(actual_rr>=MIN_RR-1e-9 and rr_raw>=MIN_ENTRY_RR)
    prior_rr_pass=bool(actual_rr>=MIN_RR-1e-9 and rr_raw>=MIN_RR)
    symbol=InstrumentRegistry.normalize_symbol(r.get("instrument") or PRIMARY_INSTRUMENT)
    profile=instrument_profile(symbol)
    vetoes={
        "minimum_rr": not bool(safety.get("minimum_rr",current_rr_pass)),
        "barrier_room_ok": not bool(safety.get("barrier_room_ok",checks.get("barrier_room_ok",True))),
        "low_room_low_rr": bool(flags["low_room_low_rr"]),
        "low_room_extended": bool(flags["low_room_extended"]),
    }
    effective_vetoes={
        "minimum_rr":vetoes["minimum_rr"],
        "barrier_room_ok":vetoes["barrier_room_ok"],
        "low_room_low_rr":bool(vetoes["low_room_low_rr"] and profile.has_veto("LOW_ROOM_LOW_RR")),
        "low_room_extended":bool(vetoes["low_room_extended"] and profile.has_veto("LOW_ROOM_EXTENDED")),
    }
    experiment_policy=forward_policy(symbol) if _forward_experiment_active(symbol) else forward_policy("")
    experiment_eval=forward_experiment_gate(r)
    if experiment_policy.get("bypass_low_room_vetoes"):
        effective_vetoes["low_room_low_rr"]=False
        effective_vetoes["low_room_extended"]=False
    return {
        "schema":"FORWARD_ATTRIBUTION_V1",
        "observational_only":True,
        "instrument":symbol,
        "score_pre_filters":float(r.get("score",0) or 0),
        "buy_score":buy_score,"sell_score":sell_score,"direction_edge":edge,
        "direction_score_pass":bool(score_pass),"direction_edge_pass":bool(edge_pass),
        "rr_raw":rr_raw,"actual_rr":actual_rr,"room_to_barrier_r":room,
        "extension_atr":extension,"barrier_class":r.get("barrier_class"),
        "min_entry_rr_current":MIN_ENTRY_RR,"min_entry_rr_prior_reference":MIN_RR,
        "passes_current_rr_gate":current_rr_pass,"passes_prior_rr_reference":prior_rr_pass,
        "admitted_only_by_min_entry_rr_relaxation":bool(current_rr_pass and not prior_rr_pass),
        "vetoes":vetoes,
        "effective_vetoes":effective_vetoes,
        "instrument_specific_vetoes":sorted(profile.specific_vetoes),
        "paper_forward_filters_active":paper_forward_filters_active(symbol),
        "forward_experiment_active":bool(experiment_eval.get("active")),
        "forward_experiment_policy":experiment_policy,
        "forward_experiment":experiment_eval,
        "legacy_v331_buy_score":f.get("legacy_v331_buy_score"),
        "legacy_v331_sell_score":f.get("legacy_v331_sell_score"),
        "legacy_v331_directional_score":f.get("legacy_v331_directional_score"),
        "legacy_v331_chosen_direction":f.get("legacy_v331_chosen_direction"),
        "confidence":conf.get("probability"),"required_confidence":conf.get("required_confidence"),
        "confidence_samples":conf.get("samples"),
    }


def execution_decision(r: Dict[str, Any], conf: Dict[str, Any]) -> Dict[str, Any]:
    if r["signal"] == "WAIT":
        return {"execute": False, "reason": "WAIT: no hay señal direccional"}
    if r.get("blocked"):
        failed = [k for k,v in r.get("safety_checks", {}).items() if not v]
        return {"execute": False, "reason": "Safety veto: " + ", ".join(failed)}
    q = quality_entry_gate(r, conf)
    if not q["ok"]:
        return {"execute": False, "reason": "Quality veto: " + q["reason"]}

    experimental_gate=forward_experiment_gate(r)
    if not experimental_gate["ok"]:
        return {"execute":False,"reason":"Experimental forward veto: "+str(experimental_gate.get("reason")),
                "forward_experiment":experimental_gate}

    research_gate=evaluate_active_research_rules(r)
    if not research_gate["ok"]:
        keys="; ".join(f"{x['source']}:{x['rule_key']}" for x in research_gate.get("vetoes",[])[:5])
        return {"execute":False,"reason":f"Research veto(s): {keys}"}

    strategy_gate=strategy_execution_gate(r)
    if not strategy_gate["ok"]:
        return {"execute":False,"reason":"Strategy health veto: "+strategy_gate["reason"]}

    rg = reentry_guard(r)
    if not rg["ok"]:
        return {"execute": False, "reason": "Re-entry veto: " + rg["reason"]}
    p = float(conf.get("probability") or 0)
    required = float(conf.get("required_confidence") or EXECUTION_MIN_CONFIDENCE)
    executed_evidence = int(conf.get("samples") or 0)
    if executed_evidence < CONFIDENCE_MIN_SAMPLES:
        return {
            "execute": True,
            "reason":(
                "Adaptive OBSERVE_ONLY: "
                f"executed_evidence={executed_evidence}/{CONFIDENCE_MIN_SAMPLES}; "
                f"stored_confidence={p:.1%}; RR={float(r.get('rr_raw',0)):.2f}"
            ),
        }
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
    safety_ok=bool(r.get("signal") in ("BUY","SELL") and not r.get("blocked",True))
    try:
        quality_ok=bool(safety_ok and quality_entry_gate(r,conf).get("ok"))
    except Exception:
        quality_ok=False
    # Legacy hard_filters_ok now means all deterministic pre-confidence gates,
    # not merely the low-level price/risk safety checks.
    hard_ok=bool(safety_ok and quality_ok)
    try:
        forward_audit=json.dumps(forward_observation_snapshot(r,conf),separators=(",",":"),sort_keys=True)
    except Exception as e:
        forward_audit=json.dumps({"schema":"FORWARD_ATTRIBUTION_V1","observational_only":True,"error":str(e)},separators=(",",":"))
    c.execute("""
        INSERT INTO decision_log(ts,candle_ts,instrument,signal,setup_variant,quality_score,dynamic_confidence,
          confidence_source,confidence_samples,required_confidence,recent_win_rate,performance_penalty,
          hard_filters_ok,safety_filters_ok,quality_filters_ok,auto_trade,executed,reason,forward_audit_json)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        now_iso(), r.get("candle_ts"), r["instrument"], r["signal"], conf.get("variant"),
        r.get("score"), conf.get("probability"), conf.get("source"), conf.get("samples"),
        conf.get("required_confidence"), conf.get("recent_win_rate"), conf.get("performance_penalty"),
        int(hard_ok),int(safety_ok),int(quality_ok),int(AUTO),int(executed),reason,forward_audit
    ))
    c.commit(); c.close()


def _shadow_model_acceptance(metrics: Dict[str,Any]) -> Dict[str,Any]:
    """Conservative, non-optimized governance gate for shadow ML artifacts.

    The model must show discrimination better than random and must not underperform
    the simple majority-class accuracy baseline on the same walk-forward folds.
    This gate is intentionally fixed; it is not tuned to improve replay results.
    """
    try:
        auc=float(metrics.get("roc_auc"))
        acc=float(metrics.get("accuracy"))
        baseline=float(metrics.get("baseline_accuracy"))
    except Exception:
        return {"accepted":False,"reason":"missing_validation_metrics"}
    accepted=bool(math.isfinite(auc) and math.isfinite(acc) and math.isfinite(baseline) and auc>0.5 and acc>=baseline)
    reasons=[]
    if not math.isfinite(auc) or auc<=0.5: reasons.append("roc_auc_not_above_random")
    if not math.isfinite(acc) or not math.isfinite(baseline) or acc<baseline: reasons.append("accuracy_below_majority_baseline")
    return {"accepted":accepted,"reason":"accepted" if accepted else ";".join(reasons),
            "roc_auc":auc,"accuracy":acc,"baseline_accuracy":baseline}


def shadow_model_path(instrument: Optional[str]=None) -> str:
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    # Preserve the exact historical artifact path for EUR/USD/primary baseline.
    if instrument=="EUR_USD":
        return MODEL_PATH
    base=Path(MODEL_PATH)
    suffix=base.suffix or ".joblib"
    return str(base.with_name(f"{base.stem}.{instrument}{suffix}"))

def shadow_model_governance_status(instrument: Optional[str]=None) -> Dict[str,Any]:
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    model_path=shadow_model_path(instrument)
    if not Path(model_path).exists():
        return {"ready":False,"reason":"model_artifact_missing","instrument":instrument,"model_path":model_path}
    try:
        artifact=joblib.load(model_path)
        samples=int(artifact.get("samples") or 0) if isinstance(artifact,dict) else 0
        artifact_instrument=str(artifact.get("instrument") or instrument) if isinstance(artifact,dict) else instrument
        if InstrumentRegistry.normalize_symbol(artifact_instrument)!=instrument:
            return {"ready":False,"reason":"model_instrument_mismatch","instrument":instrument,"model_path":model_path}
        c=conn()
        row=c.execute("""SELECT id,samples,baseline_accuracy,accuracy,roc_auc,log_loss,accepted,trained_ts,instrument
                         FROM model_runs WHERE instrument=? AND samples=? ORDER BY id DESC LIMIT 1""",
                      (instrument,samples)).fetchone() if samples else None
        c.close()
        if not row:
            return {"ready":False,"reason":"validation_record_missing","samples":samples,"instrument":instrument,"model_path":model_path}
        gate=_shadow_model_acceptance(dict(row))
        return {"ready":bool(gate["accepted"]),"reason":gate["reason"],"samples":samples,"model_run_id":row["id"],
                "stored_accepted":bool(row["accepted"]),"validation":gate,"instrument":instrument,"model_path":model_path}
    except Exception as e:
        return {"ready":False,"reason":"model_governance_error","error":str(e),"instrument":instrument,"model_path":model_path}

def load_shadow_probability(features: Dict[str, Any], instrument: Optional[str]=None) -> Optional[float]:
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    model_path=shadow_model_path(instrument)
    if not ML_SHADOW or not Path(model_path).exists():
        return None
    governance=shadow_model_governance_status(instrument)
    if not governance.get("ready"):
        log.debug("shadow model disabled by validation governance instrument=%s: %s",instrument,governance.get("reason"))
        return None
    try:
        artifact=joblib.load(model_path)
        if isinstance(artifact, dict):
            model=artifact.get("model");feature_names=artifact.get("features") or FEATURE_COLUMNS
        else:
            model=artifact;feature_names=FEATURE_COLUMNS
        if model is None or not hasattr(model,"predict_proba"):
            raise TypeError("shadow model artifact does not contain a predict_proba-capable model")
        vector=[features.get(name,0.0) for name in feature_names]
        return float(model.predict_proba([vector])[0][1])
    except Exception as e:
        log.warning("shadow model prediction failed instrument=%s: %s",instrument,e)
        return None


async def haspos(client: httpx.AsyncClient, inst: str) -> bool:
    d = await req(client, "GET", "/v3/accounts/{account}/openPositions")
    return any(x.get("instrument") == inst for x in d.get("positions", []))



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

def _protection_price_tolerance(instrument: str) -> float:
    meta=instrument_metadata(instrument)
    price_quantum=10.0**(-int(meta.display_precision))
    return max(price_quantum*0.51,pip_size(instrument)*1e-6,1e-12)


def post_fill_protection_geometry(instrument: str, side: str, planned_entry: float,
                                  planned_stop: float, planned_target: float,
                                  fill_price: float) -> Dict[str,Any]:
    """Re-anchor planned risk/reward distances to the broker-confirmed fill.

    This is execution geometry only. It does not alter strategy, sizing or risk
    authority. Prices are normalized through the instrument registry.
    """
    instrument=InstrumentRegistry.normalize_symbol(instrument)
    side=str(side or "").upper()
    pe=float(planned_entry); ps=float(planned_stop); pt=float(planned_target); fill=float(fill_price)
    risk_distance=abs(pe-ps); reward_distance=abs(pt-pe)
    if side not in ("BUY","SELL"):
        raise ValueError("INVALID_SIDE")
    if not all(math.isfinite(x) for x in (pe,ps,pt,fill,risk_distance,reward_distance)):
        raise ValueError("NON_FINITE_PROTECTION_GEOMETRY")
    if risk_distance<=0 or reward_distance<=0:
        raise ValueError("INVALID_PROTECTION_DISTANCE")
    raw_stop=fill-risk_distance if side=="BUY" else fill+risk_distance
    raw_target=fill+reward_distance if side=="BUY" else fill-reward_distance
    stop=float(format_instrument_price(instrument,raw_stop))
    target=float(format_instrument_price(instrument,raw_target))
    if side=="BUY" and not (stop<fill<target):
        raise ValueError("ROUNDED_BUY_PROTECTION_ORIENTATION_INVALID")
    if side=="SELL" and not (target<fill<stop):
        raise ValueError("ROUNDED_SELL_PROTECTION_ORIENTATION_INVALID")
    return {
        "instrument":instrument,"side":side,"planned_entry":pe,"planned_stop":ps,"planned_target":pt,
        "fill_price":fill,"planned_risk_distance":risk_distance,"planned_reward_distance":reward_distance,
        "applied_stop":stop,"applied_target":target,
        "risk_pips":abs(fill-stop)/pip_size(instrument),
        "reward_pips":abs(target-fill)/pip_size(instrument),
        "rr":abs(target-fill)/max(abs(fill-stop),1e-18),
    }


async def replace_trade_protection(client: httpx.AsyncClient, trade_id: str, instrument: str,
                                   stop_price: float, target_price: float) -> Dict[str,Any]:
    instrument=InstrumentRegistry.normalize_symbol(instrument)
    if not trade_id:
        raise ValueError("MISSING_TRADE_ID")
    body={
        "stopLoss":{"price":format_instrument_price(instrument,stop_price),"timeInForce":"GTC"},
        "takeProfit":{"price":format_instrument_price(instrument,target_price),"timeInForce":"GTC"},
    }
    # One atomic protective-order replacement. req() passes writes to Recovery
    # Manager with allow_retry=False; never convert this to params or split writes.
    return await req(client,"PUT",f"/v3/accounts/{{account}}/trades/{trade_id}/orders",body=body)


async def verify_trade_protection(client, trade_id: str, instrument: Optional[str]=None,
                                  expected_stop: Optional[float]=None,
                                  expected_target: Optional[float]=None):
    if not trade_id:
        return {"status":"PROTECTION_ERROR","sl_ok":False,"tp_ok":False,"stop_match":False,"target_match":False,
                "detail":"No trade ID returned","expected_stop":expected_stop,"expected_target":expected_target}
    try:
        d=await req(client,"GET",f"/v3/accounts/{{account}}/trades/{trade_id}")
        tr=d.get("trade",{}); sl_order=tr.get("stopLossOrder") or {}; tp_order=tr.get("takeProfitOrder") or {}
        broker_stop=_risk_float(sl_order.get("price")); broker_target=_risk_float(tp_order.get("price"))
        sl=bool(sl_order); tp=bool(tp_order)
        if instrument is None or expected_stop is None or expected_target is None:
            return {"status":"PROTECTION_PRESENT_UNVERIFIED" if sl and tp else "PROTECTION_ERROR",
                    "sl_ok":sl,"tp_ok":tp,"stop_match":False,"target_match":False,
                    "expected_stop":expected_stop,"broker_stop":broker_stop,
                    "expected_target":expected_target,"broker_target":broker_target,
                    "detail":"expected instrument/stop/target required for exact protection verification"}
        instrument=InstrumentRegistry.normalize_symbol(instrument)
        tol=_protection_price_tolerance(instrument)
        stop_match=bool(sl and broker_stop is not None and abs(float(broker_stop)-float(expected_stop))<=tol)
        target_match=bool(tp and broker_target is not None and abs(float(broker_target)-float(expected_target))<=tol)
        ok=bool(sl and tp and stop_match and target_match)
        return {"status":"OK" if ok else "PROTECTION_ERROR","sl_ok":bool(sl and stop_match),
                "tp_ok":bool(tp and target_match),"stop_exists":sl,"target_exists":tp,
                "expected_stop":float(expected_stop),"broker_stop":broker_stop,"stop_match":stop_match,
                "expected_target":float(expected_target),"broker_target":broker_target,"target_match":target_match,
                "tolerance":tol,
                "detail":f"stop_exists={sl}; stop_match={stop_match}; target_exists={tp}; target_match={target_match}"}
    except Exception as e:
        return {"status":"PROTECTION_ERROR","sl_ok":False,"tp_ok":False,"stop_match":False,"target_match":False,
                "expected_stop":expected_stop,"expected_target":expected_target,"detail":str(e)}


def _post_fill_protection_observability(event: str, correlation_id: Optional[str], details: Dict[str,Any],
                                        *, severity: str="HIGH") -> None:
    log.error("%s trade=%s instrument=%s detail=%s",event,details.get("trade_id"),details.get("instrument"),details)
    try:
        if RECOVERY_MANAGER_ENABLED:
            recovery_manager.journal(event,correlation_id,payload=details)
    except Exception:
        pass
    try:
        if OBSERVABILITY_ENABLED:
            observability_manager.alert(f"{event}:{details.get('trade_id') or correlation_id}",severity,
                "Execution Engine",event,event.replace("_"," ").title(),correlation_id=correlation_id,details=details)
    except Exception:
        pass


async def reanchor_post_fill_protection(client: httpx.AsyncClient, r: Dict[str,Any], trade_id: str,
                                        fill_price: float, correlation_id: Optional[str]=None) -> Dict[str,Any]:
    """Replace and broker-verify SL+TP after a confirmed MARKET fill.

    Initial stopLossOnFill/takeProfitOnFill remain in place until OANDA atomically
    replaces both levels. No write retry is performed here or by req().
    """
    inst=InstrumentRegistry.normalize_symbol(r.get("instrument"))
    planned_target=float(r.get("managed_target",r.get("target")))
    base={"instrument":inst,"trade_id":str(trade_id or ""),"planned_entry":float(r.get("entry")),
          "fill_price":float(fill_price),"planned_stop":float(r.get("stop")),"planned_target":planned_target}
    try:
        geometry=post_fill_protection_geometry(inst,r.get("signal"),base["planned_entry"],base["planned_stop"],
                                               planned_target,float(fill_price))
    except Exception as e:
        details={**base,"error":str(e)}
        if RECOVERY_MANAGER_ENABLED:
            recovery_manager.enter_safe_mode(f"Post-fill protection geometry invalid for {inst}: {e}",
                                             correlation_id=correlation_id,severity="CRITICAL")
        _post_fill_protection_observability("POST_FILL_PROTECTION_REANCHOR_FAILED",correlation_id,details,severity="CRITICAL")
        return {"status":"INVALID_GEOMETRY","confirmed":False,"geometry":None,"verification":None,
                "effective_stop":None,"effective_target":None,"error":str(e)}
    details={**base,"applied_stop":geometry["applied_stop"],"applied_target":geometry["applied_target"],
             "slippage_pips":(float(fill_price)-float(r.get("entry")))/pip_size(inst)}
    if r.get("signal")=="SELL":details["slippage_pips"]=-details["slippage_pips"]
    try:
        put_response=await replace_trade_protection(client,trade_id,inst,geometry["applied_stop"],geometry["applied_target"])
    except (httpx.TimeoutException,httpx.TransportError,asyncio.TimeoutError) as e:
        # req()/RecoveryManager already marks uncertain writes safe-mode. Preserve
        # that state; do not retry the PUT. A GET may observe current broker levels.
        if RECOVERY_MANAGER_ENABLED:
            recovery_manager.enter_safe_mode(f"Post-fill protection reanchor outcome unknown for {inst}: {e}",
                                             correlation_id=correlation_id,severity="CRITICAL")
        verification=await verify_trade_protection(client,trade_id,inst,geometry["applied_stop"],geometry["applied_target"])
        event_details={**details,"error":str(e),"verification":verification}
        _post_fill_protection_observability("POST_FILL_PROTECTION_REANCHOR_UNKNOWN",correlation_id,event_details,severity="CRITICAL")
        return {"status":"UNKNOWN","confirmed":False,"geometry":geometry,"verification":verification,
                "effective_stop":verification.get("broker_stop"),"effective_target":verification.get("broker_target"),
                "error":str(e)}
    except Exception as e:
        # Known rejection: original on-fill protections remain. Enter safe mode
        # because planned R could not be restored, then observe actual broker state.
        if RECOVERY_MANAGER_ENABLED:
            recovery_manager.enter_safe_mode(f"Post-fill protection reanchor failed for {inst}: {e}",
                                             correlation_id=correlation_id,severity="CRITICAL")
        verification=await verify_trade_protection(client,trade_id,inst,geometry["applied_stop"],geometry["applied_target"])
        event_details={**details,"error":str(e),"verification":verification}
        _post_fill_protection_observability("POST_FILL_PROTECTION_REANCHOR_FAILED",correlation_id,event_details,severity="CRITICAL")
        return {"status":"FAILED","confirmed":False,"geometry":geometry,"verification":verification,
                "effective_stop":verification.get("broker_stop"),"effective_target":verification.get("broker_target"),
                "error":str(e)}

    verification=await verify_trade_protection(client,trade_id,inst,geometry["applied_stop"],geometry["applied_target"])
    if verification.get("status")!="OK":
        if RECOVERY_MANAGER_ENABLED:
            recovery_manager.enter_safe_mode(f"Post-fill protection verification mismatch for {inst}",
                                             correlation_id=correlation_id,severity="CRITICAL")
        event_details={**details,"put_response":put_response,"verification":verification}
        _post_fill_protection_observability("POST_FILL_PROTECTION_VERIFY_MISMATCH",correlation_id,event_details,severity="CRITICAL")
        return {"status":"VERIFY_MISMATCH","confirmed":False,"geometry":geometry,"verification":verification,
                "effective_stop":verification.get("broker_stop"),"effective_target":verification.get("broker_target")}
    event_details={**details,"verification":verification}
    try:
        if RECOVERY_MANAGER_ENABLED:
            recovery_manager.journal("POST_FILL_PROTECTION_REANCHOR_OK",correlation_id,payload=event_details)
    except Exception:
        pass
    log.info("POST_FILL_PROTECTION_REANCHOR_OK %s trade=%s fill=%s stop=%s target=%s",
             inst,trade_id,fill_price,geometry["applied_stop"],geometry["applied_target"])
    return {"status":"OK","confirmed":True,"geometry":geometry,"verification":verification,
            "effective_stop":verification.get("broker_stop"),"effective_target":verification.get("broker_target"),
            "put_response":put_response}

async def recovery_reconcile_primary(client: httpx.AsyncClient, reason: str="periodic") -> Dict[str,Any]:
    if not RECOVERY_MANAGER_ENABLED:
        return {"enabled":False}
    try:
        result=await recovery_manager.reconnect_and_reconcile(client,max_attempts=3)
        if OBSERVABILITY_ENABLED:
            rec=result.get("reconciliation") or {}
            status=rec.get("status")
            _obs_module("Recovery Manager","OK" if status in ("MATCHED","MINOR_MISMATCH") else "DEGRADED",
                        last_operation=f"reconcile:{reason}",details=rec)
            if status in ("RECONCILIATION_REQUIRED","CRITICAL_MISMATCH"):
                observability_manager.alert("RECOVERY_RECONCILIATION",
                    "CRITICAL" if status=="CRITICAL_MISMATCH" else "HIGH",
                    "Recovery Manager","STATE_RECONCILIATION_REQUIRED",
                    f"Recovery reconciliation returned {status}",details=rec)
            else:
                observability_manager.recover("RECOVERY_RECONCILIATION",
                                              "Recovery reconciliation matched broker state",rec)
        return result
    except Exception as e:
        recovery_manager.enter_safe_mode(f"reconciliation failed: {e}",severity="CRITICAL")
        if OBSERVABILITY_ENABLED:
            _obs_module("Recovery Manager","ERROR",errors=[str(e)])
            observability_manager.alert("RECOVERY_FAILURE","CRITICAL","Recovery Manager","RECOVERY_FAILURE",
                                        "Recovery/reconciliation failed",details={"reason":reason,"error":str(e)})
        return {"connected":False,"error":str(e)}


async def recovery_startup_sequence() -> Dict[str,Any]:
    if not RECOVERY_MANAGER_ENABLED:
        return {"status":"DISABLED"}
    recovery_manager.ensure_schema()
    recovery_manager.set_state("RECOVERING","BOOT",safe_mode=True,new_trades_allowed=False)
    recovery_manager.startup_stage("BOOT")
    recovery_manager.startup_stage("LOAD_PERSISTED_STATE","OK",{"previous":recovery_manager.state()})
    try:
        c=conn();c.execute("SELECT 1").fetchone();c.close()
        recovery_manager.startup_stage("CONNECT_DATABASE","OK")
    except Exception as e:
        recovery_manager.set_state("CRITICAL_FAILURE",f"database unavailable: {e}",safe_mode=True,new_trades_allowed=False)
        recovery_manager.startup_stage("CONNECT_DATABASE","ERROR",{"error":str(e)})
        return {"status":"CRITICAL_FAILURE","stage":"CONNECT_DATABASE","error":str(e)}

    async with httpx.AsyncClient() as client:
        market_ok=False;market_error=None;market_ts=None;market_by_instrument={}
        try:
            for startup_inst in INSTRUMENTS:
                try:
                    m1=await candles(client,startup_inst,"M1",5)
                    ts=m1[-1]["t"] if m1 else None;dt=_parse_iso(ts) if ts else None
                    age=(datetime.now(timezone.utc)-dt).total_seconds() if dt else 999999
                    ok=bool(m1) and (age<=RECOVERY_MARKET_DATA_MAX_AGE_SECONDS or market_is_weekend_closed())
                    market_by_instrument[startup_inst]={"ok":ok,"timestamp":str(ts) if ts else None,"age_seconds":age}
                except Exception as inst_e:
                    market_by_instrument[startup_inst]={"ok":False,"error":str(inst_e)}
            market_ok=bool(market_by_instrument) and all(x.get("ok") for x in market_by_instrument.values())
            primary_state=market_by_instrument.get(PRIMARY_INSTRUMENT) or {}
            market_ts=primary_state.get("timestamp")
            recovery_manager.market_data_update(market_ts,market_ok)
            recovery_manager.startup_stage("CONNECT_MARKET_DATA","OK" if market_ok else "ERROR",
                                           {"primary_timestamp":market_ts,"by_instrument":market_by_instrument})
        except Exception as e:
            market_error=str(e)
            recovery_manager.startup_stage("CONNECT_MARKET_DATA","ERROR",{"error":market_error,"by_instrument":market_by_instrument})

        recovery_manager.startup_stage("CONNECT_BROKER")
        rr=await recovery_reconcile_primary(client,"startup")
        if not rr.get("connected"):
            return {"status":"SAFE_MODE","stage":"CONNECT_BROKER","error":rr.get("error")}
        rec=rr.get("reconciliation") or {}
        recovery_manager.startup_stage("FETCH_BROKER_STATE","OK")
        recovery_manager.startup_stage("RECONCILE",rec.get("status","UNKNOWN"),rec)
        recovery_manager.startup_stage("VERIFY_OPEN_POSITIONS",
            "OK" if rec.get("status") in ("MATCHED","MINOR_MISMATCH") else "ERROR",rec)
        protective_bad=(rec.get("counts") or {}).get("CRITICAL_MISMATCH",0)>0
        recovery_manager.startup_stage("VERIFY_PROTECTIVE_ORDERS","ERROR" if protective_bad else "OK")
        risk_ok=False
        try:
            ctx=await build_broker_risk_context(client)
            risk_ok=not bool(ctx.get("system_abnormal")) and ctx.get("nav") is not None
            recovery_manager.verify_risk(risk_ok,ctx)
        except Exception as e:
            recovery_manager.verify_risk(False,{"error":str(e)})
        recovery_manager.startup_stage("VERIFY_RISK_ENGINE","OK" if risk_ok else "ERROR")
        try:
            dep=deployment_manager.dashboard()
            recovery_manager.startup_stage("VERIFY_DEPLOYMENT_STATE","OK",
                                           {"deployments":len(dep.get("deployments",[]))})
        except Exception as e:
            recovery_manager.startup_stage("VERIFY_DEPLOYMENT_STATE","ERROR",{"error":str(e)})
            recovery_manager.enter_safe_mode("Deployment state verification failed",severity="CRITICAL")
            return {"status":"SAFE_MODE","stage":"VERIFY_DEPLOYMENT_STATE","error":str(e)}
        recovery_manager.startup_stage("VERIFY_MARKET_DATA_FRESHNESS","OK" if market_ok else "ERROR",
                                       {"timestamp":market_ts,"error":market_error})
        if market_ok and risk_ok and rec.get("status") in ("MATCHED","MINOR_MISMATCH") and not recovery_manager.state().get("emergency_stop"):
            recovery_manager.exit_safe_mode("startup recovery sequence passed")
            return {"status":"READY","reconciliation":rec,"market_timestamp":market_ts}
        recovery_manager.enter_safe_mode("Startup recovery incomplete; no new trades",severity="CRITICAL")
        return {"status":"SAFE_MODE","reconciliation":rec,"market_ok":market_ok,"risk_ok":risk_ok}


async def recovery_price_preflight(client: httpx.AsyncClient, r: Dict[str,Any]) -> Dict[str,Any]:
    """
    Deterministic execution sanity check using broker pricing immediately before submission.
    If bid/ask/timestamp/spread look unreliable, no order is sent.
    """
    try:
        d=await req(client,"GET","/v3/accounts/{account}/pricing",params={"instruments":r["instrument"]})
        prices=d.get("prices") or []
        if not prices:
            return {"ok":False,"reason":"NO_BROKER_PRICE"}
        q=prices[0]
        bid=_risk_float(q.get("closeoutBid"))
        ask=_risk_float(q.get("closeoutAsk"))
        if bid is None:
            bids=q.get("bids") or [];bid=_risk_float((bids[0] if bids else {}).get("price"))
        if ask is None:
            asks=q.get("asks") or [];ask=_risk_float((asks[0] if asks else {}).get("price"))
        if bid is None or ask is None or ask<=bid:
            return {"ok":False,"reason":"INVALID_BID_ASK","quote":q}
        qt=_parse_iso(q.get("time"))
        age=(datetime.now(timezone.utc)-qt).total_seconds() if qt else 999999
        spread=(ask-bid)/pip_size(r["instrument"])
        mid=(ask+bid)/2.0
        deviation=abs(mid-float(r["entry"]))/pip_size(r["instrument"])
        market_status=str(q.get("status") or "tradeable").lower()
        bids=q.get("bids") or []; asks=q.get("asks") or []
        bid_liquidity=sum(float(x.get("liquidity") or 0) for x in bids if _risk_float(x.get("liquidity")) is not None)
        ask_liquidity=sum(float(x.get("liquidity") or 0) for x in asks if _risk_float(x.get("liquidity")) is not None)
        available_liquidity=ask_liquidity if r.get("signal")=="BUY" else bid_liquidity
        if available_liquidity<=0: available_liquidity=None
        ok=(age<=RECOVERY_MAX_QUOTE_AGE_SECONDS and spread<=RECOVERY_MAX_SPREAD_PIPS
            and deviation<=RECOVERY_MAX_PRICE_DEVIATION_PIPS
            and market_status not in ("non-tradeable","halted","closed"))
        conversion_factors=q.get("quoteHomeConversionFactors") or {}
        conversion=_risk_float(conversion_factors.get("negativeUnits") if r.get("signal")=="SELL" else conversion_factors.get("positiveUnits"))
        return {"ok":ok,"bid":bid,"ask":ask,"mid":mid,"last_price":mid,"spread_pips":spread,
                "quote_age_seconds":age,"deviation_pips":deviation,"market_status":market_status,
                "quote_time":q.get("time"),"available_liquidity":available_liquidity,
                "bid_liquidity":bid_liquidity or None,"ask_liquidity":ask_liquidity or None,
                "quote_home_conversion":conversion,"quote_home_conversion_factors":conversion_factors,
                "reason":"OK" if ok else "REQUIRE_REVALIDATION"}
    except Exception as e:
        return {"ok":False,"reason":"PRICE_PREFLIGHT_ERROR","error":str(e)}


async def execute_recoverable(client: httpx.AsyncClient, r: Dict[str,Any],
                              correlation_id: Optional[str], decision_id: Optional[str],
                              risk_decision_id: Optional[str]) -> Dict[str,Any]:
    # Hard execution invariants: known-closed FX markets take precedence, then
    # restricted new-entry windows. Open trades are managed independently.
    if market_is_weekend_closed():
        payload={"reason":"MARKET_CLOSED","market_closed":True,"market_data_state":"MARKET_CLOSED"}
        try:
            recovery_manager.journal("REJECT_EXECUTION",correlation_id,strategy_id=setup_variant(r),payload=payload)
        except Exception:
            pass
        return {"skipped":"MARKET_CLOSED",**payload}
    entry_gate=new_entry_time_gate()
    if not entry_gate["allowed"]:
        payload={"reason":entry_gate["reason"],"entry_time_gate":entry_gate}
        try:
            recovery_manager.journal("REJECT_EXECUTION",correlation_id,strategy_id=setup_variant(r),payload=payload)
        except Exception:
            pass
        return {"skipped":entry_gate["reason"],**payload}
    mode=instrument_mode(r["instrument"])
    if mode != "ENABLED":
        return {"skipped":"INSTRUMENT_NOT_EXECUTION_ENABLED","instrument_mode":mode}
    attached_guard=r.get("portfolio_execution_guard")
    if attached_guard is None:
        risk_ctx=r.get("broker_risk_context")
        if not isinstance(risk_ctx,dict):
            return {"skipped":"GLOBAL_PORTFOLIO_RISK_CONTEXT_REQUIRED"}
        attached_guard=portfolio_execution_guard(r["instrument"],risk_ctx)
        r["portfolio_execution_guard"]=attached_guard
    if not attached_guard.get("allow",False):
        return {"skipped":"GLOBAL_PORTFOLIO_RISK_GUARD","portfolio_execution_guard":attached_guard}
    # Any newly PAPER-enabled secondary instrument must be verified against broker
    # metadata before an order can be built. EUR/USD retains its established fallback
    # during transient metadata outages, preserving the frozen baseline behavior.
    if r["instrument"] != PRIMARY_INSTRUMENT and instrument_metadata(r["instrument"]).source != "OANDA":
        try:
            await refresh_instrument_metadata(client,[r["instrument"]],force=True)
        except Exception as e:
            return {"skipped":"INSTRUMENT_METADATA_UNVERIFIED","error":str(e)}
        if instrument_metadata(r["instrument"]).source != "OANDA":
            return {"skipped":"INSTRUMENT_METADATA_UNVERIFIED"}
    if SINGLE and await haspos(client,r["instrument"]):
        return {"skipped":"existing_position"}
    preflight=await recovery_price_preflight(client,r)
    if not preflight.get("ok"):
        recovery_manager.enter_safe_mode(f"Execution price preflight rejected: {preflight.get('reason')}",
                                         correlation_id=correlation_id,severity="CRITICAL")
        recovery_manager.journal("REJECT_EXECUTION",correlation_id,strategy_id=setup_variant(r),
                                 payload={"reason":"PRICE_PREFLIGHT","preflight":preflight})
        return {"skipped":"REQUIRE_REVALIDATION","price_preflight":preflight}
    sizing=instrument_sizing(
        r["instrument"],min(UNITS,int(managed_value("execution.trade_units",UNITS))),
        r["entry"],r["stop"],risk_context=r.get("broker_risk_context"),
        quote_home_conversion=preflight.get("quote_home_conversion"))
    effective_units=float(sizing["effective_units"])
    r["instrument_sizing"]=sizing
    if effective_units <= 0:
        return {"skipped":"INSTRUMENT_SIZE_BELOW_MINIMUM","sizing":sizing}
    if TRADING_ENVIRONMENT=="PRODUCTION" and PRODUCTION_READINESS_ENABLED:
        pst=production_readiness_gate.state();stage=pst.get("production_stage")
        if stage in production_readiness_gate.stage_limits:
            cap=float(production_readiness_gate.effective_stage_limits(stage,production_hard_limits()).get("risk_cap_multiplier") or 0.0)
            effective_units=abs(float(normalize_instrument_units(r["instrument"],effective_units*cap,allow_zero=True)))
            if effective_units <= 0:
                return {"skipped":"PRODUCTION_SIZE_CAP_BELOW_MINIMUM","sizing":sizing}
    smart_intent=None; smart_snapshot=None; smart_shadow=None
    if SMART_EXECUTION_ENABLED:
        try:
            smart_intent=smart_execution_engine.create_intent(
                strategy_id=setup_variant(r),symbol=r["instrument"],side=r["signal"],
                target_quantity=effective_units,maximum_quantity=effective_units,risk_approved_quantity=effective_units,
                expected_price=float(r["entry"]),urgency="NORMAL",
                maximum_slippage_bps=float(managed_value("smart_execution.max_slippage_bps",SMART_EXECUTION_DEFAULT_MAX_SLIPPAGE_BPS)),
                time_limit_seconds=int(managed_value("smart_execution.intent_ttl_seconds",SMART_EXECUTION_INTENT_TTL_SECONDS)),
                signal_time=r.get("candle_ts") or now_iso(),decision_id=str(decision_id) if decision_id is not None else None,
                risk_decision_id=str(risk_decision_id) if risk_decision_id is not None else None,
                risk_approval_valid=True,emergency_stop=bool(recovery_manager.state().get("emergency_stop")),
                policy_version=f"smart_execution_shadow@{VERSION_TAG}"
            )
            rg=r.get("market_regime") if isinstance(r.get("market_regime"),dict) else {}
            smart_snapshot=smart_execution_engine.capture_snapshot(
                smart_intent["execution_intent_id"],bid=preflight.get("bid"),ask=preflight.get("ask"),
                last_price=preflight.get("last_price") or preflight.get("mid"),
                available_liquidity=preflight.get("available_liquidity"),recent_volume=None,
                volatility=rg.get("volatility_state") or "UNKNOWN",market_regime=rg.get("market_regime") or "UNKNOWN",
                timestamp=preflight.get("quote_time") or now_iso(),broker_health="OK",
                market_status=preflight.get("market_status") or "tradeable",
                metadata={"preflight":preflight,"order_book_available":False}
            )
            smart_shadow=smart_execution_engine.recommend(
                smart_intent["execution_intent_id"],smart_snapshot,
                risk_approval_valid=True,strategy_intent_valid=True,position_state_valid=True,
                emergency_stop=bool(recovery_manager.state().get("emergency_stop")),
                actual_order_type="MARKET",actual_requested_quantity=effective_units
            )
            r["smart_execution_shadow"]={"intent":smart_intent,"snapshot":smart_snapshot,"decision":smart_shadow}
            if OBSERVABILITY_ENABLED:
                _obs_module("Smart Execution Engine","OK",last_operation="shadow execution policy evaluated",
                            details={"instrument":r["instrument"],"mode":"SHADOW","decision":smart_shadow})
        except Exception as e:
            r["smart_execution_shadow"]={"error":str(e),"mode":"SHADOW"}
            if OBSERVABILITY_ENABLED:
                _obs_module("Smart Execution Engine","DEGRADED",errors=[str(e)],
                            details={"mode":"SHADOW","existing_execution_unchanged":True})
    # Step 16 shadow boundary: actual order remains the existing safe MARKET/FOK path.
    u=effective_units if r["signal"]=="BUY" else -effective_units
    body={"order":{"instrument":r["instrument"],"units":format_instrument_units(r["instrument"],u),"type":"MARKET","timeInForce":"FOK",
                   "positionFill":"DEFAULT",
                   "stopLossOnFill":{"price":format_instrument_price(r["instrument"],r["stop"]),"timeInForce":"GTC"},
                   "takeProfitOnFill":{"price":format_instrument_price(r["instrument"],r.get("managed_target",r["target"])),"timeInForce":"GTC"}}}
    # RecoveryManager's deterministic key is deliberately stable across scan cycles:
    # the same instrument/side/strategy/market-time/geometry cannot be submitted twice
    # after a retry or restart. Batch cycle/signal identity is retained as metadata.
    key=deterministic_intent_key(
        recovery_manager.account_scope,r["instrument"],r["signal"],setup_variant(r),
        r.get("candle_ts") or now_iso(),r["entry"],r["stop"],r.get("managed_target",r["target"]))
    version_ctx=security_version_context(r)
    batch_intent_context=dict(r.get("_batch_execution_intent") or {})
    metadata={
        **version_ctx,
        "batch_execution_intent": batch_intent_context,
        "market_regime":(r.get("market_regime") or {}).get("market_regime") if isinstance(r.get("market_regime"),dict) else None,
        "volatility_state":(r.get("market_regime") or {}).get("volatility_state") if isinstance(r.get("market_regime"),dict) else None,
        "trend_strength":(r.get("market_regime") or {}).get("trend_strength") if isinstance(r.get("market_regime"),dict) else None,
        "strategy_confidence":r.get("dynamic_confidence"),
        "director_state":(r.get("ai_strategy_director") or {}).get("recommended_state"),
        "director_confidence":(r.get("ai_strategy_director") or {}).get("confidence"),
        "risk_multiplier":(r.get("adaptive_risk_engine") or {}).get("risk_multiplier"),
        "requested_risk":(r.get("adaptive_risk_engine") or {}).get("requested_risk"),
        "approved_risk":(r.get("adaptive_risk_engine") or {}).get("approved_risk"),
        "ensemble_decision_id":(r.get("ensemble_shadow") or {}).get("ensemble_decision_id"),
        "ensemble_weight_version":(r.get("ensemble_shadow") or {}).get("ensemble_weight_version"),
        "ensemble_mode":"SHADOW",
        "risk":r.get("adaptive_risk_engine") or {}
    }
    submitted=await recovery_manager.submit_order(
        client,idempotency_key=key,correlation_id=correlation_id or key,
        decision_id=str(decision_id) if decision_id is not None else None,
        risk_decision_id=str(risk_decision_id) if risk_decision_id is not None else None,
        strategy_id=setup_variant(r),symbol=r["instrument"],side=r["signal"],requested_units=abs(u),
        entry_price=r["entry"],stop_loss=r["stop"],take_profit=r.get("managed_target",r["target"]),
        order_body=body,metadata={**metadata,"smart_execution_intent_id":smart_intent.get("execution_intent_id") if smart_intent else None,
                                  "smart_execution_mode":"SHADOW","smart_execution_decision":smart_shadow or {}})
    if isinstance(submitted,dict):
        submitted["smart_execution_intent_id"]=smart_intent.get("execution_intent_id") if smart_intent else None
        submitted["smart_execution_shadow"]=smart_shadow
    return submitted


async def execute(client: httpx.AsyncClient, r: Dict[str, Any]):
    # Legacy/fallback execution path carries the same hard new-entry gates.
    mode=instrument_mode(r["instrument"])
    if mode != "ENABLED":
        return {"skipped":"INSTRUMENT_NOT_EXECUTION_ENABLED","instrument_mode":mode}
    if market_is_weekend_closed():
        return {"skipped":"MARKET_CLOSED","reason":"MARKET_CLOSED","market_closed":True,"market_data_state":"MARKET_CLOSED"}
    entry_gate=new_entry_time_gate()
    if not entry_gate["allowed"]:
        return {"skipped":entry_gate["reason"],"reason":entry_gate["reason"],"entry_time_gate":entry_gate}
    attached_guard=r.get("portfolio_execution_guard")
    if attached_guard is None:
        risk_ctx=r.get("broker_risk_context")
        if not isinstance(risk_ctx,dict):
            return {"skipped":"GLOBAL_PORTFOLIO_RISK_CONTEXT_REQUIRED"}
        attached_guard=portfolio_execution_guard(r["instrument"],risk_ctx)
        r["portfolio_execution_guard"]=attached_guard
    if not attached_guard.get("allow",False):
        return {"skipped":"GLOBAL_PORTFOLIO_RISK_GUARD","portfolio_execution_guard":attached_guard}
    if r["instrument"] != PRIMARY_INSTRUMENT and instrument_metadata(r["instrument"]).source != "OANDA":
        try:
            await refresh_instrument_metadata(client,[r["instrument"]],force=True)
        except Exception as e:
            return {"skipped":"INSTRUMENT_METADATA_UNVERIFIED","error":str(e)}
        if instrument_metadata(r["instrument"]).source != "OANDA":
            return {"skipped":"INSTRUMENT_METADATA_UNVERIFIED"}
    if SINGLE and await haspos(client, r["instrument"]):
        return {"skipped": "existing_position"}
    sizing=instrument_sizing(r["instrument"],UNITS,r["entry"],r["stop"],risk_context=r.get("broker_risk_context"))
    effective_units=float(sizing["effective_units"]); r["instrument_sizing"]=sizing
    if effective_units <= 0:
        return {"skipped":"INSTRUMENT_SIZE_BELOW_MINIMUM","sizing":sizing}
    u = effective_units if r["signal"] == "BUY" else -effective_units
    body = {"order": {
        "instrument": r["instrument"], "units": format_instrument_units(r["instrument"],u), "type": "MARKET", "timeInForce": "FOK", "positionFill": "DEFAULT",
        "stopLossOnFill": {"price": format_instrument_price(r["instrument"],r["stop"]), "timeInForce": "GTC"},
        "takeProfitOnFill": {"price": format_instrument_price(r["instrument"],r.get("managed_target", r["target"])), "timeInForce": "GTC"}
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
    ens=r.get("ensemble_shadow") or {}
    if ens.get("ensemble_decision_id"):
        c.execute("""UPDATE signals SET ensemble_decision_id=?,ensemble_direction=?,ensemble_confidence=?,
                     ensemble_agreement=?,ensemble_diversity=?,ensemble_weight_version=? WHERE id=?""",
                  (ens.get("ensemble_decision_id"),ens.get("ensemble_direction"),ens.get("ensemble_confidence"),
                   ens.get("agreement_score"),ens.get("diversity_score"),ens.get("ensemble_weight_version"),signal_id))
    c.commit()
    if ENSEMBLE_ENABLED and ens.get("ensemble_decision_id"):
        try:
            ensemble_engine.shadow_compare(
                ens["ensemble_decision_id"],
                current_direction="LONG" if r.get("signal")=="BUY" else "SHORT" if r.get("signal")=="SELL" else "ABSTAIN",
                current_confidence=conf.get("probability"),current_executed=bool(executed)
            )
        except Exception as e:
            log.warning("ENSEMBLE shadow comparison link failed signal=%s err=%s",signal_id,e)
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





def new_entry_time_gate(at: Optional[datetime] = None) -> Dict[str, Any]:
    """Hard gate for NEW entries only; open trades remain fully managed."""
    at=at or datetime.now(timezone.utc)
    ny=at.astimezone(MARKET_TZ)
    mins=ny.hour*60+ny.minute
    morning_blocked=7*60 <= mins < 10*60
    afternoon_blocked=15*60 <= mins < 19*60
    if morning_blocked:
        reason="NY_ENTRY_BLACKOUT_07_10"
        window="07:00-10:00 America/New_York"
    elif afternoon_blocked:
        reason="NY_ENTRY_BLACKOUT_15_19"
        window="15:00-19:00 America/New_York"
    else:
        reason="ALLOWED"
        window=None
    return {
        "allowed": not (morning_blocked or afternoon_blocked),
        "reason": reason,
        "ny_time": ny.isoformat(),
        "window": window,
        "blocked_windows": ["07:00-10:00", "15:00-19:00"],
        "timezone": "America/New_York",
    }


def daily_exit_cutoff_reached(at: Optional[datetime] = None) -> bool:
    """True only during the weekday 16:50-19:00 ET flattening window.

    New entries are already blocked from 15:00-19:00 ET. Limiting the cutoff to
    that same pre-rollover window prevents the flattening rule from immediately
    closing legitimate evening trades opened at/after 19:00 ET. Sunday is excluded
    because it is the weekly market reopen, not a daily rollover-close session.
    """
    at=at or datetime.now(timezone.utc)
    ny=at.astimezone(MARKET_TZ)
    mins=ny.hour*60+ny.minute
    return ny.weekday() < 5 and (16*60+50) <= mins < 19*60


async def close_managed_trades_for_daily_cutoff(client: httpx.AsyncClient, instrument: str,
                                                 at: Optional[datetime] = None) -> int:
    """Close bot-managed open trades at/after 16:50 ET; never opens or reverses exposure."""
    if not daily_exit_cutoff_reached(at):
        return 0
    c=conn()
    rows=[dict(x) for x in c.execute(
        "SELECT * FROM active_trade_management WHERE instrument=? AND closed=0",(instrument,)
    ).fetchall()]
    c.close()
    closed=0
    for tr in rows:
        trade_id=str(tr.get("trade_id") or "")
        if not trade_id:
            continue
        try:
            await req(client,"PUT",f"/v3/accounts/{{account}}/trades/{trade_id}/close",
                      body={"units":"ALL"})
            now=now_iso(); c=conn()
            c.execute("UPDATE active_trade_management SET closed=1,last_action=?,updated_ts=? WHERE trade_id=?",
                      ("DAILY_CUTOFF_CLOSE",now,trade_id))
            c.commit(); c.close(); closed += 1
            if RECOVERY_MANAGER_ENABLED:
                try:
                    recovery_manager.journal("DAILY_CUTOFF_CLOSE",trade_id,strategy_id=tr.get("setup_variant"),
                        payload={"trade_id":trade_id,"instrument":instrument,"cutoff":"16:50 America/New_York"})
                except Exception:
                    pass
        except Exception as e:
            log.exception("Daily cutoff close failed trade=%s: %s",trade_id,e)
            if OBSERVABILITY_ENABLED:
                try:
                    observability_manager.alert(
                        f"DAILY_CUTOFF_CLOSE_FAILED:{trade_id}","CRITICAL","Execution Engine","DAILY_CUTOFF_CLOSE_FAILED",
                        "Open trade could not be closed at the 16:50 ET daily cutoff",
                        details={"trade_id":trade_id,"instrument":instrument,"error":str(e),
                                 "cutoff":"16:50 America/New_York"})
                except Exception:
                    pass
            if RECOVERY_MANAGER_ENABLED:
                try:
                    recovery_manager.journal("DAILY_CUTOFF_CLOSE_FAILED",trade_id,strategy_id=tr.get("setup_variant"),
                        payload={"trade_id":trade_id,"instrument":instrument,"error":str(e),
                                 "cutoff":"16:50 America/New_York"})
                except Exception:
                    pass
    return closed


def market_is_weekend_closed(at: Optional[datetime] = None) -> bool:
    at=at or datetime.now(timezone.utc); ny=at.astimezone(MARKET_TZ); wd=ny.weekday()
    return bool((wd==4 and ny.hour>=17) or wd==5 or (wd==6 and ny.hour<17))

def weekend_id_for_time(at: Optional[datetime] = None) -> Optional[str]:
    at=at or datetime.now(timezone.utc); ny=at.astimezone(MARKET_TZ); wd=ny.weekday()
    if wd==4 and ny.hour>=17: friday=ny.date()
    elif wd==5: friday=(ny-timedelta(days=1)).date()
    elif wd==6: friday=(ny-timedelta(days=2)).date()
    elif wd==0: friday=(ny-timedelta(days=3)).date()
    else:return None
    return friday.isoformat()

def latest_relevant_weekend_id(at: Optional[datetime] = None) -> Optional[str]:
    at=at or datetime.now(timezone.utc); ny=at.astimezone(MARKET_TZ); wd=ny.weekday()
    if wd==4 and ny.hour>=17:return ny.date().isoformat()
    if wd==5:return (ny-timedelta(days=1)).date().isoformat()
    if wd==6:return (ny-timedelta(days=2)).date().isoformat()
    if wd==0:return (ny-timedelta(days=3)).date().isoformat()
    return None

def _weekend_bucket(at: datetime) -> str:
    at=at.astimezone(timezone.utc)
    minutes=max(30,WEEKEND_NEWS_INTERVAL_MIN)
    epoch=int(at.timestamp()); bucket=epoch-(epoch%(minutes*60))
    return datetime.fromtimestamp(bucket,tz=timezone.utc).isoformat()

async def collect_weekend_news_snapshot(client: httpx.AsyncClient,instrument: str,at: Optional[datetime]=None)->Dict[str,Any]:
    at=at or datetime.now(timezone.utc)
    if not WEEKEND_RESEARCH_ENABLED or not market_is_weekend_closed(at):return {"collected":False,"reason":"not_weekend_closed"}
    wid=weekend_id_for_time(at); bucket=_weekend_bucket(at)
    c=conn(); exists=c.execute("SELECT 1 FROM weekend_context WHERE weekend_id=? AND instrument=? AND bucket_ts=?",(wid,instrument,bucket)).fetchone(); c.close()
    if exists:return {"collected":False,"reason":"bucket_already_collected","weekend_id":wid}
    base,quote=instrument.split('_'); q=f"({base} OR {quote}) (forex OR currency OR inflation OR rates OR central bank OR jobs OR GDP OR election OR geopolitical)"
    try:
        x=await client.get(GDELT,params={"query":q,"mode":"ArtList","maxrecords":"25","format":"json","timespan":"360min","sort":"HybridRel"},timeout=10); x.raise_for_status()
        arts=x.json().get('articles',[]); text=' '.join(a.get('title','').lower() for a in arts)
        pos=sum(text.count(w) for w in ['hawkish','rate hike','strong jobs','jobs beat','growth beats','currency gains','higher rates','inflation rises','economy strong'])
        neg=sum(text.count(w) for w in ['dovish','rate cut','weak jobs','jobs miss','recession','currency falls','lower rates','economy weak','growth slows'])
        bias='BULLISH' if pos-neg>=2 else 'BEARISH' if neg-pos>=2 else 'NEUTRAL'; titles=[a.get('title','') for a in arts[:12]]
        c=conn(); c.execute("""INSERT OR IGNORE INTO weekend_context(weekend_id,instrument,bucket_ts,collected_ts,bias,positive_hits,negative_hits,article_count,titles_json) VALUES(?,?,?,?,?,?,?,?,?)""",(wid,instrument,bucket,now_iso(),bias,pos,neg,len(arts),json.dumps(titles,separators=(',',':')))); c.commit(); c.close()
        return {"collected":True,"weekend_id":wid,"bucket":bucket,"bias":bias,"positive_hits":pos,"negative_hits":neg,"article_count":len(arts)}
    except Exception as e:return {"collected":False,"weekend_id":wid,"error":str(e)}

def summarize_weekend_context(weekend_id: str,instrument: str)->Optional[Dict[str,Any]]:
    c=conn(); rows=c.execute("SELECT * FROM weekend_context WHERE weekend_id=? AND instrument=? ORDER BY bucket_ts",(weekend_id,instrument)).fetchall(); c.close()
    if not rows:return None
    pos=sum(int(r['positive_hits'] or 0) for r in rows); neg=sum(int(r['negative_hits'] or 0) for r in rows); articles=sum(int(r['article_count'] or 0) for r in rows); score=float(pos-neg)
    bias='BULLISH' if score>=2 else 'BEARISH' if score<=-2 else 'NEUTRAL'; titles=[]; seen=set()
    for r in rows:
        try:arr=json.loads(r['titles_json'] or '[]')
        except Exception:arr=[]
        for t in arr:
            if t and t not in seen:seen.add(t);titles.append(t)
            if len(titles)>=20:break
        if len(titles)>=20:break
    return {"weekend_id":weekend_id,"instrument":instrument,"bias":bias,"score":score,"positive_hits":pos,"negative_hits":neg,"article_count":articles,"snapshots":len(rows),"titles":titles}

def ensure_weekend_session(instrument: str,current_price: float,at: Optional[datetime]=None)->Optional[Dict[str,Any]]:
    if not WEEKEND_RESEARCH_ENABLED or not current_price:return None
    at=at or datetime.now(timezone.utc)
    if market_is_weekend_closed(at):return None
    wid=latest_relevant_weekend_id(at)
    if not wid:return None
    ny=at.astimezone(MARKET_TZ)
    if not ((ny.weekday()==6 and ny.hour>=17) or ny.weekday()==0):return None
    c=conn(); row=c.execute("SELECT * FROM weekend_sessions WHERE weekend_id=? AND instrument=?",(wid,instrument)).fetchone(); c.close()
    if row:return dict(row)
    summary=summarize_weekend_context(wid,instrument)
    if not summary:return None
    c=conn(); c.execute("""INSERT OR IGNORE INTO weekend_sessions(weekend_id,instrument,opened_ts,open_price,context_bias,context_score,article_count,context_json,updated_ts) VALUES(?,?,?,?,?,?,?,?,?)""",(wid,instrument,at.astimezone(timezone.utc).isoformat(),float(current_price),summary['bias'],float(summary['score']),int(summary['article_count']),json.dumps(summary,separators=(',',':')),now_iso())); c.commit(); row=c.execute("SELECT * FROM weekend_sessions WHERE weekend_id=? AND instrument=?",(wid,instrument)).fetchone(); c.close(); return dict(row) if row else None

def update_weekend_reactions(instrument: str,current_price: float,at: Optional[datetime]=None)->Dict[str,Any]:
    at=at or datetime.now(timezone.utc); c=conn(); row=c.execute("SELECT * FROM weekend_sessions WHERE instrument=? ORDER BY opened_ts DESC LIMIT 1",(instrument,)).fetchone()
    if not row:c.close();return {"updated":False,"reason":"no_weekend_session"}
    opened=datetime.fromisoformat(row['opened_ts'].replace('Z','+00:00')); elapsed=(at.astimezone(timezone.utc)-opened).total_seconds()/3600; pip=pip_size(instrument); move=(float(current_price)-float(row['open_price']))/pip if pip else 0; changes={}
    for h in WEEKEND_REACTION_HORIZONS:
        col=f'reaction_{h}h_pips'
        if elapsed>=h and row[col] is None:changes[col]=float(move)
    if changes:
        sets=', '.join(f'{k}=?' for k in changes); vals=list(changes.values())+[now_iso(),row['weekend_id'],instrument]
        c.execute(f"UPDATE weekend_sessions SET {sets},updated_ts=? WHERE weekend_id=? AND instrument=?",vals); c.commit()
    c.close();return {"updated":bool(changes),"elapsed_hours":elapsed,"changes":changes}

def active_weekend_context(instrument: str,at: Optional[datetime]=None)->Optional[Dict[str,Any]]:
    at=at or datetime.now(timezone.utc); c=conn(); row=c.execute("SELECT * FROM weekend_sessions WHERE instrument=? ORDER BY opened_ts DESC LIMIT 1",(instrument,)).fetchone(); c.close()
    if not row:return None
    opened=datetime.fromisoformat(row['opened_ts'].replace('Z','+00:00')); age=(at.astimezone(timezone.utc)-opened).total_seconds()/3600
    if age<0 or age>WEEKEND_SIGNAL_CONTEXT_HOURS:return None
    out=dict(row);out['age_hours']=age;return out

def attach_weekend_context_observation(instrument: str,candle_ts: Optional[str],at: Optional[datetime]=None)->Optional[Dict[str,Any]]:
    ctx=active_weekend_context(instrument,at)
    if not ctx or not candle_ts:return None
    record_external_observation(instrument,'WEEKEND_CONTEXT',f'{instrument}_WEEKEND',float(ctx['context_score']),str(ctx['context_bias']),{"weekend_id":ctx['weekend_id'],"age_hours":ctx['age_hours'],"article_count":ctx['article_count'],"reaction_1h_pips":ctx['reaction_1h_pips'],"reaction_4h_pips":ctx['reaction_4h_pips'],"reaction_12h_pips":ctx['reaction_12h_pips'],"reaction_24h_pips":ctx['reaction_24h_pips']},candle_ts)
    return ctx

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



def _ensemble_execution_cost_bps(symbol: str, before_ts: Optional[str]) -> Optional[float]:
    """Execution-cost estimate using only TCA known before this ensemble decision."""
    if not SMART_EXECUTION_ENABLED:
        return None
    c=conn()
    params=[symbol]
    where="symbol=?"
    if before_ts:
        where+=" AND ts<?";params.append(before_ts)
    rows=c.execute(f"""SELECT total_execution_cost,filled_quantity,expected_price FROM smart_execution_tca
                         WHERE {where} ORDER BY ts DESC LIMIT 50""",params).fetchall()
    c.close()
    vals=[]
    for x in rows:
        q=_risk_float(x["filled_quantity"]);px=_risk_float(x["expected_price"]);cost=_risk_float(x["total_execution_cost"])
        if q and px and cost is not None and q>0 and px>0:
            vals.append(max(0.0,float(cost)/(float(q)*float(px))*10000.0))
    return float(statistics.median(vals)) if vals else None


def _ensemble_expected_technical_edge_bps(r: Dict[str,Any], conf: Dict[str,Any]) -> Optional[float]:
    if r.get("signal") not in ("BUY","SELL"):
        return None
    entry=_risk_float(r.get("entry"));stop=_risk_float(r.get("stop"));target=_risk_float(r.get("managed_target",r.get("target")))
    p=_risk_float(conf.get("probability"))
    if entry is None or stop is None or target is None or p is None or entry<=0:
        return None
    reward=abs(target-entry)/entry*10000.0;risk=abs(entry-stop)/entry*10000.0
    return float(p*reward-(1-p)*risk)


def build_ensemble_shadow_signals(r: Dict[str,Any], conf: Dict[str,Any], mlp: Optional[float]) -> List[Dict[str,Any]]:
    """Adapt current V3.24 sources to the Step-17 Standard Signal Interface.

    Price-derived subcomponents stay inside TECHNICAL_CORE; they are NOT counted as
    separate independent voters. The ML model predicts success of the selected setup,
    so it is a CALIBRATOR, not an independent LONG/SHORT voter.
    """
    symbol=r["instrument"];ts=r.get("candle_ts") or now_iso();reg=(r.get("market_regime") or {}) if isinstance(r.get("market_regime"),dict) else {}
    dq=0.0 if r.get("market_data_stale") else 1.0
    signal=r.get("signal")
    tech_dir="LONG" if signal=="BUY" else "SHORT" if signal=="SELL" else "ABSTAIN"
    tech_conf=clamp(float(r.get("technical") or 0)/100.0,0.0,1.0)
    signals=[{
        "strategy_id":"TECHNICAL_CORE","strategy_version":f"technical@{VERSION_TAG}","symbol":symbol,"timestamp":ts,
        "direction":tech_dir,"confidence":tech_conf,"expected_edge":_ensemble_expected_technical_edge_bps(r,conf),
        "market_regime":reg.get("market_regime"),"time_horizon":"INTRADAY","signal_strength":clamp(float(r.get("direction_edge") or 0)/30.0,0.15,1.0),
        "risk_characteristics":{"rr":r.get("rr"),"countertrend":r.get("direction_state")=="COUNTERTREND"},
        "data_quality":dq,"family":"TREND_STRUCTURE",
        "input_dependencies":["H1_PRICE","M15_PRICE","M5_PRICE","M1_PRICE","EMA","ATR","STRUCTURE","PULLBACK","MOMENTUM"],
        "role":"DIRECTIONAL","ttl_seconds":ENSEMBLE_SIGNAL_TTL_SECONDS,
        "metadata":{"setup_variant":setup_variant(r),"buy_score":r.get("buy_score"),"sell_score":r.get("sell_score")}
    }]
    # ML is explicitly a success-probability calibrator for the technical setup.
    ml_model_exists=Path(MODEL_PATH).exists()
    ml_lifecycle="READY" if mlp is not None else ("WAITING_FOR_EVIDENCE" if not ml_model_exists else "PREDICTION_UNAVAILABLE")
    signals.append({
        "strategy_id":"ML_SUCCESS_CALIBRATOR","strategy_version":f"ml@{VERSION_TAG}","symbol":symbol,"timestamp":ts,
        "direction":"ABSTAIN","confidence":float(mlp) if mlp is not None else .5,"expected_edge":None,
        "market_regime":reg.get("market_regime"),"time_horizon":"INTRADAY","signal_strength":float(mlp) if mlp is not None else 0.0,
        "risk_characteristics":{},"data_quality":dq if mlp is not None else 0.0,"family":"TECHNICAL_CALIBRATION",
        "input_dependencies":["TECHNICAL_FEATURE_VECTOR","RESOLVED_LABELS"],"role":"CALIBRATOR",
        "ttl_seconds":ENSEMBLE_SIGNAL_TTL_SECONDS,
        "status":"ONLINE" if (mlp is not None or not ml_model_exists) else "OFFLINE",
        "metadata":{"meaning":"probability current setup resolves positively; not a direction model",
                    "lifecycle_state":ml_lifecycle,"model_exists":ml_model_exists}
    })
    bias=str(r.get("news_bias") or "NEUTRAL").upper()
    news_dir="LONG" if bias=="BULLISH" else "SHORT" if bias=="BEARISH" else "ABSTAIN"
    hits=abs(int(r.get("news_positive_hits") or 0)-int(r.get("news_negative_hits") or 0));arts=len(r.get("news_articles") or [])
    signals.append({
        "strategy_id":"NEWS_CONTEXT","strategy_version":f"gdelt@{VERSION_TAG}","symbol":symbol,"timestamp":ts,
        "direction":news_dir,"confidence":clamp(.45+.08*hits+.01*arts,.45,.80) if news_dir!="ABSTAIN" else .35,
        "expected_edge":None,"market_regime":reg.get("market_regime"),"time_horizon":"INTRADAY",
        "signal_strength":clamp(.25+.12*hits,0,1),"risk_characteristics":{"headline_count":arts},
        "data_quality":1.0 if r.get("alignment") not in ("UNKNOWN",None) else .5,"family":"NEWS_MACRO",
        "input_dependencies":["GDELT_180M","FX_CURRENCY_TERMS"],"role":"DIRECTIONAL","ttl_seconds":ENSEMBLE_SIGNAL_TTL_SECONDS,
        "metadata":{"bias":bias,"alignment":r.get("alignment")}
    })
    # Regime is context, not another vote from the same price stream.
    regime_dir="LONG" if reg.get("market_regime")=="BULLISH_TREND" else "SHORT" if reg.get("market_regime")=="BEARISH_TREND" else "NEUTRAL"
    signals.append({
        "strategy_id":"MARKET_REGIME_CONTEXT","strategy_version":f"regime@{VERSION_TAG}","symbol":symbol,"timestamp":ts,
        "direction":regime_dir,"confidence":float(reg.get("confidence") or 0),"expected_edge":None,
        "market_regime":reg.get("market_regime"),"time_horizon":"INTRADAY","signal_strength":float(reg.get("trend_strength") or 0),
        "risk_characteristics":{"volatility_state":reg.get("volatility_state")},"data_quality":dq,"family":"MARKET_REGIME",
        "input_dependencies":["H1_PRICE","M15_PRICE","M5_PRICE","M1_PRICE","ATR","EFFICIENCY_RATIO"],"role":"CONTEXT",
        "ttl_seconds":ENSEMBLE_SIGNAL_TTL_SECONDS
    })
    weekend=((r.get("weekend_research") or {}).get("active_signal_context") or {})
    if weekend:
        wb=str(weekend.get("context_bias") or weekend.get("bias") or "NEUTRAL").upper();wv=_risk_float(weekend.get("context_score"),0.0) or 0.0
        wd="LONG" if wb=="BULLISH" or wv>0 else "SHORT" if wb=="BEARISH" or wv<0 else "ABSTAIN"
        signals.append({"strategy_id":"WEEKEND_CONTEXT","strategy_version":f"weekend@{VERSION_TAG}","symbol":symbol,"timestamp":ts,
          "direction":wd,"confidence":clamp(.5+abs(wv)*.2,.5,.75),"expected_edge":None,"market_regime":reg.get("market_regime"),
          "time_horizon":"INTRADAY","signal_strength":clamp(abs(wv),0,1),"risk_characteristics":{},"data_quality":dq,
          "family":"WEEKEND_CONTEXT","input_dependencies":["GDELT_WEEKEND","MARKET_REOPEN_REACTION"],"role":"DIRECTIONAL",
          "ttl_seconds":ENSEMBLE_SIGNAL_TTL_SECONDS,"metadata":{"weekend_id":weekend.get("weekend_id")}})
    # Cross-asset observations are kept as CONTEXT until a directional relationship is validated.
    c=conn();obs=c.execute("""SELECT e.* FROM external_research_observations e
                         JOIN (SELECT source_key,MAX(id) AS id FROM external_research_observations
                               WHERE instrument=? AND candle_ts=? AND source_type='CROSS_ASSET' GROUP BY source_key) latest
                           ON latest.id=e.id
                         ORDER BY e.id""",(symbol,r.get("candle_ts"))).fetchall();c.close()
    for o in obs:
        key=str(o["source_key"]);val=float(o["value_num"] or 0);mid=f"CROSS_ASSET_{key}"
        ensemble_engine.register_model(mid,f"cross_asset@{VERSION_TAG}","CROSS_ASSET","CONTEXT",
            [f"{key}_M5_PRICE","FX_SHARED_FACTOR"],"INTRADAY")
        signals.append({"strategy_id":mid,"strategy_version":f"cross_asset@{VERSION_TAG}","symbol":symbol,"timestamp":ts,
          "direction":"LONG" if val>0 else "SHORT" if val<0 else "NEUTRAL","confidence":clamp(.4+abs(val)*.15,.4,.75),
          "expected_edge":None,"market_regime":reg.get("market_regime"),"time_horizon":"INTRADAY","signal_strength":clamp(abs(val),0,1),
          "risk_characteristics":{},"data_quality":dq,"family":"CROSS_ASSET","input_dependencies":[f"{key}_M5_PRICE","FX_SHARED_FACTOR"],
          "role":"CONTEXT","ttl_seconds":ENSEMBLE_SIGNAL_TTL_SECONDS,"metadata":{"research_only":True,"composite":val}})
    return signals


def evaluate_ensemble_shadow(r: Dict[str,Any], conf: Dict[str,Any], mlp: Optional[float]) -> Dict[str,Any]:
    if not ENSEMBLE_ENABLED:
        return {"enabled":False,"mode":"DISABLED","ensemble_direction":"ABSTAIN","hypothetical_only":True}
    try:
        signals=build_ensemble_shadow_signals(r,conf,mlp)
        reg=(r.get("market_regime") or {}).get("market_regime") if isinstance(r.get("market_regime"),dict) else None
        execution_cost=_ensemble_execution_cost_bps(r["instrument"],r.get("candle_ts"))
        out=ensemble_engine.evaluate(signals,method="REGIME_WEIGHTED",regime=reg,execution_cost=execution_cost,
            target_horizon="INTRADAY",current_system_direction="LONG" if r.get("signal")=="BUY" else "SHORT" if r.get("signal")=="SELL" else "ABSTAIN",
            current_system_confidence=conf.get("probability"),current_executed=False)
        out["enabled"]=True;out["policy_authority"]=False;out["risk_multiplier_authority"]=False
        return out
    except Exception as e:
        log.exception("ENSEMBLE shadow evaluation failed: %s",e)
        return {"enabled":True,"mode":"SHADOW","ensemble_direction":"ABSTAIN","ensemble_confidence":0.0,
                "hypothetical_only":True,"error":str(e),"policy_authority":False}


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
        elif typ=="WEEKEND_CONTEXT":
            v=float(val or 0); bullish=v>0; bearish=v<0
            aligned=(direction=="BUY" and bullish) or (direction=="SELL" and bearish)
            inverse=(direction=="BUY" and bearish) or (direction=="SELL" and bullish)
            try: meta=json.loads(o.get("metadata_json") or "{}")
            except Exception: meta={}
            age=float(meta.get("age_hours",999) or 999)
            out[f"weekend::{key}::bias_aligned"]={"family":"WEEKEND_CONTEXT","aligned":bool(aligned),"description":f"{key} weekend bias aligned with signal"}
            out[f"weekend::{key}::bias_inverse"]={"family":"WEEKEND_CONTEXT","aligned":bool(inverse),"description":f"{key} weekend bias inverse to signal"}
            out[f"weekend::{key}::first_4h"]={"family":"WEEKEND_CONTEXT","aligned":bool(age<=4),"description":f"{key} signal within first 4h after reopen"}
            out[f"weekend::{key}::first_12h"]={"family":"WEEKEND_CONTEXT","aligned":bool(age<=12),"description":f"{key} signal within first 12h after reopen"}
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
    "sell_score","direction_edge","h1_gap_atr","h1_slope_atr","transition_state",
    "session_strength","session_displacement_atr","session_momentum_atr"
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
                           WHERE ls.label IN (0,1) AND s.instrument=?""",(PRIMARY_INSTRUMENT,)).fetchall()
    shadow=c.execute("""SELECT st.label,s.candle_ts,s.instrument,s.signal,s.features_json,
                               'SHADOW' source,st.variant variant
                        FROM shadow_trials st JOIN signals s ON s.id=st.signal_id
                        WHERE st.label IN (0,1) AND s.instrument=?""",(PRIMARY_INSTRUMENT,)).fetchall()
    c.close()
    raw=[]
    for row in list(canonical)+list(shadow):
        try:f=json.loads(row["features_json"] or "{}")
        except Exception:f={}
        raw.append({"label":int(row["label"]),"features":f,"signal":row["signal"],
                    "instrument":row["instrument"],"candle_ts":row["candle_ts"],
                    "source":row["source"],"variant":row["variant"],
                    "weight":1.0 if row["source"]=="CANONICAL" else AUTONOMOUS_SHADOW_WEIGHT})

    # Assign episodes across the canonical+shadow union first. Then retain one
    # canonical observation and one observation per shadow variant per episode.
    # This preserves counterfactual comparisons without giving a long trend dozens
    # of independent votes.
    annotated=annotate_market_episodes(raw,gap_minutes=RESEARCH_EPISODE_GAP_MINUTES)
    out=[];seen=set()
    for row in annotated:
        identity=(row["episode_id"],row["source"],row.get("variant") if row["source"]=="SHADOW" else None)
        if identity in seen:continue
        seen.add(identity);out.append(row)
    out.sort(key=lambda r:(r.get("candle_ts") or "",r["episode_id"],r["source"],str(r.get("variant") or "")))
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

    discovery,validation=split_episode_holdout(rows,AUTONOMOUS_DISCOVERY_HOLDOUT,min_holdout_episodes=1)
    discovery_episodes=len({r["episode_id"] for r in discovery})
    validation_episodes=len({r["episode_id"] for r in validation})
    if len(validation)<30 or validation_episodes<2:
        return {"enabled":True,"rows":len(rows),"episodes":len({r["episode_id"] for r in rows}),
                "reason":"not_enough_holdout","validation_rows":len(validation),
                "validation_episodes":validation_episodes}

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
    return {"enabled":True,"rows":len(rows),"episodes":len({r["episode_id"] for r in rows}),
            "discovery_rows":len(discovery),"validation_rows":len(validation),
            "discovery_episodes":discovery_episodes,"validation_episodes":validation_episodes,
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
                      WHERE ls.label IN (0,1) AND s.instrument=? ORDER BY s.id DESC LIMIT ?""",
                   (PRIMARY_INSTRUMENT,limit)).fetchall()
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

def security_queue_validated_research_changes() -> Dict[str,Any]:
    actor=security_manager.internal_actor("RESEARCH_ENGINE","SYSTEM_RECOMMENDER")
    created=[];existing=[];errors=[]
    for x in _validated_promotion_candidates():
        key=f"research_rule.{x['source']}.{x['rule_key']}"
        c=conn()
        prev=c.execute("""SELECT change_id,status FROM security_change_requests
                          WHERE config_key=? AND status IN ('PENDING_REVIEW','APPROVED','APPLIED')
                          ORDER BY requested_ts DESC LIMIT 1""",(key,)).fetchone()
        c.close()
        if prev:
            existing.append({"config_key":key,"change_id":prev["change_id"],"status":prev["status"]})
            continue
        try:
            cr=security_manager.create_change_request(
                actor,component="strategy.research_filters",key=key,proposed=True,
                reason=f"Validated research rule recommendation: {x.get('description')}",
                expected_impact=f"Potential filter edge={x.get('edge')} samples={x.get('samples')}; no execution change until human approval.",
                rollback_plan="Deactivate the approved research rule and rollback the previous configuration snapshot."
            )
            created.append({"candidate":x,"change_request":cr})
        except Exception as e:
            errors.append({"candidate":x,"error":str(e)})
    return {"created":created,"existing":existing,"errors":errors,"auto_activation":False}


def activate_research_rule_from_applied_change(change_id: str, actor: Dict[str,str]) -> Dict[str,Any]:
    bundle=security_manager.change_request(change_id);req=bundle.get("change")
    if not req or req.get("status")!="APPLIED":
        return {"activated":False,"reason":"CHANGE_NOT_APPLIED"}
    key=req.get("config_key") or ""
    if not key.startswith("research_rule."):
        return {"activated":False,"reason":"NOT_RESEARCH_RULE"}
    rest=key[len("research_rule."):]
    source,sep,rule_key=rest.partition(".")
    if not sep:
        return {"activated":False,"reason":"INVALID_RESEARCH_RULE_KEY"}
    proposed=json.loads(req.get("proposed_value_json") or "null")
    if proposed is False:
        c=conn()
        row=c.execute("""SELECT * FROM active_research_rules WHERE source=? AND rule_key=?
                         AND status IN ('ACTIVE','CONFIRMED') ORDER BY id DESC LIMIT 1""",
                      (source,rule_key)).fetchone()
        if not row:
            c.close();return {"deactivated":True,"already_inactive":True}
        c.execute("""UPDATE active_research_rules SET status='REVERTED',deactivated_ts=?,reason=?
                     WHERE id=?""",(now_iso(),f"Deactivated by approved Change Request {change_id}",row["id"]))
        c.commit();c.close()
        _audit_research_rule("REVERTED",source,rule_key,{"change_id":change_id,"actor":actor["actor"]})
        security_manager.audit(actor,"STRATEGY_CONFIGURATION_APPLIED",f"research_rule:{source}:{rule_key}",
                               True,False,f"approved Change Request {change_id}","APPLIED")
        return {"deactivated":True,"change_id":change_id}

    candidates=[x for x in _validated_promotion_candidates() if x["source"]==source and x["rule_key"]==rule_key]
    if not candidates:
        return {"activated":False,"reason":"RULE_NO_LONGER_VALIDATED"}
    x=candidates[0]
    active=get_active_research_rules()
    if any(r["source"]==source and r["rule_key"]==rule_key for r in active):
        return {"activated":True,"already_active":True}
    compat=assess_rule_compatibility(x,active)
    if not compat["compatible"]:
        return {"activated":False,"reason":"INCOMPATIBLE_WITH_CURRENT_RULES","compatibility":compat}
    c=conn()
    c.execute("""INSERT INTO active_research_rules(
      source,rule_key,description,status,activated_ts,evidence_samples,evidence_edge,baseline_win_rate,
      review_after_samples,post_samples,post_wins,reviewed_matches,reason)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (x["source"],x["rule_key"],x["description"],"ACTIVE",now_iso(),x["samples"],x["edge"],x["baseline"],
       AUTO_PROMOTE_REVIEW_SAMPLES,0,0,0,f"Activated by approved Change Request {change_id}"))
    c.commit();c.close()
    _audit_research_rule("PROMOTED",x["source"],x["rule_key"],{**x,"compatibility":compat,"change_id":change_id})
    security_manager.audit(actor,"STRATEGY_CONFIGURATION_APPLIED",f"research_rule:{source}:{rule_key}",
                           False,True,f"approved Change Request {change_id}","APPLIED")
    return {"activated":True,"rule":x,"compatibility":compat,"change_id":change_id}

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
    instrument=InstrumentRegistry.normalize_symbol((r or {}).get("instrument") or PRIMARY_INSTRUMENT)
    if not instrument_profile(instrument).learned_research_veto_authority:
        return {"ok":True,"active":False,"rules":[],"vetoes":[],
                "reason":"instrument_scoped_research_not_validated","instrument":instrument}
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
                      WHERE ls.label IN (0,1) AND s.instrument=? AND s.ts>=? ORDER BY s.id""",
                   (PRIMARY_INSTRUMENT,rule["activated_ts"])).fetchall()
    c.close()
    return [r for r in rows if _signal_passes_rule_for_review(rule["source"],rule["rule_key"],r) is True]

def review_one_active_research_rule(rule):
    """
    V3.19 observation-only strategy health review.
    It may recommend deactivation through Change Management, but never changes
    an active rule's behavioral state by itself.
    """
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
        key=f"research_rule.{rule['source']}.{rule['rule_key']}"
        c=conn()
        prior=c.execute("""SELECT change_id,status FROM security_change_requests
                           WHERE config_key=? AND proposed_value_json='false'
                           AND status IN ('PENDING_REVIEW','APPROVED','APPLIED')
                           ORDER BY requested_ts DESC LIMIT 1""",(key,)).fetchone()
        c.close()
        change=None
        if not prior:
            actor=security_manager.internal_actor("STRATEGY_HEALTH_MONITOR","SYSTEM_RECOMMENDER")
            change=security_manager.create_change_request(
                actor,component="strategy.research_filters",key=key,proposed=False,
                reason=f"Independent health block degraded: {wr:.3f} < baseline {float(baseline):.3f}",
                expected_impact="Reduce reliance on a degraded learned filter; no automatic deactivation.",
                rollback_plan="Restore the prior config snapshot and reactivate only after human review.")
        d={"block_samples":n,"block_win_rate":wr,"baseline_win_rate":baseline,
           "reviewed_matches":reviewed+n,"change_request":change or dict(prior) if prior else change,
           "auto_deactivation":False}
        _audit_research_rule("DEACTIVATION_RECOMMENDED",rule["source"],rule["rule_key"],d)
        return {"reviewed":True,"status":"DEACTIVATION_RECOMMENDED","source":rule["source"],"rule_key":rule["rule_key"],**d}
    d={"block_samples":n,"block_win_rate":wr,"baseline_win_rate":baseline,
       "reviewed_matches":reviewed+n,"status":"HEALTH_CONFIRMED","behavior_changed":False}
    _audit_research_rule("HEALTH_CONFIRMED",rule["source"],rule["rule_key"],d)
    return {"reviewed":True,"status":"HEALTH_CONFIRMED","source":rule["source"],"rule_key":rule["rule_key"],**d}

def review_active_research_rules():
    results=[review_one_active_research_rule(r) for r in get_active_research_rules()]
    return {"reviewed_rules":results,
            "deactivation_recommendations":[x for x in results if x.get("status")=="DEACTIVATION_RECOMMENDED"],
            "behavior_changed":False,
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
    # Supplemental research-only trend-continuation trial.
    # This does NOT change execution, safety checks, minimum_rr, or live orders.
    f=r.get("features") or {}
    flt=r.get("filters") or {}

    try:
        shadow_rr=float(f.get("rr_raw",0) or 0)
        shadow_room=float(f.get("room_to_barrier_r",0) or 0)
        shadow_ext=float(f.get("extension_atr",999) or 999)
        shadow_vol=float(f.get("volatility_ratio",0) or 0)
    except (TypeError,ValueError):
        shadow_rr=0.0
        shadow_room=0.0
        shadow_ext=999.0
        shadow_vol=0.0

    trend_continuation_shadow=(
        1.20 <= shadow_rr < 1.50
        and shadow_room >= 1.75
        and str(f.get("barrier_class") or r.get("barrier_class") or "NONE") != "STRONG"
        and bool(flt.get("h1_context"))
        and bool(flt.get("m15_context"))
        and bool(flt.get("m5_structure"))
        and bool(f.get("m1_shadow_confirm"))
        and shadow_ext <= 0.90
        and 0.65 <= shadow_vol <= 2.0
    )

    if trend_continuation_shadow:
        before=c.total_changes
        c.execute(
            """INSERT OR IGNORE INTO shadow_trials(
                   signal_id,created_ts,candle_ts,instrument,direction,
                   variant,entry,stop,target,risk,status
               ) VALUES(?,?,?,?,?,?,?,?,?,?, 'PENDING')""",
            (
                signal_id,now_iso(),r.get("candle_ts"),r["instrument"],
                direction,"TREND_CONTINUATION_SHADOW",
                entry,stop,target,risk
            )
        )
        made += int(c.total_changes>before)

    c.commit();c.close();return made

def resolve_shadow_trials(inst: str,m1: List[Dict[str,Any]])->int:
    if not RESEARCH_LAB_ENABLED:return 0
    c=conn();rows=c.execute("SELECT * FROM shadow_trials WHERE status='PENDING' AND instrument=? ORDER BY id",(inst,)).fetchall();n=0
    for row in rows:
        fake={"candle_ts":row["candle_ts"],"created_ts":row["created_ts"],"instrument":row["instrument"],"direction":row["direction"],"entry":row["entry"],"stop":row["stop"],"target":row["target"]}
        out=resolve_one(fake,m1)
        if out:
            c.execute("UPDATE shadow_trials SET status=?,label=?,resolved_ts=?,bars_to_resolution=?,mfe_r=?,mae_r=?,note=?,outcome_cost_r=?,effective_target=?,effective_stop=? WHERE id=?",(out["status"],out["label"],now_iso(),out["bars"],out["mfe_r"],out["mae_r"],out["note"],out.get("cost_r"),out.get("effective_target"),out.get("effective_stop"),row["id"]));n+=1
    c.commit();c.close();return n

def experimental_filter_candidates(r: Dict[str,Any])->Dict[str,Dict[str,Any]]:
    f=r.get("features",{});flt=r.get("filters",{});ext=float(f.get("extension_atr",0) or 0);vol=float(f.get("volatility_ratio",0) or 0);slope=abs(float(f.get("m15_slope_atr",0) or 0));bar=float(f.get("barrier_score",0) or 0);edge=float(f.get("direction_edge",0) or 0)
    confs=sum(1 for k in ("m5_structure","second_pullback","m1_confirmation","not_extended","volatility_ok") if flt.get(k));m5=float(f.get("m5_momentum",0) or 0);m1=float(f.get("m1_momentum",0) or 0);aligned=(r.get("signal")=="BUY" and m5>0 and m1>0) or (r.get("signal")=="SELL" and m5<0 and m1<0)
    return {"ext_le_0_8":{"pass":ext<=.8,"description":"Extensión <= 0.8 ATR"},"ext_le_1_0":{"pass":ext<=1.0,"description":"Extensión <= 1.0 ATR"},"vol_normal":{"pass":.75<=vol<=1.35,"description":"Volatilidad 0.75–1.35"},"trend_abs_ge_0_30":{"pass":slope>=.30,"description":"Pendiente M15 >= 0.30 ATR"},"momentum_aligned":{"pass":aligned,"description":"Momentum M5 y M1 alineado"},"confirmations_ge_3":{"pass":confs>=3,"description":"Al menos 3 confirmaciones"},"barrier_score_lt_0_75":{"pass":bar<.75,"description":"Barrera estructural < 0.75"},"direction_edge_ge_15":{"pass":edge>=15,"description":"Ventaja BUY/SELL >= 15"}}

def refresh_filter_hypotheses()->Dict[str,Any]:
    c=conn()
    terminal=c.execute("""SELECT ls.label,ls.status,s.id signal_id,s.candle_ts,s.instrument,s.signal,
                                 s.features_json,s.filters_json
                          FROM learning_samples ls JOIN signals s ON s.id=ls.signal_id
                          WHERE ls.status IN ('WIN','LOSS','TIMEOUT','AMBIGUOUS') AND s.instrument=?
                          ORDER BY s.candle_ts,s.id""",(PRIMARY_INSTRUMENT,)).fetchall()
    episodes=collapse_market_episodes(terminal,gap_minutes=RESEARCH_EPISODE_GAP_MINUTES)
    rows=[x for x in episodes if x.get("label") in (0,1)]
    unresolved=sum(1 for x in episodes if x.get("label") not in (0,1))
    if not rows:
        c.close()
        return {"samples":0,"terminal_episodes":len(episodes),"unresolved_episodes":unresolved,
                "experimental":0,"evaluating":0,"validated":0,"rejected":0}
    stats={}
    for row in rows:
        rr={"signal":row["signal"],"features":json.loads(row["features_json"] or "{}"),
            "filters":json.loads(row["filters_json"] or "{}")}
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
    c.commit();c.close()
    return {"samples":len(rows),"terminal_episodes":len(episodes),"unresolved_episodes":unresolved,
            "experimental":counts["EXPERIMENTAL"],"evaluating":counts["EVALUATING"],
            "validated":counts["VALIDATED"],"rejected":counts["REJECTED"]}


def should_retrain_model(instrument: Optional[str]=None)->Dict[str,Any]:
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    c=conn()
    rows=c.execute("""SELECT ls.label,s.candle_ts,s.instrument,s.signal
                      FROM learning_samples ls JOIN signals s ON s.id=ls.signal_id
                      WHERE ls.instrument=? AND s.instrument=? AND ls.label IN (0,1) ORDER BY s.candle_ts,s.id""",
                   (instrument,instrument)).fetchall()
    last=c.execute("SELECT samples FROM model_runs WHERE instrument=? AND accepted=1 ORDER BY id DESC LIMIT 1",(instrument,)).fetchone();c.close()
    labeled=len(collapse_market_episodes(rows,gap_minutes=RESEARCH_EPISODE_GAP_MINUTES))
    last_n=int(last["samples"]) if last else 0
    threshold=ML_MIN_SAMPLES if not last else last_n+MODEL_MIN_NEW_LABELS
    return {"ready":labeled>=threshold,"labeled":labeled,"last_model_samples":last_n,"next_training_at":threshold,
            "sample_unit":"market_episode","episode_gap_minutes":RESEARCH_EPISODE_GAP_MINUTES,"instrument":instrument}


def resolve_one(sample: sqlite3.Row, m1: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Backward-compatible wrapper around the research outcome engine."""
    payload=dict(sample) if not isinstance(sample, dict) else sample
    return research_resolve_outcome(
        payload, m1, horizon_bars=OUTCOME_HORIZON_MIN,
        round_trip_cost_pips=RESEARCH_ROUND_TRIP_COST_PIPS,
    )



def resolve_ensemble_actual_outcome(signal_id: int, label: int) -> Dict[str,Any]:
    """Attach only ACTUAL canonical outcomes to the shadow ensemble.

    Opposing model signals are not marked as winners/losers from the canonical trade
    because that would be a counterfactual assumption. They require their own shadow/
    paper resolution path later.
    """
    if not ENSEMBLE_ENABLED:
        return {"enabled":False}
    c=conn();sig=c.execute("SELECT * FROM signals WHERE id=?",(int(signal_id),)).fetchone()
    if not sig or not sig["ensemble_decision_id"]:
        c.close();return {"enabled":True,"linked":False}
    out=c.execute("SELECT * FROM ensemble_outputs WHERE ensemble_decision_id=?",(sig["ensemble_decision_id"],)).fetchone()
    if not out:
        c.close();return {"enabled":True,"linked":False}
    canonical_dir="LONG" if sig["signal"]=="BUY" else "SHORT" if sig["signal"]=="SELL" else None
    actual_return=1.0 if int(label)==1 else -1.0
    updated=0
    if canonical_dir:
        cur=c.execute("""UPDATE ensemble_signals SET resolved_label=?,resolved_return=?,resolved_ts=?
                         WHERE ensemble_cycle_id=? AND role='DIRECTIONAL' AND direction=? AND resolved_label IS NULL""",
                      (int(label),actual_return,now_iso(),out["ensemble_cycle_id"],canonical_dir))
        updated=cur.rowcount
    c.execute("""UPDATE ensemble_shadow_comparisons SET actual_result=?,real_result_source='CANONICAL_RESOLVED_SIGNAL'
                 WHERE ensemble_decision_id=?""",(actual_return,sig["ensemble_decision_id"]))
    c.commit();c.close()
    return {"enabled":True,"linked":True,"ensemble_decision_id":sig["ensemble_decision_id"],
            "actual_models_resolved":updated,"counterfactual_opposing_models_resolved":0}


def resolve_pending(inst: str, m1: List[Dict[str, Any]]) -> int:
    c = conn()
    rows = c.execute("SELECT * FROM learning_samples WHERE status='PENDING' AND instrument=? ORDER BY id ASC", (inst,)).fetchall()
    resolved = 0
    for s in rows:
        out = resolve_one(s, m1)
        if out:
            c.execute("""UPDATE learning_samples SET status=?,label=?,resolved_ts=?,bars_to_resolution=?,mfe_r=?,mae_r=?,note=?,
                      outcome_cost_r=?,effective_target=?,effective_stop=? WHERE id=?""",
                      (out["status"], out["label"], now_iso(), out["bars"], out["mfe_r"], out["mae_r"], out["note"],
                       out.get("cost_r"),out.get("effective_target"),out.get("effective_stop"),s["id"]))
            resolved += 1
            c.commit()
            if out["label"] in (0, 1):
                try:
                    attach_ai_director_outcome(int(s["signal_id"]), int(out["label"]))
                except Exception as e:
                    log.warning("AI_DIRECTOR outcome link failed signal_id=%s err=%s", s["signal_id"], e)
                try:
                    resolve_ensemble_actual_outcome(int(s["signal_id"]), int(out["label"]))
                except Exception as e:
                    log.warning("ENSEMBLE outcome link failed signal_id=%s err=%s", s["signal_id"], e)
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
    shadow_episode_rows=c.execute("""SELECT st.label,s.candle_ts,s.instrument,s.signal,st.variant
                                     FROM shadow_trials st JOIN signals s ON s.id=st.signal_id""").fetchall()
    canonical_episode_rows=c.execute("""SELECT ls.label,s.candle_ts,s.instrument,s.signal
                                        FROM learning_samples ls JOIN signals s ON s.id=ls.signal_id
                                        WHERE ls.label IN (0,1)""").fetchall()
    stages={x["stage"]:x["n"] for x in c.execute("SELECT stage,COUNT(*) n FROM filter_hypotheses GROUP BY stage").fetchall()}
    c.close()
    shadow_eps_all=collapse_market_episodes(shadow_episode_rows,gap_minutes=RESEARCH_EPISODE_GAP_MINUTES)
    shadow_eps_resolved=collapse_market_episodes([x for x in shadow_episode_rows if x["label"] in (0,1)],gap_minutes=RESEARCH_EPISODE_GAP_MINUTES)
    canonical_eps=collapse_market_episodes(canonical_episode_rows,gap_minutes=RESEARCH_EPISODE_GAP_MINUTES)
    retrain_policy=should_retrain_model()
    return {
        "samples_total": total, "resolved_labeled": resolved, "pending_or_unlabeled": total - resolved,
        "pending": pending, "ambiguous": ambiguous, "timeouts": timeouts,
        "db_path": DB,
        # "recommended" means a recommendation is outstanding, not that persistence is already configured.
        "persistent_db_recommended": not DB_PERSISTENT,
        "persistent_db_configured": DB_PERSISTENT,
        "db_persistence": storage_status(),
        "win_rate_all": (wins / resolved) if resolved else None,
        "executed_resolved": executed_resolved, "win_rate_executed": (executed_wins / executed_resolved) if executed_resolved else None,
        "blocked_resolved": blocked_resolved, "counterfactual_win_rate_blocked": (blocked_wins / blocked_resolved) if blocked_resolved else None,
        "ml_min_samples": ML_MIN_SAMPLES, "model_ready": bool(shadow_model_governance_status().get("ready")), "model_governance": shadow_model_governance_status(), "last_model_run": dict(run) if run else None,
        "mode":"CONTINUOUS_RESEARCH",
        # Keep authority semantics explicit: adaptive learning itself is
        # observation-only and cannot mutate production execution. The separate
        # calibrated-confidence gate may influence whether the legacy signal
        # pipeline executes, but that is not Adaptive Learning deployment authority.
        "changes_execution":False,
        "adaptive_learning_changes_production_execution":False,
        "adaptive_confidence_gate_enabled":bool(ADAPTIVE_CONFIDENCE),
        "ml_role":"secondary_refinement","discovery_min_samples":DISCOVERY_MIN_SAMPLES,
        "shadow_lab":{"enabled":RESEARCH_LAB_ENABLED,
                      "trials_total":shadow_total,"trials_resolved_labeled":shadow_resolved,"pending":shadow_pending,
                      "episodes_total":len(shadow_eps_all),"episodes_resolved_labeled":len(shadow_eps_resolved)},
        "evidence_integrity":{"canonical_samples_raw":resolved,"canonical_episodes":len(canonical_eps),
                              "shadow_trials_raw":shadow_total,"shadow_episodes":len(shadow_eps_all)},
        "filter_research":{"experimental":stages.get("EXPERIMENTAL",0),"evaluating":stages.get("EVALUATING",0),"validated":stages.get("VALIDATED",0),"rejected":stages.get("REJECTED",0),"automatic_live_activation":False},
        "external_research":{"enabled":EXTERNAL_RESEARCH_ENABLED,"symbols":EXTERNAL_RESEARCH_SYMBOLS,
                             "granularity":EXTERNAL_RESEARCH_GRANULARITY,"news_research":EXTERNAL_NEWS_RESEARCH,
                             "automatic_live_activation":False},
        "weekend_research":{"enabled":WEEKEND_RESEARCH_ENABLED,"signal_context_hours":WEEKEND_SIGNAL_CONTEXT_HOURS,
                            "reaction_horizons_hours":list(WEEKEND_REACTION_HORIZONS),
                            "creates_trade_labels_while_closed":False},
        "strategy_self_evaluation":{"enabled":STRATEGY_SELF_EVAL_ENABLED,"auto_pause":STRATEGY_AUTO_PAUSE,
                                    "baseline_window":STRATEGY_BASELINE_WINDOW,"recent_window":STRATEGY_RECENT_WINDOW,
                                    "recovery_samples":STRATEGY_RECOVERY_SAMPLES,"health":all_strategy_health()},
        "adaptive_risk_engine":{"enabled":RISK_ENGINE_ENABLED,"shadow_mode":True,
                                "authority_over_execution":False,
                                "authority_over_position_size":False,
                                "rules_self_modifiable":False},
        "trade_memory":{"enabled":TRADE_MEMORY_ENABLED,
                        "storage":"SQLite existing DB",
                        "min_sample_size":TRADE_MEMORY_MIN_SAMPLE_SIZE,
                        "auto_strategy_changes":False,
                        "degradation":trade_memory_recent_degradation()},
        "adaptive_learning_engine":{"enabled":ADAPTIVE_LEARNING_ENABLED,
                                    "observation_only":True,
                                    "candidate_activation_authority":False,
                                    "production_mutation":False},
        "candidate_validation_pipeline":{"enabled":VALIDATION_PIPELINE_ENABLED,
                                         "maximum_state":VALIDATION_MAX_STATE,
                                         "paper_trading_mandatory":True,
                                         "auto_deploy":False},
        "retrain_policy":retrain_policy
    }


def train_shadow_model(force: bool=False, instrument: Optional[str]=None) -> Dict[str,Any]:
    instrument=InstrumentRegistry.normalize_symbol(instrument or PRIMARY_INSTRUMENT)
    model_path=shadow_model_path(instrument)
    c=conn(); rows=c.execute("""SELECT features_json,label,resolved_ts FROM learning_samples
                                 WHERE instrument=? AND label IN (0,1) ORDER BY resolved_ts,id""",
                              (instrument,)).fetchall(); c.close()
    if len(rows)<ML_MIN_SAMPLES and not force:return {"trained":False,"reason":f"need {ML_MIN_SAMPLES}, have {len(rows)}","samples":len(rows),"instrument":instrument}
    if len(rows)<20:return {"trained":False,"reason":"need at least 20 resolved samples for temporal validation","samples":len(rows),"instrument":instrument}
    X=[];y=[]
    for row in rows:
        f=json.loads(row["features_json"]);X.append([float(f.get(k,0) or 0) for k in FEATURE_COLUMNS]);y.append(int(row["label"]))
    X=np.asarray(X);y=np.asarray(y)
    if len(set(y.tolist()))<2:return {"trained":False,"reason":"need WIN and LOSS labels","samples":len(y),"instrument":instrument}
    folds=[];splits=min(5,max(2,len(y)//20))
    for tr,te in TimeSeriesSplit(n_splits=splits).split(X):
        if len(set(y[tr].tolist()))<2 or len(set(y[te].tolist()))<2:continue
        model=Pipeline([("scale",StandardScaler()),("clf",LogisticRegression(max_iter=1000,class_weight="balanced"))])
        model.fit(X[tr],y[tr]);prob=model.predict_proba(X[te])[:,1];pred=(prob>=.5).astype(int)
        folds.append({"train":len(tr),"test":len(te),"accuracy":float(accuracy_score(y[te],pred)),
          "auc":float(roc_auc_score(y[te],prob)),"log_loss":float(log_loss(y[te],prob)),
          "brier":float(brier_score_loss(y[te],prob)),"baseline":float(max(np.mean(y[te]),1-np.mean(y[te])))})
    if not folds:return {"trained":False,"reason":"insufficient class diversity across time folds","samples":len(y),"instrument":instrument}
    final=Pipeline([("scale",StandardScaler()),("clf",LogisticRegression(max_iter=1000,class_weight="balanced"))]);final.fit(X,y)
    avg={k:float(np.mean([f[k] for f in folds])) for k in ["accuracy","auc","log_loss","brier","baseline"]}
    gate=_shadow_model_acceptance({"roc_auc":avg["auc"],"accuracy":avg["accuracy"],"baseline_accuracy":avg["baseline"]})
    accepted=bool(gate["accepted"])
    if accepted:
        Path(model_path).parent.mkdir(parents=True,exist_ok=True)
        joblib.dump({"model":final,"features":FEATURE_COLUMNS,"trained_at":now_iso(),"samples":len(y),"walk_forward":folds,
                     "validation_gate":gate,"instrument":instrument},model_path)
    c=conn();c.execute("""INSERT INTO model_runs(trained_ts,samples,train_samples,test_samples,win_rate,baseline_accuracy,accuracy,roc_auc,log_loss,accepted,model_path,note,instrument)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (now_iso(),len(y),folds[-1]["train"],folds[-1]["test"],float(np.mean(y)),avg["baseline"],avg["accuracy"],avg["auc"],avg["log_loss"],int(accepted),model_path if accepted else None,
       json.dumps({"validation":"TimeSeriesSplit_walk_forward","folds":folds,"brier":avg["brier"],"acceptance_gate":gate,"instrument":instrument}),instrument));c.commit();c.close()
    return {"trained":True,"accepted":accepted,"acceptance_gate":gate,"samples":len(y),"validation":"TimeSeriesSplit_walk_forward","folds":folds,"average":avg,"instrument":instrument,"model_path":model_path}


async def replace_trade_stop(client: httpx.AsyncClient, trade_id: str, price: float) -> Dict[str, Any]:
    # Preserve the hardened public call signature while resolving precision from
    # the persisted trade namespace. Existing callers/tests therefore remain
    # compatible and GBP/JPY do not inherit EUR/USD formatting.
    instrument="EUR_USD"
    try:
        c=conn(); row=c.execute("SELECT instrument FROM active_trade_management WHERE trade_id=?",(str(trade_id),)).fetchone(); c.close()
        if row and row["instrument"]:
            instrument=str(row["instrument"])
    except Exception:
        pass
    body = {"stopLoss": {"price": format_instrument_price(instrument,price), "timeInForce": "GTC"}}
    # IMPORTANT: req()'s fourth positional argument is params, not body. Always
    # pass protective-order updates explicitly as JSON body.
    return await req(client, "PUT", f"/v3/accounts/{{account}}/trades/{trade_id}/orders", body=body)

def register_trade_management(trade_id: str, r: Dict[str, Any], target: float,
                              filled_units: Optional[float]=None,
                              entry_price: Optional[float]=None,
                              applied_stop: Optional[float]=None,
                              applied_target: Optional[float]=None):
    if not trade_id:
        return
    tscore=trend_runner_score(r)
    policy="BE_PROFIT_TRAIL"
    # Broker-confirmed fill is the real entry. When post-fill protection has
    # been broker-observed, store that effective geometry so R calculations use
    # the same SL/TP that OANDA actually holds.
    management_entry=float(entry_price if entry_price is not None else r["entry"])
    management_stop=float(applied_stop if applied_stop is not None else r["stop"])
    management_target=float(applied_target if applied_target is not None else target)
    c=conn()
    try:
        c.execute("""INSERT OR REPLACE INTO active_trade_management(
          trade_id,instrument,side,entry,initial_stop,initial_target,current_stop,setup_variant,policy,trend_score,
          opened_ts,last_r,last_action,updated_ts,closed,current_units)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)""",
          (trade_id,r["instrument"],r["signal"],management_entry,management_stop,management_target,
           management_stop,setup_variant(r),policy,tscore,now_iso(),0.0,"OPEN",now_iso(),
           abs(float(filled_units if filled_units is not None else UNITS))))
    except sqlite3.OperationalError:
        c.execute("""INSERT OR REPLACE INTO active_trade_management(
          trade_id,instrument,side,entry,initial_stop,initial_target,current_stop,setup_variant,policy,trend_score,
          opened_ts,last_r,last_action,updated_ts,closed)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
          (trade_id,r["instrument"],r["signal"],management_entry,management_stop,management_target,
           management_stop,setup_variant(r),policy,tscore,now_iso(),0.0,"OPEN",now_iso()))
    c.execute("""INSERT OR IGNORE INTO trade_forward_observations(
      trade_id,instrument,side,opened_ts,be_trigger_r,be_lock_r,max_r_seen,updated_ts)
      VALUES(?,?,?,?,?,?,0,?)""",
      (trade_id,r["instrument"],r["signal"],now_iso(),float(BREAK_EVEN_TRIGGER_R),float(BREAK_EVEN_LOCK_R),now_iso()))
    c.commit();c.close()

def _record_trade_forward_telemetry(tr: Dict[str,Any], proposal: Dict[str,Any], *, action_applied: bool=False) -> None:
    """Best-effort observational telemetry. Failures never affect trade management."""
    try:
        trade_id=str(tr["trade_id"]); r_now=float(proposal.get("r_multiple",0) or 0)
        r_prev=float(tr.get("last_r",0) or 0)
        c=conn(); now=now_iso()
        c.execute("""INSERT OR IGNORE INTO trade_forward_observations(
          trade_id,instrument,side,opened_ts,be_trigger_r,be_lock_r,max_r_seen,updated_ts)
          VALUES(?,?,?,?,?,?,?,?)""",
          (trade_id,tr.get("instrument") or "",tr.get("side") or "",tr.get("opened_ts") or now,
           float(BREAK_EVEN_TRIGGER_R),float(BREAK_EVEN_LOCK_R),max(0.0,r_now),now))
        c.execute("UPDATE trade_forward_observations SET max_r_seen=MAX(max_r_seen,?),updated_ts=? WHERE trade_id=?",
                  (r_now,now,trade_id))
        for threshold,label in ((0.50,"REACHED_0_50R"),(0.75,"REACHED_0_75R"),(1.00,"REACHED_1_00R"),(1.25,"REACHED_1_25R"),(1.50,"REACHED_1_50R")):
            if r_prev < threshold <= r_now:
                c.execute("""INSERT OR IGNORE INTO trade_forward_events(trade_id,ts,event,r_multiple,detail_json)
                  VALUES(?,?,?,?,?)""",(trade_id,now,label,r_now,"{}"))
        row=c.execute("SELECT be_activated_ts FROM trade_forward_observations WHERE trade_id=?",(trade_id,)).fetchone()
        be_was_active=bool(row and row["be_activated_ts"])
        if action_applied and proposal.get("action") in ("BREAK_EVEN","PROFIT_LOCK","TRAIL","TREND_RUNNER_TRAIL") and not be_was_active:
            c.execute("""UPDATE trade_forward_observations SET be_activated_ts=?,be_activation_r=?,max_r_after_be=?,updated_ts=? WHERE trade_id=?""",
                      (now,r_now,r_now,now,trade_id))
            c.execute("""INSERT OR IGNORE INTO trade_forward_events(trade_id,ts,event,r_multiple,detail_json)
              VALUES(?,?,?,?,?)""",(trade_id,now,"BE_ACTIVATED",r_now,json.dumps({"trigger_r":BREAK_EVEN_TRIGGER_R,"lock_r":BREAK_EVEN_LOCK_R,"action":proposal.get("action")},separators=(",",":"))))
            be_was_active=True
        if be_was_active:
            c.execute("UPDATE trade_forward_observations SET max_r_after_be=MAX(COALESCE(max_r_after_be,?),?),updated_ts=? WHERE trade_id=?",
                      (r_now,r_now,now,trade_id))
        c.commit();c.close()
    except Exception as e:
        log.warning("Forward trade telemetry failed trade=%s err=%s",tr.get("trade_id"),e)

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
                _record_trade_forward_telemetry(tr,proposal,action_applied=True)
                changed += 1
            except Exception as e:
                log.exception("Trade management update failed: %s",e)
                # Keep forward telemetry current even when the broker rejects the
                # protective-order update; the prior implementation froze last_r
                # at the last pre-trigger value and hid the failure in stdout.
                try:
                    c=conn()
                    c.execute("UPDATE active_trade_management SET last_r=?,updated_ts=? WHERE trade_id=?",
                              (float(proposal["r_multiple"]),now_iso(),tr["trade_id"]))
                    c.commit(); c.close()
                except Exception as db_e:
                    log.warning("Trade management failure-state persistence failed trade=%s err=%s",tr.get("trade_id"),db_e)
                _record_trade_forward_telemetry(tr,proposal,action_applied=False)
                failure_details={
                    "trade_id":str(tr.get("trade_id") or ""),
                    "instrument":str(tr.get("instrument") or instrument),
                    "side":str(tr.get("side") or ""),
                    "action":str(proposal.get("action") or "NONE"),
                    "r_multiple":float(proposal.get("r_multiple",0) or 0),
                    "current_stop":old,
                    "proposed_stop":float(proposal.get("new_stop",old) or old),
                    "current_price":float(current_price),
                    "error":str(e),
                }
                if OBSERVABILITY_ENABLED:
                    try:
                        observability_manager.alert(
                            f"TRADE_MANAGEMENT_UPDATE_FAILED:{tr['trade_id']}","CRITICAL","Execution Engine",
                            "PROTECTIVE_ORDER_UPDATE_FAILED",
                            "Broker rejected or failed a protective stop update; trade management will retry on later scans",
                            details=failure_details)
                    except Exception as obs_e:
                        log.warning("Trade management observability alert failed trade=%s err=%s",tr.get("trade_id"),obs_e)
                if RECOVERY_MANAGER_ENABLED:
                    try:
                        recovery_manager.journal("PROTECTIVE_ORDER_UPDATE_FAILED",payload=failure_details)
                    except Exception as rec_e:
                        log.warning("Trade management recovery journal failed trade=%s err=%s",tr.get("trade_id"),rec_e)
        else:
            c=conn()
            c.execute("UPDATE active_trade_management SET last_r=?,updated_ts=? WHERE trade_id=?",
                      (float(proposal["r_multiple"]),now_iso(),tr["trade_id"]))
            c.commit(); c.close()
            _record_trade_forward_telemetry(tr,proposal,action_applied=False)
    return changed


def _obs_module(module,status="OK",latency_ms=None,errors=None,warnings=None,last_operation=None,details=None):
    if not OBSERVABILITY_ENABLED:return None
    result=observability_manager.heartbeat(
        module,OBSERVABILITY_DEPENDENCIES.get(module,DEPENDENCY_NON_CRITICAL),status,
        latency_ms=latency_ms,errors=errors,warnings=warnings,
        last_successful_operation=last_operation,details=details)
    if status=="OK": observability_manager.recover(f"HEARTBEAT:{module}",f"{module} heartbeat recovered")
    return result


def _obs_latest_market_age(candles_: List[Dict[str,Any]]) -> Optional[float]:
    if not candles_:return None
    t=candles_[-1].get("t")
    if isinstance(t,datetime):dt=t
    else:dt=_parse_iso(str(t))
    return (datetime.now(timezone.utc)-dt).total_seconds() if dt else None


def observability_market_data_update(inst: str,m1: List[Dict[str,Any]],fetch_latency_ms: float) -> Dict[str,Any]:
    age=_obs_latest_market_age(m1)
    market_closed=market_is_weekend_closed()
    stale=(not market_closed) and (age is None or age>OBSERVABILITY_MARKET_STALE_SECONDS)
    fresh=(not market_closed) and (not stale)
    market_data_state="MARKET_CLOSED" if market_closed else ("STALE" if stale else "FRESH")
    # MARKET_CLOSED is healthy for monitoring/research but is not equivalent to
    # fresh tradable market data. This prevents weekend candles from being
    # reported as fresh merely because the market is intentionally closed.
    status="STALE" if stale else ("MARKET_CLOSED" if market_closed else "OK")
    _obs_module("Market Data",status,fetch_latency_ms,
                errors=["MARKET_DATA_STALE"] if stale else [],
                warnings=["MARKET_CLOSED"] if market_closed else [],
                last_operation="M1 candle batch received" if (fresh or market_closed) else None,
                details={"instrument":inst,"last_candle":m1[-1]["t"].isoformat() if m1 else None,
                         "market_data_age_seconds":age,"market_closed":market_closed,"market_data_state":market_data_state,
                         "fresh_for_trading":fresh,"tick_feed":"NOT_AVAILABLE","order_book":"NOT_AVAILABLE"})
    key=f"MARKET_DATA_STALE:{inst}"
    if stale:
        observability_manager.alert(key,"CRITICAL","Market Data","MARKET_DATA_STALE",
                                    f"{inst} market data is stale",group_key="MARKET_DATA_STALE",
                                    details={"age_seconds":age,"threshold":OBSERVABILITY_MARKET_STALE_SECONDS})
    else:
        observability_manager.recover(key,f"{inst} market data recovered/closed normally",
                                      {"age_seconds":age,"market_closed":market_closed,"market_data_state":market_data_state})
    return {"stale":stale,"fresh":fresh,"market_closed":market_closed,"market_data_state":market_data_state,
            "age_seconds":age,"fetch_latency_ms":fetch_latency_ms}


def _obs_realized_period_pnl(days: int) -> float:
    cutoff=(datetime.now(timezone.utc)-timedelta(days=days)).isoformat()
    c=conn();row=c.execute("SELECT COALESCE(SUM(net_result),0) v FROM trade_memory WHERE status='CLOSED' AND exit_ts>=?",(cutoff,)).fetchone();c.close()
    return float(row["v"] or 0)


async def observability_broker_snapshot(client: httpx.AsyncClient) -> Dict[str,Any]:
    t0=time.perf_counter()
    try:
        summary,positions=await asyncio.gather(
            req(client,"GET","/v3/accounts/{account}/summary"),
            req(client,"GET","/v3/accounts/{account}/openPositions")
        )
        latency=(time.perf_counter()-t0)*1000
        a=summary.get("account") or {}
        nav=_risk_float(a.get("NAV"));balance=_risk_float(a.get("balance"));margin=_risk_float(a.get("marginUsed"),0.0)
        margin_usage=(margin/nav) if nav and nav>0 and margin is not None else None
        broker_instruments=[x.get("instrument") for x in positions.get("positions",[]) if x.get("instrument")]
        c=conn();internal=[x["instrument"] for x in c.execute("SELECT DISTINCT instrument FROM active_trade_management WHERE closed=0").fetchall()]
        ps=c.execute("SELECT * FROM portfolio_risk_state WHERE id=1").fetchone()
        prev_cap=c.execute("SELECT MAX(COALESCE(peak_equity,equity)) peak FROM observability_capital_history").fetchone();c.close()
        peak=max(nav or 0,float(prev_cap["peak"] or 0)) if nav is not None else _risk_float(prev_cap["peak"])
        drawdown=max(0.0,(peak-nav)/peak) if nav is not None and peak and peak>0 else None
        open_risk=float(ps["portfolio_open_risk"] or 0) if ps else 0.0
        exposure=_risk_float(a.get("positionValue"),margin)
        snap={"ok":True,"latency_ms":latency,"equity":nav,"cash":balance,
              "unrealized_pnl":_risk_float(a.get("unrealizedPL")),"realized_pnl":_risk_float(a.get("pl")),
              "daily_pnl":_obs_realized_period_pnl(1),"weekly_pnl":_obs_realized_period_pnl(7),
              "drawdown":drawdown,"peak_equity":peak,"exposure":exposure,"margin_usage":margin_usage,
              "open_risk":open_risk,"remaining_risk_budget":max(0.0,float(managed_value("risk.max_portfolio_fraction",RISK_MAX_PORTFOLIO_FRACTION))-open_risk),
              "broker_instruments":broker_instruments,"internal_instruments":internal,"account":a}
        observability_manager.record_capital(snap,"OANDA_PRACTICE")
        state.setdefault("observability",{})["last_broker_snapshot"]={
            "ts":now_iso(),"ok":True,"latency_ms":latency,"nav":nav,"balance":balance,
            "margin_usage":margin_usage,"open_positions":len(broker_instruments),
            "open_risk":open_risk,"reconciliation":snap.get("reconciliation")
        }
        state["observability"]["last_refresh"]=now_iso()
        _obs_module("Broker Connection","OK",latency,last_operation="account summary/open positions",
                    details={"nav":nav,"margin_usage":margin_usage,"open_positions":len(broker_instruments),"broker_instruments":broker_instruments})
        observability_manager.recover("BROKER_DISCONNECTED","Broker connection recovered",{"latency_ms":latency})
        rec=observability_reconciliation_status(internal,broker_instruments)
        snap["reconciliation"]=rec
        state.setdefault("observability",{}).setdefault("last_broker_snapshot",{})["reconciliation"]=rec
        if rec["status"]!="CONSISTENT":
            observability_manager.alert("POSITION_STATE_MISMATCH","HIGH","Execution Engine","STATE_RECONCILIATION_REQUIRED",
                "Broker positions differ from internal managed positions",details=rec)
        else:
            observability_manager.recover("POSITION_STATE_MISMATCH","Broker/internal position state reconciled",rec)
        if ps and ps["margin_usage"] is not None and margin_usage is not None and abs(float(ps["margin_usage"])-float(margin_usage))>.05:
            observability_manager.alert("RISK_BROKER_STATE_MISMATCH","HIGH","Risk Engine","STATE_RECONCILIATION_REQUIRED",
                "Risk Engine margin state differs materially from broker account state",
                details={"risk_margin_usage":ps["margin_usage"],"broker_margin_usage":margin_usage})
        else:
            observability_manager.recover("RISK_BROKER_STATE_MISMATCH","Risk/broker exposure state reconciled")
        if drawdown is not None:
            if drawdown>=float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP)):
                observability_manager.alert("DRAWDOWN_CRITICAL","CRITICAL","Risk Engine","CRITICAL_DRAWDOWN",
                    "Account drawdown reached critical risk limit",details={"drawdown":drawdown,"limit":float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP))})
            elif drawdown>=OBSERVABILITY_DRAWDOWN_WARNING_FRACTION:
                observability_manager.alert("DRAWDOWN_WARNING","HIGH","Risk Engine","DRAWDOWN_WARNING",
                    "Account drawdown is approaching the hard limit",details={"drawdown":drawdown,"hard_limit":float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP))})
                observability_manager.recover("DRAWDOWN_CRITICAL")
            else:
                observability_manager.recover("DRAWDOWN_WARNING");observability_manager.recover("DRAWDOWN_CRITICAL")
        return snap
    except Exception as e:
        latency=(time.perf_counter()-t0)*1000
        _obs_module("Broker Connection","ERROR",latency,errors=[str(e)])
        observability_manager.alert("BROKER_DISCONNECTED","CRITICAL","Broker Connection","BROKER_DISCONNECTED",
                                    "Broker connection/read failed",details={"error":str(e),"latency_ms":latency})
        state.setdefault("observability",{})["last_broker_snapshot"]={"ts":now_iso(),"ok":False,"latency_ms":latency,"error":str(e)}
        state["observability"]["last_refresh"]=now_iso()
        return {"ok":False,"latency_ms":latency,"error":str(e),"broker_instruments":[]}


def observability_silent_anomalies() -> List[Dict[str,Any]]:
    findings=[]
    if market_is_weekend_closed():return findings
    c=conn()
    latest_signal=c.execute("SELECT ts FROM signals WHERE signal IN ('BUY','SELL') ORDER BY id DESC LIMIT 1").fetchone()
    latest_tm=c.execute("SELECT updated_ts FROM trade_memory ORDER BY id DESC LIMIT 1").fetchone()
    latest_exec=c.execute("SELECT ts FROM execution_audit ORDER BY id DESC LIMIT 1").fetchone()
    risk_rows=[dict(x) for x in c.execute("SELECT risk_multiplier,market_regime,ts FROM adaptive_risk_decisions ORDER BY id DESC LIMIT ?",(OBSERVABILITY_RISK_CONSTANT_WINDOW,)).fetchall()]
    regime_rows=[dict(x) for x in c.execute("SELECT market_regime,ts FROM market_regime_history ORDER BY id DESC LIMIT 100").fetchall()]
    c.close()
    sigdt=_parse_iso(latest_signal["ts"]) if latest_signal else None
    if sigdt and (datetime.now(timezone.utc)-sigdt).total_seconds()>OBSERVABILITY_SIGNAL_SILENCE_HOURS*3600:
        x={"event":"NO_STRATEGY_SIGNALS","age_hours":(datetime.now(timezone.utc)-sigdt).total_seconds()/3600};findings.append(x)
        observability_manager.alert("NO_STRATEGY_SIGNALS","WARNING","Strategies","SILENT_BEHAVIOR_ANOMALY",
                                    "No BUY/SELL strategy signals for an unusually long period",details=x)
    else:observability_manager.recover("NO_STRATEGY_SIGNALS")
    if len(risk_rows)>=OBSERVABILITY_RISK_CONSTANT_WINDOW:
        vals=[round(float(x["risk_multiplier"] or 0),8) for x in risk_rows];regimes={x.get("market_regime") for x in risk_rows}
        if len(set(vals))==1 and len(regimes)>=2:
            x={"event":"RISK_OUTPUT_CONSTANT","value":vals[0],"samples":len(vals),"regimes":sorted(str(r) for r in regimes)};findings.append(x)
            observability_manager.alert("RISK_OUTPUT_CONSTANT","WARNING","Risk Engine","SILENT_BEHAVIOR_ANOMALY",
                                        "Risk Engine output has remained exactly constant across different regimes",details=x)
        else:observability_manager.recover("RISK_OUTPUT_CONSTANT")
    if len(regime_rows)>=20:
        unique={x["market_regime"] for x in regime_rows};old=_parse_iso(regime_rows[-1]["ts"])
        span=(datetime.now(timezone.utc)-old).total_seconds()/3600 if old else 0
        if len(unique)==1 and span>=OBSERVABILITY_REGIME_STATIC_HOURS:
            x={"event":"REGIME_STATIC_TOO_LONG","regime":next(iter(unique)),"hours":span};findings.append(x)
            observability_manager.alert("REGIME_STATIC_TOO_LONG","WARNING","Market Regime Detector","SILENT_BEHAVIOR_ANOMALY",
                                        "Market regime has not changed for an unusually long observation span",details=x)
        else:observability_manager.recover("REGIME_STATIC_TOO_LONG")
    # Trade Memory should follow executed trades.
    if latest_exec:
        exdt=_parse_iso(latest_exec["ts"]);tmdt=_parse_iso(latest_tm["updated_ts"]) if latest_tm else None
        if exdt and (tmdt is None or tmdt<exdt-timedelta(minutes=5)):
            x={"event":"TRADE_MEMORY_LAGGING","execution_ts":latest_exec["ts"],"memory_ts":latest_tm["updated_ts"] if latest_tm else None};findings.append(x)
            observability_manager.alert("TRADE_MEMORY_LAGGING","HIGH","Trade Memory","SILENT_BEHAVIOR_ANOMALY",
                                        "Executed trades are newer than Trade Memory persistence",details=x)
        else:observability_manager.recover("TRADE_MEMORY_LAGGING")
    return findings


def observability_refresh_noncritical_modules():
    c=conn()
    al=c.execute("SELECT completed_ts,status,summary_json FROM adaptive_learning_runs ORDER BY id DESC LIMIT 1").fetchone()
    vr=c.execute("SELECT completed_ts,final_status FROM candidate_validation_runs ORDER BY completed_ts DESC LIMIT 1").fetchone()
    paper=c.execute("SELECT MAX(created_ts) ts,COUNT(*) n FROM candidate_paper_trades").fetchone()
    c.close()
    _obs_module("Adaptive Learning","OK" if ADAPTIVE_LEARNING_ENABLED else "PAUSED",last_operation=al["completed_ts"] if al else None,
                details={"latest_status":al["status"] if al else "NO_RUN_YET"})
    _obs_module("Validation Pipeline","OK" if VALIDATION_PIPELINE_ENABLED else "PAUSED",last_operation=vr["completed_ts"] if vr else None,
                details={"latest_status":vr["final_status"] if vr else "NO_VALIDATION_YET"})
    _obs_module("Paper Trading","OK" if VALIDATION_PIPELINE_ENABLED else "PAUSED",last_operation=paper["ts"] if paper else None,
                details={"paper_records":int(paper["n"] or 0) if paper else 0})
    try:
        d=deployment_manager.dashboard();_obs_module("Deployment Manager","OK",last_operation=now_iso(),details={"deployments":len(d.get("deployments",[]))})
    except Exception as e:
        _obs_module("Deployment Manager","ERROR",errors=[str(e)])
        observability_manager.alert("DEPLOYMENT_MANAGER_ERROR","HIGH","Deployment Manager","MODULE_ERROR",
                                    "Deployment Manager state could not be read",details={"error":str(e)})


def observability_strategy_degradation_summary() -> List[Dict[str,Any]]:
    c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM trade_memory_degradation ORDER BY ts DESC").fetchall()]
    drift={x["scope_key"]:dict(x) for x in c.execute("SELECT * FROM concept_drift_alerts").fetchall()};c.close()
    out=[]
    for r in rows:
        st=observability_degradation_state(r.get("historical_expectancy"),r.get("recent_expectancy"),
                                           r.get("historical_profit_factor"),r.get("recent_profit_factor"),
                                           bool(drift.get(r["scope_key"],{}).get("status")=="POSSIBLE_CONCEPT_DRIFT"))
        x={"scope_key":r["scope_key"],"strategy":r["strategy"],"regime":r.get("market_regime"),"state":st,
           "historical_pf":r.get("historical_profit_factor"),"recent_pf":r.get("recent_profit_factor"),
           "historical_expectancy":r.get("historical_expectancy"),"recent_expectancy":r.get("recent_expectancy"),
           "degradation_status":r.get("status"),"concept_drift":drift.get(r["scope_key"])}
        out.append(x)
        key="STRATEGY_DEGRADATION:"+r["scope_key"]
        if st=="CRITICAL_DEGRADATION":
            observability_manager.alert(key,"HIGH","Strategies","CRITICAL_DEGRADATION",
                                        "Strategy/regime behavior shows critical degradation",details=x)
        elif st=="DEGRADING":
            observability_manager.alert(key,"WARNING","Strategies","STRATEGY_DEGRADING",
                                        "Strategy/regime behavior is degrading",details=x)
        else:observability_manager.recover(key)
    return out


def observability_trace_bundle(identifier: str) -> Dict[str,Any]:
    c=conn()
    tr=c.execute("""SELECT * FROM observability_traces WHERE correlation_id=? OR trade_id=? OR order_id=? OR CAST(signal_id AS TEXT)=?
                    ORDER BY created_ts DESC LIMIT 1""",(identifier,identifier,identifier,identifier)).fetchone()
    if not tr:
        live=c.execute("SELECT signal_id FROM deployment_live_trades WHERE trade_id=? OR order_id=? ORDER BY id DESC LIMIT 1",(identifier,identifier)).fetchone()
        if live: tr=c.execute("SELECT * FROM observability_traces WHERE signal_id=? ORDER BY created_ts DESC LIMIT 1",(live["signal_id"],)).fetchone()
    if not tr:c.close();return {"error":"TRACE_NOT_FOUND","identifier":identifier}
    t=dict(tr);sid=t.get("signal_id");did=t.get("decision_id");rid=t.get("risk_decision_id");trade=t.get("trade_id")
    def one(sql,args):
        r=c.execute(sql,args).fetchone();return dict(r) if r else None
    bundle={"trace":t,
            "signal":one("SELECT * FROM signals WHERE id=?",(sid,)) if sid else None,
            "ai_director":one("SELECT * FROM ai_strategy_director_decisions WHERE id=?",(did,)) if did else None,
            "risk_engine":one("SELECT * FROM adaptive_risk_decisions WHERE id=?",(rid,)) if rid else None,
            "execution":one("SELECT * FROM execution_audit WHERE signal_id=? ORDER BY id DESC LIMIT 1",(sid,)) if sid else None,
            "trade_memory":one("SELECT * FROM trade_memory WHERE trade_id=? OR signal_id=? ORDER BY id DESC LIMIT 1",(trade,sid)) if (trade or sid) else None,
            "candidate_paper":[dict(x) for x in c.execute("SELECT * FROM candidate_paper_trades WHERE signal_id=? ORDER BY id",(sid,)).fetchall()] if sid else [],
            "candidate_live":[dict(x) for x in c.execute("SELECT * FROM deployment_live_trades WHERE signal_id=? ORDER BY id",(sid,)).fetchall()] if sid else [],
            "decision_log":one("SELECT * FROM decision_log WHERE instrument=? AND candle_ts=(SELECT candle_ts FROM signals WHERE id=?) ORDER BY id DESC LIMIT 1",(t.get("symbol"),sid)) if sid else None,
            "structured_logs":[dict(x) for x in c.execute("SELECT * FROM observability_structured_logs WHERE correlation_id=? ORDER BY id",(t["correlation_id"],)).fetchall()]}
    c.close();return bundle


async def observability_startup_health_check() -> Dict[str,Any]:
    checks={};reconciliation={};broker={}
    # Database
    t=time.perf_counter()
    try:
        c=conn();c.execute("SELECT 1").fetchone();c.close();lat=(time.perf_counter()-t)*1000
        checks["database"]={"ok":True,"latency_ms":lat};_obs_module("Database","OK",lat,last_operation="startup SELECT 1")
    except Exception as e:
        checks["database"]={"ok":False,"error":str(e)};_obs_module("Database","ERROR",errors=[str(e)])
    storage=storage_status()
    storage_ok=bool(storage["persistent"] or not PERSISTENCE_REQUIRED)
    checks["storage"]={"ok":storage_ok,**storage}
    _obs_module("Persistent Storage","OK" if storage["persistent"] else "DEGRADED",
                warnings=[] if storage["persistent"] else [storage.get("action") or "persistent storage not configured"],
                last_operation="storage configuration validated",details=storage)
    if not storage["persistent"]:
        observability_manager.alert("PERSISTENT_STORAGE_NOT_CONFIGURED",
            "HIGH" if PERSISTENCE_REQUIRED else "WARNING","Database","PERSISTENT_STORAGE_NOT_CONFIGURED",
            "Learning/model storage is ephemeral and will not survive a Railway redeploy.",details=storage)
    else:
        observability_manager.recover("PERSISTENT_STORAGE_NOT_CONFIGURED","Persistent learning/model storage is configured",storage)
    async with httpx.AsyncClient() as client:
        broker=await observability_broker_snapshot(client);checks["broker"]={"ok":bool(broker.get("ok")),"latency_ms":broker.get("latency_ms")}
        # Every execution-enabled instrument is critical; shadow-only symbols are
        # observed separately and cannot take EUR/USD offline when unavailable.
        market_checks={}
        for startup_inst in INSTRUMENTS:
            try:
                t=time.perf_counter();m1=await candles(client,startup_inst,"M1",60);lat=(time.perf_counter()-t)*1000
                mh=observability_market_data_update(startup_inst,m1,lat)
                market_checks[startup_inst]={"ok":(not mh["stale"]),**mh}
            except Exception as e:
                market_checks[startup_inst]={"ok":False,"error":str(e)}
        checks["market_data"]={"ok":bool(market_checks) and all(x.get("ok") for x in market_checks.values()),"by_instrument":market_checks}
        for shadow_inst in SHADOW_INSTRUMENTS:
            try:
                t=time.perf_counter();sm1=await candles(client,shadow_inst,"M1",60);slat=(time.perf_counter()-t)*1000
                checks.setdefault("shadow_market_data",{})[shadow_inst]={"ok":True,**observability_market_data_update(shadow_inst,sm1,slat)}
            except Exception as e:
                checks.setdefault("shadow_market_data",{})[shadow_inst]={"ok":False,"error":str(e)}
    # Positions reconciliation is broker source of truth when available.
    reconciliation=broker.get("reconciliation") or {"status":"UNKNOWN"}
    checks["positions_reconciled"]={"ok":reconciliation.get("status")=="CONSISTENT","detail":reconciliation}
    # Risk Engine readiness: pure calculation with conservative startup context.
    try:
        test=adaptive_risk_recommendation(PRIMARY_INSTRUMENT,"STARTUP_HEALTH",
            {"market_regime":"RANGE","confidence":.5,"volatility_state":"NORMAL","trend_strength":0},
            {"confidence":.5},.5,
            {"nav":broker.get("equity"),"current_drawdown":broker.get("drawdown"),"margin_usage":broker.get("margin_usage"),
             "portfolio_open_risk":broker.get("open_risk",0),"open_instruments":broker.get("broker_instruments",[]),
             "consecutive_losses":0,"data_stale":not broker.get("ok",False),"system_abnormal":not broker.get("ok",False)},UNITS)
        checks["risk_engine"]={"ok":bool(test.get("enabled")),"allow":test.get("allow_new_trades")};_obs_module("Risk Engine","OK",last_operation="startup risk calculation")
    except Exception as e:
        checks["risk_engine"]={"ok":False,"error":str(e)};_obs_module("Risk Engine","ERROR",errors=[str(e)])
    try:
        c=conn();n=c.execute("SELECT COUNT(*) n FROM strategy_health").fetchone()["n"];c.close()
        checks["strategies_loaded"]={"ok":True,"strategy_health_records":n};_obs_module("Strategies","OK",last_operation="strategy states loaded")
    except Exception as e:checks["strategies_loaded"]={"ok":False,"error":str(e)}
    try:
        d=deployment_manager.dashboard();checks["deployments_loaded"]={"ok":True,"deployments":len(d.get("deployments",[]))};_obs_module("Deployment Manager","OK",last_operation="deployment states loaded")
    except Exception as e:
        checks["deployments_loaded"]={"ok":False,"error":str(e)};_obs_module("Deployment Manager","ERROR",errors=[str(e)])
    critical_keys=["database","broker","market_data","positions_reconciled","risk_engine","strategies_loaded","deployments_loaded"]
    if PERSISTENCE_REQUIRED: critical_keys.append("storage")
    ready=all(bool(checks.get(k,{}).get("ok")) for k in critical_keys)
    status="SYSTEM_READY" if ready else "STARTUP_HEALTH_FAILED"
    state["system_ready"]=ready;state["startup_health"]={"status":status,"checks":checks,"reconciliation":reconciliation,"ts":now_iso()}
    observability_manager.startup_record(status,checks,reconciliation,{"startup_block_trading":OBSERVABILITY_STARTUP_BLOCK_TRADING})
    if ready:
        observability_manager.recover("STARTUP_HEALTH_FAILED","Startup health recovered; SYSTEM_READY",checks)
    else:
        observability_manager.alert("STARTUP_HEALTH_FAILED","CRITICAL","System","STARTUP_HEALTH_FAILURE",
                                    "Startup health check did not reach SYSTEM_READY",details=checks)
    return state["startup_health"]



def run_system_evaluation(as_of: Optional[str]=None,source: str="periodic") -> Dict[str,Any]:
    if not SYSTEM_EVALUATION_ENABLED:
        return {"enabled":False,"status":"DISABLED","autonomous_actions":False}
    try:
        system_evaluation_engine.risk_drawdown_limit=float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP))
        system_evaluation_engine.min_samples=int(managed_value("system_evaluation.min_samples",SYSTEM_EVALUATION_MIN_SAMPLES))
        system_evaluation_engine.report_period_hours=int(managed_value("system_evaluation.period_hours",SYSTEM_EVALUATION_PERIOD_HOURS))
        raw_weights={
            "trading":float(managed_value("system_evaluation.trading_weight",SYSTEM_EVALUATION_TRADING_WEIGHT)),
            "risk":float(managed_value("system_evaluation.risk_weight",SYSTEM_EVALUATION_RISK_WEIGHT)),
            "operational":float(managed_value("system_evaluation.operational_weight",SYSTEM_EVALUATION_OPERATIONAL_WEIGHT)),
            "stability":float(managed_value("system_evaluation.stability_weight",SYSTEM_EVALUATION_STABILITY_WEIGHT))
        }
        total_w=sum(max(0.0,v) for v in raw_weights.values())
        if total_w<=0:
            raw_weights={"trading":.30,"risk":.30,"operational":.25,"stability":.15};total_w=1.0
        system_evaluation_engine.score_weights={k:max(0.0,v)/total_w for k,v in raw_weights.items()}
        result=system_evaluation_engine.evaluate(as_of)
        if OBSERVABILITY_ENABLED:
            _obs_module("System Evaluation Engine","OK",
                        last_operation=f"evaluate:{source}",
                        details={"system_status":result["system_status"],
                                 "system_score":result["system_score"],
                                 "main_degradation":(result["degradation"]["types"] or [None])[0],
                                 "recommendations":[x["recommendation"] for x in result["recommendations"][:5]],
                                 "observation_only":True})
            deg=result.get("degradation") or {}
            if deg.get("detected"):
                observability_manager.alert(
                    "SYSTEM_DEGRADATION_DETECTED","HIGH","System Evaluation Engine",
                    "SYSTEM_DEGRADATION_DETECTED",
                    f"System evaluation detected {deg.get('classification')}",
                    details={"evaluation_id":result["evaluation_id"],
                             "types":deg.get("types"),"factors":deg.get("factors"),
                             "system_score":result.get("system_score")})
            else:
                observability_manager.recover("SYSTEM_DEGRADATION_DETECTED",
                                              "System evaluation no longer detects material degradation",
                                              {"evaluation_id":result["evaluation_id"]})
            if result.get("system_status") in ("CRITICAL","PAUSED"):
                observability_manager.alert(
                    "SYSTEM_EVALUATION_CRITICAL","CRITICAL","System Evaluation Engine",
                    "SYSTEM_CRITICAL",
                    f"System evaluation status is {result.get('system_status')}",
                    details={"evaluation_id":result["evaluation_id"],
                             "system_score":result.get("system_score"),
                             "dimensions":result.get("dimensions")})
            else:
                observability_manager.recover("SYSTEM_EVALUATION_CRITICAL",
                                              "System evaluation is not CRITICAL/PAUSED",
                                              {"evaluation_id":result["evaluation_id"]})
            if (result.get("model_reality_gap") or {}).get("status")=="MODEL_REALITY_GAP":
                observability_manager.alert(
                    "MODEL_REALITY_GAP","HIGH","System Evaluation Engine","MODEL_REALITY_GAP",
                    "Backtest/paper/live performance divergence is material",
                    details=result["model_reality_gap"])
            else:
                observability_manager.recover("MODEL_REALITY_GAP","Model/reality gap recovered")
            if (result.get("diversification") or {}).get("status")=="HIDDEN_CONCENTRATION_RISK":
                observability_manager.alert(
                    "HIDDEN_CONCENTRATION_RISK","HIGH","System Evaluation Engine","HIDDEN_CONCENTRATION_RISK",
                    "Highly correlated strategy return streams detected",
                    details=result["diversification"])
            else:
                observability_manager.recover("HIDDEN_CONCENTRATION_RISK","Hidden concentration is below alert threshold")
            if (result.get("regime_coverage") or {}).get("status")=="REGIME_COVERAGE_GAP":
                observability_manager.alert(
                    "REGIME_COVERAGE_GAP","WARNING","System Evaluation Engine","REGIME_COVERAGE_GAP",
                    "Observed market regimes contain strategy coverage gaps",
                    details=result["regime_coverage"])
            else:
                observability_manager.recover("REGIME_COVERAGE_GAP","Regime coverage gap recovered")
            if (result.get("data_quality") or {}).get("score",1.0)<.75:
                observability_manager.alert(
                    "DATA_QUALITY_DEGRADATION","HIGH","System Evaluation Engine","DATA_QUALITY_DEGRADATION",
                    "Data quality reduced evaluation confidence",
                    details=result["data_quality"])
            else:
                observability_manager.recover("DATA_QUALITY_DEGRADATION","Data quality score recovered")
            if "EXECUTION_DEGRADATION" in (result.get("degradation") or {}).get("types",[]):
                observability_manager.alert(
                    "EXECUTION_DEGRADATION","HIGH","System Evaluation Engine","EXECUTION_DEGRADATION",
                    "Execution quality degradation detected",
                    details={"trading":result.get("trading"),"operational":result.get("operational")})
            else:
                observability_manager.recover("EXECUTION_DEGRADATION","Execution quality is within expected range")
        return result
    except Exception as e:
        if OBSERVABILITY_ENABLED:
            _obs_module("System Evaluation Engine","ERROR",errors=[str(e)])
            observability_manager.alert("SYSTEM_EVALUATION_FAILED","HIGH","System Evaluation Engine",
                                        "SYSTEM_EVALUATION_FAILED",
                                        "System evaluation cycle failed",
                                        details={"error":str(e),"source":source})
        return {"enabled":True,"status":"FAILED","error":str(e),"autonomous_actions":False}



def run_governance_cycle(trigger: str="periodic") -> Dict[str,Any]:
    if not GOVERNANCE_ENABLED:
        return {"enabled":False,"mode":"DISABLED"}
    try:
        sync_governance_runtime_config()
        result=governance_engine.evaluate(trigger)
        meta=result.get("meta") or {}
        if OBSERVABILITY_ENABLED:
            _obs_module("Governance Engine","OK",last_operation=f"governance:{trigger}",
                        details={"mode":result.get("governance_mode"),
                                 "meta_risk_score":result.get("meta_risk_score"),
                                 "meta_risk_state":result.get("meta_risk_state"),
                                 "adaptation_state":result.get("adaptation_state"),
                                 "recommended_state":result.get("recommended_state"),
                                 "decision":result.get("decision"),
                                 "would_block":result.get("would_block"),
                                 "enforced":result.get("enforced")})
            alert_specs=[
                ("ADAPTATION_LOOP_DETECTED",bool((meta.get("adaptation_loop") or {}).get("detected")),"HIGH",
                 "Adaptation loop/churn pattern detected",meta.get("adaptation_loop")),
                ("MODULE_DECISION_CONFLICT",bool(meta.get("conflicts")),"HIGH",
                 "Adaptive modules have materially conflicting decisions",meta.get("conflicts")),
                ("META_RISK_HIGH",meta.get("state")=="HIGH","HIGH",
                 "Meta-risk is HIGH",{"score":meta.get("score"),"components":meta.get("components")}),
                ("META_RISK_CRITICAL",meta.get("state")=="CRITICAL","CRITICAL",
                 "Meta-risk is CRITICAL",{"score":meta.get("score"),"components":meta.get("components")}),
                ("STRATEGY_CHURN_DETECTED",bool((meta.get("strategy_churn") or {}).get("detected")),"WARNING",
                 "AI Strategy Director state churn detected",meta.get("strategy_churn")),
                ("PARAMETER_CHURN_DETECTED",bool((meta.get("parameter_churn") or {}).get("detected")),"WARNING",
                 "Parameter/configuration churn detected",meta.get("parameter_churn")),
                ("DEPLOYMENT_CHURN_DETECTED",bool((meta.get("deployment_churn") or {}).get("detected")),"HIGH",
                 "Candidate deployment churn detected",meta.get("deployment_churn")),
                ("OBJECTIVE_DRIFT_DETECTED",bool((meta.get("objective_drift") or {}).get("detected")),"HIGH",
                 "Optimization objective drift detected",meta.get("objective_drift")),
                ("HIGH_MODEL_DISAGREEMENT",(meta.get("model_disagreement") or {}).get("status")=="HIGH_MODEL_DISAGREEMENT","HIGH",
                 "Multiple adaptive modules materially disagree",meta.get("model_disagreement")),
                ("CONFIDENCE_MIS_CALIBRATION",bool((meta.get("confidence_calibration") or {}).get("detected")),"WARNING",
                 "Predicted confidence is poorly calibrated to realized outcomes",meta.get("confidence_calibration")),
            ]
            for key,active,sev,msg,details in alert_specs:
                if active:
                    observability_manager.alert(key,sev,"Governance Engine",key,msg,details=details or {})
                else:
                    observability_manager.recover(key,f"{key} no longer detected")
            st=governance_engine.state()
            if int(st.get("governance_lock") or 0):
                observability_manager.alert("GOVERNANCE_LOCK_ACTIVATED","CRITICAL","Governance Engine",
                                            "GOVERNANCE_LOCK_ACTIVATED",
                                            "Persistent Governance Lock is active",
                                            details={"reason":st.get("lock_reason"),"source":st.get("lock_source")})
            else:
                observability_manager.recover("GOVERNANCE_LOCK_ACTIVATED","Governance Lock is not active")
        return result
    except Exception as e:
        if OBSERVABILITY_ENABLED:
            _obs_module("Governance Engine","ERROR",errors=[str(e)])
            observability_manager.alert("GOVERNANCE_EVALUATION_FAILED","HIGH","Governance Engine",
                                        "GOVERNANCE_EVALUATION_FAILED",
                                        "Governance evaluation cycle failed",details={"error":str(e)})
        return {"enabled":True,"status":"FAILED","error":str(e),"mode":governance_engine.mode}


def refresh_smart_execution_observability() -> Dict[str,Any]:
    if not SMART_EXECUTION_ENABLED:
        return {"enabled":False}
    try:
        dash=smart_execution_engine.dashboard()
        deg=dash.get("degradation") or {}
        _obs_module("Smart Execution Engine","DEGRADED" if deg.get("status")=="EXECUTION_DEGRADATION_DETECTED" else "OK",
                    last_operation="smart execution shadow monitoring",details=dash)
        if deg.get("status")=="EXECUTION_DEGRADATION_DETECTED":
            observability_manager.alert("EXECUTION_DEGRADATION_DETECTED","HIGH","Smart Execution Engine",
                "EXECUTION_DEGRADATION_DETECTED","Smart execution quality deteriorated",details=deg)
        else:
            observability_manager.recover("EXECUTION_DEGRADATION_DETECTED","Smart execution quality recovered")
        reasons=set(deg.get("reasons") or [])
        if "FILL_RATE_DOWN" in reasons:
            observability_manager.alert("FILL_RATE_DEGRADED","HIGH","Smart Execution Engine","FILL_RATE_DEGRADED",
                                        "Fill rate deteriorated versus historical execution baseline",details=deg)
        else: observability_manager.recover("FILL_RATE_DEGRADED","Fill rate recovered")
        if "BROKER_LATENCY_DEGRADATION" in reasons:
            observability_manager.alert("BROKER_LATENCY_DEGRADED","WARNING","Smart Execution Engine","BROKER_LATENCY_DEGRADED",
                                        "Broker execution latency deteriorated",details=deg)
        else: observability_manager.recover("BROKER_LATENCY_DEGRADED","Broker execution latency recovered")
        # Bridge engine-local alerts into the central monitoring layer with deduplication handled by ObservabilityManager.
        c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM smart_execution_alerts ORDER BY id DESC LIMIT 50").fetchall()];c.close()
        for a in rows:
            observability_manager.alert(f"SMART_EXEC:{a['event_type']}:{a.get('execution_intent_id') or a.get('symbol')}",
                                        a.get("severity") or "WARNING","Smart Execution Engine",a["event_type"],
                                        a.get("message") or a["event_type"],details=_obs_json_value(a.get("details_json"),{}))
        return dash
    except Exception as e:
        _obs_module("Smart Execution Engine","ERROR",errors=[str(e)])
        observability_manager.alert("SMART_EXECUTION_MONITOR_FAILED","WARNING","Smart Execution Engine",
                                    "SMART_EXECUTION_MONITOR_FAILED","Smart Execution monitoring refresh failed",details={"error":str(e)})
        return {"enabled":True,"status":"ERROR","error":str(e)}


def refresh_ensemble_observability() -> Dict[str,Any]:
    if not ENSEMBLE_ENABLED:
        return {"enabled":False}
    try:
        dash=ensemble_engine.dashboard()
        status="DEGRADED" if dash.get("ensemble_status")=="ABSTAIN" and dash.get("active_models") else "OK"
        _obs_module("Ensemble Engine",status,last_operation="ensemble shadow monitoring",details=dash)
        # Bridge only the current ensemble decision into central observability.
        # Stable keys are event+symbol, never per-decision UUIDs, so repeated cycles
        # update one condition instead of creating unbounded alert cardinality.
        c=conn()
        latest=c.execute("SELECT ensemble_decision_id,symbol FROM ensemble_outputs ORDER BY ts DESC LIMIT 1").fetchone()
        rows=[]
        if latest:
            rows=[dict(x) for x in c.execute("SELECT * FROM ensemble_alerts WHERE ensemble_decision_id=? ORDER BY id DESC",
                                             (latest["ensemble_decision_id"],)).fetchall()]
        c.close()
        current={a["event_type"]:a for a in rows}
        symbol=(latest["symbol"] if latest else "UNKNOWN") or "UNKNOWN"
        known={"ENSEMBLE_CONFLICT","LOW_MODEL_DIVERSITY","HIGH_MODEL_CORRELATION","MODEL_OFFLINE",
               "SIGNAL_STALE","INSUFFICIENT_ENSEMBLE_INFORMATION","CONFIDENCE_MISCALIBRATION","ENSEMBLE_DEGRADATION"}
        expected_closed={"LOW_MODEL_DIVERSITY","SIGNAL_STALE","INSUFFICIENT_ENSEMBLE_INFORMATION"}
        market_closed=market_is_weekend_closed()
        for event in known:
            key=f"ENSEMBLE:{event}:{symbol}"
            a=current.get(event)
            if a and not (market_closed and event in expected_closed):
                observability_manager.alert(key,a.get("severity") or "WARNING","Ensemble Engine",event,
                                            a.get("message") or event,group_key=f"ENSEMBLE:{event}:{symbol}",
                                            details=_obs_json_value(a.get("details_json"),{}))
            else:
                reason="market closed; stale/abstention is expected" if market_closed and event in expected_closed else "condition no longer present"
                observability_manager.recover(key,f"{event} recovered: {reason}",{"market_closed":market_closed})
        return dash
    except Exception as e:
        _obs_module("Ensemble Engine","ERROR",errors=[str(e)])
        observability_manager.alert("ENSEMBLE_MONITOR_FAILED","WARNING","Ensemble Engine",
                                    "ENSEMBLE_MONITOR_FAILED","Ensemble monitoring refresh failed",details={"error":str(e)})
        return {"enabled":True,"status":"ERROR","error":str(e)}


async def observability_loop_monitor():
    obs_loop_interval=int(managed_value("observability.loop_interval_seconds",OBSERVABILITY_LOOP_INTERVAL_SECONDS))
    while True:
        obs_loop_interval=int(managed_value("observability.loop_interval_seconds",OBSERVABILITY_LOOP_INTERVAL_SECONDS))
        expected=time.monotonic()+obs_loop_interval
        await asyncio.sleep(obs_loop_interval)
        now=time.monotonic();lag=max(0.0,(now-expected)*1000)
        observability_manager.set_event_loop_lag(lag)
        if lag>=OBSERVABILITY_LOOP_LAG_CRITICAL_MS:
            observability_manager.alert("EVENT_LOOP_LAG","CRITICAL","System","EVENT_LOOP_LAG_HIGH",
                                        "Event loop lag is critical",details={"lag_ms":lag})
        elif lag>=OBSERVABILITY_LOOP_LAG_WARNING_MS:
            observability_manager.alert("EVENT_LOOP_LAG","WARNING","System","EVENT_LOOP_LAG_HIGH",
                                        "Event loop lag is elevated",details={"lag_ms":lag})
        else:observability_manager.recover("EVENT_LOOP_LAG","Event loop lag recovered",{"lag_ms":lag})
        try:
            if SYSTEM_EVALUATION_ENABLED and system_evaluation_engine.due():
                await asyncio.to_thread(run_system_evaluation, source="periodic")
            if GOVERNANCE_ENABLED and governance_engine.due(GOVERNANCE_EVALUATION_INTERVAL_MINUTES):
                await asyncio.to_thread(run_governance_cycle, "periodic")
            if SMART_EXECUTION_ENABLED:
                await asyncio.to_thread(refresh_smart_execution_observability)
                await asyncio.to_thread(refresh_ensemble_observability)
            if PRODUCTION_READINESS_ENABLED:
                pst=production_readiness_gate.state()
                if pst.get("production_stage")=="CERTIFICATION":
                    _obs_module(
                        "Production Readiness Gate",
                        "OK",
                        last_operation="certification stage monitoring",
                        details={
                            "readiness_state":pst.get("readiness_state"),
                            "production_stage":pst.get("production_stage"),
                            "release_id":pst.get("release_id"),
                            "certification_id":pst.get("certification_id"),
                        },
                    )
                if pst.get("production_stage") in ("MINIMAL_LIVE","LIMITED_LIVE","CONTROLLED_LIVE","PRODUCTION_APPROVED","SUSPENDED"):
                    pctx=production_runtime_context()
                    pctx.update({
                        "risk_ready":bool(RISK_ENGINE_ENABLED and not RISK_ENGINE_SHADOW_MODE),
                        "broker_stable":bool(pctx.get("broker_ready")),
                        "data_quality_ok":float(pctx.get("data_quality") or 0)>=.75,
                        "p0_incident":False,
                        "critical_incident":pctx.get("system_status")=="CRITICAL",
                    })
                    rid=pst.get("release_id")
                    if rid:
                        unchanged=production_readiness_gate.verify_release_unchanged(rid,production_release_files(),security_manager.current_config(),production_release_versions())
                        if not unchanged.get("passed"):
                            cres={"status":"CERTIFICATION_INVALIDATED","triggers":["RELEASE_FINGERPRINT_CHANGED"],"release_check":unchanged}
                            production_readiness_gate.invalidate_certification("RELEASE_FINGERPRINT_CHANGED","CONTINUOUS_CERTIFICATION",rid)
                        else:
                            cres=production_readiness_gate.continuous_certification(pctx)
                    else:
                        cres={"status":"NO_CERTIFIED_RELEASE"}
                    if cres.get("status") in ("CERTIFICATION_INVALIDATED","DEGRADED") and OBSERVABILITY_ENABLED:
                        observability_manager.alert("PRODUCTION_READINESS_LOST","CRITICAL","Production Readiness Gate",
                                                    "PRODUCTION_READINESS_LOST",
                                                    "Continuous certification detected loss of production readiness",details=cres)
                        if cres.get("status")=="CERTIFICATION_INVALIDATED":
                            observability_manager.alert("CERTIFICATION_INVALIDATED","CRITICAL","Production Readiness Gate",
                                                        "CERTIFICATION_INVALIDATED","Production certification is no longer valid",details=cres)
                        safety=(cres.get("safety_action") or {})
                        if safety.get("action")=="DOWNGRADE":
                            observability_manager.alert("PRODUCTION_STAGE_DOWNGRADED","HIGH","Production Readiness Gate",
                                                        "PRODUCTION_STAGE_DOWNGRADED","Production stage reduced by deterministic safety gate",details=safety)
                        elif safety.get("action")=="SUSPEND":
                            observability_manager.alert("PRODUCTION_SUSPENDED","CRITICAL","Production Readiness Gate",
                                                        "PRODUCTION_SUSPENDED","Production suspended by deterministic safety gate",details=safety)
                    _obs_module("Production Readiness Gate","OK" if cres.get("status")=="VALID" else "DEGRADED",
                                last_operation="continuous certification",details=cres)
        except Exception:
            pass
        try:
            thresholds={"Market Data":OBSERVABILITY_HEARTBEAT_STALE_SECONDS,"Broker Connection":OBSERVABILITY_BROKER_STALE_SECONDS,
                        "Risk Engine":OBSERVABILITY_HEARTBEAT_STALE_SECONDS,"Execution Engine":OBSERVABILITY_HEARTBEAT_STALE_SECONDS,
                        "Smart Execution Engine":OBSERVABILITY_HEARTBEAT_STALE_SECONDS*3,
                        "Ensemble Engine":OBSERVABILITY_HEARTBEAT_STALE_SECONDS*3,
                        "AI Strategy Director":OBSERVABILITY_HEARTBEAT_STALE_SECONDS*2,
                        "Market Regime Detector":OBSERVABILITY_HEARTBEAT_STALE_SECONDS*2,
                        "Trade Memory":OBSERVABILITY_HEARTBEAT_STALE_SECONDS*3,
                        "System Evaluation Engine":max(OBSERVABILITY_HEARTBEAT_STALE_SECONDS*3,SYSTEM_EVALUATION_PERIOD_HOURS*3600*2),
                        "Governance Engine":max(OBSERVABILITY_HEARTBEAT_STALE_SECONDS*3,GOVERNANCE_EVALUATION_INTERVAL_MINUTES*60*2),
                        "Production Readiness Gate":max(OBSERVABILITY_HEARTBEAT_STALE_SECONDS*3,3600)}
            observability_manager.mark_stale_modules(thresholds)
        except Exception:pass


def _worker_heartbeat() -> None:
    # Lightweight liveness signal for the watchdog; it does not imply that the
    # current scan completed successfully.
    state["worker_last_heartbeat"] = now_iso()


async def scan(client: httpx.AsyncClient, inst: str, *, batch_collect: bool=False) -> Dict[str, Any]:
    _worker_heartbeat()
    obs_scan_started=time.perf_counter()
    obs_trace_id=observability_manager.new_trace(inst,context={"instrument":inst,"cycle":state.get("cycles")}) if OBSERVABILITY_ENABLED else None
    obs_market_started=time.perf_counter()
    h1, m15, m5, m1 = await asyncio.gather(
        candles(client, inst, "H1", 140),
        candles(client, inst, "M15", 140),
        candles(client, inst, "M5", 130),
        candles(client, inst, "M1", max(220, OUTCOME_HORIZON_MIN + 30))
    )
    _worker_heartbeat()
    obs_market_ms=(time.perf_counter()-obs_market_started)*1000
    obs_market_health=observability_market_data_update(inst,m1,obs_market_ms) if OBSERVABILITY_ENABLED else {"stale":False,"age_seconds":None}
    if RECOVERY_MANAGER_ENABLED and m1:
        # During a normal weekend close, old candles are expected and should not
        # open the MARKET_DATA circuit. Outside a scheduled close, reliability
        # still requires non-stale data.
        market_reliable=bool(obs_market_health.get("market_closed")) or not bool(obs_market_health.get("stale"))
        recovery_manager.market_data_update(m1[-1]["t"],market_reliable)
    if obs_trace_id: observability_manager.trace_phase(obs_trace_id,"market_data")
    current_price=float(m1[-1]["c"]) if m1 else 0.0
    trade_memory_excursions=update_trade_memory_excursions(inst,m1) if TRADE_MEMORY_ENABLED else 0
    trade_memory_reconcile=await reconcile_trade_memory(client,inst) if TRADE_MEMORY_ENABLED else {"enabled":False}
    if OBSERVABILITY_ENABLED:
        _obs_module("Trade Memory","OK" if TRADE_MEMORY_ENABLED else "PAUSED",last_operation="reconcile/observe",
                    details={"instrument":inst,"reconcile":trade_memory_reconcile,"excursion_updates":trade_memory_excursions})
    daily_cutoff_closes=await close_managed_trades_for_daily_cutoff(client,inst)
    managed_changes=await manage_open_trades(client,inst,current_price) if current_price else 0
    # The outcome resolvers are synchronous SQLite/CPU research workloads.
    # Run them off the asyncio event loop so broker management, watchdogs and
    # health heartbeats remain responsive even when the pending research set is large.
    resolved = await asyncio.to_thread(resolve_pending, inst, m1)
    counterfactual_resolved = 0
    if COUNTERFACTUAL_SHADOW_ENABLED:
        try:
            counterfactual_resolved = await asyncio.to_thread(counterfactual_tracker().resolve_open, inst, m1)
        except Exception as e:
            log.exception("counterfactual shadow resolution failed instrument=%s: %s",inst,e)
            if OBSERVABILITY_ENABLED:
                observability_manager.alert(f"COUNTERFACTUAL_TRACKER:{inst}","WARNING","Observability","COUNTERFACTUAL_TRACKER_FAILURE",
                                            f"Counterfactual shadow resolution failed for {inst}",details={"error":str(e),"instrument":inst,"execution_authority":False})
    shadow_resolved = await asyncio.to_thread(resolve_shadow_trials, inst, m1)
    candidate_paper_resolved = (
        await asyncio.to_thread(resolve_candidate_paper_trades, inst, m1)
        if VALIDATION_PIPELINE_ENABLED else 0
    )
    deployment_live_reconcile = await deployment_manager.reconcile(client) if DEPLOYMENT_MANAGER_ENABLED and CANARY_ACCOUNT and CANARY_TOKEN else {"checked":0,"closed":0,"errors":[]}
    recovery_periodic={"skipped":True}
    if RECOVERY_MANAGER_ENABLED:
        rst=recovery_manager.state();lastrec=_parse_iso(rst.get("last_reconciliation_ts"))
        if lastrec is None or (datetime.now(timezone.utc)-lastrec).total_seconds()>=RECOVERY_RECONCILE_INTERVAL_SECONDS:
            recovery_periodic=await recovery_reconcile_primary(client,"periodic")
            rec=(recovery_periodic.get("reconciliation") or {}) if isinstance(recovery_periodic,dict) else {}
            if recovery_periodic.get("connected") and rec.get("status") in ("MATCHED","MINOR_MISMATCH") and not bool(obs_market_health.get("stale")):
                try:
                    rctx=await build_broker_risk_context(client)
                    rok=not bool(rctx.get("system_abnormal")) and rctx.get("nav") is not None
                    recovery_manager.verify_risk(rok,rctx)
                    if rok and not recovery_manager.state().get("emergency_stop") and recovery_manager.state().get("safe_mode"):
                        recovery_manager.exit_safe_mode("periodic broker/position/risk recovery passed")
                        if OBSERVABILITY_ENABLED:
                            observability_manager.recover("RECOVERY_FAILURE","Recovery Manager returned to READY",rec)
                except Exception as e:
                    recovery_manager.verify_risk(False,{"error":str(e)})
    if resolved or shadow_resolved or candidate_paper_resolved:
        # These research/learning refreshes are synchronous SQLite/CPU work too.
        # Keep their original order and await completion, but execute them outside
        # the asyncio thread so trade management/watchdogs remain responsive.
        await asyncio.to_thread(refresh_discovered_patterns, inst)
        await asyncio.to_thread(refresh_filter_hypotheses)
        await asyncio.to_thread(refresh_external_hypotheses)
        await asyncio.to_thread(autonomous_discovery_refresh)
        await asyncio.to_thread(review_active_research_rules)
        await asyncio.to_thread(security_queue_validated_research_changes)
        state["strategy_health"]=await asyncio.to_thread(evaluate_all_strategy_health)
        await asyncio.to_thread(reconcile_ai_director_outcomes)
        # Close the learning loop as soon as enough labeled outcomes exist instead
        # of waiting for the hourly maintenance tick. Training still enforces its
        # own minimum sample and temporal-validation requirements.
        retrain=await asyncio.to_thread(should_retrain_model,inst)
        if retrain["ready"]:
            try:
                trained=await asyncio.to_thread(train_shadow_model,False,inst)
                state.setdefault("learning_by_instrument",{})[inst]={**trained,"last_train":now_iso(),"model_ready":bool(shadow_model_governance_status(inst).get("ready")),"retrain_policy":retrain}
                if inst==PRIMARY_INSTRUMENT: state["learning"]=state["learning_by_instrument"][inst]
            except Exception as e: log.exception("evidence-gated learning refresh failed: %s",e)
    # V3.19: autonomous research cannot self-activate rules.
    if AUTO_PROMOTE_RESEARCH:
        await asyncio.to_thread(security_queue_validated_research_changes)
    if STRATEGY_SELF_EVAL_ENABLED:
        state["strategy_health"]=await asyncio.to_thread(evaluate_all_strategy_health)

    obs_strategy_started=time.perf_counter()
    r = analyze(h1, m15, m5, m1, inst)
    obs_strategy_ms=(time.perf_counter()-obs_strategy_started)*1000
    r["market_data_stale"]=bool(obs_market_health.get("stale"))
    r["market_closed"]=bool(obs_market_health.get("market_closed"))
    r["market_data_state"]=obs_market_health.get("market_data_state") or ("STALE" if r["market_data_stale"] else "FRESH")
    r["market_data_fresh_for_trading"]=bool(obs_market_health.get("fresh"))
    r["correlation_id"]=obs_trace_id
    if obs_trace_id: observability_manager.trace_phase(obs_trace_id,"signal",strategy_id=setup_variant(r))
    if OBSERVABILITY_ENABLED:
        _obs_module("Strategies","OK",obs_strategy_ms,last_operation="strategy analysis",
                    details={"instrument":inst,"signal":r.get("signal"),"score":r.get("score"),"variant":setup_variant(r)})

    if MARKET_REGIME_ENABLED:
        prev_regime=(state.get("market_regimes",{}).get(inst) or {}).get("market_regime")
        log_market_regime(inst,r["market_regime"])
        state.setdefault("market_regimes",{})[inst]=r["market_regime"]
        record_market_regime(inst,r.get("candle_ts"),r["market_regime"])
        if OBSERVABILITY_ENABLED:
            _obs_module("Market Regime Detector","OK",last_operation="regime classified",details={"instrument":inst,**r["market_regime"]})
            new_regime=r["market_regime"].get("market_regime")
            if prev_regime and new_regime and prev_regime!=new_regime:
                observability_manager.structured_log("INFO","Market Regime Detector","REGIME_CHANGED",
                    f"{inst} regime changed {prev_regime} -> {new_regime}",correlation_id=obs_trace_id,symbol=inst,
                    metrics={"previous":prev_regime,"current":new_regime,"confidence":r["market_regime"].get("confidence")})
    elif OBSERVABILITY_ENABLED:
        _obs_module("Market Regime Detector","PAUSED",warnings=["detector disabled"])

    weekend_session=ensure_weekend_session(inst,current_price)
    weekend_reaction=update_weekend_reactions(inst,current_price) if current_price else {"updated":False}
    weekend_signal_context=attach_weekend_context_observation(inst,r.get("candle_ts"))
    r["weekend_research"]={"session":weekend_session,"reaction_update":weekend_reaction,"active_signal_context":weekend_signal_context}

    # Research brain runs independently of order execution.
    r["external_research_collection"] = await collect_cross_asset_research(client, inst, r.get("candle_ts"))
    _worker_heartbeat()

    r = await news(client, r) if r["signal"] != "WAIT" and r["technical"] >= 50 else {**r, "alignment": "N/A"}
    _worker_heartbeat()
    if r.get("news_articles") is not None:
        record_news_research(r)
    target_plan=desired_target_for_trade(r) if r["signal"]!="WAIT" else {"target":r.get("target"),"runner":False,"trend_score":0.0}
    if r["signal"]!="WAIT":
        r["managed_target"]=target_plan["target"]
        r["trend_runner"]=target_plan["runner"]
        r["trend_score"]=target_plan["trend_score"]
    mlp = load_shadow_probability(r["features"],inst) if r["signal"] != "WAIT" else None
    conf = dynamic_confidence(r, mlp) if r["signal"] != "WAIT" else {
        "probability": None, "source": "WAIT", "samples": 0, "local_samples": 0,
        "variant": "WAIT", "mature": False, "recent_win_rate": None,
        "performance_penalty": 0.0, "required_confidence": EXECUTION_MIN_CONFIDENCE
    }
    r["strategy_health"]=strategy_health_snapshot(setup_variant(r),inst) if r["signal"]!="WAIT" else None
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
    # Ensemble Engine — SHADOW ONLY. It evaluates multiple current information sources
    # before the Director but cannot change r, conf, units or execution_decision().
    ensemble_shadow=evaluate_ensemble_shadow(r,conf,mlp)
    r["ensemble_shadow"]=ensemble_shadow
    if OBSERVABILITY_ENABLED:
        _obs_module("Ensemble Engine","OK" if not ensemble_shadow.get("error") else "DEGRADED",
                    last_operation="shadow ensemble evaluation",
                    details={"instrument":inst,"direction":ensemble_shadow.get("ensemble_direction"),
                             "confidence":ensemble_shadow.get("ensemble_confidence"),
                             "agreement":ensemble_shadow.get("agreement_score"),
                             "diversity":ensemble_shadow.get("diversity_score"),
                             "policy_authority":False})

    # AI Strategy Director — OBSERVATION ONLY.
    # It reviews Ensemble shadow context but the ensemble has no authority over execution.
    current_regime = r.get("market_regime")
    if not isinstance(current_regime, dict):
        current_regime = state.get("market_regimes",{}).get(inst)
    obs_director_started=time.perf_counter()
    director = ai_strategy_director_recommendation(
        instrument=inst,
        variant=setup_variant(r),
        regime=current_regime,
        signal_confidence=conf.get("probability"),
        ensemble_shadow=ensemble_shadow
    )
    director_id = log_ai_director_decision(director)
    obs_director_ms=(time.perf_counter()-obs_director_started)*1000
    if obs_trace_id: observability_manager.trace_phase(obs_trace_id,"director",decision_id=director_id)
    if OBSERVABILITY_ENABLED:
        _obs_module("AI Strategy Director","OK" if director.get("enabled",True) else "PAUSED",obs_director_ms,
                    last_operation="strategy recommendation",details={"instrument":inst,"variant":setup_variant(r),
                    "recommended_state":director.get("recommended_state"),"confidence":director.get("confidence"),"decision_id":director_id})
    r["ai_strategy_director"] = director
    r["ai_strategy_director_decision_id"] = director_id

    # Adaptive Risk Engine: observation/shadow only.
    # Read-only broker context; the recommendation is NOT passed into execution_decision().
    try:
        broker_risk_context = await build_broker_risk_context(client)
        r["broker_risk_context"]=broker_risk_context
        persist_portfolio_risk_context(broker_risk_context)
    except Exception as e:
        broker_risk_context = {
            "balance":None,"nav":None,"peak_nav":None,"current_drawdown":None,
            "margin_used":None,"margin_usage":None,"open_positions":0,
            "portfolio_open_risk":0.0,"open_instruments":[],
            "consecutive_losses":_executed_loss_streak(),
            "data_stale":False,"system_abnormal":True,"errors":[str(e)]
        }
        persist_portfolio_risk_context(broker_risk_context)

    obs_risk_started=time.perf_counter()
    risk_shadow = adaptive_risk_recommendation(
        instrument=inst,
        variant=setup_variant(r),
        regime=current_regime,
        director=director,
        signal_confidence=conf.get("probability"),
        risk_context=broker_risk_context,
        requested_units=UNITS
    )
    risk_shadow.setdefault("metrics",{})["ensemble_shadow"]={
        "ensemble_decision_id":ensemble_shadow.get("ensemble_decision_id"),
        "direction":ensemble_shadow.get("ensemble_direction"),
        "confidence":ensemble_shadow.get("ensemble_confidence"),
        "agreement":ensemble_shadow.get("agreement_score"),
        "diversity":ensemble_shadow.get("diversity_score"),
        "family_weights":(ensemble_shadow.get("family_weight_info") or {}).get("family_totals"),
        "risk_increase_authority":False,
        "correlated_votes_do_not_multiply_risk":True
    }
    risk_shadow_id = log_adaptive_risk_decision(risk_shadow)
    obs_risk_ms=(time.perf_counter()-obs_risk_started)*1000
    if obs_trace_id: observability_manager.trace_phase(obs_trace_id,"risk",risk_decision_id=risk_shadow_id)
    if OBSERVABILITY_ENABLED:
        _obs_module("Risk Engine","OK",obs_risk_ms,last_operation="risk recommendation",details={"instrument":inst,
                    "risk_multiplier":risk_shadow.get("risk_multiplier"),"allow_new_trades":risk_shadow.get("allow_new_trades"),
                    "emergency_stop":risk_shadow.get("emergency_stop"),"risk_decision_id":risk_shadow_id})
    r["adaptive_risk_engine"] = risk_shadow
    r["adaptive_risk_decision_id"] = risk_shadow_id
    portfolio_guard=portfolio_execution_guard(inst,broker_risk_context)
    r["portfolio_execution_guard"]=portfolio_guard

    decision = execution_decision(r, conf)
    # Strategy-valid candidates are ranked before broker/instrument/portfolio
    # authorization. This lets all five analysis instruments compete while
    # preserving fail-closed execution for OANDA-disabled symbols.
    batch_strategy_eligible = bool(decision.get("execute"))
    batch_strategy_reason = str(decision.get("reason") or "")
    if not portfolio_guard["allow"]:
        decision={"execute":False,"reason":"GLOBAL_PORTFOLIO_RISK_GUARD: "+", ".join(portfolio_guard["reasons"])}
    r["instrument_mode"]=instrument_mode(inst)
    if r["instrument_mode"] != "ENABLED":
        decision={"execute":False,"reason":f"INSTRUMENT_{r['instrument_mode']}: signal/research only; order authority disabled"}
    if RECOVERY_MANAGER_ENABLED and not recovery_manager.new_trades_allowed():
        decision={"execute":False,"reason":"RECOVERY_SAFE_MODE: broker/internal state not sufficiently certain"}
    if OBSERVABILITY_STARTUP_BLOCK_TRADING and not state.get("system_ready"):
        decision={"execute":False,"reason":"SYSTEM_NOT_READY: startup health check incomplete/failed"}
    if r.get("market_data_stale"):
        decision={"execute":False,"reason":"MARKET_DATA_STALE: current data rejected by fail-safe"}
    if r.get("market_closed") or r.get("market_data_state")=="MARKET_CLOSED":
        decision={"execute":False,"reason":"MARKET_CLOSED: new orders blocked by hard execution gate"}
    entry_gate=new_entry_time_gate()
    r["new_entry_time_gate"]=entry_gate
    if not entry_gate["allowed"]:
        decision={"execute":False,"reason":f"{entry_gate['reason']}: new entries blocked during {entry_gate.get('window')}"}
    if DEPLOYMENT_MANAGER_ENABLED and deployment_manager.kill("SYSTEM").get("active"):
        decision={"execute":False,"reason":"GLOBAL KILL SWITCH: new trades blocked"}
    if TRADING_ENVIRONMENT=="PRODUCTION" and PRODUCTION_READINESS_ENABLED and decision.get("execute"):
        prod_ctx=production_runtime_context()
        if PRODUCTION_DRY_RUN_MODE:
            pst=production_readiness_gate.state();rid=pst.get("release_id")
            if rid:
                production_readiness_gate.record_dry_run(
                    rid,
                    {"market_data":not bool(r.get("market_data_stale")),"signal":r.get("signal") in ("BUY","SELL"),
                     "director":director.get("recommended_state") is not None,"risk":risk_shadow_id is not None,
                     "governance":GOVERNANCE_ENABLED,"execution_prepared":True},
                    {"instrument":inst,"side":r.get("signal"),"units":min(UNITS,int(managed_value("execution.trade_units",UNITS))),
                     "entry":r.get("entry"),"stop":r.get("stop"),"target":r.get("managed_target",r.get("target"))},
                    blocked_before_send=True,real_broker_request_count=0,actor="EXECUTION_DRY_RUN")
            decision={"execute":False,"reason":"PRODUCTION_DRY_RUN_MODE: full decision pipeline prepared; real order send blocked"}
        else:
            prod_gate=production_readiness_gate.pretrade_health_gate(prod_ctx)
            r["production_readiness_gate"]=prod_gate
            if not prod_gate.get("allow_new_real_order"):
                decision={"execute":False,"reason":"PRODUCTION_READINESS_GATE: "+", ".join(prod_gate.get("reasons") or [])}
            else:
                sec_guard=security_manager.real_order_guard(
                    broker_account_verified=bool(production_readiness_gate._latest("production_account_verification","WHERE release_id=? AND passed=1 ORDER BY ts DESC LIMIT 1",(production_readiness_gate.state().get("release_id"),))),
                    risk_engine_ready=bool(RISK_ENGINE_ENABLED and not RISK_ENGINE_SHADOW_MODE),
                    reconciliation_complete=bool(prod_ctx.get("reconciliation_ok")),
                    emergency_stop=bool(prod_ctx.get("emergency_stop")),
                    deployment_authorized=production_readiness_gate.state().get("production_stage") in ("MINIMAL_LIVE","LIMITED_LIVE","CONTROLLED_LIVE","PRODUCTION_APPROVED"),
                    runtime_verified=bool(security_manager.last_integrity.get("verified")),
                    running_under_test=running_under_test())
                if not sec_guard.get("allow"):
                    decision={"execute":False,"reason":"SECURITY_REAL_ORDER_GUARD: "+", ".join(sec_guard.get("reasons") or [])}
    if OBSERVABILITY_ENABLED:
        _obs_module("Execution Engine","OK",last_operation="execution decision evaluated",details={"instrument":inst,"execute":decision.get("execute"),"reason":decision.get("reason")})
    batch_selection_eligible = bool(batch_strategy_eligible) if batch_collect else bool(decision.get("execute"))
    batch_preselection_reason = batch_strategy_reason if batch_collect else str(decision.get("reason") or "")
    if batch_collect and batch_selection_eligible:
        decision={"execute":False,"reason":"MULTI_ASSET_BATCH_COLLECT: strategy-valid candidate deferred until ranking/slot/broker/portfolio selection"}
    pre_execution_reason = str(decision["reason"])
    executed, oid = 0, ""
    trade_id=""; fill={}; fill_price=None; slippage=None
    if AUTO and bool(managed_value("execution.auto_trade",AUTO)) and decision["execute"]:
        if obs_trace_id: observability_manager.trace_phase(obs_trace_id,"order_created")
        obs_order_sent=now_iso();obs_broker_started=time.perf_counter()
        if obs_trace_id: observability_manager.trace_phase(obs_trace_id,"order_sent",ts=obs_order_sent)
        try:
            x=await execute_recoverable(client,r,obs_trace_id,director_id,risk_shadow_id) if RECOVERY_MANAGER_ENABLED else await execute(client,r)
        except Exception as e:
            if RECOVERY_MANAGER_ENABLED:
                recovery_manager.enter_safe_mode(f"execution exception before confirmed broker state: {e}",
                                                 correlation_id=obs_trace_id,severity="CRITICAL")
            if OBSERVABILITY_ENABLED:
                observability_manager.alert(f"ORDER_ERROR:{obs_trace_id}","CRITICAL","Execution Engine","ORDER_REJECTED_OR_UNCONFIRMED",
                                            "Order submission failed; trading moved to safe mode",
                                            correlation_id=obs_trace_id,details={"instrument":inst,"error":str(e)})
                _obs_module("Execution Engine","ERROR",errors=[str(e)])
            x={"status_unknown":True,"error":str(e)}
        obs_broker_ms=(time.perf_counter()-obs_broker_started)*1000
        if x and x.get("status_unknown"):
            decision["reason"] += "; ORDER_STATUS_UNKNOWN: no automatic resend; reconciliation required"
            if OBSERVABILITY_ENABLED:
                observability_manager.alert(f"ORDER_UNKNOWN:{obs_trace_id}","CRITICAL","Recovery Manager","ORDER_STATUS_UNKNOWN",
                    "Order outcome is unknown; duplicate resend prevented until reconciliation",
                    correlation_id=obs_trace_id,details=x)
                _obs_module("Recovery Manager","DEGRADED",errors=[str(x.get("error") or "ORDER_STATUS_UNKNOWN")])
        elif x and x.get("duplicate_prevented"):
            decision["reason"] += "; DUPLICATE_ORDER_PREVENTED"
        elif x and x.get("rejected"):
            decision["reason"] += "; ORDER_REJECTED"
        elif x and not x.get("skipped"):
            if obs_trace_id: observability_manager.trace_phase(obs_trace_id,"broker_ack")
            executed = 1
            fill=(x.get("orderFillTransaction") or {})
            oid=str(fill.get("id","")); trade_id=str((fill.get("tradeOpened") or {}).get("tradeID",""))
            fill_price=float(fill.get("price") or r["entry"]); slippage=(fill_price-r["entry"])/pip_size(inst)
            if r["signal"]=="SELL": slippage=-slippage
            if OBSERVABILITY_ENABLED:
                if not oid or not trade_id:
                    observability_manager.alert(f"ORDER_NO_CONFIRM:{obs_trace_id}","CRITICAL","Execution Engine","ORDER_CONFIRMATION_MISSING",
                        "Broker response did not contain expected fill/trade identifiers",correlation_id=obs_trace_id,details={"fill":fill})
                if oid:
                    cdup=conn();dup=cdup.execute("SELECT COUNT(*) n FROM execution_audit WHERE order_id=?",(oid,)).fetchone()["n"];cdup.close()
                    if dup:
                        observability_manager.alert(f"DUPLICATE_ORDER:{oid}","CRITICAL","Execution Engine","DUPLICATE_ORDER",
                            "Order identifier already exists in execution audit",correlation_id=obs_trace_id,details={"order_id":oid,"existing":dup})
                actual_units=_risk_float((fill.get("tradeOpened") or {}).get("units"))
                if actual_units is None and x.get("filled_units") is not None:
                    actual_units=float(x.get("filled_units"))
                requested_execution_units=abs(float(((x.get("intent") or {}).get("requested_units") if isinstance(x,dict) else None) or UNITS))
                if actual_units is not None and abs(abs(actual_units)-requested_execution_units)>0.5:
                    observability_manager.alert(f"PARTIAL_FILL:{oid or obs_trace_id}","HIGH","Execution Engine","PARTIAL_FILL_UNEXPECTED",
                        "Filled units differ from requested units; remaining amount will not be resent automatically",correlation_id=obs_trace_id,details={"requested_units":requested_execution_units,"filled_units":actual_units,"remaining_units":x.get("remaining_units")})
            if obs_trace_id:
                observability_manager.trace_phase(obs_trace_id,"fill",order_id=oid,trade_id=trade_id)
                observability_manager.link_trace(obs_trace_id,order_id=oid,trade_id=trade_id)
            if OBSERVABILITY_ENABLED:
                if abs(float(slippage or 0))>DEPLOYMENT_MAX_SLIPPAGE_PIPS:
                    observability_manager.alert(f"SLIPPAGE:{oid or obs_trace_id}","HIGH","Execution Engine","EXCESSIVE_SLIPPAGE",
                        "Fill price deviated materially from expected entry",correlation_id=obs_trace_id,
                        details={"slippage_pips":slippage,"expected_entry":r["entry"],"fill_price":fill_price})
                if obs_broker_ms>OBSERVABILITY_BROKER_LATENCY_WARNING_MS:
                    observability_manager.alert("BROKER_LATENCY","WARNING","Broker Connection","BROKER_LATENCY_HIGH",
                        "Broker order acknowledgement latency is elevated",correlation_id=obs_trace_id,details={"latency_ms":obs_broker_ms})
                else:observability_manager.recover("BROKER_LATENCY","Broker order latency recovered",{"latency_ms":obs_broker_ms})
            protection_reanchor=await reanchor_post_fill_protection(client,r,trade_id,fill_price,obs_trace_id)
            protection_verification=protection_reanchor.get("verification") or {}
            protection={**protection_verification,
                        "status":"OK" if protection_reanchor.get("confirmed") else "PROTECTION_ERROR",
                        "reanchor_status":protection_reanchor.get("status"),
                        "detail":f"reanchor={protection_reanchor.get('status')}; {protection_verification.get('detail','')}"}
            if protection["status"]!="OK": decision["reason"] += f"; PROTECTION_REANCHOR_{protection_reanchor.get('status')}"
            actual_filled_units=_risk_float((fill.get("tradeOpened") or {}).get("units"))
            if actual_filled_units is None:
                actual_filled_units=_risk_float(x.get("filled_units"),UNITS)
            if SMART_EXECUTION_ENABLED and isinstance(x,dict) and x.get("smart_execution_intent_id") and actual_filled_units is not None:
                try:
                    smart_execution_engine.link_trade(x["smart_execution_intent_id"],trade_id)
                    smart_fill=smart_execution_engine.record_fill(
                        x["smart_execution_intent_id"],fill_quantity=abs(float(actual_filled_units)),
                        fill_price=float(fill_price),broker_order_id=oid or None,broker_fill_id=fill.get("id"),
                        broker_event_id=str(fill.get("id") or oid or "") or None,order_type="MARKET",
                        broker_ack_latency_ms=obs_broker_ms,first_fill_latency_ms=obs_broker_ms,
                        fees=float(_risk_float(fill.get("commission"),0.0) or 0.0),session=_tm_session(r.get("candle_ts") or now_iso())
                    )
                    smart_tca=smart_fill.get("tca") or {}
                    smart_cmp=smart_execution_engine.shadow_compare(
                        x["smart_execution_intent_id"],actual_order_type="MARKET",actual_quantity=abs(float(actual_filled_units)),
                        actual_slippage_bps=smart_tca.get("slippage_bps"),actual_cost=smart_tca.get("total_execution_cost"),
                        actual_fill_rate=smart_tca.get("fill_rate"))
                    r["smart_execution_actual"]={"fill":smart_fill,"tca":smart_tca,"shadow_comparison":smart_cmp}
                except Exception as e:
                    r["smart_execution_actual"]={"error":str(e)}
                    if OBSERVABILITY_ENABLED:
                        observability_manager.alert(f"SMART_EXECUTION_RECORD:{oid or obs_trace_id}","WARNING","Smart Execution Engine",
                            "SMART_EXECUTION_TCA_RECORD_FAILED","Smart Execution shadow/TCA recording failed; existing execution state is unchanged",
                            correlation_id=obs_trace_id,details={"error":str(e)})
            effective_stop=protection_reanchor.get("effective_stop")
            effective_target=protection_reanchor.get("effective_target")
            if trade_id and effective_stop is not None and effective_target is not None:
                register_trade_management(trade_id,r,float(r.get("managed_target",r["target"])),actual_filled_units,fill_price,
                                          applied_stop=float(effective_stop),applied_target=float(effective_target))
            elif trade_id:
                if RECOVERY_MANAGER_ENABLED:
                    recovery_manager.enter_safe_mode("Post-fill broker protection geometry unavailable for trade management",
                                                     correlation_id=obs_trace_id,severity="CRITICAL")
                decision["reason"] += "; TRADE_MANAGEMENT_GEOMETRY_UNVERIFIED"
            log.info("EXECUTED %s %s quality=%s confidence=%s order=%s slippage=%.2f protection=%s",
                     r["signal"], inst, r["score"], conf.get("probability"), oid, slippage, protection["status"])
        elif x and x.get("skipped"):
            decision["reason"] += "; no ejecutada: posición existente"
    elif decision["execute"] and not (AUTO and bool(managed_value("execution.auto_trade",AUTO))):
        decision["reason"] += "; AUTO_TRADE=false"

    signal_id = save_signal(r, executed, oid, mlp, conf, decision["reason"])
    if RECOVERY_MANAGER_ENABLED and locals().get("x") and isinstance(x,dict):
        intent_obj=x.get("intent") or {}
        if intent_obj.get("execution_intent_id"):
            try: recovery_manager.link_signal(intent_obj["execution_intent_id"],signal_id)
            except Exception as e:
                recovery_manager.enter_safe_mode(f"failed to link signal to execution intent: {e}",severity="HIGH")
    if obs_trace_id:
        observability_manager.link_trace(obs_trace_id,signal_id=signal_id,decision_id=director_id,risk_decision_id=risk_shadow_id,
                                         order_id=oid or None,trade_id=trade_id or None,strategy_id=setup_variant(r),symbol=inst)
        observability_manager.structured_log("INFO","Execution Engine","DECISION_RECORDED",decision.get("reason",""),
            strategy_id=setup_variant(r),trade_id=trade_id or None,decision_id=director_id,correlation_id=obs_trace_id,symbol=inst,
            metrics={"signal_id":signal_id,"risk_decision_id":risk_shadow_id,"executed":bool(executed),"confidence":conf.get("probability")})
    candidate_paper_created = record_candidate_paper_signals(signal_id,r,conf,director,risk_shadow,current_price) if VALIDATION_PIPELINE_ENABLED else 0

    candidate_live_results=[]
    if DEPLOYMENT_MANAGER_ENABLED and DEPLOYMENT_LIVE_EXECUTION_ENABLED and CANARY_OANDA_ENV=="live" and r["signal"] in ("BUY","SELL"):
        live_deps=deployment_manager.live(setup_variant(r))
        if live_deps:
            try:
                canary_health=await deployment_manager.account_health(client)
            except Exception as e:
                canary_health={"broker_ok":False,"data_ok":False,"system_abnormal":True,"errors":[str(e)]}
            for dep in live_deps:
                if not canary_recovery_manager.new_trades_allowed():
                    candidate_live_results.append({"candidate_id":dep["candidate_id"],"executed":False,
                                                   "reasons":["CANARY_RECOVERY_SAFE_MODE"]})
                    continue
                ctx={"instrument":inst,"strategy_confidence_entry":conf.get("probability"),
                     "director_confidence_entry":director.get("confidence"),
                     "director_state":director.get("recommended_state"),
                     "market_regime_entry":current_regime.get("market_regime") if isinstance(current_regime,dict) else None,
                     "volatility_state_entry":current_regime.get("volatility_state") if isinstance(current_regime,dict) else None}
                try:
                    canary_entry_gate=new_entry_time_gate()
                    if not canary_entry_gate["allowed"]:
                        candidate_live_results.append({"candidate_id":dep["candidate_id"],"executed":False,
                                                       "reasons":[canary_entry_gate["reason"]],"entry_time_gate":canary_entry_gate})
                        continue
                    canary_risk=adaptive_risk_recommendation(
                        instrument=inst,variant=setup_variant(r),regime=current_regime,director=director,
                        signal_confidence=conf.get("probability"),
                        risk_context={"nav":canary_health.get("nav"),"current_drawdown":canary_health.get("current_drawdown"),
                                      "margin_usage":canary_health.get("margin_usage"),"portfolio_open_risk":0.0,
                                      "open_instruments":canary_health.get("open_instruments") or [],
                                      "consecutive_losses":0,"data_stale":not canary_health.get("data_ok"),
                                      "system_abnormal":canary_health.get("system_abnormal")},
                        requested_units=min(UNITS,int(managed_value("execution.trade_units",UNITS))))
                    gate=deployment_manager.signal_gate(dep,ctx,canary_risk,canary_health)
                    sec_guard=security_manager.real_order_guard(
                        broker_account_verified=BROKER_ACCOUNT_VERIFIED,
                        risk_engine_ready=bool(canary_risk.get("allow_new_trades") and not canary_risk.get("emergency_stop")),
                        reconciliation_complete=canary_recovery_manager.state().get("last_reconciliation_status") in ("MATCHED","MINOR_MISMATCH"),
                        emergency_stop=bool(canary_recovery_manager.state().get("emergency_stop")),
                        deployment_authorized=dep.get("current_stage") in ("CANARY_LIVE","LIMITED_PRODUCTION"),
                        runtime_verified=bool(security_manager.last_integrity.get("verified")),
                        running_under_test=running_under_test()
                    )
                    if not sec_guard["allow"]:
                        gate["allow"]=False
                        gate["reasons"].extend(sec_guard["reasons"])
                        if OBSERVABILITY_ENABLED:
                            observability_manager.alert(f"REAL_ORDER_GUARD:{dep['candidate_id']}","CRITICAL",
                                "Security Manager","PRODUCTION_GUARDRAIL_BLOCK",
                                "Real-order guardrail blocked Candidate execution",
                                correlation_id=obs_trace_id,details={"candidate_id":dep["candidate_id"],"reasons":sec_guard["reasons"]})
                    if not gate["allow"]:
                        candidate_live_results.append({"candidate_id":dep["candidate_id"],"executed":False,"reasons":gate["reasons"]})
                        continue
                    allocation=float(dep["allocation_fraction"])
                    approved=min(float(managed_value("risk.base_fraction",RISK_BASE_FRACTION))*allocation,float(canary_risk.get("approved_risk") or 0))
                    if approved<=0:
                        candidate_live_results.append({"candidate_id":dep["candidate_id"],"executed":False,"reasons":["RISK_ENGINE_APPROVED_ZERO"]})
                        continue
                    units=max(1,int(math.floor(min(UNITS,int(managed_value("execution.trade_units",UNITS)))*min(1.0,approved/max(float(managed_value("risk.base_fraction",RISK_BASE_FRACTION)),1e-9)))))
                    sig={"signal_id":signal_id,"instrument":inst,"signal":r["signal"],"entry":r["entry"],"stop":r["stop"],
                         "target":r["target"],"managed_target":r.get("managed_target",r["target"]),
                         "market_regime":ctx["market_regime_entry"],"volatility_state":ctx["volatility_state_entry"],
                         "director_state":ctx["director_state"],"director_confidence":ctx["director_confidence_entry"],
                         "risk_multiplier":canary_risk.get("risk_multiplier")}
                    canary_key=deterministic_intent_key(
                        "CANARY",inst,r["signal"],dep["candidate_version"],r.get("candle_ts") or now_iso(),
                        r["entry"],r["stop"],r.get("managed_target",r["target"]))
                    async def _canary_submitter(body):
                        return await canary_recovery_manager.submit_order(
                            client,idempotency_key=canary_key,correlation_id=obs_trace_id or canary_key,
                            decision_id=str(director_id) if director_id is not None else None,
                            risk_decision_id=str(risk_shadow_id) if risk_shadow_id is not None else None,
                            strategy_id=dep["candidate_version"],symbol=inst,side=r["signal"],requested_units=units,
                            entry_price=r["entry"],stop_loss=r["stop"],take_profit=r.get("managed_target",r["target"]),
                            order_body=body,metadata={"candidate_id":dep["candidate_id"],
                            "deployment_stage":dep["current_stage"],"allocation_fraction":dep["allocation_fraction"]})
                    x=await deployment_manager.execute(client,dep,sig,units,approved,submitter=_canary_submitter)
                    candidate_live_results.append({"executed":bool(x.get("trade_id")),**x})
                except Exception as e:
                    deployment_manager.pause(dep["candidate_id"],f"fail-safe execution error: {e}","EXECUTION_FAIL_SAFE")
                    candidate_live_results.append({"candidate_id":dep["candidate_id"],"executed":False,"error":str(e)})
    if executed:
        c=conn(); c.execute("""INSERT INTO execution_audit(ts,signal_id,instrument,order_id,trade_id,expected_entry,fill_price,slippage_pips,
          stop_loss_ok,take_profit_ok,protection_status,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
          (now_iso(),signal_id,inst,oid,trade_id,r["entry"],fill_price,slippage,int(protection["sl_ok"]),int(protection["tp_ok"]),protection["status"],protection["detail"]))
        c.commit(); c.close()
        record_trade_memory_entry(
            trade_id=trade_id,signal_id=signal_id,order_id=oid,r=r,conf=conf,
            director=director,risk_shadow=risk_shadow,
            pre_execution_reason=pre_execution_reason,fill=fill,
            fill_price=float(fill_price),entry_slippage_pips=slippage,
            protection_reanchor=locals().get("protection_reanchor")
        )
        if obs_trace_id: observability_manager.trace_phase(obs_trace_id,"trade_memory",trade_id=trade_id)
        if TRADING_ENVIRONMENT=="PRODUCTION" and PRODUCTION_READINESS_ENABLED:
            pst=production_readiness_gate.state()
            if pst.get("production_stage") in ("MINIMAL_LIVE","LIMITED_LIVE","CONTROLLED_LIVE","PRODUCTION_APPROVED"):
                post_fill_rec=await recovery_reconcile_primary(client,"post_real_fill") if RECOVERY_MANAGER_ENABLED else {"reconciliation":{"status":"UNKNOWN"}}
                rec_status=((post_fill_rec or {}).get("reconciliation") or {}).get("status")
                production_readiness_gate.record_live_execution({
                    "trade_id":trade_id,
                    "expected_order":{"instrument":inst,"side":r.get("signal"),"entry":r.get("entry"),"stop":r.get("stop"),
                                      "target":r.get("managed_target",r.get("target"))},
                    "actual_order":{"order_id":oid,"trade_id":trade_id},"fill":fill,
                    "slippage_pips":slippage,"latency_ms":locals().get("obs_broker_ms"),
                    "fees":_risk_float(fill.get("commission"),0.0),
                    "partial_fill":bool(locals().get("actual_filled_units") is not None and abs(abs(float(actual_filled_units))-abs(float(locals().get("requested_execution_units",UNITS))))>0.5),
                    "rejected":False,"reconciliation_ok":rec_status in ("MATCHED","MINOR_MISMATCH"),
                    "protection_ok":protection.get("status")=="OK","audit_ok":True,"trade_memory_ok":True,
                    "details":{"correlation_id":obs_trace_id,"risk_decision_id":risk_shadow_id,"director_decision_id":director_id,
                               "reconciliation_status":rec_status}
                })
    save_decision(r, conf, executed, decision["reason"])
    obs_total_ms=(time.perf_counter()-obs_scan_started)*1000
    if OBSERVABILITY_ENABLED:
        observability_manager.sample_system_metrics(processing_time_ms=obs_total_ms,
            broker_latency_ms=locals().get("obs_broker_ms"),market_data_latency_ms=(obs_market_health.get("age_seconds") or 0)*1000,
            details={"instrument":inst,"signal":r.get("signal"),"executed":bool(executed)})
        if obs_total_ms>15000:
            observability_manager.alert("SCAN_PROCESSING_SLOW","WARNING","System","PROCESSING_LATENCY_HIGH",
                                        "End-to-end scan processing time is elevated",correlation_id=obs_trace_id,details={"processing_ms":obs_total_ms})
        else:observability_manager.recover("SCAN_PROCESSING_SLOW","Scan processing latency recovered",{"processing_ms":obs_total_ms})
    if obs_trace_id: observability_manager.trace_phase(obs_trace_id,"complete")
    return {
        **r,
        "executed": bool(executed), "order_id": oid, "signal_id": signal_id, "correlation_id": obs_trace_id,
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
        "batch_selection_eligible": bool(batch_selection_eligible),
        "batch_preselection_reason": batch_preselection_reason,
        "_batch_context": {
            "confidence": conf,
            "director": director,
            "director_id": director_id,
            "risk_shadow": risk_shadow,
            "risk_shadow_id": risk_shadow_id,
            "ml_probability": mlp,
            "signal_id": signal_id,
        } if batch_collect else None,
        "management_updates_this_cycle": managed_changes,
        "trend_runner": r.get("trend_runner",False),
        "trend_score": r.get("trend_score",0.0),
        "managed_target": r.get("managed_target",r.get("target")),
        "learning_resolved_this_cycle": resolved,
        "shadow_resolved_this_cycle": shadow_resolved,
        "trade_memory_excursions_updated": trade_memory_excursions,
        "trade_memory_reconciliation": trade_memory_reconcile,
        "candidate_paper_created": candidate_paper_created,
        "candidate_paper_resolved_this_cycle": candidate_paper_resolved,
        "candidate_live_results": candidate_live_results,
        "deployment_live_reconciliation": deployment_live_reconcile,
        "recovery": {"state":recovery_manager.state() if RECOVERY_MANAGER_ENABLED else None,
                     "periodic_reconciliation":recovery_periodic}
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
    # Watchdog liveness must follow the most recent worker heartbeat. A scan may
    # legitimately take longer than one minute because it performs broker,
    # research, reconciliation and shadow-learning work. Using only last_scan
    # can falsely classify a busy-but-healthy worker as dead.
    heartbeat = _parse_iso(state.get("worker_last_heartbeat"))
    last_scan = _parse_iso(state.get("last_scan"))
    liveness_ts = heartbeat or last_scan
    age = (now - liveness_ts).total_seconds() if liveness_ts else None
    scan_age = (now - last_scan).total_seconds() if last_scan else None
    stale = age is None or age > WATCHDOG_STALE_SECONDS
    return {
        "worker_running": bool(state.get("worker_running")),
        "last_scan_age_seconds": scan_age,
        "last_heartbeat_age_seconds": age,
        "stale": stale,
        "watchdog_enabled": WATCHDOG_ENABLED,
        "watchdog_stale_seconds": WATCHDOG_STALE_SECONDS,
        "worker_restarts": state.get("worker_restarts", 0),
        "successful_cycles": state.get("successful_cycles", 0),
        "cycles": state.get("cycles", 0),
    }


async def _batch_scan_candidate(client: httpx.AsyncClient, inst: str) -> Dict[str, Any]:
    """Collect a full pre-entry decision without sending an order.

    Compatibility fallback is only for test doubles whose signature predates
    V3.37.0; the production scan() supports batch_collect explicitly.
    """
    try:
        return await scan(client,inst,batch_collect=True)
    except TypeError as e:
        if "batch_collect" not in str(e):
            raise
        return await scan(client,inst)


def _oanda_batch_broker_guard(candidate: Dict[str, Any], selected: List[Dict[str, Any]],
                              risk_context: Dict[str, Any]) -> Dict[str, Any]:
    inst=InstrumentRegistry.normalize_symbol(candidate.get("instrument"))
    meta=instrument_metadata(inst)
    adapter=OandaBrokerRiskAdapter()
    secondary=inst!=PRIMARY_INSTRUMENT
    context={
        "environment":TRADING_ENVIRONMENT,
        "instrument_execution_allowed":instrument_mode(inst)=="ENABLED",
        "secondary_instrument":secondary,
        "metadata_verified":(meta.source=="OANDA") if secondary else True,
        "available_margin_ok":not (
            risk_context.get("margin_usage") is not None and
            float(risk_context.get("margin_usage"))>=float(managed_value("risk.max_margin_usage",RISK_MAX_MARGIN_USAGE))
        ),
    }
    return adapter.prospective_check(candidate,selected,context).as_dict()


def _batch_portfolio_guard(candidate: Dict[str, Any], selected: List[Dict[str, Any]],
                           risk_context: Dict[str, Any]) -> Dict[str, Any]:
    ctx=dict(risk_context or {})
    open_instruments=list(ctx.get("open_instruments") or [])
    pending=[]
    for x in selected:
        symbol=InstrumentRegistry.normalize_symbol(x.get("instrument"))
        if symbol and symbol not in open_instruments:
            open_instruments.append(symbol)
            pending.append(x)
    ctx["open_instruments"]=open_instruments
    max_trade=float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION))
    # Fresh broker context may already include a just-confirmed first fill. Only
    # simulate selected exposure that is not yet represented in that snapshot.
    ctx["portfolio_open_risk"]=float(ctx.get("portfolio_open_risk") or 0.0)+(len(pending)*max_trade)
    return portfolio_execution_guard(candidate.get("instrument"),ctx,prospective_trade_risk=max_trade)


def _counterfactual_rejection_category(reason: str) -> str:
    reason=str(reason or "")
    if reason in {"NO_SLOT_AVAILABLE","NO_SLOT","LOWER_RANK","BEST_SAFE_SET_NOT_SELECTED"}:return "SELECTION_REJECTED"
    if reason in {"GLOBAL_PORTFOLIO_RISK_GUARD","BROKER_RISK_GUARD","INSTRUMENT_METADATA_UNVERIFIED",
                  "GLOBAL_ENTRY_TIME_GATE","RECOVERY_SAFE_MODE","SINGLE_EXECUTION_WORKER_REQUIRED","AUTO_TRADE=false"}:return "SAFETY_REJECTED"
    if reason in {"BROKER_EXPLICIT_REJECTION","ORDER_STATUS_UNKNOWN","ORDER_SUBMITTED_NOT_CONFIRMED",
                  "BROKER_RESULT_MISSING","BROKER_RESULT_WITHOUT_CONFIRMED_FILL","DUPLICATE_INTENT_REQUIRES_RECONCILIATION"}:return "EXECUTION_REJECTED"
    return "OTHER_REJECTED"


def _shadow_candidate_safety(candidate: Dict[str,Any], risk_context: Dict[str,Any]) -> Dict[str,Any]:
    """Read-only classification for selector observability; never changes productive authority."""
    inst=InstrumentRegistry.normalize_symbol(candidate.get("instrument"))
    if instrument_mode(inst)!="ENABLED":return {"safe":False,"reason":"EXECUTION_ELIGIBILITY"}
    if inst!=PRIMARY_INSTRUMENT and instrument_metadata(inst).source!="OANDA":return {"safe":False,"reason":"METADATA"}
    pg=_batch_portfolio_guard(candidate,[],risk_context)
    if not pg.get("allow"):return {"safe":False,"reason":"PORTFOLIO_RISK","details":pg}
    bg=_oanda_batch_broker_guard(candidate,[],risk_context)
    if not bg.get("allow"):return {"safe":False,"reason":"BROKER_RISK","details":bg}
    if not new_entry_time_gate().get("allowed"):return {"safe":False,"reason":"GLOBAL_GATE"}
    if RECOVERY_MANAGER_ENABLED and not recovery_manager.new_trades_allowed():return {"safe":False,"reason":"RECOVERY"}
    return {"safe":True,"reason":"SELECTION_ONLY"}


def _record_counterfactual_cycle(cycle: Dict[str,Any], ranked: List[Any], allocation: Dict[str,Any],
                                 executions: List[Dict[str,Any]], risk_context: Dict[str,Any]) -> Dict[str,Any]:
    """Persist selector evidence after productive decisions; failures are isolated and observable."""
    if not COUNTERFACTUAL_SHADOW_ENABLED:return {"enabled":False,"created":0}
    tracker=counterfactual_tracker();selected=allocation.get("selected") or []
    winner=None
    if selected:
        best=min(selected,key=lambda x: next((i for i,r in enumerate(ranked,1) if r.instrument==x.get("instrument")),10**9))
        winner_rank=next((i for i,r in enumerate(ranked,1) if r.instrument==best.get("instrument")),None)
        ex=next((x for x in executions if x.get("executed") and x.get("instrument")==best.get("instrument")),{})
        intent=ex.get("intent") or {}
        batch=best.get("_batch_context") or {}
        winner={"instrument":best.get("instrument"),"rank":winner_rank,"rank_score":best.get("opportunity_rank_score"),
                "signal_id":batch.get("signal_id") or best.get("signal_id"),"trade_id":ex.get("trade_id"),
                "intent_id":intent.get("execution_intent_id")}
    selected_symbols={x.get("instrument") for x in selected}
    rejected_by_inst={x.get("instrument"):x for x in allocation.get("rejected") or []}
    created=0;events=0
    for rank,item in enumerate(ranked,1):
        if item.instrument in selected_symbols:continue
        rej=rejected_by_inst.get(item.instrument) or {}
        reason=rej.get("reason") or "NOT_SELECTED"
        if reason=="NO_SLOT_AVAILABLE":
            safety=_shadow_candidate_safety(item.candidate,risk_context)
            if safety.get("safe") and winner:
                out=tracker.record_selection_rejected(cycle_id=cycle["cycle_id"],candidate=item.candidate,rank=rank,
                    rank_score=item.rank_score,components=item.components,slot_capacity=cycle["max_slots"],
                    slots_available=cycle["slots_available"],cycle_size=len(ranked),winner=winner,rejection_reason="NO_SLOT")
                created+=int(out.get("created",False))
            else:
                tracker.record_non_counterfactual_rejection(cycle_id=cycle["cycle_id"],instrument=item.instrument,
                    reason=safety.get("reason") or reason,category="SAFETY_REJECTED",detail={"rank":rank,"rank_score":item.rank_score})
                events+=1
        else:
            category=_counterfactual_rejection_category(reason)
            tracker.record_non_counterfactual_rejection(cycle_id=cycle["cycle_id"],instrument=item.instrument,
                reason=reason,category=category,detail={"rank":rank,"rank_score":item.rank_score})
            events+=1
    if winner:
        tracker.link_winner(cycle["cycle_id"],**winner)
    return {"enabled":True,"created":created,"non_counterfactual_rejections":events,
            "execution_authority":False,"research_authority":False,"look_ahead":False}


def _persist_multi_asset_cycle(cycle: Dict[str, Any]) -> None:
    c=conn()
    c.execute("""INSERT OR REPLACE INTO multi_asset_decision_cycles(
      cycle_id,ts,broker_mode,trading_environment,nlv,slot_tier,max_slots,slots_available,
      open_positions,candidates_json,ranking_json,selected_json,rejected_json,metadata_json)
      VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
      (cycle["cycle_id"],cycle["ts"],cycle["broker_mode"],TRADING_ENVIRONMENT,cycle.get("nlv"),
       cycle.get("slot_tier"),int(cycle.get("max_slots") or 0),int(cycle.get("slots_available") or 0),
       int(cycle.get("open_positions") or 0),json.dumps(cycle.get("candidates") or [],separators=(",",":"),default=str),
       json.dumps(cycle.get("ranking") or [],separators=(",",":"),default=str),
       json.dumps(cycle.get("selected") or [],separators=(",",":"),default=str),
       json.dumps(cycle.get("rejected") or [],separators=(",",":"),default=str),
       json.dumps(cycle.get("metadata") or {},separators=(",",":"),default=str)))
    c.commit();c.close()


async def execute_ranked_candidate(client: httpx.AsyncClient, candidate: Dict[str, Any], cycle_id: str,
                                   *, rank: Optional[int] = None, slot_index: Optional[int] = None,
                                   selected_confirmed: Optional[List[Dict[str, Any]]] = None,
                                   max_slots: Optional[int] = None) -> Dict[str, Any]:
    """Freshly revalidate and submit one ranked OANDA candidate.

    Clear pre-submit rejections and explicit broker rejections may fall through to
    the next ranked candidate. Any submitted/unknown outcome blocks fallback until
    RecoveryManager reconciliation establishes authoritative broker state.
    """
    inst=InstrumentRegistry.normalize_symbol(candidate.get("instrument"))
    selected_confirmed=list(selected_confirmed or [])
    try:
        ctx=await build_broker_risk_context(client)
    except Exception as e:
        return {"executed":False,"instrument":inst,"reason":"BROKER_RISK_CONTEXT_UNAVAILABLE",
                "pre_execution_rejection":True,"fallback_allowed":True,"error":str(e)}
    if max_slots is not None and int(ctx.get("open_positions") or 0) >= int(max_slots):
        return {"executed":False,"instrument":inst,"reason":"NO_FRESH_SLOT_AVAILABLE",
                "pre_execution_rejection":True,"fallback_allowed":True}
    guard=_batch_portfolio_guard(candidate,selected_confirmed,ctx)
    if not guard.get("allow"):
        return {"executed":False,"instrument":inst,"reason":"GLOBAL_PORTFOLIO_RISK_GUARD","guard":guard,
                "pre_execution_rejection":True,"fallback_allowed":True}
    if inst != PRIMARY_INSTRUMENT and instrument_metadata(inst).source != "OANDA":
        try:
            await refresh_instrument_metadata(client,[inst],force=True)
        except Exception as e:
            return {"executed":False,"instrument":inst,"reason":"INSTRUMENT_METADATA_UNVERIFIED",
                    "pre_execution_rejection":True,"fallback_allowed":True,"error":str(e)}
        if instrument_metadata(inst).source != "OANDA":
            return {"executed":False,"instrument":inst,"reason":"INSTRUMENT_METADATA_UNVERIFIED",
                    "pre_execution_rejection":True,"fallback_allowed":True}
    broker_guard=_oanda_batch_broker_guard(candidate,selected_confirmed,ctx)
    if not broker_guard.get("allow"):
        return {"executed":False,"instrument":inst,"reason":"BROKER_RISK_GUARD","guard":broker_guard,
                "pre_execution_rejection":True,"fallback_allowed":True}
    if not new_entry_time_gate().get("allowed"):
        return {"executed":False,"instrument":inst,"reason":"GLOBAL_ENTRY_TIME_GATE",
                "pre_execution_rejection":True,"fallback_allowed":True}
    if RECOVERY_MANAGER_ENABLED and not recovery_manager.new_trades_allowed():
        return {"executed":False,"instrument":inst,"reason":"RECOVERY_SAFE_MODE",
                "pre_execution_rejection":True,"fallback_allowed":True}
    if not (AUTO and bool(managed_value("execution.auto_trade",AUTO))):
        return {"executed":False,"instrument":inst,"reason":"AUTO_TRADE=false",
                "pre_execution_rejection":True,"fallback_allowed":True}

    batch_ctx=candidate.get("_batch_context") or {}
    signal_id=(batch_ctx.get("signal_id") or candidate.get("signal_id"))
    candidate["broker_risk_context"]=ctx
    candidate["portfolio_execution_guard"]=guard
    candidate["_batch_execution_intent"]={
        "cycle_id":cycle_id,"instrument":inst,"signal_id":signal_id,
        "decision_id":batch_ctx.get("director_id"),"created_at":now_iso(),
        "status":"RESERVED","rank":rank,"slot_index":slot_index,
        "broker":"OANDA","environment":TRADING_ENVIRONMENT,
    }
    trace_id=candidate.get("correlation_id")
    x=await execute_recoverable(
        client,candidate,trace_id,batch_ctx.get("director_id"),batch_ctx.get("risk_shadow_id")
    ) if RECOVERY_MANAGER_ENABLED else await execute(client,candidate)
    intent_obj=(x or {}).get("intent") if isinstance(x,dict) else None
    intent_state=(intent_obj or {}).get("state")
    if not x:
        return {"executed":False,"instrument":inst,"reason":"BROKER_RESULT_MISSING",
                "uncertain":True,"fallback_allowed":False}
    if x.get("status_unknown"):
        return {"executed":False,"instrument":inst,"reason":"ORDER_STATUS_UNKNOWN","broker_result":x,
                "intent":intent_obj,"intent_state":intent_state or "UNKNOWN","uncertain":True,"fallback_allowed":False}
    if x.get("submitted") or intent_state in {"SUBMITTING","SUBMITTED","ACKNOWLEDGED","UNKNOWN"}:
        return {"executed":False,"instrument":inst,"reason":"ORDER_SUBMITTED_NOT_CONFIRMED","broker_result":x,
                "intent":intent_obj,"intent_state":intent_state,"uncertain":True,"fallback_allowed":False}
    if x.get("rejected"):
        return {"executed":False,"instrument":inst,"reason":"BROKER_EXPLICIT_REJECTION","broker_result":x,
                "intent":intent_obj,"intent_state":intent_state or "REJECTED","explicit_rejection":True,"fallback_allowed":True}
    if x.get("skipped"):
        duplicate=x.get("skipped")=="DUPLICATE_INTENT_PREVENTED"
        existing_state=(intent_obj or {}).get("state")
        if duplicate and existing_state not in {"REJECTED","CANCELLED"}:
            return {"executed":False,"instrument":inst,"reason":"DUPLICATE_INTENT_REQUIRES_RECONCILIATION",
                    "broker_result":x,"intent":intent_obj,"intent_state":existing_state,
                    "uncertain":True,"fallback_allowed":False}
        return {"executed":False,"instrument":inst,"reason":str(x.get("skipped")),"broker_result":x,
                "intent":intent_obj,"intent_state":existing_state,"pre_execution_rejection":True,"fallback_allowed":True}

    fill=x.get("orderFillTransaction") or {}
    if not fill:
        return {"executed":False,"instrument":inst,"reason":"BROKER_RESULT_WITHOUT_CONFIRMED_FILL","broker_result":x,
                "intent":intent_obj,"intent_state":intent_state,"uncertain":True,"fallback_allowed":False}
    order_id=str(fill.get("id") or "")
    trade_id=str((fill.get("tradeOpened") or {}).get("tradeID") or "")
    fill_price=float(fill.get("price") or candidate.get("entry"))
    actual_units=_risk_float((fill.get("tradeOpened") or {}).get("units"))
    if actual_units is None:
        actual_units=_risk_float(x.get("filled_units"),UNITS)
    if trade_id:
        protection_reanchor=await reanchor_post_fill_protection(client,candidate,trade_id,fill_price,trace_id)
        protection_verification=protection_reanchor.get("verification") or {}
        protection={**protection_verification,
                    "status":"OK" if protection_reanchor.get("confirmed") else "PROTECTION_ERROR",
                    "reanchor_status":protection_reanchor.get("status"),
                    "detail":f"reanchor={protection_reanchor.get('status')}; {protection_verification.get('detail','')}"}
        effective_stop=protection_reanchor.get("effective_stop")
        effective_target=protection_reanchor.get("effective_target")
        if effective_stop is not None and effective_target is not None:
            register_trade_management(trade_id,candidate,float(candidate.get("managed_target",candidate.get("target"))),actual_units,fill_price,
                                      applied_stop=float(effective_stop),applied_target=float(effective_target))
        elif RECOVERY_MANAGER_ENABLED:
            recovery_manager.enter_safe_mode("Post-fill broker protection geometry unavailable for trade management",
                                             correlation_id=trace_id,severity="CRITICAL")
    else:
        protection_reanchor={"status":"MISSING_TRADE_ID","confirmed":False,"verification":None,
                             "effective_stop":None,"effective_target":None}
        protection={"status":"PROTECTION_ERROR","sl_ok":False,"tp_ok":False,"detail":"missing trade id",
                    "reanchor_status":"MISSING_TRADE_ID"}
    slip=(fill_price-float(candidate.get("entry")))/pip_size(inst)
    if candidate.get("signal")=="SELL": slip=-slip
    if signal_id:
        c=conn()
        c.execute("UPDATE signals SET executed=1,order_id=?,decision_reason=decision_reason||? WHERE id=?",
                  (order_id,f"; BATCH_SELECTED cycle={cycle_id}",signal_id))
        try:c.execute("UPDATE learning_samples SET executed=1 WHERE signal_id=?",(signal_id,))
        except sqlite3.OperationalError:pass
        c.commit();c.close()
        if RECOVERY_MANAGER_ENABLED and intent_obj and intent_obj.get("execution_intent_id"):
            recovery_manager.link_signal(intent_obj["execution_intent_id"],int(signal_id))
    conf=batch_ctx.get("confidence") or {"probability":candidate.get("dynamic_confidence"),"source":candidate.get("confidence_source"),"samples":candidate.get("confidence_samples"),"variant":candidate.get("setup_variant")}
    director=batch_ctx.get("director") or candidate.get("ai_strategy_director") or {}
    risk_shadow=batch_ctx.get("risk_shadow") or candidate.get("adaptive_risk_engine") or {}
    if trade_id and signal_id:
        record_trade_memory_entry(
            trade_id=trade_id,signal_id=int(signal_id),order_id=order_id,r=candidate,conf=conf,
            director=director,risk_shadow=risk_shadow,pre_execution_reason=f"BATCH_SELECTED cycle={cycle_id}",
            fill=fill,fill_price=float(fill_price),entry_slippage_pips=slip,
            protection_reanchor=protection_reanchor
        )
    save_decision(candidate,conf,1,f"BATCH_SELECTED cycle={cycle_id}; broker fill confirmed")
    if trace_id:
        observability_manager.link_trace(trace_id,signal_id=signal_id,order_id=order_id or None,trade_id=trade_id or None,symbol=inst)
    c=conn(); c.execute("""INSERT INTO execution_audit(ts,signal_id,instrument,order_id,trade_id,expected_entry,fill_price,slippage_pips,
      stop_loss_ok,take_profit_ok,protection_status,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
      (now_iso(),signal_id,inst,order_id,trade_id,candidate.get("entry"),fill_price,slip,
       int(bool(protection.get("sl_ok"))),int(bool(protection.get("tp_ok"))),protection.get("status"),
       f"V3.37 batch cycle={cycle_id}; reanchor={protection_reanchor.get('status')}; {protection.get('detail')}"))
    c.commit();c.close()
    return {"executed":True,"instrument":inst,"order_id":order_id,"trade_id":trade_id,
            "fill_price":fill_price,"slippage_pips":slip,"protection":protection,
            "protection_reanchor":protection_reanchor,"cycle_id":cycle_id,
            "intent":intent_obj,"intent_state":intent_state or "FILLED","fallback_allowed":False}


async def scan_instruments_once(client: httpx.AsyncClient) -> bool:
    """COLLECT -> RANK -> SLOT/BROKER/PORTFOLIO CHECK -> EXECUTE.

    Every configured analysis instrument is collected before any new order is
    sent, eliminating loop-order slot bias. Existing open-trade management still
    runs inside each scan and remains independent of new-entry selection.
    """
    cycle_ok=True
    collected=[]
    for inst in SCAN_INSTRUMENTS:
        try:
            if WEEKEND_RESEARCH_ENABLED and market_is_weekend_closed():
                snap=await collect_weekend_news_snapshot(client,inst)
                state.setdefault("weekend_research",{})[inst]=snap
            result=await _batch_scan_candidate(client,inst)
            collected.append(result)
            state["last_results"][inst]=result
            state.setdefault("instrument_state",{})[inst]={
                "instrument":inst,"mode":instrument_mode(inst),"last_scan":now_iso(),"ok":True,
                "last_candle":result.get("candle_ts") if isinstance(result,dict) else None,
                "metadata":instrument_metadata(inst).as_dict(),
            }
            if OBSERVABILITY_ENABLED:
                observability_manager.recover(f"SCAN_FAILURE:{inst}",f"{inst} scan recovered")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            cycle_ok=False
            state["last_results"][inst]={"error":str(e)}
            state.setdefault("instrument_state",{})[inst]={
                "instrument":inst,"mode":instrument_mode(inst),"last_scan":now_iso(),"ok":False,"error":str(e),
                "metadata":instrument_metadata(inst).as_dict(),
            }
            state["last_error"]=str(e)
            if OBSERVABILITY_ENABLED:
                observability_manager.alert(f"SCAN_FAILURE:{inst}","HIGH","Execution Engine","SCAN_FAILURE",
                                            f"Scan failed for {inst}",details={"error":str(e),"instrument":inst})
            log.exception("scan failed for %s",inst)

    eligible=[x for x in collected if isinstance(x,dict) and x.get("batch_selection_eligible") and x.get("signal") in ("BUY","SELL")]
    try:
        risk_context=await build_broker_risk_context(client)
    except Exception as e:
        risk_context={"nav":0.0,"open_positions":0,"open_instruments":[],"portfolio_open_risk":0.0,
                      "margin_usage":None,"system_abnormal":True,"errors":[str(e)]}
        cycle_ok=False
    nlv=float(risk_context.get("nav") or 0.0)
    policy=slot_policy(nlv)
    slots_available=max(0,int(policy["max_slots"])-max(0,int(risk_context.get("open_positions") or 0)))
    ranked=rank_opportunities(eligible)
    allocation={
        "policy":policy,"slots_available":slots_available,"selected":[],"rejected":[],
        "ranking":[{"rank":i+1,"instrument":x.instrument,"rank_score":x.rank_score,"components":x.components}
                   for i,x in enumerate(ranked)],
    }
    cycle_id="MA-"+datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    executions=[]
    confirmed=[]
    if MULTI_WORKER_EXECUTION_BLOCKED and ranked:
        allocation["rejected"].extend({
            "instrument":x.instrument,"reason":"SINGLE_EXECUTION_WORKER_REQUIRED",
            "details":EXECUTION_WORKER_CONFIG,"rank_score":x.rank_score,
        } for x in ranked)
        cycle_ok=False
    else:
        for rank_index,item in enumerate(ranked,1):
            if len(confirmed)>=slots_available:
                allocation["rejected"].append({"instrument":item.instrument,"reason":"NO_SLOT_AVAILABLE","rank_score":item.rank_score})
                continue
            selected_candidate={**item.candidate,
                "opportunity_rank_score":item.rank_score,
                "opportunity_rank_components":item.components,
                "selection_reason":"highest_ranked_candidate_passing_fresh_hard_guards",
            }
            # Every submit gets a fresh broker/account/portfolio/metadata/recovery
            # revalidation. A confirmed first fill therefore changes the context
            # used before a possible second slot is submitted.
            result=await execute_ranked_candidate(
                client,selected_candidate,cycle_id,rank=rank_index,slot_index=len(confirmed)+1,
                selected_confirmed=confirmed,max_slots=int(policy["max_slots"]),
            )
            executions.append(result)
            if result.get("executed"):
                confirmed.append(selected_candidate)
                allocation["selected"].append(selected_candidate)
                state["last_results"][selected_candidate["instrument"]]={**state["last_results"].get(selected_candidate["instrument"],{}),
                                                                         "batch_execution":result,"executed":True}
                continue
            allocation["rejected"].append({
                "instrument":item.instrument,"reason":result.get("reason") or "EXECUTION_REJECTED",
                "details":result,"rank_score":item.rank_score,
                "fallback_allowed":bool(result.get("fallback_allowed")),
            })
            if result.get("uncertain") or not result.get("fallback_allowed",False):
                # A submit may have reached OANDA. Do not consume another candidate
                # until RecoveryManager reconciliation resolves the intent.
                cycle_ok=False
                break

    cycle={
        "cycle_id":cycle_id,"ts":now_iso(),"broker_mode":"OANDA_PRACTICE_BATCH_SELECTOR",
        "nlv":nlv,"slot_tier":allocation["policy"]["tier"],"max_slots":allocation["policy"]["max_slots"],
        "slots_available":allocation["slots_available"],"open_positions":int(risk_context.get("open_positions") or 0),
        "candidates":[{"instrument":x.get("instrument"),"signal":x.get("signal"),"score":x.get("score"),
                       "dynamic_confidence":x.get("dynamic_confidence"),"rr_raw":x.get("rr_raw"),
                       "metadata_verified":instrument_metadata(x.get("instrument")).source=="OANDA",
                       "instrument_mode":instrument_mode(x.get("instrument"))} for x in eligible],
        "ranking":allocation.get("ranking") or [],
        "selected":[{"instrument":x.get("instrument"),"rank_score":x.get("opportunity_rank_score")} for x in allocation.get("selected") or []],
        "rejected":allocation.get("rejected") or [],
        "metadata":{"executions":executions,"execution_intents":[x.get("intent") for x in executions if x.get("intent")],
                    "uncertain_submit":any(bool(x.get("uncertain")) for x in executions),
                    "fallback_attempted":len(executions)>len(allocation.get("selected") or []),
                    "worker_configuration":EXECUTION_WORKER_CONFIG,
                    "research_authority":False,"ibkr_execution_authority":False,
                    "look_ahead":False,"configured_universe":list(SCAN_INSTRUMENTS)},
    }
    try:
        cf_obs=_record_counterfactual_cycle(cycle,ranked,allocation,executions,risk_context)
        cycle["metadata"]["counterfactual_shadow"]=cf_obs
    except Exception as e:
        log.exception("counterfactual shadow persistence failed cycle=%s: %s",cycle_id,e)
        cycle["metadata"]["counterfactual_shadow"]={"enabled":True,"error":str(e),"execution_authority":False,"research_authority":False}
        if OBSERVABILITY_ENABLED:
            observability_manager.alert(f"COUNTERFACTUAL_TRACKER:{cycle_id}","WARNING","Observability","COUNTERFACTUAL_TRACKER_FAILURE",
                                        "Counterfactual shadow persistence failed",details={"cycle_id":cycle_id,"error":str(e),"execution_authority":False})
    state["multi_asset_decision_cycle"]=cycle
    _persist_multi_asset_cycle(cycle)
    return cycle_ok


async def worker():
    state["worker_running"] = True
    state["worker_started_at"] = now_iso()
    if OBSERVABILITY_ENABLED:
        _obs_module("Execution Engine","OK",last_operation="scanner worker started")
        observability_manager.recover("EXECUTION_WORKER_OFFLINE","Execution worker recovered")
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
                cycle_ok = await scan_instruments_once(client)
                if OBSERVABILITY_ENABLED:
                    obs_broker=await observability_broker_snapshot(client)
            if OBSERVABILITY_ENABLED:
                try:
                    observability_refresh_noncritical_modules()
                    observability_strategy_degradation_summary()
                    observability_silent_anomalies()
                    observability_manager.prune()
                    storage_lifecycle_manager.prune()
                    m=observability_manager.sample_system_metrics(broker_latency_ms=(obs_broker.get("latency_ms") if 'obs_broker' in locals() else None))
                    db_status="DEGRADED" if m.get("db_latency_ms") is not None and m["db_latency_ms"]>OBSERVABILITY_DB_LATENCY_WARNING_MS else "OK"
                    _obs_module("Database",db_status,m.get("db_latency_ms"),warnings=["database latency elevated"] if db_status!="OK" else [],last_operation="observability SELECT 1")
                    if db_status!="OK":
                        observability_manager.alert("DATABASE_LATENCY","WARNING","Database","DATABASE_LATENCY_HIGH",
                                                    "Database latency is elevated",details={"latency_ms":m.get("db_latency_ms")})
                    else:observability_manager.recover("DATABASE_LATENCY","Database latency recovered",{"latency_ms":m.get("db_latency_ms")})
                    cc=conn();latest_risk=cc.execute("SELECT emergency_stop FROM adaptive_risk_decisions ORDER BY id DESC LIMIT 1").fetchone();cc.close()
                    emergency=bool(latest_risk and latest_risk["emergency_stop"])
                    paused=bool(not AUTO or deployment_manager.kill("SYSTEM").get("active"))
                    gh=observability_manager.global_health(trading_paused=paused,emergency_stop=emergency)
                    critical_active=[a for a in observability_manager.active_alerts() if a.get("severity")=="CRITICAL"]
                    if critical_active and gh["status"]!="EMERGENCY_STOP":
                        gh={"status":"CRITICAL","reasons":list(dict.fromkeys(gh.get("reasons",[])+[a["event_type"] for a in critical_active]))}
                    state.setdefault("observability",{})["system_health"]=gh
                    state["observability"]["last_refresh"]=now_iso()
                    if OBSERVABILITY_CRITICAL_FAILSAFE_ENABLED and gh["status"]=="CRITICAL" and not deployment_manager.kill("SYSTEM").get("active"):
                        deployment_manager.set_kill("SYSTEM",True,"Observability critical fail-safe: "+";".join(gh.get("reasons",[])),"OBSERVABILITY")
                except Exception as e:
                    log.exception("observability refresh failed: %s",e)
            if cycle_ok:
                state["successful_cycles"] += 1
                state["last_successful_scan"] = now_iso()
                state["last_error"] = None
                if OBSERVABILITY_ENABLED:
                    for inst in SCAN_INSTRUMENTS: observability_manager.recover(f"SCAN_FAILURE:{inst}",f"{inst} scan recovered")
                    if OBSERVABILITY_STARTUP_BLOCK_TRADING and not state.get("system_ready"):
                        try: await observability_startup_health_check()
                        except Exception as e: log.warning("startup health recheck failed: %s",e)
            if datetime.now(timezone.utc) - last_train_check >= timedelta(hours=1):
                try:
                    state.setdefault("learning_by_instrument",{})
                    for learning_inst in SCAN_INSTRUMENTS:
                        retrain=should_retrain_model(learning_inst)
                        result=train_shadow_model(False,learning_inst) if retrain["ready"] else {"trained":False,"reason":f"waiting for evidence: {retrain['labeled']}/{retrain['next_training_at']}","samples":retrain["labeled"],"instrument":learning_inst}
                        state["learning_by_instrument"][learning_inst]={**result,"last_train":now_iso(),"model_ready":bool(shadow_model_governance_status(learning_inst).get("ready")),"retrain_policy":retrain}
                    state["learning"]=state["learning_by_instrument"].get(PRIMARY_INSTRUMENT,{})
                except Exception as e:
                    log.exception("learning cycle failed")
                    state["learning"] = {"trained": False, "last_train": now_iso(), "model_ready": Path(MODEL_PATH).exists(), "error": str(e)}
                last_train_check = datetime.now(timezone.utc)
    finally:
        state["worker_running"] = False
        if OBSERVABILITY_ENABLED:
            _obs_module("Execution Engine","OFFLINE",errors=["scanner worker stopped"])
            observability_manager.alert("EXECUTION_WORKER_OFFLINE","CRITICAL","Execution Engine","MODULE_OFFLINE",
                                        "Scanner/execution worker stopped")
        log.warning("Scanner worker stopped")


async def supervised_worker_loop():
    first_launch=True
    while True:
        app.state.restart_requested = False
        # The initial worker launch is not a restart. Increment only when the
        # supervisor has to launch a replacement worker after the first one.
        if not first_launch:
            state["worker_restarts"] += 1
        first_launch=False
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



def _obs_json_value(value,default=None):
    try:return json.loads(value) if value else (default if default is not None else {})
    except Exception:return default if default is not None else {}


def observability_global_health_snapshot() -> Dict[str,Any]:
    system_kill=deployment_manager.kill("SYSTEM") if DEPLOYMENT_MANAGER_ENABLED else {"active":False}
    c=conn();lr=c.execute("SELECT emergency_stop,reason FROM adaptive_risk_decisions ORDER BY id DESC LIMIT 1").fetchone();c.close()
    recovery_state=recovery_manager.state() if RECOVERY_MANAGER_ENABLED else {}
    emergency=bool((lr and lr["emergency_stop"]) or recovery_state.get("emergency_stop"))
    recovery_paused=bool(RECOVERY_MANAGER_ENABLED and not recovery_manager.new_trades_allowed())
    gh=observability_manager.global_health(
        trading_paused=bool(not AUTO or system_kill.get("active") or recovery_paused),
        emergency_stop=emergency
    )
    critical_alerts=[x for x in observability_manager.active_alerts() if x.get("severity")=="CRITICAL"]
    if critical_alerts and gh["status"] not in ("EMERGENCY_STOP",):
        gh={"status":"CRITICAL","reasons":list(dict.fromkeys(gh.get("reasons",[])+[x["event_type"] for x in critical_alerts]))}
    return {**gh,"system_ready":bool(state.get("system_ready")),
            "trading_enabled":bool(AUTO and state.get("system_ready") and not system_kill.get("active") and not recovery_paused),
            "emergency_stop":emergency,"global_kill_switch":system_kill,
            "recovery_state":recovery_state,"active_critical_alerts":len(critical_alerts)}


def observability_dashboard_snapshot() -> Dict[str,Any]:
    c=conn()
    capital=c.execute("SELECT * FROM observability_capital_history ORDER BY id DESC LIMIT 1").fetchone()
    sysm=c.execute("SELECT * FROM observability_metrics ORDER BY id DESC LIMIT 1").fetchone()
    strategies=[dict(x) for x in c.execute("SELECT * FROM strategy_health ORDER BY setup_variant").fetchall()]
    directors=[dict(x) for x in c.execute("""SELECT d.* FROM ai_strategy_director_decisions d JOIN (
        SELECT setup_variant,MAX(id) id FROM ai_strategy_director_decisions GROUP BY setup_variant) x ON x.id=d.id
        ORDER BY d.id DESC LIMIT 100""").fetchall()]
    risk=c.execute("SELECT * FROM adaptive_risk_decisions ORDER BY id DESC LIMIT 1").fetchone()
    risk_by_strategy=[dict(x) for x in c.execute("""SELECT a.* FROM adaptive_risk_decisions a JOIN (
        SELECT setup_variant,MAX(id) id FROM adaptive_risk_decisions GROUP BY setup_variant) x ON x.id=a.id
        ORDER BY a.id DESC LIMIT 100""").fetchall()]
    risk_by_asset=[dict(x) for x in c.execute("""SELECT a.* FROM adaptive_risk_decisions a JOIN (
        SELECT instrument,MAX(id) id FROM adaptive_risk_decisions GROUP BY instrument) x ON x.id=a.id
        ORDER BY a.id DESC LIMIT 100""").fetchall()]
    portfolio=c.execute("SELECT * FROM portfolio_risk_state WHERE id=1").fetchone()
    risk_blocks=c.execute("SELECT COUNT(*) n FROM adaptive_risk_decisions WHERE allow_new_trades=0 AND ts>=?",
                          ((datetime.now(timezone.utc)-timedelta(days=1)).isoformat(),)).fetchone()["n"]
    positions=[dict(x) for x in c.execute("SELECT * FROM active_trade_management WHERE closed=0 ORDER BY opened_ts").fetchall()]
    candidates=[dict(x) for x in c.execute("""SELECT cr.*,cs.strategy_id parent_strategy,cs.candidate_version,cs.status learning_status,
        cs.expected_improvement,dr.current_stage deployment_stage,dr.allocation_fraction,dr.last_health_check_ts
        FROM candidate_registry cr JOIN candidate_strategies cs ON cs.candidate_id=cr.candidate_id
        LEFT JOIN deployment_registry dr ON dr.candidate_id=cr.candidate_id ORDER BY cr.updated_ts DESC""").fetchall()]
    paper=[dict(x) for x in c.execute("""SELECT candidate_id,COUNT(*) trades,
        SUM(CASE WHEN status='DONE' THEN 1 ELSE 0 END) resolved,
        SUM(CASE WHEN realized_r IS NOT NULL THEN realized_r ELSE 0 END) total_r,
        AVG(CASE WHEN realized_r IS NOT NULL THEN realized_r END) expectancy_r,
        MAX(resolved_ts) last_update FROM candidate_paper_trades GROUP BY candidate_id""").fetchall()]
    al=c.execute("SELECT * FROM adaptive_learning_runs ORDER BY id DESC LIMIT 1").fetchone()
    al_counts={x["status"]:x["n"] for x in c.execute("SELECT status,COUNT(*) n FROM candidate_strategies GROUP BY status").fetchall()}
    next_eval=c.execute("SELECT MIN(cooldown_until) ts FROM candidate_strategies WHERE cooldown_until>?",(now_iso(),)).fetchone()["ts"]
    pending_recommendations=[dict(x) for x in c.execute("""SELECT candidate_id,strategy_id,candidate_version,parameter_name,proposed_value_json,
        reason,candidate_score,confidence FROM candidate_strategies WHERE status='ACCEPTED_AS_CANDIDATE' ORDER BY id DESC LIMIT 50""").fetchall()]
    val=[dict(x) for x in c.execute("SELECT candidate_id,candidate_version,completed_ts,final_status,validation_score,final_reason FROM candidate_validation_runs ORDER BY started_ts DESC LIMIT 50").fetchall()]
    drift=[dict(x) for x in c.execute("SELECT * FROM concept_drift_alerts WHERE status='POSSIBLE_CONCEPT_DRIFT' ORDER BY ts DESC").fetchall()]
    latest_exec=[dict(x) for x in c.execute("SELECT * FROM execution_audit ORDER BY id DESC LIMIT 20").fetchall()]
    latest_signal=c.execute("SELECT * FROM signals ORDER BY id DESC LIMIT 1").fetchone()
    c.close()
    dir_by={x["setup_variant"]:x for x in directors}
    for st in strategies:
        d=dir_by.get(st["setup_variant"]);st["director_confidence"]=d.get("confidence") if d else None
        st["director_state"]=d.get("recommended_state") if d else None
        st["monitor_state"]="PAUSED" if st.get("status") in ("PAUSED","DEGRADED") else "ACTIVE"
    depdash=deployment_manager.dashboard() if DEPLOYMENT_MANAGER_ENABLED else {"deployments":[],"kill_switches":[]}
    canary_by={x["candidate_id"]:x.get("live_metrics",{}) for x in depdash.get("deployments",[])}
    paper_by={x["candidate_id"]:x for x in paper}
    for x in candidates:
        x["paper_metrics"]=paper_by.get(x["candidate_id"],{})
        x["canary_metrics"]=canary_by.get(x["candidate_id"],{})
    broker_module=next((x for x in observability_manager.module_rows() if x["module_name"]=="Broker Connection"),None)
    broker_details=_obs_json_value(broker_module.get("details_json") if broker_module else None,{})
    riskd=dict(risk) if risk else {}
    portd=dict(portfolio) if portfolio else {}
    open_risk=portd.get("portfolio_open_risk") or riskd.get("portfolio_open_risk") or 0
    risk_dashboard={"latest_decision":riskd,"portfolio":portd,"by_strategy":risk_by_strategy,"by_asset":risk_by_asset,
                    "blocked_last_24h":int(risk_blocks),
                    "remaining_risk_budget":max(0.0,float(managed_value("risk.max_portfolio_fraction",RISK_MAX_PORTFOLIO_FRACTION))-float(open_risk or 0)),
                    "hard_limits":{"trade":float(managed_value("risk.max_trade_fraction",RISK_MAX_TRADE_FRACTION)),
                                   "strategy":float(managed_value("risk.max_strategy_fraction",RISK_MAX_STRATEGY_FRACTION)),
                                   "portfolio":float(managed_value("risk.max_portfolio_fraction",RISK_MAX_PORTFOLIO_FRACTION)),
                                   "drawdown_warn":float(managed_value("risk.drawdown_warning",RISK_DRAWDOWN_WARN)),
                                   "drawdown_stop":float(managed_value("risk.drawdown_stop",RISK_DRAWDOWN_STOP))}}
    security_snapshot=security_manager.dashboard()
    actor_roles={}
    for a in security_snapshot.pop("configured_actors",[]):
        actor_roles[a.get("role")]=actor_roles.get(a.get("role"),0)+1
    security_snapshot["configured_actor_role_counts"]=actor_roles
    security_snapshot["risk_config_version"]=f"config_v{security_manager.current_version()}"
    security_snapshot["production_strategy_versions"]=[
        {"strategy":x.get("setup_variant"),"version":f"{x.get('setup_variant')}@{VERSION_TAG}"}
        for x in strategies
    ]
    security_snapshot["deployment_versions"]=[
        {"candidate_id":x.get("candidate_id"),"production_version":x.get("production_version"),
         "candidate_version":x.get("candidate_version"),"stage":x.get("current_stage")}
        for x in depdash.get("deployments",[])
    ]
    system_evaluation_snapshot={"enabled":SYSTEM_EVALUATION_ENABLED,"observation_only":True}
    if SYSTEM_EVALUATION_ENABLED:
        try:
            latest_eval=system_evaluation_engine.latest()
            if latest_eval:
                system_evaluation_snapshot.update({
                    "evaluation_id":latest_eval.get("evaluation_id"),
                    "generated_at":latest_eval.get("generated_at"),
                    "SYSTEM_SCORE":latest_eval.get("system_score"),
                    "current_status":latest_eval.get("system_status"),
                    "main_degradation_factor":latest_eval.get("main_degradation_factor"),
                    "biggest_risk_contributor":latest_eval.get("biggest_risk_contributor"),
                    "recommendations":latest_eval.get("recommendations",[]),
                    "executive_summary":latest_eval.get("executive_summary",{}),
                    "confidence_level":latest_eval.get("confidence_level"),
                    "data_quality_score":latest_eval.get("data_quality_score")
                })
            else:
                system_evaluation_snapshot["status"]="NO_EVALUATION_YET"
        except Exception as e:
            system_evaluation_snapshot.update({"status":"ERROR","error":str(e)})
    governance_snapshot={"enabled":GOVERNANCE_ENABLED,"trading_signal_authority":False}
    if GOVERNANCE_ENABLED:
        try:
            governance_snapshot.update(governance_engine.dashboard())
        except Exception as e:
            governance_snapshot.update({"status":"ERROR","error":str(e)})
    smart_execution_snapshot={"enabled":SMART_EXECUTION_ENABLED,"mode":"SHADOW","policy_authority":False}
    if SMART_EXECUTION_ENABLED:
        try:
            smart_execution_snapshot.update(smart_execution_engine.dashboard())
        except Exception as e:
            smart_execution_snapshot.update({"status":"ERROR","error":str(e)})
    ensemble_snapshot={"enabled":ENSEMBLE_ENABLED,"mode":"SHADOW","policy_authority":False,
                       "risk_increase_authority":False,"signal_authority":False}
    if ENSEMBLE_ENABLED:
        try:ensemble_snapshot.update(ensemble_engine.dashboard())
        except Exception as e:ensemble_snapshot.update({"status":"ERROR","error":str(e)})
    recovery_snapshot={"enabled":RECOVERY_MANAGER_ENABLED}
    if RECOVERY_MANAGER_ENABLED:
        try:
            recovery_snapshot.update({"state":recovery_manager.state(),"metrics":recovery_manager.metrics(),
                                      "unknown_orders":[x for x in recovery_manager.orders(100) if x.get("state")=="UNKNOWN"],
                                      "incidents":recovery_manager.incidents(50)})
        except Exception as e:
            recovery_snapshot.update({"state":{"state":"CRITICAL_FAILURE"},"error":str(e)})
    return {
        "generated_at":now_iso(),"version":VERSION_TAG,"system":observability_global_health_snapshot(),
        "recovery":recovery_snapshot,
        "system_evaluation":system_evaluation_snapshot,
        "governance":governance_snapshot,
        "smart_execution":smart_execution_snapshot,
        "ensemble":ensemble_snapshot,
        "security_change_control":security_snapshot,
        "startup_health":state.get("startup_health"),"modules":observability_manager.module_rows(),
        "alerts":observability_manager.active_alerts(),"system_metrics":dict(sysm) if sysm else None,
        "capital":dict(capital) if capital else None,
        "broker":{"module":broker_module,"details":broker_details,"connection_status":broker_module.get("status") if broker_module else "OFFLINE"},
        "market":{"symbols":INSTRUMENTS,"regimes":state.get("market_regimes",{}),
                  "market_data_capabilities":{"candles":True,"tick_feed":False,"order_book":False}},
        "strategies":strategies,"strategy_degradation":observability_strategy_degradation_summary(),
        "ai_strategy_director":{"latest_by_strategy":directors,"pending_recommendations":pending_recommendations},"risk_engine":risk_dashboard,
        "positions":{"internal_open":positions,"broker_open_instruments":broker_details.get("broker_instruments",[]),
                     "source_of_truth":"broker when available"},
        "candidates":candidates,"deployment":depdash,
        "adaptive_learning":{"latest_run":dict(al) if al else None,"candidate_counts":al_counts,"concept_drift":drift,"next_evaluation":next_eval},
        "validation":{"latest":val},"execution":{"recent_audit":latest_exec,"latest_signal":dict(latest_signal) if latest_signal else None},
        "production_readiness":production_readiness_gate.dashboard(production_runtime_context()) if PRODUCTION_READINESS_ENABLED else {"enabled":False},
        "observability":{"critical_fail_safe_enabled":OBSERVABILITY_CRITICAL_FAILSAFE_ENABLED,
                         "startup_block_trading":OBSERVABILITY_STARTUP_BLOCK_TRADING,
                         "last_refresh":state.get("observability",{}).get("last_refresh")}
    }


@app.on_event("startup")
async def start():
    conn().close()
    deployment_manager.ensure_schema()
    security_manager.protect_existing_history_tables()
    deployment_manager.mark_restart()
    observability_manager.ensure_schema()
    observability_manager.begin_session()
    if SYSTEM_EVALUATION_ENABLED:
        system_evaluation_engine.ensure_schema()
    if GOVERNANCE_ENABLED:
        governance_engine.ensure_schema()
        sync_governance_runtime_config()
    if PRODUCTION_READINESS_ENABLED:
        production_readiness_gate.ensure_schema()
        sync_production_readiness_config()
    if RECOVERY_MANAGER_ENABLED:
        recovery_manager.ensure_schema()
        canary_recovery_manager.ensure_schema()
        recovery_manager.set_state("RECOVERING","BOOT after process start",safe_mode=True,new_trades_allowed=False)
    security_result=security_startup_check()
    _obs_module("System Evaluation Engine","OK" if SYSTEM_EVALUATION_ENABLED else "PAUSED",
                last_operation="evaluation schema loaded",
                details={"observation_only":True,"period_hours":SYSTEM_EVALUATION_PERIOD_HOURS})
    _obs_module("Governance Engine","OK" if GOVERNANCE_ENABLED else "PAUSED",
                last_operation="governance policy/state loaded",
                details={"mode":governance_engine.mode if GOVERNANCE_ENABLED else "DISABLED",
                         "shadow_first":True,"trading_signal_authority":False})
    _obs_module("Smart Execution Engine","OK" if SMART_EXECUTION_ENABLED else "PAUSED",
                last_operation="smart execution schema/policy loaded",
                details={"mode":"SHADOW" if SMART_EXECUTION_ENABLED else "DISABLED",
                         "shadow_first":True,"signal_authority":False,"risk_increase_authority":False,
                         "actual_order_policy_unchanged":True})
    if PRODUCTION_READINESS_ENABLED:
        pst=production_readiness_gate.state()
        _obs_module("Production Readiness Gate","OK" if pst.get("readiness_state") not in ("BLOCKED","SUSPENDED") else "DEGRADED",
                    last_operation="production readiness state loaded",
                    details={"readiness_state":pst.get("readiness_state"),"production_stage":pst.get("production_stage"),
                             "production_authorized_env":PRODUCTION_AUTHORIZED,"dry_run_mode":PRODUCTION_DRY_RUN_MODE,
                             "risk_engine_shadow_mode":RISK_ENGINE_SHADOW_MODE})
    _obs_module("Security Manager","OK" if security_result.get("status")=="SECURITY_READY" else "ERROR",
                last_operation="startup security validation",
                errors=[] if security_result.get("status")=="SECURITY_READY" else security_result.get("environment",{}).get("reasons",[]),
                details=security_result)
    if security_result.get("status")=="SECURITY_READY" and OBSERVABILITY_ENABLED:
        # Clear startup alerts from older deployments only after the current runtime
        # has passed the same fail-closed checks that originally raised them.
        for _key,_msg in (
            ("STARTUP_SECURITY_FAILED","Current startup security validation passed"),
            ("UNKNOWN_CODE_VERSION","Current runtime integrity is verified"),
            ("UNKNOWN_STRATEGY_VERSION","Current deployment/strategy version is valid"),
            ("ADMIN_PERMISSION_CHANGED","Current role configuration is verified"),
            ("CONFIGURATION_CORRUPTION","Current configuration integrity is verified"),
        ):
            observability_manager.recover(_key,_msg,{"security_status":"SECURITY_READY"})
    if security_result.get("status")!="SECURITY_READY":
        state["system_ready"]=False
        if RECOVERY_MANAGER_ENABLED and SECURITY_STARTUP_FAIL_CLOSED:
            recovery_manager.enter_safe_mode("Startup security check failed",severity="CRITICAL")
        if OBSERVABILITY_ENABLED:
            observability_manager.alert("STARTUP_SECURITY_FAILED","CRITICAL","Security Manager","SECURITY_STARTUP_FAILED",
                                        "Security startup validation failed",details=security_result)
            integrity=security_result.get("integrity") or {}
            if integrity.get("reason") in ("UNVERIFIED_RUNTIME_STATE","UNVERIFIED_ROLE_CONFIG_CHANGE"):
                observability_manager.alert("UNKNOWN_CODE_VERSION","CRITICAL","Security Manager","UNKNOWN_CODE_VERSION",
                                            "Runtime code/dependency/role manifest does not match the registered version",
                                            details=integrity)
            if integrity.get("role_config_changed"):
                observability_manager.alert("ADMIN_PERMISSION_CHANGED","CRITICAL","Security Manager","ADMIN_PERMISSION_CHANGED",
                                            "Configured actor/role permissions changed outside an approved runtime change",
                                            details={"role_config_hash":integrity.get("role_config_hash")})
            if not (security_result.get("checks") or {}).get("config_integrity",True):
                observability_manager.alert("CONFIGURATION_CORRUPTION","CRITICAL","Security Manager","CRITICAL_CONFIG_CHANGED",
                                            "Configuration snapshot hash validation failed",details=security_result)
            if not (security_result.get("checks") or {}).get("deployment_state_valid",True):
                observability_manager.alert("UNKNOWN_STRATEGY_VERSION","CRITICAL","Security Manager","UNKNOWN_CODE_VERSION",
                                            "Deployment references an unknown or mismatched strategy version",
                                            details=security_result)
    _obs_module("Database","OK",last_operation="schema initialized")
    _obs_module("Execution Engine","OK",last_operation="execution module loaded")
    _obs_module("Recovery Manager","DEGRADED" if RECOVERY_MANAGER_ENABLED else "PAUSED",last_operation="startup recovery pending")
    _obs_module("AI Strategy Director","OK" if AI_DIRECTOR_ENABLED else "PAUSED",last_operation="director module loaded")
    _obs_module("Trade Memory","OK" if TRADE_MEMORY_ENABLED else "PAUSED",last_operation="trade memory module loaded")
    _obs_module("Market Regime Detector","OK" if MARKET_REGIME_ENABLED else "PAUSED",last_operation="regime module loaded")
    recovery_start={"status":"DISABLED"}
    if RECOVERY_MANAGER_ENABLED:
        try:
            recovery_start=await recovery_startup_sequence()
            _obs_module("Recovery Manager","OK" if recovery_start.get("status")=="READY" else "DEGRADED",
                        last_operation="startup recovery",details=recovery_start)
        except Exception as e:
            recovery_manager.enter_safe_mode(f"startup recovery exception: {e}",severity="CRITICAL")
            recovery_start={"status":"CRITICAL_FAILURE","error":str(e)}
            _obs_module("Recovery Manager","ERROR",errors=[str(e)])
    try:
        await observability_startup_health_check()
        if security_result.get("status")!="SECURITY_READY":
            state["system_ready"]=False
            state["startup_health"]={**(state.get("startup_health") or {}),"security":security_result}
        if RECOVERY_MANAGER_ENABLED and recovery_start.get("status")!="READY":
            state["system_ready"]=False
            state["startup_health"]={**(state.get("startup_health") or {}),"status":"STARTUP_HEALTH_FAILED","recovery":recovery_start}
    except Exception as e:
        state["system_ready"]=False
        state["startup_health"]={"status":"STARTUP_HEALTH_FAILED","error":str(e),"ts":now_iso(),"recovery":recovery_start}
        observability_manager.alert("STARTUP_HEALTH_FAILED","CRITICAL","System","STARTUP_HEALTH_FAILURE",
                                    "Startup health check raised an error",details={"error":str(e),"recovery":recovery_start})
    app.state.restart_requested=False
    app.state.worker_supervisor_task=asyncio.create_task(supervised_worker_loop(),name="scanner-supervisor")
    app.state.watchdog_task=asyncio.create_task(watchdog_loop(),name="scanner-watchdog")
    app.state.observability_loop_task=asyncio.create_task(observability_loop_monitor(),name="observability-loop")
    log.info("V3.21 governance shadow active. AUTO=%s system_ready=%s recovery=%s",
             AUTO,state.get("system_ready"),recovery_manager.state().get("state") if RECOVERY_MANAGER_ENABLED else "DISABLED")

@app.on_event("shutdown")
async def shutdown():
    for name in ("scanner_worker_task", "worker_supervisor_task", "watchdog_task", "observability_loop_task"):
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
    gh=observability_global_health_snapshot() if OBSERVABILITY_ENABLED else {"status":"UNKNOWN","reasons":[]}
    return {"ok": ok and bool(state.get("system_ready")), "practice_only": True, "auto_trade": AUTO,
            "system_ready":state.get("system_ready"),"system_health":gh,"last_scan": state["last_scan"],
            "last_successful_scan": state["last_successful_scan"], "last_error": state["last_error"],
            "learning_mode": "adaptive_confidence", "adaptive_confidence": ADAPTIVE_CONFIDENCE,
            "startup_health":state.get("startup_health"),
            "recovery":recovery_manager.state() if RECOVERY_MANAGER_ENABLED else None,
            "scanner": {**snap, "stale_effective": stale_effective}}



def security_alert_for_change(bundle: Dict[str,Any]):
    req=(bundle or {}).get("change") or {}
    if not req or not OBSERVABILITY_ENABLED:
        return
    key=req.get("config_key") or ""
    risk=req.get("risk_level")
    if req.get("status")=="APPLIED":
        if key.startswith("risk."):
            event="RISK_LIMIT_CHANGED";sev="CRITICAL"
        elif key.startswith("deployment."):
            event="CRITICAL_CONFIG_CHANGED";sev="CRITICAL"
        else:
            event="CRITICAL_CONFIG_CHANGED" if risk=="CRITICAL" else "CONFIG_CHANGED"
            sev="HIGH" if risk in ("HIGH_RISK","CRITICAL") else "WARNING"
        observability_manager.alert(f"SECURITY_CHANGE:{req.get('change_id')}",sev,"Security Manager",event,
                                    f"Approved configuration change applied: {key}",
                                    details={"change_id":req.get("change_id"),"risk_level":risk,
                                             "status":req.get("status")})


def apply_security_change_side_effects(change_id: str, actor: Dict[str,str], result: Dict[str,Any]) -> Dict[str,Any]:
    sync_security_runtime_config()
    if GOVERNANCE_ENABLED:
        sync_governance_runtime_config()
    if PRODUCTION_READINESS_ENABLED:
        sync_production_readiness_config()
    req=(result.get("change") or {})
    side_effect={"type":"CONFIG_ONLY","applied":True}
    if req.get("config_key","").startswith("research_rule."):
        side_effect=activate_research_rule_from_applied_change(change_id,actor)
    if PRODUCTION_READINESS_ENABLED and production_readiness_gate.state().get("certification_id"):
        material_prefixes=("risk.","strategy.","deployment.","governance.","execution.","system_evaluation.")
        if req.get("risk_level") in ("HIGH_RISK","CRITICAL") or str(req.get("config_key") or "").startswith(material_prefixes):
            production_readiness_gate.invalidate_certification(
                f"MATERIAL_CONFIG_CHANGE:{req.get('config_key')}",actor.get("actor","CHANGE_MANAGER"))
            if OBSERVABILITY_ENABLED:
                observability_manager.alert("CERTIFICATION_INVALIDATED","CRITICAL","Production Readiness Gate",
                                            "CERTIFICATION_INVALIDATED",
                                            "Material configuration change invalidated production certification",
                                            details={"change_id":change_id,"config_key":req.get("config_key")})
    security_alert_for_change(result)
    return side_effect





@app.get("/api/production-readiness/dashboard")
async def production_readiness_dashboard_api(authorization: Optional[str]=Header(None)):
    _security_actor(authorization,"read",allow_read=True)
    return {"enabled":PRODUCTION_READINESS_ENABLED,
            "dashboard":production_readiness_gate.dashboard(production_runtime_context()) if PRODUCTION_READINESS_ENABLED else None}


@app.post("/api/production-readiness/release-candidate/freeze")
async def production_release_candidate_freeze_api(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"candidate_review")
    if not PRODUCTION_READINESS_ENABLED: raise HTTPException(409,"PRODUCTION_READINESS_DISABLED")
    rc=freeze_current_release_candidate(actor["actor"])
    security_manager.audit(actor,"PRODUCTION_RELEASE_CANDIDATE_FROZEN",f"release:{rc['release_id']}",None,
                           {"release_version":VERSION_TAG,"code_fingerprint":rc["code_fingerprint"],"config_fingerprint":rc["config_fingerprint"]},
                           "Step 15 release candidate freeze","APPLIED")
    return {"release_candidate":rc,"important":"Any material code/config/dependency change requires a new release candidate."}


@app.post("/api/production-readiness/account/verify")
async def production_account_verify_api(payload: Dict[str,Any]=Body(...),authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"reset_emergency_stop")
    st=production_readiness_gate.state();rid=st.get("release_id")
    if not rid: raise HTTPException(409,"NO_FROZEN_RELEASE_CANDIDATE")
    if TRADING_ENVIRONMENT!="PRODUCTION" or PRIMARY_OANDA_ENV!="live" or not PRODUCTION_AUTHORIZED:
        result=production_readiness_gate.verify_account(rid,TRADING_ENVIRONMENT,payload.get("expected") or {},
            {"broker":"OANDA","account_id":ACCOUNT,"account_type":PRIMARY_OANDA_ENV,"currency":None,
             "permissions_ok":False,"market_access_ok":False,"leverage_ok":False,"margin_settings_ok":False,
             "balance_within_expected_range":False},actor["actor"])
        raise HTTPException(409,{"reason":"LIVE_ACCOUNT_VERIFICATION_REQUIRES_EXPLICIT_PRODUCTION_ENVIRONMENT_AND_AUTHORIZATION","result":result})
    try:
        async with httpx.AsyncClient() as client:
            account_data=await req(client,"GET","/v3/accounts/{account}")
        acct=account_data.get("account") or {}
        expected=payload.get("expected") or {}
        balance=float(acct.get("balance") or 0)
        observed={"broker":"OANDA","account_id":ACCOUNT,"account_type":"live","currency":acct.get("currency"),
                  "permissions_ok":not bool(acct.get("tradingDisabled",False)),"market_access_ok":not bool(acct.get("tradingDisabled",False)),
                  "leverage_ok": expected.get("margin_rate") is None or abs(float(acct.get("marginRate") or 0)-float(expected["margin_rate"]))<1e-12,
                  "margin_settings_ok":acct.get("marginRate") is not None,
                  "balance_within_expected_range":float(expected.get("min_balance",0))<=balance<=float(expected.get("max_balance",1e100)),
                  "balance":balance,"margin_rate":acct.get("marginRate"),"hedging_enabled":acct.get("hedgingEnabled")}
        result=production_readiness_gate.verify_account(rid,"PRODUCTION",expected,observed,actor["actor"])
        if not result["passed"] and OBSERVABILITY_ENABLED:
            observability_manager.alert("ACCOUNT_MISMATCH","CRITICAL","Production Readiness Gate","ACCOUNT_MISMATCH",
                                        "Production account verification failed",details=result)
        return {"observed":security_sanitize(observed),"result":result}
    except HTTPException: raise
    except Exception as e:
        if OBSERVABILITY_ENABLED:
            observability_manager.alert("ACCOUNT_VERIFICATION_FAILED","CRITICAL","Production Readiness Gate","ACCOUNT_MISMATCH",
                                        "Production account verification could not complete",details={"error":str(e)})
        raise HTTPException(503,"PRODUCTION_ACCOUNT_VERIFICATION_FAILED")


@app.post("/api/production-readiness/final-paper/record")
async def production_final_paper_record_api(payload: Dict[str,Any]=Body(...),authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"candidate_review")
    st=production_readiness_gate.state();rid=st.get("release_id")
    if not rid: raise HTTPException(409,"NO_FROZEN_RELEASE_CANDIDATE")
    release_check=production_readiness_gate.verify_release_unchanged(rid,production_release_files(),security_manager.current_config(),production_release_versions())
    if not release_check.get("passed"): raise HTTPException(409,{"reason":"NEW_RELEASE_CANDIDATE_REQUIRED","release_check":release_check})
    return production_readiness_gate.record_final_paper(rid,payload,actor["actor"])


@app.post("/api/production-readiness/dry-run/record")
async def production_dry_run_record_api(payload: Dict[str,Any]=Body(...),authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"candidate_review")
    st=production_readiness_gate.state();rid=st.get("release_id")
    if not rid: raise HTTPException(409,"NO_FROZEN_RELEASE_CANDIDATE")
    return production_readiness_gate.record_dry_run(rid,payload.get("pipeline") or {},payload.get("expected_order"),
        blocked_before_send=bool(payload.get("blocked_before_send",True)),real_broker_request_count=int(payload.get("real_broker_request_count",0)),actor=actor["actor"])


@app.post("/api/production-readiness/certify")
async def production_certify_api(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"reset_emergency_stop")
    st=production_readiness_gate.state();rid=st.get("release_id")
    if not rid: raise HTTPException(409,"NO_FROZEN_RELEASE_CANDIDATE")
    unchanged=production_readiness_gate.verify_release_unchanged(rid,production_release_files(),security_manager.current_config(),production_release_versions())
    if not unchanged.get("passed"):
        production_readiness_gate.invalidate_certification("NEW_RELEASE_CANDIDATE_REQUIRED:"+",".join(unchanged.get("mismatches") or []),actor["actor"],rid)
        raise HTTPException(409,{"reason":"NEW_RELEASE_CANDIDATE_REQUIRED","release_check":unchanged})
    context=production_certification_context()
    result=production_readiness_gate.certify(context,rid,actor["actor"])
    if OBSERVABILITY_ENABLED:
        key="PRODUCTION_READINESS_LOST" if result.get("go_no_go")!="GO" else "PRODUCTION_READINESS_GO"
        sev="CRITICAL" if result.get("go_no_go")=="NO_GO" and result.get("readiness_state")=="BLOCKED" else "INFO"
        observability_manager.alert(key,sev,"Production Readiness Gate","PRODUCTION_CERTIFICATION",
                                    f"Production certification result: {result.get('go_no_go')}",details={"blockers":result.get("blockers"),"release_id":rid})
    return result


@app.post("/api/production-readiness/activate-minimal-live")
async def production_activate_minimal_live_api(reason: str,authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"reset_emergency_stop")
    if PRODUCTION_DRY_RUN_MODE: raise HTTPException(409,"PRODUCTION_DRY_RUN_MODE_MUST_BE_EXPLICITLY_DISABLED_AFTER_CERTIFICATION")
    context=production_runtime_context()
    result=production_readiness_gate.activate_minimal_live(context,actor["actor"],reason)
    if not result.get("ok"): raise HTTPException(409,result)
    security_manager.audit(actor,"MINIMAL_LIVE_ACTIVATED","production:stage","CERTIFICATION","MINIMAL_LIVE",reason,"APPLIED")
    return result


@app.post("/api/production-readiness/promote/{target_stage}")
async def production_promote_api(target_stage: str,payload: Dict[str,Any]=Body(default={}),authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"reset_emergency_stop")
    if target_stage not in ("LIMITED_LIVE","CONTROLLED_LIVE","PRODUCTION_APPROVED"):
        raise HTTPException(400,"INVALID_PRODUCTION_TARGET_STAGE")
    context={**production_runtime_context(),**(payload or {})}
    result=production_readiness_gate.promotion_gate(target_stage,context,actor["actor"])
    if result.get("action")!="PROMOTE": raise HTTPException(409,result)
    security_manager.audit(actor,"PRODUCTION_STAGE_PROMOTED","production:stage",None,target_stage,
                           "evidence-based production promotion","APPLIED")
    return result


@app.post("/api/production-readiness/suspend")
async def production_suspend_api(reason: str,authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"manual_pause")
    result=production_readiness_gate.suspend(reason,actor["actor"],automatic=False)
    security_manager.audit(actor,"PRODUCTION_SUSPENDED","production:stage",None,"SUSPENDED",reason,"APPLIED")
    if OBSERVABILITY_ENABLED:
        observability_manager.alert("PRODUCTION_SUSPENDED","CRITICAL","Production Readiness Gate","PRODUCTION_SUSPENDED",reason,details=result)
    return result


@app.post("/api/production-readiness/resume")
async def production_resume_api(payload: Dict[str,Any]=Body(default={}),authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"reset_emergency_stop")
    result=production_readiness_gate.resume_gate({**production_runtime_context(),**payload},actor["actor"])
    if result.get("action")!="LIMITED_RESTART": raise HTTPException(409,result)
    security_manager.audit(actor,"PRODUCTION_LIMITED_RESTART","production:stage","SUSPENDED","MINIMAL_LIVE",
                           "incident resolved + reconciliation + health checks","APPLIED")
    return result


@app.post("/api/production-readiness/incidents")
async def production_incident_open_api(payload: Dict[str,Any]=Body(...),authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"manual_pause")
    iid=production_readiness_gate.open_incident(str(payload.get("severity") or "P2"),str(payload.get("incident_type") or "UNKNOWN"),
                                               str(payload.get("summary") or "production incident"))
    security_manager.audit(actor,"PRODUCTION_INCIDENT_OPENED",f"incident:{iid}",None,payload,"production incident","APPLIED")
    return {"incident_id":iid,"state":production_readiness_gate.state()}


@app.post("/api/production-readiness/incidents/{incident_id}/resolve")
async def production_incident_resolve_api(incident_id: str,payload: Dict[str,Any]=Body(...),authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"reset_emergency_stop")
    production_readiness_gate.resolve_incident(incident_id,str(payload.get("root_cause") or ""),payload.get("corrective_actions") or [],
                                               payload.get("controls_worked") or [],payload.get("controls_failed") or [])
    security_manager.audit(actor,"PRODUCTION_INCIDENT_RESOLVED",f"incident:{incident_id}","OPEN","RESOLVED",
                           str(payload.get("root_cause") or "resolved"),"APPLIED")
    return {"resolved":True,"incident_id":incident_id}


@app.get("/api/governance/dashboard")
async def governance_dashboard_api(authorization: Optional[str]=Header(None)):
    _security_actor(authorization,"read",allow_read=True)
    return {"enabled":GOVERNANCE_ENABLED,
            "dashboard":governance_engine.dashboard() if GOVERNANCE_ENABLED else None}


@app.get("/api/governance/decisions")
async def governance_decisions_api(limit: int=200,authorization: Optional[str]=Header(None)):
    _security_actor(authorization,"read",allow_read=True)
    c=conn()
    rows=[dict(x) for x in c.execute("SELECT * FROM governance_decisions ORDER BY timestamp DESC LIMIT ?",
                                     (min(max(limit,1),2000),)).fetchall()]
    c.close()
    return {"decisions":rows}


@app.get("/api/governance/effectiveness")
async def governance_effectiveness_api(authorization: Optional[str]=Header(None)):
    _security_actor(authorization,"read",allow_read=True)
    return governance_engine.effectiveness()


@app.post("/api/governance/evaluate")
async def governance_evaluate_api(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    result=run_governance_cycle("manual")
    security_manager.audit(actor,"GOVERNANCE_EVALUATION_RUN","governance",None,
                           {"decision_id":result.get("governance_decision_id"),
                            "meta_risk":result.get("meta_risk_state"),
                            "decision":result.get("decision")},
                           "manual governance evaluation","COMPLETED" if result.get("governance_decision_id") else "FAILED")
    return result


@app.post("/api/governance/check")
async def governance_check_api(payload: Dict[str,Any]=Body(...),authorization: Optional[str]=Header(None)):
    _security_actor(authorization,"read",allow_read=True)
    return governance_engine.check_action(
        str(payload.get("action_type") or "CHANGE_APPLY"),
        target=str(payload.get("target") or ""),
        context=payload.get("context") or {}
    )


@app.post("/api/governance/lock")
async def governance_lock_api(active: bool,reason: str,authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"activate_kill_switch" if active else "reset_emergency_stop")
    result=governance_engine.set_lock(active,reason,actor["actor"])
    security_manager.audit(actor,"GOVERNANCE_LOCK_ACTIVATED" if active else "GOVERNANCE_LOCK_CLEAR_REQUEST",
                           "governance:lock",not active,active,reason,"APPLIED")
    if OBSERVABILITY_ENABLED:
        if active:
            observability_manager.alert("GOVERNANCE_LOCK_ACTIVATED","CRITICAL","Governance Engine",
                                        "GOVERNANCE_LOCK_ACTIVATED",
                                        "Persistent Governance Lock activated",
                                        details={"actor":actor["actor"],"reason":reason})
        else:
            observability_manager.alert("GOVERNANCE_LOCK_CLEAR_REVIEW_REQUIRED","HIGH","Governance Engine",
                                        "GOVERNANCE_LOCK_CLEAR_REVIEW_REQUIRED",
                                        "Governance lock cleared but adaptation remains frozen until explicit staged review",
                                        details={"actor":actor["actor"],"reason":reason})
    return result


@app.post("/api/governance/review")
async def governance_review_api(reason: str,authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"reset_emergency_stop")
    result=governance_engine.review_transition(actor["actor"],reason)
    security_manager.audit(actor,"GOVERNANCE_ADAPTATION_REVIEW","governance:adaptation_state",
                           result.get("from_state"),result.get("to_state"),reason,result.get("result"))
    return result


@app.get("/api/system-evaluation/latest")
async def system_evaluation_latest_api(authorization: Optional[str]=Header(None)):
    _security_actor(authorization,"read",allow_read=True)
    latest=system_evaluation_engine.latest() if SYSTEM_EVALUATION_ENABLED else None
    return {"enabled":SYSTEM_EVALUATION_ENABLED,"evaluation":latest,"observation_only":True}


@app.get("/api/system-evaluation/history")
async def system_evaluation_history_api(limit: int=100,authorization: Optional[str]=Header(None)):
    _security_actor(authorization,"read",allow_read=True)
    return {"enabled":SYSTEM_EVALUATION_ENABLED,
            "history":system_evaluation_engine.history(limit) if SYSTEM_EVALUATION_ENABLED else [],
            "historical_records_immutable":True}


@app.get("/api/system-evaluation/detail/{evaluation_id}")
async def system_evaluation_detail_api(evaluation_id: str,authorization: Optional[str]=Header(None)):
    _security_actor(authorization,"read",allow_read=True)
    c=conn();row=c.execute("SELECT * FROM system_evaluations WHERE evaluation_id=?",(evaluation_id,)).fetchone()
    recs=[dict(x) for x in c.execute("SELECT * FROM system_evaluation_recommendations WHERE evaluation_id=? ORDER BY id",(evaluation_id,)).fetchall()]
    attrs=[dict(x) for x in c.execute("SELECT * FROM system_evaluation_attribution WHERE evaluation_id=? ORDER BY dimension,key",(evaluation_id,)).fetchall()]
    c.close()
    if not row:raise HTTPException(404,"SYSTEM_EVALUATION_NOT_FOUND")
    out=dict(row)
    for key in list(out):
        if key.endswith("_json"):
            try:out[key[:-5]]=json.loads(out[key])
            except Exception:pass
    out["recommendation_records"]=recs;out["attribution_records"]=attrs
    return out


@app.post("/api/system-evaluation/run")
async def system_evaluation_run_api(payload: Dict[str,Any]=Body(default={}),
                                    authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    result=run_system_evaluation(payload.get("as_of"),source="manual")
    security_manager.audit(actor,"SYSTEM_EVALUATION_RUN","system_evaluation",None,
                           {"evaluation_id":result.get("evaluation_id"),
                            "status":result.get("system_status"),
                            "score":result.get("system_score")},
                           "manual continuous-system evaluation","COMPLETED" if result.get("evaluation_id") else "FAILED")
    return result


@app.get("/api/security/dashboard")
async def security_dashboard_api(authorization: Optional[str]=Header(None)):
    _security_actor(authorization,"read",allow_read=True)
    out=security_manager.dashboard()
    c=conn()
    out["production_strategy_versions"]=[
        {"strategy":x["setup_variant"],"version":f"{x['setup_variant']}@{VERSION_TAG}"}
        for x in c.execute("SELECT setup_variant FROM strategy_health ORDER BY setup_variant").fetchall()
    ]
    out["deployment_history"]=[dict(x) for x in c.execute(
        "SELECT candidate_id,strategy_version,event_type,previous_stage,new_stage,capital_allocation,reason,approval_source,ts "
        "FROM deployment_events ORDER BY id DESC LIMIT 100").fetchall()]
    c.close()
    out["risk_config_version"]=f"config_v{security_manager.current_version()}"
    out["director_version"]=f"director@{VERSION_TAG}:config_v{security_manager.current_version()}"
    out["regime_model_version"]=f"regime@{VERSION_TAG}:config_v{security_manager.current_version()}"
    return out


@app.get("/api/security/config")
async def security_config_api(authorization: Optional[str]=Header(None),limit: int=100):
    _security_actor(authorization,"read",allow_read=True)
    return {"environment":TRADING_ENVIRONMENT,
            "current_version":security_manager.current_version(),
            "config_hash":security_manager.current_hash(),
            "config":security_manager.current_config(),
            "versions":security_manager.versions(limit),
            "runtime_integrity":security_manager.last_integrity}


@app.get("/api/security/audit")
async def security_audit_api(authorization: Optional[str]=Header(None),limit: int=200):
    _security_actor(authorization,"read",allow_read=True)
    c=conn()
    rows=[dict(x) for x in c.execute("SELECT * FROM security_audit_log ORDER BY seq DESC LIMIT ?",
                                     (min(max(limit,1),2000),)).fetchall()]
    c.close()
    return {"integrity":security_manager.verify_audit_chain(),"events":rows}


@app.get("/api/security/change-requests")
async def security_change_requests_api(status: Optional[str]=None,
                                       authorization: Optional[str]=Header(None),
                                       limit: int=200):
    _security_actor(authorization,"read",allow_read=True)
    c=conn();params=[]
    q="SELECT * FROM security_change_requests"
    if status:
        q+=" WHERE status=?";params.append(status.upper())
    q+=" ORDER BY requested_ts DESC LIMIT ?";params.append(min(max(limit,1),1000))
    rows=[dict(x) for x in c.execute(q,tuple(params)).fetchall()]
    c.close()
    return {"changes":rows}


@app.post("/api/security/change-requests")
async def security_create_change_api(payload: Dict[str,Any]=Body(...),
                                     authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization)
    try:
        result=security_manager.create_change_request(
            actor,component=str(payload.get("component") or ""),
            key=str(payload.get("config_key") or ""),
            proposed=payload.get("proposed_value"),
            reason=str(payload.get("reason") or "no reason supplied"),
            expected_impact=str(payload.get("expected_impact") or ""),
            rollback_plan=str(payload.get("rollback_plan") or "restore previous configuration snapshot"),
            correlation_id=payload.get("correlation_id")
        )
    except PermissionError as e:
        raise HTTPException(403,str(e))
    if (result.get("change") or {}).get("status")=="REJECTED" and OBSERVABILITY_ENABLED:
        observability_manager.alert(f"CHANGE_REJECTED:{(result.get('change') or {}).get('change_id')}",
                                    "HIGH","Security Manager","CRITICAL_CONFIG_CHANGE_REJECTED",
                                    "Configuration change request failed validation",
                                    details=security_sanitize(result))
    return result


@app.post("/api/security/change-requests/{change_id}/review")
async def security_review_change_api(change_id: str,payload: Dict[str,Any]=Body(...),
                                     authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization)
    current_bundle=security_manager.change_request(change_id)
    current_req=current_bundle.get("change") or {}
    if str(payload.get("decision") or "").upper()=="APPROVE" and str(current_req.get("component") or "").startswith("strategy_candidate."):
        candidate_id=str(current_req["component"]).split(".",1)[1]
        c=conn();reg=c.execute("SELECT current_state FROM candidate_registry WHERE candidate_id=?",(candidate_id,)).fetchone();c.close()
        if not reg or reg["current_state"]!="READY_FOR_REVIEW":
            security_manager.audit(actor,"CHANGE_APPROVAL_DENIED",f"candidate:{candidate_id}",None,None,
                                   "Validation Pipeline has not reached READY_FOR_REVIEW","DENIED")
            raise HTTPException(409,"CANDIDATE_NOT_READY_FOR_REVIEW")
    try:
        result=security_manager.review_change(actor,change_id,
                                              str(payload.get("decision") or ""),
                                              str(payload.get("reason") or ""))
    except PermissionError as e:
        raise HTTPException(403,str(e))
    except (ValueError,KeyError) as e:
        raise HTTPException(409,str(e))
    return result


@app.post("/api/security/change-requests/{change_id}/apply")
async def security_apply_change_api(change_id: str,
                                    authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization)
    bundle=security_manager.change_request(change_id)
    req=bundle.get("change") or {}
    governance=None
    if GOVERNANCE_ENABLED:
        try:
            governance=governance_engine.check_action(
                "CHANGE_APPLY",target=f"{req.get('component')}:{req.get('config_key')}",
                context={"trigger":"CHANGE_REQUEST_APPLY","component":req.get("config_key"),
                         "current_value":json.loads(req.get("current_value_json") or "null"),
                         "proposed_value":json.loads(req.get("proposed_value_json") or "null"),
                         "risk_level":req.get("risk_level"),"requester":req.get("requested_by"),
                         "approver":actor.get("actor"),"affected_modules":["CHANGE_MANAGEMENT"]})
            if governance.get("enforced"):
                security_manager.audit(actor,"GOVERNANCE_CHANGE_BLOCK",f"change:{change_id}",None,None,
                                       governance.get("reason"),"BLOCKED")
                raise HTTPException(409,f"GOVERNANCE_BLOCK:{governance.get('reason')}")
        except HTTPException:
            raise
    try:
        result=security_manager.apply_change(actor,change_id)
    except PermissionError as e:
        raise HTTPException(403,str(e))
    except (ValueError,KeyError) as e:
        raise HTTPException(409,str(e))
    side_effect=apply_security_change_side_effects(change_id,actor,result)
    return {**result,"side_effect":side_effect,"governance":governance}


@app.post("/api/security/config/rollback/{target_version}")
async def security_rollback_config_api(target_version: int,payload: Dict[str,Any]=Body(default={}),
                                       authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization)
    try:
        result=security_manager.rollback_config(actor,target_version,
                                               str(payload.get("reason") or "manual configuration rollback"),
                                               payload.get("correlation_id"))
    except PermissionError as e:
        raise HTTPException(403,str(e))
    except (ValueError,KeyError) as e:
        raise HTTPException(409,str(e))
    sync_security_runtime_config()
    if GOVERNANCE_ENABLED:
        sync_governance_runtime_config()
    if OBSERVABILITY_ENABLED:
        observability_manager.alert(f"CONFIG_ROLLBACK:{result.get('new_config_version')}",
                                    "HIGH","Security Manager","CONFIG_ROLLBACK",
                                    "Configuration rollback applied; trading state was not rewound",
                                    details=result)
    return result


@app.post("/api/security/integrity/recheck")
async def security_integrity_recheck_api(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"manual_reconcile",allow_read=False)
    result=security_manager.runtime_integrity_check()
    security_manager.audit(actor,"RUNTIME_INTEGRITY_RECHECK","runtime",None,result,
                           "manual integrity recheck","VERIFIED" if result.get("verified") else "UNVERIFIED")
    if not result.get("verified") and RECOVERY_MANAGER_ENABLED:
        recovery_manager.enter_safe_mode("UNVERIFIED_RUNTIME_STATE",severity="CRITICAL")
    return result


@app.get("/api/recovery/status")
async def recovery_status_api():
    return {"enabled":RECOVERY_MANAGER_ENABLED,
            "state":recovery_manager.state() if RECOVERY_MANAGER_ENABLED else None,
            "metrics":recovery_manager.metrics() if RECOVERY_MANAGER_ENABLED else None,
            "circuit_breaker":recovery_manager.circuit("BROKER") if RECOVERY_MANAGER_ENABLED else None}

@app.get("/api/recovery/orders")
async def recovery_orders_api(limit: int=200):
    return {"orders":recovery_manager.orders(limit)}

@app.get("/api/recovery/incidents")
async def recovery_incidents_api(limit: int=200):
    return {"incidents":recovery_manager.incidents(limit)}

@app.get("/api/recovery/timeline")
async def recovery_timeline_api(correlation_id: Optional[str]=None,execution_intent_id: Optional[str]=None,limit: int=500):
    return {"events":recovery_manager.timeline(correlation_id,execution_intent_id,limit)}

@app.post("/api/recovery/reconcile")
async def recovery_reconcile_api(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"manual_reconcile")
    security_manager.audit(actor,"MANUAL_RECONCILIATION","recovery:primary",None,"STARTED",
                           "manual recovery reconciliation","STARTED")
    async with httpx.AsyncClient() as client:
        result=await recovery_reconcile_primary(client,"manual")
        if result.get("connected"):
            rec=result.get("reconciliation") or {}
            try:
                ctx=await build_broker_risk_context(client)
                risk_ok=not bool(ctx.get("system_abnormal")) and ctx.get("nav") is not None
                recovery_manager.verify_risk(risk_ok,ctx)
            except Exception as e:
                risk_ok=False;recovery_manager.verify_risk(False,{"error":str(e)})
            if rec.get("status") in ("MATCHED","MINOR_MISMATCH") and risk_ok and not recovery_manager.state().get("emergency_stop"):
                recovery_manager.exit_safe_mode("manual reconciliation and risk verification passed")
                state["system_ready"]=True
        return {"result":result,"state":recovery_manager.state()}

@app.post("/api/recovery/emergency-stop")
async def recovery_emergency_stop_api(active: bool,reason: str,authorization: Optional[str]=Header(None)):
    if active:
        actor=_security_actor(authorization,"activate_kill_switch")
        out=recovery_manager.set_emergency_stop(True,reason)
        deployment_manager.set_kill("SYSTEM",True,reason,actor["actor"])
        state["system_ready"]=False
        security_manager.audit(actor,"EMERGENCY_STOP","recovery:primary",False,True,reason,"APPLIED")
        return out
    actor=_security_actor(authorization,"reset_emergency_stop")
    rst=recovery_manager.state()
    health_ok=bool(state.get("system_ready") or (rst.get("state") in ("READY","NORMAL") and not rst.get("safe_mode")))
    reconciliation_ok=rst.get("last_reconciliation_status") in ("MATCHED","MINOR_MISMATCH")
    authz=security_manager.authorize_emergency_reset(actor,health_ok,reconciliation_ok,reason)
    if not authz.get("authorized"):
        raise HTTPException(409,"EMERGENCY_STOP_RESET_REQUIRES_HEALTHY_RECONCILIATION")
    out=recovery_manager.set_emergency_stop(False,reason)
    deployment_manager.set_kill("SYSTEM",False,reason,actor["actor"])
    if OBSERVABILITY_ENABLED:
        observability_manager.alert("EMERGENCY_STOP_RESET","HIGH","Security Manager","EMERGENCY_STOP_RESET",
                                    "Emergency stop explicitly reset after authorization and health checks",
                                    details={"actor":actor["actor"],"reason":reason})
    return out


@app.get("/api/observability/dashboard")
async def observability_dashboard_api():
    return observability_dashboard_snapshot()

@app.get("/api/observability/health/modules")
async def observability_modules_api():
    return {"system":observability_global_health_snapshot(),"modules":observability_manager.module_rows()}

@app.get("/api/observability/alerts")
async def observability_alerts_api(status: Optional[str]=None,severity: Optional[str]=None,limit: int=200):
    c=conn();where=[];params=[]
    if status:where.append("status=?");params.append(status.upper())
    if severity:where.append("severity=?");params.append(severity.upper())
    sql="SELECT * FROM observability_alerts"+(" WHERE "+" AND ".join(where) if where else "")+" ORDER BY last_seen DESC LIMIT ?"
    params.append(min(max(limit,1),1000));rows=[dict(x) for x in c.execute(sql,tuple(params)).fetchall()]
    history=[dict(x) for x in c.execute("SELECT * FROM observability_alert_history ORDER BY id DESC LIMIT ?",(min(max(limit,1),1000),)).fetchall()]
    c.close();return {"alerts":rows,"history":history}

@app.get("/api/observability/metrics")
async def observability_metrics_api(limit: int=300):
    c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM observability_metrics ORDER BY id DESC LIMIT ?",(min(max(limit,1),2000),)).fetchall()]
    capital=[dict(x) for x in c.execute("SELECT * FROM observability_capital_history ORDER BY id DESC LIMIT ?",(min(max(limit,1),2000),)).fetchall()]
    c.close();return {"system_metrics":rows,"capital":capital}

@app.get("/api/observability/logs")
async def observability_logs_api(module: Optional[str]=None,level: Optional[str]=None,event_type: Optional[str]=None,correlation_id: Optional[str]=None,limit: int=300):
    c=conn();w=[];p=[]
    for col,val in (("module",module),("level",level),("event_type",event_type),("correlation_id",correlation_id)):
        if val:w.append(f"{col}=?");p.append(val.upper() if col=="level" else val)
    q="SELECT * FROM observability_structured_logs"+(" WHERE "+" AND ".join(w) if w else "")+" ORDER BY id DESC LIMIT ?"
    p.append(min(max(limit,1),2000));rows=[dict(x) for x in c.execute(q,tuple(p)).fetchall()];c.close();return rows

@app.get("/api/observability/trace/{identifier}")
async def observability_trace_api(identifier: str):
    result=observability_trace_bundle(identifier)
    if result.get("error"):raise HTTPException(404,result["error"])
    return result

@app.get("/api/observability/startup-health")
async def observability_startup_health_api():
    c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM observability_startup_checks ORDER BY id DESC LIMIT 20").fetchall()];c.close()
    return {"current":state.get("startup_health"),"history":rows}

@app.post("/api/observability/startup-health/recheck")
async def observability_startup_health_recheck_api(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"manual_reconcile")
    result=await observability_startup_health_check()
    security_manager.audit(actor,"STARTUP_HEALTH_RECHECK","system:startup_health",None,result,
                           "manual startup health recheck",result.get("status","UNKNOWN"))
    return result

@app.get("/api/observability/capital")
async def observability_capital_api(limit: int=100):
    c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM observability_capital_history ORDER BY id DESC LIMIT ?",(min(max(limit,1),1000),)).fetchall()];c.close()
    return {"latest":rows[0] if rows else None,"history":rows}

@app.get("/observability",response_class=HTMLResponse)
async def observability_html_dashboard():
    return HTMLResponse("""<!doctype html>
<html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Market Alert Observability</title>
<style>body{font-family:system-ui;background:#0f1115;color:#e8e8e8;margin:0;padding:18px}h1{margin-top:0}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:12px}.card{background:#181c22;border:1px solid #2b313b;border-radius:10px;padding:12px}.ok{color:#55d98b}.warn{color:#f2c94c}.bad{color:#ff6b6b}pre{white-space:pre-wrap;overflow:auto;max-height:480px;font-size:12px}.muted{color:#9aa4b2}</style></head>
<body><h1>Market Alert V3.17 — Observability</h1><div id='headline' class='card'>Loading…</div><div class='grid'>
<div class='card'><h2>Modules</h2><pre id='modules'></pre></div><div class='card'><h2>Risk & Capital</h2><pre id='risk'></pre></div>
<div class='card'><h2>Market & Strategies</h2><pre id='market'></pre></div><div class='card'><h2>Candidates</h2><pre id='candidates'></pre></div>
<div class='card'><h2>Alerts</h2><pre id='alerts'></pre></div><div class='card'><h2>System Metrics</h2><pre id='metrics'></pre></div></div>
<p class='muted'>Read-only dashboard. It does not promote strategies or change risk rules.</p>
<script>function j(x){return JSON.stringify(x,null,2)}async function refresh(){try{const r=await fetch('/api/observability/dashboard');const d=await r.json();const h=d.system.status;const cls=(h==='HEALTHY'?'ok':(h==='WARNING'||h==='DEGRADED'||h==='TRADING_PAUSED'?'warn':'bad'));document.getElementById('headline').innerHTML='<b class='+cls+'>'+h+'</b> | SYSTEM_READY='+d.system.system_ready+' | Trading='+d.system.trading_enabled+' | Critical alerts='+d.system.active_critical_alerts;document.getElementById('modules').textContent=j(d.modules);document.getElementById('risk').textContent=j({capital:d.capital,risk:d.risk_engine,positions:d.positions});document.getElementById('market').textContent=j({market:d.market,strategies:d.strategies,degradation:d.strategy_degradation,director:d.ai_strategy_director});document.getElementById('candidates').textContent=j(d.candidates);document.getElementById('alerts').textContent=j(d.alerts);document.getElementById('metrics').textContent=j(d.system_metrics)}catch(e){document.getElementById('headline').textContent='Dashboard error: '+e}}refresh();setInterval(refresh,5000)</script></body></html>""")


@app.get("/api/market-regime")
async def market_regime_api(instrument: Optional[str] = None, limit: int = 100):
    c=conn()
    if instrument:
        inst=instrument.upper().replace("/","_")
        rows=c.execute("""SELECT * FROM market_regime_history
                          WHERE instrument=? ORDER BY id DESC LIMIT ?""",
                       (inst,min(max(limit,1),500))).fetchall()
    else:
        rows=c.execute("""SELECT * FROM market_regime_history
                          ORDER BY id DESC LIMIT ?""",
                       (min(max(limit,1),500),)).fetchall()
    c.close()
    history=[]
    for row in rows:
        d=dict(row)
        try:d["supporting_metrics"]=json.loads(d.pop("supporting_metrics_json") or "{}")
        except Exception:d["supporting_metrics"]={}
        history.append(d)
    return {"enabled":MARKET_REGIME_ENABLED,
            "current":state.get("market_regimes",{}),
            "history":history}


@app.get("/api/status")
async def status():
    dataset=learning_stats()
    training=dict(state.get("learning") or {})
    # Older code used a generic "samples" field for labeled training rows while
    # /api/learning used samples_total for all research rows. Expose unambiguous names.
    training.pop("samples",None)
    training.update({
        "training_labeled_samples":dataset.get("resolved_labeled",0),
        "research_samples_total":dataset.get("samples_total",0),
        "pending_samples":dataset.get("pending",0),
        "model_ready":dataset.get("model_ready",False),
        "retrain_policy":dataset.get("retrain_policy"),
    })
    return {**state,"version":VERSION_TAG,"learning":training,"storage":storage_status(),
            "practice_only": OANDA.endswith("fxpractice.oanda.com"), "operation_count_limit": None, "auto_trade": AUTO,
            "instruments": INSTRUMENTS, "trade_units": UNITS, "quality_threshold": THRESH,
            "bootstrap_score_threshold": BOOTSTRAP_SCORE_THRESHOLD,
            "execution_min_confidence": EXECUTION_MIN_CONFIDENCE, "confidence_min_samples": CONFIDENCE_MIN_SAMPLES,
            "single_position_per_instrument": SINGLE, "adaptive_confidence": ADAPTIVE_CONFIDENCE,
            "ml_shadow": ML_SHADOW, "ml_role": "secondary_refinement",
            "market_regime_enabled": MARKET_REGIME_ENABLED,
            "market_regimes": state.get("market_regimes",{}),
            "smart_execution":{"enabled":SMART_EXECUTION_ENABLED,"mode":"SHADOW","policy_authority":False},
            "capital_allocation":{"enabled":CAPITAL_ALLOCATION_ENABLED,"mode":"SHADOW","risk_limit_authority":False,"order_authority":False},
            "scanner": scanner_health_snapshot()}


@app.get("/api/storage")
async def storage_api():
    return storage_status()


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

@app.get("/api/smart-execution/dashboard")
async def smart_execution_dashboard_api():
    return smart_execution_engine.dashboard() if SMART_EXECUTION_ENABLED else {"enabled":False}

@app.get("/api/smart-execution/intents")
async def smart_execution_intents_api(limit: int=100):
    if not SMART_EXECUTION_ENABLED:return {"enabled":False,"intents":[]}
    c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM smart_execution_intents ORDER BY created_at DESC LIMIT ?",(min(max(limit,1),500),)).fetchall()];c.close()
    return {"enabled":True,"mode":"SHADOW","intents":rows}

@app.get("/api/smart-execution/tca")
async def smart_execution_tca_api(limit: int=100):
    if not SMART_EXECUTION_ENABLED:return {"enabled":False,"tca":[]}
    c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM smart_execution_tca ORDER BY ts DESC LIMIT ?",(min(max(limit,1),500),)).fetchall()];c.close()
    return {"enabled":True,"mode":"SHADOW","tca":rows,"daily_costs":smart_execution_engine.daily_costs(),
            "degradation":smart_execution_engine.degradation()}

@app.get("/api/smart-execution/shadow-comparisons")
async def smart_execution_shadow_comparisons_api(limit: int=100):
    if not SMART_EXECUTION_ENABLED:return {"enabled":False,"comparisons":[]}
    c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM smart_execution_shadow_comparisons ORDER BY ts DESC LIMIT ?",(min(max(limit,1),500),)).fetchall()];c.close()
    return {"enabled":True,"hypothetical_fill_not_assumed":True,"comparisons":rows}

@app.post("/api/smart-execution/policy-candidate")
async def smart_execution_policy_candidate_api(payload: Dict[str,Any]=Body(...),authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    if not SMART_EXECUTION_ENABLED:raise HTTPException(409,"SMART_EXECUTION_DISABLED")
    candidate=smart_execution_engine.candidate_execution_policy(
        str(payload.get("parent_policy") or f"smart_execution_shadow@{VERSION_TAG}"),
        payload.get("proposal") or {},payload.get("evidence") or {})
    security_manager.audit(actor,"SMART_EXECUTION_POLICY_CANDIDATE_CREATED",f"execution:{candidate['candidate_id']}",None,candidate,
                           "execution-policy candidate is research-only and cannot auto-deploy","CREATED")
    return candidate


@app.get("/api/capital-allocation/dashboard")
async def capital_allocation_dashboard_api():
    return capital_allocation_engine.dashboard() if CAPITAL_ALLOCATION_ENABLED else {"enabled":False}

@app.get("/api/capital-allocation/decisions")
async def capital_allocation_decisions_api(limit:int=100):
    if not CAPITAL_ALLOCATION_ENABLED:return {"enabled":False,"decisions":[]}
    c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM allocation_decisions ORDER BY ts DESC LIMIT ?",(min(max(limit,1),500),)).fetchall()];c.close()
    return {"enabled":True,"mode":"SHADOW","decisions":rows}

@app.post("/api/capital-allocation/policy-candidate")
async def capital_allocation_policy_candidate_api(payload:Dict[str,Any]=Body(...),authorization:Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    if not CAPITAL_ALLOCATION_ENABLED:raise HTTPException(409,"CAPITAL_ALLOCATION_DISABLED")
    governance=governance_engine.check_action("ALLOCATION_POLICY_CHANGE",target="capital_allocation.policy",context={"trigger":"ALLOCATION_POLICY_CANDIDATE","magnitude":"MODERATE","affected_modules":["CAPITAL_ALLOCATION_ENGINE","RISK_ENGINE"]}) if GOVERNANCE_ENABLED else None
    candidate=capital_allocation_engine.candidate_policy(str(payload.get("parent_policy") or "CURRENT_SHADOW"),payload.get("proposal") or {},payload.get("evidence") or {})
    security_manager.audit(actor,"ALLOCATION_POLICY_CANDIDATE_CREATED",f"allocation:{candidate['candidate_id']}",None,candidate,"allocation candidate cannot auto-deploy or increase hard risk limits","CREATED")
    return {"candidate":candidate,"governance":governance}

@app.get("/api/ensemble/dashboard")
async def ensemble_dashboard_api():
    return ensemble_engine.dashboard() if ENSEMBLE_ENABLED else {"enabled":False}

@app.get("/api/ensemble/model-map")
async def ensemble_model_map_api():
    if not ENSEMBLE_ENABLED:return {"enabled":False}
    return {"enabled":True,"mode":"SHADOW","models":ensemble_engine.registry(),
            "correlation_audit":ensemble_engine.correlation_audit(),
            "production_replacement":False,"meta_model_implemented":False}

@app.get("/api/ensemble/outputs")
async def ensemble_outputs_api(limit:int=100):
    if not ENSEMBLE_ENABLED:return {"enabled":False,"outputs":[]}
    c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM ensemble_outputs ORDER BY ts DESC LIMIT ?",(min(max(limit,1),500),)).fetchall()];c.close()
    return {"enabled":True,"mode":"SHADOW","outputs":rows,"value_added":ensemble_engine.value_added()}

@app.get("/api/ensemble/weights")
async def ensemble_weights_api(limit:int=100):
    if not ENSEMBLE_ENABLED:return {"enabled":False,"weights":[]}
    c=conn();rows=[dict(x) for x in c.execute("SELECT * FROM ensemble_weight_versions ORDER BY created_at DESC LIMIT ?",(min(max(limit,1),500),)).fetchall()];c.close()
    return {"enabled":True,"mode":"SHADOW","weights":rows}

@app.post("/api/ensemble/weight-candidate")
async def ensemble_weight_candidate_api(payload:Dict[str,Any]=Body(...),authorization:Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    if not ENSEMBLE_ENABLED:raise HTTPException(409,"ENSEMBLE_DISABLED")
    governance=governance_engine.check_action("ENSEMBLE_WEIGHT_CHANGE",target="ensemble.weights",
        context={"trigger":"ENSEMBLE_WEIGHT_CANDIDATE","magnitude":"MODERATE","affected_modules":["ENSEMBLE_ENGINE"]}) if GOVERNANCE_ENABLED else None
    candidate=ensemble_engine.candidate_weights(str(payload.get("parent_weight_version") or "CURRENT_SHADOW"),
                                                payload.get("proposal") or {},payload.get("evidence") or {})
    security_manager.audit(actor,"ENSEMBLE_WEIGHT_CANDIDATE_CREATED",f"ensemble:{candidate['candidate_id']}",None,candidate,
                           "ensemble weights remain research-only and cannot auto-deploy","CREATED")
    return {"candidate":candidate,"governance":governance}


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
async def research_external_observation(payload: Dict[str, Any],authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    instrument=str(payload.get("instrument") or "EUR_USD").upper()
    source_type=str(payload.get("source_type") or "").upper()
    source_key=str(payload.get("source_key") or "").upper()
    if not source_type or not source_key:
        raise HTTPException(status_code=400,detail="source_type and source_key are required")
    record_external_observation(instrument,source_type,source_key,payload.get("value_num"),
                                payload.get("value_text"),payload.get("metadata") or {},
                                payload.get("candle_ts"))
    result={"ok":True,"research_only":True,"automatic_live_activation":False}
    security_manager.audit(actor,"RESEARCH_OBSERVATION_ADDED","research.external",None,
                           {"instrument":instrument,"source_type":source_type,"source_key":source_key},
                           "manual external research observation","APPLIED")
    return result


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










@app.get("/api/deployment")
async def deployment_dashboard_api():
    return deployment_manager.dashboard()

@app.get("/api/deployment/{candidate_id}")
async def deployment_candidate_api(candidate_id: str):
    dep=next((x for x in deployment_manager.dashboard()["deployments"] if x["candidate_id"]==candidate_id),None)
    return {"readiness":deployment_manager.readiness(candidate_id),"deployment":dep,
            "evaluation":deployment_manager.evaluate(candidate_id,auto=False) if dep else None}

@app.post("/api/deployment/{candidate_id}/approve-canary")
async def deployment_approve_api(candidate_id: str, approval_source: str="", approval_note: str="",
                                 authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"candidate_review")
    c=conn()
    authorized_change=c.execute("""SELECT change_id FROM security_change_requests
                                   WHERE component=? AND status='APPLIED'
                                   ORDER BY applied_ts DESC LIMIT 1""",
                                (f"strategy_candidate.{candidate_id}",)).fetchone()
    c.close()
    if not authorized_change:
        security_manager.audit(actor,"PRODUCTION_DEPLOYMENT_APPROVAL_DENIED",f"candidate:{candidate_id}",
                               None,None,"candidate change request has not been approved/applied","DENIED")
        raise HTTPException(409,"CANDIDATE_CHANGE_REQUEST_NOT_APPROVED")
    governance=None
    if GOVERNANCE_ENABLED:
        c=conn();reg=c.execute("SELECT current_state FROM candidate_registry WHERE candidate_id=?",(candidate_id,)).fetchone();c.close()
        governance=governance_engine.check_action(
            "DEPLOYMENT_APPROVAL",target=candidate_id,
            context={"trigger":"CANARY_APPROVAL","magnitude":"MAJOR",
                     "validation_state":reg["current_state"] if reg else None,
                     "affected_modules":["DEPLOYMENT_MANAGER","VALIDATION_PIPELINE","SYSTEM_EVALUATION_ENGINE"]})
        if governance.get("enforced"):
            security_manager.audit(actor,"GOVERNANCE_DEPLOYMENT_BLOCK",f"candidate:{candidate_id}",None,None,
                                   governance.get("reason"),"BLOCKED")
            raise HTTPException(409,f"GOVERNANCE_BLOCK:{governance.get('reason')}")
    result=deployment_manager.approve(candidate_id,actor["actor"],approval_note)
    if result.get("ok") and governance:
        governance_engine.link_deployment_authorization(candidate_id,governance)
    security_manager.audit(actor,"PRODUCTION_DEPLOYMENT_APPROVED",f"candidate:{candidate_id}",
                           "READY_FOR_REVIEW",result.get("stage"),approval_note or "approved for canary",
                           "APPROVED" if result.get("ok") else "DENIED")
    if result.get("ok") and OBSERVABILITY_ENABLED:
        observability_manager.alert(f"PRODUCTION_DEPLOYMENT_APPROVED:{candidate_id}","HIGH",
                                    "Security Manager","PRODUCTION_DEPLOYMENT_APPROVED",
                                    "Candidate approved for controlled Canary deployment",
                                    details={"candidate_id":candidate_id,"actor":actor["actor"]})
    return {**result,"governance":governance}

@app.post("/api/deployment/{candidate_id}/start-canary")
async def deployment_start_api(candidate_id: str, approval_source: str="", authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"candidate_review")
    governance=None
    if GOVERNANCE_ENABLED:
        dep=next((x for x in deployment_manager.dashboard().get("deployments",[]) if x.get("candidate_id")==candidate_id),{})
        governance=governance_engine.check_action(
            "DEPLOYMENT_PROMOTION",target=candidate_id,
            context={"trigger":"CANARY_START","magnitude":"MAJOR",
                     "validation_state":dep.get("registry_state") or dep.get("current_stage"),
                     "affected_modules":["DEPLOYMENT_MANAGER","RISK_ENGINE"]})
        if governance.get("enforced"):
            security_manager.audit(actor,"GOVERNANCE_DEPLOYMENT_BLOCK",f"candidate:{candidate_id}",None,None,
                                   governance.get("reason"),"BLOCKED")
            raise HTTPException(409,f"GOVERNANCE_BLOCK:{governance.get('reason')}")
    result=await deployment_manager.start(candidate_id,actor["actor"])
    if result.get("ok") and governance:
        governance_engine.link_deployment_authorization(candidate_id,governance)
    if result.get("ok") and CANARY_ACCOUNT and CANARY_TOKEN:
        canary_recovery_manager.ensure_schema()
        canary_recovery_manager.set_state("RECOVERING","canary start health/reconciliation",safe_mode=True,new_trades_allowed=False)
        async with httpx.AsyncClient() as client:
            rr=await canary_recovery_manager.reconnect_and_reconcile(client,max_attempts=3)
        if rr.get("connected") and (rr.get("reconciliation") or {}).get("status") in ("MATCHED","MINOR_MISMATCH"):
            canary_recovery_manager.verify_risk(True,{"source":"Deployment Manager canary health"})
            canary_recovery_manager.exit_safe_mode("canary broker state reconciled")
        else:
            deployment_manager.pause(candidate_id,"Canary Recovery Manager did not reach reconciled state","RECOVERY_MANAGER")
            result={**result,"ok":False,"recovery":rr,"status":"CANARY_RECOVERY_FAILED"}
    security_manager.audit(actor,"CANARY_START",f"candidate:{candidate_id}",None,result.get("stage"),
                           "controlled canary start", "APPLIED" if result.get("ok") else "FAILED")
    return {**result,"governance":governance}

@app.post("/api/deployment/{candidate_id}/resume")
async def deployment_resume_api(candidate_id: str, approval_source: str="", authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"candidate_review")
    governance=None
    if GOVERNANCE_ENABLED:
        governance=governance_engine.check_action(
            "DEPLOYMENT_PROMOTION",target=candidate_id,
            context={"trigger":"CANARY_RESUME","magnitude":"MAJOR",
                     "validation_state":"CANARY_LIVE",
                     "affected_modules":["DEPLOYMENT_MANAGER","RECOVERY_MANAGER"]})
        if governance.get("enforced"):
            raise HTTPException(409,f"GOVERNANCE_BLOCK:{governance.get('reason')}")
    result=await deployment_manager.resume(candidate_id,actor["actor"])
    if result.get("ok") and governance:
        governance_engine.link_deployment_authorization(candidate_id,governance)
    if result.get("ok") and CANARY_ACCOUNT and CANARY_TOKEN:
        canary_recovery_manager.ensure_schema()
        canary_recovery_manager.set_state("RECOVERING","canary resume reconciliation",safe_mode=True,new_trades_allowed=False)
        async with httpx.AsyncClient() as client:
            rr=await canary_recovery_manager.reconnect_and_reconcile(client,max_attempts=3)
        if rr.get("connected") and (rr.get("reconciliation") or {}).get("status") in ("MATCHED","MINOR_MISMATCH"):
            canary_recovery_manager.verify_risk(True,{"source":"Canary restart recovery"})
            canary_recovery_manager.exit_safe_mode("canary resume reconciled")
        else:
            deployment_manager.pause(candidate_id,"Canary recovery after restart failed","RECOVERY_MANAGER")
            result={**result,"ok":False,"recovery":rr,"status":"CANARY_RECOVERY_FAILED"}
    security_manager.audit(actor,"MANUAL_RESUME",f"candidate:{candidate_id}",None,result.get("stage"),
                           "manual canary resume","APPLIED" if result.get("ok") else "FAILED")
    return {**result,"governance":governance}

@app.post("/api/deployment/{candidate_id}/promote")
async def deployment_promote_api(candidate_id: str, approval_source: str="", authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"candidate_review")
    governance=None
    if GOVERNANCE_ENABLED:
        dep=next((x for x in deployment_manager.dashboard().get("deployments",[]) if x.get("candidate_id")==candidate_id),{})
        governance=governance_engine.check_action(
            "DEPLOYMENT_PROMOTION",target=candidate_id,
            context={"trigger":"PROMOTION_GATE","magnitude":"MAJOR",
                     "validation_state":dep.get("current_stage"),
                     "affected_modules":["DEPLOYMENT_MANAGER","RISK_ENGINE","AI_STRATEGY_DIRECTOR","SYSTEM_EVALUATION_ENGINE"]})
        if governance.get("enforced"):
            security_manager.audit(actor,"GOVERNANCE_DEPLOYMENT_BLOCK",f"candidate:{candidate_id}",None,None,
                                   governance.get("reason"),"BLOCKED")
            raise HTTPException(409,f"GOVERNANCE_BLOCK:{governance.get('reason')}")
    result=deployment_manager.promote(candidate_id,actor["actor"],risk_ok=True)
    if result.get("action")=="PROMOTE" and governance:
        governance_engine.link_deployment_authorization(candidate_id,governance)
    security_manager.audit(actor,"CANDIDATE_PROMOTION",f"candidate:{candidate_id}",None,result.get("stage"),
                           "promotion gate request",result.get("action","UNKNOWN"))
    return {**result,"governance":governance}

@app.post("/api/deployment/{candidate_id}/pause")
async def deployment_pause_api(candidate_id: str, reason: str, approval_source: str="", authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"manual_pause")
    result=deployment_manager.pause(candidate_id,reason,actor["actor"])
    security_manager.audit(actor,"MANUAL_PAUSE",f"candidate:{candidate_id}",None,"CANARY_PAUSED",reason,
                           "APPLIED" if result.get("ok") else "FAILED")
    return result

@app.post("/api/deployment/{candidate_id}/rollback")
async def deployment_rollback_api(candidate_id: str, reason: str, approval_source: str="", authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"candidate_review")
    result=deployment_manager.rollback(candidate_id,reason,actor["actor"])
    security_manager.audit(actor,"MANUAL_ROLLBACK",f"candidate:{candidate_id}",None,"ROLLED_BACK",reason,
                           "APPLIED" if result.get("ok") else "FAILED")
    return result

@app.post("/api/deployment/kill-switch")
async def deployment_kill_api(scope: str, active: bool, reason: str, source: str="", authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"activate_kill_switch" if active else "reset_emergency_stop")
    result=deployment_manager.set_kill(scope,active,reason,actor["actor"])
    security_manager.audit(actor,"KILL_SWITCH_ACTIVATED" if active else "KILL_SWITCH_RESET",
                           f"kill_switch:{scope}",not active,active,reason,"APPLIED")
    return result

@app.post("/api/deployment/reconcile")
async def deployment_reconcile_api(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"manual_reconcile")
    async with httpx.AsyncClient() as client:
        result=await deployment_manager.reconcile(client)
    security_manager.audit(actor,"MANUAL_RECONCILIATION","deployment",None,result,
                           "manual deployment reconciliation","COMPLETED")
    return {"result":result,"dashboard":deployment_manager.dashboard()}


@app.get("/api/candidate-validation")
async def candidate_validation_api(limit: int = 100):
    c=conn();runs=c.execute("SELECT * FROM candidate_validation_runs ORDER BY started_ts DESC LIMIT ?",(min(max(limit,1),500),)).fetchall();
    events=c.execute("SELECT * FROM validation_events ORDER BY id DESC LIMIT ?",(min(max(limit*2,1),1000),)).fetchall();c.close()
    return {"enabled":VALIDATION_PIPELINE_ENABLED,"maximum_state":VALIDATION_MAX_STATE,"auto_deploy":False,
            "registry":candidate_registry_snapshot(),"validation_runs":[dict(x) for x in runs],"events":[dict(x) for x in events]}


@app.post("/api/candidate-validation/{candidate_id}/run")
async def candidate_validation_run_api(candidate_id: str,authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"candidate_review")
    result=validate_candidate_advanced(candidate_id)
    security_manager.audit(actor,"CANDIDATE_VALIDATION_RUN",f"candidate:{candidate_id}",None,
                           result.get("final_status"),"manual advanced validation","COMPLETED")
    return result


@app.get("/api/candidate-validation/{candidate_id}/paper")
async def candidate_validation_paper_api(candidate_id: str):
    return evaluate_candidate_paper_state(candidate_id)


@app.get("/api/candidate-validation/{candidate_id}/walk-forward")
async def candidate_validation_walk_forward_api(candidate_id: str):
    c=conn();reg=c.execute("SELECT latest_validation_id FROM candidate_registry WHERE candidate_id=?",(candidate_id,)).fetchone()
    rows=[] if not reg or not reg["latest_validation_id"] else [dict(x) for x in c.execute(
        "SELECT * FROM validation_walk_forward_windows WHERE validation_id=? ORDER BY window_no",(reg["latest_validation_id"],)).fetchall()]
    c.close();return {"candidate_id":candidate_id,"windows":rows,"auto_deploy":False}


@app.get("/api/candidate-registry")
async def candidate_registry_api():
    return {"registry":candidate_registry_snapshot(),"maximum_state":VALIDATION_MAX_STATE,"auto_deploy":False}


@app.get("/api/adaptive-learning")
async def adaptive_learning_api(limit: int = 100):
    c=conn()
    candidates=c.execute("""SELECT * FROM candidate_strategies
                            ORDER BY id DESC LIMIT ?""",(min(max(limit,1),500),)).fetchall()
    runs=c.execute("""SELECT * FROM adaptive_learning_runs
                      ORDER BY id DESC LIMIT 20""").fetchall()
    drift=c.execute("""SELECT * FROM concept_drift_alerts
                       ORDER BY ts DESC LIMIT 100""").fetchall()
    c.close()
    return {
        "enabled":ADAPTIVE_LEARNING_ENABLED,
        "observation_only":True,
        "production_mutation":False,
        "candidate_activation_authority":False,
        "runs":[dict(x) for x in runs],
        "candidates":[dict(x) for x in candidates],
        "concept_drift":[dict(x) for x in drift]
    }


@app.post("/api/adaptive-learning/run")
async def adaptive_learning_run_api(force: bool = False,authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    result=run_adaptive_learning(force=force)
    security_manager.audit(actor,"ADAPTIVE_LEARNING_RUN","adaptive_learning",None,
                           {"status":result.get("status") if isinstance(result,dict) else None},
                           "manual research cycle","COMPLETED")
    return result


@app.get("/api/adaptive-learning/insights")
async def adaptive_learning_insights_api(query: str,
                                         strategy: Optional[str] = None):
    return adaptive_learning_insights(query,strategy)


@app.get("/api/adaptive-learning/events")
async def adaptive_learning_events_api(limit: int = 200):
    c=conn()
    rows=c.execute("""SELECT * FROM adaptive_learning_events
                      ORDER BY id DESC LIMIT ?""",(min(max(limit,1),1000),)).fetchall()
    c.close()
    return {"events":[dict(x) for x in rows],"production_mutation":False}


@app.get("/api/trade-memory")
async def trade_memory_api(limit: int = 100, status: Optional[str] = None):
    c=conn()
    if status:
        rows=c.execute("""SELECT * FROM trade_memory WHERE status=?
                          ORDER BY id DESC LIMIT ?""",
                       (status.upper(),min(max(limit,1),1000))).fetchall()
    else:
        rows=c.execute("""SELECT * FROM trade_memory
                          ORDER BY id DESC LIMIT ?""",
                       (min(max(limit,1),1000),)).fetchall()
    c.close()
    return {
        "enabled":TRADE_MEMORY_ENABLED,
        "storage":"existing SQLite database",
        "auto_strategy_changes":False,
        "trades":[dict(x) for x in rows]
    }


@app.get("/api/trade-memory/analysis")
async def trade_memory_analysis_api(group_by: str = "strategy",
                                    min_samples: int = TRADE_MEMORY_MIN_SAMPLE_SIZE,
                                    period: str = "month"):
    return trade_memory_group_analysis(group_by,min_samples,period)


@app.get("/api/trade-memory/combination")
async def trade_memory_combination_api(
    strategy: Optional[str] = None,
    regime: Optional[str] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    volatility: Optional[str] = None,
    min_confidence: Optional[float] = None,
    min_samples: int = TRADE_MEMORY_MIN_SAMPLE_SIZE
):
    return trade_memory_combination_analysis(
        strategy,regime,symbol,direction,volatility,min_confidence,min_samples
    )


@app.get("/api/trade-memory/degradation")
async def trade_memory_degradation_api(strategy: Optional[str] = None):
    refresh_trade_memory_degradation()
    return {
        "auto_strategy_changes":False,
        "results":trade_memory_recent_degradation(strategy)
    }


@app.get("/api/trade-memory/insights")
async def trade_memory_insights_api(query: str,
                                    strategy: Optional[str] = None,
                                    regime: Optional[str] = None):
    return trade_memory_insights(query,strategy,regime)


@app.post("/api/trade-memory/reconcile")
async def trade_memory_reconcile_api(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"manual_reconcile")
    async with httpx.AsyncClient() as client:
        result=await reconcile_trade_memory(client,None)
    degradation=refresh_trade_memory_degradation()
    security_manager.audit(actor,"MANUAL_RECONCILIATION","trade_memory",None,result,
                           "manual trade-memory reconciliation","COMPLETED")
    return {"result":result,"degradation":degradation,"auto_strategy_changes":False}


@app.get("/api/adaptive-risk")
async def adaptive_risk_api(limit: int = 200):
    return adaptive_risk_report(limit)


@app.get("/api/adaptive-risk/state")
async def adaptive_risk_state_api():
    c=conn()
    row=c.execute("SELECT * FROM portfolio_risk_state WHERE id=1").fetchone()
    c.close()
    return {
        "shadow_mode":True,
        "authority_over_execution":False,
        "authority_over_position_size":False,
        "portfolio_state":dict(row) if row else None
    }


@app.post("/api/adaptive-risk/refresh")
async def adaptive_risk_refresh(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"manual_reconcile")
    async with httpx.AsyncClient() as client:
        ctx=await build_broker_risk_context(client)
    persist_portfolio_risk_context(ctx)
    results=[]
    c=conn()
    pairs=[(x["instrument"],x["setup_variant"]) for x in c.execute(
        """SELECT DISTINCT instrument,setup_variant FROM signals
           WHERE setup_variant IS NOT NULL AND setup_variant NOT IN ('','WAIT') AND instrument IS NOT NULL"""
    ).fetchall()]
    c.close()
    for inst,variant in pairs:
        regime=state.get("market_regimes",{}).get(inst)
        director=ai_strategy_director_recommendation(inst,variant,regime,None)
        d=adaptive_risk_recommendation(inst,variant,regime,director,None,ctx,UNITS)
        log_adaptive_risk_decision(d)
        results.append(d)
    result={"shadow_mode":True,"risk_context":ctx,"results":results}
    security_manager.audit(actor,"ADAPTIVE_RISK_REFRESH","risk.engine.shadow",None,
                           {"strategies":len(results)},"manual shadow-risk refresh","COMPLETED")
    return result


@app.get("/api/ai-strategy-director")
async def ai_strategy_director_api(limit: int = 100):
    return ai_director_report(limit)


@app.post("/api/ai-strategy-director/refresh")
async def ai_strategy_director_refresh(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    """
    Recompute observation recommendations for currently known strategies
    using the latest regime snapshot. No trading authority.
    """
    c=conn()
    pairs=[(x["instrument"],x["setup_variant"]) for x in c.execute(
        """SELECT DISTINCT instrument,setup_variant FROM signals
           WHERE setup_variant IS NOT NULL AND setup_variant NOT IN ('','WAIT') AND instrument IS NOT NULL"""
    ).fetchall()]
    c.close()

    results=[]
    for inst,variant in pairs:
        regime=state.get("market_regimes",{}).get(inst)
        d=ai_strategy_director_recommendation(inst,variant,regime,None)
        log_ai_director_decision(d)
        results.append(d)
    result={"observation_only":True,"results":results}
    security_manager.audit(actor,"AI_DIRECTOR_REFRESH","ai_strategy_director",None,
                           {"strategies":len(results)},"manual director observation refresh","COMPLETED")
    return result


@app.get("/api/ai-strategy-director/outcomes")
async def ai_strategy_director_outcomes_api():
    reconcile_ai_director_outcomes()
    return ai_director_report(200)


@app.get("/api/strategy-health")
async def strategy_health_api():
    return {"enabled":STRATEGY_SELF_EVAL_ENABLED,"auto_pause":STRATEGY_AUTO_PAUSE,
            "baseline_window":STRATEGY_BASELINE_WINDOW,"recent_window":STRATEGY_RECENT_WINDOW,
            "watch_drop":STRATEGY_WATCH_DROP,"degraded_drop":STRATEGY_DEGRADED_DROP,
            "recovery_samples":STRATEGY_RECOVERY_SAMPLES,"strategies":all_strategy_health()}

@app.post("/api/strategy-health/refresh")
async def strategy_health_refresh_api(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    result=evaluate_all_strategy_health()
    security_manager.audit(actor,"STRATEGY_HEALTH_REFRESH","strategy.health",None,result,
                           "manual strategy-health refresh","COMPLETED")
    return result

@app.get("/api/strategy-health/audit")
async def strategy_health_audit_api(limit: int = 200):
    c=conn(); rows=c.execute("SELECT * FROM strategy_health_audit ORDER BY id DESC LIMIT ?",(min(max(limit,1),1000),)).fetchall(); c.close()
    return {"events":[dict(x) for x in rows]}


@app.get("/api/research/weekends")
async def research_weekends(limit: int = 20):
    c=conn(); sessions=c.execute("SELECT * FROM weekend_sessions ORDER BY opened_ts DESC LIMIT ?",(min(max(limit,1),200),)).fetchall(); recent=c.execute("SELECT * FROM weekend_context ORDER BY collected_ts DESC LIMIT ?",(min(max(limit*10,10),500),)).fetchall(); c.close()
    return {"enabled":WEEKEND_RESEARCH_ENABLED,"market_closed_now":market_is_weekend_closed(),"signal_context_hours":WEEKEND_SIGNAL_CONTEXT_HOURS,"reaction_horizons_hours":list(WEEKEND_REACTION_HORIZONS),"sessions":[dict(x) for x in sessions],"recent_context":[dict(x) for x in recent]}

@app.post("/api/research/weekends/collect")
async def research_weekend_collect(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    async with httpx.AsyncClient() as client:
        result={"results":[await collect_weekend_news_snapshot(client,inst) for inst in INSTRUMENTS]}
    security_manager.audit(actor,"WEEKEND_RESEARCH_COLLECTION","research.weekend",None,
                           {"symbols":len(result["results"])},"manual weekend research collection","COMPLETED")
    return result

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
async def research_autonomous_refresh(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    result=autonomous_discovery_refresh()
    security_manager.audit(actor,"AUTONOMOUS_RESEARCH_REFRESH","adaptive_learning.research",None,
                           {"status":result.get("status") if isinstance(result,dict) else None},
                           "manual autonomous-research refresh","COMPLETED")
    return result


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
async def research_promote(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    result=security_queue_validated_research_changes()
    security_manager.audit(actor,"RESEARCH_CHANGE_REQUESTS_QUEUED","strategy.research_filters",None,
                           {"created":len(result["created"])},"manual research review queue","CREATED")
    return result


@app.post("/api/research/review-active")
async def research_review_active(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    result=review_active_research_rules()
    security_manager.audit(actor,"RESEARCH_RULE_HEALTH_REVIEW","strategy.research_filters",None,result,
                           "manual active-rule health review","COMPLETED")
    return result


@app.post("/api/research/refresh")
async def research_refresh(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    external_research = refresh_external_hypotheses()
    autonomous=autonomous_discovery_refresh()
    return {"external":external_research,"autonomous":autonomous,
            "patterns":refresh_discovered_patterns(),"filters":refresh_filter_hypotheses(),
            "retrain_policy":should_retrain_model(),
            "note":"Autonomous rules are discovered on older data and validated on a later holdout before the 100/50 cycle."}

@app.post("/api/learning/train")
async def train_now(authorization: Optional[str]=Header(None)):
    actor=_security_actor(authorization,"run_research")
    result=train_shadow_model(force=True)
    security_manager.audit(actor,"SHADOW_MODEL_TRAIN","adaptive_learning.shadow_model",None,
                           {"trained":True},"manual shadow-model training","COMPLETED")
    return result


@app.get("/api/discovery")
async def discovery():
    c=conn()
    rows=[dict(x) for x in c.execute("SELECT * FROM discovered_patterns ORDER BY validated DESC, ABS(weight) DESC, samples DESC LIMIT 100").fetchall()]
    c.close()
    return {"minimum_samples": DISCOVERY_MIN_SAMPLES, "minimum_edge": DISCOVERY_MIN_EDGE, "patterns": rows}

@app.get("/", response_class=HTMLResponse)
async def home():
    return """<!doctype html><html lang='es'><meta name='viewport' content='width=device-width'><title>Market Alert V3.27</title>
<style>body{font-family:system-ui;background:#0b1020;color:#eef2ff;max-width:1050px;margin:auto;padding:24px}.c{background:#151c32;border:1px solid #2c3656;border-radius:16px;padding:18px;margin:12px 0}pre{white-space:pre-wrap;word-break:break-word;background:#080c17;padding:14px;border-radius:12px}.tag{display:inline-block;padding:5px 9px;border-radius:999px;background:#25304f;margin-right:6px}</style>
<h1>BotsTrader V3.37.0 · IBKR Multi-Asset Preparation</h1><div class=c><span class=tag>OANDA PRACTICE ONLY</span><span class=tag>24/7</span><span class=tag>Sin límite diario</span><span class=tag>Confianza calibrada</span>
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


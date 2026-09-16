Warning: truncated output (original token count: 184764)
Total output lines: 12715

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
from governance_engine import GovernanceEngine
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
from managed_strategy_rules import evaluate_managed_strategy_rules, managed_strategy_identity, non_v3_managed_strategy_identity
from directional_strategies import (
    STRATEGY_IDS as DIRECTIONAL_STRATEGY_IDS,
    all_strategy_definitions,
    directional_strategy_id,
    evaluate_directional_strategy,
)
from slot_allocator import slot_policy
from opportunity_ranker import rank_opportunities
from broker_risk import OandaBrokerRiskAdapter
from counterfactual_tracker import CounterfactualTracker
from legacy_v331_scoring import legacy_v331_score, choose_legacy_v331_direction
from forward_experiment import forward_policy, evaluate_forward_experiment
from observability import (
    ObservabilityManager, DEPENDENCY_CRITICAL, DEPENDENCY_IMPORTANT, DEPENDENCY_NON_CRITICAL,
    reconciliation_status as observability_reconciliation_status,
    degradation_state as observability_degradation_state,
)
from deployment_runtime import DeploymentManager
from adaptive_learning import (
    dataset_fingerprint as al_dataset_fingerprint,
    candidate_uses_entry_only as al_candidate_uses_entry_only,
    metrics as al_metrics,
    validate_candidate as al_validate_candidate,
    concept_drift as al_concept_drift,
)
from validation_pipeline import (
    run_historical_validation as vp_run_historical_validation,
    strict_temporal_split as vp_strict_temporal_split,
    candidate_passes as vp_candidate_passes,
    dataset_fingerprint as vp_dataset_fingerprint,
)

import httpx
from fastapi import FastAPI, HTTPException, Header, Body
from fastapi.responses import HTMLResponse, PlainTextResponse

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
VERSION_TAG = "3.40.2"
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
RECOVERY_STARTUP_CANDLE_COUNT = max(60,int(os.getenv("RECOVERY_STARTUP_CANDLE_COUNT","60")))
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
        "server.py","directional_strategies.py","production_readiness.py","governance_engine.py","system_evaluation.py",
        "security_manager.py","recovery_manager.py","order_state.py","observability.py","smart_execution.py","ensemble_engine.py","capital_allocation.py",
        "adaptive_learning.py","validation_pipeline.py","deployment_manager.py","deployment_runtime.py",
        "instrument_registry.py","instrument_profiles.py","opportunity_ranker.py","slot_allocator.py","broker_risk.py","counterfactual_tracker.py","requirements.txt","Dockerfile"
    ]
    return [str(root/n) for n in names]

def production_release_versions() -> Dict[str,Any]:
    return {
        "system_release":VERSION_TAG,
        "strategy_versions":[f"{x}@{VERSION_TAG}" for x in sorted(DIRECTIONAL_STRATEGY_IDS)],
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
            signal_id INTEG…134764 tokens truncated…   "candidate_version":x.get("candidate_version"),"stage":x.get("current_stage")}
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


@app.get("/api/directional-strategies")
async def directional_strategies_api():
    return {
        "version":VERSION_TAG,
        "environment":TRADING_ENVIRONMENT,
        "strategy_count":len(DIRECTIONAL_STRATEGY_IDS),
        "max_simultaneous_positions_per_instrument":1 if SINGLE else None,
        "production_authority":False,
        "strategies":all_strategy_definitions(),
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
    _security_actor(authorization,"run_research")
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

"""Automation V3 structured data-integrity diagnostics and recovery policy.

This module classifies existing integrity failures without changing any research
threshold, BID/ASK rule, warmup, horizon, gap tolerance, or look-ahead rule.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

RECOVERABLE_FAILURES = {
    "INSUFFICIENT_COVERAGE",
    "MISSING_WARMUP",
    "MISSING_HORIZON",
    "MISSING_TIMEFRAME_DATA",
    "STALE_OR_PARTIAL_CACHE",
}

NON_RECOVERABLE_FAILURES = {
    "MIDPOINT_ONLY",
    "BID_ASK_INVALID",
    "NON_WEEKEND_GAPS",
    "INSTRUMENT_MISMATCH",
    "CORRUPT_DATASET",
    "LOOKAHEAD_OR_IDENTITY_VIOLATION",
    "METHODOLOGY_INVALID",
}

SECRET_MARKERS = (
    "OANDA_TOKEN", "RAILWAY_TOKEN", "GH_TOKEN", "API_KEY", "SECRET_KEY",
    "PASSWORD", "AUTHORIZATION", "BEARER ",
)


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_text(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _safe_text(v) for k, v in value.items() if not any(m in str(k).upper() for m in SECRET_MARKERS)}
    if isinstance(value, list):
        return [_safe_text(v) for v in value]
    if isinstance(value, str):
        upper = value.upper()
        if any(marker in upper for marker in SECRET_MARKERS):
            return "[REDACTED]"
    return value


def _classes(failures: list[str], *, cache_preexisting: bool = False) -> list[str]:
    classes: set[str] = set()
    for raw in failures:
        failure = raw.upper()
        if "WARMUP_COVERAGE_INCOMPLETE" in failure:
            classes.add("MISSING_WARMUP")
        elif "HORIZON_COVERAGE_INCOMPLETE" in failure:
            classes.add("MISSING_HORIZON")
        elif failure.endswith("_MISSING"):
            classes.add("MISSING_TIMEFRAME_DATA")
        elif "MIDPOINT_ONLY" in failure:
            classes.add("MIDPOINT_ONLY")
        elif "BID_ASK" in failure or "NEGATIVE_SPREAD" in failure:
            classes.add("BID_ASK_INVALID")
        elif "NON_WEEKEND_GAP" in failure:
            classes.add("NON_WEEKEND_GAPS")
        elif "CROSS_ASSET" in failure or "INSTRUMENT_MISMATCH" in failure:
            classes.add("INSTRUMENT_MISMATCH")
        elif failure in {"DATA_SHA256_MISMATCH", "CODE_SHA_MISMATCH", "LOOKAHEAD_DETECTED"}:
            classes.add("LOOKAHEAD_OR_IDENTITY_VIOLATION")
        elif any(token in failure for token in ("DUPLICATES", "NOT_STRICTLY_ORDERED", "INVALID_OHLC", "INTERPOLATION_DETECTED", "CORRUPT")):
            classes.add("CORRUPT_DATASET")
        elif failure in {"INVALID_WINDOW", "INVALID_WARMUP_OR_HORIZON"}:
            classes.add("METHODOLOGY_INVALID")
        else:
            classes.add("METHODOLOGY_INVALID")
    if failures and all(c in {"MISSING_WARMUP", "MISSING_HORIZON", "MISSING_TIMEFRAME_DATA"} for c in classes):
        classes.add("INSUFFICIENT_COVERAGE")
        if cache_preexisting:
            classes.add("STALE_OR_PARTIAL_CACHE")
    return sorted(classes)


def build_integrity_diagnostic(
    report: Mapping[str, Any], *, artifact_path: str | Path, cache_path: str | Path,
    requested_start: str, requested_end: str, cache_preexisting: bool = False,
    retry_count: int = 0,
) -> dict[str, Any]:
    artifact = Path(artifact_path)
    cache = Path(cache_path)
    failures = [str(x) for x in (report.get("failures") or [])]
    classes = _classes(failures, cache_preexisting=cache_preexisting)
    recoverable = bool(classes) and all(c in RECOVERABLE_FAILURES for c in classes)
    timeframes = report.get("timeframes") if isinstance(report.get("timeframes"), Mapping) else {}
    coverage = report.get("coverage") if isinstance(report.get("coverage"), Mapping) else {}
    present = sorted(name for name, frame in timeframes.items() if isinstance(frame, Mapping) and int(frame.get("count") or 0) > 0)
    missing = sorted(name for name in ("H1", "M15", "M5", "M1") if name not in present)
    non_weekend = sum(int((frame or {}).get("non_weekend_gaps") or 0) for frame in timeframes.values() if isinstance(frame, Mapping))
    coverage_starts = [str(frame.get("first")) for frame in timeframes.values() if isinstance(frame, Mapping) and frame.get("first")]
    coverage_ends = [str(frame.get("last")) for frame in timeframes.values() if isinstance(frame, Mapping) and frame.get("last")]
    warnings = []
    if report.get("gaps_present"):
        warnings.append("GAPS_PRESENT")
    if non_weekend:
        warnings.append(f"NON_WEEKEND_GAPS_REPORTED:{non_weekend}")
    if retry_count:
        warnings.append(f"RETRY_COUNT:{retry_count}")
    if recoverable:
        recommended = "REACQUIRE_SAME_LOOKBACK" if retry_count == 0 else "EXPAND_LOOKBACK"
    else:
        recommended = "HARD_BLOCK"
    diagnostic = {
        "integrity_status": str(report.get("status") or "UNKNOWN").upper(),
        "failed_checks": failures,
        "failure_classes": classes,
        "recoverable": recoverable,
        "warnings": warnings,
        "coverage_start": min(coverage_starts) if coverage_starts else None,
        "coverage_end": max(coverage_ends) if coverage_ends else None,
        "requested_start": requested_start,
        "requested_end": requested_end,
        "warmup_days": report.get("warmup_days"),
        "horizon_minutes": report.get("horizon_minutes"),
        "timeframes_present": present,
        "coverage": coverage,
        "bid_ask_status": "PASS" if report.get("bid_ask_real") is True and report.get("midpoint_only") is False else "FAIL",
        "gap_status": "REPORTED" if report.get("gaps_present") else "CLEAR",
        "non_weekend_gap_count": non_weekend,
        "missing_timeframes": missing,
        "cache_id": cache.name,
        "cache_path": str(cache),
        "artifact_path": str(artifact),
        "artifact_sha256": sha256_file(artifact) if artifact.is_file() else None,
        "retry_count": int(retry_count),
        "recommended_action": recommended,
        "production_authority": False,
    }
    return _safe_text(diagnostic)


def terminal_for_nonrecoverable(diagnostic: Mapping[str, Any]) -> str:
    classes = set(str(x) for x in (diagnostic.get("failure_classes") or []))
    if classes & {"LOOKAHEAD_OR_IDENTITY_VIOLATION", "METHODOLOGY_INVALID"}:
        return "METHODOLOGY_BLOCKED"
    return "DATA_INTEGRITY_FAILED"

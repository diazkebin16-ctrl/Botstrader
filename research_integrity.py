"""Offline dataset identity, integrity, determinism and isolation checks."""
from __future__ import annotations

import hashlib
import json
import math
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from research_manager import sha256_file


TIMEFRAME_SECONDS = {"H1": 3600, "M15": 900, "M5": 300, "M1": 60}
M1_BID_ASK = {
    "bid_o", "bid_h", "bid_l", "bid_c", "ask_o", "ask_h", "ask_l", "ask_c",
}


def _dt(value: Any) -> datetime:
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _git_head(repo: str | Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=str(repo), check=True,
        text=True, capture_output=True,
    )
    return result.stdout.strip()


def _ohlc_valid(row: Mapping[str, Any], prefix: str = "") -> bool:
    try:
        opened = float(row[f"{prefix}o"])
        high = float(row[f"{prefix}h"])
        low = float(row[f"{prefix}l"])
        closed = float(row[f"{prefix}c"])
    except (KeyError, TypeError, ValueError):
        return False
    return all(math.isfinite(value) for value in (opened, high, low, closed)) and high >= max(opened, closed, low) and low <= min(opened, closed, high)


def _timeframe_report(name: str, rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    timestamps = [_dt(row["t"]) for row in rows if row.get("t")]
    ordered = all(left < right for left, right in zip(timestamps, timestamps[1:]))
    duplicates = len(timestamps) - len(set(timestamps))
    expected = TIMEFRAME_SECONDS[name]
    gaps = []
    for left, right in zip(timestamps, timestamps[1:]):
        seconds = int((right - left).total_seconds())
        if seconds > expected:
            gaps.append({
                "after": left.isoformat(), "before": right.isoformat(),
                "seconds": seconds, "missing_intervals": max(0, seconds // expected - 1),
                "weekend_boundary": left.weekday() >= 4 or right.weekday() == 0,
            })
    invalid_mid = sum(not _ohlc_valid(row) for row in rows)
    return {
        "count": len(rows),
        "first": timestamps[0].isoformat() if timestamps else None,
        "last": timestamps[-1].isoformat() if timestamps else None,
        "coverage_end": (timestamps[-1] + timedelta(seconds=expected)).isoformat() if timestamps else None,
        "strictly_ordered": ordered,
        "duplicates": duplicates,
        "invalid_mid_ohlc": invalid_mid,
        "gaps": gaps,
        "non_weekend_gaps": sum(not gap["weekend_boundary"] for gap in gaps),
    }


def validate_dataset(
    cache_path: str,
    *,
    instrument: str,
    start: str,
    end: str,
    warmup_days: int,
    horizon_minutes: int,
    repo: str | Path = ".",
    expected_data_sha256: Optional[str] = None,
    expected_code_sha: Optional[str] = None,
) -> Dict[str, Any]:
    cache = Path(cache_path).resolve()
    with cache.open("r", encoding="utf-8") as handle:
        bundle = json.load(handle)
    if not isinstance(bundle, dict):
        raise ValueError("Historical cache must be an object keyed by timeframe")
    data_sha = sha256_file(cache)
    code_sha = _git_head(repo)
    start_dt, end_dt = _dt(start), _dt(end)
    frames = {}
    failures: List[str] = []
    for timeframe in TIMEFRAME_SECONDS:
        rows = bundle.get(timeframe)
        if not isinstance(rows, list) or not rows:
            failures.append(f"{timeframe}_MISSING")
            frames[timeframe] = {"count": 0}
            continue
        frames[timeframe] = _timeframe_report(timeframe, rows)
        if not frames[timeframe]["strictly_ordered"]:
            failures.append(f"{timeframe}_NOT_STRICTLY_ORDERED")
        if frames[timeframe]["duplicates"]:
            failures.append(f"{timeframe}_DUPLICATES")
        if frames[timeframe]["invalid_mid_ohlc"]:
            failures.append(f"{timeframe}_INVALID_OHLC")
    m1 = bundle.get("M1") or []
    midpoint_only = any(not M1_BID_ASK.issubset(row) for row in m1)
    invalid_bid_ask = sum(
        not _ohlc_valid(row, "bid_") or not _ohlc_valid(row, "ask_")
        for row in m1 if M1_BID_ASK.issubset(row)
    )
    crossed = sum(
        any(float(row[f"ask_{part}"]) < float(row[f"bid_{part}"]) for part in "ohlc")
        for row in m1 if M1_BID_ASK.issubset(row)
    )
    interpolation_markers = sum(
        bool(row.get("interpolated") or row.get("is_interpolated") or str(row.get("source") or "").upper() == "INTERPOLATED")
        for rows in bundle.values() if isinstance(rows, list) for row in rows
    )
    instruments = sorted({
        str(row.get("instrument")).upper()
        for rows in bundle.values() if isinstance(rows, list) for row in rows
        if row.get("instrument")
    })
    if midpoint_only:
        failures.append("M1_MIDPOINT_ONLY_OR_INCOMPLETE_BID_ASK")
    if invalid_bid_ask:
        failures.append("M1_INVALID_BID_ASK_OHLC")
    if crossed:
        failures.append("M1_NEGATIVE_SPREAD")
    if interpolation_markers:
        failures.append("INTERPOLATION_DETECTED")
    if instruments and instruments != [instrument.upper()]:
        failures.append("CROSS_ASSET_DATASET_CONTAMINATION")
    if expected_data_sha256 and data_sha != expected_data_sha256:
        failures.append("DATA_SHA256_MISMATCH")
    if expected_code_sha and code_sha != expected_code_sha:
        failures.append("CODE_SHA_MISMATCH")
    if start_dt >= end_dt:
        failures.append("INVALID_WINDOW")
    if warmup_days < 1 or horizon_minutes < 1:
        failures.append("INVALID_WARMUP_OR_HORIZON")
    required_start = start_dt - timedelta(days=max(0, int(warmup_days)))
    required_end = end_dt + timedelta(minutes=max(0, int(horizon_minutes)))
    coverage = {}
    for timeframe, frame in frames.items():
        first = _dt(frame["first"]) if frame.get("first") else None
        complete_end = _dt(frame["coverage_end"]) if frame.get("coverage_end") else None
        tolerance = timedelta(seconds=TIMEFRAME_SECONDS[timeframe])
        starts_early = first is not None and first <= required_start + tolerance
        ends_late = complete_end is not None and complete_end >= required_end
        coverage[timeframe] = {"warmup_covered": starts_early, "horizon_covered": ends_late, "actual_complete_end": complete_end.isoformat() if complete_end else None, "required_horizon_end": required_end.isoformat()}
        if not starts_early:
            failures.append(f"{timeframe}_WARMUP_COVERAGE_INCOMPLETE")
        if not ends_late:
            failures.append(f"{timeframe}_HORIZON_COVERAGE_INCOMPLETE")
    return {
        "status": "PASS" if not failures else "FAIL",
        "stage": "data_integrity",
        "instrument": instrument.upper(),
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
        "warmup_days": int(warmup_days),
        "horizon_minutes": int(horizon_minutes),
        "research_end": end_dt.isoformat(),
        "required_horizon_end": required_end.isoformat(),
        "input_sha256": data_sha,
        "dataset_identity": hashlib.sha256(
            json.dumps({
                "instrument": instrument.upper(), "start": start_dt.isoformat(),
                "end": end_dt.isoformat(), "warmup_days": warmup_days,
                "horizon_minutes": horizon_minutes, "data_sha256": data_sha,
            }, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "code_sha": code_sha,
        "bid_ask_real": not midpoint_only and invalid_bid_ask == 0 and crossed == 0,
        "midpoint_only": midpoint_only,
        "interpolation_detected": interpolation_markers > 0,
        "timeframes": frames,
        "coverage": coverage,
        "gaps_present": any(frame.get("gaps") for frame in frames.values()),
        "instrument_values_in_rows": instruments,
        "cross_asset_isolation": not instruments or instruments == [instrument.upper()],
        "determinism_requirements": {
            "chronological_order": True,
            "random_shuffle": False,
            "fixed_input_sha256": data_sha,
            "fixed_code_sha": code_sha,
        },
        "failures": failures,
    }


def compare_determinism(first_path: str, second_path: str, *, ignored_keys: Iterable[str] = ("created_at", "updated_at", "frozen_at")) -> Dict[str, Any]:
    with Path(first_path).open("r", encoding="utf-8") as handle:
        first = json.load(handle)
    with Path(second_path).open("r", encoding="utf-8") as handle:
        second = json.load(handle)
    ignored = set(ignored_keys)

    def scrub(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: scrub(child) for key, child in value.items() if key not in ignored}
        if isinstance(value, list):
            return [scrub(child) for child in value]
        return value

    first_scrubbed, second_scrubbed = scrub(first), scrub(second)
    first_hash = hashlib.sha256(json.dumps(first_scrubbed, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    second_hash = hashlib.sha256(json.dumps(second_scrubbed, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {
        "status": "PASS" if first_hash == second_hash else "FAIL",
        "stage": "determinism",
        "first_content_sha256": first_hash,
        "second_content_sha256": second_hash,
        "identical_after_explicit_ignores": first_hash == second_hash,
        "ignored_keys": sorted(ignored),
    }

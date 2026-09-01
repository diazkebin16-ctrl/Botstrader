"""Leakage-resistant chronological validation helpers for historical replay."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Sequence, Tuple


def _dt(v: Any) -> datetime:
    if isinstance(v, datetime): d = v
    else: d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    if d.tzinfo is None: d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


@dataclass(frozen=True)
class ReplayValidationConfig:
    discovery_fraction: float = 0.60
    validation_fraction: float = 0.20
    embargo_minutes: int = 30
    walk_forward_train_episodes: int = 60
    walk_forward_test_episodes: int = 20
    walk_forward_step_episodes: int = 20


def _event_end(row: Mapping[str, Any], horizon_bars: int) -> datetime:
    if row.get("exit_ts"):
        return _dt(row["exit_ts"])
    return _dt(row["candle_ts"]) + timedelta(minutes=max(1, int(horizon_bars)))


def _purge_before(rows: Sequence[Mapping[str, Any]], boundary: datetime, horizon_bars: int) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows if _event_end(r, horizon_bars) < boundary]


def _embargo_after(rows: Sequence[Mapping[str, Any]], boundary: datetime, embargo_minutes: int) -> List[Dict[str, Any]]:
    cutoff = boundary + timedelta(minutes=max(0, int(embargo_minutes)))
    return [dict(r) for r in rows if _dt(r["candle_ts"]) >= cutoff]


def chronological_holdout(rows: Sequence[Mapping[str, Any]], *, horizon_bars: int,
                          config: ReplayValidationConfig = ReplayValidationConfig()) -> Dict[str, Any]:
    data = sorted((dict(r) for r in rows), key=lambda r: _dt(r["candle_ts"]))
    n = len(data)
    if n < 3:
        return {"status": "INSUFFICIENT_DATA", "discovery": data, "validation": [], "test": [], "purged": 0, "embargoed": 0}

    d_frac = min(max(float(config.discovery_fraction), 0.05), 0.90)
    v_frac = min(max(float(config.validation_fraction), 0.05), 0.90)
    if d_frac + v_frac >= 0.95:
        raise ValueError("discovery_fraction + validation_fraction must leave a test holdout")
    d_idx = max(1, min(n - 2, int(n * d_frac)))
    v_idx = max(d_idx + 1, min(n - 1, int(n * (d_frac + v_frac))))
    val_boundary = _dt(data[d_idx]["candle_ts"])
    test_boundary = _dt(data[v_idx]["candle_ts"])

    discovery_raw = data[:d_idx]
    validation_raw = data[d_idx:v_idx]
    test_raw = data[v_idx:]
    discovery = _purge_before(discovery_raw, val_boundary, horizon_bars)
    validation_purged = _purge_before(validation_raw, test_boundary, horizon_bars)
    validation = _embargo_after(validation_purged, val_boundary, config.embargo_minutes)
    test = _embargo_after(test_raw, test_boundary, config.embargo_minutes)
    purged = (len(discovery_raw) - len(discovery)) + (len(validation_raw) - len(validation_purged))
    embargoed = (len(validation_purged) - len(validation)) + (len(test_raw) - len(test))
    def identity(row: Mapping[str, Any], partition: str, reason: str) -> Dict[str, Any]:
        return {
            "partition": partition, "reason": reason, "candle_ts": row.get("candle_ts"),
            "instrument": row.get("instrument"),
            "direction": row.get("research_direction") or row.get("signal") or row.get("chosen_signal"),
        }
    discovery_ids = {str(row.get("candle_ts")) for row in discovery}
    validation_purged_ids = {str(row.get("candle_ts")) for row in validation_purged}
    validation_ids = {str(row.get("candle_ts")) for row in validation}
    test_ids = {str(row.get("candle_ts")) for row in test}
    purged_rows = [identity(row, "discovery", "OVERLAPS_VALIDATION_BOUNDARY") for row in discovery_raw if str(row.get("candle_ts")) not in discovery_ids]
    purged_rows.extend(identity(row, "validation", "OVERLAPS_HOLDOUT_BOUNDARY") for row in validation_raw if str(row.get("candle_ts")) not in validation_purged_ids)
    embargoed_rows = [identity(row, "validation", "POST_DISCOVERY_EMBARGO") for row in validation_purged if str(row.get("candle_ts")) not in validation_ids]
    embargoed_rows.extend(identity(row, "holdout", "POST_VALIDATION_EMBARGO") for row in test_raw if str(row.get("candle_ts")) not in test_ids)
    return {
        "status": "OK" if discovery and validation and test else "INSUFFICIENT_DATA",
        "discovery": discovery, "validation": validation, "test": test,
        "purged": purged, "embargoed": embargoed,
        "purged_rows": purged_rows, "embargoed_rows": embargoed_rows,
        "boundaries": {"validation": val_boundary.isoformat(), "test": test_boundary.isoformat()},
    }


def walk_forward_splits(rows: Sequence[Mapping[str, Any]], *, horizon_bars: int,
                        config: ReplayValidationConfig = ReplayValidationConfig()) -> List[Dict[str, Any]]:
    data = sorted((dict(r) for r in rows), key=lambda r: _dt(r["candle_ts"]))
    train_n = max(2, int(config.walk_forward_train_episodes))
    test_n = max(1, int(config.walk_forward_test_episodes))
    step_n = max(1, int(config.walk_forward_step_episodes))
    folds = []
    start = 0
    while start + train_n + test_n <= len(data):
        train_raw = data[start:start + train_n]
        test_raw = data[start + train_n:start + train_n + test_n]
        boundary = _dt(test_raw[0]["candle_ts"])
        train = _purge_before(train_raw, boundary, horizon_bars)
        test = _embargo_after(test_raw, boundary, config.embargo_minutes)
        folds.append({
            "train": train, "test": test,
            "purged": len(train_raw) - len(train),
            "embargoed": len(test_raw) - len(test),
            "boundary": boundary.isoformat(),
        })
        start += step_n
    return folds

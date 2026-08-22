from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def pip_size(instrument: str) -> float:
    inst = (instrument or "").upper()
    return 0.01 if "_JPY" in inst or inst.endswith("JPY") else 0.0001


def resolve_outcome(
    sample: Mapping[str, Any],
    m1: List[Dict[str, Any]],
    *,
    horizon_bars: int,
    round_trip_cost_pips: float = 0.0,
) -> Optional[Dict[str, Any]]:
    """Resolve one directional opportunity using M1 bars.

    The input candles are midpoint OHLC. ``round_trip_cost_pips`` therefore shifts
    the effective TP/SL touch levels against the trade so research can model the
    economic effect of spread/slippage without pretending bid/ask candles exist.

    TIMEOUT and AMBIGUOUS are explicit terminal outcomes with ``label=None``.
    Consumers must account for them separately instead of silently treating the
    resolved dataset as binary-only.
    """
    start = _parse_dt(sample.get("candle_ts")) or _parse_dt(sample.get("created_ts"))
    if start is None:
        return {"status": "INVALID", "label": None, "bars": 0, "mfe_r": 0.0, "mae_r": 0.0,
                "note": "invalid start timestamp", "cost_r": 0.0}

    bars = [x for x in m1 if x.get("t") is not None and x["t"] > start]
    if not bars:
        return None

    direction = str(sample.get("direction") or "").upper()
    entry = float(sample["entry"])
    stop = float(sample["stop"])
    target = float(sample["target"])
    risk = abs(entry - stop)
    if direction not in ("BUY", "SELL"):
        return {"status": "INVALID", "label": None, "bars": 0, "mfe_r": 0.0, "mae_r": 0.0,
                "note": "invalid direction", "cost_r": 0.0}
    if risk <= 0:
        return {"status": "INVALID", "label": None, "bars": 0, "mfe_r": 0.0, "mae_r": 0.0,
                "note": "zero risk", "cost_r": 0.0}

    cost_distance = max(0.0, float(round_trip_cost_pips)) * pip_size(str(sample.get("instrument") or ""))
    cost_r = cost_distance / risk if risk > 0 else 0.0

    # With midpoint candles, adverse round-trip costs make the profit barrier
    # farther away and the economic loss barrier closer to entry.
    if direction == "BUY":
        effective_target = target + cost_distance
        effective_stop = stop + cost_distance
    else:
        effective_target = target - cost_distance
        effective_stop = stop - cost_distance

    mfe, mae = 0.0, 0.0
    max_bars = max(1, int(horizon_bars))
    for idx, x in enumerate(bars[:max_bars], start=1):
        if direction == "BUY":
            mfe = max(mfe, (float(x["h"]) - entry) / risk)
            mae = min(mae, (float(x["l"]) - entry) / risk)
            hit_tp = float(x["h"]) >= effective_target
            hit_sl = float(x["l"]) <= effective_stop
        else:
            mfe = max(mfe, (entry - float(x["l"])) / risk)
            mae = min(mae, (entry - float(x["h"])) / risk)
            hit_tp = float(x["l"]) <= effective_target
            hit_sl = float(x["h"]) >= effective_stop

        common = {
            "bars": idx,
            "mfe_r": mfe,
            "mae_r": mae,
            "cost_r": cost_r,
            "effective_target": effective_target,
            "effective_stop": effective_stop,
        }
        if hit_tp and hit_sl:
            return {"status": "AMBIGUOUS", "label": None,
                    "note": "SL y TP efectivos tocados en la misma vela M1", **common}
        if hit_tp:
            return {"status": "WIN", "label": 1, "note": None, **common}
        if hit_sl:
            return {"status": "LOSS", "label": 0, "note": None, **common}

    if len(bars) >= max_bars:
        return {
            "status": "TIMEOUT", "label": None, "bars": max_bars,
            "mfe_r": mfe, "mae_r": mae, "cost_r": cost_r,
            "effective_target": effective_target, "effective_stop": effective_stop,
            "note": f"No resolvió en {max_bars} min",
        }
    return None



def economic_realized_r(sample: Mapping[str, Any], outcome: Mapping[str, Any]) -> Optional[float]:
    """Return net realized R under the same shifted-barrier cost model.

    ``resolve_outcome`` already moves the midpoint TP farther away and the midpoint
    SL closer to entry by ``cost_distance``.  That means the round-trip cost is
    already embedded in the barrier that must be touched.  We therefore compute
    gross midpoint R to the *effective* barrier and subtract ``cost_r`` exactly
    once.  Under the current model this simplifies to the nominal target R on a
    WIN and -1R on a LOSS.  Keeping the arithmetic explicit prevents accidental
    double counting if the barrier model changes later.
    """
    status=str(outcome.get("status") or "").upper()
    if status not in ("WIN","LOSS"):
        return None
    direction=str(sample.get("direction") or "").upper()
    entry=float(sample["entry"]); stop=float(sample["stop"]); target=float(sample["target"]); risk=abs(entry-stop)
    if direction not in ("BUY","SELL") or risk<=0:
        return None
    cost_r=max(0.0,float(outcome.get("cost_r",0.0) or 0.0))
    if status=="WIN":
        px=float(outcome.get("effective_target",target))
        gross_r=((px-entry)/risk) if direction=="BUY" else ((entry-px)/risk)
    else:
        px=float(outcome.get("effective_stop",stop))
        gross_r=((px-entry)/risk) if direction=="BUY" else ((entry-px)/risk)
    return gross_r-cost_r

def collapse_market_episodes(
    rows: Iterable[Mapping[str, Any]],
    *,
    gap_minutes: int = 15,
    timestamp_key: str = "candle_ts",
    instrument_key: str = "instrument",
    direction_key: str = "signal",
    variant_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Collapse highly-correlated minute snapshots into independent episodes.

    Rows are grouped when instrument, direction and (optionally) shadow variant are
    unchanged and consecutive observations are no more than ``gap_minutes`` apart.
    The first observation is retained, matching the earliest actionable opportunity
    rather than allowing a long trend to count as dozens of independent samples.
    """
    normalized = [dict(r) for r in rows]
    normalized.sort(key=lambda r: (_parse_dt(r.get(timestamp_key)) or datetime.min))
    out: List[Dict[str, Any]] = []
    last_by_group: Dict[tuple, datetime] = {}

    for row in normalized:
        dt = _parse_dt(row.get(timestamp_key))
        if dt is None:
            # Bad timestamps should not silently merge unrelated evidence.
            out.append(row)
            continue
        group = (
            row.get(instrument_key),
            row.get(direction_key),
            row.get(variant_key) if variant_key else None,
        )
        prev = last_by_group.get(group)
        if prev is None or (dt - prev).total_seconds() > max(1, int(gap_minutes)) * 60:
            out.append(row)
        last_by_group[group] = dt
    return out


def annotate_market_episodes(
    rows: Iterable[Mapping[str, Any]], *, gap_minutes: int = 15,
    timestamp_key: str = "candle_ts", instrument_key: str = "instrument",
    direction_key: str = "signal",
) -> List[Dict[str, Any]]:
    """Return all rows with a stable episode_id, without collapsing variants.

    Episode boundaries depend only on instrument+direction+time. Source and shadow
    variant deliberately do not participate, so canonical and counterfactual rows
    from the same market move cannot leak across a temporal holdout boundary.
    """
    normalized=[dict(r) for r in rows]
    normalized.sort(key=lambda r: (_parse_dt(r.get(timestamp_key)) or datetime.min,
                                   str(r.get(instrument_key) or ""), str(r.get(direction_key) or "")))
    last_by_group: Dict[tuple, datetime] = {}
    seq_by_group: Dict[tuple, int] = {}
    start_by_group: Dict[tuple, str] = {}
    for row in normalized:
        dt=_parse_dt(row.get(timestamp_key))
        group=(row.get(instrument_key),row.get(direction_key))
        prev=last_by_group.get(group)
        if dt is None or prev is None or (dt-prev).total_seconds()>max(1,int(gap_minutes))*60:
            seq_by_group[group]=seq_by_group.get(group,0)+1
            start_by_group[group]=(dt.isoformat() if dt else f"invalid-{seq_by_group[group]}")
        seq=seq_by_group[group]
        row["episode_id"]=f"{group[0]}::{group[1]}::{seq}::{start_by_group[group]}"
        if dt is not None:last_by_group[group]=dt
    return normalized


def split_episode_holdout(rows: Iterable[Mapping[str, Any]], holdout_fraction: float, *, min_holdout_episodes: int = 1):
    """Chronological train/holdout split with whole episodes kept on one side."""
    data=[dict(r) for r in rows]
    data.sort(key=lambda r: (_parse_dt(r.get("candle_ts")) or datetime.min, str(r.get("episode_id") or "")))
    episodes=[]
    seen=set()
    for r in data:
        eid=r.get("episode_id")
        if eid not in seen:
            seen.add(eid);episodes.append(eid)
    if len(episodes)<2:return data,[]
    n_hold=max(int(min_holdout_episodes), int(round(len(episodes)*max(0.0,min(1.0,float(holdout_fraction))))))
    n_hold=min(n_hold,len(episodes)-1)
    hold=set(episodes[-n_hold:])
    discovery=[r for r in data if r.get("episode_id") not in hold]
    validation=[r for r in data if r.get("episode_id") in hold]
    return discovery,validation

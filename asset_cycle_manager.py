"""Multi-asset research rotation state; no asset is permanently complete."""
from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence


DEFAULT_ROTATION = ("GBP_USD", "EUR_USD", "USD_JPY", "AUD_USD", "USD_CAD")


def cycle_status(state: Mapping[str, Any], rotation: Sequence[str] = DEFAULT_ROTATION) -> Dict[str, Any]:
    assets = state.get("assets") or {}
    rows = []
    for instrument in rotation:
        asset = assets.get(instrument) or {}
        lifecycle = asset.get("lifecycle") or {}
        rows.append({
            "instrument": instrument,
            "dataset_identity": asset.get("dataset_identity"),
            "last_valid_stage": lifecycle.get("last_valid_stage"),
            "next_allowed_stage": lifecycle.get("next_allowed_stage") or "data_integrity",
            "status": lifecycle.get("status") or "NEW_WINDOW_REQUIRED",
            "permanently_complete": False,
        })
    next_row = next((row for row in rows if row["next_allowed_stage"] != "HUMAN_IA1_REVIEW"), rows[0] if rows else None)
    return {"rotation": rows, "next_asset": next_row, "repeat_with_recent_window": True}

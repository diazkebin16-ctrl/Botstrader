#!/usr/bin/env python3
"""Durable, cross-code governance for Automation V3 holdout exposure."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from autonomous_asset_optimizer import canonical_sha256, load_json, utc_now, write_json


SCHEMA_VERSION = 1


def holdout_population_identity(
    target: Mapping[str, Any], phase2: Mapping[str, Any]
) -> tuple[str, dict[str, Any]]:
    """Identify the market population without treating a code SHA as new data.

    The identity intentionally excludes code and candidate/methodology fields.  A
    code-only retry or a repartition of the same already exposed observations must
    not manufacture a fresh holdout.
    """
    dataset = target.get("dataset_identity")
    if not isinstance(dataset, Mapping):
        raise ValueError("HOLDOUT_IDENTITY_MISSING: target dataset identity")
    instrument = str(target.get("instrument") or dataset.get("instrument") or "").upper()
    if instrument != str(phase2.get("instrument") or "").upper():
        raise ValueError("HOLDOUT_IDENTITY_MISMATCH: instrument")
    material = {
        "instrument": instrument,
        "source_input_sha256": dataset.get("input_sha256") or dataset.get("data_sha256"),
        "start": target.get("start") or dataset.get("start"),
        "research_end": target.get("end") or dataset.get("research_end") or dataset.get("end"),
        "required_horizon_end": dataset.get("required_horizon_end"),
        "horizon_minutes": dataset.get("horizon_minutes")
        or (phase2.get("partition_config") or {}).get("horizon_minutes"),
    }
    required = ("instrument", "source_input_sha256", "start", "research_end", "horizon_minutes")
    missing = [key for key in required if material.get(key) in (None, "")]
    if missing:
        raise ValueError(f"HOLDOUT_IDENTITY_MISSING: {','.join(missing)}")
    return canonical_sha256(material), material


def ledger_path(root: str | Path, instrument: str) -> Path:
    return Path(root) / instrument / "autonomous_v3" / "holdout_consumption_ledger.json"


def _empty(instrument: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "instrument": instrument,
        "records": [],
        "production_authority": False,
    }


def load_consumption_ledger(path: str | Path, instrument: str) -> dict[str, Any]:
    path = Path(path)
    payload = load_json(path) if path.is_file() else _empty(instrument)
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("instrument") != instrument
        or payload.get("production_authority") is not False
        or not isinstance(payload.get("records"), list)
    ):
        raise ValueError("HOLDOUT_LEDGER_INVALID")
    return payload


def record_holdout_opening(path: str | Path, record: Mapping[str, Any]) -> dict[str, Any]:
    instrument = str(record.get("instrument") or "").upper()
    payload = load_consumption_ledger(path, instrument)
    population = record.get("holdout_population_identity_sha256")
    if not population:
        raise ValueError("HOLDOUT_IDENTITY_MISSING: population")
    if any(item.get("holdout_population_identity_sha256") == population for item in payload["records"]):
        raise ValueError("HOLDOUT_ALREADY_CONSUMED: new selection requires genuinely new data")
    opened = dict(record)
    opened.update(status="OPENING", opened_at=record.get("opened_at") or utc_now(), production_authority=False)
    payload["records"].append(opened)
    write_json(path, payload)
    return opened


def complete_holdout_record(
    path: str | Path,
    *,
    instrument: str,
    population_identity: str,
    holdout_sha256: str,
    holdout_status: str,
) -> dict[str, Any]:
    payload = load_consumption_ledger(path, instrument)
    matches = [
        item for item in payload["records"]
        if item.get("holdout_population_identity_sha256") == population_identity
    ]
    if len(matches) != 1:
        raise ValueError("HOLDOUT_LEDGER_UNCERTAIN: opening record missing or duplicated")
    matches[0].update(
        status="OPENED",
        holdout_sha256=holdout_sha256,
        holdout_status=holdout_status,
        completed_at=utc_now(),
        production_authority=False,
    )
    write_json(path, payload)
    return matches[0]


def merge_recovered_holdout(path: str | Path, record: Mapping[str, Any]) -> dict[str, Any]:
    """Idempotently import trusted evidence recovered from a repository artifact."""
    instrument = str(record.get("instrument") or "").upper()
    payload = load_consumption_ledger(path, instrument)
    population = record.get("holdout_population_identity_sha256")
    if not population or not record.get("candidate_definition_sha256") or not record.get("freeze_sha256"):
        raise ValueError("HOLDOUT_LEDGER_INVALID: recovered identity incomplete")
    existing = next(
        (item for item in payload["records"] if item.get("holdout_population_identity_sha256") == population),
        None,
    )
    recovered = dict(record)
    recovered["status"] = recovered.get("status") if recovered.get("status") in {"OPENING", "OPENED"} else "OPENED"
    recovered["production_authority"] = False
    if existing is None:
        payload["records"].append(recovered)
    else:
        immutable = (
            "instrument",
            "holdout_population_identity_sha256",
            "candidate_definition_sha256",
            "freeze_sha256",
            "holdout_sha256",
        )
        if any(
            existing.get(key) is not None
            and recovered.get(key) is not None
            and existing.get(key) != recovered.get(key)
            for key in immutable
        ):
            raise ValueError("HOLDOUT_LEDGER_CONFLICT: recovered evidence differs")
        if existing.get("status") == "OPENED" and recovered.get("status") == "OPENING":
            recovered["status"] = "OPENED"
        existing.update({key: value for key, value in recovered.items() if value is not None})
    write_json(path, payload)
    return recovered

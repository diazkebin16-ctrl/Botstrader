#!/usr/bin/env python3
"""Recover governed holdout-consumption evidence from the latest Actions artifact."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

from autonomous_asset_optimizer import canonical_sha256
from automation_v3_governance import (
    holdout_population_identity,
    ledger_path,
    merge_recovered_holdout,
)


def _json_member(archive: zipfile.ZipFile, name: str) -> dict[str, Any]:
    try:
        value = json.loads(archive.read(name))
    except (KeyError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"RECOVERY_ARTIFACT_INVALID: {name}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"RECOVERY_ARTIFACT_INVALID: {name}")
    return value


def recover_archive(
    archive_path: str | Path,
    *,
    root: str | Path,
    instrument: str,
    artifact_id: str = "local",
) -> int:
    symbol = instrument.upper()
    recovered = 0
    with zipfile.ZipFile(archive_path) as archive:
        names = set(archive.namelist())
        for ledger_name in sorted(name for name in names if name.endswith("/holdout_consumption_ledger.json")):
            source_ledger = _json_member(archive, ledger_name)
            if (
                source_ledger.get("instrument") != symbol
                or source_ledger.get("production_authority") is not False
                or not isinstance(source_ledger.get("records"), list)
            ):
                raise ValueError("RECOVERY_ARTIFACT_INVALID: holdout ledger")
            for source_record in source_ledger["records"]:
                if not isinstance(source_record, dict) or source_record.get("instrument") != symbol:
                    raise ValueError("RECOVERY_ARTIFACT_INVALID: holdout ledger record")
                record = dict(source_record)
                record.update(source_artifact_id=str(artifact_id), source_member=ledger_name, recovered=True)
                merge_recovered_holdout(ledger_path(root, symbol), record)
                recovered += 1
        for holdout_name in sorted(name for name in names if name.endswith("/10_holdout.json")):
            holdout = _json_member(archive, holdout_name)
            if holdout.get("holdout_opened_once") is not True:
                continue
            parent = PurePosixPath(holdout_name).parent
            target_name = str(parent / "03_target_population.json")
            phase2_name = str(parent / "05_phase_2.json")
            freeze_name = str(parent / "09_freeze.json")
            target = _json_member(archive, target_name)
            phase2 = _json_member(archive, phase2_name)
            freeze = _json_member(archive, freeze_name)
            population_sha, population_material = holdout_population_identity(target, phase2)
            if str(holdout.get("instrument") or "").upper() != symbol:
                raise ValueError("RECOVERY_ARTIFACT_INVALID: cross-asset holdout")
            candidate_sha = holdout.get("candidate_definition_sha256")
            freeze_sha = holdout.get("freeze_sha256")
            if candidate_sha != freeze.get("candidate_definition_sha256"):
                raise ValueError("RECOVERY_ARTIFACT_INVALID: candidate provenance")
            record = {
                "instrument": symbol,
                "holdout_population_identity_sha256": population_sha,
                "holdout_population_identity": population_material,
                "dataset_identity_sha256": canonical_sha256(phase2.get("dataset_identity") or {}),
                "candidate_definition_sha256": candidate_sha,
                "freeze_sha256": freeze_sha,
                "holdout_sha256": hashlib.sha256(archive.read(holdout_name)).hexdigest(),
                "holdout_status": holdout.get("status"),
                "source_artifact_id": str(artifact_id),
                "source_member": holdout_name,
                "opened_at": holdout.get("holdout_opened_at"),
                "recovered": True,
            }
            merge_recovered_holdout(ledger_path(root, symbol), record)
            recovered += 1
    return recovered


def _request_json(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "Botstrader-Automation-V3",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise ValueError("RECOVERY_API_INVALID")
    return value


class _ArtifactRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Do not forward GitHub credentials to the signed artifact storage host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and urlparse(req.full_url).netloc != urlparse(newurl).netloc:
            redirected.remove_header("Authorization")
        return redirected


def _download_artifact(artifact: dict[str, Any], token: str) -> bytes:
    request = urllib.request.Request(
        str(artifact["archive_download_url"]),
        headers={"Authorization": f"Bearer {token}", "User-Agent": "Botstrader-Automation-V3"},
    )
    opener = urllib.request.build_opener(_ArtifactRedirectHandler())
    with opener.open(request, timeout=120) as response:
        return response.read()


def _recover_download(content: bytes, *, root: Path, instrument: str, artifact_id: str) -> int:
    with zipfile.ZipFile(io.BytesIO(content)) as source:
        root.mkdir(parents=True, exist_ok=True)
        temporary = root / f".recovery-{os.getpid()}.zip"
        with zipfile.ZipFile(temporary, "w") as target:
            for member in source.infolist():
                target.writestr(member, source.read(member.filename))
        try:
            return recover_archive(temporary, root=root, instrument=instrument, artifact_id=artifact_id)
        finally:
            temporary.unlink(missing_ok=True)


def recover_latest(*, root: Path, instrument: str, repository: str, token: str) -> int:
    listing = _request_json(
        f"https://api.github.com/repos/{repository}/actions/artifacts?per_page=100",
        token,
    )
    prefix = f"automation-v3-{instrument}-"
    artifacts = [
        item for item in listing.get("artifacts") or []
        if isinstance(item, dict)
        and str(item.get("name") or "").startswith(prefix)
        and not item.get("expired")
    ]
    artifacts.sort(key=lambda item: (str(item.get("created_at") or ""), int(item.get("id") or 0)), reverse=True)
    scan_limit = min(10, max(1, int(os.getenv("BOTS_V3_RECOVERY_ARTIFACT_SCAN_LIMIT", "5"))))
    for artifact in artifacts[:scan_limit]:
        recovered = _recover_download(
            _download_artifact(artifact, token),
            root=root,
            instrument=instrument,
            artifact_id=str(artifact["id"]),
        )
        if recovered:
            return recovered
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--root", default=os.getenv("BOTS_RESEARCH_ROOT"))
    parser.add_argument("--archive")
    args = parser.parse_args()
    if not args.root:
        raise SystemExit("BOTS_RESEARCH_ROOT is required")
    if args.archive:
        count = recover_archive(args.archive, root=args.root, instrument=args.instrument)
    else:
        repository = os.getenv("GITHUB_REPOSITORY", "")
        token = os.getenv("GH_TOKEN", "")
        if not repository or not token:
            raise SystemExit("GitHub Actions recovery requires GITHUB_REPOSITORY and GH_TOKEN")
        count = recover_latest(
            root=Path(args.root), instrument=args.instrument.upper(), repository=repository,
            token=token,
        )
    print(json.dumps({"status": "PASS", "recovered_holdouts": count, "production_authority": False}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

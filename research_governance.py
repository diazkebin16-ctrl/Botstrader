"""Research lifecycle gates with no production or trading authority."""
from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Mapping, Optional


class DecisionGateEngine:
    """Return explicit ALLOWED/BLOCKED transitions from immutable evidence."""

    @staticmethod
    def evaluate(
        transition: str,
        *,
        integrity: Optional[Mapping[str, Any]] = None,
        replay: Optional[Mapping[str, Any]] = None,
        target_population: Optional[Mapping[str, Any]] = None,
        phase1: Optional[Mapping[str, Any]] = None,
        discovery: Optional[Mapping[str, Any]] = None,
        frozen: Optional[Mapping[str, Any]] = None,
        holdout: Optional[Mapping[str, Any]] = None,
        pre_audit: Optional[Mapping[str, Any]] = None,
        phase1_approval: Optional[Mapping[str, Any]] = None,
        instrument: Optional[str] = None,
        dataset_identity: Optional[str] = None,
        code_sha: Optional[str] = None,
        phase1_artifact_sha256: Optional[str] = None,
        ia1_approved: bool = False,
    ) -> Dict[str, Any]:
        transition = transition.upper()
        reasons = []
        checks: Dict[str, bool] = {}

        def require(name: str, condition: bool, reason: str) -> None:
            checks[name] = bool(condition)
            if not condition:
                reasons.append(reason)

        if transition == "PHASE_2":
            require("data_verified", bool(integrity) and integrity.get("status") == "PASS", "DATA NOT VERIFIED")
            methodology = (replay or {}).get("methodology") or {}
            require("replay_verified", methodology.get("no_lookahead_decision") is True, "REPLAY LOOK-AHEAD EVIDENCE MISSING")
            require("target_population_verified", bool(target_population) and target_population.get("lookahead_protection") is True, "TARGET POPULATION NOT VERIFIED")
            unrecovered = len((phase1 or {}).get("unrecovered_target_wins") or [])
            recovered_all = bool(phase1) and phase1.get("all_target_wins_recovered") is True
            reviewed_best_viable = False
            if not recovered_all and isinstance(phase1_approval, Mapping):
                best_policy = (phase1 or {}).get("best_policy")
                best_policy_sha = None
                if isinstance(best_policy, Mapping):
                    material = json.dumps(
                        dict(best_policy), sort_keys=True, separators=(",", ":"), default=str
                    ).encode("utf-8")
                    best_policy_sha = hashlib.sha256(material).hexdigest()
                reviewed_best_viable = (
                    phase1_approval.get("approval_type") == "BEST_VIABLE_POLICY"
                    and phase1_approval.get("active") is True
                    and phase1_approval.get("ia1_approved") is True
                    and phase1_approval.get("production_authority") is False
                    and phase1_approval.get("instrument") == str(instrument or "").upper()
                    and phase1_approval.get("dataset_identity") == dataset_identity
                    and phase1_approval.get("code_sha") == code_sha
                    and phase1_approval.get("phase1_artifact_sha256") == phase1_artifact_sha256
                    and phase1_approval.get("best_policy_sha256") == best_policy_sha
                )
            require(
                "phase_1_complete_or_best_viable",
                recovered_all or reviewed_best_viable,
                f"{unrecovered} target WINs unrecovered in Phase 1",
            )
            require("phase_1_discovery_only", bool(phase1) and phase1.get("selection_scope") == "DISCOVERY_ONLY", "PHASE 1 WAS NOT DISCOVERY-ONLY")
        elif transition == "HOLDOUT":
            require("discovery_complete", bool(discovery) and discovery.get("status") == "OK", "DISCOVERY NOT COMPLETE")
            require("candidate_frozen", bool(frozen) and frozen.get("immutable") is True, "CANDIDATE NOT FROZEN BEFORE HOLDOUT")
            require("holdout_unopened_at_freeze", bool(frozen) and frozen.get("holdout_opened") is False, "FREEZE OCCURRED AFTER HOLDOUT OPEN")
        elif transition == "AUDIT":
            require("holdout_executed", bool(holdout) and holdout.get("holdout_opened_once") is True, "HOLDOUT NOT EXECUTED FROM FROZEN CANDIDATE")
            require("no_retuning", bool(holdout) and holdout.get("retuning_after_holdout") is False, "RETUNING AFTER HOLDOUT DETECTED")
        elif transition == "FORWARD":
            require("holdout_pass", bool(holdout) and holdout.get("status") == "PASS", "HOLDOUT DID NOT PASS")
            require("audit_accept", bool(pre_audit) and pre_audit.get("verdict") in {"ACCEPT", "ACCEPT WITH LIMITATIONS"}, "PRE-AUDIT DID NOT ACCEPT")
            risk = ((holdout or {}).get("overfitting_risk") or {}).get("severity")
            require("overfitting_not_high", risk != "HIGH", "OVERFITTING RISK HIGH")
            require("ia1_approval", ia1_approved, "IA #1 APPROVAL REQUIRED")
        else:
            raise ValueError(f"Unknown research transition: {transition}")
        return {
            "status": "ALLOWED" if not reasons else "BLOCKED",
            "transition": transition,
            "checks": checks,
            "reasons": reasons,
            "production_authority": False,
            "auto_production_change": False,
        }

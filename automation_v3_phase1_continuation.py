"""Fail-closed Automation V3 continuation for an exact Phase 1 review artifact."""
from __future__ import annotations

from pathlib import Path
from typing import Any


AUTOMATION_APPROVAL_TYPE = "AUTONOMOUS_RESEARCH_POLICY_APPROVAL"
AUTOMATION_AUTHORITY = "AUTOMATION_V3_POLICY"
AUTOMATION_SCOPE = "RESEARCH_CONTINUATION_ONLY"


def _is_review_required(artifact: dict[str, Any]) -> bool:
    return (
        artifact.get("stage") == "phase_1"
        and artifact.get("status") == "REVIEW_REQUIRED"
        and artifact.get("all_target_wins_recovered") is False
    )


def run_with_phase1_autonomous_continuation(
    *,
    cascade: Any,
    manager: Any,
    ledger: Any,
    instrument: str,
    stages: Any,
    through: str,
    phase1_artifact: str | Path,
    load_json: Any,
    utc_now: Any,
) -> dict[str, Any] | None:
    """Run the cascade and handle exactly one eligible Phase 1 review boundary.

    Every policy/binding validation remains in the manager's existing autonomous
    approval implementation.  This function only guarantees that the same
    continuation path is used regardless of whether the cascade is the initial
    run or a post-data-reacquire run.
    """
    try:
        cascade.run(instrument, stages, through=through)
        return None
    except Exception:
        path = Path(phase1_artifact)
        if not path.is_file():
            raise
        artifact = load_json(path)
        if not _is_review_required(artifact):
            raise

        approval = manager.active_phase1_best_viable_approval(instrument, path)
        approval_state = "REUSED_CURRENT"
        if approval is None:
            approval = manager.approve_phase1_autonomous(instrument, path)
            approval_state = "CREATED"

        # Do not accept a generic/human-shaped object as an autonomous approval.
        if approval.get("production_authority") is not False:
            raise ValueError("Phase 1 continuation attempted production authority")
        if approval_state == "CREATED":
            if approval.get("approval_type") != AUTOMATION_APPROVAL_TYPE:
                raise ValueError("autonomous Phase 1 approval type mismatch")
            if approval.get("approval_authority") != AUTOMATION_AUTHORITY:
                raise ValueError("autonomous Phase 1 approval authority mismatch")
            if approval.get("authorization_scope") != AUTOMATION_SCOPE:
                raise ValueError("autonomous Phase 1 approval scope mismatch")
            if approval.get("ia1_approved") is not False or approval.get("human_approval") is not False:
                raise ValueError("autonomous Phase 1 continuation cannot impersonate human approval")

        ledger.append(
            instrument,
            "decision_history",
            {
                "decision": "AUTONOMOUS_PHASE1_BEST_VIABLE",
                "approval": approval,
                "approval_state": approval_state,
                "at": utc_now(),
            },
        )
        ledger.mutate(
            instrument,
            phase1_status="REVIEW_REQUIRED",
            autonomous_approval=approval_state,
        )
        manager.update_phase(
            instrument,
            "phase_1",
            "COMPLETED",
            artifact=str(path),
            details={
                "review_status": "REVIEW_REQUIRED",
                "approval_type": approval.get("approval_type"),
                "approval_authority": approval.get("approval_authority"),
                "authorization_scope": approval.get("authorization_scope"),
                "ia1_approved": approval.get("ia1_approved", False),
                "human_approval": approval.get("human_approval", False),
                "production_authority": False,
            },
        )

        # CascadeOptimizer is responsible for validating the evidence-bound
        # approval and resuming without rewriting the exact Phase 1 artifact.
        cascade.run(instrument, stages, through=through)
        return dict(approval)

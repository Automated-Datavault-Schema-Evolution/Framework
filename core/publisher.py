"""Event payload builders for Schema Evolution Framework notifications."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from core.model import ChangeContext, ExecutionResult, Plan, PolicyOutcome, VerificationResult


def _utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format with a trailing Z."""
    return datetime.utcnow().isoformat() + "Z"



def _serialize_operations(ops: Any) -> List[Dict[str, Any]]:
    """Return JSON-safe operation payloads for event emission."""
    out: List[Dict[str, Any]] = []
    for op in ops or []:
        if isinstance(op, dict):
            out.append(op)
        else:
            out.append({"op": str(op)})
    return out



def build_schema_evolved_event(
    context: ChangeContext,
    policy: PolicyOutcome,
    plan: Plan,
    execution: ExecutionResult,
    verification: VerificationResult,
    new_version: int,
) -> Dict[str, Any]:
    """Build the success event published after a verified schema evolution."""
    now = _utc_now_iso()
    execution_ok = bool(execution.get("successful"))

    return {
        "event_type": "schema.evolved",
        "dataset": {
            "id": context["dataset_id"],
            "source": context.get("source", ""),
        },
        "correlation_id": context.get("correlation_id"),
        "previous_version": context.get("previous_version"),
        "new_version": new_version,
        "compatibility": policy.get("compatibility"),
        "plan": {
            "plan_id": plan.get("plan_id"),
            "dataset_id": plan.get("dataset_id"),
            "correlation_id": plan.get("correlation_id"),
            "executed_at": now,
            "operations": _serialize_operations(plan.get("operations")),
            "handler_results": {
                "lake": "success" if execution_ok else "failed",
                "vault": "success" if execution_ok else "failed",
            },
        },
        "verification": {
            "status": verification.get("status"),
            "checked_at": now,
            "evidence_snapshot_refs": verification.get("evidence_snapshot_refs", []),
            "issues": verification.get("issues", []),
        },
        "metadata": {
            "emitted_by": "sef",
            "emitted_at": now,
        },
    }



def build_success_event(
    context: ChangeContext,
    new_version: int,
    policy: PolicyOutcome,
    plan: Plan,
    execution: ExecutionResult,
    verification: VerificationResult,
) -> Dict[str, Any]:
    """Build the backward-compatible success event expected by the pipeline."""
    return build_schema_evolved_event(
        context=context,
        policy=policy,
        plan=plan,
        execution=execution,
        verification=verification,
        new_version=new_version,
    )



def build_failure_event(context: Dict[str, Any], reason: str, details: str) -> Dict[str, Any]:
    """Build the failure event published when policy or verification fails."""
    now = _utc_now_iso()
    return {
        "event_type": "schema.evolution.failed",
        "dataset": {"id": context["dataset_id"]},
        "correlation_id": context.get("correlation_id"),
        "reason": reason,
        "details": details,
        "metadata": {
            "emitted_by": "sef",
            "emitted_at": now,
        },
    }

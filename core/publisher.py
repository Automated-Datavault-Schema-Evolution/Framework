from datetime import datetime
from typing import Dict, Any, List

from core.model import ChangeContext, PolicyOutcome, ExecutionResult, VerificationResult, Plan


def _serialize_operations(ops: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for op in ops or []:
        if isinstance(op, dict):
            out.append(op)
        else:
            # best-effort fallback; keeps event JSON-safe
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
    now = datetime.utcnow().isoformat() + "Z"

    event = {
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
                "lake": "success" if execution.get("successful") else "failed",
                "vault": "success" if execution.get("successful") else "failed",
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
    return event


def build_failure_event(context: Dict[str, Any], reason: str, details: str) -> Dict[str, Any]:
    now = datetime.utcnow().isoformat() + "Z"
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

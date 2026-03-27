
from datetime import datetime
from typing import Any, Dict, List

from core.model import ChangeContext, ExecutionResult, Plan, PolicyOutcome, VerificationResult


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _serialize_operations(ops: Any) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for operation in ops or []:
        if isinstance(operation, dict):
            output.append(operation)
        else:
            output.append({"op": str(operation)})
    return output


def build_schema_evolved_event(
    context: ChangeContext,
    policy: PolicyOutcome,
    plan: Plan,
    execution: ExecutionResult,
    verification: VerificationResult,
    new_version: int,
) -> Dict[str, Any]:
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
    return build_schema_evolved_event(
        context=context,
        policy=policy,
        plan=plan,
        execution=execution,
        verification=verification,
        new_version=new_version,
    )


def build_failure_event(context: Dict[str, Any], reason: str, details: str) -> Dict[str, Any]:
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

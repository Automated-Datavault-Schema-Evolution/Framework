import datetime
from typing import Dict, Any

from core.model import ChangeContext, PolicyOutcome, ExecutionResult, VerificationResult, Plan


def build_schema_evolved_event(context: ChangeContext, policy: PolicyOutcome, plan: Plan, execution: ExecutionResult,
                               verification: VerificationResult, new_version: int) -> Dict[str, Any]:
    atoms = policy.get("atoms", [])

    event = {
        "event_type": "schema.evolved",
        "dataset": {
            "id": context["dataset_id"],
            "source": context.get("source", ""),
        },
        "correlation_id": context["correlation_id"],
        "previous_version": context.get("previous_version"),
        "new_version": new_version,
        "compatibility": policy["compatibility"],
        "change_summary": {
            "added": [],
            "removed": [],
            "renamed": [],
            "type_changed": [],
        },
        "plan": {
            "plan_id": plan["plan_id"],
            "executed_at": datetime.utcnow().isoformat() + "Z",
            "handler_results": {
                "lake": "success" if execution["successful"] else "unknown",
                "vault": "success" if execution["successful"] else "unknown",
            },
        },
        "verification": {
            "status": verification["status"],
            "checked_at": datetime.utcnow().isoformat() + "Z",
            "evidence_snapshot_refs": verification["evidence_snapshot_refs"],
            "issues": verification["issues"],
        },
        "metadata": {
            "emitted_by": "sef",
            "emitted_at": datetime.utcnow().isoformat() + "Z",
        },
    }
    return event


def build_failure_event(context: Dict[str, Any], reason: str, details: str) -> Dict[str, Any]:
    return {
        "event_type": "schema.evolution.failed",
        "dataset": {"id": context["dataset_id"]},
        "correlation_id": context.get("correlation_id"),
        "reason": reason,
        "details": details,
        "metadata": {
            "emitted_by": "sef",
            "emitted_at": datetime.utcnow().isoformat() + "Z",
        },
    }

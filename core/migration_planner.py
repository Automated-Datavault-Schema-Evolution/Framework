import hashlib
import json
from typing import Any, Dict, List
from uuid import uuid4

from core.model import ChangeAtom, Plan, PlanOperation, PolicyOutcome


def _op_idempotency_key(
        dataset_id: str,
        layer: str,
        kind: str,
        target: str,
        params: Dict[str, Any],
) -> str:
    payload = json.dumps(
        {
            "dataset_id": dataset_id,
            "layer": layer,
            "kind": kind,
            "target": target,
            "params": params,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _build_vault_structure_operations(
        dataset_id: str,
        correlation_id: str,
        plan_id: str,
        policy: PolicyOutcome,
) -> List[PlanOperation]:
    obligations = [obligation.upper() for obligation in policy.get("obligations", [])]
    operations: List[PlanOperation] = []

    if "NEW_HUB" in obligations or "CREATE_NEW_HUB" in obligations:
        params = {"table_name": dataset_id}
        operations.append(
            {
                "idempotency_key": _op_idempotency_key(dataset_id, "vault", "NEW_HUB", dataset_id, params),
                "layer": "vault",
                "kind": "NEW_HUB",
                "target": dataset_id,
                "params": params,
                "correlation_id": correlation_id,
                "plan_id": plan_id,
            }
        )

    if "NEW_LINK" in obligations or "CREATE_NEW_LINK" in obligations:
        params = {"table_name": dataset_id}
        operations.append(
            {
                "idempotency_key": _op_idempotency_key(dataset_id, "vault", "NEW_LINK", dataset_id, params),
                "layer": "vault",
                "kind": "NEW_LINK",
                "target": dataset_id,
                "params": params,
                "correlation_id": correlation_id,
                "plan_id": plan_id,
            }
        )

    return operations


def build_plan(
        dataset_id: str,
        correlation_id: str,
        atoms: List[ChangeAtom],
        policy: PolicyOutcome,
) -> Plan:
    plan_id = str(uuid4())
    operations: List[PlanOperation] = _build_vault_structure_operations(
        dataset_id=dataset_id,
        correlation_id=correlation_id,
        plan_id=plan_id,
        policy=policy,
    )

    for atom in atoms:
        attribute = atom["attribute"]

        if atom["kind"] == "ADD_COLUMN":
            for layer in ("lake", "vault"):
                params = {
                    "column_name": attribute,
                    "logical_type": atom.get("to_type"),
                }
                operations.append(
                    {
                        "idempotency_key": _op_idempotency_key(dataset_id, layer, "ADD_COLUMN", dataset_id, params),
                        "layer": layer,
                        "kind": "ADD_COLUMN",
                        "target": dataset_id,
                        "params": params,
                        "correlation_id": correlation_id,
                        "plan_id": plan_id,
                    }
                )
            continue

        if atom["kind"] == "CHANGE_TYPE":
            for layer in ("lake", "vault"):
                params = {
                    "column_name": attribute,
                    "logical_type": atom.get("to_type"),
                    "to_logical_type": atom.get("to_type"),
                    "from_logical_type": atom.get("from_type"),
                }
                operations.append(
                    {
                        "idempotency_key": _op_idempotency_key(dataset_id, layer, "CHANGE_TYPE", dataset_id, params),
                        "layer": layer,
                        "kind": "CHANGE_TYPE",
                        "target": dataset_id,
                        "params": params,
                        "correlation_id": correlation_id,
                        "plan_id": plan_id,
                    }
                )
            continue

        if atom["kind"] == "DROP_COLUMN":
            for layer in ("lake", "vault"):
                params = {
                    "column_name": attribute,
                    "logical_type": atom.get("from_type"),
                    "from_logical_type": atom.get("from_type"),
                }
                operations.append(
                    {
                        "idempotency_key": _op_idempotency_key(dataset_id, layer, "DROP_COLUMN", dataset_id, params),
                        "layer": layer,
                        "kind": "DROP_COLUMN",
                        "target": dataset_id,
                        "params": params,
                        "correlation_id": correlation_id,
                        "plan_id": plan_id,
                    }
                )

    return {
        "plan_id": plan_id,
        "dataset_id": dataset_id,
        "correlation_id": correlation_id,
        "operations": operations,
        "checkpoints": list(range(1, len(operations) + 1)),
    }

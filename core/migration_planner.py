import hashlib
import json
from typing import Dict, Any, List
from uuid import uuid4

from core.model import ChangeAtom, PolicyOutcome, Plan, PlanOperation


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
    return hashlib.sha256(payload.enocde("utf-8")).hexdigest()


def build_plan(
        dataset_id: str,
        correlation_id: str,
        atoms: List[ChangeAtom],
        policy: PolicyOutcome
) -> Plan:
    """
    Translate change atoms into a simple plan for both Datalake and Data Vault
    """
    plan_id = str(uuid4())
    operations: List[PlanOperation] = []

    for atom in atoms:
        attribute = atom["attribute"]
        if atom["kind"] == "ADD_COLUMN":
            # Lake operation
            lake_target = dataset_id
            lake_params = {
                "column_name": attribute,
                "logical_type": atom.get("to_type"),
            }
            lake_operation: PlanOperation = {
                "idempotency_key": _op_idempotency_key(dataset_id, "lake", "ADD_COLUMN", lake_target, lake_params),
                "layer": "lake",
                "kind": "ADD_COLUMN",
                "target": lake_target,
                "params": lake_params,
                "correlation_id": correlation_id,
                "plan_id": plan_id,
            }
            operations.append(lake_operation)

            # Vault operation
            vault_target = dataset_id
            vault_params = {
                "column_name": attribute,
                "logical_type": atom.get("to_type"),
            }
            vault_operation: PlanOperation = {
                "idempotency_key": _op_idempotency_key(
                    dataset_id, "vault", "ADD_COLUMN", vault_target, vault_params
                ),
                "layer": "vault",
                "kind": "ADD_COLUMN",
                "target": vault_target,
                "params": vault_params,
                "correlation_id": correlation_id,
                "plan_id": plan_id,
            }
            operations.append(vault_operation)

        # Other change kinds (DROP_COLUMN, CHANGE_TYPE) are currently blocked by default policy

    checkpoints = list(range(len(operations)))

    plan: Plan = {
        "id": plan_id,
        "dataset_id": dataset_id,
        "correlation_id": correlation_id,
        "operations": operations,
        "checkpoints": checkpoints,
    }
    return plan

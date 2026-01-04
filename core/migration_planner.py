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
    """
    Build a deterministic idempotency key for a single operation.
    """
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
    # BUGFIX: .encode, not .enocde
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _build_vault_structure_operations(
        dataset_id: str,
        correlation_id: str,
        plan_id: str,
        policy: PolicyOutcome,
) -> List[PlanOperation]:
    """
    Optionally create vault-structure operations (NEW_HUB / NEW_LINK)
    based on policy obligations.

    This function does NOT try to decide whether a hub/link must change.
    It delegates that to the vault handler, which:
      - creates the first hub/link if none exist, or
      - creates a side-by-side variant if the inferred structure would differ,
      - or treats the request as ALREADY_APPLIED if nothing needs to change.

    We only emit these operations when the policy explicitly asks for them
    via obligations, e.g.:

        policy["obligations"] = ["NEW_HUB", "NEW_LINK"]

    or

        policy["obligations"] = ["create_new_hub"]
    """
    obligations = [o.upper() for o in policy.get("obligations", [])]
    ops: List[PlanOperation] = []

    def _has(name: str) -> bool:
        return name in obligations

    # NEW HUB
    if _has("NEW_HUB") or _has("CREATE_NEW_HUB"):
        params: Dict[str, Any] = {
            "table_name": dataset_id,
        }
        ops.append(
            {
                "idempotency_key": _op_idempotency_key(
                    dataset_id, "vault", "NEW_HUB", dataset_id, params
                ),
                "layer": "vault",
                "kind": "NEW_HUB",
                "target": dataset_id,
                "params": params,
                "correlation_id": correlation_id,
                "plan_id": plan_id,
            }
        )

    # NEW LINK
    if _has("NEW_LINK") or _has("CREATE_NEW_LINK"):
        params = {
            "table_name": dataset_id,
        }
        ops.append(
            {
                "idempotency_key": _op_idempotency_key(
                    dataset_id, "vault", "NEW_LINK", dataset_id, params
                ),
                "layer": "vault",
                "kind": "NEW_LINK",
                "target": dataset_id,
                "params": params,
                "correlation_id": correlation_id,
                "plan_id": plan_id,
            }
        )

    return ops


def build_plan(
        dataset_id: str,
        correlation_id: str,
        atoms: List[ChangeAtom],
        policy: PolicyOutcome,
) -> Plan:
    """
    Translate change atoms into a simple plan for both Datalake and Data Vault.

    Behaviour:
      - Always emits lake- and vault-layer ADD_COLUMN operations for
        additive changes.
      - Optionally emits NEW_HUB / NEW_LINK vault operations based on the
        policy obligations (see _build_vault_structure_operations).
    """
    plan_id = str(uuid4())
    operations: List[PlanOperation] = []

    # 1) Optional structural operations for the vault (NEW_HUB / NEW_LINK)
    operations.extend(
        _build_vault_structure_operations(
            dataset_id=dataset_id,
            correlation_id=correlation_id,
            plan_id=plan_id,
            policy=policy,
        )
    )

    # 2) Column-level operations (ADD_COLUMN) for lake and vault
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
                "idempotency_key": _op_idempotency_key(
                    dataset_id, "lake", "ADD_COLUMN", lake_target, lake_params
                ),
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
        elif atom["kind"] == "CHANGE_TYPE":
            to_type = atom.get("to_type")

            # Lake op
            lake_params = {
                "column_name": attribute,
                "logical_type": to_type,  # back-compat key
                "to_logical_type": to_type,  # explicit
                "from_logical_type": atom.get("from_type"),
            }
            operations.append(
                {
                    "idempotency_key": _op_idempotency_key(
                        dataset_id, "lake", "CHANGE_TYPE", dataset_id, lake_params
                    ),
                    "layer": "lake",
                    "kind": "CHANGE_TYPE",
                    "target": dataset_id,
                    "params": lake_params,
                    "correlation_id": correlation_id,
                    "plan_id": plan_id,
                }
            )

            # Vault op (optional, see note below)
            vault_params = {
                "column_name": attribute,
                "logical_type": to_type,
                "to_logical_type": to_type,
                "from_logical_type": atom.get("from_type"),
            }
            operations.append(
                {
                    "idempotency_key": _op_idempotency_key(
                        dataset_id, "vault", "CHANGE_TYPE", dataset_id, vault_params
                    ),
                    "layer": "vault",
                    "kind": "CHANGE_TYPE",
                    "target": dataset_id,
                    "params": vault_params,
                    "correlation_id": correlation_id,
                    "plan_id": plan_id,
                }
            )
        # Other change kinds (DROP_COLUMN, ...) are currently
        # blocked by default policy and therefore do not generate operations.

    # Checkpoints: one after each operation (simple linear plan)
    checkpoints = list(range(len(operations)))

    plan: Plan = {
        "plan_id": plan_id,
        "dataset_id": dataset_id,
        "correlation_id": correlation_id,
        "operations": operations,
        "checkpoints": checkpoints,
    }
    return plan

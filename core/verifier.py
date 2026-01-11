from typing import Any, List

from logger import log

from core.model import Plan, ExecutionResult, VerificationResult
from handler import lake_handler_client, vault_handler_client


def verify(plan: Plan, execution: ExecutionResult, lake_stub: Any, vault_stub: Any) -> VerificationResult:
    """
    Minimal verifier:
        - checks all operations reported OK/ALREADY_APPLIED
        - performs a live introspection via gRPC for structural evidence
    """
    issues: List[str] = []
    correlation_id = plan["correlation_id"]
    plan_id = plan["plan_id"]

    if not execution["successful"]:
        issues.append("[SEF_CORE][VERIFIER] Execution did not complete successfully")

    # Live introspection for lake
    try:
        lake_evidence = lake_handler_client.introspect_evidence(lake_stub, correlation_id, plan_id, plan["dataset_id"])
        log.info(f"[SEF_CORE][VERIFIER] Lake introspection returned {len(lake_evidence['tables'])} tables")
    except Exception as e:
        issues.append(f"Lake introspection failed: {e}")
        log.error(f"[SEF_CORE][VERIFIER] Lake introspection failed: {e}")

    # Live introspection for data vault
    try:
        vault_evidence = vault_handler_client.introspect_evidence(vault_stub, correlation_id, plan_id,
                                                                  plan["dataset_id"])
        log.info(f"[SEF_CORE][VERIFIER] Vault introspection returned {len(vault_evidence.get("vault_structures", []))} "
                 f"structures")
    except Exception as e:
        issues.append(f"Vault introspection failed: {e}")
        log.error(f"[SEF_CORE][VERIFIER] Vault introspection failed: {e}")

    status = "passed" if not issues else "failed"

    evidence_refs: List[str] = []
    for result in execution["operation_results"]:
        eid = result.get("evidence_snapshot_id")
        if eid:
            evidence_refs.append(eid)

    verification: VerificationResult = {
        "plan_id": plan_id,
        "correlation_id": correlation_id,
        "status": status,
        "issues": issues,
        "evidence_snapshot_refs": evidence_refs,
    }
    return verification

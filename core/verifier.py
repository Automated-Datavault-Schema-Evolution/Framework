import os
import time
from typing import Any, List

from logger import log

from core.model import Plan, ExecutionResult, VerificationResult
from handler import lake_handler_client, vault_handler_client


def verify(plan: Plan, execution: ExecutionResult, lake_stub: Any, vault_stub: Any) -> VerificationResult:
    """
    Minimal verifier:
        - checks all operations reported OK/ALREADY_APPLIED
        - performs a live introspection via gRPC for structural evidence

    IMPORTANT:
      Vault IntrospectEvidence returns structural metadata (hub/satellites/links), not column-level details.
      Therefore, we only require presence of vault structures when vault operations exist.
    """
    issues: List[str] = []
    correlation_id = plan["correlation_id"]
    plan_id = plan["plan_id"]
    dataset_id = plan["dataset_id"]

    if not execution.get("successful", False):
        issues.append("[SEF_CORE][VERIFIER] Execution did not complete successfully")

    # If there are vault-layer operations in the plan, we require vault_structures evidence.
    ops = plan.get("operations", []) or []
    requires_vault = any(op.get("layer") == "vault" for op in ops)

    # Bounded wait: only retry if evidence is not yet visible (eventual consistency).
    max_wait_s = float(os.getenv("SEF_VERIFY_MAX_WAIT_S", "30"))
    interval_s = float(os.getenv("SEF_VERIFY_INTERVAL_S", "1.0"))
    deadline = time.monotonic() + max_wait_s

    last_lake_exc: Exception | None = None
    last_vault_exc: Exception | None = None

    last_lake_tables = 0
    last_vault_structures = 0

    while True:
        lake_evidence = None
        vault_evidence = None

        # Live introspection for lake
        try:
            lake_evidence = lake_handler_client.introspect_evidence(
                lake_stub, correlation_id, plan_id, dataset_id
            )
            last_lake_tables = len(lake_evidence.get("tables", []) or [])
            log.info(f"[SEF_CORE][VERIFIER] Lake introspection returned {last_lake_tables} tables")
        except Exception as e:
            last_lake_exc = e
            log.error(f"[SEF_CORE][VERIFIER] Lake introspection failed: {e}")

        # Live introspection for data vault
        try:
            vault_evidence = vault_handler_client.introspect_evidence(
                vault_stub, correlation_id, plan_id, dataset_id
            )
            last_vault_structures = len(vault_evidence.get("vault_structures", []) or [])
            log.info(
                f"[SEF_CORE][VERIFIER] Vault introspection returned {last_vault_structures} structures"
            )
        except Exception as e:
            last_vault_exc = e
            log.error(f"[SEF_CORE][VERIFIER] Vault introspection failed: {e}")

        # Readiness conditions:
        # - lake is "ready" if it can introspect at least one table
        # - vault is "ready" if either we don't need vault evidence OR it introspects at least one structure
        lake_ready = last_lake_tables > 0 and last_lake_exc is None
        vault_ready = (not requires_vault) or (last_vault_structures > 0 and last_vault_exc is None)

        # If we already have execution failure, do not spin forever.
        if issues:
            break

        if lake_ready and vault_ready:
            break

        if time.monotonic() >= deadline:
            # Only at the end, convert persistent missing evidence into issues.
            if not lake_ready:
                if last_lake_exc is not None:
                    issues.append(f"Lake introspection failed: {last_lake_exc}")
                else:
                    issues.append("Lake introspection returned no tables")
            if not vault_ready:
                if last_vault_exc is not None:
                    issues.append(f"Vault introspection failed: {last_vault_exc}")
                else:
                    issues.append("Vault introspection returned no vault structures")
            break

        time.sleep(interval_s)

    status = "passed" if not issues else "failed"

    evidence_refs: List[str] = []
    for result in execution.get("operation_results", []) or []:
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

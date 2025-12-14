from typing import Dict, Any, List

from logger import log

from core.model import ExecutionResult, Plan
from handler import lake_handler_client, vault_handler_client


def execute_plan(plan: Plan, lake_stub: Any, vault_stub: Any) -> ExecutionResult:
    """
    Execute operations sequentially via gRPC handerls, with naive retry
    """
    operation_results: List[Dict[str, Any]] = []
    MAX_RETIRES = 3

    for index, operation in enumerate(plan["operations"]):
        stub = lake_stub if operation["layer"] == "lake" else vault_stub

        for attempt in range(1, MAX_RETIRES + 1):
            try:
                if operation["layer"] == "lake":
                    results = lake_handler_client.apply_operations(stub, [operation])
                else:
                    results = vault_handler_client.apply_operations(stub, [operation])
                result = results[0]
                operation_results.append(result)

                status = operation["status"]
                if status in (1, 2):  # OK or ALREADY_APPLIED
                    log.info(
                        f"[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} succeeded with status {status}")
                    break
                else:
                    log.warning(f"[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} failed with status "
                                f"{status}, attempt {attempt}: {result.get("error_message")}")
            except Exception as e:
                log.error(f"[SEF_CORE][EXECUTOR] Exception occured during execution of operation "
                          f"{operation["idempotency_key"]}, attempt {attempt}: {e}")

        else:
            log.error(
                f"[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} failed after {MAX_RETIRES} attempts")
            if not operation_results or operation_results[-1]["idempotency_key"] != operation["idempotency_key"]:
                operation_results.append(
                    {
                        "correlation_id": operation["correlation_id"],
                        "plan_id": operation["plan_id"],
                        "idempotency_key": operation["idempotency_key"],
                        "status": 4,  # PERMANENT_ERROR
                        "error_code": "RETRIES_EXHAUSTED",
                        "error_message": "All retries exhausted",
                        "evidence_snapshot_id": "",
                        "evidence_snapshot_uri": "",
                    }
                )
            break
    successful = all(result["status"] in (1, 2) for result in operation_results)
    return {
        "plan_id": plan["plan_id"],
        "correlation_id": plan["correlation_id"],
        "operation_results": operation_results,
        "successful": successful,
    }

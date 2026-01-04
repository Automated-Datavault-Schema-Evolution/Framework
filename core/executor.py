from typing import Dict, Any, List, Optional

from logger import log

from core.model import ExecutionResult, Plan
from handler import lake_handler_client, vault_handler_client


def execute_plan(plan: Plan, lake_stub: Any, vault_stub: Any) -> ExecutionResult:
    """
    Execute operations sequentially via gRPC handerls, with naive retry
    """
    operation_results: List[Dict[str, Any]] = []
    MAX_RETRIES = 3

    for index, operation in enumerate(plan["operations"]):
        stub = lake_stub if operation["layer"] == "lake" else vault_stub
        final_result: Optional[Dict[str, Any]] = None
        should_abort = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if operation["layer"] == "lake":
                    results = lake_handler_client.apply_operations(stub, [operation])
                else:
                    results = vault_handler_client.apply_operations(stub, [operation])
                result = results[0]
                final_result = result

                status = final_result.get("status")
                if status in (1, 2):  # OK or ALREADY_APPLIED
                    log.info(
                        f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} '
                        f'succeeded with status {status}'
                    )
                    operation_results.append(final_result)
                    break
                if status == 4:  # PERMANENT_ERROR
                    log.error(
                        f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} failed with status '
                        f'{status}, attempt {attempt}: {final_result.get("error_message")}'
                    )
                    operation_results.append(final_result)
                    should_abort = True
                    break

                log.warning(
                    f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} failed with status '
                    f'{status}, attempt {attempt}: {final_result.get("error_message")}'
                )
            except Exception as e:
                log.error(
                    f'[SEF_CORE][EXECUTOR] Exception occured during execution of operation '
                    f'{operation["idempotency_key"]}, attempt {attempt}: {e}'
                )

            if attempt == MAX_RETRIES:
                break

        if not operation_results or operation_results[-1]["idempotency_key"] != operation["idempotency_key"]:
            if final_result is None:
                log.error(
                    f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} failed after {MAX_RETRIES} attempts'
                )
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
                should_abort = True
            else:
                log.error(
                    f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} failed after {MAX_RETRIES} attempts'
                )
                operation_results.append(final_result)
                should_abort = True

        if should_abort:
            break
    successful = all(result.get("status") in (1, 2) for result in operation_results)
    return {
        "plan_id": plan["plan_id"],
        "correlation_id": plan["correlation_id"],
        "operation_results": operation_results,
        "successful": successful,
    }

import time
from typing import Dict, Any, List, Optional

from logger import log

from config.config import BACKOFF_MAX_S, BACKOFF_INITIAL_S, BACKOFF_MULT, MAX_RETRIES
from core.model import ExecutionResult, Plan
from handler import lake_handler_client, vault_handler_client


def _backoff_sleep(attempt: int) -> None:
    # attempt is 1-based
    s = min(BACKOFF_MAX_S, BACKOFF_INITIAL_S * (BACKOFF_MULT ** max(0, attempt - 1)))
    time.sleep(s)


def execute_plan(plan: Plan, lake_stub: Any, vault_stub: Any) -> ExecutionResult:
    """
    Execute operations sequentially via gRPC handlers with transient-aware retry + backoff.

    Status codes (current convention):
      1 = OK
      2 = ALREADY_APPLIED
      3 = TRANSIENT_ERROR
      4 = PERMANENT_ERROR
    """
    operation_results: List[Dict[str, Any]] = []

    STATUS_OK = 1
    STATUS_ALREADY_APPLIED = 2
    STATUS_TRANSIENT_ERROR = 3
    STATUS_PERMANENT_ERROR = 4

    for operation in plan["operations"]:
        stub = lake_stub if operation["layer"] == "lake" else vault_stub
        final_result: Optional[Dict[str, Any]] = None
        should_abort = False
        appended = False

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if operation["layer"] == "lake":
                    results = lake_handler_client.apply_operations(stub, [operation])
                else:
                    results = vault_handler_client.apply_operations(stub, [operation])

                final_result = results[0]
                status = final_result.get("status")

                if status in (STATUS_OK, STATUS_ALREADY_APPLIED):
                    log.info(
                        f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} '
                        f'succeeded with status {status}'
                    )
                    operation_results.append(final_result)
                    appended = True
                    break

                if status == STATUS_PERMANENT_ERROR:
                    log.error(
                        f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} failed with status '
                        f'{status}, attempt {attempt}: {final_result.get("error_message")}'
                    )
                    operation_results.append(final_result)
                    appended = True
                    should_abort = True
                    break

                # Explicit transient handling
                if status == STATUS_TRANSIENT_ERROR:
                    log.warning(
                        f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} transient error '
                        f'(attempt {attempt}/{MAX_RETRIES}): {final_result.get("error_code")}: '
                        f'{final_result.get("error_message")}'
                    )
                    if attempt < MAX_RETRIES:
                        _backoff_sleep(attempt)
                        continue

                    # retries exhausted on transient -> abort
                    operation_results.append(final_result)
                    appended = True
                    should_abort = True
                    break

                # Unknown/non-OK/non-permanent: treat as retryable, but log as warning.
                log.warning(
                    f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} failed with status '
                    f'{status}, attempt {attempt}: {final_result.get("error_message")}'
                )
                if attempt < MAX_RETRIES:
                    _backoff_sleep(attempt)

            except Exception as e:
                log.error(
                    f'[SEF_CORE][EXECUTOR] Exception occurred during execution of operation '
                    f'{operation["idempotency_key"]}, attempt {attempt}: {e}'
                )
                if attempt < MAX_RETRIES:
                    _backoff_sleep(attempt)

        # If nothing was appended for this operation, convert into a deterministic terminal failure.
        if not appended:
            if final_result is None:
                log.error(
                    f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} failed after '
                    f'{MAX_RETRIES} attempts (no result)'
                )
                operation_results.append(
                    {
                        "correlation_id": operation["correlation_id"],
                        "plan_id": operation["plan_id"],
                        "idempotency_key": operation["idempotency_key"],
                        "status": STATUS_PERMANENT_ERROR,
                        "error_code": "RETRIES_EXHAUSTED",
                        "error_message": "All retries exhausted",
                        "evidence_snapshot_id": "",
                        "evidence_snapshot_uri": "",
                    }
                )
            else:
                log.error(
                    f'[SEF_CORE][EXECUTOR] Operation {operation["idempotency_key"]} failed after '
                    f'{MAX_RETRIES} attempts'
                )
                operation_results.append(final_result)
            should_abort = True

        if should_abort:
            break

    successful = all(result.get("status") in (STATUS_OK, STATUS_ALREADY_APPLIED) for result in operation_results)
    return {
        "plan_id": plan["plan_id"],
        "correlation_id": plan["correlation_id"],
        "operation_results": operation_results,
        "successful": successful,
    }

from typing import Dict, Any, List

import grpc

from config.config import LAKE_HANDLER_GRPC_TARGET, GRPC_TIMEOUT_S
from proto import sef_handlers_pb2 as pb
from proto import sef_handlers_pb2_grpc as pb_grpc


def _normalize_kind(kind: Any) -> int:
    if isinstance(kind, int):
        return kind
    if not kind:
        return pb.OPERATION_KIND_UNSPECIFIED
    normalized = str(kind).upper()
    if not normalized.startswith("OPERATION_"):
        normalized = f"OPERATION_{normalized}"
    return pb.OperationKind.Value(normalized)


def create_stub() -> pb_grpc.LakeHandlerStub:
    target = LAKE_HANDLER_GRPC_TARGET.strip()
    if not target.startswith("dns:///"):
        target = f"dns:///{target}"

    channel = grpc.insecure_channel(
        target,
        options=[
            ("grpc.dns_resolver_refresh_rate_ms", 1000),
            ("grpc.enable_retries", 1),
        ],
    )
    return pb_grpc.LakeHandlerStub(channel)


def apply_operations(stub: pb_grpc.LakeHandlerStub, operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pb_operations = []
    for operation in operations:
        pb_operations.append(
            pb.Operation(
                correlation_id=operation['correlation_id'],
                plan_id=operation['plan_id'],
                idempotency_key=operation['idempotency_key'],
                layer=pb.LAYER_LAKE,
                kind=_normalize_kind(operation['kind']),
                target=operation['target'],
                params=operation.get("params", {}),
            )
        )
    request = pb.OperationBatch(operations=pb_operations)
    response: pb.OperationBatchResult = stub.ApplyOperations(request, timeout=GRPC_TIMEOUT_S)

    results: List[Dict[str, Any]] = []
    for result in response.results:
        results.append(
            {
                "correlation_id": result.correlation_id,
                "plan_id": result.plan_id,
                "idempotency_key": result.idempotency_key,
                "status": result.status,
                "error_code": result.error_code,
                "error_message": result.error_message,
                "evidence_snapshot_id": result.evidence_snapshot_id,
                "evidence_snapshot_uri": result.evidence_snapshot_uri,
            }
        )
    return results


def introspect_evidence(stub: pb_grpc.LakeHandlerStub, correlation_id: str, plan_id: str, dataset_id: str) -> Dict[
    str, Any]:
    request = pb.EvidenceRequest(
        correlation_id=correlation_id,
        plan_id=plan_id,
        dataset_id=dataset_id
    )
    response: pb.EvidenceResponse = stub.IntrospectEvidence(request, timeout=GRPC_TIMEOUT_S)
    return {
        "correlation_id": response.correlation_id,
        "plan_id": response.plan_id,
        "tables": [
            {
                "name": table.name,
                "attributes": [
                    {
                        "name": attribute.name,
                        "logical_type": attribute.logical_type,
                        "physical_type": attribute.physical_type,
                        "nullable": attribute.nullable,
                    }
                    for attribute in table.attributes
                ],
            }
            for table in response.tables
        ],
        "raw_evidence_json": response.raw_evidence_json,
    }

from typing import Any, Dict, List

import grpc

from config.config import VAULT_HANDLER_GRPC_TARGET
from proto import sef_handlers_pb2 as pb
from proto import sef_handlers_pb2_grpc as pb_grpc


def create_stub() -> pb_grpc.VaultHandlerStub:
    channel = grpc.insecure_channel(VAULT_HANDLER_GRPC_TARGET)
    return pb_grpc.VaultHandlerStub(channel)


def apply_operations(stub: pb_grpc.VaultHandlerStub, operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pb_operations = []
    for operation in operations:
        pb_operations.append(
            pb.Operation(
                correlation_id=operation['correlation_id'],
                plan_id=operation['plan_id'],
                idempotency_key=operation['idempotency_key'],
                layer=pb.LAYER_VAULT,
                kind=operation['kind'],
                target=operation['target'],
                params=operation.get('params', {}),
            )
        )
    request = pb.OperationBatch(operations=pb_operations)
    response: pb.OperationBatchResult = stub.ApplyOperations(request)

    results: List[Dict[str, Any]] = []
    for result in response.results:
        results.append(
            {
                'correlation_id': result.correlation_id,
                'plan_id': result.plan_id,
                'idempotency_key': result.idempotency_key,
                'status': result.status,
                'error_code': result.error_code,
                'error_message': result.error_message,
                'evidence_snapshot_id': result.evidence_snapshot_id,
                'evidence_snapshot_uri': result.evidence_snapshot_uri,
            }
        )
    return results


def introspect_evidence(stub: pb_grpc.VaultHandlerStub, correlation_id: str, plan_id: str, dataset_id: str) -> Dict[
    str, Any]:
    request = pb.EvidenceRequest(
        correlation_id=correlation_id,
        plan_id=plan_id,
        dataset_id=dataset_id,
    )
    response: pb.EvidenceResponse = stub.IntrospectEvidence(request)
    return {
        "correlation_id": response.correlation_id,
        "plan_id": response.plan_id,
        "vault_structures": [
            {
                "hub": vault.hub,
                "satellites": list(vault.satellites),
                "links": list(vault.links),
            }
            for vault in response.vault_structures
        ],
        "raw_evidence_json": response.raw_evidence_json,
    }

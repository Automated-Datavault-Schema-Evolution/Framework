from typing import TypedDict, List, Dict, Optional, Literal, Any

ChangeKind = Literal[
    "ADD_COLUMN",
    "DROP_COLUMN",
    "CHANGE_TYPE",
]

Decision = Literal[
    "allow",
    "block",
    "needs_approval",
]

Compatibility = Literal[
    "backward",
    "forward",
    "breaking",
    "unknown",
]


class AttributeDescriptor(TypedDict):
    name: str
    logical_type: str
    physical_type: str
    nullable: bool


class HeaderSnapshot(TypedDict, total=False):
    attributes: List[AttributeDescriptor]
    primary_key: List[str]
    schema_fingerprint: str


class DatasetRef(TypedDict, total=False):
    id: str
    source: str
    zone: str


class SchemaNotification(TypedDict, total=False):
    event_type: str
    dataset: DatasetRef
    header: HeaderSnapshot
    ingestion: Dict[str, Any]
    previous_schema_version: Optional[int]
    metadata: Dict[str, Any]


class ChangeAtom(TypedDict, total=False):
    kind: ChangeKind
    attribute: str
    from_type: Optional[str]
    to_type: Optional[str]


class ImpactAnalysis(TypedDict, total=False):
    change_atoms: List[ChangeAtom]
    impacted_lake_tables: List[str]
    impacted_vault_objects: List[str]
    impacted_transformations: List[str]


class PolicyOutcome(TypedDict, total=False):
    decision: Decision
    compatibility: Compatibility
    obligations: List[str]
    reasons: List[str]


class PlanOperation(TypedDict, total=False):
    idempotency_key: str
    layer: Literal["lake", "vault"]
    kind: str
    target: str
    params: Dict[str, Any]
    correlation_id: str
    plan_id: str


class Plan(TypedDict, total=False):
    plan_id: str
    dataset_id: str
    correlation_id: str
    operations: List[PlanOperation]
    checkpoints: List[int]


class ExecutionResult(TypedDict, total=False):
    plan_id: str
    correlation_id: str
    operation_results: List[Dict[str, Any]]
    successful: bool


class VerificationResult(TypedDict, total=False):
    plan_id: str
    correlation_id: str
    status: Literal["passed", "failed"]
    issues: List[str]
    evidence_snapshot_refs: List[str]


class ChangeContext(TypedDict, total=False):
    dataset_id: str
    source: str
    correlation_id: str
    previous_schema: Optional[HeaderSnapshot]
    new_schema: HeaderSnapshot
    previous_version: Optional[int]
    ingestion_ts: str
    raw_notification: SchemaNotification

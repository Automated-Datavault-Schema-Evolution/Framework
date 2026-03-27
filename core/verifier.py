import time
from typing import Any, Dict, List, Optional

import grpc
from logger import log

from config.config import LAKE_TYPE, SEF_VERIFY_INTERVAL_S, SEF_VERIFY_MAX_WAIT_S, TECHNICAL_COLUMNS
from core.model import ExecutionResult, HeaderSnapshot, Plan, VerificationResult
from handler import lake_handler_client, vault_handler_client


_TECHNICAL_COLUMNS = {str(name).strip().lower() for name in TECHNICAL_COLUMNS}
_TYPE_ALIASES = {
    "text": "string",
    "varchar": "string",
    "character varying": "string",
    "bpchar": "string",
    "char": "string",
    "string": "string",
    "int": "integer",
    "int4": "integer",
    "integer": "integer",
    "bigint": "bigint",
    "int8": "bigint",
    "float": "float",
    "float8": "float",
    "double": "float",
    "double precision": "float",
    "real": "float",
    "bool": "boolean",
    "boolean": "boolean",
    "timestamp": "timestamp",
    "timestamptz": "timestamptz",
    "timestamp with time zone": "timestamptz",
    "timestamp without time zone": "timestamp",
}


_PG_IDENTIFIER_MAX = 63


def _sanitize_table_name(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    if "." in raw:
        raw = raw.split(".", 1)[-1]
    return raw.strip('"').replace('.', '_').replace('-', '_')


def _physical_identifier(value: Any) -> str:
    sanitized = _sanitize_table_name(value)
    if not sanitized:
        return ""
    if str(LAKE_TYPE).strip().lower() == "rdbms":
        return sanitized[:_PG_IDENTIFIER_MAX].lower()
    return sanitized.lower()


_IDENTIFIER_PREFIXES = (
    "hub_",
    "sat_",
    "satellite_",
    "link_",
    "lnk_",
)


def _expand_identifier_variants(value: str) -> set[str]:
    variants = {value}
    for prefix in _IDENTIFIER_PREFIXES:
        if value.startswith(prefix):
            stripped = value[len(prefix):]
            if stripped:
                variants.add(stripped)
    return {variant for variant in variants if variant}


def _normalize_identifier(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _identifier_candidates(value: Any) -> set[str]:
    normalized = _normalize_identifier(value)
    physical = _physical_identifier(value)

    seeds = {seed for seed in (normalized, physical) if seed}
    if not seeds:
        return set()

    for delimiter in ("/", "."):
        expanded = set()
        for seed in seeds:
            expanded.add(seed)
            if delimiter in seed:
                expanded.add(seed.rsplit(delimiter, 1)[-1])
        seeds = expanded

    candidates = set()
    for seed in seeds:
        candidates.update(_expand_identifier_variants(seed))
    return {candidate for candidate in candidates if candidate}


def _normalize_type(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    return _TYPE_ALIASES.get(normalized, normalized)


def _extract_effective_type(attribute: Dict[str, Any]) -> Optional[str]:
    logical = _normalize_type(attribute.get("logical_type"))
    physical = _normalize_type(attribute.get("physical_type"))
    fallback = _normalize_type(attribute.get("type"))

    if logical:
        return logical
    if physical:
        return physical
    return fallback


def _attribute_map(attributes: List[Dict[str, Any]] | None) -> Dict[str, Dict[str, Any]]:
    output: Dict[str, Dict[str, Any]] = {}
    for attribute in attributes or []:
        if not isinstance(attribute, dict):
            continue
        name = _normalize_identifier(attribute.get("name"))
        if not name or name in _TECHNICAL_COLUMNS:
            continue
        output[name] = attribute
    return output


def _select_target_table(lake_evidence: Dict[str, Any], dataset_id: str) -> Optional[Dict[str, Any]]:
    tables = lake_evidence.get("tables", []) or []
    if not tables:
        return None

    dataset_candidates = _identifier_candidates(dataset_id)
    for table in tables:
        if _identifier_candidates(table.get("name")) & dataset_candidates:
            return table

    if len(tables) == 1:
        return tables[0]
    return None


def _extract_lake_expected_type(attribute: Dict[str, Any], preference: str = "physical") -> Optional[str]:
    physical = _normalize_type(attribute.get("physical_type"))
    logical = _normalize_type(attribute.get("logical_type"))
    fallback = _normalize_type(attribute.get("type"))

    if preference == "logical":
        if logical:
            return logical
        if physical:
            return physical
        return fallback

    if physical:
        return physical
    if logical:
        return logical
    return fallback


def _extract_lake_actual_type(attribute: Dict[str, Any]) -> Optional[str]:
    physical = _normalize_type(attribute.get("physical_type"))
    logical = _normalize_type(attribute.get("logical_type"))
    fallback = _normalize_type(attribute.get("type"))

    if physical:
        return physical
    if logical:
        return logical
    return fallback


def _format_columns(column_names: List[str]) -> str:
    return ", ".join(sorted(column_names)) if column_names else "none"


def _lake_expected_type_preferences(operations: List[Dict[str, Any]] | None) -> Dict[str, str]:
    preferences: Dict[str, str] = {}
    for operation in operations or []:
        if not isinstance(operation, dict):
            continue
        if operation.get("layer") != "lake":
            continue

        params = operation.get("params", {}) or {}
        column_name = _normalize_identifier(params.get("column_name"))
        if not column_name:
            continue

        kind = str(operation.get("kind") or "").strip().upper()
        if kind == "CHANGE_TYPE":
            
            
            
            preferences[column_name] = "logical"
        elif kind == "ADD_COLUMN":
            preferences.setdefault(column_name, "physical")

    return preferences


def _verify_lake_schema(
    dataset_id: str,
    expected_schema: Optional[HeaderSnapshot],
    lake_evidence: Optional[Dict[str, Any]],
    operations: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    if not expected_schema:
        return []
    if not lake_evidence:
        return ["Lake evidence missing while verifying expected schema"]

    target_table = _select_target_table(lake_evidence, dataset_id)
    if target_table is None:
        available_tables = [str(table.get("name")) for table in lake_evidence.get("tables", []) or []]
        return [
            "Unable to identify target lake table for verification "
            f"(dataset_id={dataset_id}, available_tables={available_tables})"
        ]

    expected_attributes = _attribute_map(expected_schema.get("attributes", []) or [])
    actual_attributes = _attribute_map(target_table.get("attributes", []) or [])

    expected_names = set(expected_attributes.keys())
    actual_names = set(actual_attributes.keys())

    issues: List[str] = []
    expected_type_preferences = _lake_expected_type_preferences(operations)
    missing = sorted(expected_names - actual_names)
    unexpected = sorted(actual_names - expected_names)

    if missing:
        issues.append(f"Lake table missing expected columns: {_format_columns(missing)}")
    if unexpected:
        issues.append(f"Lake table contains unexpected columns: {_format_columns(unexpected)}")

    for name in sorted(expected_names & actual_names):
        expected = expected_attributes[name]
        actual = actual_attributes[name]

        expected_type = _extract_lake_expected_type(
            expected,
            preference=expected_type_preferences.get(name, "physical"),
        )
        actual_type = _extract_lake_actual_type(actual)
        if expected_type != actual_type:
            issues.append(
                f"Lake column '{name}' type mismatch: expected {expected_type or 'unknown'} got {actual_type or 'unknown'}"
            )

        if "nullable" in expected and bool(expected.get("nullable")) != bool(actual.get("nullable")):
            issues.append(
                f"Lake column '{name}' nullability mismatch: expected {bool(expected.get('nullable'))} "
                f"got {bool(actual.get('nullable'))}"
            )

    return issues


def _verify_vault_evidence(dataset_id: str, operations: List[Dict[str, Any]], vault_evidence: Optional[Dict[str, Any]]) -> List[str]:
    if not operations:
        return []
    if not vault_evidence:
        return ["Vault evidence missing while verifying plan execution"]

    structures = vault_evidence.get("vault_structures", []) or []
    if not structures:
        return ["Vault introspection returned no vault structures"]

    dataset_candidates = _identifier_candidates(dataset_id)
    issues: List[str] = []

    if any(op.get("kind") == "NEW_HUB" for op in operations):
        has_matching_hub = any(_identifier_candidates(structure.get("hub")) & dataset_candidates for structure in structures)
        if not has_matching_hub:
            issues.append(f"Vault evidence does not show expected hub for dataset {dataset_id}")

    if any(op.get("kind") == "NEW_LINK" for op in operations):
        has_any_link = any((structure.get("links", []) or []) for structure in structures)
        if not has_any_link:
            issues.append(f"Vault evidence does not show any links after NEW_LINK for dataset {dataset_id}")

    if not issues and any(op.get("kind") in {"ADD_COLUMN", "DROP_COLUMN", "CHANGE_TYPE"} for op in operations):
        has_related_structure = any(
            _identifier_candidates(structure.get("hub")) & dataset_candidates
            or any(dataset_candidates & _identifier_candidates(link) for link in structure.get("links", []) or [])
            or any(dataset_candidates & _identifier_candidates(satellite) for satellite in structure.get("satellites", []) or [])
            for structure in structures
        )
        if not has_related_structure:
            issues.append(f"Vault evidence does not expose a structure related to dataset {dataset_id}")

    return issues


def _is_transient_grpc_error(exc: Exception) -> bool:
    if isinstance(exc, grpc.RpcError):
        try:
            code = exc.code()
        except Exception:
            code = None

        if code in (
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        ):
            return True

    msg = str(exc).lower()
    transient_markers = [
        "statuscode.unavailable",
        "connection refused",
        "failed to connect to all addresses",
        "connect: connection refused",
        "unavailable",
        "temporarily unavailable",
    ]
    return any(marker in msg for marker in transient_markers)


def verify(
    plan: Plan,
    execution: ExecutionResult,
    lake_stub: Any,
    vault_stub: Any,
    expected_schema: Optional[HeaderSnapshot] = None,
) -> VerificationResult:
    issues: List[str] = []
    correlation_id = plan["correlation_id"]
    plan_id = plan["plan_id"]
    dataset_id = plan["dataset_id"]

    if not execution.get("successful", False):
        issues.append("[SEF_CORE][VERIFIER] Execution did not complete successfully")

    operations = plan.get("operations", []) or []
    requires_vault = any(op.get("layer") == "vault" for op in operations)

    deadline = time.monotonic() + SEF_VERIFY_MAX_WAIT_S
    interval_s = SEF_VERIFY_INTERVAL_S

    last_lake_exc: Exception | None = None
    last_vault_exc: Exception | None = None
    last_lake_tables = 0
    last_vault_structures = 0
    last_lake_evidence: Optional[Dict[str, Any]] = None
    last_vault_evidence: Optional[Dict[str, Any]] = None
    last_lake_schema_issues: List[str] = []
    last_vault_schema_issues: List[str] = []
    vault_operations = [op for op in operations if op.get("layer") == "vault"]

    while True:
        try:
            lake_evidence = lake_handler_client.introspect_evidence(
                lake_stub,
                correlation_id,
                plan_id,
                dataset_id,
            )
            last_lake_evidence = lake_evidence
            last_lake_tables = len(lake_evidence.get("tables", []) or [])
            last_lake_exc = None
            last_lake_schema_issues = _verify_lake_schema(
                dataset_id,
                expected_schema,
                lake_evidence,
                operations=operations,
            )
            log.info(f"[SEF_CORE][VERIFIER] Lake introspection returned {last_lake_tables} tables")
        except Exception as exc:
            last_lake_exc = exc
            last_lake_schema_issues = []
            if _is_transient_grpc_error(exc):
                log.warning(f"[SEF_CORE][VERIFIER] Lake introspection transient error; will retry: {exc}")
                try:
                    lake_stub = lake_handler_client.create_stub()
                except Exception as stub_exc:
                    log.warning(f"[SEF_CORE][VERIFIER] Lake stub recreation failed: {stub_exc}")
            else:
                log.error(f"[SEF_CORE][VERIFIER] Lake introspection failed: {exc}")

        try:
            vault_evidence = vault_handler_client.introspect_evidence(
                vault_stub,
                correlation_id,
                plan_id,
                dataset_id,
            )
            last_vault_evidence = vault_evidence
            last_vault_structures = len(vault_evidence.get("vault_structures", []) or [])
            last_vault_exc = None
            last_vault_schema_issues = _verify_vault_evidence(dataset_id, vault_operations, vault_evidence)
            log.info(f"[SEF_CORE][VERIFIER] Vault introspection returned {last_vault_structures} structures")
        except Exception as exc:
            last_vault_exc = exc
            last_vault_schema_issues = []
            if _is_transient_grpc_error(exc):
                log.warning(f"[SEF_CORE][VERIFIER] Vault introspection transient error; will retry: {exc}")
                try:
                    vault_stub = vault_handler_client.create_stub()
                except Exception as stub_exc:
                    log.warning(f"[SEF_CORE][VERIFIER] Vault stub recreation failed: {stub_exc}")
            else:
                log.error(f"[SEF_CORE][VERIFIER] Vault introspection failed: {exc}")

        lake_ready = last_lake_tables > 0 and last_lake_exc is None and not last_lake_schema_issues
        vault_ready = (not requires_vault) or (
            last_vault_structures > 0 and last_vault_exc is None and not last_vault_schema_issues
        )

        if issues or (lake_ready and vault_ready):
            break

        if time.monotonic() >= deadline:
            if not lake_ready:
                if last_lake_exc is not None:
                    issues.append(f"Lake introspection failed: {last_lake_exc}")
                else:
                    issues.extend(last_lake_schema_issues or ["Lake introspection returned no tables"])
            if not vault_ready:
                if last_vault_exc is not None:
                    issues.append(f"Vault introspection failed: {last_vault_exc}")
                else:
                    issues.extend(last_vault_schema_issues or ["Vault introspection returned no vault structures"])
            break

        time.sleep(interval_s)

    evidence_refs = [
        result.get("evidence_snapshot_id")
        for result in execution.get("operation_results", []) or []
        if result.get("evidence_snapshot_id")
    ]

    return {
        "plan_id": plan_id,
        "correlation_id": correlation_id,
        "status": "passed" if not issues else "failed",
        "issues": issues,
        "evidence_snapshot_refs": evidence_refs,
    }

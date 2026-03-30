
from collections import Counter
from typing import Any, Dict, Iterable, Optional, Tuple


def attributes_list(header: Any) -> Optional[list]:
    if isinstance(header, dict):
        attributes = header.get("attributes")
        if isinstance(attributes, list):
            return attributes
    return None


def attribute_names(header: Any) -> list[str]:
    names: list[str] = []
    for attribute in attributes_list(header) or []:
        if isinstance(attribute, dict) and attribute.get("name"):
            names.append(str(attribute.get("name")))
    return names


def normalize_previous_header(latest: Optional[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], str]:
    if not latest or not isinstance(latest, dict):
        return None, "no_latest"

    header = latest.get("header")
    if isinstance(header, dict) and isinstance(header.get("attributes"), list):
        return header, "direct"
    if isinstance(header, dict) and isinstance(header.get("header"), dict) and isinstance(header["header"].get("attributes"), list):
        return header["header"], "wrapped_header"
    if isinstance(latest.get("attributes"), list):
        return latest, "latest_is_header"
    return None, "invalid_shape"


def resolve_previous_version(previous_version: Any, current_version: Optional[int]) -> Optional[int]:
    return current_version if previous_version is None else previous_version


def summarize_operations(operations: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts = Counter((operation.get("layer"), operation.get("kind")) for operation in operations if isinstance(operation, dict))
    return {f"{layer}::{kind}": count for (layer, kind), count in counts.items()}


def new_link_operations(operations: Iterable[Dict[str, Any]], dataset_id: str) -> list[Dict[str, Any]]:
    return [
        operation
        for operation in operations
        if operation.get("layer") == "vault"
        and operation.get("kind") == "NEW_LINK"
        and operation.get("target") == dataset_id
    ]


def drop_operations(
    operations: Iterable[Dict[str, Any]],
    operations_to_drop: Iterable[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    blocked = {id(operation) for operation in operations_to_drop}
    return [operation for operation in operations if id(operation) not in blocked]

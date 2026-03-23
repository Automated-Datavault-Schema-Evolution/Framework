"""Pure state normalization helpers for schema notifications and plans."""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, Optional, Tuple



def attributes_list(header: Any) -> Optional[list]:
    """Return the ``attributes`` array when the header has the expected shape."""
    if isinstance(header, dict):
        attrs = header.get("attributes")
        if isinstance(attrs, list):
            return attrs
    return None



def attribute_names(header: Any) -> list[str]:
    """Return attribute names from a header snapshot."""
    names: list[str] = []
    for attribute in attributes_list(header) or []:
        if isinstance(attribute, dict) and attribute.get("name"):
            names.append(str(attribute.get("name")))
    return names



def normalize_previous_header(latest: Optional[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], str]:
    """Normalize stored metadata into a direct header document when possible."""
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
    """Prefer the producer-supplied previous version and fall back to the metastore version."""
    return current_version if previous_version is None else previous_version



def summarize_operations(operations: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    """Return a compact count keyed by ``layer::kind``."""
    counts = Counter((op.get("layer"), op.get("kind")) for op in operations if isinstance(op, dict))
    return {f"{layer}::{kind}": count for (layer, kind), count in counts.items()}



def new_link_operations(operations: Iterable[Dict[str, Any]], dataset_id: str) -> list[Dict[str, Any]]:
    """Return NEW_LINK vault operations that target the current dataset."""
    return [
        op
        for op in operations
        if op.get("layer") == "vault" and op.get("kind") == "NEW_LINK" and op.get("target") == dataset_id
    ]



def drop_operations(operations: Iterable[Dict[str, Any]], operations_to_drop: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """Return a new operation list without the rejected operations."""
    blocked = {id(op) for op in operations_to_drop}
    return [op for op in operations if id(op) not in blocked]

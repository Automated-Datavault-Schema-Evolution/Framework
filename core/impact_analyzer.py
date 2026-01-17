from __future__ import annotations

from typing import List, Optional, Dict, Any

from core.model import HeaderSnapshot, ChangeAtom, ImpactAnalysis
from helper import lineage_helper

# --- type normalization -------------------------------------------------------

_TYPE_ALIASES = {
    # strings
    "text": "string",
    "varchar": "string",
    "character varying": "string",
    "bpchar": "string",
    "char": "string",
    "string": "string",

    # ints
    "int": "integer",
    "int4": "integer",
    "integer": "integer",
    "bigint": "bigint",
    "int8": "bigint",

    # floats
    "float": "float",
    "float8": "float",
    "double": "float",
    "double precision": "float",
    "real": "float",

    # bool
    "bool": "boolean",
    "boolean": "boolean",

    # timestamps (keep as-is but normalized)
    "timestamp": "timestamp",
    "timestamptz": "timestamptz",
    "timestamp with time zone": "timestamptz",
    "timestamp without time zone": "timestamp",
}


def _norm_type(t: Any) -> Optional[str]:
    if t is None:
        return None
    s = str(t).strip().lower()
    if not s:
        return None
    return _TYPE_ALIASES.get(s, s)


def _extract_effective_type(attr: Dict[str, Any]) -> Optional[str]:
    """
    Effective type used for schema diff.

    Key behavior for CSV:
      - physical_type is typically "string" for all columns (placeholder),
        while logical_type carries the inferred type. We must diff on logical_type
        to detect widening (e.g., integer -> float).
    """
    logical = attr.get("logical_type")
    physical = attr.get("physical_type")
    legacy = attr.get("type")  # some producers use "type"

    logical_n = _norm_type(logical)
    physical_n = _norm_type(physical)
    legacy_n = _norm_type(legacy)

    # If logical_type exists, trust it (especially when physical is a CSV placeholder).
    if logical_n:
        return logical_n

    # Otherwise fall back to physical/type.
    if physical_n:
        return physical_n
    if legacy_n:
        return legacy_n

    return None


# --- diff + impact ------------------------------------------------------------

def diff_headers(prev: Optional[HeaderSnapshot], new: HeaderSnapshot) -> List[ChangeAtom]:
    """
    Schema diff: add/drop/change_type by column name.

    NOTE:
      - Uses logical_type preferentially so CSV-driven logical widening is detected.
    """
    new_attrs = {a["name"]: a for a in (new.get("attributes", []) or [])}
    prev_attrs = {a["name"]: a for a in (prev.get("attributes", []) or [])} if prev else {}

    atoms: List[ChangeAtom] = []

    for name, attr in new_attrs.items():
        if name not in prev_attrs:
            atoms.append(
                {
                    "kind": "ADD_COLUMN",
                    "attribute": name,
                    "from_type": None,
                    "to_type": _extract_effective_type(attr),
                }
            )
        else:
            old = prev_attrs[name]
            old_type = _extract_effective_type(old)
            new_type = _extract_effective_type(attr)

            # Only emit CHANGE_TYPE when both sides are known and differ.
            if old_type and new_type and old_type != new_type:
                atoms.append(
                    {
                        "kind": "CHANGE_TYPE",
                        "attribute": name,
                        "from_type": old_type,
                        "to_type": new_type,
                    }
                )

    for name, attr in prev_attrs.items():
        if name not in new_attrs:
            atoms.append(
                {
                    "kind": "DROP_COLUMN",
                    "attribute": name,
                    "from_type": _extract_effective_type(attr),
                    "to_type": None,
                }
            )

    return atoms


def analyze_impact(dataset_id: str, atoms: List[ChangeAtom]) -> ImpactAnalysis:
    changed_attributes = [a["attribute"] for a in atoms]
    lineage = lineage_helper.get_impacted_artifacts(dataset_id, changed_attributes)

    impact: ImpactAnalysis = {
        "change_atoms": atoms,
        "impacted_lake_tables": lineage.get("lake_tables", []),
        "impacted_vault_objects": lineage.get("vault_objects", []),
        "impacted_transformations": lineage.get("downstream_transformations", []),
    }
    return impact

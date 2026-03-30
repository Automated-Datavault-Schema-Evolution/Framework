from typing import Any, Dict, List, Optional

from core.model import ChangeAtom, HeaderSnapshot, ImpactAnalysis
from helper import lineage_helper


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


def _norm_type(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    return _TYPE_ALIASES.get(normalized, normalized)


def _extract_effective_type(attribute: Dict[str, Any]) -> Optional[str]:
    logical = _norm_type(attribute.get("logical_type"))
    physical = _norm_type(attribute.get("physical_type"))
    fallback = _norm_type(attribute.get("type"))

    if logical:
        return logical
    if physical:
        return physical
    return fallback


def diff_headers(prev: Optional[HeaderSnapshot], new: HeaderSnapshot) -> List[ChangeAtom]:
    new_attrs = {attribute["name"]: attribute for attribute in (new.get("attributes", []) or [])}
    prev_attrs = {attribute["name"]: attribute for attribute in (prev.get("attributes", []) or [])} if prev else {}

    atoms: List[ChangeAtom] = []
    for name, attribute in new_attrs.items():
        if name not in prev_attrs:
            atoms.append(
                {
                    "kind": "ADD_COLUMN",
                    "attribute": name,
                    "from_type": None,
                    "to_type": _extract_effective_type(attribute),
                }
            )
            continue

        old_type = _extract_effective_type(prev_attrs[name])
        new_type = _extract_effective_type(attribute)
        if old_type and new_type and old_type != new_type:
            atoms.append(
                {
                    "kind": "CHANGE_TYPE",
                    "attribute": name,
                    "from_type": old_type,
                    "to_type": new_type,
                }
            )

    for name, attribute in prev_attrs.items():
        if name not in new_attrs:
            atoms.append(
                {
                    "kind": "DROP_COLUMN",
                    "attribute": name,
                    "from_type": _extract_effective_type(attribute),
                    "to_type": None,
                }
            )

    return atoms


def analyze_impact(dataset_id: str, atoms: List[ChangeAtom]) -> ImpactAnalysis:
    changed_attributes = [atom["attribute"] for atom in atoms]
    lineage = lineage_helper.get_impacted_artifacts(dataset_id, changed_attributes)
    return {
        "change_atoms": atoms,
        "impacted_lake_tables": lineage.get("lake_tables", []),
        "impacted_vault_objects": lineage.get("vault_objects", []),
        "impacted_transformations": lineage.get("downstream_transformations", []),
    }

from typing import List, Optional

from core.model import (
    HeaderSnapshot,
    ChangeAtom,
    ImpactAnalysis,
)
from helper import lineage_helper


def _canonical_type(attr: dict) -> Optional[str]:
    """
    Canonicalize attribute types for comparison.

    For CSV-derived schemas we prefer physical/storage types over inferred logical types to avoid
    false CHANGE_TYPE detection (e.g., lake reports Postgres 'text' while filewatcher infers 'integer'
    but physical_type remains 'string').
    """
    if not isinstance(attr, dict):
        return None

    t = attr.get("physical_type") or attr.get("logical_type") or attr.get("type")
    if t is None:
        return None

    s = str(t).strip().lower()

    # normalize common synonyms
    if s in ("text", "varchar", "char", "character varying", "string", "str"):
        return "string"
    if s in ("int", "integer", "bigint", "smallint", "long"):
        return "integer"
    if s in ("float", "double", "decimal", "numeric", "number"):
        return "number"
    if s in ("bool", "boolean"):
        return "boolean"

    return s


def diff_headers(
        prev: Optional[HeaderSnapshot],
        new: HeaderSnapshot,
) -> List[ChangeAtom]:
    """
    Very simple schema diff: add/drop/change_type by name.

    IMPORTANT:
    - Use canonicalized types (preferring physical/storage types) to avoid spurious CHANGE_TYPE
      in CSV/RDBMS-backed lake setups.
    """
    new_attrs = {a["name"]: a for a in new.get("attributes", [])}
    prev_attrs = {a["name"]: a for a in prev.get("attributes", [])} if prev else {}

    atoms: List[ChangeAtom] = []

    for name, attr in new_attrs.items():
        if name not in prev_attrs:
            atoms.append(
                {
                    "kind": "ADD_COLUMN",
                    "attribute": name,
                    "from_type": None,
                    "to_type": _canonical_type(attr),
                }
            )
        else:
            old = prev_attrs[name]
            old_type = _canonical_type(old)
            new_type = _canonical_type(attr)

            # If one side has no type, don't manufacture a change.
            if old_type is None or new_type is None:
                continue

            if old_type != new_type:
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
                    "from_type": _canonical_type(attr),
                    "to_type": None,
                }
            )

    return atoms


def analyze_impact(
        dataset_id: str,
        atoms: List[ChangeAtom],
) -> ImpactAnalysis:
    changed_attributes = [a["attribute"] for a in atoms]
    lineage = lineage_helper.get_impacted_artifacts(dataset_id, changed_attributes)

    impact: ImpactAnalysis = {
        "change_atoms": atoms,
        "impacted_lake_tables": lineage.get("lake_tables", []),
        "impacted_vault_objects": lineage.get("vault_objects", []),
        "impacted_transformations": lineage.get("downstream_transformations", []),
    }
    return impact

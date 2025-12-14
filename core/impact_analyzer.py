from typing import List, Optional

from core.model import (
    HeaderSnapshot,
    ChangeAtom,
    ImpactAnalysis,
)
from helper import lineage_helper


def diff_headers(
        prev: Optional[HeaderSnapshot],
        new: HeaderSnapshot,
) -> List[ChangeAtom]:
    """
    Very simple schema diff: add/drop/change_type by name.
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
                    "to_type": attr.get("logical_type"),
                }
            )
        else:
            old = prev_attrs[name]
            old_type = old.get("logical_type")
            new_type = attr.get("logical_type")
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
                    "from_type": attr.get("logical_type"),
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

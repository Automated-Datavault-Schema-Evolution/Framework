import os
from functools import lru_cache
from typing import Any, Dict, List, Tuple

import yaml

from config.config import SEF_POLICY_CONFIG
from core.model import ChangeAtom, PolicyOutcome


def _normalize_logical_type(type_name: str | None) -> str:
    if not type_name:
        return "string"
    value = str(type_name).strip().lower()
    if "bool" in value:
        return "boolean"
    if "bigint" in value or "long" in value:
        return "bigint"
    if "int" in value:
        return "integer"
    if "double" in value or "float" in value or "real" in value:
        return "float"
    if "decimal" in value or "numeric" in value:
        return "numeric"
    if "timestamp" in value or "datetime" in value:
        return "timestamp"
    if value == "date" or ("date" in value and "time" not in value):
        return "date"
    return "string"


def _is_widening_change(from_type: str | None, to_type: str | None) -> bool:
    from_normalized = _normalize_logical_type(from_type)
    to_normalized = _normalize_logical_type(to_type)

    if from_normalized == to_normalized:
        return True

    widening = {
        "boolean": {"string"},
        "integer": {"bigint", "float", "numeric", "string"},
        "bigint": {"float", "numeric", "string"},
        "float": {"numeric", "string"},
        "numeric": {"string"},
        "date": {"timestamp", "string"},
        "timestamp": {"string"},
        "string": set(),
    }
    return to_normalized in widening.get(from_normalized, set())


def _default_policies() -> Dict[str, Dict[str, Any]]:
    production = {
        "allow_drop": False,
        "allow_change_type": False,
        "obligations_on_additive": ["NEW_HUB", "NEW_LINK"],
        "compatibility_for_drop": "breaking",
        "compatibility_for_change_type": "breaking",
        "impact_mode": "breaking_only",
        "max_impacted_total": 0,
    }
    sandbox = {
        **production,
        "allow_drop": True,
        "allow_change_type": True,
        "impact_mode": "off",
        "max_impacted_total": None,
    }
    return {
        "production": production,
        "sandbox": sandbox,
        "default": production,
    }


def _impact_counts(impact: Dict[str, Any]) -> Dict[str, int]:
    return {
        "lake_tables": len(impact.get("impacted_lake_tables", []) or []),
        "vault_objects": len(impact.get("impacted_vault_objects", []) or []),
        "transformations": len(impact.get("impacted_transformations", []) or []),
    }


def _coerce_optional_int(value: Any) -> int | None:
    if value is None or value == "" or value is False:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _evaluate_impact_guards(
    atoms: List[ChangeAtom],
    impact: Dict[str, Any],
    policy_cfg: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    mode = str(policy_cfg.get("impact_mode", "off") or "off").strip().lower()
    if mode in {"", "off", "disabled", "none"}:
        return False, []

    only_additive = bool(atoms) and all(atom.get("kind") == "ADD_COLUMN" for atom in atoms)
    if mode == "breaking_only" and only_additive:
        return False, []

    counts = _impact_counts(impact)
    total = sum(counts.values())
    if total <= 0:
        return False, []

    reasons: List[str] = []
    max_total = _coerce_optional_int(policy_cfg.get("max_impacted_total"))
    max_lake_tables = _coerce_optional_int(policy_cfg.get("max_impacted_lake_tables"))
    max_vault_objects = _coerce_optional_int(policy_cfg.get("max_impacted_vault_objects"))
    max_transformations = _coerce_optional_int(policy_cfg.get("max_impacted_transformations"))

    if max_total is not None and total > max_total:
        reasons.append(f"Impacted artifact count {total} exceeds policy limit {max_total}")
    if max_lake_tables is not None and counts["lake_tables"] > max_lake_tables:
        reasons.append(
            f"Impacted lake tables {counts['lake_tables']} exceed policy limit {max_lake_tables}"
        )
    if max_vault_objects is not None and counts["vault_objects"] > max_vault_objects:
        reasons.append(
            f"Impacted vault objects {counts['vault_objects']} exceed policy limit {max_vault_objects}"
        )
    if max_transformations is not None and counts["transformations"] > max_transformations:
        reasons.append(
            f"Impacted downstream transformations {counts['transformations']} exceed policy limit {max_transformations}"
        )

    if not reasons and bool(policy_cfg.get("block_on_any_impact", False)):
        reasons.append(
            "Policy blocks changes with downstream impact "
            f"(lake_tables={counts['lake_tables']}, vault_objects={counts['vault_objects']}, "
            f"transformations={counts['transformations']})"
        )

    return bool(reasons), reasons


@lru_cache(maxsize=1)
def _load_policy_file() -> Dict[str, Dict[str, Any]]:
    repo_root = os.path.dirname(os.path.dirname(__file__))
    default_path = os.path.join(repo_root, "config", "policies.yaml")
    path = SEF_POLICY_CONFIG or default_path

    if not os.path.exists(path):
        return _default_policies()

    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return _default_policies()


def _get_policy_cfg(policy_name: str) -> Dict[str, Any]:
    policies = _load_policy_file()
    if policy_name in policies:
        return policies[policy_name]
    if "default" in policies:
        return policies["default"]
    return next(iter(policies.values()))


def evaluate_policies(
    atoms: List[ChangeAtom],
    impact: Dict[str, Any],
    policy_name: str = "default",
) -> PolicyOutcome:
    policy_cfg = _get_policy_cfg(policy_name)

    if not atoms:
        return {
            "decision": "allow",
            "compatibility": "backward",
            "obligations": [],
            "reasons": ["No structural changes detected"],
            "policy_name": policy_name,
        }

    has_drop = any(atom.get("kind") == "DROP_COLUMN" for atom in atoms)
    has_change = any(atom.get("kind") == "CHANGE_TYPE" for atom in atoms)
    only_additive = all(atom.get("kind") == "ADD_COLUMN" for atom in atoms)

    allow_drop = bool(policy_cfg.get("allow_drop", False))
    allow_change_raw = policy_cfg.get("allow_change_type", False)
    allow_change_any = False
    allow_change_widen_only = False

    if isinstance(allow_change_raw, bool):
        allow_change_any = allow_change_raw
    else:
        mode = str(allow_change_raw).strip().lower()
        allow_change_any = mode in {"1", "true", "yes", "on"}
        allow_change_widen_only = mode in {"widen_only", "widen", "widening"}

    change_atoms = [atom for atom in atoms if atom.get("kind") == "CHANGE_TYPE"]
    widening_only = bool(change_atoms) and all(
        _is_widening_change(atom.get("from_type"), atom.get("to_type")) for atom in change_atoms
    )

    if has_drop and not allow_drop:
        return {
            "decision": "block",
            "compatibility": "breaking",
            "obligations": [],
            "reasons": ["DROP_COLUMN not allowed by policy"],
            "policy_name": policy_name,
        }

    if has_change and not (allow_change_any or allow_change_widen_only):
        return {
            "decision": "block",
            "compatibility": "breaking",
            "obligations": [],
            "reasons": ["CHANGE_TYPE not allowed by policy"],
            "policy_name": policy_name,
        }

    if has_change and allow_change_widen_only and not widening_only:
        details = ", ".join(
            f"{atom.get('attribute')}:{atom.get('from_type')}->{atom.get('to_type')}"
            for atom in change_atoms
            if not _is_widening_change(atom.get("from_type"), atom.get("to_type"))
        )
        return {
            "decision": "block",
            "compatibility": "breaking",
            "obligations": [],
            "reasons": [f"Non-widening CHANGE_TYPE blocked by policy: {details}"],
            "policy_name": policy_name,
        }

    impact_blocked, impact_reasons = _evaluate_impact_guards(
        atoms=atoms,
        impact=impact,
        policy_cfg=policy_cfg,
    )
    if impact_blocked:
        return {
            "decision": "block",
            "compatibility": "breaking",
            "obligations": [],
            "reasons": impact_reasons,
            "policy_name": policy_name,
        }

    if only_additive:
        return {
            "decision": "allow",
            "compatibility": "backward",
            "obligations": policy_cfg.get("obligations_on_additive", []),
            "reasons": ["Only additive changes detected"],
            "policy_name": policy_name,
        }

    compatibility = "breaking"
    reasons: List[str] = ["Destructive changes allowed by policy"]

    if has_drop:
        compatibility = policy_cfg.get("compatibility_for_drop", compatibility)

    if has_change:
        if widening_only:
            compatibility = policy_cfg.get("compatibility_for_change_type_widening", "backward")
            reasons = ["Only widening CHANGE_TYPE detected"]
        else:
            compatibility = policy_cfg.get("compatibility_for_change_type", compatibility)

    return {
        "decision": "allow",
        "compatibility": compatibility,
        "obligations": [],
        "reasons": reasons,
        "policy_name": policy_name,
    }

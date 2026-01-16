import os
from functools import lru_cache
from typing import Dict, Any, List

import yaml  # make sure PyYAML is in requirements.txt

from core.model import ChangeAtom, PolicyOutcome


def _normalize_logical_type(t: str | None) -> str:
    if not t:
        return "string"
    s = str(t).strip().lower()
    if "bool" in s:
        return "boolean"
    if "bigint" in s or "long" in s:
        return "bigint"
    if "int" in s:
        return "integer"
    if "double" in s or "float" in s or "real" in s:
        return "float"
    if "decimal" in s or "numeric" in s:
        return "numeric"
    if "timestamp" in s or "datetime" in s:
        return "timestamp"
    if s == "date" or ("date" in s and "time" not in s):
        return "date"
    return "string"


def _is_widening_change(from_type: str | None, to_type: str | None) -> bool:
    f = _normalize_logical_type(from_type)
    t = _normalize_logical_type(to_type)

    if f == t:
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
    return t in widening.get(f, set())


def _default_policies() -> Dict[str, Dict[str, Any]]:
    """
    Built-in fallback policy set used when we cannot load a YAML file.
    Mirrors production/sandbox/default semantics.
    """
    production = {
        "allow_drop": False,
        "allow_change_type": False,
        "obligations_on_additive": ["NEW_HUB", "NEW_LINK"],
        "compatibility_for_drop": "breaking",
        "compatibility_for_change_type": "breaking",
    }
    sandbox = {
        **production,
        "allow_drop": True,
        "allow_change_type": True,
    }
    return {
        "production": production,
        "sandbox": sandbox,
        "default": production,
    }


@lru_cache(maxsize=1)
def _load_policy_file() -> Dict[str, Dict[str, Any]]:
    """
    Load policies from a YAML file.

    File path:
      - SEF_POLICY_CONFIG env var, if set
      - otherwise: <repo_root>/config/policies.yaml

    Returns: mapping policy_name -> config dict.
    """
    repo_root = os.path.dirname(os.path.dirname(__file__))
    default_path = os.path.join(repo_root, "config", "policies.yaml")
    path = os.getenv("SEF_POLICY_CONFIG", default_path)

    if not os.path.exists(path):
        return _default_policies()

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if not isinstance(data, dict):
            return _default_policies()
        return data
    except Exception:
        return _default_policies()


def _get_policy_cfg(policy_name: str) -> Dict[str, Any]:
    """
    Return the configuration dict for the given policy name,
    falling back to 'default'.
    """
    policies = _load_policy_file()

    if policy_name in policies:
        return policies[policy_name]
    if "default" in policies:
        return policies["default"]
    # Defensive fallback: return any one policy
    return next(iter(policies.values()))


def evaluate_policies(
        atoms: List[ChangeAtom],
        impact: Dict[str, Any],
        policy_name: str = "default",
) -> PolicyOutcome:
    """
    Evaluate schema changes against a named policy.

    Key improvement:
      - If CHANGE_TYPE changes are widening only, classify as backward-compatible
        (unless policy blocks CHANGE_TYPE entirely).
    """
    policy_cfg = _get_policy_cfg(policy_name)

    # No changes at all
    if not atoms:
        return {
            "decision": "allow",
            "compatibility": "backward",
            "obligations": [],
            "reasons": ["No structural changes detected"],
            "policy_name": policy_name,
        }

    has_drop = any(a.get("kind") == "DROP_COLUMN" for a in atoms)
    has_change = any(a.get("kind") == "CHANGE_TYPE" for a in atoms)
    only_additive = all(a.get("kind") == "ADD_COLUMN" for a in atoms)

    allow_drop = bool(policy_cfg.get("allow_drop", False))

    # allow_change_type may be bool or a string mode ("widen_only")
    allow_change_raw = policy_cfg.get("allow_change_type", False)
    allow_change_any = False
    allow_change_widen_only = False
    if isinstance(allow_change_raw, bool):
        allow_change_any = allow_change_raw
    else:
        mode = str(allow_change_raw).strip().lower()
        allow_change_any = mode in {"1", "true", "yes", "on"}
        allow_change_widen_only = mode in {"widen_only", "widen", "widening"}

    change_atoms = [a for a in atoms if a.get("kind") == "CHANGE_TYPE"]
    widening_only = bool(change_atoms) and all(
        _is_widening_change(a.get("from_type"), a.get("to_type")) for a in change_atoms
    )

    # DROP_COLUMN handling
    if has_drop and not allow_drop:
        return {
            "decision": "block",
            "compatibility": "breaking",
            "obligations": [],
            "reasons": ["DROP_COLUMN not allowed by policy"],
            "policy_name": policy_name,
        }

    # CHANGE_TYPE handling
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
            f"{a.get('attribute')}:{a.get('from_type')}->{a.get('to_type')}" for a in change_atoms
            if not _is_widening_change(a.get("from_type"), a.get("to_type"))
        )
        return {
            "decision": "block",
            "compatibility": "breaking",
            "obligations": [],
            "reasons": [f"Non-widening CHANGE_TYPE blocked by policy: {details}"],
            "policy_name": policy_name,
        }

    # If only additive changes: allow + obligations
    if only_additive:
        obligations: List[str] = policy_cfg.get("obligations_on_additive", [])
        return {
            "decision": "allow",
            "compatibility": "backward",
            "obligations": obligations,
            "reasons": ["Only additive changes detected"],
            "policy_name": policy_name,
        }

    # Otherwise: allowed but compatibility depends on what happened
    compatibility = "breaking"
    reasons: List[str] = ["Destructive changes allowed by policy"]

    if has_drop:
        compatibility = policy_cfg.get("compatibility_for_drop", compatibility)

    if has_change:
        # IMPORTANT: widening-only type changes are backward compatible
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

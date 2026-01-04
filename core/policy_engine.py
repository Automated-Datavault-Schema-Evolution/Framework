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

    Policy configuration (per policy_name):

        allow_drop: bool
        allow_change_type: bool
        obligations_on_additive: List[str]
        compatibility_for_drop: "backward" | "breaking" | ...
        compatibility_for_change_type: "backward" | "breaking" | ...

    This preserves your original behaviour for 'production' / 'default':
      - DROP_COLUMN / CHANGE_TYPE -> block
      - ADD_COLUMN only -> allow + obligations NEW_HUB / NEW_LINK
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

    has_drop = any(a["kind"] == "DROP_COLUMN" for a in atoms)
    has_change = any(a["kind"] == "CHANGE_TYPE" for a in atoms)

    allow_drop = bool(policy_cfg.get("allow_drop", False))
    allow_change_type = bool(policy_cfg.get("allow_change_type", False))

    allow_change_raw = policy_cfg.get("allow_change_type", False)
    allow_change_mode = str(allow_change_raw).strip().lower()
    allow_change_any = allow_change_mode in {"1", "true", "yes", "on"}
    allow_change_widen_only = allow_change_mode in {"widen_only", "widen", "widening"}

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

    if has_change and allow_change_widen_only:
        non_widening = [
            a for a in atoms
            if a["kind"] == "CHANGE_TYPE" and not _is_widening_change(a.get("from_type"), a.get("to_type"))
        ]
        if non_widening:
            details = ", ".join(
                f"{a.get('attribute')}:{a.get('from_type')}->{a.get('to_type')}" for a in non_widening
            )
            return {
                "decision": "block",
                "compatibility": "breaking",
                "obligations": [],
                "reasons": [f"Non-widening CHANGE_TYPE blocked by policy: {details}"],
                "policy_name": policy_name,
            }

    # If destructive changes are allowed, we let them through but mark as breaking.
    if has_drop or has_change:
        compatibility = "breaking"
        if has_drop:
            compatibility = policy_cfg.get(
                "compatibility_for_drop",
                compatibility,
            )
        if has_change:
            if allow_change_widen_only:
                compatibility = policy_cfg.get("compatibility_for_change_type_widening", "backward")
            else:
                compatibility = policy_cfg.get("compatibility_for_change_type", compatibility)

        return {
            "decision": "allow",
            "compatibility": compatibility,
            "obligations": [],
            "reasons": ["Destructive changes allowed by policy"],
            "policy_name": policy_name,
        }

    # Only additive changes (ADD_COLUMN etc.)
    obligations: List[str] = policy_cfg.get("obligations_on_additive", [])
    return {
        "decision": "allow",
        "compatibility": "backward",
        "obligations": obligations,
        "reasons": ["Only additive changes detected"],
        "policy_name": policy_name,
    }

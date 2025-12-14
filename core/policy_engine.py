from typing import Dict, Any, List

from core.model import ChangeAtom, PolicyOutcome


def evaluate_policies(
        atoms: List[ChangeAtom],
        impact: Dict[str, Any],
) -> PolicyOutcome:
    """
    Default conservative policy:
      - ADD_COLUMN only: allow, backward compatible
      - Any DROP_COLUMN or CHANGE_TYPE: block, breaking
    """
    has_drop = any(a["kind"] == "DROP_COLUMN" for a in atoms)
    has_change = any(a["kind"] == "CHANGE_TYPE" for a in atoms)
    reasons: List[str] = []

    if not atoms:
        outcome: PolicyOutcome = {
            "decision": "allow",
            "compatibility": "backward",
            "obligations": [],
            "reasons": ["No structural changes detected"],
        }
        return outcome

    if has_drop or has_change:
        reasons.append("Destructive or type-changing modifications detected")
        outcome = {
            "decision": "block",
            "compatibility": "breaking",
            "obligations": [],
            "reasons": reasons,
        }
        return outcome

    reasons.append("Only additive changes detected")
    outcome = {
        "decision": "allow",
        "compatibility": "backward",
        "obligations": [],
        "reasons": reasons,
    }
    return outcome

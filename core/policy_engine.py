from typing import Dict, Any, List

from core.model import ChangeAtom, PolicyOutcome


def evaluate_policies(
        atoms: List[ChangeAtom],
        impact: Dict[str, Any],
) -> PolicyOutcome:
    """
    Policy for schema evolution:

      - If there are no atoms:
          -> allow, backward compatible, no obligations

      - If any DROP_COLUMN or CHANGE_TYPE:
          -> block, breaking, no obligations

      - If only ADD_COLUMN:
          -> allow, backward compatible
          -> request non-destructive vault structure handling via obligations:
               * NEW_HUB  : let vault handler create/align hubs if needed
               * NEW_LINK : let vault handler create/align links if needed

    The vault handler is responsible for deciding whether a new hub/link must
    actually be created or whether the request is already satisfied. The
    handler will:
      - create first-time hubs/links if none exist, or
      - create side-by-side versions when the inferred structure would change,
      - or treat the request as ALREADY_APPLIED when nothing needs to change.
    """
    # No changes at all
    if not atoms:
        outcome: PolicyOutcome = {
            "decision": "allow",
            "compatibility": "backward",
            "obligations": [],
            "reasons": ["No structural changes detected"],
        }
        return outcome

    has_drop = any(a["kind"] == "DROP_COLUMN" for a in atoms)
    has_change = any(a["kind"] == "CHANGE_TYPE" for a in atoms)

    # Any destructive or type-changing modification is blocked by default.
    if has_drop or has_change:
        reasons: List[str] = ["Destructive or type-changing modifications detected"]
        outcome: PolicyOutcome = {
            "decision": "block",
            "compatibility": "breaking",
            "obligations": [],
            "reasons": reasons,
        }
        return outcome

    # Only additive changes: allow and ask the planner to include
    # non-destructive hub/link operations for the vault layer.
    reasons = ["Only additive changes detected"]
    obligations: List[str] = [
        "NEW_HUB",
        "NEW_LINK",
    ]

    outcome = {
        "decision": "allow",
        "compatibility": "backward",
        "obligations": obligations,
        "reasons": reasons,
    }
    return outcome

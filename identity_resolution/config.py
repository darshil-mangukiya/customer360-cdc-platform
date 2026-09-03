"""Identity stewardship configuration.

Confidence-band thresholds and status/decision state machines used by
`identity_resolution.stewardship`. Centralized here (rather than hard-coded in the
stewardship module) so thresholds are easy to find, tune, and unit test.

Bands, using the same match-rule confidence scale as `resolver._match_rule` /
`resolver._rule_for_identifier` (0.60-0.99):

  confidence >= AUTO_MERGE_MIN          -> deterministic resolver auto-merges (unchanged
                                            existing behavior in resolver.py)
  REVIEW_MIN <= confidence < AUTO_MERGE_MIN -> ambiguous: routed to the identity review
                                            queue instead of being merged automatically
  confidence < REVIEW_MIN               -> too weak to act on; identities stay separate
"""

from __future__ import annotations

AUTO_MERGE_MIN = 0.75
REVIEW_MIN = 0.55

# Valid review-case lifecycle states (spec section 13).
REVIEW_STATUSES = ("OPEN", "IN_REVIEW", "APPROVED", "REJECTED", "RESOLVED")

# Valid reviewer decisions (spec section 13).
REVIEW_DECISIONS = (
    "APPROVE_MERGE",
    "REJECT_MERGE",
    "NEEDS_REVIEW",
    "APPROVE_UNMERGE",
    "IGNORE_FALSE_POSITIVE",
)

# Allowed status -> {allowed next statuses}. Enforced by
# `stewardship.apply_decision`; anything not listed here is rejected.
VALID_STATUS_TRANSITIONS: dict[str, set[str]] = {
    "OPEN": {"IN_REVIEW", "REJECTED", "RESOLVED"},
    "IN_REVIEW": {"APPROVED", "REJECTED"},
    "APPROVED": {"RESOLVED"},
    "REJECTED": {"RESOLVED"},
    "RESOLVED": set(),
}

# A decision moves a case to exactly one resulting status.
DECISION_TO_STATUS: dict[str, str] = {
    "APPROVE_MERGE": "APPROVED",
    "REJECT_MERGE": "REJECTED",
    "NEEDS_REVIEW": "IN_REVIEW",
    "APPROVE_UNMERGE": "APPROVED",
    "IGNORE_FALSE_POSITIVE": "RESOLVED",
}

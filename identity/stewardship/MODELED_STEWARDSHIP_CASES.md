# Modeled stewardship cases

These are synthetic, modeled cases; no external reviewer or enterprise stewardship team participated.

The current bounded run generated 8 open cases. Each preserves the tenant, candidate identity, source record, conflict type, deterministic match rule, confidence, evidence summary, suggested survivorship rule, status, and eventual reviewer/audit fields. The executed examples are conflicting-email cases inside components already justified by a stronger identifier. The resolver does not undo the strong merge; it routes the attribute conflict to review.

The workflow supports `OPEN → IN_REVIEW → APPROVED/REJECTED → RESOLVED`, audited merge/unmerge decisions, and deterministic rollback tests. Weak device-only links (0.62) fall in the review band; they never auto-merge. Automatic merge begins at 0.75.

Evidence: `identity_resolution/output/identity_review_queue.csv`, `identity_resolution/output/identity_survivorship_rule.csv`, and `tests/test_identity_stewardship.py`.

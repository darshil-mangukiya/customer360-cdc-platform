from datetime import datetime, timezone

from identity_resolution.repository import FileReviewCaseRepository
from identity_resolution.stewardship import ReviewCase


def test_file_repository_duplicate_decision_is_idempotent(tmp_path):
    now = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    case = ReviewCase(
        review_case_id="revcase_file_idempotent",
        tenant_id="tenant_us",
        canonical_customer_id="cc_a",
        candidate_customer_id="cc_b",
        source_system="account_app",
        source_customer_id="source_1",
        conflict_type="test",
        match_rule="test",
        confidence_score=0.7,
        evidence_summary="synthetic",
        current_status="OPEN",
        created_at=now,
        updated_at=now,
        resolved_at=None,
        reviewer=None,
        decision=None,
        decision_reason=None,
        survivorship_rule=None,
        source_event_id=None,
    )
    repository = FileReviewCaseRepository(tmp_path / "queue.csv")
    repository.create_case(case)
    first = repository.decide(
        case.review_case_id,
        tenant_id="tenant_us",
        decision="NEEDS_REVIEW",
        reviewer="steward",
        reason="inspect",
    )
    second = repository.decide(
        case.review_case_id,
        tenant_id="tenant_us",
        decision="NEEDS_REVIEW",
        reviewer="steward",
        reason="inspect",
    )
    assert second == first
    cases, total = repository.list_cases(tenant_id="tenant_us", case_status=None, limit=10, offset=0)
    assert total == 1 and cases == [first]

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg
from fastapi.testclient import TestClient

from identity_resolution.repository import PostgresReviewCaseRepository
from identity_resolution.stewardship import ReviewCase


def _case(case_id: str, *, tenant_id: str, canonical: str, candidate: str, source_id: str) -> ReviewCase:
    now = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return ReviewCase(
        review_case_id=case_id,
        tenant_id=tenant_id,
        canonical_customer_id=canonical,
        candidate_customer_id=candidate,
        source_system="account_app",
        source_customer_id=source_id,
        conflict_type="integration_verification",
        match_rule="steward_review",
        confidence_score=0.7,
        evidence_summary="synthetic PostgreSQL stewardship verification evidence",
        current_status="OPEN",
        created_at=now,
        updated_at=now,
        resolved_at=None,
        reviewer=None,
        decision=None,
        decision_reason=None,
        survivorship_rule="source_priority_then_latest_non_null",
        source_event_id="synthetic_stewardship_verification",
    )


def _seed_identity(conn: psycopg.Connection, canonical_id: str, tenant_id: str, source_id: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into identity.dim_customer_canonical (
                canonical_customer_id, tenant_id, business_unit, primary_email, primary_phone,
                external_account_id, first_name, last_name, customer_status, first_seen_at,
                last_seen_at, source_record_count, survivorship_rule, canonical_customer_version
            ) values (%s, %s, 'verification', null, null, null, 'Synthetic', 'Steward',
                      'active', now(), now(), 1, 'verification', 1)
            on conflict (canonical_customer_id) do nothing
            """,
            (canonical_id, tenant_id),
        )
        if source_id:
            cur.execute(
                """
                insert into identity.customer_identity_map (
                    canonical_customer_id, tenant_id, source_system, source_table, source_record_id,
                    match_rule, match_confidence, identifier_fingerprint, first_seen_at, last_seen_at
                ) values (%s, %s, 'account_app', 'customers', %s, 'verification', 0.7, %s, now(), now())
                on conflict (tenant_id, source_system, source_record_id) do update
                set canonical_customer_id = excluded.canonical_customer_id
                """,
                (canonical_id, tenant_id, source_id, f"fingerprint_{source_id}"),
            )
    conn.commit()


def verify(dsn: str) -> None:
    repository = PostgresReviewCaseRepository(dsn)
    with psycopg.connect(dsn) as conn:
        _seed_identity(conn, "cc_steward_target_us", "tenant_us")
        _seed_identity(conn, "cc_steward_source_us", "tenant_us", "steward_source_001")
        _seed_identity(conn, "cc_steward_unmerge_us", "tenant_us", "steward_unmerge_001")
        _seed_identity(conn, "cc_steward_rollback_source", "tenant_us", "steward_rollback_001")

    merge_case = _case(
        "revcase_pg_merge_verification",
        tenant_id="tenant_us",
        canonical="cc_steward_target_us",
        candidate="cc_steward_source_us",
        source_id="steward_source_001",
    )
    unmerge_case = _case(
        "revcase_pg_unmerge_verification",
        tenant_id="tenant_us",
        canonical="cc_steward_unmerge_us",
        candidate="cc_unused_candidate",
        source_id="steward_unmerge_001",
    )
    rollback_case = _case(
        "revcase_pg_rollback_verification",
        tenant_id="tenant_us",
        canonical="cc_missing_target_for_rollback",
        candidate="cc_steward_rollback_source",
        source_id="steward_rollback_001",
    )
    for case in (merge_case, unmerge_case, rollback_case):
        repository.create_case(case)

    os.environ["STEWARDSHIP_DSN"] = dsn
    import api.main as api_main

    client = TestClient(api_main.app)
    headers = {"X-API-Key": os.getenv("ACTIVATION_API_KEY", "local-dev-key")}
    assert client.get(
        f"/identity/review/{merge_case.review_case_id}", params={"tenant_id": "tenant_eu"}, headers=headers
    ).status_code == 404
    assert client.get(
        f"/identity/review/{merge_case.review_case_id}", params={"tenant_id": "tenant_us"}, headers=headers
    ).status_code == 200

    for case, final_decision in ((merge_case, "APPROVE_MERGE"), (unmerge_case, "APPROVE_UNMERGE")):
        first = client.post(
            f"/identity/review/{case.review_case_id}/decision",
            json={"tenant_id": "tenant_us", "decision": "NEEDS_REVIEW", "reviewer": "integration_steward", "reason": "verify evidence"},
            headers=headers,
        )
        assert first.status_code == 200 and first.json()["current_status"] == "IN_REVIEW"
        final = client.post(
            f"/identity/review/{case.review_case_id}/decision",
            json={"tenant_id": "tenant_us", "decision": final_decision, "reviewer": "integration_steward", "reason": "synthetic approval"},
            headers=headers,
        )
        assert final.status_code == 200 and final.json()["current_status"] == "APPROVED"
        repeated = client.post(
            f"/identity/review/{case.review_case_id}/decision",
            json={"tenant_id": "tenant_us", "decision": final_decision, "reviewer": "integration_steward", "reason": "synthetic approval"},
            headers=headers,
        )
        assert repeated.status_code == 200
        assert repeated.json()["current_status"] == final.json()["current_status"]
        assert repeated.json()["decision"] == final.json()["decision"]

    repository.decide(
        rollback_case.review_case_id,
        tenant_id="tenant_us",
        decision="NEEDS_REVIEW",
        reviewer="integration_steward",
        reason="verify rollback",
    )
    try:
        repository.decide(
            rollback_case.review_case_id,
            tenant_id="tenant_us",
            decision="APPROVE_MERGE",
            reviewer="integration_steward",
            reason="must roll back",
        )
    except Exception:
        pass
    else:
        raise AssertionError("expected the missing merge target to fail")

    reinitialized = PostgresReviewCaseRepository(dsn)
    persisted = reinitialized.get_case(merge_case.review_case_id, tenant_id="tenant_us")
    rolled_back = reinitialized.get_case(rollback_case.review_case_id, tenant_id="tenant_us")
    assert persisted and persisted.current_status == "APPROVED"
    assert rolled_back and rolled_back.current_status == "IN_REVIEW" and rolled_back.decision == "NEEDS_REVIEW"

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "select count(*) from identity.identity_merge_audit where review_case_id = %s",
            (merge_case.review_case_id,),
        )
        assert cur.fetchone()[0] == 1
        cur.execute(
            "select count(*) from identity.identity_unmerge_audit where review_case_id = %s",
            (unmerge_case.review_case_id,),
        )
        assert cur.fetchone()[0] == 1
        cur.execute(
            "select canonical_customer_id from identity.customer_identity_map where tenant_id='tenant_us' and source_system='account_app' and source_record_id='steward_source_001'"
        )
        assert cur.fetchone()[0] == "cc_steward_target_us"

    print(
        "stewardship_postgres=PASS decision_persistence=PASS merge_audit=PASS "
        "unmerge_audit=PASS reinitialize_persistence=PASS rollback=PASS idempotency=PASS tenant_isolation=PASS"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify persistent PostgreSQL identity stewardship.")
    parser.add_argument("--dsn", default=os.getenv("STEWARDSHIP_DSN", "postgresql://c360:c360@127.0.0.1:55432/c360"))
    args = parser.parse_args()
    verify(args.dsn)


if __name__ == "__main__":
    main()

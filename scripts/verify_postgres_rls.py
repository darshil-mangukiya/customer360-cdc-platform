from __future__ import annotations

import argparse
import os
import secrets

import psycopg
from psycopg import sql


ROLE_US = "p7_tenant_us_app"
ROLE_EU = "p7_tenant_eu_app"


def _seed(admin_dsn: str) -> None:
    with psycopg.connect(admin_dsn) as conn, conn.cursor() as cur:
        for canonical_id, tenant_id in (("cc_rls_us", "tenant_us"), ("cc_rls_eu", "tenant_eu")):
            cur.execute(
                """
                insert into identity.dim_customer_canonical (
                    canonical_customer_id, tenant_id, business_unit, primary_email, primary_phone,
                    external_account_id, first_name, last_name, customer_status, first_seen_at,
                    last_seen_at, source_record_count, survivorship_rule, canonical_customer_version
                ) values (%s, %s, 'rls_verification', null, null, null, 'RLS', 'Fixture',
                          'active', now(), now(), 1, 'verification', 1)
                on conflict (canonical_customer_id) do update set tenant_id = excluded.tenant_id
                """,
                (canonical_id, tenant_id),
            )
            cur.execute(
                """
                insert into identity.identity_review_queue (
                    review_case_id, tenant_id, canonical_customer_id, candidate_customer_id,
                    source_system, source_customer_id, conflict_type, match_rule,
                    confidence_score, evidence_summary, current_status
                ) values (%s, %s, %s, %s, 'rls_verification', %s, 'rls_verification',
                          'restricted_role_policy', 0.7, 'synthetic RLS evidence', 'OPEN')
                on conflict (review_case_id) do update
                set tenant_id = excluded.tenant_id, current_status = 'OPEN', decision = null,
                    reviewer = null, decision_reason = null
                """,
                (f"revcase_rls_{tenant_id}", tenant_id, canonical_id, f"cc_candidate_{tenant_id}", f"source_{tenant_id}"),
            )
            cur.execute(
                """
                insert into activation.export_customer_segment (
                    canonical_customer_id, tenant_id, business_unit, email_sha256, phone_sha256,
                    export_timestamp, customer_segment, source_lineage_refs
                ) values (%s, %s, 'rls_verification', null, null, now(), 'verification', 'synthetic')
                on conflict (canonical_customer_id) do update set tenant_id = excluded.tenant_id
                """,
                (canonical_id, tenant_id),
            )


def _restricted_dsn(admin_dsn: str, role: str, password: str) -> str:
    info = psycopg.conninfo.conninfo_to_dict(admin_dsn)
    info.update(user=role, password=password)
    return psycopg.conninfo.make_conninfo(**info)


def _verify_role(dsn: str, *, role: str, own_tenant: str, hidden_tenant: str) -> None:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("select session_user, current_user")
        assert cur.fetchone() == (role, role)
        cur.execute("select tenant_id from identity.identity_review_queue order by tenant_id")
        assert {row[0] for row in cur.fetchall()} == {own_tenant}
        cur.execute("select tenant_id from identity.dim_customer_canonical where canonical_customer_id like 'cc_rls_%'")
        assert {row[0] for row in cur.fetchall()} == {own_tenant}
        cur.execute("select tenant_id from activation.export_customer_segment where canonical_customer_id like 'cc_rls_%'")
        assert {row[0] for row in cur.fetchall()} == {own_tenant}

        own_case_id = f"revcase_rls_insert_{own_tenant}"
        cur.execute(
            """
            insert into identity.identity_review_queue (
                review_case_id, tenant_id, canonical_customer_id, candidate_customer_id,
                source_system, source_customer_id, conflict_type, match_rule,
                confidence_score, evidence_summary, current_status
            ) values (%s, %s, 'cc_rls_own', 'cc_rls_candidate', 'rls_verification',
                      'source_own', 'rls_verification', 'restricted_role_policy',
                      0.7, 'synthetic own-tenant insert', 'OPEN')
            on conflict (review_case_id) do update set updated_at = now()
            """,
            (own_case_id, own_tenant),
        )
        cur.execute(
            "update identity.identity_review_queue set evidence_summary='own tenant update allowed' where review_case_id=%s",
            (own_case_id,),
        )
        assert cur.rowcount == 1
        cur.execute(
            "update identity.identity_review_queue set evidence_summary='cross tenant update forbidden' where tenant_id=%s",
            (hidden_tenant,),
        )
        assert cur.rowcount == 0
        cur.execute("delete from identity.identity_review_queue where tenant_id=%s", (hidden_tenant,))
        assert cur.rowcount == 0
        cur.execute("delete from identity.identity_review_queue where review_case_id=%s", (own_case_id,))
        assert cur.rowcount == 1

    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                insert into identity.identity_review_queue (
                    review_case_id, tenant_id, canonical_customer_id, candidate_customer_id,
                    source_system, source_customer_id, conflict_type, match_rule,
                    confidence_score, evidence_summary, current_status
                ) values (%s, %s, 'cc_cross', 'cc_cross_candidate', 'rls_verification',
                          'source_cross', 'rls_verification', 'restricted_role_policy',
                          0.7, 'must be rejected', 'OPEN')
                """,
                (f"revcase_rls_forbidden_{role}", hidden_tenant),
            )
    except psycopg.errors.InsufficientPrivilege:
        pass
    else:
        raise AssertionError(f"{role} cross-tenant INSERT unexpectedly succeeded")


def verify(admin_dsn: str) -> None:
    password_us = secrets.token_urlsafe(24)
    password_eu = secrets.token_urlsafe(24)
    _seed(admin_dsn)
    try:
        with psycopg.connect(admin_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                sql.SQL("alter role {} login password {}").format(sql.Identifier(ROLE_US), sql.Literal(password_us))
            )
            cur.execute(
                sql.SQL("alter role {} login password {}").format(sql.Identifier(ROLE_EU), sql.Literal(password_eu))
            )
        _verify_role(
            _restricted_dsn(admin_dsn, ROLE_US, password_us),
            role=ROLE_US,
            own_tenant="tenant_us",
            hidden_tenant="tenant_eu",
        )
        _verify_role(
            _restricted_dsn(admin_dsn, ROLE_EU, password_eu),
            role=ROLE_EU,
            own_tenant="tenant_eu",
            hidden_tenant="tenant_us",
        )
        with psycopg.connect(admin_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                select schemaname, tablename, policyname, roles, cmd, qual, with_check
                from pg_policies
                where policyname in (
                    'tenant_review_queue_isolation',
                    'tenant_customer_360_identity_visibility',
                    'tenant_activation_output_visibility'
                ) order by schemaname, tablename
                """
            )
            policies = cur.fetchall()
            assert len(policies) == 3
            for row in policies:
                print("policy=" + " | ".join(str(value) for value in row))
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("alter role p7_tenant_us_app nologin password null")
            cur.execute("alter role p7_tenant_eu_app nologin password null")
    print(
        "postgres_rls=PASS authenticated_restricted_roles=PASS tenant_visibility=PASS "
        "cross_tenant_insert=BLOCKED cross_tenant_update=BLOCKED pg_policies=PASS"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify PostgreSQL RLS with actual restricted login roles.")
    parser.add_argument("--dsn", default=os.getenv("WAREHOUSE_DSN", "postgresql://c360:c360@127.0.0.1:55432/c360"))
    args = parser.parse_args()
    verify(args.dsn)


if __name__ == "__main__":
    main()

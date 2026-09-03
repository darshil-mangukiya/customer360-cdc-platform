from __future__ import annotations

import csv
import hashlib
from dataclasses import asdict
from pathlib import Path
from typing import Any, Protocol

from identity_resolution.stewardship import ReviewCase, apply_decision


class ReviewCaseNotFoundError(LookupError):
    pass


class ReviewCaseRepository(Protocol):
    def list_cases(
        self, *, tenant_id: str | None, case_status: str | None, limit: int, offset: int
    ) -> tuple[list[ReviewCase], int]: ...

    def get_case(self, review_case_id: str, *, tenant_id: str | None = None) -> ReviewCase | None: ...

    def create_case(self, case: ReviewCase) -> ReviewCase: ...

    def decide(
        self,
        review_case_id: str,
        *,
        tenant_id: str | None,
        decision: str,
        reviewer: str,
        reason: str,
    ) -> ReviewCase: ...


def _text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _case_from_mapping(row: dict[str, Any]) -> ReviewCase:
    return ReviewCase(
        review_case_id=str(row["review_case_id"]),
        tenant_id=_text(row.get("tenant_id")),
        canonical_customer_id=str(row["canonical_customer_id"]),
        candidate_customer_id=str(row["candidate_customer_id"]),
        source_system=str(row["source_system"]),
        source_customer_id=str(row["source_customer_id"]),
        conflict_type=str(row["conflict_type"]),
        match_rule=str(row["match_rule"]),
        confidence_score=float(row["confidence_score"]),
        evidence_summary=str(row["evidence_summary"]),
        current_status=str(row["current_status"]),
        created_at=_text(row["created_at"]) or "",
        updated_at=_text(row["updated_at"]) or "",
        resolved_at=_text(row.get("resolved_at")),
        reviewer=_text(row.get("reviewer")),
        decision=_text(row.get("decision")),
        decision_reason=_text(row.get("decision_reason")),
        survivorship_rule=_text(row.get("survivorship_rule")),
        source_event_id=_text(row.get("source_event_id")),
    )


class FileReviewCaseRepository:
    """Offline fixture repository retained for unit tests and file-only demos."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.fields = list(ReviewCase.__dataclass_fields__)

    def _read(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        with self.path.open(encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def _write(self, rows: list[dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.fields)
            writer.writeheader()
            writer.writerows(rows)

    def list_cases(
        self, *, tenant_id: str | None, case_status: str | None, limit: int, offset: int
    ) -> tuple[list[ReviewCase], int]:
        rows = self._read()
        if tenant_id:
            rows = [row for row in rows if row.get("tenant_id") == tenant_id]
        if case_status:
            rows = [row for row in rows if row.get("current_status") == case_status]
        return [_case_from_mapping(row) for row in rows[offset : offset + limit]], len(rows)

    def get_case(self, review_case_id: str, *, tenant_id: str | None = None) -> ReviewCase | None:
        row = next(
            (
                row
                for row in self._read()
                if row.get("review_case_id") == review_case_id
                and (tenant_id is None or row.get("tenant_id") == tenant_id)
            ),
            None,
        )
        return _case_from_mapping(row) if row else None

    def create_case(self, case: ReviewCase) -> ReviewCase:
        rows = self._read()
        index = next((i for i, row in enumerate(rows) if row.get("review_case_id") == case.review_case_id), None)
        if index is None:
            rows.append(asdict(case))
        else:
            rows[index] = asdict(case)
        self._write(rows)
        return case

    def decide(
        self,
        review_case_id: str,
        *,
        tenant_id: str | None,
        decision: str,
        reviewer: str,
        reason: str,
    ) -> ReviewCase:
        rows = self._read()
        index = next(
            (
                i
                for i, row in enumerate(rows)
                if row.get("review_case_id") == review_case_id
                and (tenant_id is None or row.get("tenant_id") == tenant_id)
            ),
            None,
        )
        if index is None:
            raise ReviewCaseNotFoundError(review_case_id)
        case = _case_from_mapping(rows[index])
        if (case.decision, case.reviewer, case.decision_reason) == (decision, reviewer, reason):
            return case
        updated = apply_decision(case, decision=decision, reviewer=reviewer, reason=reason)
        rows[index] = asdict(updated)
        self._write(rows)
        return updated


class PostgresReviewCaseRepository:
    """Transactional PostgreSQL stewardship repository used by the integrated API."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    @staticmethod
    def _connect(dsn: str):
        import psycopg
        from psycopg.rows import dict_row

        return psycopg.connect(dsn, row_factory=dict_row)

    def list_cases(
        self, *, tenant_id: str | None, case_status: str | None, limit: int, offset: int
    ) -> tuple[list[ReviewCase], int]:
        filters: list[str] = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if tenant_id:
            filters.append("tenant_id = %(tenant_id)s")
            params["tenant_id"] = tenant_id
        if case_status:
            filters.append("current_status = %(case_status)s")
            params["case_status"] = case_status
        where = f"where {' and '.join(filters)}" if filters else ""
        with self._connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(f"select count(*) as total from identity.identity_review_queue {where}", params)
            total = int(cur.fetchone()["total"])
            cur.execute(
                f"select * from identity.identity_review_queue {where} order by created_at, review_case_id limit %(limit)s offset %(offset)s",
                params,
            )
            return [_case_from_mapping(row) for row in cur.fetchall()], total

    def get_case(self, review_case_id: str, *, tenant_id: str | None = None) -> ReviewCase | None:
        query = "select * from identity.identity_review_queue where review_case_id = %(review_case_id)s"
        params: dict[str, Any] = {"review_case_id": review_case_id}
        if tenant_id:
            query += " and tenant_id = %(tenant_id)s"
            params["tenant_id"] = tenant_id
        with self._connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return _case_from_mapping(row) if row else None

    def create_case(self, case: ReviewCase) -> ReviewCase:
        values = asdict(case)
        columns = ", ".join(values)
        placeholders = ", ".join(f"%({column})s" for column in values)
        updates = ", ".join(
            f"{column} = excluded.{column}" for column in values if column not in {"review_case_id", "created_at"}
        )
        with self._connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                f"insert into identity.identity_review_queue ({columns}) values ({placeholders}) "
                f"on conflict (review_case_id) do update set {updates}",
                values,
            )
        return case

    def decide(
        self,
        review_case_id: str,
        *,
        tenant_id: str | None,
        decision: str,
        reviewer: str,
        reason: str,
    ) -> ReviewCase:
        with self._connect(self.dsn) as conn, conn.transaction(), conn.cursor() as cur:
            query = "select * from identity.identity_review_queue where review_case_id = %(review_case_id)s"
            params: dict[str, Any] = {"review_case_id": review_case_id}
            if tenant_id:
                query += " and tenant_id = %(tenant_id)s"
                params["tenant_id"] = tenant_id
            cur.execute(query + " for update", params)
            row = cur.fetchone()
            if not row:
                raise ReviewCaseNotFoundError(review_case_id)
            case = _case_from_mapping(row)
            if (case.decision, case.reviewer, case.decision_reason) == (decision, reviewer, reason):
                return case
            updated = apply_decision(case, decision=decision, reviewer=reviewer, reason=reason)
            cur.execute(
                """
                update identity.identity_review_queue
                set current_status = %(current_status)s, updated_at = %(updated_at)s,
                    resolved_at = %(resolved_at)s, reviewer = %(reviewer)s,
                    decision = %(decision)s, decision_reason = %(decision_reason)s
                where review_case_id = %(review_case_id)s and tenant_id = %(tenant_id)s
                """,
                asdict(updated),
            )
            if decision == "APPROVE_MERGE":
                self._apply_merge(cur, updated, reviewer=reviewer, reason=reason)
            elif decision == "APPROVE_UNMERGE":
                self._apply_unmerge(cur, updated, reviewer=reviewer, reason=reason)
            return updated

    @staticmethod
    def _audit_id(prefix: str, review_case_id: str) -> str:
        digest = hashlib.sha256(review_case_id.encode("utf-8")).hexdigest()[:20]
        return f"{prefix}_{digest}"

    def _apply_merge(self, cur: Any, case: ReviewCase, *, reviewer: str, reason: str) -> None:
        cur.execute(
            """
            update identity.customer_identity_map
            set canonical_customer_id = %(target)s
            where tenant_id = %(tenant_id)s and canonical_customer_id = %(source)s
            """,
            {"target": case.canonical_customer_id, "source": case.candidate_customer_id, "tenant_id": case.tenant_id},
        )
        if cur.rowcount == 0:
            raise ValueError("merge candidate has no tenant-scoped identity mappings")
        cur.execute(
            """
            insert into identity.identity_merge_audit (
                merge_audit_id, tenant_id, review_case_id, source_canonical_customer_id,
                target_canonical_customer_id, reviewer, reason
            ) values (%(audit_id)s, %(tenant_id)s, %(case_id)s, %(source)s, %(target)s, %(reviewer)s, %(reason)s)
            on conflict (merge_audit_id) do nothing
            """,
            {
                "audit_id": self._audit_id("mergeaudit", case.review_case_id),
                "tenant_id": case.tenant_id,
                "case_id": case.review_case_id,
                "source": case.candidate_customer_id,
                "target": case.canonical_customer_id,
                "reviewer": reviewer,
                "reason": reason,
            },
        )

    def _apply_unmerge(self, cur: Any, case: ReviewCase, *, reviewer: str, reason: str) -> None:
        new_id = "cc_unmerge_" + hashlib.sha256(case.review_case_id.encode("utf-8")).hexdigest()[:12]
        cur.execute(
            """
            insert into identity.dim_customer_canonical (
                canonical_customer_id, tenant_id, business_unit, primary_email, primary_phone,
                external_account_id, first_name, last_name, customer_status, first_seen_at,
                last_seen_at, source_record_count, survivorship_rule, canonical_customer_version
            )
            select %(new_id)s, tenant_id, business_unit, primary_email, primary_phone,
                   null, first_name, last_name, customer_status, first_seen_at, last_seen_at,
                   1, survivorship_rule, canonical_customer_version + 1
            from identity.dim_customer_canonical
            where canonical_customer_id = %(original_id)s and tenant_id = %(tenant_id)s
            on conflict (canonical_customer_id) do nothing
            """,
            {"new_id": new_id, "original_id": case.canonical_customer_id, "tenant_id": case.tenant_id},
        )
        cur.execute(
            """
            update identity.customer_identity_map set canonical_customer_id = %(new_id)s
            where tenant_id = %(tenant_id)s and canonical_customer_id = %(original_id)s
              and source_system = %(source_system)s and source_record_id = %(source_record_id)s
            """,
            {
                "new_id": new_id,
                "tenant_id": case.tenant_id,
                "original_id": case.canonical_customer_id,
                "source_system": case.source_system,
                "source_record_id": case.source_customer_id,
            },
        )
        if cur.rowcount != 1:
            raise ValueError("unmerge target must identify exactly one tenant-scoped mapping")
        cur.execute(
            """
            insert into identity.identity_unmerge_audit (
                unmerge_audit_id, tenant_id, review_case_id, original_canonical_customer_id,
                new_canonical_customer_id, source_system, source_record_id, reviewer, reason
            ) values (
                %(audit_id)s, %(tenant_id)s, %(case_id)s, %(original_id)s,
                %(new_id)s, %(source_system)s, %(source_record_id)s, %(reviewer)s, %(reason)s
            ) on conflict (unmerge_audit_id) do nothing
            """,
            {
                "audit_id": self._audit_id("unmergeaudit", case.review_case_id),
                "tenant_id": case.tenant_id,
                "case_id": case.review_case_id,
                "original_id": case.canonical_customer_id,
                "new_id": new_id,
                "source_system": case.source_system,
                "source_record_id": case.source_customer_id,
                "reviewer": reviewer,
                "reason": reason,
            },
        )

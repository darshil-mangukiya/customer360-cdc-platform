"""Activation reconciliation: source-to-target validation for reverse ETL exports.

Reads the artifacts already produced by
`identity_resolution.resolver` (canonical population), `privacy.activation_policy`
(suppression list), `reverse_etl.exporter` (per-export CSVs), and
`reverse_etl.destinations.simulator` (per-row payload audit + run logs), and proves —
per (destination, tenant) — that:

    warehouse_eligible_count == successful_count + failed_count
                              + suppressed_count + skipped_count + duplicate_count

Any variance is not just counted but drilled down into concrete findings: which
canonical customer IDs are missing, unexpected, privacy-suppressed-but-exported, or
duplicated, so a reviewer can act on the mismatch instead of just seeing a number.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from reverse_etl.destinations.simulator import DESTINATIONS, DestinationConfig


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _tenant_scope(tenant_id: str | None) -> str:
    return tenant_id or "tenant_unknown"


def _load_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _run_id(destination: str, tenant_id: str) -> str:
    seed = f"{destination}:{tenant_id}:{_now()}"
    return f"reconrun_{hashlib.sha1(seed.encode('utf-8')).hexdigest()[:12]}"


@dataclass(frozen=True)
class ActivationReconciliation:
    run_id: str
    tenant_id: str
    destination: str
    export_name: str
    warehouse_eligible_count: int
    export_count: int
    attempted_count: int
    successful_count: int
    failed_count: int
    suppressed_count: int
    skipped_count: int
    duplicate_count: int
    variance_count: int
    variance_pct: float
    status: str
    started_at: str
    completed_at: str


@dataclass(frozen=True)
class ReconciliationFinding:
    finding_id: str
    run_id: str
    tenant_id: str
    destination: str
    export_name: str
    finding_type: str
    severity: str
    canonical_customer_id: str | None
    detail: str
    detected_at: str


def _finding_id(*parts: str) -> str:
    return f"reconfind_{hashlib.sha1('|'.join(parts).encode('utf-8')).hexdigest()[:16]}"


def _tenants_for(*row_sets: list[dict[str, Any]]) -> list[str]:
    tenants: set[str] = set()
    for rows in row_sets:
        tenants.update(_tenant_scope(row.get("tenant_id")) for row in rows)
    return sorted(tenants)


def reconcile(
    *,
    identity_dir: Path,
    privacy_dir: Path,
    export_dir: Path,
    sync_log_dir: Path,
    destinations: list[DestinationConfig] | None = None,
) -> tuple[list[ActivationReconciliation], list[ReconciliationFinding]]:
    configs = destinations or DESTINATIONS
    canonical_rows = _load_csv(identity_dir / "dim_customer_canonical.csv")
    suppression_rows = _load_csv(privacy_dir / "activation_suppression_list.csv")
    payload_audit_rows = _load_csv(sync_log_dir / "payload_audit.csv")

    canonical_by_tenant: dict[str, set[str]] = defaultdict(set)
    tenant_by_customer: dict[str, str] = {}
    for row in canonical_rows:
        tenant = _tenant_scope(row.get("tenant_id"))
        canonical_by_tenant[tenant].add(row["canonical_customer_id"])
        tenant_by_customer[row["canonical_customer_id"]] = tenant

    suppressed_by_tenant: dict[str, set[str]] = defaultdict(set)
    for row in suppression_rows:
        suppressed_by_tenant[_tenant_scope(row.get("tenant_id"))].add(row["canonical_customer_id"])

    now = _now()
    reconciliations: list[ActivationReconciliation] = []
    findings: list[ReconciliationFinding] = []

    for config in configs:
        export_rows = _load_csv(export_dir / config.export_file)
        audit_rows = [row for row in payload_audit_rows if row.get("destination_name") == config.destination_name]

        export_rows_by_tenant: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in export_rows:
            export_rows_by_tenant[_tenant_scope(row.get("tenant_id"))].append(row)

        audit_rows_by_tenant: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in audit_rows:
            audit_rows_by_tenant[_tenant_scope(row.get("tenant_id"))].append(row)

        tenants = _tenants_for(export_rows) or list(canonical_by_tenant.keys())
        for tenant_id in tenants:
            run_id = _run_id(config.destination_name, tenant_id)
            canonical_ids = canonical_by_tenant.get(tenant_id, set())
            suppressed_ids = suppressed_by_tenant.get(tenant_id, set())
            eligible_ids = canonical_ids - suppressed_ids

            tenant_export_rows = export_rows_by_tenant.get(tenant_id, [])
            export_ids = [row.get("canonical_customer_id") for row in tenant_export_rows]
            export_id_set = set(export_ids)

            tenant_audit_rows = audit_rows_by_tenant.get(tenant_id, [])
            successful_rows = [row for row in tenant_audit_rows if row.get("sync_status") in {"inserted", "updated"}]
            duplicate_rows = [row for row in tenant_audit_rows if row.get("sync_status") == "skipped_unchanged"]
            failed_rows = [row for row in tenant_audit_rows if row.get("sync_status") == "failed"]
            attempted_count = len(tenant_audit_rows)
            # Rows written to the export file but never attempted by the destination
            # simulator (e.g. simulator not yet run for this batch) are an intentional
            # skip in the reconciliation sense, not a failure.
            skipped_count = max(0, len(tenant_export_rows) - attempted_count)

            # NOTE: "eligible" here follows the spec's reconciliation invariant
            # (eligible_population = successful + failed + suppressed + skipped +
            # duplicate), so it is the *total* candidate population considered for
            # activation for this tenant — suppression is one of the dispositions a
            # candidate can land in, not a pre-filter applied before counting.
            warehouse_eligible_count = len(canonical_ids)
            successful_count = len(successful_rows)
            failed_count = len(failed_rows)
            suppressed_count = len(suppressed_ids)
            duplicate_count = len(duplicate_rows)

            variance_count = warehouse_eligible_count - (
                successful_count + failed_count + suppressed_count + skipped_count + duplicate_count
            )
            variance_pct = round((variance_count / warehouse_eligible_count) * 100, 2) if warehouse_eligible_count else 0.0
            status = "reconciled" if variance_count == 0 else "variance_detected"

            reconciliations.append(
                ActivationReconciliation(
                    run_id=run_id,
                    tenant_id=tenant_id,
                    destination=config.destination_name,
                    export_name=config.export_file.removesuffix(".csv"),
                    warehouse_eligible_count=warehouse_eligible_count,
                    export_count=len(tenant_export_rows),
                    attempted_count=attempted_count,
                    successful_count=successful_count,
                    failed_count=failed_count,
                    suppressed_count=suppressed_count,
                    skipped_count=skipped_count,
                    duplicate_count=duplicate_count,
                    variance_count=variance_count,
                    variance_pct=variance_pct,
                    status=status,
                    started_at=now,
                    completed_at=now,
                )
            )

            # --- Drill-down findings (spec section 21) ---
            for missing_id in sorted(eligible_ids - export_id_set):
                findings.append(
                    ReconciliationFinding(
                        finding_id=_finding_id(run_id, "missing_eligible_row", missing_id),
                        run_id=run_id,
                        tenant_id=tenant_id,
                        destination=config.destination_name,
                        export_name=config.export_file.removesuffix(".csv"),
                        finding_type="missing_eligible_row",
                        severity="high",
                        canonical_customer_id=missing_id,
                        detail=f"{missing_id} is activation-eligible but absent from {config.export_file}.",
                        detected_at=now,
                    )
                )

            for unexpected_id in sorted(export_id_set - canonical_ids):
                findings.append(
                    ReconciliationFinding(
                        finding_id=_finding_id(run_id, "unexpected_exported_row", unexpected_id or "none"),
                        run_id=run_id,
                        tenant_id=tenant_id,
                        destination=config.destination_name,
                        export_name=config.export_file.removesuffix(".csv"),
                        finding_type="unexpected_exported_row",
                        severity="high",
                        canonical_customer_id=unexpected_id,
                        detail=f"{unexpected_id} appears in {config.export_file} but is not a known canonical customer for tenant {tenant_id}.",
                        detected_at=now,
                    )
                )

            for leaked_id in sorted(suppressed_ids & export_id_set):
                findings.append(
                    ReconciliationFinding(
                        finding_id=_finding_id(run_id, "privacy_suppressed_row_exported", leaked_id),
                        run_id=run_id,
                        tenant_id=tenant_id,
                        destination=config.destination_name,
                        export_name=config.export_file.removesuffix(".csv"),
                        finding_type="privacy_suppressed_row_exported",
                        severity="critical",
                        canonical_customer_id=leaked_id,
                        detail=f"{leaked_id} is on the privacy suppression list but appears in {config.export_file}.",
                        detected_at=now,
                    )
                )

            for customer_id, count in Counter(export_ids).items():
                if count > 1:
                    findings.append(
                        ReconciliationFinding(
                            finding_id=_finding_id(run_id, "duplicate_customer_export", customer_id or "none"),
                            run_id=run_id,
                            tenant_id=tenant_id,
                            destination=config.destination_name,
                            export_name=config.export_file.removesuffix(".csv"),
                            finding_type="duplicate_customer_export",
                            severity="medium",
                            canonical_customer_id=customer_id,
                            detail=f"{customer_id} appears {count} times in {config.export_file} (expected at most once).",
                            detected_at=now,
                        )
                    )

            for row in tenant_export_rows:
                if not row.get("email_sha256") and not row.get("phone_sha256"):
                    findings.append(
                        ReconciliationFinding(
                            finding_id=_finding_id(run_id, "missing_hashed_identifier", row.get("canonical_customer_id", "")),
                            run_id=run_id,
                            tenant_id=tenant_id,
                            destination=config.destination_name,
                            export_name=config.export_file.removesuffix(".csv"),
                            finding_type="missing_hashed_identifier",
                            severity="medium",
                            canonical_customer_id=row.get("canonical_customer_id"),
                            detail=f"{row.get('canonical_customer_id')} has neither email_sha256 nor phone_sha256 in {config.export_file}.",
                            detected_at=now,
                        )
                    )
                row_tenant = _tenant_scope(row.get("tenant_id"))
                owning_tenant = tenant_by_customer.get(row.get("canonical_customer_id", ""))
                if owning_tenant and owning_tenant != row_tenant:
                    findings.append(
                        ReconciliationFinding(
                            finding_id=_finding_id(run_id, "wrong_tenant_row", row.get("canonical_customer_id", "")),
                            run_id=run_id,
                            tenant_id=tenant_id,
                            destination=config.destination_name,
                            export_name=config.export_file.removesuffix(".csv"),
                            finding_type="wrong_tenant_row",
                            severity="critical",
                            canonical_customer_id=row.get("canonical_customer_id"),
                            detail=f"{row.get('canonical_customer_id')} is owned by tenant {owning_tenant} but appears under tenant {row_tenant} in {config.export_file}.",
                            detected_at=now,
                        )
                    )

            for key, count in Counter(row.get("idempotency_key") for row in tenant_audit_rows).items():
                if key and count > 1:
                    findings.append(
                        ReconciliationFinding(
                            finding_id=_finding_id(run_id, "duplicate_idempotency_key", key),
                            run_id=run_id,
                            tenant_id=tenant_id,
                            destination=config.destination_name,
                            export_name=config.export_file.removesuffix(".csv"),
                            finding_type="duplicate_idempotency_key",
                            severity="medium",
                            canonical_customer_id=None,
                            detail=f"idempotency_key {key} appears {count} times in payload_audit for {config.destination_name}.",
                            detected_at=now,
                        )
                    )

    return reconciliations, findings


def write_reconciliation_outputs(
    *,
    reconciliations: list[ActivationReconciliation],
    findings: list[ReconciliationFinding],
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "activation_reconciliation.csv").open("w", newline="", encoding="utf-8") as fh:
        fieldnames = [f.name for f in ActivationReconciliation.__dataclass_fields__.values()]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(asdict(row) for row in reconciliations)
    with (output_dir / "activation_reconciliation_findings.csv").open("w", newline="", encoding="utf-8") as fh:
        fieldnames = [f.name for f in ReconciliationFinding.__dataclass_fields__.values()]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(asdict(row) for row in findings)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile reverse ETL activation exports against destination sync results.")
    parser.add_argument("--identity-dir", default="identity_resolution/output")
    parser.add_argument("--privacy-dir", default="privacy/output")
    parser.add_argument("--export-dir", default="reverse_etl/exports")
    parser.add_argument("--sync-log-dir", default="reverse_etl/sync_logs")
    parser.add_argument("--output-dir", default="reverse_etl/reconciliation")
    args = parser.parse_args()
    reconciliations, findings = reconcile(
        identity_dir=Path(args.identity_dir),
        privacy_dir=Path(args.privacy_dir),
        export_dir=Path(args.export_dir),
        sync_log_dir=Path(args.sync_log_dir),
    )
    write_reconciliation_outputs(reconciliations=reconciliations, findings=findings, output_dir=Path(args.output_dir))
    variance_runs = sum(1 for row in reconciliations if row.status == "variance_detected")
    critical_findings = sum(1 for row in findings if row.severity == "critical")
    print(
        f"reconciliation_runs={len(reconciliations)} variance_runs={variance_runs} "
        f"findings={len(findings)} critical_findings={critical_findings}"
    )


if __name__ == "__main__":
    main()

import csv
from pathlib import Path

from reverse_etl.destinations.simulator import DestinationConfig
from reverse_etl.reconciliation import reconcile

TEST_CONFIG = [DestinationConfig("test_dest", "test_export.csv", "obj/v1", 10, 3)]


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _setup(
    tmp_path: Path,
    *,
    canonical: list[dict],
    suppressed: list[dict],
    export_rows: list[dict],
    audit_rows: list[dict],
) -> dict[str, Path]:
    identity_dir = tmp_path / "identity"
    privacy_dir = tmp_path / "privacy"
    export_dir = tmp_path / "exports"
    sync_log_dir = tmp_path / "sync_logs"
    _write_csv(identity_dir / "dim_customer_canonical.csv", canonical)
    _write_csv(privacy_dir / "activation_suppression_list.csv", suppressed)
    _write_csv(export_dir / "test_export.csv", export_rows)
    _write_csv(sync_log_dir / "payload_audit.csv", audit_rows)
    return {"identity_dir": identity_dir, "privacy_dir": privacy_dir, "export_dir": export_dir, "sync_log_dir": sync_log_dir}


def _customer(customer_id: str, tenant_id: str = "tenant_us") -> dict:
    return {"canonical_customer_id": customer_id, "tenant_id": tenant_id}


def _export_row(customer_id: str, tenant_id: str = "tenant_us", email_hash: str = "hash123", phone_hash: str = "") -> dict:
    return {
        "canonical_customer_id": customer_id,
        "tenant_id": tenant_id,
        "email_sha256": email_hash,
        "phone_sha256": phone_hash,
        "export_timestamp": "2026-01-01T00:00:00Z",
    }


def _audit_row(customer_id: str, tenant_id: str, status: str, idempotency_key: str) -> dict:
    return {
        "destination_name": "test_dest",
        "canonical_customer_id": customer_id,
        "tenant_id": tenant_id,
        "sync_status": status,
        "idempotency_key": idempotency_key,
    }


def test_happy_path_reconciles_with_zero_variance(tmp_path):
    paths = _setup(
        tmp_path,
        canonical=[_customer("cc_1"), _customer("cc_2"), _customer("cc_3")],
        suppressed=[{"canonical_customer_id": "cc_3", "tenant_id": "tenant_us"}],
        export_rows=[_export_row("cc_1"), _export_row("cc_2")],
        audit_rows=[
            _audit_row("cc_1", "tenant_us", "inserted", "key1"),
            _audit_row("cc_2", "tenant_us", "updated", "key2"),
        ],
    )
    recs, findings = reconcile(destinations=TEST_CONFIG, **paths)
    assert len(recs) == 1
    rec = recs[0]
    assert rec.warehouse_eligible_count == 3
    assert rec.successful_count == 2
    assert rec.suppressed_count == 1
    assert rec.variance_count == 0
    assert rec.status == "reconciled"
    assert findings == []


def test_missing_eligible_row_is_flagged(tmp_path):
    paths = _setup(
        tmp_path,
        canonical=[_customer("cc_1"), _customer("cc_2")],
        suppressed=[],
        export_rows=[_export_row("cc_1")],  # cc_2 should be here but isn't
        audit_rows=[_audit_row("cc_1", "tenant_us", "inserted", "key1")],
    )
    recs, findings = reconcile(destinations=TEST_CONFIG, **paths)
    assert recs[0].status == "variance_detected"
    missing = [f for f in findings if f.finding_type == "missing_eligible_row"]
    assert len(missing) == 1
    assert missing[0].canonical_customer_id == "cc_2"
    assert missing[0].severity == "high"


def test_privacy_suppressed_row_exported_is_critical(tmp_path):
    paths = _setup(
        tmp_path,
        canonical=[_customer("cc_1")],
        suppressed=[{"canonical_customer_id": "cc_1", "tenant_id": "tenant_us"}],
        export_rows=[_export_row("cc_1")],  # should never appear here: cc_1 is suppressed
        audit_rows=[_audit_row("cc_1", "tenant_us", "inserted", "key1")],
    )
    _recs, findings = reconcile(destinations=TEST_CONFIG, **paths)
    leaks = [f for f in findings if f.finding_type == "privacy_suppressed_row_exported"]
    assert len(leaks) == 1
    assert leaks[0].canonical_customer_id == "cc_1"
    assert leaks[0].severity == "critical"


def test_duplicate_customer_export_is_flagged(tmp_path):
    paths = _setup(
        tmp_path,
        canonical=[_customer("cc_1")],
        suppressed=[],
        export_rows=[_export_row("cc_1"), _export_row("cc_1")],
        audit_rows=[_audit_row("cc_1", "tenant_us", "inserted", "key1")],
    )
    _recs, findings = reconcile(destinations=TEST_CONFIG, **paths)
    dupes = [f for f in findings if f.finding_type == "duplicate_customer_export"]
    assert len(dupes) == 1
    assert dupes[0].canonical_customer_id == "cc_1"


def test_missing_hashed_identifier_is_flagged(tmp_path):
    paths = _setup(
        tmp_path,
        canonical=[_customer("cc_1")],
        suppressed=[],
        export_rows=[_export_row("cc_1", email_hash="", phone_hash="")],
        audit_rows=[_audit_row("cc_1", "tenant_us", "inserted", "key1")],
    )
    _recs, findings = reconcile(destinations=TEST_CONFIG, **paths)
    missing_hash = [f for f in findings if f.finding_type == "missing_hashed_identifier"]
    assert len(missing_hash) == 1


def test_wrong_tenant_row_is_flagged(tmp_path):
    paths = _setup(
        tmp_path,
        canonical=[_customer("cc_1", tenant_id="tenant_us")],
        suppressed=[],
        export_rows=[_export_row("cc_1", tenant_id="tenant_eu")],  # leaked into wrong tenant's export
        audit_rows=[_audit_row("cc_1", "tenant_eu", "inserted", "key1")],
    )
    _recs, findings = reconcile(destinations=TEST_CONFIG, **paths)
    wrong_tenant = [f for f in findings if f.finding_type == "wrong_tenant_row"]
    assert len(wrong_tenant) == 1
    assert wrong_tenant[0].severity == "critical"


def test_duplicate_idempotency_key_is_flagged(tmp_path):
    paths = _setup(
        tmp_path,
        canonical=[_customer("cc_1")],
        suppressed=[],
        export_rows=[_export_row("cc_1")],
        audit_rows=[
            _audit_row("cc_1", "tenant_us", "inserted", "same_key"),
            _audit_row("cc_1", "tenant_us", "inserted", "same_key"),
        ],
    )
    _recs, findings = reconcile(destinations=TEST_CONFIG, **paths)
    dup_keys = [f for f in findings if f.finding_type == "duplicate_idempotency_key"]
    assert len(dup_keys) == 1


def test_failed_and_duplicate_rows_count_toward_the_invariant(tmp_path):
    paths = _setup(
        tmp_path,
        canonical=[_customer("cc_1"), _customer("cc_2"), _customer("cc_3"), _customer("cc_4")],
        suppressed=[],
        export_rows=[_export_row("cc_1"), _export_row("cc_2"), _export_row("cc_3"), _export_row("cc_4")],
        audit_rows=[
            _audit_row("cc_1", "tenant_us", "inserted", "k1"),
            _audit_row("cc_2", "tenant_us", "failed", "k2"),
            _audit_row("cc_3", "tenant_us", "skipped_unchanged", "k3"),
            _audit_row("cc_4", "tenant_us", "updated", "k4"),
        ],
    )
    recs, _findings = reconcile(destinations=TEST_CONFIG, **paths)
    rec = recs[0]
    assert rec.warehouse_eligible_count == 4
    assert rec.successful_count == 2  # inserted + updated
    assert rec.failed_count == 1
    assert rec.duplicate_count == 1  # skipped_unchanged
    assert rec.variance_count == 0
    assert rec.status == "reconciled"

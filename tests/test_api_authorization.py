import csv
import json
import os
import subprocess
import sys

import pytest
from fastapi.testclient import TestClient

import api.main as api_main


PRINCIPALS = {
    "admin-test-key": {"subject": "admin", "scopes": ["*"], "global_access": True},
    "customer-test-key": {
        "subject": "customer-reader",
        "scopes": ["customer:read"],
        "allowed_tenant_ids": ["tenant_us"],
    },
    "steward-test-key": {
        "subject": "identity-steward",
        "scopes": ["stewardship:read", "stewardship:write"],
        "allowed_tenant_ids": ["tenant_us"],
    },
    "activation-test-key": {
        "subject": "activation-service",
        "scopes": ["activation:read"],
        "allowed_tenant_ids": ["tenant_us"],
    },
    "privacy-test-key": {
        "subject": "privacy-service",
        "scopes": ["privacy:write"],
        "allowed_tenant_ids": ["tenant_us"],
    },
    "observer-test-key": {
        "subject": "operator",
        "scopes": ["observability:read"],
        "allowed_tenant_ids": ["tenant_us"],
    },
    "global-activation-key": {
        "subject": "internal-global-activation",
        "scopes": ["activation:read"],
        "global_access": True,
    },
}


def _headers(key: str) -> dict[str, str]:
    return {"X-API-Key": key}


def _write_csv(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def authorized_client(tmp_path, monkeypatch):
    monkeypatch.setenv("API_PRINCIPALS_JSON", json.dumps(PRINCIPALS))
    monkeypatch.delenv("STEWARDSHIP_DSN", raising=False)
    monkeypatch.setattr(api_main, "ROOT", tmp_path)
    monkeypatch.setattr(api_main, "EXPORT_DIR", tmp_path / "reverse_etl/exports")
    monkeypatch.setattr(api_main, "REVIEW_QUEUE_PATH", tmp_path / "identity_review_queue.csv")

    canonical = [
        {"tenant_id": "tenant_us", "canonical_customer_id": "cc_us", "primary_email": "us@example.test"},
        {"tenant_id": "tenant_eu", "canonical_customer_id": "cc_eu", "primary_email": "eu@example.test"},
    ]
    _write_csv(tmp_path / "identity_resolution/output/dim_customer_canonical.csv", canonical)
    export_rows = [
        {"tenant_id": "tenant_us", "canonical_customer_id": "cc_us", "churn_risk_band": "low"},
        {"tenant_id": "tenant_eu", "canonical_customer_id": "cc_eu", "churn_risk_band": "high"},
    ]
    for filename in [
        "customer_segment_export.csv",
        "churn_risk_export.csv",
        "lifecycle_stage_export.csv",
        "customer_health_score_export.csv",
        "support_priority_export.csv",
        "campaign_target_export.csv",
    ]:
        _write_csv(api_main.EXPORT_DIR / filename, export_rows)
    operational_rows = [
        {"tenant_id": "tenant_us", "status": "success", "run_id": "run_us"},
        {"tenant_id": "tenant_eu", "status": "success", "run_id": "run_eu"},
    ]
    for path in [
        "observability/output/pipeline_run_log.csv",
        "observability/output/freshness_status.csv",
        "validation/output/quality_summary.csv",
        "reverse_etl/sync_logs/sync_run_log.csv",
        "reverse_etl/sync_logs/sync_failed_rows.csv",
        "reverse_etl/sync_logs/destination_sync_state.csv",
    ]:
        _write_csv(tmp_path / path, operational_rows)
    _write_csv(
        api_main.REVIEW_QUEUE_PATH,
        [
            {
                "review_case_id": "case_us",
                "tenant_id": "tenant_us",
                "canonical_customer_id": "cc_us",
                "candidate_customer_id": "cc_us_2",
                "source_system": "account_app",
                "source_customer_id": "src_us",
                "conflict_type": "low_confidence_match",
                "match_rule": "review",
                "confidence_score": "0.6",
                "evidence_summary": "fixture",
                "current_status": "OPEN",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "resolved_at": "",
                "reviewer": "",
                "decision": "",
                "decision_reason": "",
                "survivorship_rule": "",
                "source_event_id": "",
            },
            {
                "review_case_id": "case_eu",
                "tenant_id": "tenant_eu",
                "canonical_customer_id": "cc_eu",
                "candidate_customer_id": "cc_eu_2",
                "source_system": "account_app",
                "source_customer_id": "src_eu",
                "conflict_type": "low_confidence_match",
                "match_rule": "review",
                "confidence_score": "0.6",
                "evidence_summary": "fixture",
                "current_status": "OPEN",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "resolved_at": "",
                "reviewer": "",
                "decision": "",
                "decision_reason": "",
                "survivorship_rule": "",
                "source_event_id": "",
            },
        ],
    )
    api_main._load_csv.cache_clear()
    return TestClient(api_main.app)


def test_missing_and_invalid_api_keys_return_401(authorized_client):
    assert authorized_client.get("/exports/churn-risk").status_code == 401
    assert authorized_client.get("/exports/churn-risk", headers=_headers("invalid-key")).status_code == 401


def test_valid_principal_missing_scope_returns_403(authorized_client):
    assert authorized_client.get(
        "/activation/reconciliation", headers=_headers("customer-test-key")
    ).status_code == 403


def test_tenant_principal_omission_filters_to_allowed_tenant(authorized_client):
    response = authorized_client.get("/exports/churn-risk", headers=_headers("activation-test-key"))
    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert {row["tenant_id"] for row in response.json()["rows"]} == {"tenant_us"}


@pytest.mark.parametrize(
    "path",
    [
        "/exports/customer-segments",
        "/exports/churn-risk",
        "/exports/lifecycle-stage",
        "/exports/customer-health",
        "/exports/support-priority",
        "/activation/reconciliation",
        "/activation/runs",
    ],
)
def test_important_activation_collections_never_default_to_all_tenants(authorized_client, path):
    response = authorized_client.get(path, headers=_headers("activation-test-key"))
    assert response.status_code == 200
    assert all(row.get("tenant_id") == "tenant_us" for row in response.json()["rows"])


@pytest.mark.parametrize(
    ("path", "payload_key"),
    [
        ("/observability/pipeline-health", "rows"),
        ("/observability/freshness", "rows"),
        ("/observability/quality-summary", "rows"),
        ("/api/sync-runs", None),
    ],
)
def test_operational_collections_omit_tenant_safely(authorized_client, path, payload_key):
    response = authorized_client.get(path, headers=_headers("observer-test-key"))
    assert response.status_code == 200
    payload = response.json()
    rows = payload[payload_key] if payload_key else payload
    assert rows
    assert {row["tenant_id"] for row in rows} == {"tenant_us"}


def test_tenant_inventory_and_generic_export_are_principal_filtered(authorized_client):
    tenants = authorized_client.get("/tenants", headers=_headers("customer-test-key"))
    export = authorized_client.get(
        "/exports/campaign_target_export", headers=_headers("activation-test-key")
    )
    assert tenants.status_code == 200
    assert {row["tenant_id"] for row in tenants.json()} == {"tenant_us"}
    assert export.status_code == 200
    assert {row["tenant_id"] for row in export.json()} == {"tenant_us"}


def test_explicit_alternate_tenant_is_denied(authorized_client):
    response = authorized_client.get(
        "/exports/churn-risk",
        params={"tenant_id": "tenant_eu"},
        headers=_headers("activation-test-key"),
    )
    assert response.status_code == 403


def test_direct_foreign_customer_ids_are_not_visible(authorized_client):
    activation = authorized_client.get(
        "/api/customers/cc_eu/churn-risk", headers=_headers("activation-test-key")
    )
    customer = authorized_client.get(
        "/customers/cc_eu/profile", headers=_headers("customer-test-key")
    )
    assert activation.status_code == 404
    assert customer.status_code == 404


def test_activation_principal_is_tenant_limited_and_cannot_read_customer_surface(authorized_client):
    allowed = authorized_client.get(
        "/api/customers/cc_us/churn-risk", headers=_headers("activation-test-key")
    )
    denied_scope = authorized_client.get(
        "/customers/cc_us/profile", headers=_headers("activation-test-key")
    )
    assert allowed.status_code == 200
    assert denied_scope.status_code == 403


def test_steward_is_tenant_limited_and_cannot_submit_privacy_deletion(authorized_client):
    queue = authorized_client.get("/identity/review", headers=_headers("steward-test-key"))
    foreign = authorized_client.get(
        "/identity/review/case_eu",
        params={"tenant_id": "tenant_eu"},
        headers=_headers("steward-test-key"),
    )
    deletion = authorized_client.post(
        "/privacy/delete-request",
        headers=_headers("steward-test-key"),
        json={
            "tenant_id": "tenant_us",
            "canonical_customer_id": "cc_us",
            "email": "synthetic@example.test",
        },
    )
    foreign_decision = authorized_client.post(
        "/identity/review/case_eu/decision",
        headers=_headers("steward-test-key"),
        json={
            "tenant_id": "tenant_eu",
            "decision": "APPROVE_MERGE",
            "reviewer": "steward",
            "reason": "cross-tenant probe",
        },
    )
    assert queue.status_code == 200
    assert {row["tenant_id"] for row in queue.json()["rows"]} == {"tenant_us"}
    assert foreign.status_code == 403
    assert foreign_decision.status_code == 403
    assert deletion.status_code == 403


def test_privacy_principal_is_limited_to_authorized_tenant_customer(authorized_client):
    allowed = authorized_client.post(
        "/privacy/delete-request",
        headers=_headers("privacy-test-key"),
        json={
            "tenant_id": "tenant_us",
            "canonical_customer_id": "cc_us",
            "email": "synthetic@example.test",
        },
    )
    foreign = authorized_client.post(
        "/privacy/delete-request",
        headers=_headers("privacy-test-key"),
        json={
            "tenant_id": "tenant_eu",
            "canonical_customer_id": "cc_eu",
            "email": "synthetic@example.test",
        },
    )
    assert allowed.status_code == 200
    assert allowed.json()["tenant_id"] == "tenant_us"
    assert foreign.status_code == 403


def test_explicit_global_service_principal_reads_all_tenants(authorized_client):
    response = authorized_client.get("/exports/churn-risk", headers=_headers("global-activation-key"))
    assert response.status_code == 200
    assert response.json()["total"] == 2
    assert {row["tenant_id"] for row in response.json()["rows"]} == {"tenant_us", "tenant_eu"}


def test_observer_can_read_operations_but_activation_cannot(authorized_client):
    allowed = authorized_client.get("/api/platform-summary", headers=_headers("observer-test-key"))
    denied = authorized_client.get("/api/platform-summary", headers=_headers("activation-test-key"))
    assert allowed.status_code == 200
    assert denied.status_code == 403


def test_only_health_is_public_when_docs_are_disabled_by_default(authorized_client):
    assert authorized_client.get("/health").status_code == 200
    assert authorized_client.get("/").status_code == 401
    assert authorized_client.get("/docs").status_code == 404
    assert authorized_client.get("/redoc").status_code == 404
    assert authorized_client.get("/openapi.json").status_code == 404


def test_docs_can_be_explicitly_enabled_for_local_development():
    environment = os.environ.copy()
    environment["API_ENABLE_DOCS"] = "true"
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import api.main; print(sorted(route.path for route in api.main.app.routes if route.path in {'/docs','/redoc','/openapi.json'}))",
        ],
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "['/docs', '/openapi.json', '/redoc']"


def test_non_global_principal_configuration_requires_tenants(monkeypatch):
    monkeypatch.setenv(
        "API_PRINCIPALS_JSON",
        json.dumps({"bad-key": {"subject": "bad", "scopes": ["customer:read"]}}),
    )
    with pytest.raises(api_main.HTTPException) as exc:
        api_main._api_principals()
    assert exc.value.status_code == 503

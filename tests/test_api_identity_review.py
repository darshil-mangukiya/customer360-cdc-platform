import csv

import pytest
from fastapi.testclient import TestClient

import api.main as api_main

API_KEY_HEADERS = {"X-API-Key": "local-dev-key"}


def _write_queue(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=api_main.REVIEW_QUEUE_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def review_queue_path(tmp_path, monkeypatch):
    path = tmp_path / "identity_review_queue.csv"
    monkeypatch.setattr(api_main, "REVIEW_QUEUE_PATH", path)
    return path


def _sample_case(**overrides) -> dict:
    base = {
        "review_case_id": "revcase_test001",
        "tenant_id": "tenant_us",
        "canonical_customer_id": "cc_aaa111",
        "candidate_customer_id": "cc_bbb222",
        "source_system": "product_analytics",
        "source_customer_id": "eng_1",
        "conflict_type": "low_confidence_match",
        "match_rule": "device_id_behavioral_supporting_match",
        "confidence_score": "0.62",
        "evidence_summary": "shared device_id evidence",
        "current_status": "OPEN",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "resolved_at": "",
        "reviewer": "",
        "decision": "",
        "decision_reason": "",
        "survivorship_rule": "",
        "source_event_id": "",
    }
    base.update(overrides)
    return base


def test_identity_review_queue_requires_api_key(review_queue_path):
    client = TestClient(api_main.app)
    resp = client.get("/identity/review")
    assert resp.status_code == 401


def test_identity_review_queue_lists_and_filters_by_tenant(review_queue_path):
    _write_queue(
        review_queue_path,
        [_sample_case(review_case_id="revcase_a", tenant_id="tenant_us"), _sample_case(review_case_id="revcase_b", tenant_id="tenant_eu")],
    )
    client = TestClient(api_main.app)
    resp = client.get("/identity/review", headers=API_KEY_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2

    resp = client.get("/identity/review", params={"tenant_id": "tenant_us"}, headers=API_KEY_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["rows"][0]["tenant_id"] == "tenant_us"


def test_identity_review_case_not_found(review_queue_path):
    _write_queue(review_queue_path, [_sample_case()])
    client = TestClient(api_main.app)
    resp = client.get("/identity/review/revcase_does_not_exist", headers=API_KEY_HEADERS)
    assert resp.status_code == 404


def test_identity_review_case_cross_tenant_lookup_is_not_found(review_queue_path):
    _write_queue(review_queue_path, [_sample_case(review_case_id="revcase_x", tenant_id="tenant_us")])
    client = TestClient(api_main.app)
    resp = client.get(
        "/identity/review/revcase_x", params={"tenant_id": "tenant_eu"}, headers=API_KEY_HEADERS
    )
    assert resp.status_code == 404


def test_identity_review_decision_valid_transition_updates_status(review_queue_path):
    _write_queue(review_queue_path, [_sample_case(review_case_id="revcase_y", current_status="OPEN")])
    client = TestClient(api_main.app)

    resp = client.post(
        "/identity/review/revcase_y/decision",
        json={"tenant_id": "tenant_us", "decision": "NEEDS_REVIEW", "reviewer": "steward_1", "reason": "checking evidence"},
        headers=API_KEY_HEADERS,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["current_status"] == "IN_REVIEW"
    assert body["reviewer"] == "steward_1"

    # Verify persistence: reading the case again reflects the update.
    resp = client.get("/identity/review/revcase_y", headers=API_KEY_HEADERS)
    assert resp.json()["current_status"] == "IN_REVIEW"


def test_identity_review_decision_illegal_transition_returns_400(review_queue_path):
    _write_queue(review_queue_path, [_sample_case(review_case_id="revcase_z", current_status="RESOLVED")])
    client = TestClient(api_main.app)
    resp = client.post(
        "/identity/review/revcase_z/decision",
        json={"tenant_id": "tenant_us", "decision": "APPROVE_MERGE", "reviewer": "steward_1", "reason": "too late"},
        headers=API_KEY_HEADERS,
    )
    assert resp.status_code == 400


def test_identity_review_decision_unknown_case_returns_404(review_queue_path):
    _write_queue(review_queue_path, [_sample_case()])
    client = TestClient(api_main.app)
    resp = client.post(
        "/identity/review/does_not_exist/decision",
        json={"tenant_id": "tenant_us", "decision": "NEEDS_REVIEW", "reviewer": "steward_1", "reason": "n/a"},
        headers=API_KEY_HEADERS,
    )
    assert resp.status_code == 404


def test_health_endpoint_no_auth_required():
    client = TestClient(api_main.app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}

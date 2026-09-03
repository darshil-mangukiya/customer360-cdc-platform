import csv

import pytest
from fastapi.testclient import TestClient

import api.main as api_main

API_KEY_HEADERS = {"X-API-Key": "local-dev-key"}


def _write_csv(path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def reconciliation_root(tmp_path, monkeypatch):
    monkeypatch.setattr(api_main, "ROOT", tmp_path)
    return tmp_path


def test_activation_reconciliation_requires_api_key(reconciliation_root):
    client = TestClient(api_main.app)
    resp = client.get("/activation/reconciliation")
    assert resp.status_code == 401


def test_activation_reconciliation_filters_by_status_and_tenant(reconciliation_root):
    _write_csv(
        reconciliation_root / "reverse_etl/reconciliation/activation_reconciliation.csv",
        [
            {"run_id": "r1", "tenant_id": "tenant_us", "destination": "braze", "status": "reconciled"},
            {"run_id": "r2", "tenant_id": "tenant_eu", "destination": "braze", "status": "variance_detected"},
        ],
    )
    client = TestClient(api_main.app)
    resp = client.get("/activation/reconciliation", params={"status": "variance_detected"}, headers=API_KEY_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["rows"][0]["run_id"] == "r2"

    resp = client.get("/activation/reconciliation", params={"tenant_id": "tenant_us"}, headers=API_KEY_HEADERS)
    assert resp.json()["total"] == 1


def test_activation_runs_returns_findings_for_a_run(reconciliation_root):
    _write_csv(
        reconciliation_root / "reverse_etl/reconciliation/activation_reconciliation_findings.csv",
        [
            {"finding_id": "f1", "run_id": "r2", "tenant_id": "tenant_eu", "finding_type": "missing_eligible_row"},
            {"finding_id": "f2", "run_id": "r1", "tenant_id": "tenant_us", "finding_type": "duplicate_customer_export"},
        ],
    )
    client = TestClient(api_main.app)
    resp = client.get("/activation/runs", params={"run_id": "r2"}, headers=API_KEY_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["rows"][0]["finding_id"] == "f1"

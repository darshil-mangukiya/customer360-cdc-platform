from pathlib import Path

from privacy.deletion_workflow import build_deletion_request
from scripts.build_e2e_health_report import build_health_checks
from scripts.generate_data_product_catalog import render_catalog
from scripts.simulate_incident import run_scenarios
from validation.great_expectations_runner import run_suite


ROOT = Path(__file__).resolve().parents[1]


def test_great_expectations_suite_passes_against_smoke_outputs():
    results = run_suite()
    assert results
    assert all(result.success for result in results)


def test_e2e_health_report_has_core_checks():
    checks = build_health_checks()
    names = {check.check_name for check in checks}
    assert {"raw_cdc_landed", "canonical_customers_built", "activation_exports_present"}.issubset(names)
    assert not any(check.status == "fail" and check.severity == "critical" for check in checks)


def test_incident_simulations_cover_requested_failure_modes():
    rows = run_scenarios()
    names = {row.incident_name for row in rows}
    assert {
        "schema_drift_break",
        "duplicate_replay_spike",
        "identity_merge_anomaly_spike",
        "activation_reconciliation_mismatch",
        "stale_customer_360",
        "cross_tenant_identity_attempt",
    }.issubset(names)
    assert all(row.verification_status == "pass" for row in rows)


def test_deletion_workflow_hashes_direct_identifier():
    request = build_deletion_request(
        tenant_id="tenant_us", canonical_customer_id="cc_test", email="user@example.com"
    )
    assert request.tenant_id == "tenant_us"
    assert request.deletion_request_id.startswith("del_")
    assert len(request.email_sha256) == 64
    assert request.status == "queued_for_privacy_review"


def test_privacy_delete_make_target_requires_tenant_and_customer_identifiers():
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    target = makefile.split("privacy-delete:", 1)[1].split("\n\nairflow-proof:", 1)[0]

    assert 'test -n "$(TENANT_ID)"' in target
    assert 'test -n "$(CUSTOMER_ID)"' in target
    assert 'test -n "$(CUSTOMER_EMAIL)"' in target
    assert '--tenant-id "$(TENANT_ID)"' in target
    assert '--canonical-customer-id "$(CUSTOMER_ID)"' in target
    assert '--email "$(CUSTOMER_EMAIL)"' in target


def test_data_product_catalog_renderer_outputs_markdown():
    markdown = render_catalog(
        {
            "products": [
                {
                    "name": "mart_customer_360_current",
                    "domain": "customer_360",
                    "owner": "Data Platform",
                    "tier": "gold",
                    "grain": "one row per customer",
                    "sla": "hourly",
                    "freshness_minutes": 90,
                    "contract_status": "enforced",
                    "pii_classification": "restricted",
                    "consumers": ["analytics"],
                }
            ]
        }
    )
    assert "mart_customer_360_current" in markdown
    assert "Generated Data Product Catalog" in markdown

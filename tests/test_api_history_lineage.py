import csv

import pytest
from fastapi import HTTPException

import api.main as api


def _principal(tenant_id: str) -> api.ApiPrincipal:
    return api.ApiPrincipal(
        subject="test-customer",
        scopes=frozenset({"customer:read"}),
        allowed_tenant_ids=frozenset({tenant_id}),
    )


def _billing_mapping():
    with (api.ROOT / "identity_resolution/output/customer_identity_map.csv").open(encoding="utf-8") as fh:
        return next(row for row in csv.DictReader(fh) if row["source_system"] == "billing_platform")


def test_history_endpoint_is_point_in_time_and_tenant_scoped():
    mapping = _billing_mapping()
    result = api.customer_history(
        mapping["canonical_customer_id"],
        tenant_id=mapping["tenant_id"],
        as_of="2026-02-01T00:00:00Z",
        principal=_principal(mapping["tenant_id"]),
    )
    assert result["tenant_id"] == mapping["tenant_id"]
    assert result["history"]
    assert all(row["tenant_id"] == mapping["tenant_id"] for row in result["history"])
    with pytest.raises(HTTPException) as exc:
        api.customer_history(
            mapping["canonical_customer_id"],
            tenant_id="tenant_other",
            principal=_principal(mapping["tenant_id"]),
        )
    assert exc.value.status_code == 403


def test_lineage_endpoint_denies_cross_tenant_lookup():
    mapping = _billing_mapping()
    result = api.customer_lineage(
        mapping["canonical_customer_id"],
        tenant_id=mapping["tenant_id"],
        principal=_principal(mapping["tenant_id"]),
    )
    assert result["source_records"]
    assert result["destination_mode"] == "local_destination_simulator"
    with pytest.raises(HTTPException) as exc:
        api.customer_lineage(
            mapping["canonical_customer_id"],
            tenant_id="tenant_other",
            principal=_principal(mapping["tenant_id"]),
        )
    assert exc.value.status_code == 403

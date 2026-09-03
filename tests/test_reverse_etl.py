from data_generation.cdc_generator import build_cdc_events
from identity_resolution.resolver import resolve_identity
from ingestion.loader import normalize_records
from reverse_etl.exporter import build_activation_rows


def test_reverse_etl_rows_are_activation_ready():
    landed, _ = normalize_records([event.__dict__ for event in build_cdc_events(seed=19)])
    canonical, mappings, _ = resolve_identity(landed)
    rows = build_activation_rows(landed, canonical, mappings)
    assert rows
    assert {row.churn_risk_band for row in rows}.issubset({"low", "medium", "high"})
    assert all(row.last_refresh_time for row in rows)
    assert all(row.source_lineage_refs for row in rows)
    assert any(row.email_sha256 for row in rows)

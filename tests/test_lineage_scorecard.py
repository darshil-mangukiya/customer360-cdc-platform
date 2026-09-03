from observability.openlineage_events import build_lineage_edges
from validation.scorecard import DOMAIN_WEIGHTS, build_scorecard


def test_machine_readable_lineage_is_tenant_scoped_and_complete():
    edges = build_lineage_edges(
        run_id="run_001",
        tenant_counts={"tenant_emea": 2, "tenant_us": 3},
        observed_at="2026-01-01T00:00:00Z",
    )
    assert len(edges) == 10
    assert {row.tenant_id for row in edges} == {"tenant_emea", "tenant_us"}
    assert all(row.status == "success" and row.record_count > 0 for row in edges)
    assert any(row.target_name == "destination_sync_state" for row in edges)


def test_quality_scorecard_formula_is_reproducible():
    results = {domain: [True, False] for domain in DOMAIN_WEIGHTS}
    rows, overall = build_scorecard(results)
    assert all(row.score_pct == 50.0 for row in rows)
    assert overall == 50.0
    assert sum(row.weight_pct for row in rows) == 100

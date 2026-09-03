from __future__ import annotations

import csv
import json
import os
import secrets
from dataclasses import asdict, dataclass
from functools import lru_cache
from html import escape
from pathlib import Path
from typing import Any

from customer360.history import build_scd2_history, point_in_time

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from api.export_registry import resolve_export_filename
from identity_resolution import config as identity_config
from identity_resolution.repository import (
    FileReviewCaseRepository,
    PostgresReviewCaseRepository,
    ReviewCaseNotFoundError,
)
from identity_resolution.stewardship import ReviewCase
from privacy.deletion_workflow import build_deletion_request, write_deletion_outputs


ROOT = Path(__file__).resolve().parents[1]
EXPORT_DIR = Path(os.getenv("ACTIVATION_EXPORT_DIR", "reverse_etl/exports"))
if not EXPORT_DIR.is_absolute():
    EXPORT_DIR = ROOT / EXPORT_DIR

API_DOCS_ENABLED = os.getenv("API_ENABLE_DOCS", "false").strip().lower() in {"1", "true", "yes"}

app = FastAPI(
    title="Customer 360 Activation API",
    description="Tenant-aware Customer 360 data product and reverse ETL activation API for local technical validation.",
    version="0.2.0",
    docs_url="/docs" if API_DOCS_ENABLED else None,
    redoc_url="/redoc" if API_DOCS_ENABLED else None,
    openapi_url="/openapi.json" if API_DOCS_ENABLED else None,
)


class PaginatedResponse(BaseModel):
    tenant_id: str | None = None
    limit: int
    offset: int
    total: int
    rows: list[dict[str, Any]]


class PrivacyDeleteRequest(BaseModel):
    tenant_id: str = Field(..., min_length=1)
    canonical_customer_id: str = Field(..., min_length=3)
    email: str = Field(..., min_length=3)
    request_type: str = "gdpr_erasure"


class PrivacyDeleteResponse(BaseModel):
    deletion_request_id: str
    tenant_id: str
    canonical_customer_id: str
    status: str
    handling_notes: str


class IdentityReviewDecisionRequest(BaseModel):
    decision: str = Field(..., description=f"One of {identity_config.REVIEW_DECISIONS}")
    reviewer: str = Field(..., min_length=1)
    reason: str = Field(..., min_length=1)
    tenant_id: str | None = Field(
        default=None, description="If set, the request is rejected unless it matches the case's tenant_id."
    )


class IdentityReviewCaseResponse(BaseModel):
    review_case_id: str
    tenant_id: str | None
    canonical_customer_id: str
    candidate_customer_id: str
    source_system: str
    source_customer_id: str
    conflict_type: str
    match_rule: str
    confidence_score: float
    evidence_summary: str
    current_status: str
    created_at: str
    updated_at: str
    resolved_at: str | None
    reviewer: str | None
    decision: str | None
    decision_reason: str | None
    survivorship_rule: str | None
    source_event_id: str | None


@dataclass(frozen=True)
class ApiPrincipal:
    subject: str
    scopes: frozenset[str]
    allowed_tenant_ids: frozenset[str]
    global_access: bool = False


def _api_principals() -> dict[str, ApiPrincipal]:
    """Load API-key principals without exposing key material in responses or logs.

    ``API_PRINCIPALS_JSON`` is a JSON object keyed by secret API key. Each value
    contains a non-secret subject, scopes, allowed tenant IDs, and an explicit
    ``global_access`` flag for the rare trusted service principal. The single local
    fallback preserves the offline proof workflow and is a global administrator only
    in that explicitly local mode; deployed environments must inject managed keys.
    """
    raw = os.getenv("API_PRINCIPALS_JSON")
    if not raw:
        local_key = os.getenv("ACTIVATION_API_KEY", "local-dev-key")
        return {
            local_key: ApiPrincipal(
                subject="local-proof-admin",
                scopes=frozenset({"*"}),
                allowed_tenant_ids=frozenset(),
                global_access=True,
            )
        }
    try:
        configured = json.loads(raw)
        if not isinstance(configured, dict):
            raise ValueError("principal registry must be an object")
        principals: dict[str, ApiPrincipal] = {}
        for api_key, value in configured.items():
            if not isinstance(value, dict) or not value.get("subject") or not isinstance(value.get("scopes"), list):
                raise ValueError("each principal requires subject and scopes")
            global_access = value.get("global_access") is True
            tenant_values = value.get("allowed_tenant_ids", [])
            if not isinstance(tenant_values, list) or any(not isinstance(item, str) or not item for item in tenant_values):
                raise ValueError("allowed_tenant_ids must be a list of non-empty strings")
            allowed_tenant_ids = frozenset(tenant_values)
            if not global_access and not allowed_tenant_ids:
                raise ValueError("non-global principals require at least one allowed tenant")
            principals[str(api_key)] = ApiPrincipal(
                subject=str(value["subject"]),
                scopes=frozenset(str(scope) for scope in value["scopes"]),
                allowed_tenant_ids=allowed_tenant_ids,
                global_access=global_access,
            )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=503, detail="API authorization is not configured") from exc
    if not principals:
        raise HTTPException(status_code=503, detail="API authorization is not configured")
    return principals


def require_api_key(x_api_key: str | None = Header(default=None)) -> ApiPrincipal:
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
        )
    for candidate, principal in _api_principals().items():
        if secrets.compare_digest(x_api_key, candidate):
            return principal
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="missing or invalid API key",
    )


def require_scopes(*required_scopes: str):
    required = frozenset(required_scopes)

    def authorize(principal: ApiPrincipal = Depends(require_api_key)) -> ApiPrincipal:
        if "*" not in principal.scopes and not required.issubset(principal.scopes):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="insufficient API scope",
            )
        return principal

    return authorize


def _authorized_tenant_ids(
    principal: ApiPrincipal,
    tenant_id: str | None = None,
    *,
    require_explicit_for_global: bool = False,
) -> frozenset[str] | None:
    """Return the trusted tenant scope; ``None`` means explicit global access."""
    if tenant_id is not None:
        if not principal.global_access and tenant_id not in principal.allowed_tenant_ids:
            raise HTTPException(status_code=403, detail="tenant access denied")
        return frozenset({tenant_id})
    if principal.global_access:
        if require_explicit_for_global:
            raise HTTPException(status_code=400, detail="tenant_id is required for this operation")
        return None
    return principal.allowed_tenant_ids


def _tenant_rows(
    rows: list[dict[str, Any]], principal: ApiPrincipal, tenant_id: str | None = None
) -> list[dict[str, Any]]:
    allowed = _authorized_tenant_ids(principal, tenant_id)
    if allowed is None:
        return rows
    return [row for row in rows if row.get("tenant_id") in allowed]


def _authorize_direct_row(
    row: dict[str, Any] | None,
    principal: ApiPrincipal,
    tenant_id: str | None = None,
    *,
    not_found_detail: str = "resource not found",
) -> dict[str, Any]:
    allowed = _authorized_tenant_ids(principal, tenant_id)
    if row is None:
        raise HTTPException(status_code=404, detail=not_found_detail)
    row_tenant = row.get("tenant_id")
    if tenant_id is not None and row_tenant != tenant_id:
        raise HTTPException(status_code=404, detail=not_found_detail)
    if allowed is not None and row_tenant not in allowed:
        raise HTTPException(status_code=404, detail=not_found_detail)
    return row


@lru_cache(maxsize=32)
def _load_csv(filename: str) -> list[dict[str, Any]]:
    path = EXPORT_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"export not found: {filename}")
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _load_local_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _filter_rows(
    rows: list[dict[str, Any]],
    *,
    principal: ApiPrincipal,
    tenant_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> PaginatedResponse:
    filtered = _tenant_rows(rows, principal, tenant_id)
    return PaginatedResponse(
        tenant_id=tenant_id,
        limit=limit,
        offset=offset,
        total=len(filtered),
        rows=filtered[offset : offset + limit],
    )


def _tenant_ids(principal: ApiPrincipal) -> list[str]:
    canonical = _load_local_csv(ROOT / "identity_resolution/output/dim_customer_canonical.csv")
    tenants = {row.get("tenant_id") for row in canonical if row.get("tenant_id")}
    for path in EXPORT_DIR.glob("*_export.csv"):
        tenants.update(row.get("tenant_id") for row in _load_local_csv(path) if row.get("tenant_id"))
    if principal.global_access:
        return sorted(tenants)
    return sorted(tenants & principal.allowed_tenant_ids)


def _count_jsonl(path: Path, principal: ApiPrincipal | None = None) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as fh:
        rows = [json.loads(line) for line in fh if line.strip()]
    if principal is None:
        return len(rows)
    allowed = _authorized_tenant_ids(principal)
    if allowed is None:
        return len(rows)
    return sum(
        1
        for row in rows
        if (row.get("tenant_id") or (row.get("raw_event") or {}).get("tenant_id")) in allowed
    )


def _activation_export_counts(principal: ApiPrincipal) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(EXPORT_DIR.glob("*_export.csv")):
        data = _tenant_rows(_load_local_csv(path), principal)
        latest_export = max((row.get("export_timestamp", "") for row in data), default="")
        tenant_count = len({row.get("tenant_id") for row in data if row.get("tenant_id")})
        rows.append(
            {
                "export_name": path.stem,
                "row_count": len(data),
                "tenant_count": tenant_count,
                "latest_export_timestamp": latest_export,
            }
        )
    return rows


REVIEW_QUEUE_PATH = ROOT / "identity_resolution/output/identity_review_queue.csv"
REVIEW_QUEUE_FIELDS = list(ReviewCase.__dataclass_fields__.keys())


def _review_repository() -> FileReviewCaseRepository | PostgresReviewCaseRepository:
    dsn = os.getenv("STEWARDSHIP_DSN")
    return PostgresReviewCaseRepository(dsn) if dsn else FileReviewCaseRepository(REVIEW_QUEUE_PATH)


def _find_export_row(
    filename: str,
    canonical_customer_id: str,
    principal: ApiPrincipal,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    rows = _load_csv(filename)
    match = next((row for row in rows if row.get("canonical_customer_id") == canonical_customer_id), None)
    return _authorize_direct_row(
        match,
        principal,
        tenant_id,
        not_found_detail=f"customer not found in {filename}",
    )


def _table(rows: list[dict[str, Any]], columns: list[str], limit: int = 8) -> str:
    if not rows:
        return "<p class='muted'>No rows available.</p>"
    header = "".join(f"<th>{escape(col)}</th>" for col in columns)
    body = "".join(
        "<tr>" + "".join(f"<td>{escape(str(row.get(col, '')))}</td>" for col in columns) + "</tr>"
        for row in rows[:limit]
    )
    return f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"


def _metric_card(label: str, value: Any) -> str:
    return f"<div class='card'><span>{escape(label)}</span><strong>{escape(str(value))}</strong></div>"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/tenants")
def tenants(principal: ApiPrincipal = Depends(require_scopes("customer:read"))) -> list[dict[str, Any]]:
    return [
        {
            "tenant_id": tenant_id,
            "is_active": True,
            "api_scope": "local_demo",
            "isolation_strategy": "tenant_id filtered shared warehouse schema",
        }
        for tenant_id in _tenant_ids(principal)
    ]


@app.get("/", response_class=HTMLResponse, dependencies=[Depends(require_scopes("observability:read"))])
def root_dashboard_link() -> str:
    return "<meta http-equiv='refresh' content='0; url=/dashboard'>"


@app.get("/api/platform-summary")
def platform_summary(
    principal: ApiPrincipal = Depends(require_scopes("observability:read")),
) -> dict[str, Any]:
    ingestion_log = _tenant_rows(_load_local_csv(ROOT / "ingestion/output/ingestion_log.csv"), principal)
    canonical = _tenant_rows(
        _load_local_csv(ROOT / "identity_resolution/output/dim_customer_canonical.csv"), principal
    )
    churn = _tenant_rows(_load_local_csv(EXPORT_DIR / "churn_risk_export.csv"), principal)
    quality = _tenant_rows(_load_local_csv(ROOT / "validation/output/quality_summary.csv"), principal)
    freshness = _tenant_rows(_load_local_csv(ROOT / "observability/output/freshness_status.csv"), principal)
    identity_map = _tenant_rows(
        _load_local_csv(ROOT / "identity_resolution/output/customer_identity_map.csv"), principal
    )
    sync_runs = _tenant_rows(_load_local_csv(ROOT / "reverse_etl/sync_logs/sync_run_log.csv"), principal)
    sync_state = _tenant_rows(
        _load_local_csv(ROOT / "reverse_etl/sync_logs/destination_sync_state.csv"), principal
    )
    drift = _load_local_csv(ROOT / "data_generation/schema_drift_output/schema_drift_results.csv")
    benchmark = _load_local_csv(ROOT / "benchmark/output/benchmark_summary.csv")
    return {
        "landed_cdc_events": sum(int(row.get("landed_count") or 0) for row in ingestion_log),
        "rejected_events": _count_jsonl(
            ROOT / "ingestion/output/rejected_events.jsonl", principal
        ),
        "canonical_customers": len(canonical),
        "identity_mappings": len(identity_map),
        "high_churn_risk_customers": sum(1 for row in churn if row.get("churn_risk_band") == "high"),
        "failed_quality_checks": sum(1 for row in quality if row.get("status") == "fail"),
        "stale_cdc_sources": sum(1 for row in freshness if str(row.get("status", "")).startswith("stale")),
        "activation_exports": len(_activation_export_counts(principal)),
        "destination_sync_runs": len(sync_runs),
        "destination_sync_failures": sum(int(row.get("failed_count") or 0) for row in sync_runs),
        "destination_state_rows": len(sync_state),
        "schema_drift_scenarios": len(drift),
        "benchmark_stages": len(benchmark),
    }


@app.get("/api/sync-runs")
def sync_runs(
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    principal: ApiPrincipal = Depends(require_scopes("observability:read")),
) -> list[dict[str, Any]]:
    return _tenant_rows(
        _load_local_csv(ROOT / "reverse_etl/sync_logs/sync_run_log.csv"), principal, tenant_id
    )[:limit]


@app.get("/api/destination-sync-status")
def destination_sync_status(
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    principal: ApiPrincipal = Depends(require_scopes("observability:read")),
) -> dict[str, Any]:
    runs = _tenant_rows(
        _load_local_csv(ROOT / "reverse_etl/sync_logs/sync_run_log.csv"), principal, tenant_id
    )[:limit]
    failed_rows = _tenant_rows(
        _load_local_csv(ROOT / "reverse_etl/sync_logs/sync_failed_rows.csv"), principal, tenant_id
    )[:limit]
    state = _tenant_rows(
        _load_local_csv(ROOT / "reverse_etl/sync_logs/destination_sync_state.csv"), principal, tenant_id
    )[:limit]
    return {"sync_runs": runs, "failed_rows": failed_rows, "destination_state": state}


@app.get("/api/export-freshness")
def export_freshness(
    principal: ApiPrincipal = Depends(require_scopes("observability:read")),
) -> list[dict[str, Any]]:
    return _activation_export_counts(principal)


@app.get("/api/schema-drift", dependencies=[Depends(require_scopes("observability:read"))])
def schema_drift(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
    return _load_local_csv(ROOT / "data_generation/schema_drift_output/schema_drift_results.csv")[:limit]


@app.get("/api/benchmark", dependencies=[Depends(require_scopes("observability:read"))])
def benchmark(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, Any]]:
    return _load_local_csv(ROOT / "benchmark/output/benchmark_summary.csv")[:limit]


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    principal: ApiPrincipal = Depends(require_scopes("observability:read")),
) -> str:
    summary = platform_summary(principal)
    quality = _tenant_rows(_load_local_csv(ROOT / "validation/output/quality_summary.csv"), principal)
    freshness = _tenant_rows(_load_local_csv(ROOT / "observability/output/freshness_status.csv"), principal)
    sync = _tenant_rows(_load_local_csv(ROOT / "reverse_etl/sync_logs/sync_run_log.csv"), principal)
    sync_state = _tenant_rows(
        _load_local_csv(ROOT / "reverse_etl/sync_logs/destination_sync_state.csv"), principal
    )
    activation_counts = _activation_export_counts(principal)
    drift = _load_local_csv(ROOT / "data_generation/schema_drift_output/schema_drift_results.csv")
    benchmark_rows = _load_local_csv(ROOT / "benchmark/output/benchmark_summary.csv")
    churn = _tenant_rows(_load_local_csv(EXPORT_DIR / "churn_risk_export.csv"), principal)
    css = """
    body { margin: 0; font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #15252d; background: #f5f7f8; }
    header { padding: 30px 40px; background: #173b45; color: white; }
    h1 { margin: 0 0 6px; font-size: 30px; }
    main { padding: 26px 40px 44px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 14px; margin-bottom: 28px; }
    .card { background: white; border: 1px solid #dbe5e8; border-radius: 8px; padding: 16px; }
    .card span { display: block; color: #667980; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .card strong { display: block; margin-top: 7px; font-size: 26px; color: #173b45; }
    section { margin: 0 0 28px; }
    h2 { margin: 0 0 12px; font-size: 20px; color: #173b45; }
    table { width: 100%; border-collapse: collapse; background: white; border: 1px solid #dbe5e8; border-radius: 8px; overflow: hidden; }
    th, td { padding: 10px 12px; border-bottom: 1px solid #edf1f3; text-align: left; font-size: 13px; vertical-align: top; }
    th { background: #e8eff1; }
    .muted { color: #687a82; }
    """
    cards = "".join(_metric_card(key.replace("_", " ").title(), value) for key, value in summary.items())
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Customer 360 Control Room</title><style>{css}</style></head>
<body>
<header><h1>Customer 360 Control Room</h1><p>Pipeline health, activation syncs, schema drift, benchmark, and churn-risk outputs.</p></header>
<main>
  <div class="grid">{cards}</div>
  <section><h2>CDC Freshness</h2>{_table(freshness, ["tenant_id", "entity_name", "max_event_timestamp", "lag_minutes", "status"])}</section>
  <section><h2>Activation Exports</h2>{_table(activation_counts, ["export_name", "row_count", "tenant_count", "latest_export_timestamp"])}</section>
  <section><h2>Quality Checks</h2>{_table(quality, ["check_name", "severity", "status", "failure_count"])}</section>
  <section><h2>Destination Sync Runs</h2>{_table(sync, ["destination_name", "export_file", "attempted_count", "success_count", "failed_count", "inserted_count", "updated_count", "skipped_count", "retry_count", "sync_status"])}</section>
  <section><h2>Destination State</h2>{_table(sync_state, ["destination_name", "export_file", "canonical_customer_id", "sync_action", "last_synced_at"], limit=8)}</section>
  <section><h2>Schema Drift</h2>{_table(drift, ["scenario_name", "drift_type", "expected_status", "actual_status", "passed_expectation"])}</section>
  <section><h2>Benchmark</h2>{_table(benchmark_rows, ["stage_name", "row_count", "elapsed_seconds", "rows_per_second"])}</section>
  <section><h2>Churn Risk Sample</h2>{_table(churn, ["canonical_customer_id", "tenant_id", "churn_risk_score", "churn_risk_band"], limit=6)}</section>
</main>
</body>
</html>"""


@app.get("/customers/{canonical_customer_id}/activation")
def get_customer_activation(
    canonical_customer_id: str,
    tenant_id: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> dict[str, Any]:
    payload: dict[str, Any] = {"canonical_customer_id": canonical_customer_id}
    for filename in [
        "customer_segment_export.csv",
        "churn_risk_export.csv",
        "lifecycle_stage_export.csv",
        "customer_health_score_export.csv",
        "support_priority_export.csv",
        "campaign_target_export.csv",
    ]:
        rows = _load_csv(filename)
        match = next(
            (
                row
                for row in _tenant_rows(rows, principal, tenant_id)
                if row["canonical_customer_id"] == canonical_customer_id
            ),
            None,
        )
        if match:
            payload[filename.replace("_export.csv", "")] = match
    if len(payload) == 1:
        raise HTTPException(status_code=404, detail="customer not found in activation exports")
    return payload


@app.get("/api/customers/{canonical_customer_id}/activation-profile")
def get_activation_profile(
    canonical_customer_id: str,
    tenant_id: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> dict[str, Any]:
    return get_customer_activation(canonical_customer_id, tenant_id, principal)


@app.get("/api/customers/{canonical_customer_id}/churn-risk")
def get_churn_risk(
    canonical_customer_id: str,
    tenant_id: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> dict[str, Any]:
    return _find_export_row("churn_risk_export.csv", canonical_customer_id, principal, tenant_id)


@app.get("/api/customers/{canonical_customer_id}/segment")
def get_customer_segment(
    canonical_customer_id: str,
    tenant_id: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> dict[str, Any]:
    return _find_export_row("customer_segment_export.csv", canonical_customer_id, principal, tenant_id)


@app.get("/api/customers/{canonical_customer_id}/health-score")
def get_customer_health_score(
    canonical_customer_id: str,
    tenant_id: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> dict[str, Any]:
    return _find_export_row("customer_health_score_export.csv", canonical_customer_id, principal, tenant_id)


@app.get("/api/customers/{canonical_customer_id}/support-priority")
def get_support_priority(
    canonical_customer_id: str,
    tenant_id: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> dict[str, Any]:
    return _find_export_row("support_priority_export.csv", canonical_customer_id, principal, tenant_id)


@app.get("/exports/customer-segments")
def export_customer_segments(
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> PaginatedResponse:
    return _filter_rows(
        _load_csv("customer_segment_export.csv"),
        principal=principal,
        tenant_id=tenant_id,
        limit=limit,
        offset=offset,
    )


@app.get("/exports/churn-risk")
def export_churn_risk(
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> PaginatedResponse:
    return _filter_rows(
        _load_csv("churn_risk_export.csv"),
        principal=principal,
        tenant_id=tenant_id,
        limit=limit,
        offset=offset,
    )


@app.get("/exports/lifecycle-stage")
def export_lifecycle_stage(
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> PaginatedResponse:
    return _filter_rows(
        _load_csv("lifecycle_stage_export.csv"),
        principal=principal,
        tenant_id=tenant_id,
        limit=limit,
        offset=offset,
    )


@app.get("/exports/customer-health")
def export_customer_health(
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> PaginatedResponse:
    return _filter_rows(
        _load_csv("customer_health_score_export.csv"),
        principal=principal,
        tenant_id=tenant_id,
        limit=limit,
        offset=offset,
    )


@app.get("/exports/support-priority")
def export_support_priority(
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> PaginatedResponse:
    return _filter_rows(
        _load_csv("support_priority_export.csv"),
        principal=principal,
        tenant_id=tenant_id,
        limit=limit,
        offset=offset,
    )


@app.get("/customers/{canonical_customer_id}/profile")
def customer_profile(
    canonical_customer_id: str,
    tenant_id: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("customer:read")),
) -> dict[str, Any]:
    canonical = _load_local_csv(ROOT / "identity_resolution/output/dim_customer_canonical.csv")
    customer = next((row for row in canonical if row.get("canonical_customer_id") == canonical_customer_id), None)
    customer = _authorize_direct_row(
        customer, principal, tenant_id, not_found_detail="customer not found"
    )
    return {
        "canonical_customer": customer,
        "activation_profile": get_customer_activation(
            canonical_customer_id, customer.get("tenant_id"), principal
        ),
    }


@app.get("/customers/{canonical_customer_id}/timeline")
def customer_timeline(
    canonical_customer_id: str,
    tenant_id: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("customer:read")),
) -> dict[str, Any]:
    allowed = _authorized_tenant_ids(principal, tenant_id)
    mappings = _load_local_csv(ROOT / "identity_resolution/output/customer_identity_map.csv")
    source_refs = {
        (row.get("source_system"), row.get("source_record_id"))
        for row in mappings
        if row.get("canonical_customer_id") == canonical_customer_id
        and (allowed is None or row.get("tenant_id") in allowed)
    }
    if not source_refs:
        raise HTTPException(status_code=404, detail="customer timeline not found")
    events_path = ROOT / "ingestion/output/raw_cdc_events.jsonl"
    events: list[dict[str, Any]] = []
    if events_path.exists():
        with events_path.open(encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                event = json.loads(line)
                if allowed is not None and event.get("tenant_id") not in allowed:
                    continue
                if (event.get("source_system"), event.get("record_primary_key")) in source_refs:
                    events.append(
                        {
                            "event_id": event.get("event_id"),
                            "tenant_id": event.get("tenant_id"),
                            "source_system": event.get("source_system"),
                            "source_table": event.get("source_table"),
                            "operation_type": event.get("operation_type"),
                            "event_timestamp": event.get("event_timestamp"),
                            "source_lsn": event.get("source_lsn"),
                            "kafka_topic": event.get("kafka_topic") or event.get("topic_name"),
                            "kafka_offset": event.get("kafka_offset"),
                            "is_replay": event.get("is_replay"),
                        }
                    )
    return {
        "canonical_customer_id": canonical_customer_id,
        "event_count": len(events),
        "events": sorted(events, key=lambda row: row.get("event_timestamp") or ""),
    }


@app.get("/customers/{canonical_customer_id}/history")
def customer_history(
    canonical_customer_id: str,
    tenant_id: str = Query(..., min_length=1),
    as_of: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("customer:read")),
) -> dict[str, Any]:
    """Tenant-scoped subscription history with an optional point-in-time answer."""
    _authorized_tenant_ids(principal, tenant_id)
    mappings = _load_local_csv(ROOT / "identity_resolution/output/customer_identity_map.csv")
    source_refs = {
        (row.get("source_system"), row.get("source_record_id"))
        for row in mappings
        if row.get("canonical_customer_id") == canonical_customer_id and row.get("tenant_id") == tenant_id
    }
    changes: list[dict[str, Any]] = []
    events_path = ROOT / "ingestion/output/raw_cdc_events.jsonl"
    if events_path.exists():
        with events_path.open(encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                event = json.loads(line)
                if event.get("tenant_id") != tenant_id or event.get("source_table") != "subscriptions":
                    continue
                if (event.get("source_system"), event.get("record_primary_key")) not in source_refs:
                    continue
                payload = event.get("payload_after") or event.get("payload_before") or {}
                changes.append(
                    {
                        "tenant_id": tenant_id,
                        "entity_id": canonical_customer_id,
                        "effective_timestamp": event["event_timestamp"],
                        "subscription_status": payload.get("subscription_status"),
                        "source_event_id": event["event_id"],
                        "change_reason": "source_delete" if event.get("operation_type") == "delete" else event.get("operation_type"),
                        "is_deleted": event.get("operation_type") == "delete",
                        "attributes": {"plan_name": payload.get("plan_name"), "mrr": payload.get("mrr")},
                    }
                )
    if not changes:
        raise HTTPException(status_code=404, detail="customer history not found for this tenant")
    history = build_scd2_history(changes, state_field="subscription_status")
    selected = point_in_time(
        history,
        tenant_id=tenant_id,
        entity_id=canonical_customer_id,
        as_of_timestamp=as_of,
    ) if as_of else None
    return {
        "canonical_customer_id": canonical_customer_id,
        "tenant_id": tenant_id,
        "as_of_timestamp": as_of,
        "as_of_state": asdict(selected) if selected else None,
        "history": [asdict(row) for row in history],
    }


@app.get("/customers/{canonical_customer_id}/identity-lineage")
def customer_identity_lineage(
    canonical_customer_id: str,
    tenant_id: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("customer:read")),
) -> dict[str, Any]:
    allowed = _authorized_tenant_ids(principal, tenant_id)
    explanations = _load_local_csv(ROOT / "identity_resolution/output/identity_link_explanation.csv")
    rows = [
        row
        for row in explanations
        if row.get("canonical_customer_id") == canonical_customer_id
        and (allowed is None or row.get("tenant_id") in allowed)
    ]
    if not rows:
        raise HTTPException(status_code=404, detail="identity lineage not found")
    return {"canonical_customer_id": canonical_customer_id, "link_explanations": rows}


@app.get("/lineage/{canonical_customer_id}")
def customer_lineage(
    canonical_customer_id: str,
    tenant_id: str = Query(..., min_length=1),
    principal: ApiPrincipal = Depends(require_scopes("customer:read")),
) -> dict[str, Any]:
    _authorized_tenant_ids(principal, tenant_id)
    mappings = [
        row
        for row in _load_local_csv(ROOT / "identity_resolution/output/customer_identity_map.csv")
        if row.get("canonical_customer_id") == canonical_customer_id and row.get("tenant_id") == tenant_id
    ]
    if not mappings:
        raise HTTPException(status_code=404, detail="customer lineage not found for this tenant")
    source_refs = {(row.get("source_system"), row.get("source_record_id")) for row in mappings}
    source_events: list[dict[str, Any]] = []
    events_path = ROOT / "ingestion/output/raw_cdc_events.jsonl"
    if events_path.exists():
        with events_path.open(encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                event = json.loads(line)
                if event.get("tenant_id") == tenant_id and (
                    event.get("source_system"), event.get("record_primary_key")
                ) in source_refs:
                    source_events.append(
                        {
                            "event_id": event.get("event_id"),
                            "source_system": event.get("source_system"),
                            "source_table": event.get("source_table"),
                            "event_timestamp": event.get("event_timestamp"),
                        }
                    )
    activation_outputs = []
    for export_path in sorted(EXPORT_DIR.glob("*.csv")):
        if any(
            row.get("canonical_customer_id") == canonical_customer_id and row.get("tenant_id") == tenant_id
            for row in _load_local_csv(export_path)
        ):
            activation_outputs.append(export_path.stem)
    return {
        "canonical_customer_id": canonical_customer_id,
        "tenant_id": tenant_id,
        "source_records": mappings,
        "cdc_events": source_events,
        "warehouse_models": ["identity.dim_customer_canonical", "mart.mart_customer_360_current"],
        "activation_outputs": activation_outputs,
        "destination_mode": "local_destination_simulator",
    }


@app.get("/identity/review")
def identity_review_queue(
    tenant_id: str | None = None,
    case_status: str | None = Query(
        default=None, alias="status", description=f"Filter by status, one of {identity_config.REVIEW_STATUSES}"
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("stewardship:read")),
) -> PaginatedResponse:
    allowed = _authorized_tenant_ids(principal, tenant_id)
    if allowed is not None:
        if tenant_id is None and len(allowed) != 1:
            raise HTTPException(status_code=400, detail="tenant_id is required for multi-tenant stewardship")
        tenant_id = next(iter(allowed))
    cases, total = _review_repository().list_cases(
        tenant_id=tenant_id, case_status=case_status, limit=limit, offset=offset
    )
    return PaginatedResponse(
        tenant_id=tenant_id,
        limit=limit,
        offset=offset,
        total=total,
        rows=[asdict(case) for case in cases],
    )


@app.get(
    "/identity/review/{review_case_id}",
    response_model=IdentityReviewCaseResponse,
)
def identity_review_case(
    review_case_id: str,
    tenant_id: str | None = None,
    principal: ApiPrincipal = Depends(require_scopes("stewardship:read")),
) -> IdentityReviewCaseResponse:
    allowed = _authorized_tenant_ids(principal, tenant_id)
    if allowed is not None:
        if tenant_id is None and len(allowed) != 1:
            raise HTTPException(status_code=400, detail="tenant_id is required for multi-tenant stewardship")
        tenant_id = next(iter(allowed))
    case = _review_repository().get_case(review_case_id, tenant_id=tenant_id)
    if not case:
        raise HTTPException(status_code=404, detail="review case not found")
    return IdentityReviewCaseResponse(**asdict(case))


@app.post(
    "/identity/review/{review_case_id}/decision",
    response_model=IdentityReviewCaseResponse,
)
def identity_review_decision(
    review_case_id: str,
    request: IdentityReviewDecisionRequest,
    principal: ApiPrincipal = Depends(require_scopes("stewardship:write")),
) -> IdentityReviewCaseResponse:
    """Apply a tenant-scoped decision through the configured stewardship repository."""
    allowed = _authorized_tenant_ids(
        principal, request.tenant_id, require_explicit_for_global=True
    )
    decision_tenant_id = request.tenant_id or next(iter(allowed or ()), None)
    try:
        updated_case = _review_repository().decide(
            review_case_id,
            tenant_id=decision_tenant_id,
            decision=request.decision,
            reviewer=request.reviewer,
            reason=request.reason,
        )
    except ReviewCaseNotFoundError as exc:
        raise HTTPException(status_code=404, detail="review case not found for this tenant") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return IdentityReviewCaseResponse(**asdict(updated_case))


@app.post("/privacy/delete-request")
def privacy_delete_request(
    request: PrivacyDeleteRequest,
    principal: ApiPrincipal = Depends(require_scopes("privacy:write")),
) -> PrivacyDeleteResponse:
    _authorized_tenant_ids(principal, request.tenant_id)
    canonical = _load_local_csv(ROOT / "identity_resolution/output/dim_customer_canonical.csv")
    customer = next(
        (row for row in canonical if row.get("canonical_customer_id") == request.canonical_customer_id),
        None,
    )
    _authorize_direct_row(
        customer,
        principal,
        request.tenant_id,
        not_found_detail="customer not found for this tenant",
    )
    deletion = build_deletion_request(
        tenant_id=request.tenant_id,
        canonical_customer_id=request.canonical_customer_id,
        email=request.email,
        request_type=request.request_type,
    )
    write_deletion_outputs(deletion, ROOT / "privacy/output")
    return PrivacyDeleteResponse(
        deletion_request_id=deletion.deletion_request_id,
        tenant_id=deletion.tenant_id,
        canonical_customer_id=deletion.canonical_customer_id,
        status=deletion.status,
        handling_notes=deletion.handling_notes,
    )


@app.get(
    "/observability/pipeline-health",
)
def observability_pipeline_health(
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("observability:read")),
) -> PaginatedResponse:
    return _filter_rows(
        _load_local_csv(ROOT / "observability/output/pipeline_run_log.csv"),
        principal=principal,
        tenant_id=tenant_id,
        limit=limit,
        offset=offset,
    )


@app.get("/observability/freshness")
def observability_freshness(
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("observability:read")),
) -> PaginatedResponse:
    return _filter_rows(
        _load_local_csv(ROOT / "observability/output/freshness_status.csv"),
        principal=principal,
        tenant_id=tenant_id,
        limit=limit,
        offset=offset,
    )


@app.get(
    "/observability/quality-summary",
)
def observability_quality_summary(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("observability:read")),
) -> PaginatedResponse:
    return _filter_rows(
        _load_local_csv(ROOT / "validation/output/quality_summary.csv"),
        principal=principal,
        limit=limit,
        offset=offset,
    )


@app.get("/activation/reconciliation")
def activation_reconciliation(
    tenant_id: str | None = None,
    destination: str | None = None,
    run_status: str | None = Query(
        default=None, alias="status", description="reconciled or variance_detected"
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> PaginatedResponse:
    rows = _load_local_csv(ROOT / "reverse_etl/reconciliation/activation_reconciliation.csv")
    if destination:
        rows = [row for row in rows if row.get("destination") == destination]
    if run_status:
        rows = [row for row in rows if row.get("status") == run_status]
    return _filter_rows(
        rows, principal=principal, tenant_id=tenant_id, limit=limit, offset=offset
    )


@app.get("/activation/runs")
def activation_runs(
    run_id: str | None = None,
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> PaginatedResponse:
    """Drill-down findings for reconciliation runs — what specifically caused a
    variance_detected status (missing/unexpected/duplicate/suppressed-but-exported
    rows), not just the aggregate counts."""
    rows = _load_local_csv(ROOT / "reverse_etl/reconciliation/activation_reconciliation_findings.csv")
    if run_id:
        rows = [row for row in rows if row.get("run_id") == run_id]
    return _filter_rows(
        rows, principal=principal, tenant_id=tenant_id, limit=limit, offset=offset
    )


@app.get("/exports/{export_name}")
def get_export(
    export_name: str,
    tenant_id: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    principal: ApiPrincipal = Depends(require_scopes("activation:read")),
) -> list[dict[str, Any]]:
    filename = resolve_export_filename(export_name)
    if filename is None:
        raise HTTPException(status_code=404, detail=f"unsupported export: {export_name}")
    return _tenant_rows(_load_csv(filename), principal, tenant_id)[:limit]

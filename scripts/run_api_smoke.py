from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _compact(value: Any, *, max_rows: int = 1) -> Any:
    if hasattr(value, "model_dump"):
        value = value.model_dump()
    if isinstance(value, dict):
        compacted = {}
        for key, inner in value.items():
            if key in {"primary_email", "primary_phone", "email", "phone"}:
                continue
            if key.endswith("_timestamp") or key in {"started_at", "ended_at", "last_refresh_time", "generated_at"}:
                compacted[key] = "<local_smoke_timestamp>"
                continue
            compacted[key] = _compact(inner, max_rows=max_rows)
        return compacted
    if isinstance(value, list):
        return [_compact(item, max_rows=max_rows) for item in value[:max_rows]]
    return value


def _load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise RuntimeError(f"api-smoke expected generated file: {path}")
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _route_source_proof() -> dict[str, Any]:
    source = (ROOT / "api/main.py").read_text(encoding="utf-8")
    required_routes = [
        '@app.get("/health")',
        '@app.get("/customers/{canonical_customer_id}/profile"',
        '@app.get("/customers/{canonical_customer_id}/identity-lineage"',
        '@app.get("/exports/churn-risk"',
        '@app.get("/observability/pipeline-health"',
    ]
    missing = [route for route in required_routes if route not in source]
    if missing:
        raise RuntimeError(f"api-smoke missing expected route declarations: {missing}")

    churn_rows = _load_csv(ROOT / "reverse_etl/exports/churn_risk_export.csv")
    canonical_rows = _load_csv(ROOT / "identity_resolution/output/dim_customer_canonical.csv")
    lineage_rows = _load_csv(ROOT / "identity_resolution/output/identity_link_explanation.csv")
    health_rows = _load_csv(ROOT / "observability/output/pipeline_run_log.csv")
    churn_row = next((row for row in churn_rows if row.get("tenant_id") == "tenant_us"), churn_rows[0])
    canonical_id = churn_row["canonical_customer_id"]
    canonical = next(row for row in canonical_rows if row.get("canonical_customer_id") == canonical_id)
    lineage = [row for row in lineage_rows if row.get("canonical_customer_id") == canonical_id][:1]
    return {
        "validation_mode": "route_source_and_generated_output_validation",
        "health": {"status": "ok"},
        "customer_profile": {
            "canonical_customer": _compact(canonical),
            "activation_profile": {"canonical_customer_id": canonical_id, "churn_risk": _compact(churn_row)},
        },
        "identity_lineage": {"canonical_customer_id": canonical_id, "link_explanations": _compact(lineage)},
        "churn_export": {
            "tenant_id": "tenant_us",
            "limit": 1,
            "offset": 0,
            "total": sum(1 for row in churn_rows if row.get("tenant_id") == "tenant_us"),
            "rows": _compact([churn_row]),
        },
        "pipeline_health": {
            "tenant_id": "tenant_us",
            "limit": 1,
            "offset": 0,
            "total": sum(1 for row in health_rows if row.get("tenant_id") == "tenant_us"),
            "rows": _compact([row for row in health_rows if row.get("tenant_id") == "tenant_us"][:1]),
        },
    }


def collect_api_smoke() -> dict[str, Any]:
    import api.main as api

    api._load_csv.cache_clear()
    principal = api.ApiPrincipal(
        subject="local-api-smoke",
        scopes=frozenset({"customer:read", "activation:read", "observability:read"}),
        allowed_tenant_ids=frozenset({"tenant_us"}),
    )
    health = api.health()
    churn_page = api.export_churn_risk(
        tenant_id="tenant_us", limit=1, offset=0, principal=principal
    )
    if not churn_page.rows:
        raise RuntimeError("api-smoke requires local activation exports; run make smoke first")
    canonical_customer_id = churn_page.rows[0]["canonical_customer_id"]
    profile = api.customer_profile(
        canonical_customer_id, tenant_id="tenant_us", principal=principal
    )
    lineage = api.customer_identity_lineage(
        canonical_customer_id, tenant_id="tenant_us", principal=principal
    )
    pipeline_health = api.observability_pipeline_health(
        tenant_id="tenant_us", limit=1, offset=0, principal=principal
    )
    return {
        "validation_mode": "route_function_validation",
        "fastapi_import_status": "ok",
        "health": health,
        "customer_profile": _compact(profile),
        "identity_lineage": _compact(lineage),
        "churn_export": _compact(churn_page),
        "pipeline_health": _compact(pipeline_health),
    }


def build_proof_markdown() -> str:
    payload = collect_api_smoke()
    return f"""# Sample API Responses

Local synthetic proof output.

Related command:

```bash
make api-smoke
```

## Proof Type

FastAPI technical smoke proof over local generated outputs. The command runs the local smoke pipeline first, then validates representative API behavior for health, customer profile, identity lineage, churn export, and pipeline health.

Validation mode: `{payload["validation_mode"]}`

FastAPI import status: `{payload["fastapi_import_status"]}`

## Health

```json
{json.dumps(payload["health"], indent=2, sort_keys=True)}
```

## Customer Profile

```json
{json.dumps(payload["customer_profile"], indent=2, sort_keys=True)}
```

## Identity Lineage

```json
{json.dumps(payload["identity_lineage"], indent=2, sort_keys=True)}
```

## Tenant-Filtered Churn Export

```json
{json.dumps(payload["churn_export"], indent=2, sort_keys=True)}
```

## Pipeline Health

```json
{json.dumps(payload["pipeline_health"], indent=2, sort_keys=True)}
```
"""


def write_proof(output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(build_proof_markdown(), encoding="utf-8")
    print(f"api_smoke_proof={output}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FastAPI smoke proof artifact from local generated outputs.")
    parser.add_argument("--output", default="reports/sample_api_responses.md")
    args = parser.parse_args()
    write_proof(ROOT / args.output)


if __name__ == "__main__":
    main()

# API Authorization Matrix

The API key is resolved to a server-configured `ApiPrincipal` containing a subject,
scopes, allowed tenant IDs, and an explicit `global_access` flag. A request tenant is
only a filter: it never grants access. If a tenant is omitted, collection responses
are restricted to the principal's allowed tenant set. Direct foreign-tenant IDs are
not visible. Global access is available only when explicitly configured server-side;
the wildcard fallback is local-only.

| Method | Route | Required scope | Tenant-sensitive | Tenant source | Enforcement / expected behavior |
|---|---|---|---|---|---|
| GET | `/health` | Public | No | N/A | Liveness only; the sole default-public route. |
| GET | `/tenants` | `customer:read` | Yes | Principal | Returns only allowed tenant IDs; explicit global principals can enumerate all fixture tenants. |
| GET | `/` | `observability:read` | No data | N/A | Authenticated redirect to the dashboard. |
| GET | `/api/platform-summary` | `observability:read` | Yes | Principal | Tenant-bearing source rows are filtered before aggregation. |
| GET | `/api/sync-runs` | `observability:read` | Yes | Principal + optional query | Omission filters to allowed tenants; unauthorized query tenant returns 403. |
| GET | `/api/destination-sync-status` | `observability:read` | Yes | Principal + optional query | Runs, failures, and destination state are filtered before response. |
| GET | `/api/export-freshness` | `observability:read` | Yes | Principal | Export counts are computed from authorized rows only. |
| GET | `/api/schema-drift` | `observability:read` | Intentionally global | N/A | Schema scenario metadata contains no tenant records. |
| GET | `/api/benchmark` | `observability:read` | Intentionally global | N/A | Stage benchmark metadata contains no tenant records. |
| GET | `/dashboard` | `observability:read` | Yes | Principal | All tenant-bearing tables and cards are filtered before HTML rendering. |
| GET | `/customers/{id}/activation` | `activation:read` | Yes | Principal + optional query + row | Reads only authorized export rows; foreign ID returns 404. |
| GET | `/api/customers/{id}/activation-profile` | `activation:read` | Yes | Principal + optional query + row | Same direct-row boundary as the activation record. |
| GET | `/api/customers/{id}/churn-risk` | `activation:read` | Yes | Principal + optional query + row | Foreign ID returns 404; unauthorized query tenant returns 403. |
| GET | `/api/customers/{id}/segment` | `activation:read` | Yes | Principal + optional query + row | Foreign ID returns 404; unauthorized query tenant returns 403. |
| GET | `/api/customers/{id}/health-score` | `activation:read` | Yes | Principal + optional query + row | Foreign ID returns 404; unauthorized query tenant returns 403. |
| GET | `/api/customers/{id}/support-priority` | `activation:read` | Yes | Principal + optional query + row | Foreign ID returns 404; unauthorized query tenant returns 403. |
| GET | `/exports/customer-segments` | `activation:read` | Yes | Principal + optional query | Omission filters to allowed tenants. |
| GET | `/exports/churn-risk` | `activation:read` | Yes | Principal + optional query | Omission filters to allowed tenants. |
| GET | `/exports/lifecycle-stage` | `activation:read` | Yes | Principal + optional query | Omission filters to allowed tenants. |
| GET | `/exports/customer-health` | `activation:read` | Yes | Principal + optional query | Omission filters to allowed tenants. |
| GET | `/exports/support-priority` | `activation:read` | Yes | Principal + optional query | Omission filters to allowed tenants. |
| GET | `/customers/{id}/profile` | `customer:read` | Yes | Principal + optional query + canonical row | Canonical row is authorized before the profile is assembled; foreign ID returns 404. |
| GET | `/customers/{id}/timeline` | `customer:read` | Yes | Principal + optional query + mapping/event rows | Identity mappings and CDC events are restricted to authorized tenants. |
| GET | `/customers/{id}/history` | `customer:read` | Yes | Principal + required query + event rows | Requested tenant must be authorized; history is built only from that tenant. |
| GET | `/customers/{id}/identity-lineage` | `customer:read` | Yes | Principal + optional query + lineage rows | Only authorized lineage rows are returned; no match returns 404. |
| GET | `/lineage/{id}` | `customer:read` | Yes | Principal + required query + source rows | Requested tenant must be authorized; sources, events, and outputs stay tenant-scoped. |
| GET | `/identity/review` | `stewardship:read` | Yes | Principal + optional query | Single-tenant principals default safely; multi-tenant principals must select a tenant. |
| GET | `/identity/review/{case_id}` | `stewardship:read` | Yes | Principal + optional query + case | Repository lookup is tenant-bound; foreign explicit tenant returns 403. |
| POST | `/identity/review/{case_id}/decision` | `stewardship:write` | Yes | Principal + request body + case | Tenant is checked before mutation; global principals must provide it explicitly. |
| POST | `/privacy/delete-request` | `privacy:write` | Yes | Principal + required body + canonical row | Both requested tenant and customer ownership are validated before output is written. |
| GET | `/observability/pipeline-health` | `observability:read` | Yes | Principal + optional query | Omission filters to allowed tenants. |
| GET | `/observability/freshness` | `observability:read` | Yes | Principal + optional query | Omission filters to allowed tenants. |
| GET | `/observability/quality-summary` | `observability:read` | Yes when rows carry tenant IDs | Principal | Tenant-bearing rows are filtered before response. |
| GET | `/activation/reconciliation` | `activation:read` | Yes | Principal + optional query | Destination/status filters apply within the authorized tenant rows. |
| GET | `/activation/runs` | `activation:read` | Yes | Principal + optional query | Run filtering applies within the authorized tenant findings. |
| GET | `/exports/{export_name}` | `activation:read` | Yes | Principal + optional query | Registry-approved export is filtered before response. |

`/docs`, `/redoc`, and `/openapi.json` are disabled by default. Setting
`API_ENABLE_DOCS=true` explicitly enables them for local development; that flag does
not change route authentication or tenant authorization.

# Tenant Isolation Runbook

Treat cross-tenant identity, API, history, lineage, privacy, or activation access as a
blocking critical event. Stop activation and retain the event reference. Run
`python3 scripts/simulate_incident.py --scenario cross_tenant_identity_attempt`, then
inspect identity maps and merge audits for differing tenant IDs. Repair the key or
mapping, rebuild from a safe checkpoint, and rerun isolation and reconciliation tests.

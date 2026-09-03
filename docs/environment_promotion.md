# Environment Promotion

| Environment | Warehouse target | Namespace | Activation | Credentials |
| --- | --- | --- | --- | --- |
| local | PostgreSQL Docker | `c360` / `mart_*` | simulator only | local `.env` |
| dev | Snowflake | dedicated dev database/schema | disabled | managed environment secret |
| test | Snowflake | dedicated test database/schema | simulator only | protected CI environment |
| prod-like | Snowflake | isolated prod-like database/schema | disabled by default | managed identity/key pair |

The default CI is credential-free. `.github/workflows/snowflake-validation.yml` is
manual and protected; its build step is separately gated. Ordinary tests never call
an external activation destination. Promotion requires dbt tests, policy checks,
migration parity, activation reconciliation, and review of the validation outputs. Schema,
warehouse, role, and credentials are distinct per environment.

# Privacy Governance

The platform implements privacy-safe activation patterns expected in a Customer Data Platform.

## Row-level security status

**IMPLEMENTED AND VERIFIED FOR SELECTED POSTGRESQL TABLES.** The review queue,
canonical identity table, and a selected activation output have RLS policies. The
policies derive tenancy from `session_user` through an owner-controlled role-binding
table. Verification authenticated as two actual non-owner tenant roles and proved
visibility isolation plus blocked cross-tenant insert/update/delete behavior. Clients
cannot select a tenant with an arbitrary `SET app.tenant_id`.

Snowflake governance was also live-verified: a Row Access Policy limited the analyst
role to eight `tenant_us` rows while the steward saw all 12 fixture customers across
four tenants; email and phone masking policies hid direct values from the analyst and
allowed steward access. Four governance tags were created and attached to live
objects/columns. The recorded counts come from the controlled seed-42 workload.

## Implemented Controls

- PII hashing for email and phone
- masked Customer 360 mart
- consent history output
- activation suppression list
- deletion request artifact
- retention policy SQL
- privacy audit table design
- privacy-safe export generation

## Suppression Logic

Customers are excluded from activation when they have:

- `marketing_consent_status = opted_out`
- `unsubscribe_status = unsubscribed`
- `do_not_contact_flag = true`
- deletion requested
- no active eligible channel consent
- missing destination identifier

Suppression rows include the reason so marketing, support, and data teams can audit why a customer was not activated.

## Deletion request workflow

`privacy/deletion_workflow.py` creates a deletion request and activation suppression
artifact. Retained CDC history is governed separately by deployment-specific legal,
retention, and tokenization policy.

## Local Implementation

- CSV suppression artifacts
- hashed identifiers
- deletion request artifacts
- masked Customer 360 mart
- activation export filtering before sync payload construction

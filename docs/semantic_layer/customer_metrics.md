# Customer Metrics

These are the core governed metrics used by the Customer 360 marts, activation exports, and API. The local implementation is rules-based and intentionally explainable.

## customer_health_score

- **Business definition:** Composite customer health signal from engagement, revenue, support friction, refunds, and subscription status.
- **Grain:** one row per `tenant_id`, `canonical_customer_id`
- **Rules:** starts from a baseline score, adds engagement/revenue points, subtracts support, CSAT, refund, and billing-risk penalties; bounded from 0 to 100.
- **Source model:** `mart_customer_health`
- **Refresh cadence:** hourly in the orchestration design
- **Owner:** Lifecycle Analytics
- **Quality checks:** score between 0 and 100, one row per canonical customer, source lineage present
- **Activation usage:** Salesforce health field, customer success prioritization, Customer 360 API
- **Interpretation:** rules-based score; calibrate weights against observed retention outcomes before operational use

## churn_risk_score

- **Business definition:** Retention-risk score derived from health score, subscription status, support friction, and unsubscribe signals.
- **Grain:** one row per `tenant_id`, `canonical_customer_id`
- **Rules:** inverse of health score with extra penalties for `past_due`, `canceled`, and unsubscribe behavior; mapped to low/medium/high bands.
- **Source model:** `export_churn_risk`
- **Refresh cadence:** hourly
- **Owner:** Customer Success Analytics
- **Quality checks:** score between 0 and 100, band in `low`, `medium`, `high`, suppressed customers excluded from campaign exports
- **Activation usage:** retention queue, save campaign targeting, customer success outreach
- **Interpretation:** transparent rules; predictive scoring can replace them behind the same contract

## lifecycle_stage

- **Business definition:** Current customer lifecycle state from subscription and customer status.
- **Grain:** one row per `tenant_id`, `canonical_customer_id`
- **Rules:** canceled -> churned, past_due -> at_risk, active -> active_customer, trialing/lead -> trial_or_lead
- **Source model:** `export_lifecycle_stage`
- **Refresh cadence:** hourly
- **Owner:** Growth Analytics
- **Quality checks:** lifecycle stage not null, accepted stage values only
- **Activation usage:** onboarding, lifecycle campaigns, churned/reactivation paths
- **Interpretation:** lifecycle states can be extended with product-qualified events

## support_priority

- **Business definition:** Operational priority for support routing and customer success escalation.
- **Grain:** one row per `tenant_id`, `canonical_customer_id`
- **Rules:** high churn plus open support issue -> `p1_retention`; low CSAT -> `p2_csat_recovery`; otherwise `standard`
- **Source model:** `export_support_priority`
- **Refresh cadence:** hourly
- **Owner:** Support Operations
- **Quality checks:** priority enum valid, no orphan activation customer
- **Activation usage:** Zendesk routing, support escalation, customer success follow-up
- **Interpretation:** routing priority complements the support team's SLA policy

## active_customer

- **Business definition:** Customer with active subscription or recent product/order activity.
- **Grain:** one row per `tenant_id`, `canonical_customer_id`
- **Rules:** current subscription active, or recent paid order/product engagement
- **Source model:** `mart_customer_360_current`
- **Refresh cadence:** hourly
- **Owner:** Revenue Analytics
- **Quality checks:** valid subscription status, canonical ID not null
- **Activation usage:** active base segmentation and reporting
- **Caveat:** use lifecycle history for point-in-time analysis

## expansion_candidate

- **Business definition:** Healthy and engaged customer likely to respond to expansion or upgrade motion.
- **Grain:** one row per `tenant_id`, `canonical_customer_id`
- **Rules:** low churn risk, strong health score, high-value or enterprise segment
- **Source model:** `customer_segment_export`
- **Refresh cadence:** hourly
- **Owner:** Growth Analytics
- **Quality checks:** segment enum valid, health score present, privacy eligibility enforced before activation
- **Activation usage:** expansion nurture, account management prioritization
- **Interpretation:** account hierarchy and territory assignment are outside this metric's inputs

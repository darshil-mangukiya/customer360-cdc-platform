from __future__ import annotations


ALLOWED_EXPORT_FILES = {
    "campaign_target_export": "campaign_target_export.csv",
    "churn_risk_export": "churn_risk_export.csv",
    "customer_health_score_export": "customer_health_score_export.csv",
    "customer_segment_export": "customer_segment_export.csv",
    "lifecycle_stage_export": "lifecycle_stage_export.csv",
    "support_priority_export": "support_priority_export.csv",
}


def resolve_export_filename(export_name: str) -> str | None:
    return ALLOWED_EXPORT_FILES.get(export_name.removesuffix(".csv"))

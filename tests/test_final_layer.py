from observability.openlineage_events import build_openlineage_events


def test_openlineage_events_describe_key_pipeline_jobs():
    events = build_openlineage_events()
    job_names = {event["job"]["name"] for event in events}
    assert {
        "generate_cdc_events",
        "land_raw_cdc",
        "resolve_identity",
        "dbt_customer_360_marts",
        "reverse_etl_exports",
        "reverse_etl_destination_sync",
    }.issubset(job_names)
    assert all(event["eventType"] == "COMPLETE" for event in events)


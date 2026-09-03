from scripts.run_benchmark import build_scaled_events, run_benchmark


def test_scaled_benchmark_events_have_unique_delivery_keys():
    rows = build_scaled_events(2)
    valid = [row for row in rows if row["operation_type"] != "upsert"]
    assert len({row["event_id"] for row in rows}) == len(rows)
    assert len({(row["kafka_topic"], row["kafka_partition"], row["kafka_offset"]) for row in valid}) == len(valid)
    assert all(row["event_hash"] is None for row in rows)


def test_benchmark_normalizes_every_valid_scaled_event():
    metrics = run_benchmark(2)
    by_stage = {row.stage_name: row for row in metrics}
    assert by_stage["normalize_cdc"].row_count == by_stage["generate_scaled_events"].row_count - 2

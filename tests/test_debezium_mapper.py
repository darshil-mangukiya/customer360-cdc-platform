from ingestion.debezium_mapper import debezium_to_normalized_envelope, is_debezium_message


def test_debezium_message_maps_to_normalized_cdc_envelope():
    message = {
        "before": None,
        "after": {
            "customer_id": "cust_123",
            "external_account_id": "acct_ext_123",
            "email": "demo@example.com",
            "phone": "+14155550123",
            "first_name": "Demo",
            "last_name": "User",
            "tenant_id": "tenant_us",
            "business_unit": "self_serve",
            "customer_status": "active",
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-01T00:00:00Z",
            "source_updated_at": "2025-01-01T00:00:00Z",
        },
        "source": {
            "connector": "postgresql",
            "name": "customer_ops",
            "db": "customer_ops",
            "schema": "public",
            "table": "customers",
            "lsn": 12345,
            "txId": 99,
            "ts_ms": 1735689600000,
        },
        "op": "c",
        "ts_ms": 1735689601000,
    }
    assert is_debezium_message(message)
    envelope = debezium_to_normalized_envelope(message, topic="cdc.customers")
    assert envelope.event_id.startswith("dbz_")
    assert envelope.operation_type == "insert"
    assert envelope.source_system == "account_app"
    assert envelope.record_primary_key == "cust_123"
    assert envelope.topic_name == "cdc.customers"


def test_debezium_delete_uses_before_payload():
    message = {
        "before": {
            "subscription_id": "sub_123",
            "external_account_id": "acct_ext_123",
            "plan_name": "pro",
            "subscription_status": "canceled",
            "billing_period": "monthly",
            "mrr": 99.0,
            "updated_at": "2025-01-02T00:00:00Z",
        },
        "after": None,
        "source": {"name": "customer_ops", "db": "customer_ops", "table": "subscriptions", "ts_ms": 1735776000000},
        "op": "d",
        "ts_ms": 1735776001000,
    }
    envelope = debezium_to_normalized_envelope(message, topic="cdc.subscriptions")
    assert envelope.operation_type == "delete"
    assert envelope.source_system == "billing_platform"
    assert envelope.payload_after is None
    assert envelope.payload_before["subscription_id"] == "sub_123"


def test_debezium_decimal_string_is_normalized_to_contract_number():
    message = {
        "before": None,
        "after": {
            "order_id": "ord_123",
            "tenant_id": "tenant_us",
            "business_unit": "self_serve",
            "order_customer_ref": "cust_123",
            "email": None,
            "subscription_id": "sub_123",
            "order_status": "paid",
            "gross_amount": "149.95",
            "currency": "USD",
            "ordered_at": "2026-09-02T00:00:00Z",
            "updated_at": "2026-09-02T00:00:00Z",
        },
        "source": {"name": "customer_ops", "db": "customer_ops", "table": "orders", "lsn": 1},
        "op": "c",
        "ts_ms": 1788307200000,
    }
    envelope = debezium_to_normalized_envelope(message, topic="cdc.orders")
    assert envelope.payload_after["gross_amount"] == 149.95


def test_schema_wrapped_runtime_shape_preserves_kafka_and_postgres_positions():
    message = {
        "schema": {"type": "struct"},
        "payload": {
            "before": {
                "customer_id": "runtime_1",
                "external_account_id": "acct_runtime_1",
                "tenant_id": "tenant_us",
                "business_unit": "demo",
                "customer_status": "lead",
                "updated_at": "2026-08-21T02:40:34Z",
            },
            "after": {
                "customer_id": "runtime_1",
                "external_account_id": "acct_runtime_1",
                "tenant_id": "tenant_us",
                "business_unit": "demo",
                "customer_status": "active",
                "updated_at": "2026-08-21T02:40:42Z",
            },
            "source": {
                "version": "2.7.3.Final",
                "connector": "postgresql",
                "name": "customer_ops",
                "db": "customer_ops",
                "schema": "public",
                "table": "source_customers_cdc_demo",
                "txId": 764,
                "lsn": 26944320,
                "ts_ms": 1787280042841,
            },
            "op": "u",
            "ts_ms": 1787280042987,
        },
    }
    envelope = debezium_to_normalized_envelope(
        message,
        topic="cdc.source_customers_cdc_demo",
        kafka_partition=0,
        kafka_offset=1,
    )
    assert envelope.operation_type == "update"
    assert envelope.payload_before["customer_id"] == "runtime_1"
    assert envelope.payload_after["customer_status"] == "active"
    assert envelope.source_lsn == "26944320"
    assert envelope.event_sequence_number == 26944320
    assert envelope.source_transaction_id == "764"
    assert (envelope.kafka_topic, envelope.kafka_partition, envelope.kafka_offset) == (
        "cdc.source_customers_cdc_demo",
        0,
        1,
    )

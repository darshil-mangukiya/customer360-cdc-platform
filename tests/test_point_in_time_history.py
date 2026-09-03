from customer360.history import build_scd2_history, point_in_time, validate_scd2_invariants


def _change(at, state, event, *, tenant="tenant_us", deleted=False):
    return {
        "tenant_id": tenant,
        "entity_id": "customer_1",
        "effective_timestamp": at,
        "subscription_status": state,
        "source_event_id": event,
        "change_reason": "subscription_changed",
        "is_deleted": deleted,
        "attributes": {"plan": "pro"},
    }


def test_point_in_time_fixture_never_leaks_future_state():
    history = build_scd2_history(
        [
            _change("2026-01-01T00:00:00Z", "trialing", "evt_1"),
            _change("2026-01-10T00:00:00Z", "active", "evt_2"),
            _change("2026-02-15T00:00:00Z", "past_due", "evt_3"),
            _change("2026-02-20T00:00:00Z", "active", "evt_4"),
            _change("2026-03-01T00:00:00Z", "canceled", "evt_5"),
        ],
        state_field="subscription_status",
    )
    expected = {
        "2026-01-05T00:00:00Z": "trialing",
        "2026-01-20T00:00:00Z": "active",
        "2026-02-16T00:00:00Z": "past_due",
        "2026-02-25T00:00:00Z": "active",
        "2026-03-05T00:00:00Z": "canceled",
    }
    assert not validate_scd2_invariants(history)
    for timestamp, state in expected.items():
        result = point_in_time(history, tenant_id="tenant_us", entity_id="customer_1", as_of_timestamp=timestamp)
        assert result is not None and result.state == state


def test_late_arrival_rebuilds_correct_intervals_and_delete_closes_state():
    history = build_scd2_history(
        [
            _change("2026-01-01T00:00:00Z", "trialing", "evt_1"),
            _change("2026-03-01T00:00:00Z", "canceled", "evt_3"),
            _change("2026-02-01T00:00:00Z", "active", "evt_late"),
            _change("2026-04-01T00:00:00Z", "active", "evt_delete", deleted=True),
        ],
        state_field="subscription_status",
    )
    assert [row.state for row in history] == ["trialing", "active", "canceled", "deleted"]
    result = point_in_time(history, tenant_id="tenant_us", entity_id="customer_1", as_of_timestamp="2026-02-15T00:00:00Z")
    assert result is not None and result.state == "active"
    assert point_in_time(history, tenant_id="tenant_us", entity_id="customer_1", as_of_timestamp="2026-04-02T00:00:00Z") is None


def test_history_is_tenant_isolated_for_identical_entity_ids():
    history = build_scd2_history(
        [
            _change("2026-01-01T00:00:00Z", "active", "evt_us"),
            _change("2026-01-01T00:00:00Z", "past_due", "evt_eu", tenant="tenant_emea"),
        ],
        state_field="subscription_status",
    )
    us = point_in_time(history, tenant_id="tenant_us", entity_id="customer_1", as_of_timestamp="2026-01-02T00:00:00Z")
    eu = point_in_time(history, tenant_id="tenant_emea", entity_id="customer_1", as_of_timestamp="2026-01-02T00:00:00Z")
    assert us is not None and us.state == "active"
    assert eu is not None and eu.state == "past_due"

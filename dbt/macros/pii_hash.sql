{#
    Deterministic, salted SHA-256 hash of a PII field for privacy-safe activation
    artifacts (masked marts, hashed identifiers in reverse ETL exports). Adapter-
    dispatched: Postgres needs the pgcrypto `digest()`/`encode()` pair, Snowflake's
    native `SHA2()` already returns a hex string.
#}
{% macro pii_hash(field) -%}
    {{ return(adapter.dispatch('pii_hash', 'customer_360_cdc_platform')(field)) }}
{%- endmacro %}

{% macro default__pii_hash(field) %}
    case
        when {{ field }} is null then null
        else encode(
            digest(
                '{{ env_var("PII_HASH_SALT", "customer-360-local-demo-salt") }}' || '::' || lower(cast({{ field }} as text)),
                'sha256'
            ),
            'hex'
        )
    end
{% endmacro %}

{% macro snowflake__pii_hash(field) %}
    case
        when {{ field }} is null then null
        else sha2(
            '{{ env_var("PII_HASH_SALT", "customer-360-local-demo-salt") }}' || '::' || lower(cast({{ field }} as varchar)),
            256
        )
    end
{% endmacro %}

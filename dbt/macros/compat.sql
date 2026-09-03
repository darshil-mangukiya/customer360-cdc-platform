{#
    Cross-database compatibility macros for PostgreSQL and Snowflake.

    The staging layer parses semi-structured CDC payloads and casts timestamp/PII
    fields. Postgres (JSONB `->>`, `timestamptz`, pgcrypto `digest()`) and Snowflake
    (VARIANT `:field`, `TIMESTAMP_TZ`, native `SHA2()`) diverge on exactly these points.
    Every model stays adapter-agnostic by calling these macros instead of writing
    vendor-specific SQL directly, dispatched on `target.type` via dbt's adapter.dispatch.
#}

{% macro json_field(column, key) -%}
    {{ return(adapter.dispatch('json_field', 'customer_360_cdc_platform')(column, key)) }}
{%- endmacro %}

{% macro default__json_field(column, key) %}
    {{ column }} ->> '{{ key }}'
{% endmacro %}

{% macro snowflake__json_field(column, key) %}
    {{ column }}:{{ key }}::string
{% endmacro %}

{#
    Timezone-aware timestamp type name. Postgres calls it `timestamptz`; Snowflake
    calls the equivalent `timestamp_tz`. Used anywhere a payload field or literal is
    cast to a timestamp, e.g. `nullif(x, '')::{{ tstz_type() }}`.
#}
{% macro tstz_type() -%}
    {%- if target.type == 'snowflake' -%}
        timestamp_tz
    {%- else -%}
        timestamptz
    {%- endif -%}
{%- endmacro %}

{# Force warehouse "now" expressions to the shared timezone-aware contract type. #}
{% macro current_tstz() -%}
    current_timestamp::{{ tstz_type() }}
{%- endmacro %}

{# Avoid Snowflake's scale-zero NUMBER default for bare NUMERIC casts. #}
{% macro decimal_type() -%}
    {%- if target.type == 'snowflake' -%}
        number(38, 6)
    {%- else -%}
        numeric(38, 6)
    {%- endif -%}
{%- endmacro %}

{#
    Minutes elapsed between now and a timestamp column. Postgres uses
    `extract(epoch from ...)`; Snowflake uses `DATEDIFF`.
#}
{% macro minutes_since(ts_column) -%}
    {{ return(adapter.dispatch('minutes_since', 'customer_360_cdc_platform')(ts_column)) }}
{%- endmacro %}

{% macro default__minutes_since(ts_column) %}
    extract(epoch from (current_timestamp - {{ ts_column }})) / 60
{% endmacro %}

{% macro snowflake__minutes_since(ts_column) %}
    datediff('second', {{ ts_column }}, current_timestamp()) / 60
{% endmacro %}

{% macro conditional_count(condition) -%}
    sum(case when {{ condition }} then 1 else 0 end)
{%- endmacro %}

{% macro json_text_object(pairs) -%}
    {{ return(adapter.dispatch('json_text_object', 'customer_360_cdc_platform')(pairs)) }}
{%- endmacro %}

{% macro default__json_text_object(pairs) %}
    jsonb_build_object(
    {%- for pair in pairs %}
        '{{ pair[0] }}', {{ pair[1] }}{% if not loop.last %},{% endif %}
    {%- endfor %}
    )::text
{% endmacro %}

{% macro snowflake__json_text_object(pairs) %}
    to_json(object_construct(
    {%- for pair in pairs %}
        '{{ pair[0] }}', {{ pair[1] }}{% if not loop.last %},{% endif %}
    {%- endfor %}
    ))
{% endmacro %}

{% macro days_before(ts_column, days) -%}
    {{ return(adapter.dispatch('days_before', 'customer_360_cdc_platform')(ts_column, days)) }}
{%- endmacro %}

{% macro default__days_before(ts_column, days) %}
    {{ ts_column }} - interval '{{ days }} days'
{% endmacro %}

{% macro snowflake__days_before(ts_column, days) %}
    dateadd('day', -{{ days }}, {{ ts_column }})
{% endmacro %}

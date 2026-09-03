{% macro surrogate_key(fields) -%}
    md5(concat_ws('||', {%- for field in fields -%} coalesce(cast({{ field }} as text), '') {%- if not loop.last -%}, {%- endif -%}{%- endfor -%}))
{%- endmacro %}


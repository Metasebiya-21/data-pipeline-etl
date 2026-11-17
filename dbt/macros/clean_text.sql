{# Trim a text value and turn empty strings into NULL (the NULLIF(BTRIM(...), '') pattern). #}
{% macro clean_text(expr) -%}
    nullif(trim({{ expr }}), '')
{%- endmacro %}

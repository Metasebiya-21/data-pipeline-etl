{# MongoDB stores dates as epoch seconds; convert to a Snowflake timestamp. #}
{% macro epoch_to_ts(column) -%}
    to_timestamp_ntz({{ column }}::number)
{%- endmacro %}

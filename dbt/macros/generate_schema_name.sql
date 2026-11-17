{#
  In prod, use the custom schema name as-is (STAGING, MARTS, ...).
  In dev, prefix it with the developer's schema (dev_me_staging) so developers
  never overwrite production tables.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

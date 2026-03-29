-- Overrides dbt's default schema naming behavior.
-- By default dbt concatenates: default_schema + custom_schema
-- e.g. RAW + CORE = RAW_CORE which is wrong.
--
-- This macro makes dbt use the custom schema name directly:
-- silver models → CORE
-- gold models   → GOLD
-- everything else → default schema (RAW)
 
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
 
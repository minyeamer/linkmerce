{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set schema_name = target.schema if custom_schema_name is none else custom_schema_name | trim -%}
  {%- if target.name == 'dev' -%}
    {{ schema_name }}_dev
  {%- else -%}
    {{ schema_name }}
  {%- endif -%}
{%- endmacro %}

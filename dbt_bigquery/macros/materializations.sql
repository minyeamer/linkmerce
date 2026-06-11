{% materialization tvf, adapter='bigquery' %}
  -- Load params from model configuration
  {%- set params = config.get('params', []) -%}
  {%- set declared_params = [] -%}
  {%- for param in params -%}
    {%- do declared_params.append(param['name'] ~ ' ' ~ param['type']) -%}
  {% endfor %}
  {%- set joined_params = declared_params | join(', ') -%}

  -- Load model query
  {%- set body_query = sql -%}

  -- Run the final query to create TVF
  {%- call statement('main') -%}
    create or replace table function {{ this }}
    ({{ joined_params }}) as ({{ body_query }})
  {%- endcall -%}

  {{ return({'relations': [this]}) }}
{% endmaterialization %}

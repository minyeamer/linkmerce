{#
  Table-valued-function materialization for PostgreSQL.

  Models declare inputs in `meta.params` and use those names in the SQL body.
  PostgreSQL requires a `RETURNS TABLE` signature, so a typed zero-row temporary
  view infers the output columns before the SQL function is created and committed.
#}

{% materialization tvf, adapter='postgres' -%}

  -- Convert `meta.params` to PostgreSQL arguments. The `p_` prefix avoids output
  -- column conflicts while the model body keeps the declared parameter names.
  {%- set params = config.get('meta', {}).get('params', []) -%}
  {%- set arguments = [] -%}
  {%- set bindings = [] -%}
  {%- set probe_bindings = [] -%}

  {%- for param in params -%}
    {%- set argument_name = 'p_' ~ param['name'] | lower -%}
    {%- do arguments.append(argument_name ~ ' ' ~ param['type']) -%}
    {%- do bindings.append(argument_name ~ ' AS ' ~ param['name']) -%}
    {%- do probe_bindings.append('NULL::' ~ param['type'] ~ ' AS ' ~ param['name']) -%}
  {%- endfor -%}

  {%- set arguments_sql = arguments | join(', ') -%}
  {%- set probe_relation = this.identifier ~ '__tvf_probe' -%}

  -- Bind arguments through a one-row relation so model SQL can use the declared
  -- names directly. The probe uses typed NULLs and returns no rows.
  {%- if params | length > 0 -%}
    {%- set function_body -%}
      SELECT tvf_body.*
      FROM (SELECT {{ bindings | join(', ') }}) AS tvf_params
      CROSS JOIN LATERAL ({{ sql }}) AS tvf_body
    {%- endset -%}
    {%- set probe_body -%}
      SELECT tvf_body.*
      FROM (SELECT {{ probe_bindings | join(', ') }}) AS tvf_params
      CROSS JOIN LATERAL ({{ sql }}) AS tvf_body
      LIMIT 0
    {%- endset -%}
  {%- else -%}
    {%- set function_body = sql -%}
    {%- set probe_body = 'SELECT tvf_body.* FROM (' ~ sql ~ ') AS tvf_body LIMIT 0' -%}
  {%- endif -%}

  -- Create a session-local zero-row view to resolve model output names and types.
  {%- call statement('drop_tvf_probe_before') -%}
    DROP VIEW IF EXISTS pg_temp.{{ adapter.quote(probe_relation) }}
  {%- endcall -%}
  {%- call statement('create_tvf_probe') -%}
    CREATE TEMPORARY VIEW {{ adapter.quote(probe_relation) }} AS {{ probe_body }}
  {%- endcall -%}

  {%- set result_columns = run_query(
    "SELECT attname, pg_catalog.format_type(atttypid, atttypmod) "
    ~ "FROM pg_catalog.pg_attribute "
    ~ "WHERE attrelid = 'pg_temp." ~ probe_relation ~ "'::regclass "
    ~ "AND attnum > 0 AND NOT attisdropped ORDER BY attnum"
  ) -%}
  {%- set returns = [] -%}
  {%- for column in result_columns.rows -%}
    {%- do returns.append(adapter.quote(column[0]) ~ ' ' ~ column[1]) -%}
  {%- endfor -%}

  -- Remove the temporary probe after reading its catalog metadata.
  {%- call statement('drop_tvf_probe_after') -%}
    DROP VIEW pg_temp.{{ adapter.quote(probe_relation) }}
  {%- endcall -%}

  {%- if returns | length == 0 -%}
    {{ exceptions.raise_compiler_error('Could not infer TVF return columns for ' ~ this) }}
  {%- endif -%}

  -- Create the SQL function with the inferred `RETURNS TABLE` signature.
  {%- call statement('main') -%}
    CREATE OR REPLACE FUNCTION {{ this }}({{ arguments_sql }})
    RETURNS TABLE ({{ returns | join(', ') }})
    LANGUAGE SQL
    STABLE
    AS $dbt_tvf$
      {{ function_body }}
    $dbt_tvf$
  {%- endcall -%}

  -- Custom materializations must commit; otherwise dbt rolls back CREATE FUNCTION.
  {%- do adapter.commit() -%}

  {{ return({'relations': [this]}) }}
{%- endmaterialization %}

{#
  Table materialization for native PostgreSQL daily range partitions.

  The model query is evaluated once into a temporary staging table. Without a
  `partitions` config, the staging table supplies the partition values and the entire
  target is replaced. With `partitions`, only those daily partitions are replaced.
  pg_partman is deliberately not used: each run creates only the required children.
#}

{% materialization partitioned_table, adapter='postgres' %}

  {%- set partition_by = config.require('partition_by') -%}
  {%- set partitions = config.get('partitions', none) -%}

  {%- do pg_validate_partition_by(partition_by) -%}

  {% if partitions is not none %}
    {%- do pg_validate_partitions(partitions) -%}
  {% endif %}

  {%- set partition_field = partition_by['field'] -%}

  {%- set existing_relation = load_cached_relation(this) -%}

  {%- set target_relation = this.incorporate(type='table') -%}

  -- The staging table is temporary, so it is isolated to the current database
  -- session. Its name is still derived from the target for readable query logs.
  {%- set stage_identifier = target_relation.identifier ~ '__dbt_partition_stage' -%}

  -- Grab current table grants config for comparison later on.
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `begin` happens here.
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- Build the model once. The staging table is the source of truth for the target
  -- columns and inserted rows, preventing a second evaluation from differing.
  {% call statement('build_partition_stage') -%}
    create temporary table {{ adapter.quote(stage_identifier) }}
    on commit drop as
    select * from (
      {{ sql }}
    ) as model_subquery
  {%- endcall %}

  -- Reject null partition keys before changing the target. When `partitions` is
  -- configured, also require every staged row to belong to that explicit list.
  {{ pg_validate_partition_stage(stage_identifier, partition_field, partitions) }}

  {% if existing_relation is none %}

    -- On the first run, use the staging columns to create a native PostgreSQL
    -- partitioned parent. A parent stores no rows itself; child partitions do.
    {% call statement('create_partitioned_parent') -%}
      create table {{ target_relation }}
      (like {{ adapter.quote(stage_identifier) }} including all)
      partition by range ({{ adapter.quote(partition_field) }})
    {%- endcall %}

    -- Match the default table materialization: indexes are created with the new
    -- target relation. Existing partitioned parents retain their own indexes.
    {% do create_indexes(target_relation) %}

  {% else %}

    -- This materialization updates a parent in place. Replacing a normal table
    -- with a partitioned table is not supported by PostgreSQL, so fail early.
    {{ pg_assert_partitioned_parent(target_relation) }}

  {% endif %}

  -- Child creation always follows the distinct partition values that are present
  -- in the staged model result. `partitions` controls replacement scope only.
  {{ pg_create_daily_partitions_from_stage(target_relation, stage_identifier, partition_field) }}

  {% if partitions is none %}

    -- Preserve full-table behavior when no explicit replacement range is given.
    {% call statement('truncate_partitioned_target') -%}
      truncate table {{ target_relation }}
    {%- endcall %}

  {% else %}

    -- An explicit list selects partial replacement mode. Empty lists are valid:
    -- they truncate no children, and validation requires an empty stage.
    {{ pg_truncate_daily_partitions(target_relation, partitions) }}

  {% endif %}

  -- Insert only after validation, partition creation, and the selected truncate
  -- operation have completed successfully in the same transaction.
  {% call statement('main') -%}
    insert into {{ target_relation }}
    select * from {{ adapter.quote(stage_identifier) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- The relation is retained instead of swapped, so existing grants do not need
  -- full-refresh revocation semantics.
  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=False) %}

  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  -- `commit` happens here. `on commit drop` removes the staging table.
  {% do adapter.commit() %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro pg_validate_partition_by(partition_by) %}

  -- Keep the first implementation intentionally narrow. The partition-creation
  -- SQL below assumes a date value and one child table per calendar day.
  {% if partition_by is not mapping %}
    {{ exceptions.raise_compiler_error('partitioned_table: `partition_by` must be a dictionary.') }}
  {% endif %}

  {% for key in ['field', 'data_type', 'granularity'] %}
    {% if key not in partition_by %}
      {{ exceptions.raise_compiler_error('partitioned_table: `partition_by` is missing `' ~ key ~ '`.') }}
    {% endif %}
  {% endfor %}

  {% if partition_by['data_type'] | lower != 'date' %}
    {{ exceptions.raise_compiler_error('partitioned_table currently supports only `partition_by.data_type: date`.') }}
  {% endif %}

  {% if partition_by['granularity'] | lower != 'day' %}
    {{ exceptions.raise_compiler_error('partitioned_table currently supports only `partition_by.granularity: day`.') }}
  {% endif %}

{% endmacro %}

{% macro pg_validate_partitions(partitions) %}

  -- `partitions` is a list of PostgreSQL date expressions. In particular, an
  -- empty list remains distinct from an omitted config and selects partial mode.
  {% if partitions is string or partitions is not sequence %}
    {{ exceptions.raise_compiler_error('partitioned_table: `partitions` must be a list.') }}
  {% endif %}

{% endmacro %}

{% macro pg_render_date_array(partitions) -%}
  array[
    {%- for partition in partitions -%}
      {{ partition }}{% if not loop.last %}, {% endif %}
    {%- endfor -%}
  ]::date[]
{%- endmacro %}

{% macro pg_validate_partition_stage(stage_identifier, partition_field, partitions=none) %}

  {% call statement('validate_partition_stage') -%}
    do $dbt_partitioned_table$
    begin
      -- A null cannot be routed to a range partition, so reject it before the
      -- target is created or any existing child is truncated.
      if exists (
        select 1
        from {{ adapter.quote(stage_identifier) }}
        where {{ adapter.quote(partition_field) }} is null
      ) then
        raise exception
          'partitioned_table partition field % contains null values',
          {{ dbt.string_literal(partition_field) }};
      end if;

      {% if partitions is not none %}
        -- Explicit partitions define the complete allowed insertion set. This
        -- also makes a non-empty stage invalid when an empty list was supplied.
        if exists (
          select 1
          from {{ adapter.quote(stage_identifier) }}
          where {{ adapter.quote(partition_field) }}::date
            <> all ({{ pg_render_date_array(partitions) }})
        ) then
          raise exception
            'partitioned_table staged values for % fall outside `partitions`',
            {{ dbt.string_literal(partition_field) }};
        end if;
      {% endif %}
    end;
    $dbt_partitioned_table$;
  {%- endcall %}

{% endmacro %}

{% macro pg_assert_partitioned_parent(relation) %}

  {% call statement('assert_partitioned_parent') -%}
    do $dbt_partitioned_table$
    begin
      -- PostgreSQL records partitioned parents as relkind = 'p'. Checking this
      -- before truncate makes a mistaken normal-table target fail without writes.
      if not exists (
        select 1
        from pg_class as c
        join pg_namespace as n on n.oid = c.relnamespace
        where n.nspname = {{ dbt.string_literal(relation.schema) }}
          and c.relname = {{ dbt.string_literal(relation.identifier) }}
          and c.relkind = 'p'
      ) then
        raise exception
          'partitioned_table target %.% must be an existing partitioned table',
          {{ dbt.string_literal(relation.schema) }},
          {{ dbt.string_literal(relation.identifier) }};
      end if;
    end;
    $dbt_partitioned_table$;
  {%- endcall %}

{% endmacro %}

{% macro pg_create_daily_partitions_from_stage(relation, stage_identifier, partition_field) %}

  {% set partition_dates_sql %}
    select distinct {{ adapter.quote(partition_field) }}::date
    from {{ adapter.quote(stage_identifier) }}
  {% endset %}

  {{ pg_create_daily_partitions(relation, partition_dates_sql) }}

{% endmacro %}

{% macro pg_create_daily_partitions(relation, partition_dates_sql) %}

  {% call statement('create_daily_partitions') -%}
    do $dbt_partitioned_table$
    declare
      partition_date date;
      child_identifier text;
    begin
      -- Both full and partial replacement modes use the staged model result as
      -- their source of daily lower bounds. The half-open range is deterministic.
      for partition_date in
        {{ partition_dates_sql }}
      loop
        child_identifier := {{ dbt.string_literal(relation.identifier ~ '_p') }}
          || to_char(partition_date, 'YYYYMMDD');

        -- Do not recreate a child made by an earlier run. If a differently named
        -- partition already owns this range, PostgreSQL raises its normal overlap error.
        if to_regclass(format('%I.%I', {{ dbt.string_literal(relation.schema) }}, child_identifier)) is null then
          execute format(
            'create table %I.%I partition of %I.%I for values from (%L) to (%L)',
            {{ dbt.string_literal(relation.schema) }},
            child_identifier,
            {{ dbt.string_literal(relation.schema) }},
            {{ dbt.string_literal(relation.identifier) }},
            partition_date,
            partition_date + 1
          );
        end if;
      end loop;
    end;
    $dbt_partitioned_table$;
  {%- endcall %}

{% endmacro %}

{% macro pg_truncate_daily_partitions(relation, partitions) %}

  {% call statement('truncate_daily_partitions') -%}
    do $dbt_partitioned_table$
    declare
      partition_date date;
      child_identifier text;
    begin
      -- Truncating the selected children avoids row-by-row delete overhead and
      -- leaves every unlisted partition untouched. An empty array runs no loop.
      for partition_date in
        select distinct unnest({{ pg_render_date_array(partitions) }})
      loop
        child_identifier := {{ dbt.string_literal(relation.identifier ~ '_p') }}
          || to_char(partition_date, 'YYYYMMDD');

        -- A requested date can legitimately have no staged rows and therefore no
        -- child table. In that case there is no existing partition data to clear.
        if to_regclass(format('%I.%I', {{ dbt.string_literal(relation.schema) }}, child_identifier)) is not null then
          execute format(
            'truncate table %I.%I',
            {{ dbt.string_literal(relation.schema) }},
            child_identifier
          );
        end if;
      end loop;
    end;
    $dbt_partitioned_table$;
  {%- endcall %}

{% endmacro %}

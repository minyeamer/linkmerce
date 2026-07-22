{#
  Table materialization for native PostgreSQL daily range partitions.

  With batch_size zero, the model query is evaluated once into a temporary staging
  table. With a positive batch_size and `partitions`, each consecutive partition
  batch is evaluated and committed independently. Without `partitions`, batch_size
  is ignored and the entire target is replaced in one transaction.
  pg_partman is deliberately not used: each run creates only the required children.
#}

{% materialization partitioned_table, adapter='postgres' %}

  {%- set partition_by = config.require('partition_by') -%}
  {%- set partitions = config.get('partitions', none) -%}
  {%- set batch_size = var('batch_size') | int -%}

  {%- do pg_validate_partition_by(partition_by) -%}

  {% if batch_size < 0 %}
    {{ exceptions.raise_compiler_error('partitioned_table: `batch_size` must be zero or greater.') }}
  {% endif %}

  {% if partitions is not none %}
    {%- do pg_validate_partitions(partitions) -%}
  {% endif %}

  {% if partitions is none %}
    -- A model without an explicit date list is an all-partition replacement.
    -- It cannot be split safely, so always use the legacy single-run path.
    {%- set batch_size = 0 -%}
  {% endif %}

  {%- set partition_field = partition_by['field'] -%}

  {%- set existing_relation = load_cached_relation(this) -%}

  {%- set target_relation = this.incorporate(type='table') -%}

  -- Each staging table is temporary and isolated to the current database session.
  {%- set stage_identifier = target_relation.identifier ~ '__dbt_partition_stage' -%}

  -- Grab current table grants config for comparison later on.
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `begin` happens here.
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if batch_size > 0 %}

    -- Each batch reads only its selected output dates, refreshes those target
    -- partitions, then commits. A later failure cannot roll back prior batches.
    {% if existing_relation is not none %}
      {{ pg_assert_partitioned_parent(target_relation) }}
    {% endif %}
    {% set batch_state = namespace(target_exists=(existing_relation is not none)) %}

    {% for partition_batch in partitions | batch(batch_size) %}
      -- Render concrete date literals so PostgreSQL prunes source partitions
      -- during planning, before it consumes relation locks.
      {% set batch_start_date = partition_batch | first %}
      {% set batch_end_date = partition_batch | last %}
      {% set batch_sql = sql
        | replace('__dbt_batch_start_date__', batch_start_date)
        | replace('__dbt_batch_end_date__', batch_end_date) %}

      {% call statement('drop_partition_stage_before_batch') -%}
        drop table if exists {{ adapter.quote(stage_identifier) }}
      {%- endcall %}

      {% call statement('build_partition_stage') -%}
        create temporary table {{ adapter.quote(stage_identifier) }}
        on commit preserve rows as
        select * from (
          {{ batch_sql }}
        ) as model_subquery
        where {{ adapter.quote(partition_field) }}::date
          = any ({{ pg_date_array(partition_batch) }})
      {%- endcall %}

      {{ pg_validate_partition_stage(stage_identifier, partition_field, partition_batch) }}

      {% if not batch_state.target_exists %}
        {% call statement('create_partitioned_parent') -%}
          create table {{ target_relation }}
          (like {{ adapter.quote(stage_identifier) }} including all)
          partition by range ({{ adapter.quote(partition_field) }})
        {%- endcall %}
        {% do create_indexes(target_relation) %}
        {% set batch_state.target_exists = true %}
      {% endif %}

      {% set staged_partition_dates = pg_stage_dates(stage_identifier, partition_field) %}
      {{ pg_create_stage_partitions(target_relation, stage_identifier, partition_field) }}
      {{ pg_truncate_dates(target_relation, partition_batch) }}
      {{ pg_insert_partitions(target_relation, stage_identifier, partition_field, staged_partition_dates) }}

      {% do adapter.commit() %}

    {% endfor %}

    {% call statement('main') -%}
      drop table if exists {{ adapter.quote(stage_identifier) }}
    {%- endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=False) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
    {% do persist_docs(target_relation, model) %}
    {% do adapter.commit() %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

  {% endif %}

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

  -- Child creation follows the staged dates, then the target is refreshed in
  -- one transaction to preserve the original non-batch behavior.
  {{ pg_create_stage_partitions(target_relation, stage_identifier, partition_field) }}

  {% if partitions is none %}

    {% call statement('truncate_partitioned_target') -%}
      truncate table {{ target_relation }}
    {%- endcall %}

  {% else %}

    {{ pg_truncate_dates(target_relation, partitions) }}

  {% endif %}

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

{% macro pg_date_array(partitions) -%}
  array[
    {%- for partition in partitions -%}
      {{ partition }}{% if not loop.last %}, {% endif %}
    {%- endfor -%}
  ]::date[]
{%- endmacro %}

{% macro pg_batch_start_date() -%}
  {% if var('batch_size') | int > 0 %}
    __dbt_batch_start_date__
  {% else %}
    DATE {{ dbt.string_literal(var('ds_start_date')) }}
  {% endif %}
{%- endmacro %}

{% macro pg_batch_end_date() -%}
  {% if var('batch_size') | int > 0 %}
    __dbt_batch_end_date__
  {% else %}
    DATE {{ dbt.string_literal(var('ds_end_date')) }}
  {% endif %}
{%- endmacro %}

{% macro pg_partition_date_array(partition_dates) -%}
  array[
    {%- for partition_date in partition_dates -%}
      DATE '{{ partition_date }}'{% if not loop.last %}, {% endif %}
    {%- endfor -%}
  ]::date[]
{%- endmacro %}

{% macro pg_stage_dates(stage_identifier, partition_field) %}
  {% set partition_dates_sql %}
    select distinct to_char({{ adapter.quote(partition_field) }}::date, 'YYYY-MM-DD')
    from {{ adapter.quote(stage_identifier) }}
    order by 1
  {% endset %}

  {% set partition_dates = run_query(partition_dates_sql) %}

  {% if execute %}
    {% do return(partition_dates.columns[0].values() | list) %}
  {% endif %}

  {% do return([]) %}
{% endmacro %}

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
            <> all ({{ pg_date_array(partitions) }})
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

{% macro pg_insert_partitions(relation, stage_identifier, partition_field, partition_dates) %}
  {% call statement('insert_daily_partition_batch_from_stage') -%}
    do $dbt_partitioned_table$
    declare
      partition_date date;
      child_identifier text;
    begin
      -- Insert into each child directly so this statement locks only the
      -- current batch instead of routing rows through the whole parent tree.
      for partition_date in
        select distinct unnest({{ pg_partition_date_array(partition_dates) }})
      loop
        child_identifier := {{ dbt.string_literal(relation.identifier ~ '_p') }}
          || to_char(partition_date, 'YYYYMMDD');

        execute format(
          'insert into %I.%I select * from %I where %I::date = %L',
          {{ dbt.string_literal(relation.schema) }},
          child_identifier,
          {{ dbt.string_literal(stage_identifier) }},
          {{ dbt.string_literal(partition_field) }},
          partition_date
        );
      end loop;
    end;
    $dbt_partitioned_table$;
  {%- endcall %}
{% endmacro %}

{% macro pg_create_stage_partitions(relation, stage_identifier, partition_field) %}
  {% set partition_dates_sql %}
    select distinct {{ adapter.quote(partition_field) }}::date
    from {{ adapter.quote(stage_identifier) }}
  {% endset %}

  {{ pg_create_partitions_from_query(relation, partition_dates_sql) }}
{% endmacro %}

{% macro pg_create_partitions_from_query(relation, partition_dates_sql) %}
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

{% macro pg_truncate_dates(relation, partitions) %}
  {% call statement('truncate_daily_partitions') -%}
    do $dbt_partitioned_table$
    declare
      partition_date date;
      child_identifier text;
    begin
      -- Truncating the selected children avoids row-by-row delete overhead and
      -- leaves every unlisted partition untouched. An empty array runs no loop.
      for partition_date in
        select distinct unnest({{ pg_date_array(partitions) }})
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

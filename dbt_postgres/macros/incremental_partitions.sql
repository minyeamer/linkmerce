{% macro pg_date_partitions(start_var='ds_start_date', end_var='ds_end_date') -%}
  -- Set `start_date` and `end_date` from arguments.
  {%- set start_date = modules.datetime.datetime.strptime(var(start_var), '%Y-%m-%d').date() -%}
  {%- set end_date = modules.datetime.datetime.strptime(var(end_var), '%Y-%m-%d').date() -%}

  -- Generate PostgreSQL date expressions for the inclusive date range.
  {%- set partitions = [] -%}
  {%- for offset in range((end_date - start_date).days + 1) -%}
    {%- set partition_date = start_date + modules.datetime.timedelta(days=offset) -%}
    {%- do partitions.append("DATE '" ~ partition_date.isoformat() ~ "'") -%}
  {%- endfor -%}

  {%- do return(partitions) -%}
{%- endmacro %}

-- Expand an input date to the Monday-Sunday week boundary for partition filtering
-- Used by Coupang rocket settlement sources (`sales` and `shipping`)

{% macro pg_week_start_date(date_var='ds_start_date') -%}
  {%- set base_date = modules.datetime.datetime.strptime(var(date_var), '%Y-%m-%d').date() -%}
  {%- set week_start = base_date - modules.datetime.timedelta(days=base_date.weekday()) -%}
  {{- week_start.isoformat() -}}
{%- endmacro %}

{% macro pg_week_end_date(date_var='ds_end_date') -%}
  {%- set base_date = modules.datetime.datetime.strptime(var(date_var), '%Y-%m-%d').date() -%}
  {%- set week_end = base_date + modules.datetime.timedelta(days=(6 - base_date.weekday())) -%}
  {{- week_end.isoformat() -}}
{%- endmacro %}

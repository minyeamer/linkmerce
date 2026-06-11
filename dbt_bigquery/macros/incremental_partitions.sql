{% macro bq_date_partitions(start_var='ds_start_date', end_var='ds_end_date') -%}
  -- Set `start_date` and `end_date` from arguments
  {%- set start_date = modules.datetime.datetime.strptime(var(start_var), '%Y-%m-%d').date() -%}
  {%- set end_date = modules.datetime.datetime.strptime(var(end_var), '%Y-%m-%d').date() -%}

  -- Generate date array between `start_date` and `end_date`
  {%- set partitions = [] -%}
  {%- for offset in range((end_date - start_date).days + 1) -%}
    {%- set partition_date = start_date + modules.datetime.timedelta(days=offset) -%}
    {%- do partitions.append("DATE('" ~ partition_date.isoformat() ~ "')") -%}
  {%- endfor -%}

  {%- do return(partitions) -%}
{%- endmacro %}

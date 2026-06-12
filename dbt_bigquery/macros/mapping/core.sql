{% macro core__product_renewal_mapping() -%}
SELECT *
FROM UNNEST([
  STRUCT('100169' AS product_id_old, '100863' AS product_id_new, DATE(2026, 2, 10) AS renewal_date)
])
{%- endmacro %}

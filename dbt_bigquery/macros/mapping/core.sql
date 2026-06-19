{% macro core__product_renewal_mapping() -%}
SELECT *
FROM UNNEST([
  STRUCT('100169' AS product_id_old, '100863' AS product_id_new, DATE(2026, 2, 10) AS renewal_date)
])
{%- endmacro %}

{% macro core__product_delivery_unit() -%}
SELECT *
FROM UNNEST([
    STRUCT('100330' AS product_id, 100 AS unit)
  , STRUCT('100399' AS product_id, 100 AS unit)
])
{%- endmacro %}

{% macro core__order_status_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, '정상' AS label, '*' AS metrics)
  , STRUCT(1 AS seq, 1 AS code, '반품' AS label, 'delivery_fee' AS metrics)
  , STRUCT(2 AS seq, 2 AS code, '교환' AS label, 'supply_cost, delivery_fee' AS metrics)
  , STRUCT(3 AS seq, 3 AS code, '증정' AS label, 'supply_cost' AS metrics)
  , STRUCT(4 AS seq, 5 AS code, '빈박스' AS label, 'delivery_fee' AS metrics)
  , STRUCT(5 AS seq, 7 AS code, '배송' AS label, 'delivery_fee' AS metrics)
  , STRUCT(6 AS seq, 8 AS code, '광고' AS label, 'ad_cost' AS metrics)
  , STRUCT(7 AS seq, 9 AS code, '비용' AS label, 'extra_cost' AS metrics)
])
{%- endmacro %}

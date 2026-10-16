{% macro coupang__product_status_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, '심사중' AS label)
  , STRUCT(1 AS seq, 1 AS code, '임시저장' AS label)
  , STRUCT(2 AS seq, 2 AS code, '승인대기중' AS label)
  , STRUCT(3 AS seq, 3 AS code, '승인완료' AS label)
  , STRUCT(4 AS seq, 4 AS code, '부분승인완료' AS label)
  , STRUCT(5 AS seq, 5 AS code, '승인반려' AS label)
  , STRUCT(6 AS seq, 6 AS code, '상품삭제' AS label)
])
{%- endmacro %}

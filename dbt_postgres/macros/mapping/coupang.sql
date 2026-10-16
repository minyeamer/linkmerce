{% macro coupang__product_status_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, '심사중')
  , (1, 1, '임시저장')
  , (2, 2, '승인대기중')
  , (3, 3, '승인완료')
  , (4, 4, '부분승인완료')
  , (5, 5, '승인반려')
  , (6, 6, '상품삭제')
) AS mapping(seq, code, label)
{%- endmacro %}

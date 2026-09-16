{% macro ebay__sell_status_mapping() %}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, '11' AS code, '판매가능' AS label)
  , STRUCT(1 AS seq, '21' AS code, '판매불가' AS label)
  , STRUCT(2 AS seq, '22' AS code, '판매중지' AS label)
  , STRUCT(3 AS seq, '31' AS code, 'SKU품절' AS label)
  , STRUCT(4 AS seq, '01' AS code, '등록대기' AS label)
])
{% endmacro %}

{% macro ebay_ads__status_mapping() %}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 1 AS code, '운영중' AS label)
  , STRUCT(1 AS seq, 2 AS code, '운영대기' AS label)
  , STRUCT(2 AS seq, 3 AS code, '운영종료' AS label)
  , STRUCT(3 AS seq, 4 AS code, '운영중지' AS label)
  , STRUCT(4 AS seq, 5 AS code, '운영제한' AS label)
])
{% endmacro %}

{% macro ebay_ads__campaign_group_type_mapping() %}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 107020 AS code, 'G마켓-통합운영형' AS label)
  , STRUCT(1 AS seq, 109020 AS code, 'G마켓-집중운영형' AS label)
  , STRUCT(2 AS seq, 111020 AS code, 'G마켓-직접운영형' AS label)
  , STRUCT(3 AS seq, 1 AS code, '옥션-AI매출업' AS label)
  , STRUCT(4 AS seq, 2 AS code, '옥션-파워클릭' AS label)
])
{% endmacro %}
{% macro coupang_ads__vendor_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, 'Wing' AS label)
  , STRUCT(1 AS seq, 1 AS code, '서플라이어 허브' AS label)
])
{%- endmacro %}

{% macro coupang_ads__campaign_type_mapping() -%}
SELECT *
FROM UNNEST([
  STRUCT(0 AS seq, 'PA' AS code, '상품광고' AS label)
])
{%- endmacro %}

{% macro coupang_ads__goal_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, '매출 성장' AS label)
  , STRUCT(1 AS seq, 1 AS code, '신규 구매 고객 확보' AS label)
  , STRUCT(2 AS seq, 2 AS code, '인지도 상승' AS label)
])
{%- endmacro %}

{% macro coupang_ads__placement_group_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, '검색 영역' AS label)
  , STRUCT(1 AS seq, 1 AS code, '비검색 영역' AS label)
  , STRUCT(2 AS seq, 2 AS code, '리타겟팅(외부 채널)' AS label)
])
{%- endmacro %}

{% macro coupang_ads__vendor_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, 'Wing')
  , (1, 1, '서플라이어 허브')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro coupang_ads__campaign_type_mapping() -%}
SELECT *
FROM (VALUES
  (0, 'PA', '상품광고')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro coupang_ads__goal_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, '매출 성장')
  , (1, 1, '신규 구매 고객 확보')
  , (2, 2, '인지도 상승')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro coupang_ads__placement_group_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, '검색 영역')
  , (1, 1, '비검색 영역')
  , (2, 2, '리타겟팅(외부 채널)')
) AS mapping(seq, code, label)
{%- endmacro %}

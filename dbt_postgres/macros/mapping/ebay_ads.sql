{% macro ebay__sell_status_mapping() -%}
SELECT *
FROM (VALUES
    (0, '11', '판매가능')
  , (1, '21', '판매불가')
  , (2, '22', '판매중지')
  , (3, '31', 'SKU품절')
  , (4, '01', '등록대기')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro ebay_ads__status_mapping() -%}
SELECT *
FROM (VALUES
    (0, 1, '운영중')
  , (1, 2, '운영대기')
  , (2, 3, '운영종료')
  , (3, 4, '운영중지')
  , (4, 5, '운영제한')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro ebay_ads__campaign_group_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 107020, 'G마켓-통합운영형')
  , (1, 109020, 'G마켓-집중운영형')
  , (2, 111020, 'G마켓-직접운영형')
  , (3, 1, '옥션-AI매출업')
  , (4, 2, '옥션-파워클릭')
) AS mapping(seq, code, label)
{%- endmacro %}

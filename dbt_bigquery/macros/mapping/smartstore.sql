{% macro smartstore__product_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, '단일상품' AS label)
  , STRUCT(1 AS seq, 1 AS code, '옵션상품' AS label)
  , STRUCT(2 AS seq, 2 AS code, '추가구성상품' AS label)
])
{%- endmacro %}

{% macro smartstore__payment_location_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, 'PC' AS label)
  , STRUCT(1 AS seq, 1 AS code, '모바일' AS label)
])
{%- endmacro %}

{% macro smartstore__delivery_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, '일반배송' AS label)
  , STRUCT(1 AS seq, 1 AS code, '오늘출발' AS label)
  , STRUCT(2 AS seq, 2 AS code, '옵션별 오늘출발' AS label)
  , STRUCT(3 AS seq, 3 AS code, '희망일배송' AS label)
  , STRUCT(4 AS seq, 4 AS code, '당일배송' AS label)
  , STRUCT(5 AS seq, 5 AS code, '새벽배송' AS label)
  , STRUCT(6 AS seq, 6 AS code, '예약구매' AS label)
  , STRUCT(7 AS seq, 7 AS code, 'N배송' AS label)
  , STRUCT(8 AS seq, 8 AS code, 'N판매자배송' AS label)
  , STRUCT(9 AS seq, 9 AS code, 'N희망일배송' AS label)
  , STRUCT(10 AS seq, 10 AS code, '픽업' AS label)
  , STRUCT(11 AS seq, 11 AS code, '즉시배달' AS label)
])
{%- endmacro %}

{% macro smartstore__delivery_tag_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'TODAY' AS code, '오늘배송' AS label)
  , STRUCT(1 AS seq, 'TOMORROW' AS code, '내일배송' AS label)
  , STRUCT(2 AS seq, 'DAWN' AS code, '새벽배송' AS label)
  , STRUCT(3 AS seq, 'SUNDAY' AS code, '일요배송' AS label)
  , STRUCT(4 AS seq, 'STANDARD' AS code, 'D+2이상배송' AS label)
  , STRUCT(5 AS seq, 'HOPE' AS code, '희망일배송' AS label)
])
{%- endmacro %}

{% macro smartstore__order_status_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, '결제 대기' AS label)
  , STRUCT(1 AS seq, 1 AS code, '결제 완료' AS label)
  , STRUCT(2 AS seq, 2 AS code, '배송 중' AS label)
  , STRUCT(3 AS seq, 3 AS code, '배송 완료' AS label)
  , STRUCT(4 AS seq, 4 AS code, '구매 확정' AS label)
  , STRUCT(5 AS seq, 5 AS code, '교환' AS label)
  , STRUCT(6 AS seq, 6 AS code, '취소' AS label)
  , STRUCT(7 AS seq, 7 AS code, '반품' AS label)
  , STRUCT(8 AS seq, 8 AS code, '미결제 취소' AS label)
])
{%- endmacro %}

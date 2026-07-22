{% macro smartstore__delivery_product_mapping() -%}
SELECT *
FROM (VALUES
    (7, '100177', '100732')
  , (7, '100182', '100733')
) AS mapping(delivery_type, original_product_id, delivery_product_id)
{%- endmacro %}

{% macro smartstore__product_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, '단품상품')
  , (1, 1, '옵션상품')
  , (2, 2, '추가상품')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro smartstore__product_status_mapping() -%}
SELECT *
FROM (VALUES
    (0, 'WAIT', '판매대기')
  , (1, 'SALE', '판매중')
  , (2, 'OUTOFSTOCK', '품절')
  , (3, 'UNADMISSION', '승인대기')
  , (4, 'REJECTION', '승인거부')
  , (5, 'SUSPENSION', '판매중지')
  , (6, 'CLOSE', '판매종료')
  , (7, 'PROHIBITION', '판매금지')
  , (8, 'DELETE', '삭제')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro smartstore__display_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 'WAIT', '전시대기')
  , (1, 'ON', '전시중')
  , (2, 'SUSPENSION', '전시중지')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro smartstore__payment_location_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, 'PC')
  , (1, 1, '모바일')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro smartstore__delivery_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, '일반배송')
  , (1, 1, '오늘출발')
  , (2, 2, '옵션별 오늘출발')
  , (3, 3, '희망일배송')
  , (4, 4, '당일배송')
  , (5, 5, '새벽배송')
  , (6, 6, '예약구매')
  , (7, 7, 'N배송')
  , (8, 8, 'N판매자배송')
  , (9, 9, 'N희망일배송')
  , (10, 10, '픽업')
  , (11, 11, '즉시배달')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro smartstore__delivery_tag_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 'TODAY', '오늘배송')
  , (1, 'TOMORROW', '내일배송')
  , (2, 'DAWN', '새벽배송')
  , (3, 'SUNDAY', '일요배송')
  , (4, 'STANDARD', 'D+2이상배송')
  , (5, 'HOPE', '희망일배송')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro smartstore__order_status_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, '결제 대기')
  , (1, 1, '결제 완료')
  , (2, 2, '배송 중')
  , (3, 3, '배송 완료')
  , (4, 4, '구매 확정')
  , (5, 5, '교환')
  , (6, 6, '취소')
  , (7, 7, '반품')
  , (8, 8, '미결제 취소')
) AS mapping(seq, code, label)
{%- endmacro %}

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
    STRUCT(0 AS seq, 0 AS code, '정상' AS label, 'sku_quantity, payment_amount, supply_amount, supply_cost, delivery_fee' AS metrics)
  , STRUCT(1 AS seq, 1 AS code, '반품' AS label, 'delivery_fee' AS metrics)
  , STRUCT(2 AS seq, 2 AS code, '교환' AS label, 'supply_cost, delivery_fee' AS metrics)
  , STRUCT(3 AS seq, 3 AS code, '취소' AS label, '' AS metrics)
  , STRUCT(4 AS seq, 5 AS code, '빈박스' AS label, 'delivery_fee' AS metrics)
  , STRUCT(5 AS seq, 6 AS code, '증정' AS label, '0 AS sku_quantity, 0 AS payment_amount, 0 AS supply_amount, supply_cost, 0 AS delivery_fee' AS metrics)
  , STRUCT(6 AS seq, 7 AS code, '배송' AS label, 'delivery_fee' AS metrics)
  , STRUCT(7 AS seq, 8 AS code, '광고' AS label, 'ad_cost' AS metrics)
  , STRUCT(8 AS seq, 9 AS code, '비용' AS label, 'extra_cost' AS metrics)
])
{%- endmacro %}

{% macro core__unpivot_metric_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 1 AS sort_seq, NULL AS sub_seq, 'profit' AS name_en, '이익' AS name_ko)
  , STRUCT(1 AS seq, 2 AS sort_seq, NULL AS sub_seq, 'unit_quantity' AS name_en, '세트수량' AS name_ko)
  , STRUCT(2 AS seq, 3 AS sort_seq, NULL AS sub_seq, 'payment_amount' AS name_en, '결제금액' AS name_ko)
  , STRUCT(3 AS seq, 4 AS sort_seq, NULL AS sub_seq, 'supply_amount' AS name_en, '정산금액' AS name_ko)
  , STRUCT(4 AS seq, 5 AS sort_seq, NULL AS sub_seq, 'supply_cost' AS name_en, '원가*수량' AS name_ko)
  , STRUCT(5 AS seq, 6 AS sort_seq, NULL AS sub_seq, 'delivery_fee' AS name_en, '배송비' AS name_ko)
  , STRUCT(6 AS seq, 7 AS sort_seq, NULL AS sub_seq, 'margin_amount' AS name_en, '마진금액' AS name_ko)
  , STRUCT(7 AS seq, 8 AS sort_seq, 0 AS sub_seq, 'ad_cost' AS name_en, '광고비' AS name_ko)
  , STRUCT(8 AS seq, 8 AS sort_seq, 1 AS sub_seq, 'ad_cost__searchad' AS name_en, '광고비(네이버)' AS name_ko)
  , STRUCT(9 AS seq, 8 AS sort_seq, 2 AS sub_seq, 'ad_cost__coupang' AS name_en, '광고비(쿠팡)' AS name_ko)
  , STRUCT(10 AS seq, 8 AS sort_seq, 3 AS sub_seq, 'ad_cost__google' AS name_en, '광고비(구글)' AS name_ko)
  , STRUCT(11 AS seq, 8 AS sort_seq, 4 AS sub_seq, 'ad_cost__meta' AS name_en, '광고비(메타)' AS name_ko)
  , STRUCT(12 AS seq, 8 AS sort_seq, 5 AS sub_seq, 'ad_cost__tiktok' AS name_en, '광고비(틱톡)' AS name_ko)
  , STRUCT(13 AS seq, 9 AS sort_seq, 0 AS sub_seq, 'extra_cost' AS name_en, '지출비용' AS name_ko)
  , STRUCT(14 AS seq, 9 AS sort_seq, 1 AS sub_seq, 'extra_cost__marketing' AS name_en, '마케팅비용' AS name_ko)
  , STRUCT(15 AS seq, 9 AS sort_seq, 2 AS sub_seq, 'extra_cost__sales' AS name_en, '영업비용' AS name_ko)
  , STRUCT(16 AS seq, 9 AS sort_seq, 3 AS sub_seq, 'extra_cost__expense' AS name_en, '고정지출(재무)' AS name_ko)
  , STRUCT(17 AS seq, 10 AS sort_seq, NULL AS sub_seq, 'roi__top' AS name_en, 'ROI(%)' AS name_ko)
  , STRUCT(18 AS seq, 10 AS sort_seq, NULL AS sub_seq, 'roi__bottom' AS name_en, 'ROI(%)' AS name_ko)
])
{%- endmacro %}

{% macro core__dayofweek_name_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 1 AS dayofweek, '(일)' AS name_ko)
  , STRUCT(1 AS seq, 2 AS dayofweek, '(월)' AS name_ko)
  , STRUCT(2 AS seq, 3 AS dayofweek, '(화)' AS name_ko)
  , STRUCT(3 AS seq, 4 AS dayofweek, '(수)' AS name_ko)
  , STRUCT(4 AS seq, 5 AS dayofweek, '(목)' AS name_ko)
  , STRUCT(5 AS seq, 6 AS dayofweek, '(금)' AS name_ko)
  , STRUCT(6 AS seq, 7 AS dayofweek, '(토)' AS name_ko)
])
{%- endmacro %}

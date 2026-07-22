{% macro core__product_renewal_mapping() -%}
SELECT *
FROM (VALUES
  ('100169', '100863', make_date(2026, 2, 10))
) AS mapping(product_id_old, product_id_new, renewal_date)
{%- endmacro %}

{% macro core__product_delivery_unit() -%}
SELECT *
FROM (VALUES
    ('100330', 100)
  , ('100399', 100)
) AS mapping(product_id, unit)
{%- endmacro %}

{% macro core__order_status_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, '정상', 'sku_quantity, payment_amount, supply_amount, supply_cost, delivery_fee')
  , (1, 1, '반품', 'delivery_fee')
  , (2, 2, '교환', 'supply_cost, delivery_fee')
  , (3, 3, '취소', '')
  , (4, 5, '빈박스', 'delivery_fee')
  , (5, 6, '증정', 'supply_cost')
  , (6, 7, '배송', 'delivery_fee')
  , (7, 8, '광고', 'ad_cost')
  , (8, 9, '비용', 'extra_cost')
) AS mapping(seq, code, label, metrics)
{%- endmacro %}

{% macro core__unpivot_metric_mapping() -%}
SELECT *
FROM (VALUES
    (0, 1, NULL, 'profit', '이익')
  , (1, 2, NULL, 'unit_quantity', '세트수량')
  , (2, 3, NULL, 'payment_amount', '결제금액')
  , (3, 4, NULL, 'supply_amount', '정산금액')
  , (4, 5, NULL, 'supply_cost', '원가*수량')
  , (5, 6, NULL, 'delivery_fee', '배송비')
  , (6, 7, NULL, 'margin_amount', '마진금액')
  , (7, 8, 0, 'ad_cost', '광고비')
  , (8, 8, 1, 'ad_cost__searchad', '광고비(네이버)')
  , (9, 8, 2, 'ad_cost__coupang', '광고비(쿠팡)')
  , (10, 8, 3, 'ad_cost__google', '광고비(구글)')
  , (11, 8, 4, 'ad_cost__meta', '광고비(메타)')
  , (12, 8, 5, 'ad_cost__tiktok', '광고비(틱톡)')
  , (13, 9, 0, 'extra_cost', '지출비용')
  , (14, 9, 1, 'extra_cost__marketing', '마케팅비용')
  , (15, 9, 2, 'extra_cost__sales', '영업비용')
  , (16, 9, 3, 'extra_cost__expense', '고정지출(재무)')
  , (17, 10, NULL, 'roi__top', 'ROI(%)')
  , (18, 10, NULL, 'roi__bottom', 'ROI(%)')
) AS mapping(seq, sort_seq, sub_seq, name_en, name_ko)
{%- endmacro %}

{% macro core__dayofweek_name_mapping() -%}
SELECT *
FROM (VALUES
    (0, 1, '(일)')
  , (1, 2, '(월)')
  , (2, 3, '(화)')
  , (3, 4, '(수)')
  , (4, 5, '(목)')
  , (5, 6, '(금)')
  , (6, 7, '(토)')
) AS mapping(seq, dayofweek, name_ko)
{%- endmacro %}

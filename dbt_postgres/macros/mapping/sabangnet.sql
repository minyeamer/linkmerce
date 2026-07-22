{% macro sabangnet__shop_id_rules() -%}
CASE
  WHEN starts_with(order_id, '병원출고') THEN 'chop9022'
  ELSE shop_id
END
{%- endmacro %}

{% macro sabangnet__bundle_option_rules() -%}
CASE
  WHEN (option_id = '100345-0001') AND (sku_quantity >= 100) THEN '100330-0001:1'
  WHEN (option_id = '100377-0001') AND (sku_quantity >= 100) THEN '100741-0001:1'
  WHEN bundle_option_ids IS NULL THEN NULL
  WHEN bundle_option_ids = '100345-0001:100' THEN '100330-0001:1'
  WHEN bundle_option_ids = '100377-0001:100' THEN '100741-0001:1'
  ELSE bundle_option_ids
END
{%- endmacro %}

{% macro sabangnet__order_status_rules() -%}
CASE
  WHEN order_status_cor IS NOT NULL THEN order_status_cor
  WHEN order_status_sbn IN (9, 12, 25, 26) THEN 1
  WHEN order_status_sbn IN (8, 11, 21, 22, 23, 24) THEN 2
  WHEN order_status_sbn IN (7, 10, 999) THEN 3
  ELSE 0
END
{%- endmacro %}

{% macro sabangnet__sku_quantity_rules() -%}
CASE
  WHEN option_id IN ('100330-0001', '100741-0001') THEN order_quantity
  ELSE sku_quantity
END
{%- endmacro %}

{% macro sabangnet__payment_amount_rules() -%}
CASE
  WHEN (shop_id = 'shop0666') AND (SUM(payment_amount) OVER (PARTITION BY account_no, order_id) < 19800)
    THEN SUM(payment_amount) OVER (PARTITION BY account_no, order_id) + 3000
  WHEN ROW_NUMBER() OVER (PARTITION BY account_no, order_id ORDER BY order_seq) = 1
    THEN COALESCE(SUM(payment_amount) OVER (PARTITION BY account_no, order_id), 0)
  ELSE 0
END
{%- endmacro %}

{% macro sabangnet__net_rate_rules() -%}
CASE
  WHEN shop_id = 'shop0273'
    AND product_id_shop = '147454696'
    AND order_dt BETWEEN '2025-10-30 17:00:00' AND '2025-11-02 23:59:59' THEN 0.9
  ELSE 1.0 - commission_rate
END
{%- endmacro %}

{% macro sabangnet__product_status_mapping() -%}
SELECT *
FROM (VALUES
    (0, 1, '대기중')
  , (1, 2, '공급중')
  , (2, 3, '일시중지')
  , (3, 4, '완전품절')
  , (4, 5, '미사용')
  , (5, 6, '삭제')
  , (6, 7, '자료없음')
  , (7, 8, '비노출')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro sabangnet__option_status_mapping() -%}
SELECT *
FROM (VALUES
    (0, 1, '판매')
  , (1, 2, '일시품절')
  , (2, 3, '미사용')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro sabangnet__option_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, '대표') -- custom type
  , (1, 1, '세트')
  , (2, 2, '모음전')
  , (3, 3, '일반')
) AS mapping(seq, code, label)
{%- endmacro %}

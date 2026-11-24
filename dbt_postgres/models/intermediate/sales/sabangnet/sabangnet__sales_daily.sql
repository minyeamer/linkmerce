{{
  config(
    materialized = 'partitioned_table',
    schema = 'xfm_sales',
    partition_by = {
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = pg_date_partitions('ds_start_date', 'ds_end_date')
  )
}}

WITH{#

-- order_status IN (0, 1, 2, 3, 5)

#} ecount_product AS (
  SELECT DISTINCT ON (option_id)
      option_id
    , org_price
  FROM {{ source('ecount', 'product') }}
  ORDER BY option_id, expiration_date ASC, product_code DESC
),{#

#} order_invoice AS (
  SELECT
      order_seq
    , MAX(invoice_no) AS invoice_no
    , MAX(order_status) AS order_status
  FROM {{ source('sabangnet', 'order_invoice') }}
  WHERE order_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND order_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
  GROUP BY order_seq
),{#

#} order_status AS (
  SELECT
      cor.order_id
    , MAX(cor.order_status) AS order_status
  FROM {{ source('core', 'order_status') }} AS cor
  WHERE cor.order_date BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
    AND cor.shop_name NOT IN ('스마트스토어', '쿠팡')
  GROUP BY cor.order_id
),{#

#} order_detail AS (
  SELECT
      ord.order_seq
    , ord.order_id
    , sbn.invoice_no
    -- Sales dimensions
    , acc.shop_id
    , ord.account_no
    , (string_to_array(ord.option_id, '-'))[1] AS product_id
    , ord.option_id
    , opt.bundle_option_ids
    , ord.product_id_shop
    , cor.order_status AS order_status_cor
    , COALESCE(sbn.order_status, 1) AS order_status_sbn
    -- Sales metrics
    , COALESCE(ord.order_quantity, 0) AS order_quantity
    , COALESCE(ord.sku_quantity, 0) AS sku_quantity
    , COALESCE(ord.payment_amount, 0) AS payment_amount
    , COALESCE(acc.commission_rate, 0.0) AS commission_rate
    -- Sales partition key
    , ord.order_dt
  FROM {{ source('sabangnet', 'order') }} AS ord
  LEFT JOIN {{ source('sabangnet', 'account') }} AS acc
    ON ord.account_no = acc.account_no
  -- Resolve bundle_product_ids
  LEFT JOIN {{ source('sabangnet', 'option') }} AS opt
    ON ord.option_id = opt.option_id
  -- Resolve order_status
  LEFT JOIN order_invoice AS sbn
    ON ord.order_seq = sbn.order_seq
  LEFT JOIN order_status AS cor
    ON ord.order_id = cor.order_id
  -- Filter orders
  WHERE ord.order_dt >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND ord.order_dt < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
    AND acc.exclude_sales IS NOT TRUE
),{#

#} bundle_product_order AS (
  SELECT
      order_seq
    , order_id
    , invoice_no
    , shop_id
    , account_no
    , product_id
    , option_id
    , 1 AS product_quantity
    , bundle_option_ids
    , order_status
    , order_quantity
    , payment_amount
    , ROUND(payment_amount::numeric * net_rate::numeric, 0)::integer AS supply_amount
    , order_date
  FROM (
    SELECT
        order_seq
      , order_id
      , invoice_no
      -- Sales dimensions
      , ({{ sabangnet__shop_id_rules() }}) AS shop_id
      , account_no
      , product_id
      , option_id
      , ({{ sabangnet__bundle_option_rules() }}) AS bundle_option_ids
      , ({{ sabangnet__order_status_rules() }}) AS order_status
      -- Sales metrics
      , order_quantity
      , ({{ sabangnet__payment_amount_rules() }}) AS payment_amount
      , ({{ sabangnet__net_rate_rules() }}) AS net_rate
      -- Sales partition key
      , (order_dt)::date AS order_date
    FROM order_detail
  ) AS t_
  WHERE shop_id != 'chop9022'
),{#

#} exploded_option_order AS (
  SELECT
      ord.order_seq
    , ord.order_id
    , ord.invoice_no
    -- Sales dimensions
    , ord.shop_id
    , ord.account_no
    , (string_to_array(bundle_option, '-'))[1] AS product_id
    , (string_to_array(bundle_option, ':'))[1] AS option_id
    , (CASE
        WHEN split_part(bundle_option, ':', 2) ~ '^[0-9]+$'
          THEN split_part(bundle_option, ':', 2)::integer
        ELSE 1
      END) AS product_quantity
    , ord.bundle_option_ids
    , ord.order_status
    -- Sales metrics
    , ord.order_quantity
    , (CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ord.account_no, ord.order_id ORDER BY ord.order_seq) = 1
          THEN MAX(ord.payment_amount) OVER (PARTITION BY ord.account_no, ord.order_id)
        ELSE 0
      END) AS payment_amount
    , (CASE
        WHEN ROW_NUMBER() OVER (PARTITION BY ord.account_no, ord.order_id ORDER BY ord.order_seq) = 1
          THEN MAX(ord.supply_amount) OVER (PARTITION BY ord.account_no, ord.order_id)
        ELSE 0
      END) AS supply_amount
    -- Sales partition key
    , ord.order_date
  FROM bundle_product_order AS ord
  CROSS JOIN LATERAL unnest(string_to_array(ord.bundle_option_ids, ',')) AS t(bundle_option)
  WHERE ord.bundle_option_ids IS NOT NULL
),{#

#} exploded_product_order AS (
  SELECT
      ord.order_date
    -- Sales dimensions
    , ord.order_id
    , ord.invoice_no
    , ord.shop_id
    , ord.account_no
    , ord.product_id
    , ord.product_quantity
    , ord.order_status
    -- Sales metrics
    , ord.order_quantity
    , ord.payment_amount
    , ord.supply_amount
    -- Cost and delivery data
    , COALESCE(prd.org_price, itm.org_price, 0) + COALESCE(itm.extra_cost, 0) AS org_price
    , COALESCE(itm.delivery_group, '-') AS delivery_group
    , COALESCE(itm.delivery_fee, 0) AS delivery_fee
  FROM (
    (SELECT * FROM bundle_product_order WHERE bundle_option_ids IS NULL)
    UNION ALL
    (SELECT * FROM exploded_option_order)
  ) AS ord
  LEFT JOIN ecount_product AS prd
    ON ord.option_id = prd.option_id
  LEFT JOIN {{ source('core', 'item') }} AS itm
    ON ord.product_id = itm.product_id
),

{%- set input_dimensions = [
    'order_id'
  , 'invoice_no'
  , 'shop_id'
  , 'account_no'
] -%}

{{ split_sales_amounts(
    input_dimensions = input_dimensions,
    order_partitions = ['account_no', 'order_id'],
    agg_func = 'sum'
) }},{#

#} {{ split_delivery_fee(
    input_dimensions = input_dimensions,
    delivery_partitions = ['order_id', 'invoice_no'],
    cj_invoice_key = 'invoice_no'
) }},{#

#} sales_daily AS (
  SELECT
      shop_id
    , account_no
    , product_id
    , product_quantity
    , order_status
    , SUM(order_quantity) AS order_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , order_date
  FROM product_order_with_delivery_fee
  GROUP BY order_date, shop_id, account_no, product_id, product_quantity, order_status
){#

#} SELECT * FROM sales_daily

{{
  config(
    materialized = 'incremental',
    schema = 'xfm_sales',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = true
  )
}}

WITH

-- order_status IN (0, 1, 2, 3, 5)

order_invoice AS (
  SELECT
      order_seq
    , MAX(invoice_no) AS invoice_no
    , MAX(order_status) AS order_status
  FROM {{ source('sabangnet', 'order_invoice') }}
  WHERE order_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND order_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
  GROUP BY order_seq
),

order_status AS (
  SELECT
      cor.order_id
    , MAX(cor.order_status) AS order_status
  FROM {{ source('core', 'order_status') }} AS cor
  WHERE cor.order_date BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
    AND cor.shop_name NOT IN ('스마트스토어', '쿠팡')
  GROUP BY cor.order_id
),

order_detail AS (
  SELECT
      ord.order_dt
    -- Split partitions
    , ord.order_seq
    , ord.order_id
    , sbn.invoice_no
    -- Sales dimensions
    , acc.shop_id
    , ord.account_no
    , SPLIT(ord.option_id, '-')[SAFE_OFFSET(0)] AS product_id
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
  WHERE ord.order_dt >= DATETIME('{{ var("ds_start_date") }}')
    AND ord.order_dt < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
    AND acc.exclude_sales IS NOT TRUE
),

bundle_option_order AS (
  SELECT
      *
    , CAST(ROUND(CAST(payment_amount AS NUMERIC) * CAST(net_rate AS NUMERIC), 0) AS INT64) AS supply_amount
  FROM (
    SELECT
        DATE(order_dt) AS order_date
      -- Split partitions
      , order_seq
      , order_id
      , invoice_no
      -- Sales dimensions
      , ({{ sabangnet__shop_id_rules() }}) AS shop_id
      , account_no
      , product_id
      , option_id
      , 1 AS product_quantity
      , ({{ sabangnet__bundle_option_rules() }}) AS bundle_option_ids
      , ({{ sabangnet__order_status_rules() }}) AS order_status
      -- Sales metrics
      , order_quantity
      , ({{ sabangnet__payment_amount_rules() }}) AS payment_amount
      , ({{ sabangnet__net_rate_rules() }}) AS net_rate
      -- Sales partition key
    FROM order_detail
  ) AS t_
  WHERE shop_id != 'chop9022'
),

exploded_option_order AS (
  SELECT
      ord.order_date
    -- Split partitions
    , ord.order_seq
    , ord.order_id
    , ord.invoice_no
    -- Sales dimensions
    , ord.shop_id
    , ord.account_no
    , SPLIT(bundle_option, '-')[SAFE_OFFSET(0)] AS product_id
    , SPLIT(bundle_option, ':')[SAFE_OFFSET(0)] AS option_id
    , COALESCE(
          SAFE_CAST(SPLIT(bundle_option, ':')[SAFE_OFFSET(1)] AS INT64)
        , 1
      ) AS product_quantity
    , ord.bundle_option_ids
    , ord.order_status
    -- Sales metrics
    , ord.order_quantity
    , IF(ROW_NUMBER() OVER (PARTITION BY ord.account_no, ord.order_id ORDER BY ord.order_seq) = 1
        , MAX(ord.payment_amount) OVER (PARTITION BY ord.account_no, ord.order_id)
        , 0
      ) AS payment_amount
    , ord.net_rate
    , IF(ROW_NUMBER() OVER (PARTITION BY ord.account_no, ord.order_id ORDER BY ord.order_seq) = 1
        , MAX(ord.supply_amount) OVER (PARTITION BY ord.account_no, ord.order_id)
        , 0
      ) AS supply_amount
  FROM bundle_option_order AS ord
  CROSS JOIN UNNEST(SPLIT(ord.bundle_option_ids, ',')) AS bundle_option
  WHERE ord.bundle_option_ids IS NOT NULL
),

ecount_product AS (
  SELECT
      option_id
    , org_price
  FROM {{ source('ecount', 'product') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY option_id ORDER BY expiration_date ASC, product_code DESC) = 1
),

exploded_product_order AS (
  SELECT
      ord.order_date
    -- Split partitions
    , ord.order_id
    , ord.invoice_no
    -- Sales dimensions
    , ord.shop_id
    , ord.account_no
    , ord.product_id
    , ord.product_quantity
    , ord.order_status
    -- Sales metrics
    , ord.order_quantity
    , ord.payment_amount
    , ord.supply_amount
    , COALESCE(prd.org_price, itm.org_price, 0) + COALESCE(itm.extra_cost, 0) AS org_price
    -- Delivery data
    , COALESCE(itm.delivery_group, '-') AS delivery_group
    , COALESCE(itm.delivery_fee, 0) AS delivery_fee
  FROM (
    (SELECT * FROM bundle_option_order WHERE bundle_option_ids IS NULL)
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
) }},

{{ split_delivery_fee(
    input_dimensions = input_dimensions,
    delivery_partitions = ['order_id', 'invoice_no'],
    cj_invoice_key = 'invoice_no'
) }},

sales_daily AS (
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
)

SELECT * FROM sales_daily

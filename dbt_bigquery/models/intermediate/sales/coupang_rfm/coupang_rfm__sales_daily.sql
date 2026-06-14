{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = false
  )
}}

WITH

ecount_product AS (
  SELECT
      SPLIT(option_id, '-')[SAFE_OFFSET(0)] AS product_id
    , org_price
  FROM {{ source('ecount', 'product') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY option_id ORDER BY expiration_date ASC, product_code DESC) = 1
),

-- Step 1: prepare sales and delivery data

rocket_sales AS (
  SELECT
      order_id
    , option_id
    , vendor_id
    , MAX(settlement_type) AS order_status
    , SUM(order_quantity) AS order_quantity
    , SUM(COALESCE(unit_price, 0) * COALESCE(order_quantity, 0)
        - COALESCE(coupang_discount, 0)
        - COALESCE(seller_discount, 0)
      ) AS sales_amount
    , SUM(settlement_amount) AS settlement_amount
    , MAX(sales_date) AS sales_date
  FROM {{ source('coupang_rfm', 'sales') }}
  WHERE sales_date BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
  GROUP BY order_id, option_id, vendor_id
),

rocket_shipping AS (
  SELECT
      order_id
    , option_id
    , vendor_id
    , 7 AS order_status
    , SUM(COALESCE(warehousing_fee, 0)
        - COALESCE(discount_amount, 0)
        + COALESCE(extra_fee, 0)
      ) AS delivery_fee
    , MAX(sales_date) AS sales_date
  FROM {{ source('coupang_rfm', 'shipping') }}
  WHERE sales_date BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
  GROUP BY order_id, option_id, vendor_id
),

rocket_sales_shipping AS (
  SELECT
      COALESCE(sales.order_id, shipping.order_id) AS order_id
    , COALESCE(sales.option_id, shipping.option_id) AS option_id
    , COALESCE(sales.vendor_id, shipping.vendor_id) AS vendor_id
    , COALESCE(sales.order_status, shipping.order_status) AS order_status
    , COALESCE(sales.order_quantity, 0) AS order_quantity
    , COALESCE(sales.sales_amount, 0) AS sales_amount
    , COALESCE(sales.settlement_amount, 0) AS settlement_amount
    , COALESCE(shipping.delivery_fee, 0) AS delivery_fee
    , COALESCE(sales.sales_date, shipping.sales_date) AS sales_date
  FROM rocket_sales AS sales
  FULL OUTER JOIN rocket_shipping AS shipping
    ON sales.order_id = shipping.order_id
      AND sales.option_id = shipping.option_id
      AND sales.vendor_id = shipping.vendor_id
),

bundle_product_order AS (
  SELECT
      ord.order_id
    , ord.option_id
    -- Sales dimensions
    , COALESCE(
          rel.bundle_product_ids
        , vdr.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , ord.order_status
    -- Sales metrics
    , ord.order_quantity
    , ord.sales_amount AS payment_amount
    , ord.settlement_amount AS supply_amount
    , ord.delivery_fee
    -- Sales partition key
    , ord.sales_date AS order_date
  FROM rocket_sales_shipping AS ord
  LEFT JOIN {{ source('relation', 'cpg_opt_to_sbn_ids') }} AS rel
    ON ord.option_id = rel.option_id
  LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
    ON ord.vendor_id = vdr.vendor_id
  WHERE NOT (ord.order_quantity = 0 AND ord.delivery_fee = 0)
),

-- Step 2: explode bundle products and attach cost data

exploded_product_order AS (
  SELECT
      *
    -- Allocation metrics
    , IF(order_status = 3, 0, sku_quantity) AS actual_quantity
    , COUNT(*) OVER (PARTITION BY order_id, option_id) AS bundle_product_count
  FROM (
    SELECT
        ord.order_id
      , ord.option_id
      -- Sales dimensions
      , SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] AS product_id
      , (CASE
          WHEN (ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9') THEN 3
          ELSE ord.order_status
        END) AS order_status
      -- Sales metrics
      , (ord.order_quantity
          * COALESCE(SAFE_CAST(SPLIT(bundle_product, ':')[SAFE_OFFSET(1)] AS INT64), 1)
        ) AS sku_quantity
      , IF(LEFT(bundle_product, 1) = '9', 0, ord.payment_amount) AS payment_amount
      , IF(LEFT(bundle_product, 1) = '9', 0, ord.supply_amount) AS supply_amount
      , COALESCE(prd.org_price, itm.org_price, 0) + COALESCE(itm.extra_cost, 0) AS org_price
      , ord.delivery_fee
      -- Sales partition key
      , ord.order_date
    FROM bundle_product_order AS ord
    CROSS JOIN UNNEST(SPLIT(ord.bundle_product_ids, ',')) AS bundle_product
    LEFT JOIN ecount_product AS prd
      ON SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] = prd.product_id
    LEFT JOIN {{ source('core', 'item') }} AS itm
      ON SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] = itm.product_id
  ) AS t_
),

-- Step 3: allocate amounts across bundle products by cost weight

product_order_with_split_amount AS (
  -- Step 3-3: adjust rounding remainders to preserve the original totals
  SELECT
      order_id
    , option_id
    -- Sales dimensions
    , product_id
    , order_status
    -- Sales metrics
    , sku_quantity
    , (payment_amount_split + IF(order_option_offset = 1
        , payment_amount - SUM(payment_amount_split) OVER (PARTITION BY order_id, option_id), 0)
      ) AS payment_amount
    , (supply_amount_split + IF(order_option_offset = 1
        , supply_amount - SUM(supply_amount_split) OVER (PARTITION BY order_id, option_id), 0)
      ) AS supply_amount
    , org_price
    , (delivery_fee_split + IF(order_option_offset = 1
        , delivery_fee - SUM(delivery_fee_split) OVER (PARTITION BY order_id, option_id), 0)
      ) AS delivery_fee
    -- Sales partition key
    , order_date
  FROM (
    -- Step 3-2: split amounts by cost weight
    SELECT
        *
      , COALESCE(SAFE_CAST(payment_amount * cost_weight AS INT64), 0) AS payment_amount_split
      , COALESCE(SAFE_CAST(supply_amount * cost_weight AS INT64), 0) AS supply_amount_split
      , COALESCE(SAFE_CAST(delivery_fee * cost_weight AS INT64), 0) AS delivery_fee_split
    FROM (
      SELECT
          *
        -- Step 3-1: calculate cost weights within each order option
        , COALESCE(
            (org_price * actual_quantity)
              / NULLIF(SUM(org_price * actual_quantity) OVER (PARTITION BY order_id, option_id), 0)
            , 0.0
          ) AS cost_weight
        , ROW_NUMBER() OVER (PARTITION BY order_id, option_id ORDER BY product_id) AS order_option_offset
      FROM exploded_product_order
      WHERE bundle_product_count > 1
    ) AS t0_
  ) AS t1_
),

-- Step 4: aggregate daily sales

sales_daily AS (
  SELECT
      product_id
    , order_status
    , SUM(sku_quantity) AS sku_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(CASE
        WHEN order_status IN (1, 7) THEN 0
        ELSE sku_quantity * org_price
      END) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , order_date
  FROM (
    (SELECT * EXCEPT (actual_quantity, bundle_product_count)
    FROM exploded_product_order WHERE bundle_product_count = 1)
    UNION ALL
    (SELECT * FROM product_order_with_split_amount)
  ) AS t_
  GROUP BY order_date, product_id, order_status
)

SELECT * FROM sales_daily

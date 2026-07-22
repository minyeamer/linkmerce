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

-- order_status IN (0, 1, 3, 6, 7)

#} delivery_group AS (
  SELECT
      dlv.delivery_group
    , COALESCE(dlv.min_unit, 1) AS min_unit
    , (CASE
        WHEN MAX(dlv.min_unit) OVER (PARTITION BY dlv.delivery_group) = dlv.min_unit THEN 9999
        ELSE COALESCE(LEAD(dlv.min_unit) OVER (PARTITION BY dlv.delivery_group ORDER BY dlv.min_unit))
      END) AS max_unit
    , (COALESCE(dlv.coolant_cost, 0) + COALESCE(dlv.label_cost, 0)
      + COALESCE(dlv.wrap_cost, 0) + COALESCE(dlv.box_cost, 0)) AS extra_cost
  FROM {{ source('core', 'delivery_group') }} AS dlv
),{#

#} ecount_product AS (
  SELECT DISTINCT ON (option_id)
      (string_to_array(option_id, '-'))[1] AS product_id
    , org_price
  FROM {{ source('ecount', 'product') }}
  ORDER BY option_id, expiration_date ASC, product_code DESC
),{#

#} product_delivery_unit AS (
  {{ core__product_delivery_unit() }}
),{#

-- Step 1: prepare sales and delivery data

#} rocket_sales AS (
  SELECT
      order_id
    , option_id
    , ANY_VALUE(vendor_id) AS vendor_id
    , MAX(settlement_type) AS order_status
    , SUM(order_quantity) AS order_quantity
    , SUM(COALESCE(unit_price, 0) * COALESCE(order_quantity, 0)
        - COALESCE(coupang_discount, 0)
        - COALESCE(seller_discount, 0)
      ) AS sales_amount
    , SUM(settlement_amount) AS settlement_amount
    , MAX(sales_date) AS sales_date
  FROM {{ source('coupang_rfm', 'sales') }}
  WHERE sales_date
    BETWEEN DATE '{{ pg_week_start_date("ds_start_date") }}'
    AND DATE '{{ pg_week_end_date("ds_end_date") }}'
  GROUP BY order_id, option_id
),{#

#} rocket_shipping AS (
  SELECT
      order_id
    , option_id
    , ANY_VALUE(vendor_id) AS vendor_id
    , SUM(COALESCE(warehousing_fee, 0)
        - COALESCE(discount_amount, 0)
        + COALESCE(extra_fee, 0)
      ) AS delivery_fee
    , MAX(sales_date) AS sales_date
  FROM {{ source('coupang_rfm', 'shipping') }}
  WHERE sales_date
    BETWEEN DATE '{{ pg_week_start_date("ds_start_date") }}'
    AND DATE '{{ pg_week_end_date("ds_end_date") }}'
  GROUP BY order_id, option_id
),{#

#} rocket_sales_shipping AS (
  SELECT
      order_id
    , option_id
    , ANY_VALUE(vendor_id) AS vendor_id
    , (CASE
        WHEN MAX(order_status) IS NULL THEN 7
        ELSE LEAST(MAX(order_status), 3)
      END) AS order_status
    , COALESCE(SUM(order_quantity), 0) AS order_quantity
    , COALESCE(SUM(sales_amount), 0) AS sales_amount
    , COALESCE(SUM(settlement_amount), 0) AS settlement_amount
    , COALESCE(SUM(delivery_fee), 0) AS delivery_fee
    , sales_date
  FROM (
    SELECT
        COALESCE(sales.order_id, shipping.order_id) AS order_id
      , COALESCE(sales.option_id, shipping.option_id) AS option_id
      , COALESCE(sales.vendor_id, shipping.vendor_id) AS vendor_id
      , sales.order_status
      , sales.order_quantity
      , sales.sales_amount
      , sales.settlement_amount
      , shipping.delivery_fee
      , COALESCE(sales.sales_date, shipping.sales_date) AS sales_date
    FROM rocket_sales AS sales
    FULL OUTER JOIN rocket_shipping AS shipping
      ON sales.order_id = shipping.order_id AND sales.option_id = shipping.option_id
  ) AS t_
  GROUP BY sales_date, order_id, option_id
),{#

#} bundle_product_order AS (
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
  WHERE ord.sales_date BETWEEN {{ pg_batch_start_date() }} AND {{ pg_batch_end_date() }}
    AND NOT (ord.order_quantity = 0 AND ord.delivery_fee = 0)
),{#

-- Step 2: explode bundle products and attach cost data

#} exploded_product_order AS (
  SELECT
      ord.order_id
    , ord.option_id
    -- Sales dimensions
    , (string_to_array(bundle_product, ':'))[1] AS product_id
    , (CASE
        WHEN (ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9') THEN 6
        ELSE ord.order_status
      END) AS order_status
    -- Sales metrics
    , (CASE
        WHEN split_part(bundle_product, ':', 2) ~ '^[0-9]+$'
          THEN split_part(bundle_product, ':', 2)::integer
        ELSE 1
      END) * ord.order_quantity AS sku_quantity
    , ord.payment_amount
    , ord.supply_amount
    , COALESCE(prd.org_price, itm.org_price, 0) + COALESCE(itm.extra_cost, 0) AS org_price
    , ord.delivery_fee
    , itm.delivery_group
    -- Sales partition key
    , ord.order_date
  FROM bundle_product_order AS ord
  CROSS JOIN LATERAL unnest(string_to_array(ord.bundle_product_ids, ',')) AS t(bundle_product)
  LEFT JOIN ecount_product AS prd
    ON (string_to_array(bundle_product, ':'))[1] = prd.product_id
  LEFT JOIN {{ source('core', 'item') }} AS itm
    ON (string_to_array(bundle_product, ':'))[1] = itm.product_id
),{#

-- Step 3: add delivery extra cost per order option before amount allocation

#} product_order_with_delivery_extra AS (
  SELECT
      ord.order_id
    , ord.option_id
    -- Sales dimensions
    , ord.product_id
    , ord.order_status
    -- Sales metrics
    , ord.sku_quantity
    , ord.payment_amount
    , ord.supply_amount
    , ord.org_price * ord.sku_quantity AS supply_cost
    , (COALESCE(MAX(dlv.extra_cost) OVER (PARTITION BY order_id, option_id), 0)
      + ord.delivery_fee) AS delivery_fee
    -- Sales partition key
    , ord.order_date
    -- Allocation metrics
    , COUNT(*) OVER (PARTITION BY ord.order_id, ord.option_id) AS bundle_product_count
    , (CASE WHEN ord.order_status = 6 THEN 0 ELSE ord.org_price * ord.sku_quantity END) AS cost_amount
  FROM exploded_product_order AS ord
  LEFT JOIN product_delivery_unit AS unit
    ON ord.product_id = unit.product_id
  LEFT JOIN delivery_group AS dlv
    ON ord.delivery_group = dlv.delivery_group
      AND (ord.sku_quantity * COALESCE(unit.unit, 1)) BETWEEN dlv.min_unit AND dlv.max_unit
),{#

-- Step 4: allocate amounts across bundle products by cost weight

#} product_order_with_split_amount AS (
  -- Step 4-3: adjust rounding remainders to preserve the original totals
  SELECT
      order_id
    , option_id
    -- Sales dimensions
    , product_id
    , order_status
    -- Sales metrics
    , sku_quantity
    , (CASE
        WHEN order_option_offset = 1
          THEN payment_amount - SUM(payment_amount_split) OVER (PARTITION BY order_id, option_id)
        ELSE 0
      END) + payment_amount_split AS payment_amount
    , (CASE
        WHEN order_option_offset = 1
          THEN supply_amount - SUM(supply_amount_split) OVER (PARTITION BY order_id, option_id)
        ELSE 0
      END) + supply_amount_split AS supply_amount
    , supply_cost
    , (CASE
        WHEN order_option_offset = 1
          THEN delivery_fee - SUM(delivery_fee_split) OVER (PARTITION BY order_id, option_id)
        ELSE 0
      END) + delivery_fee_split AS delivery_fee
    -- Sales partition key
    , order_date
    -- Allocation metrics
    , bundle_product_count
    , cost_amount
  FROM (
    -- Step 4-2: split amounts by cost weight
    SELECT
        *
      , COALESCE((payment_amount * cost_weight)::integer, 0) AS payment_amount_split
      , COALESCE((supply_amount * cost_weight)::integer, 0) AS supply_amount_split
      , COALESCE((delivery_fee * cost_weight)::integer, 0) AS delivery_fee_split
    FROM (
      SELECT
          *
        -- Step 4-1: calculate cost weights within each order option
        , cost_amount::numeric / NULLIF(SUM(cost_amount) OVER (PARTITION BY order_id, option_id), 0) AS cost_weight
        , ROW_NUMBER() OVER (PARTITION BY order_id, option_id ORDER BY product_id) AS order_option_offset
      FROM product_order_with_delivery_extra
      WHERE bundle_product_count > 1
    ) AS t0_
  ) AS t1_
),{#

-- Step 5: aggregate daily sales

#} sales_daily AS (
  SELECT
      product_id
    , order_status
    , SUM(sku_quantity) AS sku_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , order_date
  FROM (
    (SELECT * FROM product_order_with_delivery_extra WHERE bundle_product_count = 1)
    UNION ALL
    (SELECT * FROM product_order_with_split_amount)
  ) AS t_
  GROUP BY order_date, product_id, order_status
){#

#} SELECT * FROM sales_daily

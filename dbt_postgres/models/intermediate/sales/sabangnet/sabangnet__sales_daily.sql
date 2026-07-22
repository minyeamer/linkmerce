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

#} delivery_group AS (
  SELECT
      dlv.delivery_group
    , COALESCE(dlv.min_unit, 1) AS min_unit
    , (CASE
        WHEN MAX(dlv.min_unit) OVER (PARTITION BY dlv.delivery_group) = dlv.min_unit THEN 9999
        ELSE COALESCE(LEAD(dlv.min_unit) OVER (PARTITION BY dlv.delivery_group ORDER BY dlv.min_unit))
      END) AS max_unit
    , COALESCE(dlv.delivery_fee, 0) AS delivery_fee
    , (COALESCE(dlv.coolant_cost, 0) + COALESCE(dlv.label_cost, 0)
      + COALESCE(dlv.wrap_cost, 0) + COALESCE(dlv.box_cost, 0)) AS extra_cost
  FROM {{ source('core', 'delivery_group') }} AS dlv
),{#

#} ecount_product AS (
  SELECT DISTINCT ON (option_id)
      option_id
    , org_price
  FROM {{ source('ecount', 'product') }}
  ORDER BY option_id, expiration_date ASC, product_code DESC
),{#

#} product_delivery_unit AS (
  {{ core__product_delivery_unit() }}
),{#

-- Step 1: prepare orders

#} order_invoice AS (
  SELECT
      order_seq
    , ANY_VALUE(invoice_no) AS invoice_no
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
    AND cor.shop_name != '스마트스토어'
  GROUP BY cor.order_id
),{#

#} order_detail AS (
  SELECT
      ord.order_seq
    , COALESCE(ord.order_id, '-') AS order_id
    , COALESCE(sbn.invoice_no, '-') AS invoice_no
    , ord.account_no
    -- Sales dimensions
    , acc.shop_id
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
    AND acc.shop_id NOT IN ('shop0055', 'chop0022', 'chop0027', 'chop0028', 'chop0029')
),{#

-- Step 2: apply bundle product rules

#} bundle_product_order AS (
  SELECT
      order_seq
    , order_id
    , invoice_no
    , account_no
    , shop_id
    , product_id
    , option_id
    , bundle_option_ids
    , order_status
    , order_quantity
    , sku_quantity
    , payment_amount
    , (payment_amount * net_rate)::integer AS supply_amount
    , order_date
  FROM (
    SELECT
        order_seq
      , order_id
      , invoice_no
      , account_no
      -- Sales dimensions
      , ({{ sabangnet__shop_id_rules() }}) AS shop_id
      , product_id
      , option_id
      , ({{ sabangnet__bundle_option_rules() }}) AS bundle_option_ids
      , ({{ sabangnet__order_status_rules() }}) AS order_status
      -- Sales metrics
      , order_quantity
      , ({{ sabangnet__sku_quantity_rules() }}) AS sku_quantity
      , ({{ sabangnet__payment_amount_rules() }}) AS payment_amount
      , ({{ sabangnet__net_rate_rules() }}) AS net_rate
      -- Sales partition key
      , (order_dt)::date AS order_date
    FROM order_detail
  ) AS t_
  WHERE shop_id != 'chop9022'
),{#

-- Step 3: explode bundle products with bundle options

#} exploded_product_order AS (
  SELECT
      ord.order_seq
    , ord.order_id
    , ord.invoice_no
    , ord.account_no
    -- Sales dimensions
    , ord.shop_id
    , (string_to_array(bundle_option, '-'))[1] AS product_id
    , (string_to_array(bundle_option, ':'))[1] AS option_id
    , ord.bundle_option_ids
    , ord.order_status
    , ord.order_quantity
    -- Sales metrics
    , (CASE
        WHEN split_part(bundle_option, ':', 2) ~ '^[0-9]+$'
          THEN split_part(bundle_option, ':', 2)::integer
        ELSE 1
      END) * ord.order_quantity AS sku_quantity
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
  WHERE ord.bundle_option_ids IS NULL
),{#

-- Step 4: attach cost data

#} product_order_with_cost_data AS (
  SELECT
      *
    -- Allocation metrics
    , COUNT(*) OVER (PARTITION BY account_no, order_id) AS bundle_product_count
    , org_price * sku_quantity AS cost_amount
  FROM (
    SELECT
        ord.order_id
      , ord.invoice_no
      , ord.account_no
      -- Sales dimensions
      , ord.shop_id
      , ord.product_id
      , ord.order_status
      -- Sales metrics
      , ord.sku_quantity
      , ord.payment_amount
      , ord.supply_amount
      -- Cost data
      , COALESCE(prd.org_price, itm.org_price, 0) + COALESCE(itm.extra_cost, 0) AS org_price
      , COALESCE(itm.delivery_group, '-') AS delivery_group
      , COALESCE(itm.delivery_fee, 0) AS delivery_fee
      -- Sales partition key
      , ord.order_date
    FROM (
      (SELECT * FROM bundle_product_order WHERE bundle_option_ids IS NULL)
      UNION ALL
      (SELECT * FROM exploded_product_order)
    ) AS ord
    LEFT JOIN ecount_product AS prd
      ON ord.option_id = prd.option_id
    LEFT JOIN {{ source('core', 'item') }} AS itm
      ON ord.product_id = itm.product_id
  ) AS t_
),{#

-- Step 5: allocate amounts across order products by cost weight

#} product_order_with_split_amount AS (
  -- Step 5-3: adjust rounding remainders to preserve the original totals
  SELECT
      order_id
    , invoice_no
    , account_no
    -- Sales dimensions
    , shop_id
    , product_id
    , order_status
    -- Sales metrics
    , sku_quantity
    , (CASE
        WHEN order_offset = 1
          THEN total_payment_amount - SUM(payment_amount_split) OVER (PARTITION BY account_no, order_id)
        ELSE 0
      END) + payment_amount_split AS payment_amount
    , (CASE
        WHEN order_offset = 1
          THEN total_supply_amount - SUM(supply_amount_split) OVER (PARTITION BY account_no, order_id)
        ELSE 0
      END) + supply_amount_split AS supply_amount
    -- Cost data
    , org_price
    , delivery_group
    , delivery_fee
    -- Sales partition key
    , order_date
    -- Allocation metrics
    , bundle_product_count
    , cost_amount
  FROM (
    -- Step 5-2: split amounts by cost weight
    SELECT
        *
      , COALESCE((total_payment_amount * cost_weight)::integer, 0) AS payment_amount_split
      , COALESCE((total_supply_amount * cost_weight)::integer, 0) AS supply_amount_split
    FROM (
      SELECT
          *
        -- Step 5-1: calculate cost weights within each order
        , SUM(payment_amount) OVER (PARTITION BY account_no, order_id) AS total_payment_amount
        , SUM(supply_amount) OVER (PARTITION BY account_no, order_id) AS total_supply_amount
        , cost_amount::numeric / NULLIF(SUM(cost_amount) OVER (PARTITION BY account_no, order_id), 0) AS cost_weight
        , ROW_NUMBER() OVER (PARTITION BY account_no, order_id ORDER BY product_id) AS order_offset
      FROM product_order_with_cost_data
      WHERE bundle_product_count > 1
    ) AS t0_
  ) AS t1_
),{#

-- Step 6: prepare delivery data

#} product_order_with_cj_delivery AS (
  SELECT
      ord.order_id
    , ord.invoice_no
    -- Sales dimensions
    , ord.shop_id
    , ord.product_id
    , ord.order_status
    -- Sales metrics
    , ord.sku_quantity
    , ord.payment_amount
    , ord.supply_amount
    , ord.org_price * ord.sku_quantity AS supply_cost
    -- Delivery data
    , ord.org_price
    , ord.delivery_group
    , ord.sku_quantity * COALESCE(unit.unit, 1) AS delivery_quantity
    , COALESCE(cj_inv.delivery_fee, cj_ord.delivery_fee, ord.delivery_fee, 0) AS delivery_fee
    , COALESCE(cj_inv.box_cost, cj_ord.box_cost, 0) AS box_cost
    -- Sales partition key
    , ord.order_date
    -- Allocation metrics
    , ord.cost_amount
  FROM (
    (SELECT * FROM product_order_with_cost_data WHERE bundle_product_count = 1)
    UNION ALL
    (SELECT * FROM product_order_with_split_amount)
  ) AS ord
  LEFT JOIN {{ ref('cj__invoice') }}(
      {{ pg_batch_start_date() }} - 7
    , {{ pg_batch_end_date() }} + 7
  ) AS cj_inv
    ON ord.invoice_no = cj_inv.invoice_no
  LEFT JOIN {{ ref('cj__invoice_order') }}(
      {{ pg_batch_start_date() }} - 7
    , {{ pg_batch_end_date() }} + 7
  ) AS cj_ord
    ON ord.order_id = cj_ord.order_id
  LEFT JOIN product_delivery_unit AS unit
    ON ord.product_id = unit.product_id
),{#

-- Step 7: determine the maximum delivery fee at the order level

#} max_delivery_fee AS (
  -- Step 7-3: select the delivery fee with the largest absolute value for each order
  SELECT DISTINCT ON (order_id, invoice_no) *
  FROM (
    -- Step 7-2: calculate delivery fees under each delivery group rule
    SELECT
        ord.order_id
      , ord.invoice_no
      , ord.delivery_group
      , (CASE
          WHEN dlv.delivery_group IS NULL
            THEN ord.delivery_fee
          WHEN ord.delivery_fee > 0
            THEN (ord.delivery_fee + (CASE WHEN ord.box_cost > 0 THEN ord.box_cost ELSE dlv.extra_cost END))
          ELSE dlv.delivery_fee + dlv.extra_cost
        END) AS delivery_fee
    FROM (
      -- Step 7-1: aggregate delivery data by each delivery group
      SELECT
          order_id
        , invoice_no
        , delivery_group
        , MAX(delivery_fee) AS delivery_fee
        , SUM(box_cost) AS box_cost
        , COALESCE(SUM(delivery_quantity), 0) AS delivery_quantity
      FROM product_order_with_cj_delivery
      GROUP BY order_id, invoice_no, delivery_group
    ) AS ord
    LEFT JOIN delivery_group AS dlv
      ON ord.delivery_group = dlv.delivery_group
        AND ord.delivery_quantity BETWEEN dlv.min_unit AND dlv.max_unit
  ) AS t_
  ORDER BY order_id, invoice_no, ABS(delivery_fee) DESC
),{#

-- Step 8: attach the maximum delivery fee to product orders

#} product_order_with_max_delivery AS (
  SELECT
      ord.order_id
    , ord.invoice_no
    -- Sales dimensions
    , ord.shop_id
    , ord.product_id
    , ord.order_status
    -- Sales metrics
    , ord.sku_quantity
    , ord.payment_amount
    , ord.supply_amount
    , ord.supply_cost
    , ord.org_price
    , dlv.delivery_fee
    -- Sales partition key
    , ord.order_date
    -- Allocation metrics
    , COUNT(*) OVER (PARTITION BY ord.order_id, ord.invoice_no) AS bundle_invoice_count
    , ord.cost_amount
  FROM product_order_with_cj_delivery AS ord
  LEFT JOIN max_delivery_fee AS dlv
    ON ord.order_id = dlv.order_id
      AND ord.invoice_no = dlv.invoice_no
),{#

-- Step 9: allocate delivery fees across complex product orders

#} product_order_with_split_delivery AS (
  -- Step 9-3: adjust rounding remainders to preserve the original totals
  SELECT
      order_id
    , invoice_no
    -- Sales dimensions
    , shop_id
    , product_id
    , order_status
    -- Sales metrics
    , sku_quantity
    , payment_amount
    , supply_amount
    , supply_cost
    , org_price
    , (CASE
        WHEN order_invoice_offset = 1
          THEN delivery_fee - (SUM(delivery_fee_split) OVER (PARTITION BY order_id, invoice_no))
        ELSE 0
      END) + delivery_fee_split AS delivery_fee
    -- Sales partition key
    , order_date
    -- Allocation metrics
    , bundle_invoice_count
    , cost_amount
  FROM (
    -- Step 9-2: split delivery fees by cost weight
    SELECT
        *
      , COALESCE((delivery_fee * cost_weight)::integer, 0) AS delivery_fee_split
    FROM (
      SELECT
          *
        -- Step 9-1: calculate cost weights within each order invoice
        , cost_amount::numeric / NULLIF(SUM(cost_amount) OVER (PARTITION BY order_id, invoice_no), 0) AS cost_weight
        , ROW_NUMBER() OVER (PARTITION BY order_id, invoice_no ORDER BY product_id) AS order_invoice_offset
      FROM product_order_with_max_delivery
      WHERE bundle_invoice_count > 1
    ) AS t0_
  ) AS t1_
),{#

-- Step 10: aggregate daily sales

#} sales_daily AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , SUM(sku_quantity) AS sku_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , order_date
  FROM (
    (SELECT * FROM product_order_with_max_delivery WHERE bundle_invoice_count = 1)
    UNION ALL
    (SELECT * FROM product_order_with_split_delivery)
  ) AS t_
  GROUP BY order_date, product_id, shop_id, order_status
){#

#} SELECT * FROM sales_daily

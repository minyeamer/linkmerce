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

#} order_invoice AS (
  SELECT
      order_seq
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

-- Step 1: prepare orders

#} order_detail AS (
  SELECT
      ord.order_id
    , ord.order_id_dup AS product_order_id
    , acc.shop_id
    , (string_to_array(ord.option_id, '-'))[1] AS product_id
    , ord.option_id
    , opt.bundle_option_ids
    , cor.order_status AS order_status_cor
    , COALESCE(sbn.order_status, 1) AS order_status_sbn
    , COALESCE(ord.order_quantity, 0) AS order_quantity
    , COALESCE(ord.sku_quantity, 0) AS sku_quantity
    , (ord.order_dt)::date AS order_date
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
  SELECT *
  FROM (
    SELECT
        order_id
      , product_order_id
      , ({{ sabangnet__shop_id_rules() }}) AS shop_id
      , product_id
      , ({{ sabangnet__bundle_option_rules() }}) AS bundle_option_ids
      , ({{ sabangnet__order_status_rules() }}) AS order_status
      , order_quantity
      , order_date
    FROM order_detail
  ) AS t_
  WHERE shop_id != 'chop9022'
),{#

-- Step 3: explode orders with bundle options

#} exploded_product_order AS (
  SELECT
      ord.order_id
    , ord.product_order_id
    , ord.shop_id
    , (string_to_array(bundle_option, '-'))[1] AS product_id
    , ord.bundle_option_ids
    , ord.order_status
    , ord.order_quantity
    , ord.order_date
  FROM bundle_product_order AS ord
  CROSS JOIN LATERAL unnest(string_to_array(ord.bundle_option_ids, ',')) AS t(bundle_option)
  WHERE ord.bundle_option_ids IS NOT NULL
),{#

#} order_count AS (
  SELECT
      order_id
    , product_order_id
    , product_id
    , shop_id
    , order_status
    , SUM(order_quantity) AS order_quantity
    , order_date
  FROM (
    (SELECT * FROM bundle_product_order WHERE bundle_option_ids IS NULL)
    UNION ALL
    (SELECT * FROM exploded_product_order)
  ) AS t_
  GROUP BY order_id, product_order_id, order_date, product_id, shop_id, order_status
){#

#} SELECT * FROM order_count

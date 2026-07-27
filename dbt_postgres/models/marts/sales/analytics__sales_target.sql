{{
  config(
    materialized = 'view',
    schema = 'analytics',
    alias = 'sales_target'
  )
}}

WITH{#

#} pivot_date AS (
  SELECT date_trunc('month',
      (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date - 1
    )::date AS start_of_month
),{#

#} prd_id_to_brd_id AS (
  SELECT
      product.product_id
    , brand.brand_id
  FROM {{ ref('core__product_master') }} AS product
  LEFT JOIN {{ ref('core__brand_master') }} AS brand
    ON product.brand_name = brand.brand_name
),{#

#} pivot_base AS (
  SELECT
      COALESCE(rel.brand_id, '200000') AS brand_id
    , fact.shop_id
    , (CASE
        WHEN date_trunc('month', fact.order_date)::date = pvt.start_of_month
          THEN 1
        ELSE 0
      END) AS group_id
    , fact.payment_amount
    , fact.order_date
  FROM {{ ref('analytics__profit_base') }}(
      (date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date - 1)
      - (1) * INTERVAL '1 month')::date
    , (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date - 1
  ) AS fact
  CROSS JOIN pivot_date AS pvt
  LEFT JOIN prd_id_to_brd_id AS rel
    ON fact.product_id = rel.product_id
  WHERE fact.order_status = 0
),{#

#} pivot_period AS (
  SELECT
      MIN((CASE WHEN group_id = 0 THEN order_date ELSE NULL END)) AS previous_start_date
    , MAX((CASE WHEN group_id = 0 THEN order_date ELSE NULL END)) AS previous_end_date
    , MIN((CASE WHEN group_id = 1 THEN order_date ELSE NULL END)) AS current_start_date
    , MAX((CASE WHEN group_id = 1 THEN order_date ELSE NULL END)) AS current_end_date
  FROM pivot_base
),{#

#} brand_sales AS (
  SELECT
      brand_id
    , shop_id
    , SUM((CASE WHEN group_id = 0 THEN payment_amount ELSE 0 END)) AS previous_sales
    , SUM((CASE WHEN group_id = 1 THEN payment_amount ELSE 0 END)) AS current_sales
  FROM pivot_base
  WHERE payment_amount != 0
  GROUP BY brand_id, shop_id
),{#

#} target_sales AS (
  SELECT
      COALESCE(fact.brand_id, tgt.brand_id) AS brand_id
    , brand.brand_name
    , COALESCE(fact.shop_id, tgt.shop_id) AS shop_id
    , shop.shop_group
    , shop.shop_alias AS shop_name
    , brand.team_name
    , COALESCE(fact.previous_sales, 0) AS previous_sales
    , COALESCE(fact.current_sales, 0) AS current_sales
    , NULLIF(tgt.monthly_amount, 0) AS target_sales
    , dt.previous_start_date
    , dt.previous_end_date
    , dt.current_start_date
    , dt.current_end_date
    , (date_trunc('month', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date - 1)
      + INTERVAL '1 month - 1 day')::date AS current_eomonth
  FROM brand_sales AS fact
  FULL OUTER JOIN {{ source('core', 'target_sales') }} AS tgt
    ON fact.brand_id = tgt.brand_id AND fact.shop_id = tgt.shop_id
  LEFT JOIN {{ ref('core__brand_master') }} AS brand
    ON COALESCE(fact.brand_id, tgt.brand_id) = brand.brand_id
  LEFT JOIN {{ source('sabangnet', 'shop') }} AS shop
    ON COALESCE(fact.shop_id, tgt.shop_id) = shop.shop_id
  CROSS JOIN pivot_period AS dt
){#

#} SELECT * FROM target_sales

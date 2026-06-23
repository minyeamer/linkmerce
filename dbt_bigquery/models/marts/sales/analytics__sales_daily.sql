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
    require_partition_filter = true,
    schema = "analytics",
    alias = "sales_daily"
  )
}}

WITH

-- Step 1: prepare sales data

sabangnet_sales_daily AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , sku_quantity
    , payment_amount
    , supply_amount
    , supply_cost
    , delivery_fee
    , NULL AS ad_cost
    , NULL AS extra_cost
    , order_date
  FROM {{ ref('sabangnet__sales_daily') }}
  WHERE order_date BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

smartstore_sales_daily AS (
  SELECT
      product_id
    , IF(delivery_type = 7, 'shop9000', 'shop0055') AS shop_id
    , order_status
    , sku_quantity
    , payment_amount
    , supply_amount
    , supply_cost
    , delivery_fee
    , NULL AS ad_cost
    , NULL AS extra_cost
    , order_date
  FROM {{ ref('smartstore__sales_daily') }}
  WHERE order_date BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

coupang_rfm_sales_daily AS (
  SELECT
      product_id
    , 'shop9001' AS shop_id
    , order_status
    , sku_quantity
    , payment_amount
    , supply_amount
    , supply_cost
    , delivery_fee
    , NULL AS ad_cost
    , NULL AS extra_cost
    , order_date
  FROM {{ ref('coupang_rfm__sales_daily') }}
  WHERE order_date BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

extra_sales_daily AS (
  SELECT
      product_id
    , shop_id
    , 0 AS order_status
    , 0 AS sku_quantity
    , sales_amount AS payment_amount
    , supply_amount
    , 0 AS supply_cost
    , 0 AS delivery_fee
    , NULL AS ad_cost
    , NULL AS extra_cost
    , sales_date AS order_date
  FROM {{ source('core', 'extra_sales') }}
  WHERE sales_date BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

-- Step 2: prepare ads data

searchad_insight_daily AS (
  SELECT
      product_id
    , 'shop0055' AS shop_id
    , SUM(ad_cost) AS ad_cost
    , ymd AS order_date
  FROM {{ ref('searchad__insight_daily') }}
  WHERE ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
  GROUP BY ymd, product_id
),

searchad_contract_daily AS (
  SELECT
      product_id
    , 'shop0055' AS shop_id
    , SUM(ad_cost) AS ad_cost
    , ymd AS order_date
  FROM {{ ref('searchad__contract_daily') }}
  WHERE ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
  GROUP BY ymd, product_id
),

coupang_ads_insight_daily AS (
  SELECT
      product_id
    , 'shop9001' AS shop_id
    , SUM(ad_cost) AS ad_cost
    , ymd AS order_date
  FROM {{ ref('coupang_ads__insight_daily') }}
  WHERE ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
  GROUP BY ymd, product_id
),

google_ads_insight_daily AS (
  SELECT
      product_id
    , 'adop0001' AS shop_id
    , SUM(ad_cost) AS ad_cost
    , ymd AS order_date
  FROM {{ ref('google_ads__insight_daily') }}
  WHERE ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
  GROUP BY ymd, product_id
),

meta_ads_insight_daily AS (
  SELECT
      product_id
    , 'adop0002' AS shop_id
    , SUM(ad_cost) AS ad_cost
    , ymd AS order_date
  FROM {{ ref('meta_ads__insight_daily') }}
  WHERE ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
  GROUP BY ymd, product_id
),

extra_ads_insight_daily AS (
  SELECT
      brand_id AS product_id
    , shop_id
    , ad_cost
    , ymd AS order_date
  FROM {{ source('core', 'extra_ads') }}
  WHERE ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

-- Step 3: assign searchad cost to the highest-sales shop_id

smartstore_product_daily AS (
  SELECT
      order_date
    , product_id
    , shop_id
  FROM (
    SELECT
        order_date
      , product_id
      , shop_id
      , SUM(payment_amount) AS payment_amount
    FROM smartstore_sales_daily
    GROUP BY order_date, product_id, shop_id
  ) AS t_
  -- Step 3-1: find the highest-sales smartstore shop_id for each product-day
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_date, product_id
    ORDER BY payment_amount DESC, shop_id ASC
  ) = 1
),

smartstore_brand_daily AS (
  SELECT
      order_date
    , brand_name
    , shop_id
  FROM (
    SELECT
        smt.order_date
      , prd.brand_name
      , smt.shop_id
      , SUM(smt.payment_amount) AS payment_amount
    FROM smartstore_sales_daily AS smt
    INNER JOIN {{ ref('core__product_master') }} AS prd
      ON NULLIF(smt.product_id, '200000') = prd.product_id
    GROUP BY smt.order_date, prd.brand_name, smt.shop_id
  ) AS t_
  -- Step 3-2: find the highest-sales smartstore shop_id for each brand-day
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_date, brand_name
    ORDER BY payment_amount DESC, shop_id ASC
  ) = 1
),

searchad_insight_daily_with_shop_mapping AS (
  SELECT
      ads.product_id
    , COALESCE(prd.shop_id, brd.shop_id, ads.shop_id) AS shop_id
    , ads.ad_cost
    , ads.order_date
  FROM (
    (SELECT * FROM searchad_insight_daily)
    UNION ALL
    (SELECT * FROM searchad_contract_daily)
  ) AS ads
  LEFT JOIN {{ ref('core__product_master') }} AS itm
    ON ads.product_id = itm.product_id
  -- Step 3-3: map searchad cost to shop_id using product first, then brand fallback
  LEFT JOIN smartstore_product_daily AS prd
    ON ads.order_date = prd.order_date AND ads.product_id = prd.product_id
  LEFT JOIN smartstore_brand_daily AS brd
    ON ads.order_date = brd.order_date AND itm.brand_name = brd.brand_name
),

-- Step 4: assign coupang-ads cost to the highest-sales shop_id

coupang_product_daily AS (
  SELECT
      order_date
    , product_id
    , shop_id
  FROM (
    SELECT
        order_date
      , product_id
      , shop_id
      , SUM(payment_amount) AS payment_amount
    FROM (
      (SELECT * FROM coupang_rfm_sales_daily)
      UNION ALL
      (SELECT * FROM sabangnet_sales_daily WHERE shop_id = 'shop0075')
    ) AS t_
    GROUP BY order_date, product_id, shop_id
  ) AS t_
  -- Step 4-1: find the highest-sales coupang shop_id for each product-day
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_date, product_id
    ORDER BY payment_amount DESC, shop_id DESC
  ) = 1
),

coupang_brand_daily AS (
  SELECT
      order_date
    , brand_name
    , shop_id
  FROM (
    SELECT
        cpg.order_date
      , prd.brand_name
      , cpg.shop_id
      , SUM(cpg.payment_amount) AS payment_amount
    FROM (
      (SELECT * FROM coupang_rfm_sales_daily)
      UNION ALL
      (SELECT * FROM sabangnet_sales_daily WHERE shop_id = 'shop0075')
    ) AS cpg
    INNER JOIN {{ ref('core__product_master') }} AS prd
      ON NULLIF(cpg.product_id, '200000') = prd.product_id
    GROUP BY cpg.order_date, prd.brand_name, cpg.shop_id
  ) AS t_
  -- Step 4-2: find the highest-sales coupang shop_id for each brand-day
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_date, brand_name
    ORDER BY payment_amount DESC, shop_id DESC
  ) = 1
),

coupang_ads_insight_daily_with_shop_mapping AS (
  SELECT
      ads.product_id
    , COALESCE(prd.shop_id, brd.shop_id, ads.shop_id) AS shop_id
    , ads.ad_cost
    , ads.order_date
  FROM coupang_ads_insight_daily AS ads
  LEFT JOIN {{ ref('core__product_master') }} AS itm
    ON ads.product_id = itm.product_id
  -- Step 4-3: map coupang ads cost to shop_id using product first, then brand fallback
  LEFT JOIN coupang_product_daily AS prd
    ON ads.order_date = prd.order_date AND ads.product_id = prd.product_id
  LEFT JOIN coupang_brand_daily AS brd
    ON ads.order_date = brd.order_date AND itm.brand_name = brd.brand_name
),

-- Step 6: prepare cost data

expense_daily AS (
  SELECT
      '200000' AS product_id
    , 'adop0005' AS shop_id
    , SUM(amount) AS extra_cost
    , ymd AS order_date
  FROM {{ source('core', 'expense') }}
  WHERE ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
  GROUP BY ymd
),

opex_daily AS (
  SELECT
      brand_id AS product_id
    , IF(dept_id = 1, 'adop0004', 'adop0003') AS shop_id
    , SUM(amount) AS extra_cost
    , ymd AS order_date
  FROM {{ ref('core__opex_daily') }}
  WHERE ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
  GROUP BY ymd, product_id, dept_id
),

-- Step 5: concat sales, ads, and cost data

insight_daily AS (
  SELECT
      product_id
    , shop_id
    , 8 AS order_status
    , NULL AS sku_quantity
    , NULL AS payment_amount
    , NULL AS supply_amount
    , NULL AS supply_cost
    , NULL AS delivery_fee
    , ad_cost
    , NULL AS extra_cost
    , order_date
  FROM (
    (SELECT * FROM searchad_insight_daily_with_shop_mapping)
    UNION ALL
    (SELECT * FROM coupang_ads_insight_daily_with_shop_mapping)
    UNION ALL
    (SELECT * FROM google_ads_insight_daily)
    UNION ALL
    (SELECT * FROM meta_ads_insight_daily)
    UNION ALL
    (SELECT * FROM extra_ads_insight_daily)
  ) AS t_
),

cost_daily AS (
  SELECT
      product_id
    , shop_id
    , 9 AS order_status
    , NULL AS sku_quantity
    , NULL AS payment_amount
    , NULL AS supply_amount
    , NULL AS supply_cost
    , NULL AS delivery_fee
    , NULL AS ad_cost
    , extra_cost
    , order_date
  FROM (
    (SELECT * FROM expense_daily)
    UNION ALL
    (SELECT * FROM opex_daily)
  ) AS t_
),

sales_daily AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , SUM(sku_quantity) AS sku_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , SUM(ad_cost) AS ad_cost
    , SUM(extra_cost) AS extra_cost
    , order_date
  FROM (
    (SELECT * FROM sabangnet_sales_daily)
    UNION ALL
    (SELECT * FROM smartstore_sales_daily)
    UNION ALL
    (SELECT * FROM coupang_rfm_sales_daily)
    UNION ALL
    (SELECT * FROM extra_sales_daily)
    UNION ALL
    (SELECT * FROM insight_daily)
    UNION ALL
    (SELECT * FROM cost_daily)
  )
  GROUP BY order_date, product_id, shop_id, order_status
)

SELECT * FROM sales_daily

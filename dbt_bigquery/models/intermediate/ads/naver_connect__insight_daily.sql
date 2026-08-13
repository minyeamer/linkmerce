{{
  config(
    materialized = 'incremental',
    schema = 'xfm_ads',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "ymd",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = true
  )
}}

WITH

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

-- Step 1: prepare relations

smt_prd_to_ranged_sbn_ids AS (
  SELECT
      product_id
    , bundle_product_ids
    , start_date
    , end_date
  FROM {{ ref('relation__smt_prd_to_ranged_sbn_ids') }}
),

smt_prd_to_ranged_prd_ids AS (
  SELECT
      product_id
    , bundle_product_ids
    , start_date
    , end_date
  FROM smt_prd_to_ranged_sbn_ids
  WHERE NOT STARTS_WITH(bundle_product_ids, '2')
),

smt_prd_to_ranged_brd_ids AS (
  SELECT
      product_id
    , bundle_product_ids AS bundle_brand_ids
    , start_date
    , end_date
  FROM smt_prd_to_ranged_sbn_ids
  WHERE STARTS_WITH(bundle_product_ids, '2')
),

-- Step 2: prepare naver connect sales

sales_daily AS (
  SELECT
      sales.space_id
    , COALESCE(product.mall_product_id, 0) AS mall_product_id
    , COALESCE(
          rel_prd.bundle_product_ids
        , rel_brd.bundle_brand_ids
        , space.brand_id
        , '200000'
      ) AS bundle_product_ids
    , sales.sales_count
    , sales.sales_amount
    , sales.commission_amount
    , sales.order_date AS ymd
  FROM {{ source('naver_connect', 'sales') }} AS sales
  -- Resolve mall_product_id using product_id
  LEFT JOIN {{ source('naver_connect', 'product') }} AS product
    ON sales.space_id = product.space_id AND sales.product_id = product.product_id
  -- Resolve bundle_product_ids using mall_product_id
  LEFT JOIN smt_prd_to_ranged_prd_ids AS rel_prd
    ON product.mall_product_id = rel_prd.product_id
    AND sales.order_date BETWEEN rel_prd.start_date AND rel_prd.end_date
  LEFT JOIN smt_prd_to_ranged_brd_ids AS rel_brd
    ON product.mall_product_id = rel_brd.product_id
    AND sales.order_date BETWEEN rel_brd.start_date AND rel_brd.end_date
  -- Resolve bundle_product_ids using space_id
  LEFT JOIN {{ source('naver_connect', 'space') }} AS space
    ON sales.space_id = space.space_id
  WHERE sales.order_date BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

-- Step 3: aggregate naver connect sales by dimensions

bundle_product_insight AS (
  SELECT
      space_id
    , mall_product_id
    , ANY_VALUE(bundle_product_ids) AS bundle_product_ids
    , SUM(commission_amount) AS ad_cost
    , SUM(sales_count) AS conv_count
    , SUM(sales_amount) AS conv_amount
    , ymd
  FROM sales_daily
  GROUP BY ymd, space_id, mall_product_id
),

-- Step 4: explode bundle products and allocate metrics with equal weight

exploded_product_insight AS (
  SELECT
      space_id
    , mall_product_id
    , bundle_product_id AS product_id
    , (DIV(ad_cost, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(ad_cost, bundle_product_count), 0)) AS ad_cost
    , (DIV(conv_count, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(conv_count, bundle_product_count), 0)) AS conv_count
    , (DIV(conv_amount, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(conv_amount, bundle_product_count), 0)) AS conv_amount
    , ymd
  FROM (
    SELECT
        sales.*
      , COALESCE(renewal.product_id_old, bundle_product_id) AS bundle_product_id
      , bundle_product_offset
      , ARRAY_LENGTH(SPLIT(sales.bundle_product_ids, ',')) AS bundle_product_count
    FROM bundle_product_insight AS sales
    CROSS JOIN UNNEST(SPLIT(sales.bundle_product_ids, ',')) AS bundle_product_id WITH OFFSET AS bundle_product_offset
    LEFT JOIN product_renewal_mapping AS renewal
      ON (bundle_product_id = renewal.product_id_new) AND (sales.ymd < renewal.renewal_date)
  ) AS t_
)

SELECT * FROM exploded_product_insight

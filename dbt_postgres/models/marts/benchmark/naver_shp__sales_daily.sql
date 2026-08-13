{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'DS_START_DATE', 'type': 'date'},
        {'name': 'DS_END_DATE', 'type': 'date'},
        {'name': 'MALL_HIGHLIGHT', 'type': 'text'}
      ]
    },
    schema = 'naver_shp',
    alias = 'benchmark_sales'
  )
}}

WITH{#

#} dayofweek_name_mapping AS (
  {{ core__dayofweek_name_mapping() }}
),{#

#} naver_sales AS (
  SELECT
      product_id
    , product_id AS option_id
    , NULL::integer AS sales_price
    , click_count
    , payment_count
    , payment_amount
    , payment_date
  FROM {{ source('ss_hcenter', 'sales') }}
  WHERE payment_date BETWEEN DS_START_DATE AND DS_END_DATE
),{#

#} stock_sales AS (
  SELECT
      product_id
    , option_id
    , sales_price
    , NULL::integer AS click_count
    , payment_count
    , payment_amount
    , payment_date
  FROM {{ ref('naver_shp__stock_sales') }}
  WHERE payment_date BETWEEN DS_START_DATE AND DS_END_DATE
),{#

#} sales_daily AS (
  SELECT
      fact.product_id
    , fact.option_id
    -- Mall attributes
    , opt.mall_seq
    , COALESCE(opt.mall_type, '-') AS mall_type
    , COALESCE(opt.mall_group, '-') AS mall_group
    , COALESCE(
          opt.mall_name || (CASE WHEN MALL_HIGHLIGHT = opt.mall_name THEN ' *' ELSE '' END)
        , '-'
      ) AS mall_name
    , opt.mall_url
    -- Category attributes
    , opt.category_id
    , COALESCE(opt.category_name, '-') AS category_name
    , COALESCE(opt.category_name1, '-') AS category_name1
    , COALESCE(opt.category_name2, '-') AS category_name2
    , COALESCE(opt.category_name3, '-') AS category_name3
    , COALESCE(opt.category_name4, '-') AS category_name4
    , COALESCE(opt.full_category_name, '-') AS full_category_name
    -- Group attributes
    , COALESCE(opt.group_id, 999) AS group_id
    , COALESCE(opt.group_name1, '기타') AS group_name1
    , COALESCE(opt.group_name2, '-') AS group_name2
    -- Product attributes
    , COALESCE(opt.product_name, '-') AS product_name
    , COALESCE(opt.option_name, '-') AS option_name
    , opt.product_url
    -- Sales attributes
    , COALESCE(fact.sales_price, opt.sales_price) AS sales_price
    , fact.click_count
    , fact.payment_count
    , fact.payment_amount
    , NULLIF(SUM(fact.payment_amount) OVER (
        PARTITION BY COALESCE(opt.group_id, opt.category_id3)
      ), 0) AS category_total
    , NULLIF(SUM(fact.payment_amount) OVER (
        PARTITION BY COALESCE(opt.group_id, opt.category_id3), fact.payment_date
      ), 0) AS daily_category_total
    -- Date attributes
    , fact.payment_date
    , to_char(fact.payment_date, 'DD일') || payment_day.name_ko AS day_name
    , payment_day.dayofweek::text || ' ' || payment_day.name_ko AS day_option
  FROM (
    (SELECT * FROM naver_sales)
    UNION ALL
    (SELECT * FROM stock_sales)
  ) AS fact
  LEFT JOIN {{ ref('naver_shp__option_master') }}(DS_START_DATE, DS_END_DATE) AS opt
    ON fact.product_id = opt.product_id
      AND fact.option_id = opt.option_id
  LEFT JOIN dayofweek_name_mapping AS payment_day
    ON (EXTRACT(DOW FROM fact.payment_date) + 1) = payment_day.dayofweek
){#

#} SELECT * FROM sales_daily

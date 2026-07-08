{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'DS_START_DATE', 'type': 'date'},
        {'name': 'DS_END_DATE', 'type': 'date'},
        {'name': 'DS_INTERVAL_MONTH', 'type': 'int64'}
      ]
    },
    schema = 'analytics',
    alias = 'stock_cost_mom'
  )
}}

WITH

dayofweek_name_mapping AS (
  {{ core__dayofweek_name_mapping() }}
),

-- Step 1: aggregate monthly stock cost from the latest stock batch in each month

ecount_product AS (
  SELECT
      product_code
    , SPLIT(option_id, '-')[SAFE_OFFSET(0)] AS product_id
    , org_price
  FROM {{ source('ecount', 'product') }} AS eco
  WHERE COALESCE(option_id, '') != ''
),

stock_cost_monthly AS (
  SELECT
      product_id
    , SUM(stock_cost) AS stock_cost
    , stock_ymd
    , DATE_TRUNC(stock_ymd, MONTH) AS order_ym
  FROM (
    SELECT
        COALESCE(prd.product_id, '200000') AS product_id
      , COALESCE(prd.org_price, 0) * COALESCE(qty.stock_qty, 0) AS stock_cost
      , qty.ymd AS stock_ymd
    FROM {{ ref('core__stock_qty_batch') }} AS qty
    LEFT JOIN ecount_product AS prd
      ON qty.product_code = prd.product_code
    WHERE qty.ymd BETWEEN DATE_TRUNC(DATE_SUB(DS_END_DATE, INTERVAL DS_INTERVAL_MONTH MONTH), MONTH)
      AND DS_END_DATE
    QUALIFY
      qty.ymd = MAX(qty.ymd) OVER (PARTITION BY DATE_TRUNC(qty.ymd, MONTH))
      AND qty.batch = MAX(qty.batch) OVER (PARTITION BY qty.ymd)
  ) AS t_
  GROUP BY stock_ymd, product_id
),

-- Step 2: add zero-cost monthly rows for sales periods without stock cost

sales_ds_range AS (
  SELECT DISTINCT
      product_id
    , DS_END_DATE AS stock_ymd
    , DATE_TRUNC(DS_END_DATE, MONTH) AS order_ym
  FROM {{ ref('core__sales_daily') }}
  WHERE order_date BETWEEN DS_START_DATE AND DS_END_DATE
),

sales_monthly_lookback AS (
  SELECT DISTINCT
      product_id
    , DATE_SUB(DATE_ADD(DATE_TRUNC(order_date, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY) AS stock_ymd
    , DATE_TRUNC(order_date, MONTH) AS order_ym
  FROM {{ ref('core__sales_daily') }}
  WHERE order_date BETWEEN DATE_TRUNC(DATE_SUB(DS_END_DATE, INTERVAL DS_INTERVAL_MONTH MONTH), MONTH)
    AND DATE_SUB(DATE_TRUNC(DS_END_DATE, MONTH), INTERVAL 1 DAY)
),

stock_cost_monthly_fallback AS (
  SELECT
      sales.product_id
    , 0 AS stock_cost
    , COALESCE(monthly.stock_ymd, sales.stock_ymd) AS stock_ymd
    , sales.order_ym
  FROM (
    (SELECT * FROM sales_ds_range)
    UNION DISTINCT
    (SELECT * FROM sales_monthly_lookback)
  ) AS sales
  LEFT JOIN stock_cost_monthly AS stock
    ON sales.product_id = stock.product_id
      AND sales.order_ym = stock.order_ym
  LEFT JOIN (SELECT DISTINCT order_ym, stock_ymd FROM stock_cost_monthly) AS monthly
    ON sales.order_ym = monthly.order_ym
  WHERE stock.product_id IS NULL
),

-- Step 3: enrich monthly stock cost rows with item attributes

stock_cost_mom AS (
  SELECT
      fact.product_id
    -- Item attributes
    , COALESCE(item.item_id, 'NA-AAAAAA-00') AS item_id
    , COALESCE(item.item_seq, 99999999) AS item_seq
    , COALESCE(item.team_name, '담당팀 없음') AS team_name
    , COALESCE(item.brand_name, '브랜드 없음') AS brand_name
    , COALESCE(item.category_name1, '-') AS category_name1
    , COALESCE(item.category_name2, '-') AS category_name2
    , COALESCE(item.category_name3, '-') AS category_name3
    , COALESCE(item.category_name4, '-') AS category_name4
    , COALESCE(item.color, '-') AS color
    , COALESCE(item.product_name, '매칭 불가 상품') AS product_name
    , COALESCE(
        IF(item.unit_name IS NULL
          , item.category_name3
          , CONCAT(item.category_name3, ' (', item.unit_name, ')'))
        , '-'
      ) AS category_unit_name
    -- Stock attributes
    , fact.stock_cost
    -- Date attributes
    , fact.stock_ymd
    , fact.order_ym
    , CONCAT(
        '[ '
        , FORMAT_DATE('%y/%m/%d', fact.stock_ymd)
        , end_day.name_ko
        , ' ]'
      ) AS stock_date_label
  FROM (
    (SELECT * FROM stock_cost_monthly)
    UNION ALL
    (SELECT * FROM stock_cost_monthly_fallback)
  ) AS fact
  LEFT JOIN {{ ref('core__product_master') }} AS item
    ON fact.product_id = item.product_id
  LEFT JOIN dayofweek_name_mapping AS end_day
    ON EXTRACT(DAYOFWEEK FROM fact.stock_ymd) = end_day.dayofweek
)

SELECT * FROM stock_cost_mom

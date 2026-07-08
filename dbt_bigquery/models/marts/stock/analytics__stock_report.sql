{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'REPORT_DATE', 'type': 'date'},
        {'name': 'REPORT_BATCH', 'type': 'int64'}
      ]
    },
    schema = 'analytics',
    alias = 'stock_report'
  )
}}

WITH

ecount_product AS (
  SELECT
      product_code
    , SPLIT(option_id, '-')[SAFE_OFFSET(0)] AS product_id
    , product_name
    , product_keyword
    , brand_name
    , remarks AS product_remarks
    , org_price
    , SAFE.PARSE_DATE('%Y%m%d', expiration_date) AS expiration_date
  FROM {{ source('ecount', 'product') }} AS eco
  WHERE COALESCE(option_id, '') != ''
),

core_product AS (
  SELECT
      product_id
    , item_seq
    , in_stock_yn
  FROM {{ source('core', 'item') }}
  WHERE product_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY item_seq ASC NULLS LAST) = 1
),

-- Step 1: prepare quantity data

stock_qty_batch AS (
  SELECT
      product_code
    , stock_qty
    , ecount__stock_qty
    , cj_eflexs__stock_qty
    , coupang_rfm__stock_qty
  FROM {{ ref('core__stock_qty_batch') }}
  WHERE ymd = REPORT_DATE
    AND batch = (
      CASE
        WHEN REPORT_BATCH IN (10, 20) THEN REPORT_BATCH
        ELSE (
          SELECT MAX(batch)
          FROM {{ ref('core__stock_qty_batch') }}
          WHERE ymd = REPORT_DATE
        )
      END
    )
),

sold_qty_daily_30d AS (
  SELECT
      product_id
    , sold_qty_30d
    , sabangnet__sold_qty_30d
    , cj_eflexs__sold_qty_30d
    , coupang_rfm__sold_qty_30d
  FROM {{ ref('core__sold_qty_30d_daily') }}
  WHERE ymd = REPORT_DATE
),

-- Step 2: attach stock quantity to ecount products

stock_report_with_stock_qty AS (
  SELECT
      product.product_code
    , product.product_id
    , product.expiration_date
    -- Stock metrics
    , COALESCE(qty.stock_qty, 0) AS stock_qty
    , COALESCE(qty.ecount__stock_qty, 0) AS ecount__stock_qty
    , COALESCE(qty.cj_eflexs__stock_qty, 0) AS cj_eflexs__stock_qty
    , COALESCE(qty.coupang_rfm__stock_qty, 0) AS coupang_rfm__stock_qty
    -- Join metrics
    , SUM(qty.stock_qty) OVER (PARTITION BY product.product_id) AS product__stock_qty
    , ROW_NUMBER() OVER (
        PARTITION BY product.product_id
        ORDER BY product.expiration_date ASC NULLS LAST, product.product_code ASC
      ) AS product_seq
  FROM ecount_product AS product
  LEFT JOIN stock_qty_batch AS qty
    ON product.product_code = qty.product_code
),

-- Step 3: attach schedule data to stock rows

stock_report_with_schedule AS (
  SELECT
      report.product_code
    , report.product_id
    , report.expiration_date
    -- Stock metrics
    , report.stock_qty
    , report.ecount__stock_qty
    , report.cj_eflexs__stock_qty
    , report.coupang_rfm__stock_qty
    -- Schedule attributes
    , schedule.order_date
    , schedule.delivery_date
    , COALESCE(schedule.remarks, '-') AS schedule_remarks
    -- Join metrics
    , ROW_NUMBER() OVER (
        PARTITION BY report.product_id
        ORDER BY report.expiration_date ASC NULLS LAST, report.product_code ASC
      ) AS product_seq
  FROM stock_report_with_stock_qty AS report
  LEFT JOIN {{ source('ecount', 'schedule') }} AS schedule
    ON report.product_code = schedule.product_code
  WHERE report.stock_qty > 0
    OR (
      schedule.remarks IN ('리뉴얼-예정', '제조원-변경 예정')
      AND report.product__stock_qty = 0
      AND report.product_seq = 1
    )
),

-- Step 4: attach sold quantity and average sold quantity

stock_report_with_sold_qty AS (
  SELECT
      report.product_code
    , report.product_id
    , report.expiration_date
    -- Stock metrics (total)
    , report.stock_qty
    , COALESCE(qty.sold_qty_30d, 0) AS sold_qty_30d
    , NULLIF(
        SUM(qty.sold_qty_30d) OVER (PARTITION BY report.product_id)
      , 0) / 30 AS avg_sold_qty_30d
    -- Stock metrics (partial)
    , report.ecount__stock_qty
    , COALESCE(qty.sabangnet__sold_qty_30d, 0) AS sabangnet__sold_qty_30d
    , NULLIF(
        SUM(qty.sabangnet__sold_qty_30d) OVER (PARTITION BY report.product_id)
      , 0) / 30 AS sabangnet__avg_sold_qty_30d
    , report.cj_eflexs__stock_qty
    , COALESCE(qty.cj_eflexs__sold_qty_30d, 0) AS cj_eflexs__sold_qty_30d
    , NULLIF(
        SUM(qty.cj_eflexs__sold_qty_30d) OVER (PARTITION BY report.product_id)
      , 0) / 30 AS cj_eflexs__avg_sold_qty_30d
    , report.coupang_rfm__stock_qty
    , COALESCE(qty.coupang_rfm__sold_qty_30d, 0) AS coupang_rfm__sold_qty_30d
    , NULLIF(
        SUM(qty.coupang_rfm__sold_qty_30d) OVER (PARTITION BY report.product_id)
      , 0) / 30 AS coupang_rfm__avg_sold_qty_30d
    -- Schedule attributes
    , report.order_date
    , report.delivery_date
    , report.schedule_remarks
    -- Join metrics
    , ROW_NUMBER() OVER (
        PARTITION BY report.product_id
        ORDER BY report.expiration_date ASC NULLS LAST, report.product_code ASC
      ) AS cumsum_seq
  FROM stock_report_with_schedule AS report
  LEFT JOIN sold_qty_daily_30d AS qty
    ON report.product_seq = 1 AND report.product_id = qty.product_id
),

-- Step 5: calculate cumulative stock and remaining days

stock_qty_cumsum AS (
  SELECT
      base.product_code
    , SUM(cumsum.stock_qty) AS stock_qty
    , SUM(cumsum.ecount__stock_qty) AS ecount__stock_qty
    , SUM(cumsum.cj_eflexs__stock_qty) AS cj_eflexs__stock_qty
    , SUM(cumsum.coupang_rfm__stock_qty) AS coupang_rfm__stock_qty
  FROM stock_report_with_sold_qty AS base
  INNER JOIN stock_report_with_sold_qty AS cumsum
    ON base.product_id = cumsum.product_id
      AND base.cumsum_seq >= cumsum.cumsum_seq
  GROUP BY base.product_code
),

stock_report_with_remain_days AS (
  SELECT
      report.product_code
    , report.product_id
    , report.expiration_date
    -- Stock metrics (total)
    , report.stock_qty
    , report.sold_qty_30d
    , COALESCE(
        SAFE_CAST(report.avg_sold_qty_30d AS INT64)
      , 0) AS avg_sold_qty_30d
    , COALESCE(
        SAFE_CAST(
          FLOOR(cumsum.stock_qty / report.avg_sold_qty_30d)
        AS INT64)
      , 0) AS remain_days
    -- Stock metrics (partial)
    , report.ecount__stock_qty
    , report.sabangnet__sold_qty_30d
    , COALESCE(
        SAFE_CAST(report.sabangnet__avg_sold_qty_30d AS INT64)
      , 0) AS sabangnet__avg_sold_qty_30d
    , COALESCE(
        SAFE_CAST(
          FLOOR(cumsum.ecount__stock_qty / report.sabangnet__avg_sold_qty_30d)
        AS INT64)
      , 0) AS ecount__remain_days
    , report.cj_eflexs__stock_qty
    , report.cj_eflexs__sold_qty_30d
    , COALESCE(
        SAFE_CAST(report.cj_eflexs__avg_sold_qty_30d AS INT64)
      , 0) AS cj_eflexs__avg_sold_qty_30d
    , COALESCE(
        SAFE_CAST(
          FLOOR(cumsum.cj_eflexs__stock_qty / report.cj_eflexs__avg_sold_qty_30d)
        AS INT64)
      , 0) AS cj_eflexs__remain_days
    , report.coupang_rfm__stock_qty
    , report.coupang_rfm__sold_qty_30d
    , COALESCE(
        SAFE_CAST(report.coupang_rfm__avg_sold_qty_30d AS INT64)
      , 0) AS coupang_rfm__avg_sold_qty_30d
    , COALESCE(
        SAFE_CAST(
          FLOOR(cumsum.coupang_rfm__stock_qty / report.coupang_rfm__avg_sold_qty_30d)
        AS INT64)
      , 0) AS coupang_rfm__remain_days
    -- Schedule attributes
    , report.order_date
    , report.delivery_date
    , report.schedule_remarks
  FROM stock_report_with_sold_qty AS report
  LEFT JOIN stock_qty_cumsum AS cumsum
    ON report.product_code = cumsum.product_code
),

-- Step 6: complete stock report

brand_order AS (
  SELECT
      brand_name
    , MIN(brand_seq) AS brand_seq
  FROM {{ source('smartstore', 'channel') }}
  GROUP BY brand_name
),

product_group_dates AS (
  SELECT
      product_id
    , STRING_AGG(expiration_date, '\n' ORDER BY expiration_date) AS expiration_dates
  FROM (
    SELECT DISTINCT
        product_id
      , FORMAT_DATE('%Y-%m-%d', expiration_date) AS expiration_date
    FROM stock_report_with_remain_days
    WHERE expiration_date IS NOT NULL
  ) AS t_
  GROUP BY product_id
),

stock_report_final AS (
  SELECT
      ROW_NUMBER() OVER (
        ORDER BY
            brand.brand_seq ASC NULLS LAST
          , item.item_seq ASC NULLS LAST
          , report.expiration_date ASC NULLS LAST
          , report.product_code ASC
      ) AS lot_seq
    -- Product dimensions
    , product.brand_name
    , report.product_id
    , product.product_remarks
    , report.product_code
    , product.product_name
    , product.product_keyword
    , report.expiration_date
    , dates.expiration_dates
    -- Stock metrics (total)
    , report.stock_qty
    , report.sold_qty_30d
    , report.avg_sold_qty_30d
    , (CASE
        WHEN CONTAINS_SUBSTR(product.product_keyword, '1포') THEN NULL
        ELSE report.remain_days
      END) AS remain_days
    -- Stock metrics (partial)
    , report.ecount__stock_qty
    , report.sabangnet__sold_qty_30d
    , report.sabangnet__avg_sold_qty_30d
    , report.ecount__remain_days
    , report.cj_eflexs__stock_qty
    , report.cj_eflexs__sold_qty_30d
    , report.cj_eflexs__avg_sold_qty_30d
    , report.cj_eflexs__remain_days
    , report.coupang_rfm__stock_qty
    , report.coupang_rfm__sold_qty_30d
    , report.coupang_rfm__avg_sold_qty_30d
    , report.coupang_rfm__remain_days
    -- Expected date
    , report.expected_date
    , (CASE
        WHEN CONTAINS_SUBSTR(product.product_name, '1포')
          THEN '제외 상품 - 1포'
        WHEN CONTAINS_SUBSTR(product.product_name, '불량')
          THEN '제외 상품 - 불량'
        WHEN CONTAINS_SUBSTR(product.product_name, '비가용')
          THEN '제외 상품 - 비가용'
        WHEN report.expiration_date IS NULL
          THEN '소비기한 없음'
        WHEN REPORT_DATE > report.expiration_date
          THEN '소비기한 초과'
        WHEN SUM(report.sold_qty_30d) OVER (PARTITION BY report.product_id) = 0
          THEN '판매량 없음'
        WHEN report.expected_date > report.expiration_date
          THEN '소비기한 초과'
        WHEN DATE_ADD(report.expected_date, INTERVAL 6 MONTH) > report.expiration_date
          THEN '판매부진'
        ELSE '정상'
      END) AS performance
    -- Product metrics
    , product.org_price
    , product.org_price * report.stock_qty AS stock_cost
    , item.in_stock_yn
    -- Schedule attributes
    , report.order_date
    , report.delivery_date
    , report.schedule_remarks
  FROM (
    SELECT *, DATE_ADD(REPORT_DATE, INTERVAL remain_days DAY) AS expected_date
    FROM stock_report_with_remain_days
  ) AS report
  INNER JOIN ecount_product AS product
    ON report.product_code = product.product_code
  LEFT JOIN core_product AS item
    ON product.product_id = item.product_id
  LEFT JOIN brand_order AS brand
    ON product.brand_name = brand.brand_name
  LEFT JOIN product_group_dates AS dates
    ON report.product_id = dates.product_id
)

SELECT
    MIN(lot_seq) OVER (PARTITION BY product_id) AS option_seq
  , *
FROM stock_report_final
ORDER BY lot_seq

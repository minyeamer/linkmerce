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
    alias = 'stock_time_ds'
  )
}}

WITH primary_stock_time AS (
  SELECT
      ymd AS report_date
    , batch AS report_batch
    , DATE_SUB(ymd, INTERVAL 30 DAY) AS order_start_date
    , DATE_SUB(ymd, INTERVAL 1 DAY) AS order_end_date
    , max_updated_at
    , ecount__max_updated_at
    , cj_eflexs__max_updated_at
    , coupang_rfm__max_updated_at
  FROM {{ ref('core__stock_time_batch') }}
  WHERE ymd = (
      CASE
        WHEN REPORT_BATCH IN (10, 20) THEN REPORT_DATE
        ELSE CURRENT_DATE('Asia/Seoul')
      END
    )
    AND batch = (
      CASE
        WHEN REPORT_BATCH IN (10, 20) THEN REPORT_BATCH
        ELSE (
          SELECT MAX(batch)
          FROM {{ ref('core__stock_time_batch') }}
          WHERE ymd = CURRENT_DATE('Asia/Seoul')
        )
      END
    )
),

fallback_stock_time AS (
  SELECT
      ymd AS report_date
    , batch AS report_batch
    , DATE_SUB(ymd, INTERVAL 30 DAY) AS order_start_date
    , DATE_SUB(ymd, INTERVAL 1 DAY) AS order_end_date
    , max_updated_at
    , ecount__max_updated_at
    , cj_eflexs__max_updated_at
    , coupang_rfm__max_updated_at
  FROM {{ ref('core__stock_time_batch') }}
  WHERE ymd = (
      CASE
        WHEN REPORT_BATCH IN (10, 20)
          -- Disable fallback by returning no rows
          THEN DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
        -- Fallback to the previous day if no rows are found for today.
        ELSE DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
      END
    )
    AND batch = (
      CASE
        WHEN REPORT_BATCH IN (10, 20) THEN REPORT_BATCH
        ELSE (
          SELECT MAX(batch)
          FROM {{ ref('core__stock_time_batch') }}
          WHERE ymd = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
        )
      END
    )
    AND REPORT_BATCH NOT IN (10, 20)
)

SELECT * FROM primary_stock_time

UNION ALL

SELECT *
FROM fallback_stock_time
WHERE NOT EXISTS (SELECT 1 FROM primary_stock_time)

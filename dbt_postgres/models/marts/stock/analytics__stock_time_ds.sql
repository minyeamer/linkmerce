{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'REPORT_DATE', 'type': 'date'},
        {'name': 'REPORT_BATCH', 'type': 'integer'}
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
    , ymd - 30 AS order_start_date
    , ymd - 1 AS order_end_date
    , max_updated_at
    , ecount__max_updated_at
    , cj_eflexs__max_updated_at
    , coupang_rfm__max_updated_at
  FROM {{ ref('core__stock_time_batch') }}
  WHERE ymd = (
      CASE
        WHEN REPORT_BATCH IN (10, 20) THEN REPORT_DATE
        ELSE (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date
      END
    )
    AND batch = (
      CASE
        WHEN REPORT_BATCH IN (10, 20) THEN REPORT_BATCH
        ELSE (
          SELECT MAX(batch)
          FROM {{ ref('core__stock_time_batch') }}
          WHERE ymd = (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date
        )
      END
    )
),{#

#} fallback_stock_time AS (
  SELECT
      ymd AS report_date
    , batch AS report_batch
    , (ymd - 30)::date AS order_start_date
    , (ymd - 1)::date AS order_end_date
    , max_updated_at
    , ecount__max_updated_at
    , cj_eflexs__max_updated_at
    , coupang_rfm__max_updated_at
  FROM {{ ref('core__stock_time_batch') }}
  WHERE ymd = (
      CASE
        WHEN REPORT_BATCH IN (10, 20)
          -- Disable fallback by returning no rows
          THEN (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date + 1
        -- Fallback to the previous day if no rows are found for today.
        ELSE (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date - 1
      END
    )
    AND batch = (
      CASE
        WHEN REPORT_BATCH IN (10, 20) THEN REPORT_BATCH
        ELSE (
          SELECT MAX(batch)
          FROM {{ ref('core__stock_time_batch') }}
          WHERE ymd = (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date - 1
        )
      END
    )
    AND REPORT_BATCH NOT IN (10, 20)
){#

#} SELECT * FROM primary_stock_time{#

#} UNION ALL{#

#} SELECT *
FROM fallback_stock_time
WHERE NOT EXISTS (SELECT 1 FROM primary_stock_time)

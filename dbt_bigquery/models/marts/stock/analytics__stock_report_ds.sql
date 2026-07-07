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
    alias = 'stock_report_ds'
  )
}}

WITH primary_report AS (
  SELECT *
  FROM {{ ref('analytics__stock_report') }}(
    REPORT_DATE =>
      CASE
        WHEN REPORT_BATCH IN (10, 20) THEN REPORT_DATE
        ELSE CURRENT_DATE('Asia/Seoul')
      END,
    REPORT_BATCH => REPORT_BATCH
  )
),

fallback_report AS (
  SELECT *
  FROM {{ ref('analytics__stock_report') }}(
    REPORT_DATE =>
      CASE
        WHEN REPORT_BATCH IN (10, 20)
          -- Disable fallback by returning no rows
          THEN DATE_ADD(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
        -- Fallback to the previous day if no rows are found for today.
        ELSE DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
      END,
    REPORT_BATCH => REPORT_BATCH
  )
)

SELECT * FROM primary_report

UNION ALL

SELECT *
FROM fallback_report
WHERE NOT EXISTS (SELECT 1 FROM primary_report)

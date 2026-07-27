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
    alias = 'stock_report_ds'
  )
}}

WITH primary_report AS (
  SELECT *
  FROM {{ ref('analytics__stock_report') }}(
    p_report_date =>
      CASE
        WHEN REPORT_BATCH IN (10, 20) THEN REPORT_DATE
        ELSE (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date
      END,
    p_report_batch => REPORT_BATCH
  )
),{#

#} fallback_report AS (
  SELECT *
  FROM {{ ref('analytics__stock_report') }}(
    p_report_date =>
      CASE
        WHEN REPORT_BATCH IN (10, 20)
          -- Disable fallback by returning no rows
          THEN (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date + 1
        -- Fallback to the previous day if no rows are found for today.
        ELSE (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date - 1
      END,
    p_report_batch => REPORT_BATCH
  )
){#

#} SELECT * FROM primary_report{#

#} UNION ALL{#

#} SELECT *
FROM fallback_report
WHERE NOT EXISTS (SELECT 1 FROM primary_report)

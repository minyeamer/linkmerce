{{
  config(
    materialized = 'table',
    schema = 'xfm_stock'
  )
}}

SELECT
    DATE(MAX(updated_at)) AS report_date
  , IF(TIME(MAX(updated_at)) < TIME '17:00:00', 10, 20) AS report_batch
  , DATE_SUB(DATE(MAX(updated_at)), INTERVAL 30 DAY) AS order_start_date
  , DATE_SUB(DATE(MAX(updated_at)), INTERVAL 1 DAY) AS order_end_date
  , MAX(updated_at) AS last_updated_at
  , MAX(IF(group_id = 0, updated_at, NULL)) AS ecount__last_updated_at
  , MAX(IF(group_id = 1, updated_at, NULL)) AS cj_eflexs__last_updated_at
  , MAX(IF(group_id = 2, updated_at, NULL)) AS coupang_rfm__last_updated_at
FROM (
  (
    SELECT 0 AS group_id, updated_at
    FROM `ecount.inventory`
    WHERE updated_at >= DATETIME('{{ var("ds_start_date") }}')
      AND updated_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
  )
  UNION ALL
  (
    SELECT 1 AS group_id, updated_at
    FROM `cj_eflexs.stock`
    WHERE updated_at >= DATETIME('{{ var("ds_start_date") }}')
      AND updated_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
  )
  UNION ALL
  (
    SELECT 2 AS group_id, updated_at
    FROM `coupang_rfm.inventory`
    WHERE updated_at >= DATETIME('{{ var("ds_start_date") }}')
      AND updated_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
  )
)

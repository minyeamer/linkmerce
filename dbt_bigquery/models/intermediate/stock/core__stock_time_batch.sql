{{
  config(
    materialized = 'incremental',
    schema = 'xfm_stock',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "ymd",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = bq_date_partitions('ds_start_date', 'ds_end_date'),
    require_partition_filter = false
  )
}}

WITH

-- Step 1: prepare batch stock update time

ecount_updated_at AS (
  SELECT
      DATE(updated_at) AS ymd
    , IF(TIME(updated_at) < TIME '17:00:00', 10, 20) AS batch
    , 0 AS group_id
    , updated_at
  FROM {{ source('ecount', 'inventory') }}
  WHERE updated_at >= DATETIME('{{ var("ds_start_date") }}')
    AND updated_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
),

cj_eflexs_updated_at AS (
  SELECT
      DATE(updated_at) AS ymd
    , IF(TIME(updated_at) < TIME '17:00:00', 10, 20) AS batch
    , 1 AS group_id
    , updated_at
  FROM {{ source('cj_eflexs', 'stock') }}
  WHERE updated_at >= DATETIME('{{ var("ds_start_date") }}')
    AND updated_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
),

coupang_rfm_updated_at AS (
  SELECT
      DATE(updated_at) AS ymd
    , IF(TIME(updated_at) < TIME '17:00:00', 10, 20) AS batch
    , 2 AS group_id
    , updated_at
  FROM {{ source('coupang_rfm', 'inventory') }}
  WHERE updated_at >= DATETIME('{{ var("ds_start_date") }}')
    AND updated_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
),

-- Step 2: aggregate stock update time by channel

stock_time_batch AS (
  SELECT
      ymd
    , batch
    , MAX(updated_at) AS max_updated_at
    , MAX(IF(group_id = 0, updated_at, NULL)) AS ecount__max_updated_at
    , MAX(IF(group_id = 1, updated_at, NULL)) AS cj_eflexs__max_updated_at
    , MAX(IF(group_id = 2, updated_at, NULL)) AS coupang_rfm__max_updated_at
  FROM (
    (SELECT * FROM ecount_updated_at)
    UNION ALL
    (SELECT * FROM cj_eflexs_updated_at)
    UNION ALL
    (SELECT * FROM coupang_rfm_updated_at)
  ) AS t_
  GROUP BY ymd, batch
)

SELECT * FROM stock_time_batch

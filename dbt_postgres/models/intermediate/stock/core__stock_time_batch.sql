{{
  config(
    materialized = 'partitioned_table',
    schema = 'xfm_stock',
    partition_by = {
      "field": "ymd",
      "data_type": "date",
      "granularity": "day"
    },
    partitions = pg_date_partitions('ds_start_date', 'ds_end_date')
  )
}}

WITH{#

-- Step 1: prepare batch stock update time

#} ecount_updated_at AS (
  SELECT
      (updated_at)::date AS ymd
    , (CASE WHEN (updated_at)::time < TIME '17:00:00' THEN 10 ELSE 20 END) AS batch
    , 0 AS group_id
    , updated_at
  FROM {{ source('ecount', 'inventory') }}
  WHERE updated_at >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND updated_at < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
),{#

#} cj_eflexs_updated_at AS (
  SELECT
      (updated_at)::date AS ymd
    , (CASE WHEN (updated_at)::time < TIME '17:00:00' THEN 10 ELSE 20 END) AS batch
    , 1 AS group_id
    , updated_at
  FROM {{ source('cj_eflexs', 'stock') }}
  WHERE updated_at >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND updated_at < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
),{#

#} coupang_rfm_updated_at AS (
  SELECT
      (updated_at)::date AS ymd
    , (CASE WHEN (updated_at)::time < TIME '17:00:00' THEN 10 ELSE 20 END) AS batch
    , 2 AS group_id
    , updated_at
  FROM {{ source('coupang_rfm', 'inventory') }}
  WHERE updated_at >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND updated_at < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
),{#

-- Step 2: aggregate stock update time by channel

#} stock_time_batch AS (
  SELECT
      ymd
    , batch
    , MAX(updated_at) AS max_updated_at
    , MAX((CASE WHEN group_id = 0 THEN updated_at ELSE NULL END)) AS ecount__max_updated_at
    , MAX((CASE WHEN group_id = 1 THEN updated_at ELSE NULL END)) AS cj_eflexs__max_updated_at
    , MAX((CASE WHEN group_id = 2 THEN updated_at ELSE NULL END)) AS coupang_rfm__max_updated_at
  FROM (
    (SELECT * FROM ecount_updated_at)
    UNION ALL
    (SELECT * FROM cj_eflexs_updated_at)
    UNION ALL
    (SELECT * FROM coupang_rfm_updated_at)
  ) AS t_
  GROUP BY ymd, batch
){#

#} SELECT * FROM stock_time_batch

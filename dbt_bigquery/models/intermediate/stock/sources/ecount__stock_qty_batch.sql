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
    require_partition_filter = true
  )
}}

SELECT
    ymd
  , batch
  , product_code
  , SUM(quantity) AS stock_quantity
FROM (
  SELECT
      DATE(updated_at) AS ymd
    , (CASE
        WHEN TIME(updated_at) < TIME '17:00:00' THEN 10
        WHEN TIME(updated_at) < TIME '20:00:00' THEN 20
        ELSE 30
      END) AS batch
    , product_code
    , quantity
  FROM {{ source('ecount', 'inventory') }}
  WHERE updated_at >= DATETIME('{{ var("ds_start_date") }}')
    AND updated_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
) AS t_
GROUP BY ymd, batch, product_code

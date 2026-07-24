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

SELECT
    ymd
  , batch
  , product_code
  , SUM(quantity) AS stock_quantity
FROM (
  SELECT
      updated_at::date AS ymd
    , (CASE WHEN updated_at::time < TIME '17:00:00' THEN 10 ELSE 20 END) AS batch
    , product_code
    , quantity
  FROM {{ source('ecount', 'inventory') }}
  WHERE updated_at >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND updated_at < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
) AS t_
GROUP BY ymd, batch, product_code

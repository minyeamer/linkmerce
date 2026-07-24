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

#} item_mapping AS (
  SELECT DISTINCT
      TRIM(item_code) AS item_code
    , product_id
  FROM {{ source('core', 'item') }}
  CROSS JOIN LATERAL unnest(string_to_array(COALESCE(eflexs_item_code, ''), ',')) AS t(item_code)
  WHERE product_id IS NOT NULL
    AND TRIM(item_code) != ''
){#

#} SELECT
    ymd
  , batch
  , product_id
  , validate_date AS expiration_date
  , SUM(usable_quantity) AS stock_quantity
FROM (
  SELECT
      (stock.updated_at)::date AS ymd
    , (CASE WHEN stock.updated_at::time < TIME '17:00:00' THEN 10 ELSE 20 END) AS batch
    , COALESCE(item.product_id, '200000') AS product_id
    , COALESCE(stock.validate_date, DATE '2999-12-31') AS validate_date
    , stock.usable_quantity
  FROM {{ source('cj_eflexs', 'stock') }} AS stock
  LEFT JOIN item_mapping AS item
    ON stock.item_code = item.item_code
  WHERE stock.updated_at >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND stock.updated_at < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
) AS t_
GROUP BY ymd, batch, product_id, validate_date

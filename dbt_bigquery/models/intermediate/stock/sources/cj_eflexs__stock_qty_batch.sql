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

WITH

item_mapping AS (
  SELECT DISTINCT
      TRIM(item_code) AS item_code
    , product_id
  FROM {{ source('core', 'item') }}
  CROSS JOIN UNNEST(SPLIT(COALESCE(eflexs_item_code, ''), ',')) AS item_code
  WHERE product_id IS NOT NULL
    AND TRIM(item_code) != ''
)

SELECT
    ymd
  , batch
  , product_id
  , validate_date AS expiration_date
  , SUM(usable_quantity) AS stock_quantity
FROM (
  SELECT
      DATE(stock.updated_at) AS ymd
    , IF(TIME(stock.updated_at) < TIME '17:00:00', 10, 20) AS batch
    , COALESCE(item.product_id, '200000') AS product_id
    , COALESCE(stock.validate_date, DATE(2999, 12, 31)) AS validate_date
    , stock.usable_quantity
  FROM {{ source('cj_eflexs', 'stock') }} AS stock
  LEFT JOIN item_mapping AS item
    ON stock.item_code = item.item_code
  WHERE stock.updated_at >= DATETIME('{{ var("ds_start_date") }}')
    AND stock.updated_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
) AS t_
GROUP BY ymd, batch, product_id, validate_date

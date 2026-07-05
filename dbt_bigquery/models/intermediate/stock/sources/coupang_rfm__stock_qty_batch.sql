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
  , product_id
  , expiration_date
  , SUM(stock_quantity) AS stock_quantity
FROM (
  SELECT
      DATE(inv.updated_at) AS ymd
    , IF(TIME(inv.updated_at) < TIME '17:00:00', 10, 20) AS batch
    , COALESCE(SPLIT(product, ':')[SAFE_OFFSET(0)], '200000') AS product_id
    , COALESCE(exp.expiration_date, DATE(2999, 12, 31)) AS expiration_date
    , inv.stock_quantity * COALESCE(SAFE_CAST(SPLIT(product, ':')[SAFE_OFFSET(1)] AS INT64), 1) AS stock_quantity
  FROM {{ source('coupang_rfm', 'inventory') }} AS inv
  LEFT JOIN {{ source('coupang_rfm', 'inventory_exp') }} AS exp
    ON inv.option_id = exp.option_id
      AND inv.updated_at BETWEEN exp.start_time AND exp.end_time
  LEFT JOIN {{ source('relation', 'cpg_opt_to_sbn_ids') }} AS rel
    ON exp.option_id = rel.option_id
  LEFT JOIN UNNEST(SPLIT(rel.bundle_product_ids, ',')) AS product
  WHERE inv.updated_at >= DATETIME('{{ var("ds_start_date") }}')
    AND inv.updated_at < DATETIME(DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 1 DAY))
) AS t_
GROUP BY ymd, batch, product_id, expiration_date

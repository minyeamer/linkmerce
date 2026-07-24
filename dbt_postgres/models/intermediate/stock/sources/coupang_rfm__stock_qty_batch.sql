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
  , product_id
  , expiration_date
  , SUM(stock_quantity) AS stock_quantity
FROM (
  SELECT
      inv.updated_at::date AS ymd
    , (CASE WHEN inv.updated_at::time < TIME '17:00:00' THEN 10 ELSE 20 END) AS batch
    , COALESCE((string_to_array(product, ':'))[1], '200000') AS product_id
    , COALESCE(exp.expiration_date, DATE '2999-12-31') AS expiration_date
      , (CASE
          WHEN split_part(product, ':', 2) ~ '^[0-9]+$'
            THEN split_part(product, ':', 2)::integer
          ELSE 1
        END) * inv.stock_quantity AS stock_quantity
  FROM {{ source('coupang_rfm', 'inventory') }} AS inv
  LEFT JOIN {{ source('coupang_rfm', 'inventory_exp') }} AS exp
    ON inv.option_id = exp.option_id
      AND inv.updated_at BETWEEN exp.start_time AND exp.end_time
  LEFT JOIN {{ source('relation', 'cpg_opt_to_sbn_ids') }} AS rel
    ON exp.option_id = rel.option_id
  LEFT JOIN LATERAL unnest(string_to_array(rel.bundle_product_ids, ',')) AS t(product) ON TRUE
  WHERE inv.updated_at >= {{ pg_batch_start_date() }}::timestamp without time zone
    AND inv.updated_at < ({{ pg_batch_end_date() }} + 1)::timestamp without time zone
) AS t_
GROUP BY ymd, batch, product_id, expiration_date

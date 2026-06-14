{{
  config(
    materialized = 'incremental',
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

ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '네이버'
),

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

contract_base AS (
  SELECT
      ctr.contract_id
    , ctr.adgroup_id
    , ctr.customer_id
    , (ctr.contract_amount - COALESCE(ctr.refund_amount, 0)) AS ad_cost
    , ctr.exposure_start_date
    , ctr.exposure_end_date
    , DATE_DIFF(ctr.exposure_end_date, ctr.exposure_start_date, DAY) + 1 AS date_count
  FROM {{ source('searchad', 'contract') }} AS ctr
  WHERE ctr.contract_end_date >= DATE('{{ var("ds_start_date") }}')
    AND ctr.exposure_start_date IS NOT NULL
    AND ctr.exposure_end_date IS NOT NULL
),

contract_expand AS (
  SELECT
      ctr.contract_id
    , ctr.adgroup_id
    , ctr.customer_id
    , SAFE_CAST(ctr.ad_cost / NULLIF(ctr.date_count, 0) AS INT64) AS ad_cost
    , DATE_ADD(ctr.exposure_start_date, INTERVAL date_offset DAY) AS ymd
  FROM contract_base AS ctr
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, ctr.date_count - 1)) AS date_offset
  WHERE DATE_ADD(ctr.exposure_start_date, INTERVAL date_offset DAY)
    BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

contract_daily AS (
  SELECT
      ctr.contract_id
    , ctr.adgroup_id
    , COALESCE(
          rel_grp.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , acc.bundle_brand_ids
        , '200000'
      ) AS bundle_product_ids
    , ctr.ad_cost
    , ctr.ymd
  FROM contract_expand AS ctr
  LEFT JOIN {{ source('searchad', 'adgroup') }} AS grp
    ON ctr.adgroup_id = grp.adgroup_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_grp
    ON ctr.adgroup_id = rel_grp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON grp.campaign_id = rel_cmp.ad_id
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON ctr.customer_id = acc.customer_id
),

exploded_product_contract AS (
  SELECT
      contract_id
    , adgroup_id
    , bundle_product_id AS product_id
    , (DIV(ad_cost, bundle_product_count)
      + IF(bundle_product_offset = 0, MOD(ad_cost, bundle_product_count), 0)) AS ad_cost
    , ymd
  FROM (
    SELECT
        ctr.*
      , COALESCE(renewal.product_id_old, bundle_product_id) AS bundle_product_id
      , bundle_product_offset
      , ARRAY_LENGTH(SPLIT(ctr.bundle_product_ids, ',')) AS bundle_product_count
    FROM contract_daily AS ctr
    CROSS JOIN UNNEST(SPLIT(ctr.bundle_product_ids, ',')) AS bundle_product_id WITH OFFSET AS bundle_product_offset
    LEFT JOIN product_renewal_mapping AS renewal
      ON (bundle_product_id = renewal.product_id_new) AND (ctr.ymd < renewal.renewal_date)
  ) AS t_
)

SELECT * FROM exploded_product_contract

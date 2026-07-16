{{
  config(
    materialized = 'table',
    schema = 'xfm_ads',
    cluster_by = ["dept_id", "brand_id"],
    partition_by = {
      "field": "ymd",
      "data_type": "date",
      "granularity": "day"
    },
    require_partition_filter = false
  )
}}

WITH

opex_source AS (
  SELECT
      expense_id
    , expense_name
    , dept_id
    , brand_id
    , amount
    , COALESCE(start_date, end_date) AS start_date
    , end_date
    , DATE_DIFF(end_date, COALESCE(start_date, end_date), DAY) + 1 AS date_count
  FROM {{ source('core', 'opex') }}
),

opex_daily AS (
  SELECT
      expense_id
    , expense_name
    , dept_id
    , brand_id
    , (DIV(amount, date_count)
      + IF(date_offset = 0, MOD(amount, date_count), 0)) AS amount
    , DATE_ADD(start_date, INTERVAL date_offset DAY) AS ymd
  FROM opex_source,
    UNNEST(GENERATE_ARRAY(0, date_count - 1)) AS date_offset
)

SELECT * FROM opex_daily

{{
  config(
    materialized = 'partitioned_table',
    schema = 'xfm_ads',
    partition_by = {
      "field": "ymd",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

WITH{#

#} opex_source AS (
  SELECT
      expense_id
    , expense_name
    , dept_id
    , brand_id
    , amount
    , COALESCE(start_date, end_date) AS start_date
    , end_date
    , (end_date - COALESCE(start_date, end_date)) + 1 AS date_count
  FROM {{ source('core', 'opex') }}
),{#

#} opex_daily AS (
  SELECT
      expense_id
    , expense_name
    , dept_id
    , brand_id
    , (DIV(amount, date_count)
      + (CASE WHEN date_offset = 0 THEN MOD(amount, date_count) ELSE 0 END)) AS amount
    , start_date + date_offset AS ymd
  FROM opex_source
  CROSS JOIN LATERAL generate_series(0, date_count - 1) AS t(date_offset)
){#

#} SELECT * FROM opex_daily

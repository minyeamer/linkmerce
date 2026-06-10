{{
  config(
    materialized = 'view',
    schema = 'transformed'
  )
}}

WITH opex_source AS (
  SELECT
      expense_id
    , expense_name
    , dept_name
    , team_name
    , brand_name
    , amount
    , COALESCE(start_date, end_date) AS start_date
    , end_date
    , DATE_DIFF(end_date, COALESCE(start_date, end_date), DAY) + 1 AS date_count
  FROM {{ source('core', 'opex') }}
  WHERE end_date >= DATE('{{ var("ds_start_date") }}')
),

opex_extended AS (
  SELECT *
  FROM (
    SELECT
        expense_id
      , expense_name
      , dept_name
      , team_name
      , brand_name
      , IF(date_count > 1, SAFE_CAST(amount / date_count AS INT64), amount) AS amount
      , DATE_ADD(start_date, INTERVAL date_offset DAY) AS ymd
    FROM opex_source,
      UNNEST(GENERATE_ARRAY(0, date_count - 1)) AS date_offset
  )
  WHERE ymd BETWEEN DATE('{{ var("ds_start_date") }}') AND DATE('{{ var("ds_end_date") }}')
),

brand_item AS (
  SELECT
      brand_name
    , item_id
    , item_seq
    , product_id
    , team_name
  FROM {{ source('core', 'item') }}
  WHERE category_name1 IS NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY brand_name) = 1
),

opex_daily AS (
  SELECT
      ext.expense_id
    , COALESCE(brd.item_id, 'NA-AAAAAA-00') AS item_id
    , COALESCE(brd.item_seq, 99999999) AS item_seq
    , COALESCE(brd.product_id, '900000') AS product_id
    , ext.expense_name
    , COALESCE(ext.dept_name, '부서 없음') AS dept_name
    , COALESCE(ext.team_name, brd.team_name, '담당팀 없음') AS team_name
    , COALESCE(ext.brand_name, '브랜드 없음') AS brand_name
    , ext.amount
    , ext.ymd
  FROM opex_extended AS ext
  LEFT JOIN brand_item AS brd
    ON ext.brand_name = brd.brand_name
  )

SELECT * FROM opex_daily

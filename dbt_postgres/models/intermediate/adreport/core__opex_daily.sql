WITH{{var("line_break")

}} opex_source AS (
  SELECT
      expense_id
    , expense_name
    , dept_name
    , team_name
    , brand_name
    , amount
    , COALESCE(start_date, end_date) AS start_date
    , end_date
    , (end_date - COALESCE(start_date, end_date)) + 1 AS date_count
  FROM {{ source('core', 'opex') }}
  WHERE end_date >= DATE '{{ var("ds_start_date") }}'
),{{var("line_break")

}} opex_extended AS (
  SELECT *
  FROM (
    SELECT
        expense_id
      , expense_name
      , dept_name
      , team_name
      , brand_name
      , (CASE
          WHEN date_count > 1
            THEN (amount::NUMERIC / date_count)::BIGINT
          ELSE amount
        END) AS amount
      , start_date + (INTERVAL '1 DAY' * date_offset) AS ymd
    FROM opex_source
    CROSS JOIN LATERAL generate_series(0, date_count - 1) AS _t0(date_offset)
  ) AS _t
  WHERE ymd BETWEEN DATE '{{ var("ds_start_date") }}' AND DATE '{{ var("ds_end_date") }}'
),{{var("line_break")

}} brand_item AS (
  SELECT *
  FROM (
    SELECT
        brand_name
      , item_id
      , item_seq
      , product_id
      , team_name
      , ROW_NUMBER() OVER (PARTITION BY brand_name) AS rn
    FROM {{ source('core', 'item') }}
    WHERE category_name1 IS NULL
  ) AS _t
  WHERE rn = 1
),{{var("line_break")

}} opex_daily AS (
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

{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'DS_START_DATE', 'type': 'date'},
        {'name': 'DS_END_DATE', 'type': 'date'},
        {'name': 'DS_INTERVAL_MONTH', 'type': 'integer'}
      ]
    },
    schema = 'analytics',
    alias = 'order_count_mom'
  )
}}

WITH{#

#} dayofweek_name_mapping AS (
  {{ core__dayofweek_name_mapping() }}
),{#

-- Step 1: load order rows within the requested DS_START_DATE to DS_END_DATE range

#} order_count_ds_range AS (
  SELECT
      order_id
    , product_order_id
    , product_id
    , item_id
    , item_seq
    , team_name
    , brand_name
    , category_name1
    , category_name2
    , category_name3
    , category_name4
    , color
    , product_name
    , category_unit_name
    , shop_id
    , shop_group
    , shop_name
    , order_status
    , order_quantity
    , MIN(order_date) OVER (PARTITION BY date_trunc('month', DS_END_DATE)::date) AS order_start_date
    , MAX(order_date) OVER (PARTITION BY date_trunc('month', DS_END_DATE)::date) AS order_end_date
    , date_trunc('month', DS_END_DATE)::date AS order_ym
  FROM {{ ref('analytics__order_count') }}(DS_START_DATE, DS_END_DATE)
),{#

-- Step 2: load order rows for dates before the DS_END_DATE month

#} order_count_monthly_lookback AS (
  SELECT
      order_id
    , product_order_id
    , product_id
    , item_id
    , item_seq
    , team_name
    , brand_name
    , category_name1
    , category_name2
    , category_name3
    , category_name4
    , color
    , product_name
    , category_unit_name
    , shop_id
    , shop_group
    , shop_name
    , order_status
    , order_quantity
    , MIN(order_date) OVER (PARTITION BY date_trunc('month', order_date)::date) AS order_start_date
    , MAX(order_date) OVER (PARTITION BY date_trunc('month', order_date)::date) AS order_end_date
    , date_trunc('month', order_date)::date AS order_ym
  FROM {{ ref('analytics__order_count') }}(
      date_trunc('month', (DS_END_DATE) - (DS_INTERVAL_MONTH) * INTERVAL '1 month')::date
    , (date_trunc('month', DS_END_DATE)::date) - 1
  )
),{#

-- Step 3: combine order rows without monthly aggregation so order_id remains distinct-countable

#} order_count_mom AS (
  SELECT
      fact.order_id
    , fact.product_order_id
    , fact.product_id
    -- Item attributes
    , fact.item_id
    , fact.item_seq
    , fact.team_name
    , fact.brand_name
    , fact.category_name1
    , fact.category_name2
    , fact.category_name3
    , fact.category_name4
    , fact.color
    , fact.product_name
    , fact.category_unit_name
    -- Shop attributes
    , fact.shop_id
    , fact.shop_group
    , fact.shop_name
    -- Order attributes
    , fact.order_status
    , fact.order_quantity
    -- Date attributes
    , fact.order_ym
    , fact.order_start_date
    , fact.order_end_date
    , CONCAT(
          to_char(fact.order_end_date, '[ YY년 MM월 ]')
        , E'\n'
        , (CASE
            WHEN fact.order_start_date != fact.order_end_date
              THEN CONCAT(
                  to_char(fact.order_start_date, 'YY/MM/DD')
                , start_day.name_ko
                , E'\n~ '
              )
            ELSE E'\n'
          END)
        , to_char(fact.order_end_date, 'YY/MM/DD')
        , end_day.name_ko
      ) AS order_date_range
  FROM (
    (SELECT * FROM order_count_ds_range)
    UNION ALL
    (SELECT * FROM order_count_monthly_lookback)
  ) AS fact
  LEFT JOIN dayofweek_name_mapping AS start_day
    ON (EXTRACT(DOW FROM fact.order_start_date) + 1) = start_day.dayofweek
  LEFT JOIN dayofweek_name_mapping AS end_day
    ON (EXTRACT(DOW FROM fact.order_end_date) + 1) = end_day.dayofweek
){#

#} SELECT * FROM order_count_mom

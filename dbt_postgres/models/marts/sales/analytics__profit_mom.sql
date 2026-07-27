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
    alias = 'profit_mom'
  )
}}

WITH{#

#} order_status_mapping AS (
  {{ core__order_status_mapping() }}
),{#

#} unpivot_metric_mapping AS (
  {{ core__unpivot_metric_mapping() }}
),{#

#} dayofweek_name_mapping AS (
  {{ core__dayofweek_name_mapping() }}
),{#

-- Step 1: aggregate sales within the requested DS_START_DATE to DS_END_DATE range

#} sales_ds_range AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , SUM(sku_quantity) AS sku_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , SUM(margin_amount) AS margin_amount
    , SUM(ad_cost) AS ad_cost
    , SUM(extra_cost) AS extra_cost
    , SUM(profit) AS profit
    , MIN(order_date) AS order_start_date
    , MAX(order_date) AS order_end_date
    , date_trunc('month', DS_END_DATE)::date AS order_ym
  FROM {{ ref('analytics__profit_base') }}(DS_START_DATE, DS_END_DATE)
  GROUP BY product_id, shop_id, order_status
),{#

-- Step 2: aggregate monthly sales for dates before the DS_END_DATE month

#} sales_monthly_lookback AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , SUM(sku_quantity) AS sku_quantity
    , SUM(payment_amount) AS payment_amount
    , SUM(supply_amount) AS supply_amount
    , SUM(supply_cost) AS supply_cost
    , SUM(delivery_fee) AS delivery_fee
    , SUM(margin_amount) AS margin_amount
    , SUM(ad_cost) AS ad_cost
    , SUM(extra_cost) AS extra_cost
    , SUM(profit) AS profit
    , MIN(order_date) AS order_start_date
    , MAX(order_date) AS order_end_date
    , date_trunc('month', order_date)::date AS order_ym
  FROM {{ ref('analytics__profit_base') }}(
      date_trunc('month', (DS_END_DATE) - (DS_INTERVAL_MONTH) * INTERVAL '1 month')::date
    , date_trunc('month', DS_END_DATE)::date - 1
  )
  GROUP BY date_trunc('month', order_date)::date, product_id, shop_id, order_status
),{#

-- Step 4: combine monthly sales and add custom metrics for reporting

#} sales_monthly AS (
  SELECT
      fact.product_id
    , fact.shop_id
    , fact.order_status
    -- Primary metrics
    , fact.profit
    -- Sales metrics
    , COALESCE(fact.sku_quantity * COALESCE(item.unit_scale, 1), 0) AS unit_quantity
    , fact.payment_amount
    , fact.supply_amount
    , fact.supply_cost
    , fact.delivery_fee
    , fact.margin_amount
    -- Ad metrics
    , fact.ad_cost
    , (CASE WHEN fact.shop_id IN ('shop0055', 'shop9000') THEN fact.ad_cost ELSE 0 END) AS ad_cost__searchad
    , (CASE WHEN fact.shop_id IN ('shop0075', 'shop9001') THEN fact.ad_cost ELSE 0 END) AS ad_cost__coupang
    , (CASE WHEN fact.shop_id = 'adop0001' THEN fact.ad_cost ELSE 0 END) AS ad_cost__google
    , (CASE WHEN fact.shop_id = 'adop0002' THEN fact.ad_cost ELSE 0 END) AS ad_cost__meta
    , (CASE WHEN fact.shop_id = 'adop0006' THEN fact.ad_cost ELSE 0 END) AS ad_cost__tiktok
    -- Cost metrics
    , fact.extra_cost
    , (CASE WHEN fact.shop_id = 'adop0003' THEN fact.extra_cost ELSE 0 END) AS extra_cost__marketing
    , (CASE WHEN fact.shop_id = 'adop0004' THEN fact.extra_cost ELSE 0 END) AS extra_cost__sales
    , (CASE WHEN fact.shop_id = 'adop0005' THEN fact.extra_cost ELSE 0 END) AS extra_cost__expense
    -- Roi fractions
    , fact.profit + 0 AS roi__top -- Column names in UNPIVOT IN clause cannot be repeated
    , fact.ad_cost + fact.extra_cost AS roi__bottom
    -- Date attributes
    , MIN(fact.order_start_date) OVER (PARTITION BY fact.order_ym) AS order_start_date
    , MAX(fact.order_end_date) OVER (PARTITION BY fact.order_ym) AS order_end_date
    , fact.order_ym
  FROM (
    (SELECT * FROM sales_ds_range)
    UNION ALL
    (SELECT * FROM sales_monthly_lookback)
  ) AS fact
  LEFT JOIN {{ ref('core__product_master') }} AS item
    ON fact.product_id = item.product_id
),{#

-- Step 5: unpivot monthly metrics into metric_name and metric_value for reporting

#} sales_monthly_unpivot AS (
  SELECT
      product_id
    , shop_id
    , order_status
    , metric_name
    , metric_value
    , MIN(order_start_date) OVER (PARTITION BY order_ym) AS order_start_date
    , MAX(order_end_date) OVER (PARTITION BY order_ym) AS order_end_date
    , order_ym
  FROM sales_monthly
  CROSS JOIN LATERAL (VALUES
      ('profit', profit)
    , ('unit_quantity', unit_quantity)
    , ('payment_amount', payment_amount)
    , ('supply_amount', supply_amount)
    , ('supply_cost', supply_cost)
    , ('delivery_fee', delivery_fee)
    , ('margin_amount', margin_amount)
    , ('ad_cost', ad_cost)
    , ('ad_cost__searchad', ad_cost__searchad)
    , ('ad_cost__coupang', ad_cost__coupang)
    , ('ad_cost__google', ad_cost__google)
    , ('ad_cost__meta', ad_cost__meta)
    , ('ad_cost__tiktok', ad_cost__tiktok)
    , ('extra_cost', extra_cost)
    , ('extra_cost__marketing', extra_cost__marketing)
    , ('extra_cost__sales', extra_cost__sales)
    , ('extra_cost__expense', extra_cost__expense)
    , ('roi__top', roi__top)
    , ('roi__bottom', roi__bottom)
  ) AS metrics(metric_name, metric_value)
  WHERE metric_value IS NOT NULL
),{#

-- Step 5: enrich unpivoted metrics with item, shop, and sales attributes

#} profit_mom AS (
  SELECT
      fact.product_id
    -- Item attributes
    , COALESCE(item.item_id, 'NA-AAAAAA-00') AS item_id
    , COALESCE(item.item_seq, 99999999) AS item_seq
    , COALESCE(item.team_name, '담당팀 없음') AS team_name
    , COALESCE(item.brand_name, '브랜드 없음') AS brand_name
    , COALESCE(item.category_name1, '-') AS category_name1
    , COALESCE(item.category_name2, '-') AS category_name2
    , COALESCE(item.category_name3, '-') AS category_name3
    , COALESCE(item.category_name4, '-') AS category_name4
    , COALESCE(item.color, '-') AS color
    , COALESCE(item.product_name, '매칭 불가 상품') AS product_name
    , COALESCE(
        (CASE
          WHEN item.unit_name IS NULL
            THEN item.category_name3
          ELSE item.category_name3 || ' (' || item.unit_name || ')'
        END)
        , '-'
      ) AS category_unit_name
    -- Shop attributes
    , fact.shop_id
    , COALESCE(shop.shop_group, '-') AS shop_group
    , COALESCE(shop.shop_alias, '-') AS shop_name
    -- Sales attributes
    , COALESCE(order_status.label, '알 수 없음') AS order_status
    -- Unpivot metrics
    , fact.metric_name AS metric_name_en
    , CONCAT(
          lpad(metric.sort_seq::text, 2, '0')
        , COALESCE('-' || lpad(metric.sub_seq::text, 1, '0'), '')
        , '. '
        , metric.name_ko
      ) AS metric_name_ko
    , fact.metric_value
    -- Date attributes
    , fact.order_start_date
    , fact.order_end_date
    , fact.order_ym
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
  FROM sales_monthly_unpivot AS fact
  LEFT JOIN {{ ref('core__product_master') }} AS item
    ON fact.product_id = item.product_id
  LEFT JOIN {{ source('sabangnet', 'shop') }} AS shop
    ON fact.shop_id = shop.shop_id
  LEFT JOIN order_status_mapping AS order_status
    ON fact.order_status = order_status.code
  LEFT JOIN unpivot_metric_mapping AS metric
    ON fact.metric_name = metric.name_en
  LEFT JOIN dayofweek_name_mapping AS start_day
    ON (EXTRACT(DOW FROM fact.order_start_date) + 1) = start_day.dayofweek
  LEFT JOIN dayofweek_name_mapping AS end_day
    ON (EXTRACT(DOW FROM fact.order_end_date) + 1) = end_day.dayofweek
){#

#} SELECT * FROM profit_mom

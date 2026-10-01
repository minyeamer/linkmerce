{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'DS_START_DATE', 'type': 'date'},
        {'name': 'DS_END_DATE', 'type': 'date'},
        {'name': 'SHOP_FILTER', 'type': 'text'}
      ]
    },
    schema = 'analytics',
    alias = 'sales_summary'
  )
}}

WITH{#

#} smartstore_sales_summary AS (
  SELECT
      '스마트스토어' AS shop_name
    , summary.channel_seq AS account_no
    , channel.channel_name AS account_name
    , channel.corp_name
    , channel.team_name
    , summary.payment_amount
    , summary.max_order_dt
    , summary.order_date
  FROM {{ ref('smartstore__sales_summary') }} AS summary
  LEFT JOIN {{ source('smartstore', 'channel') }} AS channel
    ON summary.channel_seq = channel.channel_seq
  WHERE summary.order_date BETWEEN DS_START_DATE AND DS_END_DATE
){#

#} SELECT *
FROM smartstore_sales_summary
WHERE shop_name = SHOP_FILTER

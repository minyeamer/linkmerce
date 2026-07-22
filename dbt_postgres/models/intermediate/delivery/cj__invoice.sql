{{
  config(
    materialized = 'tvf',
    meta = {
      'params': [
        {'name': 'DS_START_DATE', 'type': 'date'},
        {'name': 'DS_END_DATE', 'type': 'date'}
      ]
    },
    schema = 'xfm_sales'
  )
}}

WITH{#

#} loisparcel AS (
  SELECT
      REPLACE(invoice_no, '-', '') AS invoice_no
    , COALESCE(order_id, 'none') AS order_id
    , SUM(delivery_fee) AS delivery_fee
    , 0 AS box_cost
  FROM {{ source('cj_loisparcel', 'invoice') }}
  WHERE register_date BETWEEN DS_START_DATE AND DS_END_DATE
  GROUP BY invoice_no, order_id
),{#

#} eflexs AS (
  SELECT
      invoice_no
    , order_id
    , SUM(delivery_fee) AS delivery_fee
    , SUM(box_cost) AS box_cost
  FROM {{ source('cj_eflexs', 'invoice_order') }}
  WHERE order_date BETWEEN DS_START_DATE AND DS_END_DATE
  GROUP BY invoice_no, order_id
),{#

#} cj_invoice AS (
  SELECT
      invoice_no
    , SUM(delivery_fee) AS delivery_fee
    , SUM(box_cost) AS box_cost
  FROM (
    (SELECT * FROM loisparcel)
    UNION ALL
    (SELECT * FROM eflexs)
  )
  GROUP BY invoice_no
){#

#} SELECT * FROM cj_invoice

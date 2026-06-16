{{
  config(
    materialized = 'view',
    schema = 'xfm_sales'
  )
}}

WITH

loisparcel AS (
  SELECT
      REPLACE(invoice_no, '-', '') AS invoice_no
    , COALESCE(order_id, 'none') AS order_id
    , SUM(delivery_fee) AS delivery_fee
    , 0 AS box_cost
  FROM {{ source('cj_loisparcel', 'invoice') }}
  WHERE register_date
    BETWEEN DATE_SUB(DATE('{{ var("ds_start_date") }}'), INTERVAL 7 DAY)
      AND DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 7 DAY)
  GROUP BY invoice_no, order_id
),

eflexs AS (
  SELECT
      invoice_no
    , order_id
    , SUM(delivery_fee) AS delivery_fee
    , SUM(box_cost) AS box_cost
  FROM {{ source('cj_eflexs', 'invoice_order') }}
  WHERE order_date
    BETWEEN DATE_SUB(DATE('{{ var("ds_start_date") }}'), INTERVAL 3 DAY)
      AND DATE('{{ var("ds_end_date") }}')
  GROUP BY invoice_no, order_id
),

cj_invoice_order AS (
  SELECT
      order_id
    , SUM(delivery_fee) AS delivery_fee
    , SUM(box_cost) AS box_cost
  FROM (
    (SELECT * FROM loisparcel)
    UNION ALL
    (SELECT * FROM eflexs)
  )
  WHERE order_id != 'none'
  GROUP BY order_id
)

SELECT * FROM cj_invoice_order

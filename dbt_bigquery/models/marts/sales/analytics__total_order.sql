{{
config(
  materialized = 'tvf',
  meta = {
    'params': [
      {'name': 'DS_START_DATETIME', 'type': 'datetime'},
      {'name': 'DS_END_DATETIME', 'type': 'datetime'}
    ]
  },
  schema = 'analytics',
  alias = 'total_order'
)
}}

WITH{#

-- BigQuery allows a maximum definition body size of 32,768 bytes.
-- Omit comments at compile time with dbt tags and reduce indentation by one level.

#} delivery_group AS (
SELECT
    dlv.delivery_group
  , COALESCE(dlv.min_unit, 1) AS min_unit
  , (CASE
      WHEN MAX(dlv.min_unit) OVER (PARTITION BY dlv.delivery_group) = dlv.min_unit THEN 9999
      ELSE COALESCE(LEAD(dlv.min_unit) OVER (PARTITION BY dlv.delivery_group ORDER BY dlv.min_unit))
    END) AS max_unit
  , COALESCE(dlv.delivery_fee, 0) AS delivery_fee
  , (COALESCE(dlv.coolant_cost, 0) + COALESCE(dlv.label_cost, 0)
    + COALESCE(dlv.wrap_cost, 0) + COALESCE(dlv.box_cost, 0)) AS extra_cost
  , COALESCE(dlv.n_arrival_fee, 0) AS n_arrival_fee
  , COALESCE(dlv.n_arrival_add, 0) AS n_arrival_add
FROM {{ source('core', 'delivery_group') }} AS dlv
),{#

#} ecount_product AS (
SELECT
    SPLIT(option_id, '-')[SAFE_OFFSET(0)] AS product_id
  , option_id
  , org_price
FROM {{ source('ecount', 'product') }}
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY option_id ORDER BY expiration_date ASC, product_code DESC
) = 1
),{#

#} product_delivery_unit AS (
{{ core__product_delivery_unit() }}
),{#

#} order_status_mapping AS (
{{ core__order_status_mapping() }}
),{#

-- ################################################################
-- ########################### Sabangnet ##########################
-- ################################################################

-- Step 1: prepare orders

#} sabangnet__order_invoice AS (
SELECT
    order_seq
  , ANY_VALUE(invoice_no) AS invoice_no
  , MAX(order_status) AS order_status
FROM {{ source('sabangnet', 'order_invoice') }}
WHERE order_dt >= DS_START_DATETIME AND order_dt <= DS_END_DATETIME
GROUP BY order_seq
),{#

#} sabangnet__order_status AS (
SELECT
    cor.order_id
  , MAX(cor.order_status) AS order_status
FROM {{ source('core', 'order_status') }} AS cor
WHERE cor.order_date BETWEEN DATE(DS_START_DATETIME) AND DATE(DS_END_DATETIME)
  AND cor.shop_name != '스마트스토어'
GROUP BY cor.order_id
),{#

#} sabangnet__order_detail AS (
SELECT
    ord.order_seq
  , COALESCE(ord.order_id, '-') AS order_id
  , COALESCE(ord.order_id_dup, '-') AS product_order_id
  , COALESCE(sbn.invoice_no, '-') AS invoice_no
  , ord.account_no
  {# -- Sales dimensions
#}, acc.shop_id
  , SPLIT(ord.option_id, '-')[SAFE_OFFSET(0)] AS product_id
  , ord.option_id
  , opt.bundle_option_ids
  , ord.product_id_shop
  , cor.order_status AS order_status_cor
  , COALESCE(sbn.order_status, 1) AS order_status_sbn
  {# -- Sales metrics
#}, COALESCE(ord.order_quantity, 0) AS order_quantity
  , COALESCE(ord.sku_quantity, 0) AS sku_quantity
  , COALESCE(ord.payment_amount, 0) AS payment_amount
  , COALESCE(acc.commission_rate, 0.0) AS commission_rate
  {# -- Sales partition key
#}, ord.order_dt
FROM {{ source('sabangnet', 'order') }} AS ord
LEFT JOIN {{ source('sabangnet', 'account') }} AS acc
  ON ord.account_no = acc.account_no
{# -- Resolve bundle_product_ids
#}LEFT JOIN {{ source('sabangnet', 'option') }} AS opt
  ON ord.option_id = opt.option_id
{# -- Resolve order_status
#}LEFT JOIN sabangnet__order_invoice AS sbn
  ON ord.order_seq = sbn.order_seq
LEFT JOIN sabangnet__order_status AS cor
  ON ord.order_id = cor.order_id
{# -- Filter orders
#}WHERE ord.order_dt >= DS_START_DATETIME AND ord.order_dt <= DS_END_DATETIME
  AND acc.shop_id NOT IN ('shop0055', 'chop0022', 'chop0027', 'chop0028', 'chop0029')
),{#

-- Step 2: apply bundle product rules

#} sabangnet__bundle_product_order AS (
SELECT
    * EXCEPT (net_rate, order_dt)
  , CAST(ROUND(CAST(payment_amount AS NUMERIC) * CAST(net_rate AS NUMERIC), 0) AS INT64) AS supply_amount
  , order_dt
FROM (
  SELECT
      order_seq
    , order_id
    , product_order_id
    , invoice_no
    , account_no
    {# -- Sales dimensions
  #}, ({{ sabangnet__shop_id_rules() }}) AS shop_id
    , product_id
    , option_id
    , ({{ sabangnet__bundle_option_rules() }}) AS bundle_option_ids
    , ({{ sabangnet__order_status_rules() }}) AS order_status
    {# -- Sales metrics
  #}, order_quantity
    , ({{ sabangnet__sku_quantity_rules() }}) AS sku_quantity
    , ({{ sabangnet__payment_amount_rules() }}) AS payment_amount
    , ({{ sabangnet__net_rate_rules() }}) AS net_rate
    {# -- Sales partition key
  #}, order_dt
  FROM sabangnet__order_detail
) AS t_
WHERE shop_id != 'chop9022'
),{#

-- Step 3: explode bundle products with bundle options

#} sabangnet__exploded_product_order AS (
SELECT
    ord.order_seq
  , ord.order_id
  , ord.product_order_id
  , ord.invoice_no
  , ord.account_no
  {# -- Sales dimensions
#}, ord.shop_id
  , SPLIT(bundle_option, '-')[SAFE_OFFSET(0)] AS product_id
  , SPLIT(bundle_option, ':')[SAFE_OFFSET(0)] AS option_id
  , ord.order_status
  {# -- Sales metrics
#}, (COALESCE(SAFE_CAST(SPLIT(bundle_option, ':')[SAFE_OFFSET(1)] AS INT64), 1)
    * ord.order_quantity) AS sku_quantity
  , IF(ROW_NUMBER() OVER (PARTITION BY ord.account_no, ord.order_id ORDER BY ord.order_seq) = 1
      , MAX(ord.payment_amount) OVER (PARTITION BY ord.account_no, ord.order_id)
      , 0
    ) AS payment_amount
  , IF(ROW_NUMBER() OVER (PARTITION BY ord.account_no, ord.order_id ORDER BY ord.order_seq) = 1
      , MAX(ord.supply_amount) OVER (PARTITION BY ord.account_no, ord.order_id)
      , 0
    ) AS supply_amount
  {# -- Sales partition key
#}, ord.order_dt
FROM sabangnet__bundle_product_order AS ord
CROSS JOIN UNNEST(SPLIT(ord.bundle_option_ids, ',')) AS bundle_option
WHERE ord.bundle_option_ids IS NULL
),{#

-- Step 4: attach cost data

#} sabangnet__product_order_with_cost_data AS (
SELECT
    *
  {# -- Allocation metrics
#}, COUNT(*) OVER (PARTITION BY account_no, order_id) AS bundle_product_count
  , CAST(org_price * sku_quantity AS NUMERIC) AS cost_amount
FROM (
  SELECT
      ord.order_id
    , ord.product_order_id
    , ord.invoice_no
    , ord.account_no
    {# -- Sales dimensions
  #}, ord.shop_id
    , ord.product_id
    , ord.order_status
    {# -- Sales metrics
  #}, ord.sku_quantity
    , ord.payment_amount
    , ord.supply_amount
    {# -- Cost data
  #}, COALESCE(prd.org_price, itm.org_price, 0) + COALESCE(itm.extra_cost, 0) AS org_price
    , COALESCE(itm.delivery_group, '-') AS delivery_group
    , COALESCE(itm.delivery_fee, 0) AS delivery_fee
    {# -- Sales partition key
  #}, ord.order_dt
  FROM (
    (SELECT * EXCEPT (bundle_option_ids, order_quantity)
    FROM sabangnet__bundle_product_order
    WHERE bundle_option_ids IS NULL)
    UNION ALL
    (SELECT * FROM sabangnet__exploded_product_order)
  ) AS ord
  LEFT JOIN ecount_product AS prd
    ON ord.option_id = prd.option_id
  LEFT JOIN {{ source('core', 'item') }} AS itm
    ON ord.product_id = itm.product_id
) AS t_
),{#

-- Step 5: allocate amounts across order products by cost weight

#} sabangnet__product_order_with_split_amount AS (
{# -- Step 5-3: adjust rounding remainders to preserve the original totals
#}SELECT
    order_id
  , product_order_id
  , invoice_no
  , account_no
  {# -- Sales dimensions
#}, shop_id
  , product_id
  , order_status
  {# -- Sales metrics
#}, sku_quantity
  , (CASE
      WHEN order_offset = 1
        THEN total_payment_amount - SUM(payment_amount_split) OVER (PARTITION BY account_no, order_id)
      ELSE 0
    END) + payment_amount_split AS payment_amount
  , (CASE
      WHEN order_offset = 1
        THEN total_supply_amount - SUM(supply_amount_split) OVER (PARTITION BY account_no, order_id)
      ELSE 0
    END) + supply_amount_split AS supply_amount
  {# -- Cost data
#}, org_price
  , delivery_group
  , delivery_fee
  {# -- Sales partition key
#}, order_dt
  {# -- Allocation metrics
#}, cost_amount
FROM (
  {# -- Step 5-2: split amounts by cost weight
#}SELECT
      *
    , COALESCE(CAST(ROUND(total_payment_amount * cost_weight, 0) AS INT64), 0) AS payment_amount_split
    , COALESCE(CAST(ROUND(total_supply_amount * cost_weight, 0) AS INT64), 0) AS supply_amount_split
  FROM (
    SELECT
        *
      {# -- Step 5-1: calculate cost weights within each order
    #}, SUM(payment_amount) OVER (PARTITION BY account_no, order_id) AS total_payment_amount
      , SUM(supply_amount) OVER (PARTITION BY account_no, order_id) AS total_supply_amount
      , cost_amount / NULLIF(SUM(cost_amount) OVER (PARTITION BY account_no, order_id), 0) AS cost_weight
      , ROW_NUMBER() OVER (PARTITION BY account_no, order_id ORDER BY product_id) AS order_offset
    FROM sabangnet__product_order_with_cost_data
    WHERE bundle_product_count > 1
  ) AS t0_
) AS t1_
),{#

-- Step 6: prepare delivery data

#} sabangnet__product_order_with_cj_delivery AS (
SELECT
    ord.order_id
  , ord.product_order_id
  , ord.invoice_no
  {# -- Sales dimensions
#}, ord.shop_id
  , ord.product_id
  , ord.order_status
  {# -- Sales metrics
#}, ord.sku_quantity
  , ord.payment_amount
  , ord.supply_amount
  , ord.org_price * ord.sku_quantity AS supply_cost
  {# -- Delivery data
#}, ord.org_price
  , ord.delivery_group
  , ord.sku_quantity * COALESCE(unit.unit, 1) AS delivery_quantity
  , COALESCE(cj_inv.delivery_fee, cj_ord.delivery_fee, ord.delivery_fee, 0) AS delivery_fee
  , COALESCE(cj_inv.box_cost, cj_ord.box_cost, 0) AS box_cost
  {# -- Sales partition key
#}, ord.order_dt
  {# -- Allocation metrics
#}, ord.cost_amount
FROM (
  (SELECT * EXCEPT (bundle_product_count)
  FROM sabangnet__product_order_with_cost_data
  WHERE bundle_product_count = 1)
  UNION ALL
  (SELECT * FROM sabangnet__product_order_with_split_amount)
) AS ord
LEFT JOIN {{ ref('cj__invoice') }}(
    DATE_SUB(DATE(DS_START_DATETIME), INTERVAL 7 DAY)
  , DATE_ADD(DATE(DS_END_DATETIME), INTERVAL 7 DAY)
) AS cj_inv
  ON ord.invoice_no = cj_inv.invoice_no
LEFT JOIN {{ ref('cj__invoice_order') }}(
    DATE_SUB(DATE(DS_START_DATETIME), INTERVAL 7 DAY)
  , DATE_ADD(DATE(DS_END_DATETIME), INTERVAL 7 DAY)
) AS cj_ord
  ON ord.order_id = cj_ord.order_id
LEFT JOIN product_delivery_unit AS unit
  ON ord.product_id = unit.product_id
),{#

-- Step 7: determine the maximum delivery fee at the order level

#} sabangnet__max_delivery_fee AS (
{# -- Step 7-3: select the delivery fee with the largest absolute value for each order
#}SELECT *
FROM (
  {# -- Step 7-2: calculate delivery fees under each delivery group rule
#}SELECT
      ord.order_id
    , ord.invoice_no
    , ord.delivery_group
    , (CASE
        WHEN dlv.delivery_group IS NULL
          THEN ord.delivery_fee
        WHEN ord.delivery_fee > 0
          THEN (ord.delivery_fee + IF(ord.box_cost > 0, ord.box_cost, dlv.extra_cost))
        ELSE dlv.delivery_fee + dlv.extra_cost
      END) AS delivery_fee
  FROM (
    {# -- Step 7-1: aggregate delivery data by each delivery group
  #}SELECT
        order_id
      , invoice_no
      , delivery_group
      , MAX(delivery_fee) AS delivery_fee
      , SUM(box_cost) AS box_cost
      , COALESCE(SUM(delivery_quantity), 0) AS delivery_quantity
    FROM sabangnet__product_order_with_cj_delivery
    GROUP BY order_id, invoice_no, delivery_group
  ) AS ord
  LEFT JOIN delivery_group AS dlv
    ON ord.delivery_group = dlv.delivery_group
      AND ord.delivery_quantity BETWEEN dlv.min_unit AND dlv.max_unit
) AS t_
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, invoice_no ORDER BY ABS(delivery_fee) DESC) = 1
),{#

-- Step 8: attach the maximum delivery fee to product orders

#} sabangnet__product_order_with_max_delivery AS (
SELECT
    ord.order_id
  , ord.product_order_id
  , ord.invoice_no
  {# -- Sales dimensions
#}, ord.shop_id
  , ord.product_id
  , ord.order_status
  {# -- Sales metrics
#}, ord.sku_quantity
  , ord.payment_amount
  , ord.supply_amount
  , ord.supply_cost
  , ord.org_price
  , dlv.delivery_fee
  {# -- Sales partition key
#}, ord.order_dt
  {# -- Allocation metrics
#}, COUNT(*) OVER (PARTITION BY ord.order_id, ord.invoice_no) AS bundle_invoice_count
  , ord.cost_amount
FROM sabangnet__product_order_with_cj_delivery AS ord
LEFT JOIN sabangnet__max_delivery_fee AS dlv
  ON ord.order_id = dlv.order_id
    AND ord.invoice_no = dlv.invoice_no
),{#

-- Step 9: allocate delivery fees across complex product orders

#} sabangnet__product_order_with_split_delivery AS (
{# -- Step 9-3: adjust rounding remainders to preserve the original totals
#}SELECT
    order_id
  , product_order_id
  , invoice_no
  {# -- Sales dimensions
#}, shop_id
  , product_id
  , order_status
  {# -- Sales metrics
#}, sku_quantity
  , payment_amount
  , supply_amount
  , supply_cost
  , (CASE WHEN order_invoice_offset = 1
        THEN delivery_fee - (SUM(delivery_fee_split) OVER (PARTITION BY order_id, invoice_no))
      ELSE 0
    END) + delivery_fee_split AS delivery_fee
  {# -- Sales partition key
#}, order_dt
FROM (
  {# -- Step 9-2: split delivery fees by cost weight
#}SELECT
      *
    , COALESCE(CAST(ROUND(delivery_fee * cost_weight, 0) AS INT64), 0) AS delivery_fee_split
  FROM (
    SELECT
        *
      {# -- Step 9-1: calculate cost weights within each order invoice
    #}, cost_amount / NULLIF(SUM(cost_amount) OVER (PARTITION BY order_id, invoice_no), 0) AS cost_weight
      , ROW_NUMBER() OVER (PARTITION BY order_id, invoice_no ORDER BY product_id) AS order_invoice_offset
    FROM sabangnet__product_order_with_max_delivery
    WHERE bundle_invoice_count > 1
  ) AS t0_
) AS t1_
),{#

-- Step 10: aggregate daily sales

#} sabangnet__product_order AS (
SELECT
    order_id
  , product_order_id
  , product_id
  , shop_id
  , order_status
  , sku_quantity
  , payment_amount
  , supply_amount
  , supply_cost
  , delivery_fee
  , order_dt
FROM (
  (SELECT * EXCEPT (bundle_invoice_count, cost_amount, org_price)
  FROM sabangnet__product_order_with_max_delivery WHERE bundle_invoice_count = 1)
  UNION ALL
  (SELECT * FROM sabangnet__product_order_with_split_delivery)
) AS t_
),{#

-- ################################################################
-- ########################## Smartstore ##########################
-- ################################################################

-- Step 1: prepare orders

#} smartstore__order_delivery AS (
SELECT
    product_order_id
  , ANY_VALUE(invoice_no) AS invoice_no
FROM {{ source('smartstore', 'order_delivery') }}
WHERE payment_dt >= DS_START_DATETIME AND payment_dt <= DS_END_DATETIME
GROUP BY product_order_id
),{#

#} smartstore__order_status_smt AS (
SELECT
    smt.product_order_id
  , MAX(smt.order_status) AS order_status
FROM {{ source('smartstore', 'order_status') }} AS smt
WHERE smt.payment_dt >= DS_START_DATETIME AND smt.payment_dt <= DS_END_DATETIME
GROUP BY smt.product_order_id
),{#

#} smartstore__order_status_cor AS (
SELECT
    CAST(cor.order_id AS INT64) AS order_id
  , MAX(cor.order_status) AS order_status
FROM {{ source('core', 'order_status') }} AS cor
WHERE cor.order_date BETWEEN DATE(DS_START_DATETIME) AND DATE(DS_END_DATETIME)
  AND cor.shop_name = '스마트스토어'
  AND REGEXP_CONTAINS(cor.order_id, '^[0-9]+$')
GROUP BY cor.order_id
),{#

#} smartstore__bundle_product_order AS (
SELECT
    ord.order_id
  , ord.product_order_id
  , COALESCE(dlv.invoice_no, '-') AS invoice_no
  {# -- Sales dimensions
#}, COALESCE(
        rel.bundle_product_ids
      , chl.brand_id
      , '200000'
    ) AS bundle_product_ids
  , IF(ord.delivery_type = 7, 7, 0) AS delivery_type
  , (CASE
      WHEN status_cor.order_status IS NOT NULL THEN status_cor.order_status
      WHEN status_smt.order_status = 7 THEN 1
      WHEN status_smt.order_status = 5 THEN 2
      WHEN status_smt.order_status IN (6, 8) THEN 3
      ELSE 0
    END) AS order_status
  {# -- Sales metrics
#}, COALESCE(ord.order_quantity, 0) AS order_quantity
  , ((COALESCE(ord.unit_price, 0) + COALESCE(ord.option_price, 0))
      * COALESCE(ord.order_quantity, 0)
      - COALESCE(ord.seller_discount_amount, 0)
    ) AS payment_amount
  , COALESCE(ord.supply_amount, 0) AS supply_amount
  {# -- Sales partition key
#}, ord.payment_dt AS order_dt
FROM {{ source('smartstore', 'order_detail') }} AS ord
LEFT JOIN smartstore__order_delivery AS dlv
  ON ord.product_order_id = dlv.product_order_id
{# -- Resolve bundle_product_ids
#}LEFT JOIN {{ ref('relation__smt_opt_to_sbn_ids') }} AS rel
  ON ord.option_id = rel.option_id
LEFT JOIN {{ source('smartstore', 'channel') }} AS chl
  ON ord.channel_seq = chl.channel_seq
{# -- Resolve order_status
#}LEFT JOIN smartstore__order_status_smt AS status_smt
  ON ord.product_order_id = status_smt.product_order_id
LEFT JOIN smartstore__order_status_cor AS status_cor
  ON ord.order_id = status_cor.order_id
WHERE ord.payment_dt >= DS_START_DATETIME AND ord.payment_dt <= DS_END_DATETIME
),{#

-- Step 2: explode bundle products and attach cost data

#} smartstore__exploded_product_order AS (
SELECT
    *
  {# -- Allocation metrics
#}, COUNT(*) OVER (PARTITION BY product_order_id) AS bundle_product_count
  , CAST(IF(order_status = 6, 0, org_price * sku_quantity) AS NUMERIC) AS cost_amount
FROM (
  SELECT
      ord.order_id
    , ord.product_order_id
    , ord.invoice_no
    {# -- Sales dimensions
  #}, SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] AS product_id
    , ord.delivery_type
    , (CASE
        WHEN (ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9') THEN 6
        ELSE ord.order_status
      END) AS order_status
    {# -- Sales metrics
  #}, (COALESCE(SAFE_CAST(SPLIT(bundle_product, ':')[SAFE_OFFSET(1)] AS INT64), 1)
      * ord.order_quantity) AS sku_quantity
    , ord.payment_amount
    , ord.supply_amount
    {# -- Cost data
  #}, COALESCE(prd.org_price, itm.org_price, 0) + COALESCE(itm.extra_cost, 0) AS org_price
    , COALESCE(itm.delivery_group, '-') AS delivery_group
    , COALESCE(itm.delivery_fee, 0) AS delivery_fee
    {# -- Sales partition key
  #}, ord.order_dt
  FROM smartstore__bundle_product_order AS ord
  CROSS JOIN UNNEST(SPLIT(ord.bundle_product_ids, ',')) AS bundle_product
  LEFT JOIN ecount_product AS prd
    ON SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] = prd.product_id
  LEFT JOIN {{ source('core', 'item') }} AS itm
    ON SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] = itm.product_id
) AS t_
),{#

-- Step 3: allocate amounts across bundle products by cost weight

#} smartstore__product_order_with_split_amount AS (
{# -- Step 3-3: adjust rounding remainders to preserve the original totals
#}SELECT
    order_id
  , product_order_id
  , invoice_no
  {# -- Sales dimensions
#}, product_id
  , delivery_type
  , order_status
  {# -- Sales metrics
#}, sku_quantity
  , (CASE
      WHEN product_order_offset = 1
        THEN payment_amount - SUM(payment_amount_split) OVER (PARTITION BY product_order_id)
      ELSE 0
    END) + payment_amount_split AS payment_amount
  , (CASE
      WHEN product_order_offset = 1
        THEN supply_amount - SUM(supply_amount_split) OVER (PARTITION BY product_order_id)
      ELSE 0
    END) + supply_amount_split AS supply_amount
  {# -- Cost data
#}, org_price
  , delivery_group
  , delivery_fee
  {# -- Sales partition key
#}, order_dt
  {# -- Allocation metrics
#}, cost_amount
FROM (
  {# -- Step 3-2: split amounts by cost weights
#}SELECT
      *
    , COALESCE(CAST(ROUND(payment_amount * cost_weight, 0) AS INT64), 0) AS payment_amount_split
    , COALESCE(CAST(ROUND(supply_amount * cost_weight, 0) AS INT64), 0) AS supply_amount_split
  FROM (
    SELECT
        *
      {# -- Step 3-1: calculate cost weights within each product order
    #}, cost_amount / NULLIF(SUM(cost_amount) OVER (PARTITION BY product_order_id), 0) AS cost_weight
      , ROW_NUMBER() OVER (PARTITION BY product_order_id ORDER BY product_id) AS product_order_offset
    FROM smartstore__exploded_product_order
    WHERE bundle_product_count > 1
  ) AS t0_
) AS t1_
),{#

-- Step 4: attach delivery data

#} smartstore__product_order_with_cj_delivery AS (
SELECT
    ord.order_id
  , ord.product_order_id
  , ord.invoice_no
  {# -- Sales dimensions
#}, ord.product_id
  , ord.delivery_type
  , ord.order_status
  {# -- Sales metrics
#}, ord.sku_quantity
  , ord.payment_amount
  , ord.supply_amount
  , ord.org_price * ord.sku_quantity AS supply_cost
  {# -- Delivery data
#}, ord.org_price
  , ord.delivery_group
  , (ord.sku_quantity * COALESCE(dlv.unit, 1)) AS delivery_quantity
  , COALESCE(cj_inv.delivery_fee, cj_ord.delivery_fee, ord.delivery_fee, 0) AS delivery_fee
  , COALESCE(cj_inv.box_cost, cj_ord.box_cost, 0) AS box_cost
  {# -- Sales partition key
#}, ord.order_dt
  {# -- Allocation metrics
#}, ord.cost_amount
FROM (
  (SELECT * EXCEPT (bundle_product_count)
  FROM smartstore__exploded_product_order
  WHERE bundle_product_count = 1)
  UNION ALL
  (SELECT * FROM smartstore__product_order_with_split_amount)
) AS ord
LEFT JOIN {{ ref('cj__invoice') }}(
    DATE_SUB(DATE(DS_START_DATETIME), INTERVAL 7 DAY)
  , DATE_ADD(DATE(DS_END_DATETIME), INTERVAL 7 DAY)
) AS cj_inv
  ON ord.invoice_no = cj_inv.invoice_no
LEFT JOIN {{ ref('cj__invoice_order') }}(
    DATE_SUB(DATE(DS_START_DATETIME), INTERVAL 7 DAY)
  , DATE_ADD(DATE(DS_END_DATETIME), INTERVAL 7 DAY)
) AS cj_ord
  ON CAST(ord.order_id AS STRING) = cj_ord.order_id
LEFT JOIN product_delivery_unit AS dlv
  ON ord.product_id = dlv.product_id
),{#

-- Step 5: determine the maximum delivery fee at the order level

#} smartstore__max_delivery_fee AS (
{# -- Step 5-3: select the delivery fee with the largest absolute value for each order
#}SELECT *
FROM (
  {# -- Step 5-2: caculate delivery fees under each delivery group rule
#}SELECT
      ord.order_id
    , ord.invoice_no
    , ord.delivery_group
    , (CASE
        WHEN dlv.delivery_group IS NULL
          THEN ord.delivery_fee
        WHEN ord.delivery_fee > 0
          THEN (ord.delivery_fee + IF(ord.box_cost > 0, ord.box_cost, dlv.extra_cost))
        WHEN ord.delivery_type = 7
          THEN dlv.n_arrival_fee + (dlv.n_arrival_add * (ord.delivery_quantity - dlv.min_unit))
        ELSE dlv.delivery_fee + dlv.extra_cost
      END) AS delivery_fee
  FROM (
    {# -- Step 5-1: aggregate delivery data by each delivery group
  #}SELECT
        order_id
      , invoice_no
      , MAX(delivery_type) AS delivery_type
      , delivery_group
      , MAX(delivery_fee) AS delivery_fee
      , SUM(box_cost) AS box_cost
      , COALESCE(SUM(delivery_quantity), 0) AS delivery_quantity
    FROM smartstore__product_order_with_cj_delivery
    GROUP BY order_id, invoice_no, delivery_group
  ) AS ord
  LEFT JOIN delivery_group AS dlv
    ON ord.delivery_group = dlv.delivery_group
      AND ord.delivery_quantity BETWEEN dlv.min_unit AND dlv.max_unit
) AS t_
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, invoice_no ORDER BY ABS(delivery_fee) DESC) = 1
),{#

-- Step 6: attach the maximum delivery fee to product orders

#} smartstore__product_order_with_max_delivery AS (
SELECT
    ord.order_id
  , ord.product_order_id
  , ord.invoice_no
  {# -- Sales dimensions
#}, ord.product_id
  , ord.delivery_type
  , ord.order_status
  {# -- Sales metrics
#}, ord.sku_quantity
  , ord.payment_amount
  , ord.supply_amount
  , ord.supply_cost
  {# -- Delivery data
#}, ord.org_price
  , dlv.delivery_fee
  {# -- Sales partition key
#}, ord.order_dt
  {# -- Allocation metrics
#}, COUNT(*) OVER (PARTITION BY ord.order_id, ord.invoice_no) AS bundle_invoice_count
  , ord.cost_amount
FROM smartstore__product_order_with_cj_delivery AS ord
LEFT JOIN smartstore__max_delivery_fee AS dlv
  ON ord.order_id = dlv.order_id AND ord.invoice_no = dlv.invoice_no
),{#

-- Step 7: allocate delivery fees across complex product orders

#} smartstore__product_order_with_split_delivery AS (
{# -- Step 7-3: adjust rounding remainders to preserve the original totals
#}SELECT
    order_id
  , product_order_id
  , invoice_no
  {# -- Sales dimensions
#}, product_id
  , delivery_type
  , order_status
  {# -- Sales metrics
#}, sku_quantity
  , payment_amount
  , supply_amount
  , supply_cost
  , (CASE
      WHEN order_invoice_offset = 1
        THEN delivery_fee - SUM(delivery_fee_split) OVER (PARTITION BY order_id, invoice_no)
      ELSE 0
    END) + delivery_fee_split AS delivery_fee
  {# -- Sales partition key
#}, order_dt
FROM (
  {# -- Step 7-2: split delivery fees by cost weight
#}SELECT
      *
    , COALESCE(CAST(ROUND(delivery_fee * cost_weight, 0) AS INT64), 0) AS delivery_fee_split
  FROM (
    SELECT
        *
      {# -- Step 7-1: calculate cost weights within each order invoice
    #}, cost_amount / NULLIF(SUM(cost_amount) OVER (PARTITION BY order_id, invoice_no), 0) AS cost_weight
      , ROW_NUMBER() OVER (PARTITION BY order_id, invoice_no ORDER BY product_id) AS order_invoice_offset
    FROM smartstore__product_order_with_max_delivery
    WHERE bundle_invoice_count > 1
  ) AS t0_
) AS t1_
),{#

-- Step 8: aggregate daily sales

#} smartstore__product_order AS (
SELECT
    CAST(order_id AS STRING) AS order_id
  , CAST(product_order_id AS STRING) AS product_order_id
  , product_id
  , IF(delivery_type = 7, 'shop9000', 'shop0055') AS shop_id
  , order_status
  , sku_quantity
  , payment_amount
  , supply_amount
  , supply_cost
  , delivery_fee
  , order_dt
FROM (
  (SELECT * EXCEPT (bundle_invoice_count, cost_amount, org_price)
  FROM smartstore__product_order_with_max_delivery
  WHERE bundle_invoice_count = 1)
  UNION ALL
  (SELECT * FROM smartstore__product_order_with_split_delivery)
) AS t_
),{#

-- ################################################################
-- ######################## Coupang Rocket ########################
-- ################################################################

-- Step 1: prepare sales and delivery data

#} coupang_rfm__rocket_sales AS (
SELECT
    order_id
  , option_id
  , ANY_VALUE(vendor_id) AS vendor_id
  , MAX(settlement_type) AS order_status
  , SUM(order_quantity) AS order_quantity
  , SUM(COALESCE(unit_price, 0) * COALESCE(order_quantity, 0)
      - COALESCE(coupang_discount, 0)
      - COALESCE(seller_discount, 0)
    ) AS sales_amount
  , SUM(settlement_amount) AS settlement_amount
  , MAX(sales_date) AS sales_date
FROM {{ source('coupang_rfm', 'sales') }}
WHERE sales_date BETWEEN DATE(DS_START_DATETIME) AND DATE(DS_END_DATETIME)
GROUP BY order_id, option_id
),{#

#} coupang_rfm__rocket_shipping AS (
SELECT
    order_id
  , option_id
  , ANY_VALUE(vendor_id) AS vendor_id
  , SUM(COALESCE(warehousing_fee, 0)
      - COALESCE(discount_amount, 0)
      + COALESCE(extra_fee, 0)
    ) AS delivery_fee
  , MAX(sales_date) AS sales_date
FROM {{ source('coupang_rfm', 'shipping') }}
WHERE sales_date
  BETWEEN DATE(DS_START_DATETIME) AND DATE(DS_END_DATETIME)
GROUP BY order_id, option_id
),{#

#} coupang_rfm__rocket_sales_shipping AS (
SELECT
    COALESCE(sales.order_id, shipping.order_id) AS order_id
  , COALESCE(sales.option_id, shipping.option_id) AS option_id
  , ANY_VALUE(COALESCE(sales.vendor_id, shipping.vendor_id)) AS vendor_id
  , (CASE
      WHEN MAX(sales.order_status) IS NULL THEN 7
      ELSE LEAST(MAX(sales.order_status), 3)
    END) AS order_status
  , SUM(COALESCE(sales.order_quantity, 0)) AS order_quantity
  , SUM(COALESCE(sales.sales_amount, 0)) AS sales_amount
  , SUM(COALESCE(sales.settlement_amount, 0)) AS settlement_amount
  , SUM(COALESCE(shipping.delivery_fee, 0)) AS delivery_fee
  , COALESCE(sales.sales_date, shipping.sales_date) AS sales_date
FROM coupang_rfm__rocket_sales AS sales
FULL OUTER JOIN coupang_rfm__rocket_shipping AS shipping
  ON sales.order_id = shipping.order_id AND sales.option_id = shipping.option_id
GROUP BY sales_date, order_id, option_id
),{#

#} coupang_rfm__bundle_product_order AS (
SELECT
    ord.order_id
  , ord.option_id
  {# -- Sales dimensions
#}, COALESCE(
        rel.bundle_product_ids
      , vdr.bundle_brand_ids
      , '200000'
    ) AS bundle_product_ids
  , ord.order_status
  {# -- Sales metrics
#}, ord.order_quantity
  , ord.sales_amount AS payment_amount
  , ord.settlement_amount AS supply_amount
  , ord.delivery_fee
  {# -- Sales partition key
#}, DATETIME(ord.sales_date) AS order_dt
FROM coupang_rfm__rocket_sales_shipping AS ord
LEFT JOIN {{ source('relation', 'cpg_opt_to_sbn_ids') }} AS rel
  ON ord.option_id = rel.option_id
LEFT JOIN {{ source('coupang', 'vendor') }} AS vdr
  ON ord.vendor_id = vdr.vendor_id
WHERE NOT (ord.order_quantity = 0 AND ord.delivery_fee = 0)
),{#

-- Step 2: explode bundle products and attach cost data

#} coupang_rfm__exploded_product_order AS (
SELECT
    ord.order_id
  , ord.option_id
  {# -- Sales dimensions
#}, SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] AS product_id
  , (CASE
      WHEN (ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9') THEN 6
      ELSE ord.order_status
    END) AS order_status
  {# -- Sales metrics
#}, (COALESCE(SAFE_CAST(SPLIT(bundle_product, ':')[SAFE_OFFSET(1)] AS INT64), 1)
    * ord.order_quantity) AS sku_quantity
  , ord.payment_amount
  , ord.supply_amount
  , COALESCE(prd.org_price, itm.org_price, 0) + COALESCE(itm.extra_cost, 0) AS org_price
  , ord.delivery_fee
  , itm.delivery_group
  {# -- Sales partition key
#}, ord.order_dt
FROM coupang_rfm__bundle_product_order AS ord
CROSS JOIN UNNEST(SPLIT(ord.bundle_product_ids, ',')) AS bundle_product
LEFT JOIN ecount_product AS prd
  ON SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] = prd.product_id
LEFT JOIN {{ source('core', 'item') }} AS itm
  ON SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] = itm.product_id
),{#

-- Step 3: add delivery extra cost per order option before amount allocation

#} coupang_rfm__product_order_with_delivery_extra AS (
SELECT
    ord.order_id
  , ord.option_id
  {# -- Sales dimensions
#}, ord.product_id
  , ord.order_status
  {# -- Sales metrics
#}, ord.sku_quantity
  , ord.payment_amount
  , ord.supply_amount
  , ord.org_price * ord.sku_quantity AS supply_cost
  , (COALESCE(MAX(dlv.extra_cost) OVER (PARTITION BY order_id, option_id), 0)
    + ord.delivery_fee) AS delivery_fee
  {# -- Sales partition key
#}, ord.order_dt
  {# -- Allocation metrics
#}, COUNT(*) OVER (PARTITION BY ord.order_id, ord.option_id) AS bundle_product_count
  , CAST(IF(ord.order_status = 6, 0, ord.org_price * ord.sku_quantity) AS NUMERIC) AS cost_amount
FROM coupang_rfm__exploded_product_order AS ord
LEFT JOIN product_delivery_unit AS unit
  ON ord.product_id = unit.product_id
LEFT JOIN delivery_group AS dlv
  ON ord.delivery_group = dlv.delivery_group
    AND (ord.sku_quantity * COALESCE(unit.unit, 1)) BETWEEN dlv.min_unit AND dlv.max_unit
),{#

-- Step 4: allocate amounts across bundle products by cost weight

#} coupang_rfm__product_order_with_split_amount AS (
{# -- Step 4-3: adjust rounding remainders to preserve the original totals
#}SELECT
    order_id
  , option_id
  {# -- Sales dimensions
#}, product_id
  , order_status
  {# -- Sales metrics
#}, sku_quantity
  , (CASE
      WHEN order_option_offset = 1
        THEN payment_amount - SUM(payment_amount_split) OVER (PARTITION BY order_id, option_id)
      ELSE 0
    END) + payment_amount_split AS payment_amount
  , (CASE
      WHEN order_option_offset = 1
        THEN supply_amount - SUM(supply_amount_split) OVER (PARTITION BY order_id, option_id)
      ELSE 0
    END) + supply_amount_split AS supply_amount
  , supply_cost
  , (CASE
      WHEN order_option_offset = 1
        THEN delivery_fee - SUM(delivery_fee_split) OVER (PARTITION BY order_id, option_id)
      ELSE 0
    END) + delivery_fee_split AS delivery_fee
  {# -- Sales partition key
#}, order_dt
FROM (
  {# -- Step 4-2: split amounts by cost weight
#}SELECT
      *
    , COALESCE(CAST(ROUND(payment_amount * cost_weight, 0) AS INT64), 0) AS payment_amount_split
    , COALESCE(CAST(ROUND(supply_amount * cost_weight, 0) AS INT64), 0) AS supply_amount_split
    , COALESCE(CAST(ROUND(delivery_fee * cost_weight, 0) AS INT64), 0) AS delivery_fee_split
  FROM (
    SELECT
        *
      {# -- Step 4-1: calculate cost weights within each order option
    #}, cost_amount / NULLIF(SUM(cost_amount) OVER (PARTITION BY order_id, option_id), 0) AS cost_weight
      , ROW_NUMBER() OVER (PARTITION BY order_id, option_id ORDER BY product_id) AS order_option_offset
    FROM coupang_rfm__product_order_with_delivery_extra
    WHERE bundle_product_count > 1
  ) AS t0_
) AS t1_
),{#

-- Step 5: aggregate daily sales

#} coupang_rfm__product_order AS (
SELECT
    CAST(order_id AS STRING) AS product_order_id
  , CAST(NULL AS STRING) AS product_order_id
  , product_id
  , 'shop9001' AS shop_id
  , order_status
  , sku_quantity
  , payment_amount
  , supply_amount
  , supply_cost
  , delivery_fee
  , order_dt
FROM (
  (SELECT * EXCEPT (bundle_product_count, cost_amount)
  FROM coupang_rfm__product_order_with_delivery_extra
  WHERE bundle_product_count = 1)
  UNION ALL
  (SELECT * FROM coupang_rfm__product_order_with_split_amount)
) AS t_
),{#

-- ################################################################
-- ############################# Total ############################
-- ################################################################

#} total_product_order AS (
SELECT
    order_id
  , product_order_id
  , product_id
  , shop_id
  , order_status
  , IF(order_status = 0, COALESCE(sku_quantity, 0), 0) AS sku_quantity
  , (CASE
      {# -- Exclude payment_amount from extra_sales
    #}WHEN shop_id = 'adop9000' THEN 0
      WHEN order_status = 0 THEN COALESCE(payment_amount, 0)
      ELSE 0
    END) AS payment_amount
  , IF(order_status = 0, COALESCE(supply_amount, 0), 0) AS supply_amount
  , IF(order_status IN (0, 2, 6), COALESCE(supply_cost, 0), 0) AS supply_cost
  , IF(order_status IN (0, 1, 2, 5, 7), COALESCE(delivery_fee, 0), 0) AS delivery_fee
  , order_dt
FROM (
  (SELECT * FROM sabangnet__product_order)
  UNION ALL
  (SELECT * FROM smartstore__product_order)
  UNION ALL
  (SELECT * FROM coupang_rfm__product_order)
) AS t_
){#

#} SELECT
  fact.order_id
, fact.product_order_id
, fact.product_id
{# -- Item attributes
#}, COALESCE(item.item_id, 'NA-AAAAAA-00') AS item_id
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
    IF(item.unit_name IS NULL
      , item.category_name3
      , CONCAT(item.category_name3, ' (', item.unit_name, ')'))
    , '-'
  ) AS category_unit_name
{# -- Shop attributes
#}, fact.shop_id
, COALESCE(shop.shop_group, '-') AS shop_group
, COALESCE(shop.shop_alias, '-') AS shop_name
{# -- Sales attributes
#}, COALESCE(order_status.label, '알 수 없음') AS order_status
, COALESCE(fact.sku_quantity * COALESCE(item.unit_scale, 1), 0) AS unit_quantity
, fact.sku_quantity
, fact.payment_amount
, fact.supply_amount
, fact.supply_cost
, fact.delivery_fee
, supply_amount - supply_cost - delivery_fee AS margin_amount
, fact.order_dt
FROM total_product_order AS fact
LEFT JOIN {{ ref('core__product_master') }} AS item
  ON fact.product_id = item.product_id
LEFT JOIN {{ source('sabangnet', 'shop') }} AS shop
  ON fact.shop_id = shop.shop_id
LEFT JOIN order_status_mapping AS order_status
  ON fact.order_status = order_status.code

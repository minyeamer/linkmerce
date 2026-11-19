{% macro split_delivery_fee(
  input_dimensions,
  delivery_partitions,
  cj_invoice_key = none,
  cj_order_key = none,
  use_eflexs_invoice = false,
  input_cte_name = 'adjusted_product_order',
  output_cte_name = 'product_order_with_delivery_fee'
) %}
{#-
-- Required preceding CTE: input_cte_name (default: adjusted_product_order).
-- It must include { order_date, input_dimensions, product_id, product_quantity,
-- order_status, order_quantity, payment_amount, supply_amount, supply_cost,
-- delivery_group, delivery_fee, and cost_amount }.

-- Output CTE: output_cte_name (default: product_order_with_delivery_fee).
-- It retains the input dimensions
-- and sales metrics, adds supply_cost and delivery_fee, and carries cost_amount
-- plus delivery partition fields for downstream aggregation.
#}

{%- if not delivery_partitions -%}
  {{ exceptions.raise_compiler_error("delivery_partitions must contain at least one column.") }}
{%- endif -%}

{%- for column in delivery_partitions -%}
  {%- if column not in input_dimensions -%}
    {{ exceptions.raise_compiler_error(
      "delivery_partitions column '" ~ column ~ "' must be included in input_dimensions."
    ) }}
  {%- endif -%}
{%- endfor -%}

{%- if cj_invoice_key is not none and cj_invoice_key not in input_dimensions -%}
  {{ exceptions.raise_compiler_error(
    "cj_invoice_key '" ~ cj_invoice_key ~ "' must be included in input_dimensions."
  ) }}
{%- endif -%}

{%- if cj_order_key is not none and cj_order_key not in input_dimensions -%}
  {{ exceptions.raise_compiler_error(
    "cj_order_key '" ~ cj_order_key ~ "' must be included in input_dimensions."
  ) }}
{%- endif -%}

{%- if use_eflexs_invoice and 'delivery_type' not in input_dimensions -%}
  {{ exceptions.raise_compiler_error(
    "`use_eflexs_invoice=True` requires 'delivery_type' in input_dimensions."
  ) }}
{%- endif -%}

-- Step 1: normalize CJ join keys in the input orders

product_order_with_delivery_keys AS (
  SELECT
      *
    {%- if cj_invoice_key is not none %}
    , NULLIF(REPLACE(CAST({{ cj_invoice_key }} AS STRING), '-', ''), '') AS cj_invoice_no
    {%- endif %}
    {%- if cj_order_key is not none %}
    , CAST({{ cj_order_key }} AS STRING) AS cj_order_id
    {%- endif %}
  FROM {{ input_cte_name }}
),

-- Step 2: prepare CJ invoices

{%- if cj_invoice_key is not none or cj_order_key is not none %}
-- Step 2-1: read CJ invoice data for the requested date range
cj_invoice AS (
  SELECT
      REPLACE(invoice_no, '-', '') AS invoice_no
    , COALESCE(order_id, 'none') AS order_id
    , delivery_fee
    , 0 AS box_cost
  FROM {{ source('cj_loisparcel', 'invoice') }}
  WHERE register_date BETWEEN
      DATE_SUB(DATE('{{ var("ds_start_date") }}'), INTERVAL 7 DAY)
      AND DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 7 DAY)
  {%- if use_eflexs_invoice %}
  UNION ALL
  SELECT
      invoice_no
    , order_id
    , delivery_fee
    , box_cost
  FROM {{ source('cj_eflexs', 'invoice_order') }}
  WHERE order_date BETWEEN
      DATE_SUB(DATE('{{ var("ds_start_date") }}'), INTERVAL 7 DAY)
      AND DATE_ADD(DATE('{{ var("ds_end_date") }}'), INTERVAL 7 DAY)
  {%- endif %}
),
{%- else %}
cj_invoice AS (
  SELECT
      CAST(NULL AS STRING) AS invoice_no
    , CAST(NULL AS STRING) AS order_id
    , CAST(NULL AS INTEGER) AS delivery_fee
    , CAST(NULL AS INTEGER) AS box_cost
),
{%- endif %}

{%- if cj_order_key is not none %}
-- Step 2-2: prepare order-level fallback data when no invoice match exists
cj_invoice_order AS (
  SELECT
      inv.order_id
    , SUM(inv.delivery_fee) AS delivery_fee
    , SUM(inv.box_cost) AS box_cost
  FROM cj_invoice AS inv
  {%- if cj_invoice_key is not none %}
  LEFT JOIN (
    SELECT DISTINCT
      ord.cj_order_id AS order_id
    FROM product_order_with_delivery_keys AS ord
    INNER JOIN cj_invoice AS cj_inv
      ON ord.cj_invoice_no = cj_inv.invoice_no
    WHERE ord.cj_order_id IS NOT NULL
  ) AS matched
    ON inv.order_id = matched.order_id
  {%- endif %}
  WHERE inv.order_id != 'none'
  {%- if cj_invoice_key is not none %}
    -- Exclude orders that already have a CJ invoice_no match
    AND matched.order_id IS NULL
  {%- endif %}
  GROUP BY inv.order_id
),
{%- else %}
cj_invoice_order AS (
  SELECT
      CAST(NULL AS STRING) AS order_id
    , CAST(NULL AS INTEGER) AS delivery_fee
    , CAST(NULL AS INTEGER) AS box_cost
),
{%- endif %}

product_delivery_unit AS (
  {{ core__product_delivery_unit() }}
),

-- Step 3: attach delivery data and determine delivery partitions

product_order_with_cj_delivery AS (
  SELECT
      ord.order_date
    {%- for column in input_dimensions %}
    , ord.{{ column }}
    {%- endfor %}
    , ord.product_id
    , ord.product_quantity
    , ord.order_status
    , ord.order_quantity
    , ord.payment_amount
    , ord.supply_amount
    , ord.supply_cost
    , ord.delivery_group
    , ord.product_quantity * ord.order_quantity * COALESCE(unit.unit, 1) AS delivery_quantity
    , COALESCE(cj_inv.delivery_fee, cj_ord.delivery_fee, ord.delivery_fee, 0) AS delivery_fee
    , COALESCE(cj_inv.box_cost, cj_ord.box_cost, 0) AS box_cost
    {%- if cj_invoice_key is not none or cj_order_key is not none %}
    , (CASE
        {%- if cj_invoice_key is not none %}
        WHEN cj_inv.invoice_no IS NOT NULL
          THEN 0 -- Case 1: CJ invoice match
        {%- endif %}
        {%- if cj_order_key is not none %}
        WHEN cj_ord.order_id IS NOT NULL
          THEN 1 -- Case 2: CJ order fallback
        {%- endif %}
        {%- if cj_invoice_key is not none %}
        WHEN ord.cj_invoice_no IS NOT NULL
          AND ord.cj_invoice_no != 'none'
          THEN 2 -- Case 3: Valid invoice, no CJ match
        {%- endif %}
        ELSE 3 -- Case 4: Fallback delivery partitions
      END) AS delivery_partition_type
    , (CASE
        {%- if cj_invoice_key is not none %}
        WHEN cj_inv.invoice_no IS NOT NULL
          THEN ord.cj_invoice_no -- Case 1: CJ invoice match
        {%- endif %}
        {%- if cj_order_key is not none %}
        WHEN cj_ord.order_id IS NOT NULL
          THEN ord.cj_order_id -- Case 2: CJ order fallback
        {%- endif %}
        {%- if cj_invoice_key is not none %}
        WHEN ord.cj_invoice_no IS NOT NULL
          AND ord.cj_invoice_no != 'none'
          THEN ord.cj_invoice_no -- Case 3: Valid invoice, no CJ match
        {%- endif %}
        ELSE TO_JSON_STRING([
          {%- for column in delivery_partitions %}
          CAST(ord.{{ column }} AS STRING){% if not loop.last %}, {% endif %}
          {%- endfor %}
        ]) -- Case 4: Fallback delivery partitions
      END) AS delivery_partition_value
    {%- else %}
    , 3 AS delivery_partition_type
    , TO_JSON_STRING([
        {%- for column in delivery_partitions %}
        CAST(ord.{{ column }} AS STRING){% if not loop.last %}, {% endif %}
        {%- endfor %}
      ]) AS delivery_partition_value
    {%- endif %}
    , ord.cost_amount
  FROM product_order_with_delivery_keys AS ord
  LEFT JOIN cj_invoice AS cj_inv
    {%- if cj_invoice_key is not none %}
    ON ord.cj_invoice_no = cj_inv.invoice_no
    {%- else %}
    ON FALSE
    {%- endif %}
  LEFT JOIN cj_invoice_order AS cj_ord
    {%- if cj_order_key is not none %}
    ON ord.cj_order_id = cj_ord.order_id
    {%- else %}
    ON FALSE
    {%- endif %}
  LEFT JOIN product_delivery_unit AS unit
    ON ord.product_id = unit.product_id
),

delivery_group AS (
  SELECT
      dlv.delivery_group
    , COALESCE(dlv.min_unit, 1) AS min_unit
    , (CASE
        WHEN MAX(dlv.min_unit) OVER (PARTITION BY dlv.delivery_group) = dlv.min_unit THEN 9999
        ELSE LEAD(dlv.min_unit) OVER (PARTITION BY dlv.delivery_group ORDER BY dlv.min_unit)
      END) AS max_unit
    , COALESCE(dlv.delivery_fee, 0) AS delivery_fee
    , (COALESCE(dlv.coolant_cost, 0) + COALESCE(dlv.label_cost, 0)
      + COALESCE(dlv.wrap_cost, 0) + COALESCE(dlv.box_cost, 0)) AS extra_cost
    {%- if use_eflexs_invoice %}
    , COALESCE(dlv.n_arrival_fee, 0) AS n_arrival_fee
    , COALESCE(dlv.n_arrival_add, 0) AS n_arrival_add
    {%- endif %}
  FROM {{ source('core', 'delivery_group') }} AS dlv
),

-- Step 3: calculate one delivery fee for each delivery partition

max_delivery_fee AS (
  -- Step 3-3: keep the fee with the largest absolute value in each partition
  SELECT *
  FROM (
    -- Step 3-2: calculate the fee under each delivery-group rule
    SELECT
        ord.delivery_partition_type
      , ord.delivery_partition_value
      , (CASE
          WHEN dlv.delivery_group IS NULL
            THEN ord.delivery_fee + ord.box_cost
          WHEN ord.delivery_fee > 0
            THEN ord.delivery_fee + IF(ord.box_cost > 0, ord.box_cost, dlv.extra_cost)
          {%- if use_eflexs_invoice %}
          WHEN ord.delivery_type = 7
            THEN dlv.n_arrival_fee
              + (dlv.n_arrival_add * (ord.delivery_quantity - dlv.min_unit))
          {%- endif %}
          ELSE dlv.delivery_fee + dlv.extra_cost
        END) AS delivery_fee
    FROM (
      -- Step 3-1: aggregate delivery quantities and fees by partition and delivery group
      SELECT
          delivery_partition_type
        , delivery_partition_value
        , delivery_group
        {%- if use_eflexs_invoice %}
        , MAX(delivery_type) AS delivery_type
        {%- endif %}
        , MAX(delivery_fee) AS delivery_fee
        , MAX(box_cost) AS box_cost
        , SUM(delivery_quantity) AS delivery_quantity
      FROM product_order_with_cj_delivery
      GROUP BY delivery_partition_type, delivery_partition_value, delivery_group
    ) AS ord
    LEFT JOIN delivery_group AS dlv
      ON ord.delivery_group = dlv.delivery_group
        AND ord.delivery_quantity BETWEEN dlv.min_unit AND dlv.max_unit
  ) AS t_
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY delivery_partition_type, delivery_partition_value
    ORDER BY ABS(delivery_fee) DESC, delivery_fee DESC
  ) = 1
),

-- Step 4: attach the maximum delivery fee to product orders

product_order_with_max_delivery AS (
  SELECT
      ord.order_date
    {%- for column in input_dimensions %}
    , ord.{{ column }}
    {%- endfor %}
    , ord.product_id
    , ord.product_quantity
    , ord.order_status
    , ord.order_quantity
    , ord.payment_amount
    , ord.supply_amount
    , ord.supply_cost
    , dlv.delivery_fee
    , COUNT(*) OVER (
        PARTITION BY ord.delivery_partition_type, ord.delivery_partition_value
      ) AS bundle_delivery_count
    , ord.delivery_partition_type
    , ord.delivery_partition_value
    , ord.cost_amount
  FROM product_order_with_cj_delivery AS ord
  LEFT JOIN max_delivery_fee AS dlv
    ON ord.delivery_partition_type = dlv.delivery_partition_type
      AND ord.delivery_partition_value = dlv.delivery_partition_value
),

-- Step 5: split delivery fees across product orders

product_order_with_split_delivery AS (
  -- Step 5-3: adjust rounding remainders to preserve each partition's total fee
  SELECT
      order_date
    {%- for column in input_dimensions %}
    , {{ column }}
    {%- endfor %}
    , product_id
    , product_quantity
    , order_status
    , order_quantity
    , payment_amount
    , supply_amount
    , supply_cost
    , (CASE
        WHEN delivery_offset = 1
          THEN delivery_fee - SUM(delivery_fee_split) OVER (
            PARTITION BY delivery_partition_type, delivery_partition_value
          )
        ELSE 0
      END) + delivery_fee_split AS delivery_fee
    , bundle_delivery_count
    , delivery_partition_type
    , delivery_partition_value
    , cost_amount
  FROM (
    -- Step 5-2: split delivery fees by each product order's cost weight
    SELECT
        *
      , COALESCE(CAST(ROUND(delivery_fee * cost_weight, 0) AS INT64), 0) AS delivery_fee_split
    FROM (
      -- Step 5-1: calculate cost weights and select a row for rounding adjustment
      SELECT
          *
        , cost_amount / NULLIF(SUM(cost_amount) OVER (
            PARTITION BY delivery_partition_type, delivery_partition_value
          ), 0) AS cost_weight
        , ROW_NUMBER() OVER (
            PARTITION BY delivery_partition_type, delivery_partition_value
            ORDER BY product_id ASC, product_quantity DESC
          ) AS delivery_offset
      FROM product_order_with_max_delivery
      WHERE bundle_delivery_count > 1
    ) AS t0_
  ) AS t1_
),

{{ output_cte_name }} AS (
  SELECT
      order_date
    -- Sales dimensions
    {%- for column in input_dimensions %}
    , {{ column }}
    {%- endfor %}
    , product_id
    , product_quantity
    , order_status
    -- Sales metrics
    , order_quantity
    , payment_amount
    , supply_amount
    , supply_cost
    , delivery_fee
  FROM (
    (SELECT * FROM product_order_with_max_delivery WHERE bundle_delivery_count = 1)
    UNION ALL
    (SELECT * FROM product_order_with_split_delivery)
  ) AS t_
)
{%- endmacro %}

{% macro split_sales_amounts(
  input_dimensions,
  order_partitions,
  agg_func = 'max',
  input_cte_name = 'exploded_product_order',
  output_cte_name = 'adjusted_product_order'
) %}
{#-
-- Required preceding CTE: input_cte_name (default: exploded_product_order).
-- It must include { order_date, input_dimensions, product_id, product_quantity,
-- order_status, order_quantity, payment_amount, supply_amount, org_price,
-- delivery_group, and delivery_fee }.

-- Output CTE: output_cte_name (default: adjusted_product_order).
-- It retains the input dimensions and sales metrics,
-- adjusts payment_amount and supply_amount across bundled products,
-- and carries supply_cost and cost_amount forward.

-- agg_func: 'max' for repeated totals; 'sum' for amounts stored once.
#}

{%- if agg_func not in ['sum', 'max'] -%}
  {{ exceptions.raise_compiler_error("agg_func must be 'sum' or 'max'.") }}
{%- endif -%}

{%- if not order_partitions -%}
  {{ exceptions.raise_compiler_error("order_partitions must contain at least one column.") }}
{%- endif -%}

{%- for column in order_partitions -%}
  {%- if column not in input_dimensions -%}
    {{ exceptions.raise_compiler_error(
      "order_partitions column '" ~ column ~ "' must be included in input_dimensions."
    ) }}
  {%- endif -%}
{%- endfor -%}

product_order_with_bundle_count AS (
  SELECT
      *
    , COUNT(*) OVER (PARTITION BY {{ order_partitions | join(', ') }}) AS bundle_product_count
    , CAST(IF(order_status = 6, 0, org_price * product_quantity * order_quantity) AS NUMERIC) AS cost_amount
  FROM {{ input_cte_name }}
),

product_order_with_split_amount AS (
  -- Step 3: adjust rounding remainders to preserve the original totals
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
    , (CASE
        WHEN product_order_offset = 1
          THEN total_payment_amount - SUM(payment_amount_split) OVER (PARTITION BY {{ order_partitions | join(', ') }})
        ELSE 0
      END) + payment_amount_split AS payment_amount
    , (CASE
        WHEN product_order_offset = 1
          THEN total_supply_amount - SUM(supply_amount_split) OVER (PARTITION BY {{ order_partitions | join(', ') }})
        ELSE 0
      END) + supply_amount_split AS supply_amount
    -- Delivery data
    , org_price
    , delivery_group
    , delivery_fee
    -- Allocation metrics
    , bundle_product_count
    , cost_amount
  FROM (
    -- Step 2: split amounts by cost weight
    SELECT
        *
      , COALESCE(CAST(ROUND(total_payment_amount * cost_weight, 0) AS INT64), 0) AS payment_amount_split
      , COALESCE(CAST(ROUND(total_supply_amount * cost_weight, 0) AS INT64), 0) AS supply_amount_split
    FROM (
      -- Step 1: calculate cost weights within each order
      SELECT
          *
        , {{ agg_func }}(payment_amount) OVER (PARTITION BY {{ order_partitions | join(', ') }}) AS total_payment_amount
        , {{ agg_func }}(supply_amount) OVER (PARTITION BY {{ order_partitions | join(', ') }}) AS total_supply_amount
        , cost_amount / NULLIF(SUM(cost_amount) OVER (PARTITION BY {{ order_partitions | join(', ') }}), 0) AS cost_weight
        , ROW_NUMBER() OVER (
            PARTITION BY {{ order_partitions | join(', ') }}
            ORDER BY product_id ASC, product_quantity DESC
          ) AS product_order_offset
      FROM product_order_with_bundle_count
      WHERE bundle_product_count > 1
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
    , org_price * product_quantity * order_quantity AS supply_cost
    -- Delivery data
    , delivery_group
    , delivery_fee
    -- Allocation metrics
    , cost_amount
  FROM (
    (SELECT * FROM product_order_with_bundle_count WHERE bundle_product_count = 1)
    UNION ALL
    (SELECT * FROM product_order_with_split_amount)
  ) AS t_
)
{%- endmacro %}

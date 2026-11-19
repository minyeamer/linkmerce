{% macro explode_product_order(
  input_dimensions,
  input_cte_name = 'order_detail',
  output_cte_name = 'exploded_product_order'
) %}
{#-
-- Required preceding CTE: input_cte_name (default: order_detail).
-- It must include { order_date, input_dimensions, bundle_product_ids,
-- order_status, order_quantity, payment_amount, and supply_amount }.

-- Output CTE: output_cte_name (default: exploded_product_order).
-- It adds { product_id, product_quantity, org_price,
-- delivery_group, and delivery_fee } to preceding CTE's fields.
-#}

ecount_product AS (
  SELECT DISTINCT ON (option_id)
      split_part(option_id, '-', 1) AS product_id
    , org_price
  FROM {{ source('ecount', 'product') }}
  WHERE RIGHT(option_id, 5) = '-0001'
  ORDER BY option_id, expiration_date ASC, product_code DESC
),{#

#} product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),{#

#} {{ output_cte_name }} AS (
  SELECT
      ord.order_date
    -- Sales dimensions
    {%- for column in input_dimensions %}
    , ord.{{ column }}
    {%- endfor %}
    , COALESCE(
          renewal.product_id_old
        , split_part(bundle_product, ':', 1)
      ) AS product_id
    , (CASE
        WHEN split_part(bundle_product, ':', 2) ~ '^[0-9]+$'
          THEN split_part(bundle_product, ':', 2)::integer
        ELSE 1
      END) AS product_quantity
    , (CASE
        WHEN (ord.order_status = 0) AND (LEFT(bundle_product, 1) = '9') THEN 6
        ELSE ord.order_status
      END) AS order_status
    -- Sales metrics
    , ord.order_quantity
    , ord.payment_amount
    , ord.supply_amount
    , COALESCE(prd.org_price, itm.org_price, 0) + COALESCE(itm.extra_cost, 0) AS org_price
    -- Delivery data
    , COALESCE(itm.delivery_group, '-') AS delivery_group
    , COALESCE(itm.delivery_fee, 0) AS delivery_fee
  FROM {{ input_cte_name }} AS ord
  CROSS JOIN LATERAL unnest(string_to_array(ord.bundle_product_ids, ',')) AS t(bundle_product)
  LEFT JOIN product_renewal_mapping AS renewal
    ON split_part(bundle_product, ':', 1) = renewal.product_id_new
      AND ord.order_date < renewal.renewal_date
  LEFT JOIN ecount_product AS prd
    ON COALESCE(
          renewal.product_id_old
        , split_part(bundle_product, ':', 1)
      ) = prd.product_id
  LEFT JOIN {{ source('core', 'item') }} AS itm
    ON COALESCE(
          renewal.product_id_old
        , split_part(bundle_product, ':', 1)
      ) = itm.product_id
)
{%- endmacro %}

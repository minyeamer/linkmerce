{% macro explode_product_order(
  input_dimensions,
  input_cte_name = 'order_detail',
  output_cte_name = 'exploded_product_order'
) %}
{#-
-- Required preceding CTE: input_cte_name (default: order_detail).
-- It must include { order_date, input_dimensions, bundle_product_ids,
-- order_status, order_quantity, payment_amount, and supply_amount }.

-- Output CTEs: output_cte_name (default: exploded_product_order).
-- It adds { product_id, product_quantity, org_price,
-- delivery_group, and delivery_fee } to preceding CTE's fields.
#}

ecount_product AS (
  SELECT
      SPLIT(option_id, '-')[SAFE_OFFSET(0)] AS product_id
    , org_price
  FROM {{ source('ecount', 'product') }}
  WHERE RIGHT(option_id, 5) = '-0001'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY option_id
    ORDER BY expiration_date ASC, product_code DESC
  ) = 1
),

product_renewal_mapping AS (
  {{ core__product_renewal_mapping() }}
),

{{ output_cte_name }} AS (
  SELECT
      ord.order_date
    -- Sales dimensions
    {%- for column in input_dimensions %}
    , ord.{{ column }}
    {%- endfor %}
    , COALESCE(
          renewal.product_id_old
        , SPLIT(bundle_product, ':')[SAFE_OFFSET(0)]
      ) AS product_id
    , COALESCE(
          SAFE_CAST(SPLIT(bundle_product, ':')[SAFE_OFFSET(1)] AS INT64)
        , 1
      ) AS product_quantity
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
  CROSS JOIN UNNEST(SPLIT(ord.bundle_product_ids, ',')) AS bundle_product
  LEFT JOIN product_renewal_mapping AS renewal
    ON SPLIT(bundle_product, ':')[SAFE_OFFSET(0)] = renewal.product_id_new
      AND ord.order_date < renewal.renewal_date
  LEFT JOIN ecount_product AS prd
    ON COALESCE(
          renewal.product_id_old
        , SPLIT(bundle_product, ':')[SAFE_OFFSET(0)]
      ) = prd.product_id
  LEFT JOIN {{ source('core', 'item') }} AS itm
    ON COALESCE(
          renewal.product_id_old
        , SPLIT(bundle_product, ':')[SAFE_OFFSET(0)]
      ) = itm.product_id
)
{%- endmacro %}

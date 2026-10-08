-- Inventory: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    option_id BIGINT NOT NULL
  , sku_id BIGINT
  , vendor_id VARCHAR NOT NULL
  , stock_quantity INTEGER
  , unit_sold_30d INTEGER
  , updated_at TIMESTAMP NOT NULL
  , PRIMARY KEY (option_id)
);

-- Inventory: bulk_insert
INSERT INTO {{ table }}
SELECT
    vendorItemId AS option_id
  , externalSkuId AS sku_id
  , vendorId AS vendor_id
  , inventoryDetails.totalOrderableQuantity AS stock_quantity
  , salesCountMap.SALES_COUNT_LAST_THIRTY_DAYS AS unit_sold_30d
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updated_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- Order: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    order_id BIGINT NOT NULL
  , vendor_id VARCHAR NOT NULL
  , option_id BIGINT NOT NULL
  , order_quantity INTEGER
  , unit_price INTEGER
  , payment_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (order_id, option_id)
);

-- Order: bulk_insert
INSERT INTO {{ table }}
SELECT
    orderId AS order_id
  , vendorId AS vendor_id
  , orderItem.vendorItemId AS option_id
  , orderItem.salesQuantity AS order_quantity
  , orderItem.unitSalesPrice AS unit_price
  , to_timestamp(paidAt / 1000.0) AS payment_dt
FROM {{ rows }}
ON CONFLICT DO NOTHING;
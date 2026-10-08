-- Order: create
CREATE TABLE IF NOT EXISTS {{ order }} (
    vendor_id VARCHAR NOT NULL
  , shipment_box_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , orderer_name VARCHAR
  , orderer_number VARCHAR
  , paid_location VARCHAR
  , order_dt TIMESTAMP NOT NULL
  , paid_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (vendor_id, shipment_box_id)
);

CREATE TABLE IF NOT EXISTS {{ delivery }} (
    vendor_id VARCHAR NOT NULL
  , shipment_box_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , invoice_no VARCHAR
  , post_code VARCHAR
  , shipment_type TINYINT -- {0: 'THIRD_PARTY', 1: 'CGF', 2: 'CGF LITE'}
  , shipment_status TINYINT
  , delivery_fee BIGINT
  , order_dt TIMESTAMP NOT NULL
  , send_dt TIMESTAMP
  , delivered_dt TIMESTAMP
  , PRIMARY KEY (vendor_id, shipment_box_id)
);

CREATE TABLE IF NOT EXISTS {{ detail }} (
    vendor_id VARCHAR NOT NULL
  , shipment_box_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , sequence_no VARCHAR NOT NULL
  , vendor_inventory_id BIGINT NOT NULL
  , product_id BIGINT
  , option_id BIGINT NOT NULL
  , order_quantity BIGINT
  , unit_price BIGINT
  , instant_coupon_discount BIGINT
  , download_coupon_discount BIGINT
  , coupang_discount BIGINT
  , order_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (vendor_id, shipment_box_id, sequence_no)
);

CREATE TABLE IF NOT EXISTS {{ item }} (
    vendor_inventory_id BIGINT NOT NULL
  , product_id BIGINT
  , option_id BIGINT NOT NULL
  , vendor_id VARCHAR NOT NULL
  , seller_product_name VARCHAR
  , display_product_name VARCHAR
  , option_name VARCHAR
  , sales_price BIGINT
  , first_order_dt TIMESTAMP
  , last_order_dt TIMESTAMP
  , PRIMARY KEY (option_id)
);

-- Order: bulk_insert
INSERT INTO {{ order }}
SELECT
    seller.sellerId AS vendor_id
  , shipmentBoxId AS shipment_box_id
  , orderId AS order_id
  , orderer.name AS orderer_name
  , orderer.safeNumber AS orderer_number
  , refer AS paid_location
  , TRY_STRPTIME(SUBSTR(orderedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , TRY_STRPTIME(SUBSTR(paidAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS paid_dt
FROM {{ order_rows }}
WHERE TRY_STRPTIME(SUBSTR(orderedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {{ delivery }}
SELECT
    seller.sellerId AS vendor_id
  , shipmentBoxId AS shipment_box_id
  , orderId AS order_id
  , NULLIF(invoiceNumber, '') AS invoice_no
  , receiver.postCode AS post_code
  , (CASE shipmentType
      WHEN 'THIRD_PARTY' THEN 0
      WHEN 'CGF' THEN 1
      WHEN 'CGF LITE' THEN 2
      ELSE NULL END) AS shipment_type
  , (CASE status
      WHEN 'ACCEPT' THEN 0
      WHEN 'INSTRUCT' THEN 1
      WHEN 'DEPARTURE' THEN 2
      WHEN 'DELIVERING' THEN 3
      WHEN 'FINAL_DELIVERY' THEN 4
      WHEN 'NONE_TRACKING' THEN 5
      ELSE 9 END) AS shipment_status
  , shippingPrice.units AS delivery_fee
  , TRY_STRPTIME(SUBSTR(orderedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , TRY_STRPTIME(SUBSTR(inTrasitDateTime, 1, 19), '%Y-%m-%dT%H:%M:%S') AS send_dt
  , TRY_STRPTIME(SUBSTR(deliveredDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS delivered_dt
FROM {{ order_rows }}
WHERE TRY_STRPTIME(SUBSTR(orderedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {{ detail }}
SELECT
    seller.sellerId AS vendor_id
  , shipmentBoxId AS shipment_box_id
  , orderId AS order_id
  , item.sequenceNo AS sequence_no
  , item.sellerProductId AS vendor_inventory_id
  , NULLIF(item.productId, 0) AS product_id
  , item.vendorItemId AS option_id
  , item.shippingCount AS order_quantity
  , item.salesPrice.units AS unit_price
  , item.instantCouponDiscount.units AS instant_coupon_discount
  , item.downloadableCouponDiscount.units AS download_coupon_discount
  , item.coupangDiscount.units AS coupang_discount
  , TRY_STRPTIME(SUBSTR(orderedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
FROM {{ order_item_rows }}
WHERE TRY_STRPTIME(SUBSTR(orderedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {{ item }}
SELECT
    item.sellerProductId AS vendor_inventory_id
  , NULLIF(item.productId, 0) AS product_id
  , NULLIF(item.vendorItemId, 0) AS option_id
  , seller.sellerId AS vendor_id
  , item.sellerProductName AS seller_product_name
  , item.vendorItemName AS display_product_name
  , item.sellerProductItemName AS option_name
  , item.salesPrice.units AS sales_price
  , MIN(TRY_STRPTIME(SUBSTR(orderedAt, 1, 19), '%Y-%m-%dT%H:%M:%S')) OVER (PARTITION BY item.vendorItemId) AS first_order_dt
  , MAX(TRY_STRPTIME(SUBSTR(orderedAt, 1, 19), '%Y-%m-%dT%H:%M:%S')) OVER (PARTITION BY item.vendorItemId) AS last_order_dt
FROM {{ order_item_rows }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY item.vendorItemId ORDER BY orderedAt DESC) = 1
ON CONFLICT DO UPDATE SET
    vendor_inventory_id = COALESCE(EXCLUDED.vendor_inventory_id, vendor_inventory_id)
  , product_id = COALESCE(EXCLUDED.product_id, product_id)
  , vendor_id = COALESCE(EXCLUDED.vendor_id, vendor_id)
  , seller_product_name = COALESCE(EXCLUDED.seller_product_name, seller_product_name)
  , display_product_name = COALESCE(EXCLUDED.display_product_name, display_product_name)
  , option_name = COALESCE(EXCLUDED.option_name, option_name)
  , sales_price = COALESCE(EXCLUDED.sales_price, sales_price)
  , first_order_dt = LEAST(EXCLUDED.first_order_dt, first_order_dt)
  , last_order_dt = GREATEST(EXCLUDED.last_order_dt, last_order_dt);

-- Order: shipment_status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'ACCEPT' AS name)
  , STRUCT(1 AS seq, 'INSTRUCT' AS name)
  , STRUCT(2 AS seq, 'DEPARTURE' AS name)
  , STRUCT(3 AS seq, 'DELIVERING' AS name)
  , STRUCT(4 AS seq, 'FINAL_DELIVERY' AS name)
  , STRUCT(5 AS seq, 'NONE_TRACKING' AS name)
]);
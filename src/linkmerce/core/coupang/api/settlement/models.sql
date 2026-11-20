-- RevenueHistory: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_id VARCHAR NOT NULL
  , order_id BIGINT NOT NULL
  , product_id BIGINT NOT NULL
  , option_id BIGINT NOT NULL
  , settlement_type TINYINT NOT NULL
  , order_quantity INTEGER
  , sales_price INTEGER
  , sales_amount INTEGER
  , coupang_discount INTEGER
  , seller_discount INTEGER
  , download_coupon_discount INTEGER
  , service_fee INTEGER
  , settlement_amount INTEGER
  , paid_date DATE NOT NULL
  , sales_date DATE
  , settlement_date DATE
  , PRIMARY KEY (vendor_id, order_id, option_id, settlement_type)
);

-- RevenueHistory: bulk_insert
INSERT INTO {{ table }}
SELECT
    $vendor_id AS vendor_id
  , orderId AS order_id
  , item.productId AS product_id
  , item.vendorItemId AS option_id
  , (CASE saleType WHEN 'SALE' THEN 0 WHEN 'REFUND' THEN 1 ELSE 9 END) AS settlement_type
  , item.quantity AS order_quantity
  , item.salePrice AS sales_price
  , item.saleAmount AS sales_amount
  , item.coupangDiscountCoupon AS coupang_discount
  , item.sellerDiscountCoupon AS seller_discount
  , item.downloadableCoupon AS download_coupon_discount
  , item.serviceFee + item.serviceFeeVat AS service_fee
  , item.settlementAmount AS settlement_amount
  , CAST(saleDate AS DATE) AS paid_date
  , CAST(recognitionDate AS DATE) AS sales_date
  , CAST(settlementDate AS DATE) AS settlement_date
FROM {{ rows }}
ON CONFLICT DO NOTHING;
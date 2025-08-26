-- Order: create
CREATE OR REPLACE TABLE {{ table }} (
    product_order_no BIGINT PRIMARY KEY
  , order_no BIGINT
  , orderer_no BIGINT
  , orderer_id VARCHAR
  , orderer_name VARCHAR
  , channel_seq BIGINT
  , product_id BIGINT
  , option_id BIGINT
  , seller_product_code VARCHAR
  , seller_option_code VARCHAR
  , order_status VARCHAR
  , claim_status VARCHAR
  , product_type VARCHAR
  , product_name VARCHAR
  , option_name VARCHAR
  , payment_location VARCHAR
  , inflow_path VARCHAR
  , inflow_path_add VARCHAR
  , delivery_type VARCHAR
  , order_quantity INTEGER
  , sales_price INTEGER
  , option_price INTEGER
  , delivery_fee INTEGER
  , payment_amount INTEGER
  , supply_amount INTEGER
  , order_dt TIMESTAMP
  , payment_dt TIMESTAMP
  , dispatch_dt TIMESTAMP
  , delivery_dt TIMESTAMP
  , decision_dt TIMESTAMP
);

-- Order: select
SELECT
    TRY_CAST(productOrderId AS BIGINT) AS product_order_no
  , TRY_CAST(content.order.orderId AS BIGINT) AS order_no
  , TRY_CAST(content.order.ordererNo AS BIGINT) AS orderer_no
  , content.order.ordererId AS orderer_id
  , content.order.ordererName AS orderer_name
  , TRY_CAST(content.productOrder.merchantChannelId AS BIGINT) AS channel_seq
  , TRY_CAST(content.productOrder.productId AS BIGINT) AS product_id
  , TRY_CAST(content.productOrder.optionCode AS BIGINT) AS option_id
  , content.productOrder.sellerProductCode AS seller_product_code
  , content.productOrder.optionManageCode AS seller_option_code
  , content.productOrder.productOrderStatus AS order_status
  , content.productOrder.claimStatus AS claim_status
  , content.productOrder.productClass AS product_type
  , content.productOrder.productName AS product_name
  , content.productOrder.productOption AS option_name
  , content.order.payLocationType AS payment_location
  , content.productOrder.inflowPath AS inflow_path
  , content.productOrder.inflowPathAdd AS inflow_path_add
  , content.productOrder.deliveryAttributeType AS delivery_type
  , content.productOrder.quantity AS order_quantity
  , content.productOrder.unitPrice AS sales_price
  , content.productOrder.optionPrice AS option_price
  , content.productOrder.deliveryFeeAmount AS delivery_fee
  , content.productOrder.totalPaymentAmount AS payment_amount
  , content.productOrder.expectedSettlementAmount AS supply_amount
  , TRY_STRPTIME(SUBSTR(content.order.orderDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
  , TRY_STRPTIME(SUBSTR(content.delivery.sendDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS dispatch_dt
  , TRY_STRPTIME(SUBSTR(content.delivery.deliveredDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS delivery_dt
  , TRY_STRPTIME(SUBSTR(content.productOrder.decisionDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS decision_dt
FROM {{ array }}
WHERE TRY_CAST(productOrderId AS BIGINT) IS NOT NULL;

-- Order: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ProductOrder: create_order
CREATE OR REPLACE TABLE {{ table }} (
    product_order_no BIGINT PRIMARY KEY
  , order_no BIGINT NOT NULL
  , orderer_no BIGINT
  , channel_seq BIGINT
  , product_id BIGINT
  , option_id BIGINT
  , product_type INTEGER
  , payment_location INTEGER
  , inflow_path VARCHAR
  , inflow_path_add VARCHAR
  , order_quantity INTEGER
  , payment_amount INTEGER
  , supply_amount INTEGER
  , order_dt TIMESTAMP
  , payment_dt TIMESTAMP NOT NULL
);

-- ProductOrder: select_order
SELECT
    TRY_CAST(productOrderId AS BIGINT) AS product_order_no
  , TRY_CAST(content.order.orderId AS BIGINT) AS order_no
  , TRY_CAST(content.order.ordererNo AS BIGINT) AS orderer_no
  , TRY_CAST(content.productOrder.merchantChannelId AS BIGINT) AS channel_seq
  , TRY_CAST(content.productOrder.productId AS BIGINT) AS product_id
  , TRY_CAST(content.productOrder.optionCode AS BIGINT) AS option_id
  , (CASE
      WHEN content.productOrder.productClass = '단일상품' THEN 0
      WHEN content.productOrder.productClass = '조합형옵션상품' THEN 1
      WHEN content.productOrder.productClass = '추가구성상품' THEN 2
      ELSE NULL END) AS product_type
  , (CASE
      WHEN content.order.payLocationType == 'PC' THEN 0
      WHEN content.order.payLocationType == 'MOBILE' THEN 1
      ELSE NULL END) AS payment_location
  , content.productOrder.inflowPath AS inflow_path
  , content.productOrder.inflowPathAdd AS inflow_path_add
  , content.productOrder.quantity AS order_quantity
  , content.productOrder.totalPaymentAmount AS payment_amount
  , content.productOrder.expectedSettlementAmount AS supply_amount
  , TRY_STRPTIME(SUBSTR(content.order.orderDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
FROM {{ array }}
WHERE TRY_CAST(productOrderId AS BIGINT) IS NOT NULL;

-- ProductOrder: insert_order
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- ProductOrder: create_option
CREATE OR REPLACE TABLE {{ table }} (
    product_id BIGINT
  , option_id BIGINT PRIMARY KEY
  , channel_seq BIGINT
  , seller_product_code VARCHAR
  , seller_option_code VARCHAR
  , product_type INTEGER
  , product_name VARCHAR
  , option_name VARCHAR
  , sales_price INTEGER
  , option_price INTEGER
  , delivery_fee INTEGER
  , update_date DATE
);

-- ProductOrder: select_option
SELECT
    TRY_CAST(content.productOrder.productId AS BIGINT) AS product_id
  , TRY_CAST(content.productOrder.optionCode AS BIGINT) AS option_id
  , TRY_CAST(content.productOrder.merchantChannelId AS BIGINT) AS channel_seq
  , content.productOrder.sellerProductCode AS seller_product_code
  , content.productOrder.optionManageCode AS seller_option_code
  , (CASE
      WHEN content.productOrder.productClass = '단일상품' THEN 0
      WHEN content.productOrder.productClass = '조합형옵션상품' THEN 1
      WHEN content.productOrder.productClass = '추가구성상품' THEN 2
      ELSE NULL END) AS product_type
  , content.productOrder.productName AS product_name
  , content.productOrder.productOption AS option_name
  , content.productOrder.unitPrice AS sales_price
  , content.productOrder.optionPrice AS option_price
  , content.productOrder.deliveryFeeAmount AS delivery_fee
  , TRY_CAST(content.order.paymentDate AS DATE) AS update_date
FROM {{ array }}
WHERE TRY_CAST(content.productOrder.optionCode AS BIGINT) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY content.productOrder.optionCode) = 1;

-- ProductOrder: upsert_option
INSERT INTO {{ table }} {{ values }}
ON CONFLICT DO UPDATE SET
    product_id = COALESCE(excluded.product_id, product_id)
  , channel_seq = COALESCE(excluded.channel_seq, channel_seq)
  , seller_product_code = COALESCE(excluded.seller_product_code, seller_product_code)
  , seller_option_code = COALESCE(excluded.seller_option_code, seller_option_code)
  , product_type = COALESCE(excluded.product_type, product_type)
  , product_name = COALESCE(excluded.product_name, product_name)
  , option_name = COALESCE(excluded.option_name, option_name)
  , sales_price = COALESCE(excluded.sales_price, sales_price)
  , option_price = COALESCE(excluded.option_price, option_price)
  , delivery_fee = COALESCE(excluded.delivery_fee, delivery_fee)
  , update_date = GREATEST(excluded.update_date, update_date);
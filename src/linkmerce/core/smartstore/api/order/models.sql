-- Order: create
CREATE OR REPLACE TABLE {{ table }} (
    product_order_no BIGINT PRIMARY KEY
  , order_no BIGINT
  , channel_seq BIGINT
  , product_id BIGINT
  , option_id BIGINT
  , seller_product_code VARCHAR
  , seller_option_code VARCHAR
  , orderer_no BIGINT
  , orderer_id VARCHAR
  , orderer_name VARCHAR
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
  , order_time TIMESTAMP
  , payment_time TIMESTAMP
  , dispatch_time TIMESTAMP
  , delivery_time TIMESTAMP
  , decision_time TIMESTAMP
);

-- Order: select
SELECT
    TRY_CAST(content.productOrder.productOrderId AS BIGINT) AS product_order_no
  , TRY_CAST(content.order.orderId AS BIGINT) AS order_no
  , TRY_CAST(content.productOrder.merchantChannelId AS BIGINT) AS channel_seq
  , TRY_CAST(content.productOrder.productId AS BIGINT) AS mall_pid
  , TRY_CAST(content.productOrder.optionCode AS BIGINT) AS option_id
  , content.productOrder.sellerProductCode AS seller_product_code
  , content.productOrder.optionManageCode AS seller_option_code
  , TRY_CAST(content.order.ordererNo AS BIGINT) AS orderer_no
  , content.order.ordererId AS orderer_id
  , content.order.ordererName AS orderer_name
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
  , STRPTIME(SUBSTR(content.order.orderDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
  , STRPTIME(SUBSTR(content.delivery.sendDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS dispatch_dt
  , STRPTIME(SUBSTR(content.delivery.deliveredDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS delivery_dt
  , STRPTIME(SUBSTR(content.productOrder.decisionDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS decision_dt
FROM {{ array }}
WHERE TRY_CAST(content.productOrder.productOrderId AS BIGINT) IS NOT NULL;

-- Order: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
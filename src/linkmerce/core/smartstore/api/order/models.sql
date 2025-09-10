-- Order: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_order_no BIGINT PRIMARY KEY
  , order_no BIGINT NOT NULL
  , orderer_no BIGINT
  , orderer_id VARCHAR
  , orderer_name VARCHAR
  , channel_seq BIGINT NOT NULL
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
  , order_quantity INTEGER
  , sales_price INTEGER
  , option_price INTEGER
  , payment_amount INTEGER
  , supply_amount INTEGER
  , delivery_type VARCHAR
  , delivery_fee INTEGER
  , order_dt TIMESTAMP
  , payment_dt TIMESTAMP
  , dispatch_dt TIMESTAMP
  , delivery_dt TIMESTAMP
  , decision_dt TIMESTAMP
  , claim_complete_dt TIMESTAMP
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
  , IF(content.productOrder.inflowPathAdd IN ('null','undefined'), NULL, content.productOrder.inflowPathAdd) AS inflow_path_add
  , content.productOrder.quantity AS order_quantity
  , content.productOrder.unitPrice AS sales_price
  , content.productOrder.optionPrice AS option_price
  , content.productOrder.totalPaymentAmount AS payment_amount
  , content.productOrder.expectedSettlementAmount AS supply_amount
  , content.productOrder.deliveryAttributeType AS delivery_type
  , content.productOrder.deliveryFeeAmount AS delivery_fee
  , TRY_STRPTIME(SUBSTR(content.order.orderDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
  , TRY_STRPTIME(SUBSTR(content.delivery.sendDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS dispatch_dt
  , TRY_STRPTIME(SUBSTR(content.delivery.deliveredDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS delivery_dt
  , TRY_STRPTIME(SUBSTR(content.productOrder.decisionDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS decision_dt
  , TRY_STRPTIME(SUBSTR(content.completedClaims[1].claimRequestAdmissionDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS claim_complete_dt
FROM {{ array }};

-- Order: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ProductOrder: create_order
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_order_no BIGINT PRIMARY KEY
  , order_no BIGINT NOT NULL
  , orderer_no BIGINT
  , channel_seq BIGINT NOT NULL
  , product_id BIGINT
  , option_id BIGINT
  , product_type INTEGER
  , payment_location INTEGER
  , inflow_path VARCHAR
  , inflow_path_add VARCHAR
  , order_quantity INTEGER
  , payment_amount INTEGER
  , supply_amount INTEGER
  , delivery_type INTEGER
  , delivery_fee INTEGER
  , order_dt TIMESTAMP
  , payment_dt TIMESTAMP NOT NULL
);

-- ProductOrder: delivery_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'NORMAL' AS code, '일반배송' AS name)
  , STRUCT(1 AS seq, 'TODAY' AS code, '오늘출발' AS name)
  , STRUCT(2 AS seq, 'OPTION_TODAY' AS code, '옵션별 오늘출발' AS name)
  , STRUCT(3 AS seq, 'HOPE' AS code, '희망일배송' AS name)
  , STRUCT(4 AS seq, 'TODAY_ARRIVAL' AS code, '당일배송' AS name)
  , STRUCT(5 AS seq, 'DAWN_ARRIVAL' AS code, '새벽배송' AS name)
  , STRUCT(6 AS seq, 'PRE_ORDER' AS code, '예약구매' AS name)
  , STRUCT(7 AS seq, 'ARRIVAL_GUARANTEE' AS code, 'N배송' AS name)
  , STRUCT(8 AS seq, 'SELLER_GUARANTEE' AS code, 'N판매자배송' AS name)
  , STRUCT(9 AS seq, 'HOPE_SELLER_GUARANTEE' AS code, 'N희망일배송' AS name)
  , STRUCT(10 AS seq, 'PICKUP' AS code, '픽업' AS name)
  , STRUCT(11 AS seq, 'QUICK' AS code, '즉시배달' AS name)
]);

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
      WHEN content.productOrder.productClass IN ('옵션상품','조합형옵션상품') THEN 1
      WHEN content.productOrder.productClass = '추가구성상품' THEN 2
      ELSE NULL END) AS product_type
  , (CASE
      WHEN content.order.payLocationType == 'PC' THEN 0
      WHEN content.order.payLocationType == 'MOBILE' THEN 1
      ELSE NULL END) AS payment_location
  , content.productOrder.inflowPath AS inflow_path
  , IF(content.productOrder.inflowPathAdd IN ('null','undefined'), NULL, content.productOrder.inflowPathAdd) AS inflow_path_add
  , content.productOrder.quantity AS order_quantity
  , content.productOrder.totalPaymentAmount AS payment_amount
  , content.productOrder.expectedSettlementAmount AS supply_amount
  , (CASE
      WHEN content.productOrder.deliveryAttributeType = 'NORMAL' THEN 0
      WHEN content.productOrder.deliveryAttributeType = 'TODAY' THEN 1
      WHEN content.productOrder.deliveryAttributeType = 'OPTION_TODAY' THEN 2
      WHEN content.productOrder.deliveryAttributeType = 'HOPE' THEN 3
      WHEN content.productOrder.deliveryAttributeType = 'TODAY_ARRIVAL' THEN 4
      WHEN content.productOrder.deliveryAttributeType = 'DAWN_ARRIVAL' THEN 5
      WHEN content.productOrder.deliveryAttributeType = 'PRE_ORDER' THEN 6
      WHEN content.productOrder.deliveryAttributeType = 'ARRIVAL_GUARANTEE' THEN 7
      WHEN content.productOrder.deliveryAttributeType = 'SELLER_GUARANTEE' THEN 8
      WHEN content.productOrder.deliveryAttributeType = 'HOPE_SELLER_GUARANTEE' THEN 9
      WHEN content.productOrder.deliveryAttributeType = 'PICKUP' THEN 10
      WHEN content.productOrder.deliveryAttributeType = 'QUICK' THEN 11
      ELSE NULL END) AS delivery_type
  , content.productOrder.deliveryFeeAmount AS delivery_fee
  , TRY_STRPTIME(SUBSTR(content.order.orderDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
FROM {{ array }}
WHERE TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL;

-- ProductOrder: insert_order
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- ProductOrder: create_option
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id BIGINT
  , option_id BIGINT
  , channel_seq BIGINT
  , seller_product_code VARCHAR
  , seller_option_code VARCHAR
  , product_type INTEGER
  , product_name VARCHAR
  , option_name VARCHAR
  , sales_price INTEGER
  , option_price INTEGER
  , update_date DATE
  , PRIMARY KEY (channel_seq, option_id)
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
      WHEN content.productOrder.productClass IN ('옵션상품','조합형옵션상품') THEN 1
      WHEN content.productOrder.productClass = '추가구성상품' THEN 2
      ELSE NULL END) AS product_type
  , content.productOrder.productName AS product_name
  , content.productOrder.productOption AS option_name
  , content.productOrder.unitPrice AS sales_price
  , content.productOrder.optionPrice AS option_price
  , TRY_CAST(content.order.paymentDate AS DATE) AS update_date
FROM {{ array }}
WHERE TRY_CAST(content.productOrder.optionCode AS BIGINT) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY content.productOrder.optionCode) = 1;

-- ProductOrder: upsert_option
INSERT INTO {{ table }} {{ values }}
ON CONFLICT DO UPDATE SET
    product_id = COALESCE(excluded.product_id, product_id)
  , seller_product_code = COALESCE(excluded.seller_product_code, seller_product_code)
  , seller_option_code = COALESCE(excluded.seller_option_code, seller_option_code)
  , product_type = COALESCE(excluded.product_type, product_type)
  , product_name = COALESCE(excluded.product_name, product_name)
  , option_name = COALESCE(excluded.option_name, option_name)
  , sales_price = COALESCE(excluded.sales_price, sales_price)
  , option_price = COALESCE(excluded.option_price, option_price)
  , update_date = GREATEST(excluded.update_date, update_date);


-- OrderTime: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_order_no BIGINT
  , order_no BIGINT NOT NULL
  , order_status TINYINT -- OrderStatus: order_status
  , payment_dt TIMESTAMP NOT NULL
  , updated_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (product_order_no, order_status)
);

-- OrderTime: select
SELECT os.*
FROM (
  SELECT
      product_order_no
    , order_no
    , (CASE
        WHEN dt_column = 'dispatch_dt' THEN 2
        WHEN dt_column = 'delivery_dt' THEN 3
        WHEN dt_column = 'decision_dt' THEN 4
        WHEN dt_column = 'exchange_complete_dt' THEN 5
        WHEN dt_column = 'cancel_complete_dt' THEN 6
        WHEN dt_column = 'return_complete_dt' THEN 7
        ELSE NULL END) AS order_status
    , payment_dt
    , updated_dt
  FROM (
    SELECT
        TRY_CAST(productOrderId AS BIGINT) AS product_order_no
      , TRY_CAST(content.order.orderId AS BIGINT) AS order_no
      , TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
      , TRY_STRPTIME(SUBSTR(content.delivery.sendDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS dispatch_dt
      , TRY_STRPTIME(SUBSTR(content.delivery.deliveredDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS delivery_dt
      , TRY_STRPTIME(SUBSTR(content.productOrder.decisionDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS decision_dt
      , (CASE WHEN content.completedClaims[1].claimType = 'EXCHANGE'
          THEN TRY_STRPTIME(SUBSTR(content.completedClaims[1].claimRequestAdmissionDate, 1, 19), '%Y-%m-%dT%H:%M:%S')
        ELSE NULL END) AS exchange_complete_dt
      , (CASE WHEN content.completedClaims[1].claimType = 'CANCEL'
          THEN TRY_STRPTIME(SUBSTR(content.completedClaims[1].claimRequestAdmissionDate, 1, 19), '%Y-%m-%dT%H:%M:%S')
        ELSE NULL END) AS cancel_complete_dt
      , (CASE WHEN content.completedClaims[1].claimType = 'RETURN'
          THEN TRY_STRPTIME(SUBSTR(content.completedClaims[1].claimRequestAdmissionDate, 1, 19), '%Y-%m-%dT%H:%M:%S')
        ELSE NULL END) AS return_complete_dt
    FROM {{ array }}
  ) AS ord
  UNPIVOT (
    updated_dt
    FOR dt_column IN (
        dispatch_dt
      , delivery_dt
      , decision_dt
      , exchange_complete_dt
      , cancel_complete_dt
      , return_complete_dt
    )
  )
) AS os
WHERE (os.payment_dt IS NOT NULL) AND (os.updated_dt IS NOT NULL);

-- OrderTime: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- OrderStatus: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_order_no BIGINT
  , order_no BIGINT NOT NULL
  -- , last_changed_type TINYINT -- OrderStatus: last_changed_type
  , order_status TINYINT -- OrderStatus: order_status
  -- , claim_type TINYINT -- OrderStatus: claim_type
  -- , claim_status TINYINT -- OrderStatus: claim_status
  -- , is_address_changed BOOLEAN
  -- , gift_receiving_status TINYINT -- OrderStatus: gift_receiving_status
  , payment_dt TIMESTAMP NOT NULL
  , updated_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (product_order_no, order_status)
);

-- OrderStatus: last_changed_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'PAY_WAITING' AS code, '결제 대기' AS name)
  , STRUCT(1 AS seq, 'PAYED' AS code, '결제 완료' AS name)
  , STRUCT(2 AS seq, 'EXCHANGE_OPTION' AS code, '옵션 변경 (선물하기)' AS name)
  , STRUCT(3 AS seq, 'DELIVERY_ADDRESS_CHANGED' AS code, '배송지 변경' AS name)
  , STRUCT(4 AS seq, 'GIFT_RECEIVED' AS code, '선물 수락 (선물하기)' AS name)
  , STRUCT(5 AS seq, 'CLAIM_REJECTED' AS code, '클레임 철회' AS name)
  , STRUCT(6 AS seq, 'DISPATCHED' AS code, '발송 처리' AS name)
  , STRUCT(7 AS seq, 'CLAIM_REQUESTED' AS code, '클레임 요청' AS name)
  , STRUCT(8 AS seq, 'COLLECT_DONE' AS code, '수거 완료' AS name)
  , STRUCT(9 AS seq, 'CLAIM_COMPLETED' AS code, '클레임 완료' AS name)
  , STRUCT(10 AS seq, 'PURCHASE_DECIDED' AS code, '구매 확정' AS name)
  , STRUCT(11 AS seq, 'HOPE_DELIVERY_INFO_CHANGED' AS code, '배송 희망일 변경' AS name)
  , STRUCT(12 AS seq, 'CLAIM_REDELIVERING' AS code, '교환 재배송처리' AS name)
]);

-- OrderStatus: order_status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'PAYMENT_WAITING' AS code, '결제 대기' AS name)
  , STRUCT(1 AS seq, 'PAYED' AS code, '결제 완료' AS name)
  , STRUCT(2 AS seq, 'DELIVERING' AS code, '배송 중' AS name)
  , STRUCT(3 AS seq, 'DELIVERED' AS code, '배송 완료' AS name)
  , STRUCT(4 AS seq, 'PURCHASE_DECIDED' AS code, '구매 확정' AS name)
  , STRUCT(5 AS seq, 'EXCHANGED' AS code, '교환' AS name)
  , STRUCT(6 AS seq, 'CANCELED' AS code, '취소' AS name)
  , STRUCT(7 AS seq, 'RETURNED' AS code, '반품' AS name)
  , STRUCT(8 AS seq, 'CANCELED_BY_NOPAYMENT' AS code, '미결제 취소' AS name)
]);

-- OrderStatus: claim_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'CANCEL' AS code, '취소' AS name)
  , STRUCT(1 AS seq, 'RETURN' AS code, '반품' AS name)
  , STRUCT(2 AS seq, 'EXCHANGE' AS code, '교환' AS name)
  , STRUCT(3 AS seq, 'PURCHASE_DECISION_HOLDBACK' AS code, '구매 확정 보류' AS name)
  , STRUCT(4 AS seq, 'ADMIN_CANCEL' AS code, '직권 취소' AS name)
]);

-- OrderStatus: claim_status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'CANCEL_REQUEST' AS code, '취소 요청' AS name)
  , STRUCT(1 AS seq, 'CANCELING' AS code, '취소 처리 중' AS name)
  , STRUCT(2 AS seq, 'CANCEL_DONE' AS code, '취소 처리 완료' AS name)
  , STRUCT(3 AS seq, 'CANCEL_REJECT' AS code, '취소 철회' AS name)
  , STRUCT(4 AS seq, 'RETURN_REQUEST' AS code, '반품 요청' AS name)
  , STRUCT(5 AS seq, 'EXCHANGE_REQUEST' AS code, '교환 요청' AS name)
  , STRUCT(6 AS seq, 'COLLECTING' AS code, '수거 처리 중' AS name)
  , STRUCT(7 AS seq, 'COLLECT_DONE' AS code, '수거 완료' AS name)
  , STRUCT(8 AS seq, 'EXCHANGE_REDELIVERING' AS code, '교환 재배송 중' AS name)
  , STRUCT(9 AS seq, 'RETURN_DONE' AS code, '반품 완료' AS name)
  , STRUCT(10 AS seq, 'EXCHANGE_DONE' AS code, '교환 완료' AS name)
  , STRUCT(11 AS seq, 'RETURN_REJECT' AS code, '반품 철회' AS name)
  , STRUCT(12 AS seq, 'EXCHANGE_REJECT' AS code, '교환 철회' AS name)
  , STRUCT(13 AS seq, 'PURCHASE_DECISION_HOLDBACK' AS code, '구매 확정 보류' AS name)
  , STRUCT(14 AS seq, 'PURCHASE_DECISION_REQUEST' AS code, '구매 확정 요청' AS name)
  , STRUCT(15 AS seq, 'PURCHASE_DECISION_HOLDBACK_RELEASE' AS code, '구매 확정 보류 해제' AS name)
  , STRUCT(16 AS seq, 'ADMIN_CANCELING' AS code, '직권 취소 중' AS name)
  , STRUCT(17 AS seq, 'ADMIN_CANCEL_DONE' AS code, '직권 취소 완료' AS name)
  , STRUCT(18 AS seq, 'ADMIN_CANCEL_REJECT' AS code, '직권 취소 철회' AS name)
]);

-- OrderStatus: gift_receiving_status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'WAIT_FOR_RECEIVING' AS code, '수락 대기(배송지 입력 대기)' AS name)
  , STRUCT(1 AS seq, 'RECEIVED' AS code, '수락 완료' AS name)
]);

-- OrderStatus: select
SELECT os.*
FROM (
  SELECT
      TRY_CAST(productOrderId AS BIGINT) AS product_order_no
    , TRY_CAST(orderId AS BIGINT) AS order_no
    -- , lastChangedType AS last_changed_type
    , (CASE
        WHEN productOrderStatus = 'PAYMENT_WAITING' THEN 0
        WHEN productOrderStatus = 'PAYED' THEN 1
        WHEN productOrderStatus = 'DELIVERING' THEN 2
        WHEN productOrderStatus = 'DELIVERED' THEN 3
        WHEN productOrderStatus = 'PURCHASE_DECIDED' THEN 4
        WHEN productOrderStatus = 'EXCHANGED' THEN 5
        WHEN productOrderStatus = 'CANCELED' THEN 6
        WHEN productOrderStatus = 'RETURNED' THEN 7
        WHEN productOrderStatus = 'CANCELED_BY_NOPAYMENT' THEN 8
        ELSE NULL END
      ) AS order_status
    -- , claimType AS claim_type
    -- , claimStatus AS claim_status
    -- , receiverAddressChanged AS is_address_changed
    -- , giftReceivingStatus AS gift_receiving_status
    , TRY_STRPTIME(SUBSTR(paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
    , TRY_STRPTIME(SUBSTR(lastChangedDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS updated_dt
  FROM {{ array }}
) AS os
WHERE (os.payment_dt IS NOT NULL) AND (os.updated_dt IS NOT NULL) AND (os.order_status > 1);

-- OrderStatus: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
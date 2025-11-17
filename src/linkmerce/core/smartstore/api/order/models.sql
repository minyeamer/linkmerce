-- Order: create_order
CREATE TABLE IF NOT EXISTS {{ table }} (
    order_id BIGINT PRIMARY KEY
  , channel_seq BIGINT NOT NULL
  , orderer_no BIGINT
  , payment_location INTEGER
  , order_dt TIMESTAMP
  , payment_dt TIMESTAMP NOT NULL
);

-- Order: create_product_order
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_order_id BIGINT PRIMARY KEY
  , order_id BIGINT NOT NULL
  , channel_seq BIGINT NOT NULL
  , product_id BIGINT NOT NULL
  , option_id BIGINT NOT NULL
  , product_type TINYINT -- {0: '단일상품', 1: '옵션상품', 2: '추가구성상품'}
  , delivery_type INTEGER
  , delivery_tag_type INTEGER
  , inflow_path VARCHAR
  , inflow_path_add VARCHAR
  , order_quantity INTEGER
  , unit_price INTEGER
  , option_price INTEGER
  -- , product_amount INTEGER
  , discount_amount INTEGER
  , seller_discount_amount INTEGER
  -- , payment_amount INTEGER
  , supply_amount INTEGER
  , delivery_fee INTEGER
  , payment_dt TIMESTAMP NOT NULL
);

-- Order: create_delivery
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_order_id BIGINT PRIMARY KEY
  , order_id BIGINT NOT NULL
  , channel_seq BIGINT NOT NULL
  , invoice_no VARCHAR NOT NULL
  , delivery_company VARCHAR
  , delivery_method INTEGER
  , zip_code VARCHAR
  , latitude VARCHAR
  , longitude VARCHAR
  , pickup_dt TIMESTAMP
  , send_dt TIMESTAMP
  , payment_dt TIMESTAMP NOT NULL
);

-- Order: create_option
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id BIGINT
  , option_id BIGINT PRIMARY KEY
  , channel_seq BIGINT NOT NULL
  , seller_product_code VARCHAR
  , seller_option_code VARCHAR
  , product_type TINYINT -- {0: '단일상품', 1: '옵션상품', 2: '추가구성상품'}
  , product_name VARCHAR
  , option_name VARCHAR
  , sales_price INTEGER
  , option_price INTEGER
  , update_date DATE
);

-- Order: select_order
SELECT
    TRY_CAST(content.order.orderId AS BIGINT) AS order_id
  , TRY_CAST(content.productOrder.merchantChannelId AS BIGINT) AS channel_seq
  , TRY_CAST(content.order.ordererNo AS BIGINT) AS orderer_no
  , (CASE
      WHEN content.order.payLocationType == 'PC' THEN 0
      WHEN content.order.payLocationType == 'MOBILE' THEN 1
      ELSE NULL END) AS payment_location
  , TRY_STRPTIME(SUBSTR(content.order.orderDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
FROM {{ array }}
WHERE TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL;

-- Order: select_product_order
SELECT
    TRY_CAST(productOrderId AS BIGINT) AS product_order_id
  , TRY_CAST(content.order.orderId AS BIGINT) AS order_id
  , TRY_CAST(content.productOrder.merchantChannelId AS BIGINT) AS channel_seq
  , TRY_CAST(content.productOrder.productId AS BIGINT) AS product_id
  , TRY_CAST(content.productOrder.optionCode AS BIGINT) AS option_id
  , (CASE
      WHEN content.productOrder.productClass = '단일상품' THEN 0
      WHEN content.productOrder.productClass IN ('옵션상품','조합형옵션상품') THEN 1
      WHEN content.productOrder.productClass = '추가구성상품' THEN 2
      ELSE NULL END) AS product_type
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
  , (CASE
      WHEN content.productOrder.deliveryTagType = 'TODAY' THEN 0
      WHEN content.productOrder.deliveryTagType = 'TOMORROW' THEN 1
      WHEN content.productOrder.deliveryTagType = 'DAWN' THEN 2
      WHEN content.productOrder.deliveryTagType = 'SUNDAY' THEN 3
      WHEN content.productOrder.deliveryTagType = 'STANDARD' THEN 4
      WHEN content.productOrder.deliveryTagType = 'HOPE' THEN 5
      ELSE NULL END) AS delivery_tag_type
  , content.productOrder.inflowPath AS inflow_path
  , IF(content.productOrder.inflowPathAdd IN ('null','undefined')
    , NULL
    , content.productOrder.inflowPathAdd) AS inflow_path_add
  , content.productOrder.quantity AS order_quantity
  , content.productOrder.unitPrice AS unit_price
  , content.productOrder.optionPrice AS option_price
  -- , content.productOrder.totalProductAmount AS product_amount = unit_price + option_price
  , content.productOrder.productDiscountAmount AS discount_amount
  , content.productOrder.sellerBurdenDiscountAmount AS seller_discount_amount
  -- , content.productOrder.totalPaymentAmount AS payment_amount = product_amount * order_quantity - discount_amount
  , content.productOrder.expectedSettlementAmount AS supply_amount
  , content.productOrder.deliveryFeeAmount AS delivery_fee
  , TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
FROM {{ array }}
WHERE TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL;

-- Order: select_delivery
SELECT
    TRY_CAST(productOrderId AS BIGINT) AS product_order_id
  , TRY_CAST(content.order.orderId AS BIGINT) AS order_id
  , TRY_CAST(content.productOrder.merchantChannelId AS BIGINT) AS channel_seq
  , content.delivery.trackingNumber AS invoice_no
  , content.delivery.deliveryCompany AS delivery_company
  , (CASE
      WHEN content.delivery.deliveryMethod = 'DELIVERY' THEN 0
      WHEN content.delivery.deliveryMethod = 'GDFW_ISSUE_SVC' THEN 1
      WHEN content.delivery.deliveryMethod = 'VISIT_RECEIPT' THEN 2
      WHEN content.delivery.deliveryMethod = 'DIRECT_DELIVERY' THEN 3
      WHEN content.delivery.deliveryMethod = 'QUICK_SVC' THEN 4
      WHEN content.delivery.deliveryMethod = 'NOTHING' THEN 5
      WHEN content.delivery.deliveryMethod = 'RETURN_DESIGNATED' THEN 6
      WHEN content.delivery.deliveryMethod = 'RETURN_DELIVERY' THEN 7
      WHEN content.delivery.deliveryMethod = 'RETURN_INDIVIDUAL' THEN 8
      WHEN content.delivery.deliveryMethod = 'RETURN_MERCHANT' THEN 9
      WHEN content.delivery.deliveryMethod = 'UNKNOWN' THEN 10
      ELSE NULL END) AS delivery_method
  , content.productOrder.shippingAddress.zipCode AS zip_code
  , content.productOrder.shippingAddress.latitude AS latitude
  , content.productOrder.shippingAddress.longitude AS longitude
  , TRY_STRPTIME(SUBSTR(content.delivery.pickupDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS pickup_dt
  , TRY_STRPTIME(SUBSTR(content.delivery.sendDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS send_dt
  , TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
FROM {{ array }}
WHERE (content.delivery.trackingNumber IS NOT NULL)
  AND (TRY_STRPTIME(SUBSTR(content.order.paymentDate, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL);

-- Order: select_option
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

-- Order: insert_order
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Order: insert_product_order
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Order: insert_delivery
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Order: insert_option
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
  , update_date = GREATEST(excluded.update_date, update_date);

-- Order: product_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, '단일상품' AS name)
  , STRUCT(1 AS seq, '옵션상품' AS name)
  , STRUCT(2 AS seq, '추가구성상품' AS name)
]);

-- Order: payment_location
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'PC' AS code, 'PC' AS name)
  , STRUCT(1 AS seq, 'MOBILE' AS code, '모바일' AS name)
]);

-- Order: delivery_type
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

-- Order: delivery_tag_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'TODAY' AS code, '오늘배송' AS name)
  , STRUCT(1 AS seq, 'TOMORROW' AS code, '내일배송' AS name)
  , STRUCT(2 AS seq, 'DAWN' AS code, '새벽배송' AS name)
  , STRUCT(3 AS seq, 'SUNDAY' AS code, '일요배송' AS name)
  , STRUCT(4 AS seq, 'STANDARD' AS code, 'D+2이상배송' AS name)
  , STRUCT(5 AS seq, 'HOPE' AS code, '희망일배송' AS name)
]);

-- Order: delivery_method
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'DELIVERY' AS code, '택배, 등기, 소포' AS name)
  , STRUCT(1 AS seq, 'GDFW_ISSUE_SVC' AS code, '굿스플로 송장 출력' AS name)
  , STRUCT(2 AS seq, 'VISIT_RECEIPT' AS code, '방문 수령' AS name)
  , STRUCT(3 AS seq, 'DIRECT_DELIVERY' AS code, '직접 전달' AS name)
  , STRUCT(4 AS seq, 'QUICK_SVC' AS code, '퀵서비스' AS name)
  , STRUCT(5 AS seq, 'NOTHING' AS code, '배송 없음' AS name)
  , STRUCT(6 AS seq, 'RETURN_DESIGNATED' AS code, '지정 반품 택배' AS name)
  , STRUCT(7 AS seq, 'RETURN_DELIVERY' AS code, '일반 반품 택배' AS name)
  , STRUCT(8 AS seq, 'RETURN_INDIVIDUAL' AS code, '직접 반송' AS name)
  , STRUCT(9 AS seq, 'RETURN_MERCHANT' AS code, '판매자 직접 수거(장보기 전용)' AS name)
  , STRUCT(10 AS seq, 'UNKNOWN' AS code, '알 수 없음(예외 처리에 사용)' AS name)
]);


-- OrderTime: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_order_id BIGINT
  , order_id BIGINT NOT NULL
  , channel_seq BIGINT
  , order_status TINYINT -- OrderStatus: order_status
  , payment_dt TIMESTAMP NOT NULL
  , updated_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (product_order_id, order_status)
);

-- OrderTime: select
SELECT os.*
FROM (
  SELECT
      product_order_id
    , order_id
    , $channel_seq AS channel_seq
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
        TRY_CAST(productOrderId AS BIGINT) AS product_order_id
      , TRY_CAST(content.order.orderId AS BIGINT) AS order_id
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
    product_order_id BIGINT
  , order_id BIGINT NOT NULL
  , channel_seq BIGINT
  -- , last_changed_type TINYINT -- OrderStatus: last_changed_type
  , order_status TINYINT -- OrderStatus: order_status
  -- , claim_type TINYINT -- OrderStatus: claim_type
  -- , claim_status TINYINT -- OrderStatus: claim_status
  -- , is_address_changed BOOLEAN
  -- , gift_receiving_status TINYINT -- OrderStatus: gift_receiving_status
  , payment_dt TIMESTAMP NOT NULL
  , updated_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (product_order_id, order_status)
);

-- OrderStatus: select
SELECT os.*
FROM (
  SELECT
      TRY_CAST(productOrderId AS BIGINT) AS product_order_id
    , TRY_CAST(orderId AS BIGINT) AS order_id
    , $channel_seq AS channel_seq
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
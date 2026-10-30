-- ReturnRequest: create
CREATE TABLE IF NOT EXISTS {{ receipt }} (
    vendor_id VARCHAR NOT NULL
  , receipt_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , receipt_type TINYINT NOT NULL -- {0: 'RETURN', 1: 'CANCEL'}
  , receipt_status TINYINT NOT NULL
  , cancel_reason_category1 VARCHAR
  , cancel_reason_category2 VARCHAR
  , cancel_reason VARCHAR
  , return_delivery_id BIGINT
  , return_delivery_type VARCHAR
  , fault_type VARCHAR
  , confirm_type VARCHAR
  , register_dt TIMESTAMP NOT NULL
  , modify_dt TIMESTAMP
  , confirm_dt TIMESTAMP
  , PRIMARY KEY (vendor_id, receipt_id)
);

CREATE TABLE IF NOT EXISTS {{ item }} (
    vendor_id VARCHAR NOT NULL
  , receipt_id BIGINT NOT NULL
  , shipment_box_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , vendor_inventory_id BIGINT NOT NULL
  , option_id BIGINT NOT NULL
  -- , option_name VARCHAR
  , receipt_type TINYINT NOT NULL -- {0: 'RETURN', 1: 'CANCEL'}
  , receipt_status TINYINT NOT NULL
  , return_delivery_id BIGINT
  , purchase_count INTEGER
  , cancel_count INTEGER
  , register_dt TIMESTAMP NOT NULL
  , modify_dt TIMESTAMP
  , confirm_dt TIMESTAMP
  , PRIMARY KEY (vendor_id, receipt_id, shipment_box_id, option_id)
);

-- ReturnRequest: bulk_insert
INSERT INTO {{ receipt }}
SELECT
    $vendor_id AS vendor_id
  , receiptId AS receipt_id
  , orderId AS order_id
  , (CASE receiptType WHEN 'RETURN' THEN 0 WHEN 'CANCEL' THEN 1 ELSE -1 END) AS receipt_type
  , (CASE receiptStatus
      WHEN 'RELEASE_STOP_UNCHECKED' THEN 0
      WHEN 'RETURNS_UNCHECKED' THEN 1
      WHEN 'VENDOR_WAREHOUSE_CONFIRM' THEN 2
      WHEN 'REQUEST_COUPANG_CHECK' THEN 3
      WHEN 'RETURNS_COMPLETED' THEN 4
      ELSE -1 END) AS receipt_status
  , cancelReasonCategory1 AS cancel_reason_category1
  , cancelReasonCategory2 AS cancel_reason_category2
  , NULLIF(cancelReason, '') AS cancel_reason
  , returnDeliveryId AS return_delivery_id
  , returnDeliveryType AS return_delivery_type
  , faultByType AS fault_type
  , completeConfirmType AS confirm_type
  , TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
  , TRY_STRPTIME(SUBSTR(modifiedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS modify_dt
  , TRY_STRPTIME(SUBSTR(completeConfirmDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS confirm_dt
FROM {{ receipt_rows }}
WHERE TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {{ item }}
SELECT
    $vendor_id AS vendor_id
  , receiptId AS receipt_id
  , item.shipmentBoxId AS shipment_box_id
  , orderId AS order_id
  , item.sellerProductId AS vendor_inventory_id
  , item.vendorItemId AS option_id
  -- , item.vendorItemPackageName AS option_name
  , (CASE receiptType WHEN 'RETURN' THEN 0 WHEN 'CANCEL' THEN 1 ELSE -1 END) AS receipt_type
  , (CASE receiptStatus
      WHEN 'RELEASE_STOP_UNCHECKED' THEN 0
      WHEN 'RETURNS_UNCHECKED' THEN 1
      WHEN 'VENDOR_WAREHOUSE_CONFIRM' THEN 2
      WHEN 'REQUEST_COUPANG_CHECK' THEN 3
      WHEN 'RETURNS_COMPLETED' THEN 4
      ELSE -1 END) AS receipt_status
  , returnDeliveryId AS return_delivery_id
  , item.purchaseCount AS purchase_count
  , item.cancelCount AS cancel_count
  , TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
  , TRY_STRPTIME(SUBSTR(modifiedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS modify_dt
  , TRY_STRPTIME(SUBSTR(completeConfirmDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS confirm_dt
FROM {{ receipt_item_rows }}
WHERE TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL
ON CONFLICT DO NOTHING;

-- ReturnRequest: receipt_status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'RELEASE_STOP_UNCHECKED' AS code, '출고중지요청' AS name)
  , STRUCT(1 AS seq, 'RETURNS_UNCHECKED' AS code, '반품접수' AS name)
  , STRUCT(2 AS seq, 'VENDOR_WAREHOUSE_CONFIRM' AS code, '입고완료' AS name)
  , STRUCT(3 AS seq, 'REQUEST_COUPANG_CHECK' AS code, '쿠팡확인요청' AS name)
  , STRUCT(4 AS seq, 'RETURNS_COMPLETED' AS code, '반품완료' AS name)
]);


-- ReturnWithdraw: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_id VARCHAR NOT NULL
  , receipt_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , option_id BIGINT NOT NULL
  , refund_duty TINYINT -- {0: 'COMPANY', 1: 'CUSTOMER', 2: 'COUPANG'}
  , cancel_dt TIMESTAMP NOT NULL
  , PRIMARY KEY (vendor_id, receipt_id, option_id)
);

-- ReturnWithdraw: bulk_insert
INSERT INTO {{ table }}
SELECT
    vendorId AS vendor_id
  , cancelId AS receipt_id
  , orderId AS order_id
  , vendorItemId AS option_id
  , (CASE refundDeliveryDuty WHEN 'COM' THEN 0 WHEN 'CUS' THEN 1 WHEN 'COU' THEN 2 ELSE NULL END) AS refund_duty
  , TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS cancel_dt
FROM {{ rows }}
WHERE TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL
ON CONFLICT DO NOTHING;


-- ExchangeRequest: create
CREATE TABLE IF NOT EXISTS {{ exchange }} (
    vendor_id VARCHAR NOT NULL
  , exchange_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , exchange_status TINYINT NOT NULL
  , reason_code VARCHAR
  , reason_code_text VARCHAR
  , reason_detail VARCHAR
  , cancel_reason VARCHAR
  , refer_type VARCHAR
  , fault_type VARCHAR
  , collect_status VARCHAR
  , delivery_status VARCHAR
  , register_dt TIMESTAMP NOT NULL
  , modify_dt TIMESTAMP
  , PRIMARY KEY (vendor_id, exchange_id)
);

CREATE TABLE IF NOT EXISTS {{ item }} (
    vendor_id VARCHAR NOT NULL
  , exchange_id BIGINT NOT NULL
  , shipment_box_id BIGINT NOT NULL
  , order_id BIGINT NOT NULL
  , order_option_id BIGINT NOT NULL
  , target_option_id BIGINT NOT NULL
  , exchange_status TINYINT NOT NULL
  , exchange_count INTEGER
  , order_option_price INTEGER
  , target_option_price INTEGER
  , register_dt TIMESTAMP NOT NULL
  , modify_dt TIMESTAMP
  , PRIMARY KEY (vendor_id, exchange_id, shipment_box_id, order_option_id)
);

-- ExchangeRequest: bulk_insert
INSERT INTO {{ exchange }}
SELECT
    vendorId AS vendor_id
  , exchangeId AS exchange_id
  , orderId AS order_id
  , (CASE exchangeStatus
      WHEN 'RECEIPT' THEN 0
      WHEN 'PROGRESS' THEN 1
      WHEN 'SUCCESS' THEN 2
      WHEN 'REJECT' THEN 3
      WHEN 'CANCEL' THEN 4
      ELSE -1 END) AS exchange_status
  , reasonCode AS reason_code
  , reasonCodeText AS reason_code_text
  , NULLIF(reasonEtcDetail, '') AS reason_detail
  , NULLIF(cancelReason, '') AS cancel_reason
  , referType AS refer_type
  , faultType AS fault_type
  , collectStatus AS collect_status
  , deliveryStatus AS delivery_status
  , TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
  , TRY_STRPTIME(SUBSTR(modifiedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS modify_dt
FROM {{ exchange_rows }}
WHERE TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {{ item }}
SELECT
    vendorId AS vendor_id
  , exchangeId AS exchange_id
  , item.originalShipmentBoxId AS shipment_box_id
  , orderId AS order_id
  , item.orderItemId AS order_option_id
  , item.targetItemId AS target_option_id
  , (CASE exchangeStatus
      WHEN 'RECEIPT' THEN 0
      WHEN 'PROGRESS' THEN 1
      WHEN 'SUCCESS' THEN 2
      WHEN 'REJECT' THEN 3
      WHEN 'CANCEL' THEN 4
      ELSE -1 END) AS exchange_status
  , item.quantity AS exchange_count
  , item.orderItemUnitPrice AS order_option_price
  , item.targetItemUnitPrice AS order_option_price
  , TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
  , TRY_STRPTIME(SUBSTR(modifiedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS modify_dt
FROM {{ exchange_item_rows }}
WHERE TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL
ON CONFLICT DO NOTHING;

-- ExchangeRequest: exchange_status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'RECEIPT' AS code, '접수' AS name)
  , STRUCT(1 AS seq, 'PROGRESS' AS code, '진행' AS name)
  , STRUCT(2 AS seq, 'SUCCESS' AS code, '완료' AS name)
  , STRUCT(3 AS seq, 'REJECT' AS code, '불가' AS name)
  , STRUCT(4 AS seq, 'CANCEL' AS code, '철회' AS name)
]);
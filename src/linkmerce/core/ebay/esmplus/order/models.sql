-- Order: create
CREATE TABLE IF NOT EXISTS {{ order }} (
    order_no BIGINT NOT NULL
  , site_order_no BIGINT NOT NULL
  , site_item_id VARCHAR NOT NULL
  , site_type TINYINT NOT NULL -- {1: 'auction', 2: 'gmarket'}
  , seller_id VARCHAR
  , customer_no BIGINT
  , order_status VARCHAR
  , order_status_code INTEGER
  , claim_type VARCHAR
  , order_quantity INTEGER
  , payment_amount INTEGER
  , payment_dt TIMESTAMP NOT NULL
  , delivered_dt TIMESTAMP
  , decision_dt TIMESTAMP
  , claim_dt TIMESTAMP
  , settlement_date DATE
  , PRIMARY KEY (site_order_no)
);

CREATE TABLE IF NOT EXISTS {{ item }} (
    site_item_id VARCHAR NOT NULL
  , site_type TINYINT NOT NULL -- {1: 'auction', 2: 'gmarket'}
  , item_name VARCHAR
  , seller_id VARCHAR
  , image_url VARCHAR
  , first_payment_dt TIMESTAMP
  , last_payment_dt TIMESTAMP
  , PRIMARY KEY (site_item_id)
);

-- Order: bulk_insert
INSERT INTO {{ order }}
SELECT
    orderNo AS order_no
  , siteOrderNo AS site_order_no
  , goodsNo AS site_item_id
  , siteId AS site_type
  , sellerId AS seller_id
  , TRY_CAST(buyerCustNo AS BIGINT) AS customer_no
  , deliveryStatus AS order_status
  , deliveryStatusCode AS order_status_code
  , claimType AS claim_type
  , orderQty AS order_quantity
  , CAST(tradeAmnt AS INTEGER) AS payment_amount
  , TRY_STRPTIME(SUBSTR(depositConfirmDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
  , TRY_STRPTIME(SUBSTR(deliveryFinishDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS delivered_dt
  , TRY_STRPTIME(SUBSTR(buyDecisionDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS decision_dt
  , TRY_STRPTIME(SUBSTR(refundApprDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS claim_dt
  , TRY_STRPTIME(SUBSTR(sttlDate, 1, 10), '%Y-%m-%d') AS settlement_date
FROM {{ rows }}
WHERE TRY_STRPTIME(SUBSTR(depositConfirmDate, 1, 19), '%Y-%m-%dT%H:%M:%S') IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {{ item }}
SELECT
    goodsNo AS site_item_id
  , siteId AS site_type
  , goodsName AS item_name
  , sellerId AS seller_id
  , imgFileName AS image_url
  , MIN(TRY_STRPTIME(SUBSTR(depositConfirmDate, 1, 19), '%Y-%m-%dT%H:%M:%S')) OVER (PARTITION BY goodsNo) AS first_payment_dt
  , MAX(TRY_STRPTIME(SUBSTR(depositConfirmDate, 1, 19), '%Y-%m-%dT%H:%M:%S')) OVER (PARTITION BY goodsNo) AS last_payment_dt
FROM {{ rows }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY goodsNo, siteId ORDER BY depositConfirmDate DESC) = 1
ON CONFLICT DO UPDATE SET
    item_name = COALESCE(EXCLUDED.item_name, item_name)
  , seller_id = COALESCE(EXCLUDED.seller_id, seller_id)
  , image_url = COALESCE(EXCLUDED.image_url, image_url)
  , first_payment_dt = LEAST(EXCLUDED.first_payment_dt, first_payment_dt)
  , last_payment_dt = GREATEST(EXCLUDED.last_payment_dt, last_payment_dt);
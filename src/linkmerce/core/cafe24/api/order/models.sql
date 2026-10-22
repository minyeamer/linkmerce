-- Order: create
CREATE TABLE IF NOT EXISTS {{ order }} (
    mall_id VARCHAR NOT NULL
  , order_id VARCHAR NOT NULL
  , member_id VARCHAR
  , member_name VARCHAR
  , payment_method VARCHAR
  , order_place VARCHAR
  , first_order BOOLEAN
  , order_dt TIMESTAMP
  , payment_dt TIMESTAMP
  , PRIMARY KEY (mall_id, order_id)
);

CREATE TABLE IF NOT EXISTS {{ delivery }} (
    mall_id VARCHAR NOT NULL
  , order_item_code VARCHAR NOT NULL
  , order_id VARCHAR NOT NULL
  , shipping_code VARCHAR
  , invoice_no VARCHAR
  , zip_code VARCHAR
  , order_dt TIMESTAMP
  , shipped_dt TIMESTAMP
  -- , delivered_dt TIMESTAMP
  , PRIMARY KEY (mall_id, order_item_code)
);

CREATE TABLE IF NOT EXISTS {{ detail }} (
    mall_id VARCHAR NOT NULL
  , order_item_code VARCHAR NOT NULL
  , order_id VARCHAR NOT NULL
  , product_no BIGINT NOT NULL
  , variant_code VARCHAR NOT NULL
  , order_quantity INTEGER
  , product_price INTEGER
  , option_price INTEGER
  , additional_discount_price INTEGER
  , coupon_discount_price INTEGER
  , app_item_discount_amount INTEGER
  , shipping_fee INTEGER
  , supply_price INTEGER
  , order_dt TIMESTAMP
  , PRIMARY KEY (mall_id, order_item_code)
);

CREATE TABLE IF NOT EXISTS {{ option }} (
    mall_id VARCHAR NOT NULL
  , product_no BIGINT NOT NULL
  , variant_code VARCHAR NOT NULL
  , custom_product_code VARCHAR
  , product_name VARCHAR
  , option_name VARCHAR
  , option_type TINYINT -- {0: '조합형', 1: '연동형', 2: '독립형'}
  , product_price INTEGER
  , option_price INTEGER
  , first_order_dt TIMESTAMP
  , last_order_dt TIMESTAMP
  , PRIMARY KEY (mall_id, variant_code)
);

-- Order: bulk_insert
INSERT INTO {{ order }}
SELECT
    $mall_id AS mall_id
  , order_id
  , NULLIF(member_id, '') AS member_id
  , NULLIF(billing_name, '') AS member_name
  , array_to_string(payment_method, ',') AS payment_method
  , order_place_name AS order_place
  , (CASE first_order WHEN 'T' THEN TRUE WHEN 'F' THEN FALSE ELSE NULL END) AS first_order
  , TRY_STRPTIME(SUBSTR(order_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , TRY_STRPTIME(SUBSTR(payment_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
FROM {{ order_rows }}
ON CONFLICT DO NOTHING;

INSERT INTO {{ delivery }}
SELECT
    $mall_id AS mall_id
  , item.order_item_code
  , order_id
  , item.shipping_code
  , item.tracking_no AS invoice_no
  , receivers."0".zipcode AS zip_code
  , TRY_STRPTIME(SUBSTR(order_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , TRY_STRPTIME(SUBSTR(item.shipped_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS shipped_dt
  -- , TRY_STRPTIME(SUBSTR(item.delivered_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS delivered_dt
FROM {{ order_item_rows }}
ON CONFLICT DO NOTHING;

INSERT INTO {{ detail }}
SELECT
    $mall_id AS mall_id
  , item.order_item_code AS order_item_code
  , order_id
  , item.product_no
  , item.variant_code
  , item.quantity AS order_quantity
  , CAST(split_part(item.product_price, '.', 1) AS INTEGER) AS product_price
  , CAST(split_part(item.option_price, '.', 1) AS INTEGER) AS option_price
  , CAST(split_part(item.additional_discount_price, '.', 1) AS INTEGER) AS additional_discount_price
  , CAST(split_part(item.coupon_discount_price, '.', 1) AS INTEGER) AS coupon_discount_price
  , CAST(split_part(item.app_item_discount_amount, '.', 1) AS INTEGER) AS app_item_discount_amount
  , CAST(split_part(item.individual_shipping_fee, '.', 1) AS INTEGER) AS shipping_fee
  , CAST(split_part(item.supply_price, '.', 1) AS INTEGER) AS supply_price
  , TRY_STRPTIME(SUBSTR(order_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  -- , TRY_STRPTIME(SUBSTR(item.purchaseconfirmation_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS confimed_dt
FROM {{ order_item_rows }}
ON CONFLICT DO NOTHING;

INSERT INTO {{ option }}
SELECT
    $mall_id AS mall_id
  , item.product_no
  , item.variant_code
  , NULLIF(item.custom_product_code, '') AS custom_product_code
  , item.product_name
  , item.option_value
  , (CASE item.option_type WHEN 'T' THEN 0 WHEN 'E' THEN 1 WHEN 'F' THEN 2 ELSE NULL END) AS option_type
  , CAST(split_part(item.product_price, '.', 1) AS INTEGER) AS product_price
  , CAST(split_part(item.option_price, '.', 1) AS INTEGER) AS option_price
  , TRY_STRPTIME(SUBSTR(order_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS first_order_dt
  , TRY_STRPTIME(SUBSTR(order_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS last_order_dt
FROM {{ order_item_rows }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY item.variant_code ORDER BY order_date DESC) = 1
ON CONFLICT DO UPDATE SET
    product_no = COALESCE(EXCLUDED.product_no, product_no)
  , custom_product_code = COALESCE(EXCLUDED.custom_product_code, custom_product_code)
  , product_name = COALESCE(EXCLUDED.product_name, product_name)
  , option_name = COALESCE(EXCLUDED.option_name, option_name)
  , option_type = COALESCE(EXCLUDED.option_type, option_type)
  , product_price = COALESCE(EXCLUDED.product_price, product_price)
  , option_price = COALESCE(EXCLUDED.option_price, option_price)
  , first_order_dt = LEAST(EXCLUDED.first_order_dt, first_order_dt)
  , last_order_dt = GREATEST(EXCLUDED.last_order_dt, last_order_dt);


-- OrderTime: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_id VARCHAR NOT NULL
  , order_item_code VARCHAR NOT NULL
  , order_id VARCHAR NOT NULL
  , order_dt TIMESTAMP
  , payment_dt TIMESTAMP
  , shipped_dt TIMESTAMP
  , delivered_dt TIMESTAMP
  , confimed_dt TIMESTAMP
  , cancel_dt TIMESTAMP
  , refund_dt TIMESTAMP
  , exchange_dt TIMESTAMP
  , PRIMARY KEY (mall_id, order_item_code)
);

-- OrderTime: bulk_insert
INSERT INTO {{ table }}
SELECT
    $mall_id AS mall_id
  , item.order_item_code
  , order_id
  , TRY_STRPTIME(SUBSTR(order_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS order_dt
  , TRY_STRPTIME(SUBSTR(payment_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS payment_dt
  , TRY_STRPTIME(SUBSTR(item.shipped_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS shipped_dt
  , TRY_STRPTIME(SUBSTR(item.delivered_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS delivered_dt
  , TRY_STRPTIME(SUBSTR(item.purchaseconfirmation_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS confimed_dt
  , TRY_STRPTIME(SUBSTR(item.cancel_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS cancel_dt
  , TRY_STRPTIME(SUBSTR(item.refund_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS refund_dt
  , TRY_STRPTIME(SUBSTR(item.exchange_date, 1, 19), '%Y-%m-%dT%H:%M:%S') AS exchange_dt
FROM {{ rows }}
ON CONFLICT DO UPDATE SET
    order_id = EXCLUDED.order_id
  , order_dt = COALESCE(EXCLUDED.order_dt, order_dt)
  , payment_dt = COALESCE(EXCLUDED.payment_dt, payment_dt)
  , shipped_dt = COALESCE(EXCLUDED.shipped_dt, shipped_dt)
  , delivered_dt = COALESCE(EXCLUDED.delivered_dt, delivered_dt)
  , confimed_dt = COALESCE(EXCLUDED.confimed_dt, confimed_dt)
  , cancel_dt = COALESCE(EXCLUDED.cancel_dt, cancel_dt)
  , refund_dt = COALESCE(EXCLUDED.refund_dt, refund_dt)
  , exchange_dt = COALESCE(EXCLUDED.exchange_dt, exchange_dt);


-- OrderChange: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_id VARCHAR NOT NULL
  , order_item_code VARCHAR NOT NULL
  , order_id VARCHAR NOT NULL
  , order_status VARCHAR NOT NULL
  , order_status_type TINYINT -- {0: '정상', 1: '취소', 2: '교환', 3: '반품'}
  -- 주문번호의 '주문일자'는 주문서 생성전 우선적으로 발급된다.
  -- 주문서의 생성 및 결제처리가 완료되기까지의 시간이 소요되기 때문에, 주문완료 시간과 차이가 있다.
  -- https://support.cafe24.com/hc/ko/articles/18599762628249-주문번호는-어떤-기준으로-생성되나요
  , order_date DATE NOT NULL
  , search_start_dt TIMESTAMP
  , search_end_dt TIMESTAMP
  , PRIMARY KEY (mall_id, order_item_code, order_status)
);

-- OrderChange: bulk_insert
INSERT INTO {{ table }}
SELECT
    $mall_id AS mall_id
  , order_item_code
  , order_id
  , order_status
  , (CASE LEFT(order_status, 1)
    WHEN 'N' THEN 0
    WHEN 'C' THEN 1
    WHEN 'E' THEN 2
    WHEN 'R' THEN 3
    ELSE NULL END) AS order_status_type
  , CAST(STRPTIME(split_part(order_id, '-', 1), '%Y%m%d') AS DATE) AS order_date
  , $start_datetime AS search_start_dt
  , $end_datetime AS search_end_dt
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- OrderChange: order_status_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'N' AS code, '정상' AS name)
  , STRUCT(1 AS seq, 'C' AS code, '취소' AS name)
  , STRUCT(2 AS seq, 'E' AS code, '교환' AS name)
  , STRUCT(3 AS seq, 'R' AS code, '반품' AS name)
]);

-- OrderChange: order_status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'N00' AS code, '입금전' AS name)
  , STRUCT(1 AS seq, 'N01' AS code, '교환접수 - 교환상품' AS name)
  , STRUCT(2 AS seq, 'N02' AS code, '입금전 - 카드결제대기' AS name)
  , STRUCT(3 AS seq, 'N03' AS code, '교환접수 - 카드결제대기' AS name)
  , STRUCT(10 AS seq, 'N10' AS code, '상품준비중' AS name)
  , STRUCT(20 AS seq, 'N20' AS code, '배송준비중' AS name)
  , STRUCT(21 AS seq, 'N21' AS code, '배송대기' AS name)
  , STRUCT(22 AS seq, 'N22' AS code, '배송보류' AS name)
  , STRUCT(30 AS seq, 'N30' AS code, '배송중' AS name)
  , STRUCT(40 AS seq, 'N40' AS code, '배송완료' AS name)
  , STRUCT(50 AS seq, 'N50' AS code, '구매확정' AS name)
  , STRUCT(100 AS seq, 'C00' AS code, '취소신청' AS name)
  , STRUCT(110 AS seq, 'C10' AS code, '취소접수 - 관리자' AS name)
  , STRUCT(111 AS seq, 'C11' AS code, '취소접수거부 - 관리자' AS name)
  , STRUCT(134 AS seq, 'C34' AS code, '취소처리중 - 환불전' AS name)
  , STRUCT(135 AS seq, 'C35' AS code, '취소처리중 - 환불완료' AS name)
  , STRUCT(136 AS seq, 'C36' AS code, '취소처리중 - 환불보류' AS name)
  , STRUCT(140 AS seq, 'C40' AS code, '취소완료' AS name)
  , STRUCT(141 AS seq, 'C41' AS code, '취소 완료 - 환불전' AS name)
  , STRUCT(142 AS seq, 'C42' AS code, '취소 완료 - 환불요청중' AS name)
  , STRUCT(143 AS seq, 'C43' AS code, '취소 완료 - 환불보류' AS name)
  , STRUCT(147 AS seq, 'C47' AS code, '입금전취소 - 구매자' AS name)
  , STRUCT(148 AS seq, 'C48' AS code, '입금전취소 - 자동취소' AS name)
  , STRUCT(149 AS seq, 'C49' AS code, '입금전취소 - 관리자' AS name)
  , STRUCT(200 AS seq, 'E00' AS code, '교환신청' AS name)
  , STRUCT(210 AS seq, 'E10' AS code, '교환접수' AS name)
  , STRUCT(211 AS seq, 'E11' AS code, '교환접수거부' AS name)
  , STRUCT(212 AS seq, 'E12' AS code, '교환보류' AS name)
  , STRUCT(213 AS seq, 'E13' AS code, '교환접수 - 수거완료(자동)' AS name)
  , STRUCT(220 AS seq, 'E20' AS code, '교환준비' AS name)
  , STRUCT(230 AS seq, 'E30' AS code, '교환처리중 - 수거전' AS name)
  , STRUCT(231 AS seq, 'E31' AS code, '교환처리중 - 수거완료' AS name)
  , STRUCT(232 AS seq, 'E32' AS code, '교환처리중 - 입금전' AS name)
  , STRUCT(233 AS seq, 'E33' AS code, '교환처리중 - 입금완료' AS name)
  , STRUCT(234 AS seq, 'E34' AS code, '교환처리중 - 환불전' AS name)
  , STRUCT(235 AS seq, 'E35' AS code, '교환처리중 - 환불완료' AS name)
  , STRUCT(236 AS seq, 'E36' AS code, '교환처리중 - 환불보류' AS name)
  , STRUCT(240 AS seq, 'E40' AS code, '교환완료' AS name)
  , STRUCT(300 AS seq, 'R00' AS code, '반품신청' AS name)
  , STRUCT(310 AS seq, 'R10' AS code, '반품접수' AS name)
  , STRUCT(311 AS seq, 'R11' AS code, '반품 접수 거부' AS name)
  , STRUCT(312 AS seq, 'R12' AS code, '반품보류' AS name)
  , STRUCT(313 AS seq, 'R13' AS code, '반품접수 - 수거완료(자동)' AS name)
  , STRUCT(320 AS seq, 'R20' AS code, '반품 수거 완료' AS name)
  , STRUCT(330 AS seq, 'R30' AS code, '반품처리중 - 수거전' AS name)
  , STRUCT(331 AS seq, 'R31' AS code, '반품처리중 - 수거완료' AS name)
  , STRUCT(334 AS seq, 'R34' AS code, '반품처리중 - 환불전' AS name)
  , STRUCT(335 AS seq, 'R35' AS code, '반품처리중 - 환불완료' AS name)
  , STRUCT(336 AS seq, 'R36' AS code, '반품처리중 - 환불보류' AS name)
  , STRUCT(340 AS seq, 'R40' AS code, '반품완료 - 환불완료' AS name)
  , STRUCT(341 AS seq, 'R41' AS code, '반품완료 - 환불전' AS name)
  , STRUCT(342 AS seq, 'R42' AS code, '반품완료 - 환불요청중' AS name)
  , STRUCT(343 AS seq, 'R43' AS code, '반품완료 - 환불보류' AS name)
]);

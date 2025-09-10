-- Order: create
CREATE OR REPLACE TABLE {{ table }} (
    order_seq BIGINT PRIMARY KEY
  , order_seq_org BIGINT
  , order_no VARCHAR
  , order_status_div INTEGER
  , order_status INTEGER
  , shop_id VARCHAR
  , shop_name VARCHAR
  , login_id VARCHAR
  , account_no INTEGER
  , product_no VARCHAR
  , sku_no VARCHAR
  , product_code VARCHAR
  , product_status VARCHAR
  , product_name VARCHAR
  , product_name_decided VARCHAR
  , product_name_abbr VARCHAR
  , option_name VARCHAR
  , option_name_decided VARCHAR
  , model_name VARCHAR
  , order_quantity INTEGER
  , sku_quantity INTEGER
  , order_amount INTEGER
  , supply_amount INTEGER
  , cost_amount INTEGER
  , register_dt TIMESTAMP
  , ship_hope_date DATE
);

-- Order: select
SELECT
    ordNo AS order_seq
  , NULLIF(orgnOrdNo, 0) AS order_seq_org
  , shmaOrdNo AS order_no
  , TRY_CAST(ordStsTpDivCd AS INTEGER) AS order_status_div
  , TRY_CAST(ordStsCd AS INTEGER) AS order_status
  , shmaId AS shop_id
  , shmaNm AS shop_name
  , shmaCnctnLoginId AS login_id
  , acntRegsSrno AS account_no
  , prdNo AS product_no
  , skuNo AS sku_no
  , onsfPrdCd AS product_code
  , prdSplyStsNm AS product_status
  , clctPrdNm AS product_name
  , dcdPrdNm AS product_name_decided
  , prdAbbrRmrk AS product_name_abbr
  , clctSkuNm AS option_name
  , dcdSkuNm AS option_name_decided
  , modlNm AS model_name
  , ordQt AS order_quantity
  , skuQt AS sku_quantity
  , ordSumAmt AS order_amount
  , shmaSplyUprc AS supply_amount
  , cprcSumAmt AS cost_amount
  , TRY_CAST(fstRegsDt AS TIMESTAMP) AS register_dt
  , TRY_CAST(shpmtHopeYmd AS DATE) AS ship_hope_date
FROM {{ array }}
WHERE ordNo IS NOT NULL;

-- Order: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- OrderDownload: create
CREATE OR REPLACE TABLE {{ table }} (
    order_seq BIGINT PRIMARY KEY
  , order_seq_org BIGINT
  , order_no VARCHAR
  , order_no_dup VARCHAR
  , account_no INTEGER
  , product_id VARCHAR
  , product_id_shop VARCHAR
  , order_status_div INTEGER
  , order_status INTEGER
  , order_quantity INTEGER
  , sku_quantity INTEGER
  , payment_amount INTEGER
  , order_amount INTEGER
  , order_dt TIMESTAMP
);

-- OrderDownload: select
SELECT
    TRY_CAST("주문번호(사방넷)" AS BIGINT) AS order_seq
  , NULLIF(TRY_CAST("원주문번호(사방넷)" AS BIGINT), 0) AS order_seq_org
  , "주문번호(쇼핑몰)" AS order_no
  , "부주문번호" AS order_no_dup
  , TRY_CAST("계정등록순번" AS INTEGER) AS account_no
  , "상품코드(사방넷)" AS product_id
  , "상품코드(쇼핑몰)" AS product_id_shop
  , (CASE
      WHEN "주문구분" = '주문(진행)' THEN 1
      WHEN "주문구분" = '주문(완료)' THEN 2
      WHEN "주문구분" = '교발(진행)' THEN 3
      WHEN "주문구분" = '교발(완료)' THEN 4
      WHEN "주문구분" = '회수(진행)' THEN 5
      WHEN "주문구분" = '회수(완료)' THEN 6
      ELSE NULL END
    ) AS order_status_div
  , (CASE
      WHEN "주문상태" = '신규주문' THEN 1
      WHEN "주문상태" = '주문확인' THEN 2
      WHEN "주문상태" = '출고대기' THEN 3
      WHEN "주문상태" = '출고완료' THEN 4
      WHEN "주문상태" = '배송보류' THEN 6
      WHEN "주문상태" = '취소접수' THEN 7
      WHEN "주문상태" = '교환접수' THEN 8
      WHEN "주문상태" = '반품접수' THEN 9
      WHEN "주문상태" = '취소완료' THEN 10
      WHEN "주문상태" = '교환완료' THEN 11
      WHEN "주문상태" = '반품완료' THEN 12
      WHEN "주문상태" = '교환발송준비' THEN 21
      WHEN "주문상태" = '교환발송완료' THEN 22
      WHEN "주문상태" = '교환회수준비' THEN 23
      WHEN "주문상태" = '교환회수완료' THEN 24
      WHEN "주문상태" = '반품회수준비' THEN 25
      WHEN "주문상태" = '반품회수완료' THEN 26
      WHEN "주문상태" = '폐기' THEN 999
      ELSE NULL END
    ) AS order_status
  , TRY_CAST("수량" AS INTEGER) AS order_quantity
  , TRY_CAST("EA(확정)" AS INTEGER) AS sku_quantity
  , TRY_CAST("결제금액" AS INTEGER) AS payment_amount
  , TRY_CAST("주문금액" AS INTEGER) AS order_amount
  , TRY_CAST("주문일시(YYYY-MM-DD HH:MM)" AS TIMESTAMP) AS order_dt
FROM {{ array }}
WHERE TRY_CAST("주문번호(사방넷)" AS BIGINT) IS NOT NULL;

-- OrderDownload: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- OrderStatus: create
CREATE OR REPLACE TABLE {{ table }} (
    order_seq BIGINT PRIMARY KEY
  , order_status INTEGER
  , order_dt TIMESTAMP
  , update_date DATE
);

-- OrderStatus: select
SELECT
    TRY_CAST("주문번호(사방넷)" AS BIGINT) AS order_seq
  , (CASE
      WHEN '{{ date_type }}' = '출고완료일' THEN 4
      WHEN '{{ date_type }}' = '취소접수일' THEN 7
      WHEN '{{ date_type }}' = '교환접수일' THEN 8
      WHEN '{{ date_type }}' = '반품접수일' THEN 9
      WHEN '{{ date_type }}' = '취소완료일' THEN 10
      WHEN '{{ date_type }}' = '교환완료일' THEN 11
      WHEN '{{ date_type }}' = '반품완료일' THEN 12
      ELSE NULL END
    ) AS order_status
  , TRY_CAST("주문일시(YYYY-MM-DD HH:MM)" AS TIMESTAMP) AS order_dt
  , TRY_CAST(STRPTIME("{{ date_type }}자({{ date_format }})", '{{ time_format }}') AS DATE) AS update_date
FROM {{ array }}
WHERE (TRY_CAST("주문번호(사방넷)" AS BIGINT) IS NOT NULL)
  AND TRY_CAST(STRPTIME("{{ date_type }}자({{ date_format }})", '{{ time_format }}') AS DATE) IS NOT NULL;

-- OrderStatus: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
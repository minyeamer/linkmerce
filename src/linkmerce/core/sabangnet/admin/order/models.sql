-- Order: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    order_seq BIGINT PRIMARY KEY
  , order_seq_org BIGINT
  , order_id VARCHAR
  , order_status_div INTEGER
  , order_status INTEGER
  , shop_id VARCHAR
  , shop_name VARCHAR
  , login_id VARCHAR
  , account_no INTEGER
  , option_id VARCHAR
  , sku_id VARCHAR
  , product_code VARCHAR
  , product_status VARCHAR
  , product_name VARCHAR
  , product_name_decided VARCHAR
  , product_name_abbr VARCHAR
  , option_name VARCHAR
  , option_name_decided VARCHAR
  , model_name VARCHAR
  , invoice_no VARCHAR
  , delivery_company VARCHAR
  , order_quantity INTEGER
  , sku_quantity INTEGER
  , order_amount INTEGER
  , supply_amount INTEGER
  , cost_amount INTEGER
  , register_dt TIMESTAMP
  , ship_hope_date DATE
  , invoice_date DATE
);

-- Order: select
SELECT
    ordNo AS order_seq
  , NULLIF(orgnOrdNo, 0) AS order_seq_org
  , shmaOrdNo AS order_id
  , TRY_CAST(ordStsTpDivCd AS INTEGER) AS order_status_div
  , TRY_CAST(ordStsCd AS INTEGER) AS order_status
  , shmaId AS shop_id
  , shmaNm AS shop_name
  , shmaCnctnLoginId AS login_id
  , acntRegsSrno AS account_no
  , prdNo AS option_id
  , skuNo AS sku_id
  , onsfPrdCd AS product_code
  , prdSplyStsNm AS product_status
  , clctPrdNm AS product_name
  , dcdPrdNm AS product_name_decided
  , prdAbbrRmrk AS product_name_abbr
  , clctSkuNm AS option_name
  , dcdSkuNm AS option_name_decided
  , modlNm AS model_name
  , wyblNo AS invoice_no
  , pcscpNm AS delivery_company
  , ordQt AS order_quantity
  , skuQt AS sku_quantity
  , ordSumAmt AS order_amount
  , shmaSplyUprc AS supply_amount
  , cprcSumAmt AS cost_amount
  , TRY_CAST(fstRegsDt AS TIMESTAMP) AS register_dt
  , TRY_CAST(shpmtHopeYmd AS DATE) AS ship_hope_date
  , TRY_CAST(wyblTrnmDt AS DATE) AS invoice_date
FROM {{ array }}
WHERE ordNo IS NOT NULL;

-- Order: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- OrderDownload: create_order
CREATE TABLE IF NOT EXISTS {{ table }} (
    order_seq BIGINT PRIMARY KEY
  , order_seq_org BIGINT
  , order_id VARCHAR
  , order_id_dup VARCHAR
  , account_no INTEGER
  , option_id VARCHAR
  , product_id_shop VARCHAR
  , order_quantity INTEGER
  , sku_quantity INTEGER
  , payment_amount INTEGER
  , order_amount INTEGER
  , order_dt TIMESTAMP NOT NULL
  , register_dt TIMESTAMP NOT NULL
);

-- OrderDownload: select_order
SELECT
    TRY_CAST("주문번호(사방넷)" AS BIGINT) AS order_seq
  , NULLIF(TRY_CAST("원주문번호(사방넷)" AS BIGINT), 0) AS order_seq_org
  , "주문번호(쇼핑몰)" AS order_id
  , "부주문번호" AS order_id_dup
  , TRY_CAST("계정등록순번" AS INTEGER) AS account_no
  , "상품코드(사방넷)" AS option_id
  , "상품코드(쇼핑몰)" AS product_id_shop
  , TRY_CAST("수량" AS INTEGER) AS order_quantity
  , TRY_CAST("EA(확정)" AS INTEGER) AS sku_quantity
  , TRY_CAST("결제금액" AS INTEGER) AS payment_amount
  , TRY_CAST("주문금액" AS INTEGER) AS order_amount
  , COALESCE(TRY_CAST("주문일시(YYYY-MM-DD HH:MM)" AS TIMESTAMP), TRY_CAST("수집일시(YYYY-MM-DD HH:MM:SS)" AS TIMESTAMP)) AS order_dt
  , TRY_CAST("수집일시(YYYY-MM-DD HH:MM:SS)" AS TIMESTAMP) AS register_dt
FROM {{ array }};

-- OrderDownload: insert_order
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- OrderDownload: create_option
CREATE TABLE IF NOT EXISTS {{ table }} (
    option_id VARCHAR
  , product_id_shop VARCHAR
  , account_no INTEGER
  , model_code VARCHAR
  , model_id VARCHAR
  , product_name VARCHAR
  , product_name_shop VARCHAR
  , product_name_abbr VARCHAR
  , option_name VARCHAR
  , option_name_shop VARCHAR
  , option_name_abbr VARCHAR
  , sales_price INTEGER
  , order_id VARCHAR
  , order_date DATE
  , PRIMARY KEY (account_no, product_id_shop, option_id)
);

-- OrderDownload: select_option
SELECT DISTINCT
    "상품코드(사방넷)" AS option_id
  , "상품코드(쇼핑몰)" AS product_id_shop
  , TRY_CAST("계정등록순번" AS INTEGER) AS account_no
  , "모델명" AS model_code
  , "자체상품코드" AS model_id
  , "상품명(확정)" AS product_name
  , "상품명(수집)" AS product_name_shop
  , "상품약어" AS product_name_abbr
  , "옵션(확정)" AS option_name
  , "옵션(수집)" AS option_name_shop
  , "옵션별칭" AS option_name_abbr
  , TRY_CAST("판매가(상품)" AS INTEGER) AS sales_price
  , "주문번호(쇼핑몰)" AS order_id
  , TRY_CAST("주문일시(YYYY-MM-DD HH:MM)" AS DATE) AS order_date
FROM {{ array }}
WHERE (TRY_CAST("계정등록순번" AS INTEGER) IS NOT NULL)
  AND ("상품코드(쇼핑몰)" IS NOT NULL)
  AND ("상품코드(사방넷)" IS NOT NULL)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY TRY_CAST("계정등록순번" AS INTEGER), "상품코드(쇼핑몰)", "상품코드(사방넷)"
  ORDER BY TRY_CAST("주문일시(YYYY-MM-DD HH:MM)" AS TIMESTAMP) DESC) = 1;

-- OrderDownload: insert_option
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- OrderDownload: create_invoice
CREATE TABLE IF NOT EXISTS {{ table }} (
    order_seq BIGINT PRIMARY KEY
  , account_no INTEGER
  , invoice_no VARCHAR NOT NULL
  , delivery_company VARCHAR
  , order_status_div INTEGER
  , order_status INTEGER
  , order_dt TIMESTAMP NOT NULL
  , invoice_date DATE
);

-- OrderDownload: select_invoice
SELECT
    TRY_CAST("주문번호(사방넷)" AS BIGINT) AS order_seq
  , TRY_CAST("계정등록순번" AS INTEGER) AS account_no
  , "송장번호" AS invoice_no
  , "택배사" AS delivery_company
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
  , TRY_CAST("주문일시(YYYY-MM-DD HH:MM)" AS TIMESTAMP) AS order_dt
  , TRY_CAST("송장등록일자(YYYY-MM-DD)" AS DATE) AS invoice_date
FROM {{ array }}
WHERE ("송장번호" IS NOT NULL)
  AND (TRY_CAST("주문일시(YYYY-MM-DD HH:MM)" AS TIMESTAMP) IS NOT NULL);

-- OrderDownload: insert_invoice
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- OrderDownload: create_dispatch
CREATE TABLE IF NOT EXISTS {{ table }} (
    order_seq BIGINT PRIMARY KEY
  , order_id VARCHAR
  , account_no INTEGER
  , option_id VARCHAR
  , sku_quantity INTEGER
  , orderer_name VARCHAR
  , receiver_name VARCHAR
  , zipcode VARCHAR
  , address VARCHAR
  , phone1 VARCHAR
  , phone2 VARCHAR
  , delivery_message VARCHAR
  , box_type VARCHAR
  , delivery_type VARCHAR
  , register_dt TIMESTAMP NOT NULL
  , order_dt TIMESTAMP
);

-- OrderDownload: select_dispatch
SELECT
    "주문번호(사방넷)" AS order_seq
  , "주문번호(쇼핑몰)" AS order_id
  , TRY_CAST("계정등록순번" AS INTEGER) AS account_no
  , "상품코드(사방넷)" AS option_id
  , TRY_CAST("EA(확정)" AS INTEGER) AS sku_quantity
  , "주문자명" AS orderer_name
  , "수취인명" AS receiver_name
  , "수취인우편번호1" AS zipcode
  , "수취인주소1" AS address
  , "수취인전화번호1" AS phone1
  , "수취인전화번호2" AS phone2
  , "배송메세지1" AS delivery_message
  , "박스타입" AS box_type
  , "운임구분" AS delivery_type
  , TRY_CAST("수집일시(YYYY-MM-DD HH:MM:SS)" AS TIMESTAMP) AS register_dt
  , TRY_CAST("주문일시(YYYY-MM-DD HH:MM)" AS TIMESTAMP) AS order_dt
FROM {{ array }};

-- OrderDownload: insert_dispatch
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- OrderStatus: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    order_seq BIGINT
  , order_status INTEGER
  , order_dt TIMESTAMP NOT NULL
  , update_date DATE
  , PRIMARY KEY (order_seq, order_status)
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
  AND (TRY_CAST("주문일시(YYYY-MM-DD HH:MM)" AS TIMESTAMP) IS NOT NULL)
  AND TRY_CAST(STRPTIME("{{ date_type }}자({{ date_format }})", '{{ time_format }}') AS DATE) IS NOT NULL;

-- OrderStatus: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ProductMapping: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id_shop VARCHAR
  , product_id VARCHAR NOT NULL
  , account_no INTEGER
  , shop_id VARCHAR
  -- , shop_name VARCHAR
  -- , shop_login_id VARCHAR
  , product_name VARCHAR
  -- , model_id VARCHAR
  , sales_price INTEGER
  , mapping_count INTEGER
  , PRIMARY KEY (account_no, product_id_shop)
);

-- ProductMapping: select
SELECT
    shmaPrdNo AS product_id_shop
  , prdNo AS product_id
  , TRY_CAST(acntRegsSrno AS INTEGER) AS account_no
  , shmaId AS shop_id
  -- , shmaNm AS shop_name
  -- , shmaCnctnLoginId AS shop_login_id
  , prdNm AS product_name
  -- , onsfPrdCd AS model_id
  , sepr AS sales_price
  , COALESCE(mpngCnt, 0) AS mapping_count
FROM {{ array }}
WHERE (prdNo IS NOT NULL)
  AND (shmaPrdNo IS NOT NULL)
  AND (TRY_CAST(acntRegsSrno AS INTEGER) IS NOT NULL);

-- ProductMapping: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- SkuMapping: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id_shop VARCHAR
  , option_id VARCHAR NOT NULL
  , shop_id VARCHAR
  , product_name VARCHAR
  , option_name VARCHAR
  , sku_seq INTEGER
  , sku_name VARCHAR
  , register_dt TIMESTAMP
  , PRIMARY KEY (shop_id, product_id_shop, sku_seq)
);

-- SkuMapping: select
SELECT
    shmaPrdNo AS product_id_shop
  , CONCAT(prdNo, '-', COALESCE(skuNo, '0001')) AS option_id
  , $shop_id AS shop_id
  , prdNm AS product_name
  , optDtlNm AS option_name
  , rn AS sku_seq
  , skuDscr AS sku_name
  , TRY_CAST(fstRegsDt AS TIMESTAMP) AS register_dt
FROM {{ array }}
WHERE (prdNo IS NOT NULL)
  AND (shmaPrdNo IS NOT NULL)
  AND (rn IS NOT NULL);

-- SkuMapping: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
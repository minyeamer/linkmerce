-- Product: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id VARCHAR PRIMARY KEY
  , model_code VARCHAR
  , model_id VARCHAR
  , product_name VARCHAR
  , product_keyword VARCHAR
  , brand_name VARCHAR
  , maker_name VARCHAR
  , logistics_service VARCHAR
  , product_status INTEGER
  , manufacture_year INTEGER
  , sales_price INTEGER
  , org_price INTEGER
  , image_file VARCHAR
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
);

-- Product: bulk_insert
INSERT INTO {{ table }}
SELECT
    prdNo AS product_id
  , modlNm AS model_code
  , onsfPrdCd AS model_id
  , prdNm AS product_name
  , prdAbbrRmrk AS product_keyword
  , brndNm AS brand_name
  , mkcpNm AS maker_name
  , lgstscSvcAcntIdK AS logistics_service
  , TRY_CAST(prdSplyStsCd AS INTEGER) AS product_status
  , TRY_CAST(prdcYy AS INTEGER) AS manufacture_year
  , sepr AS sales_price
  , splyCprc AS org_price
  , string_split(prdImgFilePathNm, '/')[-1] AS image_file
  , TRY_CAST(fstRegsDt AS TIMESTAMP) AS register_dt
  , TRY_CAST(fnlChgDt AS TIMESTAMP) AS modify_dt
FROM {{ rows }}
WHERE prdNo IS NOT NULL
ON CONFLICT DO NOTHING;

-- Product: product_status
SELECT *
FROM UNNEST([
    STRUCT(1 AS code, '대기중' AS name)
  , STRUCT(2 AS code, '공급중' AS name)
  , STRUCT(3 AS code, '일시중지' AS name)
  , STRUCT(4 AS code, '완전품절' AS name)
  , STRUCT(5 AS code, '미사용' AS name)
  , STRUCT(6 AS code, '삭제' AS name)
  , STRUCT(7 AS code, '자료없음' AS name)
  , STRUCT(8 AS code, '비노출' AS name)
]);


-- Option: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id VARCHAR
  , sku_id VARCHAR
  , option_group VARCHAR
  , option_name VARCHAR
  , option_status INTEGER
  , quantity INTEGER
  , option_price INTEGER
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
  , PRIMARY KEY (product_id, sku_id)
);

-- Option: bulk_insert
INSERT INTO {{ table }}
SELECT
    prdNo AS product_id
  , skuNo AS sku_id
  , optCnfgNm AS option_group
  , optDtlNm AS option_name
  , TRY_CAST(skuSplyStsCd AS INTEGER) AS option_status
  , skuQt AS quantity
  , skuAddAmt AS option_price
  , TRY_CAST(fstRegsDt AS TIMESTAMP) AS register_dt
  , TRY_CAST(fnlChgDt AS TIMESTAMP) AS modify_dt
FROM {{ rows }}
WHERE (prdNo IS NOT NULL) AND (skuNo IS NOT NULL)
ON CONFLICT DO NOTHING;

-- Option: option_status
SELECT *
FROM UNNEST([
    STRUCT(2 AS code, '판매' AS name)
  , STRUCT(4 AS code, '품절' AS name)
  , STRUCT(5 AS code, '미사용' AS name)
]);


-- OptionDownload: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    option_id VARCHAR PRIMARY KEY
  , barcode BIGINT
  , option_group VARCHAR
  , option_name VARCHAR
  , bundle_option_ids VARCHAR
  , option_status INTEGER
  , option_type INTEGER
  , option_quantity INTEGER
  , option_price INTEGER
  , register_dt TIMESTAMP
);

-- OptionDownload: bulk_insert
INSERT INTO {{ table }}
SELECT
    "사방넷상품코드" AS option_id
  , TRY_CAST("바코드" AS BIGINT) AS barcode
  , "옵션제목" AS option_group
  , "옵션상세명칭" AS option_name
  , "연결상품코드" AS bundle_option_ids
  , TRY_CAST("공급상태" AS INTEGER) AS option_status
  , TRY_CAST("옵션구분" AS INTEGER) AS option_type
  , "EA" AS option_quantity
  , TRY_CAST("단품추가금액" AS INTEGER) AS option_price
  , TRY_CAST("등록일시" AS TIMESTAMP) AS register_dt
FROM {{ rows }}
WHERE "사방넷상품코드" IS NOT NULL
ON CONFLICT DO NOTHING;

-- OptionDownload: option_status
SELECT *
FROM UNNEST([
    STRUCT(1 AS code, '판매' AS name)
  , STRUCT(2 AS code, '품절' AS name)
  , STRUCT(3 AS code, '미사용' AS name)
]);

-- OptionDownload: option_type
SELECT *
FROM UNNEST([
    STRUCT(1 AS code, '세트' AS name)
  , STRUCT(2 AS code, '모음전' AS name)
  , STRUCT(3 AS code, '일반옵션' AS name)
]);


-- AddProductGroup: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    group_id VARCHAR PRIMARY KEY
  , group_name VARCHAR
  -- , register_dt TIMESTAMP
  -- , modify_dt TIMESTAMP
);

-- AddProductGroup: bulk_insert
INSERT INTO {{ table }}
SELECT
    addPrdGrpId AS group_id
  , addPrdGrpNm AS group_name
  -- , TRY_STRPTIME(SUBSTR(fstRegsDt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
  -- , TRY_STRPTIME(SUBSTR(fnlChgDt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS modify_dt
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- AddProduct: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    group_id VARCHAR
  , group_name VARCHAR
  , shop_id VARCHAR
  , option_seq INTEGER
  , option_id VARCHAR NOT NULL
  , option_name VARCHAR
  , sales_price INTEGER
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
  , PRIMARY KEY (group_id, option_seq)
);

-- AddProduct: bulk_insert
INSERT INTO {{ table }}
SELECT
    addPrdGrpId AS group_id
  , $meta.addPrdGrpNm AS group_name
  , $meta.shmaId AS shop_id
  , addPrdSkuCnfgSrno AS option_seq
  , CONCAT(prdNo, '-', skuNo) AS option_id
  , addPrdSkuCnfgNm AS option_name
  , sepr AS sales_price
  , TRY_STRPTIME(SUBSTR($meta.fstRegsDt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
  , TRY_STRPTIME(SUBSTR($meta.fnlChgDt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS modify_dt
FROM {{ rows }}
ON CONFLICT DO NOTHING;
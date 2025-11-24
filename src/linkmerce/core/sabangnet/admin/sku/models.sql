-- MappingSearch: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id VARCHAR
  , product_id_shop VARCHAR
  , account_no INTEGER
  , shop_id VARCHAR
  , shop_name VARCHAR
  , shop_login_id VARCHAR
  , product_name VARCHAR
  , model_id VARCHAR
  , sales_price INTEGER
  , sku_quantity INTEGER
  , mapping_count INTEGER
  , PRIMARY KEY (shop_id, product_id, product_id_shop)
);

-- MappingSearch: select
SELECT
    prdNo AS product_id
  , shmaPrdNo AS product_id_shop
  , TRY_CAST(acntRegsSrno AS INTEGER) AS account_no
  , shmaId AS shop_id
  , shmaNm AS shop_name
  , shmaCnctnLoginId AS shop_login_id
  , prdNm AS product_name
  , onsfPrdCd AS model_id
  , sepr AS sales_price
  , skuCt AS sku_quantity
  , COALESCE(mpngCnt, 0) AS mapping_count
FROM {{ array }}
WHERE (prdNo IS NOT NULL)
  AND (shmaPrdNo IS NOT NULL)
  AND (shmaId IS NOT NULL)
  AND IF($sku_yn IS NULL, TRUE, (mapping_count > 0) = $sku_yn);

-- MappingSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- MappingList: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    option_id VARCHAR
  , product_id_shop VARCHAR
  , shop_id VARCHAR
  , product_name VARCHAR
  , option_name VARCHAR
  , sku_name VARCHAR
  , register_order INTEGER
  , register_dt TIMESTAMP
  , PRIMARY KEY (shop_id, product_id_shop, option_id, register_order)
);

-- MappingList: select
SELECT
    CONCAT(prdNo, '-', skuNo) AS option_id
  , shmaPrdNo AS product_id_shop
  , $shop_id AS shop_id
  , prdNm AS product_name
  , optDtlNm AS option_name
  , skuDscr AS sku_name
  , rn AS register_order
  , TRY_CAST(fstRegsDt AS TIMESTAMP) AS register_dt
FROM {{ array }}
WHERE (prdNo IS NOT NULL)
  AND (shmaPrdNo IS NOT NULL)
  AND (rn IS NOT NULL);

-- MappingList: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
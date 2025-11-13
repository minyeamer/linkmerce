-- Product: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id BIGINT PRIMARY KEY
  , product_no BIGINT NOT NULL
  , catalog_id BIGINT
  , channel_seq BIGINT NOT NULL
  -- , channel_type VARCHAR -- ['STOREFARM', 'WINDOW', 'AFFILIATE']
  , product_name VARCHAR
  , management_code VARCHAR
  , model_name VARCHAR
  , brand_name VARCHAR
  -- , maker_name VARCHAR
  , category_id INTEGER
  -- , full_category_id VARCHAR
  -- , full_category_name VARCHAR
  , status_type VARCHAR -- ['WAIT', 'SALE', 'OUTOFSTOCK', 'UNADMISSION', 'REJECTION', 'SUSPENSION', 'CLOSE', 'PROHIBITION']
  , display_type VARCHAR -- ['WAIT', 'ON', 'SUSPENSION']
  -- , image_url VARCHAR
  , tags VARCHAR
  , price INTEGER
  , sales_price INTEGER
  -- , stock_quantity INTEGER
  , delivery_type INTEGER
  , delivery_fee INTEGER
  -- , return_fee INTEGER
  -- , exchange_fee INTEGER
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
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

-- Product: select
SELECT
    TRY_CAST(channelProductNo AS BIGINT) AS product_id
  , TRY_CAST(originProductNo AS BIGINT) AS product_no
  , TRY_CAST(modelId AS BIGINT) AS catalog_id
  , CAST($channel_seq AS BIGINT) AS channel_seq
  -- , channelServiceType AS channel_type
  , name AS product_name
  , sellerManagementCode AS management_code
  , modelName AS model_name
  , brandName AS brand_name
  -- , manufacturerName AS maker_name
  , TRY_CAST(categoryId AS INTEGER) AS category_id
  -- , wholeCategoryId AS full_category_id
  -- , wholeCategoryName AS full_category_name
  , statusType AS status_type
  , channelProductDisplayStatusType AS display_type
  -- , representativeImage.url AS image_url
  , (SELECT STRING_AGG(json_extract_string(value, '$')) FROM json_each(sellerTags->'$[*].text')) AS tags
  , salePrice AS price
  , discountedPrice AS sales_price
  -- , stockQuantity AS stock_quantity
  , (CASE
      WHEN deliveryAttributeType = 'NORMAL' THEN 0
      WHEN deliveryAttributeType = 'TODAY' THEN 1
      WHEN deliveryAttributeType = 'OPTION_TODAY' THEN 2
      WHEN deliveryAttributeType = 'HOPE' THEN 3
      WHEN deliveryAttributeType = 'TODAY_ARRIVAL' THEN 4
      WHEN deliveryAttributeType = 'DAWN_ARRIVAL' THEN 5
      WHEN deliveryAttributeType = 'PRE_ORDER' THEN 6
      WHEN deliveryAttributeType = 'ARRIVAL_GUARANTEE' THEN 7
      WHEN deliveryAttributeType = 'SELLER_GUARANTEE' THEN 8
      WHEN deliveryAttributeType = 'HOPE_SELLER_GUARANTEE' THEN 9
      WHEN deliveryAttributeType = 'PICKUP' THEN 10
      WHEN deliveryAttributeType = 'QUICK' THEN 11
      ELSE NULL END) AS delivery_type
  , deliveryFee AS delivery_fee
  -- , returnFee AS return_fee
  -- , exchangeFee AS exchange_fee
  , TRY_STRPTIME(SUBSTR(regDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
  , TRY_STRPTIME(SUBSTR(modifiedDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS modify_dt
FROM {{ array }};

-- Product: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Option: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    option_id BIGINT PRIMARY KEY
  , product_id BIGINT
  , channel_seq BIGINT
  , product_type INTEGER -- {0: '옵션상품(단독형)', 1: '옵션상품(조합형)', 2: '추가상품'}
  , option_group1 VARCHAR
  , option_name1 VARCHAR
  , option_group2 VARCHAR
  , option_name2 VARCHAR
  , option_group3 VARCHAR
  , option_name3 VARCHAR
  , management_code VARCHAR
  , usable BOOLEAN
  , option_price INTEGER
  , stock_quantity INTEGER
  , register_order INTEGER
);

-- Option: select_option
SELECT
    id AS option_id
  , TRY_CAST($product_id AS BIGINT) AS product_id
  , TRY_CAST($channel_seq AS BIGINT) AS channel_seq
  , 0 AS product_type
  , groupName AS option_group1
  , name AS option_name1
  , usable
  , COALESCE(TRY_CAST(json_extract_path_text(item, 'price') AS INTEGER), 0) AS option_price
  , TRY_CAST(json_extract_path_text(item, 'stockQuantity') AS INTEGER) AS stock_quantity
  , ROW_NUMBER() OVER () AS register_order
FROM {{ array }} AS item
WHERE id IS NOT NULL;

-- Option: insert_option
INSERT INTO {{ table }} (
    option_id
  , product_id
  , channel_seq
  , product_type
  , option_group1
  , option_name1
  , usable
  , option_price
  , stock_quantity
  , register_order
) {{ values }} ON CONFLICT DO NOTHING;

-- Option: select_option_comb
SELECT
    id AS option_id
  , TRY_CAST($product_id AS BIGINT) AS product_id
  , TRY_CAST($channel_seq AS BIGINT) AS channel_seq
  , 1 AS product_type
  , optionGroupName1 AS option_group1
  , optionName1 AS option_name1
  , item->>'$.optionGroupName2' AS option_group2
  , item->>'$.optionName2' AS option_name2
  , item->>'$.optionGroupName3' AS option_group3
  , item->>'$.optionName3' AS option_name3
  , item->>'$.sellerManagerCode' AS management_code
  , usable
  , price AS option_price
  , stockQuantity AS stock_quantity
  , ROW_NUMBER() OVER () AS register_order
FROM {{ array }} AS item
WHERE id IS NOT NULL;

-- Option: insert_option_comb
INSERT INTO {{ table }} (
    option_id
  , product_id
  , channel_seq
  , product_type
  , option_group1
  , option_name1
  , option_group2
  , option_name2
  , option_group3
  , option_name3
  , management_code
  , usable
  , option_price
  , stock_quantity
  , register_order
) {{ values }} ON CONFLICT DO NOTHING;

-- Option: select_supplement
SELECT
    id AS option_id
  , TRY_CAST($product_id AS BIGINT) AS product_id
  , TRY_CAST($channel_seq AS BIGINT) AS channel_seq
  , 2 AS product_type
  , groupName AS option_group1
  , name AS option_name1
  , item->>'$.sellerManagerCode' AS management_code
  , usable
  , price AS option_price
  , stockQuantity AS stock_quantity
  , ROW_NUMBER() OVER () AS register_order
FROM {{ array }} AS item
WHERE id IS NOT NULL;

-- Option: insert_supplement
INSERT INTO {{ table }} (
    option_id
  , product_id
  , channel_seq
  , product_type
  , option_group1
  , option_name1
  , management_code
  , usable
  , option_price
  , stock_quantity
  , register_order
) {{ values }} ON CONFLICT DO NOTHING;
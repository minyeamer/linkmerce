-- Product: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id BIGINT PRIMARY KEY
  , product_no BIGINT NOT NULL
  , catalog_id BIGINT
  , channel_seq BIGINT NOT NULL
  -- , channel_type VARCHAR -- ['STOREFARM', 'WINDOW', 'AFFILIATE']
  , product_name VARCHAR
  -- , management_code VARCHAR
  -- , model_name VARCHAR
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
  , delivery_type VARCHAR -- ['NORMAL', 'TODAY', 'OPTION_TODAY', 'HOPE', 'TODAY_ARRIVAL', 'DAWN_ARRIVAL', 'ARRIVAL_GUARANTEE', 'SELLER_GUARANTEE', 'HOPE_SELLER_GUARANTEE', 'QUICK', 'PICKUP', 'QUICK_PICKUP']
  , delivery_fee INTEGER
  -- , return_fee INTEGER
  -- , exchange_fee INTEGER
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
);

-- Product: select
SELECT
    TRY_CAST(channelProductNo AS BIGINT) AS product_id
  , TRY_CAST(originProductNo AS BIGINT) AS product_no
  , TRY_CAST(modelId AS BIGINT) AS catalog_id
  , CAST($channel_seq AS BIGINT) AS channel_seq
  -- , channelServiceType AS channel_type
  , name AS product_name
  -- , sellerManagementCode AS management_code
  -- , modelName AS model_name
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
  , deliveryAttributeType AS delivery_type
  , deliveryFee AS delivery_fee
  -- , returnFee AS return_fee
  -- , exchangeFee AS exchange_fee
  , TRY_STRPTIME(SUBSTR(regDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
  , TRY_STRPTIME(SUBSTR(modifiedDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS modify_dt
FROM {{ array }};

-- Product: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
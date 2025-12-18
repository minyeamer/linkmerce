-- ProductOption: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_inventory_id BIGINT
  , vendor_inventory_item_id BIGINT
  , product_id BIGINT
  , option_id BIGINT PRIMARY KEY
  , item_id BIGINT
  , barcode VARCHAR
  , vendor_id VARCHAR
  , product_name VARCHAR
  , option_name VARCHAR
  , display_category_id INTEGER
  , category_id INTEGER
  , category_name VARCHAR
  , brand_name VARCHAR
  , maker_name VARCHAR
  , product_status TINYINT -- {0: '판매중', 1: '품절', 2: '숨김상품'}
  , is_deleted BOOLEAN
  , price INTEGER
  , sales_price INTEGER
  , delivery_fee INTEGER
  , order_quantity INTEGER
  , stock_quantity INTEGER
  , register_dt TIMESTAMP
  , modify_dt TIMESTAMP
);

-- ProductOption: select
SELECT
    vendorInventoryId AS vendor_inventory_id
  , vendorInventoryItemId AS vendor_inventory_item_id
  , NULL AS product_id
  , vendorItemId AS option_id
  , NULL AS item_id
  , barcode
  , vendorId AS vendor_id
  , productName AS product_name
  , itemName AS option_name
  , displayCategoryCode AS display_category_id
  , categoryId AS category_id
  , categoryName AS category_name
  , brand AS brand_name
  , manufacture AS maker_name
  , (CASE WHEN valid = 'VALID' THEN 0 WHEN valid = 'INVALID' THEN 1 ELSE NULL END) AS product_status
  , $is_deleted AS is_deleted
  , NULL AS price
  , salePrice AS sales_price
  , deliveryCharge AS delivery_fee
  , viUnitSoldAgg AS order_quantity
  , stockQuantity AS stock_quantity
  , TRY_CAST(createdOn AS TIMESTAMP) AS register_dt
  , TRY_CAST(modifiedOn AS TIMESTAMP) AS modify_dt
FROM {{ array }}
WHERE vendorItemId IS NOT NULL;

-- ProductOption: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ProductDetail: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_inventory_id BIGINT
  , vendor_inventory_item_id BIGINT
  , option_id BIGINT PRIMARY KEY
  , product_id BIGINT
  , item_id BIGINT
  , barcode VARCHAR
  , option_name VARCHAR
  , price INTEGER
  , sales_price INTEGER
  , stock_quantity INTEGER
);

-- ProductDetail: select
SELECT
    vendorInventoryId AS vendor_inventory_id
  , vendorInventoryItemId AS vendor_inventory_item_id
  , vendorItemId AS option_id
  , productId AS product_id
  , itemId AS item_id
  , barcode
  , itemName AS option_name
  , originalPrice AS price
  , salePrice AS sales_price
  , stockQuantity AS stock_quantity
FROM {{ array }}
WHERE vendorItemId IS NOT NULL;

-- ProductDetail: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- ProductDetail: insert_vendor
INSERT INTO {{ table }} (
    option_id
  , product_id
  , item_id
  , price
)
SELECT
    option_id
  , product_id
  , item_id
  , price
FROM ({{ values }}) AS items
ON CONFLICT (option_id) DO UPDATE SET
    product_id = EXCLUDED.product_id
  , item_id = EXCLUDED.item_id
  , price = EXCLUDED.price;

-- ProductDetail: insert_rfm
INSERT INTO {{ table }} (
    option_id
  , vendor_inventory_item_id
  , item_id
  , barcode
  , price
)
SELECT
    option_id
  , vendor_inventory_item_id
  , item_id
  , barcode
  , price
FROM ({{ values }}) AS items
WHERE EXISTS (SELECT 1 FROM {{ table }} AS T WHERE T.option_id = items.option_id)
ON CONFLICT (option_id) DO UPDATE SET
    vendor_inventory_item_id = EXCLUDED.vendor_inventory_item_id
  , item_id = EXCLUDED.item_id
  , barcode = EXCLUDED.barcode
  , price = EXCLUDED.price;


-- ProductDownload: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_inventory_id BIGINT
  , product_id BIGINT
  , option_id BIGINT PRIMARY KEY
  , barcode VARCHAR
  , vendor_id VARCHAR
  , vendor_inventory_name VARCHAR
  , product_name VARCHAR
  , option_name VARCHAR
  , product_status TINYINT -- {0: '판매중', 1: '판매중지'}
  , is_deleted BOOLEAN
  , price INTEGER
  , sales_price INTEGER
  , order_quantity INTEGER
  , stock_quantity INTEGER
);

-- ProductDownload: select
SELECT
    TRY_CAST("등록상품ID" AS BIGINT) AS vendor_inventory_id
  , TRY_CAST("Product ID" AS BIGINT) AS product_id
  , TRY_CAST("옵션 ID" AS BIGINT) AS option_id
  , "바코드" AS barcode
  , $vendor_id AS vendor_id
  , "쿠팡 노출 상품명" AS vendor_inventory_name
  , "업체 등록 상품명" AS product_name
  , "등록 옵션명" AS option_name
  , (CASE WHEN "판매상태" = '판매중' THEN 0 WHEN "판매상태" = '판매중지' THEN 1 ELSE NULL END) AS product_status -- {0: '판매중', 1: '판매중지'}
  , $is_deleted AS is_deleted
  , TRY_CAST("할인율기준가" AS INTEGER) AS price
  , TRY_CAST("판매가격" AS INTEGER) AS sales_price
  , TRY_CAST("판매수량" AS INTEGER) AS order_quantity
  , TRY_CAST("잔여수량(재고)" AS INTEGER) AS stock_quantity
FROM {{ array }}
WHERE TRY_CAST("옵션 ID" AS BIGINT) IS NOT NULL;

-- ProductDownload: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- RocketInventory: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_inventory_id BIGINT
  , vendor_inventory_item_id BIGINT
  , product_id BIGINT
  , option_id BIGINT PRIMARY KEY
  , sku_id BIGINT
  , vendor_id VARCHAR
  , stock_quantity INTEGER
  , inprogress_quantity INTEGER
  , sales_amount_7d INTEGER
  , sales_amount_30d INTEGER
  , unit_sold_7d INTEGER
  , unit_sold_30d INTEGER
  , days_of_cover INTEGER
  , fee_amount INTEGER
  , updated_at TIMESTAMP
);

-- RocketInventory: select
SELECT
    listingDetails.vendorInventoryId AS vendor_inventory_id
  , creturnConfigViewDto->'$.vendorInventoryItemId' AS vendor_inventory_item_id
  , listingDetails.productId AS product_id
  , vendorItemId AS option_id
  , creturnConfigViewDto->'$.externalSkuId' AS sku_id
  , COALESCE(creturnConfigViewDto->>'$.vendorId', $vendor_id) AS vendor_id
  , inventoryDetails.orderableQuantity AS stock_quantity
  , inventoryDetails.inProgressInboundStatistics->'$.inProgressInboundQuantity' AS inprogress_quantity
  , gmvForLast7Days AS sales_amount_7d
  , gmvForLast30Days AS sales_amount_30d
  , unitsSoldForLast7Days AS unit_sold_7d
  , unitsSoldForLast30Days AS unit_sold_30d
  , inventoryDetails.daysOfCover AS days_of_cover
  , TRY_CAST(inventoryDetails.storageFee->'$.monthlyStorageFeeAmount.amount' AS INTEGER) AS fee_amount
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updated_at
FROM {{ array }}
WHERE vendorItemId IS NOT NULL;

-- RocketInventory: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- RocketOption: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_inventory_id BIGINT
  , vendor_inventory_item_id BIGINT
  , product_id BIGINT
  , option_id BIGINT PRIMARY KEY
  , item_id BIGINT
  , barcode VARCHAR
  , vendor_id VARCHAR
  , product_name VARCHAR
  , option_name VARCHAR
  , display_category_id INTEGER
  , category_id INTEGER
  , category_name VARCHAR
  , product_status TINYINT  -- {0: '판매중', 1: '품절', 2: '숨김상품'}
  , price INTEGER
  , sales_price INTEGER
  , order_quantity INTEGER
  , stock_quantity INTEGER
  , register_dt TIMESTAMP
);

-- RocketOption: select
SELECT
    listingDetails.vendorInventoryId AS vendor_inventory_id
  , creturnConfigViewDto->'$.vendorInventoryItemId' AS vendor_inventory_item_id
  , listingDetails.productId AS product_id
  , vendorItemId AS option_id
  , creturnConfigViewDto->'$.itemId' AS item_id
  , NULL AS barcode
  , COALESCE(creturnConfigViewDto->>'$.vendorId', $vendor_id) AS vendor_id
  , COALESCE(creturnConfigViewDto->>'$.productName', listingDetails.vendorInventoryName) AS product_name
  , creturnConfigViewDto->>'$.itemName' AS option_name
  , COALESCE(
      creturnConfigViewDto->'$.displayCategoryCodeLevel5'
    , creturnConfigViewDto->'$.displayCategoryCodeLevel4'
    , creturnConfigViewDto->'$.displayCategoryCodeLevel3'
    , creturnConfigViewDto->'$.displayCategoryCodeLevel2'
    , creturnConfigViewDto->'$.displayCategoryCodeLevel1') AS display_category_id
  , creturnConfigViewDto->'$.creturnCategoryLevelThresholdDto.categoryId' AS category_id
  , creturnConfigViewDto->>'$.creturnCategoryLevelThresholdDto.kanNameEn' AS category_name
  , (CASE
      WHEN inventoryDetails.isHiddenByVendor THEN 2
      WHEN creturnConfigViewDto IS NOT NULL
        THEN IF(creturnConfigViewDto->'$.onSale', 0, 1)
      ELSE NULL END) AS product_status
  , NULL AS price
  , TRY_CAST(pricing.salesPrice.amount AS INTEGER) AS sales_price
  , unitsSoldForLast30Days AS order_quantity
  , inventoryDetails.orderableQuantity AS stock_quantity
  , TRY_CAST(listingDetails.productRegistrationDate AS TIMESTAMP) AS register_dt
FROM {{ array }}
WHERE vendorItemId IS NOT NULL;

-- RocketOption: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
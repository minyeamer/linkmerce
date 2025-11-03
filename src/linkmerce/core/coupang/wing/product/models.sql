-- ProductOption: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_inventory_id BIGINT
  , vendor_inventory_item_id BIGINT
  , product_id BIGINT
  , option_id BIGINT PRIMARY KEY
  , barcode VARCHAR
  , vendor_id VARCHAR
  , product_name VARCHAR
  , option_name VARCHAR
  , display_category_id INTEGER
  , category_id INTEGER
  , category_name VARCHAR
  , brand_name VARCHAR
  , maker_name VARCHAR
  , product_status TINYINT  -- {0: '판매중', 1: '품절'}
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
  , barcode AS barcode
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
    option_id BIGINT
  , product_id BIGINT
  , product_status TINYINT
  , price INTEGER
);

-- ProductDetail: select
SELECT
    vendorItemId AS option_id
  , productId AS product_id
  , originalPrice AS price
FROM {{ array }}
WHERE vendorItemId IS NOT NULL;

-- ProductDetail: insert
INSERT INTO {{ table }} (
    option_id
  , product_id
  , price
)
{{ values }}
ON CONFLICT (option_id) DO UPDATE SET
    product_id = EXCLUDED.product_id
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
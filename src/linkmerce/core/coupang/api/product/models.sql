-- Product: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_inventory_id BIGINT NOT NULL
  , product_id BIGINT
  , vendor_id VARCHAR NOT NULL
  , seller_product_name VARCHAR
  , display_category_id INTEGER
  , category_id INTEGER
  , brand_name VARCHAR
  , product_status TINYINT
  , sales_started_at TIMESTAMP
  , sales_ended_at TIMESTAMP
  , created_at TIMESTAMP
  , PRIMARY KEY (vendor_inventory_id)
);

-- Product: bulk_insert
INSERT INTO {{ table }}
SELECT
    sellerProductId AS vendor_inventory_id
  , productId AS product_id
  , vendorId AS vendor_id
  , sellerProductName AS seller_product_name
  , displayCategoryCode AS display_category_id
  , categoryId AS category_id
  , NULLIF(brand, '') AS brand_name
  , (CASE
      WHEN statusName = 'IN_REVIEW' THEN 0
      WHEN statusName = 'SAVED' THEN 1
      WHEN statusName = 'APPROVING' THEN 2
      WHEN statusName = 'APPROVED' THEN 3
      WHEN statusName = 'PARTIAL_APPROVED' THEN 4
      WHEN statusName = 'DENIED' THEN 5
      WHEN statusName = 'DELETED' THEN 6
      ELSE 9 END) AS product_status
  , TRY_STRPTIME(saleStartedAt, '%Y-%m-%dT%H:%M:%S') AS sales_started_at
  , TRY_STRPTIME(saleEndedAt, '%Y-%m-%dT%H:%M:%S') AS sales_ended_at
  , TRY_STRPTIME(createdAt, '%Y-%m-%dT%H:%M:%S') AS created_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- Product: product_status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'IN_REVIEW' AS name)
  , STRUCT(1 AS seq, 'SAVED' AS name)
  , STRUCT(2 AS seq, 'APPROVING' AS name)
  , STRUCT(3 AS seq, 'APPROVED' AS name)
  , STRUCT(4 AS seq, 'PARTIAL_APPROVED' AS name)
  , STRUCT(5 AS seq, 'DENIED' AS name)
  , STRUCT(6 AS seq, 'DELETED' AS name)
]);


-- ProductItem: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_inventory_id BIGINT NOT NULL
  , vendor_inventory_item_id BIGINT NOT NULL
  , product_id BIGINT
  , option_id BIGINT
  , vendor_id VARCHAR NOT NULL
  , business_type TINYINT NOT NULL -- {0: '마켓플레이스', 1: '로켓그로스'}
  , seller_product_name VARCHAR
  , option_name VARCHAR
  , display_category_id INTEGER
  , category_id INTEGER
  , brand_name VARCHAR
  , product_status TINYINT
  , sales_started_at TIMESTAMP
  , sales_ended_at TIMESTAMP
  , created_at TIMESTAMP
  , PRIMARY KEY (vendor_inventory_id, vendor_inventory_item_id)
);

-- ProductItem: bulk_insert
INSERT INTO {{ table }}
SELECT
    sellerProductId AS vendor_inventory_id
  , item.sellerProductItemId AS vendor_inventory_item_id
  , productId AS product_id
  , item.vendorItemId AS option_id
  , vendorId AS vendor_id
  , businessType AS business_type
  , sellerProductName AS seller_product_name
  , item.itemName AS option_name
  , displayCategoryCode AS display_category_id
  , categoryId AS category_id
  , NULLIF(brand, '') AS brand_name
  , (CASE
      WHEN statusName = 'IN_REVIEW' THEN 0
      WHEN statusName = 'SAVED' THEN 1
      WHEN statusName = 'APPROVING' THEN 2
      WHEN statusName = 'APPROVED' THEN 3
      WHEN statusName = 'PARTIAL_APPROVED' THEN 4
      WHEN statusName = 'DENIED' THEN 5
      WHEN statusName = 'DELETED' THEN 6
      ELSE 9 END) AS product_status
  , TRY_STRPTIME(saleStartedAt, '%Y-%m-%dT%H:%M:%S') AS sales_started_at
  , TRY_STRPTIME(saleEndedAt, '%Y-%m-%dT%H:%M:%S') AS sales_ended_at
  , TRY_STRPTIME(createdAt, '%Y-%m-%dT%H:%M:%S') AS created_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- ProductDetail: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    vendor_inventory_id BIGINT NOT NULL
  , vendor_inventory_item_id BIGINT NOT NULL
  , product_id BIGINT
  , option_id BIGINT
  , item_id BIGINT
  , vendor_id VARCHAR NOT NULL
  , business_type TINYINT NOT NULL -- {0: '마켓플레이스', 1: '로켓그로스'}
  , seller_product_name VARCHAR
  , display_product_name VARCHAR
  , option_name VARCHAR
  , display_category_id INTEGER
  , category_id INTEGER
  , barcode VARCHAR
  , brand_name VARCHAR
  , maker_name VARCHAR
  , image_url VARCHAR
  , product_status TINYINT
  , price INTEGER
  , sales_price INTEGER
  , delivery_fee INTEGER
  , sales_started_at TIMESTAMP
  , sales_ended_at TIMESTAMP
  , PRIMARY KEY (vendor_inventory_id, vendor_inventory_item_id)
);

-- ProductDetail: bulk_insert
INSERT INTO {{ table }}
SELECT
    sellerProductId AS vendor_inventory_id
  , item.sellerProductItemId AS vendor_inventory_item_id
  , productId AS product_id
  , item.vendorItemId AS option_id
  , item.itemId AS item_id
  , vendorId AS vendor_id
  , businessType AS business_type
  , sellerProductName AS seller_product_name
  , displayProductName AS display_product_name
  , item.itemName AS option_name
  , displayCategoryCode AS display_category_id
  , categoryId AS category_id
  , NULLIF(item.barcode, '') AS barcode
  , NULLIF(brand, '') AS brand_name
  , NULLIF(manufacture, '') AS maker_name
  , item.images."0".cdnPath AS image_url
  , (CASE
      WHEN statusName = '심사중' THEN 0
      WHEN statusName = '임시저장' THEN 1
      WHEN statusName = '승인대기중' THEN 2
      WHEN statusName = '승인완료' THEN 3
      WHEN statusName = '부분승인완료' THEN 4
      WHEN statusName = '승인반려' THEN 5
      WHEN statusName = '상품삭제' THEN 6
      ELSE 9 END) AS product_status
  , item.originalPrice AS price
  , item.salePrice AS sales_price
  , deliveryCharge AS delivery_fee
  , TRY_STRPTIME(saleStartedAt, '%Y-%m-%dT%H:%M:%S') AS sales_started_at
  , TRY_STRPTIME(saleEndedAt, '%Y-%m-%dT%H:%M:%S') AS sales_ended_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- Inventory: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    option_id BIGINT NOT NULL
  , vendor_id VARCHAR NOT NULL
  , stock_quantity INTEGER
  , sales_price INTEGER
  , on_sale BOOLEAN
  , updated_at TIMESTAMP NOT NULL
  , PRIMARY KEY (option_id)
);

-- Inventory: bulk_insert
INSERT INTO {{ table }}
SELECT
    sellerItemId AS option_id
  , $vendor_id AS vendor_id
  , amountInStock AS stock_quantity
  , salePrice AS sales_price
  , onSale AS on_sale
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updated_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;

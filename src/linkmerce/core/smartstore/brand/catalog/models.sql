-- BrandCatalog: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    id BIGINT PRIMARY KEY
  , catalog_name VARCHAR
  , maker_id BIGINT
  , maker_name VARCHAR
  , brand_id BIGINT
  , brand_name VARCHAR
  , category_id INTEGER
  , category_name VARCHAR
  , category_id1 INTEGER
  , category_name1 VARCHAR
  , category_id2 INTEGER
  , category_name2 VARCHAR
  , category_id3 INTEGER
  , category_name3 VARCHAR
  , category_id4 INTEGER
  , category_name4 VARCHAR
  , image_url VARCHAR
  , official_price INTEGER
  , official_price_with_fee INTEGER
  , lowest_price INTEGER
  , lowest_price_with_fee INTEGER
  , product_count INTEGER
  , review_count INTEGER
  , review_rating TINYINT
  , register_dt TIMESTAMP
);

-- BrandCatalog: select
SELECT
    identifier AS id
  , prodName AS catalog_name
  , NULLIF(makerSeq, 0) AS maker_id
  , makerName AS maker_name
  , brandSeq AS brand_id
  , brandName AS brand_name
  , TRY_CAST(category.identifier AS INTEGER) AS category_id
  , category.name AS category_name
  , TRY_CAST(SPLIT_PART(category.fullId, '>', 1) AS INTEGER) AS category_id1
  , NULLIF(SPLIT_PART(category.fullName, '>', 1), '') AS category_name1
  , TRY_CAST(SPLIT_PART(category.fullId, '>', 2) AS INTEGER) AS category_id2
  , NULLIF(SPLIT_PART(category.fullName, '>', 2), '') AS category_name2
  , TRY_CAST(SPLIT_PART(category.fullId, '>', 3) AS INTEGER) AS category_id3
  , NULLIF(SPLIT_PART(category.fullName, '>', 3), '') AS category_name3
  , TRY_CAST(SPLIT_PART(category.fullId, '>', 4) AS INTEGER) AS category_id4
  , NULLIF(SPLIT_PART(category.fullName, '>', 4), '') AS category_name4
  , image.src AS image_url
  , officialAuthLowestPriceRatio.lowestPrice AS official_price
  , officialAuthLowestPriceRatioWithFee.lowestPrice AS official_price_with_fee
  , lowestPrice AS lowest_price
  , allLowestPriceWithFee.lowestPrice AS lowest_price_with_fee
  , productCount AS product_count
  , totalReviewCount AS review_count
  , TRY_CAST(reviewRating AS INT8) AS review_rating
  , TRY_STRPTIME(SUBSTR(registerDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
FROM {{ array }}
WHERE identifier IS NOT NULL;

-- BrandCatalog: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- BrandProduct: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    id BIGINT PRIMARY KEY
  , product_id VARCHAR NOT NULL
  , catalog_id BIGINT
  , product_name VARCHAR
  , maker_id BIGINT
  , maker_name VARCHAR
  , brand_id BIGINT
  , brand_name VARCHAR
  , mall_seq BIGINT
  , mall_name VARCHAR
  , category_id INTEGER
  , category_name VARCHAR
  , category_id1 INTEGER
  , category_name1 VARCHAR
  , category_id2 INTEGER
  , category_name2 VARCHAR
  , category_id3 INTEGER
  , category_name3 VARCHAR
  , category_id4 INTEGER
  , category_name4 VARCHAR
  , product_url VARCHAR
  , image_url VARCHAR
  , sales_price INTEGER
  , delivery_fee INTEGER
  , click_count INTEGER
  , review_count INTEGER
  , register_dt TIMESTAMP
);

-- BrandProduct: select
SELECT
    identifier AS id
  , mallProductId AS product_id
  , catalogId AS catalog_id
  , name AS product_name
  , NULLIF(makerSeq, 0) AS maker_id
  , makerName AS maker_name
  , brandSeq AS brand_id
  , brandName AS brand_name
  , TRY_CAST($mall_seq AS BIGINT) AS mall_seq
  , mallName AS mall_name
  , TRY_CAST(categoryId AS INTEGER) AS category_id
  , categoryName AS category_name
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 1) AS INTEGER) AS category_id1
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 1), '') AS category_name1
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 2) AS INTEGER) AS category_id2
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 2), '') AS category_name2
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 3) AS INTEGER) AS category_id3
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 3), '') AS category_name3
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 4) AS INTEGER) AS category_id4
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 4), '') AS category_name4
  , outLinkUrl AS product_url
  , imageInfo.src AS image_url
  , lowestPrice AS sales_price
  , TRY_CAST(deliveryFee AS INTEGER) AS delivery_fee
  , clickCount AS click_count
  , totalReviewCount AS review_count
  , TRY_STRPTIME(SUBSTR(registerDate, 1, 19), '%Y-%m-%dT%H:%M:%S') AS register_dt
FROM {{ array }}
WHERE (identifier IS NOT NULL)
  AND (mallProductId IS NOT NULL);

-- BrandProduct: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- BrandPrice: create_price
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id BIGINT PRIMARY KEY
  , mall_seq BIGINT
  , category_id INTEGER
  , sales_price INTEGER NOT NULL
  , created_at TIMESTAMP NOT NULL
);

-- BrandPrice: select_price
SELECT
    TRY_CAST(mallProductId AS BIGINT) AS product_id
  , TRY_CAST($mall_seq AS BIGINT) AS mall_seq
  , TRY_CAST(categoryId AS INTEGER) AS category_id
  , lowestPrice AS sales_price
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS created_at
FROM {{ array }}
WHERE (TRY_CAST(mallProductId AS BIGINT) IS NOT NULL)
  AND (lowestPrice IS NOT NULL);

-- BrandPrice: insert_price
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- BrandPrice: create_product
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id BIGINT PRIMARY KEY
  , mall_seq BIGINT
  , category_id INTEGER
  , category_id3 INTEGER
  , product_name VARCHAR
  , sales_price INTEGER
  , register_date DATE
  , update_date DATE NOT NULL
);

-- BrandPrice: select_product
SELECT
    TRY_CAST(mallProductId AS BIGINT) AS product_id
  , TRY_CAST($mall_seq AS BIGINT) AS mall_seq
  , TRY_CAST(categoryId AS INTEGER) AS category_id
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 3) AS INTEGER) AS category_id3
  , name AS product_name
  , lowestPrice AS sales_price
  , TRY_CAST(registerDate AS DATE) AS register_date
  , CURRENT_DATE AS update_date
FROM {{ array }}
WHERE TRY_CAST(mallProductId AS BIGINT) IS NOT NULL;

-- BrandPrice: upsert_product
INSERT INTO {{ table }} {{ values }}
ON CONFLICT DO UPDATE SET
    category_id = COALESCE(excluded.category_id, category_id)
  , category_id3 = COALESCE(excluded.category_id3, category_id3)
  , product_name = COALESCE(excluded.product_name, product_name)
  , sales_price = COALESCE(excluded.sales_price, sales_price)
  , register_date = LEAST(excluded.register_date, register_date)
  , update_date = excluded.update_date;


-- ProductCatalog: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_id BIGINT PRIMARY KEY
  , catalog_id BIGINT NOT NULL
  , created_at TIMESTAMP NOT NULL
);

-- ProductCatalog: select
SELECT
    TRY_CAST(mallProductId AS BIGINT) AS product_id
  , catalogId AS catalog_id
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS created_at
FROM {{ array }}
WHERE (TRY_CAST(mallProductId AS BIGINT) IS NOT NULL)
  AND (catalogId IS NOT NULL);

-- ProductCatalog: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
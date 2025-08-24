-- BrandCatalog: create
CREATE OR REPLACE TABLE {{ table }} (
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
  , sales_price INTEGER
  , product_count INTEGER
  , review_count INTEGER
  , review_rating TINYINT
  , register_dt TIMESTAMP
);

-- BrandCatalog: select
SELECT
    TRY_CAST(id AS BIGINT) AS id
  , name AS catalog_name
  , TRY_CAST(NULLIF(makerSeq, '0') AS BIGINT) AS maker_id
  , makerName AS maker_name
  , TRY_CAST(brandSeq AS BIGINT) AS brand_id
  , brandName AS brand_name
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
  , image.SRC AS image_url
  , TRY_CAST(lowestPrice AS INTEGER) AS sales_price
  , productCount AS product_count
  , totalReviewCount AS review_count
  , TRY_CAST(reviewRating AS INT8) AS review_rating
  , DATE_TRUNC('SECOND', TRY_CAST(registerDate AS TIMESTAMP)) AS register_dt
FROM {{ array }}
WHERE TRY_CAST(id AS BIGINT) IS NOT NULL;

-- BrandCatalog: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- BrandProduct: create
CREATE OR REPLACE TABLE {{ table }} (
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
  , register_dt TIMESTAMP
);

-- BrandProduct: select
SELECT
    TRY_CAST(id AS BIGINT) AS id
  , mallProductId AS product_id
  , TRY_CAST(catalogId AS BIGINT) AS catalog_id
  , name AS product_name
  , TRY_CAST(NULLIF(makerSeq, '0') AS BIGINT) AS maker_id
  , makerName AS maker_name
  , TRY_CAST(brandSeq AS BIGINT) AS brand_id
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
  , image.SRC AS image_url
  , TRY_CAST(lowestPrice AS INTEGER) AS sales_price
  , DATE_TRUNC('SECOND', TRY_CAST(registerDate AS TIMESTAMP)) AS register_dt
FROM {{ array }}
WHERE (TRY_CAST(id AS BIGINT) IS NOT NULL)
  AND (mallProductId IS NOT NULL);

-- BrandProduct: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- BrandPrice: create_price
CREATE OR REPLACE TABLE {{ table }} (
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
  , TRY_CAST(lowestPrice AS INTEGER) AS sales_price
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS created_at
FROM {{ array }}
WHERE (TRY_CAST(mallProductId AS BIGINT) IS NOT NULL)
  AND (TRY_CAST(lowestPrice AS INTEGER) IS NOT NULL);

-- BrandPrice: insert_price
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- BrandPrice: create_product
CREATE OR REPLACE TABLE {{ table }} (
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
  , TRY_CAST(lowestPrice AS INTEGER) AS sales_price
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
CREATE OR REPLACE TABLE {{ table }} (
    product_id BIGINT PRIMARY KEY
  , catalog_id BIGINT NOT NULL
  , created_at TIMESTAMP NOT NULL
);

-- ProductCatalog: select
SELECT
    TRY_CAST(mallProductId AS BIGINT) AS product_id
  , TRY_CAST(catalogId AS BIGINT) AS catalog_id
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS created_at
FROM {{ array }}
WHERE (TRY_CAST(mallProductId AS BIGINT) IS NOT NULL)
  AND (TRY_CAST(catalogId AS BIGINT) IS NOT NULL);

-- ProductCatalog: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
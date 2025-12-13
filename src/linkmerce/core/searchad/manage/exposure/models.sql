-- ExposureDiagnosis: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    keyword VARCHAR
  , display_rank SMALLINT
  , id BIGINT
  , product_name VARCHAR
  , is_own BOOLEAN
  , full_category_name VARCHAR
  , brand_name VARCHAR
  , maker_name VARCHAR
  , image_url VARCHAR
  , sales_price INTEGER
  , PRIMARY KEY (keyword, display_rank)
);

-- ExposureDiagnosis: select
SELECT
    $keyword AS keyword
  , rank AS display_rank
  , (CASE
      WHEN PREFIX(imageUrl, 'https://shopping-') THEN
        TRY_CAST(REGEXP_EXTRACT(imageUrl, '^https://[^/]+/main_\d+/(\d+)', 1) AS BIGINT)
      WHEN PREFIX(imageUrl, 'https://searchad-') THEN
        TRY_CAST(TRY_CAST(FROM_BASE64(REGEXP_EXTRACT(imageUrl, '^https://[^/]+/[^/]+/([^.]+)', 1)) AS VARCHAR) AS BIGINT)
      ELSE NULL END) AS id
  , productTitle AS product_name
  , isOwn AS is_own
  , categoryNames AS full_category_name
  , NULLIF(fmpBrand, '') AS brand_name
  , NULLIF(fmpMaker, '') AS maker_name
  , imageUrl AS image_url
  , TRY_CAST(COALESCE(lowPrice, mobileLowPrice) AS INTEGER) AS sales_price
FROM {{ array }}
WHERE ($is_own IS NULL) OR (isOwn = $is_own);

-- ExposureDiagnosis: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ExposureRank: create_rank
CREATE TABLE IF NOT EXISTS {{ table }} (
    keyword VARCHAR
  , id BIGINT
  , display_rank SMALLINT
  , created_at TIMESTAMP NOT NULL
  , PRIMARY KEY (keyword, display_rank)
);

-- ExposureRank: select_rank
SELECT exposure.*
FROM (
  SELECT
      $keyword AS keyword
    , (CASE
        WHEN PREFIX(imageUrl, 'https://shopping-') THEN
          TRY_CAST(REGEXP_EXTRACT(imageUrl, '^https://[^/]+/main_\d+/(\d+)', 1) AS BIGINT)
        WHEN PREFIX(imageUrl, 'https://searchad-') THEN
          TRY_CAST(TRY_CAST(FROM_BASE64(REGEXP_EXTRACT(imageUrl, '^https://[^/]+/[^/]+/([^.]+)', 1)) AS VARCHAR) AS BIGINT)
        ELSE NULL END) AS id
    , rank AS display_rank
    , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS created_at
  FROM {{ array }}
  WHERE ($is_own IS NULL) OR (isOwn = $is_own)
) AS exposure
WHERE exposure.id IS NOT NULL;

-- ExposureRank: insert_rank
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- ExposureRank: create_product
CREATE TABLE IF NOT EXISTS {{ table }} (
    id BIGINT PRIMARY KEY
  , product_id BIGINT NULL -- Placeholder
  , product_type TINYINT -- {0: '가격비교 상품', 1: '일반상품', 3: '광고상품'}
  , product_name VARCHAR
  , category_id INTEGER NULL -- Placeholder
  , full_category_name VARCHAR
  , mall_name VARCHAR NULL -- Placeholder
  , brand_name VARCHAR
  , sales_price INTEGER
  , updated_at TIMESTAMP NOT NULL
);

-- ExposureRank: select_product
SELECT product.*
FROM (
  SELECT
      (CASE
        WHEN PREFIX(imageUrl, 'https://shopping-') THEN
          TRY_CAST(REGEXP_EXTRACT(imageUrl, '^https://[^/]+/main_\d+/(\d+)', 1) AS BIGINT)
        WHEN PREFIX(imageUrl, 'https://searchad-') THEN
          TRY_CAST(TRY_CAST(FROM_BASE64(REGEXP_EXTRACT(imageUrl, '^https://[^/]+/[^/]+/([^.]+)', 1)) AS VARCHAR) AS BIGINT)
        ELSE NULL END) AS id
    , NULL AS product_id
    , IF(PREFIX(imageUrl, 'https://shopping-'), 0, 3) AS product_type
    , productTitle AS product_name
    , NULL AS category_id
    , categoryNames AS full_category_name
    , NULL AS mall_name
    , NULLIF(fmpBrand, '') AS brand_name
    , TRY_CAST(COALESCE(lowPrice, mobileLowPrice) AS INTEGER) AS sales_price
    , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updated_at
  FROM {{ array }}
  WHERE ($is_own IS NULL) OR (isOwn = $is_own)
) AS product
WHERE product.id IS NOT NULL;

-- ExposureRank: upsert_product
INSERT INTO {{ table }} {{ values }}
ON CONFLICT DO UPDATE SET
    product_name = COALESCE(excluded.product_name, product_name)
  , full_category_name = COALESCE(excluded.full_category_name, full_category_name)
  , mall_name = COALESCE(excluded.mall_name, mall_name)
  , updated_at = excluded.updated_at;
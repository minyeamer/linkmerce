-- ExposureDiagnosis: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , displayRank SMALLINT
  , productId BIGINT
  , productName VARCHAR
  , isOwn BOOLEAN
  , wholeCategoryName VARCHAR
  , mallName VARCHAR
  , makerName VARCHAR
  , imageUrl VARCHAR
  , salesPrice INTEGER
  , PRIMARY KEY (keyword, displayRank)
);

-- ExposureDiagnosis: select
SELECT
    $keyword AS keyword
  , rank AS displayRank
  , (CASE
      WHEN PREFIX(imageUrl, 'https://shopping-') THEN
        TRY_CAST(REGEXP_EXTRACT(imageUrl, '^https://[^/]+/main_\d+/(\d+)', 1) AS BIGINT)
      WHEN PREFIX(imageUrl, 'https://searchad-') THEN
        TRY_CAST(TRY_CAST(FROM_BASE64(REGEXP_EXTRACT(imageUrl, '^https://[^/]+/[^/]+/([^.]+)', 1)) AS VARCHAR) AS BIGINT)
      ELSE NULL END) AS productId
  , productTitle AS productName
  , isOwn
  , categoryNames AS wholeCategoryName
  , NULLIF(fmpBrand, '') AS mallName
  , NULLIF(fmpMaker, '') AS makerName
  , imageUrl
  , TRY_CAST(COALESCE(lowPrice, mobileLowPrice) AS INTEGER) AS salesPrice
FROM {{ array }}
WHERE ($is_own IS NULL) OR (isOwn = $is_own);

-- ExposureDiagnosis: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ExposureRank: create_rank
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , productId BIGINT
  , displayRank SMALLINT
  , createdAt TIMESTAMP NOT NULL
  , PRIMARY KEY (keyword, productId)
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
        ELSE NULL END) AS productId
    , rank AS displayRank
    , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS createdAt
  FROM {{ array }}
  WHERE ($is_own IS NULL) OR (isOwn = $is_own)
) AS exposure
WHERE exposure.productId IS NOT NULL;

-- ExposureRank: insert_rank
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- ExposureRank: create_product
CREATE OR REPLACE TABLE {{ table }} (
    productId BIGINT PRIMARY KEY
  , isNvMid BOOLEAN
  , mallPid BIGINT
  , productName VARCHAR
  , wholeCategoryName VARCHAR
  , mallName VARCHAR
  , salesPrice INTEGER
  , updatedAt TIMESTAMP NOT NULL
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
        ELSE NULL END) AS productId
    , PREFIX(imageUrl, 'https://shopping-') AS isNvMid
    , NULL AS mallPid
    , productTitle AS productName
    , categoryNames AS wholeCategoryName
    , NULLIF(fmpBrand, '') AS mallName
    , TRY_CAST(COALESCE(lowPrice, mobileLowPrice) AS INTEGER) AS salesPrice
    , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updatedAt
  FROM {{ array }}
  WHERE ($is_own IS NULL) OR (isOwn = $is_own)
) AS product
WHERE product.productId IS NOT NULL;

-- ExposureRank: upsert_product
INSERT INTO {{ table }} {{ values }}
ON CONFLICT DO UPDATE SET
    productName = COALESCE(excluded.productName, productName)
  , wholeCategoryName = COALESCE(excluded.wholeCategoryName, wholeCategoryName)
  , mallName = COALESCE(excluded.mallName, mallName)
  , updatedAt = excluded.updatedAt;
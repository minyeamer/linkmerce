-- BrandCatalog: create
CREATE OR REPLACE TABLE {{ table }} (
    nvMid BIGINT PRIMARY KEY
  , catalogName VARCHAR
  , makerId BIGINT
  , makerName VARCHAR
  , brandId BIGINT
  , brandName VARCHAR
  , categoryId INTEGER
  , categoryName VARCHAR
  , categoryId1 INTEGER
  , categoryName1 VARCHAR
  , categoryId2 INTEGER
  , categoryName2 VARCHAR
  , categoryId3 INTEGER
  , categoryName3 VARCHAR
  , categoryId4 INTEGER
  , categoryName4 VARCHAR
  , imageUrl VARCHAR
  , salesPrice INTEGER
  , productCount INTEGER
  , reviewCount INTEGER
  , reviewRating TINYINT
  , registerTime TIMESTAMP
);

-- BrandCatalog: select
SELECT
    TRY_CAST(id AS BIGINT) AS nvMid
  , name AS catalogName
  , TRY_CAST(NULLIF(makerSeq, '0') AS BIGINT) AS makerId
  , makerName
  , TRY_CAST(brandSeq AS BIGINT) AS brandId
  , brandName
  , TRY_CAST(categoryId AS INTEGER) AS categoryId
  , categoryName
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 1) AS INTEGER) AS categoryId1
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 1), '') AS categoryName1
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 2) AS INTEGER) AS categoryId2
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 2), '') AS categoryName2
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 3) AS INTEGER) AS categoryId3
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 3), '') AS categoryName3
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 4) AS INTEGER) AS categoryId4
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 4), '') AS categoryName4
  , image.SRC AS imageUrl
  , TRY_CAST(lowestPrice AS INTEGER) AS salesPrice
  , productCount
  , totalReviewCount AS reviewCount
  , TRY_CAST(reviewRating AS INT8) AS reviewRating
  , DATE_TRUNC('SECOND', TRY_CAST(registerDate AS TIMESTAMP)) AS registerTime
FROM {{ array }}
WHERE TRY_CAST(id AS BIGINT) IS NOT NULL;

-- BrandCatalog: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- BrandProduct: create
CREATE OR REPLACE TABLE {{ table }} (
    nvMid BIGINT PRIMARY KEY
  , mallPid VARCHAR NOT NULL
  , catalogId BIGINT
  , productName VARCHAR
  , makerId BIGINT
  , makerName VARCHAR
  , brandId BIGINT
  , brandName VARCHAR
  , mallSeq BIGINT
  , mallName VARCHAR
  , categoryId INTEGER
  , categoryName VARCHAR
  , categoryId1 INTEGER
  , categoryName1 VARCHAR
  , categoryId2 INTEGER
  , categoryName2 VARCHAR
  , categoryId3 INTEGER
  , categoryName3 VARCHAR
  , categoryId4 INTEGER
  , categoryName4 VARCHAR
  , mallPurl VARCHAR
  , imageUrl VARCHAR
  , salesPrice INTEGER
  , registerTime TIMESTAMP
);

-- BrandProduct: select
SELECT
    TRY_CAST(id AS BIGINT) AS nvMid
  , mallProductId AS mallPid
  , TRY_CAST(catalogId AS BIGINT) AS catalogId
  , name AS productName
  , TRY_CAST(NULLIF(makerSeq, '0') AS BIGINT) AS makerId
  , makerName
  , TRY_CAST(brandSeq AS BIGINT) AS brandId
  , brandName
  , TRY_CAST($mall_seq AS BIGINT) AS mallSeq
  , mallName
  , TRY_CAST(categoryId AS INTEGER) AS categoryId
  , categoryName
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 1) AS INTEGER) AS categoryId1
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 1), '') AS categoryName1
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 2) AS INTEGER) AS categoryId2
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 2), '') AS categoryName2
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 3) AS INTEGER) AS categoryId3
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 3), '') AS categoryName3
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 4) AS INTEGER) AS categoryId4
  , NULLIF(SPLIT_PART(fullCategoryName, '>', 4), '') AS categoryName4
  , outLinkUrl AS mallPurl
  , image.SRC AS imageUrl
  , TRY_CAST(lowestPrice AS INTEGER) AS salesPrice
  , DATE_TRUNC('SECOND', TRY_CAST(registerDate AS TIMESTAMP)) AS registerTime
FROM {{ array }}
WHERE (TRY_CAST(id AS BIGINT) IS NOT NULL)
  AND (mallProductId IS NOT NULL);

-- BrandProduct: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- BrandPrice: create_price
CREATE OR REPLACE TABLE {{ table }} (
    mallPid BIGINT PRIMARY KEY
  , mallSeq BIGINT
  , categoryId INTEGER
  , salesPrice INTEGER NOT NULL
  , updateDate DATE NOT NULL
);

-- BrandPrice: select_price
SELECT
    TRY_CAST(mallProductId AS BIGINT) AS mallPid
  , TRY_CAST($mall_seq AS BIGINT) AS mallSeq
  , TRY_CAST(categoryId AS INTEGER) AS categoryId
  , TRY_CAST(lowestPrice AS INTEGER) AS salesPrice
  , CAST((CURRENT_DATE - INTERVAL 1 DAY) AS DATE) AS updateDate
FROM {{ array }}
WHERE (TRY_CAST(mallProductId AS BIGINT) IS NOT NULL)
  AND (TRY_CAST(lowestPrice AS INTEGER) IS NOT NULL);

-- BrandPrice: insert_price
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- BrandPrice: create_product
CREATE OR REPLACE TABLE {{ table }} (
    mallPid BIGINT PRIMARY KEY
  , mallSeq BIGINT
  , categoryId INTEGER
  , categoryId3 INTEGER
  , productName VARCHAR
  , salesPrice INTEGER
  , registerDate DATE
  , updateDate DATE NOT NULL
);

-- BrandPrice: select_product
SELECT
    TRY_CAST(mallProductId AS BIGINT) AS mallPid
  , TRY_CAST($mall_seq AS BIGINT) AS mallSeq
  , TRY_CAST(categoryId AS INTEGER) AS categoryId
  , TRY_CAST(SPLIT_PART(fullCategoryId, '>', 3) AS INTEGER) AS categoryId3
  , name AS productName
  , TRY_CAST(lowestPrice AS INTEGER) AS salesPrice
  , TRY_CAST(registerDate AS DATE) AS registerDate
  , CURRENT_DATE AS updateDate
FROM {{ array }}
WHERE TRY_CAST(mallProductId AS BIGINT) IS NOT NULL;

-- BrandPrice: upsert_product
INSERT INTO {{ table }} {{ values }}
ON CONFLICT DO UPDATE SET
    categoryId = COALESCE(excluded.categoryId, categoryId)
  , categoryId3 = COALESCE(excluded.categoryId3, categoryId3)
  , productName = COALESCE(excluded.productName, productName)
  , salesPrice = COALESCE(excluded.salesPrice, salesPrice)
  , registerDate = COALESCE(excluded.registerDate, registerDate)
  , updateDate = excluded.updateDate;


-- MatchCatalog: create
CREATE OR REPLACE TABLE {{ table }} (
    mallPid BIGINT PRIMARY KEY
  , catalogId BIGINT
  , createdAt TIMESTAMP NOT NULL
);

-- MatchCatalog: select
SELECT
    TRY_CAST(mallProductId AS BIGINT) AS mallPid
  , TRY_CAST(catalogId AS BIGINT) AS catalogId
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS createdAt
FROM {{ array }}
WHERE (TRY_CAST(mallProductId AS BIGINT) IS NOT NULL)
  AND (TRY_CAST(catalogId AS BIGINT) IS NOT NULL);

-- MatchCatalog: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
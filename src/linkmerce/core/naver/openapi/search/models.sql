-- BlogSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , displayRank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , description VARCHAR
  , address VARCHAR
  , bloggerUrl VARCHAR
  , postDate DATE
  , PRIMARY KEY (keyword, displayRank)
);

-- BlogSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS displayRank
  , REGEXP_REPLACE(title, '<[^>]+>', '', 'g') AS title
  , link AS url
  , REGEXP_REPLACE(description, '<[^>]+>', '', 'g') AS description
  , bloggername AS address
  , bloggerlink AS bloggerUrl
  , TRY_CAST(TRY_STRPTIME(postdate, '%Y%m%d') AS DATE) AS postDate
FROM {{ array }};

-- BlogSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- NewsSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , displayRank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , description VARCHAR
  , postDate TIMESTAMP
  , PRIMARY KEY (keyword, displayRank)
);

-- NewsSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS displayRank
  , REGEXP_REPLACE(title, '<[^>]+>', '', 'g') AS title
  , originallink AS url
  , REGEXP_REPLACE(description, '<[^>]+>', '', 'g') AS description
  , TRY_CAST(TRY_STRPTIME(pubDate, '%a, %d %b %Y %H:%M:%S %z') AS TIMESTAMP) AS postDate
FROM {{ array }};

-- NewsSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- BookSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , displayRank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , description VARCHAR
  , imageUrl VARCHAR
  , author VARCHAR
  , salesPrice INTEGER
  , publisher VARCHAR
  , isbn BIGINT
  , publishDate DATE
  , PRIMARY KEY (keyword, displayRank)
);

-- BookSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS displayRank
  , title
  , link AS url
  , NULLIF(description, '') AS description
  , image AS imageUrl
  , NULLIF(author, '') AS author
  , TRY_CAST(discount AS INTEGER) AS salesPrice
  , publisher AS publisher
  , TRY_CAST(isbn AS BIGINT) AS isbn
  , TRY_CAST(TRY_STRPTIME(pubdate, '%Y%m%d') AS DATE) AS publishDate
FROM {{ array }};

-- BookSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- CafeSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , displayRank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , description VARCHAR
  , cafeUrl VARCHAR
  , PRIMARY KEY (keyword, displayRank)
);

-- CafeSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS displayRank
  , title
  , link AS url
  , description
  , cafename AS address
  , cafeurl AS cafeUrl
FROM {{ array }};

-- CafeSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- KiNSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , displayRank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , description VARCHAR
  , PRIMARY KEY (keyword, displayRank)
);

-- KiNSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS displayRank
  , title
  , link AS url
  , description
FROM {{ array }};

-- KiNSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ImageSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , displayRank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , thumbnail VARCHAR
  , sizeheight INTEGER
  , sizewidth INTEGER
  , PRIMARY KEY (keyword, displayRank)
);

-- ImageSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS displayRank
  , title
  , link AS url
  , thumbnail
  , TRY_CAST(sizeheight AS BIGINT) AS sizeheight
  , TRY_CAST(sizewidth AS BIGINT) AS sizewidth
FROM {{ array }};

-- ImageSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ShoppingSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , displayRank SMALLINT
  , nvMid BIGINT
  , mallPid BIGINT
  , productName VARCHAR
  , productType TINYINT -- {0: "가격비교 상품", 1: "가격비교 비매칭 일반상품", 2: "가격비교 매칭 일반상품"}
  , mallName VARCHAR
  , nvMurl VARCHAR
  , mallPurl VARCHAR
  , brandName VARCHAR
  , makerName VARCHAR
  , categoryName1 VARCHAR
  , categoryName2 VARCHAR
  , categoryName3 VARCHAR
  , categoryName4 VARCHAR
  , imageUrl VARCHAR
  , salesPrice INTEGER
  , PRIMARY KEY (keyword, displayRank)
);

-- ShoppingSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS displayRank
  , TRY_CAST(productId AS BIGINT) AS nvMid
  , TRY_CAST(REGEXP_EXTRACT(link, '/products/(\d+)$', 1) AS BIGINT) AS mallPid
  , REGEXP_REPLACE(title, '<[^>]+>', '', 'g') AS productName
  , ((TRY_CAST(productType AS TINYINT) + 2) % 3) AS productType
  , NULLIF(mallName, '네이버') AS mallName
  , IF(link LIKE '%/catalog/%', link, NULL) AS nvMurl
  , IF(link LIKE '%/catalog/%', NULL, link) AS mallPurl
  , NULLIF(brand, '') AS brandName
  , maker AS makerName
  , category1 AS categoryName1
  , category2 AS categoryName2
  , category3 AS categoryName3
  , category4 AS categoryName4
  , image AS imageUrl
  , TRY_CAST(lprice AS INTEGER) AS salesPrice
FROM {{ array }};

-- ShoppingSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ShoppingRank: create_rank
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , nvMid BIGINT
  , mallPid BIGINT
  , productType TINYINT -- {0: "가격비교 상품", 1: "가격비교 비매칭 일반상품", 2: "가격비교 매칭 일반상품"}
  , displayRank SMALLINT
  , createdAt TIMESTAMP NOT NULL
  , PRIMARY KEY (keyword, nvMid)
);

-- ShoppingRank: select_rank
SELECT
    $keyword AS keyword
  , TRY_CAST(productId AS BIGINT) AS nvMid
  , TRY_CAST(REGEXP_EXTRACT(link, '/products/(\d+)$', 1) AS BIGINT) AS mallPid
  , ((TRY_CAST(productType AS TINYINT) + 2) % 3) AS productType
  , (ROW_NUMBER() OVER () + $start) AS displayRank
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS createdAt
FROM {{ array }}
WHERE TRY_CAST(productId AS BIGINT) IS NOT NULL;

-- ShoppingRank: insert_rank
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- ShoppingRank: create_product
CREATE OR REPLACE TABLE {{ table }} (
    nvMid BIGINT PRIMARY KEY
  , mallPid BIGINT
  , productName VARCHAR
  , wholeCategoryName VARCHAR
  , mallName VARCHAR
  , brandName VARCHAR
  , updatedAt TIMESTAMP NOT NULL
);

-- ShoppingRank: select_product
SELECT
    TRY_CAST(productId AS BIGINT) AS nvMid
  , TRY_CAST(REGEXP_EXTRACT(link, '/products/(\d+)$', 1) AS BIGINT) AS mallPid
  , REGEXP_REPLACE(title, '<[^>]+>', '', 'g') AS productName
  , CONCAT_WS('>', category1, category2, category3, category4) AS wholeCategoryName
  , NULLIF(mallName, '네이버') AS mallName
  , NULLIF(brand, '') AS brandName
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updatedAt
FROM {{ array }}
WHERE TRY_CAST(productId AS BIGINT) IS NOT NULL;

-- ShoppingRank: upsert_product
INSERT INTO {{ table }} {{ values }}
ON CONFLICT DO UPDATE SET
    mallPid = COALESCE(excluded.mallPid, mallPid)
  , productName = COALESCE(excluded.productName, productName)
  , wholeCategoryName = COALESCE(excluded.wholeCategoryName, wholeCategoryName)
  , mallName = COALESCE(excluded.mallName, mallName)
  , brandName = COALESCE(excluded.brandName, brandName)
  , updatedAt = excluded.updatedAt;
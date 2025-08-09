-- StoreSales: create
CREATE OR REPLACE TABLE {{ table }} (
    mallSeq BIGINT NOT NULL
  , paymentCount BIGINT
  , paymentAmount BIGINT
  , refundAmount BIGINT
  , paymentDate DATE NOT NULL
);

-- StoreSales: select
SELECT
    TRY_CAST($mall_seq AS BIGINT) AS mallSeq
  , sales.paymentCount AS paymentCount
  , sales.paymentAmount AS paymentAmount
  , sales.refundAmount AS refundAmount
  , TRY_CAST($end_date AS DATE) AS paymentDate
FROM {{ array }}
WHERE (TRY_CAST($mall_seq AS BIGINT) IS NOT NULL)
  AND (TRY_CAST($end_date AS DATE) IS NOT NULL);

-- StoreSales: insert
INSERT INTO {{ table }} {{ values }};


-- CategorySales: create
CREATE OR REPLACE TABLE {{ table }} (
    categoryId3 INTEGER NOT NULL
  , wholeCategoryName VARCHAR
  , mallSeq BIGINT
  , clickCount BIGINT
  , paymentCount BIGINT
  , paymentAmount BIGINT
  , paymentDate DATE NOT NULL
);

-- CategorySales: select
SELECT
    TRY_CAST(product.category.identifier AS INTEGER) AS categoryId3
  , product.category.fullName AS wholeCategoryName
  , TRY_CAST($mall_seq AS BIGINT) AS mallSeq
  , visit.click AS clickCount
  , sales.paymentCount AS paymentCount
  , sales.paymentAmount AS paymentAmount
  , TRY_CAST($end_date AS DATE) AS paymentDate
FROM {{ array }}
WHERE (TRY_CAST(product.category.identifier AS INTEGER) IS NOT NULL)
  AND (TRY_CAST($end_date AS DATE) IS NOT NULL);

-- CategorySales: insert
INSERT INTO {{ table }} {{ values }};


-- ProductSales: create
CREATE OR REPLACE TABLE {{ table }} (
    mallPid BIGINT NOT NULL
  , productName VARCHAR
  , mallSeq BIGINT
  , categoryId3 INTEGER
  , categoryName3 VARCHAR
  , wholeCategoryName VARCHAR
  , clickCount BIGINT
  , paymentCount BIGINT
  , paymentAmount BIGINT
  , paymentDate DATE NOT NULL
);

-- ProductSales: select
SELECT
    TRY_CAST(product.identifier AS BIGINT) AS mallPid
  , product.name AS productName
  , TRY_CAST($mall_seq AS BIGINT) AS mallSeq
  , TRY_CAST(product.category.identifier AS INTEGER) AS categoryId3
  , product.category.name AS categoryName3
  , product.category.fullName AS wholeCategoryName
  , visit.click AS clickCount
  , sales.paymentCount AS paymentCount
  , sales.paymentAmount AS paymentAmount
  , TRY_CAST($end_date AS DATE) AS paymentDate
FROM {{ array }}
WHERE (TRY_CAST(product.identifier AS BIGINT) IS NOT NULL)
  AND (TRY_CAST($end_date AS DATE) IS NOT NULL);

-- ProductSales: insert
INSERT INTO {{ table }} {{ values }};


-- AggregatedSales: create_sales
CREATE OR REPLACE TABLE {{ table }} (
    mallPid BIGINT
  , mallSeq BIGINT
  , categoryId3 INTEGER
  , clickCount BIGINT
  , paymentCount BIGINT
  , paymentAmount BIGINT
  , paymentDate DATE
  , PRIMARY KEY (mallPid, paymentDate)
);

-- AggregatedSales: select_sales
SELECT
    sales.mallPid
  , MAX(sales.mallSeq) AS mallSeq
  , MAX(sales.categoryId3) AS categoryId3
  , SUM(sales.clickCount) AS clickCount
  , SUM(sales.paymentCount) AS paymentCount
  , SUM(sales.paymentAmount) AS paymentAmount
  , sales.paymentDate
FROM (
  SELECT
      TRY_CAST(product.identifier AS BIGINT) AS mallPid
    , TRY_CAST($mall_seq AS BIGINT) AS mallSeq
    , TRY_CAST(product.category.identifier AS INTEGER) AS categoryId3
    , visit.click AS clickCount
    , sales.paymentCount AS paymentCount
    , sales.paymentAmount AS paymentAmount
    , CAST($end_date AS DATE) AS paymentDate
  FROM {{ array }}
  WHERE (TRY_CAST(product.identifier AS BIGINT) IS NOT NULL)
    AND (TRY_CAST($end_date AS DATE) IS NOT NULL)
) AS sales
GROUP BY sales.mallPid, sales.paymentDate;

-- AggregatedSales: insert_sales
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- AggregatedSales: create_product
CREATE OR REPLACE TABLE {{ table }} (
    mallPid BIGINT PRIMARY KEY
  , mallSeq BIGINT
  , categoryId INTEGER
  , categoryId3 INTEGER
  , productName VARCHAR
  , salesPrice INTEGER
  , registerDate DATE
  , updateDate DATE
);

-- AggregatedSales: select_product
SELECT sales.* EXCLUDE (seq)
FROM (
  SELECT
      TRY_CAST(product.identifier AS BIGINT) AS mallPid
    , TRY_CAST($mall_seq AS BIGINT) AS mallSeq
    , NULL AS categoryId
    , TRY_CAST(product.category.identifier AS INTEGER) AS categoryId3
    , product.name AS productName
    , NULL AS salesPrice
    , $start_date AS registerDate
    , CURRENT_DATE AS updateDate
    , ROW_NUMBER() OVER (PARTITION BY product.identifier) AS seq
  FROM {{ array }}
  WHERE TRY_CAST(product.identifier AS BIGINT) IS NOT NULL
) AS sales
WHERE sales.seq = 1;

-- AggregatedSales: upsert_product
INSERT INTO {{ table }} {{ values }}
ON CONFLICT DO UPDATE SET
    categoryId = COALESCE(excluded.categoryId, categoryId)
  , categoryId3 = COALESCE(excluded.categoryId3, categoryId3)
  , productName = COALESCE(excluded.productName, productName)
  , salesPrice = COALESCE(excluded.salesPrice, salesPrice)
  , registerDate = COALESCE(excluded.registerDate, registerDate)
  , updateDate = excluded.updateDate;
-- StoreSales: create
CREATE OR REPLACE TABLE {{ table }} (
    mall_seq BIGINT NOT NULL
  , payment_count BIGINT
  , payment_amount BIGINT
  , refund_amount BIGINT
  , payment_date DATE NOT NULL
);

-- StoreSales: select
SELECT
    TRY_CAST($mall_seq AS BIGINT) AS mall_seq
  , sales.paymentCount AS payment_count
  , sales.paymentAmount AS payment_amount
  , sales.refundAmount AS refund_amount
  , TRY_CAST($end_date AS DATE) AS payment_date
FROM {{ array }}
WHERE (TRY_CAST($mall_seq AS BIGINT) IS NOT NULL)
  AND (TRY_CAST($end_date AS DATE) IS NOT NULL);

-- StoreSales: insert
INSERT INTO {{ table }} {{ values }};


-- CategorySales: create
CREATE OR REPLACE TABLE {{ table }} (
    category_id3 INTEGER NOT NULL
  , full_category_name VARCHAR
  , mall_seq BIGINT
  , click_count BIGINT
  , payment_count BIGINT
  , payment_amount BIGINT
  , payment_date DATE NOT NULL
);

-- CategorySales: select
SELECT
    TRY_CAST(product.category.identifier AS INTEGER) AS category_id3
  , product.category.fullName AS full_category_name
  , TRY_CAST($mall_seq AS BIGINT) AS mall_seq
  , visit.click AS click_count
  , sales.paymentCount AS payment_count
  , sales.paymentAmount AS payment_amount
  , TRY_CAST($end_date AS DATE) AS payment_date
FROM {{ array }}
WHERE (TRY_CAST(product.category.identifier AS INTEGER) IS NOT NULL)
  AND (TRY_CAST($end_date AS DATE) IS NOT NULL);

-- CategorySales: insert
INSERT INTO {{ table }} {{ values }};


-- ProductSales: create
CREATE OR REPLACE TABLE {{ table }} (
    product_id BIGINT NOT NULL
  , product_name VARCHAR
  , mall_seq BIGINT
  , category_id3 INTEGER
  , category_name3 VARCHAR
  , full_category_name VARCHAR
  , click_count BIGINT
  , payment_count BIGINT
  , payment_amount BIGINT
  , payment_date DATE NOT NULL
);

-- ProductSales: select
SELECT
    TRY_CAST(product.identifier AS BIGINT) AS product_id
  , product.name AS product_name
  , TRY_CAST($mall_seq AS BIGINT) AS mall_seq
  , TRY_CAST(product.category.identifier AS INTEGER) AS category_id3
  , product.category.name AS category_name3
  , product.category.fullName AS full_category_name
  , visit.click AS click_count
  , sales.paymentCount AS payment_count
  , sales.paymentAmount AS payment_amount
  , TRY_CAST($end_date AS DATE) AS payment_date
FROM {{ array }}
WHERE (TRY_CAST(product.identifier AS BIGINT) IS NOT NULL)
  AND (TRY_CAST($end_date AS DATE) IS NOT NULL);

-- ProductSales: insert
INSERT INTO {{ table }} {{ values }};


-- AggregatedSales: create_sales
CREATE OR REPLACE TABLE {{ table }} (
    product_id BIGINT
  , mall_seq BIGINT
  , category_id3 INTEGER
  , click_count BIGINT
  , payment_count BIGINT
  , payment_amount BIGINT
  , payment_date DATE
  , PRIMARY KEY (product_id, payment_date)
);

-- AggregatedSales: select_sales
SELECT
    sales.product_id
  , MAX(sales.mall_seq) AS mall_seq
  , MAX(sales.category_id3) AS category_id3
  , SUM(sales.click_count) AS click_count
  , SUM(sales.payment_count) AS payment_count
  , SUM(sales.payment_amount) AS payment_amount
  , sales.payment_date
FROM (
  SELECT DISTINCT
      TRY_CAST(product.identifier AS BIGINT) AS product_id
    , TRY_CAST($mall_seq AS BIGINT) AS mall_seq
    , TRY_CAST(product.category.identifier AS INTEGER) AS category_id3
    , visit.click AS click_count
    , sales.paymentCount AS payment_count
    , sales.paymentAmount AS payment_amount
    , CAST($end_date AS DATE) AS payment_date
  FROM {{ array }}
  WHERE (TRY_CAST(product.identifier AS BIGINT) IS NOT NULL)
    AND (TRY_CAST($end_date AS DATE) IS NOT NULL)
) AS sales
GROUP BY sales.product_id, sales.payment_date;

-- AggregatedSales: insert_sales
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- AggregatedSales: create_product
CREATE OR REPLACE TABLE {{ table }} (
    product_id BIGINT PRIMARY KEY
  , mall_seq BIGINT
  , category_id INTEGER
  , category_id3 INTEGER NULL -- Placeholder
  , product_name VARCHAR
  , sales_price INTEGER NULL -- Placeholder
  , register_date DATE
  , update_date DATE
);

-- AggregatedSales: select_product
SELECT
    TRY_CAST(product.identifier AS BIGINT) AS product_id
  , TRY_CAST($mall_seq AS BIGINT) AS mall_seq
  , NULL AS category_id
  , TRY_CAST(product.category.identifier AS INTEGER) AS category_id3
  , product.name AS product_name
  , NULL AS sales_price
  , $start_date AS register_date
  , $start_date AS update_date
FROM {{ array }}
WHERE TRY_CAST(product.identifier AS BIGINT) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY product.identifier) = 1;

-- AggregatedSales: upsert_product
INSERT INTO {{ table }} {{ values }}
ON CONFLICT DO UPDATE SET
    category_id = COALESCE(excluded.category_id, category_id)
  , category_id3 = COALESCE(excluded.category_id3, category_id3)
  , product_name = COALESCE(excluded.product_name, product_name)
  , sales_price = COALESCE(excluded.sales_price, sales_price)
  , register_date = LEAST(excluded.register_date, register_date)
  , update_date = GREATEST(excluded.update_date, update_date);
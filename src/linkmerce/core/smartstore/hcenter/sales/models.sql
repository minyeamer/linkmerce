-- StoreSales: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    mall_seq BIGINT NOT NULL
  , payment_count BIGINT
  , payment_amount BIGINT
  , refund_amount BIGINT
  , payment_date DATE NOT NULL
  , PRIMARY KEY (payment_date, mall_seq)
);

-- StoreSales: bulk_insert
INSERT INTO {{ table }}
SELECT
    $mall_seq AS mall_seq
  , sales.paymentCount AS payment_count
  , sales.paymentAmount AS payment_amount
  , sales.refundAmount AS refund_amount
  , $end_date AS payment_date
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- CategorySales: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    category_id3 INTEGER NOT NULL
  , full_category_name VARCHAR
  , mall_seq BIGINT
  , click_count BIGINT
  , payment_count BIGINT
  , payment_amount BIGINT
  , payment_date DATE NOT NULL
  , PRIMARY KEY (payment_date, category_id3)
);

-- CategorySales: bulk_insert
INSERT INTO {{ table }}
SELECT
    CAST(product.category.identifier AS INTEGER) AS category_id3
  , product.category.fullName AS full_category_name
  , $mall_seq AS mall_seq
  , visit.click AS click_count
  , sales.paymentCount AS payment_count
  , sales.paymentAmount AS payment_amount
  , $end_date AS payment_date
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- ProductSales: create
CREATE TABLE IF NOT EXISTS {{ table }} (
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
  , PRIMARY KEY (payment_date, product_id)
);

-- ProductSales: bulk_insert
INSERT INTO {{ table }}
SELECT
    CAST(product.identifier AS BIGINT) AS product_id
  , product.name AS product_name
  , $mall_seq AS mall_seq
  , TRY_CAST(product.category.identifier AS INTEGER) AS category_id3
  , product.category.name AS category_name3
  , product.category.fullName AS full_category_name
  , visit.click AS click_count
  , sales.paymentCount AS payment_count
  , sales.paymentAmount AS payment_amount
  , $end_date AS payment_date
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- AggregatedSales: create
CREATE TABLE IF NOT EXISTS {{ sales }} (
    product_id BIGINT NOT NULL
  , mall_seq BIGINT
  , category_id3 INTEGER
  , click_count BIGINT
  , payment_count BIGINT
  , payment_amount BIGINT
  , payment_date DATE NOT NULL
  , PRIMARY KEY (payment_date, product_id)
);

CREATE TABLE IF NOT EXISTS {{ product }} (
    product_id BIGINT NOT NULL
  , mall_seq BIGINT
  , category_id INTEGER NULL -- Placeholder
  , category_id3 INTEGER
  , product_name VARCHAR
  , sales_price INTEGER NULL -- Placeholder
  , register_date DATE
  , update_date DATE
  , PRIMARY KEY (product_id)
);

-- AggregatedSales: bulk_insert
INSERT INTO {{ sales }}
SELECT
    items.product_id
  , MAX(items.mall_seq) AS mall_seq
  , MAX(items.category_id3) AS category_id3
  , SUM(items.click_count) AS click_count
  , SUM(items.payment_count) AS payment_count
  , SUM(items.payment_amount) AS payment_amount
  , items.payment_date
FROM (
  SELECT DISTINCT
      CAST(product.identifier AS BIGINT) AS product_id
    , $mall_seq AS mall_seq
    , TRY_CAST(product.category.identifier AS INTEGER) AS category_id3
    , visit.click AS click_count
    , sales.paymentCount AS payment_count
    , sales.paymentAmount AS payment_amount
    , $end_date AS payment_date
  FROM {{ rows }}
) AS items
GROUP BY items.product_id, items.payment_date
ON CONFLICT DO NOTHING;

INSERT INTO {{ product }}
SELECT
    CAST(product.identifier AS BIGINT) AS product_id
  , $mall_seq AS mall_seq
  , NULL AS category_id
  , TRY_CAST(product.category.identifier AS INTEGER) AS category_id3
  , product.name AS product_name
  , NULL AS sales_price
  , $start_date AS register_date
  , $end_date AS update_date
FROM {{ rows }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY product.identifier) = 1
ON CONFLICT DO UPDATE SET
    category_id = COALESCE(EXCLUDED.category_id, category_id)
  , category_id3 = COALESCE(EXCLUDED.category_id3, category_id3)
  , product_name = COALESCE(EXCLUDED.product_name, product_name)
  , sales_price = COALESCE(EXCLUDED.sales_price, sales_price)
  , register_date = LEAST(EXCLUDED.register_date, register_date)
  , update_date = GREATEST(EXCLUDED.update_date, update_date);
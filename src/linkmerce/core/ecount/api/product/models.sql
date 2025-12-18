-- Product: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_code VARCHAR PRIMARY KEY
  , option_id VARCHAR
  , product_name VARCHAR
  , remarks_name VARCHAR
  , brand_name VARCHAR
  , unit_quantity INTEGER
  , unit_name VARCHAR
  , org_price INTEGER
  , expiration_date VARCHAR
  , updated_at TIMESTAMP
);

-- Product: select
SELECT
    PROD_CD AS product_code
  , CONT4 AS option_id
  , PROD_DES AS product_name
  , REMARKS_WIN AS remarks_name
  , CONT1 AS brand_name
  , TRY_CAST(SIZE_DES AS INTEGER) AS unit_quantity
  , UNIT AS unit_name
  , TRY_CAST(IN_PRICE AS INTEGER) AS org_price
  , COALESCE(NULLIF(CONT2, '0'), NULLIF(CONT3, '0')) AS expiration_date
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updated_at
FROM {{ array }};

-- Product: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
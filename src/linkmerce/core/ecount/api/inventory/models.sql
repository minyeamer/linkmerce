-- Inventory: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_code VARCHAR PRIMARY KEY
  , quantity INTEGER
  , updated_at TIMESTAMP
);

-- Inventory: select
SELECT
    PROD_CD AS product_code
  , BAL_QTY AS quantity
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updated_at
FROM {{ array }};

-- Inventory: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
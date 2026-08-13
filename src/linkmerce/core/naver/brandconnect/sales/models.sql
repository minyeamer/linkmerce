-- SalesPerformances: create
CREATE TABLE IF NOT EXISTS {{ sales }} (
    space_id BIGINT NOT NULL
  , product_id BIGINT NOT NULL
  , sales_count INTEGER
  , sales_amount BIGINT
  , commission_amount BIGINT
  , order_date DATE NOT NULL
  , PRIMARY KEY (order_date, space_id, product_id)
);

CREATE TABLE IF NOT EXISTS {{ product }} (
    space_id BIGINT NOT NULL
  , product_id BIGINT NOT NULL
  , mall_product_id BIGINT NULL -- Placeholder
  , product_name VARCHAR
  , image_url VARCHAR
  , PRIMARY KEY (space_id, product_id)
);

-- SalesPerformances: bulk_insert
INSERT INTO {{ sales }}
SELECT
    CAST($space_id AS BIGINT) AS space_id
  , smartStoreProductId AS product_id
  , salesCnt AS sales_count
  , salesAmount AS sales_amount
  , commissionAmount AS commission_amount
  , CAST($end_date AS DATE) AS order_date
FROM {{ rows }}
WHERE smartStoreProductId IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO {{ product }}
SELECT
    CAST($space_id AS BIGINT) AS space_id
  , smartStoreProductId AS product_id
  , NULL AS mall_product_id
  , productName AS product_name
  , productImageUrl AS image_url
FROM {{ rows }}
WHERE smartStoreProductId IS NOT NULL
ON CONFLICT DO UPDATE SET
    product_name = COALESCE(EXCLUDED.product_name, product_name)
  , image_url = COALESCE(EXCLUDED.image_url, image_url);
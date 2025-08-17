-- ShoppingPage: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR PRIMARY KEY
  , page_unit_ad INTEGER
  , page_unit_shop INTEGER
  , created_at TIMESTAMP NOT NULL
);

-- ShoppingPage: select
SELECT
    $keyword AS keyword
  , COUNT(CASE WHEN cardType = 'AD_CARD' THEN 1 END) AS page_unit_ad
  , COUNT(CASE WHEN cardType <> 'AD_CARD' THEN 1 END) AS page_unit_shop
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS created_at
FROM {{ array }};

-- ShoppingPage: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

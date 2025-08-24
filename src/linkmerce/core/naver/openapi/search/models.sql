-- BlogSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , display_rank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , description VARCHAR
  , address VARCHAR
  , blogger_url VARCHAR
  , post_date DATE
  , PRIMARY KEY (keyword, display_rank)
);

-- BlogSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS display_rank
  , REGEXP_REPLACE(title, '<[^>]+>', '', 'g') AS title
  , link AS url
  , REGEXP_REPLACE(description, '<[^>]+>', '', 'g') AS description
  , bloggername AS address
  , bloggerlink AS blogger_url
  , TRY_CAST(TRY_STRPTIME(postdate, '%Y%m%d') AS DATE) AS post_date
FROM {{ array }};

-- BlogSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- NewsSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , display_rank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , description VARCHAR
  , publish_dt TIMESTAMP
  , PRIMARY KEY (keyword, display_rank)
);

-- NewsSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS display_rank
  , REGEXP_REPLACE(title, '<[^>]+>', '', 'g') AS title
  , originallink AS url
  , REGEXP_REPLACE(description, '<[^>]+>', '', 'g') AS description
  , TRY_CAST(TRY_STRPTIME(pubDate, '%a, %d %b %Y %H:%M:%S %z') AS TIMESTAMP) AS publish_dt
FROM {{ array }};

-- NewsSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- BookSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , display_rank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , description VARCHAR
  , image_url VARCHAR
  , author VARCHAR
  , sales_price INTEGER
  , publisher VARCHAR
  , isbn BIGINT
  , publish_date DATE
  , PRIMARY KEY (keyword, display_rank)
);

-- BookSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS display_rank
  , title
  , link AS url
  , NULLIF(description, '') AS description
  , image AS image_url
  , NULLIF(author, '') AS author
  , TRY_CAST(discount AS INTEGER) AS sales_price
  , publisher
  , TRY_CAST(isbn AS BIGINT) AS isbn
  , TRY_CAST(TRY_STRPTIME(pubdate, '%Y%m%d') AS DATE) AS publish_date
FROM {{ array }};

-- BookSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- CafeSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , display_rank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , description VARCHAR
  , address VARCHAR
  , cafe_url VARCHAR
  , PRIMARY KEY (keyword, display_rank)
);

-- CafeSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS display_rank
  , title
  , link AS url
  , description
  , cafename AS address
  , cafeurl AS cafe_url
FROM {{ array }};

-- CafeSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- KiNSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , display_rank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , description VARCHAR
  , PRIMARY KEY (keyword, display_rank)
);

-- KiNSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS display_rank
  , title
  , link AS url
  , description
FROM {{ array }};

-- KiNSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ImageSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , display_rank SMALLINT
  , title VARCHAR
  , url VARCHAR
  , thumbnail VARCHAR
  , size_height INTEGER
  , size_width INTEGER
  , PRIMARY KEY (keyword, display_rank)
);

-- ImageSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS display_rank
  , title
  , link AS url
  , thumbnail
  , TRY_CAST(sizeheight AS BIGINT) AS size_height
  , TRY_CAST(sizewidth AS BIGINT) AS size_width
FROM {{ array }};

-- ImageSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ShoppingSearch: create
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , display_rank SMALLINT
  , id BIGINT
  , product_id BIGINT
  , product_name VARCHAR
  , product_type TINYINT -- {0: '가격비교 상품', 1: '가격비교 비매칭 일반상품', 2: '가격비교 매칭 일반상품'}
  , mall_name VARCHAR
  , url VARCHAR
  , brand_name VARCHAR
  , maker_name VARCHAR
  , category_name1 VARCHAR
  , category_name2 VARCHAR
  , category_name3 VARCHAR
  , category_name4 VARCHAR
  , image_url VARCHAR
  , sales_price INTEGER
  , PRIMARY KEY (keyword, display_rank)
);

-- ShoppingSearch: select
SELECT
    $keyword AS keyword
  , (ROW_NUMBER() OVER () + $start) AS display_rank
  , TRY_CAST(productId AS BIGINT) AS id
  , TRY_CAST(REGEXP_EXTRACT(link, '/products/(\d+)$', 1) AS BIGINT) AS product_id
  , REGEXP_REPLACE(title, '<[^>]+>', '', 'g') AS product_name
  , ((TRY_CAST(productType AS TINYINT) + 2) % 3) AS product_type
  , NULLIF(mallName, '네이버') AS mall_name
  , link AS url
  , NULLIF(brand, '') AS brand_name
  , maker AS maker_name
  , category1 AS category_name1
  , category2 AS category_name2
  , category3 AS category_name3
  , category4 AS category_name4
  , image AS image_url
  , TRY_CAST(lprice AS INTEGER) AS sales_price
FROM {{ array }};

-- ShoppingSearch: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ShoppingRank: create_rank
CREATE OR REPLACE TABLE {{ table }} (
    keyword VARCHAR
  , id BIGINT
  , product_id BIGINT
  , product_type TINYINT -- {0: '가격비교 상품', 1: '가격비교 비매칭 일반상품', 2: '가격비교 매칭 일반상품', 3: '광고상품'}
  , display_rank SMALLINT
  , created_at TIMESTAMP NOT NULL
  , PRIMARY KEY (keyword, display_rank)
);

-- ShoppingRank: select_rank
SELECT
    $keyword AS keyword
  , TRY_CAST(productId AS BIGINT) AS id
  , TRY_CAST(REGEXP_EXTRACT(link, '/products/(\d+)$', 1) AS BIGINT) AS product_id
  , ((TRY_CAST(productType AS TINYINT) + 2) % 3) AS product_type
  , (ROW_NUMBER() OVER () + $start) AS display_rank
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS created_at
FROM {{ array }}
WHERE TRY_CAST(productId AS BIGINT) IS NOT NULL;

-- ShoppingRank: insert_rank
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- ShoppingRank: create_product
CREATE OR REPLACE TABLE {{ table }} (
    id BIGINT PRIMARY KEY
  , product_id BIGINT
  , product_type TINYINT -- {0: '가격비교 상품', 1: '일반상품', 3: '광고상품'}
  , product_name VARCHAR
  , category_id INTEGER NULL -- Placeholder
  , full_category_name VARCHAR
  , mall_name VARCHAR
  , brand_name VARCHAR
  , sales_price INTEGER
  , updated_at TIMESTAMP NOT NULL
);

-- ShoppingRank: select_product
SELECT
    TRY_CAST(productId AS BIGINT) AS id
  , TRY_CAST(REGEXP_EXTRACT(link, '/products/(\d+)$', 1) AS BIGINT) AS product_id
  , IF(link LIKE '%/catalog/%', 0, 1) AS product_type
  , REGEXP_REPLACE(title, '<[^>]+>', '', 'g') AS product_name
  , NULL AS category_id
  , CONCAT_WS('>', category1, category2, category3, category4) AS full_category_name
  , NULLIF(mallName, '네이버') AS mall_name
  , NULLIF(brand, '') AS brand_name
  , TRY_CAST(lprice AS INTEGER) AS sales_price
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updated_at
FROM {{ array }}
WHERE TRY_CAST(productId AS BIGINT) IS NOT NULL;

-- ShoppingRank: upsert_product
INSERT INTO {{ table }} {{ values }}
ON CONFLICT DO UPDATE SET
    product_id = COALESCE(excluded.product_id, product_id)
  , product_name = COALESCE(excluded.product_name, product_name)
  , full_category_name = COALESCE(excluded.full_category_name, full_category_name)
  , mall_name = COALESCE(excluded.mall_name, mall_name)
  , brand_name = COALESCE(excluded.brand_name, brand_name)
  , updated_at = excluded.updated_at;
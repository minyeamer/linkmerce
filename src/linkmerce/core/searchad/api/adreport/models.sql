-- Campaign: create
CREATE OR REPLACE TABLE {{ table }} (
    campaign_id VARCHAR PRIMARY KEY
  , campaign_name VARCHAR
  , campaign_type TINYINT -- Campaign: campaign_type
  , customer_id INTEGER NOT NULL
  -- , delivery_method TINYINT -- {1: '일반 노출', 2: '균등 노출'}
  -- , using_period TINYINT -- {0: '캠페인 집행 기간 제한 없음', 1: '캠페인 집행 기간 제한 있음'}
  -- , period_start_date TIMESTAMP
  -- , period_end_date TIMESTAMP
  , is_enabled BOOLEAN
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
  -- , shared_budget_id VARCHAR
);

-- Campaign: campaign_type
SELECT *
FROM UNNEST([
    STRUCT(1 AS type, '파워링크' AS name)
  , STRUCT(2 AS type, '쇼핑검색' AS name)
  , STRUCT(3 AS type, '파워컨텐츠' AS name)
  , STRUCT(4 AS type, '브랜드검색/신제품검색' AS name)
  , STRUCT(5 AS type, '플레이스' AS name)
]);

-- Campaign: select
SELECT
    "Campaign ID" AS campaign_id
  , "Campaign Name" AS campaign_name
  , "Campaign Type" AS campaign_type
  , "Customer ID" AS customer_id
  -- , "Delivery Method" AS delivery_method
  -- , "Using Period" AS using_period
  -- , "Period Start Date" AS period_start_date
  -- , "Period End Date" AS period_end_date
  , ("ON/OFF" = 0) AS is_enabled
  , "regTm" AS created_at
  , "delTm" AS deleted_at
  -- , NULLIF("Shared budget id", 'null') AS shared_budget_id
FROM {{ array }};

-- Campaign: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Adgroup: create
CREATE OR REPLACE TABLE {{ table }} (
    adgroup_id VARCHAR PRIMARY KEY
  , campaign_id VARCHAR NOT NULL
  , adgroup_name VARCHAR
  , adgroup_type TINYINT -- Adgroup: adgroup_type
  , customer_id INTEGER NOT NULL
  , is_enabled BOOLEAN
  , bid_amount INTEGER
  -- , using_network_bid TINYINT -- {0: '사용하지 않음', 1: '사용함'}
  -- , network_bid INTEGER
  -- , network_bidding_weight_pc INTEGER
  -- , network_bidding_weight_mobile BIDDING
  -- , business_channel_id_mobile VARCHAR
  -- , business_channel_id_pc VARCHAR
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
  -- , content_type VARCHAR
  -- , shared_budget_id VARCHAR
  -- , using_expanded_search TINYINT -- {0: '사용하지 않음', 1: '사용함'}
);

-- Adgroup: adgroup_type
SELECT *
FROM UNNEST([
    STRUCT(1 AS type, '파워링크' AS name)
  , STRUCT(2 AS type, '쇼핑검색-쇼핑몰 상품형' AS name)
  , STRUCT(3 AS type, '파워컨텐츠-정보형' AS name)
  , STRUCT(4 AS type, '파워컨텐츠-상품형' AS name)
  , STRUCT(5 AS type, '브랜드검색-일반형' AS name)
  , STRUCT(6 AS type, '플레이스-지역소상공인' AS name)
  , STRUCT(7 AS type, '쇼핑검색-제품 카탈로그형' AS name)
  , STRUCT(8 AS type, '브랜드검색-브랜드형' AS name)
  , STRUCT(9 AS type, '쇼핑검색-쇼핑 브랜드형' AS name)
  , STRUCT(10 AS type, '플레이스-플레이스검색' AS name)
  , STRUCT(11 AS type, '브랜드검색-신제품검색형' AS name)
]);

-- Adgroup: select
SELECT
    "Ad Group ID" AS adgroup_id
  , "Campaign ID" AS campaign_id
  , "Ad Group Name" AS adgroup_name
  , "Ad group type" AS adgroup_type
  , "Customer ID" AS customer_id
  , ("ON/OFF" = 0) AS is_enabled
  , "Ad Group Bid amount" AS bid_amount
  -- , "Using contents network bid" AS using_network_bid
  -- , "Contents network bid" AS network_bid
  -- , "PC network bidding weight" AS network_bidding_weight_pc
  -- , "Mobile network bidding weight" AS network_bidding_weight_mobile
  -- , "Business Channel Id(Mobile)" AS business_channel_id_mobile
  -- , "Business Channel Id(PC)" AS business_channel_id_pc
  , "regTm" AS created_at
  , "delTm" AS deleted_at
  -- , NULLIF("Content Type", '') AS content_type
  -- , NULLIF("Shared budget id", 'null') AS shared_budget_id
  -- , "Using Expanded Search" AS using_expanded_search
FROM {{ array }};

-- Adgroup: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Ad: create_ad
CREATE OR REPLACE TABLE {{ table }} (
    ad_id VARCHAR PRIMARY KEY
  , adgroup_id VARCHAR NOT NULL
  , ad_type TINYINT -- Ad: ad_type
  , customer_id INTEGER NOT NULL
  , title VARCHAR
  , description VARCHAR
  , landing_url_pc VARCHAR
  , landing_url_mobile VARCHAR
  , nv_mid BIGINT
  , product_id BIGINT
  , category_id INTEGER
  , is_enabled BOOLEAN
  , bid_amount INTEGER
  , sales_price INTEGER
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

-- Ad: ad_type
SELECT *
FROM UNNEST([
    STRUCT(1 AS type, '파워링크-단일형 소재' AS name)
  , STRUCT(2 AS type, '쇼핑검색-상품형 소재' AS name)
  , STRUCT(3 AS type, '파워컨텐츠-정보형 소재' AS name)
  , STRUCT(4 AS type, '파워컨텐츠-상품형 소재' AS name)
  , STRUCT(5 AS type, '브랜드검색-일반형 소재' AS name)
  , STRUCT(6 AS type, '플레이스-지역소상공인 소재' AS name)
  , STRUCT(7 AS type, '쇼핑검색-카탈로그형 소재' AS name)
  , STRUCT(9 AS type, '쇼핑검색-쇼핑 브랜드형 소재' AS name)
  , STRUCT(10 AS type, '플레이스-플레이스 검색 소재' AS name)
  , STRUCT(12 AS type, '쇼핑검색-쇼핑 브랜드형 이미지 섬네일형 소재' AS name)
  , STRUCT(13 AS type, '쇼핑검색-쇼핑 브랜드형 이미지 배너형 소재' AS name)
]);


-- Ad: create_power_link_ad
CREATE OR REPLACE TABLE {{ table }} (
    customer_id INTEGER NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , subject VARCHAR
  , description VARCHAR
  , landing_url_pc VARCHAR
  , landing_url_mobile VARCHAR
  , is_enabled BOOLEAN
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

-- Ad: select_power_link_ad
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , "Subject" AS subject
  , "Description" AS description
  , "Landing URL(PC)" AS landing_url_pc
  , "Landing URL(Mobile)" AS landing_url_mobile
  , ("ON/OFF" = 0) AS is_enabled
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ array }};

-- Ad: insert_power_link_ad
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Ad: load_power_link_ad
INSERT INTO {{ table }} (
    ad_id
  , adgroup_id
  , ad_type
  , customer_id
  , title
  , description
  , landing_url_pc
  , landing_url_mobile
  , is_enabled
  , created_at
  , deleted_at
)
SELECT
    ad_id
  , adgroup_id
  , 1 AS ad_type
  , customer_id
  , subject AS title
  , description
  , landing_url_pc
  , landing_url_mobile
  , is_enabled
  , created_at
  , deleted_at
FROM power_link_ad
ON CONFLICT DO NOTHING;


-- Ad: create_power_contents_ad
CREATE OR REPLACE TABLE {{ table }} (
    customer_id INTEGER NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , subject VARCHAR
  , description VARCHAR
  , landing_url_pc VARCHAR
  , landing_url_mobile VARCHAR
  , image_url VARCHAR
  , company_name VARCHAR
  , contents_issue_date TIMESTAMP
  , contents_release_date TIMESTAMP
  , is_enabled BOOLEAN
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

-- Ad: select_power_contents_ad
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , "Subject" AS subject
  , "Description" AS description
  , "Landing URL(PC)" AS landing_url_pc
  , "Landing URL(Mobile)" AS landing_url_mobile
  , "Image URL" AS image_url
  , "Company Name" AS company_name
  , "Contents Issue Date" AS contents_issue_date
  , "Release Date" AS contents_release_date
  , ("ON/OFF" = 0) AS is_enabled
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ array }};

-- Ad: insert_power_contents_ad
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Ad: load_power_contents_ad
INSERT INTO {{ table }} (
    ad_id
  , adgroup_id
  , ad_type
  , customer_id
  , title
  , description
  , landing_url_pc
  , landing_url_mobile
  , is_enabled
  , created_at
  , deleted_at
)
SELECT
    ad_id
  , adgroup_id
  , 3 AS ad_type
  , customer_id
  , subject AS title
  , description
  , landing_url_pc
  , landing_url_mobile
  , is_enabled
  , created_at
  , deleted_at
FROM power_contents_ad
ON CONFLICT DO NOTHING;


-- Ad: create_shopping_product_ad
CREATE OR REPLACE TABLE {{ table }} (
    customer_id INTEGER NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , is_enabled BOOLEAN
  , bid_amount INTEGER
  , using_adgroup_bid BOOLEAN
  , ad_link_status TINYINT -- {0: '연동 되고 있지 않음', 1: '연동 중'}
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
  , nv_mid BIGINT NOT NULL
  , product_id BIGINT NOT NULL
  , product_name VARCHAR
  , image_url VARCHAR
  , landing_url_pc VARCHAR
  , landing_url_mobile VARCHAR
  , sales_price INTEGER
  , delivery_fee INTEGER
  , category_name1 VARCHAR
  , category_name2 VARCHAR
  , category_name3 VARCHAR
  , category_name4 VARCHAR
  , category_id1 INTEGER
  , category_id2 INTEGER
  , category_id3 INTEGER
  , category_id4 INTEGER
  , full_category_name VARCHAR
);

-- Ad: select_shopping_product_ad
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , ("ON/OFF" = 0) AS is_enabled
  , "Bid" AS bid_amount
  , "Using Ad Group Bid Amount" AS using_adgroup_bid
  , "Ad Link Status" AS ad_link_status
  , "regTm" AS created_at
  , "delTm" AS deleted_at
  , TRY_CAST("Product ID" AS BIGINT) AS nv_mid
  , TRY_CAST("Product ID Of Mall" AS BIGINT) AS product_id
  , COALESCE(NULLIF("Ad Product Name", ''), "Product Name") AS product_name
  , COALESCE(NULLIF("Ad Image URL", ''), "Product Image URL") AS image_url
  , "PC Landing URL" AS landing_url_pc
  , "Mobile Landing URL" AS landing_url_mobile
  , "Price" AS sales_price
  , "Delivery Fee" AS delivery_fee
  , "NAVER Shopping Category Name 1" AS category_name1
  , "NAVER Shopping Category Name 2" AS category_name2
  , "NAVER Shopping Category Name 3" AS category_name3
  , "NAVER Shopping Category Name 4" AS category_name4
  , TRY_CAST("NAVER Shopping Category ID 1" AS INTEGER) AS category_id1
  , TRY_CAST("NAVER Shopping Category ID 2" AS INTEGER) AS category_id2
  , TRY_CAST("NAVER Shopping Category ID 3" AS INTEGER) AS category_id3
  , TRY_CAST("NAVER Shopping Category ID 4" AS INTEGER) AS category_id4
  , "Category Name of Mall" AS full_category_name
FROM {{ array }};

-- Ad: insert_shopping_product_ad
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Ad: load_shopping_product_ad
INSERT INTO {{ table }} (
    ad_id
  , adgroup_id
  , ad_type
  , customer_id
  , title
  , landing_url_pc
  , landing_url_mobile
  , nv_mid
  , product_id
  , category_id
  , is_enabled
  , bid_amount
  , sales_price
  , created_at
  , deleted_at
)
SELECT
    ad_id
  , adgroup_id
  , 2 AS ad_type
  , customer_id
  , product_name AS title
  , landing_url_pc
  , landing_url_mobile
  , nv_mid
  , product_id
  , COALESCE(category_id4, category_id3, category_id2, category_id1) AS category_id
  , is_enabled
  , bid_amount
  , sales_price
  , created_at
  , deleted_at
FROM shopping_product_ad
ON CONFLICT DO NOTHING;


-- Ad: create_product_group
CREATE OR REPLACE TABLE {{ table }} (
    customer_id INTEGER NOT NULL
  , product_group_id VARCHAR PRIMARY KEY
  , business_channel_id VARCHAR
  , product_group_name VARCHAR
  , registration_method TINYINT -- {1: '몰에 등록된 전체 상품을 등록', 2: '개별 상품 혹은 카테고리'}
  , registered_product_type TINYINT -- {1: '일반 상품', 2: '카탈로그형(가격비교) 상품'}
  , attribute_json VARCHAR
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

-- Ad: select_product_group
SELECT
    "Customer ID" AS customer_id
  , "Product group ID" AS product_group_id
  , "Business channel ID" AS business_channel_id
  , "Name" AS product_group_name
  , "Registration method" AS registration_method
  , "Registered product type" AS registered_product_type
  , "Attribute json1" AS attribute_json
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ array }};

-- Ad: insert_product_group
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Ad: create_product_group_rel
CREATE OR REPLACE TABLE {{ table }} (
    customer_id INTEGER NOT NULL
  , relation_id VARCHAR PRIMARY KEY
  , product_group_id VARCHAR NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

-- Ad: select_product_group_rel
SELECT
    "Customer ID" AS customer_id
  , "Product Group Relation ID" AS relation_id
  , "Product Group ID" AS product_group_id
  , "AD group ID" AS adgroup_id
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ array }};

-- Ad: insert_product_group_rel
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Ad: create_brand_thumbnail_ad
CREATE OR REPLACE TABLE {{ table }} (
    customer_id INTEGER NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , is_enabled BOOLEAN
  , headline VARCHAR
  , description VARCHAR
  , extra_description VARCHAR
  , logo_image_path VARCHAR
  , link_url VARCHAR
  , thumbnail_image_path VARCHAR
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

-- Ad: select_brand_thumbnail_ad
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , ("ON/OFF" = 0) AS is_enabled
  , "Headline" AS headline
  , "description" AS description
  , "extra Description" AS extra_description
  , "Logo image path" AS logo_image_path
  , "Link URL" AS link_url
  , "Thumbnail Image path" AS thumbnail_image_path
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ array }};

-- Ad: insert_brand_thumbnail_ad
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Ad: create_brand_banner_ad
CREATE OR REPLACE TABLE {{ table }} (
    customer_id INTEGER NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , is_enabled BOOLEAN
  , headline VARCHAR
  , description VARCHAR
  , logo_image_path VARCHAR
  , link_url VARCHAR
  , thumbnail_image_path VARCHAR
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

-- Ad: select_brand_banner_ad
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , ("ON/OFF" = 0) AS is_enabled
  , "Headline" AS headline
  , "description" AS description
  , "Logo image path" AS logo_image_path
  , "Link URL" AS link_url
  , "Thumbnail Image path" AS thumbnail_image_path
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ array }};

-- Ad: insert_brand_banner_ad
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Ad: create_brand_ad
CREATE OR REPLACE TABLE {{ table }} (
    customer_id INTEGER NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , is_enabled BOOLEAN
  , headline VARCHAR
  , description VARCHAR
  , logo_image_path VARCHAR
  , link_url VARCHAR
  , image_path VARCHAR
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

-- Ad: select_brand_ad
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , ("ON/OFF" = 0) AS is_enabled
  , "Headline" AS headline
  , "description" AS description
  , "Logo image path" AS logo_image_path
  , "Link URL" AS link_url
  , "Image path" AS image_path
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ array }};

-- Ad: insert_brand_ad
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Ad: load_brand_ad
INSERT INTO {{ table }} (
    ad_id
  , adgroup_id
  , ad_type
  , customer_id
  , title
  , description
  , landing_url_pc
  , is_enabled
  , created_at
  , deleted_at
  -- , nv_mid
)
SELECT ad.* --, grp.nv_mid
FROM (
  SELECT ad_id, adgroup_id, 9 AS ad_type, customer_id, headline AS title, description, link_url AS landing_url_pc, is_enabled, created_at, deleted_at
  FROM brand_ad
  UNION ALL
  SELECT ad_id, adgroup_id, 12 AS ad_type, customer_id, headline AS title, description, link_url AS landing_url_pc, is_enabled, created_at, deleted_at
  FROM brand_thumbnail_ad
  UNION ALL
  SELECT ad_id, adgroup_id, 13 AS ad_type, customer_id, headline AS title, description, link_url AS landing_url_pc, is_enabled, created_at, deleted_at
  FROM brand_banner_ad
) AS ad
-- LEFT JOIN product_group_rel AS rel
--   ON ad.adgroup_id = rel.adgroup_id
-- LEFT JOIN (
--     SELECT product_group_id, TRY_CAST(nv_mid.unnest AS BIGINT) AS nv_mid
--     FROM product_group,
--       UNNEST(CAST(json_extract(attribute_json, '$.productNvmids') AS VARCHAR[])) AS nv_mid
--   ) AS grp
--   ON rel.product_group_id = grp.product_group_id
ON CONFLICT DO NOTHING;
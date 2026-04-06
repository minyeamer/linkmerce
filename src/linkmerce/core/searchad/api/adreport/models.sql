-- Campaign: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_id VARCHAR PRIMARY KEY
  , campaign_name VARCHAR
  , campaign_type TINYINT -- Campaign: campaign_type
  , customer_id BIGINT NOT NULL
  -- , delivery_method TINYINT -- {1: '일반 노출', 2: '균등 노출'}
  -- , using_period TINYINT -- {0: '캠페인 집행 기간 제한 없음', 1: '캠페인 집행 기간 제한 있음'}
  -- , period_start_date TIMESTAMP
  -- , period_end_date TIMESTAMP
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
  -- , shared_budget_id VARCHAR
);

-- Campaign: bulk_insert
INSERT INTO {{ table }}
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
  , ("delTm" IS NOT NULL) AS is_deleted
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- Campaign: campaign_type
SELECT *
FROM UNNEST([
    STRUCT(1 AS type, '파워링크' AS name)
  , STRUCT(2 AS type, '쇼핑검색' AS name)
  , STRUCT(3 AS type, '파워컨텐츠' AS name)
  , STRUCT(4 AS type, '브랜드검색/신제품검색' AS name)
  , STRUCT(5 AS type, '플레이스' AS name)
]);


-- Adgroup: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    adgroup_id VARCHAR PRIMARY KEY
  , campaign_id VARCHAR NOT NULL
  , adgroup_name VARCHAR
  , adgroup_type TINYINT -- Adgroup: adgroup_type
  , customer_id BIGINT NOT NULL
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
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
);

-- Adgroup: bulk_insert
INSERT INTO {{ table }}
SELECT
    "Ad Group ID" AS adgroup_id
  , "Campaign ID" AS campaign_id
  , "Ad Group Name" AS adgroup_name
  , "Ad group type" AS adgroup_type
  , "Customer ID" AS customer_id
  , ("ON/OFF" = 0) AS is_enabled
  , ("delTm" IS NOT NULL) AS is_deleted
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
FROM {{ rows }}
ON CONFLICT DO NOTHING;

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


-- MasterAd: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    ad_id VARCHAR PRIMARY KEY
  , adgroup_id VARCHAR NOT NULL
  , ad_type TINYINT -- MasterAd: ad_type
  , customer_id BIGINT NOT NULL
  , title VARCHAR
  , description VARCHAR
  , landing_url_pc VARCHAR
  , landing_url_mobile VARCHAR
  , nv_mid BIGINT
  , product_id BIGINT
  , category_id INTEGER
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
  , bid_amount INTEGER
  , sales_price INTEGER
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{ link_ad }} (
    customer_id BIGINT NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , subject VARCHAR
  , description VARCHAR
  , landing_url_pc VARCHAR
  , landing_url_mobile VARCHAR
  , product_id BIGINT
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{ contents_ad }} (
    customer_id BIGINT NOT NULL
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
  , is_deleted BOOLEAN
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{ shopping_product }} (
    customer_id BIGINT NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
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

CREATE TABLE IF NOT EXISTS {{ product_group }} (
    customer_id BIGINT NOT NULL
  , product_group_id VARCHAR PRIMARY KEY
  , business_channel_id VARCHAR
  , product_group_name VARCHAR
  , registration_method TINYINT -- {1: '몰에 등록된 전체 상품을 등록', 2: '개별 상품 혹은 카테고리'}
  , registered_product_type TINYINT -- {1: '일반 상품', 2: '카탈로그형(가격비교) 상품'}
  , attribute_json VARCHAR
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{ product_group_rel }} (
    customer_id BIGINT NOT NULL
  , relation_id VARCHAR PRIMARY KEY
  , product_group_id VARCHAR NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{ brand_ad }} (
    customer_id BIGINT NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
  , title VARCHAR
  , description VARCHAR
  , logo_image_path VARCHAR
  , link_url VARCHAR
  , product_id BIGINT
  , image_path VARCHAR
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{ brand_thumbnail_ad }} (
    customer_id BIGINT NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
  , title VARCHAR
  , description VARCHAR
  , extra_description VARCHAR
  , logo_image_path VARCHAR
  , link_url VARCHAR
  , product_id BIGINT
  , thumbnail_image_path VARCHAR
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{ brand_banner_ad }} (
    customer_id BIGINT NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR PRIMARY KEY
  , inspect_status TINYINT -- {10: '검토 대기', 20: '통과', 30: '보류', 40: '반려'}
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
  , title VARCHAR
  , description VARCHAR
  , logo_image_path VARCHAR
  , link_url VARCHAR
  , product_id BIGINT
  , thumbnail_image_path VARCHAR
  , created_at TIMESTAMP
  , deleted_at TIMESTAMP
);

-- MasterAd: bulk_insert_link_ad
INSERT INTO {{ link_ad }}
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , "Subject" AS subject
  , "Description" AS description
  , "Landing URL(PC)" AS landing_url_pc
  , "Landing URL(Mobile)" AS landing_url_mobile
  , TRY_CAST((CASE
      WHEN REGEXP_MATCHES(
            COALESCE("Landing URL(PC)", "Landing URL(Mobile)")
          , '^https://(brand|smartstore).naver.com/[^/]+/products/(\d+)')
        THEN REGEXP_EXTRACT(
            COALESCE("Landing URL(PC)", "Landing URL(Mobile)")
          , '(\d+)$')
      ELSE NULL END) AS BIGINT) AS product_id
  , ("ON/OFF" = 0) AS is_enabled
  , ("delTm" IS NOT NULL) AS is_deleted
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- MasterAd: bulk_insert_contents_ad
INSERT INTO {{ contents_ad }}
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
  , ("delTm" IS NOT NULL) AS is_deleted
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- MasterAd: bulk_insert_shopping_product
INSERT INTO {{ shopping_product }}
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , ("ON/OFF" = 0) AS is_enabled
  , ("delTm" IS NOT NULL) AS is_deleted
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
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- MasterAd: bulk_insert_product_group
INSERT INTO {{ product_group }}
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
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- MasterAd: bulk_insert_product_group_rel
INSERT INTO {{ product_group_rel }}
SELECT
    "Customer ID" AS customer_id
  , "Product Group Relation ID" AS relation_id
  , "Product Group ID" AS product_group_id
  , "AD group ID" AS adgroup_id
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- MasterAd: bulk_insert_brand_ad
INSERT INTO {{ brand_ad }}
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , ("ON/OFF" = 0) AS is_enabled
  , ("delTm" IS NOT NULL) AS is_deleted
  , "Headline" AS title
  , "description" AS description
  , "Logo image path" AS logo_image_path
  , "Link URL" AS link_url
  , TRY_CAST((CASE
      WHEN REGEXP_MATCHES("Link URL", '^https://(brand|smartstore).naver.com/[^/]+/products/(\d+)')
        THEN REGEXP_EXTRACT("Link URL", '(\d+)$')
      ELSE NULL END) AS BIGINT) AS product_id
  , "Image path" AS image_path
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- MasterAd: bulk_insert_brand_thumbnail_ad
INSERT INTO {{ brand_thumbnail_ad }}
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , ("ON/OFF" = 0) AS is_enabled
  , ("delTm" IS NOT NULL) AS is_deleted
  , "Headline" AS title
  , "description" AS description
  , "extra Description" AS extra_description
  , "Logo image path" AS logo_image_path
  , "Link URL" AS link_url
  , TRY_CAST((CASE
      WHEN REGEXP_MATCHES("Link URL", '^https://(brand|smartstore).naver.com/[^/]+/products/(\d+)')
        THEN REGEXP_EXTRACT("Link URL", '(\d+)$')
      ELSE NULL END) AS BIGINT) AS product_id
  , "Thumbnail Image path" AS thumbnail_image_path
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- MasterAd: bulk_insert_brand_banner_ad
INSERT INTO {{ brand_banner_ad }}
SELECT
    "Customer ID" AS customer_id
  , "Ad Group ID" AS adgroup_id
  , "Ad ID" AS ad_id
  , "Ad Creative Inspect Status" AS inspect_status
  , ("ON/OFF" = 0) AS is_enabled
  , ("delTm" IS NOT NULL) AS is_deleted
  , "Headline" AS title
  , "description" AS description
  , "Logo image path" AS logo_image_path
  , "Link URL" AS link_url
  , TRY_CAST((CASE
      WHEN REGEXP_MATCHES("Link URL", '^https://(brand|smartstore).naver.com/[^/]+/products/(\d+)')
        THEN REGEXP_EXTRACT("Link URL", '(\d+)$')
      ELSE NULL END) AS BIGINT) AS product_id
  , "Thumbnail Image path" AS thumbnail_image_path
  , "regTm" AS created_at
  , "delTm" AS deleted_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;

-- MasterAd: transform_link_ad
INSERT INTO {{ table }} (
    ad_id
  , adgroup_id
  , ad_type
  , customer_id
  , title
  , description
  , landing_url_pc
  , landing_url_mobile
  , product_id
  , is_enabled
  , is_deleted
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
  , product_id
  , is_enabled
  , is_deleted
  , created_at
  , deleted_at
FROM {{ link_ad }}
WHERE customer_id = $customer_id
ON CONFLICT DO NOTHING;

-- MasterAd: transform_contents_ad
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
  , is_deleted
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
  , is_deleted
  , created_at
  , deleted_at
FROM {{ contents_ad }}
WHERE customer_id = $customer_id
ON CONFLICT DO NOTHING;

-- MasterAd: transform_shopping_product
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
  , is_deleted
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
  , is_deleted
  , bid_amount
  , sales_price
  , created_at
  , deleted_at
FROM {{ shopping_product }}
WHERE customer_id = $customer_id
ON CONFLICT DO NOTHING;

-- MasterAd: transform_brand_ad
INSERT INTO {{ table }} (
    ad_id
  , adgroup_id
  , ad_type
  , customer_id
  , title
  , description
  , landing_url_pc
  , product_id
  , is_enabled
  , is_deleted
  , created_at
  , deleted_at
  -- , nv_mid
)
SELECT ad.* --, grp.nv_mid
FROM (
  SELECT
      ad_id, adgroup_id, 9 AS ad_type, customer_id, title, description
    , link_url AS landing_url_pc, product_id, is_enabled, is_deleted, created_at, deleted_at
  FROM {{ brand_ad }}
  WHERE customer_id = $customer_id
  UNION ALL
  SELECT
      ad_id, adgroup_id, 12 AS ad_type, customer_id, title, description
    , link_url AS landing_url_pc, product_id, is_enabled, is_deleted, created_at, deleted_at
  FROM {{ brand_thumbnail_ad }}
  WHERE customer_id = $customer_id
  UNION ALL
  SELECT
      ad_id, adgroup_id, 13 AS ad_type, customer_id, title, description
    , link_url AS landing_url_pc, product_id, is_enabled, is_deleted, created_at, deleted_at
  FROM {{ brand_banner_ad }}
  WHERE customer_id = $customer_id
) AS ad
-- LEFT JOIN {{ product_group_rel }} AS rel
--   ON ad.adgroup_id = rel.adgroup_id
-- LEFT JOIN (
--     SELECT product_group_id, TRY_CAST(nv_mid.unnest AS BIGINT) AS nv_mid
--     FROM {{ product_group }},
--       UNNEST(CAST(json_extract(attribute_json, '$.productNvmids') AS VARCHAR[])) AS nv_mid
--   ) AS grp
--   ON rel.product_group_id = grp.product_group_id
ON CONFLICT DO NOTHING;

-- MasterAd: ad_type
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
  , STRUCT(11 AS type, '브랜드검색-신제품검색형 소재' AS name)
  , STRUCT(12 AS type, '쇼핑검색-쇼핑 브랜드형 이미지 섬네일형 소재' AS name)
  , STRUCT(13 AS type, '쇼핑검색-쇼핑 브랜드형 이미지 배너형 소재' AS name)
]);


-- Media: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    media_code INTEGER
  , media_name VARCHAR
  , media_type TINYINT -- {0: 'media', 1: 'group'}
  -- , media_url VARCHAR
  -- , naver_ad_networks_yn BOOLEAN
  -- , portal_site_yn BOOLEAN
  -- , pc_media_yn BOOLEAN
  -- , mobile_media_yn BOOLEAN
  -- , search_ad_yn BOOLEAN
  -- , contents_ad_yn BOOLEAN
  , group_id INTEGER
  -- , created_at TIMESTAMP
  -- , deleted_at TIMESTAMP
  , PRIMARY KEY (media_type, media_code)
);

-- Media: bulk_insert
INSERT INTO {{ table }}
SELECT
    "ID" AS media_code
  , (CASE
      WHEN $root_only THEN IF("Media Group ID" IS NULL, "Media name", NULL)
      ELSE "Media name" END) AS media_name
  , (CASE WHEN "Type" = 'media' THEN 0 WHEN "Type" = 'group' THEN 1 ELSE 2 END) AS media_type
  -- , "URL" AS media_url
  -- , "NAVER Ad Networks" AS naver_ad_networks_yn
  -- , "Portal Site" AS portal_site_yn
  -- , "PC Media" AS pc_media_yn
  -- , "Mobile Media" AS mobile_media_yn
  -- , "Search Ad Networks" AS search_ad_yn
  -- , "Contents Ad Networks" AS contents_ad_yn
  , "Media Group ID" AS group_id
  -- , "Date of conclusion of a contract" AS created_at
  -- , "Date of revocation of a contract" AS deleted_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- AdvancedReport: create
CREATE TABLE IF NOT EXISTS {{ ad_stat }} (
    ad_id VARCHAR
  , customer_id BIGINT
  , media_code BIGINT
  , pc_mobile_type TINYINT
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , ad_rank_sum INTEGER
  , ymd DATE
  , PRIMARY KEY (ymd, customer_id, ad_id, media_code, pc_mobile_type)
);

CREATE TABLE IF NOT EXISTS {{ ad_conv }} (
    ad_id VARCHAR
  , customer_id BIGINT
  , media_code BIGINT
  , pc_mobile_type TINYINT
  , conv_count INTEGER
  , direct_conv_count INTEGER
  , conv_amount INTEGER
  , direct_conv_amount INTEGER
  , ymd DATE
  , PRIMARY KEY (ymd, customer_id, ad_id, media_code, pc_mobile_type)
);

CREATE TABLE IF NOT EXISTS {{ table }} (
    ad_id VARCHAR
  , customer_id BIGINT
  , media_code BIGINT
  , pc_mobile_type TINYINT
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , ad_rank_sum INTEGER
  , conv_count INTEGER
  , direct_conv_count INTEGER
  , conv_amount INTEGER
  , direct_conv_amount INTEGER
  , ymd DATE
  , PRIMARY KEY (ymd, customer_id, ad_id, media_code, pc_mobile_type)
);

-- AdvancedReport: bulk_insert_ad_stat
INSERT INTO {{ ad_stat }}
SELECT
    ad_id
  , customer_id
  , media_code
  , pc_mobile_type
  , SUM(impression_count) AS impression_count
  , SUM(click_count) AS click_count
  , SUM(ad_cost) AS ad_cost
  , SUM(ad_rank_sum) AS ad_rank_sum
  , ymd
FROM (
  SELECT
      "AD ID" AS ad_id
    , "CUSTOMER ID" AS customer_id
    -- , "Campaign ID" AS campaign_id
    -- , "AD Group ID" AS adgroup_id
    -- , "AD Keyword ID" AS keyword_id
    -- , "Business Channel ID" AS business_channel_id
    , COALESCE("Media Code", 0) AS media_code
    , (CASE WHEN "PC Mobile Type" = 'P' THEN 0 WHEN "PC Mobile Type" = 'M' THEN 1 ELSE 2 END) AS pc_mobile_type
    , "Impression" AS impression_count
    , "Click" AS click_count
    , "Cost" AS ad_cost
    , "Sum of AD rank" AS ad_rank_sum
    -- , "View count" AS view_count
    , "Date" AS ymd
  FROM {{ rows }}
) AS report
WHERE (ad_id IS NOT NULL)
  AND (customer_id IS NOT NULL)
  AND (ymd IS NOT NULL)
GROUP BY ymd, customer_id, ad_id, media_code, pc_mobile_type;

-- AdvancedReport: bulk_insert_ad_conv
INSERT INTO {{ ad_conv }}
SELECT
    ad_id
  , customer_id
  , media_code
  , pc_mobile_type
  , SUM(conv_count) AS conv_count
  , SUM(IF(conv_method = 1, conv_count, 0)) AS direct_conv_count
  , SUM(conv_amount) AS conv_amount
  , SUM(IF(conv_method = 1, conv_amount, 0)) AS direct_conv_amount
  , ymd
FROM (
  SELECT
      "AD ID" AS ad_id
    , "CUSTOMER ID" AS customer_id
    -- , "Campaign ID" AS campaign_id
    -- , "AD Group ID" AS adgroup_id
    -- , "AD Keyword ID" AS keyword_id
    -- , "Business Channel ID" AS business_channel_id
    , COALESCE("Media Code", 0) AS media_code
    , (CASE WHEN "PC Mobile Type" = 'P' THEN 0 WHEN "PC Mobile Type" = 'M' THEN 1 ELSE 2 END) AS pc_mobile_type
    , "Conversion Method" AS conv_method -- {1: '직접 전환', 2: '간접 전환'}
    -- , "Conversion Type" AS conv_type
    , "Conversion count" AS conv_count
    , "Sales by conversion" AS conv_amount
    , "Date" AS ymd
  FROM {{ rows }}
) AS report
WHERE (ad_id IS NOT NULL)
  AND (customer_id IS NOT NULL)
  AND (ymd IS NOT NULL)
GROUP BY ymd, customer_id, ad_id, media_code, pc_mobile_type;

-- AdvancedReport: merge_insert
INSERT INTO {{ table }}
SELECT
    COALESCE(stat.ad_id, conv.ad_id) AS ad_id
  , COALESCE(stat.customer_id, conv.customer_id) AS customer_id
  , COALESCE(stat.media_code, conv.media_code) AS media_code
  , COALESCE(stat.pc_mobile_type, conv.pc_mobile_type) AS pc_mobile_type
  , COALESCE(stat.impression_count, 0) AS impression_count
  , COALESCE(stat.click_count, 0) AS click_count
  , COALESCE(stat.ad_cost, 0) AS ad_cost
  , COALESCE(stat.ad_rank_sum, 0) AS ad_rank_sum
  , COALESCE(conv.conv_count, 0) AS conv_count
  , COALESCE(conv.direct_conv_count, 0) AS direct_conv_count
  , COALESCE(conv.conv_amount, 0) AS conv_amount
  , COALESCE(conv.direct_conv_amount, 0) AS direct_conv_amount
  , COALESCE(stat.ymd, conv.ymd) AS ymd
FROM (SELECT * FROM {{ ad_stat }} WHERE (customer_id = $customer_id) AND (ymd = $report_date)) AS stat
FULL OUTER JOIN (SELECT * FROM {{ ad_conv }} WHERE (customer_id = $customer_id) AND (ymd = $report_date)) AS conv
  ON stat.ymd = conv.ymd
  AND stat.customer_id = conv.customer_id
  AND stat.ad_id = conv.ad_id
  AND stat.media_code = conv.media_code
  AND stat.pc_mobile_type = conv.pc_mobile_type;
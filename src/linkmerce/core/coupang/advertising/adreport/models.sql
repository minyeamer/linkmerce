-- Campaign: create_campaign
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_id BIGINT PRIMARY KEY
  , campaign_name VARCHAR
  , campaign_type VARCHAR -- {'PA': '상품광고'}
  , vendor_id VARCHAR
  , vendor_type TINYINT -- {0: 'Wing', 1: '서플라이어 허브'}
  , goal_type TINYINT -- {0: '매출 성장', 1: '신규 구매 고객 확보', 2: '인지도 상승'}
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , roas_target INTEGER
  -- , cap_type VARCHAR
  -- , calculated_budget INTEGER
  -- , spent_budget INTEGER
  , created_at TIMESTAMP
  , updated_at TIMESTAMP
);

-- Campaign: select_campaign
SELECT
    id AS campaign_id
  , name AS campaign_name
  , campaignType AS campaign_type
  , $vendor_id AS vendor_id
  , (CASE
      WHEN vendorType = '3P' THEN 0
      WHEN vendorType = 'Retail' THEN 1
      ELSE NULL END) AS vendor_type
  , (CASE
      WHEN goalType = 'SALES' THEN 0
      WHEN goalType = 'NCA' THEN 1
      WHEN goalType = 'REACH' THEN 2
      ELSE NULL END) AS goal_type
  , isActive AS is_active
  , isDeleted AS is_deleted
  , item->'$.roasTarget' AS roas_target
  -- , capType AS cap_type
  -- , calculatedBudget AS calculated_budget
  -- , spentBudget AS spent_budget
  , TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
  , TRY_STRPTIME(SUBSTR(updatedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS updated_at
FROM {{ array }} AS item
WHERE id IS NOT NULL;

-- Campaign: insert_campaign
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Campaign: goal_type
SELECT *
FROM UNNEST([
  , STRUCT(0 AS seq, 'SALES' AS code, '매출 성장' AS name)
  , STRUCT(1 AS seq, 'NCA' AS code, '신규 구매 고객 확보' AS name)
  , STRUCT(2 AS seq, 'REACH' AS code, '인지도 상승' AS name)
]);


-- Campaign: create_adgroup
CREATE TABLE IF NOT EXISTS {{ table }} (
    adgroup_id BIGINT PRIMARY KEY
  , campaign_id BIGINT NOT NULL
  , adgroup_name VARCHAR
  , vendor_id VARCHAR
  , goal_type TINYINT -- {0: '매출 성장', 1: '신규 구매 고객 확보', 2: '인지도 상승'}
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , roas_target INTEGER
  , created_at TIMESTAMP
  , updated_at TIMESTAMP
);

-- Campaign: select_adgroup
SELECT
    id AS adgroup_id
  , campaignId AS campaign_id
  , name AS adgroup_name
  , $vendor_id AS vendor_id
  , (CASE
      WHEN $goal_type = 'SALES' THEN 0
      WHEN $goal_type = 'NCA' THEN 1
      WHEN $goal_type = 'REACH' THEN 2
      ELSE NULL END) AS goal_type
  , isActive AS is_active
  , isDeleted AS is_deleted
  , item->'$.roasTarget' AS roas_target
  , TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
  , TRY_STRPTIME(SUBSTR(updatedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS updated_at
FROM {{ array }} AS item
WHERE (id IS NOT NULL) AND (campaignId IS NOT NULL);

-- Campaign: insert_adgroup
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Creative: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    creative_id BIGINT PRIMARY KEY
  , option_id BIGINT
  , vendor_id VARCHAR
  , creative_type VARCHAR
  , headline VARCHAR
  -- , description VARCHAR
  -- , image_url VARCHAR
  , ordering INTEGER
);

-- Creative: select
SELECT
    id AS creative_id
  , vendorItemId AS option_id
  , $vendor_id AS vendor_id
  , creativeType AS creative_type
  , headlineText AS headline
  -- , description
  -- , imageUrl AS image_url
  , ordering
FROM {{ array }}
WHERE id IS NOT NULL;

-- Creative: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- ProductAdReport: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_id BIGINT
  -- , campaign_name VARCHAR
  -- , adgroup_name VARCHAR
  , vendor_id VARCHAR
  -- , price_type VARCHAR -- {'cpc': '상품광고'}
  -- , vendor_type VARCHAR -- {'3P': 'Wing', 'Retail': '서플라이어 허브'}
  -- , target_type VARCHAR -- {'매출 최적화', '수동 성과형'}
  , option_id BIGINT
  -- , option_name VARCHAR
  , option_conv_id BIGINT
  -- , option_conv_name VARCHAR
  , placement_group TINYINT -- {0: '검색 영역', 1: '비검색 영역', 2: '리타겟팅%'}
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , conv_count INTEGER
  , direct_conv_count INTEGER
  , conv_amount INTEGER
  , direct_conv_amount INTEGER
  -- , campaign_start_date DATE
  -- , campaign_end_date DATE
  , ymd DATE
  , PRIMARY KEY (ymd, vendor_id, campaign_id, option_id, option_conv_id, placement_group)
);

-- ProductAdReport: select
SELECT
    campaign_id
  , $vendor_id AS vendor_id
  , option_id
  , option_conv_id
  , placement_group
  , SUM(impression_count) AS impression_count
  , SUM(click_count) AS click_count
  , SUM(ad_cost) AS ad_cost
  , SUM(conv_count) AS conv_count
  , SUM(direct_conv_count) AS direct_conv_count
  , SUM(conv_amount) AS conv_amount
  , SUM(direct_conv_amount) AS direct_conv_amount
  , ymd
FROM (
  SELECT
      TRY_CAST("캠페인 ID" AS BIGINT) AS campaign_id
    -- , "캠페인명" AS campaign_name
    -- , "광고그룹" AS adgroup_name
    -- , "과금방식" AS price_type
    -- , "판매방식" AS vendor_type
    -- , "광고유형" AS target_type
    , TRY_CAST("광고집행 옵션ID" AS BIGINT) AS option_id
    -- , "광고집행 상품명" AS option_name
    , COALESCE(TRY_CAST("광고전환매출발생 옵션ID" AS BIGINT), 0) AS option_conv_id
    -- , "광고전환매출발생 상품명" AS option_conv_name
    , (CASE
        WHEN "광고 노출 지면" = '검색 영역' THEN 0
        WHEN "광고 노출 지면" = '비검색 영역' THEN 1
        WHEN "광고 노출 지면" LIKE '리타겟팅%' THEN 2
        ELSE NULL END) AS placement_group
    , TRY_CAST("노출수" AS INTEGER) AS impression_count
    , TRY_CAST("클릭수" AS INTEGER) AS click_count
    , TRY_CAST("광고비" AS INTEGER) AS ad_cost
    , TRY_CAST("총 주문수(1일)" AS INTEGER) AS conv_count
    , TRY_CAST("직접 판매수량(1일)" AS INTEGER) AS direct_conv_count
    , TRY_CAST("총 전환매출액(1일)" AS INTEGER) AS conv_amount
    , TRY_CAST("직접 전환매출액(1일)" AS INTEGER) AS direct_conv_amount
    -- , TRY_CAST(TRY_STRPTIME("캠페인 시작일", '%Y.%m.%d') AS DATE) AS campaign_start_date
    -- , TRY_CAST(TRY_STRPTIME("캠페인 종료일", '%Y.%m.%d') AS DATE) AS campaign_end_date
    , TRY_CAST(TRY_STRPTIME(CAST(CAST("날짜" AS BIGINT) AS VARCHAR), '%Y%m%d') AS DATE) AS ymd
  FROM {{ array }}
) AS row
WHERE (campaign_id IS NOT NULL)
  AND (option_id IS NOT NULL)
  AND (placement_group IS NOT NULL)
  AND (ymd IS NOT NULL)
GROUP BY ymd, campaign_id, option_id, option_conv_id, placement_group;

-- ProductAdReport: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- NewCustomerAdReport: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_id BIGINT
  -- , campaign_name VARCHAR
  -- , adgroup_name VARCHAR
  , vendor_id VARCHAR
  , creative_id BIGINT
  , creative_type TINYINT -- {0: '상품', 1: '동영상'}
  -- , price_type VARCHAR -- {'cpc': '상품광고'}
  -- , vendor_type VARCHAR -- {'3P': 'Wing', 'Retail': '서플라이어 허브'}
  -- , target_type VARCHAR -- {'매출 최적화', '수동 성과형'}
  , option_id BIGINT
  -- , option_name VARCHAR
  , placement_group TINYINT -- {0: '검색 영역', 1: '비검색 영역', 2: '리타겟팅%'}
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , view_count INTEGER
  , stay_time DECIMAL(18, 2)
  -- , campaign_start_date DATE
  -- , campaign_end_date DATE
  , ymd DATE
  , PRIMARY KEY (ymd, vendor_id, campaign_id, creative_id, placement_group)
);

-- NewCustomerAdReport: select
SELECT
    campaign_id
  , $vendor_id AS vendor_id
  , creative_id
  , MIN(creative_type) AS creative_type
  , MIN(option_id) AS option_id
  , placement_group
  , SUM(impression_count) AS impression_count
  , SUM(click_count) AS click_count
  , SUM(ad_cost) AS ad_cost
  , SUM(view_count) AS view_count
  , AVG(stay_time) AS stay_time
  , ymd
FROM (
  SELECT
      TRY_CAST("캠페인 ID" AS BIGINT) AS campaign_id
    -- , "캠페인명" AS campaign_name
    -- , "광고그룹" AS adgroup_name
    , TRY_CAST("소재 ID" AS BIGINT) AS creative_id
    , (CASE WHEN "소재" = '상품' THEN 0 WHEN "소재" = '동영상' THEN 1 ELSE NULL END) AS creative_type
    -- , "과금방식" AS price_type
    -- , "판매방식" AS vendor_type
    , NULLIF(TRY_CAST("광고집행 옵션 ID" AS BIGINT), 0) AS option_id
    -- , "광고집행 상품명" AS option_name
    -- , "광고 이름" AS title
    , (CASE
        WHEN "광고 노출 지면" = '검색 영역' THEN 0
        WHEN "광고 노출 지면" = '비검색 영역' THEN 1
        WHEN "광고 노출 지면" LIKE '리타겟팅%' THEN 2
        ELSE NULL END) AS placement_group
    , TRY_CAST("노출수" AS INTEGER) AS impression_count
    , TRY_CAST("클릭수" AS INTEGER) AS click_count
    , TRY_CAST("집행 광고비" AS INTEGER) AS ad_cost
    , TRY_CAST("참여수" AS INTEGER) AS view_count
    , TRY_CAST("평균 재생 시간" AS FLOAT) AS stay_time
    -- , TRY_CAST(TRY_STRPTIME("캠페인 시작일", '%Y.%m.%d') AS DATE) AS campaign_start_date
    -- , TRY_CAST(TRY_STRPTIME("캠페인 종료일", '%Y.%m.%d') AS DATE) AS campaign_end_date
    , TRY_CAST(TRY_STRPTIME(CAST(CAST("날짜" AS BIGINT) AS VARCHAR), '%Y%m%d') AS DATE) AS ymd
  FROM {{ array }}
) AS row
WHERE (campaign_id IS NOT NULL)
  AND (creative_id IS NOT NULL)
  AND (placement_group IS NOT NULL)
  AND (ymd IS NOT NULL)
GROUP BY ymd, campaign_id, creative_id, placement_group;

-- NewCustomerAdReport: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
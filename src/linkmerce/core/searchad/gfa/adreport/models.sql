-- Campaign: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_id VARCHAR PRIMARY KEY
  , campaign_name VARCHAR
  , campaign_type TINYINT -- Campaign: campaign_type
  , customer_id BIGINT NOT NULL
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
);

-- Campaign: campaign_type
SELECT *
FROM UNNEST([
    STRUCT(101 AS type, 'CONVERSION' AS code, '웹사이트 전환' AS name)
  , STRUCT(102 AS type, 'WEB_SITE_TRAFFIC' AS code, '인지도 및 트래픽' AS name)
  , STRUCT(103 AS type, 'INSTALL_APP' AS code, '앱 전환' AS name)
  , STRUCT(104 AS type, 'WATCH_VIDEO' AS code, '동영상 조회' AS name)
  , STRUCT(105 AS type, 'CATALOG' AS code, '카탈로그 판매' AS name)
  , STRUCT(106 AS type, 'SHOPPING' AS code, '쇼핑 프로모션' AS name)
  , STRUCT(107 AS type, 'LEAD' AS code, '참여 유도' AS name)
  , STRUCT(108 AS type, 'PMAX' AS code, 'ADVoost 쇼핑' AS name)
]);

-- Campaign: select
SELECT
    CAST(no AS VARCHAR) AS campaign_id
  , name AS campaign_name
  , (CASE
      WHEN objective = 'CONVERSION' THEN 101
      WHEN objective = 'WEB_SITE_TRAFFIC' THEN 102
      WHEN objective = 'INSTALL_APP' THEN 103
      WHEN objective = 'WATCH_VIDEO' THEN 104
      WHEN objective = 'CATALOG' THEN 105
      WHEN objective = 'SHOPPING' THEN 106
      WHEN objective = 'LEAD' THEN 107
      WHEN objective = 'PMAX' THEN 108
      ELSE NULL END) AS campaign_type
  , adAccountNo AS customer_id
  , activated AS is_enabled
  , deleted AS is_deleted
FROM {{ array }}
WHERE no IS NOT NULL;

-- Campaign: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- AdSet: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    adgroup_id VARCHAR PRIMARY KEY
  , campaign_id VARCHAR NOT NULL
  , adgroup_name VARCHAR
  , adgroup_type TINYINT -- AdSet: adgroup_type
  , customer_id BIGINT NOT NULL
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
  , bid_amount INTEGER
);

-- AdSet: adgroup_type
SELECT *
FROM UNNEST([
    STRUCT(101 AS type, 'MAX_CLICK' AS code, '성과형-클릭 수 최대화' AS name)
  , STRUCT(102 AS type, 'MAX_CONV' AS code, '성과형-전환 수 최대화' AS name)
  , STRUCT(103 AS type, 'MAX_CONV_VALUE' AS code, '성과형-전환 가치 최대화' AS name)
  , STRUCT(104 AS type, 'NONE' AS code, '성과형-수동 입찰' AS name)
]);

-- AdSet: select
SELECT
    CAST(no AS VARCHAR) AS adgroup_id
  , CAST(campaignNo AS VARCHAR) AS campaign_id
  , name AS adgroup_name
  , (CASE
      WHEN bidGoal = 'MAX_CLICK' THEN 101
      WHEN bidGoal = 'MAX_CONV' THEN 102
      WHEN bidGoal = 'MAX_CONV_VALUE' THEN 103
      WHEN bidGoal = 'NONE' THEN 104
      ELSE NULL END) AS adgroup_type
  , TRY_CAST($account_no AS BIGINT) AS customer_id
  , activated AS is_enabled
  , (status = 'DELETED') AS is_deleted
  , bidPrice AS bid_amount
FROM {{ array }}
WHERE no IS NOT NULL;

-- AdSet: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Creative: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    ad_id VARCHAR PRIMARY KEY
  , adgroup_id VARCHAR NOT NULL
  , ad_type TINYINT -- Creative: creative_type
  , customer_id BIGINT NOT NULL
  , title VARCHAR
  , description VARCHAR
  , landing_url_pc VARCHAR
  , product_id BIGINT
  , is_enabled BOOLEAN
  , is_deleted BOOLEAN
);

-- Creative: creative_type
SELECT *
FROM UNNEST([
    STRUCT(101 AS type, 'SINGLE_IMAGE' AS code, '성과형-네이티브 이미지' AS name)
  , STRUCT(102 AS type, 'MULTIPLE_IMAGE' AS code, '성과형-컬렉션' AS name)
  , STRUCT(103 AS type, 'SINGLE_VIDEO' AS code, '성과형-동영상' AS name)
  , STRUCT(104 AS type, 'IMAGE_BANNER' AS code, '성과형-이미지 배너' AS name)
  , STRUCT(105 AS type, 'CATALOG' AS code, '성과형-카탈로그' AS name)
  , STRUCT(106 AS type, 'COMPOSITION' AS code, '성과형-ADVoost 소재' AS name)
]);

-- Creative: select
SELECT
    CAST(COALESCE(realCreativeNo, no) AS VARCHAR) AS ad_id
  , CAST(adSetNo AS VARCHAR) AS adgroup_id
  , (CASE
      WHEN creativeType = 'SINGLE_IMAGE' THEN 101
      WHEN creativeType = 'MULTIPLE_IMAGE' THEN 102
      WHEN creativeType = 'SINGLE_VIDEO' THEN 103
      WHEN creativeType = 'IMAGE_BANNER' THEN 104
      WHEN creativeType = 'CATALOG' THEN 105
      WHEN creativeType = 'COMPOSITION' THEN 106
      ELSE NULL END) AS ad_type
  , TRY_CAST($account_no AS BIGINT) AS customer_id
  , name AS title
  , item->>'$.message' AS description
  , item->>'$.medias[1].content.linkUrl' AS landing_url_pc
  , TRY_CAST(REGEXP_EXTRACT(item->>'$.medias[1].content.linkUrl', '(\d+)$', 1) AS BIGINT) AS product_id
  , activated AS is_enabled
  , (status = 'DELETED') AS is_deleted
FROM {{ array }} AS item
WHERE no IS NOT NULL;

-- Creative: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- PerformanceReport: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    creative_no BIGINT
  , account_no BIGINT
  , campaign_no BIGINT
  , adset_no BIGINT
  , impression_count INTEGER
  , click_count INTEGER
  , reach_count INTEGER
  , ad_cost INTEGER
  , conv_count INTEGER
  , conv_amount INTEGER
  , ymd DATE
  , PRIMARY KEY (ymd, account_no, creative_no)
);

-- PerformanceReport: select
SELECT
    TRY_CAST("광고 소재 ID" AS BIGINT) AS creative_no
  , TRY_CAST($account_no AS BIGINT) AS account_no
  , TRY_CAST("캠페인 ID" AS BIGINT) AS campaign_no
  , TRY_CAST("광고 그룹 ID" AS BIGINT) AS adset_no
  , TRY_CAST("노출" AS BIGINT) AS impression_count
  , TRY_CAST("클릭" AS BIGINT) AS click_count
  , TRY_CAST("도달" AS BIGINT) AS reach_count
  , TRY_CAST("총 비용" AS BIGINT) AS ad_cost
  , TRY_CAST("총 전환수" AS BIGINT) AS conv_count
  , TRY_CAST("총 전환 매출액" AS BIGINT) AS conv_amount
  , TRY_CAST(STRPTIME("기간", '%Y.%m.%d.') AS DATE) AS ymd
FROM {{ array }}
WHERE (TRY_CAST("광고 소재 ID" AS BIGINT) IS NOT NULL)
  AND (TRY_CAST(STRPTIME("기간", '%Y.%m.%d.') AS DATE) IS NOT NULL);

-- PerformanceReport: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
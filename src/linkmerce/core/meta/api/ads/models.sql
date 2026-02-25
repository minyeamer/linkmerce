-- Common: effective_status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'ACTIVE' AS code, '활성' AS name)
  , STRUCT(1 AS seq, 'PAUSED' AS code, '일시 중지' AS name)
  , STRUCT(2 AS seq, 'DELETED' AS code, '삭제됨' AS name)
  , STRUCT(3 AS seq, 'ARCHIVED' AS code, '보관됨' AS name)
  , STRUCT(4 AS seq, 'PENDING_REVIEW' AS code, '검토 대기' AS name)
  , STRUCT(5 AS seq, 'DISAPPROVED' AS code, '거부됨' AS name)
  , STRUCT(6 AS seq, 'PREAPPROVED' AS code, '사전 승인' AS name)
  , STRUCT(7 AS seq, 'PENDING_BILLING_INFO' AS code, '결제 정보 대기' AS name)
  , STRUCT(8 AS seq, 'CAMPAIGN_PAUSED' AS code, '캠페인 일시 중지' AS name)
  , STRUCT(9 AS seq, 'ADSET_PAUSED' AS code, '광고 세트 일시 중지' AS name)
  , STRUCT(10 AS seq, 'IN_PROCESS' AS code, '처리 중' AS name)
  , STRUCT(11 AS seq, 'WITH_ISSUES' AS code, '문제 발생' AS name)
]);


-- Campaigns: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_id VARCHAR
  , campaign_name VARCHAR
  , account_id VARCHAR
  , objective VARCHAR
  , effective_status VARCHAR
  , created_at TIMESTAMP
  , PRIMARY KEY (account_id, campaign_id)
);

-- Campaigns: select
SELECT
    id AS campaign_id
  , name AS campaign_name
  , $account_id AS account_id
  , objective
  , effective_status
  , TRY_STRPTIME(SUBSTR(created_time, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
FROM {{ array }};

-- Campaigns: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Campaigns: objective
SELECT *
FROM UNNEST([
, STRUCT(0 AS seq, 'OUTCOME_AWARENESS' AS code, '인지도' AS name)
, STRUCT(1 AS seq, 'OUTCOME_ENGAGEMENT' AS code, '참여' AS name)
, STRUCT(2 AS seq, 'OUTCOME_LEADS' AS code, '리드' AS name)
, STRUCT(3 AS seq, 'OUTCOME_SALES' AS code, '판매' AS name)
, STRUCT(4 AS seq, 'OUTCOME_TRAFFIC' AS code, '트래픽' AS name)
, STRUCT(5 AS seq, 'OUTCOME_APP_PROMOTION' AS code, '앱 홍보' AS name)
, STRUCT(6 AS seq, 'OFFER_CLAIMS' AS code, '오퍼 수령' AS name)
, STRUCT(7 AS seq, 'PAGE_LIKES' AS code, '페이지 좋아요' AS name)
, STRUCT(8 AS seq, 'EVENT_RESPONSES' AS code, '이벤트 응답' AS name)
, STRUCT(9 AS seq, 'POST_ENGAGEMENT' AS code, '게시물 참여' AS name)
, STRUCT(10 AS seq, 'WEBSITE_CONVERSIONS' AS code, '웹사이트 전환' AS name)
, STRUCT(11 AS seq, 'LINK_CLICKS' AS code, '링크 클릭' AS name)
, STRUCT(12 AS seq, 'VIDEO_VIEWS' AS code, '동영상 조회' AS name)
, STRUCT(13 AS seq, 'LOCAL_AWARENESS' AS code, '지역 인지도' AS name)
, STRUCT(14 AS seq, 'PRODUCT_CATALOG_SALES' AS code, '카탈로그 판매' AS name)
, STRUCT(15 AS seq, 'LEAD_GENERATION' AS code, '리드 생성' AS name)
, STRUCT(16 AS seq, 'BRAND_AWARENESS' AS code, '브랜드 인지도' AS name)
, STRUCT(17 AS seq, 'STORE_VISITS' AS code, '매장 방문' AS name)
, STRUCT(18 AS seq, 'REACH' AS code, '도달' AS name)
, STRUCT(19 AS seq, 'APP_INSTALLS' AS code, '앱 설치' AS name)
, STRUCT(20 AS seq, 'MESSAGES' AS code, '메시지' AS name)
]);


-- Adsets: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    adset_id VARCHAR
  , adset_name VARCHAR
  , account_id VARCHAR
  , campaign_id VARCHAR NOT NULL
  , effective_status VARCHAR
  , daily_budget INTEGER
  , created_at TIMESTAMP
  , PRIMARY KEY (account_id, adset_id)
);

-- Adsets: select
SELECT
    id AS adset_id
  , name AS adset_name
  , $account_id AS account_id
  , campaign_id AS campaign_id
  , effective_status
  , item->'$.daily_budget' AS daily_budget
  , TRY_STRPTIME(SUBSTR(created_time, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
FROM {{ array }} AS item;

-- Adsets: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Ads: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    ad_id VARCHAR
  , ad_name VARCHAR
  , account_id VARCHAR
  , campaign_id VARCHAR NOT NULL
  , adset_id VARCHAR NOT NULL
  -- , creative_id VARCHAR
  , effective_status VARCHAR
  , created_at TIMESTAMP
  , PRIMARY KEY (account_id, ad_id)
);

-- Ads: select
SELECT
    id AS ad_id
  , name AS ad_name
  , $account_id AS account_id
  , campaign_id AS campaign_id
  , adset_id AS adset_id
  -- , creative.id AS creative_id
  , effective_status
  , TRY_STRPTIME(SUBSTR(created_time, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
FROM {{ array }};

-- Ads: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Insights: create_campaigns
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_id VARCHAR
  , campaign_name VARCHAR
  , account_id VARCHAR
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , objective VARCHAR
  , created_at TIMESTAMP
  , PRIMARY KEY (account_id, campaign_id)
);

-- Insights: create_adsets
CREATE TABLE IF NOT EXISTS {{ table }} (
    adset_id VARCHAR
  , adset_name VARCHAR
  , account_id VARCHAR
  , campaign_id VARCHAR NOT NULL
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , daily_budget INTEGER
  , created_at TIMESTAMP
  , PRIMARY KEY (account_id, adset_id)
);

-- Insights: create_ads
CREATE TABLE IF NOT EXISTS {{ table }} (
    ad_id VARCHAR
  , ad_name VARCHAR
  , account_id VARCHAR
  , campaign_id VARCHAR NOT NULL
  , adset_id VARCHAR NOT NULL
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , created_at TIMESTAMP
  , PRIMARY KEY (account_id, ad_id)
);

-- Insights: select_campaigns
SELECT
    campaign_id
  , campaign_name
  , $account_id AS account_id
  , NULL AS is_active
  , NULL AS is_deleted
  , NULL AS objective
  , NULL AS created_at
FROM {{ array }};

-- Insights: select_adsets
SELECT
    adset_id
  , adset_name
  , $account_id AS account_id
  , campaign_id
  , NULL AS is_active
  , NULL AS is_deleted
  , NULL AS daily_budget
  , NULL AS created_at
FROM {{ array }};

-- Insights: select_ads
SELECT
    ad_id
  , ad_name
  , $account_id AS account_id
  , campaign_id
  , adset_id
  , NULL AS is_active
  , NULL AS is_deleted
  , NULL AS created_at
FROM {{ array }};

-- Insights: insert_campaigns
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Insights: insert_adsets
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Insights: insert_ads
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Insights: create_metrics
CREATE TABLE IF NOT EXISTS {{ table }} (
    account_id VARCHAR
  , campaign_id VARCHAR NOT NULL
  , adset_id VARCHAR NOT NULL
  , ad_id VARCHAR
  , impression_count INTEGER
  , reach_count INTEGER
  , click_count INTEGER
  , link_click_count INTEGER
  , ad_cost INTEGER
  , ymd DATE
  , PRIMARY KEY (ymd, account_id, ad_id)
);

-- Insights: select_metrics
SELECT
    $account_id AS account_id
  , campaign_id
  , adset_id
  , ad_id
  , impressions AS impression_count
  , reach AS reach_count
  , clicks AS click_count
  , inline_link_clicks AS link_click_count
  , spend AS ad_cost
  , TRY_STRPTIME(date_start, '%Y-%m-%d') AS ymd
FROM {{ array }};

-- Insights: insert_metrics
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
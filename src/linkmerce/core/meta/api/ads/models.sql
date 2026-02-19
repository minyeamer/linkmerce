-- AdObjects: status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'ACTIVE' AS code)
  , STRUCT(1 AS seq, 'PAUSED' AS code)
  , STRUCT(2 AS seq, 'DELETED' AS code)
  , STRUCT(3 AS seq, 'ARCHIVED' AS code)
]);

-- AdObjects: effective_status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'ACTIVE' AS code)
  , STRUCT(1 AS seq, 'PAUSED' AS code)
  , STRUCT(2 AS seq, 'DELETED' AS code)
  , STRUCT(3 AS seq, 'ARCHIVED' AS code)
  , STRUCT(4 AS seq, 'PENDING_REVIEW' AS code)
  , STRUCT(5 AS seq, 'DISAPPROVED' AS code)
  , STRUCT(6 AS seq, 'PREAPPROVED' AS code)
  , STRUCT(7 AS seq, 'PENDING_BILLING_INFO' AS code)
  , STRUCT(8 AS seq, 'CAMPAIGN_PAUSED' AS code)
  , STRUCT(9 AS seq, 'ADSET_PAUSED' AS code)
  , STRUCT(10 AS seq, 'IN_PROCESS' AS code)
  , STRUCT(11 AS seq, 'WITH_ISSUES' AS code)
]);


-- Campaigns: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_id VARCHAR
  , campaign_name VARCHAR
  , ad_account VARCHAR
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , objective VARCHAR
  , created_at TIMESTAMP
  , PRIMARY KEY (ad_account, campaign_id)
);

-- Campaigns: select
SELECT
    id AS campaign_id
  , name AS campaign_name
  , $ad_account AS ad_account
  , (effective_status = 'ACTIVE') AS is_active
  , (effective_status = 'DELETED') AS is_deleted
  , objective
  , TRY_STRPTIME(SUBSTR(created_time, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
FROM {{ array }};

-- Campaigns: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Adsets: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    adset_id VARCHAR
  , adset_name VARCHAR
  , campaign_id VARCHAR
  , ad_account VARCHAR
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , daily_budget INTEGER
  , created_at TIMESTAMP
  , PRIMARY KEY (ad_account, campaign_id, adset_id)
);

-- Adsets: select
SELECT
    id AS adset_id
  , name AS adset_name
  , campaign_id AS campaign_id
  , $ad_account AS ad_account
  , (effective_status = 'ACTIVE') AS is_active
  , (effective_status = 'DELETED') AS is_deleted
  , item->'$.daily_budget' AS daily_budget
  , TRY_STRPTIME(SUBSTR(created_time, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
FROM {{ array }} AS item;

-- Adsets: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Ads: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    ad_id VARCHAR
  , ad_name VARCHAR
  , campaign_id VARCHAR
  , adset_id VARCHAR
  -- , creative_id VARCHAR
  , ad_account VARCHAR
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , created_at TIMESTAMP
  , PRIMARY KEY (ad_account, campaign_id, adset_id, ad_id)
);

-- Ads: select
SELECT
    id AS ad_id
  , name AS ad_name
  , campaign_id AS campaign_id
  , adset_id AS adset_id
  -- , creative.id AS creative_id
  , $ad_account AS ad_account
  , (effective_status = 'ACTIVE') AS is_active
  , (effective_status = 'DELETED') AS is_deleted
  , TRY_STRPTIME(SUBSTR(created_time, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
FROM {{ array }};

-- Ads: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- Insights: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_id VARCHAR
  , adset_id VARCHAR
  , ad_id VARCHAR
  , ad_account VARCHAR
  , impression_count INTEGER
  , reach_count INTEGER
  , click_count INTEGER
  , link_click_count INTEGER
  , ad_cost INTEGER
  , ymd DATE
  , PRIMARY KEY (ymd, ad_account, campaign_id, adset_id, ad_id)
);

-- Insights: select
SELECT
    campaign_id
  , adset_id
  , ad_id
  , $ad_account AS ad_account
  , impressions AS impression_count
  , reach AS reach_count
  , clicks AS click_count
  , inline_link_clicks AS link_click_count
  , spend AS ad_cost
  , TRY_STRPTIME(date_start, '%Y-%m-%d') AS ymd
FROM {{ array }};

-- Insights: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
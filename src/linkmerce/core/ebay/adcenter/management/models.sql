-- Common: status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 1 AS code, '운영중' AS name)
  , STRUCT(1 AS seq, 2 AS code, '운영대기' AS name)
  , STRUCT(2 AS seq, 3 AS code, '운영종료' AS name)
  , STRUCT(3 AS seq, 4 AS code, '운영중지' AS name)
  , STRUCT(4 AS seq, 5 AS code, '운영제한' AS name)
]);


-- CampaignGroup: create
CREATE TABLE IF NOT EXISTS {{ table }} (
      campaign_group_id BIGINT NOT NULL
    , campaign_group_name VARCHAR
    , campaign_group_type INTEGER
    , campaign_group_status INTEGER
    -- , impression_count INTEGER
    -- , click_count INTEGER
    -- , ad_cost INTEGER
    -- , conv_count INTEGER
    -- , conv_amount INTEGER
    , PRIMARY KEY (campaign_group_id)
);

-- CampaignGroup: bulk_insert
INSERT INTO {{ table }}
SELECT
    campaignGroupId AS campaign_group_id
  , campaignGroupName AS campaign_group_name
  , CAST(campaignGroupType AS INTEGER) AS campaign_group_type
  , campaignGroupStatus AS campaign_group_status
  -- , impressions AS impression_count
  -- , clicks AS click_count
  -- , spend AS ad_cost
  -- , storeOrders AS conv_count
  -- , storeRevenue AS conv_amount
FROM {{ rows }}
WHERE campaignGroupId IS NOT NULL
ON CONFLICT DO NOTHING;

-- CampaignGroup: campaign_group_name
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 107020 AS code, '통합운영형' AS name)
  , STRUCT(1 AS seq, 109020 AS code, '집중운영형' AS name)
  , STRUCT(2 AS seq, 111020 AS code, '직접운영형' AS name)
]);


-- Campaign: create
CREATE TABLE IF NOT EXISTS {{ table }} (
      campaign_id BIGINT NOT NULL
    , campaign_group_id BIGINT NOT NULL
    , campaign_name VARCHAR
    , campaign_status INTEGER
    , daily_budget INTEGER
    -- , impression_count INTEGER
    -- , click_count INTEGER
    -- , ad_cost INTEGER
    -- , conv_count INTEGER
    -- , conv_amount INTEGER
    , PRIMARY KEY (campaign_id)
);

-- Campaign: bulk_insert
INSERT INTO {{ table }}
SELECT
    campaignId AS campaign_id
  , campaignGroupId AS campaign_group_id
  , campaignName AS campaign_name
  , campaignStatus AS campaign_status
  , dailyBudget AS daily_budget
  -- , impressions AS impression_count
  -- , clicks AS click_count
  -- , spend AS ad_cost
  -- , storeOrders AS conv_count
  -- , storeRevenue AS conv_amount
FROM {{ rows }}
WHERE campaignId IS NOT NULL
  AND campaignGroupId IS NOT NULL
ON CONFLICT DO NOTHING;


-- Product: create
CREATE TABLE IF NOT EXISTS {{ table }} (
      campaign_id BIGINT NOT NULL
    , adgroup_id BIGINT NOT NULL
    , item_id BIGINT NOT NULL
    , item_name VARCHAR
    , adgroup_status INTEGER
    , image_url VARCHAR
    , bid_amount INTEGER
    -- , impression_count INTEGER
    -- , click_count INTEGER
    -- , ad_cost INTEGER
    -- , conv_count INTEGER
    -- , conv_amount INTEGER
    , PRIMARY KEY (item_id)
);

-- Product: bulk_insert
INSERT INTO {{ table }}
SELECT
    campaignId AS campaign_id
  , adgroupId AS adgroup_id
  , itemId AS item_id
  , itemName AS item_name
  , adgroupStatus AS adgroup_status
  , CONCAT('https://', itemImage) AS image_url
  , bidPrice AS big_amount
  -- , impressions AS impression_count
  -- , clicks AS click_count
  -- , spend AS ad_cost
  -- , storeOrders AS conv_count
  -- , storeRevenue AS conv_amount
FROM {{ rows }}
WHERE campaignId IS NOT NULL
  AND adgroupId IS NOT NULL
  AND itemId IS NOT NULL
ON CONFLICT DO NOTHING;
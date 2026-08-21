-- Campaign: create
CREATE TABLE IF NOT EXISTS {{ campaign }} (
    campaign_id BIGINT NOT NULL
  , campaign_name VARCHAR
  , campaign_type VARCHAR -- {'PA': '상품광고'}
  , vendor_id VARCHAR NOT NULL
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
  , PRIMARY KEY (campaign_id)
);

CREATE TABLE IF NOT EXISTS {{ adgroup }} (
    adgroup_id BIGINT NOT NULL
  , adgroup_name VARCHAR
  , vendor_id VARCHAR NOT NULL
  , campaign_id BIGINT NOT NULL
  , goal_type TINYINT -- {0: '매출 성장', 1: '신규 구매 고객 확보', 2: '인지도 상승'}
  , is_active BOOLEAN
  , is_deleted BOOLEAN
  , roas_target INTEGER
  , created_at TIMESTAMP
  , updated_at TIMESTAMP
  , PRIMARY KEY (adgroup_id)
);

-- Campaign: bulk_insert
INSERT INTO {{ campaign }}
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
  , roasTarget AS roas_target
  -- , capType AS cap_type
  -- , calculatedBudget AS calculated_budget
  -- , spentBudget AS spent_budget
  , TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
  , TRY_STRPTIME(SUBSTR(updatedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS updated_at
FROM {{ campaign_rows }}
ON CONFLICT DO NOTHING;

INSERT INTO {{ adgroup }}
SELECT
    id AS adgroup_id
  , name AS adgroup_name
  , $vendor_id AS vendor_id
  , campaignId AS campaign_id
  , (CASE
      WHEN goalType = 'SALES' THEN 0
      WHEN goalType = 'NCA' THEN 1
      WHEN goalType = 'REACH' THEN 2
      ELSE NULL END) AS goal_type
  , isActive AS is_active
  , isDeleted AS is_deleted
  , roasTarget AS roas_target
  , TRY_STRPTIME(SUBSTR(createdAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
  , TRY_STRPTIME(SUBSTR(updatedAt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS updated_at
FROM {{ adgroup_rows }}
ON CONFLICT DO NOTHING;

-- Campaign: goal_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'SALES' AS code, '매출 성장' AS name)
  , STRUCT(1 AS seq, 'NCA' AS code, '신규 구매 고객 확보' AS name)
  , STRUCT(2 AS seq, 'REACH' AS code, '인지도 상승' AS name)
]);


-- Creative: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    creative_id BIGINT NOT NULL
  , option_id BIGINT
  , vendor_id VARCHAR NOT NULL
  , creative_type VARCHAR
  , headline VARCHAR
  -- , description VARCHAR
  -- , image_url VARCHAR
  , ordering INTEGER
  , PRIMARY KEY (creative_id)
);

-- Creative: bulk_insert
INSERT INTO {{ table }}
SELECT
    id AS creative_id
  , vendorItemId AS option_id
  , $vendor_id AS vendor_id
  , creativeType AS creative_type
  , headlineText AS headline
  -- , description
  -- , imageUrl AS image_url
  , ordering
FROM {{ rows }}
ON CONFLICT DO NOTHING;
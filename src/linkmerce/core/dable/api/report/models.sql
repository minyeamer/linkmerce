-- DailyReport: create
CREATE TABLE IF NOT EXISTS {{ report }} (
    campaign_id VARCHAR NOT NULL
  , expose_count INTEGER
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , conv_count INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, campaign_id)
);

CREATE TABLE IF NOT EXISTS {{ campaign }} (
    campaign_id VARCHAR NOT NULL
  , campaign_name VARCHAR
  , PRIMARY KEY (campaign_id)
);

-- DailyReport: bulk_insert
INSERT INTO {{ report }}
SELECT
    COALESCE(campaign_id, '-') AS campaign_id
  , exposes AS expose_count
  , impressions AS impression_count
  , clicks AS click_count
  , cost_spent AS ad_cost
  , convertion_cnt AS conv_count
  , CAST(STRPTIME(ymd, '%Y%m%d') AS DATE) AS ymd
FROM {{ rows }}
WHERE impressions > 0
ON CONFLICT DO NOTHING;

INSERT INTO {{ campaign }}
SELECT
    campaign_id
  , campaign_name
FROM {{ rows }}
WHERE campaign_id IS NOT NULL
ON CONFLICT DO NOTHING;
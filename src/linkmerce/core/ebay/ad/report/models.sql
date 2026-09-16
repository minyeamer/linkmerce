-- AiReport: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    -- seller_id VARCHAR NOT NULL
    site_type TINYINT NOT NULL -- {1: 'auction', 2: 'gmarket'}
  , item_id VARCHAR NOT NULL
  , click_count INTEGER
  , ad_cost INTEGER
  , order_count INTEGER
  , conv_count INTEGER
  , conv_amount INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, site_type, item_id)
);

-- AiReport: bulk_insert
INSERT INTO {{ table }}
SELECT
    -- TRIM(REGEXP_REPLACE(SellerID, '<[^>]+>.*?</[^>]+>', '', 'g')) AS seller_id
    (CASE
      WHEN SellerID LIKE '%auction%' THEN 1
      WHEN SellerID LIKE '%gmarket%' THEN 2
      ELSE 0
    END) AS site_type
  , SiteGoodsNo AS item_id
  , CAST(REPLACE(SumClickCnt, ',', '') AS INTEGER) AS click_count
  , CAST(REPLACE(SumExpense, ',', '') AS INTEGER) AS ad_cost
  , CAST(REPLACE(ItemSumOrderQty, ',', '') AS INTEGER) AS order_count
  , CAST(REPLACE(ItemSumConvertCnt, ',', '') AS INTEGER) AS conv_count
  , CAST(REPLACE(ItemSumConvertAmnt, ',', '') AS INTEGER) AS conv_amount
  , CAST($end_date AS DATE) AS ymd
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- CpcReport: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    site_type TINYINT NOT NULL -- {1: 'auction', 2: 'gmarket'}
  , item_id VARCHAR NOT NULL
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , ad_rank_sum INTEGER
  , conv_count INTEGER
  , conv_amount INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, site_type, item_id)
);

-- CpcReport: bulk_insert
INSERT INTO {{ table }}
SELECT
    siteID AS site_type
  , siteGoodsNo AS item_id
  , exposeCnt AS impression_count
  , clickCnt AS click_count
  , CAST(ROUND(sumClickExpense) AS INTEGER) AS ad_cost
  , sumExposeRank AS ad_rank_sum
  , orderCnt AS conv_count
  , CAST(ROUND(orderAmnt) AS INTEGER) AS conv_amount
  , CAST($end_date AS DATE) AS ymd
FROM {{ rows }}
ON CONFLICT DO NOTHING;
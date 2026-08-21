-- Report: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_group_id BIGINT NOT NULL
  , campaign_id BIGINT NOT NULL
  , item_id BIGINT NOT NULL
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , conv_count INTEGER
  , conv_amount INTEGER
  , cart_count INTEGER
  , sold_count INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, campaign_group_id, campaign_id, item_id)
);

-- Report: bulk_insert
INSERT INTO {{ table }}
SELECT
    groupId AS campaign_group_id
  , campaignId AS campaign_id
  , itemId AS item_id
  , impressions AS impression_count
  , clicks AS click_count
  , spend AS ad_cost
  , productOrders AS conv_count
  , skuRevenue AS conv_amount
  , a2c AS cart_count
  , unitSold AS sold_count
  , CAST(STRPTIME("date", '%Y%m%d') AS DATE) AS ymd
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- ReportDownload: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_group_name VARCHAR NOT NULL
  , campaign_name VARCHAR NOT NULL
  , item_id BIGINT NOT NULL
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , conv_count INTEGER
  , conv_amount INTEGER
  , cart_count INTEGER
  , sold_count INTEGER
  , ymd DATE NOT NULL
  , PRIMARY KEY (ymd, campaign_group_name, campaign_name, item_id)
);

-- ReportDownload: bulk_insert
INSERT INTO {{ table }}
SELECT
    "그룹명" AS campaign_group_name
  , "캠페인명" AS campaign_name
  , "상품번호" AS item_id
  , "노출 수" AS impression_count
  , "클릭 수" AS click_count
  , "광고 비용" AS ad_cost
  , "광고 상품 전환 수" AS conv_count
  , "광고 상품 전환 금액" AS conv_amount
  , "광고 상품 장바구니에 담은 수량" AS cart_count
  , "광고 상품 전환 수량" AS sold_count
  , CAST("날짜" AS DATE) AS ymd
FROM {{ rows }}
ON CONFLICT DO NOTHING;
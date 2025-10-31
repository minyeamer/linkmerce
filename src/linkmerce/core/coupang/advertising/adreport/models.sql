-- MarketingReport: create
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

-- MarketingReport: select
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
    , TRY_CAST("광고전환매출발생 옵션ID" AS BIGINT) AS option_conv_id
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
  AND (option_conv_id IS NOT NULL)
  AND (placement_group IS NOT NULL)
  AND (ymd IS NOT NULL)
GROUP BY ymd, campaign_id, option_id, option_conv_id, placement_group;

-- MarketingReport: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
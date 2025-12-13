-- DailyReport: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    ad_id VARCHAR
  , customer_id INTEGER
  , media_name VARCHAR
  , pc_mobile_type TINYINT -- {0: 'PC', 1: '모바일', 2: '기타'}
  , network_type TINYINT -- {0: '검색', 1: '콘텐츠', 2: '기타'}
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , conv_count INTEGER
  , direct_conv_count INTEGER
  , conv_amount INTEGER
  , direct_conv_amount INTEGER
  , avg_rank DECIMAL(18, 1)
  , page_view_per_visit DECIMAL(18, 2)
  , stay_time_per_visit DECIMAL(18, 2)
  , ymd DATE
  , PRIMARY KEY (ymd, customer_id, pc_mobile_type, network_type, media_name, ad_id)
);

-- DailyReport: select
SELECT
    REPLACE(nccAdId, '(삭제)', '') AS ad_id
  , TRY_CAST($customer_id AS INTEGER) AS customer_id
  , mediaNm AS media_name
  , (CASE WHEN pcMblTp = 'PC' THEN 0 WHEN pcMblTp = '모바일' THEN 1 ELSE 2 END) AS pc_mobile_type
  , (CASE WHEN ntwkTp = '검색' THEN 0 WHEN ntwkTp = '콘텐츠' THEN 1 ELSE 2 END) AS network_type
  , impCnt AS impression_count
  , clkCnt AS click_count
  , salesAmt AS ad_cost
  , ccnt AS conv_count
  , drtCcnt AS direct_conv_count
  , convAmt AS conv_amount
  , drtConvAmt AS direct_conv_amount
  , avgRnk AS avg_rank
  , pv AS page_view_per_visit
  , stayTm AS stay_time_per_visit
  , ymd
FROM {{ array }}
WHERE (nccAdId IS NOT NULL) AND (ymd IS NOT NULL);

-- DailyReport: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
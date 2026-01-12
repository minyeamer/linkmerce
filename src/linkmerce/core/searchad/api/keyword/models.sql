-- Keyword: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    keyword VARCHAR PRIMARY KEY
  , monthly_qc_cnt_pc INTEGER
  , monthly_qc_cnt_mo INTEGER
  , monthly_avg_click_cnt_pc DECIMAL(18, 1)
  , monthly_avg_click_cnt_mo DECIMAL(18, 1)
  , comp_idx TINYINT -- {0: '낮음', 1: '중간', 2: '높음'}
  , avg_depth_pc INTEGER
  , start_date DATE
  , end_date DATE
);

-- Keyword: select
SELECT
    relKeyword AS keyword
  , (CASE
      WHEN monthlyPcQcCnt = '< 10' THEN 10
      ELSE COALESCE(TRY_CAST(monthlyPcQcCnt AS INTEGER), 0) END) AS monthly_qc_cnt_pc
  , (CASE
      WHEN monthlyMobileQcCnt = '< 10' THEN 10
      ELSE COALESCE(TRY_CAST(monthlyMobileQcCnt AS INTEGER), 0) END) AS monthly_qc_cnt_mo
  , monthlyAvePcClkCnt AS monthly_avg_click_cnt_pc
  , monthlyAveMobileClkCnt AS monthly_avg_click_cnt_mo
  , (CASE
      WHEN compIdx = '낮음' THEN 0
      WHEN compIdx = '중간' THEN 1
      WHEN compIdx = '높음' THEN 2
      ELSE NULL END) AS comp_idx
  , plAvgDepth AS avg_depth_pc
  , CURRENT_DATE - INTERVAL 31 DAY AS start_date
  , CURRENT_DATE - INTERVAL 1 DAY AS end_date
FROM {{ array }};

-- Keyword: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
-- TimeContract: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    contract_id VARCHAR PRIMARY KEY
  , adgroup_id VARCHAR NOT NULL
  -- , adgroup_name VARCHAR
  , customer_id INTEGER NOT NULL
  , contract_name VARCHAR
  , contract_type TINYINT -- {0: '브랜드 검색형', 1: '신제품 검색형'}
  , contract_status TINYINT
  , contract_amount INTEGER
  , refund_amount INTEGER
  , contract_qc INTEGER
  , keyword_qc INTEGER
  , register_dt TIMESTAMP
  , edit_dt TIMESTAMP
  , contract_start_date DATE
  , contract_end_date DATE NOT NULL
  , exposure_start_date DATE
  , exposure_end_date DATE
  , cancel_date DATE
);

-- TimeContract: contract_status
SELECT *
FROM UNNEST([
    STRUCT(20 AS type, 'UPCOMING_EXPOSE' AS code, '집행 대기' AS name)
  , STRUCT(21 AS type, 'ON_EXPOSING' AS code, '집행 중' AS name)
  , STRUCT(22 AS type, 'CANCELED_BEFORE_EXPOSING' AS code, '집행 전 취소' AS name)
  , STRUCT(23 AS type, 'CANCELED_ON_EXPOSING' AS code, '집행 중 취소' AS name)
  , STRUCT(24 AS type, 'UPCOMING_CANCEL' AS code, '취소 대기' AS name)
  , STRUCT(30 AS type, 'EXPOSE_COMPLETED' AS code, '종료' AS name)
]);

-- TimeContract: select
SELECT
    nccTimeContractId AS contract_id
  , nccAdgroupId AS adgroup_id
  -- , adgroupName AS adgroup_name
  , customerId AS customer_id
  , contractName AS contract_name
  , 0 AS contract_type
  , (CASE
      WHEN contractStatus = 'UPCOMING_EXPOSE' THEN 20
      WHEN contractStatus = 'ON_EXPOSING' THEN 21
      WHEN contractStatus = 'CANCELED_BEFORE_EXPOSING' THEN 22
      WHEN contractStatus = 'CANCELED_ON_EXPOSING' THEN 23
      WHEN contractStatus = 'UPCOMING_CANCEL' THEN 24
      WHEN contractStatus = 'EXPOSE_COMPLETED' THEN 30
      ELSE NULL END) AS contract_status
  , TRY_CAST(paymentAmt * 1.1 AS INTEGER) AS contract_amount
  , refundAmt AS refund_amount
  , contractQc AS contract_qc
  , totalKeywordQc AS keyword_qc
  , (TRY_CAST(regTm AS TIMESTAMP) + INTERVAL 9 HOUR) AS register_dt
  , (TRY_CAST(editTm AS TIMESTAMP) + INTERVAL 9 HOUR) AS edit_dt
  , TRY_CAST((TRY_CAST(contractStartDt AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS contract_start_date
  , TRY_CAST((TRY_CAST(contractEndDt AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS contract_end_date
  , TRY_CAST((TRY_CAST(exposureStartDt AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS exposure_start_date
  , TRY_CAST((TRY_CAST(exposureEndDt AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS exposure_end_date
  , TRY_CAST((TRY_CAST(cancelTm AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS cancel_date
FROM {{ array }}
WHERE (nccTimeContractId IS NOT NULL)
  AND (nccAdgroupId IS NOT NULL)
  AND (customerId IS NOT NULL)
  AND (TRY_CAST(contractEndDt AS TIMESTAMP) IS NOT NULL);

-- TimeContract: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- BrandNewContract: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    contract_id VARCHAR PRIMARY KEY
  , adgroup_id VARCHAR NOT NULL
  -- , adgroup_name VARCHAR
  , customer_id INTEGER NOT NULL
  , contract_name VARCHAR
  , contract_type TINYINT -- {0: '브랜드 검색형', 1: '신제품 검색형'}
  , contract_status TINYINT
  -- , keyword_group_name VARCHAR
  -- , bidding_round VARCHAR
  -- , bid_amount INTEGER
  , contract_amount INTEGER
  , refund_amount INTEGER
  , contract_qc INTEGER
  , keyword_qc INTEGER
  , register_dt TIMESTAMP
  , edit_dt TIMESTAMP
  , contract_start_date DATE
  , contract_end_date DATE NOT NULL
  , exposure_start_date DATE
  , exposure_end_date DATE
  -- , winning_bid_date DATE
  , cancel_date DATE
);

-- BrandNewContract: contract_status
SELECT *
FROM UNNEST([
    STRUCT(10 AS type, 'BIDDING' AS code, '입찰중' AS name)
  , STRUCT(11 AS type, 'BILLING' AS code, '낙찰(계약 완료 전)' AS name)
  , STRUCT(12 AS type, 'CANCELED_ON_BIDDING' AS code, '입찰 중 취소' AS name)
  , STRUCT(13 AS type, 'REBIDDING' AS code, '재입찰중' AS name)
  , STRUCT(20 AS type, 'UPCOMING_EXPOSE' AS code, '집행 대기(계약 완료)' AS name)
  , STRUCT(21 AS type, 'ON_EXPOSING' AS code, '집행 중' AS name)
  , STRUCT(22 AS type, 'CANCELED_BEFORE_EXPOSING' AS code, '집행 전 취소' AS name)
  , STRUCT(23 AS type, 'CANCELED_ON_EXPOSING' AS code, '집행 중 취소' AS name)
  , STRUCT(30 AS type, 'EXPOSE_COMPLETED' AS code, '종료' AS name)
  , STRUCT(31 AS type, 'DEFEATED' AS code, '종료(낙찰 실패)' AS name)
  , STRUCT(32 AS type, 'BILLING_DEFEATED' AS code, '종료(비즈머니 부족)' AS name)
  , STRUCT(33 AS type, 'FAILED_CONTRACT' AS code, '종료(계약실패)' AS name)
]);

-- BrandNewContract: select
SELECT
    brandNewContractId AS contract_id
  , nccAdgroupId AS adgroup_id
  -- , adgroupName AS adgroup_name
  , customerId AS customer_id
  , contractName AS contract_name
  , 1 AS contract_type
  , (CASE
      WHEN contractStatus = 'BIDDING' THEN 10
      WHEN contractStatus = 'BILLING' THEN 11
      WHEN contractStatus = 'CANCELED_ON_BIDDING' THEN 12
      WHEN contractStatus = 'REBIDDING' THEN 13
      WHEN contractStatus = 'UPCOMING_EXPOSE' THEN 20
      WHEN contractStatus = 'ON_EXPOSING' THEN 21
      WHEN contractStatus = 'CANCELED_BEFORE_EXPOSING' THEN 22
      WHEN contractStatus = 'CANCELED_ON_EXPOSING' THEN 23
      WHEN contractStatus = 'EXPOSE_COMPLETED' THEN 30
      WHEN contractStatus = 'DEFEATED' THEN 31
      WHEN contractStatus = 'BILLING_DEFEATED' THEN 32
      WHEN contractStatus = 'FAILED_CONTRACT' THEN 33
      ELSE NULL END) AS contract_status
  -- , keywordGroupCategoryName AS keyword_group_name
  -- , biddingRound AS bidding_round
  -- , bidAmt AS bid_amount
  , paymentAmt AS contract_amount
  , refundAmt AS refund_amount
  , NULL AS contract_qc
  , NULL AS keyword_qc
  , (TRY_CAST(regTm AS TIMESTAMP) + INTERVAL 9 HOUR) AS register_dt
  , (TRY_CAST(editTm AS TIMESTAMP) + INTERVAL 9 HOUR) AS edit_dt
  , TRY_CAST((TRY_CAST(contractStartDt AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS contract_start_date
  , TRY_CAST((TRY_CAST(contractEndDt AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS contract_end_date
  , TRY_CAST((TRY_CAST(exposureStartDt AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS exposure_start_date
  , TRY_CAST((TRY_CAST(exposureEndDt AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS exposure_end_date
  -- , TRY_CAST((TRY_CAST(winningBidDt AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS winning_bid_date
  , TRY_CAST((TRY_CAST(cancelTm AS TIMESTAMP) + INTERVAL 9 HOUR) AS DATE) AS cancel_date
FROM {{ array }}
WHERE (brandNewContractId IS NOT NULL)
  AND (nccAdgroupId IS NOT NULL)
  AND (customerId IS NOT NULL)
  AND (TRY_CAST(contractEndDt AS TIMESTAMP) IS NOT NULL);

-- BrandNewContract: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;
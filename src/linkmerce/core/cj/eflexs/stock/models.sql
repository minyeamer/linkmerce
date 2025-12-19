-- Stock: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    item_code VARCHAR NOT NULL
  , barcode VARCHAR
  , customer_id BIGINT NOT NULL
  -- , customer_name VARCHAR
  , item_name VARCHAR
  , warehouse_code VARCHAR
  , warehouse_name VARCHAR
  , zone_code VARCHAR
  , location_name VARCHAR
  , lot_no BIGINT
  , total_quantity INTEGER
  , usable_quantity INTEGER
  , hold_quantity INTEGER
  , process_quantity INTEGER
  , remain_days INTEGER
  , validate_date DATE
  , inbound_date DATE
  , updated_at TIMESTAMP
);

-- Stock: select
SELECT
    itemCd AS item_code
  , itemVarcode AS barcode
  , TRY_CAST(strrId AS BIGINT) AS customer_id
  -- , strrNm AS customer_name
  , itemNm AS item_name
  , whCd AS warehouse_code
  , whNm AS warehouse_name
  , zoneCd AS zone_code
  , wcellNm AS location_name
  , TRY_CAST(lotNo AS BIGINT) AS lot_no
  , invnQty AS total_quantity
  , avlbQty AS usable_quantity
  , hldQty AS hold_quantity
  , prcsQty AS process_quantity
  , TRY_CAST(remainInvnDays AS INTEGER) AS remain_days
  , TRY_CAST(validDatetime AS DATE) AS validate_date
  , TRY_CAST(STRPTIME(CAST(inbDate AS VARCHAR), '%Y%m%d') AS DATE) AS inbound_date
  , CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP) AS TIMESTAMP) AS updated_at
FROM {{ array }}
WHERE (itemCd IS NOT NULL)
  AND (TRY_CAST(strrId AS BIGINT) IS NOT NULL);

-- Stock: insert
INSERT INTO {{ table }} {{ values }};
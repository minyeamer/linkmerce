-- Invoice: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    invoice_no VARCHAR NOT NULL
  , package_no VARCHAR
  , order_id VARCHAR
  , product_name VARCHAR
  , delivery_type VARCHAR
  , package_type VARCHAR
  , box_type VARCHAR
  , sbs_box_type VARCHAR
  , order_quantity INTEGER
  , sku_quantity INTEGER
  , delivery_fee INTEGER
  , shipping_cost INTEGER
  , jeju_cost INTEGER
  , extra_cost INTEGER
  , service_cost INTEGER
  , box_cost INTEGER
  , pickup_date DATE NOT NULL
  , PRIMARY KEY (invoice_no)
);

-- Invoice: bulk_insert
INSERT INTO {{ table }}
SELECT
    hawbNo AS invoice_no
  , refOrdNo AS package_no
  , custOrdNo AS order_id
  , repItemNm AS product_name
  , dtldlvNm AS delivery_type
  , ctryNm AS package_type
  , boxTypeNm AS box_type
  , sbsBoxType AS sbs_box_type
  , COALESCE(TRY_CAST(skuCnt AS INTEGER), 0) AS order_quantity
  , COALESCE(TRY_CAST(totalQty AS INTEGER), 0) AS sku_quantity
  , COALESCE(TRY_CAST(outbAmt AS INTEGER), 0) AS delivery_fee
  , COALESCE(TRY_CAST(feryAmt AS INTEGER), 0) AS shipping_cost
  , COALESCE(TRY_CAST(jejuAmt AS INTEGER), 0) AS jeju_cost
  , COALESCE(TRY_CAST(etcAmt AS INTEGER), 0) AS extra_cost
  , COALESCE(TRY_CAST(svcAmt AS INTEGER), 0) AS service_cost
  , COALESCE(TRY_CAST(smTotlAmt AS INTEGER), 0) AS box_cost
  , CAST(TRY_STRPTIME(billDate, '%Y%m%d') AS DATE) AS pickup_date
FROM {{ rows }}
WHERE TRY_STRPTIME(billDate, '%Y%m%d') IS NOT NULL
ON CONFLICT DO NOTHING;


-- InvoiceOrder: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    invoice_no VARCHAR NOT NULL
  , package_no VARCHAR
  , order_id VARCHAR NOT NULL
  , delivery_fee INTEGER
  , box_cost INTEGER
  , order_date DATE NOT NULL
  , pickup_date DATE NOT NULL
  , PRIMARY KEY (invoice_no)
);

-- InvoiceOrder: bulk_insert
INSERT INTO {{ table }}
SELECT
    hawbNo AS invoice_no
  , refOrdNo AS package_no
  , custOrdNo AS order_id
  , COALESCE(TRY_CAST(outbAmt AS INTEGER), 0)
    + COALESCE(TRY_CAST(feryAmt AS INTEGER), 0)
    + COALESCE(TRY_CAST(jejuAmt AS INTEGER), 0)
    + COALESCE(TRY_CAST(etcAmt AS INTEGER), 0)
    + COALESCE(TRY_CAST(svcAmt AS INTEGER), 0) AS delivery_fee
  , COALESCE(TRY_CAST(smTotlAmt AS INTEGER), 0) AS box_cost
  , CAST(TRY_STRPTIME(LEFT(custOrdNo, 8), '%Y%m%d') AS DATE) AS order_date
  , CAST(TRY_STRPTIME(billDate, '%Y%m%d') AS DATE) AS pickup_date
FROM {{ rows }}
WHERE TRY_STRPTIME(billDate, '%Y%m%d') IS NOT NULL
  AND TRY_STRPTIME(LEFT(custOrdNo, 8), '%Y%m%d') IS NOT NULL
ON CONFLICT DO NOTHING;
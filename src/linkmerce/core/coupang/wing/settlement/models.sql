-- RocketSettlement: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    group_key VARCHAR PRIMARY KEY
  , vendor_id VARCHAR
  , settlement_ratio INTEGER
  , settlement_amount INTEGER
  , sales_amount INTEGER
  , refund_amount INTEGER
  , commission_amount INTEGER
  , seller_discount INTEGER
  , seller_instant_discount INTEGER
  , seller_download_discount INTEGER
  , payable_amount INTEGER
  , milk_run_cost INTEGER
  , ad_cost INTEGER
  , additional_cost INTEGER
  , deduction_amount INTEGER
  , cfs_fee INTEGER
  , warehousing_fee INTEGER
  , fulfillment_fee INTEGER
  , storage_fee INTEGER
  , return_reverse_shopping_fee INTEGER
  , return_grading_fee INTEGER
  , return_handling_fee INTEGER
  , barcode_labeling_fee INTEGER
  , last_unpaid_cfs_fee INTEGER
  , past_cfs_fee INTEGER
  , carry_over_fee INTEGER
  , cfs_inventory_compensation_amount INTEGER
  , start_date TIMESTAMP NOT NULL
  , end_date TIMESTAMP NOT NULL
);

-- RocketSettlement: select
SELECT
    settlementGroupKey AS group_key
  , $vendor_id AS vendor_id
  , SUM(settlementRatio) OVER (PARTITION BY settlementGroupKey) AS settlement_ratio -- 지급비율
  , SUM(finalSettlementAmount) OVER (PARTITION BY settlementGroupKey) AS settlement_amount -- 최종지급액 = totalFinalSettlementAmount
  , TRY_CAST(settlementStatusReportDetail.totalSalesAmount AS INTEGER) AS sales_amount -- 판매액(a)
  , TRY_CAST(settlementStatusReportDetail.totalRefundedAmount AS INTEGER) AS refund_amount -- 취소액(b) (< 0)
  , settlementStatusReportDetail.totalTakeRateAmountWithVat AS commission_amount -- 판매수수료(B)
  , TRY_CAST(settlementStatusReportDetail.totalSellerDiscount AS INTEGER) AS seller_discount -- 판매자할인쿠폰(C)
  , TRY_CAST(settlementStatusReportDetail.totalSellerFundedInstantDiscount AS INTEGER) AS seller_instant_discount -- 즉시할인쿠폰
  , TRY_CAST(settlementStatusReportDetail.totalSellerFundedDownloadDiscount AS INTEGER) AS seller_download_discount -- 다운로드쿠폰
  , SUM(settlementStatusReportDetail.totalPayableAmount) OVER (PARTITION BY settlementGroupKey) AS payable_amount -- 지급액(H)
  , settlementStatusReportDetail.totalMilkRunDeductionAmount AS milk_run_cost -- 밀크런 이용액(c)
  , settlementStatusReportDetail.totalAdSalesDeductionAmount AS ad_cost -- 광고비(d)
  , settlementStatusReportDetail.totalAdditionalDeductionAmount AS additional_cost -- 정산 차감(e)
  , settlementStatusReportDetail.totalNegativeDeductionAmount AS deduction_amount -- 추가 상계금액(I)
  , settlementStatusReportDetail.totalFinalCfsFeeDeductionAmount AS cfs_fee -- 전체 비용(f)
  , settlementStatusReportDetail.totalWarehousingFeeDeductionAmount AS warehousing_fee -- 입출고비
  , settlementStatusReportDetail.totalFulfillmentFeeDeductionAmount AS fulfillment_fee -- 배송비
  , settlementStatusReportDetail.totalStorageFeeDeductionAmount AS storage_fee -- 보관비
  , settlementStatusReportDetail.totalCreturnReverseShippingFeeDeductionAmount AS return_reverse_shopping_fee -- 반품 회수비
  , settlementStatusReportDetail.totalCreturnGradingFeeDeductionAmount AS return_grading_fee -- 반품 재입고비
  , settlementStatusReportDetail.totalVreturnHandlingFeeDeductionAmount AS return_handling_fee -- 반출비
  , settlementStatusReportDetail.totalBarcodeLabelingFeeDeductionAmount AS barcode_labeling_fee -- 바코드 부가 서비스비
  , settlementStatusReportDetail.totalLastSettlementUnpaidCfsDeductionAmount AS last_unpaid_cfs_fee -- 지난 정산 미납 비용
  , settlementStatusReportDetail.totalPastCfsDeductionAmount AS past_cfs_fee -- 기납부된 비용
  , settlementStatusReportDetail.totalCarryOverSettlementDeductionAmount AS carry_over_fee -- 다음 정산으로 이월(g)
  , settlementStatusReportDetail.totalCfsInventoryCompensationAmount AS cfs_inventory_compensation_amount -- 재고 손실 보상 (K)
  , (TRY_CAST(settlementPeriodStartDate AS TIMESTAMP) + INTERVAL 9 HOUR) AS start_date -- 매출인식일(시작)
  , (TRY_CAST(settlementPeriodEndDate AS TIMESTAMP) + INTERVAL 9 HOUR) AS end_date -- 매출인식일(종료)
FROM {{ array }}
WHERE (settlementGroupKey IS NOT NULL) AND (settlementPeriodStartDate IS NOT NULL) AND (settlementPeriodEndDate IS NOT NULL);

-- RocketSettlement: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- RocketSettlementDownload: create_sales
CREATE TABLE IF NOT EXISTS {{ table }} (
    order_id BIGINT
  , vendor_id VARCHAR
  , product_id BIGINT
  , option_id BIGINT
  , sku_id BIGINT
  -- , product_name VARCHAR
  -- , option_name VARCHAR
  , category_id INTEGER
  -- , category_name VARCHAR
  , settlement_type TINYINT
  , period_type TINYINT
  , unit_price INTEGER
  , order_quantity INTEGER
  -- , order_amount INTEGER
  , coupang_discount INTEGER
  -- , sales_amount INTEGER
  -- , seller_instant_discount INTEGER
  -- , seller_download_discount INTEGER
  , seller_discount INTEGER
  , settlement_amount INTEGER
  -- , commission_rate DECIMAL(18, 1)
  -- , commission_amount INTEGER
  , sales_date DATE
  , settlement_date DATE
  , PRIMARY KEY (vendor_id, order_id, option_id, settlement_type)
);

-- RocketSettlementDownload: select_sales
SELECT
    TRY_CAST("주문ID" AS BIGINT) AS order_id
  , $vendor_id AS vendor_id
  , TRY_CAST("등록상품 ID" AS BIGINT) AS product_id
  , TRY_CAST("옵션ID" AS BIGINT) AS option_id
  , TRY_CAST("SKU ID" AS BIGINT) AS sku_id
  -- , "등록상품명" AS product_name
  -- , "옵션명" AS option_name
  , TRY_CAST("카테고리ID" AS INTEGER) AS category_id
  -- , "카테고리명" AS category_name
  , (CASE WHEN "거래유형" = '주문 정산' THEN 0 WHEN "거래유형" = '주문 정산취소' THEN 1 ELSE 99 END) AS settlement_type
  , (CASE WHEN "정산유형" = '주정산' THEN 0 WHEN "정산유형" = '월정산' THEN 1 ELSE 99 END) AS period_type
  , TRY_CAST("판매가(A)" AS INTEGER) AS unit_price
  , TRY_CAST("판매수량(B)" AS INTEGER) AS order_quantity
  -- , TRY_CAST("판매액(A*B)" AS INTEGER) AS order_amount
  , TRY_CAST("쿠팡지원할인(C)" AS INTEGER) AS coupang_discount
  -- , TRY_CAST("매출금액(A*B-C)" AS INTEGER) AS sales_amount
  -- , TRY_CAST("즉시할인쿠폰(D)" AS INTEGER) AS seller_instant_discount
  -- , TRY_CAST("다운로드쿠폰(E)" AS INTEGER) AS seller_download_discount
  , TRY_CAST("판매자할인쿠폰(D+E)" AS INTEGER) AS seller_discount
  , TRY_CAST("정산대상액" AS INTEGER) AS settlement_amount
  -- , "판매수수료율(%,VAT별도)" AS commission_rate
  -- , (TRY_CAST("판매수수료" AS INTEGER) + TRY_CAST("판매수수료 VAT" AS INTEGER)) AS commission_amount
  , TRY_CAST("매출인식일" AS DATE) AS sales_date
  , TRY_CAST("정산주기(종료일)" AS DATE) AS settlement_date
FROM {{ array }}
WHERE (TRY_CAST("주문ID" AS BIGINT) IS NOT NULL) AND (TRY_CAST("옵션ID" AS BIGINT) IS NOT NULL)

-- RocketSettlementDownload: insert_sales
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;


-- RocketSettlementDownload: create_shipping
CREATE TABLE IF NOT EXISTS {{ table }} (
    order_id BIGINT
  , invoice_no BIGINT
  , vendor_id VARCHAR
  , product_id BIGINT
  , option_id BIGINT
  , sku_id BIGINT
  -- , product_name VARCHAR
  -- , option_name VARCHAR
  -- , category_name1 VARCHAR
  -- , category_name2 VARCHAR
  -- , product_size INTEGER
  -- , warehouse VARCHAR
  , settlement_type TINYINT
  , period_type TINYINT
  -- , unit_price INTEGER
  -- , unit_quantity INTEGER
  -- , order_quantity INTEGER
  , warehousing_fee INTEGER
  , discount_amount INTEGER
  , extra_fee INTEGER
  , order_date DATE
  , sales_date DATE
  , settlement_date DATE
  , PRIMARY KEY (vendor_id, order_id, option_id, settlement_type)
);

-- RocketSettlementDownload: select_shipping
SELECT
    TRY_CAST("주문ID" AS BIGINT) AS order_id
  , TRY_CAST("배송ID" AS BIGINT) AS invoice_no
  , $vendor_id AS vendor_id
  , TRY_CAST("등록상품 ID" AS BIGINT) AS product_id
  , TRY_CAST("옵션ID" AS BIGINT) AS option_id
  , TRY_CAST("SKU ID" AS BIGINT) AS sku_id
  -- , "등록상품명" AS product_name
  -- , "옵션명" AS option_name
  -- , "1차" AS category_name1
  -- , "2차" AS category_name2
  -- , "개별포장 상품 사이즈" AS product_size
  -- , "물류센터" AS warehouse
  , (CASE
      WHEN "거래유형" = '입출고비 정산' THEN 2
      WHEN "거래유형" = '입출고비 정산취소' THEN 3
      WHEN "거래유형" = '배송비 정산' THEN 4
      WHEN "거래유형" = '배송비 정산취소' THEN 5
      ELSE 99 END) AS settlement_type
  , (CASE WHEN "정산유형" = '주정산' THEN 0 WHEN "정산유형" = '월정산' THEN 1 ELSE 99 END) AS period_type
  -- , TRY_CAST("단품 판매가" AS INTEGER) AS unit_price
  -- , TRY_CAST("단품 기준 구매 수량" AS INTEGER) AS unit_quantity
  -- , TRY_CAST("판매수량" AS INTEGER) AS order_quantity
  , TRY_CAST("발생비용(A)" AS INTEGER) AS warehousing_fee
  , TRY_CAST("할인가(B)" AS INTEGER) AS discount_amount
  , TRY_CAST(item->'$.추가비용' AS INTEGER) AS extra_fee
  , TRY_CAST("주문일" AS DATE) AS order_date
  , TRY_CAST("매출인식일" AS DATE) AS sales_date
  , TRY_CAST("정산주기(종료일)" AS DATE) AS settlement_date
FROM {{ array }} AS item
WHERE (TRY_CAST("주문ID" AS BIGINT) IS NOT NULL) AND (TRY_CAST("옵션ID" AS BIGINT) IS NOT NULL)

-- RocketSettlementDownload: insert_shipping
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- RocketSettlementDownload: settlement_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, '주문 정산' AS name)
  , STRUCT(1 AS seq, '주문 정산취소' AS name)
    STRUCT(2 AS seq, '입출고비 정산' AS name)
  , STRUCT(3 AS seq, '입출고비 정산취소' AS name)
  , STRUCT(4 AS seq, '배송비 정산' AS name)
  , STRUCT(5 AS seq, '배송비 정산취소' AS name)
]);

-- RocketSettlementDownload: period_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, '주정산' AS name)
  , STRUCT(1 AS seq, '월정산' AS name)
]);

-- RocketSettlementDownload: product_size
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, '극소형' AS name)
  , STRUCT(1 AS seq, '소형' AS name)
  , STRUCT(2 AS seq, '중형' AS name)
  , STRUCT(3 AS seq, '대형1' AS name)
  , STRUCT(4 AS seq, '대형2' AS name)
  , STRUCT(5 AS seq, '특대형' AS name)
]);
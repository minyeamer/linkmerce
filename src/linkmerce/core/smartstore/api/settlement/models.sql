-- Settlement: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    product_order_id HUGEINT NOT NULL
  , order_id BIGINT NOT NULL
  , channel_seq BIGINT NOT NULL
  , product_id BIGINT
  , product_order_type VARCHAR
  , settle_type TINYINT
  , payment_amount INTEGER
  , pay_commision_amount INTEGER
  , free_installment_commision_amount INTEGER
  , selling_interlock_commision_amount INTEGER
  , benefit_settle_amount INTEGER
  , settle_expect_amount INTEGER
  , payment_date DATE NOT NULL
  , settlement_date DATE
  , PRIMARY KEY (product_order_id)
);

-- Settlement: bulk_insert
INSERT INTO {{ table }}
SELECT
    CAST(productOrderId AS HUGEINT) AS product_order_id
  , CAST(orderId AS BIGINT) AS order_id
  , $channel_seq AS channel_seq
  , TRY_CAST(productId AS BIGINT) AS product_id
  , productOrderType AS product_order_type
  , (CASE
        WHEN settleType = 'NORMAL_SETTLE_ORIGINAL' THEN 0
        WHEN settleType = 'NORMAL_SETTLE_AFTER_CANCEL' THEN 1
        WHEN settleType = 'NORMAL_SETTLE_BEFORE_CANCEL' THEN 2
        WHEN settleType = 'QUICK_SETTLE_ORIGINAL' THEN 3
        WHEN settleType = 'QUICK_SETTLE_CANCEL' THEN 4
        WHEN settleType = 'QUANTITY_CANCEL_DEDUCTION' THEN 5
        WHEN settleType = 'QUANTITY_CANCEL_RESTORE' THEN 6
      ELSE NULL END) AS settle_type
  , paySettleAmount AS payment_amount
  , totalPayCommissionAmount AS pay_commision_amount
  , freeInstallmentCommissionAmount AS free_installment_commision_amount
  , sellingInterlockCommissionAmount AS selling_interlock_commision_amount
  , benefitSettleAmount AS benefit_settle_amount
  , settleExpectAmount AS settle_expect_amount
  , TRY_CAST(TRY_STRPTIME(payDate, '%Y-%m-%d') AS DATE) AS payment_date
  , TRY_CAST(TRY_STRPTIME(settleCompleteDate, '%Y-%m-%d') AS DATE) AS settlement_date
FROM {{ rows }}
WHERE TRY_CAST(TRY_STRPTIME(payDate, '%Y-%m-%d') AS DATE) IS NOT NULL
ON CONFLICT DO NOTHING;

-- Settlement: product_order_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'PROD_ORDER' AS code, '상품 주문' AS name)
  , STRUCT(1 AS seq, 'DELIVERY' AS code, '배송비' AS name)
  , STRUCT(2 AS seq, 'EXTRAFEE' AS code, '기타 비용' AS name)
  , STRUCT(3 AS seq, 'WITHDRAW' AS code, '결제 수단 출금' AS name)
  , STRUCT(4 AS seq, 'REFUND' AS code, '구매자 환불' AS name)
  , STRUCT(5 AS seq, 'PL_REFUND' AS code, '후불 결제 환불' AS name)
  , STRUCT(6 AS seq, 'DEDUCTION_RESTORE' AS code, '기타 공제 환급' AS name)
  , STRUCT(7 AS seq, 'PROD_PAY' AS code, '상품 결제' AS name)
  , STRUCT(8 AS seq, 'PURCHASE_REVIEW' AS code, '텍스트 리뷰' AS name)
  , STRUCT(9 AS seq, 'PREMIUM_PURCHASE_REVIEW' AS code, '포토/동영상 리뷰' AS name)
  , STRUCT(10 AS seq, 'REGULAR_PURCHASE_REVIEW' AS code, '알림받기 동의 회원 리뷰 추가 적립' AS name)
  , STRUCT(11 AS seq, 'ONE_MONTH_PURCHASE_REVIEW' AS code, '한 달 사용 텍스트 리뷰' AS name)
  , STRUCT(12 AS seq, 'ONE_MONTH_PREMIUM_PURCHASE_REVIEW' AS code, '한 달 사용 포토/동영상 리뷰' AS name)
  , STRUCT(13 AS seq, 'REVIEW' AS code, '리뷰 적립' AS name)
  , STRUCT(14 AS seq, 'ETC_COUPON' AS code, '기타 할인' AS name)
  , STRUCT(15 AS seq, 'QUICK_SETTLE' AS code, '빠른정산' AS name)
  , STRUCT(16 AS seq, 'QUANTITY_CANCEL' AS code, '수량 취소' AS name)
  , STRUCT(17 AS seq, 'DIFFERENCE_SETTLE' AS code, '차액 정산' AS name)
  , STRUCT(18 AS seq, 'DEPOSIT_SETTLE' AS code, '보증금' AS name)
  , STRUCT(19 AS seq, 'RENTAL_ORDER' AS code, '렌탈 주문' AS name)
  , STRUCT(20 AS seq, 'MANUAL_ORDER' AS code, '수기 주문' AS name)
  , STRUCT(21 AS seq, 'RENTAL_SCHEDULED_ORDER' AS code, '월 렌탈료 주문' AS name)
  , STRUCT(22 AS seq, 'PREFERENTIAL_COMMISSION' AS code, '우대 수수료 환급' AS name)
  , STRUCT(23 AS seq, 'POINT_ACCUMULATION' AS code, '포인트 적립' AS name)
  , STRUCT(24 AS seq, 'POST_ORDER_ADJUSTMENT_AMOUNT' AS code, '주문 후 변동 금액' AS name)
  , STRUCT(25 AS seq, 'CSF' AS code, '통관 대행료' AS name)
  , STRUCT(26 AS seq, 'CONCESSION' AS code, '구매자 보상' AS name)
]);

-- Settlement: settle_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'NORMAL_SETTLE_ORIGINAL' AS code, '일반 정산' AS name)
  , STRUCT(1 AS seq, 'NORMAL_SETTLE_AFTER_CANCEL' AS code, '정산 후 취소' AS name)
  , STRUCT(2 AS seq, 'NORMAL_SETTLE_BEFORE_CANCEL' AS code, '정산 전 취소' AS name)
  , STRUCT(3 AS seq, 'QUICK_SETTLE_ORIGINAL' AS code, '빠른정산' AS name)
  , STRUCT(4 AS seq, 'QUICK_SETTLE_CANCEL' AS code, '빠른정산 회수' AS name)
  , STRUCT(5 AS seq, 'QUANTITY_CANCEL_DEDUCTION' AS code, '수량 취소 정산(공제)' AS name)
  , STRUCT(6 AS seq, 'QUANTITY_CANCEL_RESTORE' AS code, '수량 취소 정산(환급)' AS name)
]);
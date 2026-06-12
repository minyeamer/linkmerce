WITH

campaign_type_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT(0 AS seq, 'DEMAND_GEN' AS code, '디맨드젠' AS label)
    , STRUCT(1 AS seq, 'DISPLAY' AS code, '디스플레이' AS label)
    , STRUCT(2 AS seq, 'HOTEL' AS code, '호텔' AS label)
    , STRUCT(3 AS seq, 'LOCAL' AS code, '지역' AS label)
    , STRUCT(4 AS seq, 'LOCAL_SERVICES' AS code, '지역 서비스' AS label)
    , STRUCT(5 AS seq, 'MULTI_CHANNEL' AS code, '다채널' AS label)
    , STRUCT(6 AS seq, 'PERFORMANCE_MAX' AS code, '실적 최대화' AS label)
    , STRUCT(7 AS seq, 'SEARCH' AS code, '검색' AS label)
    , STRUCT(8 AS seq, 'SHOPPING' AS code, '쇼핑' AS label)
    , STRUCT(9 AS seq, 'SMART' AS code, '스마트' AS label)
    , STRUCT(10 AS seq, 'TRAVEL' AS code, '여행' AS label)
    , STRUCT(11 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS label)
    , STRUCT(12 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS label)
    , STRUCT(13 AS seq, 'VIDEO' AS code, '동영상' AS label)
  ])
),

bidding_strategy_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT(0 AS seq, 'COMMISSION' AS code, '수수료' AS label)
    , STRUCT(1 AS seq, 'ENHANCED_CPC' AS code, '향상된 CPC' AS label)
    , STRUCT(2 AS seq, 'FIXED_CPM' AS code, '고정 CPM' AS label)
    , STRUCT(3 AS seq, 'FIXED_SHARE_OF_VOICE' AS code, '고정 음성 공유 비율' AS label)
    , STRUCT(4 AS seq, 'INVALID' AS code, '잘못됨' AS label)
    , STRUCT(5 AS seq, 'MANUAL_CPA' AS code, '수동 CPA' AS label)
    , STRUCT(6 AS seq, 'MANUAL_CPC' AS code, '수동 CPC' AS label)
    , STRUCT(7 AS seq, 'MANUAL_CPM' AS code, '수동 CPM' AS label)
    , STRUCT(8 AS seq, 'MANUAL_CPV' AS code, '수동 CPV' AS label)
    , STRUCT(9 AS seq, 'MAXIMIZE_CONVERSIONS' AS code, '전환 수 최대화' AS label)
    , STRUCT(10 AS seq, 'MAXIMIZE_CONVERSION_VALUE' AS code, '전환 가치 최대화' AS label)
    , STRUCT(11 AS seq, 'PAGE_ONE_PROMOTED' AS code, '1페이지 상단 홍보' AS label)
    , STRUCT(12 AS seq, 'PERCENT_CPC' AS code, '비율 CPC' AS label)
    , STRUCT(13 AS seq, 'TARGET_CPA' AS code, '목표 CPA' AS label)
    , STRUCT(14 AS seq, 'TARGET_CPC' AS code, '목표 CPC' AS label)
    , STRUCT(15 AS seq, 'TARGET_CPM' AS code, '목표 CPM' AS label)
    , STRUCT(16 AS seq, 'TARGET_CPV' AS code, '목표 CPV' AS label)
    , STRUCT(17 AS seq, 'TARGET_IMPRESSION_SHARE' AS code, '노출 수 공유 목표' AS label)
    , STRUCT(18 AS seq, 'TARGET_OUTRANK_SHARE' AS code, '경쟁 우위 공유 목표' AS label)
    , STRUCT(19 AS seq, 'TARGET_ROAS' AS code, '목표 ROAS' AS label)
    , STRUCT(20 AS seq, 'TARGET_SPEND' AS code, '목표 지출' AS label)
    , STRUCT(21 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS label)
    , STRUCT(22 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS label)
  ])
),

status_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT(0 AS seq, 'ENABLED' AS code, '운영 가능' AS label)
    , STRUCT(1 AS seq, 'PAUSED' AS code, '일시중지됨' AS label)
    , STRUCT(2 AS seq, 'REMOVED' AS code, '삭제됨' AS label)
    , STRUCT(3 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS label)
    , STRUCT(4 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS label)
  ])
),

campaign_master AS (
  SELECT
      cmp.customer_id
    , acc.account_name
    -- Campaign attrs
    , cmp.campaign_id
    , CONCAT(
          COALESCE(CAST(campaign_status.seq AS STRING), '9')
        , COALESCE(FORMAT('%02d', campaign_type.seq), '99')
      ) AS campaign_seq
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    , campaign_status.label AS campaign_status
    , bidding_strategy.label AS bidding_strategy
    , cmp.created_at
  FROM {{ source('google_ads', 'campaign') }} AS cmp
  LEFT JOIN {{ source('google_ads', 'account') }} AS acc
    ON cmp.customer_id = acc.customer_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN bidding_strategy_mapping AS bidding_strategy
    ON cmp.bidding_strategy = bidding_strategy.code
  LEFT JOIN status_mapping AS campaign_status
    ON cmp.campaign_status = campaign_status.code
)

SELECT * FROM campaign_master

WITH ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '구글'
),

campaign_type_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT('DEMAND_GEN' AS code, '디맨드젠' AS label)
    , STRUCT('DISPLAY' AS code, '디스플레이' AS label)
    , STRUCT('HOTEL' AS code, '호텔' AS label)
    , STRUCT('LOCAL' AS code, '지역' AS label)
    , STRUCT('LOCAL_SERVICES' AS code, '지역 서비스' AS label)
    , STRUCT('MULTI_CHANNEL' AS code, '다채널' AS label)
    , STRUCT('PERFORMANCE_MAX' AS code, '실적 최대화' AS label)
    , STRUCT('SEARCH' AS code, '검색' AS label)
    , STRUCT('SHOPPING' AS code, '쇼핑' AS label)
    , STRUCT('SMART' AS code, '스마트' AS label)
    , STRUCT('TRAVEL' AS code, '여행' AS label)
    , STRUCT('UNKNOWN' AS code, '알 수 없음' AS label)
    , STRUCT('UNSPECIFIED' AS code, '지정되지 않음' AS label)
    , STRUCT('VIDEO' AS code, '동영상' AS label)
  ])
),

bidding_strategy_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT('COMMISSION' AS code, '수수료' AS label)
    , STRUCT('ENHANCED_CPC' AS code, '향상된 CPC' AS label)
    , STRUCT('FIXED_CPM' AS code, '고정 CPM' AS label)
    , STRUCT('FIXED_SHARE_OF_VOICE' AS code, '고정 음성 공유 비율' AS label)
    , STRUCT('INVALID' AS code, '잘못됨' AS label)
    , STRUCT('MANUAL_CPA' AS code, '수동 CPA' AS label)
    , STRUCT('MANUAL_CPC' AS code, '수동 CPC' AS label)
    , STRUCT('MANUAL_CPM' AS code, '수동 CPM' AS label)
    , STRUCT('MANUAL_CPV' AS code, '수동 CPV' AS label)
    , STRUCT('MAXIMIZE_CONVERSIONS' AS code, '전환 수 최대화' AS label)
    , STRUCT('MAXIMIZE_CONVERSION_VALUE' AS code, '전환 가치 최대화' AS label)
    , STRUCT('PAGE_ONE_PROMOTED' AS code, '1페이지 상단 홍보' AS label)
    , STRUCT('PERCENT_CPC' AS code, '비율 CPC' AS label)
    , STRUCT('TARGET_CPA' AS code, '목표 CPA' AS label)
    , STRUCT('TARGET_CPC' AS code, '목표 CPC' AS label)
    , STRUCT('TARGET_CPM' AS code, '목표 CPM' AS label)
    , STRUCT('TARGET_CPV' AS code, '목표 CPV' AS label)
    , STRUCT('TARGET_IMPRESSION_SHARE' AS code, '노출 수 공유 목표' AS label)
    , STRUCT('TARGET_OUTRANK_SHARE' AS code, '경쟁 우위 공유 목표' AS label)
    , STRUCT('TARGET_ROAS' AS code, '목표 ROAS' AS label)
    , STRUCT('TARGET_SPEND' AS code, '목표 지출' AS label)
    , STRUCT('UNKNOWN' AS code, '알 수 없음' AS label)
    , STRUCT('UNSPECIFIED' AS code, '지정되지 않음' AS label)
  ])
),

adgroup_type_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT('DISPLAY_STANDARD' AS code, '표준 디스플레이' AS label)
    , STRUCT('HOTEL_ADS' AS code, '호텔 광고' AS label)
    , STRUCT('PROMOTED_HOTEL_ADS' AS code, '홍보 호텔 광고' AS label)
    , STRUCT('SEARCH_DYNAMIC_ADS' AS code, '동적 검색 광고' AS label)
    , STRUCT('SEARCH_STANDARD' AS code, '표준 검색' AS label)
    , STRUCT('SHOPPING_COMPARISON_LISTING_ADS' AS code, '쇼핑 비교 목록 광고' AS label)
    , STRUCT('SHOPPING_PRODUCT_ADS' AS code, '쇼핑 제품 광고' AS label)
    , STRUCT('SHOPPING_SMART_ADS' AS code, '쇼핑 스마트 광고' AS label)
    , STRUCT('SMART_CAMPAIGN_ADS' AS code, '스마트 캠페인 광고' AS label)
    , STRUCT('TRAVEL_ADS' AS code, '여행 광고' AS label)
    , STRUCT('UNKNOWN' AS code, '알 수 없음' AS label)
    , STRUCT('UNSPECIFIED' AS code, '지정되지 않음' AS label)
    , STRUCT('VIDEO_BUMPER' AS code, '범퍼 동영상' AS label)
    , STRUCT('VIDEO_EFFICIENT_REACH' AS code, '효율적 도달 동영상' AS label)
    , STRUCT('VIDEO_NON_SKIPPABLE_IN_STREAM' AS code, '비건너뛰기 인스트림' AS label)
    , STRUCT('VIDEO_RESPONSIVE' AS code, '반응형 동영상' AS label)
    , STRUCT('VIDEO_TRUE_VIEW_IN_DISPLAY' AS code, '디스플레이 진정한 조회' AS label)
    , STRUCT('VIDEO_TRUE_VIEW_IN_STREAM' AS code, '인스트림 진정한 조회' AS label)
    , STRUCT('YOUTUBE_AUDIO' AS code, '유튜브 오디오' AS label)
  ])
),

ad_type_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT('APP_AD' AS code, '앱 광고' AS label)
    , STRUCT('APP_ENGAGEMENT_AD' AS code, '앱 참여 광고' AS label)
    , STRUCT('APP_PRE_REGISTRATION_AD' AS code, '앱 사전 등록 광고' AS label)
    , STRUCT('CALL_AD' AS code, '전화 광고' AS label)
    , STRUCT('DEMAND_GEN_CAROUSEL_AD' AS code, '디맨드젠 캐러셀 광고' AS label)
    , STRUCT('DEMAND_GEN_MULTI_ASSET_AD' AS code, '디맨드젠 다중 자산 광고' AS label)
    , STRUCT('DEMAND_GEN_PRODUCT_AD' AS code, '디맨드젠 제품 광고' AS label)
    , STRUCT('DEMAND_GEN_VIDEO_RESPONSIVE_AD' AS code, '디맨드젠 반응형 동영상 광고' AS label)
    , STRUCT('DYNAMIC_HTML5_AD' AS code, '동적 HTML5 광고' AS label)
    , STRUCT('EXPANDED_DYNAMIC_SEARCH_AD' AS code, '확장 동적 검색 광고' AS label)
    , STRUCT('EXPANDED_TEXT_AD' AS code, '확장 텍스트 광고' AS label)
    , STRUCT('HOTEL_AD' AS code, '호텔 광고' AS label)
    , STRUCT('HTML5_UPLOAD_AD' AS code, 'HTML5 업로드 광고' AS label)
    , STRUCT('IMAGE_AD' AS code, '이미지 광고' AS label)
    , STRUCT('IN_FEED_VIDEO_AD' AS code, '피드 내 동영상 광고' AS label)
    , STRUCT('LEGACY_APP_INSTALL_AD' AS code, '레거시 앱 설치 광고' AS label)
    , STRUCT('LEGACY_RESPONSIVE_DISPLAY_AD' AS code, '레거시 반응형 디스플레이' AS label)
    , STRUCT('LOCAL_AD' AS code, '지역 광고' AS label)
    , STRUCT('RESPONSIVE_DISPLAY_AD' AS code, '반응형 디스플레이 광고' AS label)
    , STRUCT('RESPONSIVE_SEARCH_AD' AS code, '반응형 검색 광고' AS label)
    , STRUCT('SHOPPING_COMPARISON_LISTING_AD' AS code, '쇼핑 비교 목록 광고' AS label)
    , STRUCT('SHOPPING_PRODUCT_AD' AS code, '쇼핑 제품 광고' AS label)
    , STRUCT('SHOPPING_SMART_AD' AS code, '쇼핑 스마트 광고' AS label)
    , STRUCT('SMART_CAMPAIGN_AD' AS code, '스마트 캠페인 광고' AS label)
    , STRUCT('TEXT_AD' AS code, '텍스트 광고' AS label)
    , STRUCT('TRAVEL_AD' AS code, '여행 광고' AS label)
    , STRUCT('UNKNOWN' AS code, '알 수 없음' AS label)
    , STRUCT('UNSPECIFIED' AS code, '지정되지 않음' AS label)
    , STRUCT('VIDEO_AD' AS code, '동영상 광고' AS label)
    , STRUCT('VIDEO_BUMPER_AD' AS code, '범퍼 동영상 광고' AS label)
    , STRUCT('VIDEO_NON_SKIPPABLE_IN_STREAM_AD' AS code, '비건너뛰기 인스트림 동영상' AS label)
    , STRUCT('VIDEO_RESPONSIVE_AD' AS code, '반응형 동영상 광고' AS label)
    , STRUCT('VIDEO_TRUEVIEW_IN_STREAM_AD' AS code, '인스트림 TrueView 광고' AS label)
    , STRUCT('YOUTUBE_AUDIO_AD' AS code, '유튜브 오디오 광고' AS label)
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

master_report AS (
  SELECT
      ad.customer_id
    , acc.account_name
    -- Campaign attrs
    , ad.campaign_id
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    , bidding_strategy.label AS bidding_strategy
    -- Adgroup attrs
    , ad.adgroup_id
    , grp.adgroup_name
    , adgroup_type.label AS adgroup_type
    -- Ad attrs
    , ad.ad_id
    , ad.ad_name
    , ad_type.label AS ad_type
    , status_fin.label AS ad_status
    , COALESCE(
          rel_ad.bundle_product_ids
        , rel_grp.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , '900000'
      ) AS bundle_product_ids
    , cmp.created_at
  FROM {{ source('google_ads', 'ad') }} AS ad
  LEFT JOIN {{ source('google_ads', 'account') }} AS acc
    ON ad.customer_id = acc.customer_id
  LEFT JOIN {{ source('google_ads', 'campaign') }} AS cmp
    ON ad.campaign_id = cmp.campaign_id
  LEFT JOIN {{ source('google_ads', 'adgroup') }} AS grp
    ON ad.adgroup_id = grp.adgroup_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN bidding_strategy_mapping AS bidding_strategy
    ON cmp.bidding_strategy = bidding_strategy.code
  LEFT JOIN adgroup_type_mapping AS adgroup_type
    ON grp.adgroup_type = adgroup_type.code
  LEFT JOIN ad_type_mapping AS ad_type
    ON ad.ad_type = ad_type.code
  -- Resolve final status label
  LEFT JOIN status_mapping AS status_cmp
    ON cmp.campaign_status = status_cmp.code
  LEFT JOIN status_mapping AS status_grp
    ON grp.adgroup_status = status_grp.code
  LEFT JOIN status_mapping AS status_ad
    ON ad.ad_status = status_ad.code
  LEFT JOIN status_mapping AS status_fin
    ON GREATEST(status_cmp.seq, status_grp.seq, status_ad.seq) = status_fin.seq
  -- Resolve bundle_product_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON ad.campaign_id = rel_cmp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_grp
    ON ad.adgroup_id = rel_grp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON ad.ad_id = rel_ad.ad_id
)

SELECT * FROM master_report

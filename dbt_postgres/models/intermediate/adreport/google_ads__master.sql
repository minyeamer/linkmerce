WITH{{var("line_break")

}} ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '구글'
),{{var("line_break")

}} campaign_type_mapping AS (
  SELECT *
  FROM (
    VALUES
      ('DEMAND_GEN', '디맨드젠')
    , ('DISPLAY', '디스플레이')
    , ('HOTEL', '호텔')
    , ('LOCAL', '지역')
    , ('LOCAL_SERVICES', '지역 서비스')
    , ('MULTI_CHANNEL', '다채널')
    , ('PERFORMANCE_MAX', '실적 최대화')
    , ('SEARCH', '검색')
    , ('SHOPPING', '쇼핑')
    , ('SMART', '스마트')
    , ('TRAVEL', '여행')
    , ('UNKNOWN', '알 수 없음')
    , ('UNSPECIFIED', '지정되지 않음')
    , ('VIDEO', '동영상')
  ) AS _t(code, label)
),{{var("line_break")

}} bidding_strategy_mapping AS (
  SELECT *
  FROM (
    VALUES
      ('COMMISSION', '수수료')
    , ('ENHANCED_CPC', '향상된 CPC')
    , ('FIXED_CPM', '고정 CPM')
    , ('FIXED_SHARE_OF_VOICE', '고정 음성 공유 비율')
    , ('INVALID', '잘못됨')
    , ('MANUAL_CPA', '수동 CPA')
    , ('MANUAL_CPC', '수동 CPC')
    , ('MANUAL_CPM', '수동 CPM')
    , ('MANUAL_CPV', '수동 CPV')
    , ('MAXIMIZE_CONVERSIONS', '전환 수 최대화')
    , ('MAXIMIZE_CONVERSION_VALUE', '전환 가치 최대화')
    , ('PAGE_ONE_PROMOTED', '1페이지 상단 홍보')
    , ('PERCENT_CPC', '비율 CPC')
    , ('TARGET_CPA', '목표 CPA')
    , ('TARGET_CPC', '목표 CPC')
    , ('TARGET_CPM', '목표 CPM')
    , ('TARGET_CPV', '목표 CPV')
    , ('TARGET_IMPRESSION_SHARE', '노출 수 공유 목표')
    , ('TARGET_OUTRANK_SHARE', '경쟁 우위 공유 목표')
    , ('TARGET_ROAS', '목표 ROAS')
    , ('TARGET_SPEND', '목표 지출')
    , ('UNKNOWN', '알 수 없음')
    , ('UNSPECIFIED', '지정되지 않음')
  ) AS _t(code, label)
),{{var("line_break")

}} adgroup_type_mapping AS (
  SELECT *
  FROM (
    VALUES
      ('DISPLAY_STANDARD', '표준 디스플레이')
    , ('HOTEL_ADS', '호텔 광고')
    , ('PROMOTED_HOTEL_ADS', '홍보 호텔 광고')
    , ('SEARCH_DYNAMIC_ADS', '동적 검색 광고')
    , ('SEARCH_STANDARD', '표준 검색')
    , ('SHOPPING_COMPARISON_LISTING_ADS', '쇼핑 비교 목록 광고')
    , ('SHOPPING_PRODUCT_ADS', '쇼핑 제품 광고')
    , ('SHOPPING_SMART_ADS', '쇼핑 스마트 광고')
    , ('SMART_CAMPAIGN_ADS', '스마트 캠페인 광고')
    , ('TRAVEL_ADS', '여행 광고')
    , ('UNKNOWN', '알 수 없음')
    , ('UNSPECIFIED', '지정되지 않음')
    , ('VIDEO_BUMPER', '범퍼 동영상')
    , ('VIDEO_EFFICIENT_REACH', '효율적 도달 동영상')
    , ('VIDEO_NON_SKIPPABLE_IN_STREAM', '비건너뛰기 인스트림')
    , ('VIDEO_RESPONSIVE', '반응형 동영상')
    , ('VIDEO_TRUE_VIEW_IN_DISPLAY', '디스플레이 진정한 조회')
    , ('VIDEO_TRUE_VIEW_IN_STREAM', '인스트림 진정한 조회')
    , ('YOUTUBE_AUDIO', '유튜브 오디오')
  ) AS _t(code, label)
),{{var("line_break")

}} ad_type_mapping AS (
  SELECT *
  FROM (
    VALUES
      ('APP_AD', '앱 광고')
    , ('APP_ENGAGEMENT_AD', '앱 참여 광고')
    , ('APP_PRE_REGISTRATION_AD', '앱 사전 등록 광고')
    , ('CALL_AD', '전화 광고')
    , ('DEMAND_GEN_CAROUSEL_AD', '디맨드젠 캐러셀 광고')
    , ('DEMAND_GEN_MULTI_ASSET_AD', '디맨드젠 다중 자산 광고')
    , ('DEMAND_GEN_PRODUCT_AD', '디맨드젠 제품 광고')
    , ('DEMAND_GEN_VIDEO_RESPONSIVE_AD', '디맨드젠 반응형 동영상 광고')
    , ('DYNAMIC_HTML5_AD', '동적 HTML5 광고')
    , ('EXPANDED_DYNAMIC_SEARCH_AD', '확장 동적 검색 광고')
    , ('EXPANDED_TEXT_AD', '확장 텍스트 광고')
    , ('HOTEL_AD', '호텔 광고')
    , ('HTML5_UPLOAD_AD', 'HTML5 업로드 광고')
    , ('IMAGE_AD', '이미지 광고')
    , ('IN_FEED_VIDEO_AD', '피드 내 동영상 광고')
    , ('LEGACY_APP_INSTALL_AD', '레거시 앱 설치 광고')
    , ('LEGACY_RESPONSIVE_DISPLAY_AD', '레거시 반응형 디스플레이')
    , ('LOCAL_AD', '지역 광고')
    , ('RESPONSIVE_DISPLAY_AD', '반응형 디스플레이 광고')
    , ('RESPONSIVE_SEARCH_AD', '반응형 검색 광고')
    , ('SHOPPING_COMPARISON_LISTING_AD', '쇼핑 비교 목록 광고')
    , ('SHOPPING_PRODUCT_AD', '쇼핑 제품 광고')
    , ('SHOPPING_SMART_AD', '쇼핑 스마트 광고')
    , ('SMART_CAMPAIGN_AD', '스마트 캠페인 광고')
    , ('TEXT_AD', '텍스트 광고')
    , ('TRAVEL_AD', '여행 광고')
    , ('UNKNOWN', '알 수 없음')
    , ('UNSPECIFIED', '지정되지 않음')
    , ('VIDEO_AD', '동영상 광고')
    , ('VIDEO_BUMPER_AD', '범퍼 동영상 광고')
    , ('VIDEO_NON_SKIPPABLE_IN_STREAM_AD', '비건너뛰기 인스트림 동영상')
    , ('VIDEO_RESPONSIVE_AD', '반응형 동영상 광고')
    , ('VIDEO_TRUEVIEW_IN_STREAM_AD', '인스트림 TrueView 광고')
    , ('YOUTUBE_AUDIO_AD', '유튜브 오디오 광고')
  ) AS _t(code, label)
),{{var("line_break")

}} status_mapping AS (
  SELECT *
  FROM (
    VALUES
      (0, 'ENABLED', '운영 가능')
    , (1, 'PAUSED', '일시중지됨')
    , (2, 'REMOVED', '삭제됨')
    , (3, 'UNKNOWN', '알 수 없음')
    , (4, 'UNSPECIFIED', '지정되지 않음')
  ) AS _t(seq, code, label)
),{{var("line_break")

}} master_report AS (
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

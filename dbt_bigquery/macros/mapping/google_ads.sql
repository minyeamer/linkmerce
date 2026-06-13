{% macro google_ads__campaign_type_mapping() -%}
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
{%- endmacro %}

{% macro google_ads__bidding_strategy_mapping() -%}
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
{%- endmacro %}

{% macro google_ads__adgroup_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'DISPLAY_STANDARD' AS code, '표준 디스플레이' AS label)
  , STRUCT(1 AS seq, 'HOTEL_ADS' AS code, '호텔 광고' AS label)
  , STRUCT(2 AS seq, 'PROMOTED_HOTEL_ADS' AS code, '홍보 호텔 광고' AS label)
  , STRUCT(3 AS seq, 'SEARCH_DYNAMIC_ADS' AS code, '동적 검색 광고' AS label)
  , STRUCT(4 AS seq, 'SEARCH_STANDARD' AS code, '표준 검색' AS label)
  , STRUCT(5 AS seq, 'SHOPPING_COMPARISON_LISTING_ADS' AS code, '쇼핑 비교 목록 광고' AS label)
  , STRUCT(6 AS seq, 'SHOPPING_PRODUCT_ADS' AS code, '쇼핑 제품 광고' AS label)
  , STRUCT(7 AS seq, 'SHOPPING_SMART_ADS' AS code, '쇼핑 스마트 광고' AS label)
  , STRUCT(8 AS seq, 'SMART_CAMPAIGN_ADS' AS code, '스마트 캠페인 광고' AS label)
  , STRUCT(9 AS seq, 'TRAVEL_ADS' AS code, '여행 광고' AS label)
  , STRUCT(10 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS label)
  , STRUCT(11 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS label)
  , STRUCT(12 AS seq, 'VIDEO_BUMPER' AS code, '범퍼 동영상' AS label)
  , STRUCT(13 AS seq, 'VIDEO_EFFICIENT_REACH' AS code, '효율적 도달 동영상' AS label)
  , STRUCT(14 AS seq, 'VIDEO_NON_SKIPPABLE_IN_STREAM' AS code, '비건너뛰기 인스트림' AS label)
  , STRUCT(15 AS seq, 'VIDEO_RESPONSIVE' AS code, '반응형 동영상' AS label)
  , STRUCT(16 AS seq, 'VIDEO_TRUE_VIEW_IN_DISPLAY' AS code, '디스플레이 진정한 조회' AS label)
  , STRUCT(17 AS seq, 'VIDEO_TRUE_VIEW_IN_STREAM' AS code, '인스트림 진정한 조회' AS label)
  , STRUCT(18 AS seq, 'YOUTUBE_AUDIO' AS code, '유튜브 오디오' AS label)
])
{%- endmacro %}

{% macro google_ads__ad_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'APP_AD' AS code, '앱 광고' AS label)
  , STRUCT(1 AS seq, 'APP_ENGAGEMENT_AD' AS code, '앱 참여 광고' AS label)
  , STRUCT(2 AS seq, 'APP_PRE_REGISTRATION_AD' AS code, '앱 사전 등록 광고' AS label)
  , STRUCT(3 AS seq, 'CALL_AD' AS code, '전화 광고' AS label)
  , STRUCT(4 AS seq, 'DEMAND_GEN_CAROUSEL_AD' AS code, '디맨드젠 캐러셀 광고' AS label)
  , STRUCT(5 AS seq, 'DEMAND_GEN_MULTI_ASSET_AD' AS code, '디맨드젠 다중 자산 광고' AS label)
  , STRUCT(6 AS seq, 'DEMAND_GEN_PRODUCT_AD' AS code, '디맨드젠 제품 광고' AS label)
  , STRUCT(7 AS seq, 'DEMAND_GEN_VIDEO_RESPONSIVE_AD' AS code, '디맨드젠 반응형 동영상 광고' AS label)
  , STRUCT(8 AS seq, 'DYNAMIC_HTML5_AD' AS code, '동적 HTML5 광고' AS label)
  , STRUCT(9 AS seq, 'EXPANDED_DYNAMIC_SEARCH_AD' AS code, '확장 동적 검색 광고' AS label)
  , STRUCT(10 AS seq, 'EXPANDED_TEXT_AD' AS code, '확장 텍스트 광고' AS label)
  , STRUCT(11 AS seq, 'HOTEL_AD' AS code, '호텔 광고' AS label)
  , STRUCT(12 AS seq, 'HTML5_UPLOAD_AD' AS code, 'HTML5 업로드 광고' AS label)
  , STRUCT(13 AS seq, 'IMAGE_AD' AS code, '이미지 광고' AS label)
  , STRUCT(14 AS seq, 'IN_FEED_VIDEO_AD' AS code, '피드 내 동영상 광고' AS label)
  , STRUCT(15 AS seq, 'LEGACY_APP_INSTALL_AD' AS code, '레거시 앱 설치 광고' AS label)
  , STRUCT(16 AS seq, 'LEGACY_RESPONSIVE_DISPLAY_AD' AS code, '레거시 반응형 디스플레이' AS label)
  , STRUCT(17 AS seq, 'LOCAL_AD' AS code, '지역 광고' AS label)
  , STRUCT(18 AS seq, 'RESPONSIVE_DISPLAY_AD' AS code, '반응형 디스플레이 광고' AS label)
  , STRUCT(19 AS seq, 'RESPONSIVE_SEARCH_AD' AS code, '반응형 검색 광고' AS label)
  , STRUCT(20 AS seq, 'SHOPPING_COMPARISON_LISTING_AD' AS code, '쇼핑 비교 목록 광고' AS label)
  , STRUCT(21 AS seq, 'SHOPPING_PRODUCT_AD' AS code, '쇼핑 제품 광고' AS label)
  , STRUCT(22 AS seq, 'SHOPPING_SMART_AD' AS code, '쇼핑 스마트 광고' AS label)
  , STRUCT(23 AS seq, 'SMART_CAMPAIGN_AD' AS code, '스마트 캠페인 광고' AS label)
  , STRUCT(24 AS seq, 'TEXT_AD' AS code, '텍스트 광고' AS label)
  , STRUCT(25 AS seq, 'TRAVEL_AD' AS code, '여행 광고' AS label)
  , STRUCT(26 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS label)
  , STRUCT(27 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS label)
  , STRUCT(28 AS seq, 'VIDEO_AD' AS code, '동영상 광고' AS label)
  , STRUCT(29 AS seq, 'VIDEO_BUMPER_AD' AS code, '범퍼 동영상 광고' AS label)
  , STRUCT(30 AS seq, 'VIDEO_NON_SKIPPABLE_IN_STREAM_AD' AS code, '비건너뛰기 인스트림 동영상' AS label)
  , STRUCT(31 AS seq, 'VIDEO_RESPONSIVE_AD' AS code, '반응형 동영상 광고' AS label)
  , STRUCT(32 AS seq, 'VIDEO_TRUEVIEW_IN_STREAM_AD' AS code, '인스트림 TrueView 광고' AS label)
  , STRUCT(33 AS seq, 'YOUTUBE_AUDIO_AD' AS code, '유튜브 오디오 광고' AS label)
])
{%- endmacro %}

{% macro google_ads__status_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'ENABLED' AS code, '운영 가능' AS label)
  , STRUCT(1 AS seq, 'PAUSED' AS code, '일시중지됨' AS label)
  , STRUCT(2 AS seq, 'REMOVED' AS code, '삭제됨' AS label)
  , STRUCT(3 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS label)
  , STRUCT(4 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS label)
])
{%- endmacro %}

{% macro google_ads__device_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, '데스크톱' AS label)
  , STRUCT(1 AS seq, 1 AS code, '모바일' AS label)
  , STRUCT(2 AS seq, 2 AS code, '태블릿' AS label)
  , STRUCT(3 AS seq, 3 AS code, '연결된 TV' AS label)
  , STRUCT(4 AS seq, 4 AS code, '기타' AS label)
  , STRUCT(5 AS seq, 5 AS code, '알 수 없음' AS label)
  , STRUCT(6 AS seq, 6 AS code, '지정되지 않음' AS label)
])
{%- endmacro %}

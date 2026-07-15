{% macro google_ads__campaign_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 'DEMAND_GEN', '디맨드젠')
  , (1, 'DISPLAY', '디스플레이')
  , (2, 'HOTEL', '호텔')
  , (3, 'LOCAL', '지역')
  , (4, 'LOCAL_SERVICES', '지역 서비스')
  , (5, 'MULTI_CHANNEL', '다채널')
  , (6, 'PERFORMANCE_MAX', '실적 최대화')
  , (7, 'SEARCH', '검색')
  , (8, 'SHOPPING', '쇼핑')
  , (9, 'SMART', '스마트')
  , (10, 'TRAVEL', '여행')
  , (11, 'UNKNOWN', '알 수 없음')
  , (12, 'UNSPECIFIED', '지정되지 않음')
  , (13, 'VIDEO', '동영상')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro google_ads__bidding_strategy_mapping() -%}
SELECT *
FROM (VALUES
    (0, 'COMMISSION', '수수료')
  , (1, 'ENHANCED_CPC', '향상된 CPC')
  , (2, 'FIXED_CPM', '고정 CPM')
  , (3, 'FIXED_SHARE_OF_VOICE', '고정 음성 공유 비율')
  , (4, 'INVALID', '잘못됨')
  , (5, 'MANUAL_CPA', '수동 CPA')
  , (6, 'MANUAL_CPC', '수동 CPC')
  , (7, 'MANUAL_CPM', '수동 CPM')
  , (8, 'MANUAL_CPV', '수동 CPV')
  , (9, 'MAXIMIZE_CONVERSIONS', '전환 수 최대화')
  , (10, 'MAXIMIZE_CONVERSION_VALUE', '전환 가치 최대화')
  , (11, 'PAGE_ONE_PROMOTED', '1페이지 상단 홍보')
  , (12, 'PERCENT_CPC', '비율 CPC')
  , (13, 'TARGET_CPA', '목표 CPA')
  , (14, 'TARGET_CPC', '목표 CPC')
  , (15, 'TARGET_CPM', '목표 CPM')
  , (16, 'TARGET_CPV', '목표 CPV')
  , (17, 'TARGET_IMPRESSION_SHARE', '노출 수 공유 목표')
  , (18, 'TARGET_OUTRANK_SHARE', '경쟁 우위 공유 목표')
  , (19, 'TARGET_ROAS', '목표 ROAS')
  , (20, 'TARGET_SPEND', '목표 지출')
  , (21, 'UNKNOWN', '알 수 없음')
  , (22, 'UNSPECIFIED', '지정되지 않음')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro google_ads__adgroup_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 'DISPLAY_STANDARD', '표준 디스플레이')
  , (1, 'HOTEL_ADS', '호텔 광고')
  , (2, 'PROMOTED_HOTEL_ADS', '홍보 호텔 광고')
  , (3, 'SEARCH_DYNAMIC_ADS', '동적 검색 광고')
  , (4, 'SEARCH_STANDARD', '표준 검색')
  , (5, 'SHOPPING_COMPARISON_LISTING_ADS', '쇼핑 비교 목록 광고')
  , (6, 'SHOPPING_PRODUCT_ADS', '쇼핑 제품 광고')
  , (7, 'SHOPPING_SMART_ADS', '쇼핑 스마트 광고')
  , (8, 'SMART_CAMPAIGN_ADS', '스마트 캠페인 광고')
  , (9, 'TRAVEL_ADS', '여행 광고')
  , (10, 'UNKNOWN', '알 수 없음')
  , (11, 'UNSPECIFIED', '지정되지 않음')
  , (12, 'VIDEO_BUMPER', '범퍼 동영상')
  , (13, 'VIDEO_EFFICIENT_REACH', '효율적 도달 동영상')
  , (14, 'VIDEO_NON_SKIPPABLE_IN_STREAM', '비건너뛰기 인스트림')
  , (15, 'VIDEO_RESPONSIVE', '반응형 동영상')
  , (16, 'VIDEO_TRUE_VIEW_IN_DISPLAY', '디스플레이 진정한 조회')
  , (17, 'VIDEO_TRUE_VIEW_IN_STREAM', '인스트림 진정한 조회')
  , (18, 'YOUTUBE_AUDIO', '유튜브 오디오')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro google_ads__ad_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 'APP_AD', '앱 광고')
  , (1, 'APP_ENGAGEMENT_AD', '앱 참여 광고')
  , (2, 'APP_PRE_REGISTRATION_AD', '앱 사전 등록 광고')
  , (3, 'CALL_AD', '전화 광고')
  , (4, 'DEMAND_GEN_CAROUSEL_AD', '디맨드젠 캐러셀 광고')
  , (5, 'DEMAND_GEN_MULTI_ASSET_AD', '디맨드젠 다중 자산 광고')
  , (6, 'DEMAND_GEN_PRODUCT_AD', '디맨드젠 제품 광고')
  , (7, 'DEMAND_GEN_VIDEO_RESPONSIVE_AD', '디맨드젠 반응형 동영상 광고')
  , (8, 'DYNAMIC_HTML5_AD', '동적 HTML5 광고')
  , (9, 'EXPANDED_DYNAMIC_SEARCH_AD', '확장 동적 검색 광고')
  , (10, 'EXPANDED_TEXT_AD', '확장 텍스트 광고')
  , (11, 'HOTEL_AD', '호텔 광고')
  , (12, 'HTML5_UPLOAD_AD', 'HTML5 업로드 광고')
  , (13, 'IMAGE_AD', '이미지 광고')
  , (14, 'IN_FEED_VIDEO_AD', '피드 내 동영상 광고')
  , (15, 'LEGACY_APP_INSTALL_AD', '레거시 앱 설치 광고')
  , (16, 'LEGACY_RESPONSIVE_DISPLAY_AD', '레거시 반응형 디스플레이')
  , (17, 'LOCAL_AD', '지역 광고')
  , (18, 'RESPONSIVE_DISPLAY_AD', '반응형 디스플레이 광고')
  , (19, 'RESPONSIVE_SEARCH_AD', '반응형 검색 광고')
  , (20, 'SHOPPING_COMPARISON_LISTING_AD', '쇼핑 비교 목록 광고')
  , (21, 'SHOPPING_PRODUCT_AD', '쇼핑 제품 광고')
  , (22, 'SHOPPING_SMART_AD', '쇼핑 스마트 광고')
  , (23, 'SMART_CAMPAIGN_AD', '스마트 캠페인 광고')
  , (24, 'TEXT_AD', '텍스트 광고')
  , (25, 'TRAVEL_AD', '여행 광고')
  , (26, 'UNKNOWN', '알 수 없음')
  , (27, 'UNSPECIFIED', '지정되지 않음')
  , (28, 'VIDEO_AD', '동영상 광고')
  , (29, 'VIDEO_BUMPER_AD', '범퍼 동영상 광고')
  , (30, 'VIDEO_NON_SKIPPABLE_IN_STREAM_AD', '비건너뛰기 인스트림 동영상')
  , (31, 'VIDEO_RESPONSIVE_AD', '반응형 동영상 광고')
  , (32, 'VIDEO_TRUEVIEW_IN_STREAM_AD', '인스트림 TrueView 광고')
  , (33, 'YOUTUBE_AUDIO_AD', '유튜브 오디오 광고')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro google_ads__status_mapping() -%}
SELECT *
FROM (VALUES
    (0, 'ENABLED', '운영 가능')
  , (1, 'PAUSED', '일시중지됨')
  , (2, 'REMOVED', '삭제됨')
  , (3, 'UNKNOWN', '알 수 없음')
  , (4, 'UNSPECIFIED', '지정되지 않음')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro google_ads__device_type_mapping() -%}
SELECT *
FROM (VALUES
    (0, 0, '데스크톱')
  , (1, 1, '모바일')
  , (2, 2, '태블릿')
  , (3, 3, '연결된 TV')
  , (4, 4, '기타')
  , (5, 5, '알 수 없음')
  , (6, 6, '지정되지 않음')
) AS mapping(seq, code, label)
{%- endmacro %}

-- Common: status
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'ENABLED' AS code, '운영 가능' AS name)
  , STRUCT(1 AS seq, 'PAUSED' AS code, '일시중지됨' AS name)
  , STRUCT(2 AS seq, 'REMOVED' AS code, '삭제됨' AS name)
  , STRUCT(3 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS name)
  , STRUCT(4 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS name)
]);


-- Campaign: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    campaign_id VARCHAR
  , campaign_name VARCHAR
  , customer_id VARCHAR
  , campaign_type VARCHAR
  , campaign_status VARCHAR
  , bidding_strategy VARCHAR
  , campaign_budget INTEGER
  , impression_count_30d INTEGER
  , click_count_30d INTEGER
  , ad_cost_30d INTEGER
  , created_at TIMESTAMP
  , PRIMARY KEY (customer_id, campaign_id)
);

-- Campaign: select
SELECT
    campaign.id AS campaign_id
  , campaign.name AS campaign_name
  , $customer_id AS customer_id
  , campaign.advertisingChannelType AS campaign_type
  , campaign.status AS campaign_status
  , campaign.biddingStrategyType AS bidding_strategy
  , ROUND(COALESCE(TRY_CAST(campaignBudget.amountMicros AS INTEGER), 0) / 1000000) AS campaign_budget
  , COALESCE(TRY_CAST(metrics.impressions AS INTEGER), 0) AS impression_count_30d
  , COALESCE(TRY_CAST(metrics.clicks AS INTEGER), 0) AS click_count_30d
  , ROUND(COALESCE(TRY_CAST(metrics.costMicros AS INTEGER), 0) / 1000000) AS ad_cost_30d
  , TRY_STRPTIME(campaign.startDateTime, '%Y-%m-%d %H:%M:%S') AS created_at
FROM {{ array }};

-- Campaign: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Campaign: campaign_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'DEMAND_GEN' AS code, '디맨드젠' AS name)
  , STRUCT(1 AS seq, 'DISPLAY' AS code, '디스플레이' AS name)
  , STRUCT(2 AS seq, 'HOTEL' AS code, '호텔' AS name)
  , STRUCT(3 AS seq, 'LOCAL' AS code, '지역' AS name)
  , STRUCT(4 AS seq, 'LOCAL_SERVICES' AS code, '지역 서비스' AS name)
  , STRUCT(5 AS seq, 'MULTI_CHANNEL' AS code, '다채널' AS name)
  , STRUCT(6 AS seq, 'PERFORMANCE_MAX' AS code, '실적 최대화' AS name)
  , STRUCT(7 AS seq, 'SEARCH' AS code, '검색' AS name)
  , STRUCT(8 AS seq, 'SHOPPING' AS code, '쇼핑' AS name)
  , STRUCT(9 AS seq, 'SMART' AS code, '스마트' AS name)
  , STRUCT(10 AS seq, 'TRAVEL' AS code, '여행' AS name)
  , STRUCT(11 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS name)
  , STRUCT(12 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS name)
  , STRUCT(13 AS seq, 'VIDEO' AS code, '동영상' AS name)
]);

-- Campaign: bidding_strategy
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'COMMISSION' AS code, '수수료' AS name)
  , STRUCT(1 AS seq, 'ENHANCED_CPC' AS code, '향상된 CPC' AS name)
  , STRUCT(2 AS seq, 'FIXED_CPM' AS code, '고정 CPM' AS name)
  , STRUCT(3 AS seq, 'FIXED_SHARE_OF_VOICE' AS code, '고정 음성 공유 비율' AS name)
  , STRUCT(4 AS seq, 'INVALID' AS code, '잘못됨' AS name)
  , STRUCT(5 AS seq, 'MANUAL_CPA' AS code, '수동 CPA' AS name)
  , STRUCT(6 AS seq, 'MANUAL_CPC' AS code, '수동 CPC' AS name)
  , STRUCT(7 AS seq, 'MANUAL_CPM' AS code, '수동 CPM' AS name)
  , STRUCT(8 AS seq, 'MANUAL_CPV' AS code, '수동 CPV' AS name)
  , STRUCT(9 AS seq, 'MAXIMIZE_CONVERSIONS' AS code, '전환 수 최대화' AS name)
  , STRUCT(10 AS seq, 'MAXIMIZE_CONVERSION_VALUE' AS code, '전환 가치 최대화' AS name)
  , STRUCT(11 AS seq, 'PAGE_ONE_PROMOTED' AS code, '1페이지 상단 홍보' AS name)
  , STRUCT(12 AS seq, 'PERCENT_CPC' AS code, '비율 CPC' AS name)
  , STRUCT(13 AS seq, 'TARGET_CPA' AS code, '목표 CPA' AS name)
  , STRUCT(14 AS seq, 'TARGET_CPC' AS code, '목표 CPC' AS name)
  , STRUCT(15 AS seq, 'TARGET_CPM' AS code, '목표 CPM' AS name)
  , STRUCT(16 AS seq, 'TARGET_CPV' AS code, '목표 CPV' AS name)
  , STRUCT(17 AS seq, 'TARGET_IMPRESSION_SHARE' AS code, '노출 수 공유 목표' AS name)
  , STRUCT(18 AS seq, 'TARGET_OUTRANK_SHARE' AS code, '경쟁 우위 공유 목표' AS name)
  , STRUCT(19 AS seq, 'TARGET_ROAS' AS code, '목표 ROAS' AS name)
  , STRUCT(20 AS seq, 'TARGET_SPEND' AS code, '목표 지출' AS name)
  , STRUCT(21 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS name)
  , STRUCT(22 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS name)
]);


-- AdGroup: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    adgroup_id VARCHAR
  , adgroup_name VARCHAR
  , customer_id VARCHAR
  , campaign_id VARCHAR NOT NULL
  , adgroup_type VARCHAR
  , adgroup_status VARCHAR
  , target_cpa INTEGER
  , impression_count_30d INTEGER
  , click_count_30d INTEGER
  , ad_cost_30d INTEGER
  , PRIMARY KEY (customer_id, adgroup_id)
);

-- AdGroup: select
SELECT
    adGroup.id AS adgroup_id
  , adGroup.name AS adgroup_name
  , $customer_id AS customer_id
  , campaign.id AS campaign_id
  , adGroup.type AS adgroup_type
  , adGroup.status AS adgroup_status
  , ROUND(COALESCE(TRY_CAST(adGroup.targetCpaMicros AS INTEGER), 0) / 1000000) AS target_cpa
  , COALESCE(TRY_CAST(metrics.impressions AS INTEGER), 0) AS impression_count_30d
  , COALESCE(TRY_CAST(metrics.clicks AS INTEGER), 0) AS click_count_30d
  , ROUND(COALESCE(TRY_CAST(metrics.costMicros AS INTEGER), 0) / 1000000) AS ad_cost_30d
FROM {{ array }};

-- AdGroup: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- AdGroup: adgroup_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'DISPLAY_STANDARD' AS code, '표준 디스플레이' AS name)
  , STRUCT(1 AS seq, 'HOTEL_ADS' AS code, '호텔 광고' AS name)
  , STRUCT(2 AS seq, 'PROMOTED_HOTEL_ADS' AS code, '홍보 호텔 광고' AS name)
  , STRUCT(3 AS seq, 'SEARCH_DYNAMIC_ADS' AS code, '동적 검색 광고' AS name)
  , STRUCT(4 AS seq, 'SEARCH_STANDARD' AS code, '표준 검색' AS name)
  , STRUCT(5 AS seq, 'SHOPPING_COMPARISON_LISTING_ADS' AS code, '쇼핑 비교 목록 광고' AS name)
  , STRUCT(6 AS seq, 'SHOPPING_PRODUCT_ADS' AS code, '쇼핑 제품 광고' AS name)
  , STRUCT(7 AS seq, 'SHOPPING_SMART_ADS' AS code, '쇼핑 스마트 광고' AS name)
  , STRUCT(8 AS seq, 'SMART_CAMPAIGN_ADS' AS code, '스마트 캠페인 광고' AS name)
  , STRUCT(9 AS seq, 'TRAVEL_ADS' AS code, '여행 광고' AS name)
  , STRUCT(10 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS name)
  , STRUCT(11 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS name)
  , STRUCT(12 AS seq, 'VIDEO_BUMPER' AS code, '범퍼 동영상' AS name)
  , STRUCT(13 AS seq, 'VIDEO_EFFICIENT_REACH' AS code, '효율적 도달 동영상' AS name)
  , STRUCT(14 AS seq, 'VIDEO_NON_SKIPPABLE_IN_STREAM' AS code, '비건너뛰기 인스트림' AS name)
  , STRUCT(15 AS seq, 'VIDEO_RESPONSIVE' AS code, '반응형 동영상' AS name)
  , STRUCT(16 AS seq, 'VIDEO_TRUE_VIEW_IN_DISPLAY' AS code, '디스플레이 진정한 조회' AS name)
  , STRUCT(17 AS seq, 'VIDEO_TRUE_VIEW_IN_STREAM' AS code, '인스트림 진정한 조회' AS name)
  , STRUCT(18 AS seq, 'YOUTUBE_AUDIO' AS code, '유튜브 오디오' AS name)
]);


-- Ad: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    ad_id VARCHAR
  , ad_name VARCHAR
  , customer_id VARCHAR
  , campaign_id VARCHAR NOT NULL
  , adgroup_id VARCHAR NOT NULL
  , ad_type VARCHAR
  , ad_status VARCHAR
  , impression_count_30d INTEGER
  , click_count_30d INTEGER
  , ad_cost_30d INTEGER
  , PRIMARY KEY (customer_id, ad_id)
);

-- Ad: select
SELECT
    adGroupAd.ad.id AS ad_id
  , adGroupAd.ad.name AS ad_name
  , $customer_id AS customer_id
  , campaign.id AS campaign_id
  , adGroup.id AS adgroup_id
  , adGroupAd.ad.type AS ad_type
  , adGroupAd.status AS ad_status
  , COALESCE(TRY_CAST(metrics.impressions AS INTEGER), 0) AS impression_count_30d
  , COALESCE(TRY_CAST(metrics.clicks AS INTEGER), 0) AS click_count_30d
  , ROUND(COALESCE(TRY_CAST(metrics.costMicros AS INTEGER), 0) / 1000000) AS ad_cost_30d
FROM {{ array }};

-- Ad: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Ad: ad_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'APP_AD' AS code, '앱 광고' AS name)
  , STRUCT(1 AS seq, 'APP_ENGAGEMENT_AD' AS code, '앱 참여 광고' AS name)
  , STRUCT(2 AS seq, 'APP_PRE_REGISTRATION_AD' AS code, '앱 사전 등록 광고' AS name)
  , STRUCT(3 AS seq, 'CALL_AD' AS code, '전화 광고' AS name)
  , STRUCT(4 AS seq, 'DEMAND_GEN_CAROUSEL_AD' AS code, '디맨드젠 캐러셀 광고' AS name)
  , STRUCT(5 AS seq, 'DEMAND_GEN_MULTI_ASSET_AD' AS code, '디맨드젠 다중 자산 광고' AS name)
  , STRUCT(6 AS seq, 'DEMAND_GEN_PRODUCT_AD' AS code, '디맨드젠 제품 광고' AS name)
  , STRUCT(7 AS seq, 'DEMAND_GEN_VIDEO_RESPONSIVE_AD' AS code, '디맨드젠 반응형 동영상 광고' AS name)
  , STRUCT(8 AS seq, 'DYNAMIC_HTML5_AD' AS code, '동적 HTML5 광고' AS name)
  , STRUCT(9 AS seq, 'EXPANDED_DYNAMIC_SEARCH_AD' AS code, '확장 동적 검색 광고' AS name)
  , STRUCT(10 AS seq, 'EXPANDED_TEXT_AD' AS code, '확장 텍스트 광고' AS name)
  , STRUCT(11 AS seq, 'HOTEL_AD' AS code, '호텔 광고' AS name)
  , STRUCT(12 AS seq, 'HTML5_UPLOAD_AD' AS code, 'HTML5 업로드 광고' AS name)
  , STRUCT(13 AS seq, 'IMAGE_AD' AS code, '이미지 광고' AS name)
  , STRUCT(14 AS seq, 'IN_FEED_VIDEO_AD' AS code, '피드 내 동영상 광고' AS name)
  , STRUCT(15 AS seq, 'LEGACY_APP_INSTALL_AD' AS code, '레거시 앱 설치 광고' AS name)
  , STRUCT(16 AS seq, 'LEGACY_RESPONSIVE_DISPLAY_AD' AS code, '레거시 반응형 디스플레이' AS name)
  , STRUCT(17 AS seq, 'LOCAL_AD' AS code, '지역 광고' AS name)
  , STRUCT(18 AS seq, 'RESPONSIVE_DISPLAY_AD' AS code, '반응형 디스플레이 광고' AS name)
  , STRUCT(19 AS seq, 'RESPONSIVE_SEARCH_AD' AS code, '반응형 검색 광고' AS name)
  , STRUCT(20 AS seq, 'SHOPPING_COMPARISON_LISTING_AD' AS code, '쇼핑 비교 목록 광고' AS name)
  , STRUCT(21 AS seq, 'SHOPPING_PRODUCT_AD' AS code, '쇼핑 제품 광고' AS name)
  , STRUCT(22 AS seq, 'SHOPPING_SMART_AD' AS code, '쇼핑 스마트 광고' AS name)
  , STRUCT(23 AS seq, 'SMART_CAMPAIGN_AD' AS code, '스마트 캠페인 광고' AS name)
  , STRUCT(24 AS seq, 'TEXT_AD' AS code, '텍스트 광고' AS name)
  , STRUCT(25 AS seq, 'TRAVEL_AD' AS code, '여행 광고' AS name)
  , STRUCT(26 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS name)
  , STRUCT(27 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS name)
  , STRUCT(28 AS seq, 'VIDEO_AD' AS code, '동영상 광고' AS name)
  , STRUCT(29 AS seq, 'VIDEO_BUMPER_AD' AS code, '범퍼 동영상 광고' AS name)
  , STRUCT(30 AS seq, 'VIDEO_NON_SKIPPABLE_IN_STREAM_AD' AS code, '비건너뛰기 인스트림 동영상' AS name)
  , STRUCT(31 AS seq, 'VIDEO_RESPONSIVE_AD' AS code, '반응형 동영상 광고' AS name)
  , STRUCT(32 AS seq, 'VIDEO_TRUEVIEW_IN_STREAM_AD' AS code, '인스트림 TrueView 광고' AS name)
  , STRUCT(33 AS seq, 'YOUTUBE_AUDIO_AD' AS code, '유튜브 오디오 광고' AS name)
]);


-- Asset: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    asset_id VARCHAR
  , asset_name VARCHAR
  , customer_id VARCHAR
  , asset_type VARCHAR
  , asset_url VARCHAR
  , PRIMARY KEY (customer_id, asset_id)
);

-- Asset: select
SELECT
    asset.id AS asset_id
  , (CASE
      WHEN asset.type = 'TEXT' THEN json_extract_string(item, '$.asset.textAsset.text')
      WHEN asset.type = 'IMAGE' THEN json_extract_string(item, '$.asset.name')
      WHEN asset.type = 'YOUTUBE_VIDEO' THEN json_extract_string(item, '$.asset.youtubeVideoAsset.youtubeVideoTitle')
      WHEN asset.type = 'CALLOUT' THEN json_extract_string(item, '$.asset.calloutAsset.calloutText')
      WHEN asset.type = 'STRUCTURED_SNIPPET' THEN json_extract_string(item, '$.asset.structuredSnippetAsset.header')
      ELSE NULL END) AS asset_name
  , $customer_id AS customer_id
  , asset.type AS asset_type
  , (CASE
      WHEN asset.type = 'IMAGE' THEN json_extract_string(item, '$.asset.imageAsset.fullSize.url')
      ELSE NULL END) AS asset_url
FROM {{ array }} AS item;

-- Asset: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- Asset: asset_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'APP_DEEP_LINK' AS code, '앱 딥링크' AS name)
  , STRUCT(1 AS seq, 'BOOK_ON_GOOGLE' AS code, '구글에서 예약' AS name)
  , STRUCT(2 AS seq, 'BUSINESS_MESSAGE' AS code, '비즈니스 메시지' AS name)
  , STRUCT(3 AS seq, 'CALL' AS code, '통화' AS name)
  , STRUCT(4 AS seq, 'CALLOUT' AS code, '콜아웃' AS name)
  , STRUCT(5 AS seq, 'CALL_TO_ACTION' AS code, '클릭 유도 문구' AS name)
  , STRUCT(6 AS seq, 'DEMAND_GEN_CAROUSEL_CARD' AS code, '디맨드젠 캐러셀 카드' AS name)
  , STRUCT(7 AS seq, 'DYNAMIC_CUSTOM' AS code, '동적 사용자 지정' AS name)
  , STRUCT(8 AS seq, 'DYNAMIC_EDUCATION' AS code, '동적 교육' AS name)
  , STRUCT(9 AS seq, 'DYNAMIC_FLIGHTS' AS code, '동적 항공편' AS name)
  , STRUCT(10 AS seq, 'DYNAMIC_HOTELS_AND_RENTALS' AS code, '동적 호텔 및 렌탈' AS name)
  , STRUCT(11 AS seq, 'DYNAMIC_JOBS' AS code, '동적 구인' AS name)
  , STRUCT(12 AS seq, 'DYNAMIC_LOCAL' AS code, '동적 지역' AS name)
  , STRUCT(13 AS seq, 'DYNAMIC_REAL_ESTATE' AS code, '동적 부동산' AS name)
  , STRUCT(14 AS seq, 'DYNAMIC_TRAVEL' AS code, '동적 여행' AS name)
  , STRUCT(15 AS seq, 'HOTEL_CALLOUT' AS code, '호텔 콜아웃' AS name)
  , STRUCT(16 AS seq, 'HOTEL_PROPERTY' AS code, '호텔 속성' AS name)
  , STRUCT(17 AS seq, 'IMAGE' AS code, '이미지' AS name)
  , STRUCT(18 AS seq, 'LEAD_FORM' AS code, '리드 양식' AS name)
  , STRUCT(19 AS seq, 'LOCATION' AS code, '위치' AS name)
  , STRUCT(20 AS seq, 'MEDIA_BUNDLE' AS code, '미디어 번들' AS name)
  , STRUCT(21 AS seq, 'MOBILE_APP' AS code, '모바일 앱' AS name)
  , STRUCT(22 AS seq, 'PAGE_FEED' AS code, '페이지 피드' AS name)
  , STRUCT(23 AS seq, 'PRICE' AS code, '가격' AS name)
  , STRUCT(24 AS seq, 'PROMOTION' AS code, '프로모션' AS name)
  , STRUCT(25 AS seq, 'SITELINK' AS code, '사이트링크' AS name)
  , STRUCT(26 AS seq, 'STRUCTURED_SNIPPET' AS code, '구조화된 스니펫' AS name)
  , STRUCT(27 AS seq, 'TEXT' AS code, '텍스트' AS name)
  , STRUCT(28 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS name)
  , STRUCT(29 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS name)
  , STRUCT(30 AS seq, 'YOUTUBE_VIDEO' AS code, '유튜브 동영상' AS name)
  , STRUCT(31 AS seq, 'YOUTUBE_VIDEO_LIST' AS code, '유튜브 동영상 목록' AS name)
]);


-- AssetView: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    customer_id VARCHAR
  , adgroup_id VARCHAR NOT NULL
  , ad_id VARCHAR
  , asset_id VARCHAR
  , field_type TINYINT
  , device_type TINYINT
  , impression_count INTEGER
  , click_count INTEGER
  , ad_cost INTEGER
  , ymd DATE
  , PRIMARY KEY (customer_id, ad_id, asset_id, field_type, device_type)
);

-- AssetView: select
SELECT
    $customer_id AS customer_id
  , adGroup.id AS adgroup_id
  , adGroupAd.ad.id AS ad_id
  , asset.id AS asset_id
  , (CASE
      WHEN adGroupAdAssetView.fieldType = 'HEADLINE' THEN 0
      WHEN adGroupAdAssetView.fieldType = 'DESCRIPTION' THEN 1
      WHEN adGroupAdAssetView.fieldType = 'LONG_HEADLINE' THEN 2
      WHEN adGroupAdAssetView.fieldType = 'LONG_DESCRIPTION' THEN 3
      WHEN adGroupAdAssetView.fieldType = 'AD_IMAGE' THEN 4
      WHEN adGroupAdAssetView.fieldType = 'BOOK_ON_GOOGLE' THEN 5
      WHEN adGroupAdAssetView.fieldType = 'BUSINESS_LOGO' THEN 6
      WHEN adGroupAdAssetView.fieldType = 'BUSINESS_MESSAGE' THEN 7
      WHEN adGroupAdAssetView.fieldType = 'BUSINESS_NAME' THEN 8
      WHEN adGroupAdAssetView.fieldType = 'CALL' THEN 9
      WHEN adGroupAdAssetView.fieldType = 'CALLOUT' THEN 10
      WHEN adGroupAdAssetView.fieldType = 'CALL_TO_ACTION' THEN 11
      WHEN adGroupAdAssetView.fieldType = 'CALL_TO_ACTION_SELECTION' THEN 12
      WHEN adGroupAdAssetView.fieldType = 'DEMAND_GEN_CAROUSEL_CARD' THEN 13
      WHEN adGroupAdAssetView.fieldType = 'HOTEL_CALLOUT' THEN 14
      WHEN adGroupAdAssetView.fieldType = 'HOTEL_PROPERTY' THEN 15
      WHEN adGroupAdAssetView.fieldType = 'LANDING_PAGE_PREVIEW' THEN 16
      WHEN adGroupAdAssetView.fieldType = 'LANDSCAPE_LOGO' THEN 17
      WHEN adGroupAdAssetView.fieldType = 'LEAD_FORM' THEN 18
      WHEN adGroupAdAssetView.fieldType = 'LOGO' THEN 19
      WHEN adGroupAdAssetView.fieldType = 'MANDATORY_AD_TEXT' THEN 20
      WHEN adGroupAdAssetView.fieldType = 'MARKETING_IMAGE' THEN 21
      WHEN adGroupAdAssetView.fieldType = 'MEDIA_BUNDLE' THEN 22
      WHEN adGroupAdAssetView.fieldType = 'MOBILE_APP' THEN 23
      WHEN adGroupAdAssetView.fieldType = 'PORTRAIT_MARKETING_IMAGE' THEN 24
      WHEN adGroupAdAssetView.fieldType = 'PRICE' THEN 25
      WHEN adGroupAdAssetView.fieldType = 'PROMOTION' THEN 26
      WHEN adGroupAdAssetView.fieldType = 'RELATED_YOUTUBE_VIDEOS' THEN 27
      WHEN adGroupAdAssetView.fieldType = 'SITELINK' THEN 28
      WHEN adGroupAdAssetView.fieldType = 'SQUARE_MARKETING_IMAGE' THEN 29
      WHEN adGroupAdAssetView.fieldType = 'STRUCTURED_SNIPPET' THEN 30
      WHEN adGroupAdAssetView.fieldType = 'TALL_PORTRAIT_MARKETING_IMAGE' THEN 31
      WHEN adGroupAdAssetView.fieldType = 'UNKNOWN' THEN 32
      WHEN adGroupAdAssetView.fieldType = 'UNSPECIFIED' THEN 33
      WHEN adGroupAdAssetView.fieldType = 'VIDEO' THEN 34
      WHEN adGroupAdAssetView.fieldType = 'YOUTUBE_VIDEO' THEN 35
      ELSE NULL END) AS field_type
  , (CASE
      WHEN segments.device = 'DESKTOP' THEN 0
      WHEN segments.device = 'MOBILE' THEN 1
      WHEN segments.device = 'TABLET' THEN 2
      WHEN segments.device = 'CONNECTED_TV' THEN 3
      WHEN segments.device = 'OTHER' THEN 4
      WHEN segments.device = 'UNKNOWN' THEN 5
      WHEN segments.device = 'UNSPECIFIED' THEN 6
      ELSE NULL END) AS device_type
  , COALESCE(TRY_CAST(metrics.impressions AS INTEGER), 0) AS impression_count
  , COALESCE(TRY_CAST(metrics.clicks AS INTEGER), 0) AS click_count
  , ROUND(COALESCE(TRY_CAST(metrics.costMicros AS INTEGER), 0) / 1000000) AS ad_cost
  , TRY_STRPTIME(segments.date, '%Y-%m-%d') AS ymd
FROM {{ array }};

-- AssetView: insert
INSERT INTO {{ table }} {{ values }} ON CONFLICT DO NOTHING;

-- AssetView: field_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'HEADLINE' AS code, '제목' AS name)
  , STRUCT(1 AS seq, 'DESCRIPTION' AS code, '설명' AS name)
  , STRUCT(2 AS seq, 'LONG_HEADLINE' AS code, '긴 제목' AS name)
  , STRUCT(3 AS seq, 'LONG_DESCRIPTION' AS code, '긴 설명' AS name)
  , STRUCT(4 AS seq, 'AD_IMAGE' AS code, '광고 이미지' AS name)
  , STRUCT(5 AS seq, 'BOOK_ON_GOOGLE' AS code, '구글에서 예약' AS name)
  , STRUCT(6 AS seq, 'BUSINESS_LOGO' AS code, '비즈니스 로고' AS name)
  , STRUCT(7 AS seq, 'BUSINESS_MESSAGE' AS code, '비즈니스 메시지' AS name)
  , STRUCT(8 AS seq, 'BUSINESS_NAME' AS code, '비즈니스 이름' AS name)
  , STRUCT(9 AS seq, 'CALL' AS code, '통화' AS name)
  , STRUCT(10 AS seq, 'CALLOUT' AS code, '콜아웃' AS name)
  , STRUCT(11 AS seq, 'CALL_TO_ACTION' AS code, '클릭 유도 문구' AS name)
  , STRUCT(12 AS seq, 'CALL_TO_ACTION_SELECTION' AS code, '클릭 유도 선택' AS name)
  , STRUCT(13 AS seq, 'DEMAND_GEN_CAROUSEL_CARD' AS code, '디맨드젠 캐러셀 카드' AS name)
  , STRUCT(14 AS seq, 'HOTEL_CALLOUT' AS code, '호텔 콜아웃' AS name)
  , STRUCT(15 AS seq, 'HOTEL_PROPERTY' AS code, '호텔 속성' AS name)
  , STRUCT(16 AS seq, 'LANDING_PAGE_PREVIEW' AS code, '랜딩 페이지 미리보기' AS name)
  , STRUCT(17 AS seq, 'LANDSCAPE_LOGO' AS code, '가로 로고' AS name)
  , STRUCT(18 AS seq, 'LEAD_FORM' AS code, '리드 양식' AS name)
  , STRUCT(19 AS seq, 'LOGO' AS code, '로고' AS name)
  , STRUCT(20 AS seq, 'MANDATORY_AD_TEXT' AS code, '필수 광고 텍스트' AS name)
  , STRUCT(21 AS seq, 'MARKETING_IMAGE' AS code, '마케팅 이미지' AS name)
  , STRUCT(22 AS seq, 'MEDIA_BUNDLE' AS code, '미디어 번들' AS name)
  , STRUCT(23 AS seq, 'MOBILE_APP' AS code, '모바일 앱' AS name)
  , STRUCT(24 AS seq, 'PORTRAIT_MARKETING_IMAGE' AS code, '세로 마케팅 이미지' AS name)
  , STRUCT(25 AS seq, 'PRICE' AS code, '가격' AS name)
  , STRUCT(26 AS seq, 'PROMOTION' AS code, '프로모션' AS name)
  , STRUCT(27 AS seq, 'RELATED_YOUTUBE_VIDEOS' AS code, '관련 유튜브 동영상' AS name)
  , STRUCT(28 AS seq, 'SITELINK' AS code, '사이트링크' AS name)
  , STRUCT(29 AS seq, 'SQUARE_MARKETING_IMAGE' AS code, '정사각형 마케팅 이미지' AS name)
  , STRUCT(30 AS seq, 'STRUCTURED_SNIPPET' AS code, '구조화된 스니펫' AS name)
  , STRUCT(31 AS seq, 'TALL_PORTRAIT_MARKETING_IMAGE' AS code, '세로형 마케팅 이미지' AS name)
  , STRUCT(32 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS name)
  , STRUCT(33 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS name)
  , STRUCT(34 AS seq, 'VIDEO' AS code, '동영상' AS name)
  , STRUCT(35 AS seq, 'YOUTUBE_VIDEO' AS code, '유튜브 동영상' AS name)
]);

-- AssetView: device_type
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'DESKTOP' AS code, '데스크톱' AS name)
  , STRUCT(1 AS seq, 'MOBILE' AS code, '모바일' AS name)
  , STRUCT(2 AS seq, 'TABLET' AS code, '태블릿' AS name)
  , STRUCT(3 AS seq, 'CONNECTED_TV' AS code, '연결된 TV' AS name)
  , STRUCT(4 AS seq, 'OTHER' AS code, '기타' AS name)
  , STRUCT(5 AS seq, 'UNKNOWN' AS code, '알 수 없음' AS name)
  , STRUCT(6 AS seq, 'UNSPECIFIED' AS code, '지정되지 않음' AS name)
]);
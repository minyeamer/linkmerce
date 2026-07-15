{% macro meta_ads__objective_mapping() -%}
SELECT *
FROM (VALUES
    (0, 'OUTCOME_AWARENESS', '인지도')
  , (1, 'OUTCOME_ENGAGEMENT', '참여')
  , (2, 'OUTCOME_LEADS', '리드')
  , (3, 'OUTCOME_SALES', '판매')
  , (4, 'OUTCOME_TRAFFIC', '트래픽')
  , (5, 'OUTCOME_APP_PROMOTION', '앱 홍보')
  , (6, 'OFFER_CLAIMS', '오퍼 수령')
  , (7, 'PAGE_LIKES', '페이지 좋아요')
  , (8, 'EVENT_RESPONSES', '이벤트 응답')
  , (9, 'POST_ENGAGEMENT', '게시물 참여')
  , (10, 'WEBSITE_CONVERSIONS', '웹사이트 전환')
  , (11, 'LINK_CLICKS', '링크 클릭')
  , (12, 'VIDEO_VIEWS', '동영상 조회')
  , (13, 'LOCAL_AWARENESS', '지역 인지도')
  , (14, 'PRODUCT_CATALOG_SALES', '카탈로그 판매')
  , (15, 'LEAD_GENERATION', '리드 생성')
  , (16, 'BRAND_AWARENESS', '브랜드 인지도')
  , (17, 'STORE_VISITS', '매장 방문')
  , (18, 'REACH', '도달')
  , (19, 'APP_INSTALLS', '앱 설치')
  , (20, 'MESSAGES', '메시지')
) AS mapping(seq, code, label)
{%- endmacro %}

{% macro meta_ads__effective_status_mapping() -%}
SELECT *
FROM (VALUES
    (0, 'ACTIVE', '활성')
  , (1, 'PAUSED', '일시 중지')
  , (2, 'DELETED', '삭제됨')
  , (3, 'ARCHIVED', '보관됨')
  , (4, 'PENDING_REVIEW', '검토 대기')
  , (5, 'DISAPPROVED', '거부됨')
  , (6, 'PREAPPROVED', '사전 승인')
  , (7, 'PENDING_BILLING_INFO', '결제 정보 대기')
  , (8, 'CAMPAIGN_PAUSED', '캠페인 일시 중지')
  , (9, 'ADSET_PAUSED', '광고 세트 일시 중지')
  , (10, 'IN_PROCESS', '처리 중')
  , (11, 'WITH_ISSUES', '문제 발생')
) AS mapping(seq, code, label)
{%- endmacro %}

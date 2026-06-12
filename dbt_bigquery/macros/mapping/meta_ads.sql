{% macro meta_ads__objective_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'OUTCOME_AWARENESS' AS code, '인지도' AS label)
  , STRUCT(1 AS seq, 'OUTCOME_ENGAGEMENT' AS code, '참여' AS label)
  , STRUCT(2 AS seq, 'OUTCOME_LEADS' AS code, '리드' AS label)
  , STRUCT(3 AS seq, 'OUTCOME_SALES' AS code, '판매' AS label)
  , STRUCT(4 AS seq, 'OUTCOME_TRAFFIC' AS code, '트래픽' AS label)
  , STRUCT(5 AS seq, 'OUTCOME_APP_PROMOTION' AS code, '앱 홍보' AS label)
  , STRUCT(6 AS seq, 'OFFER_CLAIMS' AS code, '오퍼 수령' AS label)
  , STRUCT(7 AS seq, 'PAGE_LIKES' AS code, '페이지 좋아요' AS label)
  , STRUCT(8 AS seq, 'EVENT_RESPONSES' AS code, '이벤트 응답' AS label)
  , STRUCT(9 AS seq, 'POST_ENGAGEMENT' AS code, '게시물 참여' AS label)
  , STRUCT(10 AS seq, 'WEBSITE_CONVERSIONS' AS code, '웹사이트 전환' AS label)
  , STRUCT(11 AS seq, 'LINK_CLICKS' AS code, '링크 클릭' AS label)
  , STRUCT(12 AS seq, 'VIDEO_VIEWS' AS code, '동영상 조회' AS label)
  , STRUCT(13 AS seq, 'LOCAL_AWARENESS' AS code, '지역 인지도' AS label)
  , STRUCT(14 AS seq, 'PRODUCT_CATALOG_SALES' AS code, '카탈로그 판매' AS label)
  , STRUCT(15 AS seq, 'LEAD_GENERATION' AS code, '리드 생성' AS label)
  , STRUCT(16 AS seq, 'BRAND_AWARENESS' AS code, '브랜드 인지도' AS label)
  , STRUCT(17 AS seq, 'STORE_VISITS' AS code, '매장 방문' AS label)
  , STRUCT(18 AS seq, 'REACH' AS code, '도달' AS label)
  , STRUCT(19 AS seq, 'APP_INSTALLS' AS code, '앱 설치' AS label)
  , STRUCT(20 AS seq, 'MESSAGES' AS code, '메시지' AS label)
])
{%- endmacro %}

{% macro meta_ads__effective_status_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 'ACTIVE' AS code, '활성' AS label)
  , STRUCT(1 AS seq, 'PAUSED' AS code, '일시 중지' AS label)
  , STRUCT(2 AS seq, 'DELETED' AS code, '삭제됨' AS label)
  , STRUCT(3 AS seq, 'ARCHIVED' AS code, '보관됨' AS label)
  , STRUCT(4 AS seq, 'PENDING_REVIEW' AS code, '검토 대기' AS label)
  , STRUCT(5 AS seq, 'DISAPPROVED' AS code, '거부됨' AS label)
  , STRUCT(6 AS seq, 'PREAPPROVED' AS code, '사전 승인' AS label)
  , STRUCT(7 AS seq, 'PENDING_BILLING_INFO' AS code, '결제 정보 대기' AS label)
  , STRUCT(8 AS seq, 'CAMPAIGN_PAUSED' AS code, '캠페인 일시 중지' AS label)
  , STRUCT(9 AS seq, 'ADSET_PAUSED' AS code, '광고 세트 일시 중지' AS label)
  , STRUCT(10 AS seq, 'IN_PROCESS' AS code, '처리 중' AS label)
  , STRUCT(11 AS seq, 'WITH_ISSUES' AS code, '문제 발생' AS label)
])
{%- endmacro %}

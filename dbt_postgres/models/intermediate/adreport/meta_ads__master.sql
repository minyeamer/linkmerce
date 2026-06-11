WITH ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '메타'
),{{var("line_break")

}} objective_mapping AS (
  SELECT *
  FROM (
    VALUES
      ('OUTCOME_AWARENESS', '인지도')
    , ('OUTCOME_ENGAGEMENT', '참여')
    , ('OUTCOME_LEADS', '리드')
    , ('OUTCOME_SALES', '판매')
    , ('OUTCOME_TRAFFIC', '트래픽')
    , ('OUTCOME_APP_PROMOTION', '앱 홍보')
    , ('OFFER_CLAIMS', '오퍼 수령')
    , ('PAGE_LIKES', '페이지 좋아요')
    , ('EVENT_RESPONSES', '이벤트 응답')
    , ('POST_ENGAGEMENT', '게시물 참여')
    , ('WEBSITE_CONVERSIONS', '웹사이트 전환')
    , ('LINK_CLICKS', '링크 클릭')
    , ('VIDEO_VIEWS', '동영상 조회')
    , ('LOCAL_AWARENESS', '지역 인지도')
    , ('PRODUCT_CATALOG_SALES', '카탈로그 판매')
    , ('LEAD_GENERATION', '리드 생성')
    , ('BRAND_AWARENESS', '브랜드 인지도')
    , ('STORE_VISITS', '매장 방문')
    , ('REACH', '도달')
    , ('APP_INSTALLS', '앱 설치')
    , ('MESSAGES', '메시지')
  ) AS _t(code, label)
),{{var("line_break")

}} effective_status_mapping AS (
  SELECT *
  FROM (
    VALUES
      ('ACTIVE', '활성')
    , ('PAUSED', '일시 중지')
    , ('DELETED', '삭제됨')
    , ('ARCHIVED', '보관됨')
    , ('PENDING_REVIEW', '검토 대기')
    , ('DISAPPROVED', '거부됨')
    , ('PREAPPROVED', '사전 승인')
    , ('PENDING_BILLING_INFO', '결제 정보 대기')
    , ('CAMPAIGN_PAUSED', '캠페인 일시 중지')
    , ('ADSET_PAUSED', '광고 세트 일시 중지')
    , ('IN_PROCESS', '처리 중')
    , ('WITH_ISSUES', '문제 발생')
  ) AS _t(code, label)
),{{var("line_break")

}} master_report AS (
  SELECT
      ad.account_id
    , acc.account_name
    -- Campaign attrs
    , ad.campaign_id
    , cmp.campaign_name
    , objective.label AS objective
    -- Adset attrs
    , ad.adset_id
    , adset.adset_name
    -- Ad attrs
    , ad.ad_id
    , ad.ad_name
    , effective_status.label AS effective_status
    , COALESCE(
          rel_ad.bundle_product_ids
        , rel_adset.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , '900000'
      ) AS bundle_product_ids
    , COALESCE(ad.created_at, adset.created_at, cmp.created_at) AS created_at
  FROM {{ source('meta_ads', 'ad') }} AS ad
  LEFT JOIN {{ source('meta_ads', 'account') }} AS acc
    ON ad.account_id = acc.account_id
  LEFT JOIN {{ source('meta_ads', 'campaign') }} AS cmp
    ON ad.account_id = cmp.account_id AND ad.campaign_id = cmp.campaign_id
  LEFT JOIN {{ source('meta_ads', 'adset') }} AS adset
    ON ad.account_id = adset.account_id AND ad.adset_id = adset.adset_id
  -- Map codes to labels
  LEFT JOIN objective_mapping AS objective
    ON cmp.objective = objective.code
  LEFT JOIN effective_status_mapping AS effective_status
    ON COALESCE(ad.effective_status, adset.effective_status, cmp.effective_status) = effective_status.code
  -- Resolve bundle_product_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON ad.campaign_id = rel_cmp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_adset
    ON ad.adset_id = rel_adset.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON ad.ad_id = rel_ad.ad_id
)
SELECT * FROM master_report

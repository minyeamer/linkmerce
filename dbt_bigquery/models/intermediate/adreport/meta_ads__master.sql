WITH ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '메타'
),

objective_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT('OUTCOME_AWARENESS' AS code, '인지도' AS label)
    , STRUCT('OUTCOME_ENGAGEMENT' AS code, '참여' AS label)
    , STRUCT('OUTCOME_LEADS' AS code, '리드' AS label)
    , STRUCT('OUTCOME_SALES' AS code, '판매' AS label)
    , STRUCT('OUTCOME_TRAFFIC' AS code, '트래픽' AS label)
    , STRUCT('OUTCOME_APP_PROMOTION' AS code, '앱 홍보' AS label)
    , STRUCT('OFFER_CLAIMS' AS code, '오퍼 수령' AS label)
    , STRUCT('PAGE_LIKES' AS code, '페이지 좋아요' AS label)
    , STRUCT('EVENT_RESPONSES' AS code, '이벤트 응답' AS label)
    , STRUCT('POST_ENGAGEMENT' AS code, '게시물 참여' AS label)
    , STRUCT('WEBSITE_CONVERSIONS' AS code, '웹사이트 전환' AS label)
    , STRUCT('LINK_CLICKS' AS code, '링크 클릭' AS label)
    , STRUCT('VIDEO_VIEWS' AS code, '동영상 조회' AS label)
    , STRUCT('LOCAL_AWARENESS' AS code, '지역 인지도' AS label)
    , STRUCT('PRODUCT_CATALOG_SALES' AS code, '카탈로그 판매' AS label)
    , STRUCT('LEAD_GENERATION' AS code, '리드 생성' AS label)
    , STRUCT('BRAND_AWARENESS' AS code, '브랜드 인지도' AS label)
    , STRUCT('STORE_VISITS' AS code, '매장 방문' AS label)
    , STRUCT('REACH' AS code, '도달' AS label)
    , STRUCT('APP_INSTALLS' AS code, '앱 설치' AS label)
    , STRUCT('MESSAGES' AS code, '메시지' AS label)
  ])
),

effective_status_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT('ACTIVE' AS code, '활성' AS label)
    , STRUCT('PAUSED' AS code, '일시 중지' AS label)
    , STRUCT('DELETED' AS code, '삭제됨' AS label)
    , STRUCT('ARCHIVED' AS code, '보관됨' AS label)
    , STRUCT('PENDING_REVIEW' AS code, '검토 대기' AS label)
    , STRUCT('DISAPPROVED' AS code, '거부됨' AS label)
    , STRUCT('PREAPPROVED' AS code, '사전 승인' AS label)
    , STRUCT('PENDING_BILLING_INFO' AS code, '결제 정보 대기' AS label)
    , STRUCT('CAMPAIGN_PAUSED' AS code, '캠페인 일시 중지')
    , STRUCT('ADSET_PAUSED' AS code, '광고 세트 일시 중지' AS label)
    , STRUCT('IN_PROCESS' AS code, '처리 중' AS label)
    , STRUCT('WITH_ISSUES' AS code, '문제 발생' AS label)
  ])
),

master_report AS (
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

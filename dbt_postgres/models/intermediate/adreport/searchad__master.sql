WITH ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '네이버'
),{{var("line_break")

}} campaign_type_mapping AS (
  SELECT *
  FROM (
    VALUES
      (1, '파워링크')
    , (2, '쇼핑검색')
    , (3, '파워컨텐츠')
    , (4, '브랜드검색/신제품검색')
    , (5, '플레이스')
    , (101, '웹사이트 전환')
    , (102, '인지도 및 트래픽')
    , (103, '앱 전환')
    , (104, '동영상 조회')
    , (105, '카탈로그 판매')
    , (106, '쇼핑 프로모션')
    , (107, '참여 유도')
    , (108, 'ADVoost 쇼핑')
  ) AS _t(code, label)
),{{var("line_break")

}} adgroup_type_mapping AS (
  SELECT *
  FROM (
    VALUES
      (1, '파워링크')
    , (2, '쇼핑검색-쇼핑몰 상품형')
    , (3, '파워컨텐츠-정보형')
    , (4, '파워컨텐츠-상품형')
    , (5, '브랜드검색-일반형')
    , (6, '플레이스-지역소상공인')
    , (7, '쇼핑검색-제품 카탈로그형')
    , (8, '브랜드검색-브랜드형')
    , (9, '쇼핑검색-쇼핑 브랜드형')
    , (10, '플레이스-플레이스검색')
    , (11, '브랜드검색-신제품검색형')
    , (101, '성과형-클릭 수 최대화')
    , (102, '성과형-전환 수 최대화')
    , (103, '성과형-전환 가치 최대화')
    , (104, '성과형-수동 입찰')
  ) AS _t(code, label)
),{{var("line_break")

}} ad_type_mapping AS (
  SELECT *
  FROM (
    VALUES
      (1, '파워링크-단일형 소재')
    , (2, '쇼핑검색-상품형 소재')
    , (3, '파워컨텐츠-정보형 소재')
    , (4, '파워컨텐츠-상품형 소재')
    , (5, '브랜드검색-일반형 소재')
    , (6, '플레이스-지역소상공인 소재')
    , (7, '쇼핑검색-카탈로그형 소재')
    , (9, '쇼핑검색-쇼핑 브랜드형 소재')
    , (10, '플레이스-플레이스 검색 소재')
    , (11, '브랜드검색-신제품검색형 소재')
    , (12, '쇼핑검색-쇼핑 브랜드형 이미지 섬네일형 소재')
    , (13, '쇼핑검색-쇼핑 브랜드형 이미지 배너형 소재')
    , (101, '성과형-네이티브 이미지')
    , (102, '성과형-컬렉션')
    , (103, '성과형-동영상')
    , (104, '성과형-이미지 배너')
    , (105, '성과형-카탈로그')
    , (106, '성과형-ADVoost 소재')
  ) AS _t(code, label)
),{{var("line_break")

}} master_report AS (
  SELECT
      ad.customer_id
    , acc.account_name
    -- Campaign attrs
    , grp.campaign_id
    , cmp.campaign_name
    , campaign_type.label AS campaign_type
    -- Adgroup attrs
    , ad.adgroup_id
    , grp.adgroup_name
    , adgroup_type.label AS adgroup_type
    -- Ad attrs
    , ad.ad_id
    , ad_type.label AS ad_type
    , ad.product_id
    , ad.title
    , ad.description
    , COALESCE(ad.is_enabled AND grp.is_enabled AND cmp.is_enabled, false) AS is_enabled
    , (ad.deleted_at IS NOT NULL) OR (grp.deleted_at IS NOT NULL) OR (cmp.deleted_at IS NOT NULL) AS is_deleted
    , COALESCE(
          rel_prd.bundle_product_ids
        , rel_ad.bundle_product_ids
        , rel_grp.bundle_product_ids
        , rel_cmp.bundle_product_ids
        , '900000'
      ) AS bundle_product_ids
    , COALESCE(ad.created_at, grp.created_at, cmp.created_at) AS created_at
  FROM {{ source('searchad', 'ad') }} AS ad
  LEFT JOIN {{ source('searchad', 'account') }} AS acc
    ON ad.customer_id = acc.customer_id
  LEFT JOIN {{ source('searchad', 'adgroup') }} AS grp
    ON ad.adgroup_id = grp.adgroup_id
  LEFT JOIN {{ source('searchad', 'campaign') }} AS cmp
    ON grp.campaign_id = cmp.campaign_id
  -- Map codes to labels
  LEFT JOIN campaign_type_mapping AS campaign_type
    ON cmp.campaign_type = campaign_type.code
  LEFT JOIN adgroup_type_mapping AS adgroup_type
    ON grp.adgroup_type = adgroup_type.code
  LEFT JOIN ad_type_mapping AS ad_type
    ON ad.ad_type = ad_type.code
  -- Resolve bundle_product_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 0) AS rel_cmp
    ON grp.campaign_id = rel_cmp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 1) AS rel_grp
    ON ad.adgroup_id = rel_grp.ad_id
  LEFT JOIN (SELECT * FROM ad_id_to_sbn_ids WHERE ad_level = 2) AS rel_ad
    ON ad.ad_id = rel_ad.ad_id
  LEFT JOIN {{ source('relation', 'smt_prd_to_sbn_ids') }} AS rel_prd
    ON ad.product_id = rel_prd.product_id
)
SELECT * FROM master_report

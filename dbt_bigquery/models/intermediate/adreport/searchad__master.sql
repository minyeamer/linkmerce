WITH ad_id_to_sbn_ids AS (
  SELECT
      ad_id
    , ad_level
    , bundle_product_ids
  FROM {{ source('relation', 'ad_id_to_sbn_ids') }}
  WHERE platform_name = '네이버'
),

campaign_type_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT(1 AS code, '파워링크' AS label)
    , STRUCT(2 AS code, '쇼핑검색' AS label)
    , STRUCT(3 AS code, '파워컨텐츠' AS label)
    , STRUCT(4 AS code, '브랜드검색/신제품검색' AS label)
    , STRUCT(5 AS code, '플레이스' AS label)
    , STRUCT(101 AS code, '웹사이트 전환' AS label)
    , STRUCT(102 AS code, '인지도 및 트래픽' AS label)
    , STRUCT(103 AS code, '앱 전환' AS label)
    , STRUCT(104 AS code, '동영상 조회' AS label)
    , STRUCT(105 AS code, '카탈로그 판매' AS label)
    , STRUCT(106 AS code, '쇼핑 프로모션' AS label)
    , STRUCT(107 AS code, '참여 유도' AS label)
    , STRUCT(108 AS code, 'ADVoost 쇼핑' AS label)
  ])
),

adgroup_type_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT(1 AS code, '파워링크' AS label)
    , STRUCT(2 AS code, '쇼핑검색-쇼핑몰 상품형' AS label)
    , STRUCT(3 AS code, '파워컨텐츠-정보형' AS label)
    , STRUCT(4 AS code, '파워컨텐츠-상품형' AS label)
    , STRUCT(5 AS code, '브랜드검색-일반형' AS label)
    , STRUCT(6 AS code, '플레이스-지역소상공인' AS label)
    , STRUCT(7 AS code, '쇼핑검색-제품 카탈로그형' AS label)
    , STRUCT(8 AS code, '브랜드검색-브랜드형' AS label)
    , STRUCT(9 AS code, '쇼핑검색-쇼핑 브랜드형' AS label)
    , STRUCT(10 AS code, '플레이스-플레이스검색' AS label)
    , STRUCT(11 AS code, '브랜드검색-신제품검색형' AS label)
    , STRUCT(101 AS code, '성과형-클릭 수 최대화' AS label)
    , STRUCT(102 AS code, '성과형-전환 수 최대화' AS label)
    , STRUCT(103 AS code, '성과형-전환 가치 최대화' AS label)
    , STRUCT(104 AS code, '성과형-수동 입찰' AS label)
  ])
),

ad_type_mapping AS (
  SELECT *
  FROM UNNEST([
      STRUCT(1 AS code, '파워링크-단일형 소재' AS label)
    , STRUCT(2 AS code, '쇼핑검색-상품형 소재' AS label)
    , STRUCT(3 AS code, '파워컨텐츠-정보형 소재' AS label)
    , STRUCT(4 AS code, '파워컨텐츠-상품형 소재' AS label)
    , STRUCT(5 AS code, '브랜드검색-일반형 소재' AS label)
    , STRUCT(6 AS code, '플레이스-지역소상공인 소재' AS label)
    , STRUCT(7 AS code, '쇼핑검색-카탈로그형 소재' AS label)
    , STRUCT(9 AS code, '쇼핑검색-쇼핑 브랜드형 소재' AS label)
    , STRUCT(10 AS code, '플레이스-플레이스 검색 소재' AS label)
    , STRUCT(11 AS code, '브랜드검색-신제품검색형 소재' AS label)
    , STRUCT(12 AS code, '쇼핑검색-쇼핑 브랜드형 이미지 섬네일형 소재' AS label)
    , STRUCT(13 AS code, '쇼핑검색-쇼핑 브랜드형 이미지 배너형 소재' AS label)
    , STRUCT(101 AS code, '성과형-네이티브 이미지' AS label)
    , STRUCT(102 AS code, '성과형-컬렉션' AS label)
    , STRUCT(103 AS code, '성과형-동영상' AS label)
    , STRUCT(104 AS code, '성과형-이미지 배너' AS label)
    , STRUCT(105 AS code, '성과형-카탈로그' AS label)
    , STRUCT(106 AS code, '성과형-ADVoost 소재' AS label)
  ])
),

master_report AS (
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
    , COALESCE(ad.is_enabled, FALSE) AND COALESCE(grp.is_enabled, FALSE) AND COALESCE(cmp.is_enabled, FALSE) AS is_enabled
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

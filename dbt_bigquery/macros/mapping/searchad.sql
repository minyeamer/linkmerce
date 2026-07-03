{% macro searchad__campaign_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 1 AS code, '파워링크' AS label)
  , STRUCT(1 AS seq, 2 AS code, '쇼핑검색' AS label)
  , STRUCT(2 AS seq, 3 AS code, '파워컨텐츠' AS label)
  , STRUCT(3 AS seq, 4 AS code, '브랜드검색/신제품검색' AS label)
  , STRUCT(4 AS seq, 5 AS code, '플레이스' AS label)
  , STRUCT(5 AS seq, 101 AS code, '웹사이트 전환' AS label)
  , STRUCT(6 AS seq, 102 AS code, '인지도 및 트래픽' AS label)
  , STRUCT(7 AS seq, 103 AS code, '앱 전환' AS label)
  , STRUCT(8 AS seq, 104 AS code, '동영상 조회' AS label)
  , STRUCT(9 AS seq, 105 AS code, '카탈로그 판매' AS label)
  , STRUCT(10 AS seq, 106 AS code, '쇼핑 프로모션' AS label)
  , STRUCT(11 AS seq, 107 AS code, '참여 유도' AS label)
  , STRUCT(12 AS seq, 108 AS code, 'ADVoost 쇼핑' AS label)
])
{%- endmacro %}

-- fallback mapping for campaign ads
{% macro searchad__campaign_ad_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 1 AS code, '파워링크-단일형 소재' AS label)
  , STRUCT(1 AS seq, 2 AS code, '쇼핑검색-상품형 소재' AS label)
  , STRUCT(2 AS seq, 3 AS code, '파워컨텐츠-정보형 소재' AS label)
  , STRUCT(3 AS seq, 4 AS code, '브랜드검색-일반형 소재' AS label)
  , STRUCT(4 AS seq, 5 AS code, '플레이스-플레이스 검색 소재' AS label)
  , STRUCT(5 AS seq, 101 AS code, '웹사이트 전환' AS label)
  , STRUCT(6 AS seq, 102 AS code, '성과형-기타' AS label)
  , STRUCT(7 AS seq, 103 AS code, '성과형-기타' AS label)
  , STRUCT(8 AS seq, 104 AS code, '성과형-동영상' AS label)
  , STRUCT(9 AS seq, 105 AS code, '성과형-카탈로그' AS label)
  , STRUCT(10 AS seq, 106 AS code, '성과형-기타' AS label)
  , STRUCT(11 AS seq, 107 AS code, '성과형-기타' AS label)
  , STRUCT(12 AS seq, 108 AS code, '성과형-ADVoost 소재' AS label)
])
{%- endmacro %}

{% macro searchad__adgroup_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 1 AS code, '파워링크' AS label)
  , STRUCT(1 AS seq, 2 AS code, '쇼핑검색-쇼핑몰 상품형' AS label)
  , STRUCT(2 AS seq, 3 AS code, '파워컨텐츠-정보형' AS label)
  , STRUCT(3 AS seq, 4 AS code, '파워컨텐츠-상품형' AS label)
  , STRUCT(4 AS seq, 5 AS code, '브랜드검색-일반형' AS label)
  , STRUCT(5 AS seq, 6 AS code, '플레이스-지역소상공인' AS label)
  , STRUCT(6 AS seq, 7 AS code, '쇼핑검색-제품 카탈로그형' AS label)
  , STRUCT(7 AS seq, 8 AS code, '브랜드검색-브랜드형' AS label)
  , STRUCT(8 AS seq, 9 AS code, '쇼핑검색-쇼핑 브랜드형' AS label)
  , STRUCT(9 AS seq, 10 AS code, '플레이스-플레이스검색' AS label)
  , STRUCT(10 AS seq, 11 AS code, '브랜드검색-신제품검색형' AS label)
  , STRUCT(11 AS seq, 101 AS code, '성과형-클릭 수 최대화' AS label)
  , STRUCT(12 AS seq, 102 AS code, '성과형-전환 수 최대화' AS label)
  , STRUCT(13 AS seq, 103 AS code, '성과형-전환 가치 최대화' AS label)
  , STRUCT(14 AS seq, 104 AS code, '성과형-수동 입찰' AS label)
])
{%- endmacro %}

{% macro searchad__ad_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 1 AS code, '파워링크-단일형 소재' AS label)
  , STRUCT(1 AS seq, 2 AS code, '쇼핑검색-상품형 소재' AS label)
  , STRUCT(2 AS seq, 3 AS code, '파워컨텐츠-정보형 소재' AS label)
  , STRUCT(3 AS seq, 4 AS code, '파워컨텐츠-상품형 소재' AS label)
  , STRUCT(4 AS seq, 5 AS code, '브랜드검색-일반형 소재' AS label)
  , STRUCT(5 AS seq, 6 AS code, '플레이스-지역소상공인 소재' AS label)
  , STRUCT(6 AS seq, 7 AS code, '쇼핑검색-카탈로그형 소재' AS label)
  , STRUCT(8 AS seq, 9 AS code, '쇼핑검색-쇼핑 브랜드형 소재' AS label)
  , STRUCT(9 AS seq, 10 AS code, '플레이스-플레이스 검색 소재' AS label)
  , STRUCT(10 AS seq, 11 AS code, '브랜드검색-신제품검색형 소재' AS label)
  , STRUCT(11 AS seq, 12 AS code, '쇼핑검색-쇼핑 브랜드형 이미지 섬네일형 소재' AS label)
  , STRUCT(12 AS seq, 13 AS code, '쇼핑검색-쇼핑 브랜드형 이미지 배너형 소재' AS label)
  , STRUCT(13 AS seq, 101 AS code, '성과형-네이티브 이미지' AS label)
  , STRUCT(14 AS seq, 102 AS code, '성과형-컬렉션' AS label)
  , STRUCT(15 AS seq, 103 AS code, '성과형-동영상' AS label)
  , STRUCT(16 AS seq, 104 AS code, '성과형-이미지 배너' AS label)
  , STRUCT(17 AS seq, 105 AS code, '성과형-카탈로그' AS label)
  , STRUCT(18 AS seq, 106 AS code, '성과형-ADVoost 소재' AS label)
])
{%- endmacro %}

{% macro searchad__device_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, 'PC' AS label)
  , STRUCT(1 AS seq, 1 AS code, '모바일' AS label)
  , STRUCT(2 AS seq, 2 AS code, '기타' AS label)
  , STRUCT(9 AS seq, 9 AS code, '성과형' AS label)
])
{%- endmacro %}

{% macro searchad__contract_type_mapping() -%}
SELECT *
FROM UNNEST([
    STRUCT(0 AS seq, 0 AS code, '브랜드검색-신제품검색형 소재' AS label)
  , STRUCT(1 AS seq, 1 AS code, '브랜드검색-일반형 소재' AS label)
])
{%- endmacro %}

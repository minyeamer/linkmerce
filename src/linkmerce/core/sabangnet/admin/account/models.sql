-- Account: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    account_no INTEGER NOT NULL
  , shop_id VARCHAR NOT NULL
  , shop_name VARCHAR
  , shop_group VARCHAR
  , userid VARCHAR
  , passwd VARCHAR
  , shop_seq INTEGER
  , shop_url VARCHAR
  , scm_url  VARCHAR
  , corp_name VARCHAR
  , use_yn VARCHAR
  , PRIMARY KEY (account_no)
);

-- Account: bulk_insert
INSERT INTO {{ table }}
SELECT
    acntRegsSrno AS account_no
  , shmaId AS shop_id
  , shmaNm AS shop_name
  -- , olMktTydvsDivCd AS shop_group_code
  , olMktTydvsDivNm AS shop_group
  , shmaCnctnLoginId AS userid
  , ecptPwd AS passwd
  , sortSrno AS shop_seq
  , shmaUrlAddr AS shop_url
  , scmUrlAddr AS scm_url
  , corpNm AS corp_name
  , useYn AS use_yn
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- ShopNormal: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    shop_id VARCHAR NOT NULL
  , shop_name VARCHAR
  , shop_group VARCHAR
  , shop_seq INTEGER
  , rep_name VARCHAR
  , use_yn VARCHAR
  , created_at TIMESTAMP
  , PRIMARY KEY (shop_id)
);

-- ShopNormal: bulk_insert
INSERT INTO {{ table }}
SELECT
    shmaId AS shop_id
  , shmaNm AS shop_name
  -- , olMktTydvsDivCd AS shop_group_code
  , olMktTydvsDivNm AS shop_group
  , exclFormSrno AS shop_seq
  , rpstNm AS rep_name
  , shmaExpoYn AS use_yn
  , TRY_STRPTIME(SUBSTR(fstRegsDt, 1, 19), '%Y-%m-%dT%H:%M:%S') AS created_at
FROM {{ rows }}
ON CONFLICT DO NOTHING;


-- AccountNormal: create
CREATE TABLE IF NOT EXISTS {{ table }} (
    account_no INTEGER NOT NULL
  , shop_id VARCHAR NOT NULL
  , shop_name VARCHAR -- NULL
  , shop_group VARCHAR -- NULL
  , userid VARCHAR -- NULL
  , passwd VARCHAR -- NULL
  , shop_seq INTEGER -- NULL
  , shop_url VARCHAR -- NULL
  , scm_url  VARCHAR -- NULL
  , corp_name VARCHAR -- NULL
  , use_yn VARCHAR -- NULL
  , PRIMARY KEY (account_no)
);

-- AccountNormal: bulk_insert
INSERT INTO {{ table }}
SELECT
    acntRegsSrno AS account_no
  , shmaId AS shop_id
  , NULL AS shop_name
  , NULL AS shop_group
  , NULL AS userid
  , NULL AS passwd
  , NULL AS shop_seq
  , NULL AS shop_url
  , NULL AS scm_url
  , NULL AS corp_name
  , NULL AS use_yn
FROM {{ rows }}
ON CONFLICT DO NOTHING;
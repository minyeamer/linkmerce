{{
  config(
    materialized = 'view',
    schema = 'relation',
    alias = 'acc_no_to_corp_name'
  )
}}

SELECT
    shop_id
  , CAST(account_no AS STRING) AS account_no
  , corp_name
FROM {{ source('sabangnet', 'account') }}

UNION ALL

SELECT
    shop_id
  , CAST(channel_seq AS STRING) AS account_no
  , corp_name
FROM {{ source('smartstore', 'channel') }}
CROSS JOIN UNNEST(['shop9000', 'shop0055']) AS shop_id

UNION ALL

SELECT
    'shop9001' AS shop_id
  , vendor_id AS account_no
  , corp_name
FROM {{ source('coupang', 'vendor') }}

UNION ALL

SELECT
    shop_id
  , userid AS account_no
  , corp_name
FROM {{ source('sabangnet', 'account') }}
WHERE shop_id IN ('shop0067', 'shop0068')
  AND userid IN (SELECT DISTINCT seller_id FROM {{ source('ebay', 'item') }})

UNION ALL

SELECT
    shop_id
  , CAST(customer_id AS STRING) AS account_no
  , corp_name
FROM {{ source('searchad', 'account') }}
CROSS JOIN UNNEST(['shop9000', 'shop0055']) AS shop_id

UNION ALL

SELECT
    'adop0001' AS shop_id
  , CAST(customer_id AS STRING) AS account_no
  , corp_name
FROM {{ source('google_ads', 'account') }}

UNION ALL

SELECT
    'adop0002' AS shop_id
  , account_id AS account_no
  , corp_name
FROM {{ source('meta_ads', 'account') }}

UNION ALL

SELECT
    'adop0009' AS shop_id
  , CAST(account_no AS STRING) AS account_no
  , corp_name
FROM {{ source('sabangnet', 'account') }}
WHERE account_no = 5000837

UNION ALL

SELECT
    'adop0010' AS shop_id
  , CAST(space_id AS STRING) AS account_no
  , corp_name
FROM {{ source('naver_connect', 'space') }}

UNION ALL

SELECT
    'adop0006' AS shop_id
  , CAST(account_no AS STRING) AS account_no
  , corp_name
FROM {{ source('sabangnet', 'account') }}
WHERE account_no = 5043630

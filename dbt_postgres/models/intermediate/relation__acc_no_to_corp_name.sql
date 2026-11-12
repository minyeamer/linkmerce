{{
  config(
    materialized = 'view',
    schema = 'relation',
    alias = 'acc_no_to_corp_name'
  )
}}

SELECT
    shop.shop_id
  , channel_seq::text AS account_no
  , corp_name
FROM {{ source('smartstore', 'channel') }}
CROSS JOIN (VALUES ('chop0029'), ('shop0055')) AS shop(shop_id){#

#} UNION ALL{#

#} SELECT
    shop.shop_id
  , vendor_id AS account_no
  , corp_name
FROM {{ source('coupang', 'vendor') }}{#

#} CROSS JOIN (VALUES ('shop0075'), ('chop0027')) AS shop(shop_id){#

#} UNION ALL{#

#} SELECT
    IF(site_type = 1, 'shop0067', 'shop0068') AS shop_id
  , seller_id AS account_no
  , corp_name
FROM {{ source('ebay', 'seller') }}{#

#} UNION ALL{#

#} SELECT
    shop.shop_id
  , customer_id::text AS account_no
  , corp_name
FROM {{ source('searchad', 'account') }}
CROSS JOIN (VALUES ('chop0029'), ('shop0055')) AS shop(shop_id){#

#} UNION ALL{#

#} SELECT
    'adop0001' AS shop_id
  , customer_id::text AS account_no
  , corp_name
FROM {{ source('google_ads', 'account') }}{#

#} UNION ALL{#

#} SELECT
    'adop0002' AS shop_id
  , account_id AS account_no
  , corp_name
FROM {{ source('meta_ads', 'account') }}{#

#} UNION ALL{#

#} SELECT
    'adop0009' AS shop_id
  , account_no::text AS account_no
  , corp_name
FROM {{ source('sabangnet', 'account') }}
WHERE account_no = 5000837{#

#} UNION ALL{#

#} SELECT
    'adop0010' AS shop_id
  , space_id::text AS account_no
  , corp_name
FROM {{ source('naver_connect', 'space') }}{#

#} UNION ALL{#

#} SELECT
    'adop0006' AS shop_id
  , account_no::text AS account_no
  , corp_name
FROM {{ source('sabangnet', 'account') }}
WHERE account_no = 5043630
